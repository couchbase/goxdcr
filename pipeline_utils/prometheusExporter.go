// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package pipeline_utils

import (
	"bytes"
	"expvar"
	"fmt"
	"strings"
	"sync"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
	utilities "github.com/couchbase/goxdcr/v8/utils"
)

type ExpVarExporter interface {
	LoadExpVarMap(m *expvar.Map) bool
	LoadSourceClustersInfoV1(srcClusterNames map[string]string, srcSpecs map[string][]*metadata.ReplicationSpecification, srcNodes map[string][]string)
	Export() ([]byte, error)
}

var PrometheusTargetClusterUuidBytes = []byte(service_def.PrometheusTargetClusterUuidLabel)
var PrometheusSourceBucketBytes = []byte(service_def.PrometheusSourceBucketLabel)
var PrometheusTargetBucketBytes = []byte(service_def.PrometheusTargetBucketLabel)
var PrometheusPipelineTypeBytes = []byte(service_def.PrometheusPipelineTypeLabel)
var PrometheusSourceClusterUuidBytes = []byte(service_def.PrometheusSourceClusterUUIDLabel)
var PrometheusSourceClusterNameBytes = []byte(service_def.PrometheusSourceClusterNameLabel)

type PrometheusExporter struct {
	// Read only
	globalLookupMap service_def.StatisticsPropertyMap
	// Each metric should have a #HELP section to output the descriptions
	globalMetricHelpMap map[string][]byte
	// Each metric should have a key for outputting
	globalMetricKeyMap map[string][]byte
	// Each metric has a "TYPE" field that should be constant
	globalMetricTypeMap map[string][]byte

	// Prometheus expects the stats to be under certain metrics name
	// Each metric name will contain one or more data depending on the number of current replications
	// Thus, each metric will take up one entry in this metricsMap
	// Each Replication will populate the stats under the IdentifierStatsMap
	metricsMap MetricsMapType

	mapsMtx        sync.RWMutex
	expVarParseMap ExpVarParseMapType

	// These are buffers used for outputting

	// Internally allocated buffer of bytes
	// Only to be modified by Export
	outputBuffer    []byte
	outputBufferMtx sync.Mutex

	utils utilities.UtilsIface

	labelsTableConstructor PrometheusLabelsConstructorType

	// Map used to keep track of known sources and to know when one drop off the map
	// Key is the source stat key - and value is the source identifier
	lastKnownIdentifiers map[string]base.StringList

	srcNodes map[string][]string
	srcSpecs map[string][]*metadata.ReplicationSpecification
}

func NewPrometheusExporter(translationMap service_def.StatisticsPropertyMap, labelsTableConstructor PrometheusLabelsConstructorType) *PrometheusExporter {
	prom := &PrometheusExporter{
		globalLookupMap:        translationMap,
		globalMetricHelpMap:    make(map[string][]byte),
		globalMetricKeyMap:     make(map[string][]byte),
		globalMetricTypeMap:    make(map[string][]byte),
		metricsMap:             make(MetricsMapType),
		expVarParseMap:         make(ExpVarParseMapType),
		outputBuffer:           make([]byte, 0),
		utils:                  utilities.NewUtilities(),
		labelsTableConstructor: labelsTableConstructor,
		lastKnownIdentifiers:   map[string]base.StringList{},
	}

	for k, statsProperty := range prom.globalLookupMap {
		promName, err := prom.globalLookupMap.GetPrometheusMetricName(k)
		if err != nil {
			panic("FIXME")
		}
		typeStr, err := prom.globalLookupMap.GetPrometheusMetricType(k)
		if err != nil {
			panic("FIXME")
		}
		prom.globalMetricKeyMap[k] = []byte(promName)
		prom.globalMetricHelpMap[k] = compilePrometheusHelpHeader(promName, statsProperty.Description)
		prom.globalMetricTypeMap[k] = compilePrometheusTypeHeader(promName, typeStr)
	}

	return prom
}

// Key is the metric name - and value is a map of identifiers + stat per identifier
type MetricsMapType map[string]IdentifierStatsMap

func (m *MetricsMapType) RecordStat(replicationId, statsConst string, value interface{}, lookupMap service_def.StatisticsPropertyMap, constructStatsTable PrometheusLabelsConstructorType) {
	// If the statsConst is not part of the initialization, then it is not meant to be exported
	replicationStatsMap, constExists := (*m)[statsConst]
	if !constExists {
		return
	}

	_, replExists := replicationStatsMap[replicationId]
	if !replExists {
		statsProperty := lookupMap.GetStatsProperty(statsConst)
		labelsTable := constructStatsTable(statsConst) // could be nil, depending on statsConst
		replicationStatsMap[replicationId] = NewPerReplicationStatType(statsProperty, labelsTable)
	}

	// Update the value
	replicationStatsMap[replicationId].SetValue(value)

	// If Prometheus labels exist, let them load the value
	labelsTable := replicationStatsMap[replicationId].GetLabelsTable()
	if labelsTable != nil {
		for _, extractor := range labelsTable {
			extractor.LoadValue(value)
		}
	}
}

var sourceClusterStats = []string{service_def.SOURCE_CLUSTER_NUM_REPL, service_def.SOURCE_CLUSTER_NUM_NODES}

func (m *MetricsMapType) RecordSourceClusterV1(clusterNameIn map[string]string, specsIn map[string][]*metadata.ReplicationSpecification, nodesIn map[string][]string, lookupMap service_def.StatisticsPropertyMap, constructStatsTable PrometheusLabelsConstructorType) {
	constValueMap := make(map[string]map[string]int)
	specsCountMap := make(map[string]int)
	nodesCountMap := make(map[string]int)
	runningSpecCountMap := make(map[string]int)
	pausedSpecCountMap := make(map[string]int)
	for uuid, specs := range specsIn {
		specsCountMap[uuid] = len(specs)
		nodesCountMap[uuid] = len(nodesIn[uuid])
		var runningCnt int
		var pausedCnt int
		for _, spec := range specs {
			if spec.Settings.Active {
				runningCnt++
			} else {
				pausedCnt++
			}
		}
		runningSpecCountMap[uuid] = runningCnt
		pausedSpecCountMap[uuid] = pausedCnt
	}

	constValueMap[service_def.SOURCE_CLUSTER_NUM_REPL] = specsCountMap
	constValueMap[service_def.SOURCE_CLUSTER_NUM_NODES] = nodesCountMap

	for _, oneStat := range sourceClusterStats {
		// As a reminder:
		// m -> key is stat (i.e. numRepl, numNodes)
		//   -> value is a map where:
		//         -> key is either replication ID or source clusterUUID
		//         -> value is IdentifierStatsMap, a KV pair for a specific stat
		srcClusterUUIDToStatsMap, exists := (*m)[oneStat]
		if !exists {
			srcClusterUUIDToStatsMap = IdentifierStatsMap{}
			(*m)[oneStat] = srcClusterUUIDToStatsMap
		}

		for srcClusterUuid, valueToStore := range constValueMap[oneStat] {
			_, srcClusterExists := srcClusterUUIDToStatsMap[srcClusterUuid]
			if !srcClusterExists {
				statsProperty := lookupMap[oneStat]
				labelsTable := constructStatsTable(oneStat)
				srcClusterUUIDToStatsMap[srcClusterUuid] = NewPerSourceClusterStatsType(statsProperty, labelsTable, []byte(oneStat), srcClusterUuid, clusterNameIn[srcClusterUuid])
			}

			srcClusterUUIDToStatsMap[srcClusterUuid].SetValue(valueToStore)

			labelsTable := srcClusterUUIDToStatsMap[srcClusterUuid].GetLabelsTable()
			if labelsTable != nil {
				for runningOrPaused, extractor := range labelsTable {
					if runningOrPaused == base.PipelineStatusPaused.String() {
						extractor.LoadValue(pausedSpecCountMap[srcClusterUuid])
					} else if runningOrPaused == base.PipelineStatusRunning.String() {
						extractor.LoadValue(runningSpecCountMap[srcClusterUuid])
					}
				}
			}
		}

		// Garbage collect any non-existant source UUIDs
		for srcClusterUuid, _ := range srcClusterUUIDToStatsMap {
			var gcNeeded = true
			for currentSourceUuid, _ := range nodesIn {
				if srcClusterUuid == currentSourceUuid {
					gcNeeded = false
					break
				}
			}
			if gcNeeded {
				// nodesIn for this sourceUUID is non existent - there's no way there's a replication coming in if there is no source KV nodes
				delete(srcClusterUUIDToStatsMap, srcClusterUuid)
			}
		}
	}
}

type ExpVarParseMapType map[string]interface{}

// Returns true if all the keys match, and the types all match
func (e ExpVarParseMapType) CheckNoKeyChanges(varMap *expvar.Map) bool {
	var missingKey bool
	var inconsistentType bool
	var subLevelCheckPasses = true
	var keyCount int
	keyLen := len(e)

	varMap.Do(func(kv expvar.KeyValue) {
		keyCount++
		eVal, exists := e[kv.Key]
		if !exists {
			missingKey = true
			return
		}
		subMap, isSubMap := kv.Value.(*expvar.Map)
		eSubMap, isSubMap2 := eVal.(ExpVarParseMapType)
		if isSubMap {
			if !isSubMap2 {
				inconsistentType = true
				return
			}
			subLevelCheckPasses = eSubMap.CheckNoKeyChanges(subMap)
			if !subLevelCheckPasses {
				return
			}
		}
	})

	// # of keys at this level should match
	// Keys have to exists all at this level
	// Any submaps must adhere to the same conditions
	return keyLen == keyCount && !missingKey && subLevelCheckPasses && !inconsistentType
}

type IdentifierStatsMap map[string]StatsPerExportedConst

type StatsPerExportedConst interface {
	SetValue(value interface{})
	GetValue() interface{}
	GetLabelsTable() PrometheusLabels
	GetOutputBuffer() []byte
	UpdateOutputBuffer(metricName []byte, id string)
	ResetBuffer()
}

type PerItemStatTypeCommon struct {
	Properties   service_def.StatsProperty
	Value        interface{}
	OutputBuffer []byte

	LabelsTable           PrometheusLabels
	IndividualLabelBuffer map[string]*[]byte // Uses PrometheusLabels key
}

// Prometheus only stores numerical values - ns_server request them to be in float64
type PerReplicationStatType struct {
	PerItemStatTypeCommon

	ReplIdDecompositionStruct *metadata.ReplIdComposition
}

func (t *PerItemStatTypeCommon) GetValue() interface{} {
	return t.Value
}

func (t *PerItemStatTypeCommon) GetOutputBuffer() []byte {
	return t.OutputBuffer
}

func (t *PerItemStatTypeCommon) GetLabelsTable() PrometheusLabels {
	return t.LabelsTable
}

func (t *PerItemStatTypeCommon) SetValue(value interface{}) {
	t.Value = value
}

// This call isn't supported really
func (t *PerReplicationStatType) ResetBuffer() {
	// do nothing
	return
}

func (t *PerReplicationStatType) UpdateOutputBuffer(metricName []byte, replId string) {
	// Output looks like:
	// metric_name {label_name=\"<labelVal>\", ...} <value>
	// Ends with a newline, but won't output it here
	t.resetBuffers()

	if t.LabelsTable != nil {
		// If this stats contains multiple labels, then go through each label and compose a single line for each label
		// The stat name would then be composed of multiple lines, each with the same label NAME but diff labels
		// For example:
		// xdcr_pipeline_status {targetClusterUUID="...", sourceBucketName="B1", targetBucketName="B2", pipelineType="Backfill", status="Paused"} 1
		// xdcr_pipeline_status {targetClusterUUID="...", sourceBucketName="B1", targetBucketName="B2", pipelineType="Backfill", status="Running"} 0
		for customLabelName, extractor := range t.LabelsTable {
			t.appendMetricName(metricName, t.IndividualLabelBuffer[customLabelName])
			t.appendStandardLabels(replId, t.IndividualLabelBuffer[customLabelName])
			t.appendCustomLabels(t.IndividualLabelBuffer[customLabelName], extractor)
			t.appendBufferWithValue(t.IndividualLabelBuffer[customLabelName], extractor.GetValueBaseUnit)
		}
		t.composeFinalOutputBufferFromLabelBuffers()
	} else {
		t.appendMetricName(metricName, &t.OutputBuffer)
		t.appendStandardLabels(replId, &t.OutputBuffer)
		t.appendBufferWithValue(&t.OutputBuffer, t.GetValueBaseUnit)
	}
}

func (t *PerItemStatTypeCommon) resetBuffers() {
	t.OutputBuffer = t.OutputBuffer[:0]
	for _, oneLabelBuffer := range t.IndividualLabelBuffer {
		*oneLabelBuffer = (*oneLabelBuffer)[:0]
	}
}

func (t *PerItemStatTypeCommon) appendMetricName(metricName []byte, buffer *[]byte) {
	*buffer = append(*buffer, metricName...)
}

var endBracketWithSpace = []byte("} ")
var endBracketWithSpaceStr = string(endBracketWithSpace)

func (t *PerReplicationStatType) appendStandardLabels(replId string, buffer *[]byte) {
	// {
	*buffer = append(*buffer, []byte(" {")...)

	t.ReplIdDecompositionStruct = metadata.DecomposeReplicationId(replId, t.ReplIdDecompositionStruct)

	// { targetClusterUUID="abcdef",
	*buffer = append(*buffer, PrometheusTargetClusterUuidBytes...)
	*buffer = append(*buffer, []byte("=\"")...)
	*buffer = append(*buffer, []byte(t.ReplIdDecompositionStruct.TargetClusterUUID)...)
	*buffer = append(*buffer, []byte("\", ")...)

	// { targetClusterUUID="abcdef", sourceBucketName="b1",
	*buffer = append(*buffer, PrometheusSourceBucketBytes...)
	*buffer = append(*buffer, []byte("=\"")...)
	*buffer = append(*buffer, []byte(t.ReplIdDecompositionStruct.SourceBucketName)...)
	*buffer = append(*buffer, []byte("\", ")...)

	// { targetClusterUUID="abcdef", sourceBucketName="b1", targetBucketName="b2",
	*buffer = append(*buffer, PrometheusTargetBucketBytes...)
	*buffer = append(*buffer, []byte("=\"")...)
	*buffer = append(*buffer, []byte(t.ReplIdDecompositionStruct.TargetBucketName)...)
	*buffer = append(*buffer, []byte("\", ")...)

	// { targetClusterUUID="abcdef", sourceBucketName="b1", targetBucketName="b2", pipelineType="Main"
	*buffer = append(*buffer, PrometheusPipelineTypeBytes...)
	*buffer = append(*buffer, []byte("=\"")...)
	*buffer = append(*buffer, []byte(t.ReplIdDecompositionStruct.PipelineType)...)
	*buffer = append(*buffer, []byte("\"")...)

	// { targetClusterUUID="abcdef", sourceBucketName="b1", targetBucketName="b2", pipelineType="Main"}
	*buffer = append(*buffer, endBracketWithSpace...)
}

func (t *PerItemStatTypeCommon) appendCustomLabels(buffer *[]byte, extractor LabelsValuesExtractor) {
	// Incoming from previous:
	// { targetClusterUUID="abcdef", sourceBucketName="b1", targetBucketName="b2", pipelineType="Main"}
	*buffer = bytes.Trim(*buffer, endBracketWithSpaceStr)

	// { targetClusterUUID="abcdef", sourceBucketName="b1", targetBucketName="b2", pipelineType="Main"
	*buffer = append(*buffer, []byte(", ")...)

	// { targetClusterUUID="abcdef", sourceBucketName="b1", targetBucketName="b2", pipelineType="Main",
	*buffer = append(*buffer, extractor.GetLabelName()...)

	// { targetClusterUUID="abcdef", sourceBucketName="b1", targetBucketName="b2", pipelineType="Main", status="running"
	*buffer = append(*buffer, endBracketWithSpace...)
}

func (t *PerItemStatTypeCommon) GetValueBaseUnit() interface{} {
	switch t.Properties.MetricType.Unit {
	case service_def.StatsMgrNonCumulativeNoUnit:
		fallthrough
	case service_def.StatsMgrNoUnit:
		return t.Value
	case service_def.StatsMgrSeconds:
		return t.Value
	case service_def.StatsMgrMilliSecond:
		if msInt, ok := t.Value.(int64); ok {
			return float64(msInt) / float64(1000)
		} else if msFloat, ok := t.Value.(float64); ok {
			return msFloat / float64(1000)
		} else {
			panic("FIXME")
		}
	case service_def.StatsMgrGolangTimeDuration:
		// Golang time duration is represented in nano seconds
		if nsInt, ok := t.Value.(int64); ok {
			return float64(nsInt) / 1e9
		} else if nsFloat, ok := t.Value.(float64); ok {
			return nsFloat / 1e9
		} else {
			panic("FIXME")
		}
	case service_def.StatsMgrBytes:
		return t.Value
	case service_def.StatsMgrMegaBytesPerSecond:
		if MBpsIntVal, ok := t.Value.(int64); ok {
			return float64(MBpsIntVal) / float64(1024*1024)
		} else if MBpsVal, ok := t.Value.(float64); ok {
			return MBpsVal / float64(1024*1024)
		} else {
			panic("FIXME")
		}
	case service_def.StatsMgrDocsPerSecond:
		return t.Value
	default:
		panic("Need to implement")
	}
}

// For specific stats that may require precision, print with precision
func (t *PerItemStatTypeCommon) appendBufferWithValue(buffer *[]byte, getValueBaseUnit func() interface{}) {
	switch t.Properties.MetricType.Unit {
	case service_def.StatsMgrNonCumulativeNoUnit:
		fallthrough
	case service_def.StatsMgrNoUnit:
		*buffer = append(*buffer, []byte(fmt.Sprintf("%v", getValueBaseUnit()))...)
	case service_def.StatsMgrSeconds:
		*buffer = append(*buffer, []byte(fmt.Sprintf("%v", getValueBaseUnit()))...)
	case service_def.StatsMgrMilliSecond:
		*buffer = append(*buffer, []byte(fmt.Sprintf("%g", getValueBaseUnit()))...)
	case service_def.StatsMgrGolangTimeDuration:
		*buffer = append(*buffer, []byte(fmt.Sprintf("%g", getValueBaseUnit()))...)
	case service_def.StatsMgrBytes:
		*buffer = append(*buffer, []byte(fmt.Sprintf("%v", getValueBaseUnit()))...)
	case service_def.StatsMgrMegaBytesPerSecond:
		*buffer = append(*buffer, []byte(fmt.Sprintf("%g", getValueBaseUnit()))...)
	case service_def.StatsMgrDocsPerSecond:
		*buffer = append(*buffer, []byte(fmt.Sprintf("%v", getValueBaseUnit()))...)
	default:
		panic("Need to implement")
	}
}

func (t *PerItemStatTypeCommon) composeFinalOutputBufferFromLabelBuffers() {
	totalLen := len(t.IndividualLabelBuffer)
	i := 0

	for _, oneLabelBuffer := range t.IndividualLabelBuffer {
		t.OutputBuffer = append(t.OutputBuffer, *oneLabelBuffer...)
		if i < totalLen-1 {
			t.OutputBuffer = append(t.OutputBuffer, fmt.Sprintf("\n")...)
		}
		i++
	}
}

func NewPerReplicationStatType(properties service_def.StatsProperty, labelsTable PrometheusLabels) *PerReplicationStatType {
	obj := &PerReplicationStatType{
		PerItemStatTypeCommon: PerItemStatTypeCommon{
			Properties:            properties,
			Value:                 nil,
			LabelsTable:           labelsTable,
			IndividualLabelBuffer: map[string]*[]byte{},
		},
	}

	for k, _ := range labelsTable {
		oneSlice := make([]byte, 0)
		obj.IndividualLabelBuffer[k] = &oneSlice
	}
	return obj
}

type PerSourceClusterStatsType struct {
	PerItemStatTypeCommon

	StatsNameToOutput []byte
	SourceClusterUuid string
	SourceClusterName string
}

func (p *PerSourceClusterStatsType) UpdateOutputBuffer(metricKey []byte, srcUuid string) {
	p.resetBuffers()

	if p.LabelsTable != nil {
		for customLabelName, extractor := range p.LabelsTable {
			p.appendMetricName(metricKey, p.IndividualLabelBuffer[customLabelName])
			p.appendStandardLabels(p.IndividualLabelBuffer[customLabelName])
			p.appendCustomLabels(p.IndividualLabelBuffer[customLabelName], extractor)
			p.appendBufferWithValue(p.IndividualLabelBuffer[customLabelName], extractor.GetValueBaseUnit)
		}
		p.composeFinalOutputBufferFromLabelBuffers()
	} else {
		p.appendMetricName(metricKey, &p.OutputBuffer)
		p.appendStandardLabels(&p.OutputBuffer)
		p.appendBufferWithValue(&p.OutputBuffer, p.GetValueBaseUnit)
	}
}

func (p *PerSourceClusterStatsType) appendStandardLabels(buffer *[]byte) {
	// {
	*buffer = append(*buffer, []byte(" {")...)

	// {sourceClusterUUID="abcdef",
	*buffer = append(*buffer, PrometheusSourceClusterUuidBytes...)
	*buffer = append(*buffer, []byte("=\"")...)
	*buffer = append(*buffer, []byte(p.SourceClusterUuid)...)
	*buffer = append(*buffer, []byte("\", ")...)

	// {sourceClusterUUID="abcdef", sourceClusterName="wxyz"
	*buffer = append(*buffer, PrometheusSourceClusterNameBytes...)
	*buffer = append(*buffer, []byte("=\"")...)
	*buffer = append(*buffer, []byte(p.SourceClusterName)...)
	*buffer = append(*buffer, []byte("\"")...)

	// {sourceClusterUUID="abcdef", sourceClusterName="wxyz"}
	*buffer = append(*buffer, endBracketWithSpace...)
}

func (p *PerSourceClusterStatsType) ResetBuffer() {
	p.resetBuffers()
}

func NewPerSourceClusterStatsType(properties service_def.StatsProperty, labelsTable PrometheusLabels, keyBytesCache []byte, uuid, clusterName string) *PerSourceClusterStatsType {
	obj := &PerSourceClusterStatsType{
		PerItemStatTypeCommon: PerItemStatTypeCommon{
			Properties:            properties,
			Value:                 nil,
			LabelsTable:           labelsTable,
			IndividualLabelBuffer: map[string]*[]byte{},
		},
		StatsNameToOutput: keyBytesCache,
		SourceClusterUuid: uuid,
		SourceClusterName: clusterName,
	}
	for k, _ := range labelsTable {
		oneSlice := make([]byte, 0)
		obj.IndividualLabelBuffer[k] = &oneSlice
	}
	return obj
}

// # HELP http_requests_total The total number of HTTP requests.
func compilePrometheusHelpHeader(key, description string) []byte {
	var compileStrings []string
	compileStrings = append(compileStrings, "# HELP ")
	compileStrings = append(compileStrings, fmt.Sprintf("%v ", key))
	compileStrings = append(compileStrings, description)
	// If description does not end with a period, add it
	if !strings.HasSuffix(description, ".") {
		compileStrings = append(compileStrings, ".")
	}

	finalString := strings.Join(compileStrings, "")
	return []byte(finalString)
}

func compilePrometheusTypeHeader(key string, metric string) []byte {
	var compileStrings []string
	compileStrings = append(compileStrings, "# TYPE ")
	compileStrings = append(compileStrings, fmt.Sprintf("%v ", key))
	compileStrings = append(compileStrings, metric)

	finalString := strings.Join(compileStrings, "")
	return []byte(finalString)
}

func parseExpMap(varMap *expvar.Map, targetMap ExpVarParseMapType) {
	varMap.Do(func(kv expvar.KeyValue) {
		if kvInt, ok := kv.Value.(*expvar.Int); ok {
			targetMap[kv.Key] = kvInt.Value()
		} else if kvFloat, ok := kv.Value.(*expvar.Float); ok {
			targetMap[kv.Key] = kvFloat.Value()
		} else if kvString, ok := kv.Value.(*expvar.String); ok {
			targetMap[kv.Key] = kvString.Value()
		} else if kvMap, ok := kv.Value.(*expvar.Map); ok {
			subMap, exists := targetMap[kv.Key].(ExpVarParseMapType)
			if !exists {
				subMap = make(ExpVarParseMapType)
			}
			parseExpMap(kvMap, subMap)
			targetMap[kv.Key] = subMap
		}
	})
}

func (p *PrometheusExporter) LoadExpVarMap(m *expvar.Map) (noKeysChanged bool) {
	p.mapsMtx.Lock()
	noKeysChanged = p.expVarParseMap.CheckNoKeyChanges(m)
	keysChanged := !noKeysChanged
	if keysChanged {
		p.expVarParseMap = make(ExpVarParseMapType)
	}

	parseExpMap(m, p.expVarParseMap)
	p.LoadMetricsMap(keysChanged)
	p.mapsMtx.Unlock()

	if keysChanged {
		p.resetOutputBuffer()
	}
	return
}

func (p *PrometheusExporter) resetOutputBuffer() {
	p.outputBufferMtx.Lock()
	p.outputBuffer = make([]byte, 0)
	p.outputBufferMtx.Unlock()
}

func (p *PrometheusExporter) LoadSourceClustersInfoV1(srcClusterNamesIn map[string]string, srcSpecsIn map[string][]*metadata.ReplicationSpecification, srcNodesIn map[string][]string) {
	p.mapsMtx.Lock()
	defer p.mapsMtx.Unlock()

	// check for changes
	var cacheOutdated bool
	if len(p.srcSpecs) != len(srcSpecsIn) ||
		len(p.srcNodes) != len(srcNodesIn) {
		cacheOutdated = true
		p.srcNodes = srcNodesIn
		p.srcSpecs = srcSpecsIn
	} else {
		// check to make sure srcNodes are correct
		for srcUuid, nodesList := range srcNodesIn {
			cachedNodesList, exists := p.srcNodes[srcUuid]
			if !exists {
				cacheOutdated = true
				break
			}
			if len(nodesList) != len(cachedNodesList) {
				cacheOutdated = true
				break
			}
			for _, node := range nodesList {
				if found := base.StringList(cachedNodesList).Search(node, false); !found {
					cacheOutdated = true
					break
				}
			}
		}

		if !cacheOutdated {
			// nodes list the same, check specs
			for srcUuid, specsIn := range srcSpecsIn {
				cachedSpecs, exists := p.srcSpecs[srcUuid]
				if !exists {
					cacheOutdated = true
					break
				}
				if !metadata.ReplSpecList(specsIn).SameAs(cachedSpecs) {
					cacheOutdated = true
					break
				}
			}
		}
	}
	if cacheOutdated {
		p.resetOutputBuffer()
		p.metricsMap.RecordSourceClusterV1(srcClusterNamesIn, srcSpecsIn, srcNodesIn, p.globalLookupMap, p.labelsTableConstructor)
	}
}

func (p *PrometheusExporter) LoadMetricsMap(needToReallocate bool) error {
	if needToReallocate {
		p.InitializeMetricsMapNoLock()
	}

	for replId, statsMap := range p.expVarParseMap {
		if !metadata.IsAReplicationId(replId) {
			return fmt.Errorf("Invalid expVarParseMap - expecting replication ID, got %v", replId)
		}
		// Now everything else is in the context of this replication
		for statConst, value := range statsMap.(ExpVarParseMapType) {
			p.metricsMap.RecordStat(replId, statConst, value, p.globalLookupMap, p.labelsTableConstructor)
		}
	}

	return nil
}

// Write lock should be held
func (p *PrometheusExporter) InitializeMetricsMapNoLock() {
	p.metricsMap = make(MetricsMapType)
	keys := p.globalLookupMap.GetAllKeys()
	for _, k := range keys {
		p.metricsMap[k] = IdentifierStatsMap{}
	}
}

// NOTE - to prevent generating garbage, this function will return a []byte that is linked
// to an internal buffer
// This means that this function is NOT thread-safe
// If someone else calls Export() before the prev caller is done with the data
// the data underneath may be changed
func (p *PrometheusExporter) Export() ([]byte, error) {
	// First, prepare each stat buffer
	needToOutput := p.prepareStatsBuffer()

	if !needToOutput {
		// Nothing to output
		return nil, nil
	}

	// Used for detecting if it needs to be reallocated
	p.outputBufferMtx.Lock()
	defer p.outputBufferMtx.Unlock()

	p.outputBuffer = p.outputBuffer[:0]

	// Then, read all the prepared stats buffer
	if needToOutput {
		p.outputReplStatsToBuffer()
	}
	return p.outputBuffer, nil
}

func (p *PrometheusExporter) outputReplStatsToBuffer() {
	p.mapsMtx.RLock()
	defer p.mapsMtx.RUnlock()
	for statsMgrMetricKey, identifierStatsMap := range p.metricsMap {
		// # HELP xdcr_dcp_datach_length_total Blah Blah Blah
		// # TYPE xdcr_dcp_datach_length_total gauge
		// xdcr_dcp_datach_length_total {repl_id="0746d42b7e44e5840dc02a9249efaef0/B0/B2"} 0
		// xdcr_dcp_datach_length_total {repl_id="0746d42b7e44e5840dc02a9249efaef0/B1/B2"} 13260

		if !p.globalLookupMap.KeyExists(statsMgrMetricKey) {
			continue
		}

		p.outputHelp(statsMgrMetricKey)
		p.outputType(statsMgrMetricKey)
		for _, statPerIdentifier := range identifierStatsMap {
			p.outputOneReplStat(statPerIdentifier.GetOutputBuffer())
		}
		// Newline not necessary but makes it more human readable
		p.outputBufferNewLine()
	}
}

func (p *PrometheusExporter) outputBufferNewLine() {
	p.outputBuffer = append(p.outputBuffer, fmt.Sprintf("\n")...)
}

func (p *PrometheusExporter) outputType(k string) {
	typeText, exists := p.globalMetricTypeMap[k]
	if !exists {
		panic("FIXME")
	}
	p.outputBuffer = append(p.outputBuffer, typeText...)
	p.outputBufferNewLine()
}

func (p *PrometheusExporter) outputHelp(k string) {
	helpText, exists := p.globalMetricHelpMap[k]
	if !exists {
		panic("FIXME")
	}
	p.outputBuffer = append(p.outputBuffer, helpText...)
	p.outputBufferNewLine()
}

func (p *PrometheusExporter) prepareStatsBuffer() (needToOutput bool) {
	p.mapsMtx.Lock()
	defer p.mapsMtx.Unlock()

	// Used to compare the latest known sources with what has been known
	// key is the stat constant - value is the identifier (i.e. replID or sourceUUID)
	currentKnownIdentifier := make(map[string][]string)

	for statConst, identifierStatsMap := range p.metricsMap {
		if !p.globalLookupMap.KeyExists(statConst) {
			continue
		}

		metricKey, exists := p.globalMetricKeyMap[statConst]
		if !exists {
			panic("FIXME")
		}

		for identifier, perIdentifierStat := range identifierStatsMap {
			if perIdentifierStat == nil {
				continue
			}
			needToOutput = true
			currentKnownIdentifier[statConst] = append(currentKnownIdentifier[statConst], identifier)
			found := p.lastKnownIdentifiers[statConst].Search(identifier, false)
			if !found {
				p.lastKnownIdentifiers[statConst] = append(p.lastKnownIdentifiers[statConst], identifier)
			}
			perIdentifierStat.UpdateOutputBuffer(metricKey, identifier)
		}
	}

	// Remove any sources that no longer exist
	for statConst, knownIdentifiers := range p.lastKnownIdentifiers {
		currentIdentifiers, statStillExists := currentKnownIdentifier[statConst]
		if !statStillExists || len(currentIdentifiers) == 0 {
			// The whole category no longer exists - wipe it clean
			needToOutput = true
			p.metricsMap[statConst] = IdentifierStatsMap{}
			continue
		}

		// check individual identifier
		currentIdentifiersList := base.SortStringList(currentKnownIdentifier[statConst])
		for _, lastKnownIdentifier := range knownIdentifiers {
			found := base.StringList(currentIdentifiersList).Search(lastKnownIdentifier, true)
			if !found {
				// need to remove lastKnownIdentifier
				needToOutput = true
				p.lastKnownIdentifiers[statConst] = p.lastKnownIdentifiers[statConst].RemoveInstances(lastKnownIdentifier)
			}
		}
	}

	return
}

func (p *PrometheusExporter) outputOneReplStat(buffer []byte) {
	p.outputBuffer = append(p.outputBuffer, buffer...)
	p.outputBufferNewLine()
}

type LabelsValuesExtractor interface {
	LoadValue(value interface{})
	GetLabelName() []byte
	GetValueBaseUnit() interface{}
}

type PrometheusLabels = map[string]LabelsValuesExtractor
type PrometheusLabelsConstructorType func(string) PrometheusLabels

// The following are not part of the automatic metrics json file generation
// XDCR originally was not built with prometheus in mind. As such, it needs the
// LabelsValuesExtractor to perform the task of converting one or multiple stats into
// labels and specific values
func NewPrometheusLabelsTable(labelName string) PrometheusLabels {
	switch labelName {
	case service_def.PIPELINE_STATUS:
		return PrometheusLabels{
			base.PipelineStatusPaused.String(): &PipelineStatusPausedExtractor{
				extractorCommon:  &extractorCommon{LabelBytes: []byte("status=\"Paused\"")},
				valueInt64Common: &valueInt64Common{},
			},
			base.PipelineStatusRunning.String(): &PipelineStatusRunningExtractor{
				extractorCommon:  &extractorCommon{LabelBytes: []byte("status=\"Running\"")},
				valueInt64Common: &valueInt64Common{},
			},
			base.PipelineStatusError.String(): &PipelineStatusErrorExtractor{
				extractorCommon:  &extractorCommon{LabelBytes: []byte("status=\"Error\"")},
				valueInt64Common: &valueInt64Common{},
			},
		}
	case service_def.SOURCE_CLUSTER_NUM_REPL:
		return PrometheusLabels{
			base.PipelineStatusPaused.String(): &IncomingReplicationCountExtractor{
				extractorCommon:  &extractorCommon{LabelBytes: []byte("status=\"Paused\"")},
				valueInt64Common: &valueInt64Common{},
			},
			base.PipelineStatusRunning.String(): &IncomingReplicationCountExtractor{
				extractorCommon:  &extractorCommon{LabelBytes: []byte("status=\"Running\"")},
				valueInt64Common: &valueInt64Common{},
			},
		}
	default:
		return nil
	}
}

type extractorCommon struct {
	LabelBytes []byte
}

func (e *extractorCommon) GetLabelName() []byte {
	return e.LabelBytes
}

type valueInt64Common struct {
	Value int64
}

func (v *valueInt64Common) GetValueBaseUnit() interface{} {
	return v.Value
}

type PipelineStatusPausedExtractor struct {
	*extractorCommon
	*valueInt64Common
}

func (p *PipelineStatusPausedExtractor) LoadValue(value interface{}) {
	incomingVal := value.(int64)
	if incomingVal == int64(base.PipelineStatusPaused) {
		p.Value = 1
	} else {
		p.Value = 0
	}
}

type PipelineStatusRunningExtractor struct {
	*extractorCommon
	*valueInt64Common
}

func (p *PipelineStatusRunningExtractor) LoadValue(value interface{}) {
	incomingVal := value.(int64)
	if incomingVal == int64(base.PipelineStatusRunning) {
		p.Value = 1
	} else {
		p.Value = 0
	}
}

type PipelineStatusErrorExtractor struct {
	*extractorCommon
	*valueInt64Common
}

func (p *PipelineStatusErrorExtractor) LoadValue(value interface{}) {
	incomingVal := value.(int64)
	if incomingVal == int64(base.PipelineStatusError) {
		p.Value = 1
	} else {
		p.Value = 0
	}
}

type IncomingReplicationCountExtractor struct {
	*extractorCommon
	*valueInt64Common
}

func (i *IncomingReplicationCountExtractor) LoadValue(value interface{}) {
	incomingVal := value.(int)
	i.Value = int64(incomingVal)
}
