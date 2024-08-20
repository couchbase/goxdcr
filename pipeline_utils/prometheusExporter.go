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
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
	utilities "github.com/couchbase/goxdcr/v8/utils"
	"strings"
	"sync"
)

type ExpVarExporter interface {
	LoadExpVarMap(m *expvar.Map) bool
	Export() ([]byte, error)
}

var PrometheusTargetClusterUuidBytes = []byte(service_def.PrometheusTargetClusterUuidLabel)
var PrometheusSourceBucketBytes = []byte(service_def.PrometheusSourceBucketLabel)
var PrometheusTargetBucketBytes = []byte(service_def.PrometheusTargetBucketLabel)
var PrometheusPipelineTypeBytes = []byte(service_def.PrometheusPipelineTypeLabel)

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
	// Each Replication will populate the stats under the ReplicationStatsMap
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

type MetricsMapType map[string]ReplicationStatsMap

func (m *MetricsMapType) RecordStat(replicationId, statsConst string, value interface{}, lookupMap service_def.StatisticsPropertyMap, constructStatsTable PrometheusLabelsConstructorType) {
	// If the statsConst is not part of the initialization, then it is not meant to be exported
	replicationStatsMap, constExists := (*m)[statsConst]
	if !constExists {
		return
	}

	_, replExists := replicationStatsMap[replicationId]
	if !replExists {
		statsProperty := lookupMap[statsConst]
		labelsTable := constructStatsTable(statsConst) // could be nil, depending on statsConst
		replicationStatsMap[replicationId] = NewPerReplicationStatType(statsProperty, labelsTable)
	}

	// Update the value
	replicationStatsMap[replicationId].Value = value

	// If Prometheus labels exist, let them load the value
	labelsTable := replicationStatsMap[replicationId].LabelsTable
	if labelsTable != nil {
		for _, extractor := range labelsTable {
			extractor.LoadValue(value)
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

type ReplicationStatsMap map[string]*PerReplicationStatType

// Prometheus only stores numerical values - ns_server request them to be in float64
type PerReplicationStatType struct {
	Properties                service_def.StatsProperty
	Value                     interface{}
	OutputBuffer              []byte
	ReplIdDecompositionStruct *metadata.ReplIdComposition

	LabelsTable           PrometheusLabels
	IndividualLabelBuffer map[string]*[]byte // Shares key as above
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
			t.appendCustomLabels(replId, t.IndividualLabelBuffer[customLabelName], extractor)
			t.appendBufferWithValue(t.IndividualLabelBuffer[customLabelName], extractor.GetValueBaseUnit)
		}
		t.composeFinalOutputBufferFromLabelBuffers()
	} else {
		t.appendMetricName(metricName, &t.OutputBuffer)
		t.appendStandardLabels(replId, &t.OutputBuffer)
		t.appendBufferWithValue(&t.OutputBuffer, t.GetValueBaseUnit)
	}
}

func (t *PerReplicationStatType) resetBuffers() {
	t.OutputBuffer = t.OutputBuffer[:0]
	for _, oneLabelBuffer := range t.IndividualLabelBuffer {
		*oneLabelBuffer = (*oneLabelBuffer)[:0]
	}
}

func (t *PerReplicationStatType) appendMetricName(metricName []byte, buffer *[]byte) {
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

func (t *PerReplicationStatType) appendCustomLabels(replId string, buffer *[]byte, extractor LabelsValuesExtractor) {
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

func (t *PerReplicationStatType) GetValueBaseUnit() interface{} {
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
func (t *PerReplicationStatType) appendBufferWithValue(buffer *[]byte, getValueBaseUnit func() interface{}) {
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

func (t *PerReplicationStatType) composeFinalOutputBufferFromLabelBuffers() {
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
		Properties:            properties,
		Value:                 nil,
		LabelsTable:           labelsTable,
		IndividualLabelBuffer: map[string]*[]byte{},
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
		p.outputBufferMtx.Lock()
		p.outputBuffer = make([]byte, 0)
		p.outputBufferMtx.Unlock()
	}
	return
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
	for k, _ := range p.globalLookupMap {
		p.metricsMap[k] = ReplicationStatsMap{}
	}
}

// NOTE - to prevent generating garbage, this function will return a []byte that is linked
// to an internal buffer
// This means that this function is NOT thread-safe
// If someone else calls Export() before the prev caller is done with the data
// the data underneath may be changed
func (p *PrometheusExporter) Export() ([]byte, error) {
	// First, prepare each stat buffer
	atLeastOneStatsActive := p.prepareStatsBuffer()

	if !atLeastOneStatsActive {
		// Nothing to output
		return nil, nil
	}

	// Used for detecting if it needs to be reallocated
	p.outputBufferMtx.Lock()
	defer p.outputBufferMtx.Unlock()

	p.outputBuffer = p.outputBuffer[:0]

	// Then, read all the prepared stats buffer
	p.outputToBuffer()

	return p.outputBuffer, nil
}

func (p *PrometheusExporter) outputToBuffer() {
	p.mapsMtx.RLock()
	defer p.mapsMtx.RUnlock()
	for statsMgrMetricKey, replicationStatsMap := range p.metricsMap {
		// # HELP xdcr_dcp_datach_length_total Blah Blah Blah
		// # TYPE xdcr_dcp_datach_length_total gauge
		// xdcr_dcp_datach_length_total {repl_id="0746d42b7e44e5840dc02a9249efaef0/B0/B2"} 0
		// xdcr_dcp_datach_length_total {repl_id="0746d42b7e44e5840dc02a9249efaef0/B1/B2"} 13260

		if _, exists := p.globalLookupMap[statsMgrMetricKey]; !exists {
			continue
		}

		p.outputHelp(statsMgrMetricKey)
		p.outputType(statsMgrMetricKey)
		for _, perReplStats := range replicationStatsMap {
			p.outputOneReplStat(perReplStats.OutputBuffer)
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

func (p *PrometheusExporter) prepareStatsBuffer() (atLeastOneStatsActive bool) {
	p.mapsMtx.Lock()
	defer p.mapsMtx.Unlock()
	for k, replicationStatsMap := range p.metricsMap {
		if _, exists := p.globalLookupMap[k]; !exists {
			continue
		}

		metricKey, exists := p.globalMetricKeyMap[k]
		if !exists {
			panic("FIXME")
		}

		for replId, perReplStats := range replicationStatsMap {
			if perReplStats == nil {
				continue
			}
			atLeastOneStatsActive = true
			perReplStats.UpdateOutputBuffer(metricKey, replId)
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
