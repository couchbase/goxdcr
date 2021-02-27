// Copyright (c) 2013-2020 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package pipeline_utils

import (
	"expvar"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	utilities "github.com/couchbase/goxdcr/utils"
	"strings"
	"sync"
)

type ExpVarExporter interface {
	LoadExpVarMap(m *expvar.Map) bool
	Export() ([]byte, error)
}

const (
	PrometheusTargetClusterUuidLabel = "targetClusterUUID"
	PrometheusSourceBucketLabel      = "sourceBucketName"
	PrometheusTargetBucketLabel      = "targetBucketName"
	PrometheusPipelineTypeLabel      = "pipelineType"
)

var PrometheusTargetClusterUuidBytes = []byte(PrometheusTargetClusterUuidLabel)
var PrometheusSourceBucketBytes = []byte(PrometheusSourceBucketLabel)
var PrometheusTargetBucketBytes = []byte(PrometheusTargetBucketLabel)
var PrometheusPipelineTypeBytes = []byte(PrometheusPipelineTypeLabel)

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
}

func NewPrometheusExporter(translationMap service_def.StatisticsPropertyMap) *PrometheusExporter {
	prom := &PrometheusExporter{
		globalLookupMap:     translationMap,
		globalMetricHelpMap: make(map[string][]byte),
		globalMetricKeyMap:  make(map[string][]byte),
		globalMetricTypeMap: make(map[string][]byte),
		metricsMap:          make(MetricsMapType),
		expVarParseMap:      make(ExpVarParseMapType),
		outputBuffer:        make([]byte, 0),
		utils:               utilities.NewUtilities(),
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

func (m *MetricsMapType) RecordStat(replicationId, statsConst string, value interface{}, lookupMap service_def.StatisticsPropertyMap) {
	// If the statsConst is not part of the initialization, then it is not meant to be exported
	replicationStatsMap, constExists := (*m)[statsConst]
	if !constExists {
		return
	}

	_, replExists := replicationStatsMap[replicationId]
	if !replExists {
		statsProperty := lookupMap[statsConst]
		replicationStatsMap[replicationId] = NewPerReplicationStatType(statsProperty)
	}

	// Update the value
	replicationStatsMap[replicationId].Value = value
}

type ExpVarParseMapType map[string]interface{}

// Returns true if all the keys match, and the types all match
func (e ExpVarParseMapType) CheckNoKeyChanges(varMap *expvar.Map, utils utilities.UtilsIface) bool {
	var missingKey bool
	var inconsistentType bool
	var subLevelCheckPasses = true
	var keyCount int
	keyLen := len(e)

	// TODO - MB-44586 - remove this before CC ships
	if utils != nil {
		stopFunc := utils.DumpStackTraceAfterThreshold("CheckNoKeyChanges", base.DiagInternalThreshold, base.PprofAllGoroutines)
		defer stopFunc()
		stopFunc2 := utils.DumpStackTraceAfterThreshold("CheckNoKeyChanges", base.DiagInternalThreshold, base.PprofBlocking)
		defer stopFunc2()
	}

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
			subLevelCheckPasses = eSubMap.CheckNoKeyChanges(subMap, nil)
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
	outputValueBuffer         []byte
	ReplIdDecompositionStruct *metadata.ReplIdComposition
}

func (t *PerReplicationStatType) UpdateOutputBuffer(metricName []byte, replId string) {
	// Output looks like:
	// metric_name {label_name=\"<labelVal>\", ...} <value>
	// Ends with a newline, but won't output it here
	t.OutputBuffer = t.OutputBuffer[:0]
	t.OutputBuffer = append(t.OutputBuffer, metricName...)
	t.appendLabels(replId)
	t.appendOutputBufferWithValue()
}

func (t *PerReplicationStatType) appendLabels(replId string) {
	// {
	t.OutputBuffer = append(t.OutputBuffer, []byte(" {")...)

	t.ReplIdDecompositionStruct = metadata.DecomposeReplicationId(replId, t.ReplIdDecompositionStruct)

	// { targetClusterUUID="abcdef",
	t.OutputBuffer = append(t.OutputBuffer, PrometheusTargetClusterUuidBytes...)
	t.OutputBuffer = append(t.OutputBuffer, []byte("=\"")...)
	t.OutputBuffer = append(t.OutputBuffer, []byte(t.ReplIdDecompositionStruct.TargetClusterUUID)...)
	t.OutputBuffer = append(t.OutputBuffer, []byte("\", ")...)

	// { targetClusterUUID="abcdef", sourceBucketName="b1",
	t.OutputBuffer = append(t.OutputBuffer, PrometheusSourceBucketBytes...)
	t.OutputBuffer = append(t.OutputBuffer, []byte("=\"")...)
	t.OutputBuffer = append(t.OutputBuffer, []byte(t.ReplIdDecompositionStruct.SourceBucketName)...)
	t.OutputBuffer = append(t.OutputBuffer, []byte("\", ")...)

	// { targetClusterUUID="abcdef", sourceBucketName="b1", targetBucketName="b2",
	t.OutputBuffer = append(t.OutputBuffer, PrometheusTargetBucketBytes...)
	t.OutputBuffer = append(t.OutputBuffer, []byte("=\"")...)
	t.OutputBuffer = append(t.OutputBuffer, []byte(t.ReplIdDecompositionStruct.TargetBucketName)...)
	t.OutputBuffer = append(t.OutputBuffer, []byte("\", ")...)

	// { targetClusterUUID="abcdef", sourceBucketName="b1", targetBucketName="b2", pipelineType="Main"
	t.OutputBuffer = append(t.OutputBuffer, PrometheusPipelineTypeBytes...)
	t.OutputBuffer = append(t.OutputBuffer, []byte("=\"")...)
	t.OutputBuffer = append(t.OutputBuffer, []byte(t.ReplIdDecompositionStruct.PipelineType)...)
	t.OutputBuffer = append(t.OutputBuffer, []byte("\"")...)

	// { targetClusterUUID="abcdef", sourceBucketName="b1", targetBucketName="b2", pipelineType="Main"}
	t.OutputBuffer = append(t.OutputBuffer, []byte("} ")...)
}

func (t *PerReplicationStatType) GetValueBaseUnit() interface{} {
	switch t.Properties.MetricType.Unit {
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
func (t *PerReplicationStatType) appendOutputBufferWithValue() {
	switch t.Properties.MetricType.Unit {
	case service_def.StatsMgrNoUnit:
		t.OutputBuffer = append(t.OutputBuffer, []byte(fmt.Sprintf("%v", t.GetValueBaseUnit()))...)
	case service_def.StatsMgrSeconds:
		t.OutputBuffer = append(t.OutputBuffer, []byte(fmt.Sprintf("%v", t.GetValueBaseUnit()))...)
	case service_def.StatsMgrMilliSecond:
		t.OutputBuffer = append(t.OutputBuffer, []byte(fmt.Sprintf("%g", t.GetValueBaseUnit()))...)
	case service_def.StatsMgrGolangTimeDuration:
		t.OutputBuffer = append(t.OutputBuffer, []byte(fmt.Sprintf("%g", t.GetValueBaseUnit()))...)
	case service_def.StatsMgrBytes:
		t.OutputBuffer = append(t.OutputBuffer, []byte(fmt.Sprintf("%v", t.GetValueBaseUnit()))...)
	case service_def.StatsMgrMegaBytesPerSecond:
		t.OutputBuffer = append(t.OutputBuffer, []byte(fmt.Sprintf("%g", t.GetValueBaseUnit()))...)
	case service_def.StatsMgrDocsPerSecond:
		t.OutputBuffer = append(t.OutputBuffer, []byte(fmt.Sprintf("%v", t.GetValueBaseUnit()))...)
	default:
		panic("Need to implement")
	}
}

func NewPerReplicationStatType(properties service_def.StatsProperty) *PerReplicationStatType {
	return &PerReplicationStatType{
		Properties: properties,
		Value:      nil,
	}
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
	noKeysChanged = p.expVarParseMap.CheckNoKeyChanges(m, p.utils)
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
			p.metricsMap.RecordStat(replId, statConst, value, p.globalLookupMap)
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
