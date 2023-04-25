// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_def

import (
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/metadata"

	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// Stats per vbucket
type VBCountMetricMap map[string]int64

type StatsMgrIface interface {
	Start(settings metadata.ReplicationSettingsMap) error
	Stop() error
	GetCountMetrics(key string) (int64, error)
	GetThroughSeqnosFromTsService() map[uint16]uint64
	GetVBCountMetrics(vb uint16) (VBCountMetricMap, error)
	SetVBCountMetrics(vb uint16, metricKVs VBCountMetricMap) error
	HandleLatestThroughSeqnos(SeqnoMap map[uint16]uint64)
}

const (
	// the number of docs written/sent to target cluster
	DOCS_WRITTEN_METRIC              = "docs_written"
	EXPIRY_DOCS_WRITTEN_METRIC       = "expiry_docs_written"
	DELETION_DOCS_WRITTEN_METRIC     = "deletion_docs_written"
	SET_DOCS_WRITTEN_METRIC          = "set_docs_written"
	ADD_DOCS_WRITTEN_METRIC          = "add_docs_written"
	DELETION_DOCS_CAS_CHANGED_METRIC = "deletion_docs_cas_changed"
	SET_DOCS_CAS_CHANGED_METRIC      = "set_docs_cas_changed"
	ADD_DOCS_CAS_CHANGED_METRIC      = "add_docs_cas_changed"

	// the number of docs merged and sent to source cluster
	DOCS_MERGED_METRIC              = "docs_merged"
	DATA_MERGED_METRIC              = "data_merged"
	EXPIRY_DOCS_MERGED_METRIC       = "expiry_docs_merged"
	DOCS_MERGE_CAS_CHANGED_METRIC   = "docs_merge_cas_changed"
	EXPIRY_MERGE_CAS_CHANGED_METRIC = "expiry_merge_cas_changed"

	// the number of docs failed to merge and sent to conflict bucket
	DOCS_MERGE_FAILED_METRIC        = "docs_merge_failed"
	DATA_MERGE_FAILED_METRIC        = "data_merge_failed"
	EXPIRY_DOCS_MERGE_FAILED_METRIC = "expiry_docs_merge_failed"

	// the number of docs processed by pipeline
	DOCS_PROCESSED_METRIC  = "docs_processed"
	DATA_REPLICATED_METRIC = "data_replicated"
	SIZE_REP_QUEUE_METRIC  = "size_rep_queue"
	DOCS_REP_QUEUE_METRIC  = base.DocsRepQueueStats

	DOCS_FILTERED_METRIC         = "docs_filtered"
	DOCS_UNABLE_TO_FILTER_METRIC = "docs_unable_to_filter"
	EXPIRY_FILTERED_METRIC       = "expiry_filtered"
	DELETION_FILTERED_METRIC     = "deletion_filtered"
	SET_FILTERED_METRIC          = "set_filtered"
	EXPIRY_STRIPPED_METRIC       = "expiry_stripped"

	// the number of docs that failed conflict resolution on the source cluster side due to optimistic replication
	DOCS_FAILED_CR_SOURCE_METRIC     = "docs_failed_cr_source"
	EXPIRY_FAILED_CR_SOURCE_METRIC   = "expiry_failed_cr_source"
	DELETION_FAILED_CR_SOURCE_METRIC = "deletion_failed_cr_source"
	SET_FAILED_CR_SOURCE_METRIC      = "set_failed_cr_source"

	// the number of add/set/delete/expire that failed conflict resolution on the target cluster side
	DOCS_FAILED_CR_TARGET_METRIC     = "docs_failed_cr_target"
	ADD_FAILED_CR_TARGET_METRIC      = "add_failed_cr_target"
	DELETION_FAILED_CR_TARGET_METRIC = "deletion_failed_cr_target"
	EXPIRY_FAILED_CR_TARGET_METRIC   = "expiry_failed_cr_target"
	SET_FAILED_CR_TARGET_METRIC      = "set_failed_cr_target"

	// for Custom CR, document skipped because they originate from target
	TARGET_DOCS_SKIPPED_METRIC          = "target_docs_skipped"
	EXPIRY_TARGET_DOCS_SKIPPED_METRIC   = "expiry_target_docs_skipped"
	DELETION_TARGET_DOCS_SKIPPED_METRIC = "deletion_target_docs_skipped"
	SET_TARGET_DOCS_SKIPPED_METRIC      = "set_target_docs_skipped"

	CHANGES_LEFT_METRIC    = base.ChangesLeftStats
	DOCS_LATENCY_METRIC    = "wtavg_docs_latency"
	MERGE_LATENCY_METRIC   = "wtavg_merge_latency"
	META_LATENCY_METRIC    = "wtavg_meta_latency"
	GET_DOC_LATENCY_METRIC = "wtavg_get_doc_latency"
	RESP_WAIT_METRIC       = "resp_wait_time"

	//checkpointing related statistics
	DOCS_CHECKED_METRIC    = "docs_checked" //calculated
	NUM_CHECKPOINTS_METRIC = "num_checkpoints"
	TIME_COMMITING_METRIC  = "time_committing"
	NUM_FAILEDCKPTS_METRIC = "num_failedckpts"
	RATE_DOC_CHECKS_METRIC = "rate_doc_checks"

	//optimistic replication replated statistics
	DOCS_OPT_REPD_METRIC = "docs_opt_repd"
	RATE_OPT_REPD_METRIC = "rate_doc_opt_repd"

	DOCS_RECEIVED_DCP_METRIC = base.DocsFromDcpStats
	RATE_RECEIVED_DCP_METRIC = "rate_received_from_dcp"

	EXPIRY_RECEIVED_DCP_METRIC   = "expiry_received_from_dcp"
	DELETION_RECEIVED_DCP_METRIC = "deletion_received_from_dcp"
	SET_RECEIVED_DCP_METRIC      = "set_received_from_dcp"

	DCP_DISPATCH_TIME_METRIC = "dcp_dispatch_time"
	DCP_DATACH_LEN           = "dcp_datach_length"

	// latency caused by bandwidth throttling
	THROTTLE_LATENCY_METRIC = "throttle_latency"

	// latency caused by throughput throttling
	THROUGHPUT_THROTTLE_LATENCY_METRIC = "throughput_throttle_latency"

	//rate
	RATE_REPLICATED_METRIC = "rate_replicated"
	BANDWIDTH_USAGE_METRIC = "bandwidth_usage"

	OVERVIEW_METRICS_KEY = "Overview"

	//statistics_manager's setting
	PUBLISH_INTERVAL = "publish_interval"

	// Memory related statistics
	DP_GET_FAIL_METRIC = "datapool_failed_gets"

	DOCS_CLONED_METRIC     = "docs_cloned"
	DELETION_CLONED_METRIC = "deletion_cloned"
)

const (
	PrometheusXDCRPrefix = "xdcr"
	PrometheusDelimiter  = "_"

	PrometheusBaseNoUnit       = "total"
	PrometheusBaseUnitTime     = "seconds"
	PrometheusBaseUnitSize     = "bytes"
	PrometheusBaseUnitDataRate = "bytes_per_second"
	PrometheusBaseUnitReplRate = "docs_per_second"

	PrometheusAccumulateUnit = PrometheusBaseNoUnit

	PrometheusMetricTypeCounter = "counter"
	PrometheusMetricTypeGauge   = "gauge"
)

// UI keys
const (
	UIChangesLeft    = "xdcr_changes_left_total"
	UIDataReplicated = "xdcr_data_replicated_bytes"
	UIDocsChecked    = "xdcr_docs_checked_total"
	UIFailedCRSrc    = "xdcr_docs_failed_cr_source_total"
	UIFailedCRTgt    = "xdcr_docs_failed_cr_target_total"
	UIDocsFiltered   = "xdcr_docs_filtered_total"
	UIOptReplicated  = "xdcr_docs_opt_repd_total"
	UIDocsFromDCP    = "xdcr_docs_received_from_dcp_total"
	UIRateReplicated = "xdcr_rate_replicated_docs_per_second"
	UIDocsLatency    = "xdcr_wtavg_docs_latency_seconds"
	UIMetaLatency    = "xdcr_wtavg_meta_latency_seconds"
)

type MetricType int

const MetricTypeInvalid MetricType = -1

// https://prometheus.io/docs/concepts/metric_types/
// For couchbase, only support these two
const (
	MetricTypeCounter MetricType = 0
	MetricTypeGauge   MetricType = 1
)

func NewMetricTypeFromString(str string) MetricType {
	switch str {
	case PrometheusMetricTypeCounter:
		return MetricTypeCounter
	case PrometheusMetricTypeGauge:
		return MetricTypeGauge
	default:
		return MetricTypeInvalid
	}
}

func (m MetricType) String() string {
	switch m {
	case MetricTypeCounter:
		return PrometheusMetricTypeCounter
	case MetricTypeGauge:
		return PrometheusMetricTypeGauge
	default:
		return ""
	}
}

type StatsMgrUnitType int

const (
	StatsMgrNoUnit             StatsMgrUnitType = iota
	StatsMgrSeconds            StatsMgrUnitType = iota
	StatsMgrMilliSecond        StatsMgrUnitType = iota
	StatsMgrGolangTimeDuration StatsMgrUnitType = iota
	StatsMgrBytes              StatsMgrUnitType = iota
	StatsMgrMegaBytesPerSecond StatsMgrUnitType = iota // MB/second, where 1 MB = 1024*1024 bytes instead of 1000*1000 bytes
	StatsMgrDocsPerSecond      StatsMgrUnitType = iota
)

func (s StatsMgrUnitType) String() string {
	if s == StatsMgrNoUnit {
		return ""
	}

	return GlobalBaseUnitTable[s]
}

var GlobalBaseUnitTable = map[StatsMgrUnitType]string{
	StatsMgrNoUnit:             PrometheusBaseNoUnit,
	StatsMgrSeconds:            PrometheusBaseUnitTime,
	StatsMgrMilliSecond:        PrometheusBaseUnitTime,
	StatsMgrGolangTimeDuration: PrometheusBaseUnitTime,
	StatsMgrBytes:              PrometheusBaseUnitSize,
	StatsMgrMegaBytesPerSecond: PrometheusBaseUnitDataRate,
	StatsMgrDocsPerSecond:      PrometheusBaseUnitReplRate,
}

var GlobalBaseReverseUnitTable = map[string]StatsMgrUnitType{
	PrometheusBaseNoUnit:       StatsMgrNoUnit,
	PrometheusBaseUnitTime:     StatsMgrSeconds,
	PrometheusBaseUnitSize:     StatsMgrBytes,
	PrometheusBaseUnitDataRate: StatsMgrMegaBytesPerSecond,
	PrometheusBaseUnitReplRate: StatsMgrDocsPerSecond,
}

type StatsUnit struct {
	Metric MetricType
	Unit   StatsMgrUnitType
}

type MetricCardinality int

const (
	LowCardinality  MetricCardinality = iota
	HighCardinality MetricCardinality = iota
)

type StatsStability int

const (
	Volatile  StatsStability = iota
	Internal  StatsStability = iota
	Committed StatsStability = iota
)

const (
	VolatileStr  = "volatile"
	InternalStr  = "internal"
	CommittedStr = "committed"
)

func (s StatsStability) String() string {
	switch s {
	case Volatile:
		return VolatileStr
	case Internal:
		return InternalStr
	case Committed:
		return CommittedStr
	default:
		return ""
	}
}

func NewStatsStabilityFromStr(str string) (StatsStability, error) {
	switch str {
	case VolatileStr:
		return Volatile, nil
	case InternalStr:
		return Internal, nil
	case CommittedStr:
		return Committed, nil
	default:
		return Volatile, fmt.Errorf("Invalid input %v", str)
	}

}

type StatsLabels []StatsLabel

func (s StatsLabels) SameAs(other StatsLabels) bool {
	if len(s) != len(other) {
		return false
	}
	for i, c := range s {
		if !c.SameAs(other[i]) {
			return false
		}
	}
	return true
}

func (s StatsLabels) ToGenerics() []interface{} {
	var generics []interface{}
	for _, label := range s {
		generics = append(generics, label)
	}
	return generics
}

type statsLabelMetaObj struct {
	Name       string `json:"name"`
	Help       string `json:"help"`
	Added      string `json:"added,omitempty"`
	Deprecated string `json:"deprecated,omitempty"`
}

type StatsLabel struct {
	Name       string
	Help       string
	Added      base.ServerVersion
	Deprecated base.ServerVersion
}

func (s StatsLabel) SameAs(other StatsLabel) bool {
	return s.Name == other.Name &&
		s.Help == other.Help &&
		s.Added.SameAs(other.Added) &&
		s.Deprecated.SameAs(other.Deprecated)
}

func (s StatsLabel) MarshalJSON() ([]byte, error) {
	obj := &statsLabelMetaObj{
		Name:       s.Name,
		Help:       s.Help,
		Added:      s.Added.String(),
		Deprecated: s.Deprecated.String(),
	}
	return json.Marshal(obj)
}

func (s StatsLabel) UnmarshalJSON(b []byte) error {
	obj := &statsLabelMetaObj{}
	err := json.Unmarshal(b, obj)
	if err != nil {
		return err
	}
	s.Name = obj.Name
	s.Help = obj.Help
	s.Added, err = base.NewServerVersionFromString(obj.Added)
	if err != nil {
		return err
	}
	s.Deprecated, err = base.NewServerVersionFromString(obj.Deprecated)
	if err != nil {
		return err
	}
	return nil
}

var (
	NetworkThroughput = StatsLabel{
		Name:  "throughput",
		Help:  "Related to replication throughput",
		Added: base.VersionForPrometheusSupport,
	}
	NetworkLatency = StatsLabel{
		Name:  "latency",
		Help:  "Related to replication latency",
		Added: base.VersionForPrometheusSupport,
	}
	KvOp = StatsLabel{
		Name:  "kvOp",
		Help:  "Involves KV operation",
		Added: base.VersionForPrometheusSupport,
	}
	DiskLatency = StatsLabel{
		Name:  "diskLatency",
		Help:  "Latency due to disk operation",
		Added: base.VersionForPrometheusSupport,
	}
	ProcessLatency = StatsLabel{
		Name:  "processLatency",
		Help:  "Latency due to how busy XDCR process is, or due to induced latency like throttler",
		Added: base.VersionForPrometheusSupport,
	}
	InternalError = StatsLabel{
		Name:  "internalError",
		Help:  "Related to potential error conditions in the XDCR processing",
		Added: base.VersionForPrometheusSupport,
	}
	DocCountDiverge = StatsLabel{
		Name:  "docCountDiverge",
		Help:  "Could cause document count between source and target bucket to be different per design",
		Added: base.VersionForPrometheusSupport,
	}
)

// See: https://docs.google.com/document/d/183VfS6fi-Tn0lHc6oEHPFQgOmYOgnwbM28zWtGbyTUg/edit#
type statsPropertyMetaObj struct {
	Type  string `json:"type"`
	Help  string `json:"help"`
	Added string `json:"added"`

	UiName     string        `json:"uiName,omitempty"`
	Unit       string        `json:"unit,omitempty"`
	Labels     []interface{} `json:"labels,omitempty"`
	Deprecated string        `json:"deprecated,omitempty"`
	Notes      string        `json:"notes,omitempty"`
	Stability  string        `json:"stability,omitempty"`
}

func (s *statsPropertyMetaObj) ToStatsProperty() (*StatsProperty, error) {
	if s == nil {
		return nil, base.ErrorNilPtr
	}

	versionAdded, err := base.NewServerVersionFromString(s.Added)
	if err != nil {
		return nil, err
	}
	versionDeprecated, err := base.NewServerVersionFromString(s.Deprecated)
	if err != nil {
		return nil, err
	}
	stability, err := NewStatsStabilityFromStr(s.Stability)
	if err != nil {
		return nil, err
	}

	var labels []StatsLabel
	for _, labelRaw := range s.Labels {
		label, ok := labelRaw.(StatsLabel)
		if !ok {
			return nil, fmt.Errorf("Type is not StatsLabel but %v", reflect.TypeOf(labelRaw))
		}
		labels = append(labels, label)
	}

	obj := &StatsProperty{
		MetricType: StatsUnit{
			Metric: NewMetricTypeFromString(s.Type),
			Unit:   GlobalBaseReverseUnitTable[s.Unit],
		},
		Cardinality:       LowCardinality, // TODO: for now default to low
		VersionAdded:      versionAdded,
		VersionDeprecated: versionDeprecated,
		Description:       s.Help,
		Notes:             s.Notes,
		Stability:         stability,
		UiName:            s.UiName,
		Labels:            labels,
	}
	return obj, nil
}

type StatsProperty struct {
	MetricType        StatsUnit          // "type" and "unit" fields
	Cardinality       MetricCardinality  // not exported
	VersionAdded      base.ServerVersion // "added" field
	VersionDeprecated base.ServerVersion // "deprecated" field
	Description       string             // "help" field
	Notes             string             // "notes" field
	Stability         StatsStability     // "stability" field
	UiName            string             // "uiName" field
	Labels            StatsLabels        // "labels" field
}

func (s *StatsProperty) SameAs(other StatsProperty) bool {
	return s.MetricType == other.MetricType &&
		s.Cardinality == other.Cardinality &&
		s.VersionAdded.SameAs(other.VersionAdded) &&
		s.VersionDeprecated.SameAs(other.VersionDeprecated) &&
		s.Description == other.Description &&
		s.Notes == other.Notes &&
		s.Stability == other.Stability &&
		s.UiName == other.UiName &&
		s.Labels.SameAs(other.Labels)

}

func (s *StatsProperty) toMetaObj() *statsPropertyMetaObj {
	obj := &statsPropertyMetaObj{
		Type:       s.MetricType.Metric.String(),
		Help:       s.Description,
		Added:      s.VersionAdded.String(),
		UiName:     s.UiName,
		Unit:       s.MetricType.Unit.String(),
		Labels:     s.Labels.ToGenerics(),
		Deprecated: s.VersionDeprecated.String(),
		Notes:      s.Notes,
		Stability:  s.Stability.String(),
	}
	return obj
}

type StatisticsPropertyMap map[string]StatsProperty

func (sm StatisticsPropertyMap) GetPrometheusMetricName(internalConst string) (string, error) {
	var output []string
	// Must be prefixed by our service name
	output = append(output, PrometheusXDCRPrefix)
	// Then followed by the internal name
	output = append(output, internalConst)
	// Output the base unit
	statsProperty, ok := sm[internalConst]
	if !ok {
		return "", fmt.Errorf("%v is not a proper stats for prometheus", internalConst)
	}
	output = append(output, GlobalBaseUnitTable[statsProperty.MetricType.Unit])

	return strings.Join(output, PrometheusDelimiter), nil
}

func (sm StatisticsPropertyMap) GetPrometheusMetricType(internalConst string) (string, error) {
	statProperty, ok := sm[internalConst]
	if !ok {
		return "", fmt.Errorf("%v is not a proper stats for prometheus", internalConst)
	}
	switch statProperty.MetricType.Metric {
	case MetricTypeCounter:
		return PrometheusMetricTypeCounter, nil
	case MetricTypeGauge:
		return PrometheusMetricTypeGauge, nil
	default:
		return "", base.ErrorInvalidInput
	}
}

func (sm StatisticsPropertyMap) MarshalJSON() ([]byte, error) {
	outputMap := make(map[string]*statsPropertyMetaObj)

	for k, v := range sm {
		outputMap[k] = v.toMetaObj()
	}
	return json.Marshal(outputMap)
}

func (sm StatisticsPropertyMap) UnmarshalJSON(b []byte) error {
	inputMap := make(map[string]*statsPropertyMetaObj)
	err := json.Unmarshal(b, &inputMap)
	if err != nil {
		return err
	}

	for k, v := range inputMap {
		statProperty, err := v.ToStatsProperty()
		if err != nil {
			return err
		}
		sm[k] = *statProperty
	}
	return nil
}

// Anything that needs to be exported to prometheus should be in this map
// Anything else that should belong in statsMgr but is not meant for exporting should not be here
// Note, the statsMgr OVERVIEW stats must include the metric as a prerequisite
// UTF-8 for the Description but do not include backslash "\" nor newline char
// ns_server prefers that any "calculated" stats not to be exported (i.e. rate)
var GlobalStatsTable = StatisticsPropertyMap{
	DOCS_WRITTEN_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Number of docs written/sent to target cluster",
		Notes:        "This metric counts any type of mutations that are sent to the target cluster, and includes mutations that had failed target side Conflict Resolution",
		Stability:    Committed,
	},
	EXPIRY_DOCS_WRITTEN_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Number of expiry written to target",
		Stability:    Committed,
	},
	DELETION_DOCS_WRITTEN_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Number of deletion written to target",
		Stability:    Committed,
	},
	SET_DOCS_WRITTEN_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Number of SET_WITH_META written to target",
		Stability:    Committed,
	},
	ADD_DOCS_WRITTEN_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Number of adds successfully written to target",
		Notes:        "This means that the target did not have the source document by name prior to this operation",
		Stability:    Internal,
	},

	DELETION_DOCS_CAS_CHANGED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Number of deletion failed because target cas changed",
		Stability:    Internal,
	},
	SET_DOCS_CAS_CHANGED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Number of set failed because target cas changed",
		Stability:    Internal,
	},
	ADD_DOCS_CAS_CHANGED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Number of add failed because target cas changed",
		Stability:    Internal,
	},
	DOCS_MERGED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Number of conflicting docs successfully merged",
		Stability:    Internal,
	},
	EXPIRY_DOCS_MERGED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Number of expiry merged and written to source",
		Stability:    Internal,
	},
	DATA_MERGED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrBytes},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Amount of data merged for a replication",
		Stability:    Internal,
	},
	DOCS_MERGE_CAS_CHANGED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Number of merges failed because source cas changed",
		Stability:    Internal,
	},
	EXPIRY_MERGE_CAS_CHANGED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Number of expiry merges failed because source cas changed",
		Stability:    Internal,
	},

	DOCS_MERGE_FAILED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Number of conflicting docs failed to merge",
		Stability:    Internal,
		Labels:       []StatsLabel{InternalError},
	},
	EXPIRY_DOCS_MERGE_FAILED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Number of conflicting expiry docs failed to merge",
		Stability:    Internal,
		Labels:       []StatsLabel{InternalError},
	},
	DATA_MERGE_FAILED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrBytes},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Amount of data failed to merge",
		Stability:    Internal,
		Labels:       []StatsLabel{InternalError},
	},
	DOCS_PROCESSED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Number of docs processed for a replication",
		Stability:    Committed,
		Notes: "Each document is considered to be a single mutation or event tagged with a sequence number. " +
			"It is considered processed when a mutation is either replicated to the target, or not replicated due to " +
			"a decision made, such as a document being filtered, or if it loses source conflict resolution. Another " +
			"example can be a system event such as a collection creation that gets its own sequence number, but is " +
			"not actually something that can be replicated. These are also counted as a doc being processed.",
	},
	DATA_REPLICATED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrBytes},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Amount of data replicated for a replication",
		Stability:    Committed,
		UiName:       UIDataReplicated,
		Notes: "The number is kept for as long as a pipeline instance is active, " +
			"and resets to 0 when a pipeline is restarted. This applies to other types of \"accumulative\" counters.",
	},
	SIZE_REP_QUEUE_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrBytes},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Amount of data being queued to be sent in an out nozzle",
		Stability:    Committed,
		Notes: "The larger the amount of data being buffered to be sent will cause the goxdcr process to take up more memory." +
			"If too much memory is being used, consider decreasing the number of nozzles or tune such that less data " +
			"will be buffered",
	},
	DOCS_FILTERED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForAdvFilteringSupport,
		Description:  "Total number of documents filtered and not replicated due to any type of filtering",
		Stability:    Committed,
		Labels:       []StatsLabel{DocCountDiverge},
		UiName:       UIDocsFiltered,
	},
	DOCS_UNABLE_TO_FILTER_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForAdvFilteringSupport,
		Description:  "Number of documents that couldn't be filtered due to inability to parse the document through Advanced Filtering engine and were not replicated",
		Stability:    Committed,
		Labels:       []StatsLabel{InternalError, DocCountDiverge},
	},
	EXPIRY_FILTERED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForAdvFilteringSupport,
		Description:  "Number of documents filtered that had expiry flag set",
		Stability:    Committed,
		Labels:       []StatsLabel{DocCountDiverge},
	},
	DELETION_FILTERED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForAdvFilteringSupport,
		Description:  "Number of documents filtered that was of a DCP deletion",
		Stability:    Committed,
		Labels:       []StatsLabel{DocCountDiverge},
	},
	SET_FILTERED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForAdvFilteringSupport,
		Description:  "Number of documents filtered that was of a DCP mutation",
		Stability:    Committed,
		Labels:       []StatsLabel{DocCountDiverge},
	},
	EXPIRY_STRIPPED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForAdvFilteringSupport,
		Description:  "Number of documents replicated that had its TTL changed to 0 before writing to target (source is unmodified)",
		Stability:    Committed,
		Labels:       []StatsLabel{DocCountDiverge},
		Notes: "The source has an expiry set and the target does not. When the source document expires, it will " +
			"trigger a expiry event that should be replicated to the target. If that expiry event is not replicated " +
			"for any reason and is purged (i.e. tombstone purge interval), then the target doucment will contiue to " +
			"live for perpetuity. If the purpose is for archival such that the target should live for perpetuity, " +
			"then it is adv filter for expiry should also be set, but will cause document count divergence",
	},
	DOCS_FAILED_CR_SOURCE_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Number of documents (or tombstones) that was not replicated to the target due to conflict resolution evaluated on the source cluster",
		Stability:    Committed,
		Labels:       []StatsLabel{DocCountDiverge},
		UiName:       UIFailedCRSrc,
		Notes: "This metric contains all types of docs that were not successfully sent to target due to failed CR, " +
			"including deletions and expiries. It is also possible that a source document could fail CR due to a " +
			"tombstone that exists in the target bucket. In that case, it may look like that a document is not " +
			"replicated to the target, but in fact it is because target contains a tombstone that causes source CR " +
			"to lose. Vice versa, it is possible that a deletion on the source could lose, and the target document " +
			"continues to exist even though the source counterpart does not.",
	},
	EXPIRY_FAILED_CR_SOURCE_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Subset of the number of documents that failed source-side conflict resolution that specifically had expiry flag set",
		Stability:    Committed,
		Labels:       []StatsLabel{DocCountDiverge},
	},
	DELETION_FAILED_CR_SOURCE_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Subset of the number of documents that failed source-side conflict resolution that were delete operations",
		Stability:    Committed,
		Labels:       []StatsLabel{DocCountDiverge},
	},
	SET_FAILED_CR_SOURCE_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Subset of the number of documents that failed source-side conflict resolution that were set operations",
		Stability:    Internal,
		Labels:       []StatsLabel{DocCountDiverge},
	},
	DOCS_FAILED_CR_TARGET_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPeerToPeerSupport,
		Description:  "Number of documents failed conflict resolution at target cluster",
		Stability:    Volatile,
		Labels:       []StatsLabel{DocCountDiverge},
		UiName:       UIFailedCRTgt,
		Notes: "This metric contains all types of docs that were replicated to the target but were ultimately " +
			"rejected due to target side conflict-resolution. This can happen either due to optimistic replication " +
			"or due to other actors that are mutating the same document on the target bucket even if source CR " +
			"succeeded.",
	},
	ADD_FAILED_CR_TARGET_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPeerToPeerSupport,
		Description:  "Number of ADD failed conflict resolution at target cluster",
		Stability:    Internal,
		Labels:       []StatsLabel{DocCountDiverge},
	},
	DELETION_FAILED_CR_TARGET_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPeerToPeerSupport,
		Description:  "Number of DELETE failed conflict resolution at target cluster",
		Stability:    Volatile,
		Labels:       []StatsLabel{DocCountDiverge},
	},
	EXPIRY_FAILED_CR_TARGET_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPeerToPeerSupport,
		Description:  "Number of EXPIRE failed conflict resolution at target cluster",
		Stability:    Volatile,
		Labels:       []StatsLabel{DocCountDiverge},
	},
	SET_FAILED_CR_TARGET_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPeerToPeerSupport,
		Description:  "Number of SET_WITH_META failed conflict resolution at target cluster",
		Stability:    Volatile,
		Labels:       []StatsLabel{DocCountDiverge},
	},
	TARGET_DOCS_SKIPPED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Number of documents that was not replicated to the target because they originated from the target",
		Stability:    Internal,
	},
	EXPIRY_TARGET_DOCS_SKIPPED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Subset of the number of documents that originated from the target that specifically had expiry flag set",
		Stability:    Internal,
	},
	DELETION_TARGET_DOCS_SKIPPED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Subset of the number of documents that originated from the target that were delete operations",
		Stability:    Internal,
	},
	SET_TARGET_DOCS_SKIPPED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Subset of the number of documents that originated from the target that were set operations",
		Stability:    Internal,
	},

	CHANGES_LEFT_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Given the VBs of this node, the number of seqnos that need to be processed (either replicated or handled) before catching up to the high sequence numbers for the VBs",
		Stability:    Committed,
		UiName:       UIChangesLeft,
		Notes: "This metric is calculated as (totalChanges - docsProcessed)." +
			"TotalChanges is calculated as: SumForEachVB(HighSeqno) that this node owns." +
			"The term 'changes' is more of a misnomer as it is still defined as each mutation or event that gets " +
			"stamped with a sequence number. Since 7.0, system events also get stamped as a seqno per VB and thus " +
			"the changes_left term no longer accurately represents actual mutations that needs to be replicated." +
			"Regardless, this number represents the concept of how much work is to be done before a replication is " +
			"considered caught up.",
	},
	DOCS_LATENCY_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrMilliSecond},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "The rolling average amount of time it takes for the source cluster to receive the acknowledgement of a SET_WITH_META response after the Memcached request has been composed to be processed by the XDCR outnozzle",
		Stability:    Committed,
		Labels:       []StatsLabel{KvOp, NetworkLatency},
		UiName:       UIDocsLatency,
		Notes: "This metric indicates the lag time of both the network as well as the target KV set latency. The " +
			"latency tracks the followings: " +
			"1. The time it takes to issue a SET_WITH_META from the source to the target. " +
			"2. The time it takes for KV to handle the SET_WITH_META request. " +
			"3. The time it takes for KV to send a response back to XDCR indicating that a SET_WITH_META has been " +
			"handled. " +
			"When combined with traditional network diagnostic tools, one can use this number to differentiate " +
			"between the network latency as well as target KV latency.",
	},
	META_LATENCY_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrMilliSecond},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "The rolling average amount of time it takes once a getMeta command is composed to be sent to the time the request is handled once the target node has responded",
		Stability:    Committed,
		Labels:       []StatsLabel{KvOp, NetworkLatency},
		UiName:       UIMetaLatency,
		Notes: "This is similar to docs_latency but specifically for the GET_META command that is used for " +
			"source side conflict resolution",
	},
	GET_DOC_LATENCY_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrMilliSecond},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "The rolling average amount of time it takes once a get document command is composed to be sent to the time the request is handled once the target node has responded",
		Stability:    Internal,
		Labels:       []StatsLabel{KvOp, NetworkLatency},
	},
	RESP_WAIT_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrMilliSecond},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "The rolling average amount of time it takes from when a MemcachedRequest is created to be ready to route to an outnozzle to the time that the response has been heard back from the target node after a successful write",
		Stability:    Committed,
		Labels:       []StatsLabel{KvOp, NetworkLatency},
		Notes: "This metric indicates just the amount of wait time it takes for KV to send a respond back to XDCR as " +
			"part of the complete docs_latency metrics. This can be used to give more granularity into how " +
			"the overall latency situation looks like.",
	},
	MERGE_LATENCY_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrMilliSecond},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "The rolling average amount of time it takes from routing, conflict detection and resolution, to receive the acknowledgement of merge",
		Stability:    Internal,
		Labels:       []StatsLabel{KvOp, NetworkLatency},
	},

	DOCS_CHECKED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Across VBs for this node, the sum of all seqnos that have been considered to be checkpointed",
		Notes:        "This metric is often used in conjunction with docs_processed. The wider the difference means the more duplicate replication would take place if a replication pipeline were to restart, as it means less information is checkpointed",
		Stability:    Committed,
		UiName:       UIDocsChecked,
	},
	NUM_CHECKPOINTS_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "The number of times checkpoint operation has completed successfully since this XDCR process instance is made aware of this replication",
		Stability:    Committed,
	},
	TIME_COMMITING_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrMilliSecond},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "The rolling average amount of time it takes for a checkpoint operation to complete",
		Notes:        "The higher the number, the more burdened XDCR is or the slower the performance of simple_store (metakv)",
		Stability:    Committed,
		Labels:       []StatsLabel{DiskLatency, ProcessLatency},
	},
	NUM_FAILEDCKPTS_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "The number of times checkpoint operation has encountered an error since this XDCR process instance is made aware of this replication",
		Stability:    Committed,
		Labels:       []StatsLabel{InternalError},
	},
	//RATE_DOC_CHECKS_METRIC: StatsProperty{MetricType: StatsUnit{MetricTypeGauge, StatsMgrDocsPerSecond}, Cardinality: LowCardinality, VersionAdded: base.VersionForPrometheusSupport, VersionAdded: base.VersionInvalid, "The rate at which documents are being checkpointed"},

	DOCS_OPT_REPD_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Number of documents optimistically replicated to the target",
		Stability:    Committed,
		UiName:       UIOptReplicated,
		Notes: "Optimistic replication takes one less RTT, but could potential cause more network usage if the " +
			"documents are decent sized",
	},
	//RATE_OPT_REPD_METRIC: StatsProperty{MetricType: StatsUnit{MetricTypeGauge, StatsMgrDocsPerSecond}, Cardinality: LowCardinality, VersionAdded: base.VersionForPrometheusSupport, VersionAdded: base.VersionInvalid, "The rate at which documents are being replicated optimistically"},

	DOCS_RECEIVED_DCP_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "The number of set operations received from DCP",
		Stability:    Committed,
		UiName:       UIDocsFromDCP,
	},
	//RATE_RECEIVED_DCP_METRIC: StatsProperty{StatsUnit{MetricTypeGauge, StatsMgrDocsPerSecond}, Cardinality: LowCardinality, VersionAdded: base.VersionForPrometheusSupport, VersionAdded: base.VersionInvalid, "The rate at which XDCR is receiving documents from DCP for this replication"},

	EXPIRY_RECEIVED_DCP_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "The subset of documents received from DCP that either 1.) Is a DCP expiration or 2) Is a document that contains an expiration time",
		Stability:    Committed,
	},
	DELETION_RECEIVED_DCP_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "The subset of documents received from DCP that is a Delete action",
		Stability:    Committed,
	},
	SET_RECEIVED_DCP_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "The subset of documents received from DCP that is a mutation (set action)",
		Stability:    Committed,
	},

	DCP_DISPATCH_TIME_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrGolangTimeDuration},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "The rolling average amount of time it takes for a document to be received by XDCR from DCP, to the time it is queued up in the out nozzle ready to be sent",
		Stability:    Committed,
		Labels:       []StatsLabel{ProcessLatency},
	},
	DCP_DATACH_LEN: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "The number of items sent by KV DCP waiting for XDCR DCP nozzle to ingest and process",
		Stability:    Committed,
		Labels:       []StatsLabel{ProcessLatency, NetworkLatency},
	},

	THROTTLE_LATENCY_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrMilliSecond},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "The rolling average of the latency time introduced due to bandwith throttler",
		Stability:    Committed,
		Labels:       []StatsLabel{NetworkThroughput},
	},

	THROUGHPUT_THROTTLE_LATENCY_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrMilliSecond},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "The rolling average of the latency time introduced due to throughput throttler",
		Stability:    Committed,
		Labels:       []StatsLabel{NetworkLatency, ProcessLatency},
	},

	//RATE_REPLICATED_METRIC: StatsProperty{MetricType: StatsUnit{MetricTypeGauge, StatsMgrDocsPerSecond}, Cardinality: LowCardinality, VersionAdded: base.VersionForPrometheusSupport, base.VersionInvalid, "The rate at which documents from this node are being replicated to target nodes"},
	//BANDWIDTH_USAGE_METRIC: StatsProperty{MetricType: StatsUnit{MetricTypeGauge, StatsMgrBytesPerSecond}, Cardinality: LowCardinality, base.VersionForPrometheusSupport, base.VersionInvalid, "The data rate at which documents from this node are being replicated to target nodes"},

	DP_GET_FAIL_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForAdvFilteringSupport,
		Description:  "The total number of failed GET() operation on a reusable datapool within XDCR for the purpose of avoiding garbage generation",
		Stability:    Committed,
		Labels:       []StatsLabel{InternalError},
		Notes:        "This stats usually should be 0. If it is non-0, it could represent that the memory is under pressure.",
	},

	DOCS_CLONED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "The number of times a source mutation is cloned to be written to different target namespace",
		Notes: "This usually means that one source mutation is now going to exist in two or more target " +
			"collections, leading to mismatching doc counts between buckets. This can happen when migration mode " +
			"is turned on, and the migration filtering expression is not specific enough, leading to a single doc " +
			"matching multiple migration filtering expressions. To prevent this, ensure that the filtering expr " +
			"are more specific such that each doc is migrated to only one target collection.",
		Stability: Committed,
		Labels:    []StatsLabel{DocCountDiverge},
	},

	DELETION_CLONED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.Version7_2_1,
		Description:  "The number of times a source deletion or expiration is cloned to be written to multiple target namespaces",
		Notes:        "This usually happens in collection migration using explicit rule-based mapping where deletions and expirations will pass all rules.",
		Stability:    Committed,
	},
}
