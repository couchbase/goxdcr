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

type StatsMgrIface interface {
	Start(settings metadata.ReplicationSettingsMap) error
	Stop() error
	GetCountMetrics(key string) (int64, error)
	GetThroughSeqnosFromTsService() map[uint16]uint64
	GetVBCountMetrics(vb uint16) (base.VBCountMetricMap, error)
	SetVBCountMetrics(vb uint16, metricKVs base.VBCountMetricMap) error
	HandleLatestThroughSeqnos(SeqnoMap map[uint16]uint64)
}

const (
	// the number of docs written/sent to target cluster
	DOCS_WRITTEN_METRIC                = "docs_written"
	EXPIRY_DOCS_WRITTEN_METRIC         = "expiry_docs_written"
	DELETION_DOCS_WRITTEN_METRIC       = "deletion_docs_written"
	SET_DOCS_WRITTEN_METRIC            = "set_docs_written"
	ADD_DOCS_WRITTEN_METRIC            = "add_docs_written"
	DELETION_DOCS_CAS_CHANGED_METRIC   = "deletion_docs_cas_changed"
	SET_DOCS_CAS_CHANGED_METRIC        = "set_docs_cas_changed"
	ADD_DOCS_CAS_CHANGED_METRIC        = "add_docs_cas_changed"
	SUBDOC_CMD_DOCS_CAS_CHANGED_METRIC = "subdoc_cmd_docs_cas_changed"
	// To avoid cas rollback for a specific mobile/xdcr case, we send the doc using subdoc command
	DOCS_SENT_WITH_SUBDOC_SET    = base.DocsSentWithSubdocSet
	DOCS_SENT_WITH_SUBDOC_DELETE = base.DocsSentWithSubdocDelete

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
	DOCS_PROCESSED_METRIC               = "docs_processed"
	DATA_REPLICATED_METRIC              = "data_replicated"
	SIZE_REP_QUEUE_METRIC               = "size_rep_queue"
	DOCS_REP_QUEUE_METRIC               = base.DocsRepQueueStats
	DATA_REPLICATED_UNCOMPRESSED_METRIC = "data_replicated_uncompress"
	DOCS_COMPRESSION_SKIPPED_METRIC     = "docs_compression_skipped"

	DOCS_FILTERED_METRIC               = base.DocsFiltered
	DOCS_UNABLE_TO_FILTER_METRIC       = base.DocsUnableToFilter
	EXPIRY_FILTERED_METRIC             = base.ExpiryFiltered
	DELETION_FILTERED_METRIC           = base.DeletionFiltered
	SET_FILTERED_METRIC                = base.SetFiltered
	BINARY_FILTERED_METRIC             = base.BinaryFiltered
	EXPIRY_STRIPPED_METRIC             = base.ExpiryStripped
	DOCS_FILTERED_TXN_ATR_METRIC       = base.AtrTxnDocsFiltered
	DOCS_FILTERED_CLIENT_TXN_METRIC    = base.ClientTxnDocsFiltered
	DOCS_FILTERED_TXN_XATTR_METRIC     = base.DocsFilteredOnTxnXattr
	DOCS_FILTERED_USER_DEFINED_METRIC  = base.DocsFilteredOnUserDefinedFilter
	DOCS_FILTERED_MOBILE_METRIC        = base.MobileDocsFiltered
	DOCS_FILTERED_CAS_POISONING_METRIC = base.DocsCasPoisoned

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

	// For mobile
	SOURCE_SYNC_XATTR_REMOVED_METRIC    = "source_sync_xattr_removed"
	TARGET_SYNC_XATTR_PRESERVED_METRIC  = "target_sync_xattr_preserved"
	IMPORT_DOCS_FAILED_CR_SOURCE_METRIC = "import_docs_failed_cr_source"
	IMPORT_DOCS_WRITTEN_METRIC          = "import_docs_written"
	HLV_UPDATED_METRIC                  = "hlv_updated"
	HLV_PRUNED_METRIC                   = "hlv_pruned"
	HLV_PRUNED_AT_MERGE_METRIC          = "hlv_pruned_at_merge"

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

	// error counter metrics
	DP_GET_FAIL_METRIC           = "datapool_failed_gets"
	TARGET_EACCESS_METRIC        = "target_eaccess"
	TARGET_TMPFAIL_METRIC        = "target_tmpfail"
	TARGET_UNKNOWN_STATUS_METRIC = "target_unknown_status"

	DOCS_CLONED_METRIC     = "docs_cloned"
	DELETION_CLONED_METRIC = "deletion_cloned"

	// Pipeline Status to be exported as a guage
	PIPELINE_STATUS = "pipeline_status"
	PIPELINE_ERRORS = "pipeline_errors"

	// Guardrails
	GUARDRAIL_RESIDENT_RATIO_METRIC = base.GuardrailResidentRatio
	GUARDRAIL_DATA_SIZE_METRIC      = base.GuardrailDataSize
	GUARDRAIL_DISK_SPACE_METRIC     = base.GuardrailDiskSpace

	SYSTEM_EVENTS_RECEIVED_DCP_METRIC = "system_events_received_from_dcp"
	SEQNO_ADV_RECEIVED_DCP_METRIC     = "seqno_adv_received_from_dcp"

	//docs sent with poisoned CAS
	DOCS_SENT_WITH_POISONED_CAS_ERROR   = base.DocsSentWithPoisonedCasErrorMode
	DOCS_SENT_WITH_POISONED_CAS_REPLACE = base.DocsSentWithPoisonedCasReplaceMode
)

const (
	PrometheusXDCRPrefix = "xdcr"
	PrometheusDelimiter  = "_"

	PrometheusBaseNoUnit       = ""
	PrometheusBaseUnitTime     = "seconds"
	PrometheusBaseUnitSize     = "bytes"
	PrometheusBaseUnitDataRate = "bytes_per_second"
	PrometheusBaseUnitReplRate = "docs_per_second"

	PrometheusAccumulateUnit = "total"

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
	StatsMgrNoUnit              StatsMgrUnitType = iota
	StatsMgrNonCumulativeNoUnit StatsMgrUnitType = iota
	StatsMgrSeconds             StatsMgrUnitType = iota
	StatsMgrMilliSecond         StatsMgrUnitType = iota
	StatsMgrGolangTimeDuration  StatsMgrUnitType = iota
	StatsMgrBytes               StatsMgrUnitType = iota
	StatsMgrMegaBytesPerSecond  StatsMgrUnitType = iota // MB/second, where 1 MB = 1024*1024 bytes instead of 1000*1000 bytes
	StatsMgrDocsPerSecond       StatsMgrUnitType = iota
)

func (s StatsMgrUnitType) String() string {
	if s == StatsMgrNoUnit || s == StatsMgrNonCumulativeNoUnit {
		return ""
	}

	return GlobalBaseUnitTable[s]
}

var GlobalBaseUnitTable = map[StatsMgrUnitType]string{
	StatsMgrNoUnit:              PrometheusAccumulateUnit,
	StatsMgrNonCumulativeNoUnit: PrometheusBaseNoUnit,
	StatsMgrSeconds:             PrometheusBaseUnitTime,
	StatsMgrMilliSecond:         PrometheusBaseUnitTime,
	StatsMgrGolangTimeDuration:  PrometheusBaseUnitTime,
	StatsMgrBytes:               PrometheusBaseUnitSize,
	StatsMgrMegaBytesPerSecond:  PrometheusBaseUnitDataRate,
	StatsMgrDocsPerSecond:       PrometheusBaseUnitReplRate,
}

var GlobalBaseReverseUnitTable = map[string]StatsMgrUnitType{
	PrometheusAccumulateUnit:   StatsMgrNoUnit,
	PrometheusBaseNoUnit:       StatsMgrNonCumulativeNoUnit,
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
		if label.HasNameOnly() {
			generics = append(generics, label.Name)
		} else {
			generics = append(generics, label)
		}
	}
	return generics
}

type statsLabelMetaObj struct {
	Name       string `json:"name"`
	Help       string `json:"help,omitempty"`
	Added      string `json:"added,omitempty"`
	Deprecated string `json:"deprecated,omitempty"`
}

// Name is mandatory
// the other fields are not
type StatsLabel struct {
	Name       string
	Help       string
	Added      base.ServerVersion
	Deprecated base.ServerVersion
}

func (s StatsLabel) SameAs(other StatsLabel) bool {
	return s.Name == other.Name && s.Help == other.Help &&
		s.Added.SameAs(other.Added) && s.Deprecated.SameAs(other.Deprecated)
}

func (s StatsLabel) HasNameOnly() bool {
	return s.Help == "" && len(s.Added) == 0 && len(s.Deprecated) == 0
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
	SourceBucketNameLabel  = StatsLabel{Name: PrometheusSourceBucketLabel}
	TargetBucketNameLabel  = StatsLabel{Name: PrometheusTargetBucketLabel}
	TargetClusterUUIDLabel = StatsLabel{Name: PrometheusTargetClusterUuidLabel}
	PipelineTypeLabel      = StatsLabel{Name: PrometheusPipelineTypeLabel}
	PipelineStatusLabel    = StatsLabel{Name: PrometheusPipelineStatusLabel}
)

var StandardLabels = StatsLabels{
	SourceBucketNameLabel,
	TargetClusterUUIDLabel,
	TargetBucketNameLabel,
	PipelineTypeLabel,
}

var PipelineStatusLabels = StatsLabels{
	SourceBucketNameLabel,
	TargetClusterUUIDLabel,
	TargetBucketNameLabel,
	PipelineTypeLabel,
	PipelineStatusLabel,
}

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
		var label StatsLabel
		labelName, ok := labelRaw.(string)
		if ok {
			label = StatsLabel{Name: labelName}
		} else {
			label, ok = labelRaw.(StatsLabel)
			if !ok {
				return nil, fmt.Errorf("Type is not StatsLabel but %v", reflect.TypeOf(labelRaw))
			}
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
	if GlobalBaseUnitTable[statsProperty.MetricType.Unit] != "" {
		output = append(output, GlobalBaseUnitTable[statsProperty.MetricType.Unit])
	}

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
		Description:  "Number of docs Document Mutations written/sent to the Target",
		Notes:        "This metric counts any type of mutations that are sent to the target cluster, and includes mutations that had failed target side Conflict Resolution",
		Stability:    Committed,
		Labels:       StandardLabels,
	},
	EXPIRY_DOCS_WRITTEN_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Number of Expirations written to the Target",
		Stability:    Committed,
		Labels:       StandardLabels,
	},
	DELETION_DOCS_WRITTEN_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Number of Deletions written to Target",
		Stability:    Committed,
		Labels:       StandardLabels,
	},
	SET_DOCS_WRITTEN_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Number of Set operations successfully written to the Target",
		Stability:    Committed,
		Labels:       StandardLabels,
	},
	ADD_DOCS_WRITTEN_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Number of Add operations successfully written to the Target",
		Notes:        "This means that the Target did not have the Source Document by name prior to this operation",
		Stability:    Internal,
		Labels:       StandardLabels,
	},

	DELETION_DOCS_CAS_CHANGED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Number of Deletions failed because Target CAS changed",
		Stability:    Internal,
		Labels:       StandardLabels,
	},
	SET_DOCS_CAS_CHANGED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Number of Set operations that failed because the CAS on the Target changed",
		Stability:    Internal,
		Labels:       StandardLabels,
	},
	ADD_DOCS_CAS_CHANGED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Number of Add operations that failed because the CAS on the Target changed",
		Stability:    Internal,
		Labels:       StandardLabels,
	},
	SUBDOC_CMD_DOCS_CAS_CHANGED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForMobileSupport,
		Description:  "Number of Subdoc sets and delete operations that failed because the CAS on the Target changed",
		Stability:    Internal,
		Labels:       StandardLabels,
	},

	DOCS_MERGED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Number of conflicting docs successfully merged and written to the Source after performing Source custom conflict-resolution",
		Stability:    Internal,
		Labels:       StandardLabels,
	},
	EXPIRY_DOCS_MERGED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Number of Expirations merged and written to the Source",
		Stability:    Internal,
		Labels:       StandardLabels,
	},
	DATA_MERGED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrBytes},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Amount of data merged for a Replication when performing Source custom conflict-resolution",
		Stability:    Internal,
		Labels:       StandardLabels,
	},
	DOCS_MERGE_CAS_CHANGED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Number of documents from Source custom conflict-resolution that failed to merge back to Source because the Source CAS changed",
		Stability:    Internal,
		Labels:       StandardLabels,
	},
	EXPIRY_MERGE_CAS_CHANGED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Number of expiry from Source custom conflict-resolution that failed to merge back to Source because the Source CAS changed",
		Stability:    Internal,
		Labels:       StandardLabels,
	},

	DOCS_MERGE_FAILED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Number of conflicting docs failed to merge as part of Source custom conflict-resolution",
		Stability:    Internal,
		Labels:       StandardLabels,
	},
	EXPIRY_DOCS_MERGE_FAILED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Number of conflicting expiry docs failed to merge as part of Source custom conflict-resolution",
		Stability:    Internal,
		Labels:       StandardLabels,
	},
	DATA_MERGE_FAILED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrBytes},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Amount of data failed to merge as part of Source custom conflict-resolution",
		Stability:    Internal,
		Labels:       StandardLabels,
	},

	DOCS_PROCESSED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Number of Documents processed for a Replication",
		Stability:    Committed,
		Labels:       StandardLabels,
		Notes: "Each Document is considered to be a single Mutation or event tagged with a Sequence Number. " +
			"It is considered processed when a Mutation is either Replicated to the Target Cluster, " +
			"or not replicated due to a decision made, such as a document being filtered, or if it loses Source " +
			"Conflict Resolution. Another example can be a system event such as a Collection creation that gets its " +
			"own Sequence Number, but is not actually something that can be replicated. " +
			"These are also counted as a doc being processed.",
	},
	DATA_REPLICATED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrBytes},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Amount of data replicated for a Replication",
		Stability:    Committed,
		UiName:       UIDataReplicated,
		Labels:       StandardLabels,
	},
	SIZE_REP_QUEUE_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrBytes},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Amount of data being queued to be sent in a Target Nozzle",
		Stability:    Committed,
		Labels:       StandardLabels,
		Notes: "The larger the amount of data being buffered to be sent will cause the goxdcr process to take up more memory.  " +
			"If too much memory is being used, consider decreasing the number of nozzles or tune such that less data " +
			"will be buffered",
	},
	DATA_REPLICATED_UNCOMPRESSED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrBytes},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Theoretical amount of data replicated for a Replication if compression were not used",
		Stability:    Committed,
		Labels:       StandardLabels,
		Notes:        "This stat is used in conjunction with data_replicated such that compression ratios can be calculated",
	},
	DOCS_COMPRESSION_SKIPPED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCasPoisonDetection,
		Description:  "Total number of documents whose compression was skipped before replication, due to a larger snappy-compressed size",
		Stability:    Committed,
		Labels:       StandardLabels,
	},

	DOCS_FILTERED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Total number of documents filtered and not replicated due to any type of filtering",
		Stability:    Committed,
		Labels:       StandardLabels,
		UiName:       UIDocsFiltered,
	},
	DOCS_UNABLE_TO_FILTER_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Number of Document Mutations that couldn't be filtered due to inability to parse the document through Advanced Filtering engine and were not replicated",
		Stability:    Committed,
		Labels:       StandardLabels,
	},
	EXPIRY_FILTERED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Number of Expirations filtered Source-side",
		Stability:    Committed,
		Labels:       StandardLabels,
	},
	DELETION_FILTERED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Number of Deletions that were filtered Source-side",
		Stability:    Committed,
		Labels:       StandardLabels,
	},
	SET_FILTERED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Number of documents filtered that was of a DCP mutation",
		Stability:    Committed,
		Labels:       StandardLabels,
	},
	BINARY_FILTERED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForSupportability,
		Description:  "Number of documents filtered that were binary documents",
		Stability:    Committed,
		Labels:       StandardLabels,
	},
	EXPIRY_STRIPPED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Number of Document Mutations replicated that had the TTL changed to 0 before writing to Target (Source is unmodified)",
		Stability:    Committed,
		Labels:       StandardLabels,
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
		Description:  "Number of documents (or tombstones) that were not replicated to the Target due to Conflict Resolution evaluated on the Source",
		Stability:    Committed,
		Labels:       StandardLabels,
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
		Description:  "Number of Expirations that failed Source-side Conflict Resolution",
		Stability:    Committed,
		Labels:       StandardLabels,
	},
	DELETION_FAILED_CR_SOURCE_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Number of Deletions that failed Source-side Conflict Resolution",
		Stability:    Committed,
		Labels:       StandardLabels,
	},
	SET_FAILED_CR_SOURCE_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Number of Set operations that failed Source-side Conflict Resolution",
		Stability:    Internal,
		Labels:       StandardLabels,
	},

	DOCS_FAILED_CR_TARGET_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPeerToPeerSupport,
		Description:  "Number of documents failed Conflict Resolution at the Target",
		Stability:    Committed,
		Labels:       StandardLabels,
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
		Description:  "Number of Add operations failed Conflict Resolution at the Target",
		Stability:    Internal,
		Labels:       StandardLabels,
	},
	DELETION_FAILED_CR_TARGET_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPeerToPeerSupport,
		Description:  "Number of Deletions that failed Conflict Resolution at the Target",
		Stability:    Committed,
		Labels:       StandardLabels,
	},
	EXPIRY_FAILED_CR_TARGET_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPeerToPeerSupport,
		Description:  "Number of Expirations that failed Conflict Resolution at the Target",
		Stability:    Volatile,
		Labels:       StandardLabels,
	},
	SET_FAILED_CR_TARGET_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPeerToPeerSupport,
		Description:  "Number of Set operations that failed Conflict Resolution at the Target",
		Stability:    Volatile,
		Labels:       StandardLabels,
	},

	TARGET_DOCS_SKIPPED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Number of Document Mutations that were not replicated to the Target because they originated from the Target",
		Stability:    Internal,
		Labels:       StandardLabels,
	},
	EXPIRY_TARGET_DOCS_SKIPPED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Subset of the number of Document Mutations that originated from the Target that specifically had Expiry flag set",
		Stability:    Internal,
		Labels:       StandardLabels,
	},
	DELETION_TARGET_DOCS_SKIPPED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Subset of the number of documents that originated from the target that were delete operations",
		Stability:    Internal,
		Labels:       StandardLabels,
	},
	SET_TARGET_DOCS_SKIPPED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCcrDpSupport,
		Description:  "Subset of the number of documents that originated from the target that were set operations",
		Stability:    Internal,
		Labels:       StandardLabels,
	},

	SOURCE_SYNC_XATTR_REMOVED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForMobileSupport,
		Description:  "Number of mutations with source mobile extended attributes removed",
		Stability:    Committed,
		Labels:       StandardLabels,
	},
	TARGET_SYNC_XATTR_PRESERVED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForMobileSupport,
		Description:  "Number of mutations with target mobile extended attributes preserved",
		Stability:    Committed,
		Labels:       StandardLabels,
	},
	IMPORT_DOCS_FAILED_CR_SOURCE_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForMobileSupport,
		Description:  "Number of mobile import mutations failed Source side Conflict Resolution. This is only counted in LWW buckets when enableCrossClusterVersioning is true",
		Stability:    Committed,
		Labels:       StandardLabels,
	},
	IMPORT_DOCS_WRITTEN_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForMobileSupport,
		Description:  "Number of import mutations sent",
		Stability:    Committed,
		Labels:       StandardLabels,
	},
	HLV_UPDATED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForMobileSupport,
		Description:  "Number of mutations with HLV updated",
		Stability:    Committed,
		Labels:       StandardLabels,
	},
	HLV_PRUNED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForMobileSupport,
		Description:  "Number of mutations with HLV pruned",
		Stability:    Committed,
		Labels:       StandardLabels,
	},
	HLV_PRUNED_AT_MERGE_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForMobileSupport,
		Description:  "Number of mutations with HLV pruned when it is merged with target document",
		Stability:    Committed,
		Labels:       StandardLabels,
	},

	CHANGES_LEFT_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Given the vBuckets of this node, the number of sequence numbers that need to be processed (either replicated or handled) before catching up to the high sequence numbers for the vBuckets",
		Stability:    Committed,
		Labels:       StandardLabels,
		UiName:       UIChangesLeft,
		Notes: "This metric is calculated as (totalChanges - docsProcessed).  TotalChanges is calculated as: " +
			"SumForEachVB(HighSeqno) that this node owns.The term 'changes' is more of a misnomer as it is still " +
			"defined as each mutation or event that gets stamped with a sequence number. Since 7.0, system events " +
			"also get stamped as a seqno per VB and thus the changes_left term no longer accurately represents " +
			"actual mutations that needs to be replicated. Regardless, this number represents the concept of how much " +
			"work is to be done before a replication is considered caught up.",
	},
	DOCS_LATENCY_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrMilliSecond},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description: "The rolling average amount of time it takes for the source cluster to receive the " +
			"acknowledgement of a SET_WITH_META response after the Memcached request has been composed to be " +
			"processed by the XDCR Target Nozzle",
		Stability: Committed,
		Labels:    StandardLabels,
		UiName:    UIDocsLatency,
		Notes: "This metric indicates the lag time of both the network as well as the target Key-Value set latency. The " +
			"latency tracks the followings: " +
			"1. The time it takes to issue a SET_WITH_META from the source to the target. " +
			"2. The time it takes for data service to handle the SET_WITH_META request. " +
			"3. The time it takes for data service to send a response back to XDCR indicating that a SET_WITH_META has been " +
			"handled. " +
			"When combined with traditional network diagnostic tools, one can use this number to differentiate " +
			"between the network latency as well as target data service latency.",
	},
	META_LATENCY_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrMilliSecond},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "The rolling average amount of time it takes once a getMeta command is composed to be sent to the time the request is handled once the target node has responded",
		Stability:    Committed,
		Labels:       StandardLabels,
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
		Labels:       StandardLabels,
	},
	RESP_WAIT_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrMilliSecond},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "The rolling average amount of time it takes from when a MemcachedRequest is created to be ready to route to an outnozzle to the time that the response has been heard back from the target node after a successful write",
		Stability:    Committed,
		Labels:       StandardLabels,
		Notes: "This metric indicates just the amount of wait time it takes for data service to send a respond back to XDCR as " +
			"part of the complete docs_latency metrics. This can be used to give more granularity into how " +
			"the overall latency situation looks like.",
	},
	MERGE_LATENCY_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrMilliSecond},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "The rolling average amount of time it takes from routing, conflict detection and resolution, to receive the acknowledgement of merge",
		Stability:    Internal,
		Labels:       StandardLabels,
	},

	DOCS_CHECKED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Across vBuckets for this node, the sum of all sequence numbers that have been considered to be checkpointed",
		Notes:        "This metric is often used in conjunction with docs_processed. The wider the difference means the more duplicate replication would take place if a replication pipeline were to restart, as it means less information is checkpointed",
		Stability:    Committed,
		Labels:       StandardLabels,
		UiName:       UIDocsChecked,
	},
	NUM_CHECKPOINTS_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "The number of times checkpoint operation has completed successfully since this XDCR process instance is made aware of this replication",
		Stability:    Committed,
		Labels:       StandardLabels,
	},
	TIME_COMMITING_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrMilliSecond},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "The rolling average amount of time it takes for a checkpoint operation to complete",
		Notes:        "The higher the number, the more burdened XDCR is or the slower the performance of simple_store (metakv)",
		Stability:    Committed,
		Labels:       StandardLabels,
	},
	NUM_FAILEDCKPTS_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "The number of times checkpoint operation has encountered an error since this XDCR process instance is made aware of this replication",
		Stability:    Committed,
		Labels:       StandardLabels,
	},
	//RATE_DOC_CHECKS_METRIC: StatsProperty{MetricType: StatsUnit{MetricTypeGauge, StatsMgrDocsPerSecond}, Cardinality: LowCardinality, VersionAdded: base.VersionForPrometheusSupport, VersionAdded: base.VersionInvalid, "The rate at which documents are being checkpointed"},

	DOCS_OPT_REPD_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Number of Documents Optimistically Replicated to the Target Cluster",
		Stability:    Committed,
		Labels:       StandardLabels,
		UiName:       UIOptReplicated,
		Notes: "Optimistic Replication takes one less round trip on the network, " +
			"but could potentially cause more network usage if the documents are reasonably sized",
	},
	//RATE_OPT_REPD_METRIC: StatsProperty{MetricType: StatsUnit{MetricTypeGauge, StatsMgrDocsPerSecond}, Cardinality: LowCardinality, VersionAdded: base.VersionForPrometheusSupport, VersionAdded: base.VersionInvalid, "The rate at which documents are being replicated optimistically"},

	DOCS_RECEIVED_DCP_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Number of Document Mutations received from the Data Service",
		Stability:    Committed,
		Labels:       StandardLabels,
		UiName:       UIDocsFromDCP,
	},
	//RATE_RECEIVED_DCP_METRIC: StatsProperty{StatsUnit{MetricTypeGauge, StatsMgrDocsPerSecond}, Cardinality: LowCardinality, VersionAdded: base.VersionForPrometheusSupport, VersionAdded: base.VersionInvalid, "The rate at which XDCR is receiving documents from DCP for this replication"},

	EXPIRY_RECEIVED_DCP_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Number of Expirations or documents with TTL received from the Data Service",
		Stability:    Committed,
		Labels:       StandardLabels,
	},
	DELETION_RECEIVED_DCP_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Number of Deletions received from the Data Service",
		Stability:    Committed,
		Labels:       StandardLabels,
	},
	SET_RECEIVED_DCP_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Number of Sets received from the Data Service",
		Stability:    Committed,
		Labels:       StandardLabels,
	},

	DCP_DISPATCH_TIME_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrGolangTimeDuration},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description: "The rolling average amount of time it takes for a document to be received by XDCR from the " +
			"Data Service, to the time it is queued up in the Target Nozzle ready to be sent",
		Stability: Committed,
		Labels:    StandardLabels,
	},
	DCP_DATACH_LEN: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "The number of items sent by the Data Service waiting for the XDCR Source Nozzle to ingest and process",
		Stability:    Committed,
		Labels:       StandardLabels,
	},

	THROTTLE_LATENCY_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrMilliSecond},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "The rolling average of the latency time introduced due to bandwith throttler",
		Stability:    Committed,
		Labels:       StandardLabels,
	},

	THROUGHPUT_THROTTLE_LATENCY_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrMilliSecond},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "The rolling average of the latency time introduced due to throughput throttler",
		Stability:    Committed,
		Labels:       StandardLabels,
	},

	//RATE_REPLICATED_METRIC: StatsProperty{MetricType: StatsUnit{MetricTypeGauge, StatsMgrDocsPerSecond}, Cardinality: LowCardinality, VersionAdded: base.VersionForPrometheusSupport, base.VersionInvalid, "The rate at which documents from this node are being replicated to target nodes"},
	//BANDWIDTH_USAGE_METRIC: StatsProperty{MetricType: StatsUnit{MetricTypeGauge, StatsMgrBytesPerSecond}, Cardinality: LowCardinality, base.VersionForPrometheusSupport, base.VersionInvalid, "The data rate at which documents from this node are being replicated to target nodes"},

	DP_GET_FAIL_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "The total number of failed GET() operation on a reusable datapool within XDCR for the purpose of avoiding garbage generation",
		Stability:    Committed,
		Labels:       StandardLabels,
		Notes:        "This stats usually should be 0. If it is non-0, it could represent that the memory is under pressure.",
	},
	TARGET_EACCESS_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForSupportability,
		Description:  "The total number of EACCESS errors returned from the target node.",
		Stability:    Committed,
		Labels:       StandardLabels,
	},
	TARGET_TMPFAIL_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForSupportability,
		Description:  "The total number of TMPFAIL errors returned from the target node.",
		Stability:    Committed,
		Labels:       StandardLabels,
	},
	TARGET_UNKNOWN_STATUS_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForSupportability,
		Description:  "The total number of writes to target data service that returned with a status code that XDCR cannot comprehend",
		Stability:    Committed,
		Labels:       StandardLabels,
	},

	DOCS_CLONED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForPrometheusSupport,
		Description:  "Number of Source Document Mutation cloned to be written to different Target Namespaces",
		Notes: "This usually means that one source mutation is now going to exist in two or more target " +
			"collections, leading to mismatching doc counts between buckets. This can happen when migration mode " +
			"is turned on, and the migration filtering expression is not specific enough, leading to a single doc " +
			"matching multiple migration filtering expressions. To prevent this, ensure that the filtering expr " +
			"are more specific such that each doc is migrated to only one target collection.",
		Stability: Committed,
		Labels:    StandardLabels,
	},

	DELETION_CLONED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.Version7_2_1,
		Description:  "The number of times a Source Deletion or Expiration is cloned to be written to multiple Target Namespaces",
		Notes:        "This usually happens in collection migration using explicit rule-based mapping where Deletions and Expirations will pass all rules.",
		Stability:    Committed,
		Labels:       StandardLabels,
	},

	PIPELINE_STATUS: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrNonCumulativeNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForSupportability,
		Description:  "The pipeline status for a specific pipeline, where it could be paused, running or, error",
		Notes:        "A set of stats that represents the state of a pipeline, whether or not it is running or manually paused, or is in a erroneous state",
		Stability:    Committed,
		Labels:       PipelineStatusLabels,
	},

	PIPELINE_ERRORS: StatsProperty{
		MetricType:   StatsUnit{MetricTypeGauge, StatsMgrNonCumulativeNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForSupportability,
		Description:  "The number of currently present errors for a specific Replication Pipeline",
		Notes: "If the number is non zero, it could indicate potential replication errors that requires some " +
			"human intervention to look into the UI console or logs to decipher what errors could currently exist.",
		Stability: Committed,
		Labels:    StandardLabels,
	},

	DOCS_FILTERED_TXN_ATR_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForSupportability,
		Description:  "Total number of documents filtered and not replicated because the documents were ATR documents",
		Stability:    Committed,
		Labels:       StandardLabels,
	},

	DOCS_FILTERED_TXN_XATTR_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForSupportability,
		Description:  "Total number of documents filtered and not replicated due to the presence of transaction related xattrs in it",
		Stability:    Committed,
		Labels:       StandardLabels,
	},

	DOCS_FILTERED_CLIENT_TXN_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForSupportability,
		Description:  "Total number of documents filtered and not replicated because the documents were transaction client records",
		Stability:    Committed,
		Labels:       StandardLabels,
	},

	DOCS_FILTERED_USER_DEFINED_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForSupportability,
		Description:  "Total number of documents filtered and not replicated because of user defined filter expressions",
		Stability:    Committed,
		Labels:       StandardLabels,
	},

	DOCS_FILTERED_MOBILE_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForSupportability,
		Description:  "Total number of documents filtered and not replicated because the documents were mobile records",
		Stability:    Committed,
		Labels:       StandardLabels,
	},

	GUARDRAIL_RESIDENT_RATIO_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForSupportability,
		Description:  "The number of writes that target rejected due to the target bucket being under the resident ratio threshold",
		Notes: "When target bucket has guardrail enabled and a threshold set, it will return this error " +
			"indicating that the bucket is currently below the resident ratio. Data writes can only continue once " +
			"the target bucket's resident ratio rises above the threshold",
		Stability: Committed,
		Labels:    StandardLabels,
	},
	GUARDRAIL_DATA_SIZE_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForSupportability,
		Description:  "The number of writes that target rejected because each target data node is holding too much data",
		Notes: "When target bucket has guardrail enabled and a threshold set, it will return this error " +
			"indicating that the target data nodes are holding too much data per node, which could lead to " +
			"rebalance failures or cluster instability. To resume replication, additional data nodes must be added",
		Stability: Committed,
		Labels:    StandardLabels,
	},
	GUARDRAIL_DISK_SPACE_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForSupportability,
		Description:  "The number of writes that target rejected because a data node is running out of disk space",
		Notes: "When target bucket has guardrail enabled and a threshold set, it will return this error " +
			"indicating that one or more data nodes is running out of disk space. " +
			"To resume replication, additional disk storage must be added",
		Stability: Committed,
		Labels:    StandardLabels,
	},
	SYSTEM_EVENTS_RECEIVED_DCP_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForSupportability,
		Description:  "The number of system events received from source data service",
		Notes: "These events are sent down from source data service and counted as a processed document in " +
			"statistics such as collection creation events, but are not actually replicable data",
		Stability: Committed,
		Labels:    StandardLabels,
	},
	SEQNO_ADV_RECEIVED_DCP_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForSupportability,
		Description:  "The number of seqno advance events received from source data service",
		Notes: "These events are sent down from source data service and counted as a processed document in " +
			"statistics, but are not actually replicable data",
		Stability: Committed,
		Labels:    StandardLabels,
	},
	DOCS_SENT_WITH_SUBDOC_DELETE: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForMobileSupport,
		Description:  "The number of deletes issued using subdoc command instead of delete_with_meta to avoid cas rollback on target",
		Stability:    Committed,
		Labels:       StandardLabels,
	},
	DOCS_SENT_WITH_SUBDOC_SET: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForMobileSupport,
		Description:  "The number of sets issued using subdoc command instead of set_with_meta to avoid cas rollback on target",
		Stability:    Committed,
		Labels:       StandardLabels,
	},
	DOCS_FILTERED_CAS_POISONING_METRIC: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCasPoisonDetection,
		Description:  "Total number of documents not replicated because cas is beyond acceptable drift threshold",
		Stability:    Committed,
		Labels:       StandardLabels,
	},
	DOCS_SENT_WITH_POISONED_CAS_ERROR: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCasPoisonDetection,
		Description:  "The total number of documents that failed the set_with_meta operation due to the set CAS exceeding the acceptable threshold on the target data service.",
		Stability:    Committed,
		Labels:       StandardLabels,
	},
	DOCS_SENT_WITH_POISONED_CAS_REPLACE: StatsProperty{
		MetricType:   StatsUnit{MetricTypeCounter, StatsMgrNoUnit},
		Cardinality:  LowCardinality,
		VersionAdded: base.VersionForCasPoisonDetection,
		Description:  "The total number of documents replicated to the target, where the target data service regenerated CAS values because the set CAS exceeded the acceptable threshold on the target's data service.",
		Stability:    Committed,
		Labels:       StandardLabels,
	},
}

const (
	PrometheusTargetClusterUuidLabel = "targetClusterUUID"
	PrometheusSourceBucketLabel      = "sourceBucketName"
	PrometheusTargetBucketLabel      = "targetBucketName"
	PrometheusPipelineTypeLabel      = "pipelineType"
	PrometheusPipelineStatusLabel    = "status"
)
