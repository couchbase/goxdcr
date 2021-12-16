// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_def

import (
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/metadata"
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

	DOCS_CLONED_METRIC = "docs_cloned"
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

type MetricType int

const MetricTypeInvalid MetricType = -1

// https://prometheus.io/docs/concepts/metric_types/
// For couchbase, only support these two
const (
	MetricTypeCounter MetricType = 0
	MetricTypeGauge   MetricType = 1
)

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

var GlobalBaseUnitTable = map[StatsMgrUnitType]string{
	StatsMgrNoUnit:             PrometheusBaseNoUnit,
	StatsMgrSeconds:            PrometheusBaseUnitTime,
	StatsMgrMilliSecond:        PrometheusBaseUnitTime,
	StatsMgrGolangTimeDuration: PrometheusBaseUnitTime,
	StatsMgrBytes:              PrometheusBaseUnitSize,
	StatsMgrMegaBytesPerSecond: PrometheusBaseUnitDataRate,
	StatsMgrDocsPerSecond:      PrometheusBaseUnitReplRate,
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

type StatsProperty struct {
	MetricType  StatsUnit
	Cardinality MetricCardinality
	Description string
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

// Anything that needs to be exported to prometheus should be in this map
// Anything else that should belong in statsMgr but is not meant for exporting should not be here
// Note, the statsMgr OVERVIEW stats must include the metric as a prerequisite
// UTF-8 for the Description but do not include backslash "\" nor newline char
// ns_server prefers that any "calculated" stats not to be exported (i.e. rate)
var GlobalStatsTable = StatisticsPropertyMap{
	DOCS_WRITTEN_METRIC:          StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Number of docs written/sent to target cluster"},
	EXPIRY_DOCS_WRITTEN_METRIC:   StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Number of expiry written to target"},
	DELETION_DOCS_WRITTEN_METRIC: StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Number of deletion written to target"},
	SET_DOCS_WRITTEN_METRIC:      StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Number of sets written to target"},
	ADD_DOCS_WRITTEN_METRIC:      StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Number of adds successfully written to target, meaning that target does not have the identical doc by name"},

	DELETION_DOCS_CAS_CHANGED_METRIC: StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Number of deletion failed because target cas changed"},
	SET_DOCS_CAS_CHANGED_METRIC:      StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Number of set failed because target cas changed"},
	ADD_DOCS_CAS_CHANGED_METRIC:      StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Number of add failed because target cas changed"},

	DOCS_MERGED_METRIC:              StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Number of conflicting docs successfully merged"},
	EXPIRY_DOCS_MERGED_METRIC:       StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Number of expiry merged and written to source"},
	DATA_MERGED_METRIC:              StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrBytes}, LowCardinality, "Amount of data merged for a replication"},
	DOCS_MERGE_CAS_CHANGED_METRIC:   StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Number of merges failed because source cas changed"},
	EXPIRY_MERGE_CAS_CHANGED_METRIC: StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Number of expiry merges failed because source cas changed"},

	DOCS_MERGE_FAILED_METRIC:        StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Number of conflicting docs failed to merge"},
	EXPIRY_DOCS_MERGE_FAILED_METRIC: StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Number of conflicting expiry docs failed to merge"},
	DATA_MERGE_FAILED_METRIC:        StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrBytes}, LowCardinality, "Amount of data failed to merge"},

	DOCS_PROCESSED_METRIC:  StatsProperty{StatsUnit{MetricTypeGauge, StatsMgrNoUnit}, LowCardinality, "Number of docs processed for a replication"},
	DATA_REPLICATED_METRIC: StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrBytes}, LowCardinality, "Amount of data replicated for a replication"},
	SIZE_REP_QUEUE_METRIC:  StatsProperty{StatsUnit{MetricTypeGauge, StatsMgrBytes}, LowCardinality, "Amount of data being queued to be sent in an out nozzle"},
	DOCS_REP_QUEUE_METRIC:  StatsProperty{StatsUnit{MetricTypeGauge, StatsMgrNoUnit}, LowCardinality, "Number of documents being queued to be sent in an out nozzle"},

	DOCS_FILTERED_METRIC:         StatsProperty{StatsUnit{MetricTypeGauge, StatsMgrNoUnit}, LowCardinality, "Total number of documents filtered and not replicated due to any type of filtering"},
	DOCS_UNABLE_TO_FILTER_METRIC: StatsProperty{StatsUnit{MetricTypeGauge, StatsMgrNoUnit}, LowCardinality, "Number of documents that couldn't be filtered due to inability to parse the document through Advanced Filtering engine and were not replicated"},
	EXPIRY_FILTERED_METRIC:       StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Number of documents filtered that had expiry flag set"},
	DELETION_FILTERED_METRIC:     StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Number of documents filtered that was of a DCP deletion"},
	SET_FILTERED_METRIC:          StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Number of documents filtered that was of a DCP mutation"},
	EXPIRY_STRIPPED_METRIC:       StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Number of documents replicated that had its TTL changed to 0 before writing to target (source is unmodified)"},

	DOCS_FAILED_CR_SOURCE_METRIC:     StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Number of documents that was not replicated to the target due to conflict resolution evaluated on the source cluster"},
	EXPIRY_FAILED_CR_SOURCE_METRIC:   StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Subset of the number of documents that failed source-side conflict resolution that specifically had expiry flag set"},
	DELETION_FAILED_CR_SOURCE_METRIC: StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Subset of the number of documents that failed source-side conflict resolution that were delete operations"},
	SET_FAILED_CR_SOURCE_METRIC:      StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Subset of the number of documents that failed source-side conflict resolution that were set operations"},

	DOCS_FAILED_CR_TARGET_METRIC:     StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Number of documents failed conflict resolution at target cluster"},
	ADD_FAILED_CR_TARGET_METRIC:      StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Number of ADD failed conflict resolution at target cluster"},
	DELETION_FAILED_CR_TARGET_METRIC: StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Number of DELETE failed conflict resolution at target cluster"},
	EXPIRY_FAILED_CR_TARGET_METRIC:   StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Number of EXPIRE failed conflict resolution at target cluster"},
	SET_FAILED_CR_TARGET_METRIC:      StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Number of SET failed conflict resolution at target cluster"},

	TARGET_DOCS_SKIPPED_METRIC:          StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Number of documents that was not replicated to the target because they originated from the target"},
	EXPIRY_TARGET_DOCS_SKIPPED_METRIC:   StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Subset of the number of documents that originated from the target that specifically had expiry flag set"},
	DELETION_TARGET_DOCS_SKIPPED_METRIC: StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Subset of the number of documents that originated from the target that were delete operations"},
	SET_TARGET_DOCS_SKIPPED_METRIC:      StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Subset of the number of documents that originated from the target that were set operations"},

	CHANGES_LEFT_METRIC:    StatsProperty{StatsUnit{MetricTypeGauge, StatsMgrNoUnit}, LowCardinality, "Given the VBs of this node, the number of seqnos that need to be processed (either replicated or handled) before catching up to the high sequence numbers for the VBs"},
	DOCS_LATENCY_METRIC:    StatsProperty{StatsUnit{MetricTypeGauge, StatsMgrMilliSecond}, LowCardinality, "The rolling average amount of time it takes for the source cluster to receive the acknowledgement of a SET_WITH_META response after the Memcached request has been composed to be processed by the XDCR outnozzle"},
	META_LATENCY_METRIC:    StatsProperty{StatsUnit{MetricTypeGauge, StatsMgrMilliSecond}, LowCardinality, "The rolling average amount of time it takes once a getMeta command is composed to be sent to the time the request is handled once the target node has responded"},
	GET_DOC_LATENCY_METRIC: StatsProperty{StatsUnit{MetricTypeGauge, StatsMgrMilliSecond}, LowCardinality, "The rolling average amount of time it takes once a get document command is composed to be sent to the time the request is handled once the target node has responded"},
	RESP_WAIT_METRIC:       StatsProperty{StatsUnit{MetricTypeGauge, StatsMgrMilliSecond}, LowCardinality, "The rolling average amount of time it takes from when a MemcachedRequest is created to be ready to route to an outnozzle to the time that the response has been heard back from the target node after a successful write"},
	MERGE_LATENCY_METRIC:   StatsProperty{StatsUnit{MetricTypeGauge, StatsMgrMilliSecond}, LowCardinality, "The rolling average amount of time it takes from routing, conflict detection and resolution, to receive the acknowledgement of merge"},

	DOCS_CHECKED_METRIC:    StatsProperty{StatsUnit{MetricTypeGauge, StatsMgrNoUnit}, LowCardinality, "Across VBs for this node, the sum of all seqnos that have been considered to be checkpointed"},
	NUM_CHECKPOINTS_METRIC: StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "The number of times checkpoint operation has completed successfully since this XDCR process instance is made aware of this replication"},
	TIME_COMMITING_METRIC:  StatsProperty{StatsUnit{MetricTypeGauge, StatsMgrMilliSecond}, LowCardinality, "The rolling average amount of time it takes for a checkpoint operation to complete"},
	NUM_FAILEDCKPTS_METRIC: StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "The number of times checkpoint operation has encountered an error since this XDCR process instance is made aware of this replication"},
	//RATE_DOC_CHECKS_METRIC: StatsProperty{StatsUnit{MetricTypeGauge, StatsMgrDocsPerSecond}, LowCardinality, "The rate at which documents are being checkpointed"},

	DOCS_OPT_REPD_METRIC: StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "Number of documents optimistically replicated to the target"},
	//RATE_OPT_REPD_METRIC: StatsProperty{StatsUnit{MetricTypeGauge, StatsMgrDocsPerSecond}, LowCardinality, "The rate at which documents are being replicated optimistically"},

	DOCS_RECEIVED_DCP_METRIC: StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "The number of set operations received from DCP"},
	//RATE_RECEIVED_DCP_METRIC: StatsProperty{StatsUnit{MetricTypeGauge, StatsMgrDocsPerSecond}, LowCardinality, "The rate at which XDCR is receiving documents from DCP for this replication"},

	EXPIRY_RECEIVED_DCP_METRIC:   StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "The subset of documents received from DCP that either 1.) Is a DCP expiration or 2) Is a document that contains an expiration time"},
	DELETION_RECEIVED_DCP_METRIC: StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "The subset of documents received from DCP that is a Delete action"},
	SET_RECEIVED_DCP_METRIC:      StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "The subset of documents received from DCP that is a mutation (set action)"},

	DCP_DISPATCH_TIME_METRIC: StatsProperty{StatsUnit{MetricTypeGauge, StatsMgrGolangTimeDuration}, LowCardinality, "The rolling average amount of time it takes for a document to be received by XDCR from DCP, to the time it is queued up in the out nozzle ready to be sent"},
	DCP_DATACH_LEN:           StatsProperty{StatsUnit{MetricTypeGauge, StatsMgrNoUnit}, LowCardinality, "The number of items sent by KV DCP waiting for XDCR DCP nozzle to ingest and process"},

	THROTTLE_LATENCY_METRIC: StatsProperty{StatsUnit{MetricTypeGauge, StatsMgrMilliSecond}, LowCardinality, "The rolling average of the latency time introduced due to bandwith throttler"},

	THROUGHPUT_THROTTLE_LATENCY_METRIC: StatsProperty{StatsUnit{MetricTypeGauge, StatsMgrMilliSecond}, LowCardinality, "The rolling average of the latency time introduced due to throughput throttler"},

	//RATE_REPLICATED_METRIC: StatsProperty{StatsUnit{MetricTypeGauge, StatsMgrDocsPerSecond}, LowCardinality, "The rate at which documents from this node are being replicated to target nodes"},
	//BANDWIDTH_USAGE_METRIC: StatsProperty{StatsUnit{MetricTypeGauge, StatsMgrBytesPerSecond}, LowCardinality, "The data rate at which documents from this node are being replicated to target nodes"},

	DP_GET_FAIL_METRIC: StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "The total number of failed GET() operation on a reusable datapool within XDCR for the purpose of avoiding garbage generation"},

	DOCS_CLONED_METRIC: StatsProperty{StatsUnit{MetricTypeCounter, StatsMgrNoUnit}, LowCardinality, "The number of times a source mutation is cloned to be written to different target namespace"},
}
