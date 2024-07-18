// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package pipeline_svc

import (
	"errors"
	"expvar"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/base/filter"
	"github.com/couchbase/goxdcr/common"
	component "github.com/couchbase/goxdcr/component"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/parts"
	pipeline_pkg "github.com/couchbase/goxdcr/pipeline"
	"github.com/couchbase/goxdcr/pipeline_utils"
	"github.com/couchbase/goxdcr/service_def"
	utilities "github.com/couchbase/goxdcr/utils"
	"github.com/rcrowley/go-metrics"
)

const (
	default_sample_size     = 1000
	default_update_interval = 1000
)

// stats to initialize for paused replications that have never been run -- mostly the stats visible from UI
var StatsToInitializeForPausedReplications = []string{service_def.DOCS_WRITTEN_METRIC, service_def.DOCS_MERGED_METRIC, service_def.DOCS_FAILED_CR_SOURCE_METRIC, service_def.DOCS_FILTERED_METRIC,
	service_def.RATE_DOC_CHECKS_METRIC, service_def.RATE_OPT_REPD_METRIC, service_def.RATE_RECEIVED_DCP_METRIC, service_def.RATE_REPLICATED_METRIC,
	service_def.BANDWIDTH_USAGE_METRIC, service_def.DOCS_LATENCY_METRIC, service_def.META_LATENCY_METRIC, service_def.GET_DOC_LATENCY_METRIC, service_def.MERGE_LATENCY_METRIC,
	service_def.TARGET_DOCS_SKIPPED_METRIC, service_def.DOCS_FAILED_CR_TARGET_METRIC}

// stats to clear when replications are paused
// 1. all rate type stats
// 2. internal stats that are not visible on UI
var StatsToClearForPausedReplications = []string{service_def.SIZE_REP_QUEUE_METRIC, service_def.DOCS_REP_QUEUE_METRIC, service_def.DOCS_LATENCY_METRIC, service_def.META_LATENCY_METRIC,
	service_def.TIME_COMMITING_METRIC, service_def.NUM_FAILEDCKPTS_METRIC, service_def.RATE_DOC_CHECKS_METRIC, service_def.RATE_OPT_REPD_METRIC, service_def.RATE_RECEIVED_DCP_METRIC,
	service_def.RATE_REPLICATED_METRIC, service_def.BANDWIDTH_USAGE_METRIC, service_def.THROTTLE_LATENCY_METRIC, service_def.THROUGHPUT_THROTTLE_LATENCY_METRIC, service_def.GET_DOC_LATENCY_METRIC,
	service_def.MERGE_LATENCY_METRIC, service_def.DOCS_CLONED_METRIC, service_def.DATA_REPLICATED_UNCOMPRESSED_METRIC, service_def.DOCS_COMPRESSION_SKIPPED_METRIC, service_def.DELETION_CLONED_METRIC, service_def.TARGET_TMPFAIL_METRIC,
	service_def.HLV_UPDATED_METRIC, service_def.HLV_PRUNED_METRIC, service_def.IMPORT_DOCS_WRITTEN_METRIC, service_def.IMPORT_DOCS_FAILED_CR_SOURCE_METRIC, service_def.SOURCE_SYNC_XATTR_REMOVED_METRIC,
	service_def.TARGET_SYNC_XATTR_PRESERVED_METRIC, service_def.TARGET_EACCESS_METRIC, service_def.HLV_PRUNED_AT_MERGE_METRIC}

// keys for metrics in overview
// Note the values used here does not correspond to the service_def GlobalStatsTable, since these are used internally
// and not for exporting purposes
var OverviewMetricKeys = map[string]service_def.MetricType{
	service_def.CHANGES_LEFT_METRIC:                 service_def.MetricTypeCounter,
	service_def.DOCS_CHECKED_METRIC:                 service_def.MetricTypeCounter,
	service_def.DOCS_WRITTEN_METRIC:                 service_def.MetricTypeCounter,
	service_def.EXPIRY_DOCS_WRITTEN_METRIC:          service_def.MetricTypeCounter,
	service_def.DELETION_DOCS_WRITTEN_METRIC:        service_def.MetricTypeCounter,
	service_def.SET_DOCS_WRITTEN_METRIC:             service_def.MetricTypeCounter,
	service_def.DOCS_PROCESSED_METRIC:               service_def.MetricTypeCounter,
	service_def.DOCS_FAILED_CR_SOURCE_METRIC:        service_def.MetricTypeCounter,
	service_def.EXPIRY_FAILED_CR_SOURCE_METRIC:      service_def.MetricTypeCounter,
	service_def.DELETION_FAILED_CR_SOURCE_METRIC:    service_def.MetricTypeCounter,
	service_def.SET_FAILED_CR_SOURCE_METRIC:         service_def.MetricTypeCounter,
	service_def.DATA_REPLICATED_METRIC:              service_def.MetricTypeCounter,
	service_def.DOCS_FILTERED_METRIC:                service_def.MetricTypeCounter,
	service_def.DOCS_UNABLE_TO_FILTER_METRIC:        service_def.MetricTypeCounter,
	service_def.EXPIRY_FILTERED_METRIC:              service_def.MetricTypeCounter,
	service_def.DELETION_FILTERED_METRIC:            service_def.MetricTypeCounter,
	service_def.SET_FILTERED_METRIC:                 service_def.MetricTypeCounter,
	service_def.BINARY_FILTERED_METRIC:              service_def.MetricTypeCounter,
	service_def.NUM_CHECKPOINTS_METRIC:              service_def.MetricTypeCounter,
	service_def.NUM_FAILEDCKPTS_METRIC:              service_def.MetricTypeCounter,
	service_def.TIME_COMMITING_METRIC:               service_def.MetricTypeCounter,
	service_def.DOCS_OPT_REPD_METRIC:                service_def.MetricTypeCounter,
	service_def.DOCS_RECEIVED_DCP_METRIC:            service_def.MetricTypeCounter,
	service_def.EXPIRY_RECEIVED_DCP_METRIC:          service_def.MetricTypeCounter,
	service_def.DELETION_RECEIVED_DCP_METRIC:        service_def.MetricTypeCounter,
	service_def.SET_RECEIVED_DCP_METRIC:             service_def.MetricTypeCounter,
	service_def.SIZE_REP_QUEUE_METRIC:               service_def.MetricTypeCounter,
	service_def.DOCS_REP_QUEUE_METRIC:               service_def.MetricTypeCounter,
	service_def.DOCS_LATENCY_METRIC:                 service_def.MetricTypeCounter,
	service_def.RESP_WAIT_METRIC:                    service_def.MetricTypeCounter,
	service_def.META_LATENCY_METRIC:                 service_def.MetricTypeCounter,
	service_def.DCP_DISPATCH_TIME_METRIC:            service_def.MetricTypeCounter,
	service_def.DCP_DATACH_LEN:                      service_def.MetricTypeCounter,
	service_def.THROTTLE_LATENCY_METRIC:             service_def.MetricTypeCounter,
	service_def.THROUGHPUT_THROTTLE_LATENCY_METRIC:  service_def.MetricTypeCounter,
	service_def.DP_GET_FAIL_METRIC:                  service_def.MetricTypeCounter,
	service_def.EXPIRY_STRIPPED_METRIC:              service_def.MetricTypeCounter,
	service_def.ADD_DOCS_WRITTEN_METRIC:             service_def.MetricTypeCounter,
	service_def.GET_DOC_LATENCY_METRIC:              service_def.MetricTypeCounter,
	service_def.DOCS_MERGED_METRIC:                  service_def.MetricTypeCounter,
	service_def.DATA_MERGED_METRIC:                  service_def.MetricTypeCounter,
	service_def.EXPIRY_DOCS_MERGED_METRIC:           service_def.MetricTypeCounter,
	service_def.MERGE_LATENCY_METRIC:                service_def.MetricTypeCounter,
	service_def.DOCS_CLONED_METRIC:                  service_def.MetricTypeCounter,
	service_def.DELETION_CLONED_METRIC:              service_def.MetricTypeCounter,
	service_def.TARGET_DOCS_SKIPPED_METRIC:          service_def.MetricTypeCounter,
	service_def.DOCS_FAILED_CR_TARGET_METRIC:        service_def.MetricTypeCounter,
	service_def.DATA_REPLICATED_UNCOMPRESSED_METRIC: service_def.MetricTypeCounter,
	service_def.DOCS_COMPRESSION_SKIPPED_METRIC:     service_def.MetricTypeCounter,
	service_def.PIPELINE_STATUS:                     service_def.MetricTypeGauge,
	service_def.PIPELINE_ERRORS:                     service_def.MetricTypeGauge,
	service_def.TARGET_TMPFAIL_METRIC:               service_def.MetricTypeCounter,
	service_def.TARGET_EACCESS_METRIC:               service_def.MetricTypeCounter,
	service_def.DOCS_FILTERED_TXN_ATR_METRIC:        service_def.MetricTypeCounter,
	service_def.DOCS_FILTERED_TXN_XATTR_METRIC:      service_def.MetricTypeCounter,
	service_def.DOCS_FILTERED_CLIENT_TXN_METRIC:     service_def.MetricTypeCounter,
	service_def.DOCS_FILTERED_USER_DEFINED_METRIC:   service_def.MetricTypeCounter,
	service_def.DOCS_FILTERED_MOBILE_METRIC:         service_def.MetricTypeCounter,
	service_def.GUARDRAIL_RESIDENT_RATIO_METRIC:     service_def.MetricTypeCounter,
	service_def.GUARDRAIL_DATA_SIZE_METRIC:          service_def.MetricTypeCounter,
	service_def.GUARDRAIL_DISK_SPACE_METRIC:         service_def.MetricTypeCounter,
	service_def.TARGET_UNKNOWN_STATUS_METRIC:        service_def.MetricTypeCounter,
	service_def.SOURCE_SYNC_XATTR_REMOVED_METRIC:    service_def.MetricTypeCounter,
	service_def.TARGET_SYNC_XATTR_PRESERVED_METRIC:  service_def.MetricTypeCounter,
	service_def.IMPORT_DOCS_FAILED_CR_SOURCE_METRIC: service_def.MetricTypeCounter,
	service_def.IMPORT_DOCS_WRITTEN_METRIC:          service_def.MetricTypeCounter,
	service_def.HLV_UPDATED_METRIC:                  service_def.MetricTypeCounter,
	service_def.HLV_PRUNED_METRIC:                   service_def.MetricTypeCounter,
	service_def.HLV_PRUNED_AT_MERGE_METRIC:          service_def.MetricTypeCounter,
	service_def.EXPIRY_DOCS_MERGE_FAILED_METRIC:     service_def.MetricTypeCounter,
	service_def.DELETION_TARGET_DOCS_SKIPPED_METRIC: service_def.MetricTypeCounter,
	service_def.ADD_FAILED_CR_TARGET_METRIC:         service_def.MetricTypeCounter,
	service_def.DOCS_MERGE_CAS_CHANGED_METRIC:       service_def.MetricTypeCounter,
	service_def.DATA_MERGE_FAILED_METRIC:            service_def.MetricTypeCounter,
	service_def.EXPIRY_FAILED_CR_TARGET_METRIC:      service_def.MetricTypeCounter,
	service_def.DELETION_DOCS_CAS_CHANGED_METRIC:    service_def.MetricTypeCounter,
	service_def.ADD_DOCS_CAS_CHANGED_METRIC:         service_def.MetricTypeCounter,
	service_def.SET_DOCS_CAS_CHANGED_METRIC:         service_def.MetricTypeCounter,
	service_def.SUBDOC_CMD_DOCS_CAS_CHANGED_METRIC:  service_def.MetricTypeCounter,
	service_def.DELETION_FAILED_CR_TARGET_METRIC:    service_def.MetricTypeCounter,
	service_def.DOCS_MERGE_FAILED_METRIC:            service_def.MetricTypeCounter,
	service_def.EXPIRY_MERGE_CAS_CHANGED_METRIC:     service_def.MetricTypeCounter,
	service_def.SET_TARGET_DOCS_SKIPPED_METRIC:      service_def.MetricTypeCounter,
	service_def.SET_FAILED_CR_TARGET_METRIC:         service_def.MetricTypeCounter,
	service_def.EXPIRY_TARGET_DOCS_SKIPPED_METRIC:   service_def.MetricTypeCounter,
	service_def.SYSTEM_EVENTS_RECEIVED_DCP_METRIC:   service_def.MetricTypeCounter,
	service_def.SEQNO_ADV_RECEIVED_DCP_METRIC:       service_def.MetricTypeCounter,
	service_def.DOCS_SENT_WITH_SUBDOC_SET:           service_def.MetricTypeCounter,
	service_def.DOCS_SENT_WITH_SUBDOC_DELETE:        service_def.MetricTypeCounter,
	service_def.DOCS_FILTERED_CAS_POISONING_METRIC:  service_def.MetricTypeCounter,
	service_def.DOCS_SENT_WITH_POISONED_CAS_ERROR:   service_def.MetricTypeCounter,
	service_def.DOCS_SENT_WITH_POISONED_CAS_REPLACE: service_def.MetricTypeCounter,
}

var RouterVBMetricKeys = []string{service_def.DOCS_FILTERED_METRIC, service_def.DOCS_UNABLE_TO_FILTER_METRIC, service_def.EXPIRY_FILTERED_METRIC,
	service_def.DELETION_FILTERED_METRIC, service_def.SET_FILTERED_METRIC, service_def.BINARY_FILTERED_METRIC, service_def.EXPIRY_STRIPPED_METRIC,
	service_def.DOCS_FILTERED_TXN_ATR_METRIC, service_def.DOCS_FILTERED_CLIENT_TXN_METRIC, service_def.DOCS_FILTERED_TXN_XATTR_METRIC,
	service_def.DOCS_FILTERED_MOBILE_METRIC, service_def.DOCS_FILTERED_USER_DEFINED_METRIC, service_def.DOCS_FILTERED_CAS_POISONING_METRIC}

var OutNozzleVBMetricKeys = []string{service_def.GUARDRAIL_RESIDENT_RATIO_METRIC, service_def.GUARDRAIL_DATA_SIZE_METRIC, service_def.GUARDRAIL_DISK_SPACE_METRIC,
	service_def.DOCS_SENT_WITH_SUBDOC_SET, service_def.DOCS_SENT_WITH_SUBDOC_DELETE,
	service_def.DOCS_SENT_WITH_POISONED_CAS_ERROR, service_def.DOCS_SENT_WITH_POISONED_CAS_REPLACE}

var VBMetricKeys []string
var compileVBMetricKeyOnce sync.Once
var vbMetricKeyLock sync.RWMutex

func MakeVBCountMetricMap(metricsKeys []string) base.VBCountMetricMap {
	newMap := make(base.VBCountMetricMap)
	for _, key := range metricsKeys {
		newMap[key] = 0
	}
	return newMap
}

var RouterVBCountMetrics = MakeVBCountMetricMap(RouterVBMetricKeys)
var OutNozzleVBCountMetrics = MakeVBCountMetricMap(OutNozzleVBMetricKeys)

func NewVBStatsMapFromCkpt(ckptDoc *metadata.CheckpointsDoc, agreedIndex int) base.VBCountMetricMap {
	if agreedIndex < 0 || ckptDoc == nil || agreedIndex >= len(ckptDoc.Checkpoint_records) {
		return nil
	}

	record := ckptDoc.Checkpoint_records[agreedIndex]

	vbStatMap := make(base.VBCountMetricMap)
	vbStatMap[service_def.DOCS_FILTERED_METRIC] = base.Uint64ToInt64(record.Filtered_Items_Cnt)
	vbStatMap[service_def.DOCS_UNABLE_TO_FILTER_METRIC] = base.Uint64ToInt64(record.Filtered_Failed_Cnt)
	vbStatMap[service_def.EXPIRY_FILTERED_METRIC] = base.Uint64ToInt64(record.FilteredItemsOnExpirationsCnt)
	vbStatMap[service_def.DELETION_FILTERED_METRIC] = base.Uint64ToInt64(record.FilteredItemsOnDeletionsCnt)
	vbStatMap[service_def.SET_FILTERED_METRIC] = base.Uint64ToInt64(record.FilteredItemsOnSetCnt)
	vbStatMap[service_def.BINARY_FILTERED_METRIC] = base.Uint64ToInt64(record.FilteredItemsOnBinaryDocsCnt)
	vbStatMap[service_def.EXPIRY_STRIPPED_METRIC] = base.Uint64ToInt64(record.FilteredItemsOnExpiryStrippedCnt)
	vbStatMap[service_def.DOCS_FILTERED_TXN_ATR_METRIC] = base.Uint64ToInt64(record.FilteredItemsOnATRDocsCnt)
	vbStatMap[service_def.DOCS_FILTERED_CLIENT_TXN_METRIC] = base.Uint64ToInt64(record.FilteredItemsOnClientTxnRecordsCnt)
	vbStatMap[service_def.DOCS_FILTERED_TXN_XATTR_METRIC] = base.Uint64ToInt64(record.FilteredItemsOnTxnXattrsDocsCnt)
	vbStatMap[service_def.DOCS_FILTERED_MOBILE_METRIC] = base.Uint64ToInt64(record.FilteredItemsOnMobileRecords)
	vbStatMap[service_def.DOCS_FILTERED_USER_DEFINED_METRIC] = base.Uint64ToInt64(record.FilteredItemsOnUserDefinedFilters)
	vbStatMap[service_def.GUARDRAIL_RESIDENT_RATIO_METRIC] = base.Uint64ToInt64(record.GuardrailResidentRatioCnt)
	vbStatMap[service_def.GUARDRAIL_DISK_SPACE_METRIC] = base.Uint64ToInt64(record.GuardrailDiskSpaceCnt)
	vbStatMap[service_def.GUARDRAIL_DATA_SIZE_METRIC] = base.Uint64ToInt64(record.GuardrailDataSizeCnt)
	vbStatMap[service_def.DOCS_SENT_WITH_SUBDOC_SET] = base.Uint64ToInt64(record.DocsSentWithSubdocSetCnt)
	vbStatMap[service_def.DOCS_SENT_WITH_SUBDOC_DELETE] = base.Uint64ToInt64(record.DocsSentWithSubdocDeleteCnt)
	vbStatMap[service_def.DOCS_FILTERED_CAS_POISONING_METRIC] = base.Uint64ToInt64(record.CasPoisonCnt)
	vbStatMap[service_def.DOCS_SENT_WITH_POISONED_CAS_ERROR] = base.Uint64ToInt64(record.DocsSentWithPoisonedCasErrorMode)
	vbStatMap[service_def.DOCS_SENT_WITH_POISONED_CAS_REPLACE] = base.Uint64ToInt64(record.DocsSentWithPoisonedCasReplaceMode)
	return vbStatMap
}

// keys for metrics that do not monotonically increase during replication, to which the "going backward" check should not be applied
var NonIncreasingMetricKeyMap = map[string]bool{
	service_def.SIZE_REP_QUEUE_METRIC: true,
	service_def.DOCS_REP_QUEUE_METRIC: true,
	service_def.DCP_DATACH_LEN:        true}

type SampleStats struct {
	Count int64
	Mean  float64
}

type notificationReqOpt struct {
	sendBack chan service_def.SourceNotification
}

// StatisticsManager mount the statics collector on the pipeline to collect raw stats
// It does stats correlation and processing on raw stats periodically (controlled by publish_interval)
// , then stores the result in expvar
// The result in expvar can be exposed to outside via different channels - log or to ns_server.
type StatisticsManager struct {
	*component.AbstractComponent
	//a map of registry with the part id as key
	//the aggregated metrics for the pipeline is the entry with key="Overall"
	//this map will be exported to expval, but only
	//the entry with key="Overview" will be reported to ns_server
	registries map[string]metrics.Registry

	//temporary map to keep checkpointed seqnos
	checkpointed_seqnos map[uint16]*base.SeqnoWithLock

	//chan for stats update tickers -- new tickers are added each time stats interval is changed
	update_ticker_ch chan *time.Ticker

	//settings - sample size
	sample_size int
	//settings - statistics update interval in milliseconds
	update_interval uint32

	//the channel to communicate finish signal with statistic updater
	finish_ch chan bool
	done_ch   chan bool
	wait_grp  *sync.WaitGroup
	initDone  chan bool

	pipeline common.Pipeline

	logger *log.CommonLogger

	collectors []MetricsCollector

	active_vbs  map[string][]uint16
	bucket_name string

	through_seqno_tracker_svc service_def.ThroughSeqnoTrackerSvc
	xdcr_topology_svc         service_def.XDCRCompTopologySvc
	remoteClusterSvc          service_def.RemoteClusterSvc
	cachedCapability          metadata.Capability
	bucketTopologySvc         service_def.BucketTopologySvc

	stats_map map[string]string

	user_agent string

	utils utilities.UtilsIface

	endSeqnos map[uint16]uint64
	// For now, backfill pipeline do not dynamically change VB tasks
	totalBackfillChanges uint64
	backfillUnsortedVBs  []uint16

	printThroughSeqnoSummaryWhenStopping bool

	highSeqnosDcpCh           chan service_def.SourceNotification
	getHighSeqnosAndSourceVBs func() (base.HighSeqnosMapType, base.KvVBMapType, func())
	latestNotificationReqCh   chan notificationReqOpt

	highSeqnosIntervalUpdater    func(time.Duration)
	highSeqnosIntervalUpdaterMtx sync.RWMutex

	// To be used by a single go-routine that calls processCalculatedStats
	updateStatsOnceCachedVBList []uint16

	replStatusGetter func(string) (pipeline_pkg.ReplicationStatusIface, error)

	// PipelineStatus is an int type underneath
	pipelineStatus int32
	pipelineErrors int32
}

func NewStatisticsManager(through_seqno_tracker_svc service_def.ThroughSeqnoTrackerSvc, xdcr_topology_svc service_def.XDCRCompTopologySvc, logger_ctx *log.LoggerContext, active_vbs map[string][]uint16, bucket_name string, utilsIn utilities.UtilsIface, remoteClusterSvc service_def.RemoteClusterSvc, bucketTopologySvc service_def.BucketTopologySvc, replStatusGetter func(string) (pipeline_pkg.ReplicationStatusIface, error)) *StatisticsManager {
	localLogger := log.NewLogger(StatsMgrId, logger_ctx)
	stats_mgr := &StatisticsManager{
		AbstractComponent:         component.NewAbstractComponentWithLogger(StatsMgrId, localLogger),
		registries:                make(map[string]metrics.Registry),
		logger:                    localLogger,
		bucket_name:               bucket_name,
		finish_ch:                 make(chan bool, 1),
		done_ch:                   make(chan bool, 1),
		update_ticker_ch:          make(chan *time.Ticker, 1000),
		sample_size:               default_sample_size,
		update_interval:           default_update_interval,
		active_vbs:                active_vbs,
		wait_grp:                  &sync.WaitGroup{},
		checkpointed_seqnos:       make(map[uint16]*base.SeqnoWithLock),
		stats_map:                 make(map[string]string),
		through_seqno_tracker_svc: through_seqno_tracker_svc,
		xdcr_topology_svc:         xdcr_topology_svc,
		utils:                     utilsIn,
		remoteClusterSvc:          remoteClusterSvc,
		bucketTopologySvc:         bucketTopologySvc,
		replStatusGetter:          replStatusGetter,
		latestNotificationReqCh:   make(chan notificationReqOpt, 100),
		initDone:                  make(chan bool),
	}
	stats_mgr.collectors = []MetricsCollector{&outNozzleCollector{}, &dcpCollector{}, &routerCollector{}, &checkpointMgrCollector{}, &conflictMgrCollector{}}

	stats_mgr.initialize()
	return stats_mgr
}

// Statistics of a pipeline which may or may not be running
// Returns a list of expVar.Map where the idx corresponds to the pipeline Type, so it's possible for one elem to be nil
func GetStatisticsForPipeline(topic string, repStatusGetter func(topic string) (pipeline_pkg.ReplicationStatusIface, error)) []*expvar.Map {
	var allPipelinesStats []*expvar.Map
	repl_status, _ := repStatusGetter(topic)
	if repl_status == nil {
		return allPipelinesStats
	}

	for pipelineType := common.PipelineTypeBegin; pipelineType < common.PipelineTypeInvalidEnd; pipelineType++ {
		oneStats := repl_status.GetOverviewStats(pipelineType)
		if oneStats == nil {
			oneStats = GetReadOnlyOverviewStats(repl_status, pipelineType)
		}
		allPipelinesStats = append(allPipelinesStats, oneStats)
	}
	return allPipelinesStats
}

func (stats_mgr *StatisticsManager) initialize() {
	for _, vb_list := range stats_mgr.active_vbs {
		for _, vb := range vb_list {
			stats_mgr.checkpointed_seqnos[vb] = base.NewSeqnoWithLock()
			stats_mgr.stats_map[fmt.Sprintf(base.VBUCKET_HIGH_SEQNO_STAT_KEY_FORMAT, vb)] = ""
		}
	}
}

func (statsMgr *StatisticsManager) getRouterCollector() *routerCollector {
	return (statsMgr.collectors[2]).(*routerCollector)
}

func (statsMgr *StatisticsManager) getdcpCollector() *dcpCollector {
	return (statsMgr.collectors[1]).(*dcpCollector)
}

func (statsMgr *StatisticsManager) getOutnozzleCollector() *outNozzleCollector {
	return (statsMgr.collectors[0]).(*outNozzleCollector)
}

func (stats_mgr *StatisticsManager) cleanupBeforeExit() error {
	rs, err := stats_mgr.getReplicationStatus()
	if err != nil {
		return err
	}
	rs.CleanupBeforeExit(StatsToClearForPausedReplications)
	statsLog, _ := stats_mgr.formatStatsForLog()
	stats_mgr.logger.Infof("%v expvar=%v\n", stats_mgr.pipeline.InstanceId(), statsLog)
	return nil
}

func (stats_mgr *StatisticsManager) getUpdateInterval() time.Duration {
	return time.Duration(atomic.LoadUint32(&stats_mgr.update_interval)) * time.Millisecond
}

func (stats_mgr *StatisticsManager) setUpdateInterval(update_interval int) {
	atomic.StoreUint32(&stats_mgr.update_interval, uint32(update_interval))
	stats_mgr.highSeqnosIntervalUpdaterMtx.RLock()
	if stats_mgr.highSeqnosIntervalUpdater != nil {
		stats_mgr.highSeqnosIntervalUpdater(time.Duration(update_interval) * time.Millisecond)
		stats_mgr.logger.Infof("%v set update interval to %v ms\n", stats_mgr.pipeline.InstanceId(), update_interval)
	} else {
		stats_mgr.logger.Warnf("%v unable to set update interval to %v ms because init isn't finished yet\n", stats_mgr.pipeline.InstanceId(), update_interval)
	}
	stats_mgr.highSeqnosIntervalUpdaterMtx.RUnlock()
}

// updateStats runs until it get finish signal
// It processes the raw stats and publish the overview stats along with the raw stats to expvar
func (stats_mgr *StatisticsManager) updateStats() error {
	stats_mgr.logger.Infof("%v updateStats started", stats_mgr.pipeline.InstanceId())
	defer stats_mgr.logger.Infof("%v updateStats exited", stats_mgr.pipeline.InstanceId())

	defer stats_mgr.wait_grp.Done()
	defer close(stats_mgr.done_ch)

	ticker := <-stats_mgr.update_ticker_ch
	defer ticker.Stop()

	init_ch := make(chan bool, 1)
	init_ch <- true
	for {
		select {
		case new_ticker := <-stats_mgr.update_ticker_ch:
			stats_mgr.logger.Infof("%v Received new ticker due to changes to stats interval setting", stats_mgr.pipeline.InstanceId())
			ticker.Stop()
			ticker = new_ticker
		case <-stats_mgr.finish_ch:
			stats_mgr.logger.Infof("%v updateStats received finish signal", stats_mgr.pipeline.InstanceId())
			stats_mgr.cleanupBeforeExit()
			goto done
		// this ensures that stats are printed out immediately after updateStats is started
		case <-init_ch:
			err := stats_mgr.updateStatsOnce()
			if err != nil {
				stats_mgr.logger.Warnf("%v updateStatsOnce encountered error = %v", stats_mgr.pipeline.InstanceId(), err)
				goto done
			}
		case <-ticker.C:
			err := stats_mgr.updateStatsOnce()
			if err != nil {
				stats_mgr.logger.Warnf("%v updateStatsOnce encountered error = %v", stats_mgr.pipeline.InstanceId(), err)
				goto done
			}
		}
	}
done:
	ticker.Stop()
	return nil
}

// periodically prints stats to log
func (stats_mgr *StatisticsManager) logStats() error {
	stats_mgr.logger.Infof("%v logStats started", stats_mgr.pipeline.InstanceId())
	defer stats_mgr.logger.Infof("%v logStats exited", stats_mgr.pipeline.InstanceId())

	defer stats_mgr.wait_grp.Done()

	logStats_ticker := time.NewTicker(base.StatsLogInterval)
	defer logStats_ticker.Stop()

	for {
		select {
		case <-stats_mgr.finish_ch:
			return nil
		case <-logStats_ticker.C:
			err := stats_mgr.logStatsOnce()
			if err != nil {
				stats_mgr.logger.Infof("%v Failed to log statistics. err=%v\n", stats_mgr.pipeline.InstanceId(), err)
			}
		}
	}
	return nil
}

func (stats_mgr *StatisticsManager) updateStatsOnce() error {
	if !pipeline_utils.IsPipelineRunning(stats_mgr.pipeline.State()) {
		//the pipeline is no longer running, kill myself
		message := "Pipeline is no longer running, exit."
		stats_mgr.logger.Infof("%v message", stats_mgr.pipeline.InstanceId())
		stats_mgr.cleanupBeforeExit()
		return errors.New(message)
	}

	stats_mgr.logger.Debugf("%v: Publishing the statistics for %v to expvar", time.Now(), stats_mgr.pipeline.InstanceId())
	err := stats_mgr.processRawStats()

	if err != nil {
		stats_mgr.logger.Infof("%v Failed to calculate the statistics for this round. Move on", stats_mgr.pipeline.InstanceId())
	}
	return nil
}

func (stats_mgr *StatisticsManager) logStatsOnce() error {
	if stats_mgr.logger.GetLogLevel() >= log.LogLevelInfo {
		statsLog, err := stats_mgr.formatStatsForLog()
		if err != nil {
			return err
		}
		stats_mgr.logger.Info(statsLog)

		//log parts summary
		outNozzle_parts := stats_mgr.pipeline.Targets()
		for _, part := range outNozzle_parts {
			if stats_mgr.pipeline.Specification().GetReplicationSpec().Settings.RepType == metadata.ReplicationTypeXmem {
				part.(*parts.XmemNozzle).PrintStatusSummary()
			} else {
				part.(*parts.CapiNozzle).PrintStatusSummary()
			}
		}
		dcp_parts := stats_mgr.pipeline.Sources()
		for _, part := range dcp_parts {
			part.(*parts.DcpNozzle).PrintStatusSummary()
		}

		// log listener summary
		async_listener_map := pipeline_pkg.GetAllAsyncComponentEventListeners(stats_mgr.pipeline)
		for _, async_listener := range async_listener_map {
			async_listener.(*component.AsyncComponentEventListenerImpl).PrintStatusSummary()
		}

		// log throttler service summary
		throttler := stats_mgr.pipeline.RuntimeContext().Service(base.BANDWIDTH_THROTTLER_SVC)
		if throttler != nil {
			throttler.(*BandwidthThrottler).PrintStatusSummary()
		}

		// log through seqno service summary
		stats_mgr.through_seqno_tracker_svc.PrintStatusSummary()

		// Conflict Manager
		if conflict_mgr := stats_mgr.pipeline.RuntimeContext().Service(base.CONFLICT_MANAGER_SVC); conflict_mgr != nil {
			conflict_mgr.(*ConflictManager).PrintStatusSummary()
		}

	}
	return nil
}

func (stats_mgr *StatisticsManager) formatStatsForLog() (string, error) {
	rs, err := stats_mgr.getReplicationStatus()
	if err != nil {
		return "", err
	}
	expvar_stats_map := rs.Storage(stats_mgr.pipeline.Type())
	return fmt.Sprintf("Stats for pipeline %v %v\n", stats_mgr.pipeline.InstanceId(), expvar_stats_map.String()), nil
}

// process the raw stats, aggregate them into overview registry
// expose the raw stats and overview stats to expvar
// Locking is done by caller
func (stats_mgr *StatisticsManager) processRawStats() error {
	rs, err := stats_mgr.getReplicationStatus()
	if err != nil {
		return err
	}

	// save existing values in overview registry for rate stats calculation
	oldSample := stats_mgr.getOverviewRegistry()
	docs_written_old := oldSample.Get(service_def.DOCS_WRITTEN_METRIC).(metrics.Counter).Count()
	docs_received_dcp_old := oldSample.Get(service_def.DOCS_RECEIVED_DCP_METRIC).(metrics.Counter).Count()
	docs_opt_repd_old := oldSample.Get(service_def.DOCS_OPT_REPD_METRIC).(metrics.Counter).Count()
	data_replicated_old := oldSample.Get(service_def.DATA_REPLICATED_METRIC).(metrics.Counter).Count()
	docs_checked_old_var := oldSample.Get(service_def.DOCS_CHECKED_METRIC)
	var docs_checked_old int64 = 0
	if docs_checked_old_var != nil {
		docs_checked_old = docs_checked_old_var.(metrics.Counter).Count()
	}
	changes_left_old_var := oldSample.Get(service_def.CHANGES_LEFT_METRIC)
	var changes_left_old int64 = 0
	if changes_left_old_var != nil {
		changes_left_old = changes_left_old_var.(metrics.Counter).Count()
	}
	docs_processed_old_var := oldSample.Get(service_def.DOCS_PROCESSED_METRIC)
	var docs_processed_old int64 = 0
	if docs_processed_old_var != nil {
		docs_processed_old = docs_processed_old_var.(metrics.Counter).Count()
	}
	stats_mgr.initOverviewRegistry()

	sample_stats_list_map := make(map[string][]*SampleStats)

	for registry_name, registry := range stats_mgr.registries {
		if registry_name != service_def.OVERVIEW_METRICS_KEY {
			map_for_registry := new(expvar.Map).Init()

			orig_registry := rs.GetStats(registry_name, stats_mgr.pipeline.Type())
			registry.Each(func(name string, i interface{}) {
				stats_mgr.publishMetricToMap(map_for_registry, name, i, true)
				switch m := i.(type) {
				case metrics.Counter:
					// skip "going backward" check on stats that do not monotonically increase
					if _, ok := NonIncreasingMetricKeyMap[name]; !ok {
						if orig_registry != nil {
							orig_val, _ := strconv.ParseInt(orig_registry.Get(name).String(), 10, 64)
							if m.Count() < orig_val {
								stats_mgr.logger.Infof("%v counter %v goes backward, maybe due to the pipeline is restarted\n", stats_mgr.pipeline.InstanceId(), name)
							}
						}
					}
					metric_overview := stats_mgr.getOverviewRegistry().Get(name)
					if metric_overview != nil {
						metric_overview.(metrics.Counter).Inc(m.Count())
					}
				case metrics.Histogram:
					var sample_stats_list []*SampleStats
					var ok bool
					sample_stats_list, ok = sample_stats_list_map[name]
					if !ok {
						sample_stats_list = make([]*SampleStats, 0)
					}

					// track sample stats from individual components
					sample := m.Sample()
					sample_stats := &SampleStats{sample.Count(), sample.Mean()}
					sample_stats_list = append(sample_stats_list, sample_stats)
					sample_stats_list_map[name] = sample_stats_list
				}
			})
			rs.SetStats(registry_name, map_for_registry, stats_mgr.pipeline.Type())
		}
	}

	// publish aggregated histogram stats to overview
	for name, sample_stats_list := range sample_stats_list_map {
		var aggregated_count int64
		var aggregated_sum float64
		var aggregated_mean int64
		for _, sample_stats := range sample_stats_list {
			aggregated_count += sample_stats.Count
			aggregated_sum += float64(sample_stats.Count) * sample_stats.Mean
		}

		if aggregated_count != 0 {
			aggregated_mean = int64(aggregated_sum / float64(aggregated_count))
		}

		metric_overview := stats_mgr.getOverviewRegistry().Get(name)
		if metric_overview != nil {
			switch metric_overview.(type) {
			case metrics.Counter:
				metric_overview.(metrics.Counter).Clear()
				metric_overview.(metrics.Counter).Inc(aggregated_mean)
			case metrics.Histogram:
				sample := metric_overview.(metrics.Histogram).Sample()
				sample.Update(aggregated_mean)
			}
		}
	}

	map_for_overview := new(expvar.Map).Init()

	//publish all the metrics in overview registry
	stats_mgr.getOverviewRegistry().Each(func(name string, i interface{}) {
		stats_mgr.publishMetricToMap(map_for_overview, name, i, false)
	})

	//calculate additional metrics
	err = stats_mgr.processCalculatedStats(map_for_overview, changes_left_old, docs_written_old, docs_received_dcp_old,
		docs_opt_repd_old, data_replicated_old, docs_checked_old, docs_processed_old)
	if err != nil {
		return err
	}

	stats_mgr.logger.Debugf("Overview=%v for pipeline\n", map_for_overview)

	// set current time to map_for_interview
	current_time_var := new(expvar.Int)
	current_time_var.Set(time.Now().UnixNano())
	map_for_overview.Set(base.CurrentTime, current_time_var)

	rs.SetOverviewStats(map_for_overview, stats_mgr.pipeline.Type())
	return nil
}

func (stats_mgr *StatisticsManager) processCalculatedStats(overview_expvar_map *expvar.Map, changes_left_old,
	docs_written_old, docs_received_dcp_old, docs_opt_repd_old, data_replicated_old, docs_checked_old,
	docs_processed_old int64) error {
	changes_left_val, docs_processed, sortedVBsList, err := stats_mgr.calculateChangesLeftAndDocProcessed()
	if err != nil {
		stats_mgr.logger.Warnf("%v Failed to calculate docs_processed and changes_left. Use old values. err=%v\n", stats_mgr.pipeline.InstanceId(), err)
		changes_left_val = changes_left_old
		docs_processed = docs_processed_old
		// And also use cached vblist since the sortedVBsList returned will be nil
		sortedVBsList = stats_mgr.updateStatsOnceCachedVBList
	} else {
		stats_mgr.updateStatsOnceCachedVBList = sortedVBsList
	}

	docs_processed_var := new(expvar.Int)
	docs_processed_var.Set(docs_processed)
	overview_expvar_map.Set(service_def.DOCS_PROCESSED_METRIC, docs_processed_var)
	setCounter(stats_mgr.getOverviewRegistry().Get(service_def.DOCS_PROCESSED_METRIC).(metrics.Counter), int(docs_processed))

	changes_left_var := new(expvar.Int)
	changes_left_var.Set(changes_left_val)
	overview_expvar_map.Set(service_def.CHANGES_LEFT_METRIC, changes_left_var)
	// also update the value in overview registry since we need it at the next stats computation time
	setCounter(stats_mgr.getOverviewRegistry().Get(service_def.CHANGES_LEFT_METRIC).(metrics.Counter), int(changes_left_val))

	//calculate rate_replication
	docs_written := stats_mgr.getOverviewRegistry().Get(service_def.DOCS_WRITTEN_METRIC).(metrics.Counter).Count()
	interval_in_sec := stats_mgr.getUpdateInterval().Seconds()
	rate_replicated := float64(docs_written-docs_written_old) / interval_in_sec
	rate_replicated_var := new(expvar.Float)
	rate_replicated_var.Set(rate_replicated)
	overview_expvar_map.Set(service_def.RATE_REPLICATED_METRIC, rate_replicated_var)

	//calculate rate_received_from_dcp
	docs_received_dcp := stats_mgr.getOverviewRegistry().Get(service_def.DOCS_RECEIVED_DCP_METRIC).(metrics.Counter).Count()
	rate_received_dcp := float64(docs_received_dcp-docs_received_dcp_old) / interval_in_sec
	rate_received_dcp_var := new(expvar.Float)
	rate_received_dcp_var.Set(rate_received_dcp)
	overview_expvar_map.Set(service_def.RATE_RECEIVED_DCP_METRIC, rate_received_dcp_var)

	//calculate rate_doc_opt_repd
	docs_opt_repd := stats_mgr.getOverviewRegistry().Get(service_def.DOCS_OPT_REPD_METRIC).(metrics.Counter).Count()
	rate_opt_repd := float64(docs_opt_repd-docs_opt_repd_old) / interval_in_sec
	rate_opt_repd_var := new(expvar.Float)
	rate_opt_repd_var.Set(rate_opt_repd)
	overview_expvar_map.Set(service_def.RATE_OPT_REPD_METRIC, rate_opt_repd_var)

	//calculate bandwidth_usage
	data_replicated := stats_mgr.getOverviewRegistry().Get(service_def.DATA_REPLICATED_METRIC).(metrics.Counter).Count()
	// bandwidth_usage is in the unit of bytes/second
	bandwidth_usage := float64(data_replicated-data_replicated_old) / interval_in_sec
	bandwidth_usage_var := new(expvar.Float)
	bandwidth_usage_var.Set(bandwidth_usage)
	overview_expvar_map.Set(service_def.BANDWIDTH_USAGE_METRIC, bandwidth_usage_var)

	//calculate docs_checked
	docs_checked := stats_mgr.calculateDocsChecked(sortedVBsList)
	docs_checked_var := new(expvar.Int)
	docs_checked_var.Set(int64(docs_checked))
	overview_expvar_map.Set(service_def.DOCS_CHECKED_METRIC, docs_checked_var)
	setCounter(stats_mgr.getOverviewRegistry().Get(service_def.DOCS_CHECKED_METRIC).(metrics.Counter), int(docs_checked))

	theoreticalUncompressedDataReplicated := stats_mgr.getOverviewRegistry().Get(service_def.DATA_REPLICATED_UNCOMPRESSED_METRIC).(metrics.Counter).Count()
	theoreticalUncompressedDataReplicatedVar := new(expvar.Int)
	theoreticalUncompressedDataReplicatedVar.Set(theoreticalUncompressedDataReplicated)
	overview_expvar_map.Set(service_def.DATA_REPLICATED_UNCOMPRESSED_METRIC, theoreticalUncompressedDataReplicatedVar)

	// calculate docs_compression_skipped
	docsCompressionSkippedVar := new(expvar.Int)
	docsCompressionSkippedVar.Set(
		stats_mgr.getOverviewRegistry().Get(service_def.DOCS_COMPRESSION_SKIPPED_METRIC).(metrics.Counter).Count(),
	)
	overview_expvar_map.Set(service_def.DOCS_COMPRESSION_SKIPPED_METRIC, docsCompressionSkippedVar)

	//calculate rate_doc_checks
	var rate_doc_checks float64
	if docs_checked_old < 0 {
		// a negative value indicates that this is the first stats run and there is no old value yet
		rate_doc_checks = 0
	} else if float64(docs_checked)-float64(docs_checked_old) < 0 {
		// VBs are now moved out of this node that leads to a very lower number than before
		rate_doc_checks = 0
	} else {
		rate_doc_checks = (float64(docs_checked) - float64(docs_checked_old)) / interval_in_sec
	}
	rate_doc_checks_var := new(expvar.Float)
	rate_doc_checks_var.Set(rate_doc_checks)
	overview_expvar_map.Set(service_def.RATE_DOC_CHECKS_METRIC, rate_doc_checks_var)

	// Set pipeline status
	curStatus := int64(atomic.LoadInt32(&stats_mgr.pipelineStatus))
	stats_mgr.getOverviewRegistry().Get(service_def.PIPELINE_STATUS).(metrics.Gauge).Update(curStatus)

	// set number of errors
	repl, err := stats_mgr.getReplicationStatus()
	if err != nil {
		return err
	}
	stats_mgr.getOverviewRegistry().Get(service_def.PIPELINE_ERRORS).(metrics.Gauge).Update(int64(len(repl.Errors())))
	return nil
}

func (stats_mgr *StatisticsManager) calculateDocsChecked(vbsList []uint16) uint64 {
	var docs_checked uint64 = 0
	vbts_map, vbts_map_lock := GetStartSeqnos(stats_mgr.pipeline, stats_mgr.logger)
	if vbts_map != nil {
		vbts_map_lock.RLock()
		defer vbts_map_lock.RUnlock()

		for vbno, vbts := range vbts_map {
			_, found := base.SearchUint16List(vbsList, vbno)
			if !found {
				continue
			}

			start_seqno := vbts.Seqno
			var docs_checked_vb uint64 = 0
			checkpointed_seqno := stats_mgr.checkpointed_seqnos[vbno].GetSeqno()
			if checkpointed_seqno > start_seqno {
				docs_checked_vb = checkpointed_seqno
			} else {
				docs_checked_vb = start_seqno
			}
			docs_checked = docs_checked + docs_checked_vb
		}
	}
	return docs_checked
}

func (stats_mgr *StatisticsManager) calculateChangesLeftAndDocProcessed() (int64, int64, []uint16, error) {
	switch stats_mgr.pipeline.Type() {
	case common.MainPipeline:
		return stats_mgr.calculateChangesLeftAndDocsProcessedMainPipeline()
	case common.BackfillPipeline:
		return stats_mgr.calculateChangesLeftAndDocsProcessedBackfillPipeline()
	default:
		return 0, 0, nil, fmt.Errorf("Invalid pipeline: %v", stats_mgr.pipeline.Type().String())
	}
}

func (stats_mgr *StatisticsManager) calculateChangesLeftAndDocsProcessedMainPipeline() (int64, int64, []uint16, error) {
	// Get throughSeqnoMap first to avoid negative stats
	throughSeqnoMap := stats_mgr.GetThroughSeqnosFromTsService()

	stats_mgr.waitForBucketTopologySvcSubscription() // getHighSeqnosAndSourceVBs() need to be set
	if stats_mgr.getHighSeqnosAndSourceVBs == nil {
		return 0, 0, nil, fmt.Errorf("initialization did not run")
	}
	highSeqnosMap, curKvVbMap, doneFunc := stats_mgr.getHighSeqnosAndSourceVBs()
	defer doneFunc()
	if highSeqnosMap == nil || curKvVbMap == nil {
		return 0, 0, nil, fmt.Errorf("getHighSeqnosAndSourceVBs returned nil maps")
	}

	total_changes, vbsList, err := calculateTotalChanges(stats_mgr.logger, highSeqnosMap, curKvVbMap)
	if err != nil {
		return 0, 0, nil, err
	}

	docs_processed := stats_mgr.getDocsProcessed(vbsList, throughSeqnoMap)

	changes_left := total_changes - docs_processed
	stats_mgr.logChangesLeft(total_changes, docs_processed, changes_left)
	return changes_left, docs_processed, vbsList, nil
}

func (stats_mgr *StatisticsManager) getDocsProcessed(vbsList []uint16, throughSeqnoMap map[uint16]uint64) int64 {
	var docs_processed int64
	for vb, seqno := range throughSeqnoMap {
		if _, found := base.SearchUint16List(vbsList, vb); !found {
			continue
		}
		docs_processed += int64(seqno)
	}
	return docs_processed
}
func (stats_mgr *StatisticsManager) logChangesLeft(total_changes, docs_processed, changes_left int64) {
	stats_mgr.logger.Infof("total_docs=%v, docs_processed=%v, changes_left=%v\n", total_changes, docs_processed, changes_left)
}

func (stats_mgr *StatisticsManager) calculateChangesLeftAndDocsProcessedBackfillPipeline() (int64, int64, []uint16, error) {
	total_changes := int64(stats_mgr.totalBackfillChanges)
	vbsList := base.SortUint16List(stats_mgr.backfillUnsortedVBs)
	throughSeqnoMap := stats_mgr.GetThroughSeqnosFromTsService()
	docs_processed := stats_mgr.getDocsProcessed(vbsList, throughSeqnoMap)
	changes_left := total_changes - docs_processed
	if changes_left < 0 {
		// Since DCP always sends until the end of a snapshot, it's possible that DCP
		// will send us seqno after our requested endpoint, which will result in a negative changes_left
		changes_left = 0
	}
	stats_mgr.logChangesLeft(total_changes, docs_processed, changes_left)
	return changes_left, docs_processed, vbsList, nil
}

func (stats_mgr *StatisticsManager) getOverviewRegistry() metrics.Registry {
	return stats_mgr.registries[service_def.OVERVIEW_METRICS_KEY]
}

func (stats_mgr *StatisticsManager) publishMetricToMap(expvar_map *expvar.Map, name string, i interface{}, includeDetails bool) {
	switch m := i.(type) {
	case metrics.Counter:
		expvar_val := new(expvar.Int)
		expvar_val.Set(m.Count())
		expvar_map.Set(name, expvar_val)
	case metrics.Histogram:
		if includeDetails {
			metrics_map := new(expvar.Map).Init()
			mean := new(expvar.Float)
			mean.Set(m.Mean())
			metrics_map.Set("mean", mean)
			max := new(expvar.Int)
			max.Set(m.Max())
			metrics_map.Set("max", max)
			min := new(expvar.Int)
			min.Set(m.Min())
			metrics_map.Set("min", min)
			count := new(expvar.Int)
			count.Set(m.Count())
			metrics_map.Set("count", count)
			expvar_map.Set(name, metrics_map)
		} else {
			mean := new(expvar.Float)
			mean.Set(m.Mean())
			expvar_map.Set(name, mean)
		}
	case metrics.Gauge:
		expvar_val := new(expvar.Int)
		expvar_val.Set(m.Value())
		expvar_map.Set(name, expvar_val)
	}
}

func (stats_mgr *StatisticsManager) getOrCreateRegistry(name string) metrics.Registry {
	registry := stats_mgr.registries[name]
	if registry == nil {
		registry = metrics.NewRegistry()
		stats_mgr.registries[name] = registry
	}
	return registry
}

func (stats_mgr *StatisticsManager) Attach(pipeline common.Pipeline) error {
	stats_mgr.pipeline = pipeline
	stats_mgr.composeUserAgent()

	//mount collectors with pipeline
	for _, collector := range stats_mgr.collectors {
		err := collector.Mount(pipeline, stats_mgr)
		if err != nil {
			stats_mgr.logger.Errorf(err.Error())
		}
	}

	if stats_mgr.pipeline.Type() == common.BackfillPipeline {
		spec := stats_mgr.pipeline.Specification()
		if spec != nil {
			replSpec := spec.GetReplicationSpec()
			if replSpec != nil && replSpec.Settings != nil {
				collectionModes := replSpec.Settings.GetCollectionModes()
				if collectionModes.IsOsoOn() {
					// When OSO pipeline runs, it may finish before it statsMgr ever had a chance to print out its
					// summary stats. So force a print at the end to ensure that oso_received count is at least
					// in the log
					stats_mgr.printThroughSeqnoSummaryWhenStopping = true
				}
			}
		}
	}

	//register the aggregation metrics for the pipeline
	stats_mgr.initOverviewRegistry()
	stats_mgr.logger.Infof("StatisticsManager is started")

	return nil
}

// compose user agent string for HELO command
func (stats_mgr *StatisticsManager) composeUserAgent() {
	spec := stats_mgr.pipeline.Specification().GetReplicationSpec()
	stats_mgr.user_agent = base.ComposeUserAgentWithBucketNames(fmt.Sprintf("Goxdcr StatsMgr %v", stats_mgr.pipeline.Type()),
		spec.SourceBucketName, spec.TargetBucketName)
}

func (stats_mgr *StatisticsManager) initOverviewRegistry() {
	if overview_registry, ok := stats_mgr.registries[service_def.OVERVIEW_METRICS_KEY]; ok {
		// reset all counters to 0
		for overview_metric_key, metricType := range OverviewMetricKeys {
			if metricType == service_def.MetricTypeCounter {
				overview_registry.Get(overview_metric_key).(metrics.Counter).Clear()
			}
		}
	} else {
		// create new overview_registry and initialize all counters to 0 except for DOCS_CHECKED_METRIC
		overview_registry = metrics.NewRegistry()
		stats_mgr.registries[service_def.OVERVIEW_METRICS_KEY] = overview_registry
		for overview_metric_key, metricType := range OverviewMetricKeys {
			switch metricType {
			case service_def.MetricTypeGauge:
				overview_registry.Register(overview_metric_key, metrics.NewGauge())
			case service_def.MetricTypeCounter:
				if overview_metric_key == service_def.DOCS_CHECKED_METRIC {
					// use a negative value to indicate that an old value of docs_checked does not exist
					docs_checked_counter := metrics.NewCounter()
					setCounter(docs_checked_counter, -1)
					overview_registry.Register(service_def.DOCS_CHECKED_METRIC, docs_checked_counter)
				} else {
					overview_registry.Register(overview_metric_key, metrics.NewCounter())
				}
			}
		}
	}
}

func (stats_mgr *StatisticsManager) Start(settings metadata.ReplicationSettingsMap) error {
	stats_mgr.logger.Infof("%v StatisticsManager Starting...", stats_mgr.pipeline.InstanceId())

	err := stats_mgr.initializeConfig(settings)
	if err != nil {
		return err
	}

	err = stats_mgr.initDataFeeds()
	if err != nil {
		return err
	}

	stats_mgr.wait_grp.Add(1)
	go stats_mgr.updateStats()

	stats_mgr.wait_grp.Add(1)
	go stats_mgr.logStats()

	return nil
}

func (stats_mgr *StatisticsManager) initializeConfig(settings metadata.ReplicationSettingsMap) error {
	var redactedSettings metadata.ReplicationSettingsMap
	var redactOnce sync.Once
	redactOnceFunc := func() {
		redactOnce.Do(func() {
			redactedSettings = settings.CloneAndRedact()
		})
	}

	err := stats_mgr.updatePipelineStatus(settings)
	if err != nil {
		stats_mgr.logger.Warnf("Unable to understand pipelineStatus %v", err)
	}

	err = stats_mgr.updatePipelineErrors(settings)
	if err != nil {
		stats_mgr.logger.Warnf("Unable to understand pipelineErrors %v", err)
	}

	var update_interval int
	var update_interval_duration time.Duration
	if update_interval_obj, ok := settings[service_def.PUBLISH_INTERVAL]; ok {
		update_interval, ok = update_interval_obj.(int)
		if !ok {
			return fmt.Errorf("%v update_interval in settings map is not of integer type. update_interval=%v\n", stats_mgr.pipeline.InstanceId(), update_interval_obj)
		}
		stats_mgr.setUpdateInterval(update_interval)
		update_interval_duration = time.Duration(update_interval) * time.Millisecond
	} else {
		redactOnceFunc()
		stats_mgr.logger.Infof("%v There is no update_interval in settings map. settings=%v\n", stats_mgr.pipeline.InstanceId(), redactedSettings)
		update_interval_duration = stats_mgr.getUpdateInterval()
	}

	vbTasksRaw, exists := settings[parts.DCP_VBTasksMap]
	if exists {
		backfillVBTasks := vbTasksRaw.(*metadata.VBTasksMapType)
		stats_mgr.endSeqnos = make(map[uint16]uint64)
		backfillVBTasks.GetLock().RLock()
		for vb, vbTasks := range backfillVBTasks.VBTasksMap {
			if vbTasks != nil && vbTasks.Len() > 0 {
				topTask, _, unlockFunc := vbTasks.GetRO(0)
				endSeqno := topTask.GetEndingTimestampSeqno()
				unlockFunc()
				stats_mgr.endSeqnos[vb] = endSeqno
				stats_mgr.totalBackfillChanges += endSeqno
				stats_mgr.backfillUnsortedVBs = append(stats_mgr.backfillUnsortedVBs, vb)
			}
		}
		backfillVBTasks.GetLock().RUnlock()
	}

	if stats_mgr.logger.GetLogLevel() >= log.LogLevelDebug {
		redactOnceFunc()
		stats_mgr.logger.Debugf("%v StatisticsManager Starts: update_interval=%v, settings=%v\n", stats_mgr.pipeline.InstanceId(), update_interval_duration, redactedSettings)
	}
	stats_mgr.update_ticker_ch <- time.NewTicker(update_interval_duration)
	return nil
}

func (stats_mgr *StatisticsManager) Stop() error {
	stats_mgr.logger.Infof("%v StatisticsManager Stopping...", stats_mgr.pipeline.InstanceId())
	close(stats_mgr.finish_ch)

	if stats_mgr.printThroughSeqnoSummaryWhenStopping {
		// Backfill pipelines can exit before ever printing the number of oso received
		stats_mgr.through_seqno_tracker_svc.PrintStatusSummary()
	}

	stats_mgr.wait_grp.Wait()
	stats_mgr.logger.Infof("%v StatisticsManager Stopped", stats_mgr.pipeline.InstanceId())

	return nil
}

func (stats_mgr *StatisticsManager) initDataFeeds() error {
	spec := stats_mgr.pipeline.Specification().GetReplicationSpec()
	ref, err := stats_mgr.remoteClusterSvc.RemoteClusterByUuid(spec.TargetClusterUUID, false /*refresh*/)
	if err != nil {
		return err
	}
	rcCapability, err := stats_mgr.remoteClusterSvc.GetCapability(ref)
	if err != nil {
		return err
	}
	stats_mgr.cachedCapability = rcCapability

	stats_mgr.wait_grp.Add(1)
	go stats_mgr.subscribeToBucketTopologySvc(rcCapability, spec)

	stats_mgr.wait_grp.Add(1)
	go stats_mgr.watchNotificationCh()

	return nil
}

func (stats_mgr *StatisticsManager) waitForBucketTopologySvcSubscription() {
	select {
	case <-stats_mgr.finish_ch:
		return
	case <-stats_mgr.initDone:
		return
	}
}

func (stats_mgr *StatisticsManager) subscribeToBucketTopologySvc(rcCapability metadata.Capability, spec *metadata.ReplicationSpecification) {
	defer stats_mgr.wait_grp.Done()

	var initSubscribeErr error
	var updater func(duration time.Duration)
	initCh := make(chan bool, 1)
	initCh <- true
	var initHasRun bool
	subscribedCh := make(chan bool, 1)

	for {
		select {
		case <-initCh:
			initHasRun = true
			subscribeFunc := func() error {
				stopFunc := stats_mgr.utils.StartDiagStopwatch(fmt.Sprintf("%v_subscribeHighSeqno", stats_mgr.pipeline.InstanceId()), base.DiagInternalThreshold)
				if !rcCapability.HasCollectionSupport() {
					// If remote cluster does not support collection, then only get stats for default collection
					// This is reverse logic because to only get stat for the default collection, we need to enable collection
					// so we can ask specifically for a subset, aka the default collection
					stats_mgr.highSeqnosDcpCh, updater, initSubscribeErr = stats_mgr.bucketTopologySvc.SubscribeToLocalBucketHighSeqnosLegacyFeed(spec, stats_mgr.pipeline.InstanceId(), stats_mgr.getUpdateInterval())
					stats_mgr.getHighSeqnosAndSourceVBs = func() (base.HighSeqnosMapType, base.KvVBMapType, func()) {
						latestNotification := stats_mgr.getNotification()
						if latestNotification == nil {
							// StatsMgr stopped
							return nil, nil, func() {}
						}
						return latestNotification.GetHighSeqnosMapLegacy(), latestNotification.GetSourceVBMapRO(), latestNotification.Recycle
					}
				} else {
					stats_mgr.highSeqnosDcpCh, updater, initSubscribeErr = stats_mgr.bucketTopologySvc.SubscribeToLocalBucketHighSeqnosFeed(spec, stats_mgr.pipeline.InstanceId(), stats_mgr.getUpdateInterval())
					stats_mgr.getHighSeqnosAndSourceVBs = func() (base.HighSeqnosMapType, base.KvVBMapType, func()) {
						latestNotification := stats_mgr.getNotification()
						if latestNotification == nil {
							// StatsMgr stopped
							return nil, nil, func() {}
						}
						return latestNotification.GetHighSeqnosMap(), latestNotification.GetSourceVBMapRO(), latestNotification.Recycle
					}
				}
				stopFunc()
				if initSubscribeErr == nil {
					subscribedCh <- true
					stats_mgr.highSeqnosIntervalUpdaterMtx.Lock()
					stats_mgr.highSeqnosIntervalUpdater = updater
					stats_mgr.highSeqnosIntervalUpdaterMtx.Unlock()
				} else {
					subscribedCh <- false
				}
				close(stats_mgr.initDone)
				return initSubscribeErr
			}
			execErr := base.ExecWithTimeout(subscribeFunc, base.TimeoutRuntimeContextStart, stats_mgr.Logger())
			if execErr != nil {
				stats_mgr.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, stats_mgr, nil, fmt.Errorf("Subscribing to bucketTopologySvc: %v", execErr)))
				// Don't exit - set the error and let Stop() gets called so clean up will ensue
			}
		case <-stats_mgr.finish_ch:
			if initHasRun {
				select {
				case shouldUnSub := <-subscribedCh:
					if shouldUnSub {
						stopFunc := stats_mgr.utils.StartDiagStopwatch(fmt.Sprintf("%v_UnSubscribeHighSeqno", stats_mgr.pipeline.InstanceId()), base.DiagInternalThreshold)
						var unSubErr error
						if !rcCapability.HasCollectionSupport() {
							unSubErr = stats_mgr.bucketTopologySvc.UnSubscribeToLocalBucketHighSeqnosLegacyFeed(spec, stats_mgr.pipeline.InstanceId())
						} else {
							unSubErr = stats_mgr.bucketTopologySvc.UnSubscribeToLocalBucketHighSeqnosFeed(spec, stats_mgr.pipeline.InstanceId())
						}
						if unSubErr != nil {
							stats_mgr.logger.Warnf("%v Unsubscribing feed resulted in %v", stats_mgr.pipeline.InstanceId(), unSubErr)
						}
						stopFunc()
					}
				}
			}
			return
		}
	}
}

func (stats_mgr *StatisticsManager) getNotification() service_def.SourceNotification {
	opts := notificationReqOpt{
		sendBack: make(chan service_def.SourceNotification),
	}
	stats_mgr.latestNotificationReqCh <- opts
	select {
	case <-stats_mgr.finish_ch:
		return nil
	case notification := <-opts.sendBack:
		return notification
	}
}

func (stats_mgr *StatisticsManager) watchNotificationCh() {
	defer stats_mgr.wait_grp.Done()
	stats_mgr.waitForBucketTopologySvcSubscription()
	for {
		select {
		case <-stats_mgr.finish_ch:
			return
		case notification := <-stats_mgr.highSeqnosDcpCh:
		FORLOOP:
			for {
				select {
				case oneRequestor := <-stats_mgr.latestNotificationReqCh:
					// Because the channel can length can vary, just do a green clone to be safe
					notificationForRequestor := notification.Clone(1).(service_def.SourceNotification)
					select {
					case oneRequestor.sendBack <- notificationForRequestor:
						break
					case <-stats_mgr.finish_ch:
						notification.Recycle()
						return
					}
				default:
					break FORLOOP
				}
			}
			notification.Recycle()
		}
	}
}

func (stats_mgr *StatisticsManager) UpdateSettings(settings metadata.ReplicationSettingsMap) error {
	if stats_mgr.logger.GetLogLevel() >= log.LogLevelDebug {
		stats_mgr.logger.Debugf("%v Updating settings on stats manager. settings=%v\n", stats_mgr.pipeline.InstanceId(), settings.CloneAndRedact())
	}

	errMap := make(base.ErrorMap)
	err := stats_mgr.updatePublishInterval(settings)
	if err != nil {
		errMap[service_def.PUBLISH_INTERVAL] = err
	}

	err = stats_mgr.updatePipelineStatus(settings)
	if err != nil {
		errMap[service_def.PIPELINE_STATUS] = err
	}

	err = stats_mgr.updatePipelineErrors(settings)
	if err != nil {
		errMap[service_def.PIPELINE_ERRORS] = err
	}

	if len(errMap) > 0 {
		return errors.New(base.FlattenErrorMap(errMap))
	} else {
		return nil
	}
}

func (stats_mgr *StatisticsManager) updatePipelineStatus(settings metadata.ReplicationSettingsMap) error {
	pipelineStatus, err := stats_mgr.utils.GetIntSettingFromSettings(settings, service_def.PIPELINE_STATUS)
	if err != nil {
		return err
	}

	if pipelineStatus < 0 {
		// Not specified
		return nil
	}

	atomic.StoreInt32(&stats_mgr.pipelineStatus, int32(pipelineStatus))
	return nil
}

func (stats_mgr *StatisticsManager) updatePipelineErrors(settings metadata.ReplicationSettingsMap) error {
	pipelineErrs, err := stats_mgr.utils.GetIntSettingFromSettings(settings, service_def.PIPELINE_ERRORS)
	if err != nil {
		return err
	}

	atomic.StoreInt32(&stats_mgr.pipelineErrors, int32(pipelineErrs))
	return nil
}

func (stats_mgr *StatisticsManager) updatePublishInterval(settings metadata.ReplicationSettingsMap) error {
	update_interval, err := stats_mgr.utils.GetIntSettingFromSettings(settings, service_def.PUBLISH_INTERVAL)
	if err != nil {
		return err
	}

	if update_interval < 0 {
		// update_interval not specified. no op
		return nil
	}

	if stats_mgr.getUpdateInterval().Nanoseconds() == int64(update_interval)*1000000 {
		// no op if no real updates
		stats_mgr.logger.Infof("Skipped update of stats collection interval for pipeline %v since it already has the value of %v ms.\n", stats_mgr.pipeline.InstanceId(), update_interval)
		return nil
	}

	stats_mgr.setUpdateInterval(update_interval)
	stats_mgr.update_ticker_ch <- time.NewTicker(stats_mgr.getUpdateInterval())
	return nil
}

type MetricsCollector interface {
	Mount(pipeline common.Pipeline, stats_mgr *StatisticsManager) error
	OnEvent(event *common.Event)
	HandleLatestThroughSeqnos(SeqnoMap map[uint16]uint64)

	AddVbSpecificMetrics(vbno uint16, compiledMap base.VBCountMetricMap) error
	UpdateCurrentVbSpecificMetrics(vbno uint16, valuesToApply base.VBCountMetricMap, currentRegistries map[string]metrics.Registry) error
}

// metrics collector for custom conflict manager
type conflictMgrCollector struct {
	id        string
	stats_mgr *StatisticsManager
	common.AsyncComponentEventHandler
	// key of outer map: component id
	// key of inner map: metric name
	// value of inner map: metric value
	component_map map[string]map[string]interface{}
}

func (conflictMgr_collector *conflictMgrCollector) UpdateCurrentVbSpecificMetrics(vbno uint16, valuesToApply base.VBCountMetricMap, currentRegistries map[string]metrics.Registry) error {
	// do nothing
	return nil
}

func (conflictMgr_collector *conflictMgrCollector) AddVbSpecificMetrics(vbno uint16, compiledMap base.VBCountMetricMap) error {
	//do nothing
	return nil
}

func (conflictMgr_collector *conflictMgrCollector) Mount(pipeline common.Pipeline, stats_mgr *StatisticsManager) error {
	conflictMgrSvc := pipeline.RuntimeContext().Service(base.CONFLICT_MANAGER_SVC)
	if conflictMgrSvc == nil {
		return nil
	}
	conflictManager := conflictMgrSvc.(*ConflictManager)
	conflictMgr_collector.id = pipeline_utils.GetElementIdFromName(pipeline, base.ConflictMgrCollector)
	conflictMgr_collector.stats_mgr = stats_mgr
	conflictMgr_collector.component_map = make(map[string]map[string]interface{})
	registry := stats_mgr.getOrCreateRegistry(conflictManager.Id())
	docs_merged := metrics.NewCounter()
	registry.Register(service_def.DOCS_MERGED_METRIC, docs_merged)
	data_merged := metrics.NewCounter()
	registry.Register(service_def.DATA_MERGED_METRIC, data_merged)
	expiry_docs_merged := metrics.NewCounter()
	registry.Register(service_def.EXPIRY_DOCS_MERGED_METRIC, expiry_docs_merged)
	merge_latency := metrics.NewHistogram(metrics.NewUniformSample(stats_mgr.sample_size))
	registry.Register(service_def.MERGE_LATENCY_METRIC, merge_latency)
	resp_wait := metrics.NewHistogram(metrics.NewUniformSample(stats_mgr.sample_size))
	registry.Register(service_def.RESP_WAIT_METRIC, resp_wait)
	docs_merge_cas_changed := metrics.NewCounter()
	registry.Register(service_def.DOCS_MERGE_CAS_CHANGED_METRIC, docs_merge_cas_changed)
	expiry_merge_cas_changed := metrics.NewCounter()
	registry.Register(service_def.EXPIRY_MERGE_CAS_CHANGED_METRIC, expiry_merge_cas_changed)
	docs_merge_failed := metrics.NewCounter()
	registry.Register(service_def.DOCS_MERGE_FAILED_METRIC, docs_merge_failed)
	expiry_docs_merge_failed := metrics.NewCounter()
	registry.Register(service_def.EXPIRY_DOCS_MERGE_FAILED_METRIC, expiry_docs_merge_failed)
	data_merge_failed := metrics.NewCounter()
	registry.Register(service_def.DATA_MERGE_FAILED_METRIC, data_merge_failed)
	hlvPruned := metrics.NewCounter()
	registry.Register(service_def.HLV_PRUNED_AT_MERGE_METRIC, hlvPruned)

	metric_map := make(map[string]interface{})
	metric_map[service_def.DOCS_MERGED_METRIC] = docs_merged
	metric_map[service_def.DATA_MERGED_METRIC] = data_merged
	metric_map[service_def.EXPIRY_DOCS_MERGED_METRIC] = expiry_docs_merged
	metric_map[service_def.MERGE_LATENCY_METRIC] = merge_latency
	metric_map[service_def.RESP_WAIT_METRIC] = resp_wait
	metric_map[service_def.DOCS_MERGE_CAS_CHANGED_METRIC] = docs_merge_cas_changed
	metric_map[service_def.EXPIRY_MERGE_CAS_CHANGED_METRIC] = expiry_merge_cas_changed
	metric_map[service_def.DOCS_MERGE_FAILED_METRIC] = docs_merge_failed
	metric_map[service_def.EXPIRY_DOCS_MERGE_FAILED_METRIC] = expiry_docs_merge_failed
	metric_map[service_def.DATA_MERGE_FAILED_METRIC] = data_merge_failed
	metric_map[service_def.HLV_PRUNED_AT_MERGE_METRIC] = hlvPruned
	conflictMgr_collector.component_map[conflictManager.Id()] = metric_map

	conflictManager.RegisterComponentEventListener(common.DataMerged, conflictMgr_collector)
	conflictManager.RegisterComponentEventListener(common.MergeCasChanged, conflictMgr_collector)
	conflictManager.RegisterComponentEventListener(common.MergeFailed, conflictMgr_collector)
	conflictManager.RegisterComponentEventListener(common.HlvPrunedAtMerge, conflictMgr_collector)

	async_listener_map := pipeline_pkg.GetAllAsyncComponentEventListeners(pipeline)
	pipeline_utils.RegisterAsyncComponentEventHandler(async_listener_map, base.DataMergedEventListener, conflictMgr_collector)
	pipeline_utils.RegisterAsyncComponentEventHandler(async_listener_map, base.MergeCasChangedEventListener, conflictMgr_collector)
	pipeline_utils.RegisterAsyncComponentEventHandler(async_listener_map, base.MergeFailedEventListener, conflictMgr_collector)
	pipeline_utils.RegisterAsyncComponentEventHandler(async_listener_map, base.HlvPrunedAtMergeEventListener, conflictMgr_collector)

	return nil
}

func (conflictMgr_collector *conflictMgrCollector) Id() string {
	return conflictMgr_collector.id
}

func (conflictMgr_collector *conflictMgrCollector) OnEvent(event *common.Event) {
	conflictMgr_collector.ProcessEvent(event)
}

func (conflictMgr_collector *conflictMgrCollector) HandleLatestThroughSeqnos(SeqnoMap map[uint16]uint64) {
	// Nothing
	return
}

func (conflictMgr_collector *conflictMgrCollector) ProcessEvent(event *common.Event) error {
	metric_map := conflictMgr_collector.component_map[event.Component.Id()]
	switch event.EventType {
	case common.DataMerged:
		event_otherInfo := event.OtherInfos.(DataMergedEventAdditional)
		req_size := event_otherInfo.Req_size
		commit_time := event_otherInfo.Commit_time
		resp_wait_time := event_otherInfo.Resp_wait_time
		metric_map[service_def.DOCS_MERGED_METRIC].(metrics.Counter).Inc(1)
		metric_map[service_def.DATA_MERGED_METRIC].(metrics.Counter).Inc(int64(req_size))

		expiry_set := event_otherInfo.IsExpirySet
		if expiry_set {
			metric_map[service_def.EXPIRY_DOCS_MERGED_METRIC].(metrics.Counter).Inc(1)
		}

		metric_map[service_def.MERGE_LATENCY_METRIC].(metrics.Histogram).Sample().Update(commit_time.Nanoseconds() / 1000000)
		metric_map[service_def.RESP_WAIT_METRIC].(metrics.Histogram).Sample().Update(resp_wait_time.Nanoseconds() / 1000000)
	case common.MergeCasChanged:
		event_otherInfo := event.OtherInfos.(DataMergeCasChangedEventAdditional)
		metric_map[service_def.DOCS_MERGE_CAS_CHANGED_METRIC].(metrics.Counter).Inc(1)

		if event_otherInfo.IsExpirySet {
			metric_map[service_def.EXPIRY_MERGE_CAS_CHANGED_METRIC].(metrics.Counter).Inc(1)
		}
	case common.MergeFailed:
		event_otherInfo := event.OtherInfos.(DataMergeFailedEventAdditional)
		metric_map[service_def.DOCS_MERGE_FAILED_METRIC].(metrics.Counter).Inc(1)
		metric_map[service_def.DATA_MERGE_FAILED_METRIC].(metrics.Counter).Inc(int64(event_otherInfo.Req_size))

		if event_otherInfo.IsExpirySet {
			metric_map[service_def.EXPIRY_DOCS_MERGE_FAILED_METRIC].(metrics.Counter).Inc(1)
		}
	case common.HlvPrunedAtMerge:
		metric_map[service_def.HLV_PRUNED_AT_MERGE_METRIC].(metrics.Counter).Inc(1)
	}
	return nil
}

// metrics collector for XMem/CapiNozzle
type outNozzleCollector struct {
	id        string
	stats_mgr *StatisticsManager
	common.AsyncComponentEventHandler
	// key of outer map: component id
	// key of inner map: metric name
	// value of inner map: metric value
	component_map map[string]map[string]interface{}

	vbMetricHelper *VbBasedMetricHelper
}

func (outNozzle_collector *outNozzleCollector) UpdateCurrentVbSpecificMetrics(vbno uint16, valuesToApply base.VBCountMetricMap, currentRegistries map[string]metrics.Registry) error {
	return outNozzle_collector.vbMetricHelper.UpdateCurrentVbSpecificMetrics(vbno, valuesToApply, currentRegistries)
}

func (outNozzle_collector *outNozzleCollector) AddVbSpecificMetrics(vbno uint16, compiledMap base.VBCountMetricMap) error {
	return outNozzle_collector.vbMetricHelper.AddVbSpecificMetrics(vbno, compiledMap)
}

func (outNozzle_collector *outNozzleCollector) Mount(pipeline common.Pipeline, stats_mgr *StatisticsManager) error {
	outNozzle_collector.id = pipeline_utils.GetElementIdFromName(pipeline, base.OutNozzleStatsCollector)
	outNozzle_collector.stats_mgr = stats_mgr
	outNozzle_collector.component_map = make(map[string]map[string]interface{})
	outNozzle_collector.vbMetricHelper = NewVbBasedMetricHelper()
	outNozzle_parts := pipeline.Targets()
	for _, part := range outNozzle_parts {
		registry := stats_mgr.getOrCreateRegistry(part.Id())
		size_rep_queue := metrics.NewCounter()
		registry.Register(service_def.SIZE_REP_QUEUE_METRIC, size_rep_queue)
		docs_rep_queue := metrics.NewCounter()
		registry.Register(service_def.DOCS_REP_QUEUE_METRIC, docs_rep_queue)
		docs_written := metrics.NewCounter()
		registry.Register(service_def.DOCS_WRITTEN_METRIC, docs_written)
		expiry_docs_written := metrics.NewCounter()
		registry.Register(service_def.EXPIRY_DOCS_WRITTEN_METRIC, expiry_docs_written)
		deletion_docs_written := metrics.NewCounter()
		registry.Register(service_def.DELETION_DOCS_WRITTEN_METRIC, deletion_docs_written)
		set_docs_written := metrics.NewCounter()
		registry.Register(service_def.SET_DOCS_WRITTEN_METRIC, set_docs_written)
		add_docs_written := metrics.NewCounter()
		registry.Register(service_def.ADD_DOCS_WRITTEN_METRIC, add_docs_written)
		docs_failed_cr := metrics.NewCounter()
		registry.Register(service_def.DOCS_FAILED_CR_SOURCE_METRIC, docs_failed_cr)
		expiry_failed_cr := metrics.NewCounter()
		registry.Register(service_def.EXPIRY_FAILED_CR_SOURCE_METRIC, expiry_failed_cr)
		deletion_failed_cr := metrics.NewCounter()
		registry.Register(service_def.DELETION_FAILED_CR_SOURCE_METRIC, deletion_failed_cr)
		set_failed_cr := metrics.NewCounter()
		registry.Register(service_def.SET_FAILED_CR_SOURCE_METRIC, set_failed_cr)
		docs_failed_cr_target := metrics.NewCounter()
		registry.Register(service_def.DOCS_FAILED_CR_TARGET_METRIC, docs_failed_cr_target)
		add_failed_cr_target := metrics.NewCounter()
		registry.Register(service_def.ADD_FAILED_CR_TARGET_METRIC, add_failed_cr_target)
		expiry_failed_cr_target := metrics.NewCounter()
		registry.Register(service_def.EXPIRY_FAILED_CR_TARGET_METRIC, expiry_failed_cr_target)
		deletion_failed_cr_target := metrics.NewCounter()
		registry.Register(service_def.DELETION_FAILED_CR_TARGET_METRIC, deletion_failed_cr_target)
		set_failed_cr_target := metrics.NewCounter()
		registry.Register(service_def.SET_FAILED_CR_TARGET_METRIC, set_failed_cr_target)
		target_docs_skipped := metrics.NewCounter()
		registry.Register(service_def.TARGET_DOCS_SKIPPED_METRIC, target_docs_skipped)
		expiry_target_docs_skipped := metrics.NewCounter()
		registry.Register(service_def.EXPIRY_TARGET_DOCS_SKIPPED_METRIC, expiry_target_docs_skipped)
		deletion_target_docs_skipped := metrics.NewCounter()
		registry.Register(service_def.DELETION_TARGET_DOCS_SKIPPED_METRIC, deletion_target_docs_skipped)
		set_target_docs_skipped := metrics.NewCounter()
		registry.Register(service_def.SET_TARGET_DOCS_SKIPPED_METRIC, set_target_docs_skipped)
		data_replicated := metrics.NewCounter()
		registry.Register(service_def.DATA_REPLICATED_METRIC, data_replicated)
		docs_opt_repd := metrics.NewCounter()
		registry.Register(service_def.DOCS_OPT_REPD_METRIC, docs_opt_repd)
		docs_latency := metrics.NewHistogram(metrics.NewUniformSample(stats_mgr.sample_size))
		registry.Register(service_def.DOCS_LATENCY_METRIC, docs_latency)
		resp_wait := metrics.NewHistogram(metrics.NewUniformSample(stats_mgr.sample_size))
		registry.Register(service_def.RESP_WAIT_METRIC, resp_wait)
		meta_latency := metrics.NewHistogram(metrics.NewUniformSample(stats_mgr.sample_size))
		registry.Register(service_def.META_LATENCY_METRIC, meta_latency)
		throttle_latency := metrics.NewHistogram(metrics.NewUniformSample(stats_mgr.sample_size))
		registry.Register(service_def.THROTTLE_LATENCY_METRIC, throttle_latency)
		dp_failed := metrics.NewCounter()
		registry.Register(service_def.DP_GET_FAIL_METRIC, dp_failed)
		get_doc_latency := metrics.NewHistogram(metrics.NewUniformSample(stats_mgr.sample_size))
		registry.Register(service_def.GET_DOC_LATENCY_METRIC, get_doc_latency)
		deletion_cas_changed := metrics.NewCounter()
		registry.Register(service_def.DELETION_DOCS_CAS_CHANGED_METRIC, deletion_cas_changed)
		set_cas_changed := metrics.NewCounter()
		registry.Register(service_def.SET_DOCS_CAS_CHANGED_METRIC, set_cas_changed)
		add_cas_changed := metrics.NewCounter()
		registry.Register(service_def.ADD_DOCS_CAS_CHANGED_METRIC, add_cas_changed)
		subdoc_cmd_cas_changed := metrics.NewCounter()
		registry.Register(service_def.SUBDOC_CMD_DOCS_CAS_CHANGED_METRIC, subdoc_cmd_cas_changed)
		data_replicated_uncompressed := metrics.NewCounter()
		registry.Register(service_def.DATA_REPLICATED_UNCOMPRESSED_METRIC, data_replicated_uncompressed)
		docs_compression_skipped := metrics.NewCounter()
		registry.Register(service_def.DOCS_COMPRESSION_SKIPPED_METRIC, docs_compression_skipped)
		eaccessReceived := metrics.NewCounter()
		registry.Register(service_def.TARGET_EACCESS_METRIC, eaccessReceived)
		tmpfailReceived := metrics.NewCounter()
		registry.Register(service_def.TARGET_TMPFAIL_METRIC, tmpfailReceived)
		guardRailRR := metrics.NewCounter()
		registry.Register(service_def.GUARDRAIL_RESIDENT_RATIO_METRIC, guardRailRR)
		guardRailDataSz := metrics.NewCounter()
		registry.Register(service_def.GUARDRAIL_DATA_SIZE_METRIC, guardRailDataSz)
		guardRailDiskSpace := metrics.NewCounter()
		registry.Register(service_def.GUARDRAIL_DISK_SPACE_METRIC, guardRailDiskSpace)
		unknownStatusReceived := metrics.NewCounter()
		registry.Register(service_def.TARGET_UNKNOWN_STATUS_METRIC, unknownStatusReceived)
		sourceSyncXattrRemoved := metrics.NewCounter()
		registry.Register(service_def.SOURCE_SYNC_XATTR_REMOVED_METRIC, sourceSyncXattrRemoved)
		targetSyncXattrPreserved := metrics.NewCounter()
		registry.Register(service_def.TARGET_SYNC_XATTR_PRESERVED_METRIC, targetSyncXattrPreserved)
		importMutationsProcessed := metrics.NewCounter()
		registry.Register(service_def.IMPORT_DOCS_FAILED_CR_SOURCE_METRIC, importMutationsProcessed)
		importMutationsSent := metrics.NewCounter()
		registry.Register(service_def.IMPORT_DOCS_WRITTEN_METRIC, importMutationsSent)
		hlvPruned := metrics.NewCounter()
		registry.Register(service_def.HLV_PRUNED_METRIC, hlvPruned)
		hlvUpdated := metrics.NewCounter()
		registry.Register(service_def.HLV_UPDATED_METRIC, hlvUpdated)
		docsSentWithSubdocSet := metrics.NewCounter()
		registry.Register(service_def.DOCS_SENT_WITH_SUBDOC_SET, docsSentWithSubdocSet)
		docsSentWithSubdocDelete := metrics.NewCounter()
		registry.Register(service_def.DOCS_SENT_WITH_SUBDOC_DELETE, docsSentWithSubdocDelete)
		docsSentWithPoisonedCasError := metrics.NewCounter()
		registry.Register(service_def.DOCS_SENT_WITH_POISONED_CAS_ERROR, docsSentWithPoisonedCasError)
		docsSentWithPoisonedCasReplace := metrics.NewCounter()
		registry.Register(service_def.DOCS_SENT_WITH_POISONED_CAS_REPLACE, docsSentWithPoisonedCasReplace)

		metric_map := make(map[string]interface{})
		metric_map[service_def.SIZE_REP_QUEUE_METRIC] = size_rep_queue
		metric_map[service_def.DOCS_REP_QUEUE_METRIC] = docs_rep_queue
		metric_map[service_def.DOCS_WRITTEN_METRIC] = docs_written
		metric_map[service_def.EXPIRY_DOCS_WRITTEN_METRIC] = expiry_docs_written
		metric_map[service_def.DELETION_DOCS_WRITTEN_METRIC] = deletion_docs_written
		metric_map[service_def.SET_DOCS_WRITTEN_METRIC] = set_docs_written
		metric_map[service_def.ADD_DOCS_WRITTEN_METRIC] = add_docs_written
		metric_map[service_def.DOCS_FAILED_CR_SOURCE_METRIC] = docs_failed_cr
		metric_map[service_def.EXPIRY_FAILED_CR_SOURCE_METRIC] = expiry_failed_cr
		metric_map[service_def.DELETION_FAILED_CR_SOURCE_METRIC] = deletion_failed_cr
		metric_map[service_def.SET_FAILED_CR_SOURCE_METRIC] = set_failed_cr
		metric_map[service_def.DOCS_FAILED_CR_TARGET_METRIC] = docs_failed_cr_target
		metric_map[service_def.ADD_FAILED_CR_TARGET_METRIC] = add_failed_cr_target
		metric_map[service_def.DELETION_FAILED_CR_TARGET_METRIC] = deletion_failed_cr_target
		metric_map[service_def.EXPIRY_FAILED_CR_TARGET_METRIC] = expiry_failed_cr_target
		metric_map[service_def.SET_FAILED_CR_TARGET_METRIC] = set_failed_cr_target
		metric_map[service_def.TARGET_DOCS_SKIPPED_METRIC] = target_docs_skipped
		metric_map[service_def.EXPIRY_TARGET_DOCS_SKIPPED_METRIC] = expiry_target_docs_skipped
		metric_map[service_def.DELETION_TARGET_DOCS_SKIPPED_METRIC] = deletion_target_docs_skipped
		metric_map[service_def.SET_TARGET_DOCS_SKIPPED_METRIC] = set_target_docs_skipped
		metric_map[service_def.DATA_REPLICATED_METRIC] = data_replicated
		metric_map[service_def.DOCS_OPT_REPD_METRIC] = docs_opt_repd
		metric_map[service_def.DOCS_LATENCY_METRIC] = docs_latency
		metric_map[service_def.RESP_WAIT_METRIC] = resp_wait
		metric_map[service_def.META_LATENCY_METRIC] = meta_latency
		metric_map[service_def.THROTTLE_LATENCY_METRIC] = throttle_latency
		metric_map[service_def.GET_DOC_LATENCY_METRIC] = get_doc_latency
		metric_map[service_def.DELETION_DOCS_CAS_CHANGED_METRIC] = deletion_cas_changed
		metric_map[service_def.SET_DOCS_CAS_CHANGED_METRIC] = set_cas_changed
		metric_map[service_def.ADD_DOCS_CAS_CHANGED_METRIC] = add_cas_changed
		metric_map[service_def.SUBDOC_CMD_DOCS_CAS_CHANGED_METRIC] = subdoc_cmd_cas_changed
		metric_map[service_def.DATA_REPLICATED_UNCOMPRESSED_METRIC] = data_replicated_uncompressed
		metric_map[service_def.DOCS_COMPRESSION_SKIPPED_METRIC] = docs_compression_skipped
		metric_map[service_def.TARGET_EACCESS_METRIC] = eaccessReceived
		metric_map[service_def.TARGET_TMPFAIL_METRIC] = tmpfailReceived
		metric_map[service_def.GUARDRAIL_RESIDENT_RATIO_METRIC] = guardRailRR
		metric_map[service_def.GUARDRAIL_DATA_SIZE_METRIC] = guardRailDataSz
		metric_map[service_def.GUARDRAIL_DISK_SPACE_METRIC] = guardRailDiskSpace
		metric_map[service_def.TARGET_UNKNOWN_STATUS_METRIC] = unknownStatusReceived
		metric_map[service_def.SOURCE_SYNC_XATTR_REMOVED_METRIC] = sourceSyncXattrRemoved
		metric_map[service_def.TARGET_SYNC_XATTR_PRESERVED_METRIC] = targetSyncXattrPreserved
		metric_map[service_def.IMPORT_DOCS_FAILED_CR_SOURCE_METRIC] = importMutationsProcessed
		metric_map[service_def.IMPORT_DOCS_WRITTEN_METRIC] = importMutationsSent
		metric_map[service_def.HLV_PRUNED_METRIC] = hlvPruned
		metric_map[service_def.HLV_UPDATED_METRIC] = hlvUpdated
		metric_map[service_def.DOCS_SENT_WITH_SUBDOC_SET] = docsSentWithSubdocSet
		metric_map[service_def.DOCS_SENT_WITH_SUBDOC_DELETE] = docsSentWithSubdocDelete
		metric_map[service_def.DOCS_SENT_WITH_POISONED_CAS_ERROR] = docsSentWithPoisonedCasError
		metric_map[service_def.DOCS_SENT_WITH_POISONED_CAS_REPLACE] = docsSentWithPoisonedCasReplace

		listOfVBs := part.ResponsibleVBs()
		outNozzle_collector.vbMetricHelper.Register(outNozzle_collector.Id(), listOfVBs, part.Id(), OutNozzleVBMetricKeys)
		outNozzle_collector.component_map[part.Id()] = metric_map
		// register outNozzle_collector as the sync event listener/handler for StatsUpdate event
		part.RegisterComponentEventListener(common.StatsUpdate, outNozzle_collector)
	}

	// register outNozzle_collector as the async event handler for relevant events
	async_listener_map := pipeline_pkg.GetAllAsyncComponentEventListeners(pipeline)
	pipeline_utils.RegisterAsyncComponentEventHandler(async_listener_map, base.DataSentEventListener, outNozzle_collector)
	pipeline_utils.RegisterAsyncComponentEventHandler(async_listener_map, base.DataFailedCREventListener, outNozzle_collector)
	pipeline_utils.RegisterAsyncComponentEventHandler(async_listener_map, base.TargetDataSkippedEventListener, outNozzle_collector)
	pipeline_utils.RegisterAsyncComponentEventHandler(async_listener_map, base.GetReceivedEventListener, outNozzle_collector)
	pipeline_utils.RegisterAsyncComponentEventHandler(async_listener_map, base.DataThrottledEventListener, outNozzle_collector)
	pipeline_utils.RegisterAsyncComponentEventHandler(async_listener_map, base.DataSentCasChangedEventListener, outNozzle_collector)
	pipeline_utils.RegisterAsyncComponentEventHandler(async_listener_map, base.DataSentFailedListener, outNozzle_collector)
	pipeline_utils.RegisterAsyncComponentEventHandler(async_listener_map, base.SrcSyncXattrRemovedEventListener, outNozzle_collector)
	pipeline_utils.RegisterAsyncComponentEventHandler(async_listener_map, base.TgtSyncXattrPreservedEventListener, outNozzle_collector)
	pipeline_utils.RegisterAsyncComponentEventHandler(async_listener_map, base.HlvUpdatedEventListener, outNozzle_collector)
	pipeline_utils.RegisterAsyncComponentEventHandler(async_listener_map, base.HlvPrunedEventListener, outNozzle_collector)
	pipeline_utils.RegisterAsyncComponentEventHandler(async_listener_map, base.DocsSentWithSubdocCmdEventListener, outNozzle_collector)
	pipeline_utils.RegisterAsyncComponentEventHandler(async_listener_map, base.DocsSentWithPoisonedCasEventListener, outNozzle_collector)

	return nil
}

func (outNozzle_collector *outNozzleCollector) Id() string {
	return outNozzle_collector.id
}

func (outNozzle_collector *outNozzleCollector) OnEvent(event *common.Event) {
	outNozzle_collector.ProcessEvent(event)
}

func (outNozzleCollector *outNozzleCollector) HandleLatestThroughSeqnos(SeqnoMap map[uint16]uint64) {
	outNozzleCollector.vbMetricHelper.HandleLatestThroughSeqnos(SeqnoMap)
}

func (outNozzle_collector *outNozzleCollector) ProcessEvent(event *common.Event) error {
	metricMap := outNozzle_collector.component_map[event.Component.Id()]

	switch event.EventType {
	case common.StatsUpdate:
		queue_size := event.OtherInfos.([]int)[0]
		queue_size_bytes := event.OtherInfos.([]int)[1]
		setCounter(metricMap[service_def.DOCS_REP_QUEUE_METRIC].(metrics.Counter), queue_size)
		setCounter(metricMap[service_def.SIZE_REP_QUEUE_METRIC].(metrics.Counter), queue_size_bytes)

	case common.DataSent:
		event_otherInfo := event.OtherInfos.(parts.DataSentEventAdditional)
		req_size := event_otherInfo.Req_size
		opti_replicated := event_otherInfo.IsOptRepd
		commit_time := event_otherInfo.Commit_time
		resp_wait_time := event_otherInfo.Resp_wait_time
		metricMap[service_def.DOCS_WRITTEN_METRIC].(metrics.Counter).Inc(1)
		metricMap[service_def.DATA_REPLICATED_METRIC].(metrics.Counter).Inc(int64(req_size))
		metricMap[service_def.DATA_REPLICATED_UNCOMPRESSED_METRIC].(metrics.Counter).Inc(int64(event_otherInfo.UncompressedReqSize))
		if event_otherInfo.SkippedRecompression {
			metricMap[service_def.DOCS_COMPRESSION_SKIPPED_METRIC].(metrics.Counter).Inc(1)
		}
		if opti_replicated {
			metricMap[service_def.DOCS_OPT_REPD_METRIC].(metrics.Counter).Inc(1)
		}
		if event_otherInfo.FailedTargetCR {
			metricMap[service_def.DOCS_FAILED_CR_TARGET_METRIC].(metrics.Counter).Inc(1)
		}
		expiry_set := event_otherInfo.IsExpirySet
		if expiry_set {
			metricMap[service_def.EXPIRY_DOCS_WRITTEN_METRIC].(metrics.Counter).Inc(1)
			if event_otherInfo.FailedTargetCR {
				metricMap[service_def.EXPIRY_FAILED_CR_TARGET_METRIC].(metrics.Counter).Inc(1)
			}
		}
		if event_otherInfo.ImportMutation {
			metricMap[service_def.IMPORT_DOCS_WRITTEN_METRIC].(metrics.Counter).Inc(1)
		}

		opcode := event_otherInfo.Opcode
		switch opcode {
		case base.DELETE_WITH_META:
			metricMap[service_def.DELETION_DOCS_WRITTEN_METRIC].(metrics.Counter).Inc(1)
			if event_otherInfo.FailedTargetCR {
				metricMap[service_def.DELETION_FAILED_CR_TARGET_METRIC].(metrics.Counter).Inc(1)
			}

		case base.SET_WITH_META:
			metricMap[service_def.SET_DOCS_WRITTEN_METRIC].(metrics.Counter).Inc(1)
			if event_otherInfo.FailedTargetCR {
				metricMap[service_def.SET_FAILED_CR_TARGET_METRIC].(metrics.Counter).Inc(1)
			}

		case base.ADD_WITH_META:
			metricMap[service_def.ADD_DOCS_WRITTEN_METRIC].(metrics.Counter).Inc(1)
			if event_otherInfo.FailedTargetCR {
				metricMap[service_def.ADD_FAILED_CR_TARGET_METRIC].(metrics.Counter).Inc(1)
			}

		case base.SUBDOC_MULTI_MUTATION:
			// SUBDOC_MULTI_MUTATION docs written as taken care as part of DocsSentWithSubdocCmd event, so ignore here.
			// There are no failed CR on target SUBDOC_MULTI_MUTATION docs, because it is only used in mobile mode.

		default:
			outNozzle_collector.stats_mgr.logger.Warnf("Invalid opcode, %v, in DataSent event from %v.", opcode, event.Component.Id())
		}
		metricMap[service_def.DOCS_LATENCY_METRIC].(metrics.Histogram).Sample().Update(commit_time.Nanoseconds() / 1000000)
		metricMap[service_def.RESP_WAIT_METRIC].(metrics.Histogram).Sample().Update(resp_wait_time.Nanoseconds() / 1000000)

	case common.DataFailedCRSource:
		metricMap[service_def.DOCS_FAILED_CR_SOURCE_METRIC].(metrics.Counter).Inc(1)
		event_otherInfos := event.OtherInfos.(parts.DataFailedCRSourceEventAdditional)
		expiry_set := event_otherInfos.IsExpirySet
		if expiry_set {
			metricMap[service_def.EXPIRY_FAILED_CR_SOURCE_METRIC].(metrics.Counter).Inc(1)
		}
		if event_otherInfos.ImportMutation {
			metricMap[service_def.IMPORT_DOCS_FAILED_CR_SOURCE_METRIC].(metrics.Counter).Inc(1)
		}

		opcode := event_otherInfos.Opcode
		switch opcode {
		case base.DELETE_WITH_META:
			metricMap[service_def.DELETION_FAILED_CR_SOURCE_METRIC].(metrics.Counter).Inc(1)

		case base.SET_WITH_META:
			metricMap[service_def.SET_FAILED_CR_SOURCE_METRIC].(metrics.Counter).Inc(1)

		default:
			outNozzle_collector.stats_mgr.logger.Warnf("Invalid opcode, %v, in DataFailedCRSource event from %v.", opcode, event.Component.Id())
		}

	case common.TargetDataSkipped:
		metricMap[service_def.TARGET_DOCS_SKIPPED_METRIC].(metrics.Counter).Inc(1)
		event_otherInfos := event.OtherInfos.(parts.TargetDataSkippedEventAdditional)
		expiry_set := event_otherInfos.IsExpirySet
		if expiry_set {
			metricMap[service_def.EXPIRY_TARGET_DOCS_SKIPPED_METRIC].(metrics.Counter).Inc(1)
		}

		opcode := event_otherInfos.Opcode
		switch opcode {
		case base.DELETE_WITH_META:
			metricMap[service_def.DELETION_TARGET_DOCS_SKIPPED_METRIC].(metrics.Counter).Inc(1)

		case base.SET_WITH_META:
			metricMap[service_def.SET_TARGET_DOCS_SKIPPED_METRIC].(metrics.Counter).Inc(1)

		default:
			outNozzle_collector.stats_mgr.logger.Warnf("Invalid opcode, %v, in DataFailedCRSource event from %v.", opcode, event.Component.Id())
		}

	case common.GetDocReceived:
		event_otherInfos := event.OtherInfos.(parts.GetReceivedEventAdditional)
		commit_time := event_otherInfos.Commit_time
		metricMap[service_def.GET_DOC_LATENCY_METRIC].(metrics.Histogram).Sample().Update(commit_time.Nanoseconds() / 1000000)

	case common.GetMetaReceived:
		event_otherInfos := event.OtherInfos.(parts.GetReceivedEventAdditional)
		commit_time := event_otherInfos.Commit_time
		metricMap[service_def.META_LATENCY_METRIC].(metrics.Histogram).Sample().Update(commit_time.Nanoseconds() / 1000000)

	case common.DataThrottled:
		throttle_latency := event.OtherInfos.(time.Duration)
		metricMap[service_def.THROTTLE_LATENCY_METRIC].(metrics.Histogram).Sample().Update(throttle_latency.Nanoseconds() / 1000000)

	case common.DataPoolGetFail:
		metricMap[service_def.DP_GET_FAIL_METRIC].(metrics.Counter).Inc(event.Data.(int64))

	case common.DataSentCasChanged:
		event_otherInfos := event.OtherInfos.(parts.SentCasChangedEventAdditional)
		opcode := event_otherInfos.Opcode
		switch opcode {
		case base.DELETE_WITH_META:
			metricMap[service_def.DELETION_DOCS_CAS_CHANGED_METRIC].(metrics.Counter).Inc(1)
		case base.SET_WITH_META:
			metricMap[service_def.SET_DOCS_CAS_CHANGED_METRIC].(metrics.Counter).Inc(1)
		case base.ADD_WITH_META:
			metricMap[service_def.ADD_DOCS_CAS_CHANGED_METRIC].(metrics.Counter).Inc(1)
		case base.SUBDOC_MULTI_MUTATION:
			metricMap[service_def.SUBDOC_CMD_DOCS_CAS_CHANGED_METRIC].(metrics.Counter).Inc(1)
		}

	case common.DataSentFailed:
		responseCode, ok := event.Data.(mc.Status)
		if !ok {
			return nil
		}
		switch responseCode {
		case mc.TMPFAIL:
			metricMap[service_def.TARGET_TMPFAIL_METRIC].(metrics.Counter).Inc(1)

		case mc.EACCESS:
			metricMap[service_def.TARGET_EACCESS_METRIC].(metrics.Counter).Inc(1)
		}

	case common.SourceSyncXattrRemoved:
		metricMap[service_def.SOURCE_SYNC_XATTR_REMOVED_METRIC].(metrics.Counter).Inc(1)
	case common.TargetSyncXattrPreserved:
		metricMap[service_def.TARGET_SYNC_XATTR_PRESERVED_METRIC].(metrics.Counter).Inc(1)
	case common.HlvPruned:
		metricMap[service_def.HLV_PRUNED_METRIC].(metrics.Counter).Inc(1)
	case common.HlvUpdated:
		metricMap[service_def.HLV_UPDATED_METRIC].(metrics.Counter).Inc(1)
	case common.DataSentHitGuardrail:
		responseCode, ok := event.Data.(mc.Status)
		if !ok {
			return nil
		}

		switch responseCode {
		case mc.BUCKET_RESIDENT_RATIO_TOO_LOW:
			metricMap[service_def.GUARDRAIL_RESIDENT_RATIO_METRIC].(metrics.Counter).Inc(1)
			err := outNozzle_collector.handleVBEvent(event, service_def.GUARDRAIL_RESIDENT_RATIO_METRIC)
			if err != nil {
				return err
			}

		case mc.BUCKET_DATA_SIZE_TOO_BIG:
			metricMap[service_def.GUARDRAIL_DATA_SIZE_METRIC].(metrics.Counter).Inc(1)
			err := outNozzle_collector.handleVBEvent(event, service_def.GUARDRAIL_DATA_SIZE_METRIC)
			if err != nil {
				return err
			}

		case mc.BUCKET_DISK_SPACE_TOO_LOW:
			metricMap[service_def.GUARDRAIL_DISK_SPACE_METRIC].(metrics.Counter).Inc(1)
			err := outNozzle_collector.handleVBEvent(event, service_def.GUARDRAIL_DISK_SPACE_METRIC)
			if err != nil {
				return err
			}

		}

	case common.DataSentFailedUnknownStatus:
		metricMap[service_def.TARGET_UNKNOWN_STATUS_METRIC].(metrics.Counter).Inc(1)

	case common.DocsSentWithSubdocCmd:
		subdocOp := event.Data.(base.SubdocOpType)
		switch subdocOp {
		case base.SubdocDelete:
			metricMap[service_def.DOCS_SENT_WITH_SUBDOC_DELETE].(metrics.Counter).Inc(1)
			err := outNozzle_collector.handleVBEvent(event, service_def.DOCS_SENT_WITH_SUBDOC_DELETE)
			if err != nil {
				return err
			}
		case base.SubdocSet:
			metricMap[service_def.DOCS_SENT_WITH_SUBDOC_SET].(metrics.Counter).Inc(1)
			err := outNozzle_collector.handleVBEvent(event, service_def.DOCS_SENT_WITH_SUBDOC_SET)
			if err != nil {
				return err
			}
		default:
			err := base.ErrorUnexpectedSubdocOp
			outNozzle_collector.stats_mgr.logger.Errorf(err.Error())
			return err
		}
	case common.DocsSentWithPoisonedCas:
		protectionMode := event.Data.(base.TargetKVCasPoisonProtectionMode)
		switch protectionMode {
		case base.ErrorMode:
			metricMap[service_def.DOCS_SENT_WITH_POISONED_CAS_ERROR].(metrics.Counter).Inc(1)
			err := outNozzle_collector.handleVBEvent(event, service_def.DOCS_SENT_WITH_POISONED_CAS_ERROR)
			if err != nil {
				return err
			}
		case base.ReplaceMode:
			metricMap[service_def.DOCS_SENT_WITH_POISONED_CAS_REPLACE].(metrics.Counter).Inc(1)
			err := outNozzle_collector.handleVBEvent(event, service_def.DOCS_SENT_WITH_POISONED_CAS_REPLACE)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (outNozzle_collector *outNozzleCollector) handleVBEvent(event *common.Event, metricKey string) error {
	switch metricKey {
	case service_def.DOCS_SENT_WITH_SUBDOC_SET:
		fallthrough
	case service_def.DOCS_SENT_WITH_SUBDOC_DELETE:
		fallthrough
	case service_def.DOCS_SENT_WITH_POISONED_CAS_ERROR:
		fallthrough
	case service_def.DOCS_SENT_WITH_POISONED_CAS_REPLACE:
		fallthrough
	case service_def.GUARDRAIL_DISK_SPACE_METRIC:
		fallthrough
	case service_def.GUARDRAIL_RESIDENT_RATIO_METRIC:
		fallthrough
	case service_def.GUARDRAIL_DATA_SIZE_METRIC:
		vbucket := event.DerivedData[0].(uint16)
		seqno := event.DerivedData[1].(uint64)
		helper, ok := outNozzle_collector.vbMetricHelper.vbBasedHelper[vbucket]
		if !ok {
			return base.ErrorNotMyVbucket
		}
		helper.handleIncomingSeqno(seqno, metricKey)
		return nil
	default:
		return base.ErrorInvalidInput
	}
}

func getStatsKeyFromDocKeyAndSeqno(key string, seqno uint64) string {
	return fmt.Sprintf("%v-%v", key, seqno)
}

// metrics collector for DcpNozzle
type dcpCollector struct {
	id        string
	stats_mgr *StatisticsManager
	// key of outer map: component id
	// key of inner map: metric name
	// value of inner map: metric value
	component_map map[string]map[string]interface{}
}

func (dcp_collector *dcpCollector) UpdateCurrentVbSpecificMetrics(vbno uint16, valuesToApply base.VBCountMetricMap, currentRegistries map[string]metrics.Registry) error {
	// do nothing
	return nil
}

func (dcp_collector *dcpCollector) AddVbSpecificMetrics(vbno uint16, compiledMap base.VBCountMetricMap) error {
	// do nothing
	return nil
}

func (dcp_collector *dcpCollector) Mount(pipeline common.Pipeline, stats_mgr *StatisticsManager) error {
	dcp_collector.id = pipeline_utils.GetElementIdFromName(pipeline, base.DcpStatsCollector)
	dcp_collector.stats_mgr = stats_mgr
	dcp_collector.component_map = make(map[string]map[string]interface{})
	dcp_parts := pipeline.Sources()
	for _, dcp_part := range dcp_parts {
		registry := stats_mgr.getOrCreateRegistry(dcp_part.Id())
		docs_received_dcp := metrics.NewCounter()
		registry.Register(service_def.DOCS_RECEIVED_DCP_METRIC, docs_received_dcp)
		expiry_received_dcp := metrics.NewCounter()
		registry.Register(service_def.EXPIRY_RECEIVED_DCP_METRIC, expiry_received_dcp)
		deletion_received_dcp := metrics.NewCounter()
		registry.Register(service_def.DELETION_RECEIVED_DCP_METRIC, deletion_received_dcp)
		set_received_dcp := metrics.NewCounter()
		registry.Register(service_def.SET_RECEIVED_DCP_METRIC, set_received_dcp)
		dcp_dispatch_time := metrics.NewHistogram(metrics.NewUniformSample(stats_mgr.sample_size))
		registry.Register(service_def.DCP_DISPATCH_TIME_METRIC, dcp_dispatch_time)
		dcp_datach_len := metrics.NewCounter()
		registry.Register(service_def.DCP_DATACH_LEN, dcp_datach_len)
		systemEventsCounter := metrics.NewCounter()
		registry.Register(service_def.SYSTEM_EVENTS_RECEIVED_DCP_METRIC, systemEventsCounter)
		seqnoAdvCounter := metrics.NewCounter()
		registry.Register(service_def.SEQNO_ADV_RECEIVED_DCP_METRIC, seqnoAdvCounter)

		metric_map := make(map[string]interface{})
		metric_map[service_def.DOCS_RECEIVED_DCP_METRIC] = docs_received_dcp
		metric_map[service_def.EXPIRY_RECEIVED_DCP_METRIC] = expiry_received_dcp
		metric_map[service_def.DELETION_RECEIVED_DCP_METRIC] = deletion_received_dcp
		metric_map[service_def.SET_RECEIVED_DCP_METRIC] = set_received_dcp
		metric_map[service_def.DCP_DISPATCH_TIME_METRIC] = dcp_dispatch_time
		metric_map[service_def.DCP_DATACH_LEN] = dcp_datach_len
		metric_map[service_def.SYSTEM_EVENTS_RECEIVED_DCP_METRIC] = systemEventsCounter
		metric_map[service_def.SEQNO_ADV_RECEIVED_DCP_METRIC] = seqnoAdvCounter
		dcp_collector.component_map[dcp_part.Id()] = metric_map

		dcp_part.RegisterComponentEventListener(common.StatsUpdate, dcp_collector)
	}

	async_listener_map := pipeline_pkg.GetAllAsyncComponentEventListeners(pipeline)
	pipeline_utils.RegisterAsyncComponentEventHandler(async_listener_map, base.DataReceivedEventListener, dcp_collector)
	pipeline_utils.RegisterAsyncComponentEventHandler(async_listener_map, base.DataProcessedEventListener, dcp_collector)
	return nil
}

func (dcp_collector *dcpCollector) Id() string {
	return dcp_collector.id
}

func (dcp_collector *dcpCollector) OnEvent(event *common.Event) {
	dcp_collector.ProcessEvent(event)
}

func (dcp_collector *dcpCollector) HandleLatestThroughSeqnos(SeqnoMap map[uint16]uint64) {
	// Do nothing
}

func (dcp_collector *dcpCollector) ProcessEvent(event *common.Event) error {
	metric_map := dcp_collector.component_map[event.Component.Id()]
	switch event.EventType {
	case common.DataReceived:
		uprEvent := event.Data.(*mcc.UprEvent)
		metric_map[service_def.DOCS_RECEIVED_DCP_METRIC].(metrics.Counter).Inc(1)

		if uprEvent.Expiry != 0 {
			metric_map[service_def.EXPIRY_RECEIVED_DCP_METRIC].(metrics.Counter).Inc(1)
		}
		if uprEvent.Opcode == mc.UPR_DELETION {
			metric_map[service_def.DELETION_RECEIVED_DCP_METRIC].(metrics.Counter).Inc(1)
		} else if uprEvent.Opcode == mc.UPR_MUTATION {
			metric_map[service_def.SET_RECEIVED_DCP_METRIC].(metrics.Counter).Inc(1)
		} else if uprEvent.Opcode == mc.UPR_EXPIRATION {
			metric_map[service_def.EXPIRY_RECEIVED_DCP_METRIC].(metrics.Counter).Inc(1)
		} else if uprEvent.IsSystemEvent() {
			// ignore system events
		} else {
			dcp_collector.stats_mgr.logger.Warnf("Invalid opcode, %v, in DataReceived event from %v.", uprEvent.Opcode, event.Component.Id())
		}
	case common.DataProcessed:
		dcp_dispatch_time := event.OtherInfos.(float64)
		metric_map[service_def.DCP_DISPATCH_TIME_METRIC].(metrics.Histogram).Sample().Update(int64(dcp_dispatch_time))
	case common.StatsUpdate:
		dcp_datach_len := event.OtherInfos.(int)
		setCounter(metric_map[service_def.DCP_DATACH_LEN].(metrics.Counter), dcp_datach_len)
	case common.SystemEventReceived:
		metric_map[service_def.SYSTEM_EVENTS_RECEIVED_DCP_METRIC].(metrics.Counter).Inc(1)
	case common.SeqnoAdvReceived:
		metric_map[service_def.SEQNO_ADV_RECEIVED_DCP_METRIC].(metrics.Counter).Inc(1)
	}

	return nil
}

type vbBasedThroughSeqnoHelper struct {
	id string

	// These are sorted because DCP streams send seqno in an increasing order
	sortedSeqnoListMap map[string]*base.SortedSeqnoListWithLock

	responsibleKeys []string
}

func newVbBasedThroughSeqnoHelper(id string, keys []string) *vbBasedThroughSeqnoHelper {
	helper := &vbBasedThroughSeqnoHelper{
		id:                 id,
		sortedSeqnoListMap: make(map[string]*base.SortedSeqnoListWithLock),
		responsibleKeys:    keys,
	}

	for _, key := range keys {
		helper.sortedSeqnoListMap[key] = base.NewSortedSeqnoListWithLock()
	}
	return helper
}

func (vbh *vbBasedThroughSeqnoHelper) handleIncomingSeqno(seqno uint64, metricKey string) {
	vbh.sortedSeqnoListMap[metricKey].AppendSeqno(seqno)
}

func (vbh *vbBasedThroughSeqnoHelper) mergeWithMetrics(metricsMap map[string]interface{}, latestSeqno uint64) {
	for _, key := range vbh.responsibleKeys {
		sortedList := vbh.sortedSeqnoListMap[key].GetSortedSeqnoList(false)
		// Figure out how many count are to be committed to metrics
		i := sort.Search(len(sortedList), func(i int) bool {
			return sortedList[i] > latestSeqno
		})
		metricsMap[key].(metrics.Counter).Inc(int64(i))

		// Clear incremented count from staging areas
		vbh.sortedSeqnoListMap[key].TruncateSeqnos(latestSeqno)
	}

}

type VbBasedMetricHelper struct {
	// For vb-based metric across all DCP nozzles
	vbBasedMetric map[uint16]map[string]interface{}

	// A map of vb-> partIDs
	partVbsIdMap map[uint16]string

	// Helpers to ensure that stored filter metrics are correct
	vbBasedHelper map[uint16]*vbBasedThroughSeqnoHelper

	responsibleKeys []string
}

func (h *VbBasedMetricHelper) Register(id string, vbs []uint16, partId string, keys []string) {
	h.responsibleKeys = keys
	for _, i := range vbs {
		h.partVbsIdMap[i] = partId
		metricsMap := make(map[string]interface{})
		h.vbBasedHelper[i] = newVbBasedThroughSeqnoHelper(id, h.responsibleKeys)
		for _, k := range keys {
			metricsMap[k] = metrics.NewCounter()
			metrics.Register(fmt.Sprintf("%v:%v", partId, i), metricsMap[k])
		}
		h.vbBasedMetric[i] = metricsMap
	}
}

func (h *VbBasedMetricHelper) HandleLatestThroughSeqnos(seqnoMap map[uint16]uint64) {
	var waitGrp sync.WaitGroup

	for vb, _ := range h.vbBasedMetric {
		waitGrp.Add(1)
		go h.handleLatestThroughSeqnoForVb(vb, seqnoMap[vb], &waitGrp)
	}

	waitGrp.Wait()

}

func (h *VbBasedMetricHelper) handleLatestThroughSeqnoForVb(vb uint16, latestSeqno uint64, waitGrp *sync.WaitGroup) {
	defer waitGrp.Done()
	metricsMap, ok := h.vbBasedMetric[vb]
	if !ok {
		return
	}
	vbHelper, ok := h.vbBasedHelper[vb]
	if !ok {
		return
	}

	vbHelper.mergeWithMetrics(metricsMap, latestSeqno)
}

func (h *VbBasedMetricHelper) AddVbSpecificMetrics(vbno uint16, compiledMap base.VBCountMetricMap) error {
	vbBasedMetric, ok := h.vbBasedMetric[vbno]
	if !ok {
		return base.ErrorNotMyVbucket
	}

	if compiledMap == nil {
		return fmt.Errorf("CompiledMap being passed into AddVbSpecificMetrics is nil")
	}

	for _, k := range h.responsibleKeys {
		registry, ok := vbBasedMetric[k]
		if !ok {
			continue
		}
		counter := registry.(metrics.Counter)
		compiledMap[k] = counter.Count()
	}
	return nil
}

func (h *VbBasedMetricHelper) UpdateCurrentVbSpecificMetrics(vb uint16, valuesToApply base.VBCountMetricMap, currentRegistries map[string]metrics.Registry) error {
	vbBasedMetric, ok := h.vbBasedMetric[vb]
	if !ok {
		return base.ErrorNotMyVbucket
	}

	// First find the part responsible for this vb
	partId, found := h.partVbsIdMap[vb]
	if !found {
		return base.ErrorNotMyVbucket
	}

	registries := currentRegistries[partId]
	if registries == nil {
		return fmt.Errorf("Unable to find registry for %v", partId)
	}

	// Keys is the "keys" being read, i.e. the filtered_cnt, etc from Checkpoint
	for k, v := range valuesToApply {
		var isResponsibleForThisKey bool
		for _, keyToCheck := range h.responsibleKeys {
			if keyToCheck == k {
				isResponsibleForThisKey = true
				break
			}
		}
		if !isResponsibleForThisKey {
			continue
		}

		// Increment vb-related counters
		registry, ok := vbBasedMetric[k]
		if !ok {
			return base.ErrorInvalidInput
		}
		counter := registry.(metrics.Counter)
		currentVal := counter.Count()
		// Difference here is to address scenario when rollback occurs
		// If rollback happens, then the difference is new - old
		// Either increment or decrement the current count in both vb specific and stats_mgr.registries
		difference := v - currentVal
		counter.Inc(difference)

		// Increment stats independent of VB that need this
		metricsIface := registries.Get(k)
		if metricsIface == nil {
			return fmt.Errorf("%v Unable to get metric\n", partId)
		}
		counter, ok = metricsIface.(metrics.Counter)
		if !ok || counter == nil {
			return fmt.Errorf("%v Unable to get metric counter\n", partId)
		}
		counter.Inc(difference)
	}

	return nil
}

func NewVbBasedMetricHelper() *VbBasedMetricHelper {
	helper := &VbBasedMetricHelper{}
	helper.vbBasedMetric = make(map[uint16]map[string]interface{})
	helper.partVbsIdMap = make(map[uint16]string)
	helper.vbBasedHelper = make(map[uint16]*vbBasedThroughSeqnoHelper)

	return helper
}

// metrics collector for Router
type routerCollector struct {
	id        string
	stats_mgr *StatisticsManager

	// key of outer map: component id
	// key of inner map: metric name
	// value of inner map: metric value
	component_map map[string]map[string]interface{}

	vbMetricHelper *VbBasedMetricHelper
}

func (r_collector *routerCollector) UpdateCurrentVbSpecificMetrics(vbno uint16, valuesToApply base.VBCountMetricMap, currentRegistries map[string]metrics.Registry) error {
	return r_collector.vbMetricHelper.UpdateCurrentVbSpecificMetrics(vbno, valuesToApply, currentRegistries)
}

func (r_collector *routerCollector) AddVbSpecificMetrics(vbno uint16, compiledMap base.VBCountMetricMap) error {
	return r_collector.vbMetricHelper.AddVbSpecificMetrics(vbno, compiledMap)
}

func (r_collector *routerCollector) Mount(pipeline common.Pipeline, stats_mgr *StatisticsManager) error {
	r_collector.id = pipeline_utils.GetElementIdFromName(pipeline, base.RouterStatsCollector)
	r_collector.stats_mgr = stats_mgr
	r_collector.component_map = make(map[string]map[string]interface{})
	r_collector.vbMetricHelper = NewVbBasedMetricHelper()
	dcp_parts := pipeline.Sources()
	for _, dcp_part := range dcp_parts {
		//get connector
		conn := dcp_part.Connector()
		registry_router := stats_mgr.getOrCreateRegistry(conn.Id())
		docs_filtered := metrics.NewCounter()
		registry_router.Register(service_def.DOCS_FILTERED_METRIC, docs_filtered)
		docs_unable_to_filter := metrics.NewCounter()
		registry_router.Register(service_def.DOCS_UNABLE_TO_FILTER_METRIC, docs_unable_to_filter)
		expiry_filtered := metrics.NewCounter()
		registry_router.Register(service_def.EXPIRY_FILTERED_METRIC, expiry_filtered)
		deletion_filtered := metrics.NewCounter()
		registry_router.Register(service_def.DELETION_FILTERED_METRIC, deletion_filtered)
		set_filtered := metrics.NewCounter()
		registry_router.Register(service_def.SET_FILTERED_METRIC, set_filtered)
		binaryFiltered := metrics.NewCounter()
		registry_router.Register(service_def.BINARY_FILTERED_METRIC, binaryFiltered)
		dp_failed := metrics.NewCounter()
		registry_router.Register(service_def.DP_GET_FAIL_METRIC, dp_failed)
		throughput_throttle_latency := metrics.NewHistogram(metrics.NewUniformSample(stats_mgr.sample_size))
		registry_router.Register(service_def.THROUGHPUT_THROTTLE_LATENCY_METRIC, throughput_throttle_latency)
		expiry_stripped := metrics.NewCounter()
		registry_router.Register(service_def.EXPIRY_STRIPPED_METRIC, expiry_stripped)
		docs_cloned := metrics.NewCounter()
		registry_router.Register(service_def.DOCS_CLONED_METRIC, docs_cloned)
		deletion_cloned := metrics.NewCounter()
		registry_router.Register(service_def.DELETION_CLONED_METRIC, deletion_cloned)
		atr_docs_filtered := metrics.NewCounter()
		registry_router.Register(service_def.DOCS_FILTERED_TXN_ATR_METRIC, atr_docs_filtered)
		client_txn_docs_filtered := metrics.NewCounter()
		registry_router.Register(service_def.DOCS_FILTERED_CLIENT_TXN_METRIC, client_txn_docs_filtered)
		docs_filtered_on_txn_xattr := metrics.NewCounter()
		registry_router.Register(service_def.DOCS_FILTERED_TXN_XATTR_METRIC, docs_filtered_on_txn_xattr)
		docs_filtered_on_user_defined_filter := metrics.NewCounter()
		registry_router.Register(service_def.DOCS_FILTERED_USER_DEFINED_METRIC, docs_filtered_on_user_defined_filter)
		mobile_docs_filtered := metrics.NewCounter()
		registry_router.Register(service_def.DOCS_FILTERED_MOBILE_METRIC, mobile_docs_filtered)
		casPoisoned := metrics.NewCounter()
		registry_router.Register(service_def.DOCS_FILTERED_CAS_POISONING_METRIC, casPoisoned)

		metric_map := make(map[string]interface{})
		metric_map[service_def.DOCS_FILTERED_METRIC] = docs_filtered
		metric_map[service_def.DOCS_UNABLE_TO_FILTER_METRIC] = docs_unable_to_filter
		metric_map[service_def.EXPIRY_FILTERED_METRIC] = expiry_filtered
		metric_map[service_def.DELETION_FILTERED_METRIC] = deletion_filtered
		metric_map[service_def.SET_FILTERED_METRIC] = set_filtered
		metric_map[service_def.BINARY_FILTERED_METRIC] = binaryFiltered
		metric_map[service_def.DP_GET_FAIL_METRIC] = dp_failed
		metric_map[service_def.THROUGHPUT_THROTTLE_LATENCY_METRIC] = throughput_throttle_latency
		metric_map[service_def.EXPIRY_STRIPPED_METRIC] = expiry_stripped
		metric_map[service_def.DOCS_CLONED_METRIC] = docs_cloned
		metric_map[service_def.DELETION_CLONED_METRIC] = deletion_cloned
		metric_map[service_def.DOCS_FILTERED_TXN_ATR_METRIC] = atr_docs_filtered
		metric_map[service_def.DOCS_FILTERED_CLIENT_TXN_METRIC] = client_txn_docs_filtered
		metric_map[service_def.DOCS_FILTERED_TXN_XATTR_METRIC] = docs_filtered_on_txn_xattr
		metric_map[service_def.DOCS_FILTERED_USER_DEFINED_METRIC] = docs_filtered_on_user_defined_filter
		metric_map[service_def.DOCS_FILTERED_MOBILE_METRIC] = mobile_docs_filtered
		metric_map[service_def.DOCS_FILTERED_CAS_POISONING_METRIC] = casPoisoned

		// VB specific stats
		listOfVbs := dcp_part.ResponsibleVBs()
		r_collector.vbMetricHelper.Register(r_collector.Id(), listOfVbs, conn.Id(), RouterVBMetricKeys)
		r_collector.component_map[conn.Id()] = metric_map
	}

	async_listener_map := pipeline_pkg.GetAllAsyncComponentEventListeners(pipeline)
	pipeline_utils.RegisterAsyncComponentEventHandler(async_listener_map, base.DataFilteredEventListener, r_collector)
	pipeline_utils.RegisterAsyncComponentEventHandler(async_listener_map, base.DataThroughputThrottledEventListener, r_collector)
	pipeline_utils.RegisterAsyncComponentEventHandler(async_listener_map, base.DataClonedEventListener, r_collector)

	return nil
}

func (r_collector *routerCollector) Id() string {
	return r_collector.id
}

func (r_collector *routerCollector) handleVBEvent(event *common.Event, metricKey string) error {
	switch metricKey {
	case service_def.DOCS_FILTERED_METRIC:
		fallthrough
	case service_def.EXPIRY_FILTERED_METRIC:
		fallthrough
	case service_def.DELETION_FILTERED_METRIC:
		fallthrough
	case service_def.SET_FILTERED_METRIC:
		fallthrough
	case service_def.EXPIRY_STRIPPED_METRIC:
		fallthrough
	case service_def.BINARY_FILTERED_METRIC:
		fallthrough
	case service_def.DOCS_FILTERED_TXN_ATR_METRIC:
		fallthrough
	case service_def.DOCS_FILTERED_CLIENT_TXN_METRIC:
		fallthrough
	case service_def.DOCS_FILTERED_TXN_XATTR_METRIC:
		fallthrough
	case service_def.DOCS_FILTERED_USER_DEFINED_METRIC:
		fallthrough
	case service_def.DOCS_FILTERED_MOBILE_METRIC:
		fallthrough
	case service_def.DOCS_UNABLE_TO_FILTER_METRIC:
		uprEvent := event.Data.(*mcc.UprEvent)
		vbucket := uprEvent.VBucket
		seqno := uprEvent.Seqno
		helper, ok := r_collector.vbMetricHelper.vbBasedHelper[vbucket]
		if !ok {
			return base.ErrorNotMyVbucket
		}
		helper.handleIncomingSeqno(seqno, metricKey)
		return nil
	case service_def.DOCS_FILTERED_CAS_POISONING_METRIC:
		mcReq := event.Data.(*base.WrappedMCRequest)
		vbucket := mcReq.Req.VBucket
		seqno := mcReq.Seqno
		helper, ok := r_collector.vbMetricHelper.vbBasedHelper[vbucket]
		if !ok {
			return base.ErrorNotMyVbucket
		}
		helper.handleIncomingSeqno(seqno, metricKey)
		return nil
	default:
		return base.ErrorInvalidInput
	}
}

func (r_collector *routerCollector) OnEvent(event *common.Event) {
	r_collector.ProcessEvent(event)
}

func (r_collector *routerCollector) HandleLatestThroughSeqnos(SeqnoMap map[uint16]uint64) {
	r_collector.vbMetricHelper.HandleLatestThroughSeqnos(SeqnoMap)
}

func (r_collector *routerCollector) ProcessEvent(event *common.Event) error {
	metric_map := r_collector.component_map[event.Component.Id()]
	var err error
	switch event.EventType {
	case common.DataFiltered:
		uprEvent := event.Data.(*mcc.UprEvent)
		metric_map[service_def.DOCS_FILTERED_METRIC].(metrics.Counter).Inc(1)

		if uprEvent.Expiry != 0 {
			metric_map[service_def.EXPIRY_FILTERED_METRIC].(metrics.Counter).Inc(1)
		}

		dataTypeIsJson := uprEvent.DataType&mcc.JSONDataType > 0
		isTombstone := (uprEvent.Opcode == mc.UPR_DELETION || uprEvent.Opcode == mc.UPR_EXPIRATION)
		if !dataTypeIsJson && !isTombstone {
			metric_map[service_def.BINARY_FILTERED_METRIC].(metrics.Counter).Inc(1)
			err = r_collector.handleVBEvent(event, service_def.BINARY_FILTERED_METRIC)
		}
		if uprEvent.Opcode == mc.UPR_DELETION {
			metric_map[service_def.DELETION_FILTERED_METRIC].(metrics.Counter).Inc(1)
			err = r_collector.handleVBEvent(event, service_def.DELETION_FILTERED_METRIC)
		} else if uprEvent.Opcode == mc.UPR_MUTATION {
			metric_map[service_def.SET_FILTERED_METRIC].(metrics.Counter).Inc(1)
			err = r_collector.handleVBEvent(event, service_def.SET_FILTERED_METRIC)
		} else if uprEvent.Opcode == mc.UPR_EXPIRATION {
			metric_map[service_def.EXPIRY_FILTERED_METRIC].(metrics.Counter).Inc(1)
			err = r_collector.handleVBEvent(event, service_def.EXPIRY_FILTERED_METRIC)
		} else {
			r_collector.stats_mgr.logger.Warnf("Invalid opcode, %v, in DataFiltered event from %v.", uprEvent.Opcode, event.Component.Id())
		}

		if err != nil {
			return err
		}

		// Handle VB specific tasks
		err = r_collector.handleVBEvent(event, service_def.DOCS_FILTERED_METRIC)
		if err != nil {
			return err
		}

		additionalEventInfoIfc := event.OtherInfos
		if additionalEventInfoIfc == nil {
			r_collector.stats_mgr.logger.Warnf("OtherInfos is nil for the DataFiltered event from %v.", event.Component.Id())
			return err
		}
		additionalEventInfo, ok := additionalEventInfoIfc.(parts.DataFilteredAdditional)
		if !ok {
			r_collector.stats_mgr.logger.Warnf("OtherInfos is not of type DataFilteredAdditional for the DataFiltered event from %v.", event.Component.Id())
			return err
		}
		filteringStatus := additionalEventInfo.FilteringStatus
		var filteredMetricKey string

		switch filteringStatus {
		case filter.FilteredOnATRDocument:
			metric_map[service_def.DOCS_FILTERED_TXN_ATR_METRIC].(metrics.Counter).Inc(1)
			filteredMetricKey = service_def.DOCS_FILTERED_TXN_ATR_METRIC
		case filter.FilteredOnTxnClientRecord:
			metric_map[service_def.DOCS_FILTERED_CLIENT_TXN_METRIC].(metrics.Counter).Inc(1)
			filteredMetricKey = service_def.DOCS_FILTERED_CLIENT_TXN_METRIC
		case filter.FilteredOnTxnsXattr:
			metric_map[service_def.DOCS_FILTERED_TXN_XATTR_METRIC].(metrics.Counter).Inc(1)
			filteredMetricKey = service_def.DOCS_FILTERED_TXN_XATTR_METRIC
		case filter.FilteredOnUserDefinedFilter:
			metric_map[service_def.DOCS_FILTERED_USER_DEFINED_METRIC].(metrics.Counter).Inc(1)
			filteredMetricKey = service_def.DOCS_FILTERED_USER_DEFINED_METRIC
		case filter.FilteredOnMobileRecord:
			metric_map[service_def.DOCS_FILTERED_MOBILE_METRIC].(metrics.Counter).Inc(1)
			filteredMetricKey = service_def.DOCS_FILTERED_MOBILE_METRIC
		default:
			return err
		}

		err = r_collector.handleVBEvent(event, filteredMetricKey)

	case common.DataUnableToFilter:
		metric_map[service_def.DOCS_UNABLE_TO_FILTER_METRIC].(metrics.Counter).Inc(1)
		// Handle VB specific tasks
		err = r_collector.handleVBEvent(event, service_def.DOCS_UNABLE_TO_FILTER_METRIC)
	case common.DataPoolGetFail:
		metric_map[service_def.DP_GET_FAIL_METRIC].(metrics.Counter).Inc(event.Data.(int64))
	case common.DataThroughputThrottled:
		throughput_throttle_latency := event.OtherInfos.(time.Duration)
		metric_map[service_def.THROUGHPUT_THROTTLE_LATENCY_METRIC].(metrics.Histogram).Sample().Update(throughput_throttle_latency.Nanoseconds() / 1000000)
	case common.ExpiryFieldStripped:
		metric_map[service_def.EXPIRY_STRIPPED_METRIC].(metrics.Counter).Inc(1)
		err = r_collector.handleVBEvent(event, service_def.EXPIRY_STRIPPED_METRIC)
	case common.DataCloned:
		data := event.Data.([]interface{})
		totalCount := data[2].(int)
		// TotalCount includes the original request + cloned count
		metric_map[service_def.DOCS_CLONED_METRIC].(metrics.Counter).Inc(int64(totalCount - 1))
		if isDelete := data[3].(bool); isDelete {
			metric_map[service_def.DELETION_CLONED_METRIC].(metrics.Counter).Inc(int64(totalCount - 1))
		}
	case common.DataNotReplicated:
		if len(event.DerivedData) > 0 {
			casPoisonErrChk := event.DerivedData[0].(error)
			if casPoisonErrChk == base.ErrorCasPoisoningDetected {
				metric_map[service_def.DOCS_FILTERED_CAS_POISONING_METRIC].(metrics.Counter).Inc(1)
				err = r_collector.handleVBEvent(event, service_def.DOCS_FILTERED_CAS_POISONING_METRIC)
			}
		}
	}

	return err
}

// metrics collector for checkpointmanager
type checkpointMgrCollector struct {
	stats_mgr *StatisticsManager
}

func (ckpt_collector *checkpointMgrCollector) UpdateCurrentVbSpecificMetrics(vbno uint16, valuesToApply base.VBCountMetricMap, currentRegistries map[string]metrics.Registry) error {
	// do nothing
	return nil
}

func (ckpt_collector *checkpointMgrCollector) AddVbSpecificMetrics(vbno uint16, compiledMap base.VBCountMetricMap) error {
	// do nothing
	return nil
}

func (ckpt_collector *checkpointMgrCollector) Mount(pipeline common.Pipeline, stats_mgr *StatisticsManager) error {
	ckpt_collector.stats_mgr = stats_mgr
	ckptmgr := pipeline.RuntimeContext().Service(base.CHECKPOINT_MGR_SVC)
	if ckptmgr == nil {
		return errors.New("Mount failed for checkpointMgrCollector. CheckpointMgr has to exist")
	}

	err := ckptmgr.(common.Component).RegisterComponentEventListener(common.ErrorEncountered, ckpt_collector)
	if err != nil {
		return err
	}
	err = ckptmgr.(common.Component).RegisterComponentEventListener(common.CheckpointDone, ckpt_collector)
	if err != nil {
		return err
	}

	err = ckptmgr.(common.Component).RegisterComponentEventListener(common.CheckpointDoneForVB, ckpt_collector)
	if err != nil {
		return err
	}
	ckpt_collector.initRegistry()
	return nil
}

func (ckpt_collector *checkpointMgrCollector) initRegistry() {
	registry_ckpt := ckpt_collector.stats_mgr.getOrCreateRegistry("CkptMgr")
	registry_ckpt.Register(service_def.TIME_COMMITING_METRIC, metrics.NewHistogram(metrics.NewUniformSample(ckpt_collector.stats_mgr.sample_size)))
	registry_ckpt.Register(service_def.NUM_CHECKPOINTS_METRIC, metrics.NewCounter())
	registry_ckpt.Register(service_def.NUM_FAILEDCKPTS_METRIC, metrics.NewCounter())

}

func (ckpt_collector *checkpointMgrCollector) HandleLatestThroughSeqnos(SeqnoMap map[uint16]uint64) {
	// Do nothing
}

func (ckpt_collector *checkpointMgrCollector) OnEvent(event *common.Event) {
	registry := ckpt_collector.stats_mgr.registries["CkptMgr"]
	if event.EventType == common.ErrorEncountered {
		registry.Get(service_def.NUM_FAILEDCKPTS_METRIC).(metrics.Counter).Inc(1)

	} else if event.EventType == common.CheckpointDoneForVB {
		vbno := event.OtherInfos.(uint16)
		ckpt_record := event.Data.(metadata.CheckpointRecord)
		ckpt_collector.stats_mgr.checkpointed_seqnos[vbno].SetSeqno(ckpt_record.Seqno)

	} else if event.EventType == common.CheckpointDone {
		time_commit := event.OtherInfos.(time.Duration).Seconds() * 1000
		registry.Get(service_def.NUM_CHECKPOINTS_METRIC).(metrics.Counter).Inc(1)
		registry.Get(service_def.TIME_COMMITING_METRIC).(metrics.Histogram).Sample().Update(int64(time_commit))
	}
}

func setCounter(counter metrics.Counter, count int) {
	counter.Clear()
	counter.Inc(int64(count))
}

func (stats_mgr *StatisticsManager) getReplicationStatus() (pipeline_pkg.ReplicationStatusIface, error) {
	topic := stats_mgr.pipeline.Topic()
	return stats_mgr.replStatusGetter(topic)
}

const updateStatsId = "ReplicationMgrUpdateStats"

var updateStatsInstanceCnt uint32

const maxConcurrentUpdateStatsInstances = 500

// Compile a instance ID to be used each time UpdateStats is called to prevent subscriber ID collision when called
// concurrently. Wraps around once max has been hit
func getUpdateStatsInstanceId() string {
	instanceCnt := atomic.AddUint32(&updateStatsInstanceCnt, 1) % maxConcurrentUpdateStatsInstances
	return fmt.Sprintf("%v_%v", updateStatsId, instanceCnt)
}

func UpdateStats(checkpoints_svc service_def.CheckpointsService, logger *log.CommonLogger, remoteClusterSvc service_def.RemoteClusterSvc, backfillReplSvc service_def.BackfillReplSvc, bucketTopologySvc service_def.BucketTopologySvc, repStatusMapGetter func() map[string]pipeline_pkg.ReplicationStatusIface) {
	logger.Debugf("updateStats for paused replications")

	subscriberId := getUpdateStatsInstanceId()

	for repl_id, repl_status := range repStatusMapGetter() {
		// For a paused replication,
		// Main pipeline stats are cumulative of both main and backfill pipeline
		// Backfill pipeline stats are just backfill stats
		overview_stats := repl_status.GetOverviewStats(common.MainPipeline)
		spec := repl_status.Spec()
		if spec == nil {
			continue
		}
		ref, err := remoteClusterSvc.RemoteClusterByUuid(spec.TargetClusterUUID, false /*refresh*/)
		if err != nil {
			logger.Errorf("Error retrieving target cluster: err %v", err)
			continue
		}

		// Check to ensure remote cluster collection capability is aligned with the current memcached connections
		remoteClusterCapability, err := remoteClusterSvc.GetCapability(ref)
		if err != nil {
			logger.Errorf("Error retrieving capability for remote cluster %v - err: %v", ref.Id(), err)
			continue
		}

		var highSeqnosMaps base.HighSeqnosMapType
		var sourceVBMap map[string][]uint16
		var recycleFunc func()
		if remoteClusterCapability.HasCollectionSupport() {
			highSeqnoFeed, _, err := bucketTopologySvc.SubscribeToLocalBucketHighSeqnosFeed(spec, subscriberId, base.ReplSpecCheckInterval)
			if err != nil {
				logger.Errorf("Error subscribing to highSeqnosFeed %v - err: %v", ref.Id(), err)
				continue
			}

			highSeqnoFeedNotification := <-highSeqnoFeed
			highSeqnosMaps = highSeqnoFeedNotification.GetHighSeqnosMap()
			sourceVBMap = highSeqnoFeedNotification.GetSourceVBMapRO()
			recycleFunc = highSeqnoFeedNotification.Recycle

			err = bucketTopologySvc.UnSubscribeToLocalBucketHighSeqnosFeed(spec, subscriberId)
			if err != nil {
				logger.Errorf("Error unsubscribing to highSeqnosFeed %v - err: %v", ref.Id(), err)
			}
		} else {
			highSeqnoFeed, _, err := bucketTopologySvc.SubscribeToLocalBucketHighSeqnosLegacyFeed(spec, subscriberId, base.ReplSpecCheckInterval)
			if err != nil {
				logger.Errorf("Error subscribing to highSeqnosLegacyFeed %v - err: %v", ref.Id(), err)
				continue
			}

			highSeqnoFeedNotification := <-highSeqnoFeed
			highSeqnosMaps = highSeqnoFeedNotification.GetHighSeqnosMap()
			sourceVBMap = highSeqnoFeedNotification.GetSourceVBMapRO()
			recycleFunc = highSeqnoFeedNotification.Recycle

			err = bucketTopologySvc.UnSubscribeToLocalBucketHighSeqnosLegacyFeed(spec, subscriberId)
			if err != nil {
				logger.Errorf("Error unsubscribing to highSeqnosLegacyFeed %v - err: %v", ref.Id(), err)
			}
		}
		highSeqnoAndSourceVBGetter := func() (base.HighSeqnosMapType, base.KvVBMapType, func()) {
			return highSeqnosMaps, sourceVBMap, recycleFunc
		}

		// Check to see if backfill replication exists
		backfillSpec, err := backfillReplSvc.BackfillReplSpec(spec.Id)
		if err != nil {
			// no backfill spec to use
			backfillSpec = nil
		}

		if overview_stats == nil {
			// overview stats may be nil the first time GetStats is called on a paused replication that has never been run in the current goxdcr session
			// or it may be nil when the underying replication is not paused but has not completed startup process
			// construct it
			err := constructStatsForReplication(repl_status, spec, checkpoints_svc, logger, backfillSpec, highSeqnoAndSourceVBGetter)
			if err != nil {
				logger.Errorf("Error constructing stats for paused replication %v. err=%v", repl_id, err)
				continue
			}
		} else {
			if repl_status.RuntimeStatus(true) != pipeline_pkg.Replicating {
				err := updateStatsForReplication(repl_status, overview_stats, checkpoints_svc, logger, backfillSpec, highSeqnoAndSourceVBGetter)
				if err != nil {
					logger.Errorf("Error updating stats for paused replication %v. err=%v", repl_id, err)
					continue
				}
			}
			// At this point, the pipeline is either paused, or have errors
			if len(repl_status.Errors()) > 0 {
				errorStateVar := new(expvar.Int)
				errorStateVar.Set(int64(base.PipelineStatusError))
				overview_stats.Set(service_def.PIPELINE_STATUS, errorStateVar)
			} else if repl_status.RuntimeStatus(true) == pipeline_pkg.Paused {
				pausedVar := new(expvar.Int)
				pausedVar.Set(int64(base.PipelineStatusPaused))
				overview_stats.Set(service_def.PIPELINE_STATUS, pausedVar)
			}
		}
	}
}

func initEmptyConnections(bucket_kv_mem_clients map[string]map[string]mcc.ClientIface, srcBucketName string) map[string]mcc.ClientIface {
	kv_mem_clients := make(map[string]mcc.ClientIface)
	bucket_kv_mem_clients[srcBucketName] = kv_mem_clients
	return kv_mem_clients
}

// Returns kv_mem_clients for convenience
func checkMccConnectionsCapability(bucket_kv_mem_clients map[string]map[string]mcc.ClientIface, srcBucketName string,
	remoteClusterCapability metadata.Capability) map[string]mcc.ClientIface {
	var connsShouldHaveCollectionsEnabled bool
	if !remoteClusterCapability.HasCollectionSupport() {
		// When remote cluster does not support collection, it means legacy replication mode
		// Legacy replication means only replicate default collections and only show stats with
		// default collection data - don't do the regular high vb stats
		connsShouldHaveCollectionsEnabled = true
	} else {
		// When remote cluster support collections, this means that we should use the traditional stats
	}

	// Check the connections
	kv_mem_clients := bucket_kv_mem_clients[srcBucketName]
	var needToRestartConnections bool
	for _, client := range kv_mem_clients {
		if connsShouldHaveCollectionsEnabled && !client.CollectionEnabled() {
			needToRestartConnections = true
			break
		} else if !connsShouldHaveCollectionsEnabled && client.CollectionEnabled() {
			needToRestartConnections = true
			break
		}
	}

	if needToRestartConnections {
		for _, client := range kv_mem_clients {
			// ignore all errors
			client.Close()
		}
		kv_mem_clients = initEmptyConnections(bucket_kv_mem_clients, srcBucketName)
	}

	return kv_mem_clients
}

func calculateBackfillStatsForPausedRepl(specId string, curVbListRo []uint16, backfillSpec *metadata.BackfillReplicationSpec, checkpoints_svc service_def.CheckpointsService, logger *log.CommonLogger) (int64, int64, int64, bool) {
	if backfillSpec == nil {
		return 0, 0, 0, false
	}
	var backfillStatsCalculated, backfillCkptsExist bool
	var backfillTotalChanges, backfillChangesLeft, backfillDocsProcessed int64
	var backfillDocsProcessedUint uint64
	var err error
	backfillTotalChanges, backfillCkptsExist, err = calculateTotalPausedBackfillChanges(backfillSpec, checkpoints_svc, curVbListRo, true)
	if err != nil {
		logger.Warnf("Unable to get backfill total changes, err=%v", err)
		return 0, 0, 0, false
	}

	backfillStatsCalculated = true

	if !backfillCkptsExist {
		logger.Warnf("No backfill checkpoints, skipping backfill docs processed")
		return backfillTotalChanges, 0, backfillTotalChanges, backfillStatsCalculated
	}

	backfillTopic := common.ComposeFullTopic(specId, common.BackfillPipeline)
	backfillDocsProcessedUint, err = getDocsProcessedForReplication(backfillTopic, curVbListRo, checkpoints_svc, logger)
	if err != nil {
		logger.Warnf("Unable to get backfill docs processed, err=%v", err)
		return backfillTotalChanges, 0, backfillTotalChanges, backfillStatsCalculated
	}

	backfillDocsProcessed = int64(backfillDocsProcessedUint)
	backfillChangesLeft = backfillTotalChanges - backfillDocsProcessed
	if backfillChangesLeft < 0 {
		// shouldn't reach here. This is just a safety check
		backfillChangesLeft = 0
		backfillDocsProcessed = 0
		backfillTotalChanges = 0
		logger.Warnf("Backfill changes_left went negative for replication %v: backfillTotalChanges=%v, backfillDocsProcessed=%v, backfillChangesLeft=%v",
			specId, backfillTotalChanges, backfillDocsProcessed, backfillChangesLeft)
	}
	return backfillTotalChanges, backfillDocsProcessed, backfillChangesLeft, backfillStatsCalculated
}

func calculateMainStatsForPausedRepl(specId string, curVbListRo []uint16, checkpoints_svc service_def.CheckpointsService, highSeqnosMap base.HighSeqnosMapType, curKvVbMapRo base.KvVBMapType,
	backfillTotalChanges, backfillDocsProcessed, backfillChangesLeft int64, backfillStatsCalculated bool, needToCalcDocsProcessed bool, docsProcessedOld int64, logger *log.CommonLogger) (int64, int64, int64, error) {

	var changes_left, docs_processed, total_changes int64
	var err error

	// only calculate docsProcessed if the caller asks for it, or else just use the old value
	if needToCalcDocsProcessed {
		docs_processed_uint, err := getDocsProcessedForReplication(specId, curVbListRo, checkpoints_svc, logger)
		if err != nil {
			return 0, 0, 0, err
		}
		docs_processed = int64(docs_processed_uint)
	} else {
		docs_processed = docsProcessedOld
	}

	total_changes, _, err = calculateTotalChanges(logger, highSeqnosMap, curKvVbMapRo)
	if err != nil {
		return 0, 0, 0, err
	}

	// for a paused replication, main pipeline overview stats stores the cumulative of both main and backfill pipeline
	if backfillStatsCalculated {
		docs_processed += backfillDocsProcessed
		total_changes += backfillTotalChanges
	}

	changes_left = total_changes - docs_processed
	if changes_left < 0 {
		// shouldn't reach here. This is just a safety check
		changes_left = 0
		docs_processed = 0
		total_changes = 0
		logger.Warnf("Main changes_left went negative for replication %v: docs_processed=%v, total_changes=%v, changes_left=%v, backfillTotalChanges=%v, backfillDocsProcessed=%v, backfillChangesLeft=%v",
			specId, docs_processed, total_changes, changes_left, backfillTotalChanges, backfillDocsProcessed, backfillChangesLeft)
	}

	return total_changes, docs_processed, changes_left, nil
}

func createOrUpdateOverviewStats(repl_status pipeline_pkg.ReplicationStatusIface, docs_processed, changes_left int64, pipelineType common.PipelineType) {
	overviewStats := repl_status.GetOverviewStats(pipelineType)
	if overviewStats == nil {
		// Create new stats
		overviewStats = new(expvar.Map).Init()
		overviewStats.Add(service_def.DOCS_PROCESSED_METRIC, docs_processed)
		overviewStats.Add(service_def.CHANGES_LEFT_METRIC, changes_left)
		for _, statsToInitialize := range StatsToInitializeForPausedReplications {
			overviewStats.Add(statsToInitialize, 0)
		}
		repl_status.SetOverviewStats(overviewStats, pipelineType)
	} else {
		// Update existing stats
		changesLeftVar := new(expvar.Int)
		changesLeftVar.Set(changes_left)
		docsProcessedVar := new(expvar.Int)
		docsProcessedVar.Set(docs_processed)
		overviewStats.Set(service_def.DOCS_PROCESSED_METRIC, docsProcessedVar)
		overviewStats.Set(service_def.CHANGES_LEFT_METRIC, changesLeftVar)
	}
}

// compute and set changes_left and docs_processed stats. set other stats to 0
func constructStatsForReplication(repl_status pipeline_pkg.ReplicationStatusIface, spec *metadata.ReplicationSpecification, checkpoints_svc service_def.CheckpointsService, logger *log.CommonLogger,
	backfillSpec *metadata.BackfillReplicationSpec, highSeqnosMapGetter func() (base.HighSeqnosMapType, base.KvVBMapType, func())) error {
	highSeqnosMap, curKvVbMapRo, doneFunc := highSeqnosMapGetter()
	defer doneFunc()

	curVbListRo := base.GetVbListFromKvVbMap(curKvVbMapRo)
	base.SortUint16List(curVbListRo)

	backfillTotalChanges, backfillDocsProcessed, backfillChangesLeft, backfillStatsCalculated := calculateBackfillStatsForPausedRepl(spec.Id, curVbListRo, backfillSpec, checkpoints_svc, logger)

	total_changes, docs_processed, changes_left, err := calculateMainStatsForPausedRepl(spec.Id, curVbListRo, checkpoints_svc, highSeqnosMap, curKvVbMapRo, backfillTotalChanges, backfillDocsProcessed,
		backfillChangesLeft, backfillStatsCalculated, true, 0, logger)
	if err != nil {
		return err
	}

	if backfillStatsCalculated {
		logger.Infof("Calculating stats for never run replication %v. kv_vb_map=%v, total_docs=%v (total_backfill_docs=%v), docs_processed=%v (backfill_docs_processed=%v), changes_left=%v\n",
			spec.Id, curKvVbMapRo, total_changes, backfillTotalChanges, docs_processed, backfillDocsProcessed, changes_left)

		createOrUpdateOverviewStats(repl_status, backfillDocsProcessed, backfillChangesLeft, common.BackfillPipeline)

	} else {
		logger.Infof("Calculating stats for never run replication %v. kv_vb_map=%v, total_docs=%v, docs_processed=%v, changes_left=%v\n", spec.Id, curKvVbMapRo, total_changes, docs_processed, changes_left)
	}

	createOrUpdateOverviewStats(repl_status, docs_processed, changes_left, common.MainPipeline)

	// set vb list to establish the base for future stats update,
	// so that we can avoid re-computation when vb list does not change
	repl_status.SetVbList(base.CloneUint16List(curVbListRo))

	return nil
}

func calculateTotalChanges(logger *log.CommonLogger, highSeqnoKvMap base.HighSeqnosMapType, kvVbMap base.KvVBMapType) (int64, []uint16, error) {
	var total_changes uint64 = 0
	var vbsList []uint16

	for serverAddr, vbnos := range kvVbMap {
		highseqno_map, found := highSeqnoKvMap[serverAddr]
		if !found || highseqno_map == nil {
			logger.Warnf("Server %v not found in high seqnoMap %v", serverAddr, highSeqnoKvMap)
			continue
		}
		for _, vbno := range vbnos {
			vbsList = append(vbsList, vbno)
			current_vb_highseqno := (*highseqno_map)[vbno]
			total_changes = total_changes + current_vb_highseqno
		}
	}
	return int64(total_changes), base.SortUint16List(vbsList), nil
}

// For a paused pipeline, we need to update what is the total number of changes for the paused backfill pipeline
// The total number of changes will be: SUM_ALL_VBS(endSeqnoRequested - startSeqnoRequested)
func calculateTotalPausedBackfillChanges(backfillSpec *metadata.BackfillReplicationSpec, checkpointsSvc service_def.CheckpointsService, cur_vb_list []uint16, findCkpts bool) (int64, bool, error) {
	var backfillTotalChanges int64 = 0
	var checkpointExists bool

	if findCkpts {
		for _, vb := range cur_vb_list {
			_, err := checkpointsSvc.CheckpointsDoc(common.ComposeFullTopic(backfillSpec.Id, common.BackfillPipeline), vb)
			if err == nil {
				checkpointExists = true
				break
			}
		}
	}

	for _, vb := range cur_vb_list {
		tasksPtr, exists, backfillVBUnlock := backfillSpec.VBTasksMap.Get(vb, false)
		if !exists || tasksPtr == nil || tasksPtr.Len() == 0 {
			backfillVBUnlock()
			continue
		}
		oneTask, _, tasksPtrUnlock := tasksPtr.GetRO(0)
		begin := oneTask.GetStartingTimestampSeqno()
		end := oneTask.GetEndingTimestampSeqno()
		backfillTotalChanges += int64(end - begin)
		tasksPtrUnlock()
		backfillVBUnlock()
	}
	return backfillTotalChanges, checkpointExists, nil
}

func updateStatsForReplication(repl_status pipeline_pkg.ReplicationStatusIface, overview_stats *expvar.Map, checkpoints_svc service_def.CheckpointsService, logger *log.CommonLogger, backfillSpec *metadata.BackfillReplicationSpec, highSeqnosAndSourceVBGetter func() (base.HighSeqnosMapType, base.KvVBMapType, func())) error {
	// if pipeline is not running, update docs_processed and changes_left stats, which are not being
	// updated by running pipeline and may have become inaccurate

	// first check if vb list on source side has changed.
	// if not, the doc_processed stats in overview stats is still accurate and we will just use it
	// otherwise, need to re-compute docs_processed stats by filtering the checkpoint docs using the current kv_vb_map

	var err error
	// old_vb_list is already sorted
	old_vb_list := repl_status.VbList()
	spec := repl_status.Spec()
	if spec == nil {
		logger.Infof("replication %v has been deleted, skip updating stats\n", repl_status.RepId())
		return nil
	}

	highSeqnoKvMap, curKvVbMap, doneFunc := highSeqnosAndSourceVBGetter()
	defer doneFunc()

	cur_vb_list := base.GetVbListFromKvVbMap(curKvVbMap)
	base.SortUint16List(cur_vb_list)
	sameList := base.AreSortedUint16ListsTheSame(old_vb_list, cur_vb_list)

	var docsProcessedOld int64
	if sameList {
		docsProcessedOld, err = strconv.ParseInt(overview_stats.Get(service_def.DOCS_PROCESSED_METRIC).String(), base.ParseIntBase, base.ParseIntBitSize)
		if err != nil {
			return err
		}
	} else {
		logger.Infof("%v Source topology changed. Re-compute docs_processed. old_vb_list=%v, cur_vb_list=%v\n", repl_status.RepId(), old_vb_list, cur_vb_list)
		repl_status.SetVbList(cur_vb_list)
	}

	backfillTotalChanges, backfillDocsProcessed, backfillChangesLeft, backfillStatsCalculated := calculateBackfillStatsForPausedRepl(spec.Id, cur_vb_list, backfillSpec, checkpoints_svc, logger)

	total_changes, docs_processed, changes_left, err := calculateMainStatsForPausedRepl(spec.Id, cur_vb_list, checkpoints_svc, highSeqnoKvMap, curKvVbMap, backfillTotalChanges, int64(backfillDocsProcessed), backfillChangesLeft, backfillStatsCalculated, !sameList, docsProcessedOld, logger)
	if err != nil {
		return err
	}

	if backfillStatsCalculated {
		logger.Infof("Updating status for paused replication %v. kv_vb_map=%v, total_docs=%v (totalBackfillDocs=%v), docs_processed=%v (totalBackfillDocsProcessed=%v), changes_left=%v (totalBackfillChangesLeft=%v)\n",
			spec.Id, curKvVbMap, total_changes, backfillTotalChanges, docs_processed, backfillDocsProcessed, changes_left, backfillChangesLeft)

		createOrUpdateOverviewStats(repl_status, backfillDocsProcessed, backfillChangesLeft, common.BackfillPipeline)
	} else {
		logger.Infof("Updating status for paused replication %v. kv_vb_map=%v, total_docs=%v, docs_processed=%v, changes_left=%v\n", spec.Id, curKvVbMap, total_changes, docs_processed, changes_left)
	}

	createOrUpdateOverviewStats(repl_status, docs_processed, changes_left, common.MainPipeline)

	return nil
}

func StatsUpdateInterval(settings metadata.ReplicationSettingsMap) time.Duration {
	update_interval := default_update_interval
	if _, ok := settings[service_def.PUBLISH_INTERVAL]; ok {
		update_interval = settings[service_def.PUBLISH_INTERVAL].(int)
	}
	return time.Duration(update_interval) * time.Millisecond
}

func (stats_mgr *StatisticsManager) GetCountMetrics(key string) (int64, error) {
	overviewRegistry, ok := stats_mgr.registries[service_def.OVERVIEW_METRICS_KEY]
	if !ok || overviewRegistry == nil {
		return 0, base.ErrorResourceDoesNotExist
	}
	registry := overviewRegistry.Get(key)
	if registry == nil {
		return 0, base.ErrorInvalidInput
	}
	registryCounter, isCounter := registry.(metrics.Counter)
	if !isCounter {
		return 0, fmt.Errorf("%v is not of type counter", key)
	}
	return registryCounter.Count(), nil
}

func (stats_mgr *StatisticsManager) GetVBCountMetrics(vb uint16) (base.VBCountMetricMap, error) {
	compiledMap := make(base.VBCountMetricMap)
	for _, collector := range stats_mgr.collectors {
		err := collector.AddVbSpecificMetrics(vb, compiledMap)
		if err != nil {
			return nil, err
		}
	}
	return compiledMap, nil
}

func (stats_mgr *StatisticsManager) SetVBCountMetrics(vb uint16, metricKVs base.VBCountMetricMap) error {
	for _, collector := range stats_mgr.collectors {
		err := collector.UpdateCurrentVbSpecificMetrics(vb, metricKVs, stats_mgr.registries)
		if err != nil {
			return err
		}
	}
	return nil
}

func (statsMgr *StatisticsManager) HandleLatestThroughSeqnos(SeqnoMap map[uint16]uint64) {
	for _, collector := range statsMgr.collectors {
		collector.HandleLatestThroughSeqnos(SeqnoMap)
	}
}

func (statsMgr *StatisticsManager) GetThroughSeqnosFromTsService() map[uint16]uint64 {
	return statsMgr.through_seqno_tracker_svc.GetThroughSeqnos()
}

func (statsMgr *StatisticsManager) IsSharable() bool {
	return false
}

func (statsMgr *StatisticsManager) Detach(pipeline common.Pipeline) error {
	return base.ErrorNotSupported
}

var readOnlyInitOverview metrics.Registry
var readOnlyInitOnce sync.Once

func GetReadOnlyOverviewStats(repStatus pipeline_pkg.ReplicationStatusIface, pipelineType common.PipelineType) *expvar.Map {
	readOnlyInitOnce.Do(func() {
		readOnlyInitOverview = metrics.NewRegistry()
		for overviewKey, metricType := range OverviewMetricKeys {
			switch metricType {
			case service_def.MetricTypeCounter:
				if overviewKey == service_def.DOCS_CHECKED_METRIC {
					docsCheckedCounter := metrics.NewCounter()
					docsCheckedCounter.Clear()
					docsCheckedCounter.Inc(int64(-1))
					readOnlyInitOverview.Register(overviewKey, docsCheckedCounter)
				} else {
					readOnlyInitOverview.Register(overviewKey, metrics.NewCounter())
				}
			case service_def.MetricTypeGauge:
				readOnlyInitOverview.Register(overviewKey, metrics.NewGauge())
			}
		}
	})

	initOverviewMap := new(expvar.Map).Init()
	readOnlyInitOverview.Each(func(name string, i interface{}) {
		_, okForCounter := i.(metrics.Counter)
		_, okForGauge := i.(metrics.Gauge)
		if okForGauge || okForCounter {
			expvarVal := new(expvar.Int)

			// For readonly stats, because StatsMgr is not running, there will be no proper pipeline status or error count
			// Need to inject it manually here and only for main pipeline
			if pipelineType == common.MainPipeline && len(repStatus.Errors()) > 0 {
				if name == service_def.PIPELINE_ERRORS {
					expvarVal.Set(int64(len(repStatus.Errors())))
				} else if name == service_def.PIPELINE_STATUS {
					expvarVal.Set(int64(base.PipelineStatusError))
				}
			}

			initOverviewMap.Set(name, expvarVal)
		}
	})

	currentTimeVar := new(expvar.Int)
	currentTimeVar.Set(time.Now().UnixNano())
	initOverviewMap.Set(base.CurrentTime, currentTimeVar)

	return initOverviewMap
}
