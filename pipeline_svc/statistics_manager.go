// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package pipeline_svc

import (
	"errors"
	"expvar"
	"fmt"
	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	component "github.com/couchbase/goxdcr/component"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/parts"
	pipeline_pkg "github.com/couchbase/goxdcr/pipeline"
	"github.com/couchbase/goxdcr/pipeline_manager"
	"github.com/couchbase/goxdcr/pipeline_utils"
	"github.com/couchbase/goxdcr/service_def"
	utilities "github.com/couchbase/goxdcr/utils"
	"github.com/rcrowley/go-metrics"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// the number of docs written/sent to target cluster
	DOCS_WRITTEN_METRIC          = "docs_written"
	EXPIRY_DOCS_WRITTEN_METRIC   = "expiry_docs_written"
	DELETION_DOCS_WRITTEN_METRIC = "deletion_docs_written"
	SET_DOCS_WRITTEN_METRIC      = "set_docs_written"

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

	CHANGES_LEFT_METRIC = base.ChangesLeftStats
	DOCS_LATENCY_METRIC = "wtavg_docs_latency"
	META_LATENCY_METRIC = "wtavg_meta_latency"
	RESP_WAIT_METRIC    = "resp_wait_time"

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

	//	TIME_COMMITTING_METRIC = "time_committing"
	//rate
	RATE_REPLICATED_METRIC = "rate_replicated"
	BANDWIDTH_USAGE_METRIC = "bandwidth_usage"

	VB_HIGHSEQNO_PREFIX = "vb_highseqno_"

	OVERVIEW_METRICS_KEY = "Overview"

	//statistics_manager's setting
	SOURCE_NODE_ADDR     = "source_host_addr"
	SOURCE_NODE_USERNAME = "source_host_username"
	SOURCE_NODE_PASSWORD = "source_host_password"
	SAMPLE_SIZE          = "sample_size"
	PUBLISH_INTERVAL     = "publish_interval"

	// Memory related statistics
	DP_GET_FAIL_METRIC = "datapool_failed_gets"
)

const (
	default_sample_size     = 1000
	default_update_interval = 1000
)

// stats to initialize for paused replications that have never been run -- mostly the stats visible from UI
var StatsToInitializeForPausedReplications = []string{DOCS_WRITTEN_METRIC, DOCS_FAILED_CR_SOURCE_METRIC, DOCS_FILTERED_METRIC,
	RATE_DOC_CHECKS_METRIC, RATE_OPT_REPD_METRIC, RATE_RECEIVED_DCP_METRIC, RATE_REPLICATED_METRIC,
	BANDWIDTH_USAGE_METRIC, DOCS_LATENCY_METRIC, META_LATENCY_METRIC}

// stats to clear when replications are paused
// 1. all rate type stats
// 2. internal stats that are not visible on UI
var StatsToClearForPausedReplications = []string{SIZE_REP_QUEUE_METRIC, DOCS_REP_QUEUE_METRIC, DOCS_LATENCY_METRIC, META_LATENCY_METRIC,
	TIME_COMMITING_METRIC, NUM_FAILEDCKPTS_METRIC, RATE_DOC_CHECKS_METRIC, RATE_OPT_REPD_METRIC, RATE_RECEIVED_DCP_METRIC,
	RATE_REPLICATED_METRIC, BANDWIDTH_USAGE_METRIC, THROTTLE_LATENCY_METRIC, THROUGHPUT_THROTTLE_LATENCY_METRIC}

// keys for metrics in overview
var OverviewMetricKeys = []string{CHANGES_LEFT_METRIC, DOCS_CHECKED_METRIC, DOCS_WRITTEN_METRIC, EXPIRY_DOCS_WRITTEN_METRIC, DELETION_DOCS_WRITTEN_METRIC,
	SET_DOCS_WRITTEN_METRIC, DOCS_PROCESSED_METRIC, DOCS_FAILED_CR_SOURCE_METRIC, EXPIRY_FAILED_CR_SOURCE_METRIC,
	DELETION_FAILED_CR_SOURCE_METRIC, SET_FAILED_CR_SOURCE_METRIC, DATA_REPLICATED_METRIC, DOCS_FILTERED_METRIC, DOCS_UNABLE_TO_FILTER_METRIC,
	EXPIRY_FILTERED_METRIC, DELETION_FILTERED_METRIC, SET_FILTERED_METRIC, NUM_CHECKPOINTS_METRIC, NUM_FAILEDCKPTS_METRIC,
	TIME_COMMITING_METRIC, DOCS_OPT_REPD_METRIC, DOCS_RECEIVED_DCP_METRIC, EXPIRY_RECEIVED_DCP_METRIC,
	DELETION_RECEIVED_DCP_METRIC, SET_RECEIVED_DCP_METRIC, SIZE_REP_QUEUE_METRIC, DOCS_REP_QUEUE_METRIC, DOCS_LATENCY_METRIC,
	RESP_WAIT_METRIC, META_LATENCY_METRIC, DCP_DISPATCH_TIME_METRIC, DCP_DATACH_LEN, THROTTLE_LATENCY_METRIC, THROUGHPUT_THROTTLE_LATENCY_METRIC,
	DP_GET_FAIL_METRIC, EXPIRY_STRIPPED_METRIC}

// Stats per vbucket
type VBCountMetricMap map[string]int64

var VBMetricKeys = []string{DOCS_FILTERED_METRIC, DOCS_UNABLE_TO_FILTER_METRIC}

func MakeVBCountMetricMap() VBCountMetricMap {
	newMap := make(VBCountMetricMap)
	for _, key := range VBMetricKeys {
		newMap[key] = 0
	}
	return newMap
}

var VBCountMetrics = MakeVBCountMetricMap()

func NewVBStatsMapFromCkpt(ckptDoc *metadata.CheckpointsDoc, agreedIndex int) VBCountMetricMap {
	if agreedIndex < 0 || ckptDoc == nil || agreedIndex >= len(ckptDoc.Checkpoint_records) {
		return nil
	}

	record := ckptDoc.Checkpoint_records[agreedIndex]

	vbStatMap := make(VBCountMetricMap)
	vbStatMap[DOCS_FILTERED_METRIC] = base.Uint64ToInt64(record.Filtered_Items_Cnt)
	vbStatMap[DOCS_UNABLE_TO_FILTER_METRIC] = base.Uint64ToInt64(record.Filtered_Failed_Cnt)
	return vbStatMap
}

// keys for metrics that do not monotonically increase during replication, to which the "going backward" check should not be applied
var NonIncreasingMetricKeyMap = map[string]bool{
	SIZE_REP_QUEUE_METRIC: true,
	DOCS_REP_QUEUE_METRIC: true,
	DCP_DATACH_LEN:        true}

// the fixed user agent string for connections to collect stats for paused replications
// it is possible to construct the user agent string dynamically by adding source and target bucket info to it
// it would cause many string re-allocations, though
var UserAgentPausedReplication = "Goxdcr client for paused replication"

type SampleStats struct {
	Count int64
	Mean  float64
}

//StatisticsManager mount the statics collector on the pipeline to collect raw stats
//It does stats correlation and processing on raw stats periodically (controlled by publish_interval)
//, then stores the result in expvar
//The result in expvar can be exposed to outside via different channels - log or to ns_server.
type StatisticsManager struct {

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

	pipeline common.Pipeline

	logger *log.CommonLogger

	collectors []MetricsCollector

	active_vbs  map[string][]uint16
	bucket_name string

	// Keeps track of all the serverAddr -> MCC client required
	kv_mem_clients      map[string]mcc.ClientIface
	kv_mem_clients_lock *sync.RWMutex

	through_seqno_tracker_svc service_def.ThroughSeqnoTrackerSvc
	cluster_info_svc          service_def.ClusterInfoSvc
	xdcr_topology_svc         service_def.XDCRCompTopologySvc

	stats_map map[string]string

	user_agent string

	utils utilities.UtilsIface
}

type StatsMgrIface interface {
	Start(settings metadata.ReplicationSettingsMap) error
	Stop() error
	GetCountMetrics(key string) (int64, error)
	GetVBCountMetrics(vb uint16) (VBCountMetricMap, error)
	SetVBCountMetrics(vb uint16, metricKVs VBCountMetricMap) error
	HandleLatestThroughSeqnos(SeqnoMap map[uint16]uint64)
}

func NewStatisticsManager(through_seqno_tracker_svc service_def.ThroughSeqnoTrackerSvc,
	cluster_info_svc service_def.ClusterInfoSvc, xdcr_topology_svc service_def.XDCRCompTopologySvc,
	logger_ctx *log.LoggerContext, active_vbs map[string][]uint16, bucket_name string,
	utilsIn utilities.UtilsIface) *StatisticsManager {
	stats_mgr := &StatisticsManager{
		registries:                make(map[string]metrics.Registry),
		logger:                    log.NewLogger("StatsMgr", logger_ctx),
		bucket_name:               bucket_name,
		finish_ch:                 make(chan bool, 1),
		done_ch:                   make(chan bool, 1),
		update_ticker_ch:          make(chan *time.Ticker, 1000),
		sample_size:               default_sample_size,
		update_interval:           default_update_interval,
		active_vbs:                active_vbs,
		wait_grp:                  &sync.WaitGroup{},
		kv_mem_clients:            make(map[string]mcc.ClientIface),
		kv_mem_clients_lock:       &sync.RWMutex{},
		checkpointed_seqnos:       make(map[uint16]*base.SeqnoWithLock),
		stats_map:                 make(map[string]string),
		through_seqno_tracker_svc: through_seqno_tracker_svc,
		cluster_info_svc:          cluster_info_svc,
		xdcr_topology_svc:         xdcr_topology_svc,
		utils:                     utilsIn,
	}
	stats_mgr.collectors = []MetricsCollector{&outNozzleCollector{}, &dcpCollector{}, &routerCollector{}, &checkpointMgrCollector{}}

	stats_mgr.initialize()
	return stats_mgr
}

//Statistics of a pipeline which may or may not be running
func GetStatisticsForPipeline(topic string) (*expvar.Map, error) {
	repl_status, _ := pipeline_manager.ReplicationStatus(topic)
	if repl_status == nil {
		return nil, nil
	}

	return repl_status.GetOverviewStats(), nil
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
	stats_mgr.logger.Infof("%v set update interval to %v ms\n", stats_mgr.pipeline.InstanceId(), update_interval)
}

func getHighSeqNos(serverAddr string, vbnos []uint16, conn mcc.ClientIface, stats_map map[string]string, utils utilities.UtilsIface) (map[uint16]uint64, error) {
	highseqno_map := make(map[uint16]uint64)

	var err error
	if stats_map != nil {
		// stats_map is not nill when getHighSeqNos is called from per-replication stats manager, reuse stats_map to avoid memory over-allocation and re-allocation
		err = conn.StatsMapForSpecifiedStats(base.VBUCKET_SEQNO_STAT_NAME, stats_map)
	} else {
		// stats_map is nill when getHighSeqNos is called on paused replications. do not reuse stats_map
		stats_map, err = conn.StatsMap(base.VBUCKET_SEQNO_STAT_NAME)
	}
	if err != nil {
		return nil, err
	}

	err = utils.ParseHighSeqnoStat(vbnos, stats_map, highseqno_map)
	if err != nil {
		return nil, err
	} else {
		return highseqno_map, nil
	}
}

//updateStats runs until it get finish signal
//It processes the raw stats and publish the overview stats along with the raw stats to expvar
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

//periodically prints stats to log
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
			if stats_mgr.pipeline.Specification().Settings.RepType == metadata.ReplicationTypeXmem {
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
	}
	return nil
}

func (stats_mgr *StatisticsManager) formatStatsForLog() (string, error) {
	rs, err := stats_mgr.getReplicationStatus()
	if err != nil {
		return "", err
	}
	expvar_stats_map := rs.Storage()
	return fmt.Sprintf("Stats for pipeline %v %v\n", stats_mgr.pipeline.InstanceId(), expvar_stats_map.String()), nil
}

//process the raw stats, aggregate them into overview registry
//expose the raw stats and overview stats to expvar
//Locking is done by caller
func (stats_mgr *StatisticsManager) processRawStats() error {
	rs, err := stats_mgr.getReplicationStatus()
	if err != nil {
		return err
	}

	// save existing values in overview registry for rate stats calculation
	oldSample := stats_mgr.getOverviewRegistry()
	docs_written_old := oldSample.Get(DOCS_WRITTEN_METRIC).(metrics.Counter).Count()
	docs_received_dcp_old := oldSample.Get(DOCS_RECEIVED_DCP_METRIC).(metrics.Counter).Count()
	docs_opt_repd_old := oldSample.Get(DOCS_OPT_REPD_METRIC).(metrics.Counter).Count()
	data_replicated_old := oldSample.Get(DATA_REPLICATED_METRIC).(metrics.Counter).Count()
	docs_checked_old_var := oldSample.Get(DOCS_CHECKED_METRIC)
	var docs_checked_old int64 = 0
	if docs_checked_old_var != nil {
		docs_checked_old = docs_checked_old_var.(metrics.Counter).Count()
	}
	changes_left_old_var := oldSample.Get(CHANGES_LEFT_METRIC)
	var changes_left_old int64 = 0
	if changes_left_old_var != nil {
		changes_left_old = changes_left_old_var.(metrics.Counter).Count()
	}

	stats_mgr.initOverviewRegistry()

	sample_stats_list_map := make(map[string][]*SampleStats)

	for registry_name, registry := range stats_mgr.registries {
		if registry_name != OVERVIEW_METRICS_KEY {
			map_for_registry := new(expvar.Map).Init()

			orig_registry := rs.GetStats(registry_name)
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
			rs.SetStats(registry_name, map_for_registry)
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
		docs_opt_repd_old, data_replicated_old, docs_checked_old)
	if err != nil {
		return err
	}

	stats_mgr.logger.Debugf("Overview=%v for pipeline %v\n", map_for_overview, stats_mgr.pipeline.Topic())

	// set current time to map_for_interview
	current_time_var := new(expvar.Int)
	current_time_var.Set(time.Now().UnixNano())
	map_for_overview.Set(base.CurrentTime, current_time_var)

	rs.SetOverviewStats(map_for_overview)
	return nil
}

func (stats_mgr *StatisticsManager) processCalculatedStats(overview_expvar_map *expvar.Map, changes_left_old,
	docs_written_old, docs_received_dcp_old, docs_opt_repd_old, data_replicated_old, docs_checked_old int64) error {

	//calculate docs_processed
	docs_processed := stats_mgr.calculateDocsProcessed()
	docs_processed_var := new(expvar.Int)
	docs_processed_var.Set(docs_processed)
	overview_expvar_map.Set(DOCS_PROCESSED_METRIC, docs_processed_var)

	//calculate changes_left
	changes_left_val, err := stats_mgr.calculateChangesLeft(docs_processed)
	if err != nil {
		stats_mgr.logger.Warnf("%v Failed to calculate changes_left. Use old changes_left. err=%v\n", stats_mgr.pipeline.InstanceId(), err)
		changes_left_val = changes_left_old
	}
	changes_left_var := new(expvar.Int)
	changes_left_var.Set(changes_left_val)
	overview_expvar_map.Set(CHANGES_LEFT_METRIC, changes_left_var)
	// also update the value in overview registry since we need it at the next stats computation time
	setCounter(stats_mgr.getOverviewRegistry().Get(CHANGES_LEFT_METRIC).(metrics.Counter), int(changes_left_val))

	//calculate rate_replication
	docs_written := stats_mgr.getOverviewRegistry().Get(DOCS_WRITTEN_METRIC).(metrics.Counter).Count()
	interval_in_sec := stats_mgr.getUpdateInterval().Seconds()
	rate_replicated := float64(docs_written-docs_written_old) / interval_in_sec
	rate_replicated_var := new(expvar.Float)
	rate_replicated_var.Set(rate_replicated)
	overview_expvar_map.Set(RATE_REPLICATED_METRIC, rate_replicated_var)

	//calculate rate_received_from_dcp
	docs_received_dcp := stats_mgr.getOverviewRegistry().Get(DOCS_RECEIVED_DCP_METRIC).(metrics.Counter).Count()
	rate_received_dcp := float64(docs_received_dcp-docs_received_dcp_old) / interval_in_sec
	rate_received_dcp_var := new(expvar.Float)
	rate_received_dcp_var.Set(rate_received_dcp)
	overview_expvar_map.Set(RATE_RECEIVED_DCP_METRIC, rate_received_dcp_var)

	//calculate rate_doc_opt_repd
	docs_opt_repd := stats_mgr.getOverviewRegistry().Get(DOCS_OPT_REPD_METRIC).(metrics.Counter).Count()
	rate_opt_repd := float64(docs_opt_repd-docs_opt_repd_old) / interval_in_sec
	rate_opt_repd_var := new(expvar.Float)
	rate_opt_repd_var.Set(rate_opt_repd)
	overview_expvar_map.Set(RATE_OPT_REPD_METRIC, rate_opt_repd_var)

	//calculate bandwidth_usage
	data_replicated := stats_mgr.getOverviewRegistry().Get(DATA_REPLICATED_METRIC).(metrics.Counter).Count()
	// bandwidth_usage is in the unit of MB/second, where 1 MB = 1024*1024 bytes instead of 1000*1000 bytes
	bandwidth_usage := float64(data_replicated-data_replicated_old) / (interval_in_sec * 1.024 * 1.024)
	bandwidth_usage_var := new(expvar.Float)
	bandwidth_usage_var.Set(bandwidth_usage)
	overview_expvar_map.Set(BANDWIDTH_USAGE_METRIC, bandwidth_usage_var)

	//calculate docs_checked
	docs_checked := stats_mgr.calculateDocsChecked()
	docs_checked_var := new(expvar.Int)
	docs_checked_var.Set(int64(docs_checked))
	overview_expvar_map.Set(DOCS_CHECKED_METRIC, docs_checked_var)
	setCounter(stats_mgr.getOverviewRegistry().Get(DOCS_CHECKED_METRIC).(metrics.Counter), int(docs_checked))

	//calculate rate_doc_checks
	var rate_doc_checks float64
	if docs_checked_old < 0 {
		// a negative value indicates that this is the first stats run and there is no old value yet
		rate_doc_checks = 0
	} else {
		rate_doc_checks = (float64(docs_checked) - float64(docs_checked_old)) / interval_in_sec
	}
	rate_doc_checks_var := new(expvar.Float)
	rate_doc_checks_var.Set(rate_doc_checks)
	overview_expvar_map.Set(RATE_DOC_CHECKS_METRIC, rate_doc_checks_var)
	return nil
}

func (stats_mgr *StatisticsManager) calculateDocsProcessed() int64 {
	var docs_processed uint64 = 0
	through_seqno_map := stats_mgr.through_seqno_tracker_svc.GetThroughSeqnos()
	for _, through_seqno := range through_seqno_map {
		docs_processed += through_seqno
	}
	return int64(docs_processed)
}

func (stats_mgr *StatisticsManager) calculateDocsChecked() uint64 {
	var docs_checked uint64 = 0
	vbts_map, vbts_map_lock := GetStartSeqnos(stats_mgr.pipeline, stats_mgr.logger)
	if vbts_map != nil {
		vbts_map_lock.RLock()
		defer vbts_map_lock.RUnlock()

		for vbno, vbts := range vbts_map {
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
func (stats_mgr *StatisticsManager) calculateChangesLeft(docs_processed int64) (int64, error) {
	stats_mgr.kv_mem_clients_lock.Lock()
	defer stats_mgr.kv_mem_clients_lock.Unlock()

	total_changes, err := calculateTotalChanges(stats_mgr.active_vbs, stats_mgr.kv_mem_clients, stats_mgr.bucket_name, stats_mgr.user_agent, stats_mgr.stats_map, stats_mgr.logger, stats_mgr.utils)
	if err != nil {
		return 0, err
	}
	changes_left := total_changes - docs_processed
	stats_mgr.logger.Infof("%v total_docs=%v, docs_processed=%v, changes_left=%v\n", stats_mgr.pipeline.Topic(), total_changes, docs_processed, changes_left)
	return changes_left, nil
}

func (stats_mgr *StatisticsManager) getOverviewRegistry() metrics.Registry {
	return stats_mgr.registries[OVERVIEW_METRICS_KEY]
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

	//register the aggregation metrics for the pipeline
	stats_mgr.initOverviewRegistry()
	stats_mgr.logger.Infof("StatisticsManager is started for pipeline %v", stats_mgr.pipeline.Topic)

	return nil
}

// compose user agent string for HELO command
func (stats_mgr *StatisticsManager) composeUserAgent() {
	spec := stats_mgr.pipeline.Specification()
	stats_mgr.user_agent = base.ComposeUserAgentWithBucketNames("Goxdcr StatsMgr", spec.SourceBucketName, spec.TargetBucketName)
}

func (stats_mgr *StatisticsManager) initOverviewRegistry() {
	if overview_registry, ok := stats_mgr.registries[OVERVIEW_METRICS_KEY]; ok {
		// reset all counters to 0
		for _, overview_metric_key := range OverviewMetricKeys {
			overview_registry.Get(overview_metric_key).(metrics.Counter).Clear()
		}
	} else {
		// create new overview_registry and initialize all counters to 0 except for DOCS_CHECKED_METRIC
		overview_registry = metrics.NewRegistry()
		stats_mgr.registries[OVERVIEW_METRICS_KEY] = overview_registry
		for _, overview_metric_key := range OverviewMetricKeys {
			if overview_metric_key == DOCS_CHECKED_METRIC {
				// use a negative value to indicate that an old value of docs_checked does not exist
				docs_checked_counter := metrics.NewCounter()
				setCounter(docs_checked_counter, -1)
				overview_registry.Register(DOCS_CHECKED_METRIC, docs_checked_counter)
			} else {
				overview_registry.Register(overview_metric_key, metrics.NewCounter())
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

	//initialize connection
	err = stats_mgr.initConnections()
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

	var update_interval int
	var update_interval_duration time.Duration
	if update_interval_obj, ok := settings[PUBLISH_INTERVAL]; ok {
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

	//close the connections
	stats_mgr.closeConnections()

	stats_mgr.wait_grp.Wait()
	stats_mgr.logger.Infof("%v StatisticsManager Stopped", stats_mgr.pipeline.InstanceId())

	return nil
}

func (stats_mgr *StatisticsManager) closeConnections() {
	stats_mgr.kv_mem_clients_lock.Lock()
	defer stats_mgr.kv_mem_clients_lock.Unlock()
	for server_addr, client := range stats_mgr.kv_mem_clients {
		err := client.Close()
		if err != nil {
			stats_mgr.logger.Infof("%v error from closing connection for %v is %v\n", stats_mgr.pipeline.InstanceId(), server_addr, err)
		}
	}
	stats_mgr.kv_mem_clients = make(map[string]mcc.ClientIface)
}

func (stats_mgr *StatisticsManager) initConnections() error {
	for serverAddr, _ := range stats_mgr.active_vbs {
		// as of now active_vbs contains only the current node and the connection is always local. use plain authentication
		conn, err := stats_mgr.utils.GetMemcachedConnection(serverAddr, stats_mgr.bucket_name, stats_mgr.user_agent, base.KeepAlivePeriod, stats_mgr.logger)
		if err != nil {
			return err
		}
		// no need to lock since this is done during initialization
		stats_mgr.kv_mem_clients[serverAddr] = conn
	}

	return nil
}

func (stats_mgr *StatisticsManager) UpdateSettings(settings metadata.ReplicationSettingsMap) error {
	if stats_mgr.logger.GetLogLevel() >= log.LogLevelDebug {
		stats_mgr.logger.Debugf("%v Updating settings on stats manager. settings=%v\n", stats_mgr.pipeline.InstanceId(), settings.CloneAndRedact())
	}

	update_interval, err := stats_mgr.utils.GetIntSettingFromSettings(settings, PUBLISH_INTERVAL)
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
}

//metrics collector for XMem/CapiNozzle
type outNozzleCollector struct {
	id        string
	stats_mgr *StatisticsManager
	common.AsyncComponentEventHandler
	// key of outer map: component id
	// key of inner map: metric name
	// value of inner map: metric value
	component_map map[string]map[string]interface{}
}

func (outNozzle_collector *outNozzleCollector) Mount(pipeline common.Pipeline, stats_mgr *StatisticsManager) error {
	outNozzle_collector.id = pipeline_utils.GetElementIdFromName(pipeline, base.OutNozzleStatsCollector)
	outNozzle_collector.stats_mgr = stats_mgr
	outNozzle_collector.component_map = make(map[string]map[string]interface{})
	outNozzle_parts := pipeline.Targets()
	for _, part := range outNozzle_parts {
		registry := stats_mgr.getOrCreateRegistry(part.Id())
		size_rep_queue := metrics.NewCounter()
		registry.Register(SIZE_REP_QUEUE_METRIC, size_rep_queue)
		docs_rep_queue := metrics.NewCounter()
		registry.Register(DOCS_REP_QUEUE_METRIC, docs_rep_queue)
		docs_written := metrics.NewCounter()
		registry.Register(DOCS_WRITTEN_METRIC, docs_written)
		expiry_docs_written := metrics.NewCounter()
		registry.Register(EXPIRY_DOCS_WRITTEN_METRIC, expiry_docs_written)
		deletion_docs_written := metrics.NewCounter()
		registry.Register(DELETION_DOCS_WRITTEN_METRIC, deletion_docs_written)
		set_docs_written := metrics.NewCounter()
		registry.Register(SET_DOCS_WRITTEN_METRIC, set_docs_written)
		docs_failed_cr := metrics.NewCounter()
		registry.Register(DOCS_FAILED_CR_SOURCE_METRIC, docs_failed_cr)
		expiry_failed_cr := metrics.NewCounter()
		registry.Register(EXPIRY_FAILED_CR_SOURCE_METRIC, expiry_failed_cr)
		deletion_failed_cr := metrics.NewCounter()
		registry.Register(DELETION_FAILED_CR_SOURCE_METRIC, deletion_failed_cr)
		set_failed_cr := metrics.NewCounter()
		registry.Register(SET_FAILED_CR_SOURCE_METRIC, set_failed_cr)
		data_replicated := metrics.NewCounter()
		registry.Register(DATA_REPLICATED_METRIC, data_replicated)
		docs_opt_repd := metrics.NewCounter()
		registry.Register(DOCS_OPT_REPD_METRIC, docs_opt_repd)
		docs_latency := metrics.NewHistogram(metrics.NewUniformSample(stats_mgr.sample_size))
		registry.Register(DOCS_LATENCY_METRIC, docs_latency)
		resp_wait := metrics.NewHistogram(metrics.NewUniformSample(stats_mgr.sample_size))
		registry.Register(RESP_WAIT_METRIC, resp_wait)
		meta_latency := metrics.NewHistogram(metrics.NewUniformSample(stats_mgr.sample_size))
		registry.Register(META_LATENCY_METRIC, meta_latency)
		throttle_latency := metrics.NewHistogram(metrics.NewUniformSample(stats_mgr.sample_size))
		registry.Register(THROTTLE_LATENCY_METRIC, throttle_latency)

		metric_map := make(map[string]interface{})
		metric_map[SIZE_REP_QUEUE_METRIC] = size_rep_queue
		metric_map[DOCS_REP_QUEUE_METRIC] = docs_rep_queue
		metric_map[DOCS_WRITTEN_METRIC] = docs_written
		metric_map[EXPIRY_DOCS_WRITTEN_METRIC] = expiry_docs_written
		metric_map[DELETION_DOCS_WRITTEN_METRIC] = deletion_docs_written
		metric_map[SET_DOCS_WRITTEN_METRIC] = set_docs_written
		metric_map[DOCS_FAILED_CR_SOURCE_METRIC] = docs_failed_cr
		metric_map[EXPIRY_FAILED_CR_SOURCE_METRIC] = expiry_failed_cr
		metric_map[DELETION_FAILED_CR_SOURCE_METRIC] = deletion_failed_cr
		metric_map[SET_FAILED_CR_SOURCE_METRIC] = set_failed_cr
		metric_map[DATA_REPLICATED_METRIC] = data_replicated
		metric_map[DOCS_OPT_REPD_METRIC] = docs_opt_repd
		metric_map[DOCS_LATENCY_METRIC] = docs_latency
		metric_map[RESP_WAIT_METRIC] = resp_wait
		metric_map[META_LATENCY_METRIC] = meta_latency
		metric_map[THROTTLE_LATENCY_METRIC] = throttle_latency
		outNozzle_collector.component_map[part.Id()] = metric_map

		// register outNozzle_collector as the sync event listener/handler for StatsUpdate event
		part.RegisterComponentEventListener(common.StatsUpdate, outNozzle_collector)
	}

	// register outNozzle_collector as the async event handler for relevant events
	async_listener_map := pipeline_pkg.GetAllAsyncComponentEventListeners(pipeline)
	pipeline_utils.RegisterAsyncComponentEventHandler(async_listener_map, base.DataSentEventListener, outNozzle_collector)
	pipeline_utils.RegisterAsyncComponentEventHandler(async_listener_map, base.DataFailedCREventListener, outNozzle_collector)
	pipeline_utils.RegisterAsyncComponentEventHandler(async_listener_map, base.GetMetaReceivedEventListener, outNozzle_collector)
	pipeline_utils.RegisterAsyncComponentEventHandler(async_listener_map, base.DataThrottledEventListener, outNozzle_collector)

	return nil
}

func (outNozzle_collector *outNozzleCollector) Id() string {
	return outNozzle_collector.id
}

func (outNozzle_collector *outNozzleCollector) OnEvent(event *common.Event) {
	outNozzle_collector.ProcessEvent(event)
}

func (outNozzleCollector *outNozzleCollector) HandleLatestThroughSeqnos(SeqnoMap map[uint16]uint64) {
	// Nothing
	return
}

func (outNozzle_collector *outNozzleCollector) ProcessEvent(event *common.Event) error {
	metric_map := outNozzle_collector.component_map[event.Component.Id()]
	if event.EventType == common.StatsUpdate {
		queue_size := event.OtherInfos.([]int)[0]
		queue_size_bytes := event.OtherInfos.([]int)[1]
		setCounter(metric_map[DOCS_REP_QUEUE_METRIC].(metrics.Counter), queue_size)
		setCounter(metric_map[SIZE_REP_QUEUE_METRIC].(metrics.Counter), queue_size_bytes)
	} else if event.EventType == common.DataSent {
		event_otherInfo := event.OtherInfos.(parts.DataSentEventAdditional)
		req_size := event_otherInfo.Req_size
		opti_replicated := event_otherInfo.IsOptRepd
		commit_time := event_otherInfo.Commit_time
		resp_wait_time := event_otherInfo.Resp_wait_time
		metric_map[DOCS_WRITTEN_METRIC].(metrics.Counter).Inc(1)
		metric_map[DATA_REPLICATED_METRIC].(metrics.Counter).Inc(int64(req_size))
		if opti_replicated {
			metric_map[DOCS_OPT_REPD_METRIC].(metrics.Counter).Inc(1)
		}

		expiry_set := event_otherInfo.IsExpirySet
		if expiry_set {
			metric_map[EXPIRY_DOCS_WRITTEN_METRIC].(metrics.Counter).Inc(1)
		}

		req_opcode := event_otherInfo.Opcode
		if req_opcode == base.DELETE_WITH_META {
			metric_map[DELETION_DOCS_WRITTEN_METRIC].(metrics.Counter).Inc(1)
		} else if req_opcode == base.SET_WITH_META {
			metric_map[SET_DOCS_WRITTEN_METRIC].(metrics.Counter).Inc(1)
		} else {
			outNozzle_collector.stats_mgr.logger.Warnf("Invalid opcode, %v, in DataSent event from %v.", req_opcode, event.Component.Id())
		}

		metric_map[DOCS_LATENCY_METRIC].(metrics.Histogram).Sample().Update(commit_time.Nanoseconds() / 1000000)
		metric_map[RESP_WAIT_METRIC].(metrics.Histogram).Sample().Update(resp_wait_time.Nanoseconds() / 1000000)
	} else if event.EventType == common.DataFailedCRSource {
		metric_map[DOCS_FAILED_CR_SOURCE_METRIC].(metrics.Counter).Inc(1)
		event_otherInfos := event.OtherInfos.(parts.DataFailedCRSourceEventAdditional)
		expiry_set := event_otherInfos.IsExpirySet
		if expiry_set {
			metric_map[EXPIRY_FAILED_CR_SOURCE_METRIC].(metrics.Counter).Inc(1)
		}

		req_opcode := event_otherInfos.Opcode
		if req_opcode == base.DELETE_WITH_META {
			metric_map[DELETION_FAILED_CR_SOURCE_METRIC].(metrics.Counter).Inc(1)
		} else if req_opcode == base.SET_WITH_META {
			metric_map[SET_FAILED_CR_SOURCE_METRIC].(metrics.Counter).Inc(1)
		} else {
			outNozzle_collector.stats_mgr.logger.Warnf("Invalid opcode, %v, in DataFailedCRSource event from %v.", req_opcode, event.Component.Id())
		}
	} else if event.EventType == common.GetMetaReceived {
		event_otherInfos := event.OtherInfos.(parts.GetMetaReceivedEventAdditional)
		commit_time := event_otherInfos.Commit_time
		metric_map[META_LATENCY_METRIC].(metrics.Histogram).Sample().Update(commit_time.Nanoseconds() / 1000000)
	} else if event.EventType == common.DataThrottled {
		throttle_latency := event.OtherInfos.(time.Duration)
		metric_map[THROTTLE_LATENCY_METRIC].(metrics.Histogram).Sample().Update(throttle_latency.Nanoseconds() / 1000000)
	}

	return nil
}

func getStatsKeyFromDocKeyAndSeqno(key string, seqno uint64) string {
	return fmt.Sprintf("%v-%v", key, seqno)
}

//metrics collector for DcpNozzle
type dcpCollector struct {
	id        string
	stats_mgr *StatisticsManager
	// key of outer map: component id
	// key of inner map: metric name
	// value of inner map: metric value
	component_map map[string]map[string]interface{}
}

func (dcp_collector *dcpCollector) Mount(pipeline common.Pipeline, stats_mgr *StatisticsManager) error {
	dcp_collector.id = pipeline_utils.GetElementIdFromName(pipeline, base.DcpStatsCollector)
	dcp_collector.stats_mgr = stats_mgr
	dcp_collector.component_map = make(map[string]map[string]interface{})
	dcp_parts := pipeline.Sources()
	for _, dcp_part := range dcp_parts {
		registry := stats_mgr.getOrCreateRegistry(dcp_part.Id())
		docs_received_dcp := metrics.NewCounter()
		registry.Register(DOCS_RECEIVED_DCP_METRIC, docs_received_dcp)
		expiry_received_dcp := metrics.NewCounter()
		registry.Register(EXPIRY_RECEIVED_DCP_METRIC, expiry_received_dcp)
		deletion_received_dcp := metrics.NewCounter()
		registry.Register(DELETION_RECEIVED_DCP_METRIC, deletion_received_dcp)
		set_received_dcp := metrics.NewCounter()
		registry.Register(SET_RECEIVED_DCP_METRIC, set_received_dcp)
		dcp_dispatch_time := metrics.NewHistogram(metrics.NewUniformSample(stats_mgr.sample_size))
		registry.Register(DCP_DISPATCH_TIME_METRIC, dcp_dispatch_time)
		dcp_datach_len := metrics.NewCounter()
		registry.Register(DCP_DATACH_LEN, dcp_datach_len)

		metric_map := make(map[string]interface{})
		metric_map[DOCS_RECEIVED_DCP_METRIC] = docs_received_dcp
		metric_map[EXPIRY_RECEIVED_DCP_METRIC] = expiry_received_dcp
		metric_map[DELETION_RECEIVED_DCP_METRIC] = deletion_received_dcp
		metric_map[SET_RECEIVED_DCP_METRIC] = set_received_dcp
		metric_map[DCP_DISPATCH_TIME_METRIC] = dcp_dispatch_time
		metric_map[DCP_DATACH_LEN] = dcp_datach_len
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
		metric_map[DOCS_RECEIVED_DCP_METRIC].(metrics.Counter).Inc(1)

		if uprEvent.Expiry != 0 {
			metric_map[EXPIRY_RECEIVED_DCP_METRIC].(metrics.Counter).Inc(1)
		}
		if uprEvent.Opcode == mc.UPR_DELETION {
			metric_map[DELETION_RECEIVED_DCP_METRIC].(metrics.Counter).Inc(1)
		} else if uprEvent.Opcode == mc.UPR_MUTATION {
			metric_map[SET_RECEIVED_DCP_METRIC].(metrics.Counter).Inc(1)
		} else if uprEvent.Opcode == mc.UPR_EXPIRATION {
			metric_map[EXPIRY_RECEIVED_DCP_METRIC].(metrics.Counter).Inc(1)
		} else if uprEvent.IsSystemEvent() {
			// ignore system events
		} else {
			dcp_collector.stats_mgr.logger.Warnf("Invalid opcode, %v, in DataReceived event from %v.", uprEvent.Opcode, event.Component.Id())
		}
	case common.DataProcessed:
		dcp_dispatch_time := event.OtherInfos.(float64)
		metric_map[DCP_DISPATCH_TIME_METRIC].(metrics.Histogram).Sample().Update(int64(dcp_dispatch_time))
	case common.StatsUpdate:
		dcp_datach_len := event.OtherInfos.(int)
		setCounter(metric_map[DCP_DATACH_LEN].(metrics.Counter), dcp_datach_len)
	}

	return nil
}

type vbBasedThroughSeqnoHelper struct {
	id       string
	statsMgr *StatisticsManager

	// These are sorted because DCP streams send seqno in an increasing order
	sortedSeqnoListMap map[string]*base.SortedSeqnoListWithLock
}

func newVbBasedThroughSeqnoHelper(id string, statsMgr *StatisticsManager) *vbBasedThroughSeqnoHelper {
	helper := &vbBasedThroughSeqnoHelper{
		id:                 id,
		statsMgr:           statsMgr,
		sortedSeqnoListMap: make(map[string]*base.SortedSeqnoListWithLock),
	}

	for _, key := range VBMetricKeys {
		helper.sortedSeqnoListMap[key] = base.NewSortedSeqnoListWithLock()
	}
	return helper
}

func (vbh *vbBasedThroughSeqnoHelper) handleIncomingSeqno(seqno uint64, metricKey string) {
	vbh.sortedSeqnoListMap[metricKey].AppendSeqno(seqno)
}

func (vbh *vbBasedThroughSeqnoHelper) mergeWithMetrics(metricsMap map[string]interface{}, latestSeqno uint64) {
	for _, key := range VBMetricKeys {
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

//metrics collector for Router
type routerCollector struct {
	id        string
	stats_mgr *StatisticsManager

	// key of outer map: component id
	// key of inner map: metric name
	// value of inner map: metric value
	component_map map[string]map[string]interface{}

	// For vb-based metric across all DCP nozzles
	vbBasedMetric map[uint16]map[string]interface{}

	// A map of vb-> routerIDs
	routerVbsIdMap map[uint16]string

	// Helpers to ensure that stored filter metrics are correct
	vbBasedHelper map[uint16]*vbBasedThroughSeqnoHelper
}

func (r_collector *routerCollector) Mount(pipeline common.Pipeline, stats_mgr *StatisticsManager) error {
	r_collector.id = pipeline_utils.GetElementIdFromName(pipeline, base.RouterStatsCollector)
	r_collector.stats_mgr = stats_mgr
	r_collector.component_map = make(map[string]map[string]interface{})
	r_collector.vbBasedMetric = make(map[uint16]map[string]interface{})
	r_collector.routerVbsIdMap = make(map[uint16]string)
	r_collector.vbBasedHelper = make(map[uint16]*vbBasedThroughSeqnoHelper)
	dcp_parts := pipeline.Sources()
	for _, dcp_part := range dcp_parts {
		//get connector
		conn := dcp_part.Connector()
		registry_router := stats_mgr.getOrCreateRegistry(conn.Id())
		docs_filtered := metrics.NewCounter()
		registry_router.Register(DOCS_FILTERED_METRIC, docs_filtered)
		docs_unable_to_filter := metrics.NewCounter()
		registry_router.Register(DOCS_UNABLE_TO_FILTER_METRIC, docs_unable_to_filter)
		expiry_filtered := metrics.NewCounter()
		registry_router.Register(EXPIRY_FILTERED_METRIC, expiry_filtered)
		deletion_filtered := metrics.NewCounter()
		registry_router.Register(DELETION_FILTERED_METRIC, deletion_filtered)
		set_filtered := metrics.NewCounter()
		registry_router.Register(SET_FILTERED_METRIC, set_filtered)
		dp_failed := metrics.NewCounter()
		registry_router.Register(DP_GET_FAIL_METRIC, dp_failed)
		throughput_throttle_latency := metrics.NewHistogram(metrics.NewUniformSample(stats_mgr.sample_size))
		registry_router.Register(THROUGHPUT_THROTTLE_LATENCY_METRIC, throughput_throttle_latency)
		expiry_stripped := metrics.NewCounter()
		registry_router.Register(EXPIRY_STRIPPED_METRIC, expiry_stripped)

		metric_map := make(map[string]interface{})
		metric_map[DOCS_FILTERED_METRIC] = docs_filtered
		metric_map[DOCS_UNABLE_TO_FILTER_METRIC] = docs_unable_to_filter
		metric_map[EXPIRY_FILTERED_METRIC] = expiry_filtered
		metric_map[DELETION_FILTERED_METRIC] = deletion_filtered
		metric_map[SET_FILTERED_METRIC] = set_filtered
		metric_map[DP_GET_FAIL_METRIC] = dp_failed
		metric_map[THROUGHPUT_THROTTLE_LATENCY_METRIC] = throughput_throttle_latency
		metric_map[EXPIRY_STRIPPED_METRIC] = expiry_stripped

		// VB specific stats
		listOfVbs := dcp_part.ResponsibleVBs()
		for _, i := range listOfVbs {
			r_collector.routerVbsIdMap[i] = conn.Id()
			metricsMap := make(map[string]interface{})
			r_collector.vbBasedHelper[i] = newVbBasedThroughSeqnoHelper(fmt.Sprintf("%v:%v", r_collector.Id(), i), r_collector.stats_mgr)
			for k, _ := range VBCountMetrics {
				metricsMap[k] = metrics.NewCounter()
				metrics.Register(fmt.Sprintf("%v:%v", conn.Id(), i), metricsMap[k])
			}
			r_collector.vbBasedMetric[i] = metricsMap
		}

		r_collector.component_map[conn.Id()] = metric_map
	}

	async_listener_map := pipeline_pkg.GetAllAsyncComponentEventListeners(pipeline)
	pipeline_utils.RegisterAsyncComponentEventHandler(async_listener_map, base.DataFilteredEventListener, r_collector)
	pipeline_utils.RegisterAsyncComponentEventHandler(async_listener_map, base.DataThroughputThrottledEventListener, r_collector)

	return nil
}

func (r_collector *routerCollector) Id() string {
	return r_collector.id
}

func (r_collector *routerCollector) handleVBEvent(event *common.Event, metricKey string) error {
	switch metricKey {
	case DOCS_FILTERED_METRIC:
		fallthrough
	case DOCS_UNABLE_TO_FILTER_METRIC:
		uprEvent := event.Data.(*mcc.UprEvent)
		vbucket := uprEvent.VBucket
		seqno := uprEvent.Seqno
		helper, ok := r_collector.vbBasedHelper[vbucket]
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
	var waitGrp sync.WaitGroup

	for vb, _ := range r_collector.vbBasedMetric {
		waitGrp.Add(1)
		go r_collector.handleLatestThroughSeqnoForVb(vb, SeqnoMap[vb], &waitGrp)
	}

	waitGrp.Wait()
}

func (r_collector *routerCollector) handleLatestThroughSeqnoForVb(vb uint16, latestSeqno uint64, waitGrp *sync.WaitGroup) {
	defer waitGrp.Done()
	metricsMap, ok := r_collector.vbBasedMetric[vb]
	if !ok {
		return
	}
	vbHelper, ok := r_collector.vbBasedHelper[vb]
	if !ok {
		return
	}

	vbHelper.mergeWithMetrics(metricsMap, latestSeqno)
}

func (r_collector *routerCollector) ProcessEvent(event *common.Event) error {
	metric_map := r_collector.component_map[event.Component.Id()]
	var err error
	switch event.EventType {
	case common.DataFiltered:
		uprEvent := event.Data.(*mcc.UprEvent)
		metric_map[DOCS_FILTERED_METRIC].(metrics.Counter).Inc(1)

		if uprEvent.Expiry != 0 {
			metric_map[EXPIRY_FILTERED_METRIC].(metrics.Counter).Inc(1)
		}
		if uprEvent.Opcode == mc.UPR_DELETION {
			metric_map[DELETION_FILTERED_METRIC].(metrics.Counter).Inc(1)
		} else if uprEvent.Opcode == mc.UPR_MUTATION {
			metric_map[SET_FILTERED_METRIC].(metrics.Counter).Inc(1)
		} else if uprEvent.Opcode == mc.UPR_EXPIRATION {
			metric_map[EXPIRY_FILTERED_METRIC].(metrics.Counter).Inc(1)
		} else {
			r_collector.stats_mgr.logger.Warnf("Invalid opcode, %v, in DataFiltered event from %v.", uprEvent.Opcode, event.Component.Id())
		}

		// Handle VB specific tasks
		err = r_collector.handleVBEvent(event, DOCS_FILTERED_METRIC)
	case common.DataUnableToFilter:
		metric_map[DOCS_UNABLE_TO_FILTER_METRIC].(metrics.Counter).Inc(1)
		// Handle VB specific tasks
		err = r_collector.handleVBEvent(event, DOCS_UNABLE_TO_FILTER_METRIC)
	case common.DataPoolGetFail:
		metric_map[DP_GET_FAIL_METRIC].(metrics.Counter).Inc(event.Data.(int64))
	case common.DataThroughputThrottled:
		throughput_throttle_latency := event.OtherInfos.(time.Duration)
		metric_map[THROUGHPUT_THROTTLE_LATENCY_METRIC].(metrics.Histogram).Sample().Update(throughput_throttle_latency.Nanoseconds() / 1000000)
	case common.ExpiryFieldStripped:
		metric_map[EXPIRY_STRIPPED_METRIC].(metrics.Counter).Inc(1)
	}

	return err
}

//metrics collector for checkpointmanager
type checkpointMgrCollector struct {
	stats_mgr *StatisticsManager
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
	registry_ckpt.Register(TIME_COMMITING_METRIC, metrics.NewHistogram(metrics.NewUniformSample(ckpt_collector.stats_mgr.sample_size)))
	registry_ckpt.Register(NUM_CHECKPOINTS_METRIC, metrics.NewCounter())
	registry_ckpt.Register(NUM_FAILEDCKPTS_METRIC, metrics.NewCounter())

}

func (ckpt_collector *checkpointMgrCollector) HandleLatestThroughSeqnos(SeqnoMap map[uint16]uint64) {
	// Do nothing
}

func (ckpt_collector *checkpointMgrCollector) OnEvent(event *common.Event) {
	registry := ckpt_collector.stats_mgr.registries["CkptMgr"]
	if event.EventType == common.ErrorEncountered {
		registry.Get(NUM_FAILEDCKPTS_METRIC).(metrics.Counter).Inc(1)

	} else if event.EventType == common.CheckpointDoneForVB {
		vbno := event.OtherInfos.(uint16)
		ckpt_record := event.Data.(metadata.CheckpointRecord)
		ckpt_collector.stats_mgr.checkpointed_seqnos[vbno].SetSeqno(ckpt_record.Seqno)

	} else if event.EventType == common.CheckpointDone {
		time_commit := event.OtherInfos.(time.Duration).Seconds() * 1000
		registry.Get(NUM_CHECKPOINTS_METRIC).(metrics.Counter).Inc(1)
		registry.Get(TIME_COMMITING_METRIC).(metrics.Histogram).Sample().Update(int64(time_commit))
	}
}

func setCounter(counter metrics.Counter, count int) {
	counter.Clear()
	counter.Inc(int64(count))
}

func (stats_mgr *StatisticsManager) getReplicationStatus() (*pipeline_pkg.ReplicationStatus, error) {
	topic := stats_mgr.pipeline.Topic()
	return pipeline_manager.ReplicationStatus(topic)
}

func UpdateStats(cluster_info_svc service_def.ClusterInfoSvc, xdcr_topology_svc service_def.XDCRCompTopologySvc,
	checkpoints_svc service_def.CheckpointsService, bucket_kv_mem_clients map[string]map[string]mcc.ClientIface,
	logger *log.CommonLogger, utils utilities.UtilsIface) {
	logger.Debug("updateStats for paused replications")

	for repl_id, repl_status := range pipeline_manager.ReplicationStatusMap() {
		overview_stats := repl_status.GetOverviewStats()
		spec := repl_status.Spec()

		if spec == nil {
			continue
		}

		cur_kv_vb_map, _, err := pipeline_utils.GetSourceVBMap(cluster_info_svc, xdcr_topology_svc, spec.SourceBucketName, logger)
		if err != nil {
			logger.Errorf("Error retrieving kv_vb_map for paused replication %v. err=%v", repl_id, err)
			continue
		}

		// get the kv_mem_client map for the corresponding source bucket
		kv_mem_clients := bucket_kv_mem_clients[spec.SourceBucketName]
		if kv_mem_clients == nil {
			kv_mem_clients = make(map[string]mcc.ClientIface)
			bucket_kv_mem_clients[spec.SourceBucketName] = kv_mem_clients
		}

		if overview_stats == nil {
			// overview stats may be nil the first time GetStats is called on a paused replication that has never been run in the current goxdcr session
			// or it may be nil when the underying replication is not paused but has not completed startup process
			// construct it
			err := constructStatsForReplication(repl_status, spec, cur_kv_vb_map, checkpoints_svc, kv_mem_clients, logger, utils)
			if err != nil {
				logger.Errorf("Error constructing stats for paused replication %v. err=%v", repl_id, err)
				continue
			}
		} else {
			if repl_status.RuntimeStatus(true) != pipeline_pkg.Replicating {
				err := updateStatsForReplication(repl_status, overview_stats, cur_kv_vb_map, checkpoints_svc, kv_mem_clients, logger, utils)
				if err != nil {
					logger.Errorf("Error updating stats for paused replication %v. err=%v", repl_id, err)
					continue
				}
			}
		}
	}
}

// compute and set changes_left and docs_processed stats. set other stats to 0
func constructStatsForReplication(repl_status *pipeline_pkg.ReplicationStatus, spec *metadata.ReplicationSpecification, cur_kv_vb_map map[string][]uint16,
	checkpoints_svc service_def.CheckpointsService, kv_mem_clients map[string]mcc.ClientIface,
	logger *log.CommonLogger, utils utilities.UtilsIface) error {
	cur_vb_list := base.GetVbListFromKvVbMap(cur_kv_vb_map)
	docs_processed, err := getDocsProcessedForReplication(spec.Id, cur_vb_list, checkpoints_svc, logger)
	if err != nil {
		return err
	}

	total_changes, err := calculateTotalChanges(cur_kv_vb_map, kv_mem_clients, spec.SourceBucketName, UserAgentPausedReplication, nil, logger, utils)
	if err != nil {
		return err
	}

	changes_left := total_changes - int64(docs_processed)

	logger.Infof("Calculating stats for never run replication %v. kv_vb_map=%v, total_docs=%v, docs_processed=%v, changes_left=%v\n", spec.Id, cur_kv_vb_map, total_changes, docs_processed, changes_left)

	overview_stats := new(expvar.Map).Init()
	overview_stats.Add(DOCS_PROCESSED_METRIC, int64(docs_processed))
	overview_stats.Add(CHANGES_LEFT_METRIC, changes_left)
	for _, statsToInitialize := range StatsToInitializeForPausedReplications {
		overview_stats.Add(statsToInitialize, 0)
	}

	repl_status.SetOverviewStats(overview_stats)

	// set vb list to establish the base for future stats update,
	// so that we can avoid re-computation when vb list does not change
	repl_status.SetVbList(cur_vb_list)

	return nil
}

func calculateTotalChanges(kv_vb_map map[string][]uint16, kv_mem_clients map[string]mcc.ClientIface,
	sourceBucketName string, user_agent string, stats_map map[string]string, logger *log.CommonLogger, utils utilities.UtilsIface) (int64, error) {
	var total_changes uint64 = 0
	for serverAddr, vbnos := range kv_vb_map {
		// as of now kv_vb_map contains only the current node and the connection is always local. use plain authentication
		client, err := utils.GetMemcachedClient(serverAddr, sourceBucketName, kv_mem_clients, user_agent, base.KeepAlivePeriod, logger)
		if err != nil {
			return 0, err
		}
		highseqno_map, err := getHighSeqNos(serverAddr, vbnos, client, stats_map, utils)
		if err != nil {
			logger.Warnf("error from getting high seqno for %v is %v\n", serverAddr, err)
			err1 := client.Close()
			if err1 != nil {
				logger.Warnf("error from closing connection for %v is %v\n", serverAddr, err1)
			}
			delete(kv_mem_clients, serverAddr)
			return 0, err
		}
		for _, vbno := range vbnos {
			current_vb_highseqno := highseqno_map[vbno]
			total_changes = total_changes + current_vb_highseqno
		}
	}
	return int64(total_changes), nil
}

func updateStatsForReplication(repl_status *pipeline_pkg.ReplicationStatus, overview_stats *expvar.Map, cur_kv_vb_map map[string][]uint16,
	checkpoints_svc service_def.CheckpointsService, kv_mem_clients map[string]mcc.ClientIface,
	logger *log.CommonLogger, utils utilities.UtilsIface) error {

	// if pipeline is not running, update docs_processed and changes_left stats, which are not being
	// updated by running pipeline and may have become inaccurate

	// first check if vb list on source side has changed.
	// if not, the doc_processed stats in overview stats is still accurate and we will just use it
	// otherwise, need to re-compute docs_processed stats by filtering the checkpoint docs using the current kv_vb_map

	var docs_processed int64
	var docs_processed_uint64 uint64
	var err error
	// old_vb_list is already sorted
	old_vb_list := repl_status.VbList()
	spec := repl_status.Spec()
	if spec == nil {
		logger.Infof("replication %v has been deleted, skip updating stats\n", repl_status.RepId())
		return nil
	}

	cur_vb_list := base.GetVbListFromKvVbMap(cur_kv_vb_map)
	base.SortUint16List(cur_vb_list)
	sameList := base.AreSortedUint16ListsTheSame(old_vb_list, cur_vb_list)
	if sameList {
		docs_processed, err = strconv.ParseInt(overview_stats.Get(DOCS_PROCESSED_METRIC).String(), base.ParseIntBase, base.ParseIntBitSize)
		if err != nil {
			return err
		}

	} else {
		logger.Infof("%v Source topology changed. Re-compute docs_processed. old_vb_list=%v, cur_vb_list=%v\n", repl_status.RepId(), old_vb_list, cur_vb_list)
		docs_processed_uint64, err = getDocsProcessedForReplication(spec.Id, cur_vb_list, checkpoints_svc, logger)
		if err != nil {
			return err
		}
		docs_processed = int64(docs_processed_uint64)
		docs_processed_var := new(expvar.Int)
		docs_processed_var.Set(docs_processed)
		overview_stats.Set(DOCS_PROCESSED_METRIC, docs_processed_var)

		repl_status.SetVbList(cur_vb_list)
	}

	total_changes, err := calculateTotalChanges(cur_kv_vb_map, kv_mem_clients, spec.SourceBucketName, UserAgentPausedReplication, nil, logger, utils)
	if err != nil {
		return err
	}

	changes_left := total_changes - docs_processed
	changes_left_var := new(expvar.Int)
	changes_left_var.Set(changes_left)

	overview_stats.Set(CHANGES_LEFT_METRIC, changes_left_var)

	logger.Infof("Updating status for paused replication %v. kv_vb_map=%v, total_docs=%v, docs_processed=%v, changes_left=%v\n", spec.Id, cur_kv_vb_map, total_changes, docs_processed, changes_left)
	return nil
}

func StatsUpdateInterval(settings metadata.ReplicationSettingsMap) time.Duration {
	update_interval := default_update_interval
	if _, ok := settings[PUBLISH_INTERVAL]; ok {
		update_interval = settings[PUBLISH_INTERVAL].(int)
	}
	return time.Duration(update_interval) * time.Millisecond
}

func (stats_mgr *StatisticsManager) GetCountMetrics(key string) (int64, error) {
	overviewRegistry, ok := stats_mgr.registries[OVERVIEW_METRICS_KEY]
	if !ok || overviewRegistry == nil {
		return 0, base.ErrorResourceDoesNotExist
	}
	registry := overviewRegistry.Get(key)
	if registry == nil {
		return 0, base.ErrorInvalidInput
	}
	return registry.(metrics.Counter).Count(), nil
}

func (stats_mgr *StatisticsManager) GetVBCountMetrics(vb uint16) (VBCountMetricMap, error) {
	// Currently only DCP has vb specific stats
	vbBasedMetric, ok := stats_mgr.getRouterCollector().vbBasedMetric[vb]
	if !ok {
		return nil, base.ErrorNotMyVbucket
	}

	metricsMap := make(map[string]int64)
	for k, _ := range VBCountMetrics {
		registry, ok := (vbBasedMetric[k])
		if !ok {
			continue
		}
		counter := registry.(metrics.Counter)
		metricsMap[k] = counter.Count()
	}

	return metricsMap, nil
}

func (stats_mgr *StatisticsManager) SetVBCountMetrics(vb uint16, metricKVs VBCountMetricMap) error {
	// Currently only DCP has vb specific stats
	vbBasedMetric, ok := stats_mgr.getRouterCollector().vbBasedMetric[vb]
	if !ok {
		return base.ErrorNotMyVbucket
	}

	// First find the router part responsible for this vb
	routerId, found := stats_mgr.getRouterCollector().routerVbsIdMap[vb]
	if !found {
		return base.ErrorNotMyVbucket
	}

	registry_router := stats_mgr.registries[routerId]
	if registry_router == nil {
		return fmt.Errorf("Unable to find registry for router ID %v", routerId)
	}

	// For keys that are passed in, set the counter appropriately
	for k, v := range metricKVs {
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
		metricsIface := registry_router.Get(k)
		if metricsIface == nil {
			return fmt.Errorf("%v Unable to get metric\n", routerId)
		}
		counter, ok = metricsIface.(metrics.Counter)
		if !ok || counter == nil {
			return fmt.Errorf("%v Unable to get metric counter\n", routerId)
		}
		counter.Inc(difference)
	}

	return nil
}

func (statsMgr *StatisticsManager) HandleLatestThroughSeqnos(SeqnoMap map[uint16]uint64) {
	for _, collector := range statsMgr.collectors {
		collector.HandleLatestThroughSeqnos(SeqnoMap)
	}
}
