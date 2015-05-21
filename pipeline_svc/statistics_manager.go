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
	"encoding/binary"
	"errors"
	"expvar"
	"fmt"
	"github.com/couchbase/gomemcached"
	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	base "github.com/couchbase/goxdcr/base"
	common "github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	parts "github.com/couchbase/goxdcr/parts"
	"github.com/couchbase/goxdcr/pipeline"
	"github.com/couchbase/goxdcr/pipeline_manager"
	"github.com/couchbase/goxdcr/pipeline_utils"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/utils"
	"github.com/rcrowley/go-metrics"
	"reflect"
	"strconv"
	"sync"
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
	DOCS_REP_QUEUE_METRIC  = "docs_rep_queue"

	DOCS_FILTERED_METRIC     = "docs_filtered"
	EXPIRY_FILTERED_METRIC   = "expiry_filtered"
	DELETION_FILTERED_METRIC = "deletion_filtered"
	SET_FILTERED_METRIC      = "set_filtered"

	// the number of docs that failed conflict resolution on the source cluster side due to optimistic replication
	DOCS_FAILED_CR_SOURCE_METRIC     = "docs_failed_cr_source"
	EXPIRY_FAILED_CR_SOURCE_METRIC   = "expiry_failed_cr_source"
	DELETION_FAILED_CR_SOURCE_METRIC = "deletion_failed_cr_source"
	SET_FAILED_CR_SOURCE_METRIC      = "set_failed_cr_source"

	CHANGES_LEFT_METRIC = "changes_left"
	DOCS_LATENCY_METRIC = "wtavg_docs_latency"
	META_LATENCY_METRIC = "wtavg_meta_latency"

	//checkpointing related statistics
	DOCS_CHECKED_METRIC    = "docs_checked" //calculated
	NUM_CHECKPOINTS_METRIC = "num_checkpoints"
	TIME_COMMITING_METRIC  = "time_committing"
	NUM_FAILEDCKPTS_METRIC = "num_failedckpts"
	RATE_DOC_CHECKS_METRIC = "rate_doc_checks"
	//optimistic replication replated statistics
	DOCS_OPT_REPD_METRIC = "docs_opt_repd"
	RATE_OPT_REPD_METRIC = "rate_doc_opt_repd"

	DOCS_RECEIVED_DCP_METRIC = "docs_received_from_dcp"
	RATE_RECEIVED_DCP_METRIC = "rate_received_from_dcp"

	EXPIRY_RECEIVED_DCP_METRIC   = "expiry_received_from_dcp"
	DELETION_RECEIVED_DCP_METRIC = "deletion_received_from_dcp"
	SET_RECEIVED_DCP_METRIC      = "set_received_from_dcp"

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
	VB_START_TS          = "v_start_ts"
)

const (
	default_sample_size     = 1000
	default_update_interval = 100 * time.Millisecond
	default_log_stats_interval = 10000 * time.Millisecond
)

// stats to initialize for paused replications that have never been run -- mostly the stats visible from UI
var StatsToInitializeForPausedReplications = [10]string{DOCS_WRITTEN_METRIC, DOCS_FAILED_CR_SOURCE_METRIC, DOCS_FILTERED_METRIC,
	RATE_DOC_CHECKS_METRIC, RATE_OPT_REPD_METRIC, RATE_RECEIVED_DCP_METRIC, RATE_REPLICATED_METRIC,
	BANDWIDTH_USAGE_METRIC, DOCS_LATENCY_METRIC, META_LATENCY_METRIC}

// stats to clear when replications are paused
// 1. all rate type stats
// 2. internal stats that are not visible on UI
var StatsToClearForPausedReplications = [13]string{SIZE_REP_QUEUE_METRIC, DOCS_REP_QUEUE_METRIC, DOCS_LATENCY_METRIC, META_LATENCY_METRIC,
	TIME_COMMITING_METRIC, NUM_FAILEDCKPTS_METRIC, RATE_DOC_CHECKS_METRIC, RATE_OPT_REPD_METRIC, RATE_RECEIVED_DCP_METRIC,
	RATE_REPLICATED_METRIC, BANDWIDTH_USAGE_METRIC}

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

	//temporary map to keep all the collected start time for getMeta requests
	//during this collection interval.
	//At the end of the collection interval, collected starttime and endtime will be correlated
	//to calculate the latencies for getMeta. The calculated latencies will be kept in "Overall"
	//entry in registries.
	//This map will be emptied after the latencies are calculated to get ready for
	//next collection period
	meta_starttime_map      map[string]interface{}
	meta_starttime_map_lock sync.RWMutex

	//temporary map to keep all the collected end time for getMeta requests during this collection
	//interval.
	//This map will be emptied after the latencies are calculated to get ready for
	//next collection period
	meta_endtime_map      map[string]interface{}
	meta_endtime_map_lock sync.RWMutex

	//temporary map to keep checkpointed seqnos
	checkpointed_seqnos map[uint16]uint64

	//chan for stats update tickers -- new tickers are added each time stats interval is changed
	update_ticker_ch chan *time.Ticker

	//settings - sample size
	sample_size int
	//settings - statistics update interval
	update_interval time.Duration

	//the channel to communicate finish signal with statistic updater
	finish_ch chan bool
	done_ch   chan bool
	wait_grp  *sync.WaitGroup

	pipeline common.Pipeline

	logger *log.CommonLogger

	collectors []MetricsCollector

	active_vbs     map[string][]uint16
	bucket_name    string
	kv_mem_clients map[string]*mcc.Client

	through_seqno_tracker_svc service_def.ThroughSeqnoTrackerSvc
}

func NewStatisticsManager(through_seqno_tracker_svc service_def.ThroughSeqnoTrackerSvc,
	logger_ctx *log.LoggerContext, active_vbs map[string][]uint16, bucket_name string) *StatisticsManager {
	stats_mgr := &StatisticsManager{registries: make(map[string]metrics.Registry),
		bucket_name:               bucket_name,
		meta_starttime_map:        make(map[string]interface{}),
		meta_endtime_map:          make(map[string]interface{}),
		meta_starttime_map_lock:   sync.RWMutex{},
		meta_endtime_map_lock:     sync.RWMutex{},
		finish_ch:                 make(chan bool, 1),
		done_ch:                   make(chan bool, 1),
		update_ticker_ch:          make(chan *time.Ticker, 1000),
		sample_size:               default_sample_size,
		update_interval:           default_update_interval,
		logger:                    log.NewLogger("StatisticsManager", logger_ctx),
		active_vbs:                active_vbs,
		wait_grp:                  &sync.WaitGroup{},
		kv_mem_clients:            make(map[string]*mcc.Client),
		checkpointed_seqnos:       make(map[uint16]uint64),
		through_seqno_tracker_svc: through_seqno_tracker_svc}
	stats_mgr.collectors = []MetricsCollector{&outNozzleCollector{}, &dcpCollector{}, &routerCollector{}, &checkpointMgrCollector{}}
	return stats_mgr
}

//Statistics of a pipeline which may or may not be running
func GetStatisticsForPipeline(topic string) (*expvar.Map, error) {
	repl_status := pipeline_manager.ReplicationStatus(topic)
	if repl_status == nil {
		return nil, nil
	}

	return repl_status.GetOverviewStats(), nil
}

func (stats_mgr *StatisticsManager) cleanupBeforeExit() error {
	rs, err := stats_mgr.getReplicationStatus()
	if err != nil {
		return err
	}
	rs.CleanupBeforeExit(StatsToClearForPausedReplications[:])
	statsLog, _ := stats_mgr.formatStatsForLog()
	stats_mgr.logger.Infof("expvar=%v\n", statsLog)
	return nil
}

func getHighSeqNos(serverAddr string, vbnos []uint16, conn *mcc.Client) (map[uint16]uint64, error) {
	highseqno_map := make(map[uint16]uint64)

	stats_map, err := conn.StatsMap(base.VBUCKET_SEQNO_STAT_NAME)
	if err != nil {
		return nil, err
	}

	err = utils.ParseHighSeqnoStat(vbnos, stats_map, highseqno_map)
	return highseqno_map, err
}

//updateStats runs until it get finish signal
//It processes the raw stats and publish the overview stats along with the raw stats to expvar
//It also log the stats to log
func (stats_mgr *StatisticsManager) updateStats() error {
	stats_mgr.logger.Info("updateStats started")

	defer stats_mgr.wait_grp.Done()
	defer close(stats_mgr.done_ch)

	ticker := <-stats_mgr.update_ticker_ch
	logStats_ticker := time.NewTicker(default_log_stats_interval)

	init_ch := make(chan bool, 1)
	init_ch <- true
	for {
		select {
		case new_ticker := <-stats_mgr.update_ticker_ch:
			stats_mgr.logger.Info("Received new ticker due to changes to stats interval setting")
			ticker.Stop()
			ticker = new_ticker
		case <-stats_mgr.finish_ch:
			stats_mgr.cleanupBeforeExit()
			return nil
		// this ensures that stats are printed out immediately after updateStats is started
		case <-init_ch:
			err := stats_mgr.updateStatsOnce()
			if err != nil {
				return nil
			}
		case <-ticker.C:
			err := stats_mgr.updateStatsOnce()
			if err != nil {
				return nil
			}
		case <-logStats_ticker.C:
			err := stats_mgr.logStats()
			if 	err != nil {
				stats_mgr.logger.Infof("Failed to log statistics. err=%v\n", err)
			}
		}
	}
	return nil
}

func (stats_mgr *StatisticsManager) updateStatsOnce() error {
	if stats_mgr.pipeline.State() != common.Pipeline_Running && stats_mgr.pipeline.State() != common.Pipeline_Starting {
		//the pipeline is no longer running, kill myself
		message := "Pipeline is no longer running, exit."
		stats_mgr.logger.Info(message)
		stats_mgr.cleanupBeforeExit()
		return errors.New(message)
	}

	stats_mgr.logger.Debugf("%v: Publishing the statistics for %v to expvar", time.Now(), stats_mgr.pipeline.InstanceId())
	err := stats_mgr.processRawStats()

	if err != nil {
		stats_mgr.logger.Info("Failed to calculate the statistics for this round. Move on")
	}
	return nil
}

func (stats_mgr *StatisticsManager) logStats() error {
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
				stats_mgr.logger.Info(part.(*parts.XmemNozzle).StatusSummary())
			} else {
				stats_mgr.logger.Info(part.(*parts.CapiNozzle).StatusSummary())
			}
		}
		dcp_parts := stats_mgr.pipeline.Sources()
		for _, part := range dcp_parts {
			conn := part.Connector()
			stats_mgr.logger.Info(conn.(*parts.Router).StatusSummary())
			stats_mgr.logger.Info(part.(*parts.DcpNozzle).StatusSummary())
		}
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
func (stats_mgr *StatisticsManager) processRawStats() error {
	rs, err := stats_mgr.getReplicationStatus()
	if err != nil {
		return err
	}

	oldSample := stats_mgr.getOverviewRegistry()
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
					if orig_registry != nil {
						orig_val, _ := strconv.ParseInt(orig_registry.Get(name).String(), 10, 64)
						if m.Count() < orig_val {
							stats_mgr.logger.Infof("counter %v goes backward, maybe due to the pipeline is restarted\n", name)
						}
					}
					metric_overview := stats_mgr.getOverviewRegistry().Get(name)
					if metric_overview != nil {
						stats_mgr.logger.Debugf("Update counter %v by %v in overview registry", name, m.Count())
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
	err = stats_mgr.processCalculatedStats(oldSample, map_for_overview)
	if err != nil {
		return err
	}

	stats_mgr.logger.Debugf("Overview=%v for pipeline %v\n", map_for_overview, stats_mgr.pipeline.Topic())
	rs.SetOverviewStats(map_for_overview)
	return nil
}

func (stats_mgr *StatisticsManager) processCalculatedStats(oldSample metrics.Registry, overview_expvar_map *expvar.Map) error {

	//calculate docs_processed
	docs_processed := stats_mgr.calculateDocsProcessed()
	docs_processed_var := new(expvar.Int)
	docs_processed_var.Set(docs_processed)
	overview_expvar_map.Set(DOCS_PROCESSED_METRIC, docs_processed_var)

	//calculate changes_left
	changes_left_val, err := stats_mgr.calculateChangesLeft(docs_processed)
	changes_left_var := new(expvar.Int)
	if err == nil {
		changes_left_var.Set(changes_left_val)
	} else {
		stats_mgr.logger.Errorf("Failed to calculate changes_left - %v\n", err)
		changes_left_var.Set(-1)
	}
	overview_expvar_map.Set(CHANGES_LEFT_METRIC, changes_left_var)

	//calculate rate_replication
	docs_written := stats_mgr.getOverviewRegistry().Get(DOCS_WRITTEN_METRIC).(metrics.Counter).Count()
	docs_written_old := oldSample.Get(DOCS_WRITTEN_METRIC).(metrics.Counter).Count()
	interval_in_sec := stats_mgr.update_interval.Seconds()
	rate_replicated := float64(docs_written-docs_written_old) / interval_in_sec
	rate_replicated_var := new(expvar.Float)
	rate_replicated_var.Set(rate_replicated)
	overview_expvar_map.Set(RATE_REPLICATED_METRIC, rate_replicated_var)

	//calculate rate_received_from_dcp
	docs_received_dcp := stats_mgr.getOverviewRegistry().Get(DOCS_RECEIVED_DCP_METRIC).(metrics.Counter).Count()
	docs_received_dcp_old := oldSample.Get(DOCS_RECEIVED_DCP_METRIC).(metrics.Counter).Count()
	rate_received_dcp := float64(docs_received_dcp-docs_received_dcp_old) / interval_in_sec
	rate_received_dcp_var := new(expvar.Float)
	rate_received_dcp_var.Set(rate_received_dcp)
	overview_expvar_map.Set(RATE_RECEIVED_DCP_METRIC, rate_received_dcp_var)

	//calculate rate_doc_opt_repd
	docs_opt_repd_old := oldSample.Get(DOCS_OPT_REPD_METRIC).(metrics.Counter).Count()
	docs_opt_repd := stats_mgr.getOverviewRegistry().Get(DOCS_OPT_REPD_METRIC).(metrics.Counter).Count()
	rate_opt_repd := float64(docs_opt_repd-docs_opt_repd_old) / interval_in_sec
	rate_opt_repd_var := new(expvar.Float)
	rate_opt_repd_var.Set(rate_opt_repd)
	overview_expvar_map.Set(RATE_OPT_REPD_METRIC, rate_opt_repd_var)

	//calculate bandwidth_usage
	data_replicated_old := oldSample.Get(DATA_REPLICATED_METRIC).(metrics.Counter).Count()
	data_replicated := stats_mgr.getOverviewRegistry().Get(DATA_REPLICATED_METRIC).(metrics.Counter).Count()
	bandwidth_usage := float64(data_replicated-data_replicated_old) / interval_in_sec
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
	docs_checked_old_var := oldSample.Get(DOCS_CHECKED_METRIC)
	var docs_checked_old int64 = 0
	if docs_checked_old_var != nil {
		docs_checked_old = docs_checked_old_var.(metrics.Counter).Count()
	}
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
	for vbno, vbts := range GetStartSeqnos(stats_mgr.pipeline, stats_mgr.logger) {
		start_seqno := vbts.Seqno
		var docs_checked_vb uint64 = 0
		if stats_mgr.checkpointed_seqnos[vbno] > start_seqno {
			docs_checked_vb = stats_mgr.checkpointed_seqnos[vbno]
		} else {
			docs_checked_vb = start_seqno
		}
		docs_checked = docs_checked + docs_checked_vb
	}
	return docs_checked
}
func (stats_mgr *StatisticsManager) calculateChangesLeft(docs_processed int64) (int64, error) {
	total_changes, err := calculateTotalChanges(stats_mgr.active_vbs, stats_mgr.kv_mem_clients, "", stats_mgr.logger)
	if err != nil {
		return 0, err
	}
	changes_left := total_changes - docs_processed
	return changes_left, nil
}

func (stats_mgr *StatisticsManager) getOverviewRegistry() metrics.Registry {
	return stats_mgr.registries[OVERVIEW_METRICS_KEY]
}

func (stats_mgr *StatisticsManager) publishMetricToMap(expvar_map *expvar.Map, name string, i interface{}, includeDetails bool) {
	switch m := i.(type) {
	case metrics.Counter:
		expvar_map.Set(name, expvar.Func(func() interface{} {
			return m.Count()
		}))
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

	//mount collectors with pipeline
	for _, collector := range stats_mgr.collectors {
		collector.Mount(pipeline, stats_mgr)
	}

	//register the aggregation metrics for the pipeline
	stats_mgr.initOverviewRegistry()
	stats_mgr.logger.Infof("StatisticsManager is started for pipeline %v", stats_mgr.pipeline.Topic)

	return nil
}

func (stats_mgr *StatisticsManager) initOverviewRegistry() {
	// preserve old docs_checked if it exists. use a negative value to indicate that it does not exist
	var old_docs_checked int = -1
	old_overview := stats_mgr.registries[OVERVIEW_METRICS_KEY]
	if old_overview != nil {
		old_docs_checked = int(old_overview.Get(DOCS_CHECKED_METRIC).(metrics.Counter).Count())
	}
	docs_checked_counter := metrics.NewCounter()
	setCounter(docs_checked_counter, old_docs_checked)

	overview_registry := metrics.NewRegistry()
	stats_mgr.registries[OVERVIEW_METRICS_KEY] = overview_registry
	overview_registry.Register(DOCS_WRITTEN_METRIC, metrics.NewCounter())
	overview_registry.Register(EXPIRY_DOCS_WRITTEN_METRIC, metrics.NewCounter())
	overview_registry.Register(DELETION_DOCS_WRITTEN_METRIC, metrics.NewCounter())
	overview_registry.Register(SET_DOCS_WRITTEN_METRIC, metrics.NewCounter())
	overview_registry.Register(DOCS_PROCESSED_METRIC, metrics.NewCounter())
	overview_registry.Register(DOCS_FAILED_CR_SOURCE_METRIC, metrics.NewCounter())
	overview_registry.Register(EXPIRY_FAILED_CR_SOURCE_METRIC, metrics.NewCounter())
	overview_registry.Register(DELETION_FAILED_CR_SOURCE_METRIC, metrics.NewCounter())
	overview_registry.Register(SET_FAILED_CR_SOURCE_METRIC, metrics.NewCounter())
	overview_registry.Register(DATA_REPLICATED_METRIC, metrics.NewCounter())
	overview_registry.Register(DOCS_FILTERED_METRIC, metrics.NewCounter())
	overview_registry.Register(EXPIRY_FILTERED_METRIC, metrics.NewCounter())
	overview_registry.Register(DELETION_FILTERED_METRIC, metrics.NewCounter())
	overview_registry.Register(SET_FILTERED_METRIC, metrics.NewCounter())
	overview_registry.Register(NUM_CHECKPOINTS_METRIC, metrics.NewCounter())
	overview_registry.Register(NUM_FAILEDCKPTS_METRIC, metrics.NewCounter())
	overview_registry.Register(TIME_COMMITING_METRIC, metrics.NewCounter())
	overview_registry.Register(DOCS_OPT_REPD_METRIC, metrics.NewCounter())
	overview_registry.Register(DOCS_RECEIVED_DCP_METRIC, metrics.NewCounter())
	overview_registry.Register(EXPIRY_RECEIVED_DCP_METRIC, metrics.NewCounter())
	overview_registry.Register(DELETION_RECEIVED_DCP_METRIC, metrics.NewCounter())
	overview_registry.Register(SET_RECEIVED_DCP_METRIC, metrics.NewCounter())
	overview_registry.Register(SIZE_REP_QUEUE_METRIC, metrics.NewCounter())
	overview_registry.Register(DOCS_REP_QUEUE_METRIC, metrics.NewCounter())
	overview_registry.Register(DOCS_LATENCY_METRIC, metrics.NewCounter())
	overview_registry.Register(META_LATENCY_METRIC, metrics.NewCounter())
	overview_registry.Register(DOCS_CHECKED_METRIC, docs_checked_counter)
}

func (stats_mgr *StatisticsManager) Start(settings map[string]interface{}) error {
	stats_mgr.logger.Infof("StatisticsManager Starting...")

	//initialize connection
	err := stats_mgr.initConnections()
	if err != nil {
		return err
	}

	if _, ok := settings[PUBLISH_INTERVAL]; ok {
		stats_mgr.update_interval = time.Duration(settings[PUBLISH_INTERVAL].(int)) * time.Millisecond
	} else {
		stats_mgr.logger.Infof("There is no update_interval in settings map. settings=%v\n", settings)
	}
	stats_mgr.logger.Debugf("StatisticsManager Starts: update_interval=%v, settings=%v\n", stats_mgr.update_interval, settings)
	stats_mgr.update_ticker_ch <- time.NewTicker(stats_mgr.update_interval)

	stats_mgr.wait_grp.Add(1)
	go stats_mgr.updateStats()

	return nil
}

func (stats_mgr *StatisticsManager) Stop() error {
	stats_mgr.logger.Infof("StatisticsManager Stopping...")
	stats_mgr.finish_ch <- true

	//close the connections
	for _, client := range stats_mgr.kv_mem_clients {
		client.Close()
	}

	stats_mgr.wait_grp.Wait()
	stats_mgr.logger.Infof("StatisticsManager Stopped")

	return nil
}

func (stats_mgr *StatisticsManager) initConnections() error {
	for serverAddr, _ := range stats_mgr.active_vbs {
		conn, err := utils.GetMemcachedConnection(serverAddr, stats_mgr.bucket_name, stats_mgr.logger)
		if err != nil {
			return err
		}

		stats_mgr.kv_mem_clients[serverAddr] = conn
	}

	return nil
}

func (stats_mgr *StatisticsManager) UpdateSettings(settings map[string]interface{}) error {
	stats_mgr.logger.Debugf("Updating settings on stats manager. settings=%v\n", settings)

	stats_interval, err := utils.GetIntSettingFromSettings(settings, PUBLISH_INTERVAL)
	if err != nil {
		return err
	}

	if stats_interval < 0 {
		// stats_interval not specified. no op
		return nil
	}

	if int(stats_mgr.update_interval.Nanoseconds()) == stats_interval*1000000 {
		// no op if no real updates
		return nil
	}

	stats_mgr.update_interval = time.Duration(stats_interval) * time.Millisecond
	stats_mgr.update_ticker_ch <- time.NewTicker(stats_mgr.update_interval)

	return nil
}

type MetricsCollector interface {
	Mount(pipeline common.Pipeline, stats_mgr *StatisticsManager) error
}

//metrics collector for XMem/CapiNozzle
type outNozzleCollector struct {
	stats_mgr *StatisticsManager
}

func (outNozzle_collector *outNozzleCollector) Mount(pipeline common.Pipeline, stats_mgr *StatisticsManager) error {
	outNozzle_collector.stats_mgr = stats_mgr
	outNozzle_parts := pipeline.Targets()
	for _, part := range outNozzle_parts {
		registry := stats_mgr.getOrCreateRegistry(part.Id())
		registry.Register(SIZE_REP_QUEUE_METRIC, metrics.NewCounter())
		registry.Register(DOCS_REP_QUEUE_METRIC, metrics.NewCounter())
		registry.Register(DOCS_WRITTEN_METRIC, metrics.NewCounter())
		registry.Register(EXPIRY_DOCS_WRITTEN_METRIC, metrics.NewCounter())
		registry.Register(DELETION_DOCS_WRITTEN_METRIC, metrics.NewCounter())
		registry.Register(SET_DOCS_WRITTEN_METRIC, metrics.NewCounter())
		registry.Register(DOCS_FAILED_CR_SOURCE_METRIC, metrics.NewCounter())
		registry.Register(EXPIRY_FAILED_CR_SOURCE_METRIC, metrics.NewCounter())
		registry.Register(DELETION_FAILED_CR_SOURCE_METRIC, metrics.NewCounter())
		registry.Register(SET_FAILED_CR_SOURCE_METRIC, metrics.NewCounter())
		registry.Register(DATA_REPLICATED_METRIC, metrics.NewCounter())
		registry.Register(DOCS_OPT_REPD_METRIC, metrics.NewCounter())
		registry.Register(DOCS_LATENCY_METRIC, metrics.NewHistogram(metrics.NewUniformSample(stats_mgr.sample_size)))
		registry.Register(META_LATENCY_METRIC, metrics.NewHistogram(metrics.NewUniformSample(stats_mgr.sample_size)))

		part.RegisterComponentEventListener(common.DataSent, outNozzle_collector)
		part.RegisterComponentEventListener(common.DataFailedCRSource, outNozzle_collector)
		part.RegisterComponentEventListener(common.StatsUpdate, outNozzle_collector)
		part.RegisterComponentEventListener(common.GetMetaReceived, outNozzle_collector)

	}
	return nil
}

func (outNozzle_collector *outNozzleCollector) OnEvent(eventType common.ComponentEventType,
	item interface{},
	component common.Component,
	derivedItems []interface{},
	otherInfos map[string]interface{}) {
	if eventType == common.StatsUpdate {
		outNozzle_collector.stats_mgr.logger.Debugf("Received a StatsUpdate event from %v", reflect.TypeOf(component))
		queue_size := otherInfos[parts.STATS_QUEUE_SIZE].(int)
		queue_size_bytes := otherInfos[parts.STATS_QUEUE_SIZE_BYTES].(int)
		registry := outNozzle_collector.stats_mgr.registries[component.Id()]
		setCounter(registry.Get(DOCS_REP_QUEUE_METRIC).(metrics.Counter), queue_size)
		setCounter(registry.Get(SIZE_REP_QUEUE_METRIC).(metrics.Counter), queue_size_bytes)
	} else if eventType == common.DataSent {
		outNozzle_collector.stats_mgr.logger.Debugf("Received a DataSent event from %v", reflect.TypeOf(component))
		req := item.(*gomemcached.MCRequest)
		size := req.Size()
		opti_replicated := otherInfos[parts.EVENT_ADDI_OPT_REPD].(bool)
		commit_time := otherInfos[parts.EVENT_ADDI_SETMETA_COMMIT_TIME].(time.Duration)
		registry := outNozzle_collector.stats_mgr.registries[component.Id()]
		registry.Get(DOCS_WRITTEN_METRIC).(metrics.Counter).Inc(1)
		registry.Get(DATA_REPLICATED_METRIC).(metrics.Counter).Inc(int64(size))
		if opti_replicated {
			registry.Get(DOCS_OPT_REPD_METRIC).(metrics.Counter).Inc(1)
		}

		expiry := binary.BigEndian.Uint32(req.Extras[4:8])
		if expiry != 0 {
			registry.Get(EXPIRY_DOCS_WRITTEN_METRIC).(metrics.Counter).Inc(1)
		}
		if req.Opcode == base.DELETE_WITH_META {
			registry.Get(DELETION_DOCS_WRITTEN_METRIC).(metrics.Counter).Inc(1)
		} else if req.Opcode == base.SET_WITH_META {
			registry.Get(SET_DOCS_WRITTEN_METRIC).(metrics.Counter).Inc(1)
		} else {
			panic(fmt.Sprintf("Invalid opcode, %v, in DataSent event from %v.", req.Opcode, component.Id()))
		}

		time_committing_reg := registry.Get(DOCS_LATENCY_METRIC).(metrics.Histogram)
		sample := time_committing_reg.Sample()
		sample.Update(commit_time.Nanoseconds() / 1000000)
	} else if eventType == common.DataFailedCRSource {
		outNozzle_collector.stats_mgr.logger.Debugf("Received a DataFailedCRSource event from %v", reflect.TypeOf(component))
		req := item.(*gomemcached.MCRequest)
		registry := outNozzle_collector.stats_mgr.registries[component.Id()]
		registry.Get(DOCS_FAILED_CR_SOURCE_METRIC).(metrics.Counter).Inc(1)

		expiry := binary.BigEndian.Uint32(req.Extras[4:8])
		if expiry != 0 {
			registry.Get(EXPIRY_FAILED_CR_SOURCE_METRIC).(metrics.Counter).Inc(1)
		}
		if req.Opcode == mc.UPR_DELETION {
			registry.Get(DELETION_FAILED_CR_SOURCE_METRIC).(metrics.Counter).Inc(1)
		} else if req.Opcode == mc.UPR_MUTATION {
			registry.Get(SET_FAILED_CR_SOURCE_METRIC).(metrics.Counter).Inc(1)
		} else {
			panic(fmt.Sprintf("Invalid opcode, %v, in DataFailedCRSource event from %v.", req.Opcode, component.Id()))
		}
	} else if eventType == common.GetMetaReceived {
		outNozzle_collector.stats_mgr.logger.Debugf("Received a GetMetaReceived event from %v", reflect.TypeOf(component))

		commit_time := otherInfos[parts.EVENT_ADDI_GETMETA_COMMIT_TIME].(time.Duration)

		registry := outNozzle_collector.stats_mgr.registries[component.Id()]
		time_committing_reg := registry.Get(META_LATENCY_METRIC).(metrics.Histogram)
		sample := time_committing_reg.Sample()
		sample.Update(commit_time.Nanoseconds() / 1000000)
	}
}

func getStatsKeyFromDocKeyAndSeqno(key string, seqno uint64) string {
	return fmt.Sprintf("%v-%v", key, seqno)
}

//metrics collector for DcpNozzle
type dcpCollector struct {
	stats_mgr *StatisticsManager
}

func (dcp_collector *dcpCollector) Mount(pipeline common.Pipeline, stats_mgr *StatisticsManager) error {
	dcp_collector.stats_mgr = stats_mgr
	dcp_parts := pipeline.Sources()
	for _, dcp_part := range dcp_parts {
		registry := stats_mgr.getOrCreateRegistry(dcp_part.Id())
		registry.Register(DOCS_RECEIVED_DCP_METRIC, metrics.NewCounter())
		registry.Register(EXPIRY_RECEIVED_DCP_METRIC, metrics.NewCounter())
		registry.Register(DELETION_RECEIVED_DCP_METRIC, metrics.NewCounter())
		registry.Register(SET_RECEIVED_DCP_METRIC, metrics.NewCounter())
		dcp_part.RegisterComponentEventListener(common.DataReceived, dcp_collector)
	}
	return nil
}

func (dcp_collector *dcpCollector) OnEvent(eventType common.ComponentEventType,
	item interface{},
	component common.Component,
	derivedItems []interface{},
	otherInfos map[string]interface{}) {
	if eventType == common.DataReceived {
		dcp_collector.stats_mgr.logger.Debugf("Received a DataReceived event from %v", reflect.TypeOf(component))
		uprEvent := item.(*mcc.UprEvent)
		registry := dcp_collector.stats_mgr.registries[component.Id()]
		registry.Get(DOCS_RECEIVED_DCP_METRIC).(metrics.Counter).Inc(1)

		if uprEvent.Expiry != 0 {
			registry.Get(EXPIRY_RECEIVED_DCP_METRIC).(metrics.Counter).Inc(1)
		}
		if uprEvent.Opcode == mc.UPR_DELETION {
			registry.Get(DELETION_RECEIVED_DCP_METRIC).(metrics.Counter).Inc(1)
		} else if uprEvent.Opcode == mc.UPR_MUTATION {
			registry.Get(SET_RECEIVED_DCP_METRIC).(metrics.Counter).Inc(1)
		} else {
			panic(fmt.Sprintf("Invalid opcode, %v, in DataReceived event from %v.", uprEvent.Opcode, component.Id()))
		}
	}
}

//metrics collector for Router
type routerCollector struct {
	stats_mgr *StatisticsManager
}

func (r_collector *routerCollector) Mount(pipeline common.Pipeline, stats_mgr *StatisticsManager) error {
	r_collector.stats_mgr = stats_mgr
	dcp_parts := pipeline.Sources()
	for _, dcp_part := range dcp_parts {
		//get connector
		conn := dcp_part.Connector()
		registry_router := stats_mgr.getOrCreateRegistry(conn.Id())
		registry_router.Register(DOCS_FILTERED_METRIC, metrics.NewCounter())
		registry_router.Register(EXPIRY_FILTERED_METRIC, metrics.NewCounter())
		registry_router.Register(DELETION_FILTERED_METRIC, metrics.NewCounter())
		registry_router.Register(SET_FILTERED_METRIC, metrics.NewCounter())
		conn.RegisterComponentEventListener(common.DataFiltered, r_collector)
	}
	return nil
}

func (l_collector *routerCollector) OnEvent(eventType common.ComponentEventType,
	item interface{},
	component common.Component,
	derivedItems []interface{},
	otherInfos map[string]interface{}) {
	if eventType == common.DataFiltered {
		uprEvent := item.(*mcc.UprEvent)
		seqno := uprEvent.Seqno
		l_collector.stats_mgr.logger.Debugf("Received a DataFiltered event for %v", seqno)
		registry := l_collector.stats_mgr.registries[component.Id()]
		registry.Get(DOCS_FILTERED_METRIC).(metrics.Counter).Inc(1)

		if uprEvent.Expiry != 0 {
			registry.Get(EXPIRY_FILTERED_METRIC).(metrics.Counter).Inc(1)
		}
		if uprEvent.Opcode == mc.UPR_DELETION {
			registry.Get(DELETION_FILTERED_METRIC).(metrics.Counter).Inc(1)
		} else if uprEvent.Opcode == mc.UPR_MUTATION {
			registry.Get(SET_FILTERED_METRIC).(metrics.Counter).Inc(1)
		} else {
			panic(fmt.Sprintf("Invalid opcode, %v, in DataFiltered event from %v.", uprEvent.Opcode, component.Id()))
		}
	}
}

//metrics collector for checkpointmanager
type checkpointMgrCollector struct {
	stats_mgr *StatisticsManager
}

func (ckpt_collector *checkpointMgrCollector) Mount(pipeline common.Pipeline, stats_mgr *StatisticsManager) error {
	ckpt_collector.stats_mgr = stats_mgr
	ckptmgr := pipeline.RuntimeContext().Service(base.CHECKPOINT_MGR_SVC)
	if ckptmgr == nil {
		return errors.New("CheckpointMgr has to exist")
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

func (ckpt_collector *checkpointMgrCollector) OnEvent(eventType common.ComponentEventType,
	item interface{},
	component common.Component,
	derivedItems []interface{},
	otherInfos map[string]interface{}) {
	registry := ckpt_collector.stats_mgr.registries["CkptMgr"]
	if eventType == common.ErrorEncountered {
		registry.Get(NUM_FAILEDCKPTS_METRIC).(metrics.Counter).Inc(1)

	} else if eventType == common.CheckpointDoneForVB {
		vbno := otherInfos[Vbno].(uint16)
		ckpt_record := item.(metadata.CheckpointRecord)
		ckpt_collector.stats_mgr.checkpointed_seqnos[vbno] = ckpt_record.Seqno

	} else if eventType == common.CheckpointDone {
		time_commit := otherInfos[TimeCommiting].(time.Duration).Seconds() * 1000
		registry.Get(NUM_CHECKPOINTS_METRIC).(metrics.Counter).Inc(1)
		registry.Get(TIME_COMMITING_METRIC).(metrics.Histogram).Sample().Update(int64(time_commit))
	}
}

func setCounter(counter metrics.Counter, count int) {
	counter.Clear()
	counter.Inc(int64(count))
}

func (stats_mgr *StatisticsManager) getReplicationStatus() (*pipeline.ReplicationStatus, error) {
	topic := stats_mgr.pipeline.Topic()
	rs := pipeline_manager.ReplicationStatus(topic)
	if rs == nil {
		return nil, utils.ReplicationStatusNotFoundError(topic)
	} else {
		return rs, nil
	}
}

func UpdateStats(cluster_info_svc service_def.ClusterInfoSvc, xdcr_topology_svc service_def.XDCRCompTopologySvc,
	checkpoints_svc service_def.CheckpointsService, kv_mem_clients map[string]*mcc.Client, logger *log.CommonLogger) {
	logger.Debug("updateStats for paused replications")

	for _, repl_id := range pipeline_manager.AllReplications() {
		repl_status := pipeline_manager.ReplicationStatus(repl_id)
		overview_stats := repl_status.GetOverviewStats()
		spec := repl_status.Spec()

		kv_vb_map, err := pipeline_utils.GetSourceVBListForReplication(cluster_info_svc, xdcr_topology_svc, spec, logger)
		if err != nil {
			logger.Errorf("Error retrieving kv_vb_map for paused replication %v. err=%v", repl_id, err)
			continue
		}

		if overview_stats == nil {
			// overview stats may be nil the first time GetStats is called on a paused replication that has never been run in the current goxdcr session
			// or it may be nil when the underying replication is not paused but has not completed startup process
			// construct it
			overview_stats, err := constructStatsForReplication(spec, kv_vb_map, checkpoints_svc, kv_mem_clients, logger)
			if err != nil {
				logger.Errorf("Error constructing stats for paused replication %v. err=%v", repl_id, err)
				continue
			}
			repl_status.SetOverviewStats(overview_stats)
		} else {
			if repl_status.RuntimeStatus() != pipeline.Replicating {
				err := updateStatsForReplication(spec, overview_stats, kv_vb_map, kv_mem_clients, logger)
				if err != nil {
					logger.Errorf("Error updating stats for paused replication %v. err=%v", repl_id, err)
					continue
				}
			}
		}
	}
}

// compute and set changes_left and docs_processed stats. set other stats to 0
func constructStatsForReplication(spec *metadata.ReplicationSpecification, kv_vb_map map[string][]uint16,
	checkpoints_svc service_def.CheckpointsService, kv_mem_clients map[string]*mcc.Client,
	logger *log.CommonLogger) (*expvar.Map, error) {
	docs_processed, err := getDocsProcessedForReplication(spec.Id, checkpoints_svc, logger)
	if err != nil {
		return nil, err
	}

	total_changes, err := calculateTotalChanges(kv_vb_map, kv_mem_clients, spec.SourceBucketName, logger)
	if err != nil {
		return nil, err
	}

	changes_left := uint64(total_changes) - docs_processed

	logger.Infof("Calculating stats for never run replication %v. kv_vb_map=%v, total_docs=%v, docs_processed=%v, changes_left=%v\n", spec.Id, kv_vb_map, total_changes, docs_processed, changes_left)

	overview_map := new(expvar.Map).Init()
	overview_map.Add(DOCS_PROCESSED_METRIC, int64(docs_processed))
	overview_map.Add(CHANGES_LEFT_METRIC, int64(changes_left))
	for _, statsToInitialize := range StatsToInitializeForPausedReplications {
		overview_map.Add(statsToInitialize, 0)
	}
	return overview_map, nil
}

func calculateTotalChanges(kv_vb_map map[string][]uint16, kv_mem_clients map[string]*mcc.Client,
	sourceBucketName string, logger *log.CommonLogger) (int64, error) {
	var total_changes uint64 = 0
	for serverAddr, vbnos := range kv_vb_map {
		client, err := getClient(serverAddr, sourceBucketName, kv_mem_clients, logger)
		if err != nil {
			return 0, err
		}
		highseqno_map, err := getHighSeqNos(serverAddr, vbnos, client)
		if err != nil {
			return 0, err
		}
		for _, vbno := range vbnos {
			current_vb_highseqno := highseqno_map[vbno]
			total_changes = total_changes + current_vb_highseqno
		}
	}
	return int64(total_changes), nil
}

func updateStatsForReplication(spec *metadata.ReplicationSpecification, overview_stats *expvar.Map,
	kv_vb_map map[string][]uint16, kv_mem_clients map[string]*mcc.Client, logger *log.CommonLogger) error {

	// if pipeline is not running, update changes_left stats, which is not being
	// updated by running pipeline and may have become inaccurate
	docs_processed, err := strconv.ParseInt(overview_stats.Get(DOCS_PROCESSED_METRIC).String(), base.ParseIntBase, base.ParseIntBitSize)
	if err != nil {
		return err
	}

	total_changes, err := calculateTotalChanges(kv_vb_map, kv_mem_clients, spec.SourceBucketName, logger)
	if err != nil {
		return err
	}

	changes_left := int64(total_changes) - docs_processed
	changes_left_var := new(expvar.Int)
	changes_left_var.Set(changes_left)

	overview_stats.Set(CHANGES_LEFT_METRIC, changes_left_var)

	logger.Infof("Updating status for paused replication %v. kv_vb_map=%v, total_docs=%v, docs_processed=%v, changes_left=%v\n", spec.Id, kv_vb_map, total_changes, docs_processed, changes_left)
	return nil
}

func getClient(serverAddr, bucketName string, kv_mem_clients map[string]*mcc.Client, logger *log.CommonLogger) (*mcc.Client, error) {
	client, ok := kv_mem_clients[serverAddr]
	if ok {
		return client, nil
	} else {
		if bucketName == "" {
			panic("unexpected empty bucketName")
		}

		var client, err = utils.GetMemcachedConnection(serverAddr, bucketName, logger)
		if err == nil {
			kv_mem_clients[serverAddr] = client
			return client, nil
		} else {
			return nil, err
		}
	}
}
