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
	"github.com/couchbase/cbauth"
	"github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	base "github.com/couchbase/goxdcr/base"
	common "github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	parts "github.com/couchbase/goxdcr/parts"
	"github.com/couchbase/goxdcr/pipeline"
	"github.com/couchbase/goxdcr/utils"
	"github.com/rcrowley/go-metrics"
	"reflect"
	"strconv"
	"sync"
	"time"
)

const (
	DOCS_WRITTEN_METRIC    = "docs_written"
	DATA_REPLICATED_METRIC = "data_replicated"
	SIZE_REP_QUEUE_METRIC  = "size_rep_queue"
	DOCS_REP_QUEUE_METRIC  = "docs_rep_queue"
	DOCS_FILTERED_METRIC   = "docs_filtered"
	CHANGES_LEFT_METRIC    = "changes_left"
	DOCS_LATENCY_METRIC    = "wtavg_docs_latency"
	META_LATENCY_METRIC    = "wtavg_meta_latency"

	//checkpointing related statistics
	DOCS_CHECKED_METRIC    = "docs_checked" //calculated
	NUM_CHECKPOINTS_METRIC = "num_checkpoints"
	TIME_COMMITING_METRIC  = "time_committing"
	NUM_FAILEDCKPTS_METRIC = "num_failedckpts"
	RATE_DOC_CHECKS_METRIC = "rate_doc_checks"
	//optimistic replication replated statistics
	DOCS_OPT_REPD_METRIC = "docs_opt_repd"
	RATE_OPT_REPD_METRIC = "rate_doc_opt_repd"

	DOCS_RECEIVED_DCP_METRICS = "docs_received_from_dcp"

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
)

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

	//temporary map to keep all the collected start time for data item
	//during this collection interval.
	//At the end of the collection interval, collected starttime and endtime will be correlated
	//to calculate the replication lag. The calculated replication lag will be kept in "Overall"
	//entry in registries.
	//This map will be emptied after the replication lags are calculated to get ready for
	//next collection period
	starttime_map      map[string]interface{}
	starttime_map_lock sync.RWMutex

	//temporary map to keep all the collected end time for data item during this collection
	//interval.
	//This map will be emptied after the replication lags are calculated to get ready for
	//next collection period
	endtime_map      map[string]interface{}
	endtime_map_lock sync.RWMutex

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

	//temporary map to keep current through seqno
	through_seqnos map[uint16]uint64

	//statistics update ticker
	publish_ticker *time.Ticker

	//settings - sample size
	sample_size int
	//settings - statistics update interval
	update_interval time.Duration

	//the channel to communicate finish signal with statistic updater
	finish_ch chan bool
	wait_grp  *sync.WaitGroup

	pipeline common.Pipeline

	logger *log.CommonLogger

	collectors []MetricsCollector

	active_vbs          map[string][]uint16
	bucket_name         string
	kv_mem_clients      map[string]*mcc.Client
}

func NewStatisticsManager(logger_ctx *log.LoggerContext, active_vbs map[string][]uint16, bucket_name string) *StatisticsManager {
	stats_mgr := &StatisticsManager{registries: make(map[string]metrics.Registry),
		bucket_name:             bucket_name,
		starttime_map:           make(map[string]interface{}),
		meta_starttime_map:      make(map[string]interface{}),
		endtime_map:             make(map[string]interface{}),
		meta_endtime_map:        make(map[string]interface{}),
		starttime_map_lock:      sync.RWMutex{},
		endtime_map_lock:        sync.RWMutex{},
		meta_starttime_map_lock: sync.RWMutex{},
		meta_endtime_map_lock:   sync.RWMutex{},
		finish_ch:               make(chan bool, 1),
		sample_size:             default_sample_size,
		update_interval:         default_update_interval,
		logger:                  log.NewLogger("StatisticsManager", logger_ctx),
		active_vbs:              active_vbs,
		wait_grp:                &sync.WaitGroup{},
		kv_mem_clients:          make(map[string]*mcc.Client),
		through_seqnos:          make(map[uint16]uint64)}
	stats_mgr.collectors = []MetricsCollector{&outNozzleCollector{}, &dcpCollector{}, &routerCollector{}, &checkpointMgrCollector{}}
	return stats_mgr
}

//Statistics of a pipeline which may or may not be running
func GetStatisticsForPipeline(topic string) *expvar.Map {
	expvar_var := pipeline.StorageForRep(topic)
	if expvar_var != nil {
		overview_map := expvar_var.Get(OVERVIEW_METRICS_KEY)
		if overview_map != nil {
			return overview_map.(*expvar.Map)
		} else {
			return nil
		}
	} else {
		return nil
	}
}

func (stats_mgr *StatisticsManager) cleanupBeforeExit() {
	expvar_stats_map := pipeline.StorageForRep(stats_mgr.pipeline.Topic())
	errlist := expvar_stats_map.Get("Errors")
	expvar_stats_map.Init()
	statusVar := new(expvar.String)
	statusVar.Set(base.Pending)
	expvar_stats_map.Set("Status", statusVar)
	expvar_stats_map.Set("Errors", errlist)
	stats_mgr.logger.Infof("expvar=%v\n", stats_mgr.formatStatsForLog())
}

//updateStats runs until it get finish signal
//It processes the raw stats and publish the overview stats along with the raw stats to expvar
//It also log the stats to log
func (stats_mgr *StatisticsManager) updateStats(finchan chan bool) error {
	stats_mgr.logger.Info("updateStats started")

	defer stats_mgr.wait_grp.Done()

	init_ch := make(chan bool, 1)
	init_ch <- true
	for {
		select {
		case <-finchan:
			stats_mgr.cleanupBeforeExit()
			return nil
		// this ensures that stats are printed out immediately after updateStats is started
		case <-init_ch:
			err := stats_mgr.updateStatsOnce()
			if err != nil {
				return nil
			}
		case <-stats_mgr.publish_ticker.C:
			err := stats_mgr.updateStatsOnce()
			if err != nil {
				return nil
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

	if err == nil {
		if stats_mgr.logger.GetLogLevel() >= log.LogLevelInfo {
			stats_mgr.logger.Info(stats_mgr.formatStatsForLog())

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

	} else {
		stats_mgr.logger.Info("Failed to calculate the statistics for this round. Move on")
	}
	return nil
}

func (stats_mgr *StatisticsManager) formatStatsForLog() string {
	expvar_stats_map := pipeline.StorageForRep(stats_mgr.pipeline.Topic())
	return fmt.Sprintf("Stats for pipeline %v %v\n", stats_mgr.pipeline.InstanceId(), expvar_stats_map.String())
}

//process the raw stats, aggregate them into overview registry
//expose the raw stats and overview stats to expvar
func (stats_mgr *StatisticsManager) processRawStats() error {
	oldSample := stats_mgr.getOverviewRegistry()
	stats_mgr.initOverviewRegistry()
	expvar_stats_map := pipeline.StorageForRep(stats_mgr.pipeline.Topic())

	stats_mgr.processTimeSample()
	for registry_name, registry := range stats_mgr.registries {
		if registry_name != OVERVIEW_METRICS_KEY {
			map_for_registry := new(expvar.Map).Init()

			orig_registry := expvar_stats_map.Get(registry_name)
			registry.Each(func(name string, i interface{}) {
				stats_mgr.publishMetricToMap(map_for_registry, name, i, true)
				switch m := i.(type) {
				case metrics.Counter:
					if orig_registry != nil {
						orig_val, _ := strconv.ParseInt(orig_registry.(*expvar.Map).Get(name).String(), 10, 64)
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
					//raw counter in its registry is type of Histogram, put its mean value
					//to overview registry
					metric_overview := stats_mgr.getOverviewRegistry().Get(name)
					if metric_overview != nil {
						metric_overview.(metrics.Counter).Clear()
						metric_overview.(metrics.Counter).Inc(int64(m.Mean()))
					}
				}
			})
			expvar_stats_map.Set(registry_name, map_for_registry)
		}
	}

	map_for_overview := new(expvar.Map).Init()

	//publish all the metrics in overview registry
	stats_mgr.getOverviewRegistry().Each(func(name string, i interface{}) {
		stats_mgr.publishMetricToMap(map_for_overview, name, i, false)
	})

	//calculate the publish additional metrics
	err := stats_mgr.processCalculatedStats(oldSample, map_for_overview)
	if err != nil {
		return err
	}

	stats_mgr.logger.Debugf("Overview=%v for pipeline %v\n", map_for_overview, stats_mgr.pipeline.Topic())
	expvar_stats_map.Set(OVERVIEW_METRICS_KEY, map_for_overview)
	return nil
}

func (stats_mgr *StatisticsManager) processCalculatedStats(oldSample metrics.Registry, overview_expvar_map *expvar.Map) error {

	//calculate changes_left
	docs_written := stats_mgr.getOverviewRegistry().Get(DOCS_WRITTEN_METRIC).(metrics.Counter).Count()
	changes_left_val, err := stats_mgr.calculateChangesLeft(docs_written)
	if err == nil {
		changes_left_var := new(expvar.Int)
		changes_left_var.Set(changes_left_val)
		overview_expvar_map.Set(CHANGES_LEFT_METRIC, changes_left_var)
	} else {
		stats_mgr.logger.Errorf("Failed to calculate changes_left - %v\n", err)
	}

	//calculate rate_replication
	docs_written_old := oldSample.Get(DOCS_WRITTEN_METRIC).(metrics.Counter).Count()
	interval_in_sec := stats_mgr.update_interval.Seconds()
	rate_replicated := float64(docs_written-docs_written_old) / interval_in_sec
	rate_replicated_var := new(expvar.Float)
	rate_replicated_var.Set(rate_replicated)
	overview_expvar_map.Set(RATE_REPLICATED_METRIC, rate_replicated_var)

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
	docs_checked_old_var := overview_expvar_map.Get(DOCS_CHECKED_METRIC)
	var docs_checked_old uint64 = 0
	if docs_checked_old_var != nil {
		docs_checked_old, err = strconv.ParseUint(docs_checked_old_var.String(), 10, 64)
		if err != nil {
			return err
		}
	}
	docs_checked := stats_mgr.calculateDocsChecked()
	docs_checked_var := new(expvar.Int)
	docs_checked_var.Set(int64(docs_checked))
	overview_expvar_map.Set(DOCS_CHECKED_METRIC, docs_checked_var)

	//calculate rate_doc_checks
	rate_doc_checks := float64(docs_checked-uint64(docs_checked_old)) / interval_in_sec
	rate_doc_checks_var := new(expvar.Float)
	rate_doc_checks_var.Set(rate_doc_checks)
	overview_expvar_map.Set(RATE_DOC_CHECKS_METRIC, rate_doc_checks_var)

	return nil
}


func (stats_mgr *StatisticsManager) calculateDocsChecked() uint64 {
	var docs_checked uint64 = 0
	for vbno, vbts := range GetStartSeqnos(stats_mgr.pipeline, stats_mgr.logger) {
		start_seqno := vbts.Seqno
		var docs_checked_vb uint64 = 0
		if stats_mgr.through_seqnos[vbno] > start_seqno {
			docs_checked_vb = stats_mgr.through_seqnos[vbno] - start_seqno
		}
		docs_checked = docs_checked + docs_checked_vb
	}
	return docs_checked
}
func (stats_mgr *StatisticsManager) calculateChangesLeft(docs_written int64) (int64, error) {
	total_doc, err := stats_mgr.calculateTotalChanges()
	if err != nil {
		return 0, err
	}
	changes_left := total_doc - docs_written
	stats_mgr.logger.Debugf("total_doc=%v, docs_written=%v, changes_left=%v\n", total_doc, docs_written, changes_left)
	return changes_left, nil
}

func (stats_mgr *StatisticsManager) calculateTotalChanges() (int64, error) {
	var total_doc uint64 = 0
	for serverAddr, vbnos := range stats_mgr.active_vbs {
		highseqno_map, err := stats_mgr.getHighSeqNos(serverAddr, vbnos)
		for _, vbno := range vbnos {
			startSeqnos_map := GetStartSeqnos(stats_mgr.pipeline, stats_mgr.logger)
			ts, ok := startSeqnos_map[vbno]
			if !ok {
				return 0, fmt.Errorf("Can't find start seqno for vbno=%v\n", vbno)
			}
			current_vb_highseqno := highseqno_map[vbno]
			if err != nil {
				return 0, err
			}
			total_doc = total_doc + current_vb_highseqno - ts.Seqno
		}
	}
	return int64(total_doc), nil
}
func (stats_mgr *StatisticsManager) getHighSeqNos(serverAddr string, vbnos []uint16) (map[uint16]uint64, error) {
	highseqno_map := make(map[uint16]uint64)
	conn := stats_mgr.kv_mem_clients[serverAddr]
	if conn == nil {
		return nil, errors.New("connection for serverAddr is not initialized")
	}

	stats_map, err := conn.StatsMap(base.VBUCKET_SEQNO_STAT_NAME)
	if err != nil {
		return nil, err
	}

	err = utils.ParseHighSeqnoStat(vbnos, stats_map, highseqno_map)
	return highseqno_map, err
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

func (stats_mgr *StatisticsManager) processTimeSample() {
	stats_mgr.processDocsLatencyTimeSample()
	stats_mgr.processMetaLatencyTimeSample()
}

// compute docs_latency metric
func (stats_mgr *StatisticsManager) processDocsLatencyTimeSample() {
	stats_mgr.logger.Debug("Process Docs Latency Time Sample...")

	time_committing := stats_mgr.getOverviewRegistry().GetOrRegister(DOCS_LATENCY_METRIC, metrics.NewHistogram(metrics.NewUniformSample(stats_mgr.sample_size))).(metrics.Histogram)
	time_committing.Clear()
	sample := time_committing.Sample()

	stats_mgr.starttime_map_lock.RLock()
	stats_mgr.endtime_map_lock.RLock()
	defer stats_mgr.starttime_map_lock.RUnlock()
	defer stats_mgr.endtime_map_lock.RUnlock()
	for name, starttime := range stats_mgr.starttime_map {
		endtime := stats_mgr.endtime_map[name]
		if endtime != nil {
			rep_duration := endtime.(time.Time).Sub(starttime.(time.Time))
			//in millisecond
			sample.Update(rep_duration.Nanoseconds() / 1000000)
		}
	}

	//clear both starttime_registry and endtime_registry
	stats_mgr.starttime_map = make(map[string]interface{})
	stats_mgr.endtime_map = make(map[string]interface{})
}

// compute meta_latency metric
func (stats_mgr *StatisticsManager) processMetaLatencyTimeSample() {
	stats_mgr.logger.Debug("Process Meta Latency Time Sample...")

	meta_time_committing := stats_mgr.getOverviewRegistry().GetOrRegister(META_LATENCY_METRIC, metrics.NewHistogram(metrics.NewUniformSample(stats_mgr.sample_size))).(metrics.Histogram)
	meta_time_committing.Clear()
	sample := meta_time_committing.Sample()

	stats_mgr.meta_starttime_map_lock.RLock()
	stats_mgr.meta_endtime_map_lock.RLock()
	defer stats_mgr.meta_starttime_map_lock.RUnlock()
	defer stats_mgr.meta_endtime_map_lock.RUnlock()
	for name, starttime := range stats_mgr.meta_starttime_map {
		endtime := stats_mgr.meta_endtime_map[name]
		if endtime != nil {
			meta_latency := endtime.(time.Time).Sub(starttime.(time.Time))
			//in millisecond
			sample.Update(meta_latency.Nanoseconds() / 1000000)
		}
	}

	//clear both starttime_registry and endtime_registry
	stats_mgr.meta_starttime_map = make(map[string]interface{})
	stats_mgr.meta_endtime_map = make(map[string]interface{})
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
	overview_registry := metrics.NewRegistry()
	stats_mgr.registries[OVERVIEW_METRICS_KEY] = overview_registry
	overview_registry.Register(DOCS_WRITTEN_METRIC, metrics.NewCounter())
	overview_registry.Register(DATA_REPLICATED_METRIC, metrics.NewCounter())
	overview_registry.Register(DOCS_FILTERED_METRIC, metrics.NewCounter())
	overview_registry.Register(NUM_CHECKPOINTS_METRIC, metrics.NewCounter())
	overview_registry.Register(NUM_FAILEDCKPTS_METRIC, metrics.NewCounter())
	overview_registry.Register(TIME_COMMITING_METRIC, metrics.NewCounter())
	overview_registry.Register(DOCS_OPT_REPD_METRIC, metrics.NewCounter())
	overview_registry.Register(DOCS_RECEIVED_DCP_METRICS, metrics.NewCounter())
	overview_registry.Register(SIZE_REP_QUEUE_METRIC, metrics.NewCounter())
	overview_registry.Register(DOCS_REP_QUEUE_METRIC, metrics.NewCounter())
}

func (stats_mgr *StatisticsManager) Start(settings map[string]interface{}) error {

	//initialize connection
	stats_mgr.initConnection()

	if _, ok := settings[PUBLISH_INTERVAL]; ok {
		stats_mgr.update_interval = settings[PUBLISH_INTERVAL].(time.Duration)
	} else {
		stats_mgr.logger.Infof("There is no update_interval in settings map. settings=%v\n", settings)
	}
	stats_mgr.logger.Debugf("StatisticsManager Starts: update_interval=%v, settings=%v\n", stats_mgr.update_interval, settings)
	stats_mgr.publish_ticker = time.NewTicker(stats_mgr.update_interval)

	//publishing status to expvar
	expvar_stats_map := pipeline.StorageForRep(stats_mgr.pipeline.Topic())
	statusVar := new(expvar.String)
	statusVar.Set(base.Replicating)
	expvar_stats_map.Set("Status", statusVar)
	stats_mgr.wait_grp.Add(1)
	go stats_mgr.updateStats(stats_mgr.finish_ch)

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

func (stats_mgr *StatisticsManager) initConnection() error {

	for serverAddr, _ := range stats_mgr.active_vbs {
		if serverAddr == "" {
			panic("serverAddr is empty")
		}
		username, password, err := cbauth.GetMemcachedServiceAuth(serverAddr)
		stats_mgr.logger.Debugf("memcached auth: username=%v, password=%v, err=%v\n", username, password, err)
		if err != nil {
			return err
		}

		conn, err := base.NewConn(serverAddr, username, password)
		if err != nil {
			return err
		}

		_, err = conn.SelectBucket(stats_mgr.bucket_name)
		if err != nil {
			return err
		}

		stats_mgr.kv_mem_clients[serverAddr] = conn
	}

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
		registry.Register(SIZE_REP_QUEUE_METRIC, metrics.NewHistogram(metrics.NewUniformSample(stats_mgr.sample_size)))
		registry.Register(DOCS_REP_QUEUE_METRIC, metrics.NewHistogram(metrics.NewUniformSample(stats_mgr.sample_size)))
		registry.Register(DOCS_WRITTEN_METRIC, metrics.NewCounter())
		registry.Register(DATA_REPLICATED_METRIC, metrics.NewCounter())
		registry.Register(DOCS_OPT_REPD_METRIC, metrics.NewCounter())
		part.RegisterComponentEventListener(common.DataSent, outNozzle_collector)
		part.RegisterComponentEventListener(common.DataReceived, outNozzle_collector)
		part.RegisterComponentEventListener(common.GetMetaSent, outNozzle_collector)
		part.RegisterComponentEventListener(common.GetMetaReceived, outNozzle_collector)

	}
	return nil
}

func (outNozzle_collector *outNozzleCollector) OnEvent(eventType common.ComponentEventType,
	item interface{},
	component common.Component,
	derivedItems []interface{},
	otherInfos map[string]interface{}) {
	if eventType == common.DataReceived {
		outNozzle_collector.stats_mgr.logger.Debugf("Received a DataReceived event from %v", reflect.TypeOf(component))
		queue_size := otherInfos[parts.STATS_QUEUE_SIZE].(int)
		queue_size_bytes := otherInfos[parts.STATS_QUEUE_SIZE_BYTES].(int)
		registry := outNozzle_collector.stats_mgr.registries[component.Id()]
		registry.Get(DOCS_REP_QUEUE_METRIC).(metrics.Histogram).Sample().Update(int64(queue_size))
		registry.Get(SIZE_REP_QUEUE_METRIC).(metrics.Histogram).Sample().Update(int64(queue_size_bytes))
	} else if eventType == common.DataSent {
		outNozzle_collector.stats_mgr.logger.Debugf("Received a DataSent event from %v", reflect.TypeOf(component))
		endTime := time.Now()
		key := string(item.(*gomemcached.MCRequest).Key)
		size := item.(*gomemcached.MCRequest).Size()
		seqno := otherInfos[parts.EVENT_ADDI_SEQNO].(uint64)
		opti_replicated := otherInfos[parts.EVENT_ADDI_OPT_REPD].(bool)
		registry := outNozzle_collector.stats_mgr.registries[component.Id()]
		registry.Get(DOCS_WRITTEN_METRIC).(metrics.Counter).Inc(1)
		registry.Get(DATA_REPLICATED_METRIC).(metrics.Counter).Inc(int64(size))
		if opti_replicated {
			registry.Get(DOCS_OPT_REPD_METRIC).(metrics.Counter).Inc(1)
		}

		outNozzle_collector.stats_mgr.endtime_map_lock.Lock()
		defer outNozzle_collector.stats_mgr.endtime_map_lock.Unlock()
		outNozzle_collector.stats_mgr.endtime_map[getStatsKeyFromDocKeyAndSeqno(key, seqno)] = endTime
	} else if eventType == common.GetMetaSent {
		outNozzle_collector.stats_mgr.logger.Debugf("Received a GetMetaSent event from %v", reflect.TypeOf(component))
		startTime := time.Now()
		key := otherInfos[parts.EVENT_ADDI_DOC_KEY].(string)
		seqno := otherInfos[parts.EVENT_ADDI_SEQNO].(uint64)

		outNozzle_collector.stats_mgr.meta_starttime_map_lock.Lock()
		defer outNozzle_collector.stats_mgr.meta_starttime_map_lock.Unlock()
		outNozzle_collector.stats_mgr.meta_starttime_map[getStatsKeyFromDocKeyAndSeqno(key, seqno)] = startTime
	} else if eventType == common.GetMetaReceived {
		outNozzle_collector.stats_mgr.logger.Debugf("Received a GetMetaReceived event from %v", reflect.TypeOf(component))
		endTime := time.Now()
		key := otherInfos[parts.EVENT_ADDI_DOC_KEY].(string)
		seqno := otherInfos[parts.EVENT_ADDI_SEQNO].(uint64)

		outNozzle_collector.stats_mgr.meta_endtime_map_lock.Lock()
		defer outNozzle_collector.stats_mgr.meta_endtime_map_lock.Unlock()
		outNozzle_collector.stats_mgr.meta_endtime_map[getStatsKeyFromDocKeyAndSeqno(key, seqno)] = endTime
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
		registry.Register(DOCS_RECEIVED_DCP_METRICS, metrics.NewCounter())
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
		startTime := time.Now()
		key := string(item.(*mcc.UprEvent).Key)
		seqno := item.(*mcc.UprEvent).Seqno
		registry := dcp_collector.stats_mgr.registries[component.Id()]
		registry.Get(DOCS_RECEIVED_DCP_METRICS).(metrics.Counter).Inc(1)

		dcp_collector.stats_mgr.starttime_map_lock.Lock()
		defer dcp_collector.stats_mgr.starttime_map_lock.Unlock()
		dcp_collector.stats_mgr.starttime_map[getStatsKeyFromDocKeyAndSeqno(key, seqno)] = startTime
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
		seqno := item.(*mcc.UprEvent).Seqno
		l_collector.stats_mgr.logger.Debugf("Received a DataFiltered event for %v", seqno)
		registry := l_collector.stats_mgr.registries[component.Id()]
		registry.Get(DOCS_FILTERED_METRIC).(metrics.Counter).Inc(1)
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
		ckpt_collector.stats_mgr.through_seqnos[vbno] = ckpt_record.Seqno

	} else if eventType == common.CheckpointDone {
		time_commit := otherInfos[TimeCommiting].(time.Duration).Seconds()
		registry.Get(NUM_CHECKPOINTS_METRIC).(metrics.Counter).Inc(1)
		registry.Get(TIME_COMMITING_METRIC).(metrics.Histogram).Sample().Update(int64(time_commit))
	}
}
