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
	"expvar"
	"fmt"
	"github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	common "github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	parts "github.com/couchbase/goxdcr/parts"
	"github.com/rcrowley/go-metrics"
	"reflect"
	"sync"
	"time"
)

const (
	DOCS_WRITTEN_METRIC    = "docs_written"
	DATA_REPLICATED_METRIC = "data_replicated"
	SIZE_REP_QUEUE_METRIC  = "size_rep_queue"
	DOCS_REP_QUEUE_METRIC  = "docs_rep_queue"
	TIME_COMMITTING_METRIC = "time_committing"
	DOCS_FILTERED          = "docs_filtered"

	OVERALL_METRICS_KEY = "Overall"

	//statistics_manager's setting
	SAMPLE_SIZE      = "sample_size"
	PUBLISH_INTERVAL = "publish_interval"
)

const (
	default_sample_size      = 1000
	default_publish_interval = 5 * time.Second
)

//doesn't change per instance
var (
	metrics_map map[string]*MetricDescriptor
)

//StatisticsManager mount the statics collector on the pipeline to collect raw stats
//It does stats correlation and processing on raw stats periodically (controlled by publish_interval)
//, then stores the result in expvar
//The result in expvar can be exposed to outside via different channels - log or to ns_server.
type StatisticsManager struct {
	//a map of registry with the part id as key
	//the aggregated metrics for the pipeline is the entry with key="Overall"
	//this map will be exported to expval, but only
	//the entry with key="Overall" will be reported to ns_server
	registries map[string]metrics.Registry

	//temporary map to keep all the collected start time for data item
	//during this collection interval.
	//At the end of the collection interval, collected starttime and endtime will be correlated
	//to calculate the replication lag. The calculated replication lag will be kept in "Overall"
	//entry in registries.
	//This map will be emptied after the replication lags are calculated to get ready for
	//next collection period
	starttime_map map[string]interface{}

	//temporary map to keep all the collected end time for data item during this collection
	//interval.
	//This map will be emptied after the replication lags are calculated to get ready for
	//next collection period
	endtime_map    map[string]interface{}
	publish_ticker *time.Ticker

	sample_size      int
	publish_interval time.Duration
	publish_chan     chan bool
	pipeline         common.Pipeline

	logger *log.CommonLogger
	once   sync.Once

	collectors []MetricsCollector
}

func init() {
	metrics_map = make(map[string]*MetricDescriptor)

	//initialize the metrics map
	//docs_written
	docs_written := &MetricDescriptor{Name: "docs_written",
		Description: "number of documents written to destination cluster via XDCR",
		Title:       "documents replicated"}
	metrics_map[docs_written.Name] = docs_written

	//data_replicated
	data_replicated := &MetricDescriptor{Name: "data_replicated",
		Description: "size of data replicated in bytes",
		Title:       "data replicated (in bytes)"}
	metrics_map[data_replicated.Name] = data_replicated

	//size_rep_queue
	size_rep_queue := &MetricDescriptor{Name: "size_rep_queue",
		Description: "size of replication queue in bytes",
		Title:       "data to be replicated(in bytes)"}
	metrics_map[size_rep_queue.Name] = size_rep_queue

	//docs_rep_queue
	docs_rep_queue := &MetricDescriptor{Name: "docs_rep_queue",
		Description: "Number of documents in replication queue",
		Title:       "documents to be replicated"}
	metrics_map[docs_rep_queue.Name] = docs_rep_queue

	//time_committing
	time_committing := &MetricDescriptor{Name: "time_committing",
		Description: "Aggregated replication delay for all documents replicated",
		Title:       "Total replication delay"}
	metrics_map[time_committing.Name] = time_committing

}

func NewStatisticsManager(logger_ctx *log.LoggerContext) *StatisticsManager {
	stats_mgr := &StatisticsManager{registries: make(map[string]metrics.Registry),
		starttime_map:    make(map[string]interface{}),
		publish_chan:     make(chan bool, 1),
		sample_size:      default_sample_size,
		publish_interval: default_publish_interval,
		logger:           log.NewLogger("StatisticsManager", logger_ctx),
		endtime_map:      make(map[string]interface{})}
	stats_mgr.collectors = []MetricsCollector{&xmemCollector{}, &dcpCollector{}, &routerCollector{}}
	return stats_mgr
}

func (stats_mgr *StatisticsManager) updateStats(finchan chan bool) error {
	select {
	case <-finchan:
		expvar_stats_map := stats_mgr.getExpvarMap(stats_mgr.pipeline.Topic())
		expvar_stats_map.Init()
		statusVar := new(expvar.String)
		statusVar.Set("Stopped")
		expvar_stats_map.Set("Status", statusVar)
		return nil
	case <-stats_mgr.publish_ticker.C:
		stats_mgr.logger.Infof("Publishing the statistics for %v to expvar", stats_mgr.pipeline.Topic())
		stats_mgr.processRawStats()
		if stats_mgr.logger.GetLogLevel() >= log.LogLevelInfo {
			stats_mgr.logger.Info (stats_mgr.formatStatsForLog())
		}
	}
	return nil
}

func (stats_mgr *StatisticsManager) formatStatsForLog () string {
	expvar_stats_map := stats_mgr.getExpvarMap(stats_mgr.pipeline.Topic())
	return fmt.Sprintf("Stats for pipeline %v\n %v\n", stats_mgr.pipeline.Topic(), expvar_stats_map.String())
}

func (stats_mgr *StatisticsManager) processRawStats () {
		expvar_stats_map := stats_mgr.getExpvarMap(stats_mgr.pipeline.Topic())

		stats_mgr.processTimeSample()
		for registry_name, registry := range stats_mgr.registries {
			if registry_name != OVERALL_METRICS_KEY {
				map_for_registry := new(expvar.Map).Init()

				registry.Each(func(name string, i interface{}) {
					stats_mgr.publishMetricToMap(map_for_registry, name, i)
					switch m := i.(type) {
					case metrics.Counter:
						metric_overall := stats_mgr.getOverallRegistry().Get(name)
						if metric_overall != nil {
							metric_overall.(metrics.Counter).Inc(m.Count())
						}

					}
				})
				expvar_stats_map.Set(registry_name, map_for_registry)
			}
		}

		map_for_overall := new(expvar.Map).Init()
		stats_mgr.getOverallRegistry().Each(func(name string, i interface{}) {
			stats_mgr.publishMetricToMap(map_for_overall, name, i)
		})
		expvar_stats_map.Set(OVERALL_METRICS_KEY, map_for_overall)
}

func (stats_mgr *StatisticsManager) getExpvarMap(name string) *expvar.Map {
	pipeline_map := expvar.Get(name)

	return pipeline_map.(*expvar.Map)
}

func (stats_mgr *StatisticsManager) getOverallRegistry() metrics.Registry {
	return stats_mgr.registries[OVERALL_METRICS_KEY]
}

func (stats_mgr *StatisticsManager) publishMetricToMap(expvar_map *expvar.Map, name string, i interface{}) {
	switch m := i.(type) {
	case metrics.Counter:
		expvar_map.Set(name, expvar.Func(func() interface{} {
			return m.Count()
		}))
	case metrics.Histogram:
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

	}

}

func (stats_mgr *StatisticsManager) processTimeSample() {
	stats_mgr.logger.Info("Process Time Sample...")
	time_committing := stats_mgr.getOverallRegistry().GetOrRegister(TIME_COMMITTING_METRIC, metrics.NewHistogram(metrics.NewUniformSample(stats_mgr.sample_size))).(metrics.Histogram)
	time_committing.Clear()
	sample := time_committing.Sample()
	for name, starttime := range stats_mgr.starttime_map {
		endtime := stats_mgr.endtime_map[name]
		if endtime != nil {
			rep_duration := endtime.(time.Time).Sub(starttime.(time.Time))
			sample.Update(rep_duration.Nanoseconds())
		}
	}

	//clear both starttime_registry and endtime_registry
	stats_mgr.starttime_map = make(map[string]interface{})
	stats_mgr.endtime_map = make(map[string]interface{})
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
	overall_registry := stats_mgr.getOrCreateRegistry(OVERALL_METRICS_KEY)
	overall_registry.Register(DOCS_WRITTEN_METRIC, metrics.NewCounter())
	overall_registry.Register(DATA_REPLICATED_METRIC, metrics.NewCounter())
	overall_registry.Register(DOCS_FILTERED, metrics.NewCounter())
	stats_mgr.logger.Infof("StatisticsManager is started for pipeline %v", stats_mgr.pipeline.Topic)

	//publish the statistics to expvar
	expvar_map := expvar.Get(stats_mgr.pipeline.Topic())
	if expvar_map == nil {
		expvar.NewMap(stats_mgr.pipeline.Topic())
	}
	return nil
}

func (stats_mgr *StatisticsManager) Start(settings map[string]interface{}) error {
	if _, ok := settings[PUBLISH_INTERVAL]; ok {
		stats_mgr.publish_interval = settings[PUBLISH_INTERVAL].(time.Duration)
	}
	stats_mgr.publish_ticker = time.NewTicker(stats_mgr.publish_interval)

	//publishing status to expvar
	expvar_stats_map := stats_mgr.getExpvarMap(stats_mgr.pipeline.Topic())
	statusVar := new(expvar.String)
	statusVar.Set("Started")
	expvar_stats_map.Set("Status", statusVar)

	go stats_mgr.updateStats(stats_mgr.publish_chan)

	return nil
}

func (stats_mgr *StatisticsManager) Stop() error {
	stats_mgr.publish_chan <- true
	return nil
}

type MetricDescriptor struct {
	Name        string
	Description string
	Title       string
}

type MetricsCollector interface {
	Mount(pipeline common.Pipeline, stats_mgr *StatisticsManager) error
}

//metrics collector for XMemNozzle
type xmemCollector struct {
	stats_mgr *StatisticsManager
}

func (xmem_collector *xmemCollector) Mount(pipeline common.Pipeline, stats_mgr *StatisticsManager) error {
	xmem_collector.stats_mgr = stats_mgr
	xmem_parts := pipeline.Targets()
	for _, part := range xmem_parts {
		registry := stats_mgr.getOrCreateRegistry(part.Id())
		registry.Register(SIZE_REP_QUEUE_METRIC, metrics.NewHistogram(metrics.NewUniformSample(stats_mgr.sample_size)))
		registry.Register(DOCS_REP_QUEUE_METRIC, metrics.NewHistogram(metrics.NewUniformSample(stats_mgr.sample_size)))
		registry.Register(DOCS_WRITTEN_METRIC, metrics.NewCounter())
		registry.Register(DATA_REPLICATED_METRIC, metrics.NewCounter())
		part.RegisterComponentEventListener(common.DataSent, xmem_collector)
		part.RegisterComponentEventListener(common.DataReceived, xmem_collector)

	}
	return nil
}

func (xmem_collector *xmemCollector) OnEvent(eventType common.ComponentEventType,
	item interface{},
	component common.Component,
	derivedItems []interface{},
	otherInfos map[string]interface{}) {
	if eventType == common.DataReceived {
		xmem_collector.stats_mgr.logger.Debugf("Received a DataReceived event from %v", reflect.TypeOf(component))
		queue_size := otherInfos[parts.XMEM_STATS_QUEUE_SIZE].(int)
		queue_size_bytes := otherInfos[parts.XMEM_STATS_QUEUE_SIZE_BYTES].(int)
		registry := xmem_collector.stats_mgr.registries[component.Id()]
		registry.Get(DOCS_REP_QUEUE_METRIC).(metrics.Histogram).Sample().Update(int64(queue_size))
		registry.Get(SIZE_REP_QUEUE_METRIC).(metrics.Histogram).Sample().Update(int64(queue_size_bytes))
	} else if eventType == common.DataSent {
		endTime := time.Now()
		size := item.(*gomemcached.MCRequest).Size()
		seqno := otherInfos[parts.XMEM_EVENT_ADDI_SEQNO].(uint64)
		registry := xmem_collector.stats_mgr.registries[component.Id()]
		registry.Get(DOCS_WRITTEN_METRIC).(metrics.Counter).Inc(1)
		registry.Get(DATA_REPLICATED_METRIC).(metrics.Counter).Inc(int64(size))

		xmem_collector.stats_mgr.endtime_map[fmt.Sprintf("%v", seqno)] = endTime
	}
}

//metrics collector for DcpNozzle
type dcpCollector struct {
	stats_mgr *StatisticsManager
}

func (dcp_collector *dcpCollector) Mount(pipeline common.Pipeline, stats_mgr *StatisticsManager) error {
	dcp_collector.stats_mgr = stats_mgr
	dcp_parts := pipeline.Sources()
	for _, dcp_part := range dcp_parts {
		stats_mgr.getOrCreateRegistry(dcp_part.Id())
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
		seqno := item.(*mcc.UprEvent).Seqno
		dcp_collector.stats_mgr.starttime_map[fmt.Sprintf("%v", seqno)] = startTime
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
		registry_router.Register(DOCS_FILTERED, metrics.NewCounter())
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
		registry.Get(DOCS_FILTERED).(metrics.Counter).Inc(1)
	}
}
