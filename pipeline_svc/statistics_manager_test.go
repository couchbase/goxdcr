package pipeline_svc

import (
	"encoding/json"
	"fmt"
	mcc "github.com/couchbase/gomemcached/client"
	commonReal "github.com/couchbase/goxdcr/common"
	common "github.com/couchbase/goxdcr/common/mocks"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/parts"
	service_def "github.com/couchbase/goxdcr/service_def/mocks"
	utilities "github.com/couchbase/goxdcr/utils/mocks"
	"github.com/rcrowley/go-metrics"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
	"io/ioutil"
	"testing"
)

var pipelineTopic string = "topic"

var testDCPPart string = "testDCP"
var testRouter string = "testRouter"

func RetrieveUprFile(fileName string) (*mcc.UprEvent, error) {
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	var uprEvent mcc.UprEvent
	err = json.Unmarshal(data, &uprEvent)
	if err != nil {
		return nil, err
	}

	return &uprEvent, nil
}

var uprEventFile string = "../parts/testdata/perfData.bin"

func setupBoilerPlate() (*log.CommonLogger,
	*service_def.ThroughSeqnoTrackerSvc,
	*service_def.ClusterInfoSvc,
	*service_def.XDCRCompTopologySvc,
	*utilities.UtilsIface,
	map[string][]uint16,
	*common.Pipeline,
	*metadata.ReplicationSpecification,
	*common.PipelineRuntimeContext,
	*service_def.CheckpointsService,
	*service_def.CAPIService,
	*service_def.RemoteClusterSvc,
	*service_def.ReplicationSpecSvc,
	map[string][]uint16,
	*metadata.RemoteClusterReference,
	*parts.DcpNozzle,
	*common.Connector) {

	testLogger := log.NewLogger("testLogger", log.DefaultLoggerContext)
	throughSeqSvc := &service_def.ThroughSeqnoTrackerSvc{}
	clusterInfoSvc := &service_def.ClusterInfoSvc{}
	xdcrTopologySvc := &service_def.XDCRCompTopologySvc{}
	utils := &utilities.UtilsIface{}

	activeVBs := make(map[string][]uint16)
	var vbs []uint16
	vbs = append(vbs, 550) // perfData
	vbs = append(vbs, 12)
	vbs = append(vbs, 102)
	activeVBs[testDCPPart] = vbs

	pipeline := &common.Pipeline{}

	replicationSpec := &metadata.ReplicationSpecification{}
	runtimeCtx := &common.PipelineRuntimeContext{}

	ckptService := &service_def.CheckpointsService{}
	capiSvc := &service_def.CAPIService{}
	remoteClusterSvc := &service_def.RemoteClusterSvc{}
	replSpecSvc := &service_def.ReplicationSpecSvc{}
	targetKVVbMap := make(map[string][]uint16)
	remoteClusterRef := &metadata.RemoteClusterReference{}

	dcpNozzle := parts.NewDcpNozzle(testDCPPart, "sourceBucket", "targetBucket", vbs, xdcrTopologySvc,
		false /*isCapi*/, log.DefaultLoggerContext, utils)

	connector := &common.Connector{}

	return testLogger, throughSeqSvc, clusterInfoSvc, xdcrTopologySvc, utils, activeVBs,
		pipeline, replicationSpec, runtimeCtx, ckptService, capiSvc, remoteClusterSvc, replSpecSvc,
		targetKVVbMap, remoteClusterRef, dcpNozzle, connector
}

func setupMocks(throughSeqSvc *service_def.ThroughSeqnoTrackerSvc,
	clusterInfoSvc *service_def.ClusterInfoSvc,
	xdcrTopologySvc *service_def.XDCRCompTopologySvc,
	utils *utilities.UtilsIface,
	activeVBs map[string][]uint16,
	pipeline *common.Pipeline,
	replicationSpec *metadata.ReplicationSpecification,
	runtimeCtx *common.PipelineRuntimeContext,
	ckptService *service_def.CheckpointsService,
	capiSvc *service_def.CAPIService,
	remoteClusterSvc *service_def.RemoteClusterSvc,
	replSpecSvc *service_def.ReplicationSpecSvc,
	targetKVVbMap map[string][]uint16,
	remoteClusterRef *metadata.RemoteClusterReference,
	dcpNozzle *parts.DcpNozzle,
	connector *common.Connector) {

	pipeline.On("Specification").Return(replicationSpec)
	pipeline.On("Topic").Return(pipelineTopic)
	sourceMap := make(map[string]commonReal.Nozzle)
	sourceMap[testDCPPart] = dcpNozzle
	pipeline.On("Sources").Return(sourceMap)
	pipeline.On("Targets").Return(nil)
	pipeline.On("GetAsyncListenerMap").Return(nil)
	pipeline.On("SetAsyncListenerMap", mock.Anything).Return(nil)
	pipeline.On("RuntimeContext").Return(runtimeCtx)

	connector.On("AsyncComponentEventListeners").Return(nil)
	connector.On("DownStreams").Return(nil)
	connector.On("Id").Return(testRouter)

	dcpNozzle.SetConnector(connector)
}

func setupInnerMock(runtimeCtx *common.PipelineRuntimeContext,
	ckptManager *CheckpointManager) {
	runtimeCtx.On("Service", "CheckpointManager").Return(ckptManager)
}

func setupCheckpointMgr(
	ckptService *service_def.CheckpointsService,
	capiSvc *service_def.CAPIService,
	remoteClusterSvc *service_def.RemoteClusterSvc,
	replSpecSvc *service_def.ReplicationSpecSvc,
	clusterInfoSvc *service_def.ClusterInfoSvc,
	xdcrTopologySvc *service_def.XDCRCompTopologySvc,
	throughSeqSvc *service_def.ThroughSeqnoTrackerSvc,
	activeVBs map[string][]uint16,
	targetKVVbMap map[string][]uint16,
	remoteClusterRef *metadata.RemoteClusterReference,
	utils *utilities.UtilsIface,
	statsMgr *StatisticsManager) *CheckpointManager {

	ckptManager, _ := NewCheckpointManager(ckptService, capiSvc, remoteClusterSvc,
		replSpecSvc, clusterInfoSvc, xdcrTopologySvc, throughSeqSvc, activeVBs,
		"targetUsername", "targetPassword", "targetBucketName", targetKVVbMap, remoteClusterRef,
		0, false, log.DefaultLoggerContext, utils, statsMgr /*statsMgr*/)

	return ckptManager
}

func TestStatsMgrWithDCPCollector(t *testing.T) {
	fmt.Println("============== Test case start: TestStatsMgrWithDCPCollector =================")
	assert := assert.New(t)
	_, throughSeqSvc, clusterInfoSvc, xdcrTopologySvc, utils, activeVBs,
		pipeline, replicationSpec, runtimeCtx, ckptService, capiSvc, remoteClusterSvc, replSpecSvc,
		targetKVVbMap, remoteClusterRef, dcpNozzle, connector := setupBoilerPlate()

	setupMocks(throughSeqSvc, clusterInfoSvc, xdcrTopologySvc, utils, activeVBs,
		pipeline, replicationSpec, runtimeCtx, ckptService, capiSvc, remoteClusterSvc, replSpecSvc,
		targetKVVbMap, remoteClusterRef, dcpNozzle, connector)

	statsMgr := NewStatisticsManager(throughSeqSvc, clusterInfoSvc, xdcrTopologySvc,
		log.DefaultLoggerContext, activeVBs, "TestBucket", utils)
	assert.NotNil(statsMgr)

	ckptManager := setupCheckpointMgr(ckptService, capiSvc, remoteClusterSvc, replSpecSvc, clusterInfoSvc,
		xdcrTopologySvc, throughSeqSvc, activeVBs, targetKVVbMap, remoteClusterRef, utils, statsMgr)
	setupInnerMock(runtimeCtx, ckptManager)

	statsMgr.Attach(pipeline)
	statsMgr.initOverviewRegistry()

	routerCollector := statsMgr.getRouterCollector()
	assert.Equal(3, len(routerCollector.vbBasedMetric))

	uprEvent, err := RetrieveUprFile(uprEventFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	passedEvent := &commonReal.Event{}
	passedEvent.EventType = commonReal.DataFiltered
	fakeComponent := &common.Component{}
	fakeComponent.On("Id").Return(testRouter)
	passedEvent.Component = fakeComponent
	passedEvent.Data = uprEvent

	routerCollector.ProcessEvent(passedEvent)

	metricsMap, err := statsMgr.GetVBCountMetrics(uprEvent.VBucket)
	assert.Nil(err)
	assert.NotNil(metricsMap)

	count, ok := metricsMap[DOCS_FILTERED_METRIC]
	assert.True(ok)
	assert.Equal(int64(1), count)
	// Simulate overview registry updated
	metric_overview := statsMgr.getOverviewRegistry().Get(DOCS_FILTERED_METRIC)
	metric_overview.(metrics.Counter).Inc(count)

	count = metricsMap[DOCS_UNABLE_TO_FILTER_METRIC]
	assert.Equal(int64(0), count)
	metric_overview = statsMgr.getOverviewRegistry().Get(DOCS_UNABLE_TO_FILTER_METRIC)
	metric_overview.(metrics.Counter).Inc(count)

	// Validate overview stats
	count, err = statsMgr.GetCountMetrics(DOCS_FILTERED_METRIC)
	assert.Nil(err)
	assert.Equal(int64(1), count)

	// Try setting something else
	metricKVs := make(map[string]int64)
	metricKVs[DOCS_UNABLE_TO_FILTER_METRIC] = 10
	err = statsMgr.SetVBCountMetrics(uprEvent.VBucket, metricKVs)
	assert.Nil(err)

	// Set some other vbucket
	metricKVs = make(map[string]int64)
	metricKVs[DOCS_UNABLE_TO_FILTER_METRIC] = 12
	err = statsMgr.SetVBCountMetrics(102, metricKVs)
	assert.Nil(err)

	// Test setting something else
	metricKVs = make(map[string]int64)
	metricKVs[DOCS_FILTERED_METRIC] = 4
	err = statsMgr.SetVBCountMetrics(102, metricKVs)
	assert.Nil(err)

	// verify
	metricsMap, err = statsMgr.GetVBCountMetrics(uprEvent.VBucket)
	assert.Nil(err)
	count = metricsMap[DOCS_UNABLE_TO_FILTER_METRIC]
	assert.Equal(int64(10), count)

	metricsMap, err = statsMgr.GetVBCountMetrics(102)
	assert.Nil(err)
	count = metricsMap[DOCS_UNABLE_TO_FILTER_METRIC]
	assert.Equal(int64(12), count)

	dcpRegistry := statsMgr.registries[testDCPPart]
	assert.NotNil(dcpRegistry)
	routerRegistry := statsMgr.registries[testRouter]
	assert.NotNil(routerRegistry)

	counter, ok := routerRegistry.Get(DOCS_UNABLE_TO_FILTER_METRIC).(metrics.Counter)
	assert.True(ok)
	assert.NotNil(counter)
	assert.Equal(int64(22), counter.Count())

	// Pretend that a rollback occurred for a VB and the number decreased
	metricKVs = make(map[string]int64)
	metricKVs[DOCS_UNABLE_TO_FILTER_METRIC] = 9
	err = statsMgr.SetVBCountMetrics(uprEvent.VBucket, metricKVs)
	assert.Nil(err)

	// validate difference calculation
	metricsMap, err = statsMgr.GetVBCountMetrics(uprEvent.VBucket)
	assert.Nil(err)
	count = metricsMap[DOCS_UNABLE_TO_FILTER_METRIC]
	assert.Equal(int64(9), count)

	routerRegistry = statsMgr.registries[testRouter]
	counter, ok = routerRegistry.Get(DOCS_UNABLE_TO_FILTER_METRIC).(metrics.Counter)
	assert.True(ok)
	assert.NotNil(counter)
	assert.Equal(int64(21), counter.Count())
	fmt.Println("============== Test case end: TestStatsMgrWithDCPCollector =================")
}
