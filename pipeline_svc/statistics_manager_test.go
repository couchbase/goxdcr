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
	service_def2 "github.com/couchbase/goxdcr/service_def"
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
	*common.Connector,
	*service_def.UILogSvc,
	*service_def.CollectionsManifestSvc,
	*service_def.BackfillReplSvc) {

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
	vbs = append(vbs, 221)
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
		false /*isCapi*/, log.DefaultLoggerContext, utils, nil /*func*/)

	connector := &common.Connector{}

	uiLogSvc := &service_def.UILogSvc{}

	collectionsManifestSvc := &service_def.CollectionsManifestSvc{}

	backfillReplSvc := &service_def.BackfillReplSvc{}

	return testLogger, throughSeqSvc, clusterInfoSvc, xdcrTopologySvc, utils, activeVBs,
		pipeline, replicationSpec, runtimeCtx, ckptService, capiSvc, remoteClusterSvc, replSpecSvc,
		targetKVVbMap, remoteClusterRef, dcpNozzle, connector, uiLogSvc, collectionsManifestSvc,
		backfillReplSvc
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
	connector *common.Connector,
	uiLogSvc *service_def.UILogSvc,
	collectionsManifestSvc *service_def.CollectionsManifestSvc,
	backfillReplSvc *service_def.BackfillReplSvc) {

	pipeline.On("Specification").Return(replicationSpec)
	pipeline.On("Topic").Return(pipelineTopic)
	sourceMap := make(map[string]commonReal.Nozzle)
	sourceMap[testDCPPart] = dcpNozzle
	pipeline.On("Sources").Return(sourceMap)
	pipeline.On("Targets").Return(nil)
	pipeline.On("GetAsyncListenerMap").Return(nil)
	pipeline.On("SetAsyncListenerMap", mock.Anything).Return(nil)
	pipeline.On("RuntimeContext").Return(runtimeCtx)
	pipeline.On("Type").Return(commonReal.MainPipeline)
	pipeline.On("FullTopic").Return(pipelineTopic)

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
	statsMgr *StatisticsManager,
	uiLogSvc *service_def.UILogSvc,
	collectionsManifestSvc *service_def.CollectionsManifestSvc,
	backfillReplSvc *service_def.BackfillReplSvc) *CheckpointManager {

	ckptManager, _ := NewCheckpointManager(ckptService, capiSvc, remoteClusterSvc,
		replSpecSvc, clusterInfoSvc, xdcrTopologySvc, throughSeqSvc, activeVBs,
		"targetUsername", "targetPassword", "targetBucketName", targetKVVbMap, remoteClusterRef,
		0, false, log.DefaultLoggerContext, utils, statsMgr, uiLogSvc, collectionsManifestSvc,
		backfillReplSvc)

	return ckptManager
}

func TestStatsMgrWithDCPCollector(t *testing.T) {
	fmt.Println("============== Test case start: TestStatsMgrWithDCPCollector =================")
	assert := assert.New(t)
	_, throughSeqSvc, clusterInfoSvc, xdcrTopologySvc, utils, activeVBs,
		pipeline, replicationSpec, runtimeCtx, ckptService, capiSvc, remoteClusterSvc, replSpecSvc,
		targetKVVbMap, remoteClusterRef, dcpNozzle, connector, uiLogSvc, collectionsManifestSvc,
		backfillReplSvc := setupBoilerPlate()

	setupMocks(throughSeqSvc, clusterInfoSvc, xdcrTopologySvc, utils, activeVBs,
		pipeline, replicationSpec, runtimeCtx, ckptService, capiSvc, remoteClusterSvc, replSpecSvc,
		targetKVVbMap, remoteClusterRef, dcpNozzle, connector, uiLogSvc, collectionsManifestSvc,
		backfillReplSvc)

	statsMgr := NewStatisticsManager(throughSeqSvc, clusterInfoSvc, xdcrTopologySvc,
		log.DefaultLoggerContext, activeVBs, "TestBucket", utils, remoteClusterSvc)
	assert.NotNil(statsMgr)

	ckptManager := setupCheckpointMgr(ckptService, capiSvc, remoteClusterSvc, replSpecSvc, clusterInfoSvc,
		xdcrTopologySvc, throughSeqSvc, activeVBs, targetKVVbMap, remoteClusterRef, utils, statsMgr, uiLogSvc,
		collectionsManifestSvc, backfillReplSvc)
	setupInnerMock(runtimeCtx, ckptManager)

	statsMgr.Attach(pipeline)
	statsMgr.initOverviewRegistry()

	routerCollector := statsMgr.getRouterCollector()
	assert.Equal(4, len(routerCollector.vbBasedMetric))

	uprEvent, err := RetrieveUprFile(uprEventFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	passedEvent := &commonReal.Event{}
	passedEvent.EventType = commonReal.DataFiltered
	fakeComponent := &common.Component{}
	fakeComponent.On("Id").Return(testRouter)
	passedEvent.Component = fakeComponent
	passedEvent.Data = uprEvent

	assert.Nil(routerCollector.ProcessEvent(passedEvent))

	assert.Equal(1, routerCollector.vbBasedHelper[uprEvent.VBucket].sortedSeqnoListMap[service_def2.DOCS_FILTERED_METRIC].GetLengthOfSeqnoList())
	assert.Equal(int64(0), (routerCollector.vbBasedMetric[uprEvent.VBucket][service_def2.DOCS_FILTERED_METRIC]).(metrics.Counter).Count())

	seqnoCommitMap := make(map[uint16]uint64)
	seqnoCommitMap[uprEvent.VBucket] = uprEvent.Seqno

	assert.Equal(int64(0), (routerCollector.vbBasedMetric[uprEvent.VBucket][service_def2.DOCS_FILTERED_METRIC]).(metrics.Counter).Count())
	routerCollector.HandleLatestThroughSeqnos(seqnoCommitMap)
	assert.Equal(int64(1), (routerCollector.vbBasedMetric[uprEvent.VBucket][service_def2.DOCS_FILTERED_METRIC]).(metrics.Counter).Count())
	assert.Equal(0, routerCollector.vbBasedHelper[uprEvent.VBucket].sortedSeqnoListMap[service_def2.DOCS_FILTERED_METRIC].GetLengthOfSeqnoList())

	metricsMap, err := statsMgr.GetVBCountMetrics(uprEvent.VBucket)
	assert.Nil(err)
	assert.NotNil(metricsMap)

	count, ok := metricsMap[service_def2.DOCS_FILTERED_METRIC]
	assert.True(ok)
	assert.Equal(int64(1), count)
	// Simulate overview registry updated
	metric_overview := statsMgr.getOverviewRegistry().Get(service_def2.DOCS_FILTERED_METRIC)
	metric_overview.(metrics.Counter).Inc(count)

	count = metricsMap[service_def2.DOCS_UNABLE_TO_FILTER_METRIC]
	assert.Equal(int64(0), count)
	metric_overview = statsMgr.getOverviewRegistry().Get(service_def2.DOCS_UNABLE_TO_FILTER_METRIC)
	metric_overview.(metrics.Counter).Inc(count)

	// Validate overview stats
	count, err = statsMgr.GetCountMetrics(service_def2.DOCS_FILTERED_METRIC)
	assert.Nil(err)
	assert.Equal(int64(1), count)

	// Try setting something else
	metricKVs := make(map[string]int64)
	metricKVs[service_def2.DOCS_UNABLE_TO_FILTER_METRIC] = 10
	err = statsMgr.SetVBCountMetrics(uprEvent.VBucket, metricKVs)
	assert.Nil(err)

	// Set some other vbucket
	metricKVs = make(map[string]int64)
	metricKVs[service_def2.DOCS_UNABLE_TO_FILTER_METRIC] = 12
	err = statsMgr.SetVBCountMetrics(102, metricKVs)
	assert.Nil(err)

	// Test setting something else
	metricKVs = make(map[string]int64)
	metricKVs[service_def2.DOCS_FILTERED_METRIC] = 4
	err = statsMgr.SetVBCountMetrics(102, metricKVs)
	assert.Nil(err)

	// verify
	metricsMap, err = statsMgr.GetVBCountMetrics(uprEvent.VBucket)
	assert.Nil(err)
	count = metricsMap[service_def2.DOCS_UNABLE_TO_FILTER_METRIC]
	assert.Equal(int64(10), count)

	metricsMap, err = statsMgr.GetVBCountMetrics(102)
	assert.Nil(err)
	count = metricsMap[service_def2.DOCS_UNABLE_TO_FILTER_METRIC]
	assert.Equal(int64(12), count)

	dcpRegistry := statsMgr.registries[testDCPPart]
	assert.NotNil(dcpRegistry)
	routerRegistry := statsMgr.registries[testRouter]
	assert.NotNil(routerRegistry)

	counter, ok := routerRegistry.Get(service_def2.DOCS_UNABLE_TO_FILTER_METRIC).(metrics.Counter)
	assert.True(ok)
	assert.NotNil(counter)
	assert.Equal(int64(22), counter.Count())

	// Pretend that a rollback occurred for a VB and the number decreased
	metricKVs = make(map[string]int64)
	metricKVs[service_def2.DOCS_UNABLE_TO_FILTER_METRIC] = 9
	err = statsMgr.SetVBCountMetrics(uprEvent.VBucket, metricKVs)
	assert.Nil(err)

	// validate difference calculation
	metricsMap, err = statsMgr.GetVBCountMetrics(uprEvent.VBucket)
	assert.Nil(err)
	count = metricsMap[service_def2.DOCS_UNABLE_TO_FILTER_METRIC]
	assert.Equal(int64(9), count)

	routerRegistry = statsMgr.registries[testRouter]
	counter, ok = routerRegistry.Get(service_def2.DOCS_UNABLE_TO_FILTER_METRIC).(metrics.Counter)
	assert.True(ok)
	assert.NotNil(counter)
	assert.Equal(int64(21), counter.Count())
	fmt.Println("============== Test case end: TestStatsMgrWithDCPCollector =================")
}

var uprEventFileWithExpiration string = "../parts/testdata/uprEventExpiration.json"

func TestStatsMgrWithExpiration(t *testing.T) {
	fmt.Println("============== Test case start: TestStatsMgrWithExpiration =================")
	assert := assert.New(t)
	_, throughSeqSvc, clusterInfoSvc, xdcrTopologySvc, utils, activeVBs,
		pipeline, replicationSpec, runtimeCtx, ckptService, capiSvc, remoteClusterSvc, replSpecSvc,
		targetKVVbMap, remoteClusterRef, dcpNozzle, connector, uiLogSvc, collectionsManifestSvc,
		backfillReplSvc := setupBoilerPlate()

	setupMocks(throughSeqSvc, clusterInfoSvc, xdcrTopologySvc, utils, activeVBs,
		pipeline, replicationSpec, runtimeCtx, ckptService, capiSvc, remoteClusterSvc, replSpecSvc,
		targetKVVbMap, remoteClusterRef, dcpNozzle, connector, uiLogSvc, collectionsManifestSvc,
		backfillReplSvc)

	statsMgr := NewStatisticsManager(throughSeqSvc, clusterInfoSvc, xdcrTopologySvc,
		log.DefaultLoggerContext, activeVBs, "TestBucket", utils, remoteClusterSvc)
	assert.NotNil(statsMgr)

	ckptManager := setupCheckpointMgr(ckptService, capiSvc, remoteClusterSvc, replSpecSvc, clusterInfoSvc,
		xdcrTopologySvc, throughSeqSvc, activeVBs, targetKVVbMap, remoteClusterRef, utils, statsMgr, uiLogSvc,
		collectionsManifestSvc, backfillReplSvc)
	setupInnerMock(runtimeCtx, ckptManager)

	statsMgr.Attach(pipeline)
	statsMgr.initOverviewRegistry()

	routerCollector := statsMgr.getRouterCollector()
	assert.Equal(4, len(routerCollector.vbBasedMetric))

	dcpCollector := statsMgr.getdcpCollector()
	assert.NotNil(dcpCollector)

	uprEvent, err := RetrieveUprFile(uprEventFileWithExpiration)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	passedEvent := &commonReal.Event{}
	passedEvent.EventType = commonReal.DataFiltered
	fakeComponent := &common.Component{}
	fakeComponent.On("Id").Return(testRouter).Once()
	passedEvent.Component = fakeComponent
	passedEvent.Data = uprEvent

	assert.Nil(routerCollector.ProcessEvent(passedEvent))
	assert.Equal(int64(1), (routerCollector.component_map[testRouter][service_def2.EXPIRY_FILTERED_METRIC]).(metrics.Counter).Count())
	assert.Equal(int64(1), (routerCollector.component_map[testRouter][service_def2.DOCS_FILTERED_METRIC]).(metrics.Counter).Count())

	passedEvent.EventType = commonReal.DataReceived
	fakeComponent.On("Id").Return(testDCPPart)
	passedEvent.Component = fakeComponent

	assert.Nil(dcpCollector.ProcessEvent(passedEvent))
	assert.Equal(int64(1), (dcpCollector.component_map[testDCPPart][service_def2.EXPIRY_RECEIVED_DCP_METRIC]).(metrics.Counter).Count())
	assert.Equal(int64(1), (dcpCollector.component_map[testDCPPart][service_def2.DOCS_RECEIVED_DCP_METRIC]).(metrics.Counter).Count())

	fmt.Println("============== Test case end: TestStatsMgrWithExpiration =================")
}

func TestFilterVBSeqnoMap(t *testing.T) {
	fmt.Println("============== Test case start: TestFilterVBSeqnoMap =================")
	assert := assert.New(t)

	maxVbSeqnoMap := make(map[uint16]uint64)
	latestVbSeqnoMap := make(map[uint16]uint64)

	latestVbSeqnoMap[0] = 1
	latestVbSeqnoMap[1] = 2
	latestVbSeqnoMap[2] = 3

	populateMaxVbSeqnoMap(latestVbSeqnoMap, maxVbSeqnoMap)
	assert.Equal(3, len(maxVbSeqnoMap))

	var filterVbs []uint16 = []uint16{0, 1}
	maxVbSeqnoMap, err := filterVbSeqnoMap(filterVbs, maxVbSeqnoMap)
	assert.Equal(2, len(maxVbSeqnoMap))
	assert.Nil(err)

	fmt.Println("============== Test case end: TestFilterVBSeqnoMap =================")
}
