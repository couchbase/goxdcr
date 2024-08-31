/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package pipeline_svc

import (
	"encoding/json"
	"expvar"
	"fmt"
	"io/ioutil"
	"strconv"
	"testing"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/pipeline"

	"github.com/couchbase/goxdcr/v8/base/filter"
	"github.com/couchbase/goxdcr/v8/utils"

	mcc "github.com/couchbase/gomemcached/client"
	commonReal "github.com/couchbase/goxdcr/v8/common"
	common "github.com/couchbase/goxdcr/v8/common/mocks"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/parts"

	pm "github.com/couchbase/goxdcr/v8/pipeline/mocks"
	service_def2 "github.com/couchbase/goxdcr/v8/service_def"
	service_def "github.com/couchbase/goxdcr/v8/service_def/mocks"
	utilities "github.com/couchbase/goxdcr/v8/utils/mocks"
	"github.com/rcrowley/go-metrics"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
)

var OverviewStatsBackfill, OverviewStatsMain *expvar.Map

var pipelineTopic string = "topic"

var testDCPPart string = "testDCP"
var testXmemPart string = "testXmem"
var testRouter string = "testRouter"

var seqnoProcessed int64 = 2500
var seqnoTotal int64 = 3000
var backfillEndSeqno int64 = 2800
var backfillStartSeqno int64 = 0
var numVbs int64 = 1024

// clusterKVVbMap = {myself: [0-499], peer1: [500-699], peer2: [700-1023]}
var startVb int64 = 0
var endVbForMyself int64 = 500
var endVbForPeer1 int64 = 700
var endVb int64 = numVbs
var myVbNos int64 = endVbForMyself - startVb

var expectedBackfillTotalChanges int64 = (backfillEndSeqno - backfillStartSeqno) * myVbNos           // 2800*500 = 1400000
var expectedBackfillDocsProcessed int64 = seqnoProcessed * myVbNos                                   // 2500*500 = 1250000
var expectedBackfillChangesLeft int64 = expectedBackfillTotalChanges - expectedBackfillDocsProcessed // 150000
var expectedDocsProcessed int64 = seqnoProcessed*myVbNos + expectedBackfillDocsProcessed             // 2500*500 + 1250000 = 2500000
var expectedTotalChanges int64 = seqnoTotal*myVbNos + expectedBackfillTotalChanges                   // 3000*500 + 1400000 = 2900000
var expectedChangesLeft int64 = expectedTotalChanges - expectedDocsProcessed                         // 400000

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

func setupBoilerPlate() (*log.CommonLogger, *service_def.ThroughSeqnoTrackerSvc, *service_def.XDCRCompTopologySvc, *utilities.UtilsIface, map[string][]uint16, *common.Pipeline, *metadata.ReplicationSpecification, *common.PipelineRuntimeContext, *service_def.CheckpointsService, *service_def.CAPIService, *service_def.RemoteClusterSvc, *service_def.ReplicationSpecSvc, map[string][]uint16, *metadata.RemoteClusterReference, *parts.DcpNozzle, *common.Connector, *service_def.UILogSvc, *service_def.CollectionsManifestSvc, *service_def.BackfillReplSvc, *parts.XmemNozzle) {

	testLogger := log.NewLogger("testLogger", log.DefaultLoggerContext)
	throughSeqSvc := &service_def.ThroughSeqnoTrackerSvc{}
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

	xmemNozzle := parts.NewXmemNozzle(testXmemPart, remoteClusterSvc, "", "", "testTopic", "connPoolPrefix", 0, "connStr", "sourceBucket", "targetBucket", "", "", "", base.CRMode_RevId, nil, utils, vbs, nil, "", "", commonReal.MainPipeline)

	connector := &common.Connector{}

	uiLogSvc := &service_def.UILogSvc{}

	collectionsManifestSvc := &service_def.CollectionsManifestSvc{}

	backfillReplSvc := &service_def.BackfillReplSvc{}

	return testLogger, throughSeqSvc, xdcrTopologySvc, utils, activeVBs,
		pipeline, replicationSpec, runtimeCtx, ckptService, capiSvc, remoteClusterSvc, replSpecSvc,
		targetKVVbMap, remoteClusterRef, dcpNozzle, connector, uiLogSvc, collectionsManifestSvc,
		backfillReplSvc, xmemNozzle
}

func setupMocks(throughSeqSvc *service_def.ThroughSeqnoTrackerSvc, xdcrTopologySvc *service_def.XDCRCompTopologySvc, utils *utilities.UtilsIface, activeVBs map[string][]uint16, pipeline *common.Pipeline, replicationSpec *metadata.ReplicationSpecification, runtimeCtx *common.PipelineRuntimeContext, ckptService *service_def.CheckpointsService, capiSvc *service_def.CAPIService, remoteClusterSvc *service_def.RemoteClusterSvc, replSpecSvc *service_def.ReplicationSpecSvc, targetKVVbMap map[string][]uint16, remoteClusterRef *metadata.RemoteClusterReference, dcpNozzle *parts.DcpNozzle, connector *common.Connector, uiLogSvc *service_def.UILogSvc, collectionsManifestSvc *service_def.CollectionsManifestSvc, backfillReplSvc *service_def.BackfillReplSvc, xmemNozzle *parts.XmemNozzle) {

	pipeline.On("Specification").Return(replicationSpec)
	pipeline.On("Topic").Return(pipelineTopic)
	sourceMap := make(map[string]commonReal.SourceNozzle)
	sourceMap[testDCPPart] = dcpNozzle
	pipeline.On("Sources").Return(sourceMap)
	targetMap := map[string]commonReal.Nozzle{
		xmemNozzle.Id(): xmemNozzle,
	}
	pipeline.On("Targets").Return(targetMap)
	pipeline.On("GetAsyncListenerMap").Return(nil)
	pipeline.On("SetAsyncListenerMap", mock.Anything).Return(nil)
	pipeline.On("RuntimeContext").Return(runtimeCtx)
	pipeline.On("Type").Return(commonReal.MainPipeline)
	pipeline.On("FullTopic").Return(pipelineTopic)

	connector.On("AsyncComponentEventListeners").Return(nil)
	downstreamMap := make(map[string]commonReal.Part)
	downstreamMap[xmemNozzle.Id()] = xmemNozzle
	connector.On("DownStreams").Return(downstreamMap)
	connector.On("Id").Return(testRouter)
	connector.On("RegisterUpstreamPart", mock.Anything).Return(nil)

	dcpNozzle.SetConnector(connector)
}

func setupInnerMock(runtimeCtx *common.PipelineRuntimeContext,
	ckptManager *CheckpointManager) {
	runtimeCtx.On("Service", "CheckpointManager").Return(ckptManager)
	runtimeCtx.On("Service", "ConflictManager").Return(nil)
}

func setupCheckpointMgr(
	ckptService *service_def.CheckpointsService,
	capiSvc *service_def.CAPIService,
	remoteClusterSvc *service_def.RemoteClusterSvc,
	replSpecSvc *service_def.ReplicationSpecSvc,
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

	ckptManager, _ := NewCheckpointManager(ckptService, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqSvc, activeVBs, "targetUsername", "targetPassword", "targetBucketName", targetKVVbMap, remoteClusterRef, log.DefaultLoggerContext, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, nil, nil, false)

	return ckptManager
}

func TestStatsMgrWithDCPCollector(t *testing.T) {
	fmt.Println("============== Test case start: TestStatsMgrWithDCPCollector =================")
	assert := assert.New(t)
	_, throughSeqSvc, xdcrTopologySvc, utils, activeVBs, pipeline, replicationSpec, runtimeCtx, ckptService, capiSvc, remoteClusterSvc, replSpecSvc, targetKVVbMap, remoteClusterRef, dcpNozzle, connector, uiLogSvc, collectionsManifestSvc, backfillReplSvc, xmemNozzle := setupBoilerPlate()

	setupMocks(throughSeqSvc, xdcrTopologySvc, utils, activeVBs,
		pipeline, replicationSpec, runtimeCtx, ckptService, capiSvc, remoteClusterSvc, replSpecSvc,
		targetKVVbMap, remoteClusterRef, dcpNozzle, connector, uiLogSvc, collectionsManifestSvc,
		backfillReplSvc, xmemNozzle)

	statsMgr := NewStatisticsManager(throughSeqSvc, xdcrTopologySvc, log.DefaultLoggerContext, activeVBs, "TestBucket", utils, remoteClusterSvc, nil, nil, false)
	assert.NotNil(statsMgr)

	ckptManager := setupCheckpointMgr(ckptService, capiSvc, remoteClusterSvc, replSpecSvc,
		xdcrTopologySvc, throughSeqSvc, activeVBs, targetKVVbMap, remoteClusterRef, utils, statsMgr, uiLogSvc,
		collectionsManifestSvc, backfillReplSvc)
	setupInnerMock(runtimeCtx, ckptManager)

	statsMgr.Attach(pipeline)
	statsMgr.initOverviewRegistry()

	routerCollector := statsMgr.getRouterCollector()
	assert.Equal(4, len(routerCollector.vbMetricHelper.vbBasedMetric))

	uprEvent, err := RetrieveUprFile(uprEventFile)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	passedEvent := &commonReal.Event{}
	passedEvent.EventType = commonReal.DataFiltered
	fakeComponent := &common.Component{}
	fakeComponent.On("Id").Return(testRouter)
	passedEvent.Component = fakeComponent
	passedEvent.Data = uprEvent
	passedEvent.OtherInfos = parts.DataFilteredAdditional{}

	assert.Nil(routerCollector.ProcessEvent(passedEvent))

	assert.Equal(1, routerCollector.vbMetricHelper.vbBasedHelper[uprEvent.VBucket].sortedSeqnoListMap[service_def2.DOCS_FILTERED_METRIC].GetLengthOfSeqnoList())
	assert.Equal(int64(0), (routerCollector.vbMetricHelper.vbBasedMetric[uprEvent.VBucket][service_def2.DOCS_FILTERED_METRIC]).(metrics.Counter).Count())

	seqnoCommitMap := make(map[uint16]uint64)
	seqnoCommitMap[uprEvent.VBucket] = uprEvent.Seqno

	assert.Equal(int64(0), (routerCollector.vbMetricHelper.vbBasedMetric[uprEvent.VBucket][service_def2.DOCS_FILTERED_METRIC]).(metrics.Counter).Count())
	routerCollector.HandleLatestThroughSeqnos(seqnoCommitMap)
	assert.Equal(int64(1), (routerCollector.vbMetricHelper.vbBasedMetric[uprEvent.VBucket][service_def2.DOCS_FILTERED_METRIC]).(metrics.Counter).Count())
	assert.Equal(0, routerCollector.vbMetricHelper.vbBasedHelper[uprEvent.VBucket].sortedSeqnoListMap[service_def2.DOCS_FILTERED_METRIC].GetLengthOfSeqnoList())

	tradtitionalMetric, err := statsMgr.GetVBCountMetrics(uprEvent.VBucket)
	metricsMap := tradtitionalMetric.(*base.TraditionalVBMetrics).GetValue()
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
	tradMetric := base.NewTraditionalVBMetrics()
	metricKVs := tradMetric.GetValue()
	metricKVs[service_def2.DOCS_UNABLE_TO_FILTER_METRIC] = 10
	err = statsMgr.SetVBCountMetrics(uprEvent.VBucket, tradMetric)
	assert.Nil(err)

	// Set some other vbucket
	tradMetric = base.NewTraditionalVBMetrics()
	metricKVs = tradMetric.GetValue()
	//metricKVs = make(map[string]int64)
	metricKVs[service_def2.DOCS_UNABLE_TO_FILTER_METRIC] = 12
	err = statsMgr.SetVBCountMetrics(102, tradMetric)
	assert.Nil(err)

	// Test setting something else
	tradMetric = base.NewTraditionalVBMetrics()
	metricKVs = tradMetric.GetValue()
	metricKVs[service_def2.DOCS_FILTERED_METRIC] = 4
	err = statsMgr.SetVBCountMetrics(102, tradMetric)
	assert.Nil(err)

	// verify
	vbMetrics, err := statsMgr.GetVBCountMetrics(uprEvent.VBucket)
	assert.Nil(err)
	metricsMap = vbMetrics.(*base.TraditionalVBMetrics).GetValue()
	count = metricsMap[service_def2.DOCS_UNABLE_TO_FILTER_METRIC]
	assert.Equal(int64(10), count)

	vbMetrics, err = statsMgr.GetVBCountMetrics(102)
	assert.Nil(err)
	metricsMap = vbMetrics.(*base.TraditionalVBMetrics).GetValue()
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
	tradMetric = base.NewTraditionalVBMetrics()
	metricKVs = tradMetric.GetValue()
	metricKVs[service_def2.DOCS_UNABLE_TO_FILTER_METRIC] = 9
	err = statsMgr.SetVBCountMetrics(uprEvent.VBucket, tradMetric)
	assert.Nil(err)

	// validate difference calculation
	vbMetrics, err = statsMgr.GetVBCountMetrics(uprEvent.VBucket)
	assert.Nil(err)
	metricsMap = vbMetrics.(*base.TraditionalVBMetrics).GetValue()
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
	_, throughSeqSvc, xdcrTopologySvc, utils, activeVBs, pipeline, replicationSpec, runtimeCtx, ckptService, capiSvc, remoteClusterSvc, replSpecSvc, targetKVVbMap, remoteClusterRef, dcpNozzle, connector, uiLogSvc, collectionsManifestSvc, backfillReplSvc, xmemNozzle := setupBoilerPlate()

	setupMocks(throughSeqSvc, xdcrTopologySvc, utils, activeVBs, pipeline, replicationSpec, runtimeCtx, ckptService, capiSvc, remoteClusterSvc, replSpecSvc, targetKVVbMap, remoteClusterRef, dcpNozzle, connector, uiLogSvc, collectionsManifestSvc, backfillReplSvc, xmemNozzle)

	statsMgr := NewStatisticsManager(throughSeqSvc, xdcrTopologySvc, log.DefaultLoggerContext, activeVBs, "TestBucket", utils, remoteClusterSvc, nil, nil, false)
	assert.NotNil(statsMgr)

	ckptManager := setupCheckpointMgr(ckptService, capiSvc, remoteClusterSvc, replSpecSvc,
		xdcrTopologySvc, throughSeqSvc, activeVBs, targetKVVbMap, remoteClusterRef, utils, statsMgr, uiLogSvc,
		collectionsManifestSvc, backfillReplSvc)
	setupInnerMock(runtimeCtx, ckptManager)

	statsMgr.Attach(pipeline)
	statsMgr.initOverviewRegistry()

	routerCollector := statsMgr.getRouterCollector()
	assert.Equal(4, len(routerCollector.vbMetricHelper.vbBasedMetric))

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
	passedEvent.OtherInfos = parts.DataFilteredAdditional{}

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

	utils.PopulateMaxVbSeqnoMap(latestVbSeqnoMap, maxVbSeqnoMap)
	assert.Equal(3, len(maxVbSeqnoMap))

	var filterVbs []uint16 = []uint16{0, 1}
	err := utils.FilterVbSeqnoMap(filterVbs, &maxVbSeqnoMap)
	assert.Equal(2, len(maxVbSeqnoMap))
	assert.Nil(err)

	fmt.Println("============== Test case end: TestFilterVBSeqnoMap =================")
}

func TestStatsMgrWithFilteringStats(t *testing.T) {
	fmt.Println("============== Test case start: TestStatsMgrWithFilteringStats =================")
	assert := assert.New(t)
	_, throughSeqSvc, xdcrTopologySvc, utils, activeVBs, pipeline, replicationSpec, runtimeCtx, ckptService, capiSvc, remoteClusterSvc, replSpecSvc, targetKVVbMap, remoteClusterRef, dcpNozzle, connector, uiLogSvc, collectionsManifestSvc, backfillReplSvc, xmemNozzle := setupBoilerPlate()

	setupMocks(throughSeqSvc, xdcrTopologySvc, utils, activeVBs, pipeline, replicationSpec, runtimeCtx, ckptService, capiSvc, remoteClusterSvc, replSpecSvc, targetKVVbMap, remoteClusterRef, dcpNozzle, connector, uiLogSvc, collectionsManifestSvc, backfillReplSvc, xmemNozzle)

	statsMgr := NewStatisticsManager(throughSeqSvc, xdcrTopologySvc, log.DefaultLoggerContext, activeVBs, "TestBucket", utils, remoteClusterSvc, nil, nil, false)
	assert.NotNil(statsMgr)

	ckptManager := setupCheckpointMgr(ckptService, capiSvc, remoteClusterSvc, replSpecSvc,
		xdcrTopologySvc, throughSeqSvc, activeVBs, targetKVVbMap, remoteClusterRef, utils, statsMgr, uiLogSvc,
		collectionsManifestSvc, backfillReplSvc)
	setupInnerMock(runtimeCtx, ckptManager)

	statsMgr.Attach(pipeline)
	statsMgr.initOverviewRegistry()

	routerCollector := statsMgr.getRouterCollector()
	assert.Equal(4, len(routerCollector.vbMetricHelper.vbBasedMetric))

	dcpCollector := statsMgr.getdcpCollector()
	assert.NotNil(dcpCollector)

	uprEvent, err := RetrieveUprFile(uprEventFileWithExpiration)
	assert.Nil(err)
	assert.NotNil(uprEvent)

	passedEvent := &commonReal.Event{}

	passedEvent.EventType = commonReal.DataFiltered
	fakeComponent := &common.Component{}

	// Filtering ATR TXN documents
	fakeComponent.On("Id").Return(testRouter).Once()
	passedEvent.Component = fakeComponent
	passedEvent.Data = uprEvent
	passedEvent.OtherInfos = parts.DataFilteredAdditional{FilteringStatus: filter.FilteredOnATRDocument}

	assert.Nil(routerCollector.ProcessEvent(passedEvent))
	assert.Equal(int64(1), (routerCollector.component_map[testRouter][service_def2.DOCS_FILTERED_TXN_ATR_METRIC]).(metrics.Counter).Count())
	assert.Equal(int64(1), (routerCollector.component_map[testRouter][service_def2.DOCS_FILTERED_METRIC]).(metrics.Counter).Count())

	// Filtering client TXN documents
	fakeComponent.On("Id").Return(testRouter).Once()
	passedEvent.Component = fakeComponent
	passedEvent.Data = uprEvent
	passedEvent.OtherInfos = parts.DataFilteredAdditional{FilteringStatus: filter.FilteredOnTxnClientRecord}

	assert.Nil(routerCollector.ProcessEvent(passedEvent))
	assert.Equal(int64(1), (routerCollector.component_map[testRouter][service_def2.DOCS_FILTERED_CLIENT_TXN_METRIC]).(metrics.Counter).Count())
	assert.Equal(int64(2), (routerCollector.component_map[testRouter][service_def2.DOCS_FILTERED_METRIC]).(metrics.Counter).Count())

	// Filtering documents with txn xattrs
	fakeComponent.On("Id").Return(testRouter).Once()
	passedEvent.Component = fakeComponent
	passedEvent.Data = uprEvent
	passedEvent.OtherInfos = parts.DataFilteredAdditional{FilteringStatus: filter.FilteredOnTxnsXattr}

	assert.Nil(routerCollector.ProcessEvent(passedEvent))
	assert.Equal(int64(1), (routerCollector.component_map[testRouter][service_def2.DOCS_FILTERED_TXN_XATTR_METRIC]).(metrics.Counter).Count())
	assert.Equal(int64(3), (routerCollector.component_map[testRouter][service_def2.DOCS_FILTERED_METRIC]).(metrics.Counter).Count())

	// Filtering mobile records
	fakeComponent.On("Id").Return(testRouter).Once()
	passedEvent.Component = fakeComponent
	passedEvent.Data = uprEvent
	passedEvent.OtherInfos = parts.DataFilteredAdditional{FilteringStatus: filter.FilteredOnMobileRecord}

	assert.Nil(routerCollector.ProcessEvent(passedEvent))
	assert.Equal(int64(1), (routerCollector.component_map[testRouter][service_def2.DOCS_FILTERED_MOBILE_METRIC]).(metrics.Counter).Count())
	assert.Equal(int64(4), (routerCollector.component_map[testRouter][service_def2.DOCS_FILTERED_METRIC]).(metrics.Counter).Count())

	// Filtered on User defined filter
	fakeComponent.On("Id").Return(testRouter).Once()
	passedEvent.Component = fakeComponent
	passedEvent.Data = uprEvent
	passedEvent.OtherInfos = parts.DataFilteredAdditional{FilteringStatus: filter.FilteredOnUserDefinedFilter}

	assert.Nil(routerCollector.ProcessEvent(passedEvent))
	assert.Equal(int64(1), (routerCollector.component_map[testRouter][service_def2.DOCS_FILTERED_USER_DEFINED_METRIC]).(metrics.Counter).Count())
	assert.Equal(int64(5), (routerCollector.component_map[testRouter][service_def2.DOCS_FILTERED_METRIC]).(metrics.Counter).Count())

	fmt.Println("============== Test case end: TestStatsMgrWithFilteringStats =================")
}

func getDocsProcessedFromOverviewStats(overviewStats *expvar.Map) int64 {
	docs_processed, err := strconv.ParseInt(overviewStats.Get(service_def2.DOCS_PROCESSED_METRIC).String(), base.ParseIntBase, base.ParseIntBitSize)
	if err != nil {
		panic("FIXME, invalid metrics type for docs_processed")
	}
	return docs_processed
}

func getChangesLeftFromOverviewStats(overviewStats *expvar.Map) int64 {
	changes_left, err := strconv.ParseInt(overviewStats.Get(service_def2.CHANGES_LEFT_METRIC).String(), base.ParseIntBase, base.ParseIntBitSize)
	if err != nil {
		panic("FIXME, invalid metrics type for changes_left")
	}
	return changes_left
}

func setupPausedStatsMocks(backfillSvc *service_def.BackfillReplSvc, remoteSvc *service_def.RemoteClusterSvc, ckptSvc *service_def.CheckpointsService, ref *metadata.RemoteClusterReference, targetKVVbMap map[string][]uint16,
	replSpec *metadata.ReplicationSpecification, assert *assert.Assertions, mainIsNil, backfillIsNil bool) (*service_def.BucketTopologySvc, func() map[string]pipeline.ReplicationStatusIface) {
	cap := metadata.UnitTestGetCollectionsCapability()
	remoteSvc.On("GetCapability", mock.Anything).Return(cap, nil)
	remoteSvc.On("RemoteClusterByUuid", mock.Anything, mock.Anything).Return(ref, nil)

	clusterMap := make(base.KvVBMapType)
	clusterMap["myself"] = make([]uint16, 0)
	clusterMap["peer1"] = make([]uint16, 0)
	clusterMap["peer2"] = make([]uint16, 0)
	targetKVVbMap["myself"] = make([]uint16, 0)
	for i := startVb; i < endVbForMyself; i++ {
		targetKVVbMap["myself"] = append(targetKVVbMap["myself"], uint16(i))
		clusterMap["myself"] = append(clusterMap["myself"], uint16(i))
	}

	for i := endVbForMyself; i < endVbForPeer1; i++ {
		clusterMap["peer1"] = append(clusterMap["peer1"], uint16(i))
	}

	for i := endVbForPeer1; i < endVb; i++ {
		clusterMap["peer2"] = append(clusterMap["peer2"], uint16(i))
	}

	var highMap base.HighSeqnosMapType
	highMap = make(base.HighSeqnosMapType)
	mp := make(map[uint16]uint64)
	highMap["myself"] = &mp
	vbList := make([]uint16, 0)
	vbTasks := metadata.NewVBTasksMap()
	ts := &metadata.BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{Seqno: uint64(backfillStartSeqno)},
		EndingTimestamp:   &base.VBTimestamp{Seqno: uint64(backfillEndSeqno)},
	}
	for i := 0; i < 500; i++ {
		mp[uint16(i)] = uint64(seqnoTotal)
		vbList = append(vbList, uint16(i))
		backfillTasks := metadata.NewBackfillTasks()
		backfillTasks.List = append(backfillTasks.List, metadata.NewBackfillTask(ts, []metadata.CollectionNamespaceMapping{}))
		vbTasks.VBTasksMap[uint16(i)] = &backfillTasks
	}

	ckpts := make(map[uint16]*metadata.CheckpointsDoc, numVbs)
	ckptRecordsList := []*metadata.CheckpointRecord{
		{
			SourceVBTimestamp: metadata.SourceVBTimestamp{
				Seqno: uint64(seqnoProcessed),
			},
		}}
	var ckptlist metadata.CheckpointRecordsList = ckptRecordsList
	for i := 0; i < int(numVbs); i++ {
		ckpts[uint16(i)] = &metadata.CheckpointsDoc{
			Checkpoint_records: ckptlist,
		}
	}

	ckptSvc.On("CheckpointsDocs", mock.Anything, mock.Anything).Return(ckpts, nil)
	var ckpt *metadata.CheckpointsDoc
	ckptSvc.On("CheckpointsDoc", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		vbno, ok := args.Get(1).(uint16)
		if !ok {
			panic("FIXME, vbno not uint16")
		}
		ckpt = ckpts[vbno]
	}).Return(ckpt, nil)

	notx := &service_def.SourceNotification{}
	notx.On("GetHighSeqnosMap").Return(highMap)
	notx.On("GetSourceVBMapRO").Return(base.KvVBMapType(targetKVVbMap))
	notx.On("GetKvVbMapRO").Return(clusterMap)
	notx.On("Recycle").Return()
	bucketSvc := &service_def.BucketTopologySvc{}
	ch := make(chan service_def2.SourceNotification, 1)
	bucketSvc.On("SubscribeToLocalBucketHighSeqnosFeed", mock.Anything, mock.Anything, mock.Anything).Run(func(mock.Arguments) {
		ch <- notx
	}).Return(ch, nil, nil)
	bucketSvc.On("SubscribeToLocalBucketFeed", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		ch <- notx
	}).Return(ch, nil, nil)
	bucketSvc.On("UnSubscribeToLocalBucketHighSeqnosFeed", mock.Anything, mock.Anything).Return(nil)
	bucketSvc.On("UnSubscribeLocalBucketFeed", mock.Anything, mock.Anything).Return(nil)

	backSpec := &metadata.BackfillReplicationSpec{Id: pipelineTopic, VBTasksMap: vbTasks}
	backfillSvc.On("BackfillReplSpec", mock.Anything, mock.Anything).Return(backSpec, nil)

	repl_status := &pm.ReplicationStatusIface{}
	OverviewStatsMain = new(expvar.Map).Init()
	OverviewStatsMain.Add(service_def2.DOCS_PROCESSED_METRIC, 90)
	OverviewStatsMain.Add(service_def2.CHANGES_LEFT_METRIC, 90)
	for _, statsToInitialize := range StatsToInitializeForPausedReplications {
		OverviewStatsMain.Add(statsToInitialize, 0)
	}
	repl_status.On("SetVbList", mock.Anything)
	repl_status.On("Spec").Return(replSpec)
	repl_status.On("RuntimeStatus", mock.Anything).Return(pipeline.Paused)
	repl_status.On("VbList").Return([]uint16{0})
	repl_status.On("RepId").Return(pipelineTopic)
	repl_status.On("Errors").Return(pipeline.PipelineErrorArray{})

	backfillPipMatcher := mock.MatchedBy(func(param interface{}) bool {
		type_, ok := param.(commonReal.PipelineType)
		if !ok {
			fmt.Printf("NOT PIPELINE TYPE")
			return false
		}
		return type_ == commonReal.BackfillPipeline
	})
	mainPipMatcher := mock.MatchedBy(func(param interface{}) bool {
		type_, ok := param.(commonReal.PipelineType)
		if !ok {
			fmt.Printf("NOT PIPELINE TYPE")
			return false
		}
		return type_ == commonReal.MainPipeline
	})

	if mainIsNil {
		OverviewStatsMain = nil
	} else {
		OverviewStatsMain = new(expvar.Map).Init()
		OverviewStatsMain.Add(service_def2.DOCS_PROCESSED_METRIC, 100)
		OverviewStatsMain.Add(service_def2.CHANGES_LEFT_METRIC, 100)
		for _, statsToInitialize := range StatsToInitializeForPausedReplications {
			OverviewStatsMain.Add(statsToInitialize, 0)
		}
	}

	if backfillIsNil {
		OverviewStatsBackfill = nil
	} else {
		OverviewStatsBackfill = new(expvar.Map).Init()
		OverviewStatsBackfill.Add(service_def2.DOCS_PROCESSED_METRIC, 100)
		OverviewStatsBackfill.Add(service_def2.CHANGES_LEFT_METRIC, 100)
		for _, statsToInitialize := range StatsToInitializeForPausedReplications {
			OverviewStatsBackfill.Add(statsToInitialize, 0)
		}
	}

	repl_status.On("SetOverviewStats", mock.Anything, backfillPipMatcher).Run(func(args mock.Arguments) {
		os, ok := args.Get(0).(*expvar.Map)
		if !ok {
			panic("FIXME, SetOverviewStats of backfill")
		}
		OverviewStatsBackfill = os
	})

	repl_status.On("SetOverviewStats", mock.Anything, mainPipMatcher).Run(func(args mock.Arguments) {
		os, ok := args.Get(0).(*expvar.Map)
		if !ok {
			panic("FIXME, SetOverviewStats of main")
		}
		OverviewStatsMain = os
	})

	repl_status.On("GetOverviewStats", mainPipMatcher).Return(OverviewStatsMain)
	repl_status.On("GetOverviewStats", backfillPipMatcher).Return(OverviewStatsBackfill)

	replGetter := func() map[string]pipeline.ReplicationStatusIface {
		return map[string]pipeline.ReplicationStatusIface{
			pipelineTopic: repl_status,
		}
	}

	return bucketSvc, replGetter
}

func assertPausedStats(OverviewStatsMain, OverviewStatsBackfill *expvar.Map, assert *assert.Assertions) {
	cl := getChangesLeftFromOverviewStats(OverviewStatsMain)
	dp := getDocsProcessedFromOverviewStats(OverviewStatsMain)
	assert.Equal(expectedDocsProcessed, dp)
	assert.Equal(expectedChangesLeft, cl)
	bcl := getChangesLeftFromOverviewStats(OverviewStatsBackfill)
	bdp := getDocsProcessedFromOverviewStats(OverviewStatsBackfill)
	assert.Equal(expectedBackfillDocsProcessed, bdp)
	assert.Equal(expectedBackfillChangesLeft, bcl)
}

func TestPausedReplicationStats1a(t *testing.T) {
	fmt.Println("============== Test case start: TestPausedReplicationContructStats 1a =================")
	defer fmt.Println("============== Test case start: TestPausedReplicationContructStats 1a =================")

	assert := assert.New(t)
	logger, _, _, _, _, _, replSpec, _, ckptSvc, _, remoteSvc, _, targetKVVbMap, ref, _, _, _, _, backfillSvc, _ := setupBoilerPlate()
	// main is nil, backfill is not
	bucketSvc, replGetter := setupPausedStatsMocks(backfillSvc, remoteSvc, ckptSvc, ref, targetKVVbMap, replSpec, assert, true, false)
	UpdateStats(ckptSvc, logger, remoteSvc, backfillSvc, bucketSvc, replGetter)
	assertPausedStats(OverviewStatsMain, OverviewStatsBackfill, assert)
}

func TestPausedReplicationStats1b(t *testing.T) {
	fmt.Println("============== Test case start: TestPausedReplicationContructStats 1b =================")
	defer fmt.Println("============== Test case start: TestPausedReplicationContructStats 1b =================")

	assert := assert.New(t)
	logger, _, _, _, _, _, replSpec, _, ckptSvc, _, remoteSvc, _, targetKVVbMap, ref, _, _, _, _, backfillSvc, _ := setupBoilerPlate()
	// main is nil, backfill is nil
	bucketSvc, replGetter := setupPausedStatsMocks(backfillSvc, remoteSvc, ckptSvc, ref, targetKVVbMap, replSpec, assert, true, true)
	UpdateStats(ckptSvc, logger, remoteSvc, backfillSvc, bucketSvc, replGetter)
	assertPausedStats(OverviewStatsMain, OverviewStatsBackfill, assert)
}

func TestPausedReplicationStats2a(t *testing.T) {
	fmt.Println("============== Test case start: TestPausedReplicationUpdateStats 2a =================")
	defer fmt.Println("============== Test case start: TestPausedReplicationUpdateStats 2a =================")

	assert := assert.New(t)
	logger, _, _, _, _, _, replSpec, _, ckptSvc, _, remoteSvc, _, targetKVVbMap, ref, _, _, _, _, backfillSvc, _ := setupBoilerPlate()
	// main is not nil, backfill is not nil
	bucketSvc, replGetter := setupPausedStatsMocks(backfillSvc, remoteSvc, ckptSvc, ref, targetKVVbMap, replSpec, assert, false, false)
	UpdateStats(ckptSvc, logger, remoteSvc, backfillSvc, bucketSvc, replGetter)
	assertPausedStats(OverviewStatsMain, OverviewStatsBackfill, assert)
}

func TestPausedReplicationStats2b(t *testing.T) {
	fmt.Println("============== Test case start: TestPausedReplicationUpdateStats 2b =================")
	defer fmt.Println("============== Test case start: TestPausedReplicationUpdateStats 2b =================")

	assert := assert.New(t)
	logger, _, _, _, _, _, replSpec, _, ckptSvc, _, remoteSvc, _, targetKVVbMap, ref, _, _, _, _, backfillSvc, _ := setupBoilerPlate()
	// main is not nil, backfill is nil
	bucketSvc, replGetter := setupPausedStatsMocks(backfillSvc, remoteSvc, ckptSvc, ref, targetKVVbMap, replSpec, assert, false, true)
	UpdateStats(ckptSvc, logger, remoteSvc, backfillSvc, bucketSvc, replGetter)
	assertPausedStats(OverviewStatsMain, OverviewStatsBackfill, assert)
}
