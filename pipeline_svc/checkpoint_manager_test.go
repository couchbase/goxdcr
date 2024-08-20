// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package pipeline_svc

import (
	"encoding/json"
	"fmt"
	mcc "github.com/couchbase/gomemcached/client"
	mocks3 "github.com/couchbase/gomemcached/client/mocks"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/common"
	"github.com/couchbase/goxdcr/v8/common/mocks"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	mocks2 "github.com/couchbase/goxdcr/v8/metadata/mocks"
	service_def_real "github.com/couchbase/goxdcr/v8/service_def"
	service_def "github.com/couchbase/goxdcr/v8/service_def/mocks"
	"github.com/couchbase/goxdcr/v8/utils"
	utilities "github.com/couchbase/goxdcr/v8/utils/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"io/ioutil"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCombineFailoverlogs(t *testing.T) {
	fmt.Println("============== Test case start: TestCombineFailoverLogs =================")
	defer fmt.Println("============== Test case end: TestCombineFailoverLogs =================")
	assert := assert.New(t)

	var goodVbUuid uint64 = 1
	var badVbUuid uint64 = 2
	var goodSeqno uint64 = 5
	failoverLog := &mcc.FailoverLog{[2]uint64{goodVbUuid, goodSeqno}}

	failoverLogMap := make(map[uint16]*mcc.FailoverLog)
	// 3 VBs from failoverlog
	failoverLogMap[0] = failoverLog
	failoverLogMap[1] = failoverLog
	failoverLogMap[2] = failoverLog

	goodRecord := &metadata.CheckpointRecord{
		Failover_uuid: goodVbUuid,
		Seqno:         goodSeqno,
	}
	badRecord := &metadata.CheckpointRecord{
		Failover_uuid: badVbUuid,
		Seqno:         goodSeqno,
	}

	goodDoc := &metadata.CheckpointsDoc{
		Checkpoint_records: []*metadata.CheckpointRecord{goodRecord},
	}

	badDoc := &metadata.CheckpointsDoc{
		Checkpoint_records: []*metadata.CheckpointRecord{badRecord},
	}

	mixedDoc := &metadata.CheckpointsDoc{
		Checkpoint_records: []*metadata.CheckpointRecord{goodRecord, badRecord},
	}

	checkMap := make(nodeVbCkptMap)
	nodeName := "node"
	checkMap[nodeName] = make(map[uint16]*metadata.CheckpointsDoc)
	checkMap[nodeName][0] = goodDoc
	checkMap[nodeName][1] = mixedDoc
	checkMap[nodeName][2] = badDoc
	checkMap[nodeName][3] = goodDoc // vb3 isn't included

	results := filterInvalidCkptsBasedOnSourceFailover([]nodeVbCkptMap{checkMap, nil}, failoverLogMap)
	result := results[0]
	assert.Len(result, 3)

	assert.Len(result[0].Checkpoint_records, 1)
	assert.Len(result[1].Checkpoint_records, 1)
	assert.Len(result[2].Checkpoint_records, 0)
}

func TestCombineFailoverlogsWithData(t *testing.T) {
	fmt.Println("============== Test case start: TestCombineFailoverLogsWithData =================")
	defer fmt.Println("============== Test case end: TestCombineFailoverLogsWithData =================")
	assert := assert.New(t)

	nodeVbCkptsMapSlice, err := ioutil.ReadFile("./unitTestdata/nodeVbCkptsMap.json")
	assert.Nil(err)
	srcFailoverLogsSlice, err := ioutil.ReadFile("./unitTestdata/srcFailoverLogs.json")
	assert.Nil(err)

	nodeVbCkptsMap := make(nodeVbCkptMap)
	assert.Nil(json.Unmarshal(nodeVbCkptsMapSlice, &nodeVbCkptsMap))
	srcFailoverLogs := make(map[uint16]*mcc.FailoverLog)
	assert.Nil(json.Unmarshal(srcFailoverLogsSlice, &srcFailoverLogs))

	for _, ckptMapDoc := range nodeVbCkptsMap {
		for vb, ckptDoc := range ckptMapDoc {
			assert.NotEqual(0, len(ckptDoc.Checkpoint_records))
			assert.NotNil(ckptDoc.Checkpoint_records[0])
			assert.NotNil(srcFailoverLogs[vb])
		}
	}

	filteredMaps := filterInvalidCkptsBasedOnSourceFailover([]nodeVbCkptMap{nodeVbCkptsMap, nil}, srcFailoverLogs)
	filteredMap := filteredMaps[0]
	for _, ckptDoc := range filteredMap {
		assert.NotEqual(0, len(ckptDoc.Checkpoint_records))
	}

	tgtFailoverLogsSlice, err := ioutil.ReadFile("./unitTestdata/tgtFailoverJson.json")
	assert.Nil(err)
	tgtFailoverLogs := make(map[uint16]*mcc.FailoverLog)
	assert.Nil(json.Unmarshal(tgtFailoverLogsSlice, &tgtFailoverLogs))
	filteredMapTgts := filterInvalidCkptsBasedOnTargetFailover([]metadata.VBsCkptsDocMap{filteredMap, nil}, tgtFailoverLogs)
	filteredMapTgt := filteredMapTgts[0]
	for _, ckptDoc := range filteredMapTgt {
		assert.NotEqual(0, len(ckptDoc.Checkpoint_records))
	}
}

func TestCheckpointSyncHelper(t *testing.T) {
	fmt.Println("============== Test case start: TestCheckpointSyncHelper =================")
	defer fmt.Println("============== Test case end: TestCheckpointSyncHelper =================")
	assert := assert.New(t)

	helper := newCheckpointSyncHelper()

	helper.disableCkptAndWait()
	_, err := helper.registerCkptOp(false)
	assert.NotNil(err)

	helper.setCheckpointAllowed()
	idx, err := helper.registerCkptOp(false)
	idx2, err2 := helper.registerCkptOp(false)
	assert.Nil(err)
	assert.Nil(err2)

	var waitIsFinished uint32
	go func() {
		helper.disableCkptAndWait()
		atomic.StoreUint32(&waitIsFinished, 1)
	}()

	helper.markTaskDone(idx)
	time.Sleep(50 * time.Millisecond)
	assert.True(atomic.LoadUint32(&waitIsFinished) == uint32(0))
	helper.markTaskDone(idx2)
	time.Sleep(50 * time.Millisecond)
	assert.True(atomic.LoadUint32(&waitIsFinished) == uint32(1))

	assert.Len(helper.ongoingOps, 0)
}

func TestMergeNoConsensusCkpt(t *testing.T) {
	fmt.Println("============== Test case start: TestMergeNoConsensusCkpt =================")
	defer fmt.Println("============== Test case end: TestMergeNoConsensusCkpt =================")
	assert := assert.New(t)

	ckptMgr := &CheckpointManager{}

	result, err := ckptMgr.checkSpecInternalID(nil)
	assert.Nil(err)
	assert.Equal("", result)

	// Test majority
	majorityInternalId := "testInternalId"
	genSpec := &mocks2.GenericSpecification{}
	majoritySpec, err := metadata.NewReplicationSpecification("", "", "", "", "")
	majoritySpec.InternalId = majorityInternalId
	genSpec.On("GetReplicationSpec").Return(majoritySpec)

	mockPipeline := &mocks.Pipeline{}
	mockPipeline.On("Specification").Return(genSpec)
	ckptMgr.pipeline = mockPipeline

	majorityMap := make(map[string]string)
	majorityMap["node1"] = majorityInternalId
	majorityMap["node2"] = majorityInternalId
	majorityMap["node3"] = "spec232tungwoin"
	result, err = ckptMgr.checkSpecInternalID(majorityMap)
	assert.Nil(err)
	assert.Equal(majorityInternalId, result)

	// Test no concensus
	noConsensusMap := make(map[string]string)
	noConsensusMap["node1"] = "abc"
	noConsensusMap["node2"] = "def"
	noConsensusMap["node3"] = "efg"
	_, err = ckptMgr.checkSpecInternalID(noConsensusMap)
	assert.NotNil(err)
}

func TestMergEmptyCkpts(t *testing.T) {
	fmt.Println("============== Test case start: TestMergEmptyCkpts =================")
	defer fmt.Println("============== Test case end: TestMergEmptyCkpts =================")
	assert := assert.New(t)

	filteredMap := make(map[uint16]*metadata.CheckpointsDoc)
	currentDocs := make(map[uint16]*metadata.CheckpointsDoc)

	spec, _ := metadata.NewReplicationSpecification("", "", "", "", "")
	filteredMap[0] = &metadata.CheckpointsDoc{
		Checkpoint_records: nil,
		SpecInternalId:     "",
		Revision:           nil,
	}

	assert.Len(currentDocs, 0)
	combinePeerCkptDocsWithLocalCkptDoc(filteredMap, nil, nil, currentDocs, spec)
	assert.Len(currentDocs, 0)

	var recordsList metadata.CheckpointRecordsList
	record := &metadata.CheckpointRecord{}
	recordsList = append(recordsList, record)
	filteredMap[0] = &metadata.CheckpointsDoc{
		Checkpoint_records: recordsList,
		SpecInternalId:     "testId",
		Revision:           nil,
	}

	assert.Len(currentDocs, 0)
	combinePeerCkptDocsWithLocalCkptDoc(filteredMap, nil, nil, currentDocs, spec)
	assert.Len(currentDocs, 1)

	filteredMap[1] = &metadata.CheckpointsDoc{
		Checkpoint_records: recordsList,
		SpecInternalId:     "testId",
		Revision:           nil,
	}
	filteredMap[2] = &metadata.CheckpointsDoc{}
	combinePeerCkptDocsWithLocalCkptDoc(filteredMap, nil, nil, currentDocs, spec)
	assert.Len(currentDocs, 2)
}

const kvKey = "serverName"
const srcBucketName = "srcBucket"
const tgtBucketName = "tgtBucket"

var vbList = []uint16{0, 1}

var utilsReal = utils.NewUtilities()

// Implements ComponentEventListner
type ckptDoneListener struct {
	ckptDoneCount uint64
}

func (cl *ckptDoneListener) OnEvent(event *common.Event) {
	if event == nil {
		return
	}
	if event.EventType == common.CheckpointDone {
		atomic.AddUint64(&cl.ckptDoneCount, 1)
	}
}

type statsMapResult struct {
	statsMap map[string]string
	err      error
}

func setupCkptMgrBoilerPlate() (*service_def.CheckpointsService, *service_def.CAPIService, *service_def.RemoteClusterSvc, *service_def.ReplicationSpecSvc, *service_def.XDCRCompTopologySvc, *service_def.ThroughSeqnoTrackerSvc, *utilities.UtilsIface, *service_def.StatsMgrIface, *service_def.UILogSvc, *service_def.CollectionsManifestSvc, *service_def.BackfillReplSvc, *service_def.BackfillMgrIface, func() service_def_real.BackfillMgrIface, *service_def.BucketTopologySvc, *metadata.ReplicationSpecification, *PipelineSupervisor, *ckptDoneListener, map[string][]uint16, map[string]*mocks3.ClientIface, map[string]int, map[string]statsMapResult) {
	ckptSvc := &service_def.CheckpointsService{}
	capiSvc := &service_def.CAPIService{}
	remoteClusterSvc := &service_def.RemoteClusterSvc{}
	replSpecSvc := &service_def.ReplicationSpecSvc{}
	xdcrTopologySvc := &service_def.XDCRCompTopologySvc{}
	throughSeqnoTrackerSvc := &service_def.ThroughSeqnoTrackerSvc{}
	utilsMock := &utilities.UtilsIface{}
	statsMgr := &service_def.StatsMgrIface{}
	uiLogSvc := &service_def.UILogSvc{}
	collectionsManifestSvc := &service_def.CollectionsManifestSvc{}
	backfillReplSvc := &service_def.BackfillReplSvc{}
	backfillMgrIface := &service_def.BackfillMgrIface{}
	getBackfillMgr := func() service_def_real.BackfillMgrIface {
		return backfillMgrIface
	}
	bucketTopologySvc := &service_def.BucketTopologySvc{}

	spec, _ := metadata.NewReplicationSpecification(srcBucketName, "", "", tgtBucketName, "")

	pipelineSupervisor := &PipelineSupervisor{}

	ckptDoneListener := &ckptDoneListener{}

	targetKvVbMap := make(map[string][]uint16)
	targetKvVbMap[kvKey] = vbList
	targetMCMap := make(map[string]*mocks3.ClientIface)
	targetMCMap[kvKey] = &mocks3.ClientIface{}

	targetMCDelayMap := make(map[string]int)
	targetMCDelayMap[kvKey] = 0

	targetMCStatMapReturn := make(map[string]statsMapResult)

	return ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc,
		utilsMock, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, getBackfillMgr,
		bucketTopologySvc, spec, pipelineSupervisor, ckptDoneListener, targetKvVbMap, targetMCMap, targetMCDelayMap,
		targetMCStatMapReturn
}

func setupBackfillPipelineMock(spec *metadata.ReplicationSpecification, supervisor *PipelineSupervisor, task *metadata.BackfillTask) *mocks.Pipeline {
	runtimeCtx := &mocks.PipelineRuntimeContext{}
	runtimeCtx.On("Service", base.PIPELINE_SUPERVISOR_SVC).Return(supervisor)

	taskMap := make(map[uint16]*metadata.BackfillTasks)
	tasks := &metadata.BackfillTasks{}
	tasks.List = append(tasks.List, task)
	taskMap[0] = tasks
	vbTaskMap := metadata.NewVBTasksMap()
	vbTaskMap.VBTasksMap = taskMap
	backfillSpec := metadata.NewBackfillReplicationSpec(spec.Id, spec.InternalId, vbTaskMap, spec, 0)

	genericSpec := &mocks2.GenericSpecification{}
	genericSpec.On("GetReplicationSpec").Return(spec)
	genericSpec.On("GetBackfillSpec").Return(backfillSpec)

	pipeline := &mocks.Pipeline{}
	pipeline.On("Type").Return(common.BackfillPipeline)
	pipeline.On("Sources").Return(nil)
	pipeline.On("Specification").Return(genericSpec)
	pipeline.On("Topic").Return("pipelineTopic")
	pipeline.On("FullTopic").Return("backfill_pipelineTopic")
	pipeline.On("RuntimeContext").Return(runtimeCtx)
	pipeline.On("InstanceId").Return("randomInstance")

	pipeline.On("UpdateSettings", mock.Anything).Return(nil)

	return pipeline
}

func setupMainPipelineMock(spec *metadata.ReplicationSpecification, supervisor *PipelineSupervisor) *mocks.Pipeline {
	runtimeCtx := &mocks.PipelineRuntimeContext{}
	runtimeCtx.On("Service", base.PIPELINE_SUPERVISOR_SVC).Return(supervisor)

	genericSpec := &mocks2.GenericSpecification{}
	genericSpec.On("GetReplicationSpec").Return(spec)

	pipeline := &mocks.Pipeline{}
	pipeline.On("Type").Return(common.MainPipeline)
	pipeline.On("Sources").Return(nil)
	pipeline.On("Specification").Return(genericSpec)
	pipeline.On("Topic").Return("pipelineTopic")
	pipeline.On("FullTopic").Return("pipelineTopic")
	pipeline.On("RuntimeContext").Return(runtimeCtx)
	pipeline.On("InstanceId").Return("randomInstance")

	pipeline.On("UpdateSettings", mock.Anything).Return(nil)

	return pipeline
}

func getBackfillTask() *metadata.BackfillTask {
	namespaceMapping := make(metadata.CollectionNamespaceMapping)
	defaultNamespace := &base.CollectionNamespace{base.DefaultScopeCollectionName, base.DefaultScopeCollectionName}
	namespaceMapping.AddSingleMapping(defaultNamespace, defaultNamespace)
	manifestsIdPair := base.CollectionsManifestIdPair{0, 0}

	ts0 := &metadata.BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{0, 0, 5, 5, 5, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{0, 0, 500, 500, 500, manifestsIdPair},
	}

	vb0Task0 := metadata.NewBackfillTask(ts0, []metadata.CollectionNamespaceMapping{namespaceMapping})
	vb0Tasks := metadata.NewBackfillTasks()
	vb0Tasks.List = append(vb0Tasks.List, vb0Task0)

	return vb0Task0
}
func setupMock(ckptSvc *service_def.CheckpointsService, capiSvc *service_def.CAPIService, remClusterSvc *service_def.RemoteClusterSvc, replSpecSvc *service_def.ReplicationSpecSvc, xdcrCompTopologySvc *service_def.XDCRCompTopologySvc, throughSeqnoSvc *service_def.ThroughSeqnoTrackerSvc, utilsMock *utilities.UtilsIface, statsMgr *service_def.StatsMgrIface, uilogSvc *service_def.UILogSvc, colManSvc *service_def.CollectionsManifestSvc, backfillReplSvc *service_def.BackfillReplSvc, backfillMgr *service_def.BackfillMgrIface, bucketTopologySvc *service_def.BucketTopologySvc, spec *metadata.ReplicationSpecification, supervisor *PipelineSupervisor, throughSeqnoMap map[uint16]uint64, upsertCkptsDoneErr error, mcMap map[string]*mocks3.ClientIface, delayMap map[string]int, result map[string]statsMapResult) {

	bucketInfoFile := "../utils/testInternalData/pools_default_buckets_b2.json"
	bucketInfoData, err := ioutil.ReadFile(bucketInfoFile)
	if err != nil {
		panic(err)
	}
	bucketMap := make(map[string]interface{})
	err = json.Unmarshal(bucketInfoData, &bucketMap)
	if err != nil {
		panic(err)
	}

	getRemoteServerVBMapFunc := func(string, string, map[string]interface{}, bool) map[string][]uint16 {
		vbMap, _ := utilsReal.GetServerVBucketsMap("", "b2", bucketMap, nil, nil)
		return *vbMap
	}
	getNilFunc := func(string, string, map[string]interface{}, bool) error {
		return nil
	}

	getCompatibilityFunc := func(map[string]interface{}, *log.CommonLogger) int {
		intVal, _ := utilsReal.GetClusterCompatibilityFromBucketInfo(bucketMap, nil)
		return intVal
	}
	getNilFunc2 := func(map[string]interface{}, *log.CommonLogger) error {
		return nil
	}

	remClusterSvc.On("ShouldUseAlternateAddress", mock.Anything).Return(false, nil)
	utilsMock.On("GetBucketInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(bucketMap, nil)
	utilsMock.On("GetRemoteServerVBucketsMap", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(getRemoteServerVBMapFunc, getNilFunc)
	utilsMock.On("GetClusterCompatibilityFromBucketInfo", mock.Anything, mock.Anything).Return(getCompatibilityFunc, getNilFunc2)
	utilsMock.On("GetNodeListFromInfoMap", mock.Anything, mock.Anything).Return(base.GetNodeListFromInfoMap(bucketMap, nil))
	utilsMock.On("GetHostAddrFromNodeInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("localhost", nil)
	utilsMock.On("ParseHighSeqnoAndVBUuidFromStats", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	utilsMock.On("ExponentialBackoffExecutorWithFinishSignal", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		utilsReal.ExponentialBackoffExecutorWithFinishSignal(args.Get(0).(string), args.Get(1).(time.Duration), args.Get(2).(int), args.Get(3).(int), args.Get(4).(utils.ExponentialOpFunc2), args.Get(5), args.Get(6).(chan bool))
	}).Return(nil, nil)

	for kvName, client := range mcMap {
		utilsMock.On("GetRemoteMemcachedConnection", kvName, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(client, nil)
	}

	capiSvc.On("PreReplicate", mock.Anything, mock.Anything, mock.Anything).Return(true, nil, nil)

	statsMgr.On("SetVBCountMetrics", mock.Anything, mock.Anything).Return(nil)
	statsMgr.On("HandleLatestThroughSeqnos", mock.Anything).Return(nil)

	throughSeqnoSvc.On("SetStartSeqno", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		timeStampMtx.Lock()
		timeStampSetSeqno = args.Get(1).(uint64)
		timeStampSetCnt++
		timeStampMtx.Unlock()
	}).Return(nil)

	throughSeqnoSvc.On("GetThroughSeqnos").Return(throughSeqnoMap, nil)
	throughSeqnoSvc.On("GetThroughSeqnosAndManifestIds").Return(nil, nil, nil)

	ckptSvc.On("UpsertCheckpointsDone", mock.Anything, mock.Anything).Return(upsertCkptsDoneErr)
	ckptSvc.On("PreUpsertBrokenMapping", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	ckptSvc.On("UpsertBrokenMapping", mock.Anything, mock.Anything).Return(nil)

	for k, client := range mcMap {
		client.On("StatsMap", base.VBUCKET_SEQNO_STAT_NAME).Run(func(args mock.Arguments) { time.Sleep(time.Duration(delayMap[k]) * time.Second) }).Return(result[k].statsMap, result[k].err)
	}

	colManSvc.On("PersistNeededManifests", mock.Anything).Return(nil)
}

var timeStampSetSeqno uint64
var timeStampSetCnt int
var timeStampMtx sync.RWMutex

func TestBackfillVBTaskResume(t *testing.T) {
	fmt.Println("============== Test case start: TestBackfillVBTaskResume =================")
	defer fmt.Println("============== Test case end: TestBackfillVBTaskResume =================")
	assert := assert.New(t)

	ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, getBackfillMgr, bucketTopologySvc, spec, pipelineSupervisor, _, targetKvVbMap, targetMCMap, targetMcDelayMap, targetMCStatsResult := setupCkptMgrBoilerPlate()

	activeVBs := make(map[string][]uint16)
	activeVBs[kvKey] = []uint16{0}
	targetRef, _ := metadata.NewRemoteClusterReference("", "C2", "", "", "",
		"", false, "", nil, nil, nil, nil)

	setupMock(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, bucketTopologySvc, spec, pipelineSupervisor, nil, nil, targetMCMap, targetMcDelayMap, targetMCStatsResult)

	ckptMgr, err := NewCheckpointManager(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc,
		throughSeqnoTrackerSvc, activeVBs, "", "", "", targetKvVbMap,
		targetRef, nil, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc,
		getBackfillMgr, bucketTopologySvc)

	assert.Nil(err)
	assert.NotNil(ckptMgr)
	ckptMgr.unitTest = true

	backfillTask := getBackfillTask()
	backfillPipeline := setupBackfillPipelineMock(spec, pipelineSupervisor, backfillTask)
	assert.Nil(ckptMgr.Attach(backfillPipeline))

	assert.Len(ckptMgr.backfillStartingTs, 1)

	// Let's mock some ckpt docs for vb0
	ckptDocs := mockVBCkptDoc(spec, 0)

	var waitGrp sync.WaitGroup
	waitGrp.Add(1)
	errCh := make(chan interface{}, 1)
	var lowestManifestId uint64
	var vbtsStartedNon0 uint32
	ckptMgr.startSeqnoGetter(0, []uint16{0}, ckptDocs, &waitGrp, errCh, &lowestManifestId, &vbtsStartedNon0)
	waitGrp.Wait()

	timeStampMtx.RLock()
	assert.Equal(1, timeStampSetCnt)
	assert.Equal(500, int(timeStampSetSeqno))
	timeStampMtx.RUnlock()
}

func mockVBCkptDoc(spec *metadata.ReplicationSpecification, i uint16) map[uint16]*metadata.CheckpointsDoc {
	ckptDocs := make(map[uint16]*metadata.CheckpointsDoc)
	records := metadata.CheckpointRecordsList{}
	record := &metadata.CheckpointRecord{
		Seqno:                  1000,
		Dcp_snapshot_seqno:     1000,
		Dcp_snapshot_end_seqno: 1000,
		Target_Seqno:           1000,
	}
	record2 := &metadata.CheckpointRecord{
		Seqno:                  500,
		Dcp_snapshot_seqno:     500,
		Dcp_snapshot_end_seqno: 500,
		Target_Seqno:           500,
	}
	records = append(records, record)
	records = append(records, record2)
	ckptDoc := &metadata.CheckpointsDoc{
		Checkpoint_records: records,
		SpecInternalId:     spec.InternalId,
		Revision:           nil,
	}
	ckptDocs[i] = ckptDoc
	return ckptDocs
}

func TestCkptMgrPeriodicMerger(t *testing.T) {
	fmt.Println("============== Test case start: TestCkptMgrPeriodicMerger =================")
	defer fmt.Println("============== Test case end: TestCkptMgrPeriodicMerger =================")
	assert := assert.New(t)

	ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, getBackfillMgr, bucketTopologySvc, spec, pipelineSupervisor, _, targetKvVbMap, targetMCMap, targetMcDelayMap, targetMCStatsResult := setupCkptMgrBoilerPlate()

	activeVBs := make(map[string][]uint16)
	activeVBs[kvKey] = []uint16{0}
	targetRef, _ := metadata.NewRemoteClusterReference("", "C2", "", "", "",
		"", false, "", nil, nil, nil, nil)

	setupMock(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, bucketTopologySvc, spec, pipelineSupervisor, nil, nil, targetMCMap, targetMcDelayMap, targetMCStatsResult)

	ckptMgr, err := NewCheckpointManager(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc,
		throughSeqnoTrackerSvc, activeVBs, "", "", "", targetKvVbMap,
		targetRef, nil, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc,
		getBackfillMgr, bucketTopologySvc)

	assert.Nil(err)
	assert.NotNil(ckptMgr)
	ckptMgr.unitTest = true

	var mergerCalledWg sync.WaitGroup
	var mergeCallOnce sync.Once //
	// Test simple merger
	simpleMerger := func() {
		for {
			select {
			case <-ckptMgr.periodicPushRequested:
				args := ckptMgr.periodicBatchGetter()
				mergeCallOnce.Do(func() {
					mergerCalledWg.Done()
				})
				// Sleep here to simulate processing time while another is queued up
				time.Sleep(500 * time.Millisecond)
				assert.NotNil(args)
				assert.Equal(1, len(args.PushRespChs))
				for _, ch := range args.PushRespChs {
					ch <- nil
				}
			case <-ckptMgr.finish_ch:
				return
			}
		}

	}
	ckptMgr.periodicMerger = simpleMerger
	// simulate Start
	go ckptMgr.periodicMerger()

	// Lazy way of creating a ckptDoc with 2 VBs
	ckptDoc := mockVBCkptDoc(spec, 12)
	pipelinCkptDocs := VBsCkptsDocMaps{ckptDoc, nil}
	//ckptDoc2 := mockVBCkptDoc(spec, 11)
	//pipelineCkptDocs2 := VBsCkptsDocMaps{ckptDoc2, ckptDoc2}

	var shaMap metadata.ShaToCollectionNamespaceMap
	brokenMapppingInternalId := "testInternalId"
	var manifestCache *metadata.ManifestsCache
	respCh := make(chan error)

	mergerCalledWg.Add(1)
	assert.Nil(ckptMgr.requestPeriodicMerge(pipelinCkptDocs, shaMap, brokenMapppingInternalId, manifestCache, manifestCache, respCh))
	mergerCalledWg.Wait()
	ckptMgr.periodicPushDedupMtx.RLock()
	assert.Nil(ckptMgr.periodicPushDedupArg)
	ckptMgr.periodicPushDedupMtx.RUnlock()

	// Try to queue one again while periodic merge is busy
	respCh2 := make(chan error)
	assert.Nil(ckptMgr.requestPeriodicMerge(pipelinCkptDocs, shaMap, brokenMapppingInternalId, manifestCache, manifestCache, respCh2))

	retErr := <-respCh
	assert.Nil(retErr)

	retErr = <-respCh2
	assert.Nil(retErr)

	close(ckptMgr.finish_ch)
}

func TestCkptMgrPeriodicMerger2(t *testing.T) {
	fmt.Println("============== Test case start: TestCkptMgrPeriodicMerger2 =================")
	defer fmt.Println("============== Test case end: TestCkptMgrPeriodicMerger2 =================")
	assert := assert.New(t)

	ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, getBackfillMgr, bucketTopologySvc, spec, pipelineSupervisor, _, targetKvVbMap, targetMCMap, targetMcDelayMap, targetMCStatsResult := setupCkptMgrBoilerPlate()

	activeVBs := make(map[string][]uint16)
	activeVBs[kvKey] = []uint16{0}
	targetRef, _ := metadata.NewRemoteClusterReference("", "C2", "", "", "",
		"", false, "", nil, nil, nil, nil)

	setupMock(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, bucketTopologySvc, spec, pipelineSupervisor, nil, nil, targetMCMap, targetMcDelayMap, targetMCStatsResult)

	ckptMgr, err := NewCheckpointManager(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc,
		throughSeqnoTrackerSvc, activeVBs, "", "", "", targetKvVbMap,
		targetRef, nil, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc,
		getBackfillMgr, bucketTopologySvc)

	assert.Nil(err)
	assert.NotNil(ckptMgr)
	ckptMgr.unitTest = true

	var simpleMergerWaiter sync.WaitGroup
	simpleMergerWaiter.Add(1)
	// Test simple merger
	simpleMerger := func() {
		defer simpleMergerWaiter.Done()
		for {
			select {
			case <-ckptMgr.periodicPushRequested:
				// Sleep here to simulate processing time while another is queued up
				args := ckptMgr.periodicBatchGetter()
				assert.NotNil(args)
				assert.Equal(2, len(args.PushRespChs))

				// This test will have VB 11 and VB 12 merged for both main and backfill pipeline
				assert.NotNil(args.PipelinesCkptDocs[common.MainPipeline])
				assert.NotNil(args.PipelinesCkptDocs[common.MainPipeline][11])
				assert.NotNil(args.PipelinesCkptDocs[common.MainPipeline][12])
				assert.NotNil(args.PipelinesCkptDocs[common.BackfillPipeline])
				assert.NotNil(args.PipelinesCkptDocs[common.BackfillPipeline][11])
				assert.Nil(args.PipelinesCkptDocs[common.BackfillPipeline][12])
				respToGcCh(args.PushRespChs, nil, ckptMgr.finish_ch)
			case <-ckptMgr.finish_ch:
				return
			}
		}

	}
	ckptMgr.periodicMerger = simpleMerger
	// simulate Start
	go ckptMgr.periodicMerger()

	// Lazy way of creating a ckptDoc with 2 VBs
	ckptDoc := mockVBCkptDoc(spec, 12)
	pipelinCkptDocs := VBsCkptsDocMaps{ckptDoc, nil}
	ckptDoc2 := mockVBCkptDoc(spec, 11)
	pipelineCkptDocs2 := VBsCkptsDocMaps{ckptDoc2, ckptDoc2}

	var shaMap metadata.ShaToCollectionNamespaceMap
	brokenMapppingInternalId := "testInternalId"
	var manifestCache *metadata.ManifestsCache

	respCh := make(chan error)
	assert.Nil(ckptMgr.requestPeriodicMerge(pipelinCkptDocs, shaMap, brokenMapppingInternalId, manifestCache, manifestCache, respCh))
	// Try to queue one again while periodic merge is busy
	respCh2 := make(chan error)
	assert.Nil(ckptMgr.requestPeriodicMerge(pipelineCkptDocs2, shaMap, brokenMapppingInternalId, manifestCache, manifestCache, respCh2))

	retErr := <-respCh
	assert.Nil(retErr)

	retErr = <-respCh2
	assert.Nil(retErr)

	close(ckptMgr.finish_ch)
	simpleMergerWaiter.Wait()
}

func TestCkptMgrPeriodicMergerCloseBeforeRespRead(t *testing.T) {
	fmt.Println("============== Test case start: TestCkptMgrPeriodicMergerCloseBeforeRespRead =================")
	defer fmt.Println("============== Test case end: TestCkptMgrPeriodicMergerCloseBeforeRespRead =================")
	assert := assert.New(t)

	ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, getBackfillMgr, bucketTopologySvc, spec, pipelineSupervisor, _, _, targetMCMap, targetMcDelayMap, targetMCStatsResult := setupCkptMgrBoilerPlate()

	activeVBs := make(map[string][]uint16)
	activeVBs[kvKey] = []uint16{0}
	targetRef, _ := metadata.NewRemoteClusterReference("", "C2", "", "", "",
		"", false, "", nil, nil, nil, nil)

	setupMock(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, bucketTopologySvc, spec, pipelineSupervisor, nil, nil, targetMCMap, targetMcDelayMap, targetMCStatsResult)

	ckptMgr, err := NewCheckpointManager(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc,
		throughSeqnoTrackerSvc, activeVBs, "", "", "", nil,
		targetRef, nil, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc,
		getBackfillMgr, bucketTopologySvc)

	assert.Nil(err)
	assert.NotNil(ckptMgr)
	ckptMgr.unitTest = true

	simpleMergerStartedCh := make(chan bool)

	// Test simple merger with delay before responding to response channels
	var simpleMergerWaiter sync.WaitGroup
	simpleMergerWaiter.Add(1)
	simpleMerger := func() {
		defer simpleMergerWaiter.Done()
		close(simpleMergerStartedCh)
		for {
			select {
			case <-ckptMgr.periodicPushRequested:
				// Sleep here to simulate processing time while another is queued up
				args := ckptMgr.periodicBatchGetter()
				if args == nil {
					return
				}

				// Insert delay
				time.Sleep(500 * time.Millisecond)
				respToGcCh(args.PushRespChs, nil, ckptMgr.finish_ch)
			case <-ckptMgr.finish_ch:
				return
			}
		}

	}
	ckptMgr.periodicMerger = simpleMerger
	// simulate Start
	go ckptMgr.periodicMerger()
	<-simpleMergerStartedCh

	// Lazy way of creating a ckptDoc with 2 VBs
	ckptDoc := mockVBCkptDoc(spec, 12)
	pipelinCkptDocs := VBsCkptsDocMaps{ckptDoc, nil}
	ckptDoc2 := mockVBCkptDoc(spec, 11)
	pipelineCkptDocs2 := VBsCkptsDocMaps{ckptDoc2, ckptDoc2}

	var shaMap metadata.ShaToCollectionNamespaceMap
	brokenMapppingInternalId := "testInternalId"
	var manifestCache *metadata.ManifestsCache

	respCh := make(chan error)
	assert.Nil(ckptMgr.requestPeriodicMerge(pipelinCkptDocs, shaMap, brokenMapppingInternalId, manifestCache, manifestCache, respCh))
	// Try to queue one again while periodic merge is busy
	respCh2 := make(chan error)
	assert.Nil(ckptMgr.requestPeriodicMerge(pipelineCkptDocs2, shaMap, brokenMapppingInternalId, manifestCache, manifestCache, respCh2))

	// Don't read respCh and just exit
	close(ckptMgr.finish_ch)

	// respToGoCh() should not hang
	simpleMergerWaiter.Wait()
}

func TestCkptMgrPerformCkpt(t *testing.T) {
	fmt.Println("============== Test case start: TestCkptMgrPerformCkpt =================")
	defer fmt.Println("============== Test case end: TestCkptMgrPerformCkpt =================")
	assert := assert.New(t)

	ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, getBackfillMgr, bucketTopologySvc, spec, pipelineSupervisor, ckptDoneCounter, targetKvVbMap, targetMCMap, targetMCDelayMap, targetMCResult := setupCkptMgrBoilerPlate()

	activeVBs := make(map[string][]uint16)
	activeVBs[kvKey] = []uint16{0}
	targetRef, _ := metadata.NewRemoteClusterReference("", "C2", "", "", "",
		"", false, "", nil, nil, nil, nil)
	throughSeqnoMap := make(map[uint16]uint64)
	var upsertCkptDoneErr error

	setupMock(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, bucketTopologySvc, spec, pipelineSupervisor, throughSeqnoMap, upsertCkptDoneErr, targetMCMap, targetMCDelayMap, targetMCResult)

	ckptMgr, err := NewCheckpointManager(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc,
		throughSeqnoTrackerSvc, activeVBs, "", "", "", targetKvVbMap,
		targetRef, nil, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc,
		getBackfillMgr, bucketTopologySvc)

	assert.Nil(ckptMgr.RegisterComponentEventListener(common.CheckpointDone, ckptDoneCounter))

	assert.Nil(err)
	assert.NotNil(ckptMgr)
	ckptMgr.unitTest = true
	ckptMgr.checkpointAllowedHelper.setCheckpointAllowed()
	ckptMgr.InitConnections()

	mainPipeline := setupMainPipelineMock(spec, pipelineSupervisor)
	assert.Nil(ckptMgr.Attach(mainPipeline))

	dummyFinCh := make(chan bool, 1)
	dummyWG := sync.WaitGroup{}
	dummyWG.Add(1)
	go ckptMgr.performCkpt(dummyFinCh, &dummyWG)
	dummyWG.Wait()

	assert.Equal(uint64(1), atomic.LoadUint64(&ckptDoneCounter.ckptDoneCount))
}

func TestCkptMgrPerformCkptWithDelay(t *testing.T) {
	fmt.Println("============== Test case start: TestCkptMgrPerformCkptWithDelay =================")
	defer fmt.Println("============== Test case end: TestCkptMgrPerformCkptWithDelay =================")
	assert := assert.New(t)

	ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, getBackfillMgr, bucketTopologySvc, spec, pipelineSupervisor, ckptDoneCounter, targetKvVbMap, targetMCMap, targetMCDelayMap, targetMCResult := setupCkptMgrBoilerPlate()

	activeVBs := make(map[string][]uint16)
	activeVBs[kvKey] = []uint16{0}
	targetRef, _ := metadata.NewRemoteClusterReference("", "C2", "", "", "",
		"", false, "", nil, nil, nil, nil)
	throughSeqnoMap := make(map[uint16]uint64)
	var upsertCkptDoneErr error

	targetMCDelayMap[kvKey] = 2

	setupMock(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, bucketTopologySvc, spec, pipelineSupervisor, throughSeqnoMap, upsertCkptDoneErr, targetMCMap, targetMCDelayMap, targetMCResult)

	ckptMgr, err := NewCheckpointManager(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc,
		throughSeqnoTrackerSvc, activeVBs, "", "", "", targetKvVbMap,
		targetRef, nil, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc,
		getBackfillMgr, bucketTopologySvc)

	assert.Nil(ckptMgr.RegisterComponentEventListener(common.CheckpointDone, ckptDoneCounter))

	assert.Nil(err)
	assert.NotNil(ckptMgr)
	ckptMgr.unitTest = true
	ckptMgr.checkpointAllowedHelper.setCheckpointAllowed()
	ckptMgr.InitConnections()

	mainPipeline := setupMainPipelineMock(spec, pipelineSupervisor)
	assert.Nil(ckptMgr.Attach(mainPipeline))

	// This test will launch 2 periodic ckpt go-routines but one should fail because it is still ongoing
	dummyFinCh := make(chan bool, 1)
	dummyWG := sync.WaitGroup{}
	dummyWG.Add(1)
	go ckptMgr.performCkpt(dummyFinCh, &dummyWG)

	time.Sleep(500 * time.Millisecond)

	dummyWG.Add(1)
	go ckptMgr.performCkpt(dummyFinCh, &dummyWG)
	dummyWG.Wait()

	assert.Equal(uint64(1), atomic.LoadUint64(&ckptDoneCounter.ckptDoneCount))
}

func TestCkptMgrPerformCkptWithDelayAndOneTime(t *testing.T) {
	fmt.Println("============== Test case start: TestCkptMgrPerformCkptWithDelayAndOneTime =================")
	defer fmt.Println("============== Test case end: TestCkptMgrPerformCkptWithDelayAndOneTime =================")
	assert := assert.New(t)

	ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, getBackfillMgr, bucketTopologySvc, spec, pipelineSupervisor, ckptDoneCounter, targetKvVbMap, targetMCMap, targetMCDelayMap, targetMCResult := setupCkptMgrBoilerPlate()

	activeVBs := make(map[string][]uint16)
	activeVBs[kvKey] = []uint16{0}
	targetRef, _ := metadata.NewRemoteClusterReference("", "C2", "", "", "",
		"", false, "", nil, nil, nil, nil)
	throughSeqnoMap := make(map[uint16]uint64)
	var upsertCkptDoneErr error

	targetMCDelayMap[kvKey] = 2

	setupMock(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, bucketTopologySvc, spec, pipelineSupervisor, throughSeqnoMap, upsertCkptDoneErr, targetMCMap, targetMCDelayMap, targetMCResult)

	ckptMgr, err := NewCheckpointManager(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc,
		throughSeqnoTrackerSvc, activeVBs, "", "", "", targetKvVbMap,
		targetRef, nil, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc,
		getBackfillMgr, bucketTopologySvc)

	assert.Nil(ckptMgr.RegisterComponentEventListener(common.CheckpointDone, ckptDoneCounter))

	assert.Nil(err)
	assert.NotNil(ckptMgr)
	ckptMgr.unitTest = true
	ckptMgr.checkpointAllowedHelper.setCheckpointAllowed()
	ckptMgr.InitConnections()

	mainPipeline := setupMainPipelineMock(spec, pipelineSupervisor)
	assert.Nil(ckptMgr.Attach(mainPipeline))

	// This test will launch 2 periodic ckpt go-routines but one should fail because it is still ongoing
	dummyFinCh := make(chan bool, 1)
	dummyWG := sync.WaitGroup{}
	dummyWG.Add(1)
	go ckptMgr.performCkpt(dummyFinCh, &dummyWG)

	time.Sleep(500 * time.Millisecond)

	ckptMgr.PerformCkpt(dummyFinCh)
	dummyWG.Wait()

	assert.Equal(uint64(2), atomic.LoadUint64(&ckptDoneCounter.ckptDoneCount))
}

func TestCkptMgrStopBeforeStart(t *testing.T) {
	fmt.Println("============== Test case start: TestCkptMgrStopBeforeStart =================")
	defer fmt.Println("============== Test case end: TestCkptMgrStopBeforeStart =================")
	assert := assert.New(t)

	ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, getBackfillMgr, bucketTopologySvc, spec, pipelineSupervisor, _, targetKvVbMap, targetMCMap, targetMCDelayMap, targetMCResult := setupCkptMgrBoilerPlate()

	activeVBs := make(map[string][]uint16)
	activeVBs[kvKey] = []uint16{0}
	targetRef, _ := metadata.NewRemoteClusterReference("", "C2", "", "", "",
		"", false, "", nil, nil, nil, nil)
	throughSeqnoMap := make(map[uint16]uint64)
	var upsertCkptDoneErr error

	targetMCDelayMap[kvKey] = 2

	setupMock(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, bucketTopologySvc, spec, pipelineSupervisor, throughSeqnoMap, upsertCkptDoneErr, targetMCMap, targetMCDelayMap, targetMCResult)

	ckptMgr, err := NewCheckpointManager(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc,
		throughSeqnoTrackerSvc, activeVBs, "", "", "", targetKvVbMap,
		targetRef, nil, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc,
		getBackfillMgr, bucketTopologySvc)

	assert.Nil(err)
	assert.Nil(ckptMgr.Stop())
}
