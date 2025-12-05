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
	"io/ioutil"
	"math"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mcc "github.com/couchbase/gomemcached/client"
	mccMock "github.com/couchbase/gomemcached/client/mocks"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/common"
	commonMock "github.com/couchbase/goxdcr/v8/common/mocks"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	mocks2 "github.com/couchbase/goxdcr/v8/metadata/mocks"
	"github.com/couchbase/goxdcr/v8/parts"
	service_def_real "github.com/couchbase/goxdcr/v8/service_def"
	service_def "github.com/couchbase/goxdcr/v8/service_def/mocks"
	"github.com/couchbase/goxdcr/v8/utils"
	utilities "github.com/couchbase/goxdcr/v8/utils/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
		PipelineReinitHash: "goodHash",
		SourceVBTimestamp: metadata.SourceVBTimestamp{
			Failover_uuid: goodVbUuid,
			Seqno:         goodSeqno,
		},
	}
	badRecord := &metadata.CheckpointRecord{
		PipelineReinitHash: "goodHash",
		SourceVBTimestamp: metadata.SourceVBTimestamp{
			Failover_uuid: badVbUuid,
			Seqno:         goodSeqno,
		},
	}
	goodRecordWithBadHash := &metadata.CheckpointRecord{
		PipelineReinitHash: "badHash",
		SourceVBTimestamp: metadata.SourceVBTimestamp{
			Failover_uuid: goodVbUuid,
			Seqno:         goodSeqno,
		},
	}

	goodDoc := &metadata.CheckpointsDoc{
		Checkpoint_records: []*metadata.CheckpointRecord{goodRecord},
	}

	badDoc := &metadata.CheckpointsDoc{
		Checkpoint_records: []*metadata.CheckpointRecord{badRecord, goodRecordWithBadHash},
	}

	mixedDoc := &metadata.CheckpointsDoc{
		Checkpoint_records: []*metadata.CheckpointRecord{goodRecord, badRecord, goodRecordWithBadHash},
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
	assert.Len(result[1].Checkpoint_records, 2)
	assert.Len(result[2].Checkpoint_records, 1)

	logger := log.NewLogger("", nil)
	hashFilteredResults := filterCkptsBasedOnPipelineReinitHash(results, "goodHash", logger)
	hashFilteredResult := hashFilteredResults[0]
	assert.Len(hashFilteredResult, len(result))

	assert.Len(hashFilteredResult[0].Checkpoint_records, 1)
	assert.Len(hashFilteredResult[1].Checkpoint_records, 1)
	assert.Len(hashFilteredResult[2].Checkpoint_records, 0)
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
	filteredMapTgts := filterInvalidCkptsBasedOnTargetFailover([]metadata.VBsCkptsDocMap{filteredMap, nil}, tgtFailoverLogs, &log.CommonLogger{})
	filteredMapTgt := filteredMapTgts[0]
	for _, ckptDoc := range filteredMapTgt {
		assert.NotEqual(0, len(ckptDoc.Checkpoint_records))
	}
}

func TestCheckpointsValidityWithFailoverLogs(t *testing.T) {
	fmt.Println("============== Test case start: TestCheckpointsValidityWithFailoverLogs =================")
	defer fmt.Println("============== Test case end: TestCheckpointsValidityWithFailoverLogs =================")
	assert := assert.New(t)

	tgtFailoverLogsSlice, err := ioutil.ReadFile("./unitTestdata/tgtFailoverJson.json")
	assert.Nil(err)
	tgtFailoverLogs := make(map[uint16]*mcc.FailoverLog)
	assert.Nil(json.Unmarshal(tgtFailoverLogsSlice, &tgtFailoverLogs))

	globalCkts := metadata.GenerateGlobalVBsCkptDocMap([]uint16{0, 1, 2}, "")
	filteredMapTgts := filterInvalidCkptsBasedOnTargetFailover([]metadata.VBsCkptsDocMap{globalCkts, nil}, tgtFailoverLogs, &log.CommonLogger{})
	filteredMapTgt := filteredMapTgts[0]
	for _, ckptDoc := range filteredMapTgt {
		assert.NotEqual(0, len(ckptDoc.Checkpoint_records))
	}
}

func TestCheckpointsValidityWithPreReplicate(t *testing.T) {
	fmt.Println("============== Test case start: TestCheckpointsValidityWithPreReplicate =================")
	defer fmt.Println("============== Test case end: TestCheckpointsValidityWithPreReplicate =================")
	assert := assert.New(t)

	ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, getBackfillMgr, bucketTopologySvc, spec, pipelineSupervisor, _, targetKvVbMap, targetMCMap, targetMCDelayMap, targetMCResult := setupCkptMgrBoilerPlate()
	activeVBs := make(map[string][]uint16)
	activeVBs[kvKey] = []uint16{0}
	targetRef, _ := metadata.NewRemoteClusterReference("", "C2", "", "", "",
		"", false, "", nil, nil, nil, nil)
	throughSeqnoMap := make(map[uint16]uint64)
	var upsertCkptDoneErr error

	targetMCDelayMap[kvKey] = 2

	ckptMgr, err := NewCheckpointManager(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc,
		throughSeqnoTrackerSvc, activeVBs, "", "", "", targetKvVbMap,
		targetRef, nil, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc,
		getBackfillMgr, bucketTopologySvc, true)

	assert.Nil(err)
	setupMock(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, bucketTopologySvc, spec, pipelineSupervisor, throughSeqnoMap, upsertCkptDoneErr, targetMCMap, targetMCDelayMap, targetMCResult, nil, nil, nil, nil)

	// Replace with a customized one
	capiSvcErr := &service_def.CAPIService{}
	var preReplicateCounter uint32
	// capiSvc that returns error
	capiSvcErr.On("PreReplicate", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		atomic.AddUint32(&preReplicateCounter, 1)
	}).Return(false, nil, fmt.Errorf("dummy"))
	ckptMgr.capi_svc = capiSvcErr

	globalCkts := metadata.GenerateGlobalVBsCkptDocMap([]uint16{0, 1, 2}, "")
	filteredMapTgts := ckptMgr.filterInvalidCkptsBasedOnPreReplicate([]metadata.VBsCkptsDocMap{globalCkts, nil})
	filteredMapTgt := filteredMapTgts[0]
	for _, ckptDoc := range filteredMapTgt {
		assert.Equal(0, len(ckptDoc.Checkpoint_records))
	}

	// Replace with a customized one that returns positive results, like positive match
	capiSvcGood := &service_def.CAPIService{}
	capiSvcGood.On("PreReplicate", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		atomic.AddUint32(&preReplicateCounter, 1)
	}).Return(true, nil, nil)
	ckptMgr.capi_svc = capiSvcGood

	filteredMapTgts = ckptMgr.filterInvalidCkptsBasedOnPreReplicate([]metadata.VBsCkptsDocMap{globalCkts, nil})
	filteredMapTgt = filteredMapTgts[0]
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
	_, err := helper.registerCkptOp(false, false)
	assert.NotNil(err)

	helper.setCheckpointAllowed()
	idx, err := helper.registerCkptOp(false, false)
	idx2, err2 := helper.registerCkptOp(false, false)
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

	mockPipeline := &commonMock.Pipeline{}
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

func (*ckptDoneListener) ListenerPipelineType() common.ListenerPipelineType {
	return common.ListenerNotShared
}

type statsMapResult struct {
	statsMap map[string]string
	err      error
}

func setupCkptMgrBoilerPlate() (*service_def.CheckpointsService, *service_def.CAPIService, *service_def.RemoteClusterSvc, *service_def.ReplicationSpecSvc, *service_def.XDCRCompTopologySvc, *service_def.ThroughSeqnoTrackerSvc, *utilities.UtilsIface, *service_def.StatsMgrIface, *service_def.UILogSvc, *service_def.CollectionsManifestSvc, *service_def.BackfillReplSvc, *service_def.BackfillMgrIface, func() service_def_real.BackfillMgrIface, *service_def.BucketTopologySvc, *metadata.ReplicationSpecification, *commonMock.PipelineSupervisorSvc, *ckptDoneListener, map[string][]uint16, map[string]*mccMock.ClientIface, map[string]int, map[string]statsMapResult) {
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

	pipelineSupervisor := &commonMock.PipelineSupervisorSvc{}

	ckptDoneListener := &ckptDoneListener{}

	targetKvVbMap := make(map[string][]uint16)
	targetKvVbMap[kvKey] = vbList
	targetMCMap := make(map[string]*mccMock.ClientIface)
	targetMCMap[kvKey] = &mccMock.ClientIface{}

	targetMCDelayMap := make(map[string]int)
	targetMCDelayMap[kvKey] = 0

	targetMCStatMapReturn := make(map[string]statsMapResult)

	return ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc,
		utilsMock, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, getBackfillMgr,
		bucketTopologySvc, spec, pipelineSupervisor, ckptDoneListener, targetKvVbMap, targetMCMap, targetMCDelayMap,
		targetMCStatMapReturn
}

func setupBackfillPipelineMock(spec *metadata.ReplicationSpecification, supervisor *commonMock.PipelineSupervisorSvc, task *metadata.BackfillTask) *commonMock.Pipeline {
	runtimeCtx := &commonMock.PipelineRuntimeContext{}
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

	pipeline := &commonMock.Pipeline{}
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

func setupMainPipelineMock(spec *metadata.ReplicationSpecification, supervisor *commonMock.PipelineSupervisorSvc) *commonMock.Pipeline {
	runtimeCtx := &commonMock.PipelineRuntimeContext{}
	runtimeCtx.On("Service", base.PIPELINE_SUPERVISOR_SVC).Return(supervisor)

	genericSpec := &mocks2.GenericSpecification{}
	genericSpec.On("GetReplicationSpec").Return(spec)

	pipeline := &commonMock.Pipeline{}
	pipeline.On("Type").Return(common.MainPipeline)
	pipeline.On("Sources").Return(nil)
	pipeline.On("Specification").Return(genericSpec)
	pipeline.On("Topic").Return("pipelineTopic")
	pipeline.On("FullTopic").Return("pipelineTopic")
	pipeline.On("RuntimeContext").Return(runtimeCtx)
	pipeline.On("InstanceId").Return("randomInstance")

	pipeline.On("UpdateSettings", mock.Anything).Return(nil)
	pipeline.On("SetBrokenMap", mock.Anything).Return(nil)

	pipeline.On("SetBrokenMap", mock.Anything).Return(nil)

	return pipeline
}

func getBackfillTask() *metadata.BackfillTask {
	namespaceMapping := make(metadata.CollectionNamespaceMapping)
	defaultNamespace := &base.CollectionNamespace{ScopeName: base.DefaultScopeCollectionName, CollectionName: base.DefaultScopeCollectionName}
	namespaceMapping.AddSingleMapping(defaultNamespace, defaultNamespace)
	manifestsIdPair := base.CollectionsManifestIdsTimestamp{0, 0, nil}

	ts0 := &metadata.BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{0, 0, 5, 5, 5, manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{0, 0, 500, 500, 500, manifestsIdPair},
	}

	vb0Task0 := metadata.NewBackfillTask(ts0, []metadata.CollectionNamespaceMapping{namespaceMapping})
	vb0Tasks := metadata.NewBackfillTasks()
	vb0Tasks.List = append(vb0Tasks.List, vb0Task0)

	return vb0Task0
}
func setupMock(ckptSvc *service_def.CheckpointsService, capiSvc *service_def.CAPIService, remClusterSvc *service_def.RemoteClusterSvc, replSpecSvc *service_def.ReplicationSpecSvc, xdcrCompTopologySvc *service_def.XDCRCompTopologySvc, throughSeqnoSvc *service_def.ThroughSeqnoTrackerSvc, utilsMock *utilities.UtilsIface, statsMgr *service_def.StatsMgrIface, uilogSvc *service_def.UILogSvc, colManSvc *service_def.CollectionsManifestSvc, backfillReplSvc *service_def.BackfillReplSvc, backfillMgr *service_def.BackfillMgrIface, bucketTopologySvc *service_def.BucketTopologySvc, spec *metadata.ReplicationSpecification, supervisor *commonMock.PipelineSupervisorSvc, throughSeqnoMap map[uint16]uint64, upsertCkptsDoneErr error, mcMap map[string]*mccMock.ClientIface, delayMap map[string]int, result map[string]statsMapResult, srcManifestIds, tgtManifestIds map[uint16]uint64, preReplicateMap map[*service_def_real.RemoteVBReplicationStatus]*metadata.TargetVBUuid, ckptHighSeqnoMap base.HighSeqnoAndVbUuidMap) {

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
	utilsMock.On("ParseHighSeqnoAndVBUuidFromStats", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		arg := args.Get(2).(map[uint16][]uint64)
		for vb, highSeqnoPair := range ckptHighSeqnoMap {
			arg[vb] = highSeqnoPair
		}
	}).Return(nil, nil)
	utilsMock.On("StartDiagStopwatch", mock.Anything, mock.Anything).Return(func() time.Duration {
		return 0
	})

	utilsMock.On("ExponentialBackoffExecutorWithFinishSignal", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		utilsReal.ExponentialBackoffExecutorWithFinishSignal(args.Get(0).(string), args.Get(1).(time.Duration), args.Get(2).(int), args.Get(3).(int), args.Get(4).(utils.ExponentialOpFunc2), args.Get(5), args.Get(6).(chan bool))
	}).Return(nil, nil)

	for kvName, client := range mcMap {
		utilsMock.On("GetRemoteMemcachedConnection", kvName, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(client, nil)
	}

	for input, output := range preReplicateMap {
		capiSvc.On("PreReplicate", mock.Anything, input, mock.Anything).Return(true, output, nil)
	}

	statsMgr.On("SetVBCountMetrics", mock.Anything, mock.Anything).Return(nil)
	statsMgr.On("HandleLatestThroughSeqnos", mock.Anything).Return(nil)

	throughSeqnoSvc.On("SetStartSeqno", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		timeStampMtx.Lock()
		timeStampSetSeqno = args.Get(1).(uint64)
		timeStampSetCnt++
		timeStampMtx.Unlock()
	}).Return(nil)

	throughSeqnoSvc.On("GetThroughSeqnos").Return(throughSeqnoMap, nil)
	throughSeqnoSvc.On("GetThroughSeqnosAndManifestIds").Return(throughSeqnoMap, srcManifestIds, tgtManifestIds)

	ckptSvc.On("UpsertCheckpointsDone", mock.Anything, mock.Anything).Return(upsertCkptsDoneErr)
	ckptSvc.On("PreUpsertBrokenMapping", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	ckptSvc.On("UpsertBrokenMapping", mock.Anything, mock.Anything).Return(nil)
	ckptSvc.On("UpsertGlobalInfo", mock.Anything, mock.Anything).Return(nil)
	// Generally speaking, return empty checkpoints
	// We want to simulate checkpoint service where each call will return a cloned object
	// If we don't have multiple lines here, it'd return the same object every time and trigger golang concurrent r/w map panic
	// See: https://stackoverflow.com/questions/46374174/how-to-mock-for-same-input-and-different-return-values-in-a-for-loop-in-golang
	// If needed, just copy and paste more instances below
	ckptSvc.On("CheckpointsDocs", mock.Anything, mock.Anything).Return(map[uint16]*metadata.CheckpointsDoc{}, nil).Once()
	ckptSvc.On("CheckpointsDocs", mock.Anything, mock.Anything).Return(map[uint16]*metadata.CheckpointsDoc{}, nil).Once()

	for k, client := range mcMap {
		client.On("StatsMap", base.VBUCKET_SEQNO_STAT_NAME).Run(func(args mock.Arguments) { time.Sleep(time.Duration(delayMap[k]) * time.Second) }).Return(result[k].statsMap, result[k].err)
	}

	colManSvc.On("PersistNeededManifests", mock.Anything).Return(nil)
	colManSvc.On("PersistReceivedManifests", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	supervisor.On("OnEvent", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		atomic.AddUint32(&supervisorErrCnt, 1)
	}).Return(nil)
}

var timeStampSetSeqno uint64
var timeStampSetCnt int
var timeStampMtx sync.RWMutex

var supervisorErrCnt uint32

func TestBackfillVBTaskResume(t *testing.T) {
	fmt.Println("============== Test case start: TestBackfillVBTaskResume =================")
	defer fmt.Println("============== Test case end: TestBackfillVBTaskResume =================")
	assert := assert.New(t)

	ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, getBackfillMgr, bucketTopologySvc, spec, pipelineSupervisor, _, targetKvVbMap, targetMCMap, targetMcDelayMap, targetMCStatsResult := setupCkptMgrBoilerPlate()

	// Let's mock some ckpt docs for vb0
	ckptDocs := mockVBCkptDoc(spec, 0)

	preReplicateTgtMap := map[*service_def_real.RemoteVBReplicationStatus]*metadata.TargetVBUuid{}
	for vb, ckpt := range ckptDocs {
		for i, _ := range ckpt.Checkpoint_records {
			preReplicateTgtMap[&service_def_real.RemoteVBReplicationStatus{VBNo: vb, VBSeqno: ckpt.Checkpoint_records[i].Seqno}] = &metadata.TargetVBUuid{}
		}
	}

	activeVBs := make(map[string][]uint16)
	activeVBs[kvKey] = []uint16{0}
	targetRef, _ := metadata.NewRemoteClusterReference("", "C2", "", "", "",
		"", false, "", nil, nil, nil, nil)

	setupMock(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, bucketTopologySvc, spec, pipelineSupervisor, nil, nil, targetMCMap, targetMcDelayMap, targetMCStatsResult, nil, nil, preReplicateTgtMap, nil)

	ckptMgr, err := NewCheckpointManager(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc,
		throughSeqnoTrackerSvc, activeVBs, "", "", "", targetKvVbMap,
		targetRef, nil, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc,
		getBackfillMgr, bucketTopologySvc, false)

	assert.Nil(err)
	assert.NotNil(ckptMgr)
	ckptMgr.unitTest = true

	backfillTask := getBackfillTask()
	backfillPipeline := setupBackfillPipelineMock(spec, pipelineSupervisor, backfillTask)
	assert.Nil(ckptMgr.Attach(backfillPipeline))

	assert.Len(ckptMgr.backfillStartingTs, 1)

	var waitGrp sync.WaitGroup
	waitGrp.Add(1)
	errCh := make(chan interface{}, 1)
	var lowestManifestId uint64
	var vbtsStartedNon0 uint32
	ckptMgr.startSeqnoGetter(0, []uint16{0}, ckptDocs, &waitGrp, errCh, &lowestManifestId, &vbtsStartedNon0, nil)
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
		SourceVBTimestamp: metadata.SourceVBTimestamp{
			Seqno:                  1000,
			Dcp_snapshot_seqno:     1000,
			Dcp_snapshot_end_seqno: 1000,
		},
		TargetVBTimestamp: metadata.TargetVBTimestamp{
			Target_Seqno: 1000,
		},
	}
	record2 := &metadata.CheckpointRecord{
		SourceVBTimestamp: metadata.SourceVBTimestamp{
			Seqno:                  500,
			Dcp_snapshot_seqno:     500,
			Dcp_snapshot_end_seqno: 500,
		},
		TargetVBTimestamp: metadata.TargetVBTimestamp{
			Target_Seqno: 500,
		},
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

	setupMock(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, bucketTopologySvc, spec, pipelineSupervisor, nil, nil, targetMCMap, targetMcDelayMap, targetMCStatsResult, nil, nil, nil, nil)

	ckptMgr, err := NewCheckpointManager(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc,
		throughSeqnoTrackerSvc, activeVBs, "", "", "", targetKvVbMap,
		targetRef, nil, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc,
		getBackfillMgr, bucketTopologySvc, false)

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
	var globalTsMap *metadata.GlobalInfoCompressedDoc
	respCh := make(chan error)

	mergerCalledWg.Add(1)
	assert.Nil(ckptMgr.requestPeriodicMerge(pipelinCkptDocs, shaMap, brokenMapppingInternalId, manifestCache, manifestCache, respCh, globalTsMap))
	mergerCalledWg.Wait()
	ckptMgr.periodicPushDedupMtx.RLock()
	assert.Nil(ckptMgr.periodicPushDedupArg)
	ckptMgr.periodicPushDedupMtx.RUnlock()

	// Try to queue one again while periodic merge is busy
	respCh2 := make(chan error)
	assert.Nil(ckptMgr.requestPeriodicMerge(pipelinCkptDocs, shaMap, brokenMapppingInternalId, manifestCache, manifestCache, respCh2, globalTsMap))

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

	setupMock(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, bucketTopologySvc, spec, pipelineSupervisor, nil, nil, targetMCMap, targetMcDelayMap, targetMCStatsResult, nil, nil, nil, nil)

	ckptMgr, err := NewCheckpointManager(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc,
		throughSeqnoTrackerSvc, activeVBs, "", "", "", targetKvVbMap,
		targetRef, nil, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc,
		getBackfillMgr, bucketTopologySvc, false)

	assert.Nil(err)
	assert.NotNil(ckptMgr)
	ckptMgr.unitTest = true

	var simpleMergerWaiter sync.WaitGroup
	var simplemergerStarted bool
	var simplemergerStartedMtx sync.Mutex
	var simplemergerStartedCond = sync.NewCond(&simplemergerStartedMtx)

	simpleMergerWaiter.Add(1)
	// Test simple merger
	simpleMerger := func() {
		simplemergerStartedMtx.Lock()
		simplemergerStarted = true
		simplemergerStartedMtx.Unlock()
		simplemergerStartedCond.Broadcast()
		defer simpleMergerWaiter.Done()
		for {
			select {
			case <-ckptMgr.periodicPushRequested:
				// Sleep here to simulate processing time while another is queued up
				args := ckptMgr.periodicBatchGetter()
				if args == nil {
					continue
				}
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

	simplemergerStartedMtx.Lock()
	for {
		if simplemergerStarted {
			simplemergerStartedMtx.Unlock()
			break
		} else {
			simplemergerStartedCond.Wait()
		}
	}

	// Lazy way of creating a ckptDoc with 2 VBs
	ckptDoc := mockVBCkptDoc(spec, 12)
	pipelinCkptDocs := VBsCkptsDocMaps{ckptDoc, nil}
	ckptDoc2 := mockVBCkptDoc(spec, 11)
	pipelineCkptDocs2 := VBsCkptsDocMaps{ckptDoc2, ckptDoc2}

	var shaMap metadata.ShaToCollectionNamespaceMap
	brokenMapppingInternalId := "testInternalId"
	var manifestCache *metadata.ManifestsCache
	var globalTsMap *metadata.GlobalInfoCompressedDoc

	respCh := make(chan error)
	assert.Nil(ckptMgr.requestPeriodicMerge(pipelinCkptDocs, shaMap, brokenMapppingInternalId, manifestCache, manifestCache, respCh, globalTsMap))
	// Try to queue one again while periodic merge is busy
	respCh2 := make(chan error)
	assert.Nil(ckptMgr.requestPeriodicMerge(pipelineCkptDocs2, shaMap, brokenMapppingInternalId, manifestCache, manifestCache, respCh2, globalTsMap))

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

	setupMock(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, bucketTopologySvc, spec, pipelineSupervisor, nil, nil, targetMCMap, targetMcDelayMap, targetMCStatsResult, nil, nil, nil, nil)

	ckptMgr, err := NewCheckpointManager(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc,
		throughSeqnoTrackerSvc, activeVBs, "", "", "", nil,
		targetRef, nil, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc,
		getBackfillMgr, bucketTopologySvc, false)

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
	var globalTsMap *metadata.GlobalInfoCompressedDoc

	respCh := make(chan error)
	assert.Nil(ckptMgr.requestPeriodicMerge(pipelinCkptDocs, shaMap, brokenMapppingInternalId, manifestCache, manifestCache, respCh, globalTsMap))
	// Try to queue one again while periodic merge is busy
	respCh2 := make(chan error)
	assert.Nil(ckptMgr.requestPeriodicMerge(pipelineCkptDocs2, shaMap, brokenMapppingInternalId, manifestCache, manifestCache, respCh2, globalTsMap))

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

	setupMock(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, bucketTopologySvc, spec, pipelineSupervisor, throughSeqnoMap, upsertCkptDoneErr, targetMCMap, targetMCDelayMap, targetMCResult, nil, nil, nil, nil)

	ckptMgr, err := NewCheckpointManager(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc,
		throughSeqnoTrackerSvc, activeVBs, "", "", "", targetKvVbMap,
		targetRef, nil, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc,
		getBackfillMgr, bucketTopologySvc, false)

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

	emptyPreReplicate := map[*service_def_real.RemoteVBReplicationStatus]*metadata.TargetVBUuid{
		&service_def_real.RemoteVBReplicationStatus{}: &metadata.TargetVBUuid{},
	}

	setupMock(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, bucketTopologySvc, spec, pipelineSupervisor, throughSeqnoMap, upsertCkptDoneErr, targetMCMap, targetMCDelayMap, targetMCResult, nil, nil, emptyPreReplicate, nil)

	ckptMgr, err := NewCheckpointManager(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc,
		throughSeqnoTrackerSvc, activeVBs, "", "", "", targetKvVbMap,
		targetRef, nil, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc,
		getBackfillMgr, bucketTopologySvc, false)

	assert.Nil(ckptMgr.RegisterComponentEventListener(common.CheckpointDone, ckptDoneCounter))

	assert.Nil(err)
	assert.NotNil(ckptMgr)
	ckptMgr.unitTest = true
	ckptMgr.checkpointAllowedHelper.setCheckpointAllowed()
	ckptMgr.InitConnections()

	mainPipeline := setupMainPipelineMock(spec, pipelineSupervisor)
	assert.Nil(ckptMgr.Attach(mainPipeline))

	// Initially, ckptDocGetter will return no docs
	ckptSvc.On("CheckpointsDocs", mainPipeline.Topic(), mock.Anything).Return(map[uint16]*metadata.CheckpointsDoc{}, nil)
	ckptMgr.checkpoints_svc = ckptSvc

	// Let's pretend ckptmgr started vbtimestamp, read no ckpt doc, and should have initialized internal ckptDoc structs
	assert.Nil(ckptMgr.SetVBTimestamps(mainPipeline.Topic()))

	for _, oneRecord := range ckptMgr.cur_ckpts {
		assert.NotNil(oneRecord.ckpt.TargetVBTimestamp)
		assert.NotNil(oneRecord.ckpt.TargetPerVBCounters)
	}

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

	emptyPreReplicate := map[*service_def_real.RemoteVBReplicationStatus]*metadata.TargetVBUuid{
		&service_def_real.RemoteVBReplicationStatus{}: &metadata.TargetVBUuid{},
	}
	setupMock(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, bucketTopologySvc, spec, pipelineSupervisor, throughSeqnoMap, upsertCkptDoneErr, targetMCMap, targetMCDelayMap, targetMCResult, nil, nil, emptyPreReplicate, nil)

	ckptMgr, err := NewCheckpointManager(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc,
		throughSeqnoTrackerSvc, activeVBs, "", "", "", targetKvVbMap,
		targetRef, nil, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc,
		getBackfillMgr, bucketTopologySvc, false)

	assert.Nil(ckptMgr.RegisterComponentEventListener(common.CheckpointDone, ckptDoneCounter))

	assert.Nil(err)
	assert.NotNil(ckptMgr)
	ckptMgr.unitTest = true
	ckptMgr.checkpointAllowedHelper.setCheckpointAllowed()
	ckptMgr.InitConnections()

	mainPipeline := setupMainPipelineMock(spec, pipelineSupervisor)
	assert.Nil(ckptMgr.Attach(mainPipeline))

	// Initially, ckptDocGetter will return no docs
	ckptSvc.On("CheckpointsDocs", mainPipeline.Topic(), mock.Anything).Return(map[uint16]*metadata.CheckpointsDoc{}, nil)
	ckptMgr.checkpoints_svc = ckptSvc

	// Let's pretend ckptmgr started vbtimestamp, read no ckpt doc, and should have initialized internal ckptDoc structs
	assert.Nil(ckptMgr.SetVBTimestamps(mainPipeline.Topic()))

	for _, oneRecord := range ckptMgr.cur_ckpts {
		assert.NotNil(oneRecord.ckpt.Target_vb_opaque)
	}

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

	setupMock(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, bucketTopologySvc, spec, pipelineSupervisor, throughSeqnoMap, upsertCkptDoneErr, targetMCMap, targetMCDelayMap, targetMCResult, nil, nil, nil, nil)

	ckptMgr, err := NewCheckpointManager(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc,
		throughSeqnoTrackerSvc, activeVBs, "", "", "", targetKvVbMap,
		targetRef, nil, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc,
		getBackfillMgr, bucketTopologySvc, false)

	assert.Nil(err)
	assert.Nil(ckptMgr.Stop())
}

func TestCkptMgrGlobalCkpt(t *testing.T) {
	fmt.Println("============== Test case start: TestCkptMgrGlobalCkpt =================")
	defer fmt.Println("============== Test case end: TestCkptMgrGlobalCkpt =================")
	assert := assert.New(t)

	ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, getBackfillMgr, bucketTopologySvc, spec, pipelineSupervisor, _, targetKvVbMap, targetMCMap, targetMCDelayMap, targetMCResult := setupCkptMgrBoilerPlate()

	activeVBs := make(map[string][]uint16)
	activeVBs[kvKey] = []uint16{0, 1}
	targetRef, _ := metadata.NewRemoteClusterReference("", "C2", "", "", "",
		"", false, "", nil, nil, nil, nil)
	var upsertCkptDoneErr error

	ckptMgr, err := NewCheckpointManager(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc,
		throughSeqnoTrackerSvc, activeVBs, "", "", "", targetKvVbMap,
		targetRef, nil, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc,
		getBackfillMgr, bucketTopologySvc, true)

	assert.Nil(err)

	throughSeqnoMap := map[uint16]uint64{
		0: 5,
		1: 6,
	}
	srcManifestIds := map[uint16]uint64{
		0: 0,
		1: 0,
	}
	tgtManifestIds := map[uint16]uint64{
		0: 0,
		1: 1,
	}

	vb0TgtVbUuid := &metadata.TargetVBUuid{
		Target_vb_uuid: 12345,
	}
	vb1TgtVbUuid := &metadata.TargetVBUuid{
		Target_vb_uuid: 23456,
	}

	preReplicateTgtMap := map[*service_def_real.RemoteVBReplicationStatus]*metadata.TargetVBUuid{
		&service_def_real.RemoteVBReplicationStatus{VBNo: 0}: vb0TgtVbUuid,
		&service_def_real.RemoteVBReplicationStatus{VBNo: 1}: vb1TgtVbUuid,
	}

	// This map is for the actual checkpoint operation to be returned
	ckptOpHighSeqnoMap := make(base.HighSeqnoAndVbUuidMap)
	for tgtVBReplStatus, tgtVbUuid := range preReplicateTgtMap {
		ckptOpHighSeqnoMap[tgtVBReplStatus.VBNo] = []uint64{tgtVBReplStatus.VBSeqno + 10, tgtVbUuid.Target_vb_uuid}
	}

	setupMock(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr,
		uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, bucketTopologySvc, spec,
		pipelineSupervisor, throughSeqnoMap, upsertCkptDoneErr, targetMCMap, targetMCDelayMap, targetMCResult,
		srcManifestIds, tgtManifestIds, preReplicateTgtMap, ckptOpHighSeqnoMap)

	assert.Nil(err)

	ckptMgr.unitTest = true
	ckptMgr.checkpointAllowedHelper.setCheckpointAllowed()
	assert.Nil(ckptMgr.InitConnections())
	mainPipeline := setupMainPipelineMock(spec, pipelineSupervisor)
	assert.Nil(ckptMgr.Attach(mainPipeline))

	assert.NotEqual(0, len(ckptMgr.getMyTgtVBs()))
	assert.NotEqual(0, len(ckptMgr.getMyVBs()))

	// Initially, ckptDocGetter will return no docs
	ckptSvc.On("CheckpointsDocs", mainPipeline.Topic(), mock.Anything).Return(map[uint16]*metadata.CheckpointsDoc{}, nil)
	ckptMgr.checkpoints_svc = ckptSvc

	// Let's pretend ckptmgr started vbtimestamp, read no ckpt doc, and should have initialized internal ckptDoc structs
	assert.Nil(ckptMgr.SetVBTimestamps(mainPipeline.Topic()))

	assert.Equal(len(ckptMgr.getMyVBs()), len(ckptMgr.cur_ckpts))
	for _, oneRecord := range ckptMgr.cur_ckpts {
		assert.NotNil(oneRecord.ckpt.GlobalTimestamp)
		assert.NotNil(oneRecord.ckpt.GlobalCounters)

		for _, oneTgtVb := range ckptMgr.getMyTgtVBs() {
			assert.NotNil(oneRecord.ckpt.GlobalTimestamp)
			assert.NotNil(oneRecord.ckpt.GlobalTimestamp[oneTgtVb])
			assert.NotNil(oneRecord.ckpt.GlobalTimestamp[oneTgtVb].GetValue())
			globalVbTimestamp := oneRecord.ckpt.GlobalTimestamp[oneTgtVb].GetValue().(*metadata.GlobalVBTimestamp)
			assert.NotNil(globalVbTimestamp.Target_vb_opaque)
			assert.False(oneRecord.ckpt.GlobalTimestamp[oneTgtVb].IsTraditional())
			assert.NotNil(oneRecord.ckpt.GlobalTimestamp.GetValue())
			assert.NotNil(oneRecord.ckpt.GlobalCounters[oneTgtVb])

			// Check target opaque as it should have been set
			var shouldHaveFoundVb bool
			for tgtVBReplStatus, tgtVbUuid := range preReplicateTgtMap {
				if tgtVBReplStatus.VBNo == oneTgtVb {
					shouldHaveFoundVb = true
					assert.True(globalVbTimestamp.Target_vb_opaque.IsSame(tgtVbUuid))
				}
			}
			if !shouldHaveFoundVb {
				panic("check test logic")
			}
		}
	}

	// Let's pretend that someone requested a checkpoint operation

	// First we feed back the highSeqno and Vbuuid, pretend it moved and vbuuuid didn't change (nofailover)
	ckptMgr.PerformCkpt(nil)
	assert.Equal(uint32(0), atomic.LoadUint32(&supervisorErrCnt))
}

func TestCkptMgrRestoreLatestTargetManifest(t *testing.T) {
	fmt.Println("============== Test case start: TestCkptMgrRestoreLatestTargetManifest =================")
	defer fmt.Println("============== Test case end: TestCkptMgrRestoreLatestTargetManifest =================")
	assert := assert.New(t)

	ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, getBackfillMgr, bucketTopologySvc, spec, pipelineSupervisor, _, targetKvVbMap, targetMCMap, targetMCDelayMap, targetMCResult := setupCkptMgrBoilerPlate()
	activeVBs := make(map[string][]uint16)
	activeVBs[kvKey] = []uint16{0}
	targetRef, _ := metadata.NewRemoteClusterReference("", "C2", "", "", "",
		"", false, "", nil, nil, nil, nil)
	throughSeqnoMap := make(map[uint16]uint64)
	var upsertCkptDoneErr error

	targetMCDelayMap[kvKey] = 2

	ckptMgr, err := NewCheckpointManager(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc,
		throughSeqnoTrackerSvc, activeVBs, "", "", "", targetKvVbMap,
		targetRef, nil, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc,
		getBackfillMgr, bucketTopologySvc, false)

	assert.Nil(err)
	setupMock(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, bucketTopologySvc, spec, pipelineSupervisor, throughSeqnoMap, upsertCkptDoneErr, targetMCMap, targetMCDelayMap, targetMCResult, nil, nil, nil, nil)

	mainPipeline := setupMainPipelineMock(spec, pipelineSupervisor)
	ckptMgr.pipeline = mainPipeline

	// Before restoring, target manifest is 0
	assert.Equal(uint64(0), ckptMgr.cachedBrokenMap.correspondingTargetManifest)

	// Let's say there are two records, one outdated with brokenmap and one most up-to-date without brokenmap
	brokenMap := make(metadata.CollectionNamespaceMapping)
	srcNs := &base.CollectionNamespace{
		ScopeName:      "s1",
		CollectionName: "col",
	}
	tgtNs := &base.CollectionNamespace{
		ScopeName:      "s1",
		CollectionName: "col1",
	}
	brokenMap.AddSingleMapping(srcNs, tgtNs)
	brokenMapShaSlice, err := brokenMap.Sha256()
	assert.Nil(err)
	brokenMapStr := fmt.Sprintf("%x", brokenMapShaSlice)
	shaMap := make(metadata.ShaToCollectionNamespaceMap)
	shaMap[brokenMapStr] = &brokenMap

	ckptDocs := make(map[uint16]*metadata.CheckpointsDoc)

	record1 := metadata.CheckpointRecord{
		TargetVBTimestamp: metadata.TargetVBTimestamp{
			TargetManifest:      1,
			Target_vb_opaque:    &metadata.TargetVBUuidAndTimestamp{},
			BrokenMappingSha256: brokenMapStr,
		},
	}

	assert.Nil(record1.LoadBrokenMapping(shaMap))

	// Record 2 is "newer" in terms of target manifest and has no brokenmap
	record2 := metadata.CheckpointRecord{
		TargetVBTimestamp: metadata.TargetVBTimestamp{
			TargetManifest:   2,
			Target_vb_opaque: &metadata.TargetVBUuidAndTimestamp{},
		},
	}

	var recordList metadata.CheckpointRecordsList
	recordList = append(recordList, &record1)
	recordList = append(recordList, &record2)

	ckptDocs[0] = &metadata.CheckpointsDoc{
		Checkpoint_records: recordList,
		SpecInternalId:     "",
		Revision:           nil,
	}

	ckptMgr.loadBrokenMappings(ckptDocs)
	assert.Len(ckptMgr.cachedBrokenMap.brokenMap, 0)
	assert.Equal(uint64(2), ckptMgr.cachedBrokenMap.correspondingTargetManifest)
}

func TestCkptMgrPreReplicateCacheCtx(t *testing.T) {
	fmt.Println("============== Test case start: TestCkptMgrPreReplicateCacheCtx =================")
	defer fmt.Println("============== Test case end: TestCkptMgrPreReplicateCacheCtx =================")
	assert := assert.New(t)

	ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, getBackfillMgr, bucketTopologySvc, spec, pipelineSupervisor, _, targetKvVbMap, targetMCMap, targetMCDelayMap, targetMCResult := setupCkptMgrBoilerPlate()
	activeVBs := make(map[string][]uint16)
	activeVBs[kvKey] = []uint16{0}
	targetRef, _ := metadata.NewRemoteClusterReference("", "C2", "", "", "",
		"", false, "", nil, nil, nil, nil)
	throughSeqnoMap := make(map[uint16]uint64)
	var upsertCkptDoneErr error

	targetMCDelayMap[kvKey] = 2

	ckptMgr, err := NewCheckpointManager(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc,
		throughSeqnoTrackerSvc, activeVBs, "", "", "", targetKvVbMap,
		targetRef, nil, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc,
		getBackfillMgr, bucketTopologySvc, true)

	assert.Nil(err)
	setupMock(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, bucketTopologySvc, spec, pipelineSupervisor, throughSeqnoMap, upsertCkptDoneErr, targetMCMap, targetMCDelayMap, targetMCResult, nil, nil, nil, nil)

	// Replace with a customized one
	capiSvcErr := &service_def.CAPIService{}
	var preReplicateCounter uint32
	// capiSvc that returns error
	capiSvcErr.On("PreReplicate", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		atomic.AddUint32(&preReplicateCounter, 1)
	}).Return(false, nil, fmt.Errorf("dummy"))
	ckptMgr.capi_svc = capiSvcErr

	cacheCtx := ckptMgr.newGlobalCkptPrereplicateCacheCtx(5*time.Second, 250*time.Millisecond)

	// Test an error scenario where the error code is stored and returned various times before expiration
	dummyRemBucketInfo := &service_def_real.RemoteBucketInfo{}
	tgtTs := &service_def_real.RemoteVBReplicationStatus{}

	_, _, err = cacheCtx.preReplicate(dummyRemBucketInfo, tgtTs)
	assert.NotNil(err)
	assert.Equal(uint32(1), atomic.LoadUint32(&preReplicateCounter))
	assert.Len(cacheCtx.lastQueriedTimers, 1)
	// Immediately do another pull, call should not increment, same error
	_, _, err = cacheCtx.preReplicate(dummyRemBucketInfo, tgtTs)
	assert.NotNil(err)
	assert.Equal(uint32(1), atomic.LoadUint32(&preReplicateCounter))
	assert.Len(cacheCtx.lastQueriedTimers, 1)

	// error should expire after 1 second
	time.Sleep(1 * time.Second)
	assert.Len(cacheCtx.lastQueriedTimers, 0)

	// Replace with a customized one that returns positive results, like positive match
	capiSvcGood := &service_def.CAPIService{}
	capiSvcGood.On("PreReplicate", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		atomic.AddUint32(&preReplicateCounter, 1)
	}).Return(true, nil, nil)
	ckptMgr.capi_svc = capiSvcGood

	cacheCtx = ckptMgr.newGlobalCkptPrereplicateCacheCtx(1*time.Second, 250*time.Millisecond)
	match, _, err := cacheCtx.preReplicate(dummyRemBucketInfo, tgtTs)
	assert.Nil(err)
	assert.True(match)
	assert.Equal(uint32(2), atomic.LoadUint32(&preReplicateCounter))
	assert.Len(cacheCtx.lastQueriedTimers, 1)
	// Immediately do another pull, call should not increment, same error
	_, _, err = cacheCtx.preReplicate(dummyRemBucketInfo, tgtTs)
	assert.Nil(err)
	assert.True(match)
	assert.Equal(uint32(2), atomic.LoadUint32(&preReplicateCounter))

}

func TestCkptmgrStopTheWorldMergeGlobal(t *testing.T) {
	fmt.Println("============== Test case start: TestCkptmgrStopTheWorldMergeGlobal =================")
	defer fmt.Println("============== Test case end: TestCkptmgrStopTheWorldMergeGlobal =================")
	assert := assert.New(t)

	ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, getBackfillMgr, bucketTopologySvc, spec, pipelineSupervisor, _, targetKvVbMap, targetMCMap, targetMCDelayMap, targetMCResult := setupCkptMgrBoilerPlate()
	activeVBs := make(map[string][]uint16)
	activeVBs[kvKey] = []uint16{0}
	targetRef, _ := metadata.NewRemoteClusterReference("", "C2", "", "", "",
		"", false, "", nil, nil, nil, nil)
	throughSeqnoMap := make(map[uint16]uint64)
	var upsertCkptDoneErr error

	targetMCDelayMap[kvKey] = 2

	ckptMgr, err := NewCheckpointManager(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc,
		throughSeqnoTrackerSvc, activeVBs, "", "", "", targetKvVbMap,
		targetRef, nil, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc,
		getBackfillMgr, bucketTopologySvc, true)

	assert.Nil(err)
	setupMock(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, bucketTopologySvc, spec, pipelineSupervisor, throughSeqnoMap, upsertCkptDoneErr, targetMCMap, targetMCDelayMap, targetMCResult, nil, nil, nil, nil)

	mainPipeline := setupMainPipelineMock(spec, pipelineSupervisor)
	ckptMgr.pipeline = mainPipeline

	mergeCkptArgs := generateMergeCkptArgs()
	assert.Nil(err)
	assert.NotEqual(0, len(mergeCkptArgs.PipelinesCkptDocs))
	assert.NotEqual(0, len(mergeCkptArgs.BrokenMappingShaMap))

	var recordWithBrokenmapSha int
	for _, vbsCkptDocMap := range mergeCkptArgs.PipelinesCkptDocs {
		for _, ckptDoc := range vbsCkptDocMap {
			assert.False(ckptDoc.IsTraditional())
			for _, aRecord := range ckptDoc.Checkpoint_records {
				if aRecord == nil {
					continue
				}
				assert.False(aRecord.IsTraditional())
				for _, gts := range aRecord.GlobalTimestamp {
					if gts.BrokenMappingSha256 != "" {
						recordWithBrokenmapSha++
					}
				}
			}
		}
	}
	assert.NotEqual(0, recordWithBrokenmapSha)

	setupStopTheWorldCkptSvcMock(ckptSvc, true, assert, recordWithBrokenmapSha, mergeCkptArgs.BrokenMappingShaMap, mergeCkptArgs.GlobalInfoShaMap)

	getter := func() *MergeCkptArgs {
		return mergeCkptArgs
	}
	assert.Nil(ckptMgr.stopTheWorldAndMergeCkpts(getter))

}

// This is used specific for randomly generated global ckpt
func setupStopTheWorldCkptSvcMock(ckptSvc *service_def.CheckpointsService, alreadyExist bool, assert *assert.Assertions,
	brokenMapCntExpected int, brokenMapShaMap metadata.ShaToCollectionNamespaceMap, gtsShaMap metadata.ShaToGlobalInfoMap) {
	dummyIncrfunc := service_def_real.IncrementerFunc(func(string, interface{}) {})
	mappingDoc := &metadata.CollectionNsMappingsDoc{}
	emptyShaMap := make(metadata.ShaToCollectionNamespaceMap)
	ckptSvc.On("LoadBrokenMappings", mock.Anything).Return(emptyShaMap, mappingDoc, dummyIncrfunc, alreadyExist, nil)
	ckptSvc.On("UpsertAndReloadCheckpointCompleteSet", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		var recordWithBrokenmapSha int
		specId := args.Get(0).(string)
		if strings.Contains(specId, "backfill") {
			return
		}
		ckptDocs := args.Get(2).(map[uint16]*metadata.CheckpointsDoc)
		assert.NotEqual(0, len(ckptDocs))
		for _, aDoc := range ckptDocs {
			assert.False(aDoc.IsTraditional())
			for _, aRecord := range aDoc.Checkpoint_records {
				if aRecord == nil {
					continue
				}
				for _, gts := range aRecord.GlobalTimestamp {
					if gts.BrokenMappingSha256 != "" {
						recordWithBrokenmapSha++
					}
				}
			}
		}
		assert.Equal(brokenMapCntExpected, recordWithBrokenmapSha)

		checkMappingDoc := args.Get(1).(*metadata.CollectionNsMappingsDoc)
		assert.Len(checkMappingDoc.NsMappingRecords, 1)

		checkGtsDoc := args.Get(4).(*metadata.GlobalInfoCompressedDoc)
		assert.Len(checkGtsDoc.NsMappingRecords, 3) // 2 from doing random generate
	}).Return(nil)

	dummyGtsCompresesdDoc := &metadata.GlobalInfoCompressedDoc{}
	ckptSvc.On("LoadGlobalInfoMapping", mock.Anything).Return(gtsShaMap, dummyGtsCompresesdDoc, dummyIncrfunc, false, nil)
}

func generatePipelinesGlobalCkptDocs(brokenMapShaKeyToInsert string) (VBsCkptsDocMaps, metadata.ShaToGlobalInfoMap) {
	oneDocMap := metadata.GenerateGlobalVBsCkptDocMap([]uint16{0, 1}, brokenMapShaKeyToInsert)
	var list VBsCkptsDocMaps = VBsCkptsDocMaps{oneDocMap}

	gtsShaMap := make(metadata.ShaToGlobalInfoMap)
	for _, checkpointDoc := range oneDocMap {
		for _, oneRecord := range checkpointDoc.Checkpoint_records {
			if oneRecord == nil {
				continue
			}

			if oneRecord.GlobalTimestampSha256 != "" {
				gtsShaMap[oneRecord.GlobalTimestampSha256] = &oneRecord.GlobalTimestamp
			}
			if oneRecord.GlobalCountersSha256 != "" {
				gtsShaMap[oneRecord.GlobalCountersSha256] = &oneRecord.GlobalCounters
			}
		}
	}
	return list, gtsShaMap
}

func generateBrokenMap() metadata.ShaToCollectionNamespaceMap {
	brokenMap := make(metadata.CollectionNamespaceMapping)
	s1C1, err := base.NewCollectionNamespaceFromString("S1.col1")
	if err != nil {
		panic(err.Error())
	}
	s1C2, err := base.NewCollectionNamespaceFromString("S1.col2")
	if err != nil {
		panic(err.Error())
	}
	brokenMap.AddSingleMapping(&s1C1, &s1C2)
	shaBytes, err := brokenMap.Sha256()
	if err != nil {
		panic(err.Error())
	}

	brokenMappingSha := make(metadata.ShaToCollectionNamespaceMap)
	brokenMappingSha[fmt.Sprintf("%x", shaBytes[:])] = &brokenMap

	return brokenMappingSha
}

func generateMergeCkptArgs() *MergeCkptArgs {
	var brokenMapShaKey string
	brokenMapSha := generateBrokenMap()
	for k, _ := range brokenMapSha {
		brokenMapShaKey = k
	}

	vbsCkptDocMaps, gInfoShaMap := generatePipelinesGlobalCkptDocs(brokenMapShaKey)

	retVal := &MergeCkptArgs{
		PipelinesCkptDocs:       vbsCkptDocMaps,
		BrokenMappingShaMap:     brokenMapSha,
		GlobalInfoShaMap:        gInfoShaMap,
		BrokenMapSpecInternalId: "",
		SrcManifests:            nil,
		TgtManifests:            nil,
		SrcFailoverLogs:         nil,
		TgtFailoverLogs:         nil,
		PushRespChs:             nil,
	}
	return retVal
}

func setupSpecSettings(replicationSpec *metadata.ReplicationSpecification) {
	specSetting := metadata.NewEmptyReplicationSettings()
	specSetting.Settings = metadata.EmptySettings(func() map[string]*metadata.SettingsConfig {
		configMap := make(map[string]*metadata.SettingsConfig)
		configMap["CollectionsMgtMulti"] = metadata.CollectionsMgtConfig
		return configMap
	})
	replicationSpec.Settings = specSetting
}

func TestCheckpointManager_OnEvent(t *testing.T) {
	assert := assert.New(t)
	settings := metadata.ReplicationSettingsMap{}

	_, throughSeqSvc, xdcrTopologySvc, utils, activeVBs, pipeline, replicationSpec, runtimeCtx, ckptService, capiSvc, remoteClusterSvc, replSpecSvc, targetKVVbMap, remoteClusterRef, dcpNozzle, connector, uiLogSvc, collectionsManifestSvc, backfillReplSvc, xmemNozzle := setupBoilerPlate()
	setupMocks(throughSeqSvc, xdcrTopologySvc, utils, activeVBs,
		pipeline, replicationSpec, runtimeCtx, ckptService, capiSvc, remoteClusterSvc, replSpecSvc,
		targetKVVbMap, remoteClusterRef, dcpNozzle, connector, uiLogSvc, collectionsManifestSvc,
		backfillReplSvc, xmemNozzle)
	setupSpecSettings(replicationSpec)

	statsMgr := NewStatisticsManager(throughSeqSvc, xdcrTopologySvc, log.DefaultLoggerContext, activeVBs, "TestBucket", utils, remoteClusterSvc, nil, nil, false, false)
	assert.NotNil(statsMgr)

	ckptManager := setupCheckpointMgr(ckptService, capiSvc, remoteClusterSvc, replSpecSvc,
		xdcrTopologySvc, throughSeqSvc, activeVBs, targetKVVbMap, remoteClusterRef, utils, statsMgr, uiLogSvc,
		collectionsManifestSvc, backfillReplSvc)
	ckptManager.pipeline = pipeline
	pipeline.On("SetBrokenMap", mock.Anything).Return()

	// initialise the settings and start the checkpointMgr
	settings["checkpoint_interval"] = 600
	err := ckptManager.Start(settings)
	assert.Nil(err)

	var info parts.CollectionsRoutingInfo
	// create namespace mappings
	c1 := &base.CollectionNamespace{ScopeName: "S1", CollectionName: "C1"}
	c2 := &base.CollectionNamespace{ScopeName: "S1", CollectionName: "C2"}
	c3 := &base.CollectionNamespace{ScopeName: "S1", CollectionName: "C3"}

	//c1_c2_mapping and c3_mapping denote the backfill mapping to be raised
	brokenMapping := make(metadata.CollectionNamespaceMapping)
	c1_c2_mapping := make(metadata.CollectionNamespaceMapping)
	c3_mapping := make(metadata.CollectionNamespaceMapping)

	brokenMapping.AddSingleMapping(c1, c1)
	brokenMapping.AddSingleMapping(c2, c2)
	brokenMapping.AddSingleMapping(c3, c3)

	c1_c2_mapping.AddSingleMapping(c1, c1)
	c1_c2_mapping.AddSingleMapping(c2, c2)

	c3_mapping.AddSingleMapping(c3, c3)

	// initially the broken map should be nil
	assert.Equal(len(ckptManager.cachedBrokenMap.brokenMap), 0)

	// 1. First raise an event to add the broken mapping. Lets say tgt manifest 1
	info.BrokenMap = brokenMapping
	info.TargetManifestId = 1
	// The sync channel here is of no use. Hence no harm in making it buffered
	syncCh := make(chan error, 1)
	event := common.NewEvent(common.BrokenRoutingUpdateEvent, info, nil, nil, syncCh)
	ckptManager.OnEvent(event)
	assert.Equal(3, len(ckptManager.cachedBrokenMap.brokenMap))
	assert.Equal(uint64(1), ckptManager.cachedBrokenMap.correspondingTargetManifest)
	assert.Equal(1, len(ckptManager.cachedBrokenMap.brokenMapHistories))

	// 2. Raise a backfill event with c1,c2 fixed (say router1 raised it)
	info.BrokenMap = metadata.CollectionNamespaceMapping{}
	info.BackfillMap = c1_c2_mapping
	info.TargetManifestId = 3
	syncCh = make(chan error, 1)
	event = common.NewEvent(common.BrokenRoutingUpdateEvent, info, nil, nil, syncCh)
	ckptManager.OnEvent(event)
	assert.Equal(1, len(ckptManager.cachedBrokenMap.brokenMap))
	assert.Equal(uint64(3), ckptManager.cachedBrokenMap.correspondingTargetManifest)
	assert.Equal(2, len(ckptManager.cachedBrokenMap.brokenMapHistories))

	// 3. Raise a another backfill event with c1,c2 fixed (say router2 raised it)
	info.BrokenMap = metadata.CollectionNamespaceMapping{}
	info.BackfillMap = c1_c2_mapping
	info.TargetManifestId = 3
	syncCh = make(chan error, 1)
	event = common.NewEvent(common.BrokenRoutingUpdateEvent, info, nil, nil, syncCh)
	ckptManager.OnEvent(event)
	assert.Equal(1, len(ckptManager.cachedBrokenMap.brokenMap))
	assert.Equal(uint64(3), ckptManager.cachedBrokenMap.correspondingTargetManifest)
	assert.Equal(2, len(ckptManager.cachedBrokenMap.brokenMapHistories))

	// 4. Raise a another backfill event with c3 fixed (say router3 raised it)
	info.BrokenMap = metadata.CollectionNamespaceMapping{}
	info.BackfillMap = c3_mapping
	info.TargetManifestId = 3
	syncCh = make(chan error, 1)
	event = common.NewEvent(common.BrokenRoutingUpdateEvent, info, nil, nil, syncCh)
	ckptManager.OnEvent(event)
	assert.Equal(0, len(ckptManager.cachedBrokenMap.brokenMap))
	assert.Equal(uint64(3), ckptManager.cachedBrokenMap.correspondingTargetManifest)
	assert.Equal(2, len(ckptManager.cachedBrokenMap.brokenMapHistories))

	// 5. Raise a another brokenMap event
	info.BrokenMap = c3_mapping
	info.BackfillMap = metadata.CollectionNamespaceMapping{}
	info.TargetManifestId = 4
	syncCh = make(chan error, 1)
	event = common.NewEvent(common.BrokenRoutingUpdateEvent, info, nil, nil, syncCh)
	ckptManager.OnEvent(event)
	assert.Equal(1, len(ckptManager.cachedBrokenMap.brokenMap))
	assert.Equal(uint64(4), ckptManager.cachedBrokenMap.correspondingTargetManifest)
	assert.Equal(3, len(ckptManager.cachedBrokenMap.brokenMapHistories))

}

func TestCheckpointManager_CleanupInMemoryBrokenMap(t *testing.T) {
	assert := assert.New(t)
	settings := metadata.ReplicationSettingsMap{}
	_, throughSeqSvc, xdcrTopologySvc, utils, activeVBs, pipeline, replicationSpec, runtimeCtx, ckptService, capiSvc, remoteClusterSvc, replSpecSvc, targetKVVbMap, remoteClusterRef, dcpNozzle, connector, uiLogSvc, collectionsManifestSvc, backfillReplSvc, xmemNozzle := setupBoilerPlate()

	setupMocks(throughSeqSvc, xdcrTopologySvc, utils, activeVBs,
		pipeline, replicationSpec, runtimeCtx, ckptService, capiSvc, remoteClusterSvc, replSpecSvc,
		targetKVVbMap, remoteClusterRef, dcpNozzle, connector, uiLogSvc, collectionsManifestSvc,
		backfillReplSvc, xmemNozzle)

	setupSpecSettings(replicationSpec)

	statsMgr := NewStatisticsManager(throughSeqSvc, xdcrTopologySvc, log.DefaultLoggerContext, activeVBs, "TestBucket", utils, remoteClusterSvc, nil, nil, false, false)
	assert.NotNil(statsMgr)

	ckptManager := setupCheckpointMgr(ckptService, capiSvc, remoteClusterSvc, replSpecSvc,
		xdcrTopologySvc, throughSeqSvc, activeVBs, targetKVVbMap, remoteClusterRef, utils, statsMgr, uiLogSvc,
		collectionsManifestSvc, backfillReplSvc)
	ckptManager.pipeline = pipeline
	pipeline.On("SetBrokenMap", mock.Anything).Return()

	// initialise the settings and start the checkpointMgr
	settings["checkpoint_interval"] = 600
	err := ckptManager.Start(settings)
	assert.Nil(err)

	// create namespace mappings
	c1 := &base.CollectionNamespace{ScopeName: "S1", CollectionName: "C1"}
	c2 := &base.CollectionNamespace{ScopeName: "S1", CollectionName: "C2"}
	c3 := &base.CollectionNamespace{ScopeName: "S1", CollectionName: "C3"}

	//c1_c2_mapping and c3_mapping denote the backfill mapping to be raised
	brokenMapping := make(metadata.CollectionNamespaceMapping)
	c1_c2_mapping := make(metadata.CollectionNamespaceMapping)
	c3_mapping := make(metadata.CollectionNamespaceMapping)
	defaultMan := metadata.NewDefaultCollectionsManifest()

	brokenMapping.AddSingleMapping(c1, c1)
	brokenMapping.AddSingleMapping(c2, c2)
	brokenMapping.AddSingleMapping(c3, c3)

	c1_c2_mapping.AddSingleMapping(c1, c1)
	c1_c2_mapping.AddSingleMapping(c2, c2)

	c3_mapping.AddSingleMapping(c3, c3)

	// initially the broken map should be nil
	assert.Equal(0, len(ckptManager.cachedBrokenMap.brokenMap))

	// 1. First initialise the broken mapping. Lets say tgt manifest 1
	ckptManager.cachedBrokenMap.brokenMap = brokenMapping
	ckptManager.cachedBrokenMap.correspondingTargetManifest = 1

	// 2. Say collection Manifest Service fixed broken mapping c1->c1 and c2->c2
	settings1 := metadata.ReplicationSettingsMap{}
	diffPair := &metadata.CollectionNamespaceMappingsDiffPair{}
	diffPair.Added = c1_c2_mapping
	settings1[metadata.CkptMgrBrokenmapIdleUpdateDiffPair] = diffPair
	settings1[metadata.CkptMgrBrokenmapIdleUpdateSrcManDelta] = []*metadata.CollectionsManifest{&defaultMan, &defaultMan}
	settings1[metadata.CkptMgrBrokenmapIdleUpdateLatestTgtManId] = uint64(3)
	ckptManager.UpdateSettings(settings1)
	assert.Equal(1, len(ckptManager.cachedBrokenMap.brokenMap))
	assert.Equal(uint64(3), ckptManager.cachedBrokenMap.correspondingTargetManifest)
	assert.Equal(1, len(ckptManager.cachedBrokenMap.brokenMapHistories))

	// 3. Say collection Manifest Service fixed broken mapping c1->c1 and c2->c2 (but by this time say router already fixed it)
	// In this case this should be a no-op
	settings1 = metadata.ReplicationSettingsMap{}
	diffPair = &metadata.CollectionNamespaceMappingsDiffPair{}
	diffPair.Added = c1_c2_mapping
	settings1[metadata.CkptMgrBrokenmapIdleUpdateDiffPair] = diffPair
	settings1[metadata.CkptMgrBrokenmapIdleUpdateSrcManDelta] = []*metadata.CollectionsManifest{&defaultMan, &defaultMan}
	settings1[metadata.CkptMgrBrokenmapIdleUpdateLatestTgtManId] = uint64(3)
	ckptManager.UpdateSettings(settings1)
	assert.Equal(1, len(ckptManager.cachedBrokenMap.brokenMap))
	assert.Equal(uint64(3), ckptManager.cachedBrokenMap.correspondingTargetManifest)
	assert.Equal(1, len(ckptManager.cachedBrokenMap.brokenMapHistories))

	// 4. Say now c3->c3 mapping is fixed and the target manifest is now 4
	settings1 = metadata.ReplicationSettingsMap{}
	diffPair = &metadata.CollectionNamespaceMappingsDiffPair{}
	diffPair.Added = c3_mapping
	settings1[metadata.CkptMgrBrokenmapIdleUpdateDiffPair] = diffPair
	settings1[metadata.CkptMgrBrokenmapIdleUpdateSrcManDelta] = []*metadata.CollectionsManifest{&defaultMan, &defaultMan}
	settings1[metadata.CkptMgrBrokenmapIdleUpdateLatestTgtManId] = uint64(4)
	ckptManager.UpdateSettings(settings1)
	assert.Equal(0, len(ckptManager.cachedBrokenMap.brokenMap))
	assert.Equal(uint64(4), ckptManager.cachedBrokenMap.correspondingTargetManifest)
	assert.Equal(2, len(ckptManager.cachedBrokenMap.brokenMapHistories))
}

func Test_prepareForLocalPreReplicate(t *testing.T) {

	var f *mcc.FailoverLog = &mcc.FailoverLog{
		{189935894142520, 7879},
		{189935894896622, 1000},
		{150911719482835, 0},
	}
	var f1 *mcc.FailoverLog = &mcc.FailoverLog{
		{150911719482835, 0},
	}
	var f2 *mcc.FailoverLog = &mcc.FailoverLog{}

	tests := []struct {
		name        string
		failoverLog *mcc.FailoverLog
		want        *mcc.FailoverLog
	}{

		{
			name:        "Test1",
			failoverLog: f,
			want:        &mcc.FailoverLog{{189935894142520, math.MaxUint64}, {189935894896622, 7879}, {150911719482835, 1000}},
		},
		{
			name:        "Test2",
			failoverLog: f1,
			want:        &mcc.FailoverLog{{150911719482835, math.MaxUint}},
		},
		{
			name:        "Test3",
			failoverLog: f2,
			want:        nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, _ := prepareForLocalPreReplicate(tt.failoverLog); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("prepareForLocalPreReplicate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_localPreReplicate(t *testing.T) {
	var f *mcc.FailoverLog = &mcc.FailoverLog{
		{189935894142520, 7879},
		{189935894896622, 1000},
		{150911719482835, 0},
	}
	modifiedFailoverLog, _ := prepareForLocalPreReplicate(f)
	tests := []struct {
		name         string
		commitOpaque [2]uint64
		failoverLog  *mcc.FailoverLog
		want         bool
	}{
		{
			name:         "Invalid UUID",
			commitOpaque: [2]uint64{1, 100},
			failoverLog:  modifiedFailoverLog,
			want:         false,
		},
		{
			name:         "Valid UUID+seq pair",
			commitOpaque: [2]uint64{189935894896622, 500},
			failoverLog:  modifiedFailoverLog,
			want:         true,
		},
		{
			name:         "Valid UUID but invalid Seq",
			commitOpaque: [2]uint64{150911719482835, 1010},
			failoverLog:  modifiedFailoverLog,
			want:         false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := localPreReplicate(tt.failoverLog, tt.commitOpaque); got != tt.want {
				t.Errorf("localPreReplicate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBackfillCollectionIdStamping(t *testing.T) {
	fmt.Println("============== Test case start: TestBackfillCollectionIdStamping =================")
	defer fmt.Println("============== Test case end: TestBackfillCollectionIdStamping =================")
	assert := assert.New(t)
	vbno := uint16(1)
	pipeline := &commonMock.Pipeline{}
	pipeline.On("Type").Return(common.BackfillPipeline)
	ckmgr := &CheckpointManager{
		backfillCollections: map[uint16][]uint32{
			vbno: {10, 11, 12},
		},
		pipeline: pipeline,
	}
	other := &metadata.CheckpointRecord{}
	notSame := &metadata.CheckpointRecord{BackfillCollections: []uint32{1000, 2000, 30000}}
	unmarshalledRecord := &metadata.CheckpointRecord{}
	marshaller := func(record *metadata.CheckpointRecord) []byte {
		res, err := json.Marshal(record)
		assert.Nil(err)
		return res
	}

	// 1. pre-fix backfill checkpoints should not be skipped
	record := &metadata.CheckpointRecord{
		BackfillCollections: nil,
	}
	assert.False(ckmgr.filterBackfillCheckpointRecordsOnColIDs(vbno, record))
	other.Load(record)
	assert.True(record.SameAs(other))
	cloned := record.Clone()
	assert.True(record.SameAs(cloned))
	assert.False(record.SameAs(notSame))
	bytes := marshaller(record)
	assert.Nil(unmarshalledRecord.UnmarshalJSON(bytes))
	assert.True(record.SameAs(unmarshalledRecord))

	// 2. exact match - can resume
	record = &metadata.CheckpointRecord{
		BackfillCollections: []uint32{10, 11, 12},
	}
	assert.False(ckmgr.filterBackfillCheckpointRecordsOnColIDs(vbno, record))
	other.Load(record)
	assert.True(record.SameAs(other))
	cloned = record.Clone()
	assert.True(record.SameAs(cloned))
	assert.False(record.SameAs(notSame))
	bytes = marshaller(record)
	assert.Nil(unmarshalledRecord.UnmarshalJSON(bytes))
	assert.True(record.SameAs(unmarshalledRecord))

	// 3. current backfill collections is a subset of checkpoint collections - can resume
	record = &metadata.CheckpointRecord{
		BackfillCollections: []uint32{5, 6, 10, 11, 12, 13, 100, 200},
	}
	assert.False(ckmgr.filterBackfillCheckpointRecordsOnColIDs(vbno, record))
	other.Load(record)
	assert.True(record.SameAs(other))
	cloned = record.Clone()
	assert.True(record.SameAs(cloned))
	assert.False(record.SameAs(notSame))
	bytes = marshaller(record)
	assert.Nil(unmarshalledRecord.UnmarshalJSON(bytes))
	assert.True(record.SameAs(unmarshalledRecord))

	// 4. current backfill collections is not a subset of checkpoint collections - can't resume
	record = &metadata.CheckpointRecord{
		BackfillCollections: []uint32{5, 6, 10, 11},
	}
	assert.True(ckmgr.filterBackfillCheckpointRecordsOnColIDs(vbno, record))
	other.Load(record)
	assert.True(record.SameAs(other))
	cloned = record.Clone()
	assert.True(record.SameAs(cloned))
	assert.False(record.SameAs(notSame))
	bytes = marshaller(record)
	assert.Nil(unmarshalledRecord.UnmarshalJSON(bytes))
	assert.True(record.SameAs(unmarshalledRecord))

	// 5. checkpoint was for a disjoint set - can't resume
	record = &metadata.CheckpointRecord{
		BackfillCollections: []uint32{20, 21},
	}
	assert.True(ckmgr.filterBackfillCheckpointRecordsOnColIDs(vbno, record))
	other.Load(record)
	assert.True(record.SameAs(other))
	cloned = record.Clone()
	assert.True(record.SameAs(cloned))
	assert.False(record.SameAs(notSame))
	bytes = marshaller(record)
	assert.Nil(unmarshalledRecord.UnmarshalJSON(bytes))
	assert.True(record.SameAs(unmarshalledRecord))

	// 6. odd unrealistic case of empty list
	record = &metadata.CheckpointRecord{
		BackfillCollections: []uint32{},
	}
	ckmgr.backfillCollections[vbno] = []uint32{}
	assert.False(ckmgr.filterBackfillCheckpointRecordsOnColIDs(vbno, record))
	other.Load(record)
	assert.True(record.SameAs(other))
	cloned = record.Clone()
	assert.True(record.SameAs(cloned))
	assert.False(record.SameAs(notSame))
	bytes = marshaller(record)
	assert.Nil(unmarshalledRecord.UnmarshalJSON(bytes))
	assert.True(record.SameAs(unmarshalledRecord))
}

func TestCkptMgrNoPanicOnMissingTargetOpaque(t *testing.T) {
	fmt.Println("============== Test case start: TestCkptMgrNoPanicOnMissingTargetOpaque =================")
	defer fmt.Println("============== Test case end: TestCkptMgrNoPanicOnMissingTargetOpaque =================")
	assert := assert.New(t)

	ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, getBackfillMgr, bucketTopologySvc, spec, pipelineSupervisor, _, targetKvVbMap, targetMCMap, targetMCDelayMap, targetMCResult := setupCkptMgrBoilerPlate()

	activeVBs := make(map[string][]uint16)
	activeVBs[kvKey] = []uint16{0}
	targetRef, _ := metadata.NewRemoteClusterReference("", "C2", "", "", "",
		"", false, "", nil, nil, nil, nil)
	throughSeqnoMap := make(map[uint16]uint64)
	throughSeqnoMap[0] = 100
	var upsertCkptDoneErr error

	// Test scenario 1: Traditional mode - target VB opaque not populated
	t.Run("Traditional mode - target opaque not populated", func(t *testing.T) {
		setupMock(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc, throughSeqnoTrackerSvc, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc, backfillMgrIface, bucketTopologySvc, spec, pipelineSupervisor, throughSeqnoMap, upsertCkptDoneErr, targetMCMap, targetMCDelayMap, targetMCResult, nil, nil, nil, nil)

		ckptMgr, err := NewCheckpointManager(ckptSvc, capiSvc, remoteClusterSvc, replSpecSvc, xdcrTopologySvc,
			throughSeqnoTrackerSvc, activeVBs, "", "", "", targetKvVbMap,
			targetRef, nil, utils, statsMgr, uiLogSvc, collectionsManifestSvc, backfillReplSvc,
			getBackfillMgr, bucketTopologySvc, false)

		assert.Nil(err)
		assert.NotNil(ckptMgr)
		ckptMgr.unitTest = true

		// Manually create a checkpoint record with nil target_vb_opaque to simulate the scenario
		ckpt_obj := &checkpointRecordWithLock{
			ckpt: &metadata.CheckpointRecord{
				SourceVBTimestamp: metadata.SourceVBTimestamp{
					Seqno: 100,
				},
				TargetVBTimestamp: metadata.TargetVBTimestamp{
					Target_vb_opaque: nil, // This is the problematic case
				},
			},
			lock: &sync.RWMutex{},
		}
		ckptMgr.cur_ckpts[0] = ckpt_obj

		high_seqno_and_vbuuid_map := make(map[uint16][]uint64)
		high_seqno_and_vbuuid_map[0] = []uint64{200, 12345}
		srcManifestIds := make(map[uint16]uint64)
		tgtManifestIds := make(map[uint16]uint64)

		_, err = ckptMgr.doCheckpoint(0, 100, high_seqno_and_vbuuid_map, srcManifestIds, tgtManifestIds)

		assert.NotNil(err)
		assert.Contains(err.Error(), "not populated properly")
	})

	// Test scenario 2: Variable VB mode - GlobalTimestamp with nil target_vb_opaque
	t.Run("Variable VB mode - global timestamp with nil opaque", func(t *testing.T) {
		ckptSvc2 := &service_def.CheckpointsService{}
		capiSvc2 := &service_def.CAPIService{}
		remoteClusterSvc2 := &service_def.RemoteClusterSvc{}
		replSpecSvc2 := &service_def.ReplicationSpecSvc{}
		xdcrTopologySvc2 := &service_def.XDCRCompTopologySvc{}
		throughSeqnoTrackerSvc2 := &service_def.ThroughSeqnoTrackerSvc{}
		utils2 := &utilities.UtilsIface{}
		statsMgr2 := &service_def.StatsMgrIface{}
		uiLogSvc2 := &service_def.UILogSvc{}
		collectionsManifestSvc2 := &service_def.CollectionsManifestSvc{}
		backfillReplSvc2 := &service_def.BackfillReplSvc{}
		backfillMgrIface2 := &service_def.BackfillMgrIface{}
		getBackfillMgr2 := func() service_def_real.BackfillMgrIface {
			return backfillMgrIface2
		}
		bucketTopologySvc2 := &service_def.BucketTopologySvc{}

		setupMock(ckptSvc2, capiSvc2, remoteClusterSvc2, replSpecSvc2, xdcrTopologySvc2, throughSeqnoTrackerSvc2, utils2, statsMgr2, uiLogSvc2, collectionsManifestSvc2, backfillReplSvc2, backfillMgrIface2, bucketTopologySvc2, spec, pipelineSupervisor, throughSeqnoMap, upsertCkptDoneErr, targetMCMap, targetMCDelayMap, targetMCResult, nil, nil, nil, nil)

		ckptMgr2, err := NewCheckpointManager(ckptSvc2, capiSvc2, remoteClusterSvc2, replSpecSvc2, xdcrTopologySvc2,
			throughSeqnoTrackerSvc2, activeVBs, "", "", "", targetKvVbMap,
			targetRef, nil, utils2, statsMgr2, uiLogSvc2, collectionsManifestSvc2, backfillReplSvc2,
			getBackfillMgr2, bucketTopologySvc2, true)

		assert.Nil(err)
		assert.NotNil(ckptMgr2)
		ckptMgr2.unitTest = true

		globalTimestamp := make(metadata.GlobalTimestamp)
		globalTimestamp[0] = &metadata.GlobalVBTimestamp{
			TargetVBTimestamp: metadata.TargetVBTimestamp{
				Target_vb_opaque: nil,
			},
		}

		ckpt_obj2 := &checkpointRecordWithLock{
			ckpt: &metadata.CheckpointRecord{
				SourceVBTimestamp: metadata.SourceVBTimestamp{
					Seqno: 100,
				},
				GlobalTimestamp: globalTimestamp,
			},
			lock: &sync.RWMutex{},
		}
		ckptMgr2.cur_ckpts[0] = ckpt_obj2

		high_seqno_and_vbuuid_map := make(map[uint16][]uint64)
		high_seqno_and_vbuuid_map[0] = []uint64{200, 12345}
		srcManifestIds := make(map[uint16]uint64)
		tgtManifestIds := make(map[uint16]uint64)

		_, err = ckptMgr2.doCheckpoint(0, 100, high_seqno_and_vbuuid_map, srcManifestIds, tgtManifestIds)

		assert.NotNil(err)
		assert.Contains(err.Error(), " is not populated properly. err=target timestamp for vb")
	})

	// Test scenario 3: Variable VB mode - GlobalTimestamp is empty
	t.Run("Variable VB mode - global timestamp is empty", func(t *testing.T) {
		ckptSvc3 := &service_def.CheckpointsService{}
		capiSvc3 := &service_def.CAPIService{}
		remoteClusterSvc3 := &service_def.RemoteClusterSvc{}
		replSpecSvc3 := &service_def.ReplicationSpecSvc{}
		xdcrTopologySvc3 := &service_def.XDCRCompTopologySvc{}
		throughSeqnoTrackerSvc3 := &service_def.ThroughSeqnoTrackerSvc{}
		utils3 := &utilities.UtilsIface{}
		statsMgr3 := &service_def.StatsMgrIface{}
		uiLogSvc3 := &service_def.UILogSvc{}
		collectionsManifestSvc3 := &service_def.CollectionsManifestSvc{}
		backfillReplSvc3 := &service_def.BackfillReplSvc{}
		backfillMgrIface3 := &service_def.BackfillMgrIface{}
		getBackfillMgr3 := func() service_def_real.BackfillMgrIface {
			return backfillMgrIface3
		}
		bucketTopologySvc3 := &service_def.BucketTopologySvc{}

		setupMock(ckptSvc3, capiSvc3, remoteClusterSvc3, replSpecSvc3, xdcrTopologySvc3, throughSeqnoTrackerSvc3, utils3, statsMgr3, uiLogSvc3, collectionsManifestSvc3, backfillReplSvc3, backfillMgrIface3, bucketTopologySvc3, spec, pipelineSupervisor, throughSeqnoMap, upsertCkptDoneErr, targetMCMap, targetMCDelayMap, targetMCResult, nil, nil, nil, nil)

		ckptMgr3, err := NewCheckpointManager(ckptSvc3, capiSvc3, remoteClusterSvc3, replSpecSvc3, xdcrTopologySvc3,
			throughSeqnoTrackerSvc3, activeVBs, "", "", "", targetKvVbMap,
			targetRef, nil, utils3, statsMgr3, uiLogSvc3, collectionsManifestSvc3, backfillReplSvc3,
			getBackfillMgr3, bucketTopologySvc3, true)

		assert.Nil(err)
		assert.NotNil(ckptMgr3)
		ckptMgr3.unitTest = true

		globalTimestamp3 := make(metadata.GlobalTimestamp)

		ckpt_obj3 := &checkpointRecordWithLock{
			ckpt: &metadata.CheckpointRecord{
				SourceVBTimestamp: metadata.SourceVBTimestamp{
					Seqno: 100,
				},
				GlobalTimestamp: globalTimestamp3,
			},
			lock: &sync.RWMutex{},
		}
		ckptMgr3.cur_ckpts[0] = ckpt_obj3

		high_seqno_and_vbuuid_map := make(map[uint16][]uint64)
		high_seqno_and_vbuuid_map[0] = []uint64{200, 12345}
		srcManifestIds := make(map[uint16]uint64)
		tgtManifestIds := make(map[uint16]uint64)

		_, err = ckptMgr3.doCheckpoint(0, 100, high_seqno_and_vbuuid_map, srcManifestIds, tgtManifestIds)

		assert.NotNil(err)
		assert.Contains(err.Error(), "ValidateTargetOpaque: GlobalTimestamp is empty")
	})
}
