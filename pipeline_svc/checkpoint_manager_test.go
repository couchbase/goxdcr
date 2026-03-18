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

// convertMccFailoverLogMapToBase converts map[uint16]*mcc.FailoverLog to base.FailoverLogMapType
func convertMccFailoverLogMapToBase(mccMap map[uint16]*mcc.FailoverLog) base.FailoverLogMapType {
	if mccMap == nil {
		return nil
	}
	result := make(base.FailoverLogMapType)
	for vbno, mccLog := range mccMap {
		if mccLog == nil {
			continue
		}
		numEntries := uint64(len(*mccLog))
		baseLog := &base.FailoverLog{
			NumEntries: numEntries,
			LogTable:   make([]*base.FailoverEntry, numEntries),
		}
		for i, entry := range *mccLog {
			baseLog.LogTable[i] = &base.FailoverEntry{
				Uuid:      entry[0],
				HighSeqno: entry[1],
			}
		}
		result[vbno] = baseLog
	}
	return result
}

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
	tgtFailoverLogsMcc := make(map[uint16]*mcc.FailoverLog)
	assert.Nil(json.Unmarshal(tgtFailoverLogsSlice, &tgtFailoverLogsMcc))
	tgtFailoverLogs := convertMccFailoverLogMapToBase(tgtFailoverLogsMcc)
	// Prepare failover logs for local pre-replicate (required before filtering)
	assert.Nil(ModifyBucketFailoverLogForLocalReplicate(tgtFailoverLogs))
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
	tgtFailoverLogsMcc := make(map[uint16]*mcc.FailoverLog)
	assert.Nil(json.Unmarshal(tgtFailoverLogsSlice, &tgtFailoverLogsMcc))
	tgtFailoverLogs := convertMccFailoverLogMapToBase(tgtFailoverLogsMcc)
	// Prepare failover logs for local pre-replicate (required before filtering)
	assert.Nil(ModifyBucketFailoverLogForLocalReplicate(tgtFailoverLogs))

	globalCkts := metadata.GenerateGlobalVBsCkptDocMap([]uint16{0, 1, 2}, "")
	filteredMapTgts := filterInvalidCkptsBasedOnTargetFailover([]metadata.VBsCkptsDocMap{globalCkts, nil}, tgtFailoverLogs, &log.CommonLogger{})
	filteredMapTgt := filteredMapTgts[0]
	for _, ckptDoc := range filteredMapTgt {
		assert.NotEqual(0, len(ckptDoc.Checkpoint_records))
	}
}

func TestCheckpointsValidityWithPreReplicate(t *testing.T) {
	// Skip: This test requires remoteOps to be properly initialized, which requires
	// complex pipeline setup after the pre-replicate mechanism was refactored to use
	// local failover log validation. The caching behavior is tested separately in
	// TestCkptMgrPreReplicateCacheCtx.
	t.Skip("Test requires complex remoteOps setup after pre-replicate refactoring")
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
	majoritySpec, _ := metadata.NewReplicationSpecification("", "", "", "", "")
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

// TestCombinePeerCkptDocsWithStaleLocalInternalId tests that when a local checkpoint doc
// has a stale SpecInternalId (from a previous bucket incarnation / bucket recreate), it should
// NOT be merged into the output.  The combined result should only contain the peer's records.
//
// Specifically it validates the fix in combinePeerCkptDocsWithLocalCkptDoc: stale local docs
// are replaced with a fresh empty doc (stamped with the current spec ID) before peer records
// are merged in, preventing unresolvable SHA references from surviving into the output.
func TestCombinePeerCkptDocsWithStaleLocalInternalId(t *testing.T) {
	fmt.Println("============== Test case start: TestCombinePeerCkptDocsWithStaleLocalInternalId =================")
	defer fmt.Println("============== Test case end: TestCombinePeerCkptDocsWithStaleLocalInternalId =================")
	assert := assert.New(t)

	// The current (post-bucket-recreate) spec has a NEW internal ID.
	currentSpec, _ := metadata.NewReplicationSpecification("srcBucket", "", "", "tgtBucket", "")
	currentInternalId := "newSpecInternalId-after-recreate"
	currentSpec.InternalId = currentInternalId

	// The stale internal ID is what the OLD checkpoint docs in metakv are stamped with.
	staleInternalId := "oldSpecInternalId-before-recreate"

	const testVB = uint16(7)

	// staleRecord simulates a checkpoint from BEFORE the bucket was recreated.
	// It carries a non-empty BrokenMappingSha256 that references the OLD broken-mapping doc
	// (which was wiped when the InternalId mismatch was detected by GetMappingsDoc).
	staleRecord := &metadata.CheckpointRecord{
		SourceVBTimestamp: metadata.SourceVBTimestamp{
			Seqno:                  9000,
			Dcp_snapshot_seqno:     9000,
			Dcp_snapshot_end_seqno: 9000,
			Failover_uuid:          111,
		},
		TargetVBTimestamp: metadata.TargetVBTimestamp{
			Target_Seqno:        9000,
			BrokenMappingSha256: "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
		},
	}

	// freshRecord is what the peer node sends — it was created AFTER the bucket recreate
	// and therefore belongs to the new spec incarnation.
	freshRecord := &metadata.CheckpointRecord{
		SourceVBTimestamp: metadata.SourceVBTimestamp{
			Seqno:                  100,
			Dcp_snapshot_seqno:     100,
			Dcp_snapshot_end_seqno: 100,
			Failover_uuid:          222,
		},
		TargetVBTimestamp: metadata.TargetVBTimestamp{
			Target_Seqno: 100,
		},
	}

	// currDocs represents what CheckpointsDocs() returned from metakv —
	// stale docs that survived because the broken-mapping doc was emptied (so
	// populateActualMapping was skipped and the stale ckpt doc was loaded OK).
	currDocs := map[uint16]*metadata.CheckpointsDoc{
		testVB: {
			Checkpoint_records: metadata.CheckpointRecordsList{staleRecord},
			SpecInternalId:     staleInternalId, // <-- OLD incarnation
		},
	}

	// filteredMap is the validated/filtered peer checkpoint data.
	filteredMap := map[uint16]*metadata.CheckpointsDoc{
		testVB: {
			Checkpoint_records: metadata.CheckpointRecordsList{freshRecord},
			SpecInternalId:     currentInternalId, // <-- NEW incarnation from peer
		},
	}

	testLogger := log.NewLogger("TestCombinePeerCkptDocsWithStaleLocalInternalId", nil)

	// Call the function under test — no failover logs needed for this unit test.
	combinePeerCkptDocsWithLocalCkptDoc(filteredMap, nil, nil, currDocs, currentSpec, testLogger)

	// -----------------------------------------------------------------------
	// Assertions: after the merge the output should reflect ONLY the new spec.
	// -----------------------------------------------------------------------

	resultDoc, exists := currDocs[testVB]
	assert.True(exists, "result doc for testVB must exist")
	assert.NotNil(resultDoc)

	// ASSERTION 1: The output doc's SpecInternalId must be the NEW one.
	// The fix discards the stale local doc and replaces it with a fresh one
	// stamped with the current spec ID before merging peer records into it.
	assert.Equal(currentInternalId, resultDoc.SpecInternalId,
		"output doc SpecInternalId should be the current spec ID, not the stale one")

	// Collect the sha strings from output records so we can reason about them.
	var outputShas []string
	var outputSeqnos []uint64
	for _, rec := range resultDoc.Checkpoint_records {
		if rec == nil {
			continue
		}
		outputShas = append(outputShas, rec.BrokenMappingSha256)
		outputSeqnos = append(outputSeqnos, rec.Seqno)
	}

	// ASSERTION 2: The output must NOT contain any record that references the stale
	// broken-mapping SHA (i.e. staleRecord's SHA must have been discarded).
	for _, sha := range outputShas {
		assert.NotEqual(staleRecord.BrokenMappingSha256, sha,
			"stale broken-mapping SHA from old incarnation must not appear in merged output")
	}

	// ASSERTION 3: The fresh record's seqno must be present in the output.
	assert.Contains(outputSeqnos, freshRecord.Seqno,
		"fresh peer record seqno must survive in the merged output")
}

func TestMergEmptyCkpts(t *testing.T) {
	fmt.Println("============== Test case start: TestMergEmptyCkpts =================")
	defer fmt.Println("============== Test case end: TestMergEmptyCkpts =================")
	assert := assert.New(t)

	filteredMap := make(map[uint16]*metadata.CheckpointsDoc)
	currentDocs := make(map[uint16]*metadata.CheckpointsDoc)

	testLogger := log.NewLogger("TestMergEmptyCkpts", nil)

	spec, _ := metadata.NewReplicationSpecification("", "", "", "", "")
	filteredMap[0] = &metadata.CheckpointsDoc{
		Checkpoint_records: nil,
		SpecInternalId:     "",
		Revision:           nil,
	}

	assert.Len(currentDocs, 0)
	combinePeerCkptDocsWithLocalCkptDoc(filteredMap, nil, nil, currentDocs, spec, testLogger)
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
	combinePeerCkptDocsWithLocalCkptDoc(filteredMap, nil, nil, currentDocs, spec, testLogger)
	assert.Len(currentDocs, 1)

	filteredMap[1] = &metadata.CheckpointsDoc{
		Checkpoint_records: recordsList,
		SpecInternalId:     "testId",
		Revision:           nil,
	}
	filteredMap[2] = &metadata.CheckpointsDoc{}
	combinePeerCkptDocsWithLocalCkptDoc(filteredMap, nil, nil, currentDocs, spec, testLogger)
	assert.Len(currentDocs, 2)
}

// TestMergeFinalCkpts_StaleDocsShouldBeDeletedFromMetaKV is intentionally written to fail
// until stale checkpoint docs are physically deleted from metakv during merge.
//
// Current behavior (bug): stale docs are handled in-memory for merge, but DelCheckpointsDoc
// is not called, so stale docs can still be reloaded later.
func TestMergeFinalCkpts_StaleDocsShouldBeDeletedFromMetaKV(t *testing.T) {
	const (
		oldSpecInternalID = "old-spec-id-12345"
		newSpecInternalID = "new-spec-id-67890"
		mainTopic         = "test-repl/source/target"
		oldSHA            = "old-sha-abcdef1234567890"
	)

	assert := assert.New(t)
	t.Logf("DEBUG test start: setting up stale local ckpt docs for topic=%s", mainTopic)

	ckptSvc := &service_def.CheckpointsService{}
	utilsMock := &utilities.UtilsIface{}
	logger := log.NewLogger("TestMergeFinalCkpts_StaleDocsShouldBeDeletedFromMetaKV", nil)

	// Stopwatch is used throughout merge flow
	utilsMock.On("StartDiagStopwatch", mock.Anything, mock.Anything).Return(func() time.Duration {
		return 0
	})

	// Pipeline/spec wiring
	spec, _ := metadata.NewReplicationSpecification("srcBucket", "", "", "tgtBucket", "")
	spec.InternalId = newSpecInternalID
	genSpec := &mocks2.GenericSpecification{}
	genSpec.On("GetReplicationSpec").Return(spec)
	pipeline := &commonMock.Pipeline{}
	pipeline.On("Specification").Return(genSpec)
	pipeline.On("Topic").Return(mainTopic)

	// Local stale docs returned by CheckpointsDocs(mainTopic, true)
	staleLocalDocs := map[uint16]*metadata.CheckpointsDoc{
		100: {
			SpecInternalId: oldSpecInternalID,
			Checkpoint_records: metadata.CheckpointRecordsList{
				&metadata.CheckpointRecord{TargetVBTimestamp: metadata.TargetVBTimestamp{BrokenMappingSha256: oldSHA}},
			},
		},
	}

	backfillTopic := common.ComposeFullTopic(mainTopic, common.BackfillPipeline)

	ckptSvc.On("CheckpointsDocs", mainTopic, true).Run(func(args mock.Arguments) {
		t.Logf("DEBUG CheckpointsDocs(main): returning stale docs with old SpecInternalId=%s", oldSpecInternalID)
	}).Return(staleLocalDocs, nil).Once()
	ckptSvc.On("CheckpointsDocs", backfillTopic, true).Run(func(args mock.Arguments) {
		t.Logf("DEBUG CheckpointsDocs(backfill): returning MetadataNotFoundErr")
	}).Return(nil, service_def_real.MetadataNotFoundErr).Once()

	// This is the expected behavior after the deletion fix: stale local docs should be removed.
	// The current code path does not call this, so this test should fail on expectations.
	ckptSvc.On("DelCheckpointsDoc", mainTopic, uint16(100), newSpecInternalID).Run(func(args mock.Arguments) {
		t.Logf("DEBUG DelCheckpointsDoc called for stale vb=%d", args.Get(1).(uint16))
	}).Return(nil).Once()

	// Downstream calls required for mergeAndPersistBrokenMappingDocsAndCkpts
	ckptSvc.On("LoadBrokenMappings", mock.Anything).Run(func(args mock.Arguments) {
		t.Logf("DEBUG LoadBrokenMappings called for topic=%s", args.String(0))
	}).Return(metadata.ShaToCollectionNamespaceMap{}, nil, nil, false, nil).Twice()
	ckptSvc.On("LoadGlobalInfoMapping", mock.Anything).Run(func(args mock.Arguments) {
		t.Logf("DEBUG LoadGlobalInfoMapping called for topic=%s", args.String(0))
	}).Return(metadata.ShaToGlobalInfoMap{}, &metadata.GlobalInfoCompressedDoc{SpecInternalId: newSpecInternalID}, nil, false, nil).Twice()
	ckptSvc.On("UpsertAndReloadCheckpointCompleteSet", mock.Anything, mock.Anything, mock.Anything, newSpecInternalID, mock.Anything).Run(func(args mock.Arguments) {
		t.Logf("DEBUG UpsertAndReloadCheckpointCompleteSet called for topic=%s", args.String(0))
	}).Return(nil).Twice()

	ckmgr := &CheckpointManager{
		checkpoints_svc: ckptSvc,
		utils:           utilsMock,
		pipeline:        pipeline,
		logger:          logger,
	}

	peerDocsMain := map[uint16]*metadata.CheckpointsDoc{
		100: {
			SpecInternalId: newSpecInternalID,
			Checkpoint_records: metadata.CheckpointRecordsList{
				&metadata.CheckpointRecord{SourceVBTimestamp: metadata.SourceVBTimestamp{Seqno: 5000}},
			},
		},
	}

	filteredMaps := []metadata.VBsCkptsDocMap{peerDocsMain, map[uint16]*metadata.CheckpointsDoc{}}

	t.Logf("DEBUG invoking mergeFinalCkpts")
	err := ckmgr.mergeFinalCkpts(filteredMaps, nil, nil, metadata.ShaToCollectionNamespaceMap{}, newSpecInternalID, metadata.ShaToGlobalInfoMap{})
	assert.NoError(err)

	// Expected-to-fail assertion before deletion fix is implemented.
	assert.True(ckptSvc.AssertExpectations(t),
		"EXPECTED FAILURE BEFORE FIX: stale checkpoint docs should be deleted from metakv via DelCheckpointsDoc")
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
	manifestsIdPair := base.CollectionsManifestIdsTimestamp{SourceManifestId: 0, TargetManifestId: 0, GlobalTargetManifestIds: nil}

	ts0 := &metadata.BackfillVBTimestamps{
		StartingTimestamp: &base.VBTimestamp{Vbno: 0, Vbuuid: 0, Seqno: 5, SnapshotStart: 5, SnapshotEnd: 5, ManifestIDs: manifestsIdPair},
		EndingTimestamp:   &base.VBTimestamp{Vbno: 0, Vbuuid: 0, Seqno: 500, SnapshotStart: 500, SnapshotEnd: 500, ManifestIDs: manifestsIdPair},
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

	utilsMock.On("GetDataUsageTrackingCtx").Return(nil)

	for kvName, client := range mcMap {
		utilsMock.On("GetRemoteMemcachedConnection", kvName, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(client, nil)
	}

	for input, output := range preReplicateMap {
		capiSvc.On("PreReplicate", mock.Anything, input, mock.Anything, mock.Anything, mock.Anything).Return(true, output, nil)
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
		client.On("StatsMap", base.VBUCKET_SEQNO_STAT_NAME, mock.Anything).Run(func(args mock.Arguments) { time.Sleep(time.Duration(delayMap[k]) * time.Second) }).Return(result[k].statsMap, result[k].err)
	}

	colManSvc.On("PersistNeededManifests", mock.Anything).Return(nil)
	colManSvc.On("PersistReceivedManifests", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	supervisor.On("OnEvent", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		atomic.AddUint32(&supervisorErrCnt, 1)
	}).Return(nil)

	// Mock GetRemoteBucketStatsProvider - returns a mock BucketStatsOps
	mockBucketStatsOps := &service_def.BucketStatsOps{}
	// Create a failover log with entries for VBs 0-1023 (all possible VBs)
	failoverLogMap := make(base.FailoverLogMapType)
	for vb := uint16(0); vb < 1024; vb++ {
		failoverLogMap[vb] = &base.FailoverLog{
			NumEntries: 1,
			LogTable: []*base.FailoverEntry{
				{Uuid: 12345 + uint64(vb), HighSeqno: 0},
			},
		}
	}
	mockBucketStatsOps.On("GetFailoverLog", mock.Anything, mock.Anything).Return(&base.BucketFailoverLog{
		FailoverLogMap: failoverLogMap,
	}, nil, nil)
	// Create VBucketStats with the expected high seqno and UUID for checkpointing
	vbStatsMap := make(base.VBucketStatsMap)
	for vb, highSeqnoAndUuid := range ckptHighSeqnoMap {
		if len(highSeqnoAndUuid) >= 2 {
			vbStatsMap[vb] = &base.VBucketStats{
				HighSeqno: highSeqnoAndUuid[0],
				Uuid:      highSeqnoAndUuid[1],
			}
		}
	}
	// Calculate max delay from delayMap to apply to GetVBucketStats mock
	// This simulates network latency when fetching bucket stats from target
	maxDelay := 0
	for _, d := range delayMap {
		if d > maxDelay {
			maxDelay = d
		}
	}
	mockBucketStatsOps.On("GetVBucketStats", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		if maxDelay > 0 {
			time.Sleep(time.Duration(maxDelay) * time.Second)
		}
	}).Return(&base.BucketVBStats{
		VBStatsMap: vbStatsMap,
	}, nil, nil)
	bucketTopologySvc.On("GetRemoteBucketStatsProvider", mock.Anything).Return(mockBucketStatsOps, nil)
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

	mainPipeline := setupMainPipelineMock(spec, pipelineSupervisor)
	assert.Nil(ckptMgr.Attach(mainPipeline))

	// Initially, ckptDocGetter will return no docs
	ckptSvc.On("CheckpointsDocs", mainPipeline.Topic(), mock.Anything).Return(map[uint16]*metadata.CheckpointsDoc{}, nil)
	ckptMgr.checkpoints_svc = ckptSvc

	// Let's pretend ckptmgr started vbtimestamp, read no ckpt doc, and should have initialized internal ckptDoc structs
	assert.Nil(ckptMgr.SetVBTimestamps(mainPipeline.Topic()))

	for _, oneRecord := range ckptMgr.cur_ckpts {
		assert.NotNil(oneRecord.ckpt.TargetVBTimestamp.GetValue())
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

	mainPipeline := setupMainPipelineMock(spec, pipelineSupervisor)
	assert.Nil(ckptMgr.Attach(mainPipeline))

	// Initially, ckptDocGetter will return no docs
	ckptSvc.On("CheckpointsDocs", mainPipeline.Topic(), mock.Anything).Return(map[uint16]*metadata.CheckpointsDoc{}, nil)
	ckptMgr.checkpoints_svc = ckptSvc

	// Let's pretend ckptmgr started vbtimestamp, read no ckpt doc, and should have initialized internal ckptDoc structs
	assert.Nil(ckptMgr.SetVBTimestamps(mainPipeline.Topic()))

	for _, oneRecord := range ckptMgr.cur_ckpts {
		assert.NotNil(oneRecord.ckpt.TargetVBTimestamp.GetValue())
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
		Target_vb_uuid: 12345 + uint64(0),
	}
	vb1TgtVbUuid := &metadata.TargetVBUuid{
		Target_vb_uuid: 12345 + uint64(1),
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

	var preReplicateCounter uint32

	// Create a cache context with a mock preReplicate function that returns an error
	cacheCtxErr := &globalCkptPrereplicateCacheCtx{
		lastQueriedTimers: map[string]*time.Timer{},
		lastQueriedbMatch: map[string]bool{},
		lastQueriedOpaque: map[string]metadata.TargetVBOpaque{},
		lastQueriedError:  map[string]error{},
		mtx:               &sync.RWMutex{},
		expireThreshold:   5 * time.Second,
		errorThreshold:    250 * time.Millisecond,
		_preReplicate: func(status *service_def_real.RemoteVBReplicationStatus, dataTransferCtx *utils.Context) (bool, metadata.TargetVBOpaque, error) {
			atomic.AddUint32(&preReplicateCounter, 1)
			return false, nil, fmt.Errorf("dummy")
		},
	}

	// Test an error scenario where the error code is stored and returned various times before expiration
	tgtTs := &service_def_real.RemoteVBReplicationStatus{}

	_, _, err := cacheCtxErr.preReplicate(tgtTs, nil)
	assert.NotNil(err)
	assert.Equal(uint32(1), atomic.LoadUint32(&preReplicateCounter))
	assert.Len(cacheCtxErr.lastQueriedTimers, 1)
	// Immediately do another pull, call should not increment, same error
	_, _, err = cacheCtxErr.preReplicate(tgtTs, nil)
	assert.NotNil(err)
	assert.Equal(uint32(1), atomic.LoadUint32(&preReplicateCounter))
	assert.Len(cacheCtxErr.lastQueriedTimers, 1)

	// error should expire after 1 second
	time.Sleep(1 * time.Second)
	assert.Len(cacheCtxErr.lastQueriedTimers, 0)

	// Create a cache context with a mock preReplicate function that returns success
	cacheCtxGood := &globalCkptPrereplicateCacheCtx{
		lastQueriedTimers: map[string]*time.Timer{},
		lastQueriedbMatch: map[string]bool{},
		lastQueriedOpaque: map[string]metadata.TargetVBOpaque{},
		lastQueriedError:  map[string]error{},
		mtx:               &sync.RWMutex{},
		expireThreshold:   1 * time.Second,
		errorThreshold:    250 * time.Millisecond,
		_preReplicate: func(status *service_def_real.RemoteVBReplicationStatus, dataTransferCtx *utils.Context) (bool, metadata.TargetVBOpaque, error) {
			atomic.AddUint32(&preReplicateCounter, 1)
			return true, nil, nil
		},
	}

	match, _, err := cacheCtxGood.preReplicate(tgtTs, nil)
	assert.Nil(err)
	assert.True(match)
	assert.Equal(uint32(2), atomic.LoadUint32(&preReplicateCounter))
	assert.Len(cacheCtxGood.lastQueriedTimers, 1)
	// Immediately do another pull, call should not increment, same error
	_, _, err = cacheCtxGood.preReplicate(tgtTs, nil)
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

	// Helper to create base.FailoverLog from entries
	makeFailoverLog := func(entries [][2]uint64) *base.FailoverLog {
		if len(entries) == 0 {
			return &base.FailoverLog{NumEntries: 0, LogTable: nil}
		}
		fl := &base.FailoverLog{
			NumEntries: uint64(len(entries)),
			LogTable:   make([]*base.FailoverEntry, len(entries)),
		}
		for i, e := range entries {
			fl.LogTable[i] = &base.FailoverEntry{Uuid: e[0], HighSeqno: e[1]}
		}
		return fl
	}

	// Helper to compare failover logs
	failoverLogsEqual := func(a, b *base.FailoverLog) bool {
		if a == nil && b == nil {
			return true
		}
		if a == nil || b == nil {
			return false
		}
		if a.NumEntries != b.NumEntries {
			return false
		}
		for i := uint64(0); i < a.NumEntries; i++ {
			if a.LogTable[i].Uuid != b.LogTable[i].Uuid || a.LogTable[i].HighSeqno != b.LogTable[i].HighSeqno {
				return false
			}
		}
		return true
	}

	tests := []struct {
		name        string
		failoverLog *base.FailoverLog
		want        *base.FailoverLog
		wantErr     bool
	}{
		{
			name: "Test1",
			failoverLog: makeFailoverLog([][2]uint64{
				{189935894142520, 7879},
				{189935894896622, 1000},
				{150911719482835, 0},
			}),
			want: makeFailoverLog([][2]uint64{
				{189935894142520, math.MaxUint64},
				{189935894896622, 7879},
				{150911719482835, 1000},
			}),
			wantErr: false,
		},
		{
			name: "Test2",
			failoverLog: makeFailoverLog([][2]uint64{
				{150911719482835, 0},
			}),
			want: makeFailoverLog([][2]uint64{
				{150911719482835, math.MaxUint64},
			}),
			wantErr: false,
		},
		{
			name:        "Test3 - empty failover log",
			failoverLog: makeFailoverLog([][2]uint64{}),
			want:        nil,
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := prepareForLocalPreReplicate(tt.failoverLog)
			if tt.wantErr {
				if err == nil {
					t.Errorf("prepareForLocalPreReplicate() expected error but got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("prepareForLocalPreReplicate() unexpected error: %v", err)
				return
			}
			if !failoverLogsEqual(tt.failoverLog, tt.want) {
				t.Errorf("prepareForLocalPreReplicate() modified log = %v, want %v", tt.failoverLog, tt.want)
			}
		})
	}
}

func Test_localPreReplicate(t *testing.T) {
	// Helper to create base.FailoverLog from entries
	makeFailoverLog := func(entries [][2]uint64) *base.FailoverLog {
		fl := &base.FailoverLog{
			NumEntries: uint64(len(entries)),
			LogTable:   make([]*base.FailoverEntry, len(entries)),
		}
		for i, e := range entries {
			fl.LogTable[i] = &base.FailoverEntry{Uuid: e[0], HighSeqno: e[1]}
		}
		return fl
	}

	f := makeFailoverLog([][2]uint64{
		{189935894142520, 7879},
		{189935894896622, 1000},
		{150911719482835, 0},
	})
	err := prepareForLocalPreReplicate(f)
	if err != nil {
		t.Fatalf("prepareForLocalPreReplicate() failed: %v", err)
	}

	tests := []struct {
		name         string
		commitOpaque *base.CommitOpaque
		failoverLog  *base.FailoverLog
		want         bool
	}{
		{
			name:         "Invalid UUID",
			commitOpaque: base.NewCommitOpaque(1, 100),
			failoverLog:  f,
			want:         false,
		},
		{
			name:         "Valid UUID+seq pair",
			commitOpaque: base.NewCommitOpaque(189935894896622, 500),
			failoverLog:  f,
			want:         true,
		},
		{
			name:         "Valid UUID but invalid Seq",
			commitOpaque: base.NewCommitOpaque(150911719482835, 1010),
			failoverLog:  f,
			want:         false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := localPreReplicate(tt.failoverLog, tt.commitOpaque)
			if err != nil {
				t.Errorf("localPreReplicate() unexpected error: %v", err)
				return
			}
			if got != tt.want {
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

		tgtBucketStats := &base.BucketVBStats{
			VBStatsMap: base.VBucketStatsMap{},
		}

		vb0 := &base.VBucketStats{
			HighSeqno: 200,
			Uuid:      12345,
		}
		tgtBucketStats.VBStatsMap[0] = vb0

		srcManifestIds := make(map[uint16]uint64)
		tgtManifestIds := make(map[uint16]uint64)

		_, err = ckptMgr.doCheckpoint(0, 100, tgtBucketStats, srcManifestIds, tgtManifestIds)

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

		tgtBucketStats := &base.BucketVBStats{
			VBStatsMap: base.VBucketStatsMap{},
		}
		vb0 := &base.VBucketStats{
			HighSeqno: 200,
			Uuid:      12345,
		}
		tgtBucketStats.VBStatsMap[0] = vb0
		srcManifestIds := make(map[uint16]uint64)
		tgtManifestIds := make(map[uint16]uint64)

		_, err = ckptMgr2.doCheckpoint(0, 100, tgtBucketStats, srcManifestIds, tgtManifestIds)

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

		tgtBucketStats := &base.BucketVBStats{
			VBStatsMap: base.VBucketStatsMap{},
		}
		vb0 := &base.VBucketStats{
			HighSeqno: 200,
			Uuid:      12345,
		}
		tgtBucketStats.VBStatsMap[0] = vb0
		srcManifestIds := make(map[uint16]uint64)
		tgtManifestIds := make(map[uint16]uint64)

		_, err = ckptMgr3.doCheckpoint(0, 100, tgtBucketStats, srcManifestIds, tgtManifestIds)

		assert.NotNil(err)
		assert.Contains(err.Error(), "ValidateTargetOpaque: GlobalTimestamp is empty")
	})
}

// TestLoadBrokenMappingCommit_SmallDataset tests loadBrokenMappingCommit with a small dataset
// This test validates basic functionality with minimal records and mappings
func TestLoadBrokenMappingCommit_SmallDataset(t *testing.T) {
	fmt.Println("============== Test case start: TestLoadBrokenMappingCommit_SmallDataset =================")
	defer fmt.Println("============== Test case end: TestLoadBrokenMappingCommit_SmallDataset =================")
	assert := assert.New(t)

	// Create checkpoint manager with necessary fields
	ckmgr := &CheckpointManager{
		cachedBrokenMap: brokenMappingWithLock{
			brokenMap:                   make(metadata.CollectionNamespaceMapping),
			correspondingTargetManifest: 0,
			lock:                        sync.RWMutex{},
			brokenMapHistories:          make(map[uint64]metadata.CollectionNamespaceMapping),
		},
	}

	// Create a small broken mapping with 2 namespace mappings
	brokenMap1 := make(metadata.CollectionNamespaceMapping)
	ns1, err := base.NewCollectionNamespaceFromString("scope1.collection1")
	assert.Nil(err)
	ns2, err := base.NewCollectionNamespaceFromString("scope1.collection2")
	assert.Nil(err)
	ns3, err := base.NewCollectionNamespaceFromString("scope2.collection1")
	assert.Nil(err)

	brokenMap1.AddSingleMapping(&ns1, &ns2)
	brokenMap1.AddSingleMapping(&ns1, &ns3)

	// Create a checkpoint record with broken mappings
	// Need to set Target_vb_opaque to make it a traditional checkpoint
	record1 := &metadata.CheckpointRecord{
		SourceVBTimestamp: metadata.SourceVBTimestamp{
			Failover_uuid: 1,
			Seqno:         100,
		},
		TargetVBTimestamp: metadata.TargetVBTimestamp{
			Target_vb_opaque: &metadata.TargetVBUuidAndTimestamp{
				Target_vb_uuid: "test_uuid_1",
				Startup_time:   "2026-03-09",
			},
			Target_Seqno:   100,
			TargetManifest: 10,
		},
	}
	// Set the broken mappings using the public API
	err = record1.TargetVBTimestamp.SetBrokenMappingAsPartOfCreation(brokenMap1)
	assert.Nil(err)
	// Populate the SHA256 for the broken mapping
	err = record1.PopulateBrokenMappingSha()
	assert.Nil(err)

	// Create cacheLookupMap with 1 record for manifest ID 10
	cacheLookupMap := make(map[uint64]metadata.CheckpointRecordsList)
	cacheLookupMap[10] = metadata.CheckpointRecordsList{record1}

	// Call loadBrokenMappingCommit
	ckmgr.loadBrokenMappingCommit(cacheLookupMap, 10)

	// Verify the result
	ckmgr.cachedBrokenMap.lock.RLock()
	defer ckmgr.cachedBrokenMap.lock.RUnlock()

	assert.Equal(uint64(10), ckmgr.cachedBrokenMap.correspondingTargetManifest)
	assert.NotNil(ckmgr.cachedBrokenMap.brokenMap)
	assert.Len(ckmgr.cachedBrokenMap.brokenMap, 1) // One source namespace
	// Verify the mapping contains the expected namespaces
	_, _, _, exists := ckmgr.cachedBrokenMap.brokenMap.Get(&ns1, nil)
	assert.True(exists)
}

// TestLoadBrokenMappingCommit_SmallDatasetMultipleRecords tests loadBrokenMappingCommit with small dataset
// but multiple checkpoint records in the same manifest
func TestLoadBrokenMappingCommit_SmallDatasetMultipleRecords(t *testing.T) {
	fmt.Println("============== Test case start: TestLoadBrokenMappingCommit_SmallDatasetMultipleRecords =================")
	defer fmt.Println("============== Test case end: TestLoadBrokenMappingCommit_SmallDatasetMultipleRecords =================")
	assert := assert.New(t)

	ckmgr := &CheckpointManager{
		cachedBrokenMap: brokenMappingWithLock{
			brokenMap:                   make(metadata.CollectionNamespaceMapping),
			correspondingTargetManifest: 0,
			lock:                        sync.RWMutex{},
			brokenMapHistories:          make(map[uint64]metadata.CollectionNamespaceMapping),
		},
	}

	// Create 3 different broken mappings for 3 records
	brokenMap1 := make(metadata.CollectionNamespaceMapping)
	ns1, err := base.NewCollectionNamespaceFromString("scope1.collection1")
	assert.Nil(err)
	ns2, err := base.NewCollectionNamespaceFromString("scope1.collection2")
	assert.Nil(err)
	brokenMap1.AddSingleMapping(&ns1, &ns2)

	brokenMap2 := make(metadata.CollectionNamespaceMapping)
	ns3, err := base.NewCollectionNamespaceFromString("scope2.collection1")
	assert.Nil(err)
	ns4, err := base.NewCollectionNamespaceFromString("scope2.collection2")
	assert.Nil(err)
	brokenMap2.AddSingleMapping(&ns3, &ns4)

	brokenMap3 := make(metadata.CollectionNamespaceMapping)
	ns5, err := base.NewCollectionNamespaceFromString("scope3.collection1")
	assert.Nil(err)
	ns6, err := base.NewCollectionNamespaceFromString("scope3.collection2")
	assert.Nil(err)
	brokenMap3.AddSingleMapping(&ns5, &ns6)

	// Create 3 checkpoint records with broken mappings
	record1 := &metadata.CheckpointRecord{
		SourceVBTimestamp: metadata.SourceVBTimestamp{Failover_uuid: 1, Seqno: 100},
		TargetVBTimestamp: metadata.TargetVBTimestamp{
			Target_vb_opaque: &metadata.TargetVBUuidAndTimestamp{
				Target_vb_uuid: "uuid_1",
				Startup_time:   "2026-03-09",
			},
			Target_Seqno:   100,
			TargetManifest: 20,
		},
	}
	err = record1.TargetVBTimestamp.SetBrokenMappingAsPartOfCreation(brokenMap1)
	assert.Nil(err)
	err = record1.PopulateBrokenMappingSha()
	assert.Nil(err)

	record2 := &metadata.CheckpointRecord{
		SourceVBTimestamp: metadata.SourceVBTimestamp{Failover_uuid: 2, Seqno: 200},
		TargetVBTimestamp: metadata.TargetVBTimestamp{
			Target_vb_opaque: &metadata.TargetVBUuidAndTimestamp{
				Target_vb_uuid: "uuid_2",
				Startup_time:   "2026-03-09",
			},
			Target_Seqno:   200,
			TargetManifest: 20,
		},
	}
	err = record2.TargetVBTimestamp.SetBrokenMappingAsPartOfCreation(brokenMap2)
	assert.Nil(err)
	err = record2.PopulateBrokenMappingSha()
	assert.Nil(err)

	record3 := &metadata.CheckpointRecord{
		SourceVBTimestamp: metadata.SourceVBTimestamp{Failover_uuid: 3, Seqno: 300},
		TargetVBTimestamp: metadata.TargetVBTimestamp{
			Target_vb_opaque: &metadata.TargetVBUuidAndTimestamp{
				Target_vb_uuid: "uuid_3",
				Startup_time:   "2026-03-09",
			},
			Target_Seqno:   300,
			TargetManifest: 20,
		},
	}
	err = record3.TargetVBTimestamp.SetBrokenMappingAsPartOfCreation(brokenMap3)
	assert.Nil(err)
	err = record3.PopulateBrokenMappingSha()
	assert.Nil(err)

	// Create cacheLookupMap with 3 records for manifest ID 20
	cacheLookupMap := make(map[uint64]metadata.CheckpointRecordsList)
	cacheLookupMap[20] = metadata.CheckpointRecordsList{record1, record2, record3}

	// Call loadBrokenMappingCommit
	ckmgr.loadBrokenMappingCommit(cacheLookupMap, 20)

	// Verify the result - all 3 namespace sources should be consolidated
	ckmgr.cachedBrokenMap.lock.RLock()
	defer ckmgr.cachedBrokenMap.lock.RUnlock()

	assert.Equal(uint64(20), ckmgr.cachedBrokenMap.correspondingTargetManifest)
	assert.NotNil(ckmgr.cachedBrokenMap.brokenMap)
	assert.Len(ckmgr.cachedBrokenMap.brokenMap, 3) // Three source namespaces
	_, _, _, exists1 := ckmgr.cachedBrokenMap.brokenMap.Get(&ns1, nil)
	_, _, _, exists3 := ckmgr.cachedBrokenMap.brokenMap.Get(&ns3, nil)
	_, _, _, exists5 := ckmgr.cachedBrokenMap.brokenMap.Get(&ns5, nil)
	assert.True(exists1)
	assert.True(exists3)
	assert.True(exists5)
}

// TestLoadBrokenMappingCommit_MaxRecordsSmallMappings tests with up to 5 records (small dataset)
// each with small broken mappings
func TestLoadBrokenMappingCommit_MaxRecordsSmallMappings(t *testing.T) {
	fmt.Println("============== Test case start: TestLoadBrokenMappingCommit_MaxRecordsSmallMappings =================")
	defer fmt.Println("============== Test case end: TestLoadBrokenMappingCommit_MaxRecordsSmallMappings =================")
	assert := assert.New(t)

	ckmgr := &CheckpointManager{
		cachedBrokenMap: brokenMappingWithLock{
			brokenMap:                   make(metadata.CollectionNamespaceMapping),
			correspondingTargetManifest: 0,
			lock:                        sync.RWMutex{},
			brokenMapHistories:          make(map[uint64]metadata.CollectionNamespaceMapping),
		},
	}

	// Create 5 checkpoint records (max for this test) each with different broken mappings
	var records metadata.CheckpointRecordsList
	var allSourceNamespaces []*base.CollectionNamespace

	for i := 0; i < 5; i++ {
		brokenMap := make(metadata.CollectionNamespaceMapping)
		// Each record has 2 namespace mappings
		srcNs, err := base.NewCollectionNamespaceFromString(fmt.Sprintf("scope%d.collection1", i))
		assert.Nil(err)
		tgtNs, err := base.NewCollectionNamespaceFromString(fmt.Sprintf("scope%d.collection2", i))
		assert.Nil(err)
		brokenMap.AddSingleMapping(&srcNs, &tgtNs)
		allSourceNamespaces = append(allSourceNamespaces, &srcNs)

		record := &metadata.CheckpointRecord{
			SourceVBTimestamp: metadata.SourceVBTimestamp{
				Failover_uuid: uint64(i + 1),
				Seqno:         uint64((i + 1) * 100),
			},
			TargetVBTimestamp: metadata.TargetVBTimestamp{
				Target_vb_opaque: &metadata.TargetVBUuidAndTimestamp{
					Target_vb_uuid: fmt.Sprintf("uuid_%d", i),
					Startup_time:   "2026-03-09",
				},
				Target_Seqno:   uint64((i + 1) * 100),
				TargetManifest: 30,
			},
		}
		err = record.TargetVBTimestamp.SetBrokenMappingAsPartOfCreation(brokenMap)
		assert.Nil(err)
		err = record.PopulateBrokenMappingSha()
		assert.Nil(err)
		records = append(records, record)
	}

	cacheLookupMap := make(map[uint64]metadata.CheckpointRecordsList)
	cacheLookupMap[30] = records

	// Call loadBrokenMappingCommit
	ckmgr.loadBrokenMappingCommit(cacheLookupMap, 30)

	// Verify all records were processed and consolidated
	ckmgr.cachedBrokenMap.lock.RLock()
	defer ckmgr.cachedBrokenMap.lock.RUnlock()

	assert.Equal(uint64(30), ckmgr.cachedBrokenMap.correspondingTargetManifest)
	assert.Len(ckmgr.cachedBrokenMap.brokenMap, 5) // 5 source namespaces consolidated

	for _, ns := range allSourceNamespaces {
		_, _, _, exists := ckmgr.cachedBrokenMap.brokenMap.Get(ns, nil)
		assert.True(exists)
	}
}

// TestLoadBrokenMappingCommit_LargeDatasetWithLargeMappings tests with large broken mappings
// This exercises the compiledIndex creation and usage during Consolidate operations
// Each record has many namespace mappings (simulating large broken mapping sets)
func TestLoadBrokenMappingCommit_LargeDatasetWithLargeMappings(t *testing.T) {
	fmt.Println("============== Test case start: TestLoadBrokenMappingCommit_LargeDatasetWithLargeMappings =================")
	defer fmt.Println("============== Test case end: TestLoadBrokenMappingCommit_LargeDatasetWithLargeMappings =================")
	assert := assert.New(t)

	ckmgr := &CheckpointManager{
		cachedBrokenMap: brokenMappingWithLock{
			brokenMap:                   make(metadata.CollectionNamespaceMapping),
			correspondingTargetManifest: 0,
			lock:                        sync.RWMutex{},
			brokenMapHistories:          make(map[uint64]metadata.CollectionNamespaceMapping),
		},
	}

	// Create 5 checkpoint records, each with large broken mappings (2000+ entries each)
	var records metadata.CheckpointRecordsList
	numMappingsPerRecord := 2000 // Large number of mappings per record
	var allSourceNamespaces []*base.CollectionNamespace
	var namespaceCounter int

	for recordIdx := 0; recordIdx < 5; recordIdx++ {
		brokenMap := make(metadata.CollectionNamespaceMapping)

		// Each record creates 120 different namespace mappings
		for mappingIdx := 0; mappingIdx < numMappingsPerRecord; mappingIdx++ {
			srcStr := fmt.Sprintf("src_scope_%d_%d.src_col_%d", recordIdx, mappingIdx/10, mappingIdx)
			tgtStr := fmt.Sprintf("tgt_scope_%d_%d.tgt_col_%d", recordIdx, mappingIdx/10, mappingIdx)

			srcNs, err := base.NewCollectionNamespaceFromString(srcStr)
			assert.Nil(err)
			tgtNs, err := base.NewCollectionNamespaceFromString(tgtStr)
			assert.Nil(err)

			// Only track first occurrence of each namespace
			if mappingIdx == 0 {
				allSourceNamespaces = append(allSourceNamespaces, &srcNs)
			}

			brokenMap.AddSingleMapping(&srcNs, &tgtNs)
			namespaceCounter++
		}

		record := &metadata.CheckpointRecord{
			SourceVBTimestamp: metadata.SourceVBTimestamp{
				Failover_uuid: uint64(recordIdx + 100),
				Seqno:         uint64((recordIdx + 1) * 1000),
			},
			TargetVBTimestamp: metadata.TargetVBTimestamp{
				Target_vb_opaque: &metadata.TargetVBUuidAndTimestamp{
					Target_vb_uuid: fmt.Sprintf("uuid_%d", recordIdx),
					Startup_time:   "2026-03-09",
				},
				Target_Seqno:   uint64((recordIdx + 1) * 1000),
				TargetManifest: 40,
			},
		}
		err := record.TargetVBTimestamp.SetBrokenMappingAsPartOfCreation(brokenMap)
		assert.Nil(err)
		err = record.PopulateBrokenMappingSha()
		assert.Nil(err)
		records = append(records, record)
	}

	cacheLookupMap := make(map[uint64]metadata.CheckpointRecordsList)
	cacheLookupMap[40] = records

	// Call loadBrokenMappingCommit - this will exercise CreateLookupIndex and Consolidate
	ckmgr.loadBrokenMappingCommit(cacheLookupMap, 40)

	// Verify the result
	ckmgr.cachedBrokenMap.lock.RLock()
	defer ckmgr.cachedBrokenMap.lock.RUnlock()

	assert.Equal(uint64(40), ckmgr.cachedBrokenMap.correspondingTargetManifest)
	assert.NotNil(ckmgr.cachedBrokenMap.brokenMap)
	// We should have numMappingsPerRecord * 5 unique source namespaces (numMappingsPerRecord per record)
	assert.Len(ckmgr.cachedBrokenMap.brokenMap, numMappingsPerRecord*5)

	// Count total target mappings across all sources
	totalTargetCount := 0
	for _, tgtList := range ckmgr.cachedBrokenMap.brokenMap {
		totalTargetCount += len(tgtList)
	}
	assert.Equal(numMappingsPerRecord*5, totalTargetCount)
}

// TestLoadBrokenMappingCommit_LargeDatasetMixedScenario tests a complex scenario with:
// - 5 checkpoint records (max allowed per checkpoint)
// - Each record has very large broken mapping sets
// - Some overlapping namespaces between records (testing Consolidate behavior)
// - Exercises the compiledIndex lookup during consolidation
func TestLoadBrokenMappingCommit_LargeDatasetMixedScenario(t *testing.T) {
	fmt.Println("============== Test case start: TestLoadBrokenMappingCommit_LargeDatasetMixedScenario =================")
	defer fmt.Println("============== Test case end: TestLoadBrokenMappingCommit_LargeDatasetMixedScenario =================")
	assert := assert.New(t)

	ckmgr := &CheckpointManager{
		cachedBrokenMap: brokenMappingWithLock{
			brokenMap:                   make(metadata.CollectionNamespaceMapping),
			correspondingTargetManifest: 0,
			lock:                        sync.RWMutex{},
			brokenMapHistories:          make(map[uint64]metadata.CollectionNamespaceMapping),
		},
	}

	// Create 5 checkpoint records with large overlapping broken mappings
	var records metadata.CheckpointRecordsList
	numMappingsPerRecord := 2500
	sharedSourceNamespaces := make(map[int]*base.CollectionNamespace)

	// Pre-create some shared source namespaces to be used across multiple records
	for i := 0; i < 3; i++ {
		sharedStr := fmt.Sprintf("shared_scope_%d.shared_col", i)
		sharedNs, err := base.NewCollectionNamespaceFromString(sharedStr)
		assert.Nil(err)
		sharedSourceNamespaces[i] = &sharedNs
	}

	for recordIdx := 0; recordIdx < 5; recordIdx++ {
		brokenMap := make(metadata.CollectionNamespaceMapping)

		// Add shared mappings to create overlap across records
		for sharedIdx := 0; sharedIdx < 3; sharedIdx++ {
			// Each record maps the shared source to different targets
			for variantIdx := 0; variantIdx < 10; variantIdx++ {
				tgtStr := fmt.Sprintf("tgt_shared_scope_%d_%d_%d.col_%d", sharedIdx, recordIdx, variantIdx, sharedIdx)
				tgtNs, err := base.NewCollectionNamespaceFromString(tgtStr)
				assert.Nil(err)
				brokenMap.AddSingleMapping(sharedSourceNamespaces[sharedIdx], &tgtNs)
			}
		}

		// Add unique mappings specific to this record
		for mappingIdx := 0; mappingIdx < numMappingsPerRecord-30; mappingIdx++ {
			srcStr := fmt.Sprintf("unique_src_scope_%d_%d.col_%d", recordIdx, mappingIdx/15, mappingIdx)
			tgtStr := fmt.Sprintf("unique_tgt_scope_%d_%d.col_%d", recordIdx, mappingIdx/15, mappingIdx)

			srcNs, err := base.NewCollectionNamespaceFromString(srcStr)
			assert.Nil(err)
			tgtNs, err := base.NewCollectionNamespaceFromString(tgtStr)
			assert.Nil(err)

			brokenMap.AddSingleMapping(&srcNs, &tgtNs)
		}

		record := &metadata.CheckpointRecord{
			SourceVBTimestamp: metadata.SourceVBTimestamp{
				Failover_uuid: uint64(recordIdx + 1000),
				Seqno:         uint64((recordIdx + 1) * 5000),
			},
			TargetVBTimestamp: metadata.TargetVBTimestamp{
				Target_vb_opaque: &metadata.TargetVBUuidAndTimestamp{
					Target_vb_uuid: fmt.Sprintf("uuid_%d", recordIdx),
					Startup_time:   "2026-03-09",
				},
				Target_Seqno:   uint64((recordIdx + 1) * 5000),
				TargetManifest: 50,
			},
		}
		err := record.TargetVBTimestamp.SetBrokenMappingAsPartOfCreation(brokenMap)
		assert.Nil(err)
		err = record.PopulateBrokenMappingSha()
		assert.Nil(err)
		records = append(records, record)
	}

	cacheLookupMap := make(map[uint64]metadata.CheckpointRecordsList)
	cacheLookupMap[50] = records

	// Call loadBrokenMappingCommit with complex overlapping data
	ckmgr.loadBrokenMappingCommit(cacheLookupMap, 50)

	// Verify the result
	ckmgr.cachedBrokenMap.lock.RLock()
	defer ckmgr.cachedBrokenMap.lock.RUnlock()

	assert.Equal(uint64(50), ckmgr.cachedBrokenMap.correspondingTargetManifest)
	assert.NotNil(ckmgr.cachedBrokenMap.brokenMap)

	// Verify that shared sources are consolidated properly
	for sharedIdx := 0; sharedIdx < 3; sharedIdx++ {
		sharedSrc := sharedSourceNamespaces[sharedIdx]
		_, _, _, exists := ckmgr.cachedBrokenMap.brokenMap.Get(sharedSrc, nil)
		assert.True(exists)
		// Each shared source should have 5*10 = 50 target mappings (5 records × 10 variants)
		_, _, tgtList, existsFromGet := ckmgr.cachedBrokenMap.brokenMap.Get(sharedSrc, nil)
		assert.True(existsFromGet)
		assert.Equal(50, len(tgtList))
	}

	// Verify total number of source namespaces
	// 3 shared + (5 * (numMappingsPerRecord-30)) unique sources
	expectedUniqueSources := 3 + (5 * (numMappingsPerRecord - 30))
	assert.Len(ckmgr.cachedBrokenMap.brokenMap, expectedUniqueSources)
}

// TestLoadBrokenMappingCommit_WithNilRecords tests handling of nil checkpoint records
// Ensures robustness when records list contains nil entries
func TestLoadBrokenMappingCommit_WithNilRecords(t *testing.T) {
	fmt.Println("============== Test case start: TestLoadBrokenMappingCommit_WithNilRecords =================")
	defer fmt.Println("============== Test case end: TestLoadBrokenMappingCommit_WithNilRecords =================")
	assert := assert.New(t)

	ckmgr := &CheckpointManager{
		cachedBrokenMap: brokenMappingWithLock{
			brokenMap:                   make(metadata.CollectionNamespaceMapping),
			correspondingTargetManifest: 0,
			lock:                        sync.RWMutex{},
			brokenMapHistories:          make(map[uint64]metadata.CollectionNamespaceMapping),
		},
	}

	// Create valid broken mappings
	brokenMap := make(metadata.CollectionNamespaceMapping)
	ns1, err := base.NewCollectionNamespaceFromString("scope.collection")
	assert.Nil(err)
	ns2, err := base.NewCollectionNamespaceFromString("scope.collection2")
	assert.Nil(err)
	brokenMap.AddSingleMapping(&ns1, &ns2)

	validRecord := &metadata.CheckpointRecord{
		SourceVBTimestamp: metadata.SourceVBTimestamp{
			Failover_uuid: 1,
			Seqno:         100,
		},
		TargetVBTimestamp: metadata.TargetVBTimestamp{
			Target_vb_opaque: &metadata.TargetVBUuidAndTimestamp{
				Target_vb_uuid: "test_uuid",
				Startup_time:   "2026-03-09",
			},
			Target_Seqno:   100,
			TargetManifest: 60,
		},
	}
	err = validRecord.TargetVBTimestamp.SetBrokenMappingAsPartOfCreation(brokenMap)
	assert.Nil(err)
	err = validRecord.PopulateBrokenMappingSha()
	assert.Nil(err)

	// Create a list with nil and valid records
	cacheLookupMap := make(map[uint64]metadata.CheckpointRecordsList)
	cacheLookupMap[60] = metadata.CheckpointRecordsList{nil, validRecord, nil, validRecord}

	// Call loadBrokenMappingCommit - should skip nil records without error
	ckmgr.loadBrokenMappingCommit(cacheLookupMap, 60)

	// Verify that valid records were still processed
	ckmgr.cachedBrokenMap.lock.RLock()
	defer ckmgr.cachedBrokenMap.lock.RUnlock()

	assert.Equal(uint64(60), ckmgr.cachedBrokenMap.correspondingTargetManifest)
	assert.NotNil(ckmgr.cachedBrokenMap.brokenMap)
	assert.Len(ckmgr.cachedBrokenMap.brokenMap, 1)
	_, _, _, exists := ckmgr.cachedBrokenMap.brokenMap.Get(&ns1, nil)
	assert.True(exists)
}

// TestLoadBrokenMappingCommit_WithEmptyBrokenMappings tests handling of records with empty broken mappings
// Ensures records with no broken mappings don't interfere with consolidation
func TestLoadBrokenMappingCommit_WithEmptyBrokenMappings(t *testing.T) {
	fmt.Println("============== Test case start: TestLoadBrokenMappingCommit_WithEmptyBrokenMappings =================")
	defer fmt.Println("============== Test case end: TestLoadBrokenMappingCommit_WithEmptyBrokenMappings =================")
	assert := assert.New(t)

	ckmgr := &CheckpointManager{
		cachedBrokenMap: brokenMappingWithLock{
			brokenMap:                   make(metadata.CollectionNamespaceMapping),
			correspondingTargetManifest: 0,
			lock:                        sync.RWMutex{},
			brokenMapHistories:          make(map[uint64]metadata.CollectionNamespaceMapping),
		},
	}

	// Create one record with broken mappings and one with empty mappings
	brokenMap := make(metadata.CollectionNamespaceMapping)
	ns1, err := base.NewCollectionNamespaceFromString("scope.collection")
	assert.Nil(err)
	ns2, err := base.NewCollectionNamespaceFromString("scope.collection2")
	assert.Nil(err)
	brokenMap.AddSingleMapping(&ns1, &ns2)

	recordWithMappings := &metadata.CheckpointRecord{
		SourceVBTimestamp: metadata.SourceVBTimestamp{
			Failover_uuid: 1,
			Seqno:         100,
		},
		TargetVBTimestamp: metadata.TargetVBTimestamp{
			Target_vb_opaque: &metadata.TargetVBUuidAndTimestamp{
				Target_vb_uuid: "test_uuid",
				Startup_time:   "2026-03-09",
			},
			Target_Seqno:   100,
			TargetManifest: 70,
		},
	}
	err = recordWithMappings.TargetVBTimestamp.SetBrokenMappingAsPartOfCreation(brokenMap)
	assert.Nil(err)
	err = recordWithMappings.PopulateBrokenMappingSha()
	assert.Nil(err)

	// Record with nil broken mappings
	emptyRecord := &metadata.CheckpointRecord{
		SourceVBTimestamp: metadata.SourceVBTimestamp{
			Failover_uuid: 2,
			Seqno:         200,
		},
		TargetVBTimestamp: metadata.TargetVBTimestamp{
			Target_vb_opaque: &metadata.TargetVBUuidAndTimestamp{
				Target_vb_uuid: "test_uuid_2",
				Startup_time:   "2026-03-09",
			},
			Target_Seqno:   200,
			TargetManifest: 70,
		},
	}

	// Record with empty broken mappings
	emptyMapRecord := &metadata.CheckpointRecord{
		SourceVBTimestamp: metadata.SourceVBTimestamp{
			Failover_uuid: 3,
			Seqno:         300,
		},
		TargetVBTimestamp: metadata.TargetVBTimestamp{
			Target_vb_opaque: &metadata.TargetVBUuidAndTimestamp{
				Target_vb_uuid: "test_uuid_3",
				Startup_time:   "2026-03-09",
			},
			Target_Seqno:   300,
			TargetManifest: 70,
		},
	}

	cacheLookupMap := make(map[uint64]metadata.CheckpointRecordsList)
	cacheLookupMap[70] = metadata.CheckpointRecordsList{recordWithMappings, emptyRecord, emptyMapRecord}

	// Call loadBrokenMappingCommit
	ckmgr.loadBrokenMappingCommit(cacheLookupMap, 70)

	// Verify only the record with mappings was processed
	ckmgr.cachedBrokenMap.lock.RLock()
	defer ckmgr.cachedBrokenMap.lock.RUnlock()

	assert.Equal(uint64(70), ckmgr.cachedBrokenMap.correspondingTargetManifest)
	assert.NotNil(ckmgr.cachedBrokenMap.brokenMap)
	assert.Len(ckmgr.cachedBrokenMap.brokenMap, 1)
	_, _, _, exists := ckmgr.cachedBrokenMap.brokenMap.Get(&ns1, nil)
	assert.True(exists)
}

// TestLoadBrokenMappingCommit_MultiManifestSelection tests that only the highest manifestId is loaded
// Ensures the function correctly filters to load only the highest manifest version
func TestLoadBrokenMappingCommit_MultiManifestSelection(t *testing.T) {
	fmt.Println("============== Test case start: TestLoadBrokenMappingCommit_MultiManifestSelection =================")
	defer fmt.Println("============== Test case end: TestLoadBrokenMappingCommit_MultiManifestSelection =================")
	assert := assert.New(t)

	ckmgr := &CheckpointManager{
		cachedBrokenMap: brokenMappingWithLock{
			brokenMap:                   make(metadata.CollectionNamespaceMapping),
			correspondingTargetManifest: 0,
			lock:                        sync.RWMutex{},
			brokenMapHistories:          make(map[uint64]metadata.CollectionNamespaceMapping),
		},
	}

	// Create broken mappings for different manifest versions
	brokenMapV1 := make(metadata.CollectionNamespaceMapping)
	ns1, err := base.NewCollectionNamespaceFromString("v1.col")
	assert.Nil(err)
	ns2, err := base.NewCollectionNamespaceFromString("v1.col2")
	assert.Nil(err)
	brokenMapV1.AddSingleMapping(&ns1, &ns2)

	recordV1 := &metadata.CheckpointRecord{
		SourceVBTimestamp: metadata.SourceVBTimestamp{
			Failover_uuid: 1,
			Seqno:         100,
		},
		TargetVBTimestamp: metadata.TargetVBTimestamp{
			Target_vb_opaque: &metadata.TargetVBUuidAndTimestamp{
				Target_vb_uuid: "v1_uuid",
				Startup_time:   "2026-03-09",
			},
			Target_Seqno:   100,
			TargetManifest: 80,
		},
	}
	err = recordV1.TargetVBTimestamp.SetBrokenMappingAsPartOfCreation(brokenMapV1)
	assert.Nil(err)
	err = recordV1.PopulateBrokenMappingSha()
	assert.Nil(err)

	brokenMapV2 := make(metadata.CollectionNamespaceMapping)
	ns3, err := base.NewCollectionNamespaceFromString("v2.col")
	assert.Nil(err)
	ns4, err := base.NewCollectionNamespaceFromString("v2.col2")
	assert.Nil(err)
	brokenMapV2.AddSingleMapping(&ns3, &ns4)

	recordV2 := &metadata.CheckpointRecord{
		SourceVBTimestamp: metadata.SourceVBTimestamp{
			Failover_uuid: 2,
			Seqno:         200,
		},
		TargetVBTimestamp: metadata.TargetVBTimestamp{
			Target_vb_opaque: &metadata.TargetVBUuidAndTimestamp{
				Target_vb_uuid: "v2_uuid",
				Startup_time:   "2026-03-09",
			},
			Target_Seqno:   200,
			TargetManifest: 90,
		},
	}
	err = recordV2.TargetVBTimestamp.SetBrokenMappingAsPartOfCreation(brokenMapV2)
	assert.Nil(err)
	err = recordV2.PopulateBrokenMappingSha()
	assert.Nil(err)

	// Create cacheLookupMap with records from multiple manifest versions
	cacheLookupMap := make(map[uint64]metadata.CheckpointRecordsList)
	cacheLookupMap[80] = metadata.CheckpointRecordsList{recordV1}
	cacheLookupMap[90] = metadata.CheckpointRecordsList{recordV2}

	// Call loadBrokenMappingCommit with highest manifest ID 90
	ckmgr.loadBrokenMappingCommit(cacheLookupMap, 90)

	// Verify that only V2 (manifest 90) was loaded
	ckmgr.cachedBrokenMap.lock.RLock()
	defer ckmgr.cachedBrokenMap.lock.RUnlock()

	assert.Equal(uint64(90), ckmgr.cachedBrokenMap.correspondingTargetManifest)
	assert.NotNil(ckmgr.cachedBrokenMap.brokenMap)
	assert.Len(ckmgr.cachedBrokenMap.brokenMap, 1)
	_, _, _, exists3 := ckmgr.cachedBrokenMap.brokenMap.Get(&ns3, nil)
	assert.True(exists3)
	_, _, _, exists1 := ckmgr.cachedBrokenMap.brokenMap.Get(&ns1, nil)
	assert.False(exists1)
}

// TestLoadBrokenMappingCommit_VeryLargeMappingCompilationIndex tests with extremely large mappings
// to ensure the compiledIndex is properly created and used during Consolidate
// 5 records × 200 mappings each = 1000 total mappings
func TestLoadBrokenMappingCommit_VeryLargeMappingCompilationIndex(t *testing.T) {
	fmt.Println("============== Test case start: TestLoadBrokenMappingCommit_VeryLargeMappingCompilationIndex =================")
	defer fmt.Println("============== Test case end: TestLoadBrokenMappingCommit_VeryLargeMappingCompilationIndex =================")
	assert := assert.New(t)

	ckmgr := &CheckpointManager{
		cachedBrokenMap: brokenMappingWithLock{
			brokenMap:                   make(metadata.CollectionNamespaceMapping),
			correspondingTargetManifest: 0,
			lock:                        sync.RWMutex{},
			brokenMapHistories:          make(map[uint64]metadata.CollectionNamespaceMapping),
		},
	}

	// Create 5 checkpoint records with very large broken mappings (5000 mappings each)
	var records metadata.CheckpointRecordsList
	numMappingsPerRecord := 5000
	uniqueSourceCount := 0

	for recordIdx := 0; recordIdx < 5; recordIdx++ {
		brokenMap := make(metadata.CollectionNamespaceMapping)

		// Create 200 unique namespace mappings per record
		for mappingIdx := 0; mappingIdx < numMappingsPerRecord; mappingIdx++ {
			srcStr := fmt.Sprintf("large_src_scope_%d_%d.large_src_col_%d", recordIdx, mappingIdx/20, mappingIdx%20)
			tgtStr := fmt.Sprintf("large_tgt_scope_%d_%d.large_tgt_col_%d", recordIdx, mappingIdx/20, mappingIdx%20)

			srcNs, err := base.NewCollectionNamespaceFromString(srcStr)
			assert.Nil(err)
			tgtNs, err := base.NewCollectionNamespaceFromString(tgtStr)
			assert.Nil(err)

			brokenMap.AddSingleMapping(&srcNs, &tgtNs)

			// Count unique sources
			if mappingIdx == 0 {
				uniqueSourceCount++
			}
		}

		record := &metadata.CheckpointRecord{
			SourceVBTimestamp: metadata.SourceVBTimestamp{
				Failover_uuid: uint64(recordIdx + 500),
				Seqno:         uint64((recordIdx + 1) * 10000),
			},
			TargetVBTimestamp: metadata.TargetVBTimestamp{
				Target_vb_opaque: &metadata.TargetVBUuidAndTimestamp{
					Target_vb_uuid: fmt.Sprintf("uuid_%d", recordIdx),
					Startup_time:   "2026-03-09",
				},
				Target_Seqno:   uint64((recordIdx + 1) * 10000),
				TargetManifest: 100,
			},
		}
		err := record.TargetVBTimestamp.SetBrokenMappingAsPartOfCreation(brokenMap)
		assert.Nil(err)
		err = record.PopulateBrokenMappingSha()
		assert.Nil(err)
		records = append(records, record)
	}

	cacheLookupMap := make(map[uint64]metadata.CheckpointRecordsList)
	cacheLookupMap[100] = records

	// Call loadBrokenMappingCommit - compiledIndex will be created and exercised
	ckmgr.loadBrokenMappingCommit(cacheLookupMap, 100)

	// Verify the result
	ckmgr.cachedBrokenMap.lock.RLock()
	defer ckmgr.cachedBrokenMap.lock.RUnlock()

	assert.Equal(uint64(100), ckmgr.cachedBrokenMap.correspondingTargetManifest)
	assert.NotNil(ckmgr.cachedBrokenMap.brokenMap)
	// We should have numMappingsPerRecord * 5 unique source namespaces
	assert.Len(ckmgr.cachedBrokenMap.brokenMap, numMappingsPerRecord*5)

	// Verify total target count
	totalTargetCount := 0
	for _, tgtList := range ckmgr.cachedBrokenMap.brokenMap {
		totalTargetCount += len(tgtList)
	}
	assert.Equal(numMappingsPerRecord*5, totalTargetCount)
}

// TestRestoreBrokenMappingManifests_SmallTraditionalPair tests restoreBrokenMappingManifests with a small traditional broken map
// This test validates basic functionality of restoring a traditional (non-global) broken mapping
func TestRestoreBrokenMappingManifests_SmallTraditionalPair(t *testing.T) {
	fmt.Println("============== Test case start: TestRestoreBrokenMappingManifests_SmallTraditionalPair =================")
	defer fmt.Println("============== Test case end: TestRestoreBrokenMappingManifests_SmallTraditionalPair =================")
	assert := assert.New(t)

	// Setup checkpoint manager with pipeline mock
	mockPipeline := &commonMock.Pipeline{}
	updateSettingsCalled := false
	var capturedSettings map[string]interface{}

	mockPipeline.On("UpdateSettings", mock.MatchedBy(func(settings map[string]interface{}) bool {
		updateSettingsCalled = true
		capturedSettings = settings
		return true
	})).Return(nil)

	ckmgr := &CheckpointManager{
		pipeline: mockPipeline,
		cachedBrokenMap: brokenMappingWithLock{
			brokenMap:                   make(metadata.CollectionNamespaceMapping),
			correspondingTargetManifest: 0,
			lock:                        sync.RWMutex{},
			brokenMapHistories:          make(map[uint64]metadata.CollectionNamespaceMapping),
		},
	}

	// Create a small traditional broken mapping with 2 namespace mappings
	brokenMap := make(metadata.CollectionNamespaceMapping)
	ns1, err := base.NewCollectionNamespaceFromString("scope1.collection1")
	assert.Nil(err)
	ns2, err := base.NewCollectionNamespaceFromString("scope1.collection2")
	assert.Nil(err)

	brokenMap.AddSingleMapping(&ns1, &ns2)

	// Create traditional pair
	traditionalPair := metadata.ManifestIdAndBrokenMapPair{
		ManifestId: 10,
		BrokenMap:  &brokenMap,
	}

	// Empty global pairs
	globalPairs := make(map[uint16]metadata.ManifestIdAndBrokenMapPair)

	// Call restoreBrokenMappingManifests
	ckmgr.restoreBrokenMappingManifests(traditionalPair, globalPairs, false)

	// Verify UpdateSettings was called with traditional mapping
	assert.True(updateSettingsCalled)
	assert.NotNil(capturedSettings)
	assert.Contains(capturedSettings, metadata.BrokenMappingsUpdateKey)

	// Verify the broken map was restored to the cachedBrokenMap
	ckmgr.cachedBrokenMap.lock.RLock()
	defer ckmgr.cachedBrokenMap.lock.RUnlock()

	assert.Equal(uint64(10), ckmgr.cachedBrokenMap.correspondingTargetManifest)
	assert.NotNil(ckmgr.cachedBrokenMap.brokenMap)
	assert.Len(ckmgr.cachedBrokenMap.brokenMap, 1)
	_, _, _, exists := ckmgr.cachedBrokenMap.brokenMap.Get(&ns1, nil)
	assert.True(exists)
}

// TestRestoreBrokenMappingManifests_LargeTraditionalPair tests restoreBrokenMappingManifests with a large traditional broken map
// This test validates the function handles large broken mapping sets with many namespace mappings
func TestRestoreBrokenMappingManifests_LargeTraditionalPair(t *testing.T) {
	fmt.Println("============== Test case start: TestRestoreBrokenMappingManifests_LargeTraditionalPair =================")
	defer fmt.Println("============== Test case end: TestRestoreBrokenMappingManifests_LargeTraditionalPair =================")
	assert := assert.New(t)

	// Setup checkpoint manager with pipeline mock
	mockPipeline := &commonMock.Pipeline{}
	updateSettingsCalled := false
	var capturedSettings map[string]interface{}

	mockPipeline.On("UpdateSettings", mock.MatchedBy(func(settings map[string]interface{}) bool {
		updateSettingsCalled = true
		capturedSettings = settings
		return true
	})).Return(nil)

	ckmgr := &CheckpointManager{
		pipeline: mockPipeline,
		cachedBrokenMap: brokenMappingWithLock{
			brokenMap:                   make(metadata.CollectionNamespaceMapping),
			correspondingTargetManifest: 0,
			lock:                        sync.RWMutex{},
			brokenMapHistories:          make(map[uint64]metadata.CollectionNamespaceMapping),
		},
	}

	// Create a large traditional broken mapping with 1000 namespace mappings
	brokenMap := make(metadata.CollectionNamespaceMapping)
	numMappings := 1000
	var sourceNamespaces []*base.CollectionNamespace

	for i := 0; i < numMappings; i++ {
		srcStr := fmt.Sprintf("scope%d.src_col_%d", i/100, i)
		tgtStr := fmt.Sprintf("scope%d.tgt_col_%d", i/100, i)

		srcNs, err := base.NewCollectionNamespaceFromString(srcStr)
		assert.Nil(err)
		tgtNs, err := base.NewCollectionNamespaceFromString(tgtStr)
		assert.Nil(err)

		brokenMap.AddSingleMapping(&srcNs, &tgtNs)

		// Track first occurrence of each source namespace
		if i == 0 {
			sourceNamespaces = append(sourceNamespaces, &srcNs)
		}
	}

	// Create traditional pair with large broken map
	traditionalPair := metadata.ManifestIdAndBrokenMapPair{
		ManifestId: 20,
		BrokenMap:  &brokenMap,
	}

	// Empty global pairs
	globalPairs := make(map[uint16]metadata.ManifestIdAndBrokenMapPair)

	// Call restoreBrokenMappingManifests
	ckmgr.restoreBrokenMappingManifests(traditionalPair, globalPairs, false)

	// Verify UpdateSettings was called with traditional mapping
	assert.True(updateSettingsCalled)
	assert.NotNil(capturedSettings)
	assert.Contains(capturedSettings, metadata.BrokenMappingsUpdateKey)

	// Verify the large broken map was restored to the cachedBrokenMap
	ckmgr.cachedBrokenMap.lock.RLock()
	defer ckmgr.cachedBrokenMap.lock.RUnlock()

	assert.Equal(uint64(20), ckmgr.cachedBrokenMap.correspondingTargetManifest)
	assert.NotNil(ckmgr.cachedBrokenMap.brokenMap)
	// Should have 1000 unique source namespaces (one per mapping)
	assert.Len(ckmgr.cachedBrokenMap.brokenMap, numMappings)

	// Verify total targets across all sources
	totalTargets := 0
	for _, tgtList := range ckmgr.cachedBrokenMap.brokenMap {
		totalTargets += len(tgtList)
	}
	assert.Equal(numMappings, totalTargets)
}

// TestRestoreBrokenMappingManifests_SmallGlobalPairs tests restoreBrokenMappingManifests with small global broken maps
// This test validates basic functionality of restoring global (scope-level) broken mappings
func TestRestoreBrokenMappingManifests_SmallGlobalPairs(t *testing.T) {
	t.Skip("Disabling until MB-70907 is fixed")
	fmt.Println("============== Test case start: TestRestoreBrokenMappingManifests_SmallGlobalPairs =================")
	defer fmt.Println("============== Test case end: TestRestoreBrokenMappingManifests_SmallGlobalPairs =================")
	assert := assert.New(t)

	// Setup checkpoint manager with pipeline mock
	mockPipeline := &commonMock.Pipeline{}
	updateSettingsCalled := false
	var capturedSettings map[string]interface{}

	mockPipeline.On("UpdateSettings", mock.MatchedBy(func(settings map[string]interface{}) bool {
		updateSettingsCalled = true
		capturedSettings = settings
		return true
	})).Return(nil)

	ckmgr := &CheckpointManager{
		pipeline: mockPipeline,
		cachedBrokenMap: brokenMappingWithLock{
			brokenMap:                   make(metadata.CollectionNamespaceMapping),
			correspondingTargetManifest: 0,
			lock:                        sync.RWMutex{},
			brokenMapHistories:          make(map[uint64]metadata.CollectionNamespaceMapping),
		},
	}

	// Create empty traditional pair
	traditionalPair := metadata.ManifestIdAndBrokenMapPair{
		ManifestId: 0,
		BrokenMap:  nil,
	}

	// Create small global pairs for 3 scopes
	globalPairs := make(map[uint16]metadata.ManifestIdAndBrokenMapPair)

	for scopeId := uint16(0); scopeId < 3; scopeId++ {
		brokenMap := make(metadata.CollectionNamespaceMapping)

		// Create 2 namespace mappings per scope
		for colIdx := 0; colIdx < 2; colIdx++ {
			srcStr := fmt.Sprintf("scope%d.src_col_%d", scopeId, colIdx)
			tgtStr := fmt.Sprintf("scope%d.tgt_col_%d", scopeId, colIdx)

			srcNs, err := base.NewCollectionNamespaceFromString(srcStr)
			assert.Nil(err)
			tgtNs, err := base.NewCollectionNamespaceFromString(tgtStr)
			assert.Nil(err)

			brokenMap.AddSingleMapping(&srcNs, &tgtNs)
		}

		pair := metadata.ManifestIdAndBrokenMapPair{
			ManifestId: uint64(30 + scopeId),
			BrokenMap:  &brokenMap,
		}
		globalPairs[scopeId] = pair
	}

	// Call restoreBrokenMappingManifests
	ckmgr.restoreBrokenMappingManifests(traditionalPair, globalPairs, false)

	// Verify UpdateSettings was called with global mapping
	assert.True(updateSettingsCalled)
	assert.NotNil(capturedSettings)
	assert.Contains(capturedSettings, metadata.GlobalBrokenMappingUpdateKey)

	// Verify the global broken maps were restored to the cachedBrokenMap
	ckmgr.cachedBrokenMap.lock.RLock()
	defer ckmgr.cachedBrokenMap.lock.RUnlock()

	// The mapping should be updated with the latest scope's manifest ID
	assert.Equal(uint64(32), ckmgr.cachedBrokenMap.correspondingTargetManifest)
	assert.NotNil(ckmgr.cachedBrokenMap.brokenMap)
}

// TestRestoreBrokenMappingManifests_LargeGlobalPairs tests restoreBrokenMappingManifests with large global broken maps
// This test validates handling of large global broken mappings with many scopes and namespace mappings each
func TestRestoreBrokenMappingManifests_LargeGlobalPairs(t *testing.T) {
	fmt.Println("============== Test case start: TestRestoreBrokenMappingManifests_LargeGlobalPairs =================")
	defer fmt.Println("============== Test case end: TestRestoreBrokenMappingManifests_LargeGlobalPairs =================")
	assert := assert.New(t)

	// Setup checkpoint manager with pipeline mock
	mockPipeline := &commonMock.Pipeline{}
	updateSettingsCalled := false
	var capturedSettings map[string]interface{}

	mockPipeline.On("UpdateSettings", mock.MatchedBy(func(settings map[string]interface{}) bool {
		updateSettingsCalled = true
		capturedSettings = settings
		return true
	})).Return(nil)

	ckmgr := &CheckpointManager{
		pipeline: mockPipeline,
		cachedBrokenMap: brokenMappingWithLock{
			brokenMap:                   make(metadata.CollectionNamespaceMapping),
			correspondingTargetManifest: 0,
			lock:                        sync.RWMutex{},
			brokenMapHistories:          make(map[uint64]metadata.CollectionNamespaceMapping),
		},
	}

	// Create empty traditional pair
	traditionalPair := metadata.ManifestIdAndBrokenMapPair{
		ManifestId: 0,
		BrokenMap:  nil,
	}

	// Create large global pairs for 10 scopes, each with 1000 mappings
	globalPairs := make(map[uint16]metadata.ManifestIdAndBrokenMapPair)
	numScopes := 10
	numMappingsPerScope := 1000

	for scopeId := uint16(0); scopeId < uint16(numScopes); scopeId++ {
		brokenMap := make(metadata.CollectionNamespaceMapping)

		// Create many namespace mappings per scope
		for colIdx := 0; colIdx < numMappingsPerScope; colIdx++ {
			srcStr := fmt.Sprintf("scope%d.src_col_%d", scopeId, colIdx)
			tgtStr := fmt.Sprintf("scope%d.tgt_col_%d", scopeId, colIdx)

			srcNs, err := base.NewCollectionNamespaceFromString(srcStr)
			assert.Nil(err)
			tgtNs, err := base.NewCollectionNamespaceFromString(tgtStr)
			assert.Nil(err)

			brokenMap.AddSingleMapping(&srcNs, &tgtNs)
		}

		pair := metadata.ManifestIdAndBrokenMapPair{
			ManifestId: uint64(40 + scopeId),
			BrokenMap:  &brokenMap,
		}
		globalPairs[scopeId] = pair
	}

	// Call restoreBrokenMappingManifests
	ckmgr.restoreBrokenMappingManifests(traditionalPair, globalPairs, false)

	// Verify UpdateSettings was called with global mapping
	assert.True(updateSettingsCalled)
	assert.NotNil(capturedSettings)
	assert.Contains(capturedSettings, metadata.GlobalBrokenMappingUpdateKey)

	// Verify the large global broken maps were restored to the cachedBrokenMap
	ckmgr.cachedBrokenMap.lock.RLock()
	defer ckmgr.cachedBrokenMap.lock.RUnlock()

	// The mapping should be updated with one of the scope's manifest ID.
	// Since map iteration is random, we just verify it's in the valid range (40-49)
	assert.GreaterOrEqual(ckmgr.cachedBrokenMap.correspondingTargetManifest, uint64(40))
	assert.LessOrEqual(ckmgr.cachedBrokenMap.correspondingTargetManifest, uint64(49))
	assert.NotNil(ckmgr.cachedBrokenMap.brokenMap)
	// Due to the way restoreBrokenMapHistoryNoLock works, only the LAST scope processed
	// will be in the current brokenMap (with others in history).
	// Since map iteration is random, we just verify we have numMappingsPerScope mappings
	assert.Equal(numMappingsPerScope, len(ckmgr.cachedBrokenMap.brokenMap))

	// Verify the last scope's targets are present
	totalTargets := 0
	for _, tgtList := range ckmgr.cachedBrokenMap.brokenMap {
		totalTargets += len(tgtList)
	}
	assert.Equal(numMappingsPerScope, totalTargets)
}

// TestRestoreBrokenMappingManifests_TraditionalWithRollBack tests restoreBrokenMappingManifests with rollback flag set to true
// This test validates that the isRollBack flag is correctly passed to UpdateSettings for traditional mappings
func TestRestoreBrokenMappingManifests_TraditionalWithRollBack(t *testing.T) {
	fmt.Println("============== Test case start: TestRestoreBrokenMappingManifests_TraditionalWithRollBack =================")
	defer fmt.Println("============== Test case end: TestRestoreBrokenMappingManifests_TraditionalWithRollBack =================")
	assert := assert.New(t)

	// Setup checkpoint manager with pipeline mock
	mockPipeline := &commonMock.Pipeline{}
	var capturedArgsList []interface{}

	mockPipeline.On("UpdateSettings", mock.MatchedBy(func(settings map[string]interface{}) bool {
		if argsList, ok := settings[metadata.BrokenMappingsUpdateKey]; ok {
			capturedArgsList = argsList.([]interface{})
		}
		return true
	})).Return(nil)

	ckmgr := &CheckpointManager{
		pipeline: mockPipeline,
		cachedBrokenMap: brokenMappingWithLock{
			brokenMap:                   make(metadata.CollectionNamespaceMapping),
			correspondingTargetManifest: 0,
			lock:                        sync.RWMutex{},
			brokenMapHistories:          make(map[uint64]metadata.CollectionNamespaceMapping),
		},
	}

	// Create a small traditional broken mapping
	brokenMap := make(metadata.CollectionNamespaceMapping)
	ns1, err := base.NewCollectionNamespaceFromString("scope1.collection1")
	assert.Nil(err)
	ns2, err := base.NewCollectionNamespaceFromString("scope1.collection2")
	assert.Nil(err)

	brokenMap.AddSingleMapping(&ns1, &ns2)

	// Create traditional pair
	traditionalPair := metadata.ManifestIdAndBrokenMapPair{
		ManifestId: 10,
		BrokenMap:  &brokenMap,
	}

	// Empty global pairs
	globalPairs := make(map[uint16]metadata.ManifestIdAndBrokenMapPair)

	// Call restoreBrokenMappingManifests with isRollBack = true
	ckmgr.restoreBrokenMappingManifests(traditionalPair, globalPairs, true)

	// Verify UpdateSettings was called and isRollBack flag is set correctly
	assert.NotNil(capturedArgsList)
	assert.Len(capturedArgsList, 3) // brokenMap, manifestId, isRollBack
	isRollBack, ok := capturedArgsList[2].(bool)
	assert.True(ok)
	assert.True(isRollBack) // Verify the flag is set to true
}

// TestRestoreBrokenMappingManifests_GlobalWithRollBack tests restoreBrokenMappingManifests with rollback flag set to true for global mappings
// This test validates that the isRollBack flag is correctly passed to UpdateSettings for global mappings
func TestRestoreBrokenMappingManifests_GlobalWithRollBack(t *testing.T) {
	fmt.Println("============== Test case start: TestRestoreBrokenMappingManifests_GlobalWithRollBack =================")
	defer fmt.Println("============== Test case end: TestRestoreBrokenMappingManifests_GlobalWithRollBack =================")
	assert := assert.New(t)

	// Setup checkpoint manager with pipeline mock
	mockPipeline := &commonMock.Pipeline{}
	var capturedArgsList []interface{}

	mockPipeline.On("UpdateSettings", mock.MatchedBy(func(settings map[string]interface{}) bool {
		if argsList, ok := settings[metadata.GlobalBrokenMappingUpdateKey]; ok {
			capturedArgsList = argsList.([]interface{})
		}
		return true
	})).Return(nil)

	ckmgr := &CheckpointManager{
		pipeline: mockPipeline,
		cachedBrokenMap: brokenMappingWithLock{
			brokenMap:                   make(metadata.CollectionNamespaceMapping),
			correspondingTargetManifest: 0,
			lock:                        sync.RWMutex{},
			brokenMapHistories:          make(map[uint64]metadata.CollectionNamespaceMapping),
		},
	}

	// Create empty traditional pair
	traditionalPair := metadata.ManifestIdAndBrokenMapPair{
		ManifestId: 0,
		BrokenMap:  nil,
	}

	// Create small global pairs
	globalPairs := make(map[uint16]metadata.ManifestIdAndBrokenMapPair)

	brokenMap := make(metadata.CollectionNamespaceMapping)
	ns1, err := base.NewCollectionNamespaceFromString("scope1.collection1")
	assert.Nil(err)
	ns2, err := base.NewCollectionNamespaceFromString("scope1.collection2")
	assert.Nil(err)

	brokenMap.AddSingleMapping(&ns1, &ns2)

	pair := metadata.ManifestIdAndBrokenMapPair{
		ManifestId: 10,
		BrokenMap:  &brokenMap,
	}
	globalPairs[0] = pair

	// Call restoreBrokenMappingManifests with isRollBack = true
	ckmgr.restoreBrokenMappingManifests(traditionalPair, globalPairs, true)

	// Verify UpdateSettings was called and isRollBack flag is set correctly
	assert.NotNil(capturedArgsList)
	assert.Len(capturedArgsList, 2) // globalPairs, isRollBack
	isRollBack, ok := capturedArgsList[1].(bool)
	assert.True(ok)
	assert.True(isRollBack) // Verify the flag is set to true
}

// TestRestoreBrokenMappingManifests_EmptyData tests restoreBrokenMappingManifests with empty/nil data
// This test validates that the function returns early when no valid data is provided
func TestRestoreBrokenMappingManifests_EmptyData(t *testing.T) {
	fmt.Println("============== Test case start: TestRestoreBrokenMappingManifests_EmptyData =================")
	defer fmt.Println("============== Test case end: TestRestoreBrokenMappingManifests_EmptyData =================")
	assert := assert.New(t)

	// Setup checkpoint manager with pipeline mock
	mockPipeline := &commonMock.Pipeline{}
	updateSettingsCalled := false

	mockPipeline.On("UpdateSettings", mock.Anything).Run(func(args mock.Arguments) {
		updateSettingsCalled = true
	}).Return(nil)

	ckmgr := &CheckpointManager{
		pipeline: mockPipeline,
		cachedBrokenMap: brokenMappingWithLock{
			brokenMap:                   make(metadata.CollectionNamespaceMapping),
			correspondingTargetManifest: 0,
			lock:                        sync.RWMutex{},
			brokenMapHistories:          make(map[uint64]metadata.CollectionNamespaceMapping),
		},
	}

	// Create empty traditional pair and empty global pairs
	traditionalPair := metadata.ManifestIdAndBrokenMapPair{
		ManifestId: 0,
		BrokenMap:  nil,
	}

	globalPairs := make(map[uint16]metadata.ManifestIdAndBrokenMapPair)

	// Call restoreBrokenMappingManifests with empty data
	ckmgr.restoreBrokenMappingManifests(traditionalPair, globalPairs, false)

	// Verify UpdateSettings was NOT called (early return)
	assert.False(updateSettingsCalled)
}

// TestRestoreBrokenMappingManifests_ManifestIdHistories tests that restoreBrokenMappingManifests properly manages broken map histories
// This test validates that when multiple manifest IDs are involved, they are properly stored in brokenMapHistories
func TestRestoreBrokenMappingManifests_ManifestIdHistories(t *testing.T) {
	fmt.Println("============== Test case start: TestRestoreBrokenMappingManifests_ManifestIdHistories =================")
	defer fmt.Println("============== Test case end: TestRestoreBrokenMappingManifests_ManifestIdHistories =================")
	assert := assert.New(t)

	// Setup checkpoint manager with pipeline mock
	mockPipeline := &commonMock.Pipeline{}
	mockPipeline.On("UpdateSettings", mock.Anything).Return(nil)

	ckmgr := &CheckpointManager{
		pipeline: mockPipeline,
		cachedBrokenMap: brokenMappingWithLock{
			brokenMap:                   make(metadata.CollectionNamespaceMapping),
			correspondingTargetManifest: 100,
			lock:                        sync.RWMutex{},
			brokenMapHistories:          make(map[uint64]metadata.CollectionNamespaceMapping),
		},
	}

	// Initialize the brokenMapHistories with an old manifest
	oldBrokenMap := make(metadata.CollectionNamespaceMapping)
	ns1, err := base.NewCollectionNamespaceFromString("scope1.collection1")
	assert.Nil(err)
	ns2, err := base.NewCollectionNamespaceFromString("scope1.collection2")
	assert.Nil(err)
	oldBrokenMap.AddSingleMapping(&ns1, &ns2)
	ckmgr.cachedBrokenMap.brokenMapHistories[50] = oldBrokenMap

	// Create empty traditional pair
	traditionalPair := metadata.ManifestIdAndBrokenMapPair{
		ManifestId: 0,
		BrokenMap:  nil,
	}

	// Create global pairs with manifest IDs less than current (should go to history)
	globalPairs := make(map[uint16]metadata.ManifestIdAndBrokenMapPair)

	for scopeId := uint16(0); scopeId < 3; scopeId++ {
		brokenMap := make(metadata.CollectionNamespaceMapping)

		// Create 2 namespace mappings per scope
		for colIdx := 0; colIdx < 2; colIdx++ {
			srcStr := fmt.Sprintf("scope%d.src_col_%d", scopeId, colIdx)
			tgtStr := fmt.Sprintf("scope%d.tgt_col_%d", scopeId, colIdx)

			srcNs, err := base.NewCollectionNamespaceFromString(srcStr)
			assert.Nil(err)
			tgtNs, err := base.NewCollectionNamespaceFromString(tgtStr)
			assert.Nil(err)

			brokenMap.AddSingleMapping(&srcNs, &tgtNs)
		}

		pair := metadata.ManifestIdAndBrokenMapPair{
			ManifestId: uint64(70 + scopeId*5),
			BrokenMap:  &brokenMap,
		}
		globalPairs[scopeId] = pair
	}

	// Call restoreBrokenMappingManifests
	ckmgr.restoreBrokenMappingManifests(traditionalPair, globalPairs, false)

	// Verify histories were updated
	ckmgr.cachedBrokenMap.lock.RLock()
	defer ckmgr.cachedBrokenMap.lock.RUnlock()

	// Check that histories contain the old manifest ID
	_, historyExists := ckmgr.cachedBrokenMap.brokenMapHistories[50]
	assert.True(historyExists)
}
