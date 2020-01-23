// +build !pcre

package pipeline_manager

import (
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	commonReal "github.com/couchbase/goxdcr/common"
	common "github.com/couchbase/goxdcr/common/mocks"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	replicationStatus "github.com/couchbase/goxdcr/pipeline"
	replicationStatusMock "github.com/couchbase/goxdcr/pipeline/mocks"
	PipelineMgrMock "github.com/couchbase/goxdcr/pipeline_manager/mocks"
	service_def "github.com/couchbase/goxdcr/service_def/mocks"
	utilities "github.com/couchbase/goxdcr/utils"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
	//	http "net/http"
	//	_ "net/http/pprof"
	"sync/atomic"
	"testing"
	"time"
)

func TestStopAllUpdaters(t *testing.T) {
	// set-up boiler plate
	pipelineMock := &common.PipelineFactory{}
	replSpecSvcMock := &service_def.ReplicationSpecSvc{}
	xdcrTopologyMock := &service_def.XDCRCompTopologySvc{}
	remoteClusterMock := &service_def.RemoteClusterSvc{}
	utilsNew := utilities.NewUtilities()

	pipelineMgr := PipelineManager{
		pipeline_factory:   pipelineMock,
		repl_spec_svc:      replSpecSvcMock,
		xdcr_topology_svc:  xdcrTopologyMock,
		remote_cluster_svc: remoteClusterMock,
		utils:              utilsNew,
	}
	// end boiler plate

	replSpecSvcMock.On("AllReplicationSpecIds").Return(nil, errors.New("Injected empty error"))
	pipelineMgr.StopAllUpdaters()
}

func setupBoilerPlate() (*log.CommonLogger,
	*common.PipelineFactory,
	*service_def.ReplicationSpecSvc,
	*service_def.XDCRCompTopologySvc,
	*service_def.RemoteClusterSvc,
	*PipelineManager,
	*PipelineUpdater,
	*replicationStatus.ReplicationStatus,
	string,
	*metadata.ReplicationSettings,
	*metadata.ReplicationSpecification,
	*metadata.RemoteClusterReference,
	*common.Pipeline,
	*service_def.UILogSvc,
	*replicationStatusMock.ReplicationStatusIface,
	*service_def.CheckpointsService,
	*service_def.ClusterInfoSvc) {

	testLogger := log.NewLogger("testLogger", log.DefaultLoggerContext)
	pipelineMock := &common.PipelineFactory{}
	replSpecSvcMock := &service_def.ReplicationSpecSvc{}
	xdcrTopologyMock := &service_def.XDCRCompTopologySvc{}
	remoteClusterMock := &service_def.RemoteClusterSvc{}
	uiLogSvcMock := &service_def.UILogSvc{}
	utilsNew := utilities.NewUtilities()
	checkPointsSvc := &service_def.CheckpointsService{}
	clusterInfoSvc := &service_def.ClusterInfoSvc{}

	pipelineMgr := NewPipelineManager(pipelineMock, replSpecSvcMock, xdcrTopologyMock,
		remoteClusterMock, clusterInfoSvc, nil, /*checkpoint_svc*/
		uiLogSvcMock, log.DefaultLoggerContext, utilsNew)

	// Some things needed for pipelinemgr
	testTopic := "testTopic"

	// needed for replicationStatus
	var testRepairer *PipelineUpdater

	// setting up replicationStatus and replicationSpecService
	//	var repStatusMtx sync.RWMutex
	//	testReplicationStatus := &replicationStatus.ReplicationStatus{
	//		Lock:             &repStatusMtx,
	//		SpecId:           testTopic,
	//		Logger:           testLogger,
	//		Pipeline_updater: testRepairer,
	//		Obj_pool:         base.NewMCRequestPool(testTopic, testLogger)}

	testReplicationSettings := metadata.DefaultReplicationSettings()
	settingsMap := make(map[string]interface{})
	settingsMap[metadata.FailureRestartIntervalKey] = 10
	settingsMap[metadata.CompressionTypeKey] = (int)(base.CompressionTypeSnappy)
	testReplicationSettings.UpdateSettingsFromMap(settingsMap)
	testReplicationSpec := &metadata.ReplicationSpecification{Settings: testReplicationSettings, Revision: 1}

	specGetterFxLiteral := func(specId string) (*metadata.ReplicationSpecification, error) { return testReplicationSpec, nil }
	testReplicationStatus := replicationStatus.NewReplicationStatus(testTopic, specGetterFxLiteral, testLogger)

	testRepairer = newPipelineUpdater(testTopic, 0 /*retry_interval*/, nil, /*cur_err*/
		testReplicationStatus, testLogger, pipelineMgr)

	/**
	 * This should prevent SetDerivedObj -> RuntimeStatus -> rs.Spec() -> rs.Spec_getter going through
	 * a double mock causing a double lock
	 */
	//	specGetterFxLiteral := func(specId string) (*metadata.ReplicationSpecification, error) { return testReplicationSpec, nil }
	//	testReplicationStatus.Spec_getter = specGetterFxLiteral

	testRemoteClusterRef := &metadata.RemoteClusterReference{}

	testPipeline := &common.Pipeline{}

	// ReplicationStatusMock for testing replSettingsRevContext
	repStatusMock := &replicationStatusMock.ReplicationStatusIface{}

	return testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic, testReplicationSettings,
		testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvcMock, repStatusMock,
		checkPointsSvc, clusterInfoSvc
}

func setupLaunchUpdater(testRepairer *PipelineUpdater, waitForStablization bool) {
	// Since we will be bypassing the launchUpdater, do manual launch here
	go testRepairer.run()
}

func setupDetailedMocking(testLogger *log.CommonLogger,
	pipelineMock *common.PipelineFactory,
	replSpecSvcMock *service_def.ReplicationSpecSvc,
	xdcrTopologyMock *service_def.XDCRCompTopologySvc,
	isKVNode bool,
	remoteClusterMock *service_def.RemoteClusterSvc,
	pipelineMgr *PipelineManager,
	testRepairer *PipelineUpdater,
	testReplicationStatus *replicationStatus.ReplicationStatus,
	testTopic string,
	testReplicationSettings *metadata.ReplicationSettings,
	testReplicationSpec *metadata.ReplicationSpecification,
	testRemoteClusterRef *metadata.RemoteClusterReference,
	testPipeline *common.Pipeline,
	uiLogSvc *service_def.UILogSvc,
	replStatusMock *replicationStatusMock.ReplicationStatusIface,
	ckptMock *service_def.CheckpointsService,
	clusterInfoSvc *service_def.ClusterInfoSvc) {

	testReplicationStatus.SetUpdater(testRepairer)
	replSpecSvcMock.On("GetDerivedObj", testTopic).Return(testReplicationStatus, nil)
	replSpecSvcMock.On("ReplicationSpec", testTopic).Return(testReplicationSpec, nil)
	replSpecSvcMock.On("SetDerivedObj", testTopic, mock.Anything).Return(nil)

	xdcrTopologyMock.On("IsKVNode").Return(isKVNode, nil)

	remoteClusterMock.On("RemoteClusterByUuid", "", false).Return(testRemoteClusterRef, nil)
	remoteClusterMock.On("RemoteClusterByUuid", "", true).Return(testRemoteClusterRef, nil)
	remoteClusterMock.On("ValidateRemoteCluster", testRemoteClusterRef).Return(nil)

	var emptyNozzles map[string]commonReal.Nozzle
	testPipeline.On("Sources").Return(emptyNozzles)
	// Test pipeline running test
	testPipeline.On("State").Return(commonReal.Pipeline_Running)
	testPipeline.On("InstanceId").Return(testTopic)
	testPipeline.On("SetProgressRecorder", mock.AnythingOfType("common.PipelineProgressRecorder")).Return(nil)
	emptyMap := make(base.ErrorMap)
	testPipeline.On("Start", mock.Anything).Return(emptyMap)
	testPipeline.On("Stop", mock.Anything).Return(emptyMap)
	testPipeline.On("Specification").Return(testReplicationSpec)
	testPipeline.On("Topic").Return(testTopic)

	pipelineMock.On("NewPipeline", testTopic, mock.AnythingOfType("common.PipelineProgressRecorder")).Return(testPipeline, nil)

	uiLogSvc.On("Write", mock.Anything).Return(nil)

	ckptMock.On("DelCheckpointsDocs", mock.Anything).Return(nil)

	pipelineMgr.checkpoint_svc = ckptMock

	// Mock that it's spock and up
	clusterInfoSvc.On("IsClusterCompatible", mock.Anything, mock.Anything).Return(true, nil)
}

/**
 * Generic mocking framework that lets updater run and have the cluster ready to receive update
 * commands for testing
 */
func setupGenericMocking(testLogger *log.CommonLogger,
	pipelineMock *common.PipelineFactory,
	replSpecSvcMock *service_def.ReplicationSpecSvc,
	xdcrTopologyMock *service_def.XDCRCompTopologySvc,
	remoteClusterMock *service_def.RemoteClusterSvc,
	pipelineMgr *PipelineManager,
	testRepairer *PipelineUpdater,
	testReplicationStatus *replicationStatus.ReplicationStatus,
	testTopic string,
	testReplicationSettings *metadata.ReplicationSettings,
	testReplicationSpec *metadata.ReplicationSpecification,
	testRemoteClusterRef *metadata.RemoteClusterReference,
	testPipeline *common.Pipeline,
	uiLogSvc *service_def.UILogSvc,
	replStatusMock *replicationStatusMock.ReplicationStatusIface,
	ckptSvc *service_def.CheckpointsService,
	clusterInfoSvc *service_def.ClusterInfoSvc) {

	setupDetailedMocking(testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, true, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptSvc, clusterInfoSvc)

	return
}

func setupReplStatusMock(replMock *replicationStatusMock.ReplicationStatusIface,
	testReplicationSpec *metadata.ReplicationSpecification) {
	replMock.On("Spec").Return(testReplicationSpec)

}

func TestPipelineMgrRemoveReplicationStatus(t *testing.T) {
	fmt.Println("============== Test case start: TestPipelineMgrRemoveReplicationStatus =================")
	assert := assert.New(t)

	testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptMock, clusterInfoSvc := setupBoilerPlate()

	setupGenericMocking(testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptMock, clusterInfoSvc)

	setupMockPipelineMgr(replSpecSvcMock, testReplicationSettings, testTopic, testRepairer, xdcrTopologyMock, uiLogSvc)

	pipelineMgr.GetOrCreateReplicationStatus("testTopic", nil)

	setupLaunchUpdater(testRepairer, true)
	assert.Equal(uint64(0), atomic.LoadUint64(&testRepairer.runCounter))

	pipelineMgr.RemoveReplicationStatus("testTopic")

	// 0 once it's stopped
	assert.Equal(uint64(0), atomic.LoadUint64(&testRepairer.runCounter))

	fmt.Println("============== Test case end: TestPipelineMgrRemoveReplicationStatus =================")
}

func TestGetAllReplications(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestGetAllReplicants =================")
	_, _, replSpecSvcMock, _, _,
		pipelineMgr, _, _, _,
		_, _, _, _, _, _, _, _ := setupBoilerPlate()

	var emptySlice []string
	replSpecSvcMock.On("AllReplicationSpecIds").Return(emptySlice, nil)

	assert.NotNil(pipelineMgr.AllReplications())
	fmt.Println("============== Test case end: TestGetAllReplicants =================")
}

/**
 * Tests the path where it launches updater on a KV node
 * Runs pipelineMgr.update() twice in a row serially to ensure that it can handle multiple updates
 */
func TestUpdateSerially(t *testing.T) {
	assert := assert.New(t)

	fmt.Println("============== Test case start: TestUpdateSerially =================")
	testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptMock, clusterInfoSvc := setupBoilerPlate()

	setupGenericMocking(testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptMock, clusterInfoSvc)

	setupLaunchUpdater(testRepairer, false)

	// Once we launch the go routine, immediately launch a manual run... it should register
	fmt.Println("========== Launching first update ==========")
	assert.Nil(pipelineMgr.Update(testTopic, nil))
	// sleep to make sure the request is honored and executed
	time.Sleep(time.Duration(1) * time.Second)
	assert.Equal(uint64(1), atomic.LoadUint64(&testRepairer.runCounter))
	assert.True(pipelineMgr.GetLastUpdateResult(testTopic))

	fmt.Println("========== Launching second update and third update serially ==========")
	// Now, we will launch 2 manual updates in a row, and they must be honored
	assert.Nil(pipelineMgr.Update(testTopic, nil))
	assert.Nil(pipelineMgr.Update(testTopic, nil))

	// sleep to make sure the requests are honored and executed
	time.Sleep(time.Duration(1) * time.Second)
	assert.Equal(uint64(3), atomic.LoadUint64(&testRepairer.runCounter))
	assert.True(pipelineMgr.GetLastUpdateResult(testTopic))

	fmt.Println("============== Test case end: TestUpdateSerially =================")
}

/**
 * Waits for update to finish stablizing
 * Then executes update with an injected error.
 * Clears the error
 * Watch for the error to clear to ensure that it runs
 */
func TestUpdateErrorInjection(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestUpdateErrorInjection =================")
	testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptMock, clusterInfoSvc := setupBoilerPlate()

	setupGenericMocking(testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptMock, clusterInfoSvc)

	setupLaunchUpdater(testRepairer, true)
	assert.Equal(uint64(0), atomic.LoadUint64(&testRepairer.runCounter))

	// Once we launch the go routine, immediately launch a manual run... it should register
	fmt.Println("========== Launching first update ==========")
	assert.Nil(pipelineMgr.Update(testTopic, nil))
	// sleep to make sure the request is honored and executed
	time.Sleep(time.Duration(1) * time.Second)
	assert.Equal(uint64(1), atomic.LoadUint64(&testRepairer.runCounter))
	assert.True(pipelineMgr.GetLastUpdateResult(testTopic))

	fmt.Println("========== Launching second update with injected error ==========")
	atomic.StoreInt32(&testRepairer.testInjectionError, pipelineUpdaterErrInjOfflineFail)
	testRepairer.testCustomScheduleTime = time.Second * time.Duration(1)
	assert.Nil(pipelineMgr.Update(testTopic, nil))
	time.Sleep(time.Duration(1) * time.Second)
	// clear the error and check
	atomic.StoreInt32(&testRepairer.testInjectionError, pipelineUpdaterErrInjNil)
	// we shouldn't have re-executed
	assert.Equal(uint64(1), atomic.LoadUint64(&testRepairer.runCounter))
	// A timer should have been scheduled
	assert.False(testRepairer.isScheduledTimerNil())

	time.Sleep(time.Duration(2) * time.Second)
	// Timer would have fired by now
	assert.True(testRepairer.isScheduledTimerNil())
	assert.Equal(uint64(2), atomic.LoadUint64(&testRepairer.runCounter))

	fmt.Println("============== Test case end: TestUpdateErrorInjection =================")
}

/**
 * Waits for update to finish stablizing
 * Then executes update with an injected error.
 * Clears the error while error wait is happening
 * Pretend an error occured while error wait is happening - should not update, and piggy back
 * Run a manual run while error is waiting - should trigger a run right away
 */
func TestUpdateErrorInjection2(t *testing.T) {

	assert := assert.New(t)
	fmt.Println("============== Test case start: TestUpdateErrorInjection2 =================")
	testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptMock, clusterInfoSvc := setupBoilerPlate()

	setupGenericMocking(testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptMock, clusterInfoSvc)

	setupLaunchUpdater(testRepairer, true)
	assert.Equal(uint64(0), atomic.LoadUint64(&testRepairer.runCounter))

	// Once we launch the go routine, immediately launch a manual run... it should register
	fmt.Println("========== Launching first update ==========")
	assert.Nil(pipelineMgr.Update(testTopic, nil))
	// sleep to make sure the request is honored and executed
	time.Sleep(time.Duration(1) * time.Second)
	assert.Equal(uint64(1), atomic.LoadUint64(&testRepairer.runCounter))
	assert.True(pipelineMgr.GetLastUpdateResult(testTopic))

	fmt.Println("========== Launching second update with injected error ==========")
	atomic.StoreInt32(&testRepairer.testInjectionError, pipelineUpdaterErrInjOfflineFail)
	testRepairer.testCustomScheduleTime = time.Second * time.Duration(3)
	assert.Nil(pipelineMgr.Update(testTopic, nil))
	time.Sleep(time.Duration(1) * time.Second)
	// clear the error and check
	atomic.StoreInt32(&testRepairer.testInjectionError, pipelineUpdaterErrInjNil)
	// we shouldn't have re-executed
	assert.Equal(uint64(1), atomic.LoadUint64(&testRepairer.runCounter))
	// A timer should have been scheduled
	assert.False(testRepairer.isScheduledTimerNil())

	fmt.Println("========== Launching third automatic error update ==========")
	assert.Nil(pipelineMgr.Update(testTopic, errors.New("Random error")))
	// Should have piggy back off of the original testCustomScheduleTime
	assert.False(testRepairer.isScheduledTimerNil())
	time.Sleep(time.Duration(3) * time.Second)
	// Timer would have fired by now
	assert.True(testRepairer.isScheduledTimerNil())
	// We should have a total of 3 runs -> 1 initialization run + 1 first update + 1 update reschedule
	// The random error injection should be counted towards the single 1 update reschedule
	assert.Equal(uint64(2), atomic.LoadUint64(&testRepairer.runCounter))

	fmt.Println("============== Test case end: TestUpdateErrorInjection2 =================")
}

///**
// * calls 3 update()'s concurrently
// */
//func TestUpdateThriceParallely(t *testing.T) {
//	assert := assert.New(t)
//	fmt.Println("============== Test case start: TestUpdateThriceParallely =================")
//	testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
//		waitGrp, pipelineMgr, testRepairer, testReplicationStatus, testTopic,
//		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
//		ckptMock, clusterInfoSvc := setupBoilerPlate()
//
//	setupGenericMocking(testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
//		waitGrp, pipelineMgr, testRepairer, testReplicationStatus, testTopic,
//		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
//		ckptMock, clusterInfoSvc)
//
//	setupLaunchUpdater(testRepairer, true)
//	assert.Equal(uint64(0), atomic.LoadUint64(&testRepairer.runCounter))
//
//	fmt.Println("========== Launching first and second and third updates parallely ==========")
//	// As of second update, we already have a updater created, so return a valid testReplicationStatus
//	// The first update will be picked up by the channel and executed
//	// The second update will be queued
//	// The third update will be considered a batch of the second and get rejected
//	go pipelineMgr.Update(testTopic, nil)
//	go pipelineMgr.Update(testTopic, nil)
//	go pipelineMgr.Update(testTopic, nil)
//
//	// We should honor all requests
//	time.Sleep(time.Duration(3) * time.Second)
//	// 1 init run, and 2 more manual runs (3rd go statement gets rejected)
//	// TODO - this seems racey and fails about 5% of the time
//	assert.Equal(uint64(2), atomic.LoadUint64(&testRepairer.runCounter))
//
//	fmt.Println("============== Test case end: TestUpdateThriceParallely =================")
//}

/**
 * When in a good state, a single error should trigger a run immediately
 */
func TestUpdateDoubleError(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestUpdateDoubleError =================")
	testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptMock, clusterInfoSvc := setupBoilerPlate()

	setupGenericMocking(testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptMock, clusterInfoSvc)

	setupLaunchUpdater(testRepairer, true)
	assert.Equal(uint64(0), atomic.LoadUint64(&testRepairer.runCounter))

	fmt.Println("========== Launching first update with an error ==========")
	pipelineMgr.Update(testTopic, errors.New("TestError"))
	assert.True(testRepairer.isScheduledTimerNil())
	time.Sleep(time.Duration(1) * time.Second)
	assert.Equal(uint64(1), atomic.LoadUint64(&testRepairer.runCounter))

	fmt.Println("========== Launching second update with an error ==========")
	// Since last error was "fixed", this one should execute right away
	pipelineMgr.Update(testTopic, errors.New("TestError"))
	assert.True(testRepairer.isScheduledTimerNil())
	time.Sleep(time.Duration(1) * time.Second)
	assert.Equal(uint64(2), atomic.LoadUint64(&testRepairer.runCounter))

	fmt.Println("============== Test case end: TestUpdateDoubleError =================")
}

// Used for testing just the pipelineUpdater
func setupMockPipelineMgr(replSpecSvcMock *service_def.ReplicationSpecSvc,
	testReplicationSettings *metadata.ReplicationSettings,
	testTopic string,
	testRepairer *PipelineUpdater,
	xdcrTopologyMock *service_def.XDCRCompTopologySvc,
	uiLogSvcMock *service_def.UILogSvc) *PipelineMgrMock.Pipeline_mgr_iface {

	pmMock := &PipelineMgrMock.Pipeline_mgr_iface{}

	emptyMap := make(base.ErrorMap)
	pmMock.On("StopPipeline", mock.Anything).Return(emptyMap)
	pmMock.On("StartPipeline", mock.Anything).Return(emptyMap)
	pmMock.On("GetReplSpecSvc").Return(replSpecSvcMock)
	pmMock.On("GetXDCRTopologySvc").Return(xdcrTopologyMock)
	pmMock.On("GetLogSvc").Return(uiLogSvcMock)
	testReplicationSettings.Active = true

	testRepairer.pipelineMgr = pmMock
	return pmMock
}

func TestUpdater(t *testing.T) {
	fmt.Println("============== Test case start: TestUpdater =================")
	assert := assert.New(t)
	testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptMock, clusterInfoSvc := setupBoilerPlate()

	setupGenericMocking(testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptMock, clusterInfoSvc)

	setupMockPipelineMgr(replSpecSvcMock, testReplicationSettings, testTopic, testRepairer, xdcrTopologyMock, uiLogSvc)

	fmt.Printf("Trying to run actual startPipeline... may have %v seconds delay\n", testRepairer.testCustomScheduleTime)
	errMap := testRepairer.update()
	assert.Equal(0, len(errMap))
	assert.Equal(uint64(1), atomic.LoadUint64(&testRepairer.runCounter))
	fmt.Println("============== Test case end: TestUpdater =================")
}

func TestUpdaterRun(t *testing.T) {
	fmt.Println("============== Test case start: TestUpdaterRun =================")
	assert := assert.New(t)
	testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptMock, clusterInfoSvc := setupBoilerPlate()

	setupGenericMocking(testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptMock, clusterInfoSvc)

	setupMockPipelineMgr(replSpecSvcMock, testReplicationSettings, testTopic, testRepairer, xdcrTopologyMock, uiLogSvc)

	fmt.Printf("Trying to run actual startPipeline... may have %v seconds delay\n", testRepairer.testCustomScheduleTime)
	setupLaunchUpdater(testRepairer, true)
	assert.Equal(uint64(0), atomic.LoadUint64(&testRepairer.runCounter))

	testRepairer.stop()
	assert.Equal(uint64(0), atomic.LoadUint64(&testRepairer.runCounter))

	fmt.Println("============== Test case end: TestUpdaterRun =================")
}

func TestUpdaterSendErrDuringCooldown(t *testing.T) {
	fmt.Println("============== Test case start: TestUpdaterSendErrDuringCooldown =================")
	assert := assert.New(t)
	testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptMock, clusterInfoSvc := setupBoilerPlate()

	setupGenericMocking(testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptMock, clusterInfoSvc)

	setupMockPipelineMgr(replSpecSvcMock, testReplicationSettings, testTopic, testRepairer, xdcrTopologyMock, uiLogSvc)

	fmt.Printf("Trying to run actual startPipeline... may have %v seconds delay\n", testRepairer.testCustomScheduleTime)
	setupLaunchUpdater(testRepairer, true)
	assert.Equal(uint64(0), atomic.LoadUint64(&testRepairer.runCounter))

	fmt.Println("Firing off 2 errors concurrently - one should be soaked up")
	// Inject an error to introduce cooldown
	atomic.StoreInt32(&testRepairer.testInjectionError, pipelineUpdaterErrInjOfflineFail)
	testRepairer.testCustomScheduleTime = time.Second * time.Duration(1)
	// Fire off 2 errors - one should fire right away and one be soaked up
	go testRepairer.refreshPipelineDueToErr(errors.New("TestError"))
	go testRepairer.refreshPipelineDueToErr(errors.New("TestError"))
	time.Sleep(time.Duration(1) * time.Second)
	// Clear error to let it go through
	atomic.StoreInt32(&testRepairer.testInjectionError, pipelineUpdaterErrInjNil)

	time.Sleep(testRepairer.testCustomScheduleTime + time.Duration(1)*time.Second)
	assert.Equal(uint64(1), atomic.LoadUint64(&testRepairer.runCounter))

	fmt.Println("Firing off 1 error - should execute asap")
	// Afterwards, we should no longer soak
	go testRepairer.refreshPipelineDueToErr(errors.New("TestError"))
	time.Sleep(testRepairer.testCustomScheduleTime + time.Duration(1)*time.Second)
	assert.Equal(uint64(2), atomic.LoadUint64(&testRepairer.runCounter))

	fmt.Println("============== Test case end: TestUpdaterSendErrDuringCooldown =================")
}

func TestPipelineMgrConcurrentGetOrCreateReplicationStatus(t *testing.T) {
	fmt.Println("============== Test case start: TestPipelineMgrConcurrentGetOrCreateReplicationStatus =================")
	testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptMock, clusterInfoSvc := setupBoilerPlate()

	setupGenericMocking(testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptMock, clusterInfoSvc)

	setupMockPipelineMgr(replSpecSvcMock, testReplicationSettings, testTopic, testRepairer, xdcrTopologyMock, uiLogSvc)

	go pipelineMgr.GetOrCreateReplicationStatus("testTopic", nil)
	go pipelineMgr.GetOrCreateReplicationStatus("testTopic", nil)

	fmt.Println("============== Test case end: TestPipelineMgrConcurrentGetOrCreateReplicationStatus =================")
}

func TestUpdaterCompressionErr(t *testing.T) {
	fmt.Println("============== Test case start: TestUpdaterCompressionErr =================")
	assert := assert.New(t)
	testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptMock, clusterInfoSvc := setupBoilerPlate()

	setupGenericMocking(testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptMock, clusterInfoSvc)

	setupMockPipelineMgr(replSpecSvcMock, testReplicationSettings, testTopic, testRepairer, xdcrTopologyMock, uiLogSvc)

	// At this time, no customSettings
	origSettings := testRepairer.rep_status.SettingsMap()
	assert.NotNil(origSettings)
	assert.Equal((int)(base.CompressionTypeSnappy), origSettings[metadata.CompressionTypeKey])

	// Pretend the last error was because Compression was not supported
	testRepairer.currentErrors.AddError("UnitTest", base.ErrorCompressionNotSupported)

	testRepairer.disableCompression(base.ErrorCompressionNotSupported)

	// Modified settings
	tempSettings := testRepairer.rep_status.SettingsMap()
	assert.Equal((int)(base.CompressionTypeForceUncompress), tempSettings[metadata.CompressionTypeKey])

	errMap := testRepairer.update()
	assert.Equal(0, len(errMap))

	checkSettings := testRepairer.rep_status.SettingsMap()
	assert.Equal((int)(base.CompressionTypeSnappy), checkSettings[metadata.CompressionTypeKey])
	fmt.Println("============== Test case end: TestUpdaterCompressionErr =================")
}

func TestReplSettingsRevision(t *testing.T) {
	fmt.Println("============== Test case start: TestReplSettingsRevision =================")
	assert := assert.New(t)
	testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptMock, clusterInfoSvc := setupBoilerPlate()

	setupGenericMocking(testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptMock, clusterInfoSvc)

	setupReplStatusMock(replStatusMock, testReplicationSpec)

	testRepairer.rep_status = replStatusMock
	assert.False(testRepairer.replSpecSettingsHelper.HasChanged())

	// Record a revision
	testRepairer.replSpecSettingsHelper.Record()

	// Recalling it should not change
	assert.False(testRepairer.replSpecSettingsHelper.HasChanged())

	// Say something changed underneath
	testReplicationSpec.Settings.BatchSize += 1
	testReplicationSpec.Settings.Revision = 2
	replStatusMock2 := &replicationStatusMock.ReplicationStatusIface{}
	setupReplStatusMock(replStatusMock2, testReplicationSpec)
	testRepairer.rep_status = replStatusMock2

	// Now it should say have changed
	assert.True(testRepairer.replSpecSettingsHelper.HasChanged())

	// Once clear,ed should not say have changed
	testRepairer.replSpecSettingsHelper.Clear()
	assert.False(testRepairer.replSpecSettingsHelper.HasChanged())

	fmt.Println("============== Test case start: TestReplSettingsRevision =================")
}

func TestUpdaterCompressionErrRevChanged(t *testing.T) {
	fmt.Println("============== Test case start: TestUpdaterCompressionErrRevChanged =================")
	assert := assert.New(t)
	testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptMock, clusterInfoSvc := setupBoilerPlate()

	setupGenericMocking(testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptMock, clusterInfoSvc)

	setupMockPipelineMgr(replSpecSvcMock, testReplicationSettings, testTopic, testRepairer, xdcrTopologyMock, uiLogSvc)

	// At this time, no customSettings
	origSettings := testRepairer.rep_status.SettingsMap()
	assert.NotNil(origSettings)
	assert.Equal((int)(base.CompressionTypeSnappy), origSettings[metadata.CompressionTypeKey])

	// Inject an error to mark the revision
	atomic.StoreInt32(&testRepairer.testInjectionError, pipelineUpdaterErrInjOfflineFail)

	// Running update should fail
	errMap := testRepairer.update()
	assert.NotEqual(0, len(errMap))

	// Also, add the compression error as part of the errorMap.
	testRepairer.currentErrors.AddError("Injected", base.ErrorCompressionNotSupported)
	assert.True(testRepairer.currentErrors.ContainsError(base.ErrorCompressionNotSupported, true))

	// Modify settings in the ReplicationSpec
	origSettings[metadata.BatchCountKey] = 1024 // magic

	// Need to recreate a new mock so that we'll return this new spec and new settings
	testLogger2, pipelineMock2, replSpecSvcMock2, xdcrTopologyMock2, remoteClusterMock2,
		pipelineMgr2, testRepairer2, testReplicationStatus2, testTopic2,
		testReplicationSettings2, testReplicationSpec2, testRemoteClusterRef2, testPipeline2, uiLogSvc2, replStatusMock2,
		ckptMock2, clusterInfoSvc2 := setupBoilerPlate()

	testReplicationSpec2.Settings.UpdateSettingsFromMap(origSettings)

	setupGenericMocking(testLogger2, pipelineMock2, replSpecSvcMock2, xdcrTopologyMock2, remoteClusterMock2,
		pipelineMgr2, testRepairer2, testReplicationStatus2, testTopic2,
		testReplicationSettings2, testReplicationSpec2, testRemoteClusterRef2, testPipeline2, uiLogSvc2, replStatusMock2,
		ckptMock2, clusterInfoSvc2)

	testRepairer.rep_status = testReplicationStatus2

	// Reset injected error
	atomic.StoreInt32(&testRepairer.testInjectionError, pipelineUpdaterErrInjNil)

	// Now pipeline updater will check the spec and see the revision has changed
	errMap = testRepairer.update()
	assert.Equal(0, len(errMap))

	// revision should have changed due to the new replicationSettings - meaning that customSettings is cleared
	assert.Nil(testRepairer.replSpecSettingsHelper.GetCurrentSettingsRevision())

	checkSettings := testRepairer.rep_status.SettingsMap()
	assert.Equal((int)(base.CompressionTypeSnappy), checkSettings[metadata.CompressionTypeKey])
	fmt.Println("============== Test case end: TestUpdaterCompressionErrRevChanged =================")
}

func TestFilterCompressionErrorParsing(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestFilterCompressionErrorParsing=================")
	testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptMock, clusterInfoSvc := setupBoilerPlate()

	setupGenericMocking(testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptMock, clusterInfoSvc)

	setupLaunchUpdater(testRepairer, true)
	assert.Equal(uint64(0), atomic.LoadUint64(&testRepairer.runCounter))

	// This is what happens in replicationManager
	dummyErrMap := make(base.ErrorMap)
	dummyErrMap["testPart"] = fmt.Errorf("Dummy error1")
	dummyErrMap["testPart2"] = fmt.Errorf("Dummy error1")
	dummyErrMap["routerId"] = base.ErrorCompressionUnableToInflate

	assert.Equal(uint8(0), testRepairer.testCurrentDisabledFeatures&disabledCompression)

	pipelineMgr.Update(testTopic, errors.New(base.FlattenErrorMap(dummyErrMap)))
	assert.True(testRepairer.isScheduledTimerNil())
	time.Sleep(time.Duration(1) * time.Second)
	assert.Equal(uint64(1), atomic.LoadUint64(&testRepairer.runCounter))

	assert.NotEqual(uint8(0), testRepairer.testCurrentDisabledFeatures&disabledCompression)

	fmt.Println("============== Test case end: TestFilterCompressionErrorParsing =================")
}

func TestCleanupPipeline(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestCleanupPipeline=================")
	testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptMock, clusterInfoSvc := setupBoilerPlate()

	setupGenericMocking(testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptMock, clusterInfoSvc)

	setupLaunchUpdater(testRepairer, true)
	assert.Equal(uint64(0), atomic.LoadUint64(&testRepairer.runCounter))

	pipelineMgr.CleanupPipeline(testTopic)

	fmt.Println("============== Test case end: TestCleanupPipeline =================")
}

func TestRaiseWarningAtBeginning(t *testing.T) {
	fmt.Println("============== Test case start: TestRaiseWarningAtBeginning =================")
	assert := assert.New(t)

	testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptMock, clusterInfoSvc := setupBoilerPlate()

	setupGenericMocking(testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline, uiLogSvc, replStatusMock,
		ckptMock, clusterInfoSvc)

	setupLaunchUpdater(testRepairer, true)
	assert.Equal(uint64(0), atomic.LoadUint64(&testRepairer.runCounter))

	pipelineMgr.Update(testTopic, base.ErrorPipelineRestartDueToClusterConfigChange)
	time.Sleep(time.Duration(1) * time.Second)

	assert.Equal(uint64(1), atomic.LoadUint64(&testRepairer.runCounter))
	assert.True(uiLogSvc.AssertNumberOfCalls(t, "Write", 1))
	fmt.Println("============== Test case end: TestRaiseWarningAtBeginning =================")
}
