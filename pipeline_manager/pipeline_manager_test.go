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
	PipelineMgrMock "github.com/couchbase/goxdcr/pipeline_manager/mocks"
	service_def "github.com/couchbase/goxdcr/service_def/mocks"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
	//	http "net/http"
	//	_ "net/http/pprof"
	"sync"
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
	waitGrp := &sync.WaitGroup{}

	pipelineMgr := PipelineManager{
		pipeline_factory:   pipelineMock,
		repl_spec_svc:      replSpecSvcMock,
		xdcr_topology_svc:  xdcrTopologyMock,
		remote_cluster_svc: remoteClusterMock,
		child_waitGrp:      waitGrp}
	// end boiler plate

	replSpecSvcMock.On("AllReplicationSpecIds").Return(nil, errors.New("Injected empty error"))
	pipelineMgr.StopAllUpdaters()
}

func setupBoilerPlate() (*log.CommonLogger,
	*common.PipelineFactory,
	*service_def.ReplicationSpecSvc,
	*service_def.XDCRCompTopologySvc,
	*service_def.RemoteClusterSvc,
	*sync.WaitGroup,
	*PipelineManager,
	*PipelineUpdater,
	*replicationStatus.ReplicationStatus,
	string,
	*metadata.ReplicationSettings,
	*metadata.ReplicationSpecification,
	*metadata.RemoteClusterReference,
	*common.Pipeline) {

	testLogger := log.NewLogger("testLogger", log.DefaultLoggerContext)
	pipelineMock := &common.PipelineFactory{}
	replSpecSvcMock := &service_def.ReplicationSpecSvc{}
	xdcrTopologyMock := &service_def.XDCRCompTopologySvc{}
	remoteClusterMock := &service_def.RemoteClusterSvc{}
	waitGrp := &sync.WaitGroup{}

	pipelineMgr := &PipelineManager{
		pipeline_factory:   pipelineMock,
		repl_spec_svc:      replSpecSvcMock,
		xdcr_topology_svc:  xdcrTopologyMock,
		remote_cluster_svc: remoteClusterMock,
		logger:             testLogger,
		child_waitGrp:      waitGrp}

	// Some things needed for pipelinemgr
	testTopic := "testTopic"

	// needed for replicationStatus
	testRepairer := &PipelineUpdater{pipeline_name: testTopic,
		fin_ch:                 make(chan bool, 1),
		done_ch:                make(chan bool, 1),
		update_now_ch:          make(chan bool, 1),
		update_err_ch:          make(chan error, 1),
		pipelineMgr:            pipelineMgr,
		lastSuccessful:         true,
		testCustomScheduleTime: (time.Duration(1) * time.Second),
		waitGrp:                waitGrp,
		logger:                 testLogger}

	// setting up replicationStatus and replicationSpecService
	var repStatusMtx sync.RWMutex
	testReplicationStatus := &replicationStatus.ReplicationStatus{
		Lock:             &repStatusMtx,
		SpecId:           testTopic,
		Logger:           testLogger,
		Pipeline_updater: testRepairer,
		Obj_pool:         base.NewMCRequestPool(testTopic, testLogger)}

	testReplicationSettings := &metadata.ReplicationSettings{FailureRestartInterval: 10}
	testReplicationSpec := &metadata.ReplicationSpecification{}
	testReplicationSpec.Settings = testReplicationSettings
	testRepairer.rep_status = testReplicationStatus

	/**
	 * This should prevent SetDerivedObj -> RuntimeStatus -> rs.Spec() -> rs.Spec_getter going through
	 * a double mock causing a double lock
	 */
	specGetterFxLiteral := func(specId string) (*metadata.ReplicationSpecification, error) { return testReplicationSpec, nil }
	testReplicationStatus.Spec_getter = specGetterFxLiteral

	testRemoteClusterRef := &metadata.RemoteClusterReference{}

	testPipeline := &common.Pipeline{}

	return testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		waitGrp, pipelineMgr, testRepairer, testReplicationStatus, testTopic, testReplicationSettings,
		testReplicationSpec, testRemoteClusterRef, testPipeline
}

func setupLaunchUpdater(testRepairer *PipelineUpdater, waitForStablization bool) {
	// Since we will be bypassing the launchUpdater, do manual launch here
	go testRepairer.run()
	testRepairer.waitGrp.Add(1)
	// When testRepairer starts, it launches one automatically
	// sleep to make sure the request is honored and executed
	if waitForStablization {
		time.Sleep(time.Duration(1) * time.Second)
	}
}

func setupDetailedMocking(testLogger *log.CommonLogger,
	pipelineMock *common.PipelineFactory,
	replSpecSvcMock *service_def.ReplicationSpecSvc,
	xdcrTopologyMock *service_def.XDCRCompTopologySvc,
	isKVNode bool,
	remoteClusterMock *service_def.RemoteClusterSvc,
	waitGrp *sync.WaitGroup,
	pipelineMgr *PipelineManager,
	testRepairer *PipelineUpdater,
	testReplicationStatus *replicationStatus.ReplicationStatus,
	testTopic string,
	testReplicationSettings *metadata.ReplicationSettings,
	testReplicationSpec *metadata.ReplicationSpecification,
	testRemoteClusterRef *metadata.RemoteClusterReference,
	testPipeline *common.Pipeline) {

	testReplicationStatus.Pipeline_updater = testRepairer
	replSpecSvcMock.On("GetDerivedObj", testTopic).Return(testReplicationStatus, nil)
	replSpecSvcMock.On("ReplicationSpec", testTopic).Return(testReplicationSpec, nil)
	replSpecSvcMock.On("SetDerivedObj", testTopic, mock.Anything).Return(nil)

	xdcrTopologyMock.On("IsKVNode").Return(isKVNode, nil)

	remoteClusterMock.On("RemoteClusterByUuid", "", false).Return(testRemoteClusterRef, nil)
	remoteClusterMock.On("ValidateRemoteCluster", testRemoteClusterRef).Return(nil)

	pipelineMock.On("NewPipeline", testTopic, mock.AnythingOfType("common.PipelineProgressRecorder")).Return(&testPipeline, nil)
	var emptyNozzles map[string]commonReal.Nozzle
	testPipeline.On("Sources").Return(emptyNozzles)
	// Test pipeline running test
	testPipeline.On("State").Return(commonReal.Pipeline_Running)
	testPipeline.On("InstanceId").Return(testTopic)
	testPipeline.On("SetProgressRecorder", mock.AnythingOfType("common.PipelineProgressRecorder")).Return(nil)
	testStatusMap := testReplicationStatus.SettingsMap()
	testPipeline.On("Start", testStatusMap).Return(nil)
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
	waitGrp *sync.WaitGroup,
	pipelineMgr *PipelineManager,
	testRepairer *PipelineUpdater,
	testReplicationStatus *replicationStatus.ReplicationStatus,
	testTopic string,
	testReplicationSettings *metadata.ReplicationSettings,
	testReplicationSpec *metadata.ReplicationSpecification,
	testRemoteClusterRef *metadata.RemoteClusterReference,
	testPipeline *common.Pipeline) {

	setupDetailedMocking(testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, true, remoteClusterMock,
		waitGrp, pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline)

	return
}

func TestGetAllReplications(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestGetAllReplicants =================")
	_, _, replSpecSvcMock, _, _,
		_, pipelineMgr, _, _, _,
		_, _, _, _ := setupBoilerPlate()

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
		waitGrp, pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline := setupBoilerPlate()

	setupGenericMocking(testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		waitGrp, pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline)

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
		waitGrp, pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline := setupBoilerPlate()

	setupGenericMocking(testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		waitGrp, pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline)

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
		waitGrp, pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline := setupBoilerPlate()

	setupGenericMocking(testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		waitGrp, pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline)

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

/**
 * calls 3 update()'s concurrently
 */
func TestUpdateThriceParallely(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestUpdateThriceParallely =================")
	testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		waitGrp, pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline := setupBoilerPlate()

	setupGenericMocking(testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		waitGrp, pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline)

	setupLaunchUpdater(testRepairer, true)
	assert.Equal(uint64(0), atomic.LoadUint64(&testRepairer.runCounter))

	fmt.Println("========== Launching first and second and third updates parallely ==========")
	// As of second update, we already have a updater created, so return a valid testReplicationStatus
	// The first update will be picked up by the channel and executed
	// The second update will be queued
	// The third update will be considered a batch of the second and get rejected
	go pipelineMgr.Update(testTopic, nil)
	go pipelineMgr.Update(testTopic, nil)
	go pipelineMgr.Update(testTopic, nil)

	// We should honor all requests
	time.Sleep(time.Duration(3) * time.Second)
	// 1 init run, and 2 more manual runs (3rd go statement gets rejected)
	// TODO - this seems racey and fails about 5% of the time
	assert.Equal(uint64(2), atomic.LoadUint64(&testRepairer.runCounter))

	fmt.Println("============== Test case end: TestUpdateThriceParallely =================")
}

/**
 * When in a good state, a single error should trigger a run immediately
 */
func TestUpdateDoubleError(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestUpdateDoubleError =================")
	testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		waitGrp, pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline := setupBoilerPlate()

	setupGenericMocking(testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		waitGrp, pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline)

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
	xdcrTopologyMock *service_def.XDCRCompTopologySvc) *PipelineMgrMock.Pipeline_mgr_iface {

	pmMock := &PipelineMgrMock.Pipeline_mgr_iface{}
	testRepairer.pipelineMgr = pmMock

	pmMock.On("StopPipeline", mock.Anything).Return(nil)
	pmMock.On("StartPipeline", testTopic).Return(nil)
	pmMock.On("GetReplSpecSvc").Return(replSpecSvcMock)
	pmMock.On("GetXDCRTopologySvc").Return(xdcrTopologyMock)
	testReplicationSettings.Active = true

	return pmMock
}

// Used for testing if Start Pipeline returns an error code
func setupMockPipelineMgrWithErrorCode(replSpecSvcMock *service_def.ReplicationSpecSvc,
	testReplicationSettings *metadata.ReplicationSettings,
	testTopic string,
	testRepairer *PipelineUpdater,
	xdcrTopologyMock *service_def.XDCRCompTopologySvc,
	retError error) *PipelineMgrMock.Pipeline_mgr_iface {

	pmMock := &PipelineMgrMock.Pipeline_mgr_iface{}
	testRepairer.pipelineMgr = pmMock

	pmMock.On("StopPipeline", mock.Anything).Return(nil)
	pmMock.On("StartPipeline", testTopic).Return(retError)
	pmMock.On("GetReplSpecSvc").Return(replSpecSvcMock)
	pmMock.On("GetXDCRTopologySvc").Return(xdcrTopologyMock)
	testReplicationSettings.Active = true

	return pmMock
}

func TestUpdater(t *testing.T) {
	fmt.Println("============== Test case start: TestUpdater =================")
	assert := assert.New(t)

	testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		waitGrp, pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline := setupBoilerPlate()

	setupGenericMocking(testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		waitGrp, pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline)

	setupMockPipelineMgr(replSpecSvcMock, testReplicationSettings, testTopic, testRepairer, xdcrTopologyMock)

	fmt.Printf("Trying to run actual startPipeline... may have %v seconds delay\n", testRepairer.testCustomScheduleTime)
	assert.Nil(testRepairer.update())
	assert.Equal(uint64(1), atomic.LoadUint64(&testRepairer.runCounter))
	fmt.Println("============== Test case end: TestUpdater =================")
}

func TestUpdaterRun(t *testing.T) {
	fmt.Println("============== Test case start: TestUpdaterRun =================")
	assert := assert.New(t)

	testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		waitGrp, pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline := setupBoilerPlate()

	setupGenericMocking(testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		waitGrp, pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline)

	setupMockPipelineMgr(replSpecSvcMock, testReplicationSettings, testTopic, testRepairer, xdcrTopologyMock)

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
		waitGrp, pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline := setupBoilerPlate()

	setupGenericMocking(testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		waitGrp, pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline)

	setupMockPipelineMgr(replSpecSvcMock, testReplicationSettings, testTopic, testRepairer, xdcrTopologyMock)

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
		waitGrp, pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline := setupBoilerPlate()

	setupGenericMocking(testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		waitGrp, pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline)

	setupMockPipelineMgr(replSpecSvcMock, testReplicationSettings, testTopic, testRepairer, xdcrTopologyMock)

	go pipelineMgr.GetOrCreateReplicationStatus("testTopic", nil)
	go pipelineMgr.GetOrCreateReplicationStatus("testTopic", nil)

	fmt.Println("============== Test case end: TestPipelineMgrConcurrentGetOrCreateReplicationStatus =================")
}

func TestPipelineMgrRemoveReplicationStatus(t *testing.T) {
	fmt.Println("============== Test case start: TestPipelineMgrRemoveReplicationStatus =================")
	assert := assert.New(t)

	testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		waitGrp, pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline := setupBoilerPlate()

	setupGenericMocking(testLogger, pipelineMock, replSpecSvcMock, xdcrTopologyMock, remoteClusterMock,
		waitGrp, pipelineMgr, testRepairer, testReplicationStatus, testTopic,
		testReplicationSettings, testReplicationSpec, testRemoteClusterRef, testPipeline)

	setupMockPipelineMgr(replSpecSvcMock, testReplicationSettings, testTopic, testRepairer, xdcrTopologyMock)

	pipelineMgr.GetOrCreateReplicationStatus("testTopic", nil)

	setupLaunchUpdater(testRepairer, true)
	assert.Equal(uint64(0), atomic.LoadUint64(&testRepairer.runCounter))

	pipelineMgr.RemoveReplicationStatus("testTopic")

	// 0 once it's stopped
	assert.Equal(uint64(0), atomic.LoadUint64(&testRepairer.runCounter))

	fmt.Println("============== Test case end: TestPipelineMgrRemoveReplicationStatus =================")
}
