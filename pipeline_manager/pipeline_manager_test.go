package pipeline_manager

import (
    "testing"
	"github.com/couchbase/goxdcr/base"
    "github.com/stretchr/testify/assert"
	common "github.com/couchbase/goxdcr/common/mocks"
	commonReal "github.com/couchbase/goxdcr/common"
	service_def "github.com/couchbase/goxdcr/service_def/mocks"
	mock "github.com/stretchr/testify/mock"
	replicationStatus "github.com/couchbase/goxdcr/pipeline"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/log"
	"errors"
	"sync"
	"fmt"
	"time"
)

func TestStopAllUpdaters(t *testing.T) {
	// set-up boiler plate
	pipelineMock := &common.PipelineFactory{}
	replSpecSvcMock	:= &service_def.ReplicationSpecSvc{}
	xdcrTopologyMock := &service_def.XDCRCompTopologySvc{}
	remoteClusterMock := &service_def.RemoteClusterSvc{}
	waitGrp := &sync.WaitGroup{}

	pipelineMgr := PipelineManager {
		pipeline_factory:	pipelineMock,
		repl_spec_svc:		replSpecSvcMock,
		xdcr_topology_svc:	xdcrTopologyMock,
		remote_cluster_svc:	remoteClusterMock,
		child_waitGrp:		waitGrp}
	// end boiler plate

	replSpecSvcMock.On("AllReplicationSpecIds").Return(nil, errors.New("Injected empty error"))
	pipelineMgr.stopAllUpdaters()
}

/**
 * Tests the path of update->updaterObj is nil, so launchUpdater on a non-KV node
 */
func TestUpdateNonKVNode(t *testing.T) {
	// set-up boiler plate
	testLogger := log.NewLogger("testLogger", log.DefaultLoggerContext)
	pipelineMock := &common.PipelineFactory{}
	replSpecSvcMock	:= &service_def.ReplicationSpecSvc{}
	xdcrTopologyMock := &service_def.XDCRCompTopologySvc{}
	remoteClusterMock := &service_def.RemoteClusterSvc{}
	waitGrp := &sync.WaitGroup{}

	pipelineMgr := PipelineManager {
		pipeline_factory:	pipelineMock,
		repl_spec_svc:		replSpecSvcMock,
		xdcr_topology_svc:	xdcrTopologyMock,
		remote_cluster_svc:	remoteClusterMock,
		logger:				testLogger,
		child_waitGrp:		waitGrp}

	assert := assert.New(t)
	// end boiler plate
	

	var cur_err error
	testTopic := "testTopic"

	var repStatusMtx sync.RWMutex
	testReplicationStatus := &replicationStatus.ReplicationStatus {Lock: &repStatusMtx}
	replSpecSvcMock.On("GetDerivedObj", testTopic).Return(testReplicationStatus, nil)

	xdcrTopologyMock.On("IsKVNode").Return(false, nil)

	assert.Nil(pipelineMgr.Update(testTopic, cur_err))

	// sleep 1 second for go routines to finish
	seconds := time.Duration(1)*time.Second
	time.Sleep(seconds)
}

/**
 * Tests the path where it launches updater on a KV node
 * Runs pipelineMgr.update() twice in a row serially to ensure that it can handle multiple updates
 */
func TestUpdateTwiceSerially(t *testing.T) {
	// set-up boiler plate
	testLogger := log.NewLogger("testLogger", log.DefaultLoggerContext)
	pipelineMock := &common.PipelineFactory{}
	replSpecSvcMock	:= &service_def.ReplicationSpecSvc{}
	xdcrTopologyMock := &service_def.XDCRCompTopologySvc{}
	remoteClusterMock := &service_def.RemoteClusterSvc{}
	waitGrp := &sync.WaitGroup{}

	pipelineMgr := PipelineManager {
		pipeline_factory:	pipelineMock,
		repl_spec_svc:		replSpecSvcMock,
		xdcr_topology_svc:	xdcrTopologyMock,
		remote_cluster_svc:	remoteClusterMock,
		logger:				testLogger,
		child_waitGrp:		waitGrp}

	assert := assert.New(t)
	// end boiler plate
	
	// Some things needed for pipelinemgr
	var cur_err error
	testTopic := "testTopic"

	// setting up replicationStatus and replicationSpecService
	var repStatusMtx sync.RWMutex
	testReplicationStatus := &replicationStatus.ReplicationStatus {
		Lock: &repStatusMtx,
		SpecId: testTopic,
		Logger: testLogger,
		Obj_pool: base.NewMCRequestPool(testTopic, testLogger)}

	// start mocking
	// ReplicationSpecSvc used for mocking
	testReplicationSettings := &metadata.ReplicationSettings{FailureRestartInterval: 10}
	testReplicationStatus.Spec_getter = replSpecSvcMock.ReplicationSpec
	testReplicationSpec := &metadata.ReplicationSpecification{}
	testReplicationSpec.Settings = testReplicationSettings

	replSpecSvcMock.On("GetDerivedObj", testTopic).Return(testReplicationStatus, nil)
	replSpecSvcMock.On("ReplicationSpec", testTopic).Return(testReplicationSpec, nil)

	xdcrTopologyMock.On("IsKVNode").Return(true, nil)

	testRemoteClusterRef := &metadata.RemoteClusterReference{}
	remoteClusterMock.On("RemoteClusterByUuid", "", false).Return(testRemoteClusterRef, nil)
	remoteClusterMock.On("ValidateRemoteCluster", testRemoteClusterRef).Return(nil)

	testPipeline := common.Pipeline{}
	pipelineMock.On("NewPipeline", testTopic, mock.AnythingOfType("common.PipelineProgressRecorder")).Return(&testPipeline, nil)
	var emptyNozzles map[string]commonReal.Nozzle
	testPipeline.On("Sources").Return(emptyNozzles)
	// Test pipeline running test
	testPipeline.On("State").Return(commonReal.Pipeline_Running)
	testPipeline.On("InstanceId").Return(testTopic)
	testPipeline.On("SetProgressRecorder", mock.AnythingOfType("common.PipelineProgressRecorder")).Return(nil)
	testStatusMap := testReplicationStatus.SettingsMap()
	testPipeline.On("Start", testStatusMap).Return(nil)
	// end mock

	fmt.Println("========== Launching first update ==========")
	assert.Nil(pipelineMgr.Update(testTopic, cur_err))
	fmt.Println("========== Launching second update ==========")
	assert.Nil(pipelineMgr.Update(testTopic, cur_err))

	// sleep 1 second for go routines to finish
	seconds := time.Duration(1)*time.Second
	time.Sleep(seconds)
}

/**
 * First calls update() to set up the updater
 * Then calls 2 more update()'s concurrently
 */
func TestUpdateTwiceParallely(t *testing.T) {
	// set-up boiler plate
	testLogger := log.NewLogger("testLogger", log.DefaultLoggerContext)
	pipelineMock := &common.PipelineFactory{}
	replSpecSvcMock	:= &service_def.ReplicationSpecSvc{}
	xdcrTopologyMock := &service_def.XDCRCompTopologySvc{}
	remoteClusterMock := &service_def.RemoteClusterSvc{}
	waitGrp := &sync.WaitGroup{}

	pipelineMgr := PipelineManager {
		pipeline_factory:	pipelineMock,
		repl_spec_svc:		replSpecSvcMock,
		xdcr_topology_svc:	xdcrTopologyMock,
		remote_cluster_svc:	remoteClusterMock,
		logger:				testLogger,
		child_waitGrp:		waitGrp}

	assert := assert.New(t)
	// end boiler plate
	
	// Some things needed for pipelinemgr
	var cur_err error
	testTopic := "testTopic"

	// setting up replicationStatus and replicationSpecService
	var repStatusMtx sync.RWMutex
	testReplicationStatus := &replicationStatus.ReplicationStatus {
		Lock: &repStatusMtx,
		SpecId: testTopic,
		Logger: testLogger,
		Obj_pool: base.NewMCRequestPool(testTopic, testLogger)}

	// start mocking
	// ReplicationSpecSvc used for mocking
	testReplicationSettings := &metadata.ReplicationSettings{FailureRestartInterval: 10}
	testReplicationStatus.Spec_getter = replSpecSvcMock.ReplicationSpec
	testReplicationSpec := &metadata.ReplicationSpecification{}
	testReplicationSpec.Settings = testReplicationSettings

	replSpecSvcMock.On("GetDerivedObj", testTopic).Return(testReplicationStatus, nil)
	replSpecSvcMock.On("ReplicationSpec", testTopic).Return(testReplicationSpec, nil)

	xdcrTopologyMock.On("IsKVNode").Return(true, nil)

	testRemoteClusterRef := &metadata.RemoteClusterReference{}
	remoteClusterMock.On("RemoteClusterByUuid", "", false).Return(testRemoteClusterRef, nil)
	remoteClusterMock.On("ValidateRemoteCluster", testRemoteClusterRef).Return(nil)

	testPipeline := common.Pipeline{}
	pipelineMock.On("NewPipeline", testTopic, mock.AnythingOfType("common.PipelineProgressRecorder")).Return(&testPipeline, nil)
	var emptyNozzles map[string]commonReal.Nozzle
	testPipeline.On("Sources").Return(emptyNozzles)
	// Test pipeline running test
	testPipeline.On("State").Return(commonReal.Pipeline_Running)
	testPipeline.On("InstanceId").Return(testTopic)
	testPipeline.On("SetProgressRecorder", mock.AnythingOfType("common.PipelineProgressRecorder")).Return(nil)
	testStatusMap := testReplicationStatus.SettingsMap()
	testPipeline.On("Start", testStatusMap).Return(nil)
	// end mock
	fmt.Println("========== Launching first update to set up ==========")
	assert.Nil(pipelineMgr.Update(testTopic, cur_err))
	fmt.Println("========== Launching second and third updates parallely ==========")
	go pipelineMgr.Update(testTopic, cur_err)
	go pipelineMgr.Update(testTopic, cur_err)

	// sleep 1 second for go routines to finish
	seconds := time.Duration(1)*time.Second
	time.Sleep(seconds)
}

func TestGetAllReplications(t *testing.T) {
	// set-up boiler plate
	testLogger := log.NewLogger("testLogger", log.DefaultLoggerContext)
	pipelineMock := &common.PipelineFactory{}
	replSpecSvcMock	:= &service_def.ReplicationSpecSvc{}
	xdcrTopologyMock := &service_def.XDCRCompTopologySvc{}
	remoteClusterMock := &service_def.RemoteClusterSvc{}
	waitGrp := &sync.WaitGroup{}

	pipelineMgr := PipelineManager {
		pipeline_factory:	pipelineMock,
		repl_spec_svc:		replSpecSvcMock,
		xdcr_topology_svc:	xdcrTopologyMock,
		remote_cluster_svc:	remoteClusterMock,
		logger:				testLogger,
		child_waitGrp:		waitGrp}

	assert := assert.New(t)
	// end boiler plate
	
	// Some things needed for pipelinemgr
	testTopic := "testTopic"

	// setting up replicationStatus and replicationSpecService
	var repStatusMtx sync.RWMutex
	testReplicationStatus := &replicationStatus.ReplicationStatus {
		Lock: &repStatusMtx,
		SpecId: testTopic,
		Logger: testLogger,
		Obj_pool: base.NewMCRequestPool(testTopic, testLogger)}

	// ReplicationSpecSvc used for mocking
	testReplicationSettings := &metadata.ReplicationSettings{FailureRestartInterval: 10}
	testReplicationStatus.Spec_getter = replSpecSvcMock.ReplicationSpec
	testReplicationSpec := &metadata.ReplicationSpecification{}
	testReplicationSpec.Settings = testReplicationSettings

	var emptySlice []string
	replSpecSvcMock.On("AllReplicationSpecIds").Return(emptySlice, nil)

	fmt.Println("========== Testing AllReplications ==========")
	assert.NotNil(pipelineMgr.AllReplications())
}
