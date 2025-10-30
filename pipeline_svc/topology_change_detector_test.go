// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package pipeline_svc

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	commonMock "github.com/couchbase/goxdcr/v8/common/mocks"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	service_def "github.com/couchbase/goxdcr/v8/service_def"
	service_def_mocks "github.com/couchbase/goxdcr/v8/service_def/mocks"
	utilities "github.com/couchbase/goxdcr/v8/utils/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Test the initialization of target_vb_server_map_original with variable VB mode disabled (same number of VBs)
func TestMonitorTarget_InitOriginalMap_SameVBs(t *testing.T) {
	fmt.Println("============== Test case start: TestMonitorTarget_InitOriginalMap_SameVBs =================")
	defer fmt.Println("============== Test case end: TestMonitorTarget_InitOriginalMap_SameVBs =================")

	assert := assert.New(t)

	// Setup mocks
	xdcrTopologySvc := &service_def_mocks.XDCRCompTopologySvc{}
	remoteClusterSvc := &service_def_mocks.RemoteClusterSvc{}
	replSpecSvc := &service_def_mocks.ReplicationSpecSvc{}
	utils := &utilities.UtilsIface{}
	bucketTopologySvc := &service_def_mocks.BucketTopologySvc{}

	// Create detector with variableVBMode = false (same number of VBs on source and target)
	topDetector := NewTopologyChangeDetectorSvc(
		xdcrTopologySvc,
		remoteClusterSvc,
		replSpecSvc,
		log.DefaultLoggerContext,
		utils,
		bucketTopologySvc,
		false, // variableVBMode = false
	)

	// Setup mock pipeline
	pipeline := &commonMock.Pipeline{}
	spec := &metadata.ReplicationSpecification{
		Id:                "testSpec",
		SourceBucketName:  "sourceBucket",
		TargetBucketName:  "targetBucket",
		TargetClusterUUID: "targetClusterUUID",
		Settings:          metadata.DefaultReplicationSettings(),
	}

	pipeline.On("Specification").Return(spec)
	pipeline.On("Topic").Return("testTopic")
	pipeline.On("InstanceId").Return("instance123")

	// Attach pipeline
	err := topDetector.Attach(pipeline)
	assert.Nil(err)

	// Setup source VBs that this node is responsible for
	topDetector.vblist_original = []uint16{0, 1, 2, 3, 4}
	close(topDetector.vblistOriginalInitDone)

	// Create target server VB map (same number of VBs: 1024)
	targetServerVBMap := make(base.KvVBMapType)
	targetServerVBMap["node1"] = []uint16{0, 1, 2}
	targetServerVBMap["node2"] = []uint16{3, 4, 5, 6}
	target_vb_server_map := targetServerVBMap.CompileLookupIndex()

	// Create channel for notifications - must match the interface type
	targetVbUpdateCh := make(chan service_def.TargetNotification, 1)

	// Setup bucket topology service mock
	bucketTopologySvc.On("SubscribeToRemoteBucketFeed", spec, mock.Anything).Return((chan service_def.TargetNotification)(targetVbUpdateCh), nil)
	bucketTopologySvc.On("UnSubscribeRemoteBucketFeed", spec, mock.Anything).Return(nil)

	// Create mock target notification
	mockTargetNotif := &service_def_mocks.TargetNotification{}
	mockTargetNotif.On("GetTargetServerVBMap").Return(targetServerVBMap)
	mockTargetNotif.On("GetTargetBucketUUID").Return("uuid123")
	mockTargetNotif.On("GetTargetBucketInfo").Return(nil)
	mockTargetNotif.On("Recycle").Return()

	// Send initial notification
	targetVbUpdateCh <- mockTargetNotif

	// Run monitorTarget in a goroutine
	var initWg sync.WaitGroup
	var initErr error
	initWg.Add(1)
	go topDetector.monitorTarget(&initWg, &initErr)

	// Wait for initialization
	initWg.Wait()
	assert.Nil(initErr)

	// Give it a moment to process the initial notification
	time.Sleep(100 * time.Millisecond)

	// Verify that target_vb_server_map_original was initialized correctly
	// For variableVBMode = false, it should use ConstructVbServerMap
	// which filters based on vblist_original
	assert.NotNil(topDetector.target_vb_server_map_original)
	assert.Equal(5, len(topDetector.target_vb_server_map_original))

	// Verify mapping for VBs in vblist_original
	assert.Equal(5, len(topDetector.target_vb_server_map_original))
	for _, vb := range topDetector.vblist_original {
		assert.NotNil(topDetector.target_vb_server_map_original[vb])
		assert.Equal(topDetector.target_vb_server_map_original[vb], target_vb_server_map[vb])
	}

	// Cleanup
	close(topDetector.finish_ch)
	time.Sleep(50 * time.Millisecond)
}

// Test the initialization of target_vb_server_map_original with variable VB mode enabled (different # of VBs)
func TestMonitorTarget_InitOriginalMap_DifferentVBs(t *testing.T) {
	fmt.Println("============== Test case start: TestMonitorTarget_InitOriginalMap_DifferentVBs =================")
	defer fmt.Println("============== Test case end: TestMonitorTarget_InitOriginalMap_DifferentVBs =================")

	assert := assert.New(t)

	// Setup mocks
	xdcrTopologySvc := &service_def_mocks.XDCRCompTopologySvc{}
	remoteClusterSvc := &service_def_mocks.RemoteClusterSvc{}
	replSpecSvc := &service_def_mocks.ReplicationSpecSvc{}
	utils := &utilities.UtilsIface{}
	bucketTopologySvc := &service_def_mocks.BucketTopologySvc{}

	// Create detector with variableVBMode = true (different number of VBs on source and target)
	topDetector := NewTopologyChangeDetectorSvc(
		xdcrTopologySvc,
		remoteClusterSvc,
		replSpecSvc,
		log.DefaultLoggerContext,
		utils,
		bucketTopologySvc,
		true, // variableVBMode = true (THIS IS THE KEY CHANGE FOR MB-69159)
	)

	// Setup mock pipeline
	pipeline := &commonMock.Pipeline{}
	spec := &metadata.ReplicationSpecification{
		Id:                "testSpec",
		SourceBucketName:  "sourceBucket",
		TargetBucketName:  "targetBucket",
		TargetClusterUUID: "targetClusterUUID",
		Settings:          metadata.DefaultReplicationSettings(),
	}

	pipeline.On("Specification").Return(spec)
	pipeline.On("Topic").Return("testTopic")
	pipeline.On("InstanceId").Return("instance123")

	// Attach pipeline
	err := topDetector.Attach(pipeline)
	assert.Nil(err)

	// Setup source VBs (e.g., 1024 VBs on source)
	topDetector.vblist_original = []uint16{256, 257, 258, 259, 260}
	close(topDetector.vblistOriginalInitDone)

	// Create target server VB map (different number: e.g., 64 VBs on target)
	// This simulates a target cluster with fewer VBs
	targetServerVBMap := make(base.KvVBMapType)
	targetServerVBMap["node1"] = []uint16{0, 1, 2, 3}
	targetServerVBMap["node2"] = []uint16{4, 5, 6, 7}
	target_vb_server_map := targetServerVBMap.CompileLookupIndex()

	// Create channel for notifications - must match the interface type
	targetVbUpdateCh := make(chan service_def.TargetNotification, 1)

	// Setup bucket topology service mock
	bucketTopologySvc.On("SubscribeToRemoteBucketFeed", spec, mock.Anything).Return((chan service_def.TargetNotification)(targetVbUpdateCh), nil)
	bucketTopologySvc.On("UnSubscribeRemoteBucketFeed", spec, mock.Anything).Return(nil)

	// Create mock target notification
	mockTargetNotif := &service_def_mocks.TargetNotification{}
	mockTargetNotif.On("GetTargetServerVBMap").Return(targetServerVBMap)
	mockTargetNotif.On("GetTargetBucketUUID").Return("uuid123")
	mockTargetNotif.On("GetTargetBucketInfo").Return(nil)
	mockTargetNotif.On("Recycle").Return()

	// Send initial notification
	targetVbUpdateCh <- mockTargetNotif

	// Run monitorTarget in a goroutine
	var initWg sync.WaitGroup
	var initErr error
	initWg.Add(1)
	go topDetector.monitorTarget(&initWg, &initErr)

	// Wait for initialization
	initWg.Wait()
	assert.Nil(initErr)

	// Give it a moment to process the initial notification
	time.Sleep(100 * time.Millisecond)

	// Verify that target_vb_server_map_original was initialized correctly
	// For variableVBMode = true, it should use CompileLookupIndex()
	// which creates a mapping for ALL target VBs (not filtered)
	assert.NotNil(topDetector.target_vb_server_map_original)
	assert.Equal(8, len(topDetector.target_vb_server_map_original)) // All target VBs

	// Verify mapping includes all target VBs
	assert.Equal(8, len(topDetector.target_vb_server_map_original))
	for vb, server := range target_vb_server_map {
		assert.NotNil(topDetector.target_vb_server_map_original[vb])
		assert.Equal(topDetector.target_vb_server_map_original[vb], server)
	}

	// Cleanup
	close(topDetector.finish_ch)
	time.Sleep(50 * time.Millisecond)
}
