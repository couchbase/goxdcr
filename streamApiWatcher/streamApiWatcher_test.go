// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.
package streamApiWatcher

import (
	"fmt"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/service_def/mocks"
	realUtils "github.com/couchbase/goxdcr/v8/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

const username = "Administrator"
const password = "wewewe"

func setupMocksCWS() *mocks.XDCRCompTopologySvc {
	xdcrTopologySvc := &mocks.XDCRCompTopologySvc{}
	xdcrTopologySvc.On("MyConnectionStr").Return("127.0.0.1:9000", nil)
	xdcrTopologySvc.On("MyCredentials").Return(username, password, base.HttpAuthMechPlain, nil, false, nil, nil, nil)
	return xdcrTopologySvc
}
func TestClusterWatch(t *testing.T) {
	fmt.Println("============= Test case start: TestClusterWatch =============")
	defer fmt.Println("============= Test case end: TestClusterWatch =============")

	if isClusterRunning() == false {
		fmt.Println("Skipping because cluster_run setup has not been detected")
		return
	}

	assert := assert.New(t)
	realUtils := realUtils.NewUtilities()
	topSvc := setupMocksCWS()

	watcher := NewStreamApiWatcher(base.ObservePoolPath, topSvc, realUtils, nil, log.NewLogger("testStreamApi", log.DefaultLoggerContext))
	watcher.Start()
	nodesInfo := watcher.GetResult()
	assert.NotNil(nodesInfo)
	assert.NotEqual(0, len(nodesInfo))
	nodes := nodesInfo[base.NodesKey]
	assert.NotNil(nodes)
	nodesList, ok := nodes.([]interface{})
	assert.Equal(true, ok)
	assert.Greater(len(nodesList), 0)
}

func TestBucketWatch(t *testing.T) {
	fmt.Println("============= Test case start: TestBucketWatch =============")
	defer fmt.Println("============= Test case end: TestBucketWatch =============")
	if isClusterRunning() == false {
		fmt.Println("Skipping because cluster_run setup has not been detected")
		return
	}

	assert := assert.New(t)
	realUtils := realUtils.NewUtilities()
	topSvc := setupMocksCWS()

	watcher := NewStreamApiWatcher(base.ObserveBucketPath+"B1", topSvc, realUtils, nil, log.NewLogger("testStreamApi", log.DefaultLoggerContext))
	watcher.Start()
	bucketInfo := watcher.GetResult()
	assert.NotNil(bucketInfo)
	assert.NotEqual(0, len(bucketInfo))
	// Make sure we can get vBucketServerMap
	vbSrvMapObj, ok := bucketInfo[base.VBucketServerMapKey]
	assert.True(ok)
	vbSrvMap, ok := vbSrvMapObj.(map[string]interface{})
	assert.True(ok)
	// Make sure we can get serverList
	serverListObj, ok := vbSrvMap[base.ServerListKey]
	assert.True(ok)
	_, ok = serverListObj.([]interface{})
	assert.True(ok)
	nodes := bucketInfo[base.NodesKey]
	assert.NotNil(nodes)
	nodesList, ok := nodes.([]interface{})
	assert.Equal(true, ok)
	assert.Greater(len(nodesList), 0)
	watcher.Stop()

	manifestUid, err := realUtils.GetCollectionManifestUidFromBucketInfo(bucketInfo)
	assert.Nil(err)
	assert.GreaterOrEqual(manifestUid, uint64(0))

	// restart and stop watcher
	watcher.Start()
	bucketInfo = watcher.GetResult()
	assert.NotNil(bucketInfo)
	watcher.Stop()
}

func isClusterRunning() bool {
	utils := realUtils.Utilities{}
	bucketInfo, err := utils.GetBucketInfo("127.0.0.1:9000", "B1", username, password, base.HttpAuthMechPlain, nil, false, nil, nil, log.NewLogger("test", log.DefaultLoggerContext))
	if err == nil && bucketInfo != nil {
		return true
	} else {
		return false
	}
}
