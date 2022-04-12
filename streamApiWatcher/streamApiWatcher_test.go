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
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/service_def/mocks"
	realUtils "github.com/couchbase/goxdcr/utils"
	"github.com/stretchr/testify/assert"
	gocb "gopkg.in/couchbase/gocb.v1"
	"net"
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
	fmt.Println("============= Test case start: TestPoolInfo =============")
	defer fmt.Println("============= Test case end: TestPoolInfo =============")

	if isClusterRunning() == false {
		fmt.Println("Skipping because cluster_run setup has not been detected")
		return
	}

	assert := assert.New(t)
	realUtils := realUtils.NewUtilities()
	topSvc := setupMocksCWS()
	watcher := NewStreamApiWatcher(base.ObservePoolPath, topSvc, realUtils, log.NewLogger("testStreamApi", log.DefaultLoggerContext))
	nodesInfo := watcher.GetResult()
	assert.NotNil(nodesInfo)
	assert.NotEqual(0, len(nodesInfo))
	nodes := nodesInfo[base.NodesKey]
	assert.NotNil(nodes)
	nodesList, ok := nodes.([]interface{})
	assert.Equal(true, ok)
	assert.Greater(len(nodesList), 0)
}

func isClusterRunning() bool {
	_, err := net.Listen("tcp4", ":9000")
	if err == nil {
		return false
	}
	cluster, err := gocb.Connect(fmt.Sprintf("http://127.0.0.1:%s", "9000"))
	if err != nil {
		return false
	}
	cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: "Administrator",
		Password: "wewewe",
	})
	_, err = cluster.OpenBucket("B0", "")
	if err != nil {
		return false
	}
	return true
}
