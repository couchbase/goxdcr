/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package peerToPeer

import (
	"fmt"
	base2 "github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	service_def "github.com/couchbase/goxdcr/v8/service_def/mocks"
	"github.com/stretchr/testify/assert"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const lifecycleId = "testLifecycleId"

func discoveryHandlerBoilerPlate() ([]chan interface{}, *log.CommonLogger, string, *KnownPeers, time.Duration, *service_def.ReplicationSpecSvc) {
	reqCh := make(chan interface{})
	respCh := make(chan interface{})
	logger := log.NewLogger("unitTest", log.DefaultLoggerContext)
	lifeCycleId := lifecycleId
	knownPeers := &KnownPeers{
		PeersMap: make(PeersMapType),
		mapMtx:   sync.RWMutex{},
	}
	cleanupInterval := 50 * time.Millisecond
	replSpecSvc := &service_def.ReplicationSpecSvc{}

	return []chan interface{}{reqCh, respCh}, logger, lifeCycleId, knownPeers, cleanupInterval, replSpecSvc
}

func TestDiscoveryHandler(t *testing.T) {
	fmt.Println("============== Test case start: TestDiscoveryHandler =================")
	defer fmt.Println("============== Test case end: TestDiscoveryHandler =================")
	assert := assert.New(t)
	handler := NewDiscoveryHandler(discoveryHandlerBoilerPlate())

	assert.Nil(handler.Start())

	// Don't know remote yet
	opaque := uint32(50)
	var cbCalled uint32
	reqCommon := NewRequestCommon("testSender", "testTarget", "", lifecycleId, opaque)
	reqCommon.responseCb = func(resp Response) (HandlerResult, error) {
		atomic.StoreUint32(&cbCalled, 1)
		handler.receiveRespCh <- resp
		return &HandlerResultImpl{Err: nil, HttpStatusCode: http.StatusOK}, nil
	}
	discoveryReq := NewP2PDiscoveryReq(reqCommon)
	handler.RegisterOpaque(discoveryReq, NewSendOpts(false, base2.PeerToPeerNonExponentialWaitTime, base2.PeerToPeerMaxRetry))

	handler.receiveReqCh <- discoveryReq

	handler.opaqueMapMtx.RLock()
	assert.NotNil(handler.opaqueMap[opaque])
	handler.opaqueMapMtx.RUnlock()

	time.Sleep(100 * time.Millisecond)
	assert.Equal(uint32(1), atomic.LoadUint32(&cbCalled))

	time.Sleep(100 * time.Millisecond)
	handler.opaqueMapMtx.RLock()
	assert.NotNil(handler.opaqueMap[opaque])
	handler.opaqueMapMtx.RUnlock()
}
