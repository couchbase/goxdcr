package peerToPeer

import (
	"fmt"
	"github.com/couchbase/goxdcr/log"
	"github.com/stretchr/testify/assert"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const lifecycleId = "testLifecycleId"

func discoveryHandlerBoilerPlate() (chan interface{}, *log.CommonLogger, string, *KnownPeers, time.Duration) {
	reqCh := make(chan interface{})
	logger := log.NewLogger("unitTest", log.DefaultLoggerContext)
	lifeCycleId := lifecycleId
	knownPeers := &KnownPeers{
		PeersMap: make(PeersMapType),
		mapMtx:   sync.RWMutex{},
	}
	cleanupInterval := 50 * time.Millisecond

	return reqCh, logger, lifeCycleId, knownPeers, cleanupInterval
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
		handler.receiveCh <- resp
		return &HandlerResultImpl{Err: nil, HttpStatusCode: http.StatusOK}, nil
	}
	discoveryReq := NewP2PDiscoveryReq(reqCommon)
	handler.RegisterOpaque(discoveryReq, NewSendOpts(false))

	handler.receiveCh <- discoveryReq

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
