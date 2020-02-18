// +build !pcre

package parts

import (
	"encoding/binary"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	service_def_mocks "github.com/couchbase/goxdcr/service_def/mocks"
	utilities "github.com/couchbase/goxdcr/utils"
	UtilitiesMock "github.com/couchbase/goxdcr/utils/mocks"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var dummyDownStream string = "dummy"

func setupBoilerPlateRouter() (routerId string, downStreamParts map[string]common.Part,
	routingMap map[uint16]string, crMode base.ConflictResolutionMode, loggerCtx *log.LoggerContext,
	req_creater ReqCreator, utilsMock utilities.UtilsIface, throughputThrottlerSvc service_def.ThroughputThrottlerSvc,
	needToThrottle bool, expDelMode base.FilterExpDelType, collectionsManifestSvc service_def.CollectionsManifestSvc,
	spec *metadata.ReplicationSpecification) {
	routerId = "routerUnitTest"

	downStreamParts = make(map[string]common.Part)
	downStreamParts[dummyDownStream] = nil
	routingMap = make(map[uint16]string)
	crMode = base.CRMode_RevId
	loggerCtx = log.DefaultLoggerContext
	utilsMock = &UtilitiesMock.UtilsIface{}
	throughputThrottlerSvc = &service_def_mocks.ThroughputThrottlerSvc{}
	req_creater = nil
	needToThrottle = false
	expDelMode = base.FilterExpDelNone
	collectionsManifestSvc = &service_def_mocks.CollectionsManifestSvc{}
	spec, _ = metadata.NewReplicationSpecification("srcBucket", "srcBucketUUID", "targetClusterUUID", "tgtBucket", "tgtBucketUUID")

	return
}

func TestRouterRouteFunc(t *testing.T) {
	fmt.Println("============== Test case start: TestRouterRouteFunc =================")
	assert := assert.New(t)

	routerId, downStreamParts, routingMap, crMode, loggerCtx,
		req_creater, utilsMock, throughputThrottlerSvc,
		needToThrottle, expDelMode, collectionsManifestSvc, spec := setupBoilerPlateRouter()

	router, err := NewRouter(routerId, spec, downStreamParts,
		routingMap, crMode, loggerCtx, req_creater, utilsMock, throughputThrottlerSvc, needToThrottle,
		expDelMode, collectionsManifestSvc)

	assert.Nil(err)
	assert.NotNil(router)

	uprEvent, err := RetrieveUprFile("./testdata/uprEventDeletion.json")
	assert.Nil(err)
	assert.NotNil(uprEvent)
	wrappedEvent := &base.WrappedUprEvent{UprEvent: uprEvent}

	// Deletion does not contain any flags
	wrappedMCRequest, err := router.ComposeMCRequest(wrappedEvent)
	assert.Nil(err)
	assert.NotNil(wrappedMCRequest)
	checkUint := binary.BigEndian.Uint32(wrappedMCRequest.Req.Extras[0:4])
	assert.False(checkUint&base.IS_EXPIRATION > 0)

	// Expiration has to set a flag of 0x10
	uprEvent, err = RetrieveUprFile("./testdata/uprEventExpiration.json")
	assert.Nil(err)
	assert.NotNil(uprEvent)
	wrappedEvent = &base.WrappedUprEvent{UprEvent: uprEvent}

	wrappedMCRequest, err = router.ComposeMCRequest(wrappedEvent)
	assert.Nil(err)
	assert.NotNil(wrappedMCRequest)
	checkUint = binary.BigEndian.Uint32(wrappedMCRequest.Req.Extras[24:28])
	assert.True(checkUint&base.IS_EXPIRATION > 0)

	fmt.Println("============== Test case end: TestRouterRouteFunc =================")
}

func TestRouterInitialNone(t *testing.T) {
	fmt.Println("============== Test case start: TestRouterInitialNone =================")
	assert := assert.New(t)

	routerId, downStreamParts, routingMap, crMode, loggerCtx,
		req_creater, utilsMock, throughputThrottlerSvc,
		needToThrottle, expDelMode, collectionsManifestSvc, spec := setupBoilerPlateRouter()

	router, err := NewRouter(routerId, spec, downStreamParts,
		routingMap, crMode, loggerCtx, req_creater, utilsMock, throughputThrottlerSvc, needToThrottle,
		expDelMode, collectionsManifestSvc)

	assert.Nil(err)
	assert.NotNil(router)

	assert.Equal(base.FilterExpDelNone, router.expDelMode.Get())

	fmt.Println("============== Test case end: TestRouterInitialNone =================")
}

func TestRouterSkipDeletion(t *testing.T) {
	fmt.Println("============== Test case start: TestRouterSkipDeletion =================")
	assert := assert.New(t)

	routerId, downStreamParts, routingMap, crMode, loggerCtx,
		req_creater, utilsMock, throughputThrottlerSvc,
		needToThrottle, expDelMode, collectionsManifestSvc, spec := setupBoilerPlateRouter()

	expDelMode = base.FilterExpDelSkipDeletes

	router, err := NewRouter(routerId, spec, downStreamParts,
		routingMap, crMode, loggerCtx, req_creater, utilsMock, throughputThrottlerSvc, needToThrottle,
		expDelMode, collectionsManifestSvc)

	assert.Nil(err)
	assert.NotNil(router)

	assert.NotEqual(base.FilterExpDelNone, router.expDelMode.Get())

	delEvent, err := RetrieveUprFile("./testdata/uprEventDeletion.json")
	assert.Nil(err)
	assert.NotNil(delEvent)

	expEvent, err := RetrieveUprFile("./testdata/uprEventExpiration.json")
	assert.Nil(err)
	assert.NotNil(expEvent)

	shouldContinue := router.ProcessExpDelTTL(delEvent)
	assert.False(shouldContinue)

	shouldContinue = router.ProcessExpDelTTL(expEvent)
	assert.True(shouldContinue)

	fmt.Println("============== Test case end: TestRouterSkipDeletion =================")
}

func TestRouterSkipExpiration(t *testing.T) {
	fmt.Println("============== Test case start: TestRouterSkipExpiration =================")
	assert := assert.New(t)

	routerId, downStreamParts, routingMap, crMode, loggerCtx,
		req_creater, utilsMock, throughputThrottlerSvc,
		needToThrottle, expDelMode, collectionsManifestSvc, spec := setupBoilerPlateRouter()

	expDelMode = base.FilterExpDelSkipExpiration

	router, err := NewRouter(routerId, spec, downStreamParts,
		routingMap, crMode, loggerCtx, req_creater, utilsMock, throughputThrottlerSvc, needToThrottle,
		expDelMode, collectionsManifestSvc)

	assert.Nil(err)
	assert.NotNil(router)

	assert.NotEqual(base.FilterExpDelNone, router.expDelMode.Get())

	delEvent, err := RetrieveUprFile("./testdata/uprEventDeletion.json")
	assert.Nil(err)
	assert.NotNil(delEvent)

	expEvent, err := RetrieveUprFile("./testdata/uprEventExpiration.json")
	assert.Nil(err)
	assert.NotNil(expEvent)

	shouldContinue := router.ProcessExpDelTTL(delEvent)
	assert.True(shouldContinue)

	shouldContinue = router.ProcessExpDelTTL(expEvent)
	assert.False(shouldContinue)

	fmt.Println("============== Test case end: TestRouterSkipExpiration =================")
}

func TestRouterSkipDeletesStripTTL(t *testing.T) {
	fmt.Println("============== Test case start: TestRouterSkipExpiryStripTTL =================")
	assert := assert.New(t)

	routerId, downStreamParts, routingMap, crMode, loggerCtx,
		req_creater, utilsMock, throughputThrottlerSvc,
		needToThrottle, expDelMode, collectionsManifestSvc, spec := setupBoilerPlateRouter()

	expDelMode = base.FilterExpDelSkipExpiration | base.FilterExpDelStripExpiration

	router, err := NewRouter(routerId, spec, downStreamParts,
		routingMap, crMode, loggerCtx, req_creater, utilsMock, throughputThrottlerSvc, needToThrottle,
		expDelMode, collectionsManifestSvc)

	assert.Nil(err)
	assert.NotNil(router)

	assert.NotEqual(base.FilterExpDelNone, router.expDelMode.Get())

	// delEvent contains expiry
	delEvent, err := RetrieveUprFile("./testdata/uprEventDeletion.json")
	assert.Nil(err)
	assert.NotNil(delEvent)

	expEvent, err := RetrieveUprFile("./testdata/uprEventExpiration.json")
	assert.Nil(err)
	assert.NotNil(expEvent)

	assert.NotEqual(0, int(delEvent.Expiry))
	shouldContinue := router.ProcessExpDelTTL(delEvent)
	assert.True(shouldContinue)
	assert.Equal(0, int(delEvent.Expiry))

	shouldContinue = router.ProcessExpDelTTL(expEvent)
	assert.False(shouldContinue)

	fmt.Println("============== Test case end: TestRouterSkipExpiryStripTTL =================")
}

func TestRouterExpDelAllMode(t *testing.T) {
	fmt.Println("============== Test case start: TestRouterExpDelAllMode =================")
	assert := assert.New(t)

	routerId, downStreamParts, routingMap, crMode, loggerCtx,
		req_creater, utilsMock, throughputThrottlerSvc,
		needToThrottle, expDelMode, collectionsManifestSvc, spec := setupBoilerPlateRouter()

	expDelMode = base.FilterExpDelAll

	router, err := NewRouter(routerId, spec, downStreamParts,
		routingMap, crMode, loggerCtx, req_creater, utilsMock, throughputThrottlerSvc, needToThrottle,
		expDelMode, collectionsManifestSvc)

	assert.Nil(err)
	assert.NotNil(router)

	assert.NotEqual(base.FilterExpDelNone, router.expDelMode.Get())

	delEvent, err := RetrieveUprFile("./testdata/uprEventDeletion.json")
	assert.Nil(err)
	assert.NotNil(delEvent)

	mutEvent, err := RetrieveUprFile("./testdata/perfDataExpiry.json")
	assert.Nil(err)
	assert.NotNil(mutEvent)

	expEvent, err := RetrieveUprFile("./testdata/uprEventExpiration.json")
	assert.Nil(err)
	assert.NotNil(expEvent)

	assert.NotEqual(0, int(mutEvent.Expiry))
	shouldContinue := router.ProcessExpDelTTL(mutEvent)
	assert.True(shouldContinue)
	assert.Equal(0, int(mutEvent.Expiry))

	shouldContinue = router.ProcessExpDelTTL(expEvent)
	assert.False(shouldContinue)

	shouldContinue = router.ProcessExpDelTTL(delEvent)
	assert.False(shouldContinue)
	fmt.Println("============== Test case end: TestRouterExpDelAllMode =================")
}

var retryBufferCapUnitTest int = 5
var retryFuncCnt int
var routingUpdateCnt int
var routingUpdateEventTimes []time.Time
var ignoreDataCnt int
var ignoreDataEventTimes []time.Time
var fatalErrorCnt int

func collectionsRouterBoilerPlate(retryInterval time.Duration, retryResult error) (*CollectionsRouter, *int) {
	_, _, _, _, _, _, _, _, _, _, collectionsManifestSvc, spec := setupBoilerPlateRouter()

	logger := log.NewLogger("unit_test", nil)
	retryFuncCnt = 0
	retryFunc := func(item interface{}) error {
		retryFuncCnt++
		return retryResult
	}

	routingUpdateCnt = 0
	routingUpdateEventTimes = routingUpdateEventTimes[:0]
	routingUpdateFunc := func(CollectionsRoutingInfo) {
		routingUpdateEventTimes = append(routingUpdateEventTimes, time.Now())
		routingUpdateCnt++
	}

	ignoreDataCnt = 0
	ignoreDataEventTimes = ignoreDataEventTimes[:0]
	ignoreDataFunc := func(*base.WrappedMCRequest) {
		ignoreDataEventTimes = append(ignoreDataEventTimes, time.Now())
		ignoreDataCnt++
	}

	fatalErrorCnt = 0
	fatalErrorFunc := func(error) {
		fatalErrorCnt++
	}

	router := NewCollectionsRouter(collectionsManifestSvc, spec, logger, retryFunc,
		retryInterval, retryBufferCapUnitTest, CollectionsRoutingUpdater(routingUpdateFunc),
		IgnoreDataEventer(ignoreDataFunc), fatalErrorFunc)

	return router, &retryFuncCnt
}

func TestCollectionsRouter(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestCollectionsRouter =================")

	router, retryFuncCntPtr := collectionsRouterBoilerPlate(50*time.Millisecond, nil /*retryResult*/)
	dummyMut := &base.WrappedMCRequest{ColInfo: &base.TargetCollectionInfo{}}

	assert.Nil(router.Start())

	// Retry with no error should work fine
	router.retryRequest(dummyMut)
	assert.NotEqual(len(router.retryBufferCh), 0)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(len(router.retryBufferCh), 0)

	assert.Equal(1, *retryFuncCntPtr)

	assert.Nil(router.Stop())
	fmt.Println("============== Test case end: TestCollectionsRouter =================")
}

func TestCollectionsRouterQueueDequeue(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestCollectionsRouterQueueDequeue =================")

	refreshTime := 50 * time.Millisecond
	router, _ := collectionsRouterBoilerPlate(refreshTime, fmt.Errorf("dummyErr") /*retryResult*/)
	colNameSpace := &base.CollectionNamespace{"dummyScope", "dummyCollection"}
	colNameSpace2 := &base.CollectionNamespace{"dummyScope", "dummyCollection2"}

	// Have 2 diff collections implicit map
	dummyMut := &base.WrappedMCRequest{ColNamespace: colNameSpace, ColInfo: &base.TargetCollectionInfo{}}
	dummyMut2 := &base.WrappedMCRequest{ColNamespace: colNameSpace2, ColInfo: &base.TargetCollectionInfo{}}

	assert.Nil(router.Start())
	assert.Equal(0, len(router.brokenMapping))

	assert.Nil(router.retryRequest(dummyMut))
	assert.Nil(router.retryRequest(dummyMut2))
	assert.Equal(2, len(router.retryBufferCh))

	// Wait for the cycles to happen 5 times - 5 times the initial start + margin
	sleepTime := refreshTime*time.Duration(base.MaxCollectionsRoutingRetry+1) + 10*time.Millisecond
	time.Sleep(sleepTime)

	// Router retry go-routine should have declared the mapping broken
	router.brokenMapMtx.RLock()
	// Two mutations are broken
	expectedLen := 2
	assert.Equal(expectedLen, len(router.brokenMapping))
	router.brokenMapMtx.RUnlock()

	// Two routing updates should have been issued
	assert.Equal(expectedLen, routingUpdateCnt)

	// Two ignore data events raised
	assert.Equal(expectedLen, ignoreDataCnt)

	// Make sure that routing update is always sent (and should be handled) before ignore data events
	for i := 0; i < expectedLen; i++ {
		assert.True(routingUpdateEventTimes[i].Before(ignoreDataEventTimes[i]))
	}

	assert.Nil(router.Stop())
	fmt.Println("============== Test case end: TestCollectionsRouterQueueDequeue =================")
}

func TestCollectionsRouterRetryPickup(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestCollectionsRouterRetryPickup =================")

	refreshTime := 200 * time.Millisecond
	router, _ := collectionsRouterBoilerPlate(refreshTime, nil /*retryResult*/)
	colNameSpace := &base.CollectionNamespace{"dummyScope", "dummyCollection"}
	//	colNameSpace2 := &base.CollectionNamespace{"dummyScope", "dummyCollection2"}

	// Have 2 diff collections implicit map
	dummyMut := &base.WrappedMCRequest{ColNamespace: colNameSpace, ColInfo: &base.TargetCollectionInfo{}}
	//	dummyMut2 := &base.WrappedMCRequest{ColNamespace: colNameSpace2, ColInfo: &base.TargetCollectionInfo{}}

	assert.Nil(router.Start())
	assert.Equal(0, len(router.brokenMapping))

	// Fill up to the buffer limit
	for i := 0; i < retryBufferCapUnitTest; i++ {
		assert.Nil(router.retryRequest(dummyMut))
	}

	assert.Equal(0, retryFuncCnt)
	time.Sleep(300 * time.Millisecond)
	assert.Equal(retryBufferCapUnitTest, retryFuncCnt)

	assert.Nil(router.Stop())
	fmt.Println("============== Test case end: TestCollectionsRouterRetryPickup =================")
}

func TestCollectionsRouterRetryOverflow(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestCollectionsRouterRetryOverflow =================")

	refreshTime := 200 * time.Millisecond
	router, _ := collectionsRouterBoilerPlate(refreshTime, fmt.Errorf("Dummy") /*retryResult*/)
	colNameSpace := &base.CollectionNamespace{"dummyScope", "dummyCollection"}
	//	colNameSpace2 := &base.CollectionNamespace{"dummyScope", "dummyCollection2"}

	// Have 2 diff collections implicit map
	dummyMut := &base.WrappedMCRequest{ColNamespace: colNameSpace, ColInfo: &base.TargetCollectionInfo{}}
	//	dummyMut2 := &base.WrappedMCRequest{ColNamespace: colNameSpace2, ColInfo: &base.TargetCollectionInfo{}}

	assert.Nil(router.Start())
	assert.Equal(0, len(router.brokenMapping))

	// Fill up to the buffer limit
	for i := 0; i < retryBufferCapUnitTest; i++ {
		assert.Nil(router.retryRequest(dummyMut))
	}
	assert.Equal(0, retryFuncCnt)
	assert.Equal(retryBufferCapUnitTest, len(router.retryBufferCh))

	//	 The next call should not block but runRetry should force emergency flush
	assert.Nil(router.retryRequest(dummyMut))

	// Immediately should just mark one of them unreachable
	assert.Equal(1, ignoreDataCnt)

	// The rest will be handled the next refresh cycle
	assert.Equal(retryBufferCapUnitTest, len(router.retryBufferCh)+len(router.tempRetryCh))

	assert.Nil(router.Stop())
	fmt.Println("============== Test case end: TestCollectionsRouterRetryOverflow =================")
}

func TestCollectionsRouterRetryQueuePush(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestCollectionsRouterRetryQueuePush =================")

	refreshTime := 100 * time.Millisecond
	router, _ := collectionsRouterBoilerPlate(refreshTime, fmt.Errorf("Dummy") /*retryResult*/)
	colNameSpace := &base.CollectionNamespace{"dummyScope", "dummyCollection"}
	//	colNameSpace2 := &base.CollectionNamespace{"dummyScope", "dummyCollection2"}

	// Have 2 diff collections implicit map
	dummyMut := &base.WrappedMCRequest{ColNamespace: colNameSpace, ColInfo: &base.TargetCollectionInfo{}}

	assert.Nil(router.Start())
	assert.Equal(0, len(router.brokenMapping))

	// Fill up to the buffer limit
	for i := 0; i < retryBufferCapUnitTest; i++ {
		assert.Nil(router.retryRequest(dummyMut))
	}
	assert.Equal(0, retryFuncCnt)
	assert.Equal(retryBufferCapUnitTest, len(router.retryBufferCh))

	// Also manually put two more in the temporary retry area
	router.tempRetryCh <- dummyMut
	router.tempRetryCh <- dummyMut
	assert.Equal(retryBufferCapUnitTest+2, len(router.retryBufferCh)+len(router.tempRetryCh))

	// Wait for timer to fire just once
	sleepTime := refreshTime + 20*time.Millisecond
	time.Sleep(sleepTime)

	// Both would have been bumped out of the retry queue due to emergencyProcessOneReq
	//	assert.Equal(retryBufferCapUnitTest, len(router.retryBufferCh)+len(router.tempRetryCh))
	assert.Equal(retryBufferCapUnitTest, len(router.retryBufferCh))
	assert.Equal(0, len(router.tempRetryCh))
	assert.Equal(2, ignoreDataCnt)

	assert.Nil(router.Stop())
	fmt.Println("============== Test case end: TestCollectionsRouterRetryQueuePush =================")
}
