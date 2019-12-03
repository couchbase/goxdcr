// +build !pcre

package parts

import (
	"encoding/binary"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/service_def"
	ThroughputThrottlerMock "github.com/couchbase/goxdcr/service_def/mocks"
	utilities "github.com/couchbase/goxdcr/utils"
	UtilitiesMock "github.com/couchbase/goxdcr/utils/mocks"
	"github.com/stretchr/testify/assert"
	"testing"
)

func setupBoilerPlateRouter() (routerId, topic, filterExpression string, downStreamParts map[string]common.Part,
	routingMap map[uint16]string, crMode base.ConflictResolutionMode, loggerCtx *log.LoggerContext,
	req_creater ReqCreator, utilsMock utilities.UtilsIface, throughputThrottlerSvc service_def.ThroughputThrottlerSvc,
	needToThrottle bool, expDelMode base.FilterExpDelType) {
	routerId = "routerUnitTest"
	topic = "testTopic"
	filterExpression = ""
	downStreamParts = make(map[string]common.Part)
	routingMap = make(map[uint16]string)
	crMode = base.CRMode_RevId
	loggerCtx = log.DefaultLoggerContext
	utilsMock = &UtilitiesMock.UtilsIface{}
	throughputThrottlerSvc = &ThroughputThrottlerMock.ThroughputThrottlerSvc{}
	req_creater = nil
	needToThrottle = false
	expDelMode = base.FilterExpDelNone

	return
}

func TestRouterRouteFunc(t *testing.T) {
	fmt.Println("============== Test case start: TestRouterRouteFunc =================")
	assert := assert.New(t)

	routerId, topic, filterExpression, downStreamParts,
		routingMap, crMode, loggerCtx,
		req_creater, utilsMock, throughputThrottlerSvc,
		needToThrottle, expDelMode := setupBoilerPlateRouter()

	router, err := NewRouter(routerId, topic, filterExpression, downStreamParts,
		routingMap, crMode, loggerCtx, req_creater, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode)

	assert.Nil(err)
	assert.NotNil(router)

	uprEvent, err := RetrieveUprFile("./testdata/uprEventDeletion.json")
	assert.Nil(err)
	assert.NotNil(uprEvent)
	wrappedUprEvent := &base.WrappedUprEvent{UprEvent: uprEvent}

	// Deletion does not contain any flags
	wrappedMCRequest, err := router.ComposeMCRequest(wrappedUprEvent)
	assert.Nil(err)
	assert.NotNil(wrappedMCRequest)
	checkUint := binary.BigEndian.Uint32(wrappedMCRequest.Req.Extras[0:4])
	assert.False(checkUint&base.IS_EXPIRATION > 0)

	// Expiration has to set a flag of 0x10
	uprEvent, err = RetrieveUprFile("./testdata/uprEventExpiration.json")
	assert.Nil(err)
	assert.NotNil(uprEvent)
	wrappedUprEvent = &base.WrappedUprEvent{UprEvent: uprEvent}

	wrappedMCRequest, err = router.ComposeMCRequest(wrappedUprEvent)
	assert.Nil(err)
	assert.NotNil(wrappedMCRequest)
	checkUint = binary.BigEndian.Uint32(wrappedMCRequest.Req.Extras[24:28])
	assert.True(checkUint&base.IS_EXPIRATION > 0)

	fmt.Println("============== Test case end: TestRouterRouteFunc =================")
}

func TestRouterInitialNone(t *testing.T) {
	fmt.Println("============== Test case start: TestRouterInitialNone =================")
	assert := assert.New(t)

	routerId, topic, filterExpression, downStreamParts,
		routingMap, crMode, loggerCtx,
		req_creater, utilsMock, throughputThrottlerSvc,
		needToThrottle, expDelMode := setupBoilerPlateRouter()

	router, err := NewRouter(routerId, topic, filterExpression, downStreamParts,
		routingMap, crMode, loggerCtx, req_creater, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode)

	assert.Nil(err)
	assert.NotNil(router)

	assert.Equal(base.FilterExpDelNone, router.expDelMode.Get())

	fmt.Println("============== Test case end: TestRouterInitialNone =================")
}

func TestRouterSkipDeletion(t *testing.T) {
	fmt.Println("============== Test case start: TestRouterSkipDeletion =================")
	assert := assert.New(t)

	routerId, topic, filterExpression, downStreamParts,
		routingMap, crMode, loggerCtx,
		req_creater, utilsMock, throughputThrottlerSvc,
		needToThrottle, expDelMode := setupBoilerPlateRouter()

	expDelMode = base.FilterExpDelSkipDeletes

	router, err := NewRouter(routerId, topic, filterExpression, downStreamParts,
		routingMap, crMode, loggerCtx, req_creater, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode)

	assert.Nil(err)
	assert.NotNil(router)

	assert.NotEqual(base.FilterExpDelNone, router.expDelMode.Get())

	delEvent, err := RetrieveUprFile("./testdata/uprEventDeletion.json")
	assert.Nil(err)
	assert.NotNil(delEvent)
	delWrapped := &base.WrappedUprEvent{UprEvent: delEvent}

	expEvent, err := RetrieveUprFile("./testdata/uprEventExpiration.json")
	assert.Nil(err)
	assert.NotNil(expEvent)
	expWrapped := &base.WrappedUprEvent{UprEvent: expEvent}

	shouldContinue := router.ProcessExpDelTTL(delWrapped)
	assert.False(shouldContinue)

	shouldContinue = router.ProcessExpDelTTL(expWrapped)
	assert.True(shouldContinue)

	fmt.Println("============== Test case end: TestRouterSkipDeletion =================")
}

func TestRouterSkipExpiration(t *testing.T) {
	fmt.Println("============== Test case start: TestRouterSkipExpiration =================")
	assert := assert.New(t)

	routerId, topic, filterExpression, downStreamParts,
		routingMap, crMode, loggerCtx,
		req_creater, utilsMock, throughputThrottlerSvc,
		needToThrottle, expDelMode := setupBoilerPlateRouter()

	expDelMode = base.FilterExpDelSkipExpiration

	router, err := NewRouter(routerId, topic, filterExpression, downStreamParts,
		routingMap, crMode, loggerCtx, req_creater, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode)

	assert.Nil(err)
	assert.NotNil(router)

	assert.NotEqual(base.FilterExpDelNone, router.expDelMode.Get())

	delEvent, err := RetrieveUprFile("./testdata/uprEventDeletion.json")
	assert.Nil(err)
	assert.NotNil(delEvent)
	delWrapped := &base.WrappedUprEvent{UprEvent: delEvent}

	expEvent, err := RetrieveUprFile("./testdata/uprEventExpiration.json")
	assert.Nil(err)
	assert.NotNil(expEvent)
	expWrapped := &base.WrappedUprEvent{UprEvent: expEvent}

	shouldContinue := router.ProcessExpDelTTL(delWrapped)
	assert.True(shouldContinue)

	shouldContinue = router.ProcessExpDelTTL(expWrapped)
	assert.False(shouldContinue)

	fmt.Println("============== Test case end: TestRouterSkipExpiration =================")
}

func TestRouterSkipDeletesStripTTL(t *testing.T) {
	fmt.Println("============== Test case start: TestRouterSkipExpiryStripTTL =================")
	assert := assert.New(t)

	routerId, topic, filterExpression, downStreamParts,
		routingMap, crMode, loggerCtx,
		req_creater, utilsMock, throughputThrottlerSvc,
		needToThrottle, expDelMode := setupBoilerPlateRouter()

	expDelMode = base.FilterExpDelSkipExpiration | base.FilterExpDelStripExpiration

	router, err := NewRouter(routerId, topic, filterExpression, downStreamParts,
		routingMap, crMode, loggerCtx, req_creater, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode)

	assert.Nil(err)
	assert.NotNil(router)

	assert.NotEqual(base.FilterExpDelNone, router.expDelMode.Get())

	// delEvent contains expiry
	delEvent, err := RetrieveUprFile("./testdata/uprEventDeletion.json")
	assert.Nil(err)
	assert.NotNil(delEvent)
	delWrapped := &base.WrappedUprEvent{UprEvent: delEvent}

	expEvent, err := RetrieveUprFile("./testdata/uprEventExpiration.json")
	assert.Nil(err)
	assert.NotNil(expEvent)
	expWrapped := &base.WrappedUprEvent{UprEvent: expEvent}

	assert.NotEqual(0, int(delEvent.Expiry))
	shouldContinue := router.ProcessExpDelTTL(delWrapped)
	assert.True(shouldContinue)
	assert.Equal(0, int(delEvent.Expiry))

	shouldContinue = router.ProcessExpDelTTL(expWrapped)
	assert.False(shouldContinue)

	fmt.Println("============== Test case end: TestRouterSkipExpiryStripTTL =================")
}

func TestRouterExpDelAllMode(t *testing.T) {
	fmt.Println("============== Test case start: TestRouterExpDelAllMode =================")
	assert := assert.New(t)

	routerId, topic, filterExpression, downStreamParts,
		routingMap, crMode, loggerCtx,
		req_creater, utilsMock, throughputThrottlerSvc,
		needToThrottle, expDelMode := setupBoilerPlateRouter()

	expDelMode = base.FilterExpDelAll

	router, err := NewRouter(routerId, topic, filterExpression, downStreamParts,
		routingMap, crMode, loggerCtx, req_creater, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode)

	assert.Nil(err)
	assert.NotNil(router)

	assert.NotEqual(base.FilterExpDelNone, router.expDelMode.Get())

	delEvent, err := RetrieveUprFile("./testdata/uprEventDeletion.json")
	assert.Nil(err)
	assert.NotNil(delEvent)
	delWrapped := &base.WrappedUprEvent{UprEvent: delEvent}

	mutEvent, err := RetrieveUprFile("./testdata/perfDataExpiry.json")
	assert.Nil(err)
	assert.NotNil(mutEvent)
	mutWrapped := &base.WrappedUprEvent{UprEvent: mutEvent}

	expEvent, err := RetrieveUprFile("./testdata/uprEventExpiration.json")
	assert.Nil(err)
	assert.NotNil(expEvent)
	expWrapped := &base.WrappedUprEvent{UprEvent: expEvent}

	assert.NotEqual(0, int(mutEvent.Expiry))
	shouldContinue := router.ProcessExpDelTTL(mutWrapped)
	assert.True(shouldContinue)
	assert.Equal(0, int(mutEvent.Expiry))

	shouldContinue = router.ProcessExpDelTTL(expWrapped)
	assert.False(shouldContinue)

	shouldContinue = router.ProcessExpDelTTL(delWrapped)
	assert.False(shouldContinue)
	fmt.Println("============== Test case end: TestRouterExpDelAllMode =================")
}
