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
	needToThrottle bool) {
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

	return
}

func TestRouterRouteFunc(t *testing.T) {
	fmt.Println("============== Test case start: TestRouterRouteFunc =================")
	assert := assert.New(t)

	routerId, topic, filterExpression, downStreamParts,
		routingMap, crMode, loggerCtx,
		req_creater, utilsMock, throughputThrottlerSvc,
		needToThrottle := setupBoilerPlateRouter()

	router, err := NewRouter(routerId, topic, filterExpression, downStreamParts,
		routingMap, crMode, loggerCtx, req_creater, utilsMock, throughputThrottlerSvc, needToThrottle)

	assert.Nil(err)
	assert.NotNil(router)

	uprEvent, err := RetrieveUprFile("./testdata/uprEventDeletion.json")
	assert.Nil(err)
	assert.NotNil(uprEvent)

	// Deletion does not contain any flags
	wrappedMCRequest, err := router.ComposeMCRequest(uprEvent)
	assert.Nil(err)
	assert.NotNil(wrappedMCRequest)
	checkUint := binary.BigEndian.Uint32(wrappedMCRequest.Req.Extras[0:4])
	assert.False(checkUint&base.IS_EXPIRATION > 0)

	// Expiration has to set a flag of 0x10
	uprEvent, err = RetrieveUprFile("./testdata/uprEventExpiration.json")
	assert.Nil(err)
	assert.NotNil(uprEvent)

	wrappedMCRequest, err = router.ComposeMCRequest(uprEvent)
	assert.Nil(err)
	assert.NotNil(wrappedMCRequest)
	checkUint = binary.BigEndian.Uint32(wrappedMCRequest.Req.Extras[0:4])
	assert.True(checkUint&base.IS_EXPIRATION > 0)

	fmt.Println("============== Test case end: TestRouterRouteFunc =================")
}
