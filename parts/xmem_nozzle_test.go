package parts

import (
	"fmt"
	mcMock "github.com/couchbase/gomemcached/client/mocks"
	base "github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	utilsReal "github.com/couchbase/goxdcr/utils"
	utilsMock "github.com/couchbase/goxdcr/utils/mocks"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
	"testing"
)

func setupBoilerPlateXmem() (*utilsMock.UtilsIface,
	base.DataObjRecycler,
	map[string]interface{},
	*XmemNozzle) {

	utilitiesMock := &utilsMock.UtilsIface{}
	dummyDataObjRecycler := func(string, *base.WrappedMCRequest) {}

	xmemNozzle := NewXmemNozzle("testId", nil, "", "testTopic", "testConnPoolNamePrefix", 5, /* connPoolConnSize*/
		"testConnectString", "testSourceBucket", "testTargetBucket", "testUserName", "testPw",
		dummyDataObjRecycler, base.CRMode_RevId, log.DefaultLoggerContext, utilitiesMock)

	// settings map
	settingsMap := make(map[string]interface{})
	settingsMap[SETTING_BATCHCOUNT] = 5

	// Enable compression by default
	settingsMap[SETTING_COMPRESSION_TYPE] = (base.CompressionType)(base.CompressionTypeSnappy)

	return utilitiesMock, dummyDataObjRecycler, settingsMap, xmemNozzle
}

func setupMocksCommon(utils *utilsMock.UtilsIface) {
	utils.On("ValidateSettings", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	utils.On("ExponentialBackoffExecutorWithFinishSignal", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&mcMock.ClientIface{}, nil)
}

func setupMocksCompressNeg(utils *utilsMock.UtilsIface) {
	setupMocksCommon(utils)

	var noCompressFeature utilsReal.HELOFeatures
	noCompressFeature.Xattribute = true
	noCompressFeature.CompressionType = base.CompressionTypeNone
	utils.On("SendHELOWithFeatures", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(noCompressFeature, nil)
}

func setupMocksXmem(utils *utilsMock.UtilsIface) {
	setupMocksCommon(utils)

	var allFeatures utilsReal.HELOFeatures
	allFeatures.Xattribute = true
	allFeatures.CompressionType = base.CompressionTypeSnappy
	utils.On("SendHELOWithFeatures", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(allFeatures, nil)
}

func TestPositiveXmemNozzle(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPositiveXmemNozzle =================")
	utils, _, settings, xmem := setupBoilerPlateXmem()
	setupMocksXmem(utils)

	assert.Nil(xmem.initialize(settings))
	fmt.Println("============== Test case end: TestPositiveXmemNozzle =================")
}

func TestNegNoCompressionXmemNozzle(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestNegNoCompressionXmemNozzle =================")
	utils, _, settings, xmem := setupBoilerPlateXmem()
	setupMocksCompressNeg(utils)

	assert.Equal(base.ErrorCompressionNotSupported, xmem.initialize(settings))
	fmt.Println("============== Test case start: TestNegNoCompressionXmemNozzle =================")
}

func TestPosNoCompressionXmemNozzle(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestNegNoCompressionXmemNozzle =================")
	utils, _, settings, xmem := setupBoilerPlateXmem()
	settings[SETTING_COMPRESSION_TYPE] = (base.CompressionType)(base.CompressionTypeNone)
	setupMocksCompressNeg(utils)

	assert.Equal(nil, xmem.initialize(settings))
	fmt.Println("============== Test case start: TestNegNoCompressionXmemNozzle =================")
}

// AUTO is no longer a supported value. XDCR Factory should have passed in a non-auto
func TestPositiveXmemNozzleAuto(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPositiveXmemNozzleAuto =================")
	utils, _, settings, xmem := setupBoilerPlateXmem()
	settings[SETTING_COMPRESSION_TYPE] = (base.CompressionType)(base.CompressionTypeAuto)
	setupMocksXmem(utils)

	assert.NotNil(xmem.initialize(settings))
	fmt.Println("============== Test case end: TestPositiveXmemNozzleAuto =================")
}
