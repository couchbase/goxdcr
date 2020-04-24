// +build !pcre

package parts

import (
	"fmt"
	mcMock "github.com/couchbase/gomemcached/client/mocks"
	base "github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	serviceDefMocks "github.com/couchbase/goxdcr/service_def/mocks"
	utilsReal "github.com/couchbase/goxdcr/utils"
	utilsMock "github.com/couchbase/goxdcr/utils/mocks"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
	gocb "gopkg.in/couchbase/gocb.v1"
	mcc "github.com/couchbase/gomemcached/client"
	"net"
	"reflect"
	"testing"
	"time"
)

const targetClusterName = "C2"
const xmemBucket = "B2"
const xmemPort = "12002"
const targetPort = "9001"
const username = "Administrator"
const password = "wewewe"

var kvString = fmt.Sprintf("%s:%s", "127.0.0.1", xmemPort)
var connString = fmt.Sprintf("%s:%s", "127.0.0.1", targetPort)

func setupBoilerPlateXmem(bname string) (*utilsMock.UtilsIface,
	map[string]interface{},
	*XmemNozzle,
	*Router,
	*serviceDefMocks.BandwidthThrottlerSvc,
	*serviceDefMocks.RemoteClusterSvc,
	*serviceDefMocks.CollectionsManifestSvc) {

	utilitiesMock := &utilsMock.UtilsIface{}
	var vbList []uint16
	for i := 0; i < 1024; i++ {
		vbList = append(vbList, uint16(i))
	}

	bandwidthThrottler := &serviceDefMocks.BandwidthThrottlerSvc{}
	remoteClusterSvc := &serviceDefMocks.RemoteClusterSvc{}

	// local cluster run has KV port starting at 12000
	xmemNozzle := NewXmemNozzle("testId", remoteClusterSvc, "", "testTopic", "testConnPoolNamePrefix", 5, /* connPoolConnSize*/
		kvString, "B1", bname, "temporaryBucketUuid", "Administrator", "wewewe",
		base.CRMode_RevId, log.DefaultLoggerContext, utilitiesMock, vbList)

	// settings map
	settingsMap := make(map[string]interface{})
	settingsMap[SETTING_BATCHCOUNT] = 5

	// Enable compression by default
	settingsMap[SETTING_COMPRESSION_TYPE] = (base.CompressionType)(base.CompressionTypeSnappy)

	// Other live XMEM settings in case cluster_run is active
	settingsMap[SETTING_SELF_MONITOR_INTERVAL] = time.Duration(15 * time.Second)
	settingsMap[SETTING_STATS_INTERVAL] = 10000
	settingsMap[SETTING_OPTI_REP_THRESHOLD] = 0
	settingsMap[SETTING_BATCHSIZE] = 1024
	settingsMap[SETTING_BATCHCOUNT] = 1

	spec, _ := metadata.NewReplicationSpecification("srcBucket", "srcBucketUUID", "targetClusterUUID", "tgtBucket", "tgtBucketUUID")

	colManifestSvc := &serviceDefMocks.CollectionsManifestSvc{}

	router, _ := NewRouter("testId", spec, nil /*downstreamparts*/, nil, /*routingMap*/
		base.CRMode_RevId, log.DefaultLoggerContext, utilitiesMock, nil /*throughputThrottler*/, false, /*highRepl*/
		base.FilterExpDelNone, colManifestSvc, nil /*recycler*/)

	return utilitiesMock, settingsMap, xmemNozzle, router, bandwidthThrottler, remoteClusterSvc, colManifestSvc
}

func targetXmemIsUpAndCorrectSetupExists() bool {
	_, err := net.Listen("tcp4", fmt.Sprintf(":"+targetPort))
	if err == nil {
		return false
	}

	cluster, err := gocb.Connect(fmt.Sprintf("http://127.0.0.1:%s", targetPort))
	if err != nil {
		return false
	}
	cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: username,
		Password: password,
	})
	_, err = cluster.OpenBucket(xmemBucket, "")
	if err != nil {
		return false
	}
	return true
}

func setupMocksCommon(utils *utilsMock.UtilsIface) {
	utils.On("ValidateSettings", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	memcachedMock := &mcMock.ClientIface{}
	memcachedMock.On("Closed").Return(true)

	utils.On("ExponentialBackoffExecutorWithFinishSignal", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(memcachedMock, nil)
}

func setupMocksCompressNeg(utils *utilsMock.UtilsIface) {
	setupMocksCommon(utils)

	var noCompressFeature utilsReal.HELOFeatures
	noCompressFeature.Xattribute = true
	noCompressFeature.CompressionType = base.CompressionTypeNone
	utils.On("SendHELOWithFeatures", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(noCompressFeature, nil)
}

func setupMocksXmem(xmem *XmemNozzle, utils *utilsMock.UtilsIface, bandwidthThrottler *serviceDefMocks.BandwidthThrottlerSvc,
	remoteClusterSvc *serviceDefMocks.RemoteClusterSvc, collectionsManifestSvc *serviceDefMocks.CollectionsManifestSvc) {
	setupMocksCommon(utils)

	var allFeatures utilsReal.HELOFeatures
	allFeatures.Xattribute = true
	allFeatures.CompressionType = base.CompressionTypeSnappy
	allFeatures.Collections = true
	utils.On("SendHELOWithFeatures", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(allFeatures, nil)

	funcThatReturnsNumberOfBytes := func(numberOfBytes, minNumberOfBytes, numberOfBytesOfFirstItem int64) int64 { return numberOfBytes }
	bandwidthThrottler.On("Throttle", mock.AnythingOfType("int64"), mock.AnythingOfType("int64"), mock.AnythingOfType("int64")).Return(funcThatReturnsNumberOfBytes, funcThatReturnsNumberOfBytes)

	xmem.SetBandwidthThrottler(bandwidthThrottler)

	remoteClusterRef, err := metadata.NewRemoteClusterReference("tempUUID", targetClusterName, "127.0.0.1:9001", username, password, false /*demandEncryption*/, "", nil, nil, nil)
	if err != nil {
		fmt.Printf("Error creating RCR: %v\n", err)
	}
	remoteClusterSvc.On("RemoteClusterByUuid", mock.Anything, mock.Anything).Return(remoteClusterRef, nil)
}

func TestPositiveXmemNozzle(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPositiveXmemNozzle =================")
	utils, settings, xmem, _, throttler, remoteClusterSvc, colManSvc := setupBoilerPlateXmem(xmemBucket)
	setupMocksXmem(xmem, utils, throttler, remoteClusterSvc, colManSvc)

	assert.Nil(xmem.initialize(settings))
	fmt.Println("============== Test case end: TestPositiveXmemNozzle =================")
}

func TestNegNoCompressionXmemNozzle(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestNegNoCompressionXmemNozzle =================")
	utils, settings, xmem, _, _, _, _ := setupBoilerPlateXmem(xmemBucket)
	setupMocksCompressNeg(utils)

	assert.Equal(base.ErrorCompressionNotSupported, xmem.initialize(settings))
	fmt.Println("============== Test case start: TestNegNoCompressionXmemNozzle =================")
}

func TestPosNoCompressionXmemNozzle(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestNegNoCompressionXmemNozzle =================")
	utils, settings, xmem, _, _, _, _ := setupBoilerPlateXmem(xmemBucket)
	settings[SETTING_COMPRESSION_TYPE] = (base.CompressionType)(base.CompressionTypeForceUncompress)
	settings[ForceCollectionDisableKey] = true
	setupMocksCompressNeg(utils)

	assert.Equal(nil, xmem.initialize(settings))
	fmt.Println("============== Test case start: TestNegNoCompressionXmemNozzle =================")
}

// AUTO is no longer a supported value. XDCR Factory should have passed in a non-auto
func TestPositiveXmemNozzleAuto(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPositiveXmemNozzleAuto =================")
	utils, settings, xmem, _, throttler, remoteClusterSvc, colManSvc := setupBoilerPlateXmem(xmemBucket)
	settings[SETTING_COMPRESSION_TYPE] = (base.CompressionType)(base.CompressionTypeAuto)
	setupMocksXmem(xmem, utils, throttler, remoteClusterSvc, colManSvc)

	assert.NotNil(xmem.initialize(settings))
	fmt.Println("============== Test case end: TestPositiveXmemNozzleAuto =================")
}

// LIVE CLUSTER RUN TESTS
/*
 * Prerequisites:
 * 1. make dataclean
 * 2. cluster_run -n 2
 * 3. tools/provision.sh
 *
 * If cluster run is up and the buckets are provisioned, this test will read an actual UPR
 * file captured from DCP and actually run it through the XMEM nozzle and write it to a live target
 * cluster, and verify the write
 */
func TestXmemSendAPacket(t *testing.T) {
	fmt.Println("============== Test case start: TestXmemSendAPacket =================")
	defer fmt.Println("============== Test case end: TestXmemSendAPacket =================")

	uprNotCompressFile := "../utils/testInternalData/uprNotCompress.json"
	xmemSendAPacket(t, uprNotCompressFile, xmemBucket)
}

func xmemSendAPacket(t *testing.T, uprfile string, bname string) {
	if !targetXmemIsUpAndCorrectSetupExists() {
		fmt.Println("Skipping since live cluster_run setup has not been detected")
		return
	}

	assert := assert.New(t)

	utilsNotUsed, settings, xmem, router, throttler, remoteClusterSvc, colManSvc := setupBoilerPlateXmem(bname)
	realUtils := utilsReal.NewUtilities()
	xmem.utils = realUtils

	setupMocksXmem(xmem, utilsNotUsed, throttler, remoteClusterSvc, colManSvc)

	// Need to find the actual running targetBucketUUID
	bucketInfo, err := realUtils.GetBucketInfo(connString, bname, username, password, base.HttpAuthMechPlain, nil, false, nil, nil, xmem.Logger())
	assert.Nil(err)
	uuid, ok := bucketInfo["uuid"].(string)
	assert.True(ok)
	xmem.targetBucketUuid = uuid

	event, err := RetrieveUprFile(uprfile)
	assert.Nil(err)

	wrappedEvent := &base.WrappedUprEvent{UprEvent: event}
	wrappedMCRequest, err := router.ComposeMCRequest(wrappedEvent)
	assert.Nil(err)
	assert.NotNil(wrappedMCRequest)

	settings[SETTING_COMPRESSION_TYPE] = base.CompressionTypeSnappy
	settings[ForceCollectionDisableKey] = true
	err = xmem.Start(settings)
	assert.Nil(err)
	xmem.Receive(wrappedMCRequest)

	// retrieve the doc to check
	cluster, err := gocb.Connect(fmt.Sprintf("http://127.0.0.1:%s", targetPort))
	assert.Nil(err)
	cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: username,
		Password: password,
	})

	bucket, err := cluster.OpenBucket(bname, "")
	assert.Nil(err)

	if event.DataType & mcc.XattrDataType == 0 {
		// Get doesn't work if it has XATTR
		var byteSlice []byte
		_, err = bucket.Get(string(event.Key), &byteSlice)
		assert.Nil(err)
		assert.NotEqual(0, len(byteSlice))
	}
}

func TestXmemGet(t *testing.T) {
	fmt.Println("============== Test case start: TestXmemGet =================")
	defer fmt.Println("============== Test case end: TestXmemGet =================")

	if !targetXmemIsUpAndCorrectSetupExists() {
		fmt.Println("Skipping since live cluster_run setup has not been detected")
		return
	}

	uprNotCompressFile := "../utils/testInternalData/uprNotCompress.json"
	xmemSendAPacket(t, uprNotCompressFile, xmemBucket)

	fmt.Println("Sent a packet. Trying to fetch it now.")
	assert := assert.New(t)

	utilsNotUsed, settings, xmem, router, throttler, remoteClusterSvc, colManSvc := setupBoilerPlateXmem(xmemBucket)
	realUtils := utilsReal.NewUtilities()
	xmem.utils = realUtils
	setupMocksXmem(xmem, utilsNotUsed, throttler, remoteClusterSvc, colManSvc)

	// Need to find the actual running targetBucketUUID
	bucketInfo, err := realUtils.GetBucketInfo(connString, xmemBucket, username, password, base.HttpAuthMechPlain, nil, false, nil, nil, xmem.Logger())
	assert.Nil(err)
	uuid, ok := bucketInfo["uuid"].(string)
	assert.True(ok)
	xmem.targetBucketUuid = uuid

	event, err := RetrieveUprFile(uprNotCompressFile)
	assert.Nil(err)
	wrappedEvent := &base.WrappedUprEvent{UprEvent: event}
	wrappedMCRequest, err := router.ComposeMCRequest(wrappedEvent)
	assert.Nil(err)
	assert.NotNil(wrappedMCRequest)

	settings[SETTING_COMPRESSION_TYPE] = base.CompressionTypeSnappy
	settings[ForceCollectionDisableKey] = true
	err = xmem.Start(settings)
	assert.Nil(err)

	possibleConflict_map := make(base.McRequestMap)
	possibleConflict_map[wrappedMCRequest.UniqueKey] = wrappedMCRequest
	rep_map, conflict_map, err := xmem.batchGetWithRetry(&possibleConflict_map)
	assert.Nil(err)
	assert.NotNil(rep_map)
	assert.NotNil(conflict_map)
	assert.Equal(1, len(conflict_map))
	if testing.Verbose() {
		fmt.Printf("event: %v\n", event)
		fmt.Printf("rep_map: %v\n", rep_map)
		fmt.Printf("conflict_map: %v\n", conflict_map)
		for _, docPair := range conflict_map {
			source := docPair.req.Req
			target := docPair.resp
			s := reflect.ValueOf(source).Elem()
			typeOf := s.Type()
			fmt.Printf("Source Document: %v\n", source)
			for i := 0; i < s.NumField(); i++ {
				f := s.Field(i)
				fmt.Printf("%d: %s %s = %v\n", i, typeOf.Field(i).Name, f.Type(), f.Interface())
			}
			s = reflect.ValueOf(target).Elem()
			typeOf = s.Type()
			fmt.Printf("Target Document: %v\n", target)
			for i := 0; i < s.NumField(); i++ {
				f := s.Field(i)
				fmt.Printf("%d: %s %s = %v\n", i, typeOf.Field(i).Name, f.Type(), f.Interface())
			}
		}
	}
}

type User struct {
	Id string `json:"uid"`
	Email string `json:"email"`
	Interests []string `json:"interests"`
}
/*
 * This testcase will create a bucket customCR, send two packets to target,
 * one pre7.0 (kingarthur1), one 7.0 (kingarthur2), and perform conflict resolution
 * using different source documents against the metadata of these two target documents.
 * Some of the source documents have XATTR with PCAS and MV.
 *
 * Test 1: Two pre-7.0 docs, source larger revSeqno/CAS: Source wins.
 * Test 2: Source pre-7.0, target 7.0: Target wins.
 * Test 3: Source 7.0, target pre-7.0, source smaller CAS: Conflict
 * Test 4: Source 7.0, target pre-7.0, source larger CAS and no PCAS/MV: Conflict
 * Test 5: Source 7.0, target pre-7.0. source larger CAS and dominating MV: Source wins
 * Test 6: Two 7.0 docs, same clusterID, source larger CAS: Source wins 
 * Test 7: Two 7.0 docs, same clusterID, source smaller CAS: Source loses
 * Test 8: Two 7.0 docs, different clusterID, source larger CAS, No PCAS: Conflict
 * Test 9: Two 7.0 docs, different clusterID, source smaller CAS: Source loses
 * Test 10: Two 7.0 docs, different clusterID, source larger CAS and dominating PCAS: Source wins
 * Test 11: Two 7.0 docs. different clusterID, source larger CAS and dominating MV: Source wins
 */
func TestGetMetaForCustomCR(t *testing.T) {
	fmt.Println("============== Test case start: GeteMetaForCustomCR =================")
	defer fmt.Println("============== Test case end: GeteMetaForCustomCR =================")

	if !targetXmemIsUpAndCorrectSetupExists() {
		fmt.Println("Skipping since live cluster_run setup has not been detected")
		return
	}

	assert := assert.New(t)
	bucketName := "CustomCR"

	cluster, err := gocb.Connect(fmt.Sprintf("http://127.0.0.1:%s", "9001"))
	assert.Nil(err)

	cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: username,
		Password: password,
	})
	cm := cluster.Manager(username, password)

	err = cm.RemoveBucket(bucketName)
	bucketSettings := gocb.BucketSettings{false, false, bucketName, "", 100, 0, gocb.Couchbase}
	err = cm.InsertBucket(&bucketSettings)
	assert.Nil(err)

	bucket, err := cluster.OpenBucket(bucketName, "")
	for err != nil {
		bucket, err = cluster.OpenBucket(bucketName, "")
	}
	assert.Nil(err)
	assert.NotNil(bucket)

	time.Sleep(2 * time.Second)
	//uprNotCompressFile := "../utils/testInternalData/uprNotCompress.json"

	// Set up target with a pre-7.0 and 7.0 document
	kingarthur1_pre7_cas1 := "testdata/customCR/kingarthur1_pre7_cas1.json"
	xmemSendAPacket(t, kingarthur1_pre7_cas1, bucketName)
	kingarthur2_cluster2_cas1 := "testdata/customCR/kingarthur2_cluster2_cas1.json"
	xmemSendAPacket(t, kingarthur2_cluster2_cas1, bucketName)

	utilsNotUsed, settings, xmem, router, throttler, remoteClusterSvc, colManSvc := setupBoilerPlateXmem(bucketName)
	realUtils := utilsReal.NewUtilities()
	xmem.utils = realUtils
	setupMocksXmem(xmem, utilsNotUsed, throttler, remoteClusterSvc, colManSvc)

	// Need to find the actual running targetBucketUUID
	bucketInfo, err := realUtils.GetBucketInfo(connString, bucketName, username, password, base.HttpAuthMechPlain, nil, false, nil, nil, xmem.Logger())
	assert.Nil(err)
	uuid, ok := bucketInfo["uuid"].(string)
	assert.True(ok)
	xmem.targetBucketUuid = uuid

	settings[SETTING_COMPRESSION_TYPE] = base.CompressionTypeSnappy
	settings[ForceCollectionDisableKey] = true
	err = xmem.Start(settings)
	assert.Nil(err)

	/*
	 * Source doc: kingarthur1_pre7_cas2
	 * Target doc: kingarthur1_pre7_cas1
	 * Source wins
	 */
	fmt.Println("Test 1: Two pre-7.0 docs, source larger revSeqno/CAS: Source wins.")
	getMetaForCustomCR(1, t, "testdata/customCR/kingarthur1_pre7_cas2.json", xmem, router, SourceDominate)

	/*
	 * Source doc: kingarthur2_pre7_cas2
	 * Target doc: kingarthur2_cluster2_cas1
	 * Target wins.
	 */

	fmt.Println("Test 2: Source pre-7.0, target 7.0: Target wins.")
	getMetaForCustomCR(2, t, "testdata/customCR/kingarthur2_pre7_cas2.json", xmem, router, TargetDominate)

	/*
	 * Source doc: kingarthur1_cluster1_cas0
	 * Target doc: kingarthur1_pre7_cas1
	 * Conflict
	 */
	fmt.Println("Test 3: Source 7.0, target pre-7.0, source smaller CAS: Conflict")
	getMetaForCustomCR(3, t, "testdata/customCR/kingarthur1_cluster1_cas0.json", xmem, router, Conflict)

	/*
	 * Source doc: kingarthur1_cluster1_cas2
	 * Target doc: kingarthur1_pre7_cas1
	 * Conflict
	 */
	fmt.Println("Test 4: Source 7.0, target pre-7.0, source larger CAS and no PCAS/MV: Conflict")
	getMetaForCustomCR(4, t, "testdata/customCR/kingarthur1_cluster1_cas2.json", xmem, router, Conflict)

	/*
	 * Source doc: kingarthur1_cluster1_pcasRevId.json
	 * Target doc: kingarthur1_pre7_cas1
	 * Conflict
	 */
	resetLogLevel := false
	if testing.Verbose() {
		log.DefaultLoggerContext.SetLogLevel(log.LogLevelDebug)
		resetLogLevel = true
	}
	fmt.Println("Test 5: Source 7.0, target pre-7.0. source larger CAS and dominating PCAS: Source wins")
	getMetaForCustomCR(5, t, "testdata/customCR/kingarthur1_cluster1_pcasRevId.json", xmem, router, SourceDominate)
	if resetLogLevel {
		log.DefaultLoggerContext.SetLogLevel(log.LogLevelInfo)
	}

	/*
	 * Source doc: kingarthur2_cluster2_cas2
	 * Target doc: kingarthur2_cluster2_cas1
	 * Source wins
	 */
	fmt.Println("Test 6: Two 7.0 docs, same clusterID, source larger CAS: Source wins")
	getMetaForCustomCR(6, t, "testdata/customCR/kingarthur2_cluster2_cas2.json", xmem, router, SourceDominate)

	/*
	 * Source doc: kingarthur2_cluster2_cas0
	 * Target doc: kingarthur2_cluster2_cas1
	 * Souce loses
	 */
	fmt.Println("Test 7: Two 7.0 docs, same clusterID, source smaller CAS: Source loses")
	getMetaForCustomCR(7, t, "testdata/customCR/kingarthur2_cluster2_cas0.json", xmem, router, TargetDominate)

	/*
	 * Source doc: kingarthur2_cluster1_cas2
	 * Target doc: kingarthur2_cluster2_cas1
	 * Conflict and it is in possibleConflict_map
	 */
	fmt.Println("Test 8: Two 7.0 docs, different clusterID, source larger CAS, No PCAS: Conflict")
	getMetaForCustomCR(8, t, "testdata/customCR/kingarthur2_cluster1_cas2.json", xmem, router, Conflict)

	/*
	 * Source doc: kingarthur2_cluster1_cas0
	 * Target doc: kingarthur2_cluster2_cas1
	 * Source loses
	 */
	fmt.Println("Test 9: Two 7.0 docs, different clusterID, source smaller CAS:	Source loses")
	getMetaForCustomCR(9, t, "testdata/customCR/kingarthur2_cluster1_cas0.json", xmem, router, TargetDominate)

	/*
	 * Source doc: kingarthur2_cluster1_pcasC2
	 * Target doc: kingarthur2_cluster2_cas1
	 * Source PCAS dominate
	 */

	if testing.Verbose() {
		log.DefaultLoggerContext.SetLogLevel(log.LogLevelDebug)
		resetLogLevel = true
	}
	fmt.Println("Test 10: Two 7.0 docs, different clusterID, source larger CAS and dominating PCAS: Source wins")
	getMetaForCustomCR(10, t, "testdata/customCR/kingarthur2_cluster1_pcasC2.json", xmem, router, SourceDominate)

	/*
	 * Source doc: kingarthur2_cluster1_mvC2
	 * Target doc: kingarthur2_cluster2_cas1
	 * Source PCAS dominate
	 */
	fmt.Println("Test 11: Two 7.0 docs. different clusterID, source larger CAS and dominating MV: Source wins")
	getMetaForCustomCR(11, t, "testdata/customCR/kingarthur2_cluster1_mvC2.json", xmem, router, SourceDominate)
	if resetLogLevel {
		log.DefaultLoggerContext.SetLogLevel(log.LogLevelInfo)
	}
}
func getMetaForCustomCR(testId uint32, t *testing.T, fname string, xmem *XmemNozzle, router *Router, expectedResult ConflictResult) {
	assert := assert.New(t)
	event, err := RetrieveUprFile(fname)
	assert.Nil(err)
	wrappedEvent := &base.WrappedUprEvent{UprEvent: event}
	wrappedMCRequest, err := router.ComposeMCRequest(wrappedEvent)
	assert.Nil(err)
	assert.NotNil(wrappedMCRequest)
	getMeta_map := make(base.McRequestMap)
	getMeta_map[wrappedMCRequest.UniqueKey] = wrappedMCRequest
	rep_map, possibleConflict_map, err := xmem.batchGetMetaForCustomCR(getMeta_map)
	assert.Nil(err)
	if expectedResult == SourceDominate {
		assert.Equal(1, len(rep_map), fmt.Sprintf("Test %d failed", testId))
	} else {
		assert.Equal(0, len(rep_map), fmt.Sprintf("Test %d failed", testId))
	}
	if expectedResult == Conflict {
		assert.Equal(1, len(possibleConflict_map), fmt.Sprintf("Test %d failed", testId))
	} else {
		assert.Equal(0, len(possibleConflict_map), fmt.Sprintf("Test %d failed", testId))
	}
}
func TestSourceXattrDominate(t *testing.T) {
	fmt.Println("============== Test case start: SourceXattrDominate =================")
	defer fmt.Println("============== Test case end: SourceXattrDominate =================")

	if !targetXmemIsUpAndCorrectSetupExists() {
		fmt.Println("Skipping since live cluster_run setup has not been detected")
		return
	}

	uprNotCompressFile := "testdata/customCR/kingarthur2_cluster1_pcasC2.json"
	xmemSendAPacket(t, uprNotCompressFile, xmemBucket)

	fmt.Println("Sent a packet. Trying to fetch it now.")
	assert := assert.New(t)

	utilsNotUsed, settings, xmem, router, throttler, remoteClusterSvc, colManSvc := setupBoilerPlateXmem(xmemBucket)
	realUtils := utilsReal.NewUtilities()
	xmem.utils = realUtils
	setupMocksXmem(xmem, utilsNotUsed, throttler, remoteClusterSvc, colManSvc)

	// Need to find the actual running targetBucketUUID
	bucketInfo, err := realUtils.GetBucketInfo(connString, xmemBucket, username, password, base.HttpAuthMechPlain, nil, false, nil, nil, xmem.Logger())
	assert.Nil(err)
	uuid, ok := bucketInfo["uuid"].(string)
	assert.True(ok)
	xmem.targetBucketUuid = uuid

	event, err := RetrieveUprFile(uprNotCompressFile)
	assert.Nil(err)
	wrappedEvent := &base.WrappedUprEvent{UprEvent: event}
	wrappedMCRequest, err := router.ComposeMCRequest(wrappedEvent)
	assert.Nil(err)
	assert.NotNil(wrappedMCRequest)

	settings[SETTING_COMPRESSION_TYPE] = base.CompressionTypeSnappy
	settings[ForceCollectionDisableKey] = true
	err = xmem.Start(settings)
	assert.Nil(err)

	var clusterID uint64 = 1<<47 + 2000
	res := xmem.sourceXattrDominate(wrappedMCRequest.Req, clusterID, 1)
	assert.Equal(res, true)
}