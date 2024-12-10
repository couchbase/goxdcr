//go:build !pcre
// +build !pcre

/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package parts

import (
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"time"

	mc "github.com/couchbase/gomemcached"
	mcMock "github.com/couchbase/gomemcached/client/mocks"
	base "github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common/mocks"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	serviceDefMocks "github.com/couchbase/goxdcr/service_def/mocks"
	utilsReal "github.com/couchbase/goxdcr/utils"
	utilsMock "github.com/couchbase/goxdcr/utils/mocks"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
	//gocb "gopkg.in/couchbase/gocb.v1"
)

const targetClusterName = "C2"
const sourceClusterName = "C1"
const xmemBucket = "B2"
const xmemPort = "12002"
const sourcePort = "9000"
const targetPort = "9001"
const username = "Administrator"
const password = "wewewe"

var kvString = fmt.Sprintf("%s:%s", "127.0.0.1", xmemPort)
var connString = fmt.Sprintf("%s:%s", "127.0.0.1", targetPort)

var events []string
var eventID int64
var err error

func setupBoilerPlateXmem(bname string, optional ...int) (*utilsMock.UtilsIface,
	map[string]interface{},
	*XmemNozzle,
	*Router,
	*serviceDefMocks.BandwidthThrottlerSvc,
	*serviceDefMocks.RemoteClusterSvc,
	*serviceDefMocks.CollectionsManifestSvc,
	*mocks.PipelineEventsProducer) {

	utilitiesMock := &utilsMock.UtilsIface{}
	utilitiesMock.On("NewDataPool").Return(base.NewFakeDataPool())
	var vbList []uint16
	for i := 0; i < 1024; i++ {
		vbList = append(vbList, uint16(i))
	}

	bandwidthThrottler := &serviceDefMocks.BandwidthThrottlerSvc{}
	remoteClusterSvc := &serviceDefMocks.RemoteClusterSvc{}

	// local cluster run has KV port starting at 12000
	xmemNozzle := NewXmemNozzle("testId", remoteClusterSvc, "", "", "testTopic", "testConnPoolNamePrefix", 5, /* connPoolConnSize*/
		kvString, "B1", bname, "temporaryBucketUuid", "Administrator", "wewewe",
		base.CRMode_RevId, log.DefaultLoggerContext, utilitiesMock, vbList, nil)

	// settings map
	settingsMap := make(map[string]interface{})
	settingsMap[SETTING_BATCHCOUNT] = 5

	// Enable compression by default
	settingsMap[SETTING_COMPRESSION_TYPE] = (base.CompressionType)(base.CompressionTypeSnappy)

	// Other live XMEM settings in case cluster_run is active
	settingsMap[SETTING_SELF_MONITOR_INTERVAL] = time.Duration(15 * time.Second)
	if len(optional) > 0 {
		settingsMap[SETTING_SELF_MONITOR_INTERVAL] = time.Duration(time.Duration(optional[0]) * time.Second)
	}
	settingsMap[SETTING_STATS_INTERVAL] = 10000
	settingsMap[SETTING_OPTI_REP_THRESHOLD] = 0
	settingsMap[SETTING_BATCHSIZE] = 1024
	settingsMap[SETTING_BATCHCOUNT] = 1

	spec, _ := metadata.NewReplicationSpecification("srcBucket", "srcBucketUUID", "targetClusterUUID", "tgtBucket", "tgtBucketUUID")

	colManifestSvc := &serviceDefMocks.CollectionsManifestSvc{}

	producer := &mocks.PipelineEventsProducer{}

	router, _ := NewRouter("testId", spec, nil, nil, base.CRMode_RevId, log.DefaultLoggerContext, utilitiesMock, nil, false, base.FilterExpDelNone, colManifestSvc, nil, nil, metadata.UnitTestGetCollectionsCapability(), nil, nil, producer)

	producer.On("DismissEvent", mock.Anything).Return(err).Run(func(args mock.Arguments) {
		id, ok := args.Get(0).(int)
		N := len(events)
		if ok && id < N && id >= 0 {
			events[id], events[N-1] = events[N-1], events[id]
			events = events[0 : N-1]
			err = nil
		} else {
			err = base.ErrorNotFound
		}
	})

	producer.On("AddEvent", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(eventID).Run(func(args mock.Arguments) {
		msg := args.Get(1).(string)
		events = append(events, msg)
		eventID = int64(len(events) - 1)
		fmt.Println(events)
	})

	producer.On("UpdateEvent", mock.Anything, mock.Anything, mock.Anything).Return(err).Run(func(args mock.Arguments) {
		id, ok := args.Get(0).(int64)
		if ok && int(id) < len(events) && id >= 0 {
			msg, ok := args.Get(1).(string)
			if ok {
				events[id] = msg
				err = nil
				return
			}
		}
		err = base.ErrorNotFound
	})

	return utilitiesMock, settingsMap, xmemNozzle, router, bandwidthThrottler, remoteClusterSvc, colManifestSvc, producer
}

//func targetXmemIsUpAndCorrectSetupExists() bool {
//	_, err := net.Listen("tcp4", fmt.Sprintf(":"+targetPort))
//	if err == nil {
//		return false
//	}
//
//	cluster, err := gocb.Connect(fmt.Sprintf("http://127.0.0.1:%s", targetPort))
//	if err != nil {
//		return false
//	}
//	cluster.Authenticate(gocb.PasswordAuthenticator{
//		Username: username,
//		Password: password,
//	})
//	_, err = cluster.OpenBucket(xmemBucket, "")
//	if err != nil {
//		return false
//	}
//	return true
//}

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
	remoteClusterSvc *serviceDefMocks.RemoteClusterSvc, collectionsManifestSvc *serviceDefMocks.CollectionsManifestSvc, evtProducer *mocks.PipelineEventsProducer) {
	setupMocksCommon(utils)

	var allFeatures utilsReal.HELOFeatures
	allFeatures.Xattribute = true
	allFeatures.CompressionType = base.CompressionTypeSnappy
	allFeatures.Collections = true
	utils.On("SendHELOWithFeatures", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(allFeatures, nil)

	funcThatReturnsNumberOfBytes := func(numberOfBytes, minNumberOfBytes, numberOfBytesOfFirstItem int64) int64 { return numberOfBytes }
	bandwidthThrottler.On("Throttle", mock.AnythingOfType("int64"), mock.AnythingOfType("int64"), mock.AnythingOfType("int64")).Return(funcThatReturnsNumberOfBytes, funcThatReturnsNumberOfBytes)

	xmem.SetBandwidthThrottler(bandwidthThrottler)

	remoteClusterRef, err := metadata.NewRemoteClusterReference("tempUUID", targetClusterName, "127.0.0.1:9001", username, password, "", /*hostnameMode*/
		false /*demandEncryption*/, "", nil, nil, nil, nil)
	if err != nil {
		fmt.Printf("Error creating RCR: %v\n", err)
	}
	remoteClusterSvc.On("RemoteClusterByUuid", mock.Anything, mock.Anything).Return(remoteClusterRef, nil)
	xmem.sourceClusterId = []byte("SourceCluster")
	xmem.targetClusterId = []byte("TargetCluster")
	xmem.eventsProducer = evtProducer
}

func setupMocksConflictMgr(xmem *XmemNozzle) *serviceDefMocks.ConflictManagerIface {
	conflictMgr := &serviceDefMocks.ConflictManagerIface{}
	conflictMgr.On("ResolveConflict", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	conflictMgr.On("SetBackToSource", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	xmem.SetConflictManager(conflictMgr)
	return conflictMgr
}
func TestPositiveXmemNozzle(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPositiveXmemNozzle =================")
	utils, settings, xmem, _, throttler, remoteClusterSvc, colManSvc, ep := setupBoilerPlateXmem(xmemBucket)
	setupMocksXmem(xmem, utils, throttler, remoteClusterSvc, colManSvc, ep)

	assert.Nil(xmem.initialize(settings))
	fmt.Println("============== Test case end: TestPositiveXmemNozzle =================")
}

func TestNegNoCompressionXmemNozzle(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestNegNoCompressionXmemNozzle =================")
	utils, settings, xmem, _, _, _, _, _ := setupBoilerPlateXmem(xmemBucket)
	setupMocksCompressNeg(utils)

	assert.Equal(base.ErrorCompressionNotSupported, xmem.initialize(settings))
	fmt.Println("============== Test case start: TestNegNoCompressionXmemNozzle =================")
}

func TestPosNoCompressionXmemNozzle(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestNegNoCompressionXmemNozzle =================")
	utils, settings, xmem, _, _, _, _, _ := setupBoilerPlateXmem(xmemBucket)
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
	utils, settings, xmem, _, throttler, remoteClusterSvc, colManSvc, ep := setupBoilerPlateXmem(xmemBucket)
	settings[SETTING_COMPRESSION_TYPE] = (base.CompressionType)(base.CompressionTypeAuto)
	setupMocksXmem(xmem, utils, throttler, remoteClusterSvc, colManSvc, ep)

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
//func TestXmemSendAPacket(t *testing.T) {
//	fmt.Println("============== Test case start: TestXmemSendAPacket =================")
//	defer fmt.Println("============== Test case end: TestXmemSendAPacket =================")
//
//	uprNotCompressFile := "../utils/testInternalData/uprNotCompress.json"
//	xmemSendPackets(t, []string{uprNotCompressFile}, xmemBucket)
//}

//func xmemSendPackets(t *testing.T, uprfiles []string, bname string) {
//	if !targetXmemIsUpAndCorrectSetupExists() {
//		fmt.Println("Skipping since live cluster_run setup has not been detected")
//		return
//	}
//
//	assert := assert.New(t)
//
//	utilsNotUsed, settings, xmem, router, throttler, remoteClusterSvc, colManSvc, ep := setupBoilerPlateXmem(bname)
//	realUtils := utilsReal.NewUtilities()
//	xmem.utils = realUtils
//
//	setupMocksXmem(xmem, utilsNotUsed, throttler, remoteClusterSvc, colManSvc, ep)
//
//	// Need to find the actual running targetBucketUUID
//	bucketInfo, err := realUtils.GetBucketInfo(connString, bname, username, password, base.HttpAuthMechPlain, nil, false, nil, nil, xmem.Logger())
//	assert.Nil(err)
//	uuid, ok := bucketInfo["uuid"].(string)
//	assert.True(ok)
//	xmem.targetBucketUuid = uuid
//	settings[SETTING_COMPRESSION_TYPE] = base.CompressionTypeSnappy
//	settings[ForceCollectionDisableKey] = true
//	err = xmem.Start(settings)
//	assert.Nil(err)
//
//	// Send the events
//	var events []*mcc.UprEvent
//	for _, uprfile := range uprfiles {
//		event, err := RetrieveUprFile(uprfile)
//		assert.Nil(err)
//		events = append(events, event)
//
//		wrappedEvent := &base.WrappedUprEvent{UprEvent: event}
//		wrappedMCRequest, err := router.ComposeMCRequest(wrappedEvent)
//		assert.Nil(err)
//		assert.NotNil(wrappedMCRequest)
//		xmem.Receive(wrappedMCRequest)
//	}
//
//	// retrieve the doc to check
//	cluster, err := gocb.Connect(fmt.Sprintf("http://127.0.0.1:%s", targetPort))
//	assert.Nil(err)
//	cluster.Authenticate(gocb.PasswordAuthenticator{
//		Username: username,
//		Password: password,
//	})
//
//	bucket, err := cluster.OpenBucket(bname, "")
//	assert.Nil(err)
//
//	for _, event := range events {
//		if event.DataType&mcc.XattrDataType == 0 {
//			// Get doesn't work if it has XATTR
//			var byteSlice []byte
//			_, err = bucket.Get(string(event.Key), &byteSlice)
//			assert.Nil(err)
//			assert.NotEqual(0, len(byteSlice))
//		}
//	}
//}
//func setUpTargetBucket(t *testing.T, bucketName string) {
//	assert := assert.New(t)
//
//	cluster, err := gocb.Connect(fmt.Sprintf("http://127.0.0.1:%s", "9001"))
//	assert.Nil(err)
//
//	cluster.Authenticate(gocb.PasswordAuthenticator{
//		Username: username,
//		Password: password,
//	})
//	cm := cluster.Manager(username, password)
//
//	bucketSettings := gocb.BucketSettings{false, false, bucketName, "", 100, 0, gocb.Couchbase}
//	_ = cm.InsertBucket(&bucketSettings)
//
//	bucket, err := cluster.OpenBucket(bucketName, "")
//	for err != nil {
//		bucket, err = cluster.OpenBucket(bucketName, "")
//	}
//	assert.Nil(err)
//	assert.NotNil(bucket)
//
//	time.Sleep(2 * time.Second)
//}

type User struct {
	Id        string   `json:"uid"`
	Email     string   `json:"email"`
	Interests []string `json:"interests"`
}

/*
 * This testcase will create a bucket GetXattrForCCR and test conflict detection.
 *
 * To avoid tightly couple the test files with real clusterID/UUID, we will use
 * "SourceCluster" and "TargetCluster" as clusterIDs for the tests.
 */
//func TestGetXattrForCustomCR(t *testing.T) {
//	fmt.Println("============== Test case start: TestGetXattrForCustomCR =================")
//	defer fmt.Println("============== Test case end: TestGetXattrForCustomCR =================")
//
//	if !targetXmemIsUpAndCorrectSetupExists() {
//		fmt.Println("Skipping since live cluster_run setup has not been detected")
//		return
//	}
//	bucketName := "GetXattrForCCR"
//	setUpTargetBucket(t, bucketName)
//
//	assert := assert.New(t)
//
//	// Set up target documents
//	xmemSendPackets(t, []string{"testdata/customCR/kingarthur2_new_cas2.json",
//		"testdata/customCR/kingarthur3_cv1_cas1.json",
//		"testdata/customCR/kingarthur4_cv1_cas2.json",
//		"testdata/customCR/kingarthur5_cv1_cas2.json",
//		"testdata/customCR/kingarthur6_mv.json",
//		"testdata/customCR/kingarthur7_mv.json",
//		"testdata/customCR/kingarthur8_cas1.json",
//	}, bucketName)
//
//	utilsNotUsed, settings, xmem, router, throttler, remoteClusterSvc, colManSvc, ep := setupBoilerPlateXmem(bucketName)
//	realUtils := utilsReal.NewUtilities()
//	xmem.utils = realUtils
//	setupMocksXmem(xmem, utilsNotUsed, throttler, remoteClusterSvc, colManSvc, ep)
//
//	// Need to find the actual running targetBucketUUID
//	bucketInfo, err := realUtils.GetBucketInfo(connString, bucketName, username, password, base.HttpAuthMechPlain, nil, false, nil, nil, xmem.Logger())
//	assert.Nil(err)
//	uuid, ok := bucketInfo["uuid"].(string)
//	assert.True(ok)
//	xmem.targetBucketUuid = uuid
//	settings[SETTING_COMPRESSION_TYPE] = base.CompressionTypeSnappy
//	settings[ForceCollectionDisableKey] = true
//	xmem.Logger().LoggerContext().SetLogLevel(log.LogLevelDebug)
//	err = xmem.Start(settings)
//	assert.Nil(err)
//	/*
//	 * Test 1: Doc never existed at target, source wins
//	 * Source doc: kingarthur1_new
//	 * Target doc: none
//	 */
//	fmt.Println("Test 1: Source dominates when it doesn't exist at target.")
//	getXattrForCustomCR(1, t, "testdata/customCR/kingarthur1_new.json", xmem, router, SourceDominate)
//	/*
//	 * Test 2: Target larger CAS. Target wins
//	 * Source: kingarthur2_new_cas1
//	 * Target: kingarthur2_new_cas2
//	 */
//	fmt.Println("Test 2: Target dominates when it has larger CAS.")
//	getXattrForCustomCR(2, t, "testdata/customCR/kingarthur2_new_cas1.json", xmem, router, TargetDominate)
//	/*
//	 * Test 3: Target doc comes from source. Source larger CAS. Source wins
//	 * Source: kingarthur3_new_cas2
//	 * Target: kingarthur3_cv1_cas1 (cv1 == cas1)
//	 *         _xdcr{"cv":"0x0000ccae473b1316","id":"SourceCluster"}
//	 */
//	fmt.Println("Test 3: Source larger CAS. Target comes from source with no new change. Source wins")
//	getXattrForCustomCR(3, t, "testdata/customCR/kingarthur3_new_cas2.json", xmem, router, SourceDominate)
//	/*
//	 * Test 4: Both has local changes and source pcas does not dominate. Conflict
//	 * Source: kingarthur4_pcasNoTarget_cas3:
//	 *		_xdcr{"cv":"0x0000ccae473b1316","id":"Cluster4","pc":{"cluster3":"Fge5BJLoAAQ"}}
//	 *		Fge5BJLoAAQ is base64 of 1587440822967074820
//	 * Target: kingarthur4_cv1_cas2, new change (Cas > cv)
//	 *		_xdcr{"cv":"0x0000ccae473b1316","id":"SourceCluster"}
//	 */
//	fmt.Println("Test 4: Both has changes (CAS > cv), source has PCAS but does not dominate. Conflict")
//	getXattrForCustomCR(4, t, "testdata/customCR/kingarthur4_pcasNoTarget_cas3.json", xmem, router, Conflict)
//
//	/*
//	 * Test 5: Both has local changes and source pcas that dominates target change
//	 * Source: kingarthur5_pcasDominateTargetCas.json:
//	 *		_xdcr{"cv":"0x0b00f8da4d881416","id":"Cluster3","pc":{"TargetCluster":"FhSITdr4AAA"}}
//	 * Target: kingarthur5_cv1_cas2, new change (Cas > cv)
//	 *		_xdcr{"cv":"0x0400e89204b90716","id":"SourceCluster"}, Cas: 1591046436336173056
//	 */
//	fmt.Println("Test 5: Both has changes (CAS > cv), source has PCAS that dominates.")
//	getXattrForCustomCR(5, t, "testdata/customCR/kingarthur5_pcasDominateTargetCas.json", xmem, router, SourceDominate)
//	/*
//	 * Test 6: Target is a merged doc (has mv, CAS == cv). Source PCAS dominates
//	 * Source: kingarthur6_pcasDominateMv
//	 *		_xdcr{"cv":"0x0b0085b25e8d1416","id":"Cluster4","pc":{"Cluster1":"FhSITdr4AAA","Cluster2":"FhSITdr4ABU","Cluster3":"FhSITdr4ACA"}}
//	 * Target: kingarthur6_mv, no new change (Cas == cv)
//	 *		_xdcr{"cv":"0x00009125328d1416","id":"TargetCluster","mv":{"Cluster1":"FhSITdr4AAA","Cluster2":"FhSITdr4ABU","Cluster3":"FhSITdr4ACA"}}
//	 */
//	fmt.Println("Test 6: Target is a merged doc, source PCAS dominates MV.")
//	getXattrForCustomCR(6, t, "testdata/customCR/kingarthur6_pcasDominateMv.json", xmem, router, SourceDominate)
//	/*
//	 * Test 7: Target is a merged doc (has mv, CAS == cv). Source PCAS does not dominate
//	 * Source: kingarthur7_pcasNotDominateMv
//	 *		_xdcr{"cv":"0x0b0085b25e8d1416","id":"Cluster4","pc":{"Cluster1":"FhSITdr4AAA","Cluster3":"FhSITdr4ACA"}}
//	 * Target: kingarthur7_mv, no new change (Cas == cv)
//	 *		_xdcr{"cv":"0x00009125328d1416","id":"TargetCluster","mv":{"Cluster1":"FhSITdr4AAA","Cluster2":"FhSITdr4ABU","Cluster3":"FhSITdr4ACA"}}
//	 */
//	fmt.Println("Test 7: Target is a merged doc, source PCAS does not dominates. Conflict.")
//	getXattrForCustomCR(7, t, "testdata/customCR/kingarthur7_pcasNotDominateMv.json", xmem, router, Conflict)
//
//	/*
//	 * Test 8: Target does not have _xdcr, Source pcas dominates target
//	 * Source: kingarthur8_pcasDominateTargetCas1
//	 *		_xdcr{"cv":"0x0b00104adc921416","id":"Cluster4","pc":{"Cluster2":"FhSITdr4ABU","TargetCluster":"FhSS3EoQAAA"}}
//	 * Target: kingarthur8_cas1
//	 */
//	fmt.Println("Test 8: Target does not have _xdcr, source PCAS dominates target CAS.")
//	getXattrForCustomCR(8, t, "testdata/customCR/kingarthur8_pcasDominateTargetCas1.json", xmem, router, SourceDominate)
//	/*
//	 * Test 9: Source larger CAS, both merged doc, but target MV dominates source MV. Setback
//	 * Source: kingarthur6_pcasDominateMv
//	 *		_xdcr{"cv":"0x0000d4a7994f1716","id":"SourceCluster","mv":{"Cluster1":"FhSITdr4AAA","Cluster2":"FhSITdr4ABU"}}
//	 * Target: kingarthur6_mv, no new change (Cas == cv)
//	 *		_xdcr{"cv":"0x00009125328d1416","id":"TargetCluster","mv":{"Cluster1":"FhSITdr4AAA","Cluster2":"FhSITdr4ABU","Cluster3":"FhSITdr4ACA"}}
//	 */
//	fmt.Println("Test 9: Source larger CAS, but target MV dominates source MV. TargetSetBack.")
//	getXattrForCustomCR(9, t, "testdata/customCR/kingarthur6_largerCasSmallerMv.json", xmem, router, TargetSetBack)
//}
func getXattrForCustomCR(testId uint32, t *testing.T, fname string, xmem *XmemNozzle, router *Router, expectedResult ConflictResult) {
	assert := assert.New(t)
	event, err := RetrieveUprFile(fname)
	assert.Nil(err)
	wrappedEvent := &base.WrappedUprEvent{UprEvent: event}
	wrappedMCRequest, err := router.ComposeMCRequest(wrappedEvent)
	assert.Nil(err)
	assert.NotNil(wrappedMCRequest)
	getMeta_map := make(base.McRequestMap)
	getMeta_map[wrappedMCRequest.UniqueKey] = wrappedMCRequest
	noRep_map, getDoc_map, err := xmem.batchGetXattrForCustomCR(getMeta_map)
	assert.Nil(err)
	if expectedResult == SourceDominate {
		assert.Equal(0, len(noRep_map), fmt.Sprintf("Test %d failed", testId))
		assert.Equal(0, len(getDoc_map), fmt.Sprintf("Test %d failed", testId))
	} else if expectedResult == TargetDominate {
		assert.Equal(1, len(noRep_map), fmt.Sprintf("Test %d failed", testId))
		assert.Equal(0, len(getDoc_map), fmt.Sprintf("Test %d failed", testId))
	} else if expectedResult == Conflict || expectedResult == TargetSetBack {
		assert.Equal(1, len(noRep_map), fmt.Sprintf("Test %d failed", testId))
		assert.Equal(1, len(getDoc_map), fmt.Sprintf("Test %d failed", testId))
	}
	if len(getDoc_map) > 0 {
		_, err := xmem.batchGetDocForCustomCR(getDoc_map, noRep_map)
		assert.Nil(err)
	}
}
func printMultiLookupResult(testId uint32, t *testing.T, body []byte) {
	assert := assert.New(t)
	var i = 0
	status := binary.BigEndian.Uint16(body[i:])
	i = i + 2
	len := int(binary.BigEndian.Uint32(body[i:]))
	i = i + 4
	fmt.Println("SUBDOC_MULTI_LOOKUP response:")
	if len > 0 {
		xattr := body[i : i+len]
		fmt.Printf("status=%v, XATTR=%s\n", status, xattr)
		i = i + len
	}
	status = binary.BigEndian.Uint16(body[i:])
	i = i + 2
	len = int(binary.BigEndian.Uint32(body[i:]))
	i = i + 4
	assert.Greater(len, 0, fmt.Sprintf("Test %d failed", testId))
	doc := body[i : i+len]
	fmt.Printf("status=%v, Document=%s\n", status, doc)
}

func eventExists(ev []string) bool {
	contains := true
	allEvents := strings.Join(events, ";")
	for _, e := range ev {
		contains = contains && strings.Contains(allEvents, e)
	}
	return contains
}
func TestNonTempErrorResponsesEvents(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping TestNonTempErrorResponsesEvents")
	}
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestNonTempErrorResponsesEvents =================")
	defer fmt.Println("============== Test case end: TestNonTempErrorResponsesEvents =================")

	utils, settings, xmem, _, throttler, remoteClusterSvc, colManSvc, eventProducer := setupBoilerPlateXmem(xmemBucket, 3)
	setupMocksXmem(xmem, utils, throttler, remoteClusterSvc, colManSvc, eventProducer)

	assert.Nil(xmem.initialize(settings))
	assert.Nil(xmem.Start(settings))

	success := mc.SUCCESS
	fail1 := mc.EINVAL
	fail2 := mc.DURABILITY_IMPOSSIBLE
	fail3 := mc.DURABILITY_INVALID_LEVEL
	fail4 := mc.NOT_STORED
	responseStr := "responseStatus"
	// send 2 success packets at pos 0 - sleep 5s - [<>, <>, <>, <>, <>, <>, <>, <>]
	xmem.updateErrsForUIAlert(&mc.MCResponse{Status: success, Opaque: 0}, false)
	time.Sleep(5 * time.Second)
	assert.Equal(0, len(events))
	// send 2 success packets at pos 1 - sleep 5s - [<>, <>, <>, <>, <>, <>, <>, <>]
	xmem.updateErrsForUIAlert(&mc.MCResponse{Status: success, Opaque: 1}, false)
	time.Sleep(5 * time.Second)
	assert.Equal(0, len(events))
	// send 3 failure1 packets at pos 0 - sleep 5 seconds - [fail1, <>, <>, <>, <>, <>, <>, <>]
	xmem.updateErrsForUIAlert(&mc.MCResponse{Status: fail1, Opaque: 0}, true)
	xmem.updateErrsForUIAlert(&mc.MCResponse{Status: fail1, Opaque: 0}, true)
	xmem.updateErrsForUIAlert(&mc.MCResponse{Status: fail1, Opaque: 0}, true)
	time.Sleep(5 * time.Second)
	assert.Equal(1, len(events))
	assert.True(eventExists([]string{fmt.Sprintf("%v=%v", responseStr, fail1)}))
	// send 2 failure 2 packets at pos 1 - sleep 5 seconds - [fail1, fail2, <>, <>, <>, <>, <>, <>]
	xmem.updateErrsForUIAlert(&mc.MCResponse{Status: fail2, Opaque: 1}, true)
	xmem.updateErrsForUIAlert(&mc.MCResponse{Status: fail2, Opaque: 1}, true)
	time.Sleep(5 * time.Second)
	assert.Equal(1, len(events))
	assert.True(eventExists([]string{fmt.Sprintf("%v=%v", responseStr, fail1), fmt.Sprintf("%v=%v", responseStr, fail2)}))
	// send 2 failure 3 packets at pos 1 and failure 1 packets at pos 2 - sleep 5 seconds - [fail1, fail3, fail1, <>, <>, <>, <>, <>]
	xmem.updateErrsForUIAlert(&mc.MCResponse{Status: fail3, Opaque: 1}, true)
	xmem.updateErrsForUIAlert(&mc.MCResponse{Status: fail3, Opaque: 1}, true)
	xmem.updateErrsForUIAlert(&mc.MCResponse{Status: fail1, Opaque: 2}, true)
	time.Sleep(5 * time.Second)
	assert.Equal(1, len(events))
	assert.True(eventExists([]string{fmt.Sprintf("%v=%v", responseStr, fail1), fmt.Sprintf("%v=%v", responseStr, fail3)}))
	// sleep 5 seconds - do nothing - same state
	time.Sleep(5 * time.Second)
	assert.Equal(1, len(events))
	assert.True(eventExists([]string{fmt.Sprintf("%v=%v", responseStr, fail1), fmt.Sprintf("%v=%v", responseStr, fail3)}))
	// sleep 5 seconds - send 1 success packet at pos 0 - [<>, fail3, fail1, <>, <>, <>, <>, <>]
	xmem.updateErrsForUIAlert(&mc.MCResponse{Status: success, Opaque: 0}, false)
	time.Sleep(5 * time.Second)
	assert.Equal(1, len(events))
	assert.True(eventExists([]string{fmt.Sprintf("%v=%v", responseStr, fail1), fmt.Sprintf("%v=%v", responseStr, fail3)}))
	// sleep 5 seconds - do nothing - same state
	time.Sleep(5 * time.Second)
	assert.Equal(1, len(events))
	assert.True(eventExists([]string{fmt.Sprintf("%v=%v", responseStr, fail1), fmt.Sprintf("%v=%v", responseStr, fail3)}))
	// send fail 4 packet at pos 3 and fail 1 packet at pos 4-7 - sleep 5 seconds - [<>, fail3, fail1, fail4, fail1, fail1, fail1, fail1]
	xmem.updateErrsForUIAlert(&mc.MCResponse{Status: fail4, Opaque: 3}, true)
	xmem.updateErrsForUIAlert(&mc.MCResponse{Status: fail1, Opaque: 4}, true)
	xmem.updateErrsForUIAlert(&mc.MCResponse{Status: fail1, Opaque: 5}, true)
	xmem.updateErrsForUIAlert(&mc.MCResponse{Status: fail1, Opaque: 6}, true)
	xmem.updateErrsForUIAlert(&mc.MCResponse{Status: fail1, Opaque: 7}, true)
	time.Sleep(5 * time.Second)
	assert.Equal(1, len(events))
	assert.True(eventExists([]string{fmt.Sprintf("%v=%v", responseStr, fail1), fmt.Sprintf("%v=%v", responseStr, fail4), fmt.Sprintf("%v=%v", responseStr, fail3)}))
	// send success at pos 5 and 6 - sleep 5s - [<>, fail3, fail1, fail4, fail1, <>, <>, fail1]
	xmem.updateErrsForUIAlert(&mc.MCResponse{Status: success, Opaque: 5}, false)
	xmem.updateErrsForUIAlert(&mc.MCResponse{Status: success, Opaque: 6}, false)
	time.Sleep(5 * time.Second)
	assert.Equal(1, len(events))
	assert.True(eventExists([]string{fmt.Sprintf("%v=%v", responseStr, fail1), fmt.Sprintf("%v=%v", responseStr, fail3), fmt.Sprintf("%v=%v", responseStr, fail3)}))
	// send success in all - sleep 5 seconds - [<>, <>, <>, <>, <>, <>, <>, <>]
	xmem.updateErrsForUIAlert(&mc.MCResponse{Status: success, Opaque: 1}, false)
	xmem.updateErrsForUIAlert(&mc.MCResponse{Status: success, Opaque: 2}, false)
	xmem.updateErrsForUIAlert(&mc.MCResponse{Status: success, Opaque: 3}, false)
	xmem.updateErrsForUIAlert(&mc.MCResponse{Status: success, Opaque: 4}, false)
	xmem.updateErrsForUIAlert(&mc.MCResponse{Status: success, Opaque: 7}, false)
	time.Sleep(5 * time.Second)
	assert.Equal(0, len(events))
	// sleep 5 seconds - do nothing - same state
	time.Sleep(5 * time.Second)
	assert.Equal(0, len(events))
	// send success in pos 10 - sleep 5 seconds - [<>, <>, <>, <>, <>, <>, <>, <>]
	xmem.updateErrsForUIAlert(&mc.MCResponse{Status: success, Opaque: 10}, false)
	time.Sleep(5 * time.Second)
	assert.Equal(0, len(events))
}
