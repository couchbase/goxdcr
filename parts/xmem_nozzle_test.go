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
	"bytes"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v9"
	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	mcMock "github.com/couchbase/gomemcached/client/mocks"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/base/generator"
	"github.com/couchbase/goxdcr/common/mocks"
	"github.com/couchbase/goxdcr/crMeta"
	"github.com/couchbase/goxdcr/hlv"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	serviceDefMocks "github.com/couchbase/goxdcr/service_def/mocks"
	utilsReal "github.com/couchbase/goxdcr/utils"
	utilsMock "github.com/couchbase/goxdcr/utils/mocks"
	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const targetClusterName = "C2"
const sourceClusterName = "C1"
const xmemBucket = "B2"
const tgtKVPort = "12002"
const srcKVPort = "12000"
const sourcePort = "9000"
const targetPort = "9001"
const username = "Administrator"
const password = "wewewe"
const poolsDefaultPath = "pools/default"
const curl = "curl"
const POST = "POST"
const GET = "GET"

var combinedCreds = fmt.Sprintf("%v:%v", username, password)
var printCmd bool = false

var commonAgentWAROptions gocbcore.WaitUntilReadyOptions = gocbcore.WaitUntilReadyOptions{
	DesiredState: gocbcore.ClusterStateOnline,
	ServiceTypes: []gocbcore.ServiceType{gocbcore.MemdService},
}

var kvStringSrc = fmt.Sprintf("%s:%s", "127.0.0.1", srcKVPort)
var kvStringTgt = fmt.Sprintf("%s:%s", "127.0.0.1", tgtKVPort)
var connString = fmt.Sprintf("%s:%s", "127.0.0.1", targetPort)
var clusterReady = true // Assume we have live cluster initially
var clusterChecked = false

var events []string
var eventID int64
var err error

func setupBoilerPlateXmem(bname string, crMode base.ConflictResolutionMode, optional ...int) (*utilsMock.UtilsIface, map[string]interface{}, *XmemNozzle, *Router, *serviceDefMocks.BandwidthThrottlerSvc, *serviceDefMocks.RemoteClusterSvc, *serviceDefMocks.CollectionsManifestSvc, *mocks.PipelineEventsProducer) {

	utilitiesMock := &utilsMock.UtilsIface{}
	utilitiesMock.On("NewDataPool").Return(base.NewFakeDataPool())
	var vbList []uint16
	for i := 0; i < 1024; i++ {
		vbList = append(vbList, uint16(i))
	}

	bandwidthThrottler := &serviceDefMocks.BandwidthThrottlerSvc{}
	remoteClusterSvc := &serviceDefMocks.RemoteClusterSvc{}

	// local cluster run has KV port starting at 12000
	xmemNozzle := NewXmemNozzle("testId", remoteClusterSvc, "", "", "testTopic", "testConnPoolNamePrefix", 5, kvStringTgt, "B1", bname, "temporaryBucketUuid", "Administrator", "wewewe", crMode, log.DefaultLoggerContext, utilitiesMock, vbList, nil, "")

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

	router, _ := NewRouter("testId", spec, nil, nil, crMode, log.DefaultLoggerContext, utilitiesMock, nil, false, base.FilterExpDelNone, colManifestSvc, nil, nil, metadata.UnitTestGetCollectionsCapability(), nil, nil, nil)

	producer := &mocks.PipelineEventsProducer{}

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

func targetXmemIsUpAndCorrectSetupExists(bname string) bool {
	if clusterChecked {
		return clusterReady
	}
	clusterChecked = true
	_, err := net.Listen("tcp4", fmt.Sprintf(":"+targetPort))
	if err == nil {
		clusterReady = false
		return false
	}
	cluster, err := gocb.Connect(targetConnStr, gocb.ClusterOptions{Authenticator: gocb.PasswordAuthenticator{
		Username: username,
		Password: password,
	}})
	if err != nil {
		clusterReady = false
		return false
	}
	mgr := cluster.Buckets()
	_, err = mgr.GetBucket(xmemBucket, nil)
	if err != nil {
		clusterReady = false
		return false
	}
	clusterReady = true
	return true
}

func setupMocksCommon(utils *utilsMock.UtilsIface) {
	utils.On("ValidateSettings", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	memcachedMock := &mcMock.ClientIface{}
	memcachedMock.On("Closed").Return(true)
	memcachedMock.On("EnableDataPool", mock.Anything, mock.Anything).Return(nil)

	emptyMap := make(map[string]interface{})
	memcachedMock.On("GetErrorMap", mock.Anything).Return(emptyMap, nil)

	utils.On("ExponentialBackoffExecutorWithFinishSignal", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(memcachedMock, nil)
}

func setupMocksCompressNeg(utils *utilsMock.UtilsIface) {
	setupMocksCommon(utils)

	var noCompressFeature utilsReal.HELOFeatures
	noCompressFeature.Xattribute = true
	noCompressFeature.CompressionType = base.CompressionTypeNone
	utils.On("SendHELOWithFeatures", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(noCompressFeature, nil)
}

func setupMocksXmem(xmem *XmemNozzle, utils *utilsMock.UtilsIface, bandwidthThrottler *serviceDefMocks.BandwidthThrottlerSvc, remoteClusterSvc *serviceDefMocks.RemoteClusterSvc, collectionsManifestSvc *serviceDefMocks.CollectionsManifestSvc, evtProducer *mocks.PipelineEventsProducer) {
	setupMocksCommon(utils)

	var allFeatures utilsReal.HELOFeatures
	allFeatures.Xattribute = true
	allFeatures.CompressionType = base.CompressionTypeSnappy
	allFeatures.Collections = true
	utils.On("SendHELOWithFeatures", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(allFeatures, nil)

	funcThatReturnsNumberOfBytes := func(numberOfBytes, minNumberOfBytes, numberOfBytesOfFirstItem int64) int64 { return numberOfBytes }
	bandwidthThrottler.On("Throttle", mock.AnythingOfType("int64"), mock.AnythingOfType("int64"), mock.AnythingOfType("int64")).Return(funcThatReturnsNumberOfBytes, funcThatReturnsNumberOfBytes)

	xmem.SetBandwidthThrottler(bandwidthThrottler)

	xmem.eventsProducer = evtProducer
	xmem.sourceActorId = hlv.DocumentSourceId("SourceCluster")
	xmem.targetActorId = hlv.DocumentSourceId("TargetCluster")

	setupMocksRC(remoteClusterSvc)
}

func setupMocksRC(remoteClusterSvc *serviceDefMocks.RemoteClusterSvc) {
	remoteClusterRef, err := metadata.NewRemoteClusterReference("tempUUID", targetClusterName, "127.0.0.1:9001", username, password, "", /*hostnameMode*/
		false /*demandEncryption*/, "", nil, nil, nil, nil)
	if err != nil {
		fmt.Printf("Error creating RCR: %v\n", err)
	}

	colCap := metadata.UnitTestGetCollectionsCapability()
	remoteClusterSvc.On("RemoteClusterByUuid", mock.Anything, mock.Anything).Return(remoteClusterRef, nil)
	remoteClusterSvc.On("GetCapability", mock.Anything).Return(colCap, nil)
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
	utils, settings, xmem, _, throttler, remoteClusterSvc, colManSvc, eventProducer := setupBoilerPlateXmem(xmemBucket, base.CRMode_RevId)
	setupMocksXmem(xmem, utils, throttler, remoteClusterSvc, colManSvc, eventProducer)
	xmem.dataPool = &base.FakeDataPool{}
	assert.Nil(xmem.initialize(settings))
	fmt.Println("============== Test case end: TestPositiveXmemNozzle =================")
}

func TestNegNoCompressionXmemNozzle(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestNegNoCompressionXmemNozzle =================")
	utils, settings, xmem, _, _, rc, _, _ := setupBoilerPlateXmem(xmemBucket, base.CRMode_RevId)
	setupMocksCompressNeg(utils)
	setupMocksRC(rc)

	xmem.dataPool = &base.FakeDataPool{}
	assert.Equal(base.ErrorCompressionNotSupported, xmem.initialize(settings))
	fmt.Println("============== Test case start: TestNegNoCompressionXmemNozzle =================")
}

func TestPosNoCompressionXmemNozzle(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestNegNoCompressionXmemNozzle =================")
	utils, settings, xmem, _, _, rc, _, _ := setupBoilerPlateXmem(xmemBucket, base.CRMode_RevId)
	settings[SETTING_COMPRESSION_TYPE] = (base.CompressionType)(base.CompressionTypeForceUncompress)
	settings[ForceCollectionDisableKey] = true
	setupMocksCompressNeg(utils)
	setupMocksRC(rc)

	xmem.dataPool = &base.FakeDataPool{}
	assert.Equal(nil, xmem.initialize(settings))
	fmt.Println("============== Test case start: TestNegNoCompressionXmemNozzle =================")
}

// AUTO is no longer a supported value. XDCR Factory should have passed in a non-auto
func TestPositiveXmemNozzleAuto(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPositiveXmemNozzleAuto =================")
	utils, settings, xmem, _, throttler, remoteClusterSvc, colManSvc, evtProducer := setupBoilerPlateXmem(xmemBucket, base.CRMode_RevId)
	settings[SETTING_COMPRESSION_TYPE] = (base.CompressionType)(base.CompressionTypeAuto)
	setupMocksXmem(xmem, utils, throttler, remoteClusterSvc, colManSvc, evtProducer)

	xmem.dataPool = &base.FakeDataPool{}
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
	xmemSendPackets(t, []string{uprNotCompressFile}, xmemBucket)

}

func xmemSendPackets(t *testing.T, uprfiles []string, bname string) {
	if !targetXmemIsUpAndCorrectSetupExists(bname) {
		fmt.Println("Skipping since live cluster_run setup has not been detected")
		return
	}

	assert := assert.New(t)

	utilsNotUsed, settings, xmem, router, throttler, remoteClusterSvc, colManSvc, eventProducer := setupBoilerPlateXmem(bname, base.CRMode_RevId)
	realUtils := utilsReal.NewUtilities()
	xmem.utils = realUtils

	setupMocksXmem(xmem, utilsNotUsed, throttler, remoteClusterSvc, colManSvc, eventProducer)

	startTargetXmem(xmem, settings, bname, assert)

	// Send the events
	var events []*mcc.UprEvent
	for _, uprfile := range uprfiles {
		event, err := RetrieveUprFile(uprfile)
		assert.Nil(err)
		events = append(events, event)

		wrappedEvent := &base.WrappedUprEvent{UprEvent: event}
		wrappedMCRequest, err := router.ComposeMCRequest(wrappedEvent)
		assert.Nil(err)
		assert.NotNil(wrappedMCRequest)
		xmem.Receive(wrappedMCRequest)
	}

	// retrieve the doc to check
	cluster, err := gocb.Connect(targetConnStr, gocb.ClusterOptions{Authenticator: gocb.PasswordAuthenticator{
		Username: username,
		Password: password,
	}})
	assert.Nil(err)
	defer cluster.Close(nil)
	bucket := cluster.Bucket(bname)

	for _, event := range events {
		fmt.Printf("Getting document %s\n", event.Key)
		_, err := bucket.DefaultCollection().Get(string(event.Key), nil)
		assert.Nil(err)
	}
}

func getBucketUuid(conStr, bucketName string, assert *assert.Assertions) string {
	realUtils := utilsReal.NewUtilities()
	bucketInfo, err := realUtils.GetBucketInfo(conStr, bucketName, username, password, base.HttpAuthMechPlain, nil, false, nil, nil, nil)
	assert.Nil(err)
	uuid, ok := bucketInfo["uuid"].(string)
	assert.True(ok)
	return uuid
}

func startTargetXmem(xmem *XmemNozzle, settings map[string]interface{}, bname string, assert *assert.Assertions) {
	// Need to find the actual running targetBucketUUID
	xmem.targetBucketUuid = getBucketUuid(connString, bname, assert)
	settings[SETTING_COMPRESSION_TYPE] = (base.CompressionType)(base.CompressionTypeSnappy)
	settings[ForceCollectionDisableKey] = true
	err := xmem.Start(settings)
	assert.Nil(err)
}

type User struct {
	Id        string   `json:"uid"`
	Email     string   `json:"email"`
	Interests []string `json:"interests"`
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

var totalSamples = 100
var docBodySizeMaxBytes = 500

func generateTestDataForSnappyDecode(numDocs int) []*base.WrappedMCRequest {
	dataSet, _, err := generator.GenerateRandomData(10)
	if err != nil {
		panic(err)
	}
	totalRows := len(dataSet)
	var generatedWrappedMCR []*base.WrappedMCRequest

	testKeyStr := "testDoc_"
	for i := 0; i < numDocs; i++ {
		key := fmt.Sprintf("%v%v", testKeyStr, i)
		newReq := &mc.MCRequest{
			Opcode:        0,
			Cas:           rand.Uint64(),
			Opaque:        rand.Uint32(),
			VBucket:       uint16(rand.Uint32() % 1024),
			Extras:        nil,
			Key:           []byte(key),
			Body:          snappy.Encode(nil, dataSet[i%totalRows]),
			ExtMeta:       nil,
			DataType:      base.SnappyDataType & base.JSONDataType,
			Keylen:        len(key),
			CollId:        [5]byte{},
			CollIdLen:     0,
			Username:      [128]byte{},
			UserLen:       0,
			FramingExtras: nil,
			FramingElen:   0,
		}
		newWrappedMCR := &base.WrappedMCRequest{
			Seqno:                      0,
			Req:                        newReq,
			Start_time:                 time.Time{},
			UniqueKey:                  string(newReq.Key),
			SrcColNamespace:            nil,
			SrcColNamespaceMtx:         sync.RWMutex{},
			ColInfo:                    nil,
			ColInfoMtx:                 sync.RWMutex{},
			SlicesToBeReleasedByXmem:   nil,
			SlicesToBeReleasedByRouter: nil,
			SlicesToBeReleasedMtx:      sync.Mutex{},
			SiblingReqs:                nil,
			SiblingReqsMtx:             sync.RWMutex{},
			RetryCRCount:               0,
		}
		generatedWrappedMCR = append(generatedWrappedMCR, newWrappedMCR)
	}
	return generatedWrappedMCR
}

// To run benchmark test between snappy.Decode and non-snappy.Decode, we should really use the same data set
var generateSharedSampleOnce sync.Once
var generatedDataSet []*base.WrappedMCRequest

func generateDataOnce() []*base.WrappedMCRequest {
	generateSharedSampleOnce.Do(func() {
		generatedDataSet = generateTestDataForSnappyDecode(totalSamples)
	})
	return generatedDataSet
}

// Run the following two benchmarks in sequence via a single command to ensure valid test data generation and
// performance comparison like so:
// $ go test -run=BenchmarkSnappyDecodeLenImpact -bench=.
func BenchmarkSnappyDecodeLenImpact(b *testing.B) {
	dataSet := generateDataOnce()

	generateDataSetAdditional := func(idx int) {
		dataIdx := idx % len(dataSet)
		wrappedReq := dataSet[dataIdx]
		req := wrappedReq.Req
		additionalInfo := DataSentEventAdditional{Seqno: dataSet[dataIdx].Seqno,
			IsOptRepd:           false,
			Opcode:              req.Opcode,
			VBucket:             req.VBucket,
			Req_size:            req.Size(),
			UncompressedReqSize: req.Size() - wrappedReq.GetBodySize() + wrappedReq.GetUncompressedBodySize(),
		}
		// Satisfy golang
		_ = additionalInfo
	}

	// Do the loop where golang benchmark tester will run b.N times
	for n := 0; n < b.N; n++ {
		for i := 0; i < totalSamples; i++ {
			generateDataSetAdditional(i)
		}
	}
}

// Original, no snappy decode
func BenchmarkSnappyDecodeLenImpactOriginal(b *testing.B) {
	dataSet := generateDataOnce()

	generateDataSetAdditional := func(idx int) {
		dataIdx := idx % len(dataSet)
		wrappedReq := dataSet[dataIdx]
		req := wrappedReq.Req
		additionalInfo := DataSentEventAdditional{Seqno: dataSet[dataIdx].Seqno,
			IsOptRepd: false,
			Opcode:    req.Opcode,
			VBucket:   req.VBucket,
			Req_size:  req.Size(),
			// For this test, do not run Uncompressed calculation
			// UncompressedReqSize: req.Size() - wrappedReq.GetBodySize() + wrappedReq.GetUncompressedBodySize(),
		}
		// Satisfy golang
		_ = additionalInfo
	}

	// Do the loop where golang benchmark tester will run b.N times
	for n := 0; n < b.N; n++ {
		for i := 0; i < totalSamples; i++ {
			generateDataSetAdditional(i)
		}
	}
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

	utils, settings, xmem, _, throttler, remoteClusterSvc, colManSvc, eventProducer := setupBoilerPlateXmem(xmemBucket, base.CRMode_RevId, 3)
	setupMocksXmem(xmem, utils, throttler, remoteClusterSvc, colManSvc, eventProducer)

	xmem.dataPool = &base.FakeDataPool{}
	assert.Nil(xmem.initialize(settings))
	assert.Nil(xmem.Start(settings))

	success := mc.SUCCESS
	fail1 := mc.EINVAL
	fail2 := mc.DURABILITY_IMPOSSIBLE
	fail3 := mc.DURABILITY_INVALID_LEVEL
	fail4 := mc.NOT_STORED
	// send 2 success packets at pos 0 - sleep 5s - [<>, <>, <>, <>, <>, <>, <>, <>]
	xmem.markNonTempErrorResponse(&mc.MCResponse{Status: success, Opaque: 0}, false)
	time.Sleep(5 * time.Second)
	assert.Equal(0, len(events))
	// send 2 success packets at pos 1 - sleep 5s - [<>, <>, <>, <>, <>, <>, <>, <>]
	xmem.markNonTempErrorResponse(&mc.MCResponse{Status: success, Opaque: 1}, false)
	time.Sleep(5 * time.Second)
	assert.Equal(0, len(events))
	// send 3 failure1 packets at pos 0 - sleep 5 seconds - [fail1, <>, <>, <>, <>, <>, <>, <>]
	xmem.markNonTempErrorResponse(&mc.MCResponse{Status: fail1, Opaque: 0}, true)
	xmem.markNonTempErrorResponse(&mc.MCResponse{Status: fail1, Opaque: 0}, true)
	xmem.markNonTempErrorResponse(&mc.MCResponse{Status: fail1, Opaque: 0}, true)
	time.Sleep(5 * time.Second)
	assert.Equal(1, len(events))
	assert.True(eventExists([]string{xmem.PrintResponseStatusError(fail1)}))
	// send 2 failure 2 packets at pos 1 - sleep 5 seconds - [fail1, fail2, <>, <>, <>, <>, <>, <>]
	xmem.markNonTempErrorResponse(&mc.MCResponse{Status: fail2, Opaque: 1}, true)
	xmem.markNonTempErrorResponse(&mc.MCResponse{Status: fail2, Opaque: 1}, true)
	time.Sleep(5 * time.Second)
	assert.Equal(1, len(events))
	assert.True(eventExists([]string{xmem.PrintResponseStatusError(fail1), xmem.PrintResponseStatusError(fail2)}))
	// send 2 failure 3 packets at pos 1 and failure 1 packets at pos 2 - sleep 5 seconds - [fail1, fail3, fail1, <>, <>, <>, <>, <>]
	xmem.markNonTempErrorResponse(&mc.MCResponse{Status: fail3, Opaque: 1}, true)
	xmem.markNonTempErrorResponse(&mc.MCResponse{Status: fail3, Opaque: 1}, true)
	xmem.markNonTempErrorResponse(&mc.MCResponse{Status: fail1, Opaque: 2}, true)
	time.Sleep(5 * time.Second)
	assert.Equal(1, len(events))
	assert.True(eventExists([]string{xmem.PrintResponseStatusError(fail1), xmem.PrintResponseStatusError(fail3)}))
	// sleep 5 seconds - do nothing - same state
	time.Sleep(5 * time.Second)
	assert.Equal(1, len(events))
	assert.True(eventExists([]string{xmem.PrintResponseStatusError(fail1), xmem.PrintResponseStatusError(fail3)}))
	// sleep 5 seconds - send 1 success packet at pos 0 - [<>, fail3, fail1, <>, <>, <>, <>, <>]
	xmem.markNonTempErrorResponse(&mc.MCResponse{Status: success, Opaque: 0}, false)
	time.Sleep(5 * time.Second)
	assert.Equal(1, len(events))
	assert.True(eventExists([]string{xmem.PrintResponseStatusError(fail1), xmem.PrintResponseStatusError(fail3)}))
	// sleep 5 seconds - do nothing - same state
	time.Sleep(5 * time.Second)
	assert.Equal(1, len(events))
	assert.True(eventExists([]string{xmem.PrintResponseStatusError(fail1), xmem.PrintResponseStatusError(fail3)}))
	// send fail 4 packet at pos 3 and fail 1 packet at pos 4-7 - sleep 5 seconds - [<>, fail3, fail1, fail4, fail1, fail1, fail1, fail1]
	xmem.markNonTempErrorResponse(&mc.MCResponse{Status: fail4, Opaque: 3}, true)
	xmem.markNonTempErrorResponse(&mc.MCResponse{Status: fail1, Opaque: 4}, true)
	xmem.markNonTempErrorResponse(&mc.MCResponse{Status: fail1, Opaque: 5}, true)
	xmem.markNonTempErrorResponse(&mc.MCResponse{Status: fail1, Opaque: 6}, true)
	xmem.markNonTempErrorResponse(&mc.MCResponse{Status: fail1, Opaque: 7}, true)
	time.Sleep(5 * time.Second)
	assert.Equal(1, len(events))
	assert.True(eventExists([]string{xmem.PrintResponseStatusError(fail1), xmem.PrintResponseStatusError(fail4), xmem.PrintResponseStatusError(fail3)}))
	// send success at pos 5 and 6 - sleep 5s - [<>, fail3, fail1, fail4, fail1, <>, <>, fail1]
	xmem.markNonTempErrorResponse(&mc.MCResponse{Status: success, Opaque: 5}, false)
	xmem.markNonTempErrorResponse(&mc.MCResponse{Status: success, Opaque: 6}, false)
	time.Sleep(5 * time.Second)
	assert.Equal(1, len(events))
	assert.True(eventExists([]string{xmem.PrintResponseStatusError(fail1), xmem.PrintResponseStatusError(fail3), xmem.PrintResponseStatusError(fail3)}))
	// send success in all - sleep 5 seconds - [<>, <>, <>, <>, <>, <>, <>, <>]
	xmem.markNonTempErrorResponse(&mc.MCResponse{Status: success, Opaque: 1}, false)
	xmem.markNonTempErrorResponse(&mc.MCResponse{Status: success, Opaque: 2}, false)
	xmem.markNonTempErrorResponse(&mc.MCResponse{Status: success, Opaque: 3}, false)
	xmem.markNonTempErrorResponse(&mc.MCResponse{Status: success, Opaque: 4}, false)
	xmem.markNonTempErrorResponse(&mc.MCResponse{Status: success, Opaque: 7}, false)
	time.Sleep(5 * time.Second)
	assert.Equal(0, len(events))
	// sleep 5 seconds - do nothing - same state
	time.Sleep(5 * time.Second)
	assert.Equal(0, len(events))
	// send success in pos 10 - sleep 5 seconds - [<>, <>, <>, <>, <>, <>, <>, <>]
	xmem.markNonTempErrorResponse(&mc.MCResponse{Status: success, Opaque: 10}, false)
	time.Sleep(5 * time.Second)
	assert.Equal(0, len(events))
}

func GetAndFlushBucket(connStr, bucketName string) (cluster *gocb.Cluster, bucket *gocb.Bucket, err error) {
	cluster, err = gocb.Connect(connStr, gocb.ClusterOptions{Authenticator: gocb.PasswordAuthenticator{
		Username: username,
		Password: password,
	}})
	if err != nil {
		return
	}
	err = cluster.WaitUntilReady(15*time.Second, nil)
	if err != nil {
		return
	}
	mgr := cluster.Buckets()
	err = mgr.FlushBucket(bucketName, nil)
	bucket = cluster.Bucket(bucketName)
	return
}

func TestMobilePreserveSync(t *testing.T) {
	fmt.Println("============== Test case start: TestMobilePreserveSync =================")
	defer fmt.Println("============== Test case end: TestMobilePreserveSync =================")
	if !targetXmemIsUpAndCorrectSetupExists(xmemBucket) {
		fmt.Println("Skipping since live cluster_run setup has not been detected")
		return
	}
	// This is a revId bucket while other mobile tests use LWW bucket.
	bucketName := "B0"
	cluster, bucket, err := GetAndFlushBucket(targetConnStr, bucketName)
	if err != nil {
		fmt.Printf("Skipping since bucket %v is not ready. err=%v.\n", bucketName, err)
		return
	}
	defer cluster.Close(nil)

	assert := assert.New(t)

	// Set up target document first
	targetDoc := "testdata/uprEventSyncTestDoc2Target.json"
	xmemSendPackets(t, []string{targetDoc}, bucketName)

	// Now set up Xmem for testing
	utilsNotUsed, settings, xmem, router, throttler, remoteClusterSvc, colManSvc, eventProducer := setupBoilerPlateXmem(bucketName, base.CRMode_RevId)
	realUtils := utilsReal.NewUtilities()
	xmem.utils = realUtils

	settings[base.EnableCrossClusterVersioningKey] = true
	settings[MOBILE_COMPATBILE] = base.MobileCompatibilityActive
	settings[base.VersionPruningWindowHrsKey] = 720
	router.SetMobileCompatibility(base.MobileCompatibilityActive)

	setupMocksXmem(xmem, utilsNotUsed, throttler, remoteClusterSvc, colManSvc, eventProducer)

	xmem.sourceBucketUuid = "12345678901234567890123456789023"
	startTargetXmem(xmem, settings, bucketName, assert)

	// Test 1. syncTestDoc1 has sync. Target does not have the document. _sync should be skipped
	uprfile := "testdata/uprEventSyncTestDoc1WithSync.json"
	key := "syncTestDoc1"
	event, err := RetrieveUprFile(uprfile)
	assert.Nil(err)
	wrappedEvent := &base.WrappedUprEvent{UprEvent: event}
	wrappedMCRequest, err := router.ComposeMCRequest(wrappedEvent)
	assert.Nil(err)
	assert.NotNil(wrappedMCRequest)
	xmem.Receive(wrappedMCRequest)
	if err = checkTarget(bucket, key, "email", []byte("\"kingarthur@couchbase.com\""), false); err != nil {
		assert.FailNow(err.Error())
	}
	if err = checkTarget(bucket, key, "_sync", nil, true); err != nil {
		assert.FailNow(err.Error())
	}

	// Test 2. send updated syncTestDoc1 with _sync. It should be skipped again
	uprfile = "testdata/uprEventSyncTestDoc1WithSyncUpdated.json"
	event, err = RetrieveUprFile(uprfile)
	assert.Nil(err)
	wrappedEvent = &base.WrappedUprEvent{UprEvent: event}
	wrappedMCRequest, err = router.ComposeMCRequest(wrappedEvent)
	assert.Nil(err)
	assert.NotNil(wrappedMCRequest)
	xmem.Receive(wrappedMCRequest)
	if err = checkTarget(bucket, key, "email", []byte("\"kingarthur@updated.couchbase.com\""), false); err != nil {
		assert.FailNow(err.Error())
	}
	if err = checkTarget(bucket, key, "_sync", nil, true); err != nil {
		assert.FailNow(err.Error())
	}

	// Test 3. Send syncTestDoc2. It should get source document body but keep the target _sync value
	uprfile = "testdata/uprEventSyncTestDoc2Source.json"
	key = "syncTestDoc2"
	event, err = RetrieveUprFile(uprfile)
	assert.Nil(err)
	wrappedEvent = &base.WrappedUprEvent{UprEvent: event}
	wrappedMCRequest, err = router.ComposeMCRequest(wrappedEvent)
	assert.Nil(err)
	assert.NotNil(wrappedMCRequest)
	xmem.Receive(wrappedMCRequest)

	if err = checkTarget(bucket, key, "email", []byte("\"kingarthur@source.couchbase.com\""), false); err != nil {
		assert.FailNow(err.Error())
	}
	if err = checkTarget(bucket, key, "_sync", []byte("\"mobile sync XATTR target\""), true); err != nil {
		assert.FailNow(err.Error())
	}
}

func checkTarget(bucket *gocb.Bucket, key, path string, expectedValue []byte, isXattr bool) error {
	var value []byte
	for i := 0; i < 10; i++ {
		res, err := bucket.DefaultCollection().LookupIn(key,
			[]gocb.LookupInSpec{gocb.GetSpec(path, &gocb.GetSpecOptions{IsXattr: isXattr})}, nil)
		if err == nil {
			res.ContentAt(0, &value)
			if bytes.Equal(value, expectedValue) {
				return nil
			}
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("value %q is not expected %q", value, expectedValue)
}

// This test was useful in development but is disabled. TestMobilePreserveSync is used instead
func DISABLE_TestMobilePreserveSyncLiveRep(t *testing.T) {
	fmt.Println("============== Test case start: TestMobilePreserveSyncLiveRep =================")
	defer fmt.Println("============== Test case end: TestMobilePreserveSyncLiveRep =================")
	if !targetXmemIsUpAndCorrectSetupExists(xmemBucket) {
		fmt.Println("Skipping since live cluster_run setup has not been detected")
		return
	}
	mobilePreserveSyncLiveRep(t, "syncTestLWW", "lww")
	mobilePreserveSyncLiveRep(t, "syncTestRevId", "seqno")
	mobilePreserveSyncLiveRep(t, "syncTestCustom", "custom")
}
func mobilePreserveSyncLiveRep(t *testing.T, bucketName string, crType gocb.ConflictResolutionType) {
	fmt.Printf("running with CR type %s \n", crType)
	assert := assert.New(t)
	srcCluster, sourceBucket, err := createBucket(sourceConnStr, bucketName, crType)
	if err != nil {
		fmt.Printf("TestMobilePreserveSyncLiveRep skipped because source cluster is not ready. Error: %v\n", err)
		return
	}
	defer srcCluster.Close(nil)
	trgCluster, targetBucket, err := createBucket(targetConnStr, bucketName, crType)
	if err != nil {
		fmt.Printf("TestMobilePreserveSyncLiveRep skipped because target cluster is not ready. Error: %v\n", err)
		return
	}
	defer trgCluster.Close(nil)
	assert.NotNil(sourceBucket)
	assert.NotNil(targetBucket)
	createReplication(t, bucketName, base.DefaultMergeFunc, base.JSFunctionTimeoutDefault, true, map[string]string{"mobile": "active"})
	createReplication(t, bucketName, base.DefaultMergeFunc, base.JSFunctionTimeoutDefault, false, map[string]string{"mobile": "active"}) // reverse direction to test pruning
	expire := 1 * time.Hour

	// Test 1. syncTestDoc1 has sync. Target does not have the document. _sync should be skipped
	key := "Doc1" + time.Now().Format(time.RFC3339)
	upsOut, err := sourceBucket.DefaultCollection().Upsert(key,
		User{Id: "kingarthur",
			Email:     "kingarthur@couchbase.com",
			Interests: []string{"Holy Grail", "African Swallows"}}, &gocb.UpsertOptions{Expiry: expire})
	if err != nil {
		assert.FailNow("Upsert failed with errror %v", err)
	}
	err = waitForReplication(key, upsOut.Cas(), targetBucket)
	assert.Nil(err)
	mutOut, err := sourceBucket.DefaultCollection().MutateIn(key,
		[]gocb.MutateInSpec{
			gocb.InsertSpec(base.XATTR_MOBILE, "cluster C1 value", &gocb.InsertSpecOptions{IsXattr: true, CreatePath: true})},
		&gocb.MutateInOptions{Expiry: expire})
	assert.Nil(err)
	err = waitForReplication(key, mutOut.Cas(), targetBucket)
	assert.Nil(err)
	if err = checkTarget(targetBucket, key, base.XATTR_MOBILE, nil, true); err != nil {
		assert.FailNow(err.Error())
	}

	// Test 2. Update and insert _sync at target. This value should be skipped and C1 keeps its original value
	mutOut, err = targetBucket.DefaultCollection().MutateIn(key,
		[]gocb.MutateInSpec{
			gocb.InsertSpec(base.XATTR_MOBILE, "cluster C2 value", &gocb.InsertSpecOptions{IsXattr: true, CreatePath: true})},
		&gocb.MutateInOptions{Expiry: expire})
	assert.Nil(err)
	err = waitForReplication(key, mutOut.Cas(), sourceBucket)
	assert.Nil(err)
	if err = checkTarget(sourceBucket, key, base.XATTR_MOBILE, []byte("\"cluster C1 value\""), true); err != nil {
		assert.FailNow(err.Error())
	}
}

// We want to avoid replicating import mutation by using its pre-import metadata for CR.
// This only works for LWW since pre-import revId is not saved so import mutations are still
// replicated for revId buckets
func TestMobileImportCasLWW(t *testing.T) {
	fmt.Println("============== Test case start: TestMobileImportCasLWW =================")
	defer fmt.Println("============== Test case end: TestMobileImportCasLWW =================")
	if !targetXmemIsUpAndCorrectSetupExists(xmemBucket) {
		fmt.Println("Skipping since live cluster_run setup has not been detected")
		return
	}
	assert := assert.New(t)
	// Create and flush target bucket
	bucketName := "mobileLWW"
	cluster, bucket, err := createBucket(targetConnStr, bucketName, "lww")
	if err != nil {
		fmt.Printf("TestMobileImportCasLWW skipped because bucket is cannot be created. Error: %v\n", err)
		return
	}
	if cluster, bucket, err = GetAndFlushBucket(targetConnStr, bucketName); err != nil {
		fmt.Printf("TestMobileImportCasLWW skipped because bucket cannot be flusehed. Error: %v\n", err)
		return
	}
	defer cluster.Close(nil)
	// Create Xmem for testing
	utilsNotUsed, settings, xmem, router, throttler, remoteClusterSvc, colManSvc, eventProducer := setupBoilerPlateXmem(bucketName, base.CRMode_LWW)
	realUtils := utilsReal.NewUtilities()
	xmem.utils = realUtils

	settings[base.EnableCrossClusterVersioningKey] = true
	settings[MOBILE_COMPATBILE] = base.MobileCompatibilityActive
	settings[base.VersionPruningWindowHrsKey] = 720
	router.SetMobileCompatibility(base.MobileCompatibilityActive)

	setupMocksXmem(xmem, utilsNotUsed, throttler, remoteClusterSvc, colManSvc, eventProducer)

	// This is the source bucket uuid when mutations in this test are generated. It may be used in the mutation HLV
	xmem.sourceBucketUuid = "93fcf4f0fcc94fdb3d6196235029d6bf"
	startTargetXmem(xmem, settings, bucketName, assert)

	// Test 1. Replicate an import document (Doc1) when target doesn't have the document at all. It should replicate
	uprfile := "testdata/uprEventDoc1ImportMutation1.json"
	key := "Doc1ImportTest"
	event, err := RetrieveUprFile(uprfile)
	assert.Nil(err)
	wrappedEvent := &base.WrappedUprEvent{UprEvent: event}
	wrappedMCRequest, err := router.ComposeMCRequest(wrappedEvent)
	assert.Nil(err)
	assert.NotNil(wrappedMCRequest)
	xmem.Receive(wrappedMCRequest)
	err = waitForReplication(key, 1700503142566854656, bucket)
	assert.Nil(err)

	out, err := bucket.DefaultCollection().Get(key, nil)
	assert.Nil(err)
	assert.Equal(gocb.Cas(1700503142566854656), out.Cas())
	err = checkTarget(bucket, key, base.XATTR_IMPORTCAS, []byte("\"0x0000223899669917\""), true)
	assert.Nil(err)

	// Test 2. Update the import document (Doc1). It should replicate with importCas removed
	uprfile = "testdata/uprEventDoc1UpdateAfterImport.json"
	event, err = RetrieveUprFile(uprfile)
	assert.Nil(err)
	wrappedEvent = &base.WrappedUprEvent{UprEvent: event}
	wrappedMCRequest, err = router.ComposeMCRequest(wrappedEvent)
	assert.Nil(err)
	assert.NotNil(wrappedMCRequest)
	xmem.Receive(wrappedMCRequest)
	err = waitForReplication(key, 1700503747140517888, bucket)
	assert.Nil(err)
	value, err := bucket.DefaultCollection().LookupIn(key,
		[]gocb.LookupInSpec{gocb.GetSpec(base.XATTR_IMPORTCAS, &gocb.GetSpecOptions{IsXattr: true})}, nil)
	assert.Nil(err)
	assert.False(value.Exists(0))

	// Test 3. Import the document again (Doc1). It should not replicate
	uprfile = "testdata/uprEventDoc1ImportAgainAfterUpdate.json"
	event, err = RetrieveUprFile(uprfile)
	assert.Nil(err)
	wrappedEvent = &base.WrappedUprEvent{UprEvent: event}
	wrappedMCRequest, err = router.ComposeMCRequest(wrappedEvent)
	assert.Nil(err)
	assert.NotNil(wrappedMCRequest)
	xmem.Receive(wrappedMCRequest)
	// To check it is not sent, we send another document, wait for it to be sent, and then check the doc
	secondDoc := "../utils/testInternalData/uprNotCompress.json"
	event, err = RetrieveUprFile(secondDoc)
	assert.Nil(err)
	secondKey := "TestDocKey"
	wrappedEvent = &base.WrappedUprEvent{UprEvent: event}
	wrappedMCRequest, err = router.ComposeMCRequest(wrappedEvent)
	assert.Nil(err)
	assert.NotNil(wrappedMCRequest)
	xmem.Receive(wrappedMCRequest)
	err = waitForReplication(secondKey, 1538667181248217088, bucket)
	assert.Nil(err)
	out, err = bucket.DefaultCollection().Get(key, nil)
	assert.Nil(err) // Should get a path not found error
	assert.Equal(gocb.Cas(1700503747140517888), out.Cas())
}

func TestMobileMixedMode(t *testing.T) {
	fmt.Println("============== Test case start: TestMobileMixedMode =================")
	defer fmt.Println("============== Test case end: TestMobileMixedMode =================")
	if !targetXmemIsUpAndCorrectSetupExists(xmemBucket) {
		fmt.Println("Skipping since live cluster_run setup has not been detected")
		return
	}
	assert := assert.New(t)
	bucketName := "mobileLWW"
	cluster, _, err := createBucket(targetConnStr, bucketName, "lww")
	if err != nil {
		fmt.Printf("TestMobileImportCasLWW skipped because bucket is cannot be created. Error: %v\n", err)
		return
	}
	defer cluster.Close(nil)
	// Create Xmem for testing
	utilsNotUsed, settings, xmem, router, throttler, remoteClusterSvc, colManSvc, eventProducer := setupBoilerPlateXmem(bucketName, base.CRMode_LWW)
	realUtils := utilsReal.NewUtilities()
	xmem.utils = realUtils

	settings[base.EnableCrossClusterVersioningKey] = true
	settings[base.VersionPruningWindowHrsKey] = 720
	router.SetMobileCompatibility(base.MobileCompatibilityActive)

	setupMocksXmem(xmem, utilsNotUsed, throttler, remoteClusterSvc, colManSvc, eventProducer)

	settings[MOBILE_COMPATBILE] = base.MobileCompatibilityOff
	xmem.sourceBucketUuid = "93fcf4f0fcc94fdb3d6196235029d6bf"
	startTargetXmem(xmem, settings, bucketName, assert)
	fmt.Println("=== Test mobile mixed mode with mobile off ===")
	mobileMixedModeTest(xmem, router, settings, bucketName, assert)

	fmt.Println("=== Test mobile mixed mode with mobile active ===")
	xmem.config.mobileCompatible = base.MobileCompatibilityActive
	mobileMixedModeTest(xmem, router, settings, bucketName, assert)
}

func mobileMixedModeTest(xmem *XmemNozzle, router *Router, settings map[string]interface{}, bucketName string, assert *assert.Assertions) {
	_, bucket, err := GetAndFlushBucket(targetConnStr, bucketName)
	if err != nil {
		fmt.Printf("TestMobileImportCasLWW skipped because bucket cannot be flusehed. Error: %v\n", err)
		return
	}

	uprfile := "./testdata/uprEventSyncTestDoc1WithSyncUpdated.json"
	doc1event, err := RetrieveUprFile(uprfile)
	assert.Nil(err)
	doc1MCRequest, err := router.ComposeMCRequest(&base.WrappedUprEvent{UprEvent: doc1event})
	assert.Nil(err)

	uprfile = "./testdata/uprEventSyncTestDoc2Source.json"
	doc2event, err := RetrieveUprFile(uprfile)
	assert.Nil(err)
	doc2MCRequest, err := router.ComposeMCRequest(&base.WrappedUprEvent{UprEvent: doc2event})
	assert.Nil(err)

	uprfile = "./testdata/uprEventDoc1UpdateAfterImport.json"
	updatedImportEvent, err := RetrieveUprFile(uprfile)
	assert.Nil(err)
	updatedImportMCRequest, err := router.ComposeMCRequest(&base.WrappedUprEvent{UprEvent: updatedImportEvent})
	assert.Nil(err)

	xmem.config.vbHlvMaxCas[doc1event.VBucket] = doc1event.Cas + 10                   // doc1 CAS is smaller
	xmem.config.vbHlvMaxCas[doc2event.VBucket] = doc2event.Cas - 10                   // doc2 CAS is larger
	xmem.config.vbHlvMaxCas[updatedImportEvent.VBucket] = updatedImportEvent.Cas + 10 // import CAS is smaller

	xmem.Receive(doc1MCRequest)
	xmem.Receive(doc2MCRequest)
	xmem.Receive(updatedImportMCRequest)

	err = waitForReplication(string(doc1event.Key), gocb.Cas(doc1event.Cas), bucket)
	assert.Nil(err)
	err = waitForReplication(string(doc2event.Key), gocb.Cas(doc2event.Cas), bucket)
	assert.Nil(err)
	err = waitForReplication(string(updatedImportEvent.Key), gocb.Cas(updatedImportEvent.Cas), bucket)
	assert.Nil(err)

	// Doc1 Cas is smaller than its vbHlvMaxCas. So it does not have HLV
	value, err := bucket.DefaultCollection().LookupIn(string(doc1event.Key),
		[]gocb.LookupInSpec{gocb.GetSpec(base.XATTR_HLV, &gocb.GetSpecOptions{IsXattr: true})}, nil)
	assert.Nil(err)
	assert.False(value.Exists(0))

	// Doc2 Cas is larger than its vbHlvMaxCas. So it does  have HLV
	value, err = bucket.DefaultCollection().LookupIn(string(doc2event.Key),
		[]gocb.LookupInSpec{gocb.GetSpec(base.XATTR_HLV, &gocb.GetSpecOptions{IsXattr: true})}, nil)
	assert.Nil(err)
	assert.True(value.Exists(0))

	// The import doc Cas is smaller than its vbHlvMaxCas, but it already has HLV. So the HLV gets updated
	value, err = bucket.DefaultCollection().LookupIn(string(updatedImportEvent.Key),
		[]gocb.LookupInSpec{gocb.GetSpec(base.XATTR_HLV, &gocb.GetSpecOptions{IsXattr: true})}, nil)
	assert.Nil(err)
	assert.True(value.Exists(0))
}

// This test can be enabled to generate some simulated import documents.
func Disable_TestLiveImportDoc(t *testing.T) {
	assert := assert.New(t)
	bucketName := "B1"
	cluster, err := gocb.Connect(sourceConnStr, gocb.ClusterOptions{Authenticator: gocb.PasswordAuthenticator{
		Username: username,
		Password: password,
	}})
	if err != nil {
		return
	}
	err = cluster.WaitUntilReady(15*time.Second, nil)
	if err != nil {
		return
	}
	bucket := cluster.Bucket(bucketName)
	err = bucket.WaitUntilReady(20*time.Second, &gocb.WaitUntilReadyOptions{DesiredState: gocb.ClusterStateOnline})
	if err != nil {
		fmt.Printf("TestMobileImportCasLWW skipped because bucket is cannot be created. Error: %v\n", err)
		return
	}
	defer cluster.Close(nil)

	key := "importDoc"
	upsOut, err := bucket.DefaultCollection().Upsert(key,
		User{Id: "kingarthur",
			Email:     "kingarthur@couchbase.com",
			Interests: []string{"Holy Grail", "African Swallows"}}, nil)
	if err != nil {
		assert.FailNow("Upsert failed with errror %v", err)
	}
	fmt.Printf("Upsert CAS: %v, result %v\n", uint64(upsOut.Cas()), upsOut.Result)

	// After enabling enableCrossClusterVersioning
	// curl -X POST -u Administrator:wewewe http://127.0.0.1:9000/pools/default/buckets/B1 -d enableCrossClusterVersioning=true
	// In goxdcr.log, we have:
	// 2024-01-03T09:55:01.810-08:00 INFO GOXDCR.XmemNozzle: pipelineFullTopic=44d5d82a3909c505c52133a353d98254/B1/B2, xmem_44d5d82a3909c505c52133a353d98254/B1/B2_127.0.0.1:12002_0: Using adX1DsoRpCb6kQWoZYq5ew(UUID 69d5f50eca11a426fa9105a8658ab97b) and QIpU9Op/zY2WDmGxkjwvPQ(UUID 408a54f4ea7fcd8d960e61b1923c2f3d) as source and target bucket IDs for HLV.
	// Use the source bucket Id.
	simulateImportOperation(assert, bucket, key, defaultScopeCol, defaultScopeCol, "adX1DsoRpCb6kQWoZYq5ew", uint64(upsOut.Cas()))
}

type mockConn struct {
}

func (c mockConn) Read(b []byte) (n int, err error) {
	return
}

func (c mockConn) Write(b []byte) (n int, err error) {
	return
}

func (c mockConn) Close() error {
	return nil
}

func (c mockConn) LocalAddr() net.Addr {
	return nil
}

func (c mockConn) RemoteAddr() net.Addr {
	return nil
}

func (c mockConn) SetDeadline(t time.Time) error {
	return nil
}

func (c mockConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (c mockConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func Test_retryAfterCasLockingFailureWithXmemRetry(t *testing.T) {
	utilsNotUsed, _, xmem, _, throttler, remoteClusterSvc, colManSvc, eventProducer := setupBoilerPlateXmem("B1", base.CRMode_LWW)
	xmem.config.mobileCompatible = base.MobileCompatibilityActive

	setupMocksXmem(xmem, utilsNotUsed, throttler, remoteClusterSvc, colManSvc, eventProducer)

	req := &base.WrappedMCRequest{}
	req.Req = &mc.MCRequest{}
	req.Req.Opaque = 1
	req.SubdocCmdOptions = &base.SubdocCmdOptions{SubdocOp: base.SubdocSet, ExtrasPreSubdocCmd: make([]byte, 24)}

	xmem.receive_token_ch = make(chan int, 10)
	xmem.buf = newReqBuffer(uint16(xmem.config.maxCount*2), uint16(float64(xmem.config.maxCount)*0.2), xmem.receive_token_ch, xmem.Logger(), xmem.dataPool)
	bufReq := newBufferedMCRequest()
	bufReq.req = req
	xmem.buf.empty_slots_pos = make(chan uint16, 1024)
	xmem.buf.slots[1] = bufReq

	finCh := make(chan bool)
	wg := sync.WaitGroup{}
	wg.Add(1)
	client := &mcMock.ClientIface{}
	xmem.setClient(base.NewXmemClient("test", 2*time.Second, 2*time.Second, client, 2, 2*time.Second, log.NewLogger("test", log.DefaultLoggerContext)), true)
	client.On("Hijack").Return(mockConn{})
	resp := &mc.MCResponse{
		Status: mc.KEY_EEXISTS,
		Opaque: 1,
	}
	client.On("Receive").Return(resp, err)

	go xmem.receiveResponse(finCh, &wg)

	xmem.receive_token_ch <- 1
	time.Sleep(2 * time.Second)
	assert.Equal(t, req.RetryCRCount, 1)

	// say the xmem buffer repair retry mechanism fired the request calls and the response comes now
	xmem.receive_token_ch <- 1
	time.Sleep(2 * time.Second)
	assert.Equal(t, req.RetryCRCount, 1) // retryCnt should remain 1
	finCh <- true
}

func getVVXattr(bucket *gocb.Bucket, key, colName, scopeName string, a *assert.Assertions) (cvCas []byte, src []byte, ver []byte, mv []byte, pv []byte, importCas []byte) {
	values, err := getPathValue(key, colName, scopeName, []string{crMeta.XATTR_CVCAS_PATH, crMeta.XATTR_SRC_PATH, crMeta.XATTR_VER_PATH, crMeta.XATTR_MV_PATH, crMeta.XATTR_PV_PATH, crMeta.XATTR_IMPORTCAS, base.XATTR_MOBILE}, bucket)
	a.Nil(err)
	if values.Exists(0) {
		err = values.ContentAt(0, &cvCas)
		a.Nil(err)
		l := len(cvCas)
		// Remove the quotes
		cvCas = cvCas[1 : l-1]
	}
	if values.Exists(1) {
		err = values.ContentAt(1, &src)
		a.Nil(err)
		l := len(src)
		src = src[1 : l-1]
	}
	if values.Exists(2) {
		err = values.ContentAt(2, &ver)
		a.Nil(err)
		l := len(ver)
		ver = ver[1 : l-1]
	}
	if values.Exists(3) {
		err = values.ContentAt(3, &mv)
		a.Nil(err)
	}
	if values.Exists(4) {
		err = values.ContentAt(4, &pv)
		a.Nil(err)
	}
	if values.Exists(5) {
		err = values.ContentAt(5, &importCas)
		a.Nil(err)
		l := len(importCas)
		importCas = importCas[1 : l-1]
	}
	return
}

// This routine was used to generate import mutations used in TestMobileImportCasLWW.
// When mobile imports a document, it will:
//  1. Update its HLV, with cvCAS set to the document CAS.
//  2. Add the mobile metadata in the document XATTR (_sync)
//  3. Write back the document. This results in a new mutation with new CAS.
//     Mobile will set an XATTR _importCAS to the same value as the new CAS
//
// The resulting import mutation has the following properties:
// 1. importCAS == document.CAS
// 2. cvCAS == pre-import document CAS
// cvCAS represents the HLV version. It is the CAS value used for conflict resolution
// If there is a new mutation on the document, the new mutation will have:
// document.CAS > importCAS
// This new mutation is no longer considered import mutation. It is a local mutation. When it is replicated to a target,
// the importCAS XATTR will be removed.
func simulateImportOperation(a *assert.Assertions, bucket *gocb.Bucket, key, colName, scopeName string, bucketId hlv.DocumentSourceId, curNonImportCas uint64) gocb.Cas {
	// xattr Lookup
	cvCas, src, ver, mv, pv, _ := getVVXattr(bucket, key, colName, scopeName, a)
	meta, err := crMeta.NewMetadataForTest([]byte(key), []byte(bucketId), curNonImportCas, 1, cvCas, src, ver, pv, mv)
	a.Nil(err)
	hlv := meta.GetHLV()

	mutateInSpec := []gocb.MutateInSpec{}
	// Update PV
	pvMap := hlv.GetPV()
	a.Nil(err)
	if len(pvMap) > 0 {
		newPvMap := srcCasMapToHex(pvMap)
		mutateInSpec = append(mutateInSpec, gocb.UpsertSpec(crMeta.XATTR_PV_PATH, newPvMap, &gocb.UpsertSpecOptions{IsXattr: true, CreatePath: true}))
	} else if meta.HadPv() {
		mutateInSpec = append(mutateInSpec, gocb.RemoveSpec(crMeta.XATTR_PV_PATH, &gocb.RemoveSpecOptions{IsXattr: true}))
	}
	// Update MV
	mvMap := hlv.GetMV()
	if len(mvMap) > 0 {
		newMvMap := srcCasMapToHex(mvMap)
		mutateInSpec = append(mutateInSpec, gocb.UpsertSpec(crMeta.XATTR_MV_PATH, newMvMap, &gocb.UpsertSpecOptions{IsXattr: true, CreatePath: true}))
	} else if meta.HadMv() {
		mutateInSpec = append(mutateInSpec, gocb.RemoveSpec(crMeta.XATTR_MV_PATH, &gocb.RemoveSpecOptions{IsXattr: true}))
	}
	// Update ver, src stays the same
	newVer := base.Uint64ToHexLittleEndian(curNonImportCas)
	mutateInSpec = append(mutateInSpec, gocb.UpsertSpec(crMeta.XATTR_VER_PATH, string(newVer), &gocb.UpsertSpecOptions{IsXattr: true, CreatePath: true}))
	// Update cvCas to the same value
	mutateInSpec = append(mutateInSpec, gocb.UpsertSpec(crMeta.XATTR_CVCAS_PATH, string(newVer), &gocb.UpsertSpecOptions{IsXattr: true, CreatePath: true}))
	// Add _mou.importCas
	mutateInSpec = append(mutateInSpec, gocb.UpsertSpec(crMeta.XATTR_IMPORTCAS, gocb.MutationMacroCAS, &gocb.UpsertSpecOptions{IsXattr: true, CreatePath: true}))
	res, err := bucket.Scope(scopeName).Collection(colName).MutateIn(key, mutateInSpec, &gocb.MutateInOptions{
		Internal: struct {
			DocFlags gocb.SubdocDocFlag
			User     []byte
		}{
			DocFlags: gocb.SubdocDocFlagAccessDeleted,
		},
	})
	a.Nil(err)
	return res.Cas()
}

func srcCasMapToHex(input hlv.VersionsMap) (output map[string]string) {
	if len(input) == 0 {
		return
	}
	output = make(map[string]string)
	for key, val := range input {
		hexVal := base.Uint64ToHexLittleEndian(val)
		output[string(key)] = string(hexVal)
	}
	return
}

func createSDKAgent(agentConfig *gocbcore.AgentConfig) (*gocbcore.Agent, func()) {
	agent, err := gocbcore.CreateAgent(agentConfig)
	closeFunc := func() {
		err = agent.Close()
		if err != nil {
			fmt.Printf("Error while closing agent, err=%v\n", err)
		}
	}
	if err != nil {
		fmt.Printf("CreateAgent err=%v\n", err)
	}

	signal := make(chan error)
	_, err = agent.WaitUntilReady(time.Now().Add(15*time.Second),
		commonAgentWAROptions, func(res *gocbcore.WaitUntilReadyResult, err error) {
			signal <- err
		})

	if err == nil {
		err = <-signal
		if err != nil {
			fmt.Printf("Waited 15 seconds for bucket to be ready, err=%v\n", err)
			return nil, func() {}
		}
	}

	return agent, closeFunc
}

func getDocMeta(agent *gocbcore.Agent, key, collection, scope string) (cas uint64, seqno uint64, datatype uint8, flags uint32, value []byte, expiry uint32) {
	signal := make(chan error)
	_, err = agent.GetMeta(
		gocbcore.GetMetaOptions{
			Key:            []byte(key),
			CollectionName: collection,
			ScopeName:      scope,
		},
		func(gmr *gocbcore.GetMetaResult, err error) {
			cas = uint64(gmr.Cas)
			seqno = uint64(gmr.SeqNo)
			datatype = gmr.Datatype
			flags = gmr.Flags
			value = gmr.Value
			expiry = gmr.Expiry
			signal <- err
		},
	)

	if err == nil {
		err = <-signal
		if err != nil {
			fmt.Printf("GetMeta error, err=%v\n", err)
			return
		}
	}
	return
}

func getDocMetaAndVV(bucket *gocb.Bucket, agent *gocbcore.Agent, key, collection, scope string, a *assert.Assertions) (cas uint64, seqno uint64, datatype uint8, flags uint32, value []byte, expiry uint32, cvCas, src, ver, importCas, pv, mv []byte) {
	cvCas, src, ver, mv, pv, importCas = getVVXattr(bucket, key, collection, scope, a)
	cas, seqno, datatype, flags, value, expiry = getDocMeta(agent, key, collection, scope)
	return
}

func writeDoc(agent *gocbcore.Agent, key, val, collection, scope string, flags uint32, datatype uint8, expiry uint32) {
	signal := make(chan error)
	agent.Set(gocbcore.SetOptions{
		Key:            []byte(key),
		CollectionName: collection,
		ScopeName:      scope,
		Value:          []byte(val),
		Flags:          flags,
		Datatype:       datatype,
		Expiry:         expiry,
	}, func(sr *gocbcore.StoreResult, err error) {
		signal <- err
	})
	if err == nil {
		err = <-signal
		if err != nil {
			fmt.Printf("Set error, err=%v\n", err)
			return
		}
	}
	return
}

func deleteDoc(agent *gocbcore.Agent, key, collection, scope string) {
	signal := make(chan error)
	agent.Delete(gocbcore.DeleteOptions{
		Key:            []byte(key),
		CollectionName: collection,
		ScopeName:      scope,
	}, func(dr *gocbcore.DeleteResult, err error) {
		signal <- err
	})
	if err == nil {
		err = <-signal
		if err != nil {
			fmt.Printf("Delete error, err=%v\n", err)
			return
		}
	}
	return
}

func getGocbBucket(connStr string, bucketName string) (bucket *gocb.Bucket, closeFunc func(opts *gocb.ClusterCloseOptions) error) {
	tgtCluster, err := gocb.Connect(connStr, gocb.ClusterOptions{Authenticator: gocb.PasswordAuthenticator{
		Username: username,
		Password: password,
	}})
	closeFunc = tgtCluster.Close
	if err != nil {
		return
	}
	err = tgtCluster.WaitUntilReady(15*time.Second, nil)
	if err != nil {
		return
	}
	bucket = tgtCluster.Bucket(bucketName)
	err = bucket.WaitUntilReady(20*time.Second, &gocb.WaitUntilReadyOptions{DesiredState: gocb.ClusterStateOnline})
	if err != nil {
		fmt.Printf("TestImportForCasRollbackCase skipped because bucket is cannot be created. Error: %v\n", err)
		return
	}
	return
}

func runCmd(cmd *exec.Cmd, printCmd bool) ([]byte, error) {
	if printCmd {
		fmt.Printf("command=%v", cmd)
	}
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		panic(err)
	}
	if printCmd {
		fmt.Printf("output=%s\n", out)
	}
	return out, err
}

func setupClusterRunCluster(port int) {
	defer time.Sleep(3 * time.Second)
	cmd := exec.Command(curl, "-u", combinedCreds, "-X", POST, fmt.Sprintf("http://localhost:%v/nodes/self/controller/settings", port))
	runCmd(cmd, printCmd)
	cmd = exec.Command(curl, "-u", combinedCreds, "-X", POST, "-d", "hostname=127.0.0.1", fmt.Sprintf("http://127.0.0.1:%v/node/controller/rename", port))
	runCmd(cmd, printCmd)
	cmd = exec.Command(curl, "-u", combinedCreds, "-X", POST, "-d", "services=kv", fmt.Sprintf("http://localhost:%v/node/controller/setupServices", port))
	runCmd(cmd, printCmd)
	cmd = exec.Command(curl, "-u", combinedCreds, "-X", POST, "-d", fmt.Sprintf("password=%v", password), "-d", fmt.Sprintf("username=%v", username), "-d", fmt.Sprintf("port=%v", port), fmt.Sprintf("http://localhost:%v/settings/web", port))
	runCmd(cmd, printCmd)
}

func createClusterRunBucket(port int, bucketname string, lww bool) {
	defer time.Sleep(3 * time.Second)
	var cmd *exec.Cmd
	if lww {
		cmd = exec.Command(curl, "-u", combinedCreds, "-X", POST, fmt.Sprintf("http://localhost:%v/%v/buckets", port, poolsDefaultPath), "-d", fmt.Sprintf("name=%v", bucketname), "-d", fmt.Sprintf("ramQuotaMB=%v", 100), "-d", fmt.Sprintf("CompressionMode=%v", "Active"), "-d", fmt.Sprintf("conflictResolutionType=%v", "lww"))
	} else {
		cmd = exec.Command(curl, "-u", combinedCreds, "-X", POST, fmt.Sprintf("http://localhost:%v/%v/buckets", port, poolsDefaultPath), "-d", fmt.Sprintf("name=%v", bucketname), "-d", fmt.Sprintf("ramQuotaMB=%v", 100), "-d", fmt.Sprintf("CompressionMode=%v", "Active"))
	}
	runCmd(cmd, printCmd)
}

func createClusterRunRemoteRef(port int, tgtPort int, refName string) {
	defer time.Sleep(5 * time.Second)
	cmd := exec.Command(curl, "-u", combinedCreds, "-X", POST, fmt.Sprintf("http://localhost:%v/%v/remoteClusters", port, poolsDefaultPath), "-d", fmt.Sprintf("name=%v", refName), "-d", fmt.Sprintf("hostname=127.0.0.1:%v", tgtPort), "-d", fmt.Sprintf("username=%v", username), "-d", fmt.Sprintf("password=%v", password))
	runCmd(cmd, printCmd)
}

func createClusterRunReplication(port int, fromBucket, toBucket, toCluster string) string {
	defer time.Sleep(5 * time.Second)
	cmd := exec.Command(curl, "-u", combinedCreds, "-X", POST, fmt.Sprintf("http://localhost:%v/controller/createReplication", port), "-d", fmt.Sprintf("fromBucket=%v", fromBucket), "-d", fmt.Sprintf("toBucket=%v", toBucket), "-d", fmt.Sprintf("toCluster=%v", toCluster), "-d", fmt.Sprintf("replicationType=%v", "continuous"), "-d", fmt.Sprintf("checkpointInterval=%v", 60), "-d", fmt.Sprintf("statsInterval=%v", 500))
	out, _ := runCmd(cmd, printCmd)
	var op map[string]interface{}
	err := json.Unmarshal(out, &op)
	if err != nil || len(op) == 0 {
		fmt.Println("err=", err)
		return ""
	}
	return op["id"].(string)
}

func pauseClusterRunReplication(port int, replID string) {
	defer time.Sleep(5 * time.Second)
	changeClusterRunReplicationSettings(port, replID, "pauseRequested", "true")
}

func resumeClusterRunReplication(port int, replID string) {
	defer time.Sleep(5 * time.Second)
	changeClusterRunReplicationSettings(port, replID, "pauseRequested", "false")
}

func changeClusterRunReplicationSettings(port int, replID, key, val string) {
	defer time.Sleep(3 * time.Second)
	cmd := exec.Command(curl, "-u", combinedCreds, "-X", POST, fmt.Sprintf("http://localhost:%v/settings/replications/%v", port, strings.Replace(replID, "/", "%2F", -1)), "-d", fmt.Sprintf("%v=%v", key, val))
	runCmd(cmd, printCmd)
}

func changeClusterRunBucketSetting(port int, bucketname, key, val string) string {
	cmd := exec.Command(curl, "-u", combinedCreds, "-X", POST, fmt.Sprintf("http://localhost:%v/%v/buckets/%v", port, poolsDefaultPath, bucketname), "-d", fmt.Sprintf("%v=%v", key, val))
	op, _ := runCmd(cmd, printCmd)
	return string(op)
}

func turnOnCrossXVersioningClusterRun(port int, bucketname string) {
	defer time.Sleep(3 * time.Second)
	op := changeClusterRunBucketSetting(port, bucketname, "enableCrossClusterVersioning", "true")
	for op != "" && op != "Cross cluster versioning already enabled" {
		time.Sleep(2 * time.Second)
		op = changeClusterRunBucketSetting(port, bucketname, "enableCrossClusterVersioning", "true")
	}
}

func changesLeft(port int) string {
	defer time.Sleep(3 * time.Second)
	cmd := exec.Command(curl, "-u", combinedCreds, "-X", POST, fmt.Sprintf("http://localhost:%v/_prometheusMetrics", port))
	out, _ := runCmd(cmd, printCmd)
	return string(out)
}

func setupForMobileConvergenceTest(a *assert.Assertions, colName, scopeName, bucketName, docKey, docVal string, srcNode, tgtNode int, bucketUUID string) (*gocb.Bucket, *gocb.Bucket, *gocbcore.Agent, *gocbcore.Agent, string, func(opts *gocb.ClusterCloseOptions) error, func(opts *gocb.ClusterCloseOptions) error, func(), func(), string, uint64, uint64) {
	setupClusterRunCluster(srcNode)
	setupClusterRunCluster(tgtNode)

	createClusterRunBucket(srcNode, bucketName, true)
	turnOnCrossXVersioningClusterRun(srcNode, bucketName)

	createClusterRunBucket(tgtNode, bucketName, true)
	turnOnCrossXVersioningClusterRun(tgtNode, bucketName)

	srcBucket, closeFunc1 := getGocbBucket(sourceConnStr, bucketName)

	tgtBucket, closeFunc2 := getGocbBucket(targetConnStr, bucketName)

	srcAgentgentConfig := &gocbcore.AgentConfig{
		BucketName:        bucketName,
		UserAgent:         sourceClusterName,
		UseTLS:            false,
		TLSRootCAProvider: func() *x509.CertPool { return nil },
		UseCollections:    true,
		AuthMechanisms:    []gocbcore.AuthMechanism{gocbcore.ScramSha256AuthMechanism},
		Auth:              gocbcore.PasswordAuthProvider{Username: username, Password: password},
		MemdAddrs:         []string{kvStringSrc},
	}

	tgtAgentgentConfig := &gocbcore.AgentConfig{
		BucketName:        bucketName,
		UserAgent:         targetClusterName,
		UseTLS:            false,
		TLSRootCAProvider: func() *x509.CertPool { return nil },
		UseCollections:    true,
		AuthMechanisms:    []gocbcore.AuthMechanism{gocbcore.ScramSha256AuthMechanism},
		Auth:              gocbcore.PasswordAuthProvider{Username: username, Password: password},
		MemdAddrs:         []string{kvStringTgt},
	}

	srcAgent, closeFunc3 := createSDKAgent(srcAgentgentConfig)

	tgtAgent, closeFunc4 := createSDKAgent(tgtAgentgentConfig)

	a.NotNil(srcAgent)
	a.NotNil(tgtAgent)
	a.NotNil(srcBucket)
	a.NotNil(tgtBucket)

	createClusterRunRemoteRef(srcNode, tgtNode, "C1")
	createClusterRunRemoteRef(tgtNode, srcNode, "C2")
	replID1 := createClusterRunReplication(srcNode, bucketName, bucketName, "C1")
	replID2 := createClusterRunReplication(tgtNode, bucketName, bucketName, "C2")
	changeClusterRunReplicationSettings(srcNode, replID1, "mobile", "Active")
	changeClusterRunReplicationSettings(tgtNode, replID2, "mobile", "Active")

	writeDoc(srcAgent, docKey, docVal, colName, scopeName, 0, base.JSONDataType, 0)
	time.Sleep(3 * time.Second) // wait for it to replicate
	cas1, _, _, _, _, _, _, _, _, _, _, _ := getDocMetaAndVV(srcBucket, srcAgent, docKey, colName, scopeName, a)
	cas2, _, _, _, _, _, _, _, _, _, _, _ := getDocMetaAndVV(tgtBucket, tgtAgent, docKey, colName, scopeName, a)

	a.Equal(cas1, cas2)

	// pause the replication to make sure we have a desired state
	pauseClusterRunReplication(srcNode, replID1)
	pauseClusterRunReplication(tgtNode, replID2)

	time.Sleep(2 * time.Second)

	return srcBucket, tgtBucket, srcAgent, tgtAgent, replID1, closeFunc1, closeFunc2, closeFunc3, closeFunc4, replID2, cas1, cas2
}

// this is a scenario where, after CAS regeneration, import happens after cas convergence from C2 to C1
func importAfterConvergence(a *assert.Assertions, colName, scopeName, replID1, replID2, docKey string, srcNode, tgtNode int, cas1, cas2 uint64, bucketUUID string, srcBucket, tgtBucket *gocb.Bucket, srcAgent, tgtAgent *gocbcore.Agent) {
	simulateImportOperation(a, tgtBucket, docKey, colName, scopeName, hlv.DocumentSourceId(bucketUUID), cas2)

	cas3, _, _, _, _, _, _, _, _, _, _, _ := getDocMetaAndVV(srcBucket, srcAgent, docKey, colName, scopeName, a)
	cas4, _, _, _, _, _, cvCas4, _, _, importCas4, _, _ := getDocMetaAndVV(tgtBucket, tgtAgent, docKey, colName, scopeName, a)
	cvCas4Int, err := base.HexLittleEndianToUint64(cvCas4)
	a.Nil(err)
	importCas4Int, err := base.HexLittleEndianToUint64(importCas4)
	a.Nil(err)

	// make sure we have a scenario where we have a cas rollback with SET_WITH_META
	a.Greater(cas4, cas3)
	a.Greater(cas3, cas1)
	a.Greater(cas4, cas2)
	a.Equal(cas2, cvCas4Int)
	a.Greater(cas4, cvCas4Int)
	a.Equal(cas4, importCas4Int)

	// resume replication from C1 to C2 only and make sure cas doesn't rollback on target
	resumeClusterRunReplication(srcNode, replID1)

	cas5, _, _, _, _, _, _, _, _, _, _, _ := getDocMetaAndVV(srcBucket, srcAgent, docKey, colName, scopeName, a)
	cas6, _, _, _, _, _, cvCas6, _, _, _, _, _ := getDocMetaAndVV(tgtBucket, tgtAgent, docKey, colName, scopeName, a)
	cvCas6Int, err := base.HexLittleEndianToUint64(cvCas6)
	a.Nil(err)

	a.Equal(cas5, cas3)      // no changes on source
	a.Greater(cas6, cas5)    // check if cas was regenerated
	a.Equal(cvCas6Int, cas6) // check if macro-expansion was successful

	// resume replication from C2 to C1 also, to make sure we have convergence after cas regeneration, but before an import process
	resumeClusterRunReplication(tgtNode, replID2)

	cas7, _, _, _, _, _, cvCas7, _, _, _, _, _ := getDocMetaAndVV(srcBucket, srcAgent, docKey, colName, scopeName, a)
	cas8, _, _, _, _, _, cvCas8, _, _, _, _, _ := getDocMetaAndVV(tgtBucket, tgtAgent, docKey, colName, scopeName, a)
	cvCas7Int, err := base.HexLittleEndianToUint64(cvCas7)
	a.Nil(err)
	cvCas8Int, err := base.HexLittleEndianToUint64(cvCas8)
	a.Nil(err)
	// check for convergence
	a.Equal(cas8, cas6) // no changes on target
	a.Equal(cvCas8Int, cvCas6Int)
	a.Equal(cas7, cas8)
	a.Equal(cvCas7Int, cvCas8Int)

	simulateImportOperation(a, tgtBucket, docKey, colName, scopeName, hlv.DocumentSourceId(bucketUUID), cas8)
	cas9, _, _, _, _, _, cvCas9, _, _, _, _, _ := getDocMetaAndVV(srcBucket, srcAgent, docKey, colName, scopeName, a)
	cas10, _, _, _, _, _, cvCas10, _, _, importCas10, _, _ := getDocMetaAndVV(tgtBucket, tgtAgent, docKey, colName, scopeName, a)
	cvCas9Int, err := base.HexLittleEndianToUint64(cvCas9)
	a.Nil(err)
	cvCas10Int, err := base.HexLittleEndianToUint64(cvCas10)
	a.Nil(err)
	importCas10Int, err := base.HexLittleEndianToUint64(importCas10)
	a.Nil(err)

	a.Equal(cas10, importCas10Int)
	a.Equal(cvCas10Int, cas8)
	a.Greater(cas10, cvCas10Int)
	a.Equal(cas9, cas7) // no changes on source - import process should lead to a no-op
	a.Equal(cvCas9Int, cvCas7Int)
}

// this is a scenario where, after CAS regeneration, import happens before cas convergence from C2 to C1
func convergenceAfterImport(a *assert.Assertions, colName, scopeName, replID1, replID2, docKey string, srcNode, tgtNode int, cas1, cas2 uint64, bucketUUID string, srcBucket, tgtBucket *gocb.Bucket, srcAgent, tgtAgent *gocbcore.Agent) {
	simulateImportOperation(a, tgtBucket, docKey, colName, scopeName, hlv.DocumentSourceId(bucketUUID), cas2)

	cas3, _, _, _, _, _, _, _, _, _, _, _ := getDocMetaAndVV(srcBucket, srcAgent, docKey, colName, scopeName, a)
	cas4, _, _, _, _, _, cvCas4, _, _, importCas4, _, _ := getDocMetaAndVV(tgtBucket, tgtAgent, docKey, colName, scopeName, a)
	cvCas4Int, err := base.HexLittleEndianToUint64(cvCas4)
	a.Nil(err)
	importCas4Int, err := base.HexLittleEndianToUint64(importCas4)
	a.Nil(err)

	// make sure we have a scenario where we have a cas rollback with SET_WITH_META
	a.Greater(cas4, cas3)
	a.Greater(cas3, cas1)
	a.Greater(cas4, cas2)
	a.Equal(cas2, cvCas4Int)
	a.Greater(cas4, cvCas4Int)
	a.Equal(cas4, importCas4Int)

	// resume replication from C1 to C2 only and make sure cas doesn't rollback on target
	resumeClusterRunReplication(srcNode, replID1)

	cas5, _, _, _, _, _, _, _, _, _, _, _ := getDocMetaAndVV(srcBucket, srcAgent, docKey, colName, scopeName, a)
	cas6, _, _, _, _, _, cvCas6, _, _, _, _, _ := getDocMetaAndVV(tgtBucket, tgtAgent, docKey, colName, scopeName, a)
	cvCas6Int, err := base.HexLittleEndianToUint64(cvCas6)
	a.Nil(err)

	a.Equal(cas5, cas3)      // no changes on source
	a.Greater(cas6, cas5)    // check if cas was regenerated
	a.Equal(cvCas6Int, cas6) // check if macro-expansion was successful

	simulateImportOperation(a, tgtBucket, docKey, colName, scopeName, hlv.DocumentSourceId(bucketUUID), cas6)
	cas7, _, _, _, _, _, _, _, _, _, _, _ := getDocMetaAndVV(srcBucket, srcAgent, docKey, colName, scopeName, a)
	cas8, _, _, _, _, _, cvCas8, _, _, importCas8, _, _ := getDocMetaAndVV(tgtBucket, tgtAgent, docKey, colName, scopeName, a)
	cvCas8Int, err := base.HexLittleEndianToUint64(cvCas8)
	a.Nil(err)
	importCas8Int, err := base.HexLittleEndianToUint64(importCas8)
	a.Nil(err)

	a.Equal(cas5, cas7) // no changes on source
	a.Equal(cvCas8Int, cas6)
	a.Greater(cas8, cvCas8Int)
	a.Equal(cas8, importCas8Int)

	// resume replication from C2 to C1 also, to make sure we have final convergence after cas regeneration and import process
	resumeClusterRunReplication(tgtNode, replID2)

	cas9, _, _, _, _, _, cvCas9, _, _, importCas9, _, _ := getDocMetaAndVV(srcBucket, srcAgent, docKey, colName, scopeName, a)
	cas10, _, _, _, _, _, cvCas10, _, _, importCas10, _, _ := getDocMetaAndVV(tgtBucket, tgtAgent, docKey, colName, scopeName, a)
	cvCas9Int, err := base.HexLittleEndianToUint64(cvCas9)
	a.Nil(err)
	cvCas10Int, err := base.HexLittleEndianToUint64(cvCas10)
	a.Nil(err)
	importCas9Int, err := base.HexLittleEndianToUint64(importCas9)
	a.Nil(err)
	importCas10Int, err := base.HexLittleEndianToUint64(importCas10)
	a.Nil(err)
	// check for final convergence
	a.Equal(cas10, cas8) // no changes on target
	a.Equal(importCas10Int, importCas8Int)
	a.Equal(cvCas10Int, cvCas8Int)
	a.Equal(cas10, cas9)
	a.Equal(cvCas10Int, cvCas9Int)
	a.Equal(importCas10Int, importCas9Int)
}

func verifyStatsForDone(a *assert.Assertions, srcNode, tgtNode int) {
	// changes left should be 0 on both source and target
	srcPromStats := changesLeft(srcNode)
	a.NotRegexp(regexp.MustCompile("changes_left.*} [1-9]"), srcPromStats)
	tgtPromStats := changesLeft(tgtNode)
	a.NotRegexp(regexp.MustCompile("changes_left.*} [1-9]"), tgtPromStats)
}

// make sure you have a "dataclean" cluster_run running. The test doesn't cleanup the cluster at the end.
// go test -timeout 120s -run ^TestCasRollbackLWWMobileScenario1$ github.com/couchbase/goxdcr/parts
func TestCasRollbackLWWMobileScenario1(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping TestCasRollbackLWWMobileScenario1")
	}

	// this is a scenario where, after CAS regeneration by subdoc op, import happens before cas convergence from C2 to C1
	// both source and target docs are not tombstones
	fmt.Println("============== Test case start: TestCasRollbackLWWMobileScenario1 =================")
	defer fmt.Println("============== Test case end: TestCasRollbackLWWMobileScenario1 =================")
	a := assert.New(t)

	// test common parameters
	bucketName := "B1" // both on source and target
	bucketUUID := "adX1DsoRpCb6kQWoZYq5ew"
	scopeName := "_default"
	colName := "_default"
	docKey := "mobileDoc"
	docVal := "{\"foo\" : \"bar\"}"
	srcNode := 9000 // cluster_run source node port
	tgtNode := 9001 // cluster_run target node port
	// printCmd = true // uncomment this line for printing cluster_run curl command while debugging

	srcBucket, tgtBucket, srcAgent, tgtAgent, replID1, closeFunc1, closeFunc2, closeFunc3, closeFunc4, replID2, cas1, cas2 := setupForMobileConvergenceTest(a, colName, scopeName, bucketName, docKey, docVal, srcNode, tgtNode, bucketUUID)
	defer closeFunc1(nil)
	defer closeFunc2(nil)
	defer closeFunc3()
	defer closeFunc4()

	writeDoc(srcAgent, docKey, docVal, colName, scopeName, 0, base.JSONDataType, 0) // create a mutation on source that is not tombstone. target is already not a tombstone.

	convergenceAfterImport(a, colName, scopeName, replID1, replID2, docKey, srcNode, tgtNode, cas1, cas2, bucketUUID, srcBucket, tgtBucket, srcAgent, tgtAgent)

	verifyStatsForDone(a, srcNode, tgtNode)
}

// make sure you have a "dataclean" cluster_run running. The test doesn't cleanup the cluster at the end.
// go test -timeout 120s -run ^TestCasRollbackLWWMobileScenario2$ github.com/couchbase/goxdcr/parts
func TestCasRollbackLWWMobileScenario2(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping TestCasRollbackLWWMobileScenario2")
	}

	// this is a scenario where, after CAS regeneration by subdoc op, import happens after cas convergence from C2 to C1
	// both source and target docs are not tombstones
	fmt.Println("============== Test case start: TestCasRollbackLWWMobileScenario2 =================")
	defer fmt.Println("============== Test case end: TestCasRollbackLWWMobileScenario2 =================")
	a := assert.New(t)

	// test common parameters
	bucketName := "B1" // both on source and target
	bucketUUID := "adX1DsoRpCb6kQWoZYq5ew"
	scopeName := "_default"
	colName := "_default"
	docKey := "mobileDoc"
	docVal := "{\"foo\" : \"bar\"}"
	srcNode := 9000 // cluster_run source node port
	tgtNode := 9001 // cluster_run target node port
	// printCmd = true // uncomment this line for printing cluster_run curl command while debugging

	srcBucket, tgtBucket, srcAgent, tgtAgent, replID1, closeFunc1, closeFunc2, closeFunc3, closeFunc4, replID2, cas1, cas2 := setupForMobileConvergenceTest(a, colName, scopeName, bucketName, docKey, docVal, srcNode, tgtNode, bucketUUID)
	defer closeFunc1(nil)
	defer closeFunc2(nil)
	defer closeFunc3()
	defer closeFunc4()

	writeDoc(srcAgent, docKey, docVal, colName, scopeName, 0, base.JSONDataType, 0) // create a mutation on source that is not tombstone. target is already not a tombstone.

	importAfterConvergence(a, colName, scopeName, replID1, replID2, docKey, srcNode, tgtNode, cas1, cas2, bucketUUID, srcBucket, tgtBucket, srcAgent, tgtAgent)

	verifyStatsForDone(a, srcNode, tgtNode)
}

// make sure you have a "dataclean" cluster_run running. The test doesn't cleanup the cluster at the end.
// go test -timeout 120s -run ^TestCasRollbackLWWMobileScenario3$ github.com/couchbase/goxdcr/parts
func TestCasRollbackLWWMobileScenario3(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping TestCasRollbackLWWMobileScenario3")
	}

	// this is a scenario where, after CAS regeneration by subdoc op, import happens before cas convergence from C2 to C1
	// target doc is a tombstone, source doc is not.
	fmt.Println("============== Test case start: TestCasRollbackLWWMobileScenario3 =================")
	defer fmt.Println("============== Test case end: TestCasRollbackLWWMobileScenario3 =================")
	a := assert.New(t)

	// test common parameters
	bucketName := "B1" // both on source and target
	bucketUUID := "adX1DsoRpCb6kQWoZYq5ew"
	scopeName := "_default"
	colName := "_default"
	docKey := "mobileDoc"
	docVal := "{\"foo\" : \"bar\"}"
	srcNode := 9000 // cluster_run source node port
	tgtNode := 9001 // cluster_run target node port
	// printCmd = true // uncomment this line for printing cluster_run curl command while debugging

	srcBucket, tgtBucket, srcAgent, tgtAgent, replID1, closeFunc1, closeFunc2, closeFunc3, closeFunc4, replID2, cas1, cas2 := setupForMobileConvergenceTest(a, colName, scopeName, bucketName, docKey, docVal, srcNode, tgtNode, bucketUUID)
	defer closeFunc1(nil)
	defer closeFunc2(nil)
	defer closeFunc3()
	defer closeFunc4()

	deleteDoc(tgtAgent, docKey, colName, scopeName)                                 // target doc is a tombstone
	writeDoc(srcAgent, docKey, docVal, colName, scopeName, 0, base.JSONDataType, 0) // source mutation is not a tombstone

	convergenceAfterImport(a, colName, scopeName, replID1, replID2, docKey, srcNode, tgtNode, cas1, cas2, bucketUUID, srcBucket, tgtBucket, srcAgent, tgtAgent)

	verifyStatsForDone(a, srcNode, tgtNode)
}

// make sure you have a "dataclean" cluster_run running. The test doesn't cleanup the cluster at the end.
// go test -timeout 120s -run ^TestCasRollbackLWWMobileScenario4$ github.com/couchbase/goxdcr/parts
func TestCasRollbackLWWMobileScenario4(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping TestCasRollbackLWWMobileScenario4")
	}

	// this is a scenario where, after CAS regeneration by subdoc op, import happens after cas convergence from C2 to C1
	// target doc is a tombstone, source doc is not.
	fmt.Println("============== Test case start: TestCasRollbackLWWMobileScenario4 =================")
	defer fmt.Println("============== Test case end: TestCasRollbackLWWMobileScenario4 =================")
	a := assert.New(t)

	// test common parameters
	bucketName := "B1" // both on source and target
	bucketUUID := "adX1DsoRpCb6kQWoZYq5ew"
	scopeName := "_default"
	colName := "_default"
	docKey := "mobileDoc"
	docVal := "{\"foo\" : \"bar\"}"
	srcNode := 9000 // cluster_run source node port
	tgtNode := 9001 // cluster_run target node port
	// printCmd = true // uncomment this line for printing cluster_run curl command while debugging

	srcBucket, tgtBucket, srcAgent, tgtAgent, replID1, closeFunc1, closeFunc2, closeFunc3, closeFunc4, replID2, cas1, cas2 := setupForMobileConvergenceTest(a, colName, scopeName, bucketName, docKey, docVal, srcNode, tgtNode, bucketUUID)
	defer closeFunc1(nil)
	defer closeFunc2(nil)
	defer closeFunc3()
	defer closeFunc4()

	deleteDoc(tgtAgent, docKey, colName, scopeName)                                 // target doc is a tombstone
	writeDoc(srcAgent, docKey, docVal, colName, scopeName, 0, base.JSONDataType, 0) // source mutation is not a tombstone

	importAfterConvergence(a, colName, scopeName, replID1, replID2, docKey, srcNode, tgtNode, cas1, cas2, bucketUUID, srcBucket, tgtBucket, srcAgent, tgtAgent)

	verifyStatsForDone(a, srcNode, tgtNode)
}

// make sure you have a "dataclean" cluster_run running. The test doesn't cleanup the cluster at the end.
// go test -timeout 120s -run ^TestCasRollbackLWWMobileScenario5$ github.com/couchbase/goxdcr/parts
func TestCasRollbackLWWMobileScenario5(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping TestCasRollbackLWWMobileScenario5")
	}

	// this is a scenario where, after CAS regeneration, import happens before cas convergence from C2 to C1
	// source doc is tombstone, target is not
	fmt.Println("============== Test case start: TestCasRollbackLWWMobileScenario5 =================")
	defer fmt.Println("============== Test case end: TestCasRollbackLWWMobileScenario5 =================")
	a := assert.New(t)

	// test common parameters
	bucketName := "B1" // both on source and target
	bucketUUID := "adX1DsoRpCb6kQWoZYq5ew"
	scopeName := "_default"
	colName := "_default"
	docKey := "mobileDoc"
	docVal := "{\"foo\" : \"bar\"}"
	srcNode := 9000 // cluster_run source node port
	tgtNode := 9001 // cluster_run target node port
	// printCmd = true // uncomment this line for printing cluster_run curl command while debugging

	srcBucket, tgtBucket, srcAgent, tgtAgent, replID1, closeFunc1, closeFunc2, closeFunc3, closeFunc4, replID2, cas1, cas2 := setupForMobileConvergenceTest(a, colName, scopeName, bucketName, docKey, docVal, srcNode, tgtNode, bucketUUID)
	defer closeFunc1(nil)
	defer closeFunc2(nil)
	defer closeFunc3()
	defer closeFunc4()

	deleteDoc(srcAgent, docKey, colName, scopeName) // create a source doc tombstone mutation. target doc is not a tombstone.

	convergenceAfterImport(a, colName, scopeName, replID1, replID2, docKey, srcNode, tgtNode, cas1, cas2, bucketUUID, srcBucket, tgtBucket, srcAgent, tgtAgent)

	verifyStatsForDone(a, srcNode, tgtNode)
}

// make sure you have a "dataclean" cluster_run running. The test doesn't cleanup the cluster at the end.
// go test -timeout 120s -run ^TestCasRollbackLWWMobileScenario6$ github.com/couchbase/goxdcr/parts
func TestCasRollbackLWWMobileScenario6(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping TestCasRollbackLWWMobileScenario6")
	}

	// this is a scenario where, after CAS regeneration, import happens after cas convergence from C2 to C1
	// source doc is tombstone, target is not
	fmt.Println("============== Test case start: TestCasRollbackLWWMobileScenario6 =================")
	defer fmt.Println("============== Test case end: TestCasRollbackLWWMobileScenario6 =================")
	a := assert.New(t)

	// test common parameters
	bucketName := "B1" // both on source and target
	bucketUUID := "adX1DsoRpCb6kQWoZYq5ew"
	scopeName := "_default"
	colName := "_default"
	docKey := "mobileDoc"
	docVal := "{\"foo\" : \"bar\"}"
	srcNode := 9000 // cluster_run source node port
	tgtNode := 9001 // cluster_run target node port
	// printCmd = true // uncomment this line for printing cluster_run curl command while debugging

	srcBucket, tgtBucket, srcAgent, tgtAgent, replID1, closeFunc1, closeFunc2, closeFunc3, closeFunc4, replID2, cas1, cas2 := setupForMobileConvergenceTest(a, colName, scopeName, bucketName, docKey, docVal, srcNode, tgtNode, bucketUUID)
	defer closeFunc1(nil)
	defer closeFunc2(nil)
	defer closeFunc3()
	defer closeFunc4()

	deleteDoc(srcAgent, docKey, colName, scopeName) // create a source doc tombstone mutation. target doc is not a tombstone.

	importAfterConvergence(a, colName, scopeName, replID1, replID2, docKey, srcNode, tgtNode, cas1, cas2, bucketUUID, srcBucket, tgtBucket, srcAgent, tgtAgent)

	verifyStatsForDone(a, srcNode, tgtNode)
}

// make sure you have a "dataclean" cluster_run running. The test doesn't cleanup the cluster at the end.
// go test -timeout 120s -run ^TestCasRollbackLWWMobileScenario7$ github.com/couchbase/goxdcr/parts
func TestCasRollbackLWWMobileScenario7(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping TestCasRollbackLWWMobileScenario7")
	}

	// this is a scenario where, after CAS regeneration, import happens before cas convergence from C2 to C1
	// both source and target docs are tombstones
	fmt.Println("============== Test case start: TestCasRollbackLWWMobileScenario7 =================")
	defer fmt.Println("============== Test case end: TestCasRollbackLWWMobileScenario7 =================")
	a := assert.New(t)

	// test common parameters
	bucketName := "B1" // both on source and target
	bucketUUID := "adX1DsoRpCb6kQWoZYq5ew"
	scopeName := "_default"
	colName := "_default"
	docKey := "mobileDoc"
	docVal := "{\"foo\" : \"bar\"}"
	srcNode := 9000 // cluster_run source node port
	tgtNode := 9001 // cluster_run target node port
	// printCmd = true // uncomment this line for printing cluster_run curl command while debugging

	srcBucket, tgtBucket, srcAgent, tgtAgent, replID1, closeFunc1, closeFunc2, closeFunc3, closeFunc4, replID2, cas1, cas2 := setupForMobileConvergenceTest(a, colName, scopeName, bucketName, docKey, docVal, srcNode, tgtNode, bucketUUID)
	defer closeFunc1(nil)
	defer closeFunc2(nil)
	defer closeFunc3()
	defer closeFunc4()

	deleteDoc(tgtAgent, docKey, colName, scopeName) // target doc is a tombstone
	deleteDoc(srcAgent, docKey, colName, scopeName) // create a source doc mutation which is also a tombstone

	convergenceAfterImport(a, colName, scopeName, replID1, replID2, docKey, srcNode, tgtNode, cas1, cas2, bucketUUID, srcBucket, tgtBucket, srcAgent, tgtAgent)

	verifyStatsForDone(a, srcNode, tgtNode)
}

// make sure you have a "dataclean" cluster_run running. The test doesn't cleanup the cluster at the end.
// go test -timeout 120s -run ^TestCasRollbackLWWMobileScenario8$ github.com/couchbase/goxdcr/parts
func TestCasRollbackLWWMobileScenario8(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping TestCasRollbackLWWMobileScenario4")
	}

	// this is a scenario where, after CAS regeneration, import happens after cas convergence from C2 to C1
	// both source and target doc are tombstone
	fmt.Println("============== Test case start: TestCasRollbackLWWMobileScenario8 =================")
	defer fmt.Println("============== Test case end: TestCasRollbackLWWMobileScenario8 =================")
	a := assert.New(t)

	// test common parameters
	bucketName := "B1" // both on source and target
	bucketUUID := "adX1DsoRpCb6kQWoZYq5ew"
	scopeName := "_default"
	colName := "_default"
	docKey := "mobileDoc"
	docVal := "{\"foo\" : \"bar\"}"
	srcNode := 9000 // cluster_run source node port
	tgtNode := 9001 // cluster_run target node port
	// printCmd = true // uncomment this line for printing cluster_run curl command while debugging

	srcBucket, tgtBucket, srcAgent, tgtAgent, replID1, closeFunc1, closeFunc2, closeFunc3, closeFunc4, replID2, cas1, cas2 := setupForMobileConvergenceTest(a, colName, scopeName, bucketName, docKey, docVal, srcNode, tgtNode, bucketUUID)
	defer closeFunc1(nil)
	defer closeFunc2(nil)
	defer closeFunc3()
	defer closeFunc4()

	deleteDoc(tgtAgent, docKey, colName, scopeName) // target doc is a tombstone
	deleteDoc(srcAgent, docKey, colName, scopeName) // create a source doc mutation which is also a tombstone

	importAfterConvergence(a, colName, scopeName, replID1, replID2, docKey, srcNode, tgtNode, cas1, cas2, bucketUUID, srcBucket, tgtBucket, srcAgent, tgtAgent)

	verifyStatsForDone(a, srcNode, tgtNode)
}
