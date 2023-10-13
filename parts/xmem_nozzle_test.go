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
	"math/rand"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	mcMock "github.com/couchbase/gomemcached/client/mocks"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/base/generator"
	"github.com/couchbase/goxdcr/common/mocks"
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
const xmemPort = "12002"
const sourcePort = "9000"
const targetPort = "9001"
const username = "Administrator"
const password = "wewewe"

var kvString = fmt.Sprintf("%s:%s", "127.0.0.1", xmemPort)
var connString = fmt.Sprintf("%s:%s", "127.0.0.1", targetPort)
var clusterReady = true // Assume we have live cluster initially
var clusterChecked = false

var events []string
var eventID int64
var err error

func setupBoilerPlateXmem(bname string, optional ...int) (*utilsMock.UtilsIface, map[string]interface{}, *XmemNozzle, *Router, *serviceDefMocks.BandwidthThrottlerSvc, *serviceDefMocks.RemoteClusterSvc, *serviceDefMocks.CollectionsManifestSvc, *mocks.PipelineEventsProducer) {

	utilitiesMock := &utilsMock.UtilsIface{}
	utilitiesMock.On("NewDataPool").Return(base.NewFakeDataPool())
	var vbList []uint16
	for i := 0; i < 1024; i++ {
		vbList = append(vbList, uint16(i))
	}

	bandwidthThrottler := &serviceDefMocks.BandwidthThrottlerSvc{}
	remoteClusterSvc := &serviceDefMocks.RemoteClusterSvc{}

	// local cluster run has KV port starting at 12000
	xmemNozzle := NewXmemNozzle("testId", remoteClusterSvc, "", "", "testTopic", "testConnPoolNamePrefix", 5, kvString, "B1", bname, "temporaryBucketUuid", "Administrator", "wewewe", base.CRMode_RevId, log.DefaultLoggerContext, utilitiesMock, vbList, nil)

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

	router, _ := NewRouter("testId", spec, nil, nil, base.CRMode_RevId, log.DefaultLoggerContext, utilitiesMock, nil, false, base.FilterExpDelNone, colManifestSvc, nil, nil, metadata.UnitTestGetCollectionsCapability(), nil, nil)

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
	xmem.sourceBucketId = hlv.DocumentSourceId("SourceCluster")
	xmem.targetBucketId = hlv.DocumentSourceId("TargetCluster")

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
	utils, settings, xmem, _, throttler, remoteClusterSvc, colManSvc, eventProducer := setupBoilerPlateXmem(xmemBucket)
	setupMocksXmem(xmem, utils, throttler, remoteClusterSvc, colManSvc, eventProducer)
	assert.Nil(xmem.initialize(settings))
	fmt.Println("============== Test case end: TestPositiveXmemNozzle =================")
}

func TestNegNoCompressionXmemNozzle(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestNegNoCompressionXmemNozzle =================")
	utils, settings, xmem, _, _, rc, _, _ := setupBoilerPlateXmem(xmemBucket)
	setupMocksCompressNeg(utils)
	setupMocksRC(rc)

	assert.Equal(base.ErrorCompressionNotSupported, xmem.initialize(settings))
	fmt.Println("============== Test case start: TestNegNoCompressionXmemNozzle =================")
}

func TestPosNoCompressionXmemNozzle(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestNegNoCompressionXmemNozzle =================")
	utils, settings, xmem, _, _, rc, _, _ := setupBoilerPlateXmem(xmemBucket)
	settings[SETTING_COMPRESSION_TYPE] = (base.CompressionType)(base.CompressionTypeForceUncompress)
	settings[ForceCollectionDisableKey] = true
	setupMocksCompressNeg(utils)
	setupMocksRC(rc)

	assert.Equal(nil, xmem.initialize(settings))
	fmt.Println("============== Test case start: TestNegNoCompressionXmemNozzle =================")
}

// AUTO is no longer a supported value. XDCR Factory should have passed in a non-auto
func TestPositiveXmemNozzleAuto(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPositiveXmemNozzleAuto =================")
	utils, settings, xmem, _, throttler, remoteClusterSvc, colManSvc, evtProducer := setupBoilerPlateXmem(xmemBucket)
	settings[SETTING_COMPRESSION_TYPE] = (base.CompressionType)(base.CompressionTypeAuto)
	setupMocksXmem(xmem, utils, throttler, remoteClusterSvc, colManSvc, evtProducer)

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

	utilsNotUsed, settings, xmem, router, throttler, remoteClusterSvc, colManSvc, eventProducer := setupBoilerPlateXmem(bname)
	realUtils := utilsReal.NewUtilities()
	xmem.utils = realUtils

	setupMocksXmem(xmem, utilsNotUsed, throttler, remoteClusterSvc, colManSvc, eventProducer)

	// Need to find the actual running targetBucketUUID
	bucketInfo, err := realUtils.GetBucketInfo(connString, bname, username, password, base.HttpAuthMechPlain, nil, false, nil, nil, xmem.Logger())
	assert.Nil(err)
	uuid, ok := bucketInfo["uuid"].(string)
	assert.True(ok)
	xmem.targetBucketUuid = uuid
	settings[SETTING_COMPRESSION_TYPE] = base.CompressionTypeSnappy
	settings[ForceCollectionDisableKey] = true
	err = xmem.Start(settings)
	assert.Nil(err)

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

	utils, settings, xmem, _, throttler, remoteClusterSvc, colManSvc, eventProducer := setupBoilerPlateXmem(xmemBucket, 3)
	setupMocksXmem(xmem, utils, throttler, remoteClusterSvc, colManSvc, eventProducer)

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
