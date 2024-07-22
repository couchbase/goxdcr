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
	"errors"
	"fmt"
	"testing"
	"time"

	mc "github.com/couchbase/gomemcached"
	mcReal "github.com/couchbase/gomemcached/client"
	mcMock "github.com/couchbase/gomemcached/client/mocks"
	"github.com/couchbase/goxdcr/base"
	common "github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	service_def "github.com/couchbase/goxdcr/service_def/mocks"
	utilsReal "github.com/couchbase/goxdcr/utils"
	utilsMock "github.com/couchbase/goxdcr/utils/mocks"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
)

func setupBoilerPlate() (*service_def.XDCRCompTopologySvc,
	*utilsMock.UtilsIface,
	*DcpNozzle,
	map[string]interface{},
	*mcMock.ClientIface,
	*mcMock.UprFeedIface,
	*base.VBTimestamp) {

	xdcrTopologyMock := &service_def.XDCRCompTopologySvc{}
	utilitiesMock := &utilsMock.UtilsIface{}
	clientIface := &mcMock.ClientIface{}
	uprfeedIface := &mcMock.UprFeedIface{}
	vblist := make([]uint16, 0, 1024)
	vblist = append(vblist, 0)
	vblist = append(vblist, 1)
	vblist = append(vblist, 5)
	nozzle := NewDcpNozzle("testNozzle", "source", "target", vblist, xdcrTopologyMock,
		false, log.DefaultLoggerContext, utilitiesMock, nil /*specificManifestGetter*/)

	// base VBTimeStamp
	vbTimestamp := &base.VBTimestamp{Vbno: 0, Seqno: 1000}
	vbReturner := func(uint16, uint64) (*base.VBTimestamp, error) {
		return vbTimestamp, nil
	}

	// settings map
	settingsMap := make(map[string]interface{})
	settingsMap[DCP_VBTimestampUpdater] = vbReturner

	// statsInterval needs to be fed something
	settingsMap[DCP_Nozzle_Stats_Interval] = 88888888

	// Enable compression by default
	settingsMap[SETTING_COMPRESSION_TYPE] = (base.CompressionType)(base.CompressionTypeSnappy)

	return xdcrTopologyMock, utilitiesMock, nozzle, settingsMap, clientIface, uprfeedIface, vbTimestamp
}

func setupUprFeedGeneric(uprFeed *mcMock.UprFeedIface) {
	uprFeed.On("StartFeedWithConfig", mock.Anything).Return(nil)
	uprFeed.On("IncrementAckBytes", mock.Anything).Return(nil)
	uprFeed.On("UprRequestStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).Return(nil)
	uprFeed.On("UprRequestCollectionsStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	uprFeed.On("Close").Return(nil)
	uprFeed.On("ClientAck", mock.Anything).Return(nil)
}

func setupUprFeedMock(uprFeed *mcMock.UprFeedIface) {
	var allFeaturesActivated mcReal.UprFeatures
	allFeaturesActivated.Xattribute = true
	allFeaturesActivated.CompressionType = base.CompressionTypeSnappy
	uprFeed.On("UprOpenWithXATTR", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	uprFeed.On("UprOpenWithFeatures", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, allFeaturesActivated)
	setupUprFeedGeneric(uprFeed)
}

func setupUprFeedMockFeatureNeg(uprFeed *mcMock.UprFeedIface, returnedFeatures mcReal.UprFeatures) {
	var dummyErr error = errors.New("Dummy")

	uprFeed.On("UprOpenWithFeatures", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(dummyErr, returnedFeatures)
	uprFeed.On("UprRequestCollectionsStream", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(dummyErr)
	setupUprFeedGeneric(uprFeed)
}

func setupUprFeedMockData(uprFeed *mcMock.UprFeedIface) chan *mcReal.UprEvent {
	eventCh := make(chan *mcReal.UprEvent, 1)
	uprFeedChanWrapper(uprFeed, eventCh)
	return eventCh
}

func feedEventToReceiver(eventCh chan *mcReal.UprEvent, event *mcReal.UprEvent) {
	eventCh <- event
}

// Use a UPR feed wrapper so that we can return a "Read-Only" channel from a bidirectional chan
func uprFeedChanWrapper(uprFeed *mcMock.UprFeedIface, eventCh <-chan *mcReal.UprEvent) {
	uprFeed.On("GetUprEventCh").Return(eventCh)
	uprFeed.On("Close").Return(nil)
	uprFeed.On("ClientAck", mock.Anything).Return(nil)
}

func setupMocksWithTs(xdcrTopology *service_def.XDCRCompTopologySvc,
	utils *utilsMock.UtilsIface,
	nozzle *DcpNozzle,
	settings metadata.ReplicationSettingsMap,
	mcClient *mcMock.ClientIface,
	uprFeed *mcMock.UprFeedIface,
	vbTs *base.VBTimestamp) {

	nozzle.vbtimestamp_updater = settings[DCP_VBTimestampUpdater].(func(uint16, uint64) (*base.VBTimestamp, error))

	setupMocks(xdcrTopology, utils, nozzle, settings, mcClient, uprFeed)

}

func setupMocks(xdcrTopology *service_def.XDCRCompTopologySvc,
	utils *utilsMock.UtilsIface,
	nozzle *DcpNozzle,
	settings metadata.ReplicationSettingsMap,
	mcClient *mcMock.ClientIface,
	uprFeed *mcMock.UprFeedIface) {

	// Turn compression on
	var features utilsReal.HELOFeatures
	features.CompressionType = base.CompressionTypeSnappy
	features.Collections = true

	setupMocksInternal(xdcrTopology, utils, nozzle, settings, mcClient, uprFeed, features)
}

func setupMocksInternal(xdcrTopology *service_def.XDCRCompTopologySvc,
	utils *utilsMock.UtilsIface,
	nozzle *DcpNozzle,
	settings metadata.ReplicationSettingsMap,
	mcClient *mcMock.ClientIface,
	uprFeed *mcMock.UprFeedIface,
	featureSet utilsReal.HELOFeatures) {

	// xdcr topology mock
	xdcrTopology.On("MyMemcachedAddr").Return("localhost", nil)

	// utils mock
	utils.On("ValidateSettings", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	utils.On("GetMemcachedConnectionWFeatures", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mcClient, featureSet, nil)
	utils.On("RecoverPanic", mock.Anything).Return(nil)

	// client mock
	mcClient.On("NewUprFeedWithConfigIface", mock.Anything, mock.Anything).Return(uprFeed, nil)
	mcClient.On("Close").Return(nil)
	uprFeed.On("Closed").Return(nozzle.bOpen)

	// UprMock
	nozzle.uprFeed = uprFeed
}

func generateUprEvent(opcode mc.CommandCode, status mc.Status, vbno uint16, opaque uint16) *mcReal.UprEvent {
	event := &mcReal.UprEvent{
		Opcode:  opcode,
		Status:  status,
		VBucket: vbno,
		Opaque:  opaque,
		Value:   make([]byte, 8, 8),
	}
	return event
}

func TestStartNozzle(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestStartStopDCPNozzle =================")
	xdcrTopology, utils, nozzle, settings, mcc, upr, _ := setupBoilerPlate()
	setupUprFeedMock(upr)
	// Test a success data coming back
	setupUprFeedMockData(upr)
	setupMocks(xdcrTopology, utils, nozzle, settings, mcc, upr)

	assert.Nil(nozzle.Start(settings))
	assert.Equal(nozzle.State(), common.Part_Running)

	// Give it some async
	time.Sleep(time.Duration(250) * time.Millisecond)

	assert.Nil(nozzle.Stop())
	assert.Equal(nozzle.State(), common.Part_Stopped)
	fmt.Println("============== Test case end: TestStartStopDCPNozzle =================")
}

func TestStartUPRStreams(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestStartUPRStreams =================")
	xdcrTopology, utils, nozzle, settings, mcc, upr, _ := setupBoilerPlate()

	// base VBTimeStamp
	var vbno uint16 = 0
	vbTimestamp := &base.VBTimestamp{Vbno: vbno}

	setupUprFeedMock(upr)
	// Test a success data coming back
	setupUprFeedMockData(upr)
	setupMocks(xdcrTopology, utils, nozzle, settings, mcc, upr)

	assert.Nil(nozzle.Start(settings))

	assert.Nil(nozzle.startUprStream(vbno, vbTimestamp))

	// Sent 1, so should have 1 in map
	assert.Equal(nozzle.vbHandshakeMap[vbno].getNumberOfOutstandingReqs(), 1)

	fmt.Println("============== Test case end: TestStartUPRStreams =================")
}

func TestSendAndReceiveSuccess(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestSendAndReceiveSuccess =================")
	xdcrTopology, utils, nozzle, settings, mcc, upr, _ := setupBoilerPlate()
	setupUprFeedMock(upr)

	/**
	 * This test simulates the following:
	 * 1. DCP -> UPR - start with seqno: A version 1
	 * 2. UPR -> DCP - SUCCESS with seqno: A version 1
	 * 3. UPR -> DCP - ROLLBACK with with a random version. Should ignore.
	 */

	// Use a opaque we can control - simulate UPR passing back the same opaque to us
	var vbno uint16 = 0
	version := uint16(1)
	// Test a success data coming back
	successEvent := generateUprEvent(mc.UPR_STREAMREQ, mc.SUCCESS, vbno, version)
	eventCh := setupUprFeedMockData(upr)
	setupMocks(xdcrTopology, utils, nozzle, settings, mcc, upr)

	assert.Nil(nozzle.Start(settings))
	time.Sleep(time.Duration(250) * time.Millisecond)
	assert.Equal(nozzle.vbHandshakeMap[vbno].getNumberOfOutstandingReqs(), 0)

	// base VBTimeStamp
	vbTimestamp := &base.VBTimestamp{Vbno: vbno}

	assert.Nil(nozzle.startUprStreamInner(vbno, vbTimestamp, version))
	assert.Equal(nozzle.vbHandshakeMap[vbno].getNumberOfOutstandingReqs(), 1)

	feedEventToReceiver(eventCh, successEvent)

	// Async takes some time
	time.Sleep(time.Duration(250) * time.Millisecond)

	// Sent one should have removed one
	assert.Equal(nozzle.vbHandshakeMap[vbno].getNumberOfOutstandingReqs(), 0)

	version++
	rollbackEvent := generateUprEvent(mc.UPR_STREAMREQ, mc.ROLLBACK, vbno, version)
	feedEventToReceiver(eventCh, rollbackEvent)

	// Async takes some time
	time.Sleep(time.Duration(250) * time.Millisecond)

	// Should not panic
	assert.Equal(nozzle.vbHandshakeMap[vbno].getNumberOfOutstandingReqs(), 0)

	fmt.Println("============== Test case end: TestSendAndReceiveSuccess =================")
}

func TestSuccessThenIgnoreRollback(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestSuccessThenIgnoreRollback =================")
	xdcrTopology, utils, nozzle, settings, mcc, upr, vbts := setupBoilerPlate()
	setupUprFeedMock(upr)

	/**
	 * This test simulates the following:
	 * 1. DCP -> UPR - start with seqno: A version 1
	 * 2. DCP -> UPR - start again with seqno: A version 2
	 * 3. UPR -> DCP - SUCCESS with version 1
	 * 4. UPR -> DCP - ROLLBACK with version 2 - should ignore
	 */

	// Use a opaque we can control - simulate UPR passing back the same opaque to us
	version := uint16(1)
	var vbno uint16 = 0
	successEvent := generateUprEvent(mc.UPR_STREAMREQ, mc.SUCCESS, vbno, version)
	eventCh := setupUprFeedMockData(upr)
	setupUprFeedMockData(upr)
	setupMocks(xdcrTopology, utils, nozzle, settings, mcc, upr)

	assert.Nil(nozzle.Start(settings))
	time.Sleep(time.Duration(250) * time.Millisecond)
	assert.Equal(nozzle.vbHandshakeMap[vbno].getNumberOfOutstandingReqs(), 0)

	vbTimestamp := vbts
	vbTimestamp.Seqno = 999

	// Before starting, call a getVersion to bump the next version in queue to 2
	assert.Equal(nozzle.vbHandshakeMap[vbno].getNewVersion(), uint16(1))

	// step 1
	assert.Nil(nozzle.startUprStreamInner(vbno, vbTimestamp, version))
	assert.Equal(nozzle.vbHandshakeMap[vbno].getNumberOfOutstandingReqs(), 1)

	// newer version for step 2
	version2 := nozzle.vbHandshakeMap[vbno].getNewVersion()
	assert.Equal(version2, uint16(2))
	rollbackEvent := generateUprEvent(mc.UPR_STREAMREQ, mc.ROLLBACK, vbno, version2)
	// step 2
	assert.Nil(nozzle.startUprStreamInner(vbno, vbTimestamp, version2))
	assert.Equal(nozzle.vbHandshakeMap[vbno].getNumberOfOutstandingReqs(), 2)

	// step 3
	feedEventToReceiver(eventCh, successEvent)
	time.Sleep(time.Duration(250) * time.Millisecond)
	assert.Equal(nozzle.vbHandshakeMap[vbno].getNumberOfOutstandingReqs(), 0)

	// After a success event, the number well should have reset
	assert.Equal(nozzle.vbHandshakeMap[vbno].getNewVersion(), uint16(1))

	// step 4
	feedEventToReceiver(eventCh, rollbackEvent)
	time.Sleep(time.Duration(250) * time.Millisecond)
	// Should ignore the request but actually do the book-keeping
	assert.Equal(nozzle.vbHandshakeMap[vbno].getNumberOfOutstandingReqs(), 0)

	fmt.Println("============== Test case end: TestSuccessThenIgnoreRollback =================")
}

func TestSendAndReceiveUnhandledError(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestSendAndReceiveUnhandledError =================")
	xdcrTopology, utils, nozzle, settings, mcc, upr, _ := setupBoilerPlate()
	setupUprFeedMock(upr)

	/**
	 * This test simulates the following:
	 * 1. DCP -> UPR - start with seqno: A version 1
	 * 2. UPR -> DCP - UNKNOWN_COMMAND with seqno: A version 1
	 */

	// Use a opaque we can control - simulate UPR passing back the same opaque to us
	version := uint16(1)
	var vbno uint16 = 0
	// Test a success data coming back
	failureEvent := generateUprEvent(mc.UPR_STREAMREQ, mc.UNKNOWN_COMMAND, vbno, version)
	eventCh := setupUprFeedMockData(upr)
	setupMocks(xdcrTopology, utils, nozzle, settings, mcc, upr)

	assert.Nil(nozzle.Start(settings))
	time.Sleep(time.Duration(250) * time.Millisecond)
	assert.Equal(nozzle.vbHandshakeMap[vbno].getNumberOfOutstandingReqs(), 0)

	// base VBTimeStamp
	vbTimestamp := &base.VBTimestamp{Vbno: vbno}

	assert.Nil(nozzle.startUprStreamInner(vbno, vbTimestamp, version))
	assert.Equal(nozzle.vbHandshakeMap[vbno].getNumberOfOutstandingReqs(), 1)

	feedEventToReceiver(eventCh, failureEvent)

	// Async takes some time
	time.Sleep(time.Duration(250) * time.Millisecond)

	// Sent one should have not removed one
	assert.Equal(nozzle.vbHandshakeMap[vbno].getNumberOfOutstandingReqs(), 1)

	fmt.Println("============== Test case end: TestSendAndReceiveUnhandledError =================")
}

func TestFeatureInitializationError(t *testing.T) {
	assert := assert.New(t)
	var noFeaturesActivated mcReal.UprFeatures
	noFeaturesActivated.CompressionType = base.CompressionTypeNone
	fmt.Println("============== Test case start: TestFeatureInitializationError =================")
	xdcrTopology, utils, nozzle, settings, mcc, upr, _ := setupBoilerPlate()
	setupMocks(xdcrTopology, utils, nozzle, settings, mcc, upr)
	setupUprFeedMockFeatureNeg(upr, noFeaturesActivated)

	assert.Equal(base.ErrorCompressionNotSupported, nozzle.initialize(settings))

	fmt.Println("============== Test case end: TestFeatureInitializationError =================")
}

func TestNonFeatureInitializationError(t *testing.T) {
	assert := assert.New(t)
	var allFeaturesActivated mcReal.UprFeatures
	allFeaturesActivated.Xattribute = true
	allFeaturesActivated.CompressionType = base.CompressionTypeSnappy
	fmt.Println("============== Test case start: TestNonFeatureInitializationError =================")
	xdcrTopology, utils, nozzle, settings, mcc, upr, _ := setupBoilerPlate()
	setupMocks(xdcrTopology, utils, nozzle, settings, mcc, upr)
	setupUprFeedMockFeatureNeg(upr, allFeaturesActivated)

	assert.NotEqual(base.ErrorCompressionNotSupported, nozzle.initialize(settings))

	fmt.Println("============== Test case end: TestNonFeatureInitializationError =================")
}

func TestMcCompressionError(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestMcCompressionError =================")
	var noFeatures utilsReal.HELOFeatures
	noFeatures.CompressionType = base.CompressionTypeNone
	xdcrTopology, utils, nozzle, settings, mcc, upr, _ := setupBoilerPlate()
	setupMocksInternal(xdcrTopology, utils, nozzle, settings, mcc, upr, noFeatures)

	assert.Equal(base.ErrorCompressionNotSupported, nozzle.initialize(settings))
	fmt.Println("============== Test case end: TestMcCompressionError =================")
}

func TestNonFeatureInitializationErrorCompressedWhenNotRequested(t *testing.T) {
	assert := assert.New(t)
	var allFeaturesActivated mcReal.UprFeatures
	allFeaturesActivated.Xattribute = true
	allFeaturesActivated.CompressionType = base.CompressionTypeSnappy
	fmt.Println("============== Test case start: TestNonFeatureInitializationErrorCompressedWhenNotRequested =================")
	xdcrTopology, utils, nozzle, settings, mcc, upr, _ := setupBoilerPlate()
	setupMocks(xdcrTopology, utils, nozzle, settings, mcc, upr)
	setupUprFeedMockFeatureNeg(upr, allFeaturesActivated)

	settings[SETTING_COMPRESSION_TYPE] = (base.CompressionType)(base.CompressionTypeNone)
	assert.Equal(base.ErrorCompressionDcpInvalidHandshake, nozzle.initialize(settings))

	fmt.Println("============== Test case end: TestNonFeatureInitializationErrorCompressedWhenNotRequested =================")
}

// AUTO is no longer a supported value. XDCR Factory should have passed in a non-auto
func TestStartNozzleAuto(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestStartStopDCPNozzleAuto =================")
	xdcrTopology, utils, nozzle, settings, mcc, upr, _ := setupBoilerPlate()
	setupUprFeedMock(upr)
	// Test a success data coming back
	setupUprFeedMockData(upr)
	setupMocks(xdcrTopology, utils, nozzle, settings, mcc, upr)

	settings[SETTING_COMPRESSION_TYPE] = (base.CompressionType)(base.CompressionTypeAuto)

	assert.NotNil(nozzle.Start(settings))
	assert.NotEqual(nozzle.State(), common.Part_Running)
	fmt.Println("============== Test case end: TestStartStopDCPNozzleAuto =================")
}

func TestUprFeedNil(t *testing.T) {
	fmt.Println("============== Test case start: TestUprFeedNil =================")
	assert := assert.New(t)
	xdcrTopology, utils, nozzle, settings, mcc, upr, _ := setupBoilerPlate()
	setupUprFeedMock(upr)
	// Test a success data coming back
	setupUprFeedMockData(upr)
	setupMocks(xdcrTopology, utils, nozzle, settings, mcc, upr)

	settings[SETTING_COMPRESSION_TYPE] = (base.CompressionType)(base.CompressionTypeAuto)

	assert.NotNil(nozzle.Start(settings))
	assert.NotNil(nozzle.getUprFeed())

	assert.Nil(nozzle.Stop())
	assert.Nil(nozzle.getUprFeed())
	fmt.Println("============== Test case end: TestUprFeedNil =================")
}
