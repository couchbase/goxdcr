//go:build !pcre
// +build !pcre

/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package parts

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/base/filter"
	"github.com/couchbase/goxdcr/v8/common"
	component "github.com/couchbase/goxdcr/v8/component"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	service_def_mocks "github.com/couchbase/goxdcr/v8/service_def/mocks"
	utilities "github.com/couchbase/goxdcr/v8/utils"
	UtilitiesMock "github.com/couchbase/goxdcr/v8/utils/mocks"
	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
)

var dummyDownStream string = "dummy"

var collectionsCap = metadata.UnitTestGetCollectionsCapability()
var nonCollectionsCap = metadata.UnitTestGetDefaultCapability()

// func setupBoilerPlateRouter() (routerId string, downStreamParts map[string]common.Part, routingMap map[uint16]string, crMode base.ConflictResolutionMode, loggerCtx *log.LoggerContext, utilsMock utilities.UtilsIface, throughputThrottlerSvc *service_def_mocks.ThroughputThrottlerSvc, needToThrottle bool, expDelMode base.FilterExpDelType, collectionsManifestSvc *service_def_mocks.CollectionsManifestSvc, spec *metadata.ReplicationSpecification, dcpRecycler utilities.RecycleObjFunc) {
func RetrieveUprFile(fileName string) (*mcc.UprEvent, error) {
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	var uprEvent mcc.UprEvent
	err = json.Unmarshal(data, &uprEvent)
	if err != nil {
		return nil, err
	}

	return &uprEvent, nil
}

func setupBoilerPlateRouter() (routerId string, downStreamParts map[string]common.Part, routingMap map[uint16]string, crMode base.ConflictResolutionMode, loggerCtx *log.LoggerContext, utilsMock *UtilitiesMock.UtilsIface, throughputThrottlerSvc *service_def_mocks.ThroughputThrottlerSvc, needToThrottle bool, expDelMode base.FilterExpDelType, collectionsManifestSvc *service_def_mocks.CollectionsManifestSvc, spec *metadata.ReplicationSpecification, dcpRecycler utilities.RecycleObjFunc, connectivityStatusGetter func() (metadata.ConnectivityStatus, error)) {
	routerId = "routerUnitTest"

	downStreamParts = make(map[string]common.Part)
	downStreamParts[dummyDownStream] = nil
	routingMap = make(map[uint16]string)
	crMode = base.CRMode_RevId
	loggerCtx = log.DefaultLoggerContext
	utilsMock = &UtilitiesMock.UtilsIface{}
	utilsMock.On("NewDataPool").Return(base.NewFakeDataPool())
	throughputThrottlerSvc = &service_def_mocks.ThroughputThrottlerSvc{}
	throughputThrottlerSvc.On("CanSend", mock.Anything).Return(true)
	needToThrottle = false
	expDelMode = base.FilterExpDelNone
	collectionsManifestSvc = &service_def_mocks.CollectionsManifestSvc{}
	spec, _ = metadata.NewReplicationSpecification("srcBucket", "srcBucketUUID", "targetClusterUUID", "tgtBucket", "tgtBucketUUID")
	dcpRecycler = func(obj interface{}) {
		// Do nothing
	}

	// Happy
	connectivityStatusGetter = func() (metadata.ConnectivityStatus, error) {
		return metadata.ConnValid, nil
	}
	return
}

func setupCollectionManifestsSvcRouter(collectionsManifestSvc *service_def_mocks.CollectionsManifestSvc) (pair metadata.CollectionsManifestPair) {
	manifestFileDir := "../metadata/testdata"
	manifestFileName := "provisionedManifest.json"

	data, err := ioutil.ReadFile(fmt.Sprintf("%v/%v", manifestFileDir, manifestFileName))
	if err != nil {
		panic(err.Error())
	}
	manifest, err := metadata.NewCollectionsManifestFromBytes(data)
	if err != nil {
		panic(err.Error())
	}
	collectionsManifestSvc.On("GetLatestManifests", mock.Anything, mock.Anything).Return(&manifest, &manifest, nil)
	pair.Source = &manifest
	pair.Target = &manifest
	return
}

func setupCollectionManifestsSvcRouterWithDefaultTarget(collectionsManifestSvc *service_def_mocks.CollectionsManifestSvc) {
	defaultManifest := metadata.NewDefaultCollectionsManifest()
	manifestFileDir := "../metadata/testdata"
	manifestFileName := "provisionedManifest.json"

	data, err := ioutil.ReadFile(fmt.Sprintf("%v/%v", manifestFileDir, manifestFileName))
	if err != nil {
		panic(err.Error())
	}
	manifest, err := metadata.NewCollectionsManifestFromBytes(data)
	if err != nil {
		panic(err.Error())
	}
	collectionsManifestSvc.On("GetLatestManifests", mock.Anything, mock.Anything).Return(&manifest, &defaultManifest, nil)
}

func setupCollectionManifestsSvcRouterWithSpecificDefaultTarget(collectionsManifestSvc *service_def_mocks.CollectionsManifestSvc) {
	defaultManifest := metadata.NewDefaultCollectionsManifest()
	manifestFileDir := "../metadata/testdata"
	manifestFileName := "provisionedManifest.json"

	data, err := ioutil.ReadFile(fmt.Sprintf("%v/%v", manifestFileDir, manifestFileName))
	if err != nil {
		panic(err.Error())
	}
	manifest, err := metadata.NewCollectionsManifestFromBytes(data)
	if err != nil {
		panic(err.Error())
	}
	collectionsManifestSvc.On("GetLatestManifests", mock.Anything, mock.Anything).Return(&manifest, &defaultManifest, nil)
	collectionsManifestSvc.On("GetSpecificTargetManifest", mock.Anything, mock.Anything).Return(&defaultManifest, nil)
}

func setupCollectionManifestsSvcRouterWithSpecificTarget(collectionsManifestSvc *service_def_mocks.CollectionsManifestSvc, version int) (pair metadata.CollectionsManifestPair) {
	manifestFileDir := "../metadata/testdata"
	manifestFileName := "provisionedManifest.json"

	if version == 2 {
		manifestFileName = "provisionedManifestv2.json"
	}

	data, err := ioutil.ReadFile(fmt.Sprintf("%v/%v", manifestFileDir, manifestFileName))
	if err != nil {
		panic(err.Error())
	}
	manifest, err := metadata.NewCollectionsManifestFromBytes(data)
	if err != nil {
		panic(err.Error())
	}
	collectionsManifestSvc.On("GetLatestManifests", mock.Anything, mock.Anything).Return(&manifest, &manifest, nil)
	collectionsManifestSvc.On("GetSpecificTargetManifest", mock.Anything, mock.Anything).Return(&manifest, nil)
	collectionsManifestSvc.On("ForceTargetManifestRefresh", mock.Anything).Return(nil)
	pair.Source = &manifest
	pair.Target = &manifest
	return
}

func TestRouterRouteFunc(t *testing.T) {
	fmt.Println("============== Test case start: TestRouterRouteFunc =================")
	assert := assert.New(t)

	routerId, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, spec, recycler, connectivityStatus := setupBoilerPlateRouter()
	router, err := NewRouter(routerId, spec, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, recycler, nil, nonCollectionsCap, nil, connectivityStatus, nil, nil)

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

	// Test route flagged upr
	flaggedUpr := &base.WrappedUprEvent{UprEvent: uprEvent}
	flaggedUpr.Flags.SetCollectionDNE()
	router.routingMap[uprEvent.VBucket] = "dummyPartId"
	result, err := router.Route(flaggedUpr)
	assert.Equal(0, len(result))
	assert.Nil(err)
	fmt.Println("============== Test case end: TestRouterRouteFunc =================")
}

func TestRouterInitialNone(t *testing.T) {
	fmt.Println("============== Test case start: TestRouterInitialNone =================")
	assert := assert.New(t)

	routerId, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, spec, recycler, connectivityStatus := setupBoilerPlateRouter()
	router, err := NewRouter(routerId, spec, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, recycler, nil, nonCollectionsCap, nil, connectivityStatus, nil, nil)

	assert.Nil(err)
	assert.NotNil(router)

	assert.Equal(base.FilterExpDelNone, router.expDelMode.Get())

	fmt.Println("============== Test case end: TestRouterInitialNone =================")
}

func TestRouterSkipDeletion(t *testing.T) {
	fmt.Println("============== Test case start: TestRouterSkipDeletion =================")
	assert := assert.New(t)

	routerId, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, spec, recycler, connectivityStatus := setupBoilerPlateRouter()
	expDelMode = base.FilterExpDelSkipDeletes
	router, err := NewRouter(routerId, spec, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, recycler, nil, nonCollectionsCap, nil, connectivityStatus, nil, nil)

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

	routerId, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, spec, recycler, connectivityStatus := setupBoilerPlateRouter()
	expDelMode = base.FilterExpDelSkipExpiration
	router, err := NewRouter(routerId, spec, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, recycler, nil, nonCollectionsCap, nil, connectivityStatus, nil, nil)

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

func TestRouterSkipExpiry(t *testing.T) {
	fmt.Println("============== Test case start: TestRouterSkipExpiry =================")
	assert := assert.New(t)

	routerId, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, spec, _, _ := setupBoilerPlateRouter()

	expDelMode = base.FilterExpDelSkipExpiration

	router, err := NewRouter(routerId, spec, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, nil, nil, nonCollectionsCap, nil, nil, nil, nil)

	assert.Nil(err)
	assert.NotNil(router)

	assert.NotEqual(base.FilterExpDelNone, router.expDelMode.Get())

	mutEvent, err := RetrieveUprFile("./testdata/mutationTypes/noXattrs.json")
	assert.Nil(err)
	assert.NotNil(mutEvent)
	shouldContinue := router.ProcessExpDelTTL(mutEvent)
	assert.True(shouldContinue)

	delEvent, err := RetrieveUprFile("./testdata/uprEventDeletion.json")
	assert.Nil(err)
	assert.NotNil(delEvent)
	shouldContinue = router.ProcessExpDelTTL(delEvent)
	assert.True(shouldContinue)

	expEvent, err := RetrieveUprFile("./testdata/uprEventExpiration.json")
	assert.Nil(err)
	assert.NotNil(expEvent)
	shouldContinue = router.ProcessExpDelTTL(expEvent)
	assert.False(shouldContinue)

	fmt.Println("============== Test case end: TestRouterSkipExpiry =================")
}

func TestRouterStripTTL(t *testing.T) {
	fmt.Println("============== Test case start: TestRouterStripTTL =================")
	assert := assert.New(t)

	routerId, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, spec, _, _ := setupBoilerPlateRouter()

	expDelMode = base.FilterExpDelStripExpiration

	router, err := NewRouter(routerId, spec, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, nil, nil, nonCollectionsCap, nil, nil, nil, nil)

	assert.Nil(err)
	assert.NotNil(router)

	assert.NotEqual(base.FilterExpDelNone, router.expDelMode.Get())

	mutEvent, err := RetrieveUprFile("./testdata/mutationTypes/noXattrs.json")
	assert.Nil(err)
	assert.NotNil(mutEvent)
	assert.NotEqual(0, int(mutEvent.Expiry))
	shouldContinue := router.ProcessExpDelTTL(mutEvent)
	assert.True(shouldContinue)
	assert.Equal(0, int(mutEvent.Expiry))

	delEvent, err := RetrieveUprFile("./testdata/uprEventDeletion.json")
	assert.Nil(err)
	assert.NotNil(delEvent)
	assert.NotEqual(0, int(delEvent.Expiry))
	expiryBefore := int(delEvent.Expiry)
	shouldContinue = router.ProcessExpDelTTL(delEvent)
	assert.True(shouldContinue)
	assert.NotEqual(0, int(delEvent.Expiry))
	assert.Equal(expiryBefore, int(delEvent.Expiry))

	expEvent, err := RetrieveUprFile("./testdata/uprEventExpiration.json")
	assert.Nil(err)
	assert.NotNil(expEvent)
	assert.NotEqual(0, int(expEvent.Expiry))
	expiryBefore = int(expEvent.Expiry)
	shouldContinue = router.ProcessExpDelTTL(expEvent)
	assert.True(shouldContinue)
	assert.NotEqual(0, int(expEvent.Expiry))
	assert.Equal(expiryBefore, int(expEvent.Expiry))

	fmt.Println("============== Test case end: TestRouterStripTTL =================")
}

func TestRouterExpDelAllMode(t *testing.T) {
	fmt.Println("============== Test case start: TestRouterExpDelAllMode =================")
	assert := assert.New(t)

	routerId, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, spec, recycler, connectivityStatus := setupBoilerPlateRouter()
	expDelMode = base.FilterExpDelAllFiltered
	router, err := NewRouter(routerId, spec, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, recycler, nil, nonCollectionsCap, nil, connectivityStatus, nil, nil)

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

var testDir string = "../metadata/testData/"

var targetv8 string = testDir + "diffTargetv8.json"
var targetv9 string = testDir + "diffTargetv9.json"

var filterPath = "../base/filter/testData/"
var bigMutationFile = filterPath + "edgyMB-33583.json"

func TestRouterManifestChange(t *testing.T) {
	fmt.Println("============== Test case start: TestRouterManifestChange =================")
	assert := assert.New(t)

	routerId, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, spec, _, connectivity := setupBoilerPlateRouter()

	setupCollectionManifestsSvcRouter(collectionsManifestSvc)

	expDelMode = base.FilterExpDelAllFiltered

	router, err := NewRouter(routerId, spec, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, nil, nil, collectionsCap, nil, connectivity, nil, nil)

	assert.Nil(err)
	assert.NotNil(router)

	assert.Nil(router.Start())
	collectionsRouter := router.collectionsRouting[dummyDownStream]
	assert.NotNil(collectionsRouter)

	// routing updater receiver
	newRoutingUpdater := func(info CollectionsRoutingInfo) error {
		// This test will show no broken map but one fixed map
		assert.Equal(0, len(info.BrokenMap))
		assert.Equal(1, len(info.BackfillMap))
		return nil
	}
	collectionsRouter.routingUpdater = newRoutingUpdater

	data, _ := ioutil.ReadFile(targetv8)
	targetv8Manifest, _ := metadata.NewCollectionsManifestFromBytes(data)

	data, _ = ioutil.ReadFile(targetv9)
	targetv9Manifest, _ := metadata.NewCollectionsManifestFromBytes(data)

	assert.Nil(collectionsRouter.handleNewTgtManifestChanges(&targetv8Manifest))
	// Force a manual brokenmap. V9 will have the following fixed
	implicitNamespace := &base.CollectionNamespace{ScopeName: "S2", CollectionName: "col3"}
	collectionsRouter.brokenDenyMtx.Lock()
	collectionsRouter.brokenMapping.AddSingleMapping(implicitNamespace, implicitNamespace)
	collectionsRouter.brokenDenyMtx.Unlock()

	assert.Nil(collectionsRouter.handleNewTgtManifestChanges(&targetv9Manifest))

	collectionsRouter.brokenDenyMtx.Lock()
	assert.Equal(0, len(collectionsRouter.brokenMapping))
	collectionsRouter.brokenDenyMtx.Unlock()

	fmt.Println("============== Test case end: TestRouterManifestChange =================")
}

func mockTargetCollectionDNE(collectionsManifestSvc *service_def_mocks.CollectionsManifestSvc) {
	// Target DNE simply means an empty manifest
	defaultManifest := metadata.NewDefaultCollectionsManifest()
	setupCollectionManifestsSvcRouter(collectionsManifestSvc)
	collectionsManifestSvc.On("GetSpecificTargetManifest", mock.Anything, mock.Anything).Return(&defaultManifest, nil)
}

func TestRouterTargetCollectionDNE(t *testing.T) {
	fmt.Println("============== Test case start: TargetCollectionDNE =================")
	assert := assert.New(t)

	routerId, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, spec, _, connectivity := setupBoilerPlateRouter()

	expDelMode = base.FilterExpDelAllFiltered

	mockTargetCollectionDNE(collectionsManifestSvc)

	router, err := NewRouter(routerId, spec, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, nil, nil, collectionsCap, nil, connectivity, nil, nil)

	assert.Nil(err)
	assert.NotNil(router)

	assert.Nil(router.Start())
	collectionsRouter := router.collectionsRouting[dummyDownStream]
	assert.NotNil(collectionsRouter)

	// routing updater receiver
	newRoutingUpdater := func(info CollectionsRoutingInfo) error {
		// This test will show one broken map and no fixed map
		assert.Equal(1, len(info.BrokenMap))
		assert.Equal(0, len(info.BackfillMap))
		// For now say persist is fine
		return nil
	}

	var ignoreCnt int
	ignoreFunc := func(*base.WrappedMCRequest) {
		ignoreCnt++
	}

	collectionsRouter.routingUpdater = newRoutingUpdater
	collectionsRouter.ignoreDataFunc = ignoreFunc

	mcReq := getMutationMCReq(err, assert)

	implicitNamespace := &base.CollectionNamespace{ScopeName: "S2", CollectionName: "col3"}
	dummyData := &base.WrappedMCRequest{
		SrcColNamespace: implicitNamespace,
		Req:             mcReq,
		// use 123 to indicate a colID that tgt manifest doesn't have
		ColInfo: &base.TargetCollectionInfo{ColId: 123},
	}

	assert.Equal(base.ErrorIgnoreRequest, router.RouteCollection(dummyData, dummyDownStream, nil))
	assert.Equal(1, ignoreCnt)
	fmt.Println("============== Test case end: TargetCollectionDNE =================")
}

func getMutationMCReq(err error, assert *assert.Assertions) *gomemcached.MCRequest {
	uprEvent, err := RetrieveUprFile(bigMutationFile)
	assert.Nil(err)
	mcReq := &gomemcached.MCRequest{
		Key:    uprEvent.Key,
		Keylen: len(uprEvent.Key),
	}
	return mcReq
}

func TestRouterTargetCollectionDNEPersistErr(t *testing.T) {
	fmt.Println("============== Test case start: TargetCollectionDNEPersistErr =================")
	assert := assert.New(t)

	routerId, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, spec, _, connectivity := setupBoilerPlateRouter()

	expDelMode = base.FilterExpDelAllFiltered

	mockTargetCollectionDNE(collectionsManifestSvc)

	router, err := NewRouter(routerId, spec, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, nil, nil, collectionsCap, nil, connectivity, nil, nil)

	assert.Nil(err)
	assert.NotNil(router)

	assert.Nil(router.Start())
	collectionsRouter := router.collectionsRouting[dummyDownStream]
	assert.NotNil(collectionsRouter)

	// routing updater receiver
	newRoutingUpdater := func(info CollectionsRoutingInfo) error {
		// This test will show one broken map and no fixed map
		assert.Equal(1, len(info.BrokenMap))
		assert.Equal(0, len(info.BackfillMap))
		// For now say persist is not fine
		return fmt.Errorf("Dummy persist err")
	}

	var ignoreCnt int
	ignoreFunc := func(*base.WrappedMCRequest) {
		ignoreCnt++
	}

	collectionsRouter.routingUpdater = newRoutingUpdater
	collectionsRouter.ignoreDataFunc = ignoreFunc

	mcReq := getMutationMCReq(err, assert)
	implicitNamespace := &base.CollectionNamespace{ScopeName: "S2", CollectionName: "col3"}
	dummyData := &base.WrappedMCRequest{
		SrcColNamespace: implicitNamespace,
		Req:             mcReq,
		ColInfo:         &base.TargetCollectionInfo{},
	}

	// Even if persist has problem, routeCollection should return a non-nil error to prevent forwarding to xmem
	assert.Equal(base.ErrorIgnoreRequest, router.RouteCollection(dummyData, dummyDownStream, nil))
	// The ignore count should be 0 to indicate that throughSeqno will not move foward
	assert.Equal(0, ignoreCnt)
	fmt.Println("============== Test case end: TargetCollectionDNEPersistErr =================")
}

func TestRouterExplicitMode(t *testing.T) {
	fmt.Println("============== Test case start: TestRouterExplicitMode =================")
	defer fmt.Println("============== Test case end: TestRouterExplicitMode =================")
	assert := assert.New(t)

	routerId, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, spec, _, _ := setupBoilerPlateRouter()

	mappingMode := spec.Settings.GetCollectionModes()
	mappingMode.SetExplicitMapping(true)

	rules := make(metadata.CollectionsMappingRulesType)
	rules["S1"] = "S2"
	updatedMap := make(map[string]interface{})
	updatedMap[metadata.CollectionsMgtMultiKey] = mappingMode
	updatedMap[metadata.CollectionsMappingRulesKey] = rules
	_, errMap := spec.Settings.UpdateSettingsFromMap(updatedMap)
	assert.Equal(0, len(errMap))

	setupCollectionManifestsSvcRouterWithDefaultTarget(collectionsManifestSvc)

	router, err := NewRouter(routerId, spec, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, nil, nil, collectionsCap, nil, nil, nil, nil)

	assert.Nil(err)
	modes := router.collectionModes.Get()
	assert.True(modes.IsExplicitMapping())

	collectionsRouter := router.collectionsRouting[dummyDownStream]
	assert.NotNil(collectionsRouter)
	assert.Nil(collectionsRouter.Start())
	assert.True(collectionsRouter.collectionMode.IsExplicitMapping())

	// routing updater receiver
	newRoutingUpdater := func(info CollectionsRoutingInfo) error {
		return nil
	}

	var ignoreCnt int
	ignoreFunc := func(*base.WrappedMCRequest) {
		ignoreCnt++
	}

	collectionsRouter.routingUpdater = newRoutingUpdater
	collectionsRouter.ignoreDataFunc = ignoreFunc

	sourceNs := &base.CollectionNamespace{ScopeName: "S1", CollectionName: "col1"}
	mcReq := &gomemcached.MCRequest{
		Key:    []byte("testKey"),
		Keylen: len("testKey"),
	}
	dummyData := &base.WrappedMCRequest{
		Req:             mcReq,
		SrcColNamespace: sourceNs,
		// use 123 to indicate a colID that tgt manifest doesn't have
		ColInfo: &base.TargetCollectionInfo{ColId: 123},
	}
	err = router.RouteCollection(dummyData, dummyDownStream, nil)
	// Right now target cluster gave back "default manifest"
	assert.Equal(base.ErrorIgnoreRequest, err)
	assert.Equal(1, ignoreCnt)
	assert.Equal(1, len(collectionsRouter.brokenMapping))
	// Second one should be already ignored
	err = router.RouteCollection(dummyData, dummyDownStream, nil)
	assert.Equal(base.ErrorRequestAlreadyIgnored, err)
	assert.Equal(2, ignoreCnt)
	assert.Equal(1, len(collectionsRouter.brokenMapping))

	collectionsManifestSvc = &service_def_mocks.CollectionsManifestSvc{}
	pair := setupCollectionManifestsSvcRouter(collectionsManifestSvc)
	collectionsRouter.collectionsManifestSvc = collectionsManifestSvc

	// TODO - change this to use actual update call
	collectionsRouter.explicitMappings, err = metadata.NewCollectionNamespaceMappingFromRules(pair, mappingMode, rules, false, false)
	assert.Nil(err)
	collectionsRouter.explicitMappingIdx = collectionsRouter.explicitMappings.CreateLookupIndex()

	err = router.RouteCollection(dummyData, dummyDownStream, nil)
	assert.Nil(err)
	//targetNs := &base.CollectionNamespace{"S2", "col1"}

	//// Even if persist has problem, routeCollection should return a non-nil error to prevent forwarding to xmem
	//assert.Equal(base.ErrorIgnoreRequest, router.RouteCollection(dummyData, dummyDownStream))
	//// The ignore count should be 0 to indicate that throughSeqno will not move foward
}

func TestRouterSpecialMigrationMode(t *testing.T) {
	fmt.Println("============== Test case start: TestRouterSpecialMigrationMode =================")
	defer fmt.Println("============== Test case end: TestRouterSpecialMigrationMode =================")
	assert := assert.New(t)

	routerId, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, spec, _, _ := setupBoilerPlateRouter()

	mappingMode := spec.Settings.GetCollectionModes()
	mappingMode.SetMigration(true)

	rules := make(metadata.CollectionsMappingRulesType)
	rules["_default._default"] = "S2.col1"
	updatedMap := make(map[string]interface{})
	updatedMap[metadata.CollectionsMgtMultiKey] = mappingMode
	updatedMap[metadata.CollectionsMappingRulesKey] = rules
	_, errMap := spec.Settings.UpdateSettingsFromMap(updatedMap)
	assert.Equal(0, len(errMap))

	setupCollectionManifestsSvcRouterWithDefaultTarget(collectionsManifestSvc)

	router, err := NewRouter(routerId, spec, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, nil, nil, collectionsCap, nil, nil, nil, nil)

	assert.Nil(err)
	modes := router.collectionModes.Get()
	assert.True(modes.IsMigrationOn())

	collectionsRouter := router.collectionsRouting[dummyDownStream]
	assert.NotNil(collectionsRouter)

	// Special migration mode will translate into a single regular explicit mapping
	assert.Nil(collectionsRouter.Start())
	collectionsRouter.mappingMtx.RLock()
	assert.False(collectionsRouter.collectionMode.IsMigrationOn())
	assert.True(collectionsRouter.collectionMode.IsExplicitMapping())
	collectionsRouter.mappingMtx.RUnlock()
}

func TestRouterImplicitWithDiffCapabilities(t *testing.T) {
	fmt.Println("============== Test case start: TestRouterImplicitWithDiffCapabilities =================")
	defer fmt.Println("============== Test case end: TestRouterImplicitWithDiffCapabilities =================")
	assert := assert.New(t)

	routerId, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, spec, _, _ := setupBoilerPlateRouter()
	setupCollectionManifestsSvcRouterWithSpecificTarget(collectionsManifestSvc, 1)

	expDelMode = base.FilterExpDelAllFiltered

	router, err := NewRouter(routerId, spec, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, nil, nil, collectionsCap, nil, nil, nil, nil)

	assert.Nil(err)
	assert.NotNil(router)

	assert.Nil(router.Start())
	collectionsRouter := router.collectionsRouting[dummyDownStream]
	assert.NotNil(collectionsRouter)

	// routing updater receiver
	newRoutingUpdater := func(info CollectionsRoutingInfo) error {
		// This test will show no broken map but one fixed map
		assert.Equal(0, len(info.BrokenMap))
		assert.Equal(1, len(info.BackfillMap))
		return nil
	}
	collectionsRouter.routingUpdater = newRoutingUpdater

	delEvent, err := RetrieveUprFile("./testdata/uprEventDeletion.json")
	delEvent2, _ := RetrieveUprFile("./testdata/uprEventDeletion.json")
	assert.Nil(err)
	assert.NotNil(delEvent)

	sourceNs := &base.CollectionNamespace{ScopeName: "S1", CollectionName: "col1"}
	mcReq := &gomemcached.MCRequest{
		Key:    delEvent.Key,
		Keylen: len(delEvent.Key),
	}
	dummyData := &base.WrappedMCRequest{
		Req:             mcReq,
		SrcColNamespace: sourceNs,
		ColInfo:         &base.TargetCollectionInfo{},
	}

	origKey := base.DeepCopyByteArray(delEvent.Key)
	err = router.RouteCollection(dummyData, dummyDownStream, nil)
	assert.Nil(err)
	assert.False(reflect.DeepEqual(origKey, dummyData.Req.Key))

	// Now try with no-collections capability - key should not be modified
	routerId, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, spec, _, _ = setupBoilerPlateRouter()
	setupCollectionManifestsSvcRouter(collectionsManifestSvc)
	router, err = NewRouter(routerId, spec, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, nil, nil, nonCollectionsCap, nil, nil, nil, nil)
	assert.Nil(err)
	assert.NotNil(router)
	assert.Nil(router.Start())
	collectionsRouter.routingUpdater = newRoutingUpdater

	assert.False(router.remoteClusterCapability.HasCollectionSupport())

	origKey = base.DeepCopyByteArray(delEvent2.Key)
	mcReq = &gomemcached.MCRequest{
		Key:    delEvent2.Key,
		Keylen: len(delEvent2.Key),
	}
	dummyData = &base.WrappedMCRequest{
		Req:             mcReq,
		SrcColNamespace: sourceNs,
		ColInfo:         &base.TargetCollectionInfo{},
	}
	err = router.RouteCollection(dummyData, dummyDownStream, nil)
	assert.Nil(err)
	assert.True(reflect.DeepEqual(origKey, dummyData.Req.Key))
}

// Given one mutation, usually would translate into one MCRequest
// But with migration, one mutation can translate into multiple target collections
// This test to ensure siblig requests are parsed correctly
func TestRouterExplicitMigrationSiblingReq(t *testing.T) {
	fmt.Println("============== Test case start: TestRouterExplicitMigrationSiblingReq =================")
	defer fmt.Println("============== Test case end: TestRouterExplicitMigrationSiblingReq =================")
	assert := assert.New(t)

	routerId, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, spec, _, _ := setupBoilerPlateRouter()

	mappingMode := spec.Settings.GetCollectionModes()
	mappingMode.SetMigration(true)
	rules := spec.Settings.GetCollectionsRoutingRules()
	rules["REGEXP_CONTAINS(META().id, \"d1\")"] = "S2.col1"
	rules["META().id == \"d1\""] = "S2.col2"

	updatedMap := make(map[string]interface{})
	updatedMap[metadata.CollectionsMgtMultiKey] = mappingMode
	updatedMap[metadata.CollectionsMappingRulesKey] = rules
	_, errMap := spec.Settings.UpdateSettingsFromMap(updatedMap)
	assert.Equal(0, len(errMap))

	setupCollectionManifestsSvcRouter(collectionsManifestSvc)

	router, err := NewRouter(routerId, spec, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, nil, nil, collectionsCap, nil, nil, nil, nil)

	assert.Nil(err)
	modes := router.collectionModes.Get()
	assert.True(modes.IsMigrationOn())

	collectionsRouter := router.collectionsRouting[dummyDownStream]
	assert.NotNil(collectionsRouter)
	assert.Nil(collectionsRouter.Start())

	// routing updater receiver
	newRoutingUpdater := func(info CollectionsRoutingInfo) error {
		return nil
	}

	var ignoreCnt int
	ignoreFunc := func(*base.WrappedMCRequest) {
		ignoreCnt++
	}

	collectionsRouter.routingUpdater = newRoutingUpdater
	collectionsRouter.ignoreDataFunc = ignoreFunc

	uprEvent, err := RetrieveUprFile("./testdata/MB-33010Upr.json")
	assert.Nil(err)
	assert.NotNil(uprEvent)

	wrappedEvent := &base.WrappedUprEvent{}
	wrappedEvent.UprEvent = uprEvent

	wrappedMCR, err := router.ComposeMCRequest(wrappedEvent)
	assert.Nil(err)

	err = router.RouteCollection(wrappedMCR, dummyDownStream, wrappedEvent)
	assert.Nil(err)

	assert.Equal(1, len(wrappedMCR.SiblingReqs))
}

func TestRouterRaceyBrokenMap(t *testing.T) {
	fmt.Println("============== Test case start: TestRouterRaceyBrokenMap =================")
	defer fmt.Println("============== Test case end: TestRouterRaceyBrokenMap =================")
	assert := assert.New(t)

	routerId, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, spec, _, connectivityGetter := setupBoilerPlateRouter()
	setupCollectionManifestsSvcRouterWithSpecificTarget(collectionsManifestSvc, 1)

	router, err := NewRouter(routerId, spec, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, nil, nil, collectionsCap, nil, connectivityGetter, nil, nil)
	assert.Nil(err)
	collectionsRouter := router.collectionsRouting[dummyDownStream]
	assert.NotNil(collectionsRouter)
	assert.Nil(collectionsRouter.Start())
	collectionsRouter.targetManifestRefresh = 1 * time.Second

	uprEvent, err := RetrieveUprFile("./testdata/MB-33010Upr.json")
	assert.Nil(err)
	assert.NotNil(uprEvent)

	wrappedEvent := &base.WrappedUprEvent{UprEvent: uprEvent,
		ColNamespace: &base.CollectionNamespace{
			ScopeName:      "S1",
			CollectionName: "col1",
		},
	}
	wrappedEvent.UprEvent = uprEvent

	wrappedMCR, err := router.ComposeMCRequest(wrappedEvent)
	wrappedMCR.ColInfo = &base.TargetCollectionInfo{
		ManifestId:          7,
		ColId:               8,
		ColIDPrefixedKey:    nil,
		ColIDPrefixedKeyLen: 0,
		TargetNamespace: &base.CollectionNamespace{
			ScopeName:      "S1",
			CollectionName: "col1",
		},
	}
	assert.Nil(err)

	// routing updater receiver
	var lastCalledBackfillMap metadata.CollectionNamespaceMapping
	newRoutingUpdater := func(info CollectionsRoutingInfo) error {
		lastCalledBackfillMap = info.BackfillMap
		return nil
	}

	var ignoreCnt int
	ignoreFunc := func(*base.WrappedMCRequest) {
		ignoreCnt++
	}

	collectionsRouter.routingUpdater = newRoutingUpdater
	collectionsRouter.ignoreDataFunc = ignoreFunc
	collectionsRouter.RouteReqToLatestTargetManifest(wrappedMCR, wrappedEvent)
	assert.Nil(lastCalledBackfillMap)

	collectionsRouter.recordUnroutableRequest(wrappedMCR)
	assert.Equal(1, ignoreCnt)
	assert.NotNil(collectionsRouter.brokenMapDblChkTicker)
	assert.Nil(lastCalledBackfillMap)
	assert.Equal(1, len(collectionsRouter.brokenMapping))

	time.Sleep(1 * time.Second)
	collectionsRouter.RouteReqToLatestTargetManifest(wrappedMCR, wrappedEvent)
	assert.Equal(1, ignoreCnt)
	assert.Nil(collectionsRouter.brokenMapDblChkTicker)
	assert.NotNil(lastCalledBackfillMap)
	assert.Equal(0, len(collectionsRouter.brokenMapping))
}

// The idle is where we allow the timer to kick off and self-heal
func TestRouterRaceyBrokenMapIdle(t *testing.T) {
	fmt.Println("============== Test case start: TestRouterRaceyBrokenMapIdle =================")
	defer fmt.Println("============== Test case end: TestRouterRaceyBrokenMapIdle =================")
	assert := assert.New(t)

	routerId, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, spec, _, _ := setupBoilerPlateRouter()
	setupCollectionManifestsSvcRouterWithSpecificTarget(collectionsManifestSvc, 1)

	var connectivityReturnHealthy = true
	var connectivityGetCnt uint32
	connectivityGetter := func() (metadata.ConnectivityStatus, error) {
		atomic.AddUint32(&connectivityGetCnt, 1)
		if connectivityReturnHealthy {
			return metadata.ConnValid, nil
		} else {
			return metadata.ConnDegraded, nil
		}
	}

	router, err := NewRouter(routerId, spec, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, nil, nil, collectionsCap, nil, connectivityGetter, nil, nil)
	assert.Nil(err)
	collectionsRouter := router.collectionsRouting[dummyDownStream]
	assert.NotNil(collectionsRouter)
	assert.Nil(collectionsRouter.Start())
	collectionsRouter.targetManifestRefresh = 100 * time.Millisecond

	uprEvent, err := RetrieveUprFile("./testdata/MB-33010Upr.json")
	assert.Nil(err)
	assert.NotNil(uprEvent)

	wrappedEvent := &base.WrappedUprEvent{UprEvent: uprEvent,
		ColNamespace: &base.CollectionNamespace{
			ScopeName:      "S1",
			CollectionName: "col1",
		},
	}
	wrappedEvent.UprEvent = uprEvent

	wrappedMCR, err := router.ComposeMCRequest(wrappedEvent)
	wrappedMCR.ColInfo = &base.TargetCollectionInfo{
		ManifestId:          7,
		ColId:               8,
		ColIDPrefixedKey:    nil,
		ColIDPrefixedKeyLen: 0,
		TargetNamespace: &base.CollectionNamespace{
			ScopeName:      "S1",
			CollectionName: "col1",
		},
	}
	assert.Nil(err)

	// routing updater receiver
	var lastCalledBackfillMap metadata.CollectionNamespaceMapping
	newRoutingUpdater := func(info CollectionsRoutingInfo) error {
		lastCalledBackfillMap = info.BackfillMap
		return nil
	}

	var ignoreCnt int
	ignoreFunc := func(*base.WrappedMCRequest) {
		ignoreCnt++
	}

	collectionsRouter.routingUpdater = newRoutingUpdater
	collectionsRouter.ignoreDataFunc = ignoreFunc
	collectionsRouter.RouteReqToLatestTargetManifest(wrappedMCR, wrappedEvent)
	assert.Nil(lastCalledBackfillMap)

	collectionsRouter.recordUnroutableRequest(wrappedMCR)
	assert.Equal(1, ignoreCnt)
	// Note that brokenMapDblChkTicker and brokenMapDblChkKicker will be not nil only when xmem raises collection error
	assert.NotNil(collectionsRouter.brokenMapDblChkTicker)
	assert.NotNil(collectionsRouter.brokenMapDblChkKicker)
	assert.Nil(lastCalledBackfillMap)
	assert.Equal(1, len(collectionsRouter.brokenMapping))

	time.Sleep(1 * time.Second)
	assert.Equal(1, ignoreCnt)
	assert.Nil(collectionsRouter.brokenMapDblChkTicker)
	assert.NotNil(lastCalledBackfillMap)
	assert.Equal(0, len(collectionsRouter.brokenMapping))
	assert.Nil(collectionsRouter.brokenMapDblChkKicker)
	assert.Equal(1, int(atomic.LoadUint32(&connectivityGetCnt)))

	// Now, let's pretend the cluster is unhealthy and the same ignore request should not be serviced
	connectivityReturnHealthy = false
	lastCalledBackfillMap = nil
	collectionsRouter.recordUnroutableRequest(wrappedMCR)
	assert.Equal(2, ignoreCnt)
	time.Sleep(2 * time.Second)
	assert.NotNil(collectionsRouter.brokenMapDblChkTicker)
	assert.Nil(lastCalledBackfillMap)
	assert.NotEqual(0, len(collectionsRouter.brokenMapping))
	assert.NotNil(collectionsRouter.brokenMapDblChkKicker)
	assert.NotEqual(1, int(atomic.LoadUint32(&connectivityGetCnt)))

	// Now let's say cluster is healthy again
	connectivityReturnHealthy = true
	lastCalledBackfillMap = nil
	time.Sleep(1 * time.Second)
	assert.Nil(collectionsRouter.brokenMapDblChkTicker)
	assert.NotNil(lastCalledBackfillMap)
	assert.Equal(0, len(collectionsRouter.brokenMapping))
	assert.Nil(collectionsRouter.brokenMapDblChkKicker)
}

func TestClosedListenerCase(t *testing.T) {
	fmt.Println("============== Test case start: TestClosedListenerCase =================")
	defer fmt.Println("============== Test case end: TestClosedListenerCase =================")
	assert := assert.New(t)

	routerId, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, spec, _, _ := setupBoilerPlateRouter()

	router, err := NewRouter(routerId, spec, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, nil, nil, collectionsCap, nil, nil, nil, nil)

	assert.Nil(err)

	listener := component.NewDefaultAsyncComponentEventListenerImpl("1", "", log.DefaultLoggerContext)
	router.RegisterComponentEventListener(common.BrokenRoutingUpdateEvent, listener)

	listener = component.NewDefaultAsyncComponentEventListenerImpl("2", "", log.DefaultLoggerContext)
	router.RegisterComponentEventListener(common.FixedRoutingUpdateEvent, listener)

	listeners := router.AsyncComponentEventListeners()
	for _, listener := range listeners {
		assert.Nil(listener.Start())
	}

	// stop the listeners
	for _, listener := range listeners {
		assert.Nil(listener.Stop())
	}

	var ignoreCnt int
	ignoreFunc := func(*base.WrappedMCRequest) {
		ignoreCnt++
	}

	sourceNs := &base.CollectionNamespace{ScopeName: "S1", CollectionName: "col1"}
	mcReq := &gomemcached.MCRequest{
		Key:    []byte("testKey"),
		Keylen: len("testKey"),
	}
	dummyData := &base.WrappedMCRequest{
		Req:             mcReq,
		SrcColNamespace: sourceNs,
		ColInfo:         &base.TargetCollectionInfo{ColId: 123},
	}

	collectionsRouter := router.collectionsRouting[dummyDownStream]
	assert.NotNil(collectionsRouter)
	collectionsRouter.ignoreDataFunc = ignoreFunc

	// 1. The target will return default manifest
	setupCollectionManifestsSvcRouterWithSpecificDefaultTarget(collectionsManifestSvc)
	assert.Nil(collectionsRouter.Start())
	router.RouteCollection(dummyData, dummyDownStream, nil)

	// 2. target will now return version 1 manifest (not default manifest)
	collectionsManifestSvc2 := &service_def_mocks.CollectionsManifestSvc{}
	setupCollectionManifestsSvcRouterWithSpecificTarget(collectionsManifestSvc2, 1)
	router.collectionsRouting[dummyDownStream].collectionsManifestSvc = collectionsManifestSvc2
	router.RouteCollection(dummyData, dummyDownStream, nil)
}

func TestRouterCasDriftCheck(t *testing.T) {
	fmt.Println("============== Test case start: CasDriftCheck =================")
	defer fmt.Println("============== Test case end: CasDriftCheck =================")
	assert := assert.New(t)

	routerId, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, spec, recycler, connectivityStatus := setupBoilerPlateRouter()
	expDelMode = base.FilterExpDelSkipDeletes
	router, err := NewRouter(routerId, spec, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, recycler, nil, nonCollectionsCap, nil, connectivityStatus, nil, nil)

	assert.Nil(err)
	assert.NotNil(router)

	delEvent, err := RetrieveUprFile("./testdata/uprEventDeletion.json")
	assert.Nil(err)
	assert.NotNil(delEvent)

	expEvent, err := RetrieveUprFile("./testdata/uprEventExpiration.json")
	assert.Nil(err)
	assert.NotNil(expEvent)

	wrappedDel := &base.WrappedUprEvent{UprEvent: delEvent}
	wrappedExp := &base.WrappedUprEvent{UprEvent: expEvent}
	assert.False(router.CheckCasDrift(wrappedDel))
	assert.False(router.CheckCasDrift(wrappedExp))

	// CAS from time.Now() but with a little bit of a drift
	onsSec := time.Duration(1) * time.Second
	driftThreshold := time.Duration(router.casDriftThreshold) * time.Second
	acceptableDriftTime := time.Now().Add(driftThreshold).Add(-onsSec).UnixNano()
	wrappedExp.UprEvent.Cas = uint64(acceptableDriftTime)
	assert.False(router.CheckCasDrift(wrappedExp))

	// CAS from CBSE
	wrappedExp.UprEvent.Cas = 2017616266604601370
	assert.True(router.CheckCasDrift(wrappedExp))

	// Turn off check
	router.casDriftThreshold = 0
	assert.False(router.CheckCasDrift(wrappedExp))
}

func setupCollectionManifestSvcMocksForGetLatestTargetManifest(collectionsManifestSvc *service_def_mocks.CollectionsManifestSvc, spec *metadata.ReplicationSpecification) {
	manifestFileDir := "../metadata/testdata"
	manifestFileNamev8 := "targetManifestv1.json"
	manifestFileNamev9 := "targetManifestv2.json"

	data, err := ioutil.ReadFile(fmt.Sprintf("%v/%v", manifestFileDir, manifestFileNamev8))
	if err != nil {
		panic(err.Error())
	}
	manifest8, err := metadata.NewCollectionsManifestFromBytes(data) // contains S2.C3

	data, err = ioutil.ReadFile(fmt.Sprintf("%v/%v", manifestFileDir, manifestFileNamev9))
	if err != nil {
		panic(err.Error())
	}
	manifest9, err := metadata.NewCollectionsManifestFromBytes(data) // does not S2.C3

	collectionsManifestSvc.On("GetSpecificTargetManifest", spec, uint64(math.MaxUint64)).Return(&manifest8, nil).Once() // before refresh
	collectionsManifestSvc.On("GetSpecificTargetManifest", spec, uint64(math.MaxUint64)).Return(&manifest9, nil)        // after refresh
	collectionsManifestSvc.On("GetSpecificTargetManifest", spec, uint64(8)).Return(&manifest8, nil)
	collectionsManifestSvc.On("GetSpecificTargetManifest", spec, uint64(9)).Return(&manifest9, nil)
	collectionsManifestSvc.On("ForceTargetManifestRefresh", mock.Anything).Return(nil)
	collectionsManifestSvc.On("GetLatestManifests", spec, false).Return(&manifest8, &manifest8, nil)
}

func TestCollectionsRouter_getLatestTargetManifest(t *testing.T) {
	fmt.Println("============== Test case start: getLatestTargetManifest =================")
	defer fmt.Println("============== Test case end: getLatestTargetManifest =================")
	assert := assert.New(t)

	routerId, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, spec, recycler, connectivityStatus := setupBoilerPlateRouter()
	setupCollectionManifestSvcMocksForGetLatestTargetManifest(collectionsManifestSvc, spec)

	router, err := NewRouter(routerId, spec, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, recycler, nil, collectionsCap, nil, connectivityStatus, nil, nil)
	assert.Nil(err)
	collectionsRouter := router.collectionsRouting[dummyDownStream]
	assert.NotNil(collectionsRouter)

	reqManifestId := 8
	reqColId, _ := strconv.ParseUint("d", base.CollectionsUidBase, 64)
	tgtColNameSpace := &base.CollectionNamespace{ScopeName: "S2", CollectionName: "col1"}

	// cache is stale - needs refresh
	id, err := collectionsRouter.getLatestTargetManifestId(spec, uint64(reqManifestId), uint32(reqColId), tgtColNameSpace)
	assert.Nil(err)
	assert.Equal(uint64(9), id)

	// next pass should read from cache
	id, err = collectionsRouter.getLatestTargetManifestId(spec, uint64(reqManifestId), uint32(reqColId), tgtColNameSpace)
	assert.Nil(err)
	assert.Equal(uint64(9), id)

	// error injection - to verify that we are hitting the cache path
	// Since we pass the tgtColNamespace field as nil, the cache path should return an error.
	_, err1 := collectionsRouter.getLatestTargetManifestId(spec, uint64(7), uint32(reqColId), nil)
	assert.NotNil(err1)
}

func TestCollectionsRouter_checkIfXmemReportedCollectionError(t *testing.T) {
	fmt.Println("============== Test case start: checkIfXmemReportedCollectionError =================")
	defer fmt.Println("============== Test case end: checkIfXmemReportedCollectionError =================")
	assert := assert.New(t)

	routerId, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, spec, recycler, connectivityStatus := setupBoilerPlateRouter()
	setupCollectionManifestSvcMocksForGetLatestTargetManifest(collectionsManifestSvc, spec)

	router, err := NewRouter(routerId, spec, downStreamParts, routingMap, crMode, loggerCtx, utilsMock, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, recycler, nil, collectionsCap, nil, connectivityStatus, nil, nil)
	assert.Nil(err)

	collectionsRouter := router.collectionsRouting[dummyDownStream]
	assert.NotNil(collectionsRouter)
	assert.Nil(collectionsRouter.Start())

	reqManifestId := 8
	reqColId, _ := strconv.ParseUint("d", base.CollectionsUidBase, 64)

	uprEvent, err := RetrieveUprFile("./testdata/MB-33010Upr.json")
	assert.Nil(err)
	assert.NotNil(uprEvent)

	wrappedEvent := &base.WrappedUprEvent{UprEvent: uprEvent,
		ColNamespace: &base.CollectionNamespace{
			ScopeName:      "S2",
			CollectionName: "col3",
		},
	}
	wrappedEvent.UprEvent = uprEvent

	wrappedMCR, err := router.ComposeMCRequest(wrappedEvent)
	wrappedMCR.ColInfo = &base.TargetCollectionInfo{
		ManifestId:          7,
		ColId:               uint32(reqColId),
		ColIDPrefixedKey:    nil,
		ColIDPrefixedKeyLen: 0,
		TargetNamespace: &base.CollectionNamespace{
			ScopeName:      "S1",
			CollectionName: "col1",
		},
	}

	// routing updater receiver
	var lastCalledBackfillMap metadata.CollectionNamespaceMapping
	newRoutingUpdater := func(info CollectionsRoutingInfo) error {
		lastCalledBackfillMap = info.BackfillMap
		return nil
	}

	var ignoreCnt int
	ignoreFunc := func(*base.WrappedMCRequest) {
		ignoreCnt++
	}
	// route one request through the router
	collectionsRouter.routingUpdater = newRoutingUpdater
	collectionsRouter.ignoreDataFunc = ignoreFunc
	// manifest 8 contains S2.C3 and hence error should be nil
	_, _, _, _, _, _, err = collectionsRouter.RouteReqToLatestTargetManifest(wrappedMCR, wrappedEvent)
	assert.Nil(err)
	assert.Nil(lastCalledBackfillMap)
	assert.Equal(0, ignoreCnt)

	xmemReported, manId, err := collectionsRouter.checkIfXmemReportedCollectionError(spec, uint64(reqManifestId), uint32(reqColId), wrappedMCR.ColInfo.TargetNamespace)
	assert.Equal(true, xmemReported)
	assert.Equal(uint64(9), manId)
	assert.Nil(err)

	// route another req through the router so that the lastKnownTgtManifest updates to 9
	_, _, _, _, _, _, err = collectionsRouter.RouteReqToLatestTargetManifest(wrappedMCR, wrappedEvent)
	assert.NotNil(err)
	collectionsRouter.recordUnroutableRequest(wrappedMCR)
	assert.Equal(1, ignoreCnt)
	assert.Nil(lastCalledBackfillMap)

	// denotes the case where router updated its cache after xmem raises a collection error for the reqs routed with older manifest ID
	xmemReported, manId, err = collectionsRouter.checkIfXmemReportedCollectionError(spec, uint64(reqManifestId), uint32(reqColId), wrappedMCR.ColInfo.TargetNamespace)
	assert.Equal(true, xmemReported)
	assert.Equal(uint64(9), manId)
	assert.Nil(err)
}

func TestMigrationFilterAfterStrippingTxnXattr(t *testing.T) {
	fmt.Println("============== Test case start: TestMigrationFilterAfterStrippingTxnXattr =================")
	defer fmt.Println("============== Test case end: TestMigrationFilterAfterStrippingTxnXattr =================")
	a := assert.New(t)

	routerId, downStreamParts, routingMap, crMode, loggerCtx, _, throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, spec, recycler, connectivityStatus := setupBoilerPlateRouter()
	router, err := NewRouter(routerId, spec, downStreamParts, routingMap, crMode, loggerCtx, utilities.NewUtilities(), throughputThrottlerSvc, needToThrottle, expDelMode, collectionsManifestSvc, recycler, nil, nonCollectionsCap, nil, connectivityStatus, nil, nil)
	a.Nil(err)

	val := `{}`
	txnXattrKey := base.TransactionXattrKey
	txnXattrVal := `{}`
	body := make([]byte, len(txnXattrKey)+len(txnXattrVal)+4+4+2+len(val))
	composer := base.NewXattrComposer(body)
	err = composer.WriteKV([]byte(txnXattrKey), []byte(txnXattrVal))
	a.Nil(err)
	body, _ = composer.FinishAndAppendDocValue([]byte(val), nil, nil)
	body = snappy.Encode(nil, body)

	uprEvent := &base.WrappedUprEvent{
		UprEvent: &mcc.UprEvent{
			Opcode:   gomemcached.UPR_MUTATION,
			VBucket:  0,
			DataType: base.JSONDataType | base.XattrDataType | base.SnappyDataType,
			Value:    body,
		},
		ByteSliceGetter: func(u uint64) ([]byte, error) {
			return make([]byte, u), nil
		},
	}

	router.filter, err = filter.NewFilterWithSharedDP("", "", utilities.NewUtilities(), base.NewFakeDataPool(), 0, base.MobileCompatibilityOff)
	a.Nil(err)

	// 1. first FilterUprEvent is applied in router.Route to strip out txn xattrs.
	// xattr bit is stripped from datatype because txn was the only xattr.
	replicate, err, errDesc, dpFails, status := router.filter.FilterUprEvent(uprEvent)
	a.True(replicate)
	a.Nil(err)
	a.Equal(len(errDesc), 0)
	a.Equal(dpFails, int64(0))
	a.Equal(status, filter.NotFiltered)

	mcReq, err := router.ComposeMCRequest(uprEvent)
	a.Nil(err)

	// 2. Now apply some regex based on document body in the migration codepath.
	// It should resue the decompressed value in (1) and should not have any errors.
	srcNs, err := metadata.NewSourceMigrationNamespace(`REGEXP_CONTAINS(type, "foo")`, base.NewFakeDataPool())
	a.Nil(err)
	c := &metadata.CollectionNamespaceMapping{
		srcNs: metadata.CollectionNamespaceList{&base.CollectionNamespace{ScopeName: "S1", CollectionName: "col1"}},
	}
	_, errMap, errMcReq := c.GetTargetUsingMigrationFilter(uprEvent, mcReq, nil)
	a.Equal(len(errMap), 0)
	a.Equal(len(errMcReq), 0)
}

func TestCollectionRouterInit_TwoRoutersOneNozzle(t *testing.T) {
	fmt.Println("============== Test case start: TestCollectionRouterInit_TwoRoutersOneNozzle =================")
	defer fmt.Println("============== Test case end: TestCollectionRouterInit_TwoRoutersOneNozzle =================")

	assert := assert.New(t)

	// Initialize utility and logger
	utilInstance := utilities.NewUtilities()
	dummyLogger := &log.CommonLogger{}

	// Shared nozzle setup: responsible for all 128 vBuckets
	sharedNozzle := &XmemNozzle{
		vbList:                 make([]uint16, 128),
		upstreamErrReporterMap: make(map[uint16]utilities.ErrReportFunc),
	}
	for i := uint16(0); i < 128; i++ {
		sharedNozzle.vbList[i] = i
	}

	// Setup boilerplate
	routerIdA, downstreamParts, routingMapA, crMode, loggerCtx, _, throttlerSvc, needToThrottle,
		expDelMode, manifestSvc, spec, recycler, connectivityStatus := setupBoilerPlateRouter()
	delete(downstreamParts, dummyDownStream)
	downstreamParts["shared"] = sharedNozzle

	// Set up Router A (0–63)
	vbsA := make([]uint16, 64)
	routingMap := routingMapA
	for i := uint16(0); i < 64; i++ {
		routingMap[i] = "shared"
		vbsA[i] = i
	}

	routerA, err := NewRouter(
		routerIdA, spec, downstreamParts, routingMap, crMode,
		loggerCtx, utilInstance, throttlerSvc,
		needToThrottle, expDelMode, manifestSvc,
		recycler, nil, nonCollectionsCap, nil, connectivityStatus,
		nil, nil,
	)
	assert.Nil(err)
	assert.NotNil(routerA)

	// Set up Router B (64–127)
	routerIdB := routerIdA + "_B"
	routingMapB := make(map[uint16]string)
	vbsB := make([]uint16, 64)
	for i := uint16(64); i < 128; i++ {
		routingMapB[i] = "shared"
		vbsB[i-64] = i
	}

	routerB, err := NewRouter(
		routerIdB, spec, downstreamParts, routingMapB, crMode,
		loggerCtx, utilInstance, throttlerSvc,
		needToThrottle, expDelMode, manifestSvc,
		recycler, nil, nonCollectionsCap, nil, connectivityStatus,
		nil, nil,
	)
	assert.Nil(err)
	assert.NotNil(routerB)

	// Initialize collection routing
	routerA.collectionsRouting.Init(
		routerIdA, downstreamParts, manifestSvc, spec,
		dummyLogger, nil, nil, nil, nil, nil, nil, vbsA,
	)
	routerB.collectionsRouting.Init(
		routerIdB, downstreamParts, manifestSvc, spec,
		dummyLogger, nil, nil, nil, nil, nil, nil, vbsB,
	)

	// Validate
	assert.Equal(128, len(sharedNozzle.upstreamErrReporterMap))
	assert.Equal(1, len(routerA.collectionsRouting))
	assert.Equal(1, len(routerB.collectionsRouting))
}

func TestCollectionRouterInit_OneRouterTwoNozzles(t *testing.T) {
	fmt.Println("============== Test case start: TestCollectionRouterInit_OneRouterTwoNozzles =================")
	defer fmt.Println("============== Test case end: TestCollectionRouterInit_OneRouterTwoNozzles =================")

	assert := assert.New(t)

	// Initialize utilities and logger
	utilInstance := utilities.NewUtilities()
	dummyLogger := &log.CommonLogger{}

	// Create two output nozzles each responsible for 64 vBuckets
	nozzle1 := &XmemNozzle{
		vbList:                 make([]uint16, 0, 64),
		upstreamErrReporterMap: make(map[uint16]utilities.ErrReportFunc),
	}
	nozzle2 := &XmemNozzle{
		vbList:                 make([]uint16, 0, 64),
		upstreamErrReporterMap: make(map[uint16]utilities.ErrReportFunc),
	}

	for i := uint16(0); i < 64; i++ {
		nozzle1.vbList = append(nozzle1.vbList, i)
	}
	for i := uint16(64); i < 128; i++ {
		nozzle2.vbList = append(nozzle2.vbList, i)
	}

	// Setup boilerplate
	routerId, downstreamParts, routingMap, crMode, loggerCtx, _, throttlerSvc, needToThrottle,
		expDelMode, collManifestSvc, spec, recycler, connectivityStatus := setupBoilerPlateRouter()

	// Assign downstream parts
	delete(downstreamParts, "dummy")
	downstreamParts["nozzle1"] = nozzle1
	downstreamParts["nozzle2"] = nozzle2

	// Set up routing map for 128 vBuckets
	routerVbs := make([]uint16, 0, 128)
	for vb := uint16(0); vb < 128; vb++ {
		if vb < 64 {
			routingMap[vb] = "nozzle1"
		} else {
			routingMap[vb] = "nozzle2"
		}
		routerVbs = append(routerVbs, vb)
	}

	// Create the router
	router, err := NewRouter(
		routerId, spec, downstreamParts, routingMap, crMode,
		loggerCtx, utilInstance, throttlerSvc,
		needToThrottle, expDelMode, collManifestSvc,
		recycler, nil, nonCollectionsCap, nil, connectivityStatus,
		nil, nil,
	)
	assert.Nil(err)
	assert.NotNil(router)

	// Initialize collections routing
	router.collectionsRouting.Init(
		routerId, downstreamParts, collManifestSvc, spec,
		dummyLogger, nil, nil, nil, nil, nil, nil, routerVbs,
	)

	// Validate
	assert.Equal(64, len(nozzle1.upstreamErrReporterMap))
	assert.Equal(64, len(nozzle2.upstreamErrReporterMap))
	assert.Equal(2, len(router.collectionsRouting))

	// Test with 1 nozzle and 1 router
	// modify the existing setup
	nozzle1.upstreamErrReporterMap = make(map[uint16]utilities.ErrReportFunc)
	for vb := uint16(64); vb < 128; vb++ {
		nozzle1.vbList = append(nozzle1.vbList, vb)
		routingMap[vb] = "nozzle1"
	}
	delete(downstreamParts, "nozzle2")
	delete(router.collectionsRouting, "nozzle2")

	router.collectionsRouting.Init(
		routerId, downstreamParts, collManifestSvc, spec,
		dummyLogger, nil, nil, nil, nil, nil, nil, routerVbs,
	)
	// Validate
	assert.Equal(128, len(nozzle1.upstreamErrReporterMap))
	assert.Equal(1, len(router.collectionsRouting))
}
