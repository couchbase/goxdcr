package backfill_manager

import (
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func setupBRHBoilerPlate() (*log.CommonLogger, BackfillPersistCb) {
	logger := log.NewLogger("BackfillReqHandler", log.DefaultLoggerContext)
	persistCb := func(info metadata.BackfillPersistInfo) error {
		return nil
	}

	return logger, persistCb
}

const handlerId = "testHandler"

func TestBackfillReqHandler(t *testing.T) {
	assert := assert.New(t)
	logger, persistCb := setupBRHBoilerPlate()
	fmt.Println("============== Test case start: TestBackfillReqHandler =================")
	rh := NewBackfillRequestHandler(logger, handlerId, persistCb)

	assert.NotNil(rh)
	assert.Nil(rh.Start())

	var dummyManifest metadata.CollectionsManifest
	var dummyManifest2 metadata.CollectionsManifest

	mapping := make(metadata.CollectionToCollectionMapping)
	var dummySrcCol metadata.Collection
	var dummyTgtCol metadata.Collection
	pair := metadata.CollectionsManifestPair{&dummyManifest, &dummyManifest2}
	mapping[&dummySrcCol] = dummyTgtCol

	request := metadata.NewBackfillRequest(pair, mapping, 0, 1000)
	assert.Nil(rh.HandleBackfillRequest(request))

	time.Sleep(100 * time.Millisecond)

	componentErr := make(chan base.ComponentError, 1)
	var waitGrp sync.WaitGroup

	// Stopping
	waitGrp.Add(1)
	rh.Stop(&waitGrp, componentErr)

	retVal := <-componentErr
	assert.NotNil(retVal)
	assert.Equal(handlerId, retVal.ComponentId)
	assert.Nil(retVal.Err)

	fmt.Println("============== Test case end: TestBackfillReqHandler =================")
}
