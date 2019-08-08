package backfill_manager

import (
	"fmt"
	service_def "github.com/couchbase/goxdcr/service_def/mocks"
	"github.com/stretchr/testify/assert"
	"testing"
)

func setupBoilerPlate() (*service_def.CollectionsManifestSvc,
	*service_def.ReplicationSpecSvc) {
	manifestSvc := &service_def.CollectionsManifestSvc{}
	replSpecSvc := &service_def.ReplicationSpecSvc{}

	return manifestSvc, replSpecSvc
}

func setupMock(manifestSvc *service_def.CollectionsManifestSvc,
	replSpecSvc *service_def.ReplicationSpecSvc) {

}

func TestBackfillMgrLaunch(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestBackfillMgrLaunch =================")
	manifestSvc, replSpecSvc := setupBoilerPlate()
	setupMock(manifestSvc, replSpecSvc)

	backfillMgr := NewBackfillManager(manifestSvc, nil /*exitFunc*/, replSpecSvc)
	assert.NotNil(backfillMgr)

	fmt.Println("============== Test case end: TestBackfillMgrLaunch =================")
}
