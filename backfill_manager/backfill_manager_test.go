package backfill_manager

import (
	"fmt"
	service_def "github.com/couchbase/goxdcr/service_def/mocks"
	"github.com/stretchr/testify/assert"
	"testing"
)

func setupBoilerPlate() (*service_def.CollectionsManifestSvc,
	*service_def.ReplicationSpecSvc,
	*service_def.BackfillReplSvc) {
	manifestSvc := &service_def.CollectionsManifestSvc{}
	replSpecSvc := &service_def.ReplicationSpecSvc{}
	backfillReplSvc := &service_def.BackfillReplSvc{}

	return manifestSvc, replSpecSvc, backfillReplSvc
}

func setupMock(manifestSvc *service_def.CollectionsManifestSvc,
	replSpecSvc *service_def.ReplicationSpecSvc,
	backfillReplSvc *service_def.BackfillReplSvc) {

}

func TestBackfillMgrLaunch(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestBackfillMgrLaunch =================")
	manifestSvc, replSpecSvc, backfillReplSvc := setupBoilerPlate()
	setupMock(manifestSvc, replSpecSvc, backfillReplSvc)

	backfillMgr := NewBackfillManager(manifestSvc, nil /*exitFunc*/, replSpecSvc, backfillReplSvc)
	assert.NotNil(backfillMgr)

	fmt.Println("============== Test case end: TestBackfillMgrLaunch =================")
}
