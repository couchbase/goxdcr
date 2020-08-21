package metadata

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDecompositionReplId(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestDecompositionReplId =================")
	defer fmt.Println("============== Test case end: TestDecompositionReplId =================")

	replId := "136082a7c89ccdc9aed81cb04a97720f/B1/B2"

	deconstructed := DecomposeReplicationId(replId, nil)

	assert.Equal("136082a7c89ccdc9aed81cb04a97720f", deconstructed.TargetClusterUUID)
	assert.Equal("B1", deconstructed.SourceBucketName)
	assert.Equal("B2", deconstructed.TargetBucketName)
	assert.Equal("Main", deconstructed.PipelineType)

	replId = "backfill_136082a7c89ccdc9aed81cb04a97720f/B1/B2"
	deconstructed = DecomposeReplicationId(replId, deconstructed)
	assert.Equal("Backfill", deconstructed.PipelineType)
}
