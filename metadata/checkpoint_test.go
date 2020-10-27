package metadata

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCheckpointDocMarshaller(t *testing.T) {
	assert := assert.New(t)

	vbUuidAndTimestamp := &TargetVBUuidAndTimestamp{
		Target_vb_uuid: "abc",
		Startup_time:   "012",
	}
	newCkptRecord := CheckpointRecord{
		Failover_uuid:                0,
		Seqno:                        1,
		Dcp_snapshot_seqno:           2,
		Dcp_snapshot_end_seqno:       3,
		Target_vb_opaque:             vbUuidAndTimestamp,
		Target_Seqno:                 4,
		Filtered_Items_Cnt:           5,
		Filtered_Failed_Cnt:          6,
		SourceManifestForDCP:         7,
		SourceManifestForBackfillMgr: 8,
		TargetManifest:               9,
		BrokenMappingSha256:          "",
		brokenMappings:               nil,
	}

	ckpt_doc := NewCheckpointsDoc("testInternalId")
	added, _ := ckpt_doc.AddRecord(&newCkptRecord)
	assert.True(added)

	marshalledData, err := json.Marshal(ckpt_doc)
	assert.Nil(err)

	var checkDoc CheckpointsDoc
	err = json.Unmarshal(marshalledData, &checkDoc)
	assert.Nil(err)

	assert.Equal(5, len(checkDoc.Checkpoint_records))
	assert.NotNil(checkDoc.Checkpoint_records[0])
	assert.True(checkDoc.Checkpoint_records[0].IsSame(&newCkptRecord))
	assert.Equal(newCkptRecord.SourceManifestForBackfillMgr, checkDoc.Checkpoint_records[0].SourceManifestForBackfillMgr)
}
