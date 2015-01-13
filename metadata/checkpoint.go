package metadata

import ()

const (
	//the maximum number of checkpoints ketp in the file
	MaxCheckpointsKept int = 1000

	FailOverUUID        string = "failover_uuid"
	Seqno               string = "seqno"
	DcpSnapshotSeqno    string = "dcp_snapshot_seqno"
	DcpSnapshotEndSeqno string = "dcp_snapshot_end_seqno"
	TargetVbUUID        string = "target_vb_uuid"
	CommitOpaque        string = "commitopaque"
)

type CheckpointRecord struct {
	//source vbucket failover uuid
	Failover_uuid uint64 `json:"failover_uuid"`
	//source vbucket high sequence number
	Seqno uint64 `json:"seqno"`
	//source snapshot start sequence number
	Dcp_snapshot_seqno uint64 `json:"dcp_snapshot_seqno"`
	//source snapshot end sequence number
	Dcp_snapshot_end_seqno uint64 `json:"dcp_snapshot_end_seqno"`
	//target vb uuid
	Target_vb_uuid uint64 `json:"target_vb_uuid"`
	//target vb high sequence number
	Commitopaque uint64 `json:"commitopaque"`
}

type CheckpointsDoc struct {
	//keep 100 checkpoint record
	Checkpoint_records []*CheckpointRecord `json:"checkpoints"`

	//revision number
	Revision interface{}
}

func (ckpt *CheckpointRecord) ToMap() map[string]interface{} {
	ckpt_record_map := make(map[string]interface{})
	ckpt_record_map[FailOverUUID] = ckpt.Failover_uuid
	ckpt_record_map[Seqno] = ckpt.Seqno
	ckpt_record_map[DcpSnapshotSeqno] = ckpt.Dcp_snapshot_seqno
	ckpt_record_map[DcpSnapshotEndSeqno] = ckpt.Dcp_snapshot_end_seqno
	ckpt_record_map[TargetVbUUID] = ckpt.Target_vb_uuid
	ckpt_record_map[CommitOpaque] = ckpt.Commitopaque
	return ckpt_record_map
}

func NewCheckpointsDoc() *CheckpointsDoc {
	ckpt_doc := &CheckpointsDoc{Checkpoint_records: []*CheckpointRecord {},
		Revision: nil}

	for i := 0; i < 100; i++ {
		ckpt_doc.Checkpoint_records = append(ckpt_doc.Checkpoint_records, nil)
	}

	return ckpt_doc
}

//Not currentcy safe. It should be used by one goroutine only
func (ckptsDoc *CheckpointsDoc) AddRecord(record *CheckpointRecord) {
	if len(ckptsDoc.Checkpoint_records) > 0 {
		for i := len(ckptsDoc.Checkpoint_records) - 1; i >= 0; i-- {
			if i+1 < 100 {
				ckptsDoc.Checkpoint_records[i+1] = ckptsDoc.Checkpoint_records[i]
			}
		}
		ckptsDoc.Checkpoint_records[0] = record
	} else {
		ckptsDoc.Checkpoint_records = append(ckptsDoc.Checkpoint_records, record)
	}
}
