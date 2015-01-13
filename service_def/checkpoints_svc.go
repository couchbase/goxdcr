package service_def

import (
	"github.com/couchbase/goxdcr/metadata"
)

type CheckpointsService interface {
	CheckpointsDoc (replicationId string, vbno uint16) (*metadata.CheckpointsDoc, error)
	DelCheckpointsDocs (replicationId string) error
	UpsertCheckpoints (replicationId string, vbno uint16, ckpt_record *metadata.CheckpointRecord) (error)
	CheckpointsDocs (replicationId string) (map[uint16]*metadata.CheckpointsDoc, error)
}