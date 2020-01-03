package service_def

import (
	"github.com/couchbase/goxdcr/metadata"
)

type CurrentSeqnoGetter func(topic string) (map[uint16]uint64, error)

type BackfillMgrIface interface {
	Start() error
	RequestIncrementalBucketBackfill(topic string, request metadata.VBucketBackfillMap) error
	SetBackfillPipelineController(pipelineMgr BackfillPipelineMgr)
}

type BackfillPipelineMgr interface {
	UpdateBackfillPipeline(topic string, backfillSpec *metadata.BackfillReplicationSpec) error
}
