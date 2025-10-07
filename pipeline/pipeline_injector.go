package pipeline

import (
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
)

type PipelineInjector interface {
	InjectDevPreCheckVBPoison(settings *metadata.ReplicationSettings, b *base.VbSeqnoMapType)
}
