//go:build dev

package parts

import (
	"sync/atomic"
	"time"

	"github.com/couchbase/goxdcr/v8/common"
)

func (xmem *XmemNozzle) checkSendDelayInjection() {
	if atomic.LoadUint32(&xmem.config.devBackfillSendDelay) > 0 {
		if _, pipelineType := common.DecomposeFullTopic(xmem.topic); pipelineType == common.BackfillPipeline {
			time.Sleep(time.Duration(atomic.LoadUint32(&xmem.config.devBackfillSendDelay)) * time.Millisecond)
		}
	}
	if atomic.LoadUint32(&xmem.config.devMainSendDelay) > 0 {
		if _, pipelineType := common.DecomposeFullTopic(xmem.topic); pipelineType == common.MainPipeline {
			time.Sleep(time.Duration(atomic.LoadUint32(&xmem.config.devMainSendDelay)) * time.Millisecond)
		}
	}
}
