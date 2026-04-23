//go:build dev

package parts

import (
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
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

// injectColErr corrupts the collection ID prefix in the outgoing request's key so
// that target KV genuinely returns UNKNOWN_COLLECTION (the mutation is never stored).
// In XDCR, the collection ID is ULEB128-encoded as a prefix of req.Key (not in CollId).
// We flip bit 6 of the first byte, changing the collection ID without altering the
// encoding length, so the packet structure stays valid.
func (xmem *XmemNozzle) injectColErr(wrappedReq *base.WrappedMCRequest) {
	if devPercent := atomic.LoadUint32(&xmem.config.devColErrPercent); devPercent > 0 {
		wrappedReq.ColInfoMtx.RLock()
		hasColPrefix := wrappedReq.ColInfo != nil && wrappedReq.ColInfo.ColIDPrefixedKeyLen > 0
		wrappedReq.ColInfoMtx.RUnlock()
		if hasColPrefix && len(wrappedReq.Req.Key) > 0 {
			if uint32(rand.Intn(100)) < devPercent {
				// Flip bit 6 of the first byte of the ULEB128 collection ID prefix.
				// This changes the collection ID (e.g. 10 → 74) without altering
				// the encoding length, so the packet structure remains valid.
				wrappedReq.Req.Key[0] ^= 0x40
			}
		}
	}
}

