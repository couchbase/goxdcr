//go:build dev

package cng

import (
	"context"
	"time"

	"github.com/couchbase/goxdcr/v8/common"
)

type sendDelayOnceCtxKey struct{}

func (n *Nozzle) withSendDelayOnce(ctx context.Context) context.Context {
	didDelay := false
	return context.WithValue(ctx, sendDelayOnceCtxKey{}, &didDelay)
}

func (n *Nozzle) checkSendDelayInjection(ctx context.Context) {
	if n == nil || n.cfg == nil {
		return
	}

	if didDelayPtr, ok := ctx.Value(sendDelayOnceCtxKey{}).(*bool); ok && didDelayPtr != nil {
		if *didDelayPtr {
			return
		}
		defer func() {
			*didDelayPtr = true
		}()
	}

	topic := n.cfg.Replication.Topic

	delayMs := int64(0)
	if topic != "" {
		_, pipelineType := common.DecomposeFullTopic(topic)
		if pipelineType == common.BackfillPipeline {
			delayMs = n.cfg.Tunables.devBackfillSendDelayMs.Load()
		} else {
			delayMs = n.cfg.Tunables.devMainSendDelayMs.Load()
		}
	} else {
		delayMs = n.cfg.Tunables.devMainSendDelayMs.Load()
	}

	if delayMs > 0 {
		time.Sleep(time.Duration(delayMs) * time.Millisecond)
	}
}
