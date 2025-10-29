package cng

import (
	"context"
	"time"
)

func (n *Nozzle) worker(ctx context.Context) {
	for {
		select {
		case <-n.stopCh:
			return
		case req := <-n.dataCh:
			trace := Trace{}
			childCtx := startTrace(ctx, &trace)
			err := n.processReq(childCtx, req)
			trace.commitTime = time.Since(req.Start_time)

			n.raiseEvents(req, &trace, err)

			n.RecycleDataObj(req)
		}
	}
}
