//go:build !dev

package cng

import "context"

func (n *Nozzle) withSendDelayOnce(ctx context.Context) context.Context {
	return ctx
}

func (n *Nozzle) checkSendDelayInjection(ctx context.Context) {
	// no-op
}
