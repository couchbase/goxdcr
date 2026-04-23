//go:build !dev

package parts

import "github.com/couchbase/goxdcr/v8/base"

func (xmem *XmemNozzle) checkSendDelayInjection() {
}

func (xmem *XmemNozzle) injectColErr(_ *base.WrappedMCRequest) {
}

