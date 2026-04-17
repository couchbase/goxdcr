//go:build dev

package cng

import (
	"fmt"
	"io"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
)

func (t *Tunables) devParamsString(w io.Writer) {
	fmt.Fprintf(w, "DevMainSendDelay=%dms, ", t.devMainSendDelayMs.Load())
	fmt.Fprintf(w, "DevBackfillSendDelay=%dms", t.devBackfillSendDelayMs.Load())
}

func (n *Nozzle) initDevInjections(settings metadata.ReplicationSettingsMap) {
	if n == nil || n.cfg == nil {
		return
	}

	n.netFaultInjector.setupErrors(settings)

	if raw, ok := settings[base.DevMainPipelineSendDelay]; ok {
		if v, ok := raw.(int); ok {
			if v < 0 {
				v = 0
			}
			n.cfg.Tunables.devMainSendDelayMs.Store(int64(v))
		}
	}

	if raw, ok := settings[base.DevBackfillPipelineSendDelay]; ok {
		if v, ok := raw.(int); ok {
			if v < 0 {
				v = 0
			}
			n.cfg.Tunables.devBackfillSendDelayMs.Store(int64(v))
		}
	}
}

func (n *Nozzle) updateDevInjections(settings metadata.ReplicationSettingsMap) {
	if n == nil || n.cfg == nil {
		return
	}

	if changed, old, newV := n.netFaultInjector.updateSettings(settings); changed {
		n.Logger().Infof("live update: CNG network I/O fault injection probability %d%% -> %d%%", old, newV)
	}

	if raw, ok := settings[base.DevMainPipelineSendDelay]; ok {
		if v, ok := raw.(int); ok {
			if v < 0 {
				v = 0
			}
			old := n.cfg.Tunables.devMainSendDelayMs.Load()
			n.cfg.Tunables.devMainSendDelayMs.Store(int64(v))
			if old != int64(v) {
				n.Logger().Infof("live update: main pipeline send delay to %d millisecond(s)", v)
			}
		}
	}

	if raw, ok := settings[base.DevBackfillPipelineSendDelay]; ok {
		if v, ok := raw.(int); ok {
			if v < 0 {
				v = 0
			}
			old := n.cfg.Tunables.devBackfillSendDelayMs.Load()
			n.cfg.Tunables.devBackfillSendDelayMs.Store(int64(v))
			if old != int64(v) {
				n.Logger().Infof("live update: backfill pipeline send delay to %d millisecond(s)", v)
			}
		}
	}
}
