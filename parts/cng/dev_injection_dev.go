//go:build dev

package cng

import (
	"fmt"
	"io"
	"math/rand"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const devInjectionsEnabled = true

func (t *Tunables) devParamsString(w io.Writer) {
	fmt.Fprintf(w, ", DevMainSendDelay=%dms, ", t.devMainSendDelayMs.Load())
	fmt.Fprintf(w, "DevBackfillSendDelay=%dms, ", t.devBackfillSendDelayMs.Load())
	fmt.Fprintf(w, "DevColErrorPercent=%d%%", t.devColErrPercent.Load())
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

	if raw, ok := settings[base.DevXmemColErrorPercent]; ok {
		if v, ok := raw.(int); ok {
			if v < 0 {
				v = 0
			} else if v > 100 {
				v = 100
			}
			n.cfg.Tunables.devColErrPercent.Store(int64(v))
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

	if raw, ok := settings[base.DevXmemColErrorPercent]; ok {
		if v, ok := raw.(int); ok {
			if v < 0 {
				v = 0
			} else if v > 100 {
				v = 100
			}
			old := n.cfg.Tunables.devColErrPercent.Load()
			n.cfg.Tunables.devColErrPercent.Store(int64(v))
			if old != int64(v) {
				n.Logger().Infof("live update: collection error injection percent %d%% -> %d%%", old, v)
			}
		}
	}

	n.Logger().Infof("nozzle tunables after update: %s", n.cfg.Tunables.String())
}

func (n *Nozzle) maybeInjectCollectionNotFound(req *base.WrappedMCRequest) error {
	if n == nil || n.cfg == nil {
		return nil
	}

	p := n.cfg.Tunables.devColErrPercent.Load()
	if p <= 0 {
		return nil
	}

	if p < 100 && rand.Intn(100) >= int(p) {
		return nil
	}

	if req == nil || req.TgtColNamespace == nil {
		return nil
	}

	resourceName := fmt.Sprintf("%s/%s/%s", n.cfg.Replication.TargetBucketName, req.TgtColNamespace.ScopeName, req.TgtColNamespace.CollectionName)
	st := status.New(codes.NotFound, "Collection not found")
	st, _ = st.WithDetails(&errdetails.ResourceInfo{
		ResourceType: ResourceTypeCollection,
		ResourceName: resourceName,
	})
	return st.Err()
}
