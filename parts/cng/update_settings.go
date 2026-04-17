package cng

import (
	"fmt"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
)

// UpdateSettings applies live configuration changes to the CNG nozzle.
// It supports:
// - base.CNGConnCountKey: adjusts the gRPC connection pool size (min 1)
// - base.CNGRPCDeadline: changes the RPC deadline for new outbound calls (ms, min 1)
// - metadata.OptimisticReplicationThresholdKey: changes optimistic replication threshold
func (n *Nozzle) UpdateSettings(settings metadata.ReplicationSettingsMap) error {
	if connCountRaw, ok := settings[base.CNGConnCountKey]; ok {
		if connCountInt, ok := connCountRaw.(int); ok {
			if connCountInt < 1 {
				connCountInt = 1
			}
			n.Logger().Infof("live update: setting conn count to %d", connCountInt)
			if n.connPool != nil {
				if err := n.connPool.ChangeConnCount(connCountInt); err != nil {
					return fmt.Errorf("failed to set conn count to %d: %w", connCountInt, err)
				}
			}
		}
	}

	if deadlineMsRaw, ok := settings[base.CNGRPCDeadline]; ok {
		if deadlineMsInt, ok := deadlineMsRaw.(int); ok {
			if deadlineMsInt < 1 {
				deadlineMsInt = 1
			}
			n.Logger().Infof("live update: setting RPC deadline to %d ms", deadlineMsInt)
			n.cfg.Tunables.rpcDeadlineMs.Store(int64(deadlineMsInt))
		}
	}

	if thresholdRaw, ok := settings[metadata.OptimisticReplicationThresholdKey]; ok {
		if thresholdInt, ok := thresholdRaw.(int); ok {
			old := n.cfg.Tunables.GetOptimisticThresholdSize()
			n.cfg.Tunables.SetOptimisticThresholdSize(thresholdInt)
			newV := n.cfg.Tunables.GetOptimisticThresholdSize()
			if old != newV {
				n.Logger().Infof("live update: optimistic replication threshold %d -> %d", old, newV)
			}
		}
	}

	n.updateDevInjections(settings)

	return nil
}

// getRPCDeadline returns the current RPC deadline in duration form.
func (n *Nozzle) getRPCDeadline() time.Duration {
	ms := n.cfg.Tunables.rpcDeadlineMs.Load()
	return time.Duration(ms) * time.Millisecond
}
