package cng

import (
	"context"
	"fmt"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/common"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/parts"
)

func (n *Nozzle) Start(settings metadata.ReplicationSettingsMap) (err error) {
	// Since the same nozzle instance is used for mulltiple source VBs,
	// we need to make sure Start() is only called once
	n.startOnce.Do(func() {
		n.Logger().Infof("starting nozzle")
		ctx := context.Background()
		err = n.SetState(common.Part_Starting)
		if err != nil {
			n.Logger().Errorf("failed to set state for CNG nozzle to Starting, err: %v", err)
			return
		}

		err = n.startInner(ctx, settings)
		if err != nil {
			n.Logger().Errorf("failed to start CNG nozzle, err: %v", err)
			return
		}

		err = n.SetState(common.Part_Running)
		if err != nil {
			return
		}
	})
	return
}

func (n *Nozzle) startInner(ctx context.Context, settings metadata.ReplicationSettingsMap) (err error) {
	if err = n.initConfig(settings); err != nil {
		return
	}

	n.Logger().Infof("nozzle tunables: %s", n.cfg.Tunables.String())
	if err = n.cfg.Tunables.Validate(); err != nil {
		return
	}

	if err = n.initConnPool(); err != nil {
		return
	}

	n.dataCh = make(chan *base.WrappedMCRequest, n.cfg.Tunables.DataChanSize)

	n.initWorkers(ctx)

	return
}

func (n *Nozzle) initConfig(settings metadata.ReplicationSettingsMap) (err error) {
	if n.cfg.Tunables.WorkerCount, err = getSetting[int](settings, base.CNGWorkerCountKey); err != nil {
		return
	}

	if n.cfg.Tunables.DataChanSize, err = getSetting[int](settings, base.CNGQueueSizeKey); err != nil {
		return
	}

	if n.cfg.Tunables.ConnCount, err = getSetting[int](settings, base.CNGConnCountKey); err != nil {
		return
	}

	var deadlineMs int
	if deadlineMs, err = getSetting[int](settings, base.CNGRPCDeadline); err != nil {
		return
	}

	n.cfg.Tunables.Deadline = time.Duration(deadlineMs) * time.Millisecond

	// CNG TODO: make retry interval configurable
	n.cfg.Tunables.RetryInterval = base.DefaultCNGRetryInterval
	// CNG TODO: remove this
	n.cfg.Tunables.InsecureSkipVerify = true

	if val, ok := settings[parts.SETTING_OPTI_REP_THRESHOLD]; ok {
		n.cfg.Tunables.OptimisticThresholdSize = val.(int)
	}

	return
}

func (n *Nozzle) initWorkers(ctx context.Context) {
	n.Logger().Infof("starting workers, count=%d", n.cfg.Tunables.WorkerCount)
	for i := 0; i < n.cfg.Tunables.WorkerCount; i++ {
		go n.worker(ctx)
	}
}

func (n *Nozzle) initConnPool() (err error) {
	n.Logger().Infof("starting conn pool, connCount=%d, retryInterval=%d",
		n.cfg.Tunables.ConnCount, n.cfg.Tunables.RetryInterval)
	poolCfg := &PoolConfig{
		ConnCount:     n.cfg.Tunables.ConnCount,
		ConnFn:        n.newCNGClient,
		RetryInterval: n.cfg.Tunables.RetryInterval,
		UtilsSvc:      n.cfg.Services.Utils,
	}

	n.connPool, err = NewConnPool(n.Logger(), poolCfg)
	if err != nil {
		return fmt.Errorf("failed to create connection pool: %v", err)
	}
	return
}

func (n *Nozzle) Close() (err error) {
	return
}

func (n *Nozzle) Stop() (err error) {
	n.Logger().Infof("stopping nozzle")
	err = n.SetState(common.Part_Stopping)
	if err != nil {
		return
	}

	close(n.stopCh)
	n.connPool.Close()

	err = n.SetState(common.Part_Stopped)
	if err != nil {
		return
	}
	n.Logger().Infof("stopped nozzle")
	return
}
func (n *Nozzle) IsOpen() bool {
	// CNG TODO: implement proper IsOpen
	return true
}
func (n *Nozzle) Open() (err error) {
	// CNG TODO: implement proper Open
	return
}

func getSetting[T ~int | ~bool | ~string](m metadata.ReplicationSettingsMap, key string) (ret T, err error) {
	var val any
	val, ok := m[key]
	if !ok {
		return ret, fmt.Errorf("setting not found")
	}
	ret, ok = val.(T)
	if !ok {
		return ret, fmt.Errorf("setting %s not of expected type, got %T", key, val)
	}

	return
}
