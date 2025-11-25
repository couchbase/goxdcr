package cng

import (
	"fmt"
	"sync"

	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	_ "github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/common"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/parts"
	"github.com/couchbase/goxdcr/v8/service_def"
	_ "google.golang.org/grpc"
	_ "google.golang.org/grpc/credentials"
)

type XDCRClient internal_xdcr_v1.XdcrServiceClient

type CNGContextKey string

const (
	MutationTraceContextKey CNGContextKey = "mtrace"
)

type Nozzle struct {
	parts.AbstractPart
	cfg   Config
	stats *Stats

	upstreamObjRecycler func(any)
	connPool            *ConnPool
	dataCh              chan *base.WrappedMCRequest
	stopCh              chan struct{}

	// used to make sure Start() is only called once
	startOnce sync.Once
}

// New creates a new nozzle for CNG protocol
func New(id string, loggerContext *log.LoggerContext, cfg Config) (n *Nozzle, err error) {
	if id == "" {
		return nil, fmt.Errorf("id is empty")
	}
	if loggerContext == nil {
		return nil, fmt.Errorf("loggerContext is nil")
	}

	if err = cfg.Replication.Validate(); err != nil {
		err = fmt.Errorf("Replication config validation failed: %w", err)
		return
	}
	if err = cfg.Services.Validate(); err != nil {
		err = fmt.Errorf("Services validation failed: %w", err)
		return
	}

	if len(cfg.Replication.vbUUIDMap) == 0 {
		err = fmt.Errorf("VBUUID map is empty in replication config")
		return
	}

	loggerContext.AddMoreContext(map[string]string{
		"id": id,
	})

	logger := log.NewLogger("CNGNozzle", loggerContext)

	n = &Nozzle{
		AbstractPart: parts.NewAbstractPartWithLogger(id, logger),
		cfg:          cfg,
		stopCh:       make(chan struct{}),
		stats:        NewStats(),
	}

	return
}

func (n *Nozzle) RecycleDataObj(obj any) {
	// recycler should never be nil
	if n.upstreamObjRecycler == nil {
		n.Logger().Errorf("upstreamObjRecycler is nil, panicking")
		panic("upstreamObjRecycler is nil")
	}
	n.upstreamObjRecycler(obj)
}

func (n *Nozzle) Receive(data any) error {
	req, ok := data.(*base.WrappedMCRequest)
	if !ok {
		return fmt.Errorf("Invalid data type %T, expected *base.WrappedMCRequest", data)
	}

	enqueued := true
	// try to enqueue without blocking first
	select {
	case n.dataCh <- req:
		return nil
	case <-n.stopCh:
		return fmt.Errorf("Nozzle %v is stopping", n.Id())
	default:
		enqueued = false
		n.stats.IncEnqueueBlocked(1)
	}

	if enqueued {
		return nil
	}

	// try again, blocking this time
	select {
	case n.dataCh <- req:
		return nil
	case <-n.stopCh:
		return fmt.Errorf("Nozzle %v is stopping", n.Id())
	}
}

func (n *Nozzle) ResponsibleVBs() (vbs []uint16) {
	for i := range uint16(1024) {
		vbs = append(vbs, i)
	}

	return
}

func (n *Nozzle) GetConflictLogger() any {
	return nil
}

func (n *Nozzle) SetUpstreamObjRecycler(recycler func(any)) {
	n.Logger().Infof("setting upstream obj")
	n.upstreamObjRecycler = recycler
}

func (n *Nozzle) SetUpstreamErrReporter(reporter func(any), vbs base.Uint16List) {}

func (n *Nozzle) SetConflictLogger(logger any) error {
	return nil
}

func (n *Nozzle) SetBandwidthThrottler(bandwidthThrottler service_def.BandwidthThrottlerSvc) {
	n.cfg.Services.BWThrottler = bandwidthThrottler
}

func (n *Nozzle) PrintStatusSummary() {
	if n.State() == common.Part_Running {
		n.stats.SetItemsInQueue(uint64(len(n.dataCh)))

		n.Logger().Infof("Nozzle stats: %v", n.stats.String())
	} else {
		n.Logger().Infof("Nozzle state: %v", n.State())
	}
}
