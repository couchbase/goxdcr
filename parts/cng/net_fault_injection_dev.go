//go:build dev

package cng

import (
	"context"
	"io"
	"math/rand"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
	"google.golang.org/grpc"
)

type cngNetFaultInjector struct {
	networkFaultPercent int
	listOfErrs          []error
	timeToWaitPerErr    []time.Duration

	rwMutex sync.RWMutex
}

func (i *cngNetFaultInjector) setupErrors(settings metadata.ReplicationSettingsMap) {
	// only update if present in the settings map
	_, ok := settings[base.DevXmemNozzleNetworkIOFaultProbability]
	if !ok {
		return
	}
	// ignore change tracking during init
	i.updateSettings(settings)
}

func (i *cngNetFaultInjector) updateSettings(settings metadata.ReplicationSettingsMap) (changed bool, old int, newV int) {
	i.rwMutex.Lock()
	defer i.rwMutex.Unlock()

	raw, ok := settings[base.DevXmemNozzleNetworkIOFaultProbability]
	if !ok {
		return false, 0, 0
	}
	percent, ok := raw.(int)
	if !ok {
		return false, 0, 0
	}
	if percent < 0 {
		percent = 0
	} else if percent > 100 {
		percent = 100
	}

	old = i.networkFaultPercent
	newV = percent
	changed = old != newV

	i.networkFaultPercent = percent
	if percent == 0 {
		i.listOfErrs = nil
		i.timeToWaitPerErr = nil
		return
	}

	i.listOfErrs = []error{
		io.EOF,
		io.ErrUnexpectedEOF,
		io.ErrClosedPipe,
		&net.OpError{Op: "rpc", Net: "tcp", Err: syscall.ECONNRESET},
		&net.OpError{Op: "rpc", Net: "tcp", Err: syscall.ECONNREFUSED},
		&net.OpError{Op: "rpc", Net: "tcp", Err: syscall.ETIMEDOUT},
		syscall.EPIPE,
	}

	// Define realistic time ranges for each error type
	timeRanges := []struct {
		min time.Duration
		max time.Duration
	}{
		{0, 10 * time.Millisecond},                      // EOF
		{0, 10 * time.Millisecond},                      // UnexpectedEOF
		{0, 10 * time.Millisecond},                      // ClosedPipe
		{50 * time.Millisecond, 500 * time.Millisecond}, // ECONNRESET
		{10 * time.Millisecond, 100 * time.Millisecond}, // ECONNREFUSED
		{1 * time.Second, 5 * time.Second},              // ETIMEDOUT
		{20 * time.Millisecond, 200 * time.Millisecond}, // EPIPE
	}

	i.timeToWaitPerErr = make([]time.Duration, len(i.listOfErrs))
	for idx := range i.listOfErrs {
		min := timeRanges[idx].min
		max := timeRanges[idx].max
		i.timeToWaitPerErr[idx] = min + time.Duration(rand.Int63n(int64(max-min+1)))
	}

	return
}

func (i *cngNetFaultInjector) maybeWrapClient(client base.CngClient) base.CngClient {
	var errToReturn error
	var timeToWait time.Duration

	i.rwMutex.RLock()
	if i.networkFaultPercent > 0 && len(i.listOfErrs) > 0 {
		if rand.Intn(100) < i.networkFaultPercent {
			idxToUse := rand.Intn(len(i.listOfErrs))
			errToReturn = i.listOfErrs[idxToUse]
			timeToWait = i.timeToWaitPerErr[idxToUse]
		}
	}
	i.rwMutex.RUnlock()

	if errToReturn == nil {
		return client
	}

	return &faultyCngClient{
		CngClient:   client,
		errToReturn: errToReturn,
		timeToWait:  timeToWait,
	}
}

type faultyCngClient struct {
	base.CngClient

	errToReturn error
	timeToWait  time.Duration
}

func (f *faultyCngClient) CheckDocument(ctx context.Context, in *internal_xdcr_v1.CheckDocumentRequest, opts ...grpc.CallOption) (*internal_xdcr_v1.CheckDocumentResponse, error) {
	time.Sleep(f.timeToWait)
	return nil, f.errToReturn
}

func (f *faultyCngClient) PushDocument(ctx context.Context, in *internal_xdcr_v1.PushDocumentRequest, opts ...grpc.CallOption) (*internal_xdcr_v1.PushDocumentResponse, error) {
	time.Sleep(f.timeToWait)
	return nil, f.errToReturn
}

type injectedCngConn struct {
	inner    cngConn
	injector *cngNetFaultInjector
}

func (c *injectedCngConn) Close() error {
	return c.inner.Close()
}

func (c *injectedCngConn) Client() base.CngClient {
	client := c.inner.Client()
	if c.injector == nil {
		return client
	}
	return c.injector.maybeWrapClient(client)
}

func (n *Nozzle) wrapConn(conn cngConn) cngConn {
	if conn == nil {
		return nil
	}
	return &injectedCngConn{inner: conn, injector: &n.netFaultInjector}
}
