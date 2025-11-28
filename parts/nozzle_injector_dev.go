//go:build dev

/*
Copyright 2025-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package parts

import (
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
)

type BaseConfigDevInjector struct{}

func NewBaseConfigInjector() *BaseConfigDevInjector {
	return &BaseConfigDevInjector{}
}

func (b *BaseConfigDevInjector) InitInjector(cfg *baseConfig, settings metadata.ReplicationSettingsMap) {
	if val, ok := settings[XMEM_DEV_MAIN_SLEEP_DELAY]; ok {
		cfg.devMainSendDelay = uint32(val.(int))
	}
	if val, ok := settings[XMEM_DEV_BACKFILL_SLEEP_DELAY]; ok {
		cfg.devBackfillSendDelay = uint32(val.(int))
	}
}

func (b *BaseConfigDevInjector) Update(cfg *baseConfig, settings metadata.ReplicationSettingsMap, logger *log.CommonLogger, id string) {
	devMainDelay, ok := settings[XMEM_DEV_MAIN_SLEEP_DELAY]
	if ok {
		devMainDelayInt := devMainDelay.(int)
		oldMainDelayInt := int(atomic.LoadUint32(&cfg.devMainSendDelay))
		atomic.StoreUint32(&cfg.devMainSendDelay, uint32(devMainDelayInt))
		if oldMainDelayInt != devMainDelayInt {
			logger.Infof("%v updated main pipeline delay to %v millisecond(s)\n", id, devMainDelayInt)
		}
	}
	devBackfillDelay, ok := settings[XMEM_DEV_BACKFILL_SLEEP_DELAY]
	if ok {
		devBackfillDelayInt := devBackfillDelay.(int)
		oldBackfillDelayInt := int(atomic.LoadUint32(&cfg.devBackfillSendDelay))
		atomic.StoreUint32(&cfg.devBackfillSendDelay, uint32(devBackfillDelayInt))
		if oldBackfillDelayInt != devBackfillDelayInt {
			logger.Infof("%v updated backfill pipeline delay to %v millisecond(s)\n", id, devBackfillDelay)
		}
	}
}

type DcpNozzleDevInjector struct{}

func NewDcpNozzleInjector() *DcpNozzleDevInjector {
	return &DcpNozzleDevInjector{}
}

func (d *DcpNozzleDevInjector) Init(dcp *DcpNozzle, settings metadata.ReplicationSettingsMap) {
	if val, ok := settings[DCP_DEV_MAIN_ROLLBACK_VB]; ok {
		intVal, ok2 := val.(int)
		if ok2 && intVal >= 0 {
			dcp.devInjectionMainRollbackVb = intVal
		}
	}

	if val, ok := settings[DCP_DEV_BACKFILL_ROLLBACK_VB]; ok {
		intVal, ok2 := val.(int)
		if ok2 && intVal >= 0 {
			dcp.devInjectionBackfillRollbackVb = intVal
		}
	}
}

type XmemNozzleDevInjector struct {
	networkFaultPercent int
	listOfErrs          []error
	timeToWaitPerErr    []time.Duration

	rwMutex sync.RWMutex
}

type injectionConn struct {
	io.ReadWriteCloser

	errToReturn error
	timeToWait  time.Duration
}

func (i *injectionConn) Read(p []byte) (n int, err error) {
	time.Sleep(i.timeToWait)
	return 0, i.errToReturn
}

func (i *injectionConn) Write(p []byte) (n int, err error) {
	time.Sleep(i.timeToWait)
	return 0, i.errToReturn
}

// Close is fine
func (i *injectionConn) Close() error {
	return nil
}

func NewXmemNozzleInjector() *XmemNozzleDevInjector {
	return &XmemNozzleDevInjector{}
}

func (x *XmemNozzleDevInjector) Init(settings metadata.ReplicationSettingsMap) {
	x.setupErrors(settings)
}

func (x *XmemNozzleDevInjector) setupErrors(settings metadata.ReplicationSettingsMap) {
	x.rwMutex.Lock()
	defer x.rwMutex.Unlock()

	if val, ok := settings[XMEM_DEV_NETWORK_IO_FAULT_PROBABILITY]; ok {
		x.networkFaultPercent = val.(int)
		x.listOfErrs = []error{
			io.EOF,
			io.ErrUnexpectedEOF,
			io.ErrClosedPipe,
			syscall.ECONNRESET,
			syscall.ECONNREFUSED,
			syscall.ETIMEDOUT,
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

		x.timeToWaitPerErr = make([]time.Duration, len(x.listOfErrs))
		for i := range x.listOfErrs {
			min := timeRanges[i].min
			max := timeRanges[i].max
			// Generate a random duration within the specified range
			x.timeToWaitPerErr[i] = min + time.Duration(rand.Int63n(int64(max-min+1)))
		}

	}
}

func (x *XmemNozzleDevInjector) Update(settings metadata.ReplicationSettingsMap) {
	x.setupErrors(settings)
}

func (x *XmemNozzleDevInjector) GetConn(client *base.XmemClient, readTimeout bool, writeTimeout bool) (io.ReadWriteCloser, int, error) {
	var errToReturn error
	var timeToWait time.Duration

	x.rwMutex.RLock()
	if x.networkFaultPercent > 0 {
		if rand.Intn(100) < x.networkFaultPercent {
			// Pick a random error to inject
			idxToUse := rand.Intn(len(x.listOfErrs))
			errToReturn = x.listOfErrs[idxToUse]
			timeToWait = x.timeToWaitPerErr[idxToUse]
		}
	}
	x.rwMutex.RUnlock()

	if errToReturn != nil {
		faultyConn := &injectionConn{
			errToReturn: errToReturn,
			timeToWait:  timeToWait,
		}
		return faultyConn, 0, nil
	}
	return client.GetConn(readTimeout, writeTimeout)
}
