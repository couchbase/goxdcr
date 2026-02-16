/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package conflictlog

import (
	"fmt"
	"strings"
	"time"
)

// LoggerOptions defines optional args for a logger implementation
type LoggerOptions struct {
	Rules                *Rules
	Mapper               Mapper
	LogQueueCap          int
	WorkerCount          int
	NetworkRetryCount    int
	NetworkRetryInterval time.Duration
	PoolGetTimeout       time.Duration
	SetMetaTimeout       time.Duration
	MaxErrorCount        int
	ErrorTimeWindow      time.Duration
	ReattemptDuration    time.Duration
	ReplPauseThreshold   int
	MonitorDuration      int

	// this is off by default and only present for
	// local testing changeable through internal setting.
	SkipTlsVerify bool
}

func (l *LoggerOptions) String() string {
	b := &strings.Builder{}

	b.WriteString(fmt.Sprintf("logQueueCap:%d,", l.LogQueueCap))
	b.WriteString(fmt.Sprintf("workerCount:%d,", l.WorkerCount))
	b.WriteString(fmt.Sprintf("networkRetryCount:%d,", l.NetworkRetryCount))
	b.WriteString(fmt.Sprintf("networkRetryInterval:%s,", l.NetworkRetryInterval))
	b.WriteString(fmt.Sprintf("poolGetTimeout:%s,", l.PoolGetTimeout))
	b.WriteString(fmt.Sprintf("setMetaTimeout:%s,", l.SetMetaTimeout))
	b.WriteString(fmt.Sprintf("maxErrorCount:%v,", l.MaxErrorCount))
	b.WriteString(fmt.Sprintf("errorTimeWindow:%v,", l.ErrorTimeWindow))
	b.WriteString(fmt.Sprintf("reattemptDuration:%v,", l.ReattemptDuration))
	b.WriteString(fmt.Sprintf("skipTlsVerify:%v,", l.SkipTlsVerify))
	b.WriteString(fmt.Sprintf("pauseThreshold:%v,", l.ReplPauseThreshold))
	b.WriteString(fmt.Sprintf("monitorDuration:%v", l.MonitorDuration))

	return b.String()
}

func WithRules(r *Rules) LoggerOpt {
	return func(o *LoggerOptions) {
		o.Rules = r
	}
}

func WithMapper(m Mapper) LoggerOpt {
	return func(o *LoggerOptions) {
		o.Mapper = m
	}
}

func WithCapacity(cap int) LoggerOpt {
	return func(o *LoggerOptions) {
		o.LogQueueCap = cap
	}
}

func WithWorkerCount(val int) LoggerOpt {
	return func(o *LoggerOptions) {
		o.WorkerCount = val
	}
}

func WithNetworkRetryCount(val int) LoggerOpt {
	return func(o *LoggerOptions) {
		o.NetworkRetryCount = val
	}
}

func WithNetworkRetryInterval(val time.Duration) LoggerOpt {
	return func(o *LoggerOptions) {
		o.NetworkRetryInterval = val
	}
}

func WithPoolGetTimeout(val time.Duration) LoggerOpt {
	return func(o *LoggerOptions) {
		o.PoolGetTimeout = val
	}
}

func WithSetMetaTimeout(val time.Duration) LoggerOpt {
	return func(o *LoggerOptions) {
		o.SetMetaTimeout = val
	}
}

func WithSkipTlsVerify(v bool) LoggerOpt {
	return func(o *LoggerOptions) {
		o.SkipTlsVerify = v
	}
}

func WithMaxErrorCount(val int) LoggerOpt {
	return func(o *LoggerOptions) {
		o.MaxErrorCount = val
	}
}

func WithErrorTimeWindow(val time.Duration) LoggerOpt {
	return func(o *LoggerOptions) {
		o.ErrorTimeWindow = val
	}
}

func WithReattemptDuration(val time.Duration) LoggerOpt {
	return func(o *LoggerOptions) {
		o.ReattemptDuration = val
	}
}

func WithAutopauseReplThreshold(val int) LoggerOpt {
	return func(o *LoggerOptions) {
		o.ReplPauseThreshold = val
	}
}

func WithAutopauseReplMonitorDuration(val int) LoggerOpt {
	return func(o *LoggerOptions) {
		o.MonitorDuration = val
	}
}

type LoggerOpt func(o *LoggerOptions)
