package conflictlog

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	baseclog "github.com/couchbase/goxdcr/v8/base/conflictlog"
)

// gLoggerId is the counter for all conflict loggers created
var gLoggerId int64

// LoggerOptions defines optional args for a logger implementation
type LoggerOptions struct {
	rules                *baseclog.Rules
	mapper               Mapper
	logQueueCap          int
	workerCount          int
	networkRetryCount    int
	networkRetryInterval time.Duration
	poolGetTimeout       time.Duration
	setMetaTimeout       time.Duration
	maxErrorCount        int
	errorTimeWindow      time.Duration
	reattemptDuration    time.Duration

	// this is off by default and only present for
	// local testing changeable through internal setting.
	skipTlsVerify bool
}

func (l *LoggerOptions) String() string {
	b := &strings.Builder{}

	b.WriteString(fmt.Sprintf("logQueueCap:%d,", l.logQueueCap))
	b.WriteString(fmt.Sprintf("workerCount:%d,", l.workerCount))
	b.WriteString(fmt.Sprintf("networkRetryCount:%d,", l.networkRetryCount))
	b.WriteString(fmt.Sprintf("networkRetryInterval:%s,", l.networkRetryInterval))
	b.WriteString(fmt.Sprintf("poolGetTimeout:%s,", l.poolGetTimeout))
	b.WriteString(fmt.Sprintf("setMetaTimeout:%s,", l.setMetaTimeout))
	b.WriteString(fmt.Sprintf("maxErrorCount:%v,", l.maxErrorCount))
	b.WriteString(fmt.Sprintf("errorTimeWindow:%v,", l.errorTimeWindow))
	b.WriteString(fmt.Sprintf("reattemptDuration:%v", l.reattemptDuration))
	b.WriteString(fmt.Sprintf("skipTlsVerify:%v", l.skipTlsVerify))

	return b.String()
}

func WithRules(r *baseclog.Rules) LoggerOpt {
	return func(o *LoggerOptions) {
		o.rules = r
	}
}

func WithMapper(m Mapper) LoggerOpt {
	return func(o *LoggerOptions) {
		o.mapper = m
	}
}

func WithCapacity(cap int) LoggerOpt {
	return func(o *LoggerOptions) {
		o.logQueueCap = cap
	}
}

func WithWorkerCount(val int) LoggerOpt {
	return func(o *LoggerOptions) {
		o.workerCount = val
	}
}

func WithNetworkRetryCount(val int) LoggerOpt {
	return func(o *LoggerOptions) {
		o.networkRetryCount = val
	}
}

func WithNetworkRetryInterval(val time.Duration) LoggerOpt {
	return func(o *LoggerOptions) {
		o.networkRetryInterval = val
	}
}

func WithPoolGetTimeout(val time.Duration) LoggerOpt {
	return func(o *LoggerOptions) {
		o.poolGetTimeout = val
	}
}

func WithSetMetaTimeout(val time.Duration) LoggerOpt {
	return func(o *LoggerOptions) {
		o.setMetaTimeout = val
	}
}

func WithSkipTlsVerify(v bool) LoggerOpt {
	return func(o *LoggerOptions) {
		o.skipTlsVerify = v
	}
}

func WithMaxErrorCount(val int) LoggerOpt {
	return func(o *LoggerOptions) {
		o.maxErrorCount = val
	}
}

func WithErrorTimeWindow(val time.Duration) LoggerOpt {
	return func(o *LoggerOptions) {
		o.errorTimeWindow = val
	}
}

func WithReattemptDuration(val time.Duration) LoggerOpt {
	return func(o *LoggerOptions) {
		o.reattemptDuration = val
	}
}

type LoggerOpt func(o *LoggerOptions)

// newLoggerId generates new unique logger Id. This is used by the implementations
// of the Logger interface
func newLoggerId() int64 {
	return atomic.AddInt64(&gLoggerId, 1)
}
