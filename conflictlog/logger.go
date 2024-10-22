package conflictlog

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
)

// gLoggerId is the counter for all conflict loggers created
var gLoggerId int64

// Logger interface allows logging of conflicts in an abstracted manner
type Logger interface {
	// Id() returns the unique id for the logger
	Id() int64

	// Log writes the conflict to the conflict buccket
	Log(c *ConflictRecord) (base.ConflictLoggerHandle, error)

	// UpdateWorkerCount changes the underlying log worker count
	UpdateWorkerCount(count int)

	// UpdateRules allow updates to the the rules which map
	// the conflict to the target conflict bucket
	UpdateRules(*base.ConflictLogRules) error

	// Closes the logger. Hence forth the logger will error out
	Close() error
}

// LoggerOptions defines optional args for a logger implementation
type LoggerOptions struct {
	rules                *base.ConflictLogRules
	mapper               Mapper
	logQueueCap          int
	workerCount          int
	networkRetryCount    int
	networkRetryInterval time.Duration
	poolGetTimeout       time.Duration
	setMetaTimeout       time.Duration
	skipTlsVerify        bool
}

func (l *LoggerOptions) String() string {
	b := &strings.Builder{}

	b.WriteString(fmt.Sprintf("logQueueCap:%d,", l.logQueueCap))
	b.WriteString(fmt.Sprintf("workerCount:%d,", l.workerCount))
	b.WriteString(fmt.Sprintf("networkRetryCount:%d,", l.networkRetryCount))
	b.WriteString(fmt.Sprintf("networkRetryInterval:%s,", l.networkRetryInterval))
	b.WriteString(fmt.Sprintf("poolGetTimeout:%s,", l.poolGetTimeout))
	b.WriteString(fmt.Sprintf("setMetaTimeout:%s,", l.setMetaTimeout))
	b.WriteString(fmt.Sprintf("skipTlsVerify:%v", l.skipTlsVerify))

	return b.String()
}

func WithRules(r *base.ConflictLogRules) LoggerOpt {
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

func (o *LoggerOptions) SetRules(rules *base.ConflictLogRules) {
	o.rules = rules
}

func (o *LoggerOptions) SetMapper(mapper Mapper) {
	o.mapper = mapper
}

func (o *LoggerOptions) SetLogQueueCap(cap int) {
	o.logQueueCap = cap
}

type LoggerOpt func(o *LoggerOptions)

// newLoggerId generates new unique logger Id. This is used by the implementations
// of the Logger interface
func newLoggerId() int64 {
	return atomic.AddInt64(&gLoggerId, 1)
}
