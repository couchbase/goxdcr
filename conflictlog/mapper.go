package conflictlog

import (
	baseclog "github.com/couchbase/goxdcr/v8/base/conflictlog"
	"github.com/couchbase/goxdcr/v8/log"
)

// Mapper evaluates and routes the conflict to the right target bucket
// It also stores the rules against which the mapping will happen
type Mapper interface {
	// Map evaluates and routes the conflict to the right target bucket
	Map(*baseclog.Rules, baseclog.Conflict) (baseclog.Target, error)
}

type FixedMapper struct {
	target baseclog.Target
	logger *log.CommonLogger
}

func NewFixedMapper(logger *log.CommonLogger, target baseclog.Target) *FixedMapper {
	return &FixedMapper{logger: logger, target: target}
}

func (m *FixedMapper) Map(_ *baseclog.Rules, c baseclog.Conflict) (baseclog.Target, error) {
	return m.target, nil
}
