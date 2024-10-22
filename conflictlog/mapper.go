package conflictlog

import (
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
)

// Mapper evaluates and routes the conflict to the right target bucket
// It also stores the rules against which the mapping will happen
type Mapper interface {
	// Map evaluates and routes the conflict to the right target bucket
	Map(*base.ConflictLogRules, Conflict) (base.ConflictLogTarget, error)
}

type FixedMapper struct {
	target base.ConflictLogTarget
	logger *log.CommonLogger
}

func NewFixedMapper(logger *log.CommonLogger, target base.ConflictLogTarget) *FixedMapper {
	return &FixedMapper{logger: logger, target: target}
}

func (m *FixedMapper) Map(_ *base.ConflictLogRules, c Conflict) (base.ConflictLogTarget, error) {
	return m.target, nil
}
