package conflictlog

import (
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
)

// ConflictMapper impements Mapper interface.
type conflictMapper struct {
	logger *log.CommonLogger
}

func NewConflictMapper(logger *log.CommonLogger) *conflictMapper {
	return &conflictMapper{logger: logger}
}

// returns the "target" to which the conflict record needs to be routed.
func (m *conflictMapper) Map(rules *base.ConflictLogRules, c Conflict) (target base.ConflictLogTarget, err error) {
	if rules == nil {
		err = ErrEmptyRules
		return
	}

	// If there are no special "loggingRules", rules.Target is the return target.
	target = rules.Target

	// Check for special "loggingRules" if any
	if len(rules.Mapping) == 0 {
		// consider as no loggingRules.
		return
	}

	source := base.CollectionNamespace{
		ScopeName:      c.Scope(),
		CollectionName: c.Collection(),
	}

	// look for exact match
	// This is have highest precedence over scope only match or fallback target.
	targetOverride, ok := rules.Mapping[source]
	if ok {
		target = targetOverride
		return
	}

	// look for scope only match.
	source.CollectionName = ""
	targetOverride, ok = rules.Mapping[source]
	if ok {
		target = targetOverride
	}

	return
}
