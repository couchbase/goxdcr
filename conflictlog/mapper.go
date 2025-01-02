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
