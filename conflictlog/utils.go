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

	"github.com/couchbase/goxdcr/v8/base"
	baseclog "github.com/couchbase/goxdcr/v8/base/conflictlog"
	"github.com/couchbase/goxdcr/v8/common"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
)

// returns a logger only if non-null rules are parsed without any errors.
func NewLoggerWithRules(conflictLoggingMap base.ConflictLoggingMappingInput, replId string, settings *metadata.ReplicationSettings, logger_ctx *log.LoggerContext, logger *log.CommonLogger, eventsProducer common.PipelineEventsProducer) (baseclog.Logger, error) {
	if conflictLoggingMap == nil {
		return nil, fmt.Errorf("nil conflictLoggingMap")
	}

	conflictLoggingEnabled := !conflictLoggingMap.Disabled()
	if !conflictLoggingEnabled {
		logger.Infof("Conflict logger will be off for pipeline=%s, with input=%v", replId, conflictLoggingMap)
		return nil, baseclog.ErrConflictLoggingIsOff
	}

	var rules *baseclog.Rules
	var err error
	rules, err = baseclog.ParseRules(conflictLoggingMap)
	if err != nil {
		return nil, fmt.Errorf("error converting %v to rules, err=%v", conflictLoggingMap, err)
	}

	if rules == nil {
		return nil, fmt.Errorf("%v maps to nil rules", conflictLoggingMap)
	}

	var clm Manager
	var conflictLogger baseclog.Logger
	clm, err = GetManager()
	if err != nil {
		return nil, fmt.Errorf("error getting conflict logging manager. err=%v", err)
	}

	fileLogger := log.NewLogger(ConflictLoggerName, logger_ctx)
	conflictLogger, err = clm.NewLogger(
		fileLogger,
		replId,
		eventsProducer,
		WithMapper(NewConflictMapper(logger)),
		WithRules(rules),
		WithCapacity(settings.GetCLogQueueCapacity()),
		WithWorkerCount(settings.GetCLogWorkerCount()),
		WithSetMetaTimeout(settings.GetCLogSetMetaTimeout()),
		WithPoolGetTimeout(settings.GetCLogPoolGetTimeout()),
		WithNetworkRetryInterval(settings.GetCLogNetworkRetryInterval()),
		WithNetworkRetryCount(settings.GetCLogNetworkRetryCount()),
		WithMaxErrorCount(settings.GetCLogMaxErrorCount()),
		WithErrorTimeWindow(settings.GetCLogErrorTimeWindow()),
		WithReattemptDuration(settings.GetCLogReattemptDuration()),
	)
	if err != nil {
		return nil, fmt.Errorf("error getting a new conflict logger for %v. err=%v", conflictLoggingMap, err)
	}

	logger.Infof("Conflict logger will be on for pipeline=%s, with rules=%s for input=%v", replId, rules, conflictLoggingMap)

	return conflictLogger, nil
}

// updates the input logger with the new rules,
// only if non-null rules are parsed without any errors.
func UpdateLoggerWithRules(conflictLoggingMap base.ConflictLoggingMappingInput, exisitingLogger baseclog.Logger, replId string, logger *log.CommonLogger) error {
	if exisitingLogger == nil {
		return fmt.Errorf("nil logger, quit updating")
	}

	if conflictLoggingMap == nil {
		return fmt.Errorf("nil conflictLoggingMap")
	}

	conflictLoggingEnabled := !conflictLoggingMap.Disabled()
	if !conflictLoggingEnabled {
		logger.Infof("Conflict logger will be off with input=%v", conflictLoggingMap)
		return baseclog.ErrConflictLoggingIsOff
	}

	var rules *baseclog.Rules
	var err error
	rules, err = baseclog.ParseRules(conflictLoggingMap)
	if err != nil {
		return fmt.Errorf("error converting %v to rules, err=%v", conflictLoggingMap, err)
	}

	if rules == nil {
		return fmt.Errorf("%v maps to nil rules", conflictLoggingMap)
	}

	err = exisitingLogger.UpdateRules(rules)
	if err != nil {
		if err == baseclog.ErrNoChange {
			return nil
		}
		return fmt.Errorf("error updating %s rules, err=%v", rules, err)
	}

	logger.Infof("Conflict logger updated for pipeline=%s, with rules=%s for input=%v", replId, rules, conflictLoggingMap)

	return nil
}

// Inserts "_xdcr_conflict": true to the input byte slice.
// If any error occurs, the original body is returned.
// Otherwise, returns new body and new datatype after xattr is successfully added.
func InsertConflictXattrToBody(body []byte, datatype uint8) ([]byte, uint8, error) {
	newbodyLen := len(body) + MaxBodyIncrease
	// TODO - Use datapool.
	newbody := make([]byte, newbodyLen)

	xattrComposer := base.NewXattrComposer(newbody)

	if base.HasXattr(datatype) {
		// insert the already existing xattrs
		it, err := base.NewXattrIterator(body)
		if err != nil {
			return body, datatype, err
		}

		for it.HasNext() {
			key, val, err := it.Next()
			if err != nil {
				return body, datatype, err
			}
			err = xattrComposer.WriteKV(key, val)
			if err != nil {
				return body, datatype, err
			}
		}
	}

	err := xattrComposer.WriteKV(base.ConflictLoggingXattrKeyBytes, base.ConflictLoggingXattrValBytes)
	if err != nil {
		return body, datatype, err
	}

	docWithoutXattr := base.FindDocBodyWithoutXattr(body, datatype)
	out, atLeastOneXattr := xattrComposer.FinishAndAppendDocValue(docWithoutXattr, nil, nil)

	if atLeastOneXattr {
		datatype = datatype | base.PROTOCOL_BINARY_DATATYPE_XATTR
	} else {
		// odd - shouldn't happen.
		datatype = datatype & ^(base.PROTOCOL_BINARY_DATATYPE_XATTR)
	}

	body = nil // no use of this body anymore, set to nil to help GC quicker.

	return out, datatype, err
}

type ConflictDocType int

const (
	SourceDoc ConflictDocType = iota
	TargetDoc ConflictDocType = iota
	CRD       ConflictDocType = iota
)

type writeError int

const (
	unknownErr   writeError = iota
	noRetryErr   writeError = iota
	needRetryErr writeError = iota
	networkErr   writeError = iota
)

func (w writeError) isNWError() bool {
	return w == networkErr
}

func (w writeError) noRetryNeeded() bool {
	return w == noRetryErr
}

type CLogReqT struct {
	Vbno  uint16
	Seqno uint64
}

type CLogRespT struct {
	// if throughSeqnoRelated=true, vbno and seqno are populated.
	ThroughSeqnoRelated bool
	Vbno                uint16
	Seqno               uint64

	// errors for statistics
	Err     error
	NwError bool

	// hibernation status update, running <-> hibernated.
	CLogStatusRelated bool
	CLogStatus        base.CLogHibernationStatusGauge
}

var GenerateAsyncListenerId func(common.Pipeline, string, int) string
