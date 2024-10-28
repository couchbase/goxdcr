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
	"errors"
	"fmt"
	"strings"

	"github.com/couchbase/goxdcr/v8/base"
)

var (
	ErrEmptyScope              error = errors.New("conflict logging scope should not be empty")
	ErrIncompleteTarget        error = errors.New("one or more of target bucket, scope or collection is empty")
	ErrInvalidLoggingRulesType error = errors.New("invalid logging rules type")
	ErrInvalidTargetType       error = errors.New("invalid target type")
	ErrInvalidCollection       error = errors.New("conflict logging collection not provided or wrong format")
	ErrNilMapping              error = errors.New("conflict logging mapping input should not be nil")
	ErrSystemNamespace         error = errors.New("conflict logging mapping target cannot be system scope or system collection")
)

// Rules captures the conflict logging rules for a replication
type Rules struct {
	// Target is the default or fallback target conflict bucket
	Target Target

	// Mapping describes the how the conflicts from a source scope
	// & collection is logged to the Target
	// Empty map implies that all conflicts will be logged
	Mapping map[base.CollectionNamespace]Target
}

func (r *Rules) Validate() (err error) {
	if !r.Target.IsComplete() {
		err = ErrIncompleteTarget
		return
	}

	if r.Target.IsSystemTarget() {
		err = ErrSystemNamespace
		return
	}

	for m, t := range r.Mapping {
		if m.ScopeName == "" {
			err = ErrEmptyScope
			return
		}

		if t.IsBlacklistTarget() {
			continue
		}

		if !t.IsComplete() {
			err = ErrIncompleteTarget
			return
		}

		if t.IsSystemTarget() {
			err = ErrSystemNamespace
			return
		}
	}

	return
}

func (r *Rules) SameAs(other *Rules) (same bool) {
	if r == nil || other == nil {
		return r == nil && other == nil
	}

	same = r.Target.SameAs(other.Target)
	if !same {
		return
	}

	if r.Mapping == nil || other.Mapping == nil {
		return r.Mapping == nil && other.Mapping == nil
	}

	same = len(r.Mapping) == len(other.Mapping)
	if !same {
		return
	}

	var otherSource Target
	for mapping, target := range r.Mapping {
		otherSource, same = other.Mapping[mapping]
		if !same {
			return
		}

		same = otherSource.SameAs(target)
		if !same {
			return
		}
	}

	same = true
	return
}

func (r *Rules) String() string {
	if r == nil {
		return "<nil>"
	}

	var loggingRules strings.Builder
	loggingRules.WriteString("Target: ")
	loggingRules.WriteString(r.Target.String())
	loggingRules.WriteString(", loggingRules: ")
	for source, target := range r.Mapping {
		loggingRules.WriteString(source.String())
		loggingRules.WriteString(" -> ")
		loggingRules.WriteString(target.String())
		loggingRules.WriteString(" | ")
	}

	return loggingRules.String()
}

func ParseString(o interface{}) (ok bool, val string) {
	if o == nil {
		return
	}
	val, ok = o.(string)
	return
}

func ParseTarget(m map[string]interface{}) (t Target, err error) {
	if len(m) == 0 {
		return
	}

	bucketObj, ok := m[base.CLogBucketKey]
	if ok {
		ok, s := ParseString(bucketObj)
		if ok {
			t.Bucket = s
		} else {
			err = fmt.Errorf("invalid bucket value")
			return
		}
	}

	collectionObj, ok := m[base.CLogCollectionKey]
	if ok {
		ok, s := ParseString(collectionObj)
		if ok {
			t.NS, err = base.NewOptionalCollectionNamespaceFromString(s)
			if err != nil {
				return
			}
		} else {
			err = fmt.Errorf("invalid collection value")
			return
		}
	}

	return
}

// ParseRules parses map[string]interface{} object into rules.
// should be in sync with ValidateConflictLoggingMapValues
// j should not be empty or nil
func ParseRules(j base.ConflictLoggingMappingInput) (rules *Rules, err error) {
	if j == nil {
		err = ErrNilMapping
		return
	}

	fallbackTarget, err := ParseTarget(j)
	if err != nil {
		return
	}

	fallbackTarget.Sanitize()
	if !fallbackTarget.IsComplete() {
		err = ErrIncompleteTarget
		return
	}

	rules = &Rules{
		Target:  fallbackTarget,
		Mapping: map[base.CollectionNamespace]Target{},
	}

	loggingRulesObj, ok := j[base.CLogLoggingRulesKey]
	if !ok || loggingRulesObj == nil {
		return
	}

	loggingRulesMap, ok := loggingRulesObj.(map[string]interface{})
	if !ok {
		rules = nil
		err = ErrInvalidLoggingRulesType
		return
	}

	for collectionStr, targetObj := range loggingRulesMap {
		if collectionStr == "" {
			rules = nil
			err = ErrInvalidCollection
			return
		}

		source, err := base.NewOptionalCollectionNamespaceFromString(collectionStr)
		if err != nil {
			return nil, err
		}

		target := fallbackTarget

		if targetObj != nil {
			targetMap, ok := targetObj.(map[string]interface{})
			if !ok {
				rules = nil
				err = ErrInvalidTargetType
				return nil, err
			}

			if len(targetMap) > 0 {
				target, err = ParseTarget(targetMap)
				if err != nil {
					return nil, err
				}
				target.Sanitize()
			}
		} else {
			target = BlacklistTarget()
		}

		rules.Mapping[source] = target
	}

	err = rules.Validate()
	if err != nil {
		rules = nil
		return
	}

	return
}

func ValidateAndConvertJsonMapToConflictLoggingMapping(value string) (base.ConflictLoggingMappingInput, error) {
	if value == "null" || value == "nil" {
		// "nil" is not a accepted value. {} is the smallest input.
		return nil, fmt.Errorf("null or nil conflict logging mapping not accepted")
	}

	// check for duplicated keys
	res, err := base.JsonStringReEncodeTest(value)
	if err != nil {
		return nil, err
	}
	if !res {
		return nil, base.ErrorJSONReEncodeFailed
	}

	// json validation
	jsonMap, err := base.ValidateAndConvertStringToJsonType(value)
	if err != nil {
		return nil, err
	}

	// explicit type check
	conflictLoggingMap, typeCheck := base.ParseConflictLoggingInputType(jsonMap)
	if !typeCheck {
		if jsonMap == nil {
			// null is not a acceptable input by design.
			return nil, fmt.Errorf("null conflict logging map not allowed. Use {} or {\"disabled\":true} for disabling the feature")
		}

		return nil, fmt.Errorf("expected non-nil json object, but invalid type was input as conflict logging map. Use {} or {\"disabled\":true} for disabling the feature")
	}

	// rules parsing check
	enabled := !conflictLoggingMap.Disabled()
	if enabled {
		// validate if input is syntactically and semantically valid
		_, err = ParseRules(conflictLoggingMap)
		if err != nil {
			return nil, fmt.Errorf("error parsing conflict logging input %v to rules, err=%v", conflictLoggingMap, err)
		}
	}

	return conflictLoggingMap, nil
}
