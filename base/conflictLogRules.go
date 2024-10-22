package base

import (
	"errors"
	"fmt"
	"strings"
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

// ConflictLogRules captures the logging rules for a replication
type ConflictLogRules struct {
	// Target is the default or fallback target conflict bucket
	Target ConflictLogTarget

	// Mapping describes the how the conflicts from a source scope
	// & collection is logged to the Target
	// Empty map implies that all conflicts will be logged
	Mapping map[CollectionNamespace]ConflictLogTarget
}

// ConflictLogTarget describes the target bucket, scope and collection where
// the conflicts will be logged. There are few terms:
// Complete => The triplet (Bucket, Scope, Collection) are populated
// Blacklist => If the target will be used as blacklist target.
// i.e. a special target value indicating blacklisting conflict logging rule.
type ConflictLogTarget struct {
	// Bucket is the conflict bucket
	Bucket string `json:"bucket"`

	// NS is namespace which defines scope and collection
	NS CollectionNamespace `json:"ns"`
}

func (t ConflictLogTarget) String() string {
	return fmt.Sprintf("%v.%v", t.Bucket, t.NS.ToIndexString())
}

func NewConflictLogTarget(bucket, scope, collection string) ConflictLogTarget {
	return ConflictLogTarget{
		Bucket: bucket,
		NS: CollectionNamespace{
			ScopeName:      scope,
			CollectionName: collection,
		},
	}
}

// Blacklist target is a special value indicating
// that the conflict mapped to this target won't be logged.
var blacklistTarget ConflictLogTarget = NewConflictLogTarget("", "", "")

func (t ConflictLogTarget) IsBlacklistTarget() bool {
	return t.SameAs(blacklistTarget)
}

func BlacklistConflictLogTarget() ConflictLogTarget {
	return blacklistTarget
}

// IsComplete implies that all components of the triplet (bucket, scope & collection)
// are populated
func (t ConflictLogTarget) IsComplete() bool {
	return t.Bucket != "" && t.NS.ScopeName != "" && t.NS.CollectionName != ""
}

// Returns true if the scope and/or collection used is a system scope and/or collection.
// The criteria checked is the underscore (_) at the beginning of the scope/collection name,
// except the _default scope/collection.
func (t ConflictLogTarget) IsSystemTarget() bool {
	if t.NS.ScopeName != DefaultScopeCollectionName &&
		len(t.NS.ScopeName) > 0 && t.NS.ScopeName[0] == '_' {
		return true
	}

	if t.NS.CollectionName != DefaultScopeCollectionName &&
		len(t.NS.CollectionName) > 0 && t.NS.CollectionName[0] == '_' {
		return true
	}

	return false
}

// The function defaults the collection and scope names to _default if not set.
func (t *ConflictLogTarget) Sanitize() {
	if t == nil {
		return
	}

	if t.Bucket == "" {
		// let Validate take care of returning the error
		return
	}

	if t.NS.ScopeName == "" {
		t.NS.ScopeName = DefaultScopeCollectionName
	}

	if t.NS.CollectionName == "" {
		t.NS.CollectionName = DefaultScopeCollectionName
	}
}

func (t ConflictLogTarget) SameAs(other ConflictLogTarget) bool {
	return t.Bucket == other.Bucket && t.NS.IsSameAs(other.NS)
}

func ParseString(o interface{}) (ok bool, val string) {
	if o == nil {
		return
	}
	val, ok = o.(string)
	return
}

func ParseConflictLogTarget(m map[string]interface{}) (t ConflictLogTarget, err error) {
	if len(m) == 0 {
		return
	}

	bucketObj, ok := m[CLBucketKey]
	if ok {
		ok, s := ParseString(bucketObj)
		if ok {
			t.Bucket = s
		} else {
			err = fmt.Errorf("invalid bucket value")
			return
		}
	}

	collectionObj, ok := m[CLCollectionKey]
	if ok {
		ok, s := ParseString(collectionObj)
		if ok {
			t.NS, err = NewOptionalCollectionNamespaceFromString(s)
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

// ParseConflictLogRules parses map[string]interface{} object into rules.
// should be in sync with ValidateConflictLoggingMapValues
// j should not be empty or nil
func ParseConflictLogRules(j ConflictLoggingMappingInput) (rules *ConflictLogRules, err error) {
	if j == nil {
		err = ErrNilMapping
		return
	}

	fallbackTarget, err := ParseConflictLogTarget(j)
	if err != nil {
		return
	}

	fallbackTarget.Sanitize()
	if !fallbackTarget.IsComplete() {
		err = ErrIncompleteTarget
		return
	}

	rules = &ConflictLogRules{
		Target:  fallbackTarget,
		Mapping: map[CollectionNamespace]ConflictLogTarget{},
	}

	loggingRulesObj, ok := j[CLLoggingRulesKey]
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

		source, err := NewOptionalCollectionNamespaceFromString(collectionStr)
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
				target, err = ParseConflictLogTarget(targetMap)
				if err != nil {
					return nil, err
				}
				target.Sanitize()
			}
		} else {
			target = BlacklistConflictLogTarget()
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

func (r *ConflictLogRules) Validate() (err error) {
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

func (r *ConflictLogRules) SameAs(other *ConflictLogRules) (same bool) {
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

	var otherSource ConflictLogTarget
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

func (r *ConflictLogRules) String() string {
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
