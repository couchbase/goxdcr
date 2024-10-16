// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package metadata

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/base/filter"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/golang/snappy"
)

type ManifestsDoc struct {
	collectionsManifests []*CollectionsManifest

	// When upserting or retrieving from metakv, compress them into a single byte slice
	// This is not to be used otherwise - use CollectionsManifests()
	CompressedCollectionsManifests []byte `json:"collection_manifests"`

	//revision number
	revision interface{}
}

func (m *ManifestsDoc) Revision() interface{} {
	return m.revision
}

func (m *ManifestsDoc) SetRevision(rev interface{}) {
	m.revision = rev
}

func (m *ManifestsDoc) CollectionsManifests() []*CollectionsManifest {
	return m.collectionsManifests
}

func (m *ManifestsDoc) SetCollectionsManifests(manifests []*CollectionsManifest) {
	m.collectionsManifests = manifests
}

func (m *ManifestsDoc) PreMarshal() error {
	serializedJson, err := json.Marshal(m.collectionsManifests)
	if err != nil {
		return err
	}
	m.CompressedCollectionsManifests = snappy.Encode(nil, serializedJson)
	return nil
}

func (m *ManifestsDoc) ClearCompressedData() {
	m.CompressedCollectionsManifests = nil
}

func (m *ManifestsDoc) Clear() {
	m.ClearCompressedData()
	m.collectionsManifests = m.collectionsManifests[:0]
	m.revision = nil
}

func (m *ManifestsDoc) PostUnmarshal() error {
	var serializedJson []byte
	serializedJson, err := snappy.Decode(serializedJson, m.CompressedCollectionsManifests)
	if err != nil {
		return err
	}

	err = json.Unmarshal(serializedJson, &(m.collectionsManifests))
	if err != nil {
		return err
	}

	return nil
}

// This method will clear and then load a manifests pair in
func (m *ManifestsDoc) LoadManifestPairAndCompress(pair *CollectionsManifestPair) ([]byte, error) {
	m.Clear()
	m.collectionsManifests = append(m.collectionsManifests, pair.Source)
	m.collectionsManifests = append(m.collectionsManifests, pair.Target)
	err := m.PreMarshal()
	if err != nil {
		return nil, err
	}
	return m.CompressedCollectionsManifests, nil
}

// This method will read the compressed data and output a pair
func (m *ManifestsDoc) DeCompressAndOutputPair(compressedStream []byte) (*CollectionsManifestPair, error) {
	if compressedStream == nil {
		return nil, fmt.Errorf("ManifestsDoc input a nil stream")
	}
	m.CompressedCollectionsManifests = compressedStream
	err := m.PostUnmarshal()
	if err != nil {
		return nil, err
	}

	if len(m.collectionsManifests) < 2 {
		return nil, fmt.Errorf("Unable to output pair as only %v manifests exist", len(m.collectionsManifests))
	}

	pair := &CollectionsManifestPair{
		Source: m.collectionsManifests[0],
		Target: m.collectionsManifests[1],
	}
	return pair, nil
}

type CollectionsManifestPair struct {
	Source *CollectionsManifest `json:"sourceManifest"`
	Target *CollectionsManifest `json:"targetManifest"`
}

func (c *CollectionsManifestPair) IsSameAs(other *CollectionsManifestPair) bool {
	return c.Source.IsSameAs(other.Source) && c.Target.IsSameAs(other.Target)
}

func NewCollectionsManifestPair(source, target *CollectionsManifest) *CollectionsManifestPair {
	return &CollectionsManifestPair{
		Source: source,
		Target: target,
	}
}

// Manifest structure representing the JSON returned from ns_server's endpoint
type CollectionsManifest struct {
	uid                        uint64
	scopes                     ScopesMap
	collectionIdToNamespaceMap CollectionsManifestReverseLookupMap
}

func UnitTestGenerateCollManifest(uid uint64, scopes ScopesMap) *CollectionsManifest {
	return &CollectionsManifest{uid: uid, scopes: scopes}
}

func (c *CollectionsManifest) String() string {
	if c == nil {
		return ""
	}
	var output []string
	output = append(output, fmt.Sprintf("CollectionsManifest uid: %v { ", c.uid))
	output = append(output, c.scopes.String())
	output = append(output, " }")
	return strings.Join(output, " ")
}

func (c *CollectionsManifest) GetScopeAndCollectionName(collectionId uint32) (scopeName, collectionName string, err error) {
	if c == nil {
		err = base.ErrorInvalidInput
		return
	}

	namespace, exists := c.collectionIdToNamespaceMap[collectionId]
	if !exists {
		err = base.ErrorNotFound
		return
	}

	scopeName = namespace.ScopeName
	collectionName = namespace.CollectionName
	return
}

func (c *CollectionsManifest) GetCollectionId(scopeName, collectionName string) (uint32, error) {
	if c == nil {
		return 0, fmt.Errorf("Nil collectionsManifest")
	}
	scope, exists := c.Scopes()[scopeName]
	if !exists {
		return 0, base.ErrorNotFound
	}

	collection, exists := scope.Collections[collectionName]
	if !exists {
		return 0, base.ErrorNotFound
	}

	return collection.Uid, nil
}

func (target *CollectionsManifest) ImplicitGetBackfillCollections(prevTarget, source *CollectionsManifest) (CollectionNamespaceMapping, CollectionNamespaceMappingsDiffPair, error) {
	var addedRemovedPair CollectionNamespaceMappingsDiffPair
	backfillsNeeded := make(CollectionNamespaceMapping)

	// First, find any previously unmapped target collections that are now mapped
	oldSrcToTargetMapping, _, _ := source.ImplicitMap(prevTarget)
	srcToTargetMapping, _, _ := source.ImplicitMap(target)

	added, removed := oldSrcToTargetMapping.Diff(srcToTargetMapping)
	addedRemovedPair.Added = added
	addedRemovedPair.Removed = removed

	backfillsNeeded.Consolidate(added)

	// Then, find out if any target Collection UID changed from underneath the manifests
	// These target collections need to be backfilled that couldn't be discovered from the namespacemappings
	// because they were never "missing", but their collection UID changed
	_, changedTargets, _, err := target.Diff(prevTarget)
	if err != nil {
		return nil, addedRemovedPair, err
	}
	backfillsNeededDueToTargetRecreation := srcToTargetMapping.GetSubsetBasedOnSpecifiedTargets(changedTargets)

	backfillsNeeded.Consolidate(backfillsNeededDueToTargetRecreation)
	return backfillsNeeded, addedRemovedPair, nil
}

func (c CollectionsManifest) GetAllCollectionsGivenScopeRO(scopeName string) (CollectionsMap, error) {
	scope, found := c.scopes[scopeName]
	if !found {
		return nil, base.ErrorNotFound
	}

	return scope.Collections, nil
}

func (c *CollectionsManifest) generateReverseLookupMap() {
	if c == nil {
		return
	}
	c.collectionIdToNamespaceMap = make(CollectionsManifestReverseLookupMap)

	for _, scope := range c.Scopes() {
		for _, collection := range scope.Collections {
			c.collectionIdToNamespaceMap[collection.Uid] = base.CollectionNamespace{
				ScopeName:      scope.Name,
				CollectionName: collection.Name,
			}
		}
	}
}

type CollectionsManifestReverseLookupMap map[uint32]base.CollectionNamespace

func (c CollectionsManifestReverseLookupMap) Clone() CollectionsManifestReverseLookupMap {
	clonedMap := make(CollectionsManifestReverseLookupMap)
	for k, v := range c {
		clonedMap[k] = v.Clone()
	}
	return clonedMap
}

// Actual obj stored in metakv
type collectionsMetaObj struct {
	// data for unmarshalling and parsing
	UidMeta_    string        `json:"uid"`
	ScopesMeta_ []interface{} `json:"scopes"`
}

func newCollectionsMetaObj() *collectionsMetaObj {
	return &collectionsMetaObj{}
}

// Used if there's an error getting collections manifest
func NewDefaultCollectionsManifest() CollectionsManifest {
	defaultManifest := CollectionsManifest{scopes: make(ScopesMap)}

	defaultScope := NewEmptyScope(base.DefaultScopeCollectionName, 0)
	defaultCollection := Collection{Uid: 0, Name: base.DefaultScopeCollectionName}

	defaultScope.Collections[base.DefaultScopeCollectionName] = defaultCollection

	defaultManifest.scopes[base.DefaultScopeCollectionName] = defaultScope

	defaultManifest.generateReverseLookupMap()
	return defaultManifest
}

func NewCollectionsManifestFromMap(manifestInfo map[string]interface{}) (CollectionsManifest, error) {
	metaObj := newCollectionsMetaObj()
	var manifest CollectionsManifest

	if uid, ok := manifestInfo["uid"].(string); ok {
		metaObj.UidMeta_ = uid
	} else {
		return manifest, fmt.Errorf("Uid is not float64, but %v instead", reflect.TypeOf(manifestInfo["uid"]))
	}

	if scopes, ok := manifestInfo["scopes"].([]interface{}); ok {
		metaObj.ScopesMeta_ = scopes
	} else {
		return manifest, base.ErrorInvalidInput
	}

	err := manifest.Load(metaObj)
	if err != nil {
		return manifest, err
	}
	manifest.generateReverseLookupMap()
	return manifest, nil
}

func NewCollectionsManifestFromBytes(data []byte) (CollectionsManifest, error) {
	var manifest CollectionsManifest
	err := manifest.LoadBytes(data)
	manifest.generateReverseLookupMap()
	return manifest, err
}

// For unit test
func TestNewCollectionsManifestFromBytesWithCustomUid(data []byte, uid uint64) (CollectionsManifest, error) {
	var manifest CollectionsManifest
	err := manifest.LoadBytes(data)
	manifest.uid = uid
	manifest.generateReverseLookupMap()
	return manifest, err
}

// Does not clone temporary variables
func (c CollectionsManifest) Clone() CollectionsManifest {
	return CollectionsManifest{
		uid:                        c.uid,
		scopes:                     c.scopes.Clone(),
		collectionIdToNamespaceMap: c.collectionIdToNamespaceMap.Clone(),
	}
}

func (this *CollectionsManifest) IsSameAs(other *CollectionsManifest) bool {
	if this == nil || other == nil {
		return false
	}

	if this.Uid() != other.Uid() {
		return false
	}

	for scopeName, scope := range this.scopes {
		otherScope, ok := other.scopes[scopeName]
		if !ok {
			return false
		}
		if !scope.IsSameAs(otherScope) {
			return false
		}
	}

	return true
}

func (c *CollectionsManifest) Diff(older *CollectionsManifest) (added, modified, removed ScopesMap, err error) {
	if c == nil || older == nil {
		return ScopesMap{}, ScopesMap{}, ScopesMap{}, base.ErrorInvalidInput
	}

	if c.Uid() < older.Uid() {
		err = fmt.Errorf("Should compare against an older version of manifest")
		return
	} else if c.IsSameAs(older) {
		return
	}

	added = make(ScopesMap)
	modified = make(ScopesMap)
	removed = make(ScopesMap)

	// First, find things that exists in the current manifest that doesn't exist in the older one
	// or versions are different, which by contract means they are modified and newer (uid never go backwards)
	for scopeName, scope := range c.Scopes() {
		olderScope, exists := older.Scopes()[scopeName]
		if !exists {
			added[scopeName] = scope
		} else if exists && !scope.IsSameAs(olderScope) {
			collectionAddedYet := false
			collectionModifiedYet := false
			// At least one collection is different
			// If this scope is different because all the collections are removed, the "removed" portion
			// should catch it
			for collectionName, collection := range scope.Collections {
				olderCollection, exists := olderScope.Collections[collectionName]
				if !exists {
					if !collectionAddedYet {
						added[scopeName] = NewEmptyScope(scopeName, scope.Uid)
						collectionAddedYet = true
					}
					added[scopeName].Collections[collectionName] = collection
				} else if exists && !collection.IsSameAs(olderCollection) {
					if !collectionModifiedYet {
						modified[scopeName] = NewEmptyScope(scopeName, scope.Uid)
						collectionModifiedYet = true
					}
					modified[scopeName].Collections[collectionName] = collection
				}
			}
		}
	}

	// Then find things that don't exist in the new but exist in the old
	if c.Count(true /*includeDefaultScope*/, true /*includeDefaultCollection*/) != older.Count(true, true) {
		for olderScopeName, olderScope := range older.Scopes() {
			scope, exists := c.Scopes()[olderScopeName]
			if !exists {
				removed[olderScopeName] = olderScope
			} else if exists && olderScope.Count(true /*includeDefaultCollection*/) != scope.Count(true) {
				// At least one collection is removed, potentially including default collection
				removed[olderScopeName] = NewEmptyScope(olderScopeName, olderScope.Uid)
				for olderCollectionName, olderCollection := range olderScope.Collections {
					_, exists := scope.Collections[olderCollectionName]
					if !exists {
						removed[olderScopeName].Collections[olderCollectionName] = olderCollection
					}
				}
			}
		}
	}

	return
}

// Counts the total number of collections in this manifest, including or excluding default scope/collection
func (c *CollectionsManifest) Count(includeDefaultScope, includeDefaultCollection bool) int {
	return c.scopes.Count(includeDefaultScope, includeDefaultCollection)
}

// Given a metadata object, load into the more user-friendly collectionsManifest
func (c *CollectionsManifest) Load(collectionsMeta *collectionsMetaObj) error {
	var err error
	c.uid, err = strconv.ParseUint(collectionsMeta.UidMeta_, base.CollectionsUidBase, 64)
	c.scopes = make(ScopesMap)
	for _, oneScopeDetail := range collectionsMeta.ScopesMeta_ {
		scopeDetailMap, ok := oneScopeDetail.(map[string]interface{})
		if !ok {
			return base.ErrorInvalidInput
		}
		scopeName, ok := scopeDetailMap[base.NameKey].(string)
		if !ok {
			return base.ErrorInvalidInput
		}
		c.scopes[scopeName], err = NewScope(scopeName, scopeDetailMap)
		if err != nil {
			return base.ErrorInvalidInput
		}
	}
	c.generateReverseLookupMap()
	return nil
}

func (c *CollectionsManifest) LoadBytes(data []byte) error {
	collectionsMeta := newCollectionsMetaObj()
	err := json.Unmarshal(data, collectionsMeta)
	if err != nil {
		return err
	}

	return c.Load(collectionsMeta)
}

// Implements the marshaller interface
func (c *CollectionsManifest) MarshalJSON() ([]byte, error) {
	collectionsMeta := newCollectionsMetaObj()
	collectionsMeta.UidMeta_ = fmt.Sprintf("%x", c.uid)

	// marshal scopes in order of names - this will ensure equality between two identical manifests
	var scopeNames []string
	for name, _ := range c.scopes {
		scopeNames = append(scopeNames, name)
	}
	scopeNames = base.SortStringList(scopeNames)

	for _, scopeName := range scopeNames {
		scope := c.scopes[scopeName]
		collectionsMeta.ScopesMeta_ = append(collectionsMeta.ScopesMeta_, scope.toScopeDetail())
	}

	outBytes, _ := json.Marshal(collectionsMeta)
	return outBytes, nil
}

// Implements the marshaller interface
func (c *CollectionsManifest) UnmarshalJSON(b []byte) error {
	collectionsMeta := newCollectionsMetaObj()

	err := json.Unmarshal(b, collectionsMeta)
	if err != nil {
		return err
	}

	return c.Load(collectionsMeta)
}

// Returns a sha256 representing the manifest data
func (c *CollectionsManifest) Sha256() (result [sha256.Size]byte, err error) {
	var jsonBytes []byte
	jsonBytes, err = c.MarshalJSON()
	if err != nil {
		return
	}

	result = sha256.Sum256(jsonBytes)
	return
}

func (c *CollectionsManifest) Uid() uint64 {
	return c.uid
}

func (c *CollectionsManifest) Scopes() ScopesMap {
	return c.scopes
}

// type CollectionsMap map[string]Collection
// Diff by name between two manifests
func (sourceManifest *CollectionsManifest) ImplicitMap(targetManifest *CollectionsManifest) (successfulMapping CollectionNamespaceMapping, unmappedSources CollectionsMap, unmappedTargets CollectionsMap) {
	if sourceManifest == nil || targetManifest == nil {
		return
	}

	successfulMapping = make(CollectionNamespaceMapping)
	unmappedSources = make(CollectionsMap)
	unmappedTargets = make(CollectionsMap)

	for _, sourceScope := range sourceManifest.Scopes() {
		_, exists := targetManifest.Scopes()[sourceScope.Name]
		if !exists {
			// Whole scope does not exist on target
			for _, collection := range sourceScope.Collections {
				unmappedSources[collection.Name] = collection
			}
		} else {
			// Some collections may or may not exist on target
			for _, sourceCollection := range sourceScope.Collections {
				_, err := targetManifest.GetCollectionId(sourceScope.Name, sourceCollection.Name)
				if err == nil {
					implicitNamespace := &base.CollectionNamespace{
						ScopeName:      sourceScope.Name,
						CollectionName: sourceCollection.Name,
					}
					successfulMapping.AddSingleMapping(implicitNamespace, implicitNamespace)
				} else {
					unmappedSources[sourceCollection.Name] = sourceCollection
				}
			}
		}
	}

	for _, targetScope := range targetManifest.Scopes() {
		_, exists := sourceManifest.Scopes()[targetScope.Name]
		if !exists {
			// Whole scope does not exist on source
			for _, collection := range targetScope.Collections {
				unmappedTargets[collection.Name] = collection
			}
		} else {
			// Some may or maynot exist on source
			for _, targetCollection := range targetScope.Collections {
				_, err := sourceManifest.GetCollectionId(targetScope.Name, targetCollection.Name)
				if err != nil {
					unmappedTargets[targetCollection.Name] = targetCollection
				}
			}
		}
	}
	return
}

func (c *CollectionsManifest) SameAs(other *CollectionsManifest) bool {
	if c == nil && other != nil {
		return false
	} else if c != nil && other == nil {
		return false
	} else if c == nil && other == nil {
		return true
	}

	return c.uid == other.uid && c.scopes.SameAs(other.scopes)
}

type CollectionsPtrList []*Collection

func (c CollectionsPtrList) Len() int           { return len(c) }
func (c CollectionsPtrList) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c CollectionsPtrList) Less(i, j int) bool { return c[i].Uid < c[j].Uid }

func SortCollectionsPtrList(list CollectionsPtrList) CollectionsPtrList {
	sort.Sort(list)
	return list
}

func (c CollectionsPtrList) IsSameAs(other CollectionsPtrList) bool {
	if len(c) != len(other) {
		return false
	}

	// Lists are logically "equal" if they have the same items but in diff order
	aList := SortCollectionsPtrList(c)
	bList := SortCollectionsPtrList(other)

	for i, col := range aList {
		if !col.IsSameAs(*(bList[i])) {
			return false
		}
	}
	return true
}

type CollectionsList []Collection

func (c CollectionsList) Len() int           { return len(c) }
func (c CollectionsList) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c CollectionsList) Less(i, j int) bool { return c[i].Uid < c[j].Uid }

func SortCollectionsList(list CollectionsList) CollectionsList {
	sort.Sort(list)
	return list
}

func (c CollectionsList) IsSameAs(other CollectionsList) bool {
	if len(c) != len(other) {
		return false
	}

	// Lists are logically "equal" if they have the same items but in diff order
	aList := SortCollectionsList(c)
	bList := SortCollectionsList(other)

	for i, col := range aList {
		if !col.IsSameAs(bList[i]) {
			return false
		}
	}
	return true
}

// Used to support marshalling CollectionToCollectionsMapping
type c2cMarshalObj struct {
	SourceCollections []*Collection `json:Source`
	// keys are integers of the index above written as strings
	IndirectTargetMap map[string][]*Collection `json:Map`
}

func newc2cMarshalObj() *c2cMarshalObj {
	return &c2cMarshalObj{
		IndirectTargetMap: make(map[string][]*Collection),
	}
}

type ScopesMap map[string]Scope

func (s ScopesMap) Len() int {
	return len(s)
}

func (s *ScopesMap) String() string {
	if s == nil {
		return "(None)"
	}
	var output []string
	for _, scope := range *s {
		output = append(output, scope.String())
	}
	return strings.Join(output, " ")
}

func (s *ScopesMap) Count(includeDefaultScope, includeDefaultCollection bool) int {
	var count int
	for scopeName, scope := range *s {
		if scopeName == base.DefaultScopeCollectionName {
			if includeDefaultScope {
				count += scope.Count(includeDefaultCollection)
			}
		} else {
			count += scope.Count(includeDefaultCollection)
		}
	}
	return count

}

func (s ScopesMap) Clone() ScopesMap {
	clone := make(ScopesMap)
	for k, v := range s {
		clone[k] = v.Clone()
	}
	return clone
}

func (s ScopesMap) GetCollection(id uint32) (col Collection, found bool) {
	for _, scope := range s {
		for _, collection := range scope.Collections {
			if collection.Uid == id {
				col = collection
				found = true
				return
			}
		}
	}
	return
}

func (s ScopesMap) GetCollectionByNames(scopeName, collectionName string) (col Collection, found bool) {
	scope, found := s[scopeName]
	if !found {
		return
	}
	col, found = scope.Collections[collectionName]
	return
}

// Note - this is for temporary internal use only, no ID used
func (s ScopesMap) AddNamespace(scopeName, collectionName string) {
	scope, found := s[scopeName]
	if !found {
		scope = Scope{Name: scopeName, Collections: make(CollectionsMap)}
		s[scopeName] = scope
	}
	_, found = scope.Collections[collectionName]
	if !found {
		scope.Collections[collectionName] = Collection{
			Name: collectionName,
		}
	}
}

func (s ScopesMap) SameAs(other ScopesMap) bool {
	if len(s) != len(other) {
		return false
	}

	for k, v := range s {
		otherV, exists := (other)[k]
		if !exists {
			return false
		}
		if !v.SameAs(&otherV) {
			return false
		}
	}
	return true
}

type Scope struct {
	Uid         uint32 `json:"Uid"`
	Name        string `json:"Name"`
	Collections CollectionsMap
}

func (s *Scope) String() string {
	if s == nil {
		return ""
	}
	var output []string
	output = append(output, fmt.Sprintf("Scope Uid: %v Name: %v { ", s.Uid, s.Name))
	output = append(output, s.Collections.String())
	output = append(output, " }")
	return strings.Join(output, " ")
}

func NewScope(name string, scopeDetail map[string]interface{}) (Scope, error) {
	uidString, ok := scopeDetail[base.UIDKey].(string)
	if !ok {
		return Scope{}, fmt.Errorf("Uid is not float64, but %v instead", reflect.TypeOf(scopeDetail[base.UIDKey]))
	}
	uid64, err := strconv.ParseUint(uidString, base.CollectionsUidBase, 64)
	if err != nil {
		return Scope{}, err
	}
	uid32 := uint32(uid64)

	collectionsDetail, ok := scopeDetail[base.CollectionsKey].([]interface{})
	isScopeEmpty := scopeDetail[base.CollectionsKey] == nil
	if !ok && !isScopeEmpty {
		return Scope{}, fmt.Errorf("Unable to parse collections - type of %v", reflect.TypeOf(scopeDetail[base.CollectionsKey]))
	}

	// It is possible that when marshalled and unmarshalled as part of P2P payload
	// a scope without any collection may have base.CollectionsKey listed as nil
	if isScopeEmpty {
		collectionsDetail = nil
	}

	collectionsMap, err := NewCollectionsMap(collectionsDetail)
	if err != nil {
		return Scope{}, err
	}

	return Scope{
		Name:        name,
		Uid:         uid32,
		Collections: collectionsMap,
	}, nil
}

func NewEmptyScope(name string, uid uint32) Scope {
	return Scope{
		Name:        name,
		Uid:         uid,
		Collections: make(CollectionsMap),
	}
}

func (this *Scope) IsSameAs(other Scope) bool {
	if this.Uid != other.Uid {
		return false
	}
	if this.Name != other.Name {
		return false
	}
	if !this.Collections.IsSameAs(other.Collections) {
		return false
	}
	return true
}

func (s *Scope) toScopeDetail() map[string]interface{} {
	detailMap := make(map[string]interface{})
	detailMap[base.NameKey] = s.Name
	detailMap[base.UIDKey] = fmt.Sprintf("%x", s.Uid)
	detailMap[base.CollectionsKey] = s.Collections.toCollectionsDetail()

	return detailMap
}

func (s *Scope) Count(includeDefaultCollection bool) int {
	return s.Collections.Count(includeDefaultCollection)
}

func (s Scope) Clone() Scope {
	return Scope{
		Uid:         s.Uid,
		Name:        s.Name,
		Collections: s.Collections.Clone(),
	}
}

func (s *Scope) SameAs(other *Scope) bool {
	if s == nil && other != nil {
		return false
	} else if s != nil && other == nil {
		return false
	} else if s == nil && other == nil {
		return true
	}

	return s.Uid == other.Uid && s.Name == other.Name && s.Collections.IsSameAs(other.Collections)
}

type Collection struct {
	Uid  uint32 `json:"Uid"`
	Name string `json:"Name"`
}

func (c *Collection) String() string {
	if c == nil {
		return ""
	}
	return fmt.Sprintf("Collection { Uid: %v Name: %v }", c.Uid, c.Name)
}

func NewCollectionsMap(collectionsList []interface{}) (map[string]Collection, error) {
	collectionMap := make(CollectionsMap)

	for _, detail := range collectionsList {
		colDetail, ok := detail.(map[string]interface{})
		if !ok {
			return nil, base.ErrorInvalidInput
		}

		name, ok := colDetail[base.NameKey].(string)
		if !ok {
			return nil, base.ErrorInvalidInput
		}

		uidStr, ok := colDetail[base.UIDKey].(string)
		if !ok {
			return nil, base.ErrorInvalidInput
		}
		uid64, err := strconv.ParseUint(uidStr, base.CollectionsUidBase, 64)
		if err != nil {
			return nil, err
		}
		uid32 := uint32(uid64)

		collectionMap[name] = Collection{
			Uid:  uid32,
			Name: name,
		}
	}
	return collectionMap, nil
}

func (this *Collection) IsSameAs(other Collection) bool {
	return this.Uid == other.Uid && this.Name == other.Name
}

func (c Collection) Clone() Collection {
	return Collection{
		Uid:  c.Uid,
		Name: c.Name,
	}
}

type CollectionsMap map[string]Collection

func (c *CollectionsMap) String() string {
	var output []string
	if c == nil {
		return ""
	}
	for _, col := range *c {
		output = append(output, col.String())
	}
	return strings.Join(output, " ")
}

func (c *CollectionsMap) toCollectionsDetail() (detailList []interface{}) {
	// Output in sorted collection name ordering
	var colNames []string
	for name, _ := range *c {
		colNames = append(colNames, name)
	}
	colNames = base.SortStringList(colNames)

	for _, colName := range colNames {
		collection := (*c)[colName]
		detail := make(map[string]interface{})
		detail[base.NameKey] = colName
		detail[base.UIDKey] = fmt.Sprintf("%x", collection.Uid)
		detailList = append(detailList, detail)
	}
	return
}

func (this *CollectionsMap) IsSameAs(other CollectionsMap) bool {
	if this == nil {
		return false
	}
	for colName, collection := range *this {
		otherCollection, ok := other[colName]
		if !ok {
			return false
		}
		if !collection.IsSameAs(otherCollection) {
			return false
		}
	}
	return true
}

func (c *CollectionsMap) Count(includeDefaultCollection bool) int {
	var count int
	for colName, _ := range *c {
		if colName == base.DefaultScopeCollectionName {
			if includeDefaultCollection {
				count++
			}
		} else {
			count++
		}
	}
	return count
}

func (c CollectionsMap) Clone() CollectionsMap {
	clone := make(CollectionsMap)
	for k, v := range c {
		clone[k] = v.Clone()
	}
	return clone
}

// Diff by name
// Modified is if the names are the same but collection IDs are different
func (c CollectionsMap) Diff(older CollectionsMap) (added, removed, modified CollectionsMap) {
	added = make(CollectionsMap)
	removed = make(CollectionsMap)
	modified = make(CollectionsMap)

	for collectionName, collection := range c {
		olderCol, exists := older[collectionName]
		if !exists {
			added[collectionName] = collection
		} else if olderCol.Uid != collection.Uid {
			modified[collectionName] = collection
		}
	}

	for olderColName, olderCol := range older {
		_, exists := c[olderColName]
		if !exists {
			removed[olderColName] = olderCol
		}
	}

	return
}

type ManifestsList []*CollectionsManifest

func (c ManifestsList) Len() int           { return len(c) }
func (c ManifestsList) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c ManifestsList) Less(i, j int) bool { return c[i].uid < c[j].uid }

func (c ManifestsList) Sort() {
	sort.Sort(c)
}

func (c ManifestsList) String() string {
	var output []string
	output = append(output, "Manifests List:")
	for i := 0; i < len(c); i++ {
		if c[i] != nil {
			output = append(output, fmt.Sprintf("%v", c[i].String()))
		}
	}
	return strings.Join(output, "\n")
}

// Remember to sort before calling Sha if needed
func (c ManifestsList) Sha256() (result [sha256.Size]byte, err error) {
	if len(c) == 0 {
		return
	}

	runningHash := sha256.New()
	var emptyManifest CollectionsManifest
	emptyManifestBytes, _ := emptyManifest.Sha256()
	for i := 0; i < len(c); i++ {
		if c[i] == nil {
			runningHash.Write(emptyManifestBytes[0:len(emptyManifestBytes)])
		} else {
			var oneBytes [sha256.Size]byte
			oneBytes, err = c[i].Sha256()
			if err != nil {
				return
			}
			runningHash.Write(oneBytes[0:len(oneBytes)])
		}
	}

	tempSlice := runningHash.Sum(nil)
	if len(tempSlice) > sha256.Size {
		err = fmt.Errorf("Invalid sha256 hash - list: %v", c.String())
	}
	copy(result[:], tempSlice[:sha256.Size])
	return
}

type CollectionNamespaceList []*base.CollectionNamespace

func (c CollectionNamespaceList) Len() int      { return len(c) }
func (c CollectionNamespaceList) Swap(i, j int) { c[i], c[j] = c[j], c[i] }
func (c CollectionNamespaceList) Less(i, j int) bool {
	return (*(c[i])).LessThan(*(c[j]))
}

// NOTE: sort.Sort seems to pivot on the element passed in and since slice references share the same backend,
// callers should be fixed to pass in clones to prevent concurrency issues
func SortCollectionsNamespaceList(list CollectionNamespaceList) CollectionNamespaceList {
	sort.Sort(list)
	return list
}

func (c CollectionNamespaceList) String() string {
	var buffer bytes.Buffer
	for _, j := range c {
		if j.IsEmpty() {
			buffer.WriteString(fmt.Sprintf("|<nil>| "))
		} else {
			buffer.WriteString(fmt.Sprintf("|Scope: %v Collection: %v| ", j.ScopeName, j.CollectionName))
		}
	}
	return buffer.String()
}

func (c CollectionNamespaceList) IsSame(otherRO CollectionNamespaceList) bool {
	if len(c) != len(otherRO) {
		return false
	}

	// this is used often as readers ... pass in clones to prevent sort.Sort as it pivots
	aList := SortCollectionsNamespaceList(c.Clone())
	bList := SortCollectionsNamespaceList(otherRO.Clone())

	for i, col := range aList {
		if *col != *bList[i] {
			return false
		}
	}

	return true
}

// If other contains everything in C
func (c CollectionNamespaceList) IsSubset(other CollectionNamespaceList) bool {
	if len(c) > len(other) {
		return false
	}

	cList := SortCollectionsNamespaceList(c.Clone())
	otherList := SortCollectionsNamespaceList(other.Clone())

	i := 0 // for other
	j := 0 // for c
	for i = 0; i < len(cList); i++ {
		var oneElemFound bool
		for j < len(otherList) {
			if c[i].IsSameAs(*(other[j])) {
				oneElemFound = true
			}
			j++
			if oneElemFound {
				break
			}
		}
		if !oneElemFound {
			return false
		}
	}
	return true
}

func (c CollectionNamespaceList) Clone() (other CollectionNamespaceList) {
	for _, j := range c {
		ns := j.Clone()
		other = append(other, &ns)
	}
	return
}

func (c CollectionNamespaceList) Contains(namespace *base.CollectionNamespace) bool {
	if namespace == nil {
		panic("Nil namespace")
	}

	if c == nil || len(c) == 0 {
		if namespace == nil || namespace.IsEmpty() {
			return true
		}
		return false
	}

	for _, j := range c {
		if j.IsSameAs(*namespace) {
			return true
		}
	}
	return false
}

// Caller should have called IsSame() before doing consolidate
func (c *CollectionNamespaceList) Consolidate(otherRO CollectionNamespaceList) {
	aMissingAction := func(item *base.CollectionNamespace) {
		*c = append(*c, item)
	}

	c.diffOrConsolidate(otherRO.Clone(), aMissingAction, nil /*bMissingAction*/)
}

// Diff will make sure that both "c" and "other" are not modified
func (c CollectionNamespaceList) Diff(other CollectionNamespaceList) (added, removed CollectionNamespaceList) {
	aMissingAction := func(item *base.CollectionNamespace) {
		added = append(added, item)
	}

	bMissingAction := func(item *base.CollectionNamespace) {
		removed = append(removed, item)
	}

	clone := c.Clone()
	clone.diffOrConsolidate(other.Clone(), aMissingAction, bMissingAction)
	return
}

func (c *CollectionNamespaceList) diffOrConsolidate(other CollectionNamespaceList, aMissingAction, bMissingAction func(item *base.CollectionNamespace)) {
	// Note SortCollectionsNamespaceList() modifies the elements - so Diff() or Consolidate() should take preventative measures
	aList := SortCollectionsNamespaceList(*c)
	bList := SortCollectionsNamespaceList(other)

	var aIdx int
	var bIdx int

	// Note - c == aList in this case

	for aIdx < len(aList) && bIdx < len(bList) {
		if aList[aIdx].IsSameAs(*(bList[bIdx])) {
			aIdx++
			bIdx++
		} else if aList[aIdx].LessThan(*(bList[bIdx])) {
			// Blist does not have something aList have.
			if bMissingAction != nil {
				bMissingAction(aList[aIdx])
			}
			aIdx++
		} else {
			// Blist[bIdx] < aList[aIdx]
			// B list has something aList does not have
			if aMissingAction != nil {
				aMissingAction(bList[bIdx])
			}
			bIdx++
		}
	}

	for bIdx < len(bList) {
		// The rest is all missing from A list
		if aMissingAction != nil {
			aMissingAction(bList[bIdx])
		}
		bIdx++
	}
}

type collectionNsMetaObj struct {
	SourceCollections CollectionNamespaceList `json:SourceCollections`
	// keys are integers of the index above
	IndirectTargetMap      map[uint64]CollectionNamespaceList `json:IndirectTargetMap`
	SourceNamespaceTypeMap map[uint64]SourceNamespaceType     `json:SourceNamespaceTypeMap`
}

func newCollectionNsMetaObj() *collectionNsMetaObj {
	return &collectionNsMetaObj{
		IndirectTargetMap:      make(map[uint64]CollectionNamespaceList),
		SourceNamespaceTypeMap: make(map[uint64]SourceNamespaceType),
	}
}

type SourceNamespaceType int

const (
	SourceCollectionNamespace     SourceNamespaceType = iota
	SourceDefaultCollectionFilter SourceNamespaceType = iota
)

type SourceNamespace struct {
	*base.CollectionNamespace
	nsType SourceNamespaceType

	filterString string
	filter       filter.Filter
}

func NewSourceCollectionNamespace(colNs *base.CollectionNamespace) *SourceNamespace {
	return &SourceNamespace{
		nsType:              SourceCollectionNamespace,
		CollectionNamespace: colNs,
	}
}

// Since the filterUtils do not need mocking, use this global
var utils = &filter.FilterUtilsImpl{}

// Used only for outputting purposes
func NewSourceMigrationNamespaceFromColNs(namespace *base.CollectionNamespace) *SourceNamespace {
	return &SourceNamespace{
		CollectionNamespace: namespace,
		nsType:              SourceDefaultCollectionFilter,
		filterString:        namespace.CollectionName,
		filter:              nil,
	}
}

func NewSourceMigrationNamespace(expr string, dp base.DataPool) (*SourceNamespace, error) {
	filterPtr, err := filter.NewFilterWithSharedDP("", expr, utils, dp, 0, base.MobileCompatibilityOff)
	if err != nil {
		return nil, err
	}

	return &SourceNamespace{
		CollectionNamespace: &base.CollectionNamespace{
			ScopeName:      base.DefaultScopeCollectionName,
			CollectionName: expr,
		},
		nsType:       SourceDefaultCollectionFilter,
		filter:       filterPtr,
		filterString: expr,
	}, nil
}

func (s *SourceNamespace) GetCollectionNamespace() *base.CollectionNamespace {
	switch s.nsType {
	case SourceCollectionNamespace:
		return s.CollectionNamespace
	case SourceDefaultCollectionFilter:
		// For collections filter for migration, every source namespace is flat
		// so put everything at a collection-level
		return &base.CollectionNamespace{
			ScopeName:      base.DefaultScopeCollectionName,
			CollectionName: s.filterString,
		}
	default:
		panic("Implement me")
	}
}

func (s *SourceNamespace) GetType() SourceNamespaceType {
	return s.nsType
}

func (s *SourceNamespace) String() string {
	switch s.nsType {
	case SourceCollectionNamespace:
		return s.ToIndexString()
	case SourceDefaultCollectionFilter:
		return fmt.Sprintf("%v", s.filterString)
	default:
		return "?? (SourceNamespace)"
	}
}

// When used as a library, other users may need to replace the filter with a separate interface
// No locking provided. Use at your own risk
func (s *SourceNamespace) ReplaceFilter(newFilter filter.Filter) {
	s.filter = newFilter
}

func (s *SourceNamespace) GetFilterString() string {
	return s.filterString
}

// This is used for namespace mapping that transcends over manifest lifecycles
// Need to use pointers because of golang hash map support of indexable type
// This means rest needs to do some gymanistics, instead of just simply checking for pointers
type CollectionNamespaceMapping map[*SourceNamespace]CollectionNamespaceList

// Note - this constructor should be used carefully - given there is no filter
func NewDefaultCollectionMigrationMapping() CollectionNamespaceMapping {
	defaultMapping := make(CollectionNamespaceMapping)
	defaultSourceNs, _ := NewSourceMigrationNamespace("", nil)
	defaultMapping.AddSingleSourceNsMapping(defaultSourceNs, &base.DefaultCollectionNamespace)
	return defaultMapping
}

func NewCollectionNamespaceMappingFromRules(manifestsPair CollectionsManifestPair, mappingMode base.CollectionsMgtType, rules CollectionsMappingRulesType, ensureSourceExists bool, migrationBackfillDiff bool) (CollectionNamespaceMapping, error) {
	if manifestsPair.Source == nil || manifestsPair.Target == nil {
		return CollectionNamespaceMapping{}, fmt.Errorf("creating collection namespace mapping pair contains at least one nil element")
	}

	if mappingMode.IsImplicitMapping() {
		successfulMapping, _, _ := manifestsPair.Source.ImplicitMap(manifestsPair.Target)
		return successfulMapping, nil
	} else if mappingMode.IsExplicitMapping() {
		return rules.GetOutputMapping(manifestsPair, mappingMode, ensureSourceExists)
	} else if mappingMode.IsMigrationOn() {
		if rules.IsExplicitMigrationRule() {
			// Special migration mode means a single explicit mapping
			specialMappingMode := mappingMode
			specialMappingMode.SetMigration(false)
			specialMappingMode.SetExplicitMapping(true)
			return rules.GetOutputMapping(manifestsPair, specialMappingMode, ensureSourceExists)
		}
		outputMapping := CollectionNamespaceMapping{}
		// Use a single shared datapool
		sharedDp := base.NewDataPool()
		for filterExpr, targetNamespaceRaw := range rules {
			// ValidateMigrateRules() should have been called
			targetNamespaceStr := targetNamespaceRaw.(string)
			targetNamespace, _ := base.NewCollectionNamespaceFromString(targetNamespaceStr)
			sourceNs, err := NewSourceMigrationNamespace(filterExpr, sharedDp)
			if err != nil {
				// should have already validated and thus shouldn't be possible here
				continue
			}
			var migrationDiffErr error
			if migrationBackfillDiff {
				// This mode means that output is going to be used for diff purposes
				// Thus, only include the namespace if it exists on the target manifest
				_, migrationDiffErr = manifestsPair.Target.GetCollectionId(targetNamespace.ScopeName, targetNamespace.CollectionName)
			}
			if migrationDiffErr == nil {
				outputMapping.AddSingleSourceNsMapping(sourceNs, &targetNamespace)
			}
		}
		return outputMapping, nil
	}
	return CollectionNamespaceMapping{}, base.ErrorInvalidInput
}

func NewCollectionNamespaceMappingFromEvent(event *base.EventInfo) CollectionNamespaceMapping {
	retNamespace := make(CollectionNamespaceMapping)

	// We should only parse a top level event
	if event.EventType != base.BrokenMappingInfoType && event.EventDesc != "" {
		return retNamespace
	}

	event.EventExtras.GetRWLock().RLock()
	defer event.EventExtras.GetRWLock().RUnlock()
	for _, scopeLevelEventRaw := range event.EventExtras.EventsMap {
		scopeEvent, ok := scopeLevelEventRaw.(*base.EventInfo)
		if !ok {
			continue
		}
		scopeEvent.EventExtras.GetRWLock().RLock()
		for _, srcCollectionEventRaw := range scopeEvent.EventExtras.EventsMap {
			srcEvent, ok := srcCollectionEventRaw.(*base.EventInfo)
			if !ok {
				continue
			}
			// Found one source namespace
			srcColNs, _ := base.NewCollectionNamespaceFromString(srcEvent.EventDesc)

			// Go through each target namespace of this source namespace
			srcEvent.EventExtras.GetRWLock().RLock()
			for _, tgtEventRaw := range srcEvent.EventExtras.EventsMap {
				tgtEvent, ok := tgtEventRaw.(*base.EventInfo)
				if !ok {
					continue
				}
				tgtNs, _ := base.NewCollectionNamespaceFromString(tgtEvent.EventDesc)
				// Found a single source -> tgt
				retNamespace.AddSingleMapping(&srcColNs, &tgtNs)
			}
			srcEvent.EventExtras.GetRWLock().RUnlock()
		}
		scopeEvent.EventExtras.GetRWLock().RUnlock()
	}

	return retNamespace
}

func (c *CollectionNamespaceMapping) AppendToTarget(srcPtr, target *base.CollectionNamespace) {
	for k, _ := range *c {
		if k.CollectionNamespace.IsSameAs(*srcPtr) {
			(*c)[k] = append((*c)[k], target)
			return
		}
	}
}

func (c *CollectionNamespaceMapping) MarshalJSON() ([]byte, error) {
	metaObj := newCollectionNsMetaObj()

	var unsortedKeys []*base.CollectionNamespace
	for k, _ := range *c {
		unsortedKeys = append(unsortedKeys, k.GetCollectionNamespace())
	}
	sortedKeys := base.SortCollectionNamespacePtrList(unsortedKeys)

	for i, k := range sortedKeys {
		metaObj.SourceCollections = append(metaObj.SourceCollections, k)
		srcNamespacePtr, _, value, exists := c.Get(k, nil)
		if exists {
			metaObj.IndirectTargetMap[uint64(i)] = value
			metaObj.SourceNamespaceTypeMap[uint64(i)] = srcNamespacePtr.GetType()
		}
	}

	return json.Marshal(metaObj)
}

func (c *CollectionNamespaceMapping) SnappyCompress() ([]byte, error) {
	if c == nil {
		return nil, base.ErrorNilPtr
	}

	uncompressedBytes, err := c.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return snappy.Encode(nil, uncompressedBytes), nil
}

func (c *CollectionNamespaceMapping) UnmarshalJSON(b []byte) error {
	if c == nil {
		return fmt.Errorf("CollectionNamespaceMapping is nil when calling UnmarshalJSON")
	}

	metaObj := newCollectionNsMetaObj()

	err := json.Unmarshal(b, metaObj)
	if err != nil {
		return err
	}

	if (*c) == nil {
		*c = make(CollectionNamespaceMapping)
	}

	var sharedDp base.DataPool

	errMap := make(base.ErrorMap)
	var i uint64
	for i = 0; i < uint64(len(metaObj.SourceCollections)); i++ {
		sourceCol := metaObj.SourceCollections[i]
		targetCols, ok := metaObj.IndirectTargetMap[i]
		errKey := fmt.Sprintf("%v:%v", sourceCol.ScopeName, sourceCol.CollectionName)
		if !ok {
			return fmt.Errorf("Unable to unmarshal CollectionNamespaceMapping raw: %v", metaObj)
		}
		srcNamespaceType, ok := metaObj.SourceNamespaceTypeMap[i]
		if !ok {
			srcNamespaceType = SourceCollectionNamespace
		}
		switch srcNamespaceType {
		case SourceCollectionNamespace:
			(*c)[NewSourceCollectionNamespace(sourceCol)] = targetCols
		case SourceDefaultCollectionFilter:
			filterExpr := sourceCol.CollectionName
			if sharedDp == nil {
				sharedDp = base.NewDataPool()
			}
			sourceNs, err := NewSourceMigrationNamespace(filterExpr, sharedDp)
			if err != nil {
				errMap[errKey] = fmt.Errorf("trying to create filterExpr with %v resulted in %v", filterExpr, err)
				continue
			}
			(*c)[sourceNs] = targetCols
		default:
			errMap[errKey] = fmt.Errorf("invalid type %v", err)
		}
	}
	if len(errMap) > 0 {
		return fmt.Errorf(base.FlattenErrorMap(errMap))
	} else {
		return nil
	}
}

func (c *CollectionNamespaceMapping) SnappyDecompress(data []byte) error {
	if c == nil {
		return base.ErrorNilPtr
	}

	uncompressedData, err := snappy.Decode(nil, data)
	if err != nil {
		return err
	}

	return c.UnmarshalJSON(uncompressedData)
}

func (c *CollectionNamespaceMapping) MigrateString() string {
	var buffer bytes.Buffer
	for src, tgtList := range *c {
		buffer.WriteString(fmt.Sprintf("SOURCE ||%v|| -> TARGET(s) %v\n", src.CollectionName, CollectionNamespaceList(tgtList).String()))
	}
	return buffer.String()
}

func (c *CollectionNamespaceMapping) String() string {
	var buffer bytes.Buffer
	for src, tgtList := range *c {
		buffer.WriteString(fmt.Sprintf("SOURCE ||%v|| -> TARGET(s) %v\n", src.String(), CollectionNamespaceList(tgtList).String()))
	}
	return buffer.String()
}

func (c CollectionNamespaceMapping) Clone() (clone CollectionNamespaceMapping) {
	var sharedDP base.DataPool
	var getDPOnce sync.Once
	getDP := func() base.DataPool {
		getDPOnce.Do(func() {
			sharedDP = base.NewDataPool()
		})
		return sharedDP
	}

	clone = make(CollectionNamespaceMapping)
	for k, v := range c {
		srcClone := &SourceNamespace{}
		*srcClone = *k
		if srcClone.nsType == SourceDefaultCollectionFilter {
			// Need to clone filter because each filter is not supposed to be run by multiple go-routines
			// Similar to NewSourceMigrationNamespace
			filterPtr, err := filter.NewFilterWithSharedDP("", k.GetFilterString(), utils, getDP(), 0, base.MobileCompatibilityOff)
			if err != nil {
				continue
			}
			srcClone.ReplaceFilter(filterPtr)
		}
		clone[srcClone] = v.Clone()
	}
	return
}

// The input "src" does not have to match the actual key pointer in the map, just the right matching values
// Returns the srcPtr for referring to the exact tgtList
// compiledIndex is optional, and will be used if passed in
func (c *CollectionNamespaceMapping) Get(src *base.CollectionNamespace, compiledIndex CollectionNamespaceMappingIdx) (srcPtr *SourceNamespace, srcNamespacePtr *base.CollectionNamespace, tgt CollectionNamespaceList, exists bool) {
	if src == nil {
		panic("Nil source")
		return
	}

	if compiledIndex == nil {
		for k, v := range *c {
			if k.CollectionNamespace.IsSameAs(*src) {
				// found
				srcPtr = k
				tgt = v
				srcNamespacePtr = k.CollectionNamespace
				exists = true
				return
			}
		}
	} else {
		lookupString := src.ToIndexString()
		srcPtr, exists = compiledIndex[lookupString]
		if !exists {
			return
		}
		srcNamespacePtr = srcPtr.CollectionNamespace
		tgt, exists = (*c)[srcPtr]
	}
	return
}

func (c *CollectionNamespaceMapping) GetTargetUsingMigrationFilter(wrappedUprEvent *base.WrappedUprEvent, mcReq *base.WrappedMCRequest, logger *log.CommonLogger) (matchedNamespaces CollectionNamespaceMapping, errMap base.ErrorMap, errMCReqMap map[string]*base.WrappedMCRequest) {
	if c == nil {
		errMap = make(base.ErrorMap)
		errMap["GetTargetUsingMigrationFilter"] = base.ErrorInvalidInput
		return
	}

	matchedNamespaces = make(CollectionNamespaceMapping)
	for k, v := range *c {
		if k.GetType() != SourceDefaultCollectionFilter {
			continue
		}
		match, matchErr, errDesc, _, _ := k.filter.FilterUprEvent(wrappedUprEvent)
		if matchErr != nil {
			if logger != nil && logger.GetLogLevel() >= log.LogLevelDebug {
				logger.Errorf("Document %v%v%v failed filtering with err: %v - %v", base.UdTagBegin, string(wrappedUprEvent.UprEvent.Key),
					base.UdTagEnd, matchErr, errDesc)
			}
			if errMap == nil {
				errMap = make(base.ErrorMap)
				errMCReqMap = make(map[string]*base.WrappedMCRequest)
			}
			errMap[string(wrappedUprEvent.UprEvent.Key)] = matchErr
			errMCReqMap[string(wrappedUprEvent.UprEvent.Key)] = mcReq
		}

		if match {
			mcReq.SrcColNamespaceMtx.Lock()
			mcReq.SrcColNamespace = k.GetCollectionNamespace()
			mcReq.SrcColNamespaceMtx.Unlock()
			matchedNamespaces[k] = v
		}
	}
	return
}

func (c *CollectionNamespaceMapping) AddSingleSourceNsMapping(src *SourceNamespace, tgt *base.CollectionNamespace) (alreadyExists bool) {
	if src == nil || tgt == nil {
		panic("Invalid input")
		return
	}
	if src.IsEmpty() {
		panic("Empty source namespace")
	}

	_, srcPtr, tgtList, found := c.Get(src.CollectionNamespace, nil)

	if !found {
		// Just use these as entries
		var newList CollectionNamespaceList
		newList = append(newList, tgt)
		(*c)[src] = newList
	} else {
		// See if tgt already exists in the current list
		if tgtList.Contains(tgt) {
			alreadyExists = true
			return
		}

		c.AppendToTarget(srcPtr, tgt)
	}
	return
}

func (c *CollectionNamespaceMapping) AddSingleMapping(src, tgt *base.CollectionNamespace) (alreadyExists bool) {
	return c.AddSingleSourceNsMapping(NewSourceCollectionNamespace(src), tgt)
}

// Given a scope and collection, see if it exists as one of the targets in the mapping
func (c *CollectionNamespaceMapping) TargetNamespaceExists(checkNamespace *base.CollectionNamespace) bool {
	if checkNamespace == nil {
		return false
	}
	for _, tgtList := range *c {
		if tgtList.Contains(checkNamespace) {
			return true
		}
	}
	return false
}

// Given a collection namespace mapping of source to target, and given a set of "ScopesMap",
// return a subset of collection namespace mapping of the original that contain the specified target scopesmap
func (c *CollectionNamespaceMapping) GetSubsetBasedOnSpecifiedTargets(targetScopeCollections ScopesMap) (retMap CollectionNamespaceMapping) {
	retMap = make(CollectionNamespaceMapping)

	for src, tgtList := range *c {
		for _, tgt := range tgtList {
			_, found := targetScopeCollections.GetCollectionByNames(tgt.ScopeName, tgt.CollectionName)
			if found {
				retMap.AddSingleMapping(src.CollectionNamespace, tgt)
			}
		}
	}
	return
}

func (c CollectionNamespaceMapping) IsSame(otherRO CollectionNamespaceMapping) bool {
	return c.IsSubset(otherRO) && otherRO.IsSubset(c)
}

// This means if other contains everything in c
func (c CollectionNamespaceMapping) IsSubset(otherRO CollectionNamespaceMapping) bool {
	for src, tgtList := range c {
		_, _, otherTgtList, exists := otherRO.Get(src.CollectionNamespace, nil)
		if !exists {
			return false
		}
		if !tgtList.IsSame(otherTgtList) {
			return false
		}
	}
	return true
}

func (c *CollectionNamespaceMapping) Delete(subset CollectionNamespaceMapping) (result CollectionNamespaceMapping) {
	// Instead of deleting, just make a brand new map
	result = make(CollectionNamespaceMapping)

	// Subtract B from A
	for aSrc, aTgtList := range *c {
		_, _, bTgtList, exists := subset.Get(aSrc.CollectionNamespace, nil)
		if !exists {
			// No need to delete
			result[aSrc] = aTgtList
			continue
		}
		if aTgtList.IsSame(bTgtList) {
			// The whole thing is deleted
			continue
		}
		// Gather the subset list
		var newList CollectionNamespaceList
		for _, ns := range aTgtList {
			if bTgtList.Contains(ns) {
				continue
			}
			newList = append(newList, ns)
		}
		result[aSrc] = newList
	}

	return
}

func (c *CollectionNamespaceMapping) Consolidate(otherRO CollectionNamespaceMapping) {
	for otherSrcRO, otherTgtListRO := range otherRO {
		srcPtr, _, tgtList, exists := c.Get(otherSrcRO.CollectionNamespace, nil)
		if !exists {
			(*c)[otherSrcRO] = otherTgtListRO.Clone()
		} else if !tgtList.IsSame(otherTgtListRO) {
			tgtList.Consolidate(otherTgtListRO)
			(*c)[srcPtr] = tgtList
		}
	}
}

func (c *CollectionNamespaceMapping) Diff(other CollectionNamespaceMapping) (added, removed CollectionNamespaceMapping) {
	added = make(CollectionNamespaceMapping)
	removed = make(CollectionNamespaceMapping)
	// First, populated "removed"
	for src, tgtList := range *c {
		_, _, oTgtList, exists := other.Get(src.CollectionNamespace, nil)
		if !exists {
			removed[src] = tgtList
		} else if !tgtList.IsSame(oTgtList) {
			listAdded, listRemoved := tgtList.Diff(oTgtList)
			for _, addedNamespace := range listAdded {
				added.AddSingleMapping(src.CollectionNamespace, addedNamespace)
			}
			for _, removedNamespace := range listRemoved {
				removed.AddSingleMapping(src.CollectionNamespace, removedNamespace)
			}
			// Need to do reverse check
			listRemoved, listAdded = oTgtList.Diff(tgtList)
			for _, addedNamespace := range listAdded {
				added.AddSingleMapping(src.CollectionNamespace, addedNamespace)
			}
			for _, removedNamespace := range listRemoved {
				removed.AddSingleMapping(src.CollectionNamespace, removedNamespace)
			}
		}
	}

	// Then populate added
	for oSrc, oTgtList := range other {
		_, _, _, exists := c.Get(oSrc.CollectionNamespace, nil)
		if !exists {
			added[oSrc] = oTgtList
			// No else - any potential intersections would have been captured above
		}
	}
	return
}

// Json marshaller will serialize the map by key, but not necessarily the values, which is ordered list
// Because the lists may not be ordered, we need to calculate sha256 with lists ordered
func (c *CollectionNamespaceMapping) Sha256() (result [sha256.Size]byte, err error) {
	if c == nil {
		err = fmt.Errorf("Calling Sha256() on a nil CollectionNamespaceMapping")
		return
	}

	// Simpler to just create a temporary map with ordered list for sha calculation
	tempMap := make(CollectionNamespaceMapping)

	for k, v := range *c {
		tempMap[k] = SortCollectionsNamespaceList(v)
	}

	marshalledJson, err := tempMap.MarshalJSON()
	if err != nil {
		return
	}

	result = sha256.Sum256(marshalledJson)
	return
}

func (c *CollectionNamespaceMapping) ToSnappyCompressed() ([]byte, error) {
	marshalledJson, err := c.MarshalJSON()
	if err != nil {
		return nil, err
	}
	return snappy.Encode(nil, marshalledJson), nil
}

func (c *CollectionNamespaceMapping) ToExternalMap() map[string][]string {
	if c == nil || len(*c) == 0 {
		return nil
	}

	externalMap := make(map[string][]string)
	for k, v := range *c {
		for _, oneV := range v {
			externalMap[k.String()] = append(externalMap[k.String()], oneV.ToIndexString())
		}
	}
	return externalMap
}

// A collection namespace Mapping Index - The key will be simply "Scope.Collection"
type CollectionNamespaceMappingIdx map[string]*SourceNamespace

func (c CollectionNamespaceMapping) CreateLookupIndex() CollectionNamespaceMappingIdx {
	idx := make(CollectionNamespaceMappingIdx)

	for srcNs, _ := range c {
		idx[srcNs.ToIndexString()] = srcNs
	}

	return idx
}

func NewCollectionNamespaceMappingFromSnappyData(data []byte) (*CollectionNamespaceMapping, error) {
	marshalledJson, err := snappy.Decode(nil, data)
	if err != nil {
		return nil, err
	}

	newMap := make(CollectionNamespaceMapping)
	err = newMap.UnmarshalJSON(marshalledJson)

	return &newMap, err
}

type CollectionNamespaceMappingsDiffPair struct {
	Added   CollectionNamespaceMapping
	Removed CollectionNamespaceMapping

	CorrespondingSrcManifestId uint64
}

type CompressedMappings struct {
	NsMappingRecords CompressedShaMappingList `json:"NsMappingRecords"`

	// internal id of repl spec - for detection of repl spec deletion and recreation event
	SpecInternalId string `json:"specInternalId"`

	//revision number
	revision interface{}
}

type GlobalTimestampCompressedDoc CompressedMappings

func (g *GlobalTimestampCompressedDoc) ToShaMap() (ShaToGlobalTimestampMap, error) {
	if g == nil {
		return nil, fmt.Errorf("Calling ToShaMap() on a nil GlobalTimestampCompresedDoc")
	}

	errorMap := make(base.ErrorMap)
	shaMap := make(ShaToGlobalTimestampMap)
	for _, oneRecord := range g.NsMappingRecords {
		if oneRecord == nil {
			continue
		}

		serializedBytes, err := snappy.Decode(nil, oneRecord.CompressedMapping)
		if err != nil {
			errorMap[oneRecord.Sha256Digest] = fmt.Errorf("Snappy decompress failed %v", err)
			continue
		}

		var serializedMap map[string]interface{}
		err = json.Unmarshal(serializedBytes, &serializedMap)
		if err != nil {
			errorMap[oneRecord.Sha256Digest] = fmt.Errorf("Unmarshalling data failed %v", err)
			continue
		}

		actualMap := make(GlobalTimestamp)
		err = actualMap.LoadUnmarshalled(serializedMap)
		if err != nil {
			errorMap[oneRecord.Sha256Digest] = fmt.Errorf("Unmarshalling failed %v", err)
			continue
		}
		// Sanity check
		checkSha, err := actualMap.Sha256()
		if err != nil {
			errorMap[oneRecord.Sha256Digest] = fmt.Errorf("Validing SHA failed %v", err)
			continue
		}
		checkShaString := fmt.Sprintf("%x", checkSha[:])
		if checkShaString != oneRecord.Sha256Digest {
			errorMap[oneRecord.Sha256Digest] = fmt.Errorf("SHA validation mismatch %v", checkShaString)
			continue
		}

		shaMap[oneRecord.Sha256Digest] = &actualMap
	}

	var err error
	if len(errorMap) > 0 {
		errStr := base.FlattenErrorMap(errorMap)
		err = fmt.Errorf(errStr)
	}
	return shaMap, err

}

type CollectionNsMappingsDoc CompressedMappings

func (b *CollectionNsMappingsDoc) Size() int {
	if b == nil {
		return 0
	}

	return len(b.SpecInternalId) + b.NsMappingRecords.Size()
}

func (b *CollectionNsMappingsDoc) ToShaMap() (ShaToCollectionNamespaceMap, error) {
	if b == nil {
		return nil, fmt.Errorf("Calling ToShaMap() on a nil CollectionNsMappingsDoc")
	}

	errorMap := make(base.ErrorMap)
	shaMap := make(ShaToCollectionNamespaceMap)

	for _, oneRecord := range b.NsMappingRecords {
		if oneRecord == nil {
			continue
		}

		serializedMap, err := snappy.Decode(nil, oneRecord.CompressedMapping)
		if err != nil {
			errorMap[oneRecord.Sha256Digest] = fmt.Errorf("Snappy decompress failed %v", err)
			continue
		}
		actualMap := make(CollectionNamespaceMapping)
		err = json.Unmarshal(serializedMap, &actualMap)
		if err != nil {
			errorMap[oneRecord.Sha256Digest] = fmt.Errorf("Unmarshalling failed %v", err)
			continue
		}
		// Sanity check
		checkSha, err := actualMap.Sha256()
		if err != nil {
			errorMap[oneRecord.Sha256Digest] = fmt.Errorf("Validing SHA failed %v", err)
			continue
		}
		checkShaString := fmt.Sprintf("%x", checkSha[:])
		if checkShaString != oneRecord.Sha256Digest {
			errorMap[oneRecord.Sha256Digest] = fmt.Errorf("SHA validation mismatch %v", checkShaString)
			continue
		}

		shaMap[oneRecord.Sha256Digest] = &actualMap
	}

	var err error
	if len(errorMap) > 0 {
		errStr := base.FlattenErrorMap(errorMap)
		err = fmt.Errorf(errStr)
	}
	return shaMap, err
}

// Will overwrite the existing records with the incoming map
func (b *CollectionNsMappingsDoc) LoadShaMap(shaMap ShaToCollectionNamespaceMap) error {
	if b == nil {
		return base.ErrorInvalidInput
	}

	errorMap := make(base.ErrorMap)
	b.NsMappingRecords = b.NsMappingRecords[:0]

	for sha, colNsMap := range shaMap {
		if colNsMap == nil {
			continue
		}
		compressedMapping, err := colNsMap.ToSnappyCompressed()
		if err != nil {
			errorMap[sha] = err
			continue
		}

		oneRecord := &CompressedShaMapping{compressedMapping, sha}
		b.NsMappingRecords.SortedInsert(oneRecord)
	}

	if len(errorMap) > 0 {
		return fmt.Errorf("Error LoadingShaMap - sha -> err: %v", base.FlattenErrorMap(errorMap))
	} else {
		return nil
	}
}

func (b *CollectionNsMappingsDoc) SameAs(other *CollectionNsMappingsDoc) bool {
	if b == nil && other != nil {
		return false
	} else if b != nil && other == nil {
		return false
	} else if b == nil && other == nil {
		return true
	}

	return b.SpecInternalId == other.SpecInternalId && b.NsMappingRecords.SameAs(other.NsMappingRecords)
}

type ShaToCollectionNamespaceMap map[string]*CollectionNamespaceMapping

func (s *ShaToCollectionNamespaceMap) Clone() (newMap ShaToCollectionNamespaceMap) {
	if s == nil {
		return
	}

	newMap = make(ShaToCollectionNamespaceMap)

	for k, v := range *s {
		clonedVal := v.Clone()
		newMap[k] = &clonedVal
	}
	return
}

func (s *ShaToCollectionNamespaceMap) String() string {
	if s == nil {
		return "<nil>"
	}

	var output []string
	for k, v := range *s {
		output = append(output, fmt.Sprintf("Sha256Digest: %v Map:", k))
		if v != nil {
			output = append(output, v.String())
		}
	}

	return strings.Join(output, "\n")
}

func (s ShaToCollectionNamespaceMap) Diff(older ShaToCollectionNamespaceMap) (added, removed ShaToCollectionNamespaceMap) {
	if len(older) == 0 && len(s) > 0 {
		added = s
		return
	} else if len(older) > 0 && len(s) == 0 {
		removed = s
		return
	}

	added = make(ShaToCollectionNamespaceMap)
	removed = make(ShaToCollectionNamespaceMap)

	for k, v := range s {
		if _, exists := older[k]; !exists {
			added[k] = v
		}
	}

	for k, v := range older {
		if _, exists := s[k]; !exists {
			removed[k] = v
		}
	}

	return
}

func (s ShaToCollectionNamespaceMap) SameAs(other ShaToCollectionNamespaceMap) bool {
	added, removed := s.Diff(other)
	return len(added) == 0 && len(removed) == 0
}

func (s *ShaToCollectionNamespaceMap) Merge(other ShaToCollectionNamespaceMap) {
	for k, v := range other {
		if _, exists := (*s)[k]; !exists {
			(*s)[k] = v
		}
	}
}

func (s *ShaToCollectionNamespaceMap) CompressToShaCompressedMap(preExistMap ShaMappingCompressedMap) error {
	if s == nil {
		return base.ErrorNilPtr
	}

	errorMap := make(base.ErrorMap)

	for sha, brokenMap := range *s {
		if _, exists := preExistMap[sha]; exists {
			continue
		}
		compressedBytes, compressErr := brokenMap.ToSnappyCompressed()
		if compressErr != nil {
			errorMap[fmt.Sprintf("BrokenMap: %v", brokenMap.String())] = fmt.Errorf("Unable to snappyCompress")
		}
		preExistMap[sha] = compressedBytes
	}

	if len(errorMap) == 0 {
		return nil
	} else {
		return fmt.Errorf(base.FlattenErrorMap(errorMap))
	}
}

type CompressedShaMapping struct {
	// Snappy compressed byte slice of Sha -> Data mapping
	CompressedMapping []byte `json:compressedMapping`
	Sha256Digest      string `json:string`
}

func (c *CompressedShaMapping) String() string {
	return fmt.Sprintf("Sha: %v Bytes: %v", c.Sha256Digest, fmt.Sprintf("%x", c.CompressedMapping[:]))
}

func (c *CompressedShaMapping) Size() int {
	if c == nil {
		return 0
	}
	return len(c.CompressedMapping) + len(c.Sha256Digest)
}

type CompressedShaMappingList []*CompressedShaMapping

func (c *CompressedShaMappingList) Size() int {
	if c == nil {
		return 0
	}

	var totalSize int
	for _, j := range *c {
		totalSize += j.Size()
	}
	return totalSize
}

func (c *CompressedShaMappingList) SortedInsert(elem *CompressedShaMapping) {
	if c == nil {
		return
	}

	if len(*c) == 0 {
		*c = append(*c, elem)
		return
	}

	var i int
	// First, find where this should be
	for i = 0; i < len(*c); i++ {
		if (*c)[i].Sha256Digest > elem.Sha256Digest {
			break
		}
	}

	*c = append(*c, nil)
	copy((*c)[i+1:], (*c)[i:])
	(*c)[i] = elem
}

func (c *CompressedShaMappingList) SameAs(other CompressedShaMappingList) bool {
	if c == nil {
		return false
	}

	if len(*c) != len(other) {
		return false
	}

	for i, v := range *c {
		if v.Sha256Digest != other[i].Sha256Digest {
			return false
		} else if !reflect.DeepEqual(v.CompressedMapping, other[i].CompressedMapping) {
			return false
		}
	}

	return true
}

func ValidateAndConvertStringToMappingRuleType(value string) (CollectionsMappingRulesType, error) {
	// Check for duplicated keys
	res, err := base.JsonStringReEncodeTest(value)
	if err != nil {
		return CollectionsMappingRulesType{}, err
	}
	if !res {
		return CollectionsMappingRulesType{}, base.ErrorJSONReEncodeFailed
	}

	// Because adv filtering won't work if space is removed - jsonMap should be the original version
	jsonMap, err := base.ValidateAndConvertStringToJsonType(value)
	if err != nil {
		return nil, err
	}

	return ValidateAndConvertJsonMapToRuleType(jsonMap)
}

func ValidateAndConvertJsonMapToRuleType(jsonMap map[string]interface{}) (CollectionsMappingRulesType, error) {
	rulesOut := make(CollectionsMappingRulesType)
	for k, v := range jsonMap {
		if v == nil {
			rulesOut[k] = nil
			continue
		}
		vString, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("value for key %v is the wrong type: %v", k, reflect.TypeOf(v))
		}
		rulesOut[k] = vString
	}
	return rulesOut, nil
}

type CollectionsMappingRulesType map[string]interface{}

func (c CollectionsMappingRulesType) Clone() CollectionsMappingRulesType {
	clonedCopy := make(CollectionsMappingRulesType)
	for k, v := range c {
		clonedCopy[k] = v
	}
	return clonedCopy
}

// Assumes these are migration rules
func (c CollectionsMappingRulesType) CloneAndRedact() CollectionsMappingRulesType {
	redactedClone := make(CollectionsMappingRulesType)

	for k, v := range c {
		redactedK := fmt.Sprintf("%v%v%v", base.UdTagBegin, k, base.UdTagEnd)
		redactedClone[redactedK] = v
	}

	return redactedClone
}

func (c CollectionsMappingRulesType) ValidateMigrateRules() error {
	errorMap := make(base.ErrorMap)
	if c.IsExplicitMigrationRule() {
		return nil
	}
	migrationExplicitRuleErr := c.CheckForExplicitMigrationRuleViolation()
	if migrationExplicitRuleErr != nil {
		return migrationExplicitRuleErr
	}

	for filterExpr, targetNamespaceRaw := range c {
		// filterExpr must be a valid gojsonsm expression
		_, err := base.ValidateAndGetAdvFilter(filterExpr)
		if err != nil {
			errorMap[filterExpr] = err
		}

		// targetnamespace must be a string type, no nil allowed
		targetNamespace, ok := targetNamespaceRaw.(string)
		if !ok {
			errorMap[fmt.Sprintf("%v (value)", filterExpr)] = fmt.Errorf("Invalid value type specified: %v", reflect.TypeOf(targetNamespaceRaw))
		}

		// targetnamespace must be a specific form of ScopeName.CollectionName
		_, err = base.NewCollectionNamespaceFromString(targetNamespace)
		if err != nil {
			errorMap[targetNamespace] = err
		}
	}

	if len(errorMap) > 0 {
		return fmt.Errorf(base.FlattenErrorMap(errorMap))
	} else {
		return nil
	}
}

// Explicit migration rule is a single explicit mapping from a source collection to a specific target collection
func (c CollectionsMappingRulesType) IsExplicitMigrationRule() bool {
	if len(c) != 1 {
		return false
	}

	for k, v := range c {
		checkSourceNs, err := base.NewCollectionNamespaceFromString(k)
		if err != nil {
			return false
		}

		if !checkSourceNs.IsDefault() {
			return false
		}

		targetNsString, ok := v.(string)
		if !ok {
			return false
		}
		targetNs, err := base.NewCollectionNamespaceFromString(targetNsString)
		if err != nil {
			return false
		}
		if targetNs.IsEmpty() {
			return false
		}
	}
	return true
}

var ErrorDenyRuleMigrationModeNotAllowed = fmt.Errorf("Deny rules are not allowed for migration mode")
var ErrorMigrationExplicitOnlyOneAllowed = fmt.Errorf("One single rule migrating from default collection to a target collection cannot be accompanied by any other rules")
var ErrorMigrationExplicitScopeToScopeNotAllowedStr = "seems like a scope to scope rule, which is not allowed under migration mode"

// If migration mode is turned on and is not explicit migration rule, check to make sure
// user didn't make an error of trying to:
// 1. Create a explicit migration rule
// 2. Add a non-explicit migration rule that contains filter expressions
func (c CollectionsMappingRulesType) CheckForExplicitMigrationRuleViolation() error {
	// Look for a default namespace key
	for k, v := range c {
		checkSourceNs, err := base.NewCollectionNamespaceFromString(k)
		if err != nil {
			// Check to see if user entered a scope -> scope rule
			vStr, ok := v.(string)
			if !ok {
				return ErrorDenyRuleMigrationModeNotAllowed
			}
			if base.CollectionNameValidationRegex.MatchString(k) && base.CollectionNameValidationRegex.MatchString(vStr) && base.ValidateAdvFilter(k) != nil {
				return fmt.Errorf("%v -> %v %v", k, vStr, ErrorMigrationExplicitScopeToScopeNotAllowedStr)
			}
		}
		if checkSourceNs.IsDefault() {
			targetNsString, ok := v.(string)
			if !ok {
				return ErrorDenyRuleMigrationModeNotAllowed
			}
			targetNs, err := base.NewCollectionNamespaceFromString(targetNsString)
			if err != nil {
				continue
			}
			if !targetNs.IsDefault() && !targetNs.IsEmpty() {
				// We have found the one and only default.default -> targetscope.targetCol rule
				// in admist of other types of rules. Print out a nice message
				return ErrorMigrationExplicitOnlyOneAllowed
			}
		} else if checkSourceNs.IsSystemScope() {
			return base.ErrorSystemScopeMapped
		}
		if vStr, ok := v.(string); ok {
			checkTargetNs, err := base.NewCollectionNamespaceFromString(vStr)
			if err == nil && checkTargetNs.IsSystemScope() {
				return base.ErrorSystemScopeMapped
			}
		}
	}
	return nil
}

func (c CollectionsMappingRulesType) ValidateExplicitMapping() error {
	validator := base.NewExplicitMappingValidator()
	for k, v := range c {
		err := validator.ValidateKV(k, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c CollectionsMappingRulesType) SameAs(otherRaw interface{}) bool {
	other, ok := otherRaw.(CollectionsMappingRulesType)
	if !ok {
		return false
	}
	if len(c) != len(other) {
		return false
	}
	for k, v := range c {
		v2, exists := other[k]
		if !exists {
			return false
		}
		if v != v2 {
			return false
		}
	}
	return true
}

// Returns non-nil error if this collection was never meant to be replicated given the rules
// Returns nil error and nil namespace if it's denied replication
func (c CollectionsMappingRulesType) GetPotentialTargetNamespaces(sourceNs *base.CollectionNamespace) ([]*base.CollectionNamespace, error) {
	var returnNamespaces []*base.CollectionNamespace
	// Check case 1 or 2
	rule1Key := fmt.Sprintf("%v%v%v", sourceNs.ScopeName, base.ScopeCollectionDelimiter, sourceNs.CollectionName)
	targetRule, exists := c[rule1Key]
	if exists {
		if targetRule == nil {
			return nil, nil
		} else {
			// Should not return error
			retNs, err := base.NewCollectionNamespaceFromString(targetRule.(string))
			returnNamespaces = append(returnNamespaces, &retNs)
			return returnNamespaces, err
		}
	}

	// This means the rules did not have S.C listed. Check just "S"
	targetRule, exists = c[sourceNs.ScopeName]
	if !exists {
		return nil, base.ErrorInvalidInput
	}

	if targetRule == nil {
		// case 4
		return nil, nil
	} else {
		// case 3
		retNs := &base.CollectionNamespace{ScopeName: targetRule.(string), CollectionName: sourceNs.CollectionName}
		returnNamespaces = append(returnNamespaces, retNs)
		return returnNamespaces, nil
	}
}

// Match in this priority
// 1. S.C -> TS.TC
// 2. S.C -> nil
// 3. S -> TS
// 4. S -> nil
func (c CollectionsMappingRulesType) GetOutputMapping(pair CollectionsManifestPair, mode base.CollectionsMgtType, ensureSourceExists bool) (CollectionNamespaceMapping, error) {
	if mode.IsMigrationOn() {
		return nil, base.ErrorInvalidInput
	}
	var sourceNotFoundErr = fmt.Errorf("the specific source namespace does not exist")

	outNamespace := make(CollectionNamespaceMapping)
	emptyNamespace := &base.CollectionNamespace{}
	// At this stage, rules should have already been validated

	var sourceDNEmap base.ErrorMap
	if ensureSourceExists {
		sourceDNEmap = make(base.ErrorMap)
	}

	// First populate rule 1 - S.C -> S.C
	sourceNamespacesWithNoTarget := make(map[string]string)
	specificNamespaceAlreadyAssigned := make(map[string]string) // key - scopeName, value - collectionName
	for ruleKey, ruleValRaw := range c {
		ruleVal, ok := ruleValRaw.(string)
		if !ok || !strings.Contains(ruleKey, base.ScopeCollectionDelimiter) || !strings.Contains(ruleVal, base.ScopeCollectionDelimiter) {
			continue
		}

		sourceNamespace, err := base.NewCollectionNamespaceFromString(ruleKey)
		if err != nil {
			return nil, err
		}
		targetNamespace, err := base.NewCollectionNamespaceFromString(ruleVal)
		if err != nil {
			return nil, err
		}

		_, sourceFoundErr := pair.Source.GetCollectionId(sourceNamespace.ScopeName, sourceNamespace.CollectionName)
		_, targetFoundErr := pair.Target.GetCollectionId(targetNamespace.ScopeName, targetNamespace.CollectionName)
		if sourceFoundErr != nil {
			if ensureSourceExists {
				sourceDNEmap[sourceNamespace.ToIndexString()] = sourceNotFoundErr
			}
			continue
		}
		if targetFoundErr != nil {
			sourceNamespacesWithNoTarget[sourceNamespace.ScopeName] = sourceNamespace.CollectionName
			continue
		}
		outNamespace.AddSingleMapping(&sourceNamespace, &targetNamespace)
		specificNamespaceAlreadyAssigned[sourceNamespace.ScopeName] = sourceNamespace.CollectionName
	}

	// Then populate rule2 S.C -> null
	for ruleKey, ruleValRaw := range c {
		if !strings.Contains(ruleKey, base.ScopeCollectionDelimiter) || ruleValRaw != nil {
			continue
		}

		sourceNamespace, err := base.NewCollectionNamespaceFromString(ruleKey)
		if err != nil {
			return nil, err
		}

		outNamespace.AddSingleMapping(&sourceNamespace, emptyNamespace)
		specificNamespaceAlreadyAssigned[sourceNamespace.ScopeName] = sourceNamespace.CollectionName
	}

	// Populate S -> S'
	for ruleKey, ruleValRaw := range c {
		ruleVal, ok := ruleValRaw.(string)
		if !ok || strings.Contains(ruleKey, base.ScopeCollectionDelimiter) || strings.Contains(ruleVal, base.ScopeCollectionDelimiter) {
			continue
		}
		sourceScopeName := ruleKey
		sourceCollections, err := pair.Source.GetAllCollectionsGivenScopeRO(sourceScopeName)
		if err == base.ErrorNotFound {
			if ensureSourceExists {
				sourceDNEmap[sourceScopeName] = sourceNotFoundErr
			}
			continue
		}

		targetScopeName := ruleVal
		targetCollections, err := pair.Target.GetAllCollectionsGivenScopeRO(targetScopeName)
		if err == base.ErrorNotFound {
			continue
		}

		for sourceColName, _ := range sourceCollections {
			targetCol, targetFound := targetCollections[sourceColName]
			if !targetFound {
				continue
			}

			if sourceColNameCheck, ok := specificNamespaceAlreadyAssigned[sourceScopeName]; ok && sourceColName == sourceColNameCheck {
				continue
			}

			sourceNamespace := &base.CollectionNamespace{
				ScopeName:      sourceScopeName,
				CollectionName: sourceColName,
			}
			targetNamespace := &base.CollectionNamespace{
				ScopeName:      targetScopeName,
				CollectionName: targetCol.Name,
			}

			// Only add if S.C doesn't exist, because any S.C is higher priority than plain S -> *
			alreadyExistsColName, scopeExists := sourceNamespacesWithNoTarget[sourceScopeName]
			if scopeExists && alreadyExistsColName == sourceColName {
				continue
			}

			outNamespace.AddSingleMapping(sourceNamespace, targetNamespace)
		}
	}

	// Populate S -> null
	for ruleKey, ruleValRaw := range c {
		if strings.Contains(ruleKey, base.ScopeCollectionDelimiter) || ruleValRaw != nil {
			continue
		}

		sourceScopeName := ruleKey
		sourceCollections, err := pair.Source.GetAllCollectionsGivenScopeRO(sourceScopeName)
		if err == base.ErrorNotFound {
			if ensureSourceExists {
				sourceDNEmap[sourceScopeName] = sourceNotFoundErr
			}
			continue
		}

		for sourceColName, _ := range sourceCollections {
			if sourceColNameCheck, ok := specificNamespaceAlreadyAssigned[sourceScopeName]; ok && sourceColName == sourceColNameCheck {
				continue
			}

			sourceNamespace := &base.CollectionNamespace{
				ScopeName:      sourceScopeName,
				CollectionName: sourceColName,
			}

			// Only add if S.C doesn't exist, because any S.C is higher priority than plain S -> *
			alreadyExistsColName, scopeExists := sourceNamespacesWithNoTarget[sourceScopeName]
			if scopeExists && alreadyExistsColName == sourceColName {
				continue
			}
			outNamespace.AddSingleMapping(sourceNamespace, emptyNamespace)
		}
	}

	if len(sourceDNEmap) > 0 {
		return nil, fmt.Errorf(base.FlattenErrorMap(sourceDNEmap))
	}

	return outNamespace, nil
}

type PipelineEventBrokenMap struct {
	// Each pipeline can have one single broken map
	cachedBrokenMapMtx     sync.RWMutex
	cachedBrokenMap        CollectionNamespaceMapping
	cachedBrokenMapUpdated bool
	// Indexes
	cachedBrokenMapSrcEventIdIdx   map[string]int64
	cachedBrokenMapSrcNamespaceIdx CollectionNamespaceMappingIdx
	cachedBrokenMapSrcScopeIdx     map[string]int64

	// remember user can dismiss specific brokenmap
	userDismissedBrokenMap CollectionNamespaceMapping
	userDismissedBrokenIdx CollectionNamespaceMappingIdx
	userDismissUpdated     bool
}

func NewPipelineEventBrokenMap() PipelineEventBrokenMap {
	ret := PipelineEventBrokenMap{
		cachedBrokenMapMtx:             sync.RWMutex{},
		cachedBrokenMap:                make(CollectionNamespaceMapping),
		cachedBrokenMapUpdated:         false,
		cachedBrokenMapSrcEventIdIdx:   make(map[string]int64),
		cachedBrokenMapSrcNamespaceIdx: make(CollectionNamespaceMappingIdx),
		cachedBrokenMapSrcScopeIdx:     make(map[string]int64),
		userDismissedBrokenMap:         make(CollectionNamespaceMapping),
		userDismissedBrokenIdx:         make(CollectionNamespaceMappingIdx),
	}
	return ret
}

func (p *PipelineEventBrokenMap) LoadLatestBrokenMap(roBrokenMap CollectionNamespaceMapping) {
	var needToLoad bool
	p.cachedBrokenMapMtx.RLock()
	if !p.cachedBrokenMap.IsSame(roBrokenMap) {
		needToLoad = true
	}
	p.cachedBrokenMapMtx.RUnlock()

	if needToLoad {
		p.cachedBrokenMapMtx.Lock()
		if needToLoad {
			p.cachedBrokenMap = roBrokenMap.Clone()
			p.cachedBrokenMapSrcNamespaceIdx = p.cachedBrokenMap.CreateLookupIndex()
			p.cachedBrokenMapUpdated = true
		}
		p.cachedBrokenMapMtx.Unlock()
	}
}

func (p *PipelineEventBrokenMap) NeedsToUpdate() bool {
	p.cachedBrokenMapMtx.RLock()
	needToUpdate := p.cachedBrokenMapUpdated || p.userDismissUpdated
	p.cachedBrokenMapMtx.RUnlock()
	return needToUpdate
}

func (p *PipelineEventBrokenMap) GetBrokenMapRO() (CollectionNamespaceMapping, func()) {
	p.cachedBrokenMapMtx.RLock()
	doneFunc := func() {
		p.cachedBrokenMapMtx.RUnlock()
	}

	return p.cachedBrokenMap, doneFunc
}

func (p *PipelineEventBrokenMap) GetDismissedMapWMtx() (CollectionNamespaceMapping, CollectionNamespaceMappingIdx, *sync.RWMutex) {
	return p.userDismissedBrokenMap, p.userDismissedBrokenIdx, &p.cachedBrokenMapMtx
}

// Returns true if event is empty
func (p *PipelineEventBrokenMap) ExportToEvent(event *base.EventInfo, idWell *int64, isMigrationMode bool) bool {
	// Note locking order -> cachedBrokenMap first then event
	p.cachedBrokenMapMtx.RLock()
	defer p.cachedBrokenMapMtx.RUnlock()

	if !event.EventExtras.IsNil() && event.EventExtras.IsEmpty() && len(p.userDismissedBrokenMap) == 0 {
		srcIndex, scopeIndex := p.loadEmptyEvent(event, idWell)
		eventIsEmpty := len(event.EventExtras.EventsMap) == 0
		p.upgradeLockAndUpdateCachedBrokenMapIndexes(eventIsEmpty, srcIndex, scopeIndex)
		p.writeLegacyErrMsg(event, isMigrationMode, p.cachedBrokenMap)
		return eventIsEmpty
	}

	if len(p.userDismissedBrokenMap) > 0 && p.userDismissedBrokenMap.IsSame(p.cachedBrokenMap) {
		// Force the event to be empty
		event.SetHint(nil)
		return true
	}

	// First delete any mappings that no longer exist or user has dismissed
	eventCompiledBrokenMap := p.cleanUpObsoleteSourcesFromEvent(event)

	// Then add things that are missing into event
	p.addMissingBrokenMappingIntoEvent(event, idWell, eventCompiledBrokenMap)

	eventIsEmpty := event.EventExtras.Len() == 0

	if eventIsEmpty {
		event.SetHint(nil)
		p.recreateNewSrcIndexes(true)
	} else {
		// Write the summary message to event
		p.writeLegacyErrMsg(event, isMigrationMode, *eventCompiledBrokenMap)
	}

	return eventIsEmpty
}

func (p *PipelineEventBrokenMap) writeLegacyErrMsg(event *base.EventInfo, isMigrationMode bool, legacyErrorMsgOutputMap CollectionNamespaceMapping) {
	var buffer bytes.Buffer
	buffer.WriteString(base.BrokenMappingUIString)
	if isMigrationMode {
		buffer.WriteString(legacyErrorMsgOutputMap.MigrateString())
	} else {
		buffer.WriteString(legacyErrorMsgOutputMap.String())
	}
	event.SetHint(buffer.String())
}

func (p *PipelineEventBrokenMap) upgradeLockAndUpdateCachedBrokenMapIndexes(eventIsEmpty bool, srcIndex map[string]int64, scopeIndex map[string]int64) {
	p.cachedBrokenMapMtx.RUnlock()
	p.cachedBrokenMapMtx.Lock()
	if !eventIsEmpty {
		p.cachedBrokenMapSrcEventIdIdx = srcIndex
		p.cachedBrokenMapSrcScopeIdx = scopeIndex
	} else {
		p.recreateNewSrcIndexes(false)
	}
	p.cachedBrokenMapMtx.Unlock()
	p.cachedBrokenMapMtx.RLock()
}

func (p *PipelineEventBrokenMap) recreateNewSrcIndexes(upgradeLockNeeded bool) {
	if upgradeLockNeeded {
		p.cachedBrokenMapMtx.RUnlock()
		p.cachedBrokenMapMtx.Lock()
	}
	p.cachedBrokenMapSrcScopeIdx = make(map[string]int64)
	p.cachedBrokenMapSrcEventIdIdx = make(map[string]int64)
	if upgradeLockNeeded {
		p.cachedBrokenMapMtx.Unlock()
		p.cachedBrokenMapMtx.RLock()
	}
}

// Takes the current broken map, filter out user's intent on dismissal, and write them to event
func (p *PipelineEventBrokenMap) addMissingBrokenMappingIntoEvent(event *base.EventInfo, idWell *int64, eventCompiledBrokenMap *CollectionNamespaceMapping) {
	event.EventExtras.GetRWLock().RLock()
	defer event.EventExtras.GetRWLock().RUnlock()

	// Given current event version's of the brokenmap, diff against the actual broken map. Whatever is not here needs to be added only if user didn't filter it out
	added, _ := eventCompiledBrokenMap.Diff(p.cachedBrokenMap)
	for srcNs, tgtList := range added {
		if p.userHasDismissedThisNamespace(srcNs, tgtList) {
			// We need to actually reflect what user has dismissed from the eventCompiledBrokenMap
			// This map will be accurate and thus can be printed as the actual brokenMap event summary string
			// for legacy UI
			cleanupMap := make(CollectionNamespaceMapping)
			cleanupMap[srcNs] = tgtList
			*eventCompiledBrokenMap = eventCompiledBrokenMap.Delete(cleanupMap)
			continue
		}

		scopeEventId, scopeEventExists := p.cachedBrokenMapSrcScopeIdx[srcNs.ScopeName]
		var scopeEvent *base.EventInfo
		if !scopeEventExists {
			scopeEvent = base.NewEventInfo()
			scopeEvent.EventType = base.BrokenMappingInfoType
			scopeEvent.EventId = base.GetEventIdFromWell(idWell)
			scopeEvent.EventDesc = srcNs.ScopeName
			eventMapLockDone := event.EventExtras.UpgradeLock()
			event.EventExtras.EventsMap[scopeEvent.EventId] = scopeEvent
			eventMapLockDone()
			p.cachedBrokenMapSrcScopeIdx[srcNs.ScopeName] = scopeEvent.EventId
		} else {
			scopeEvent = event.EventExtras.EventsMap[scopeEventId].(*base.EventInfo)
		}
		scopeEvent.EventExtras.GetRWLock().Lock()

		srcEventId, srcExists := p.cachedBrokenMapSrcEventIdIdx[srcNs.ToIndexString()]
		var srcEvent *base.EventInfo
		if !srcExists {
			srcEvent = base.NewEventInfo()
			srcEvent.EventType = base.BrokenMappingInfoType
			srcEvent.EventId = base.GetEventIdFromWell(idWell)
			srcEvent.EventDesc = srcNs.ToIndexString()
			srcEvent.SetHint(srcNs)
			scopeEvent.EventExtras.EventsMap[srcEvent.EventId] = srcEvent
			p.cachedBrokenMapSrcEventIdIdx[srcNs.ToIndexString()] = srcEvent.EventId
		} else {
			srcEvent = scopeEvent.EventExtras.EventsMap[srcEventId].(*base.EventInfo)
		}
		srcEvent.EventExtras.GetRWLock().Lock()
		// At this point all the tgts here are missing
		for _, tgtNs := range tgtList {
			tgtEvent := base.NewEventInfo()
			tgtEvent.EventId = base.GetEventIdFromWell(idWell)
			tgtEvent.EventType = base.BrokenMappingInfoType
			tgtEvent.EventDesc = tgtNs.ToIndexString()
			tgtEvent.SetHint(tgtNs)
			srcEvent.EventExtras.EventsMap[tgtEvent.EventId] = tgtEvent
		}
		srcEvent.EventExtras.GetRWLock().Unlock()

		scopeEvent.EventExtras.GetRWLock().Unlock()
	}

}

// Given the current broken map, and given an event, remove obsolete entries from event that are no longer present in
// the current broken map. At the same time, return A compiled broken map that represents an intersection between the
// current brokenmap and the event broken map
func (p *PipelineEventBrokenMap) cleanUpObsoleteSourcesFromEvent(event *base.EventInfo) *CollectionNamespaceMapping {
	event.EventExtras.GetRWLock().RLock()
	defer event.EventExtras.GetRWLock().RUnlock()

	eventCompiledBrokenMap := make(CollectionNamespaceMapping)
	// BrokenMapping Event map is in the format of:
	// Level0 : Whole Broken Map Event       <- The event incoming argument is at this level
	// ------------------------------------
	// Level1: Source Scope Event (SrcScope)
	//   Level2: SrcScope.SrcCollection Event (Could have >1 tgt collections)
	//     Level3: SrcScope.SrcCollection -> TgtScope.TgtCollection Event

	// Level 1 - whole source scope event
	for wholeScopeEventId, wholeSrcScopeEventRaw := range event.EventExtras.EventsMap {
		wholeSrcScopeEvent := wholeSrcScopeEventRaw.(*base.EventInfo)
		wholeSrcScopeEvent.EventExtras.GetRWLock().RLock()
		//  Variables used to keep track of whether or not this whole scope should be deleted
		var wholeScopeStillExists bool
		numSrcCollectionsUnderScope := len(wholeSrcScopeEvent.EventExtras.EventsMap)
		var numLevel2Deleted int
		// Level 2 - srcScope.srcCollection event
		for srcEventId, srcNsEventRaw := range wholeSrcScopeEvent.EventExtras.EventsMap {
			srcNsEvent := srcNsEventRaw.(*base.EventInfo)
			srcNamespace := srcNsEvent.GetHint().(*SourceNamespace)
			_, _, tgtList, srcExists := p.cachedBrokenMap.Get(srcNamespace.CollectionNamespace, p.cachedBrokenMapSrcNamespaceIdx)
			numTgtForThisSrcEvt := srcNsEvent.EventExtras.Len()
			if !srcExists || numTgtForThisSrcEvt == 0 {
				// The whole source scope:collection no longer exists
				p.tempUpgradeLockAndDelL2EventFromL1(wholeSrcScopeEvent, srcEventId, srcNamespace, &numSrcCollectionsUnderScope)
			} else {
				// At least one collection still exists under this scope
				wholeScopeStillExists = true
				// Level 3 - srcScope.srcCollection -> tgtScope.TgtCollection event check
				tgtLock := srcNsEvent.EventExtras.GetRWLock()
				tgtLock.RLock()
				// Variables to see if the whole srcScope.srcCollection event (lvl2) should be deleted
				numTgts := len(srcNsEvent.EventExtras.EventsMap)
				var numLevel3Deleted int
				for subEventId, tgtNsEventRaw := range srcNsEvent.EventExtras.EventsMap {
					found, tgtNamespace := p.findTargetNamespaceGivenEvent(tgtNsEventRaw, tgtList)

					// If the event's target namespace is not found in the latest broken map, which means that the event
					// is outdated since the brokenmap is always source of truth...
					// OR if it is found but the specific mapping has been dismissed
					// don't export it to the event, and ensure that it is removed from existing event
					if !found || p.userHasDismissedThisNamespace(srcNamespace, CollectionNamespaceList{tgtNamespace}) {
						p.tempUpgradeLockAndDelL3EventFromL2(srcNsEvent, subEventId, &numLevel3Deleted)
					} else {
						// At this stage, cachedBrokenMap states that srcNs->tgtNs is valid
						// And user hasn't dismissed it
						eventCompiledBrokenMap.AddSingleSourceNsMapping(srcNamespace, tgtNamespace)
					}
				}
				tgtLock.RUnlock()
				if numTgts == numLevel3Deleted {
					//All the individual target ns for this source ns is deleted - the source should be deleted too
					p.tempUpgradeLockAndDelL2EventFromL1(wholeSrcScopeEvent, srcEventId, srcNamespace, &numLevel2Deleted)
				}
			}
		}
		wholeSrcScopeEvent.EventExtras.GetRWLock().RUnlock()
		if !wholeScopeStillExists || numSrcCollectionsUnderScope == numLevel2Deleted {
			p.tempUpgradeLockAndDeleteL1EventFromL0Event(event, wholeSrcScopeEvent, wholeScopeEventId)
		}
	}

	return &eventCompiledBrokenMap
}

func (p *PipelineEventBrokenMap) findTargetNamespaceGivenEvent(tgtNsEventRaw interface{}, tgtList CollectionNamespaceList) (bool, *base.CollectionNamespace) {
	var found bool
	var tgtNamespace *base.CollectionNamespace
	tgtEvent := tgtNsEventRaw.(*base.EventInfo)
	for _, tgtNs := range tgtList {
		tgtNamespace = tgtEvent.GetHint().(*base.CollectionNamespace)
		if tgtNs.IsSameAs(*tgtNamespace) {
			found = true
			break
		}
	}
	return found, tgtNamespace
}

// Delete a S.C:St.Ct event from the parent of a S.C event
func (p *PipelineEventBrokenMap) tempUpgradeLockAndDelL3EventFromL2(srcNsEvent *base.EventInfo, subEventId int64, numDeleted *int) {
	doneFunc := srcNsEvent.EventExtras.UpgradeLock()
	defer doneFunc()
	delete(srcNsEvent.EventExtras.EventsMap, subEventId)
	*numDeleted++
}

// Delete a S.C from the parent of a S event
func (p *PipelineEventBrokenMap) tempUpgradeLockAndDelL2EventFromL1(wholeSrcScopeEvent *base.EventInfo, srcEventId int64, srcNamespace *SourceNamespace, numSrcCollectionsDeleted *int) {
	doneFunc := wholeSrcScopeEvent.EventExtras.UpgradeLock()
	defer doneFunc()
	delete(wholeSrcScopeEvent.EventExtras.EventsMap, srcEventId)
	delete(p.cachedBrokenMapSrcEventIdIdx, srcNamespace.ToIndexString())
	*numSrcCollectionsDeleted++
}

// No lock is needed because the top level of brokenMap was locked already
func (p *PipelineEventBrokenMap) tempUpgradeLockAndDeleteL1EventFromL0Event(event *base.EventInfo, wholeSrcScopeEvent *base.EventInfo, wholeScopeEventId int64) {
	doneFunc := event.EventExtras.UpgradeLock()
	defer doneFunc()
	delete(p.cachedBrokenMapSrcScopeIdx, wholeSrcScopeEvent.EventDesc)
	delete(event.EventExtras.EventsMap, wholeScopeEventId)
}

// Loads brokenmap into an empty event
// Returns the indexes needed
func (p *PipelineEventBrokenMap) loadEmptyEvent(event *base.EventInfo, idWell *int64) (map[string]int64, map[string]int64) {
	srcEventIdx := make(map[string]int64)
	scopeEventIdx := make(map[string]int64)

	event.EventExtras.GetRWLock().Lock()
	defer event.EventExtras.GetRWLock().Unlock()
	var wholeScopeEventId int64
	var exists bool
	for srcNs, tgtList := range p.cachedBrokenMap {
		wholeScopeEventId, exists = scopeEventIdx[srcNs.ScopeName]
		if !exists {
			// First create a eventID that tags the whole scope
			wholeScopeEventId = base.GetEventIdFromWell(idWell)
			newWholeScopeEvent := base.NewEventInfo()
			newWholeScopeEvent.EventId = wholeScopeEventId
			newWholeScopeEvent.EventType = base.BrokenMappingInfoType
			newWholeScopeEvent.EventDesc = srcNs.ScopeName
			event.EventExtras.EventsMap[wholeScopeEventId] = newWholeScopeEvent
			scopeEventIdx[srcNs.ScopeName] = wholeScopeEventId
		}
		wholeScopeEvent := event.EventExtras.EventsMap[wholeScopeEventId].(*base.EventInfo)
		wholeScopeEvent.EventExtras.GetRWLock().Lock()

		// Then for each individual scope:col -> scopeT:colT, create individual events underneath
		newSrcEventId := base.GetEventIdFromWell(idWell)
		newSrcEventInfo := base.NewEventInfo()
		newSrcEventInfo.EventId = newSrcEventId
		newSrcEventInfo.EventType = base.BrokenMappingInfoType
		newSrcEventInfo.EventDesc = srcNs.ToIndexString()
		newSrcEventInfo.SetHint(srcNs)
		newSrcEventInfo.EventExtras.GetRWLock().Lock()

		// create the index
		srcEventIdx[srcNs.ToIndexString()] = newSrcEventId

		for _, tgtNs := range tgtList {
			newTgtEventId := base.GetEventIdFromWell(idWell)
			newTgtEvent := base.NewEventInfo()
			newTgtEvent.EventId = newTgtEventId
			newTgtEvent.EventType = base.BrokenMappingInfoType
			newTgtEvent.EventDesc = tgtNs.ToIndexString()
			newTgtEvent.SetHint(tgtNs)
			newSrcEventInfo.EventExtras.EventsMap[newTgtEventId] = newTgtEvent
		}
		newSrcEventInfo.EventExtras.GetRWLock().Unlock()
		wholeScopeEvent.EventExtras.EventsMap[newSrcEventId] = newSrcEventInfo
		wholeScopeEvent.EventExtras.GetRWLock().Unlock()
	}
	return srcEventIdx, scopeEventIdx
}

func (p *PipelineEventBrokenMap) RegisterDismissAction(level int, srcDesc string, tgtDesc string) error {
	var userRequestedMapping CollectionNamespaceMapping
	var err error
	// First, translate input into an actual namespace mapping
	switch level {
	case 0:
		// dismiss the whole current broken mapping mapping
		p.cachedBrokenMapMtx.RLock()
		userRequestedMapping = p.cachedBrokenMap.Clone()
		p.cachedBrokenMapMtx.RUnlock()
	case 1:
		userRequestedMapping, err = p.translateLevel1(srcDesc)
	case 2:
		userRequestedMapping, err = p.translateLevel2(srcDesc)
	case 3:
		userRequestedMapping, err = p.translateLevel3(srcDesc, tgtDesc)
	default:
		panic("Invalid case")
	}

	if err != nil {
		return err
	}

	p.cachedBrokenMapMtx.Lock()
	defer p.cachedBrokenMapMtx.Unlock()
	p.userDismissedBrokenMap.Consolidate(userRequestedMapping)
	p.userDismissedBrokenIdx = p.userDismissedBrokenMap.CreateLookupIndex()
	p.userDismissUpdated = true
	return nil
}

func (p *PipelineEventBrokenMap) translateLevel1(desc string) (CollectionNamespaceMapping, error) {
	retMap := make(CollectionNamespaceMapping)
	p.cachedBrokenMapMtx.RLock()
	defer p.cachedBrokenMapMtx.RUnlock()
	// Level 1 means a whole scope
	for srcNs, v := range p.cachedBrokenMap {
		// If each s.c matches the description as the source scope name
		// there may be multiple ones... i.e. s.c1 + s.c2, both are considered hit
		if srcNs.ScopeName == desc {
			newSrc := srcNs.Clone()
			retMap[NewSourceCollectionNamespace(&newSrc)] = v.Clone()
		}
	}
	if len(retMap) == 0 {
		return nil, base.ErrorNotFound
	}
	return retMap, nil
}

func (p *PipelineEventBrokenMap) translateLevel2(desc string) (CollectionNamespaceMapping, error) {
	retMap := make(CollectionNamespaceMapping)
	p.cachedBrokenMapMtx.RLock()
	defer p.cachedBrokenMapMtx.RUnlock()
	// Level 2 means a singular s.c source namespace
	for srcNs, v := range p.cachedBrokenMap {
		if srcNs.ToIndexString() != desc {
			continue
		}
		// found
		newSrc := srcNs.Clone()
		retMap[NewSourceCollectionNamespace(&newSrc)] = v.Clone()
		return retMap, nil
	}
	return nil, base.ErrorNotFound
}

func (p *PipelineEventBrokenMap) translateLevel3(srcDesc string, tgtDesc string) (CollectionNamespaceMapping, error) {
	retMap := make(CollectionNamespaceMapping)
	p.cachedBrokenMapMtx.RLock()
	defer p.cachedBrokenMapMtx.RUnlock()
	// Level 3 means an exact match
	for srcNs, v := range p.cachedBrokenMap {
		if srcNs.ToIndexString() != srcDesc {
			continue
		}
		// found source
		for _, tgtNs := range v {
			if tgtNs.ToIndexString() != tgtDesc {
				continue
			}
			// found tgt and thus exact match
			retMap.AddSingleMapping(srcNs.CollectionNamespace, tgtNs)
			return retMap, nil
		}
	}
	return nil, base.ErrorNotFound
}

func (p *PipelineEventBrokenMap) MarkUpdated() {
	p.cachedBrokenMapMtx.Lock()
	defer p.cachedBrokenMapMtx.Unlock()

	p.cachedBrokenMapUpdated = false
	p.userDismissUpdated = false

}

// Check the user dismissed broken map to see if the input <srcNs + tgtNs's> is contained in the user's intent
// Returns true if it is a complete match - in other words, user has dismissed this
func (p *PipelineEventBrokenMap) userHasDismissedThisNamespace(ns *SourceNamespace, list CollectionNamespaceList) bool {
	if len(p.userDismissedBrokenMap) == 0 {
		return false
	}

	_, _, tgtList, exists := p.userDismissedBrokenMap.Get(ns.CollectionNamespace, p.userDismissedBrokenIdx)
	if !exists {
		return false
	}
	for _, oneNs := range list {
		if !tgtList.Contains(oneNs) {
			return false
		}
	}
	// ns exists in dismissedBrokenMap AND all elements in list exists in the tgtList
	return true
}

func (p *PipelineEventBrokenMap) ResetAllDismissedHistory() {
	p.cachedBrokenMapMtx.Lock()
	defer p.cachedBrokenMapMtx.Unlock()

	p.userDismissedBrokenMap = make(CollectionNamespaceMapping)
	p.userDismissedBrokenIdx = make(CollectionNamespaceMappingIdx)
	p.userDismissUpdated = true
}

// In implicit mapping, we need to diff srcManifests deltas to see which source namespaces have been removed
// and thus can be cleaned up
func (p *PipelineEventBrokenMap) UpdateWithNewDiffPair(diffPair *CollectionNamespaceMappingsDiffPair, srcManifestsDelta []*CollectionsManifest) error {
	p.cachedBrokenMapMtx.RLock()
	defer p.cachedBrokenMapMtx.RUnlock()

	if len(p.userDismissedBrokenMap) == 0 && len(p.cachedBrokenMap) == 0 {
		return nil
	}

	var userDismissedUpdated bool
	var brokenMapUpdated bool
	if len(diffPair.Added) > 0 {
		// added means that these namespace mappings are being backfilled
		// If anyone of them was dismissed before, remove them so that if it is broken again
		// the brokenEvent will be reported
		for srcNs, tgtList := range diffPair.Added {
			p.upgradeLockAndUpdateDismissedMapWithUpdatedPair(srcNs, tgtList, &userDismissedUpdated)
			p.upgradeLockAndUpdateCachedBrokenmapWithUpdatedPair(srcNs, tgtList, &brokenMapUpdated)
		}
	}

	if len(diffPair.Removed) > 0 {
		for srcNs, _ := range diffPair.Removed {
			p.upgradeLockAndCleanupDismissedMapWithUpdatedPair(srcNs, &userDismissedUpdated)
			p.upgradeLockAndCleanupCachedBrokenMapWithUpdatedPair(srcNs, &brokenMapUpdated)
		}
	}

	if len(srcManifestsDelta) == 2 && srcManifestsDelta[0] != nil && srcManifestsDelta[1] != nil {
		_, _, removed, err := srcManifestsDelta[1].Diff(srcManifestsDelta[0])
		if err == nil && len(removed) > 0 {
			p.upgradeLockAndRemoveOutdatedSourceNamespacesFromDismissedMap(removed, &userDismissedUpdated)
			p.upgradeLockAndRemoveOutdatedSourceNamespacesFromBrokenMap(removed, &brokenMapUpdated)
		}
	}

	if userDismissedUpdated || brokenMapUpdated {
		p.cachedBrokenMapMtx.RUnlock()
		p.cachedBrokenMapMtx.Lock()
		if userDismissedUpdated {
			p.userDismissedBrokenIdx = p.userDismissedBrokenMap.CreateLookupIndex()
			p.userDismissUpdated = true
		}
		if brokenMapUpdated {
			p.cachedBrokenMapSrcNamespaceIdx = p.cachedBrokenMap.CreateLookupIndex()
			p.cachedBrokenMapUpdated = true
		}
		p.cachedBrokenMapMtx.Unlock()
		p.cachedBrokenMapMtx.RLock()
	}
	return nil
}

func (p *PipelineEventBrokenMap) upgradeLockAndUpdateDismissedMapWithUpdatedPair(srcNs *SourceNamespace, tgtList CollectionNamespaceList, updated *bool) {
	dismissedSrcPtr, _, dismissedTgtList, exists := p.userDismissedBrokenMap.Get(srcNs.GetCollectionNamespace(), p.userDismissedBrokenIdx)
	if !exists {
		return
	}

	// Check to see if the tgt match
	var replacementList CollectionNamespaceList
	for _, oneTgt := range dismissedTgtList {
		if !tgtList.Contains(oneTgt) {
			replacementList = append(replacementList, oneTgt)
		}
	}
	if len(replacementList) > 0 && !replacementList.IsSame(dismissedTgtList) {
		p.cachedBrokenMapMtx.RUnlock()
		p.cachedBrokenMapMtx.Lock()
		p.userDismissedBrokenMap[dismissedSrcPtr] = replacementList
		*updated = true
		p.cachedBrokenMapMtx.Unlock()
		p.cachedBrokenMapMtx.RLock()
	} else if len(replacementList) == 0 {
		p.cachedBrokenMapMtx.RUnlock()
		p.cachedBrokenMapMtx.Lock()
		delete(p.userDismissedBrokenMap, dismissedSrcPtr)
		*updated = true
		p.cachedBrokenMapMtx.Unlock()
		p.cachedBrokenMapMtx.RLock()
	}
}

func (p *PipelineEventBrokenMap) upgradeLockAndUpdateCachedBrokenmapWithUpdatedPair(srcNs *SourceNamespace, tgtList CollectionNamespaceList, updated *bool) {
	brokenMapSrcPtr, _, brokenMapTgtList, exists := p.cachedBrokenMap.Get(srcNs.GetCollectionNamespace(), p.cachedBrokenMapSrcNamespaceIdx)
	if !exists {
		return
	}

	// Check to see if the tgt match
	var replacementList CollectionNamespaceList
	for _, oneTgt := range brokenMapTgtList {
		if !tgtList.Contains(oneTgt) {
			replacementList = append(replacementList, oneTgt)
		}
	}
	if len(replacementList) > 0 && !replacementList.IsSame(brokenMapTgtList) {
		p.cachedBrokenMapMtx.RUnlock()
		p.cachedBrokenMapMtx.Lock()
		p.cachedBrokenMap[brokenMapSrcPtr] = replacementList
		*updated = true
		p.cachedBrokenMapMtx.Unlock()
		p.cachedBrokenMapMtx.RLock()
	} else if len(replacementList) == 0 {
		p.cachedBrokenMapMtx.RUnlock()
		p.cachedBrokenMapMtx.Lock()
		delete(p.cachedBrokenMap, brokenMapSrcPtr)
		*updated = true
		p.cachedBrokenMapMtx.Unlock()
		p.cachedBrokenMapMtx.RLock()
	}
}

func (p *PipelineEventBrokenMap) upgradeLockAndCleanupDismissedMapWithUpdatedPair(srcNs *SourceNamespace, updated *bool) {
	srcPtr, _, _, exists := p.userDismissedBrokenMap.Get(srcNs.GetCollectionNamespace(), p.userDismissedBrokenIdx)
	if !exists {
		return
	}

	p.cachedBrokenMapMtx.RUnlock()
	p.cachedBrokenMapMtx.Lock()
	delete(p.userDismissedBrokenMap, srcPtr)
	*updated = true
	p.cachedBrokenMapMtx.Unlock()
	p.cachedBrokenMapMtx.RLock()
}

func (p *PipelineEventBrokenMap) upgradeLockAndCleanupCachedBrokenMapWithUpdatedPair(srcNs *SourceNamespace, updated *bool) {
	srcPtr, _, _, exists := p.cachedBrokenMap.Get(srcNs.GetCollectionNamespace(), p.cachedBrokenMapSrcNamespaceIdx)
	if !exists {
		return
	}

	p.cachedBrokenMapMtx.RUnlock()
	p.cachedBrokenMapMtx.Lock()
	delete(p.cachedBrokenMap, srcPtr)
	delete(p.cachedBrokenMapSrcEventIdIdx, srcPtr.ToIndexString())
	*updated = true
	p.cachedBrokenMapMtx.Unlock()
	p.cachedBrokenMapMtx.RLock()
}

// When source namespaces are removed, make sure the internals are cleaned up too
func (p *PipelineEventBrokenMap) upgradeLockAndRemoveOutdatedSourceNamespacesFromDismissedMap(removed ScopesMap, updated *bool) {
	var lockUpgraded bool

	var reusedNamespace base.CollectionNamespace
	// Check to make sure at least one needs to be removed
	for scopeName, scope := range removed {
		for collectionName, _ := range scope.Collections {
			reusedNamespace.ScopeName = scopeName
			reusedNamespace.CollectionName = collectionName

			srcPtr, _, _, exists := p.userDismissedBrokenMap.Get(&reusedNamespace, p.userDismissedBrokenIdx)
			if exists {
				// This removed source namespace needs to be cleared
				if !lockUpgraded {
					lockUpgraded = true
					p.cachedBrokenMapMtx.RUnlock()
					p.cachedBrokenMapMtx.Lock()
				}
				*updated = true
				delete(p.userDismissedBrokenMap, srcPtr)
			}
		}
	}

	if lockUpgraded {
		p.cachedBrokenMapMtx.Unlock()
		p.cachedBrokenMapMtx.RLock()
	}
}

func (p *PipelineEventBrokenMap) upgradeLockAndRemoveOutdatedSourceNamespacesFromBrokenMap(removed ScopesMap, updated *bool) {
	var lockUpgraded bool
	var reusedNamespace base.CollectionNamespace
	// Check to make sure at least one needs to be removed
	for scopeName, scope := range removed {
		for collectionName, _ := range scope.Collections {
			reusedNamespace.ScopeName = scopeName
			reusedNamespace.CollectionName = collectionName

			srcPtr, _, _, exists := p.cachedBrokenMap.Get(&reusedNamespace, p.cachedBrokenMapSrcNamespaceIdx)
			if exists {
				// This removed source namespace needs to be cleared
				if !lockUpgraded {
					lockUpgraded = true
					p.cachedBrokenMapMtx.RUnlock()
					p.cachedBrokenMapMtx.Lock()
				}
				*updated = true
				delete(p.cachedBrokenMap, srcPtr)
			}
		}
	}

	if lockUpgraded {
		p.cachedBrokenMapMtx.Unlock()
		p.cachedBrokenMapMtx.RLock()
	}
}

type ManifestsCache map[uint64]*CollectionsManifest

func (m ManifestsCache) String() string {
	var output []string
	for k, v := range m {
		output = append(output, fmt.Sprintf("%v:%v\n", k, v))
	}
	return strings.Join(output, " ")
}

func (m ManifestsCache) GetMaxManifestID() uint64 {
	var max uint64
	for k, v := range m {
		if v == nil {
			// This is a problem...
			continue
		}
		if k > max {
			max = k
		}
	}
	return max
}

func (m *ManifestsCache) Clone() *ManifestsCache {
	if m == nil {
		return nil
	}
	clonedCache := make(ManifestsCache)
	for k, v := range *m {
		vClone := v.Clone()
		clonedCache[k] = &vClone
	}
	return &clonedCache
}

func (m *ManifestsCache) LoadIfNotExists(other *ManifestsCache) {
	if other == nil || m == nil {
		return
	}
	for k, v := range *other {
		if _, exists := (*m)[k]; !exists {
			(*m)[k] = v
		}
	}
}

func (m *ManifestsCache) SameAs(other *ManifestsCache) bool {
	if m == nil && other != nil {
		return false
	} else if m != nil && other == nil {
		return false
	} else if m == nil && other == nil {
		return true
	}

	if len(*m) != len(*other) {
		return false
	}

	for k, v := range *m {
		otherV, exists := (*other)[k]
		if !exists {
			return false
		}
		if !v.SameAs(otherV) {
			return false
		}
	}
	return true
}
