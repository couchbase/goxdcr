package metadata

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

type ManifestsDoc struct {
	CollectionsManifests []*CollectionsManifest `json:"collection_manifests"`

	//revision number
	Revision interface{}
}

type CollectionsManifestPair struct {
	Source *CollectionsManifest
	Target *CollectionsManifest
}

func NewCollectionsManifestPair(source, target *CollectionsManifest) *CollectionsManifestPair {
	return &CollectionsManifestPair{
		Source: source,
		Target: target,
	}
}

// Manifest structure representing the JSON returned from ns_server's endpoint
type CollectionsManifest struct {
	uid    uint64
	scopes ScopesMap
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

func (c *CollectionsManifest) GetScopeAndCollectionName(collectionId uint64) (scopeName, collectionName string, err error) {
	for _, scope := range c.Scopes() {
		for _, collection := range scope.Collections {
			if collection.Uid == collectionId {
				scopeName = scope.Name
				collectionName = collection.Name
				return
			}
		}
	}
	err = base.ErrorNotFound
	return
}

func (c *CollectionsManifest) GetCollectionId(scopeName, collectionName string) (uint64, error) {
	for _, scope := range c.Scopes() {
		if scopeName == scope.Name {
			for _, collection := range scope.Collections {
				if collection.Name == collectionName {
					return collection.Uid, nil
				}
			}
		}
	}
	return 0, base.ErrorNotFound
}

// TODO - meta obj
type collectionsTempObj struct {
	// data for unmarshalling and parsing
	UidTemp_    string        `json:"uid"`
	ScopesTemp_ []interface{} `json:"scopes"`
}

func newCollectionsTempObj() *collectionsTempObj {
	return &collectionsTempObj{}
}

// Used if there's an error getting collections manifest
// Assume there is default collection. In the rare cases where customers deleted the default
// collection, DCP should error... but that is only if both customers deleted the default
// collection AND we have issues retrieving manifest from local ns_server, which is rare
func NewDefaultCollectionsManifest() CollectionsManifest {
	defaultManifest := CollectionsManifest{scopes: make(ScopesMap)}

	defaultScope := NewEmptyScope(base.DefaultScopeCollectionName, 0)
	defaultCollection := Collection{Uid: 0, Name: base.DefaultScopeCollectionName}

	defaultScope.Collections[base.DefaultScopeCollectionName] = defaultCollection

	defaultManifest.scopes[base.DefaultScopeCollectionName] = defaultScope

	return defaultManifest
}

func NewCollectionsManifestFromMap(manifestInfo map[string]interface{}) (CollectionsManifest, error) {
	tempObj := newCollectionsTempObj()
	var manifest CollectionsManifest

	if uid, ok := manifestInfo["uid"].(string); ok {
		tempObj.UidTemp_ = uid
	} else {
		return manifest, fmt.Errorf("Uid is not float64, but %v instead", reflect.TypeOf(manifestInfo["uid"]))
	}

	if scopes, ok := manifestInfo["scopes"].([]interface{}); ok {
		tempObj.ScopesTemp_ = scopes
	} else {
		return manifest, base.ErrorInvalidInput
	}

	err := manifest.Load(tempObj)
	if err != nil {
		return manifest, err
	}
	return manifest, nil
}

func NewCollectionsManifestFromBytes(data []byte) (CollectionsManifest, error) {
	var manifest CollectionsManifest
	err := manifest.LoadBytes(data)
	return manifest, err
}

// For unit test
func TestNewCollectionsManifestFromBytesWithCustomUid(data []byte, uid uint64) (CollectionsManifest, error) {
	var manifest CollectionsManifest
	err := manifest.LoadBytes(data)
	manifest.uid = uid
	return manifest, err
}

// Does not clone temporary variables
func (c CollectionsManifest) Clone() CollectionsManifest {
	return CollectionsManifest{
		uid:    c.uid,
		scopes: c.scopes.Clone(),
	}
}

func (this *CollectionsManifest) Equals(other *CollectionsManifest) bool {
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
		if !scope.Equals(otherScope) {
			return false
		}
	}

	return true
}

func (c *CollectionsManifest) Diff(older *CollectionsManifest) (addedOrModified, removed ScopesMap, err error) {
	if c == nil || older == nil {
		return ScopesMap{}, ScopesMap{}, base.ErrorInvalidInput
	}

	if c.Uid() < older.Uid() {
		err = fmt.Errorf("Should compare against an older version of manifest")
		return
	} else if c.Equals(older) {
		return
	}

	addedOrModified = make(ScopesMap)
	removed = make(ScopesMap)

	// First, find things that exists in the current manifest that doesn't exist in the older one
	// or versions are different, which by contract means they are modified and newer (uid never go backwards)
	for scopeName, scope := range c.Scopes() {
		olderScope, exists := older.Scopes()[scopeName]
		if !exists {
			addedOrModified[scopeName] = scope
		} else if exists && !scope.Equals(olderScope) {
			addedOrModified[scopeName] = NewEmptyScope(scopeName, scope.Uid)
			// At least one collection is different
			for collectionName, collection := range scope.Collections {
				olderCollection, exists := olderScope.Collections[collectionName]
				if !exists || exists && !collection.Equals(olderCollection) {
					addedOrModified[scopeName].Collections[collectionName] = collection
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

func (c *CollectionsManifest) Load(collectionsTemp *collectionsTempObj) error {
	var err error
	c.uid, err = strconv.ParseUint(collectionsTemp.UidTemp_, base.CollectionsUidBase, 64)
	c.scopes = make(ScopesMap)
	for _, oneScopeDetail := range collectionsTemp.ScopesTemp_ {
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
	return nil
}

func (c *CollectionsManifest) LoadBytes(data []byte) error {
	collectionsTemp := newCollectionsTempObj()
	err := json.Unmarshal(data, collectionsTemp)
	if err != nil {
		return err
	}

	return c.Load(collectionsTemp)
}

// Implements the marshaler interface
func (c *CollectionsManifest) MarshalJSON() ([]byte, error) {
	collectionsTemp := newCollectionsTempObj()
	collectionsTemp.UidTemp_ = fmt.Sprintf("%x", c.uid)

	// marshal scopes in order of names - this will ensure equality between two identical manifests
	var scopeNames []string
	for name, _ := range c.scopes {
		scopeNames = append(scopeNames, name)
	}
	scopeNames = base.SortStringList(scopeNames)

	for _, scopeName := range scopeNames {
		scope := c.scopes[scopeName]
		collectionsTemp.ScopesTemp_ = append(collectionsTemp.ScopesTemp_, scope.toScopeDetail())
	}

	outBytes, _ := json.Marshal(collectionsTemp)
	return outBytes, nil
}

func (c *CollectionsManifest) UnmarshalJSON(b []byte) error {
	collectionsTemp := newCollectionsTempObj()

	err := json.Unmarshal(b, collectionsTemp)
	if err != nil {
		return err
	}

	return c.Load(collectionsTemp)
}

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

type ScopesMap map[string]Scope

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

type Scope struct {
	Uid         uint64
	Name        string
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
	uid, err := strconv.ParseUint(uidString, base.CollectionsUidBase, 64)
	if err != nil {
		return Scope{}, err
	}

	collectionsDetail, ok := scopeDetail[base.CollectionsKey].([]interface{})
	if !ok {
		return Scope{}, base.ErrorInvalidInput
	}

	collectionsMap, err := NewCollectionsMap(collectionsDetail)
	if err != nil {
		return Scope{}, err
	}

	return Scope{
		Name:        name,
		Uid:         uid,
		Collections: collectionsMap,
	}, nil
}

func NewEmptyScope(name string, uid uint64) Scope {
	return Scope{
		Name:        name,
		Uid:         uid,
		Collections: make(CollectionsMap),
	}
}

func (this *Scope) Equals(other Scope) bool {
	if this.Uid != other.Uid {
		return false
	}
	if this.Name != other.Name {
		return false
	}
	if !this.Collections.Equals(other.Collections) {
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

type Collection struct {
	Uid  uint64
	Name string
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
		uid, err := strconv.ParseUint(uidStr, base.CollectionsUidBase, 64)
		if err != nil {
			return nil, err
		}

		collectionMap[name] = Collection{
			Uid:  uid,
			Name: name,
		}
	}
	return collectionMap, nil
}

func (this *Collection) Equals(other Collection) bool {
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

func (this *CollectionsMap) Equals(other CollectionsMap) bool {
	for colName, collection := range *this {
		otherCollection, ok := other[colName]
		if !ok {
			return false
		}
		if !collection.Equals(otherCollection) {
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
