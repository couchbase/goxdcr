package metadata

import (
	"bytes"
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
	if c == nil {
		err = base.ErrorInvalidInput
		return
	}
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
	if c == nil {
		return 0, base.ErrorInvalidInput
	}
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
	return c.scopes.Clone()
}

/**
 * When remote cluster has collections or scopes management changes, then there is potentially
 * a need for backfill
 * This method is run on the latest-pulled target manifest
 * Given the previous known target manifest, and also the current source manifest,
 * this method will pump out what collections on the targets that need to be backfilled
 * in the form of CollectionToCollectionsMapping
 */
func (target *CollectionsManifest) GetBackfillCollectionIDs(prevTarget, source *CollectionsManifest) (CollectionToCollectionsMapping, error) {
	// First, between the two different target manifests, find what has been added or modified
	added, modified, _, err := target.Diff(prevTarget)
	if err != nil {
		return nil, err
	}

	backfillNeeded := make(CollectionToCollectionsMapping)
	// These are most current collections that are mapped from source to target
	srcToTargetMapping, _, _ := source.MapAsSourceToTargetByName(target)

	for srcCol, targetCols := range srcToTargetMapping {
		for _, targetCol := range targetCols {
			// For each target collection, get the scope name and target collection name
			scopeName, collectionName, err := target.GetScopeAndCollectionName(targetCol.Uid)
			if err != nil {
				return nil, err
			}
			_, found := added.GetCollectionByNames(scopeName, collectionName)
			if found {
				backfillNeeded.Add(srcCol, targetCol)
			}
			_, found = modified.GetCollectionByNames(scopeName, collectionName)
			if found {
				backfillNeeded.Add(srcCol, targetCol)
			}
		}
	}
	return backfillNeeded, nil
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

type CollectionList []*Collection

func (c CollectionList) Contains(cid uint64) bool {
	for _, collection := range c {
		if collection != nil && collection.Uid == cid {
			return true
		}
	}
	return false
}

type CollectionToCollectionsMapping map[*Collection][]*Collection

// Implements marshaller interface
// This will be used as part of the backfill replication storage
func (c *CollectionToCollectionsMapping) MarshalJSON() ([]byte, error) {
	if c == nil {
		return nil, base.ErrorInvalidInput
	}

	marshalObj := newc2cMarshalObj()

	var i int
	for k, v := range *c {
		marshalObj.SourceCollections = append(marshalObj.SourceCollections, k)
		marshalObj.IndirectTargetMap[strconv.Itoa(i)] = v
		i++
	}

	return json.Marshal(marshalObj)
}

// Implements marshaller interface
func (c *CollectionToCollectionsMapping) UnmarshalJSON(b []byte) error {
	if c == nil {
		return base.ErrorInvalidInput
	}

	unmarshalObj := &c2cMarshalObj{}

	err := json.Unmarshal(b, unmarshalObj)
	if err != nil {
		return err
	}

	for i := 0; i < len(unmarshalObj.SourceCollections); i++ {
		sourceCol := unmarshalObj.SourceCollections[i]
		targetCols, ok := unmarshalObj.IndirectTargetMap[strconv.Itoa(i)]
		if !ok {
			return fmt.Errorf("Unable to unmarshal CollectionToCollectionsMapping raw: %v", unmarshalObj)
		}
		(*c)[sourceCol] = targetCols
	}

	return nil
}

func (c *CollectionToCollectionsMapping) Add(sourceCol, targetCol *Collection) (alreadyExists bool) {
	if c == nil {
		return
	}

	targetList, exists := (*c)[sourceCol]
	if !exists {
		(*c)[sourceCol] = make([]*Collection, 0)
		targetList = (*c)[sourceCol]
	}

	for _, checkTarget := range targetList {
		if checkTarget.IsSameAs(*targetCol) {
			alreadyExists = true
			return
		}
	}

	(*c)[sourceCol] = append((*c)[sourceCol], targetCol)
	return
}

func (c *CollectionToCollectionsMapping) IsSameAs(other *CollectionToCollectionsMapping) bool {
	if c == nil && other != nil || c != nil && other == nil {
		return false
	} else if c == nil && other == nil {
		return true
	}

	if len(*c) != len(*other) {
		return false
	}

	for k, v := range *c {
		otherV, ok := (*other)[k]
		if !ok {
			return false
		} else if !(CollectionsPtrList)(v).IsSameAs((CollectionsPtrList)(otherV)) {
			return false
		}
	}

	for k, v := range *other {
		origV, ok := (*c)[k]
		if !ok {
			return false
		} else if !(CollectionsPtrList)(v).IsSameAs((CollectionsPtrList)(origV)) {
			return false
		}
	}

	return true
}

// Used as part of Mapping source to target by name
type CollectionIdKeyedMap map[uint64]Collection

func (c CollectionIdKeyedMap) Diff(other CollectionIdKeyedMap) (missing, missingFromOther CollectionIdKeyedMap) {
	missing = make(CollectionIdKeyedMap)
	missingFromOther = make(CollectionIdKeyedMap)

	// Cheat here by using sorting and compare
	var cList []uint64
	var oList []uint64

	for cid, _ := range c {
		cList = append(cList, cid)
	}
	for cid, _ := range other {
		oList = append(oList, cid)
	}

	cList = base.SortUint64List(cList)
	oList = base.SortUint64List(oList)

	var cIdx int
	var oIdx int

	for cIdx = 0; cIdx < len(cList); cIdx++ {
		if cList[cIdx] == oList[oIdx] {
			if oIdx < len(oList) {
				oIdx++
			}
		} else if cList[cIdx] < oList[oIdx] {
			missingFromOther[cList[cIdx]] = c[cList[cIdx]]
		} else /* cList[cIdx] > oList[oIdx] */ {
			missing[oList[oIdx]] = other[oList[oIdx]]
			oIdx++
			if oIdx == len(oList) {
				break
			}
		}
	}

	for ; cIdx < len(cList); cIdx++ {
		missingFromOther[cList[cIdx]] = c[cList[cIdx]]
	}

	for ; oIdx < len(oList); oIdx++ {
		missing[oList[oIdx]] = other[oList[oIdx]]
	}

	return
}

// Given one "source" manifest and one "target" manifest, try to map implicitly by name
// Returns:
func (sourceManifest *CollectionsManifest) MapAsSourceToTargetByName(targetManifest *CollectionsManifest) (successfulMapping CollectionToCollectionsMapping, unmappedSources CollectionIdKeyedMap, unmappedTarget CollectionIdKeyedMap) {
	if sourceManifest == nil || targetManifest == nil {
		return
	}

	successfulMapping = make(CollectionToCollectionsMapping)
	unmappedSources = make(CollectionIdKeyedMap)
	unmappedTarget = make(CollectionIdKeyedMap)

	// First mark all of them as unmappedTarget
	for _, scope := range targetManifest.Scopes() {
		for _, collection := range scope.Collections {
			unmappedTarget[collection.Uid] = collection
		}
	}

	for scopeName, scope := range sourceManifest.Scopes() {
		targetScope, exists := targetManifest.Scopes()[scopeName]
		if !exists {
			// All of these collections are unmapped
			for _, collection := range scope.Collections {
				unmappedSources[collection.Uid] = collection
			}
		} else {
			for collectionName, collection := range scope.Collections {
				targetCollection, exists := targetScope.Collections[collectionName]
				if !exists {
					unmappedSources[collection.Uid] = collection
				} else {
					colCopy := collection.Clone()
					successfulMapping[&colCopy] = append(successfulMapping[&colCopy], &targetCollection)
					delete(unmappedTarget, targetCollection.Uid)
				}
			}
		}
	}
	return
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

func (s ScopesMap) GetCollection(id uint64) (col Collection, found bool) {
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

type Scope struct {
	Uid         uint64 `json:"Uid"`
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

type Collection struct {
	Uid  uint64 `json:"Uid"`
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
	if c[i].ScopeName == c[j].ScopeName {
		return c[i].CollectionName < c[j].CollectionName
	} else {
		return c[i].ScopeName < c[j].ScopeName
	}
}

func SortCollectionsNamespaceList(list CollectionNamespaceList) CollectionNamespaceList {
	sort.Sort(list)
	return list
}

func (c CollectionNamespaceList) String() string {
	var buffer bytes.Buffer
	for _, j := range c {
		buffer.WriteString(fmt.Sprintf("|Scope: %v Collection: %v| ", j.ScopeName, j.CollectionName))
	}
	return buffer.String()
}

func (c CollectionNamespaceList) IsSame(other CollectionNamespaceList) bool {
	if len(c) != len(other) {
		return false
	}

	aList := SortCollectionsNamespaceList(c)
	bList := SortCollectionsNamespaceList(other)

	for i, col := range aList {
		if *col != *bList[i] {
			return false
		}
	}

	return true
}

func (c CollectionNamespaceList) Clone() (other CollectionNamespaceList) {
	for _, j := range c {
		ns := &base.CollectionNamespace{}
		*ns = *j
		other = append(other, ns)
	}
	return
}

func (c CollectionNamespaceList) Contains(namespace *base.CollectionNamespace) bool {
	if namespace == nil {
		return false
	}

	for _, j := range c {
		if *j == *namespace {
			return true
		}
	}
	return false
}

// This is used for namespace mapping that transcends over manifest lifecycles
// Need to use pointers because of golang hash map support of indexable type
// This means rest needs to do some gymanistics, instead of just simply checking for pointers
type CollectionNamespaceMapping map[*base.CollectionNamespace]CollectionNamespaceList

func (c CollectionNamespaceMapping) String() string {
	var buffer bytes.Buffer
	for src, tgtList := range c {
		buffer.WriteString(fmt.Sprintf("SOURCE ||Scope: %v Collection: %v|| -> %v\n", src.ScopeName, src.CollectionName, CollectionNamespaceList(tgtList).String()))
	}
	return buffer.String()
}

func (c CollectionNamespaceMapping) Clone() (clone CollectionNamespaceMapping) {
	clone = make(CollectionNamespaceMapping)
	for k, v := range c {
		srcClone := &base.CollectionNamespace{}
		*srcClone = *k
		clone[srcClone] = v.Clone()
	}
	return
}

// The input "src" does not have to match the actual key pointer in the map, just the right matching values
// Returns the srcPtr for referring to the exact tgtList
func (c CollectionNamespaceMapping) Get(src *base.CollectionNamespace) (srcPtr *base.CollectionNamespace, tgt CollectionNamespaceList, exists bool) {
	if src == nil {
		return
	}

	for k, v := range c {
		if *k == *src {
			// found
			tgt = v
			srcPtr = k
			exists = true
			return
		}
	}
	return
}

func (c CollectionNamespaceMapping) AddSingleMapping(src, tgt *base.CollectionNamespace) (alreadyExists bool) {
	if src == nil || tgt == nil {
		return
	}

	srcPtr, tgtList, found := c.Get(src)

	if !found {
		// Just use these as entries
		var newList CollectionNamespaceList
		newList = append(newList, tgt)
		c[src] = newList
	} else {
		// See if tgt already exists in the current list
		if tgtList.Contains(tgt) {
			alreadyExists = true
			return
		}

		c[srcPtr] = append(c[srcPtr], tgt)
	}
	return
}

// Given a scope and collection, see if it exists as one of the targets in the mapping
func (c CollectionNamespaceMapping) TargetNamespaceExists(checkNamespace *base.CollectionNamespace) bool {
	if checkNamespace == nil {
		return false
	}
	for _, tgtList := range c {
		if tgtList.Contains(checkNamespace) {
			return true
		}
	}
	return false
}

// Given a set of added scopesmap, return a subset of c that contains elements in this added
func (c CollectionNamespaceMapping) GetSubsetBasedOnAddedTargets(added ScopesMap) (retMap CollectionNamespaceMapping) {
	retMap = make(CollectionNamespaceMapping)

	for src, tgtList := range c {
		for _, tgt := range tgtList {
			_, found := added.GetCollectionByNames(tgt.ScopeName, tgt.CollectionName)
			if found {
				retMap.AddSingleMapping(src, tgt)
			}
		}
	}
	return
}

func (c CollectionNamespaceMapping) IsSame(other CollectionNamespaceMapping) bool {
	for src, tgtList := range c {
		_, otherTgtList, exists := other.Get(src)
		if !exists {
			return false
		}
		if !tgtList.IsSame(otherTgtList) {
			return false
		}
	}
	return true
}

func (c CollectionNamespaceMapping) Delete(subset CollectionNamespaceMapping) (result CollectionNamespaceMapping) {
	// Instead of deleting, just make a brand new map
	result = make(CollectionNamespaceMapping)

	// Subtract B from A
	for aSrc, aTgtList := range c {
		_, bTgtList, exists := subset.Get(aSrc)
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
