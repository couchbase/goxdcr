package metadata

import (
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"reflect"
	"testing"
)

var testDir string = "testData/"

var emptyManifest string = testDir + "emptyCollectionManifest.json"
var provisionedFile string = testDir + "provisionedManifest.json"
var provisionedFileCustom string = testDir + "provisionedManifestv2.json"

func TestUnmarshalCollectionManifest(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestUnmarshalCollectionManifest =================")
	data, err := ioutil.ReadFile(emptyManifest)
	assert.Nil(err)

	var emptyCollection CollectionsManifest
	err = emptyCollection.LoadBytes(data)
	assert.Nil(err)
	assert.Equal(uint64(0), emptyCollection.Uid())
	assert.Equal(1, len(emptyCollection.Scopes())) // default scope
	// should have default scope
	defaultScope, ok := emptyCollection.Scopes()[base.DefaultScopeCollectionName]
	assert.True(ok)
	assert.Equal(uint64(0), defaultScope.Uid)
	assert.Equal(base.DefaultScopeCollectionName, defaultScope.Name)
	// should have default collection
	assert.Equal(1, len(defaultScope.Collections))
	defaultCol, ok := defaultScope.Collections[base.DefaultScopeCollectionName]
	assert.True(ok)
	assert.Equal(uint64(0), defaultCol.Uid)
	assert.Equal(base.DefaultScopeCollectionName, defaultCol.Name)
	// Test marshal
	marshalledBytes, _ := emptyCollection.MarshalJSON()
	var testEquality CollectionsManifest
	err = testEquality.LoadBytes(marshalledBytes)
	assert.Nil(err)
	assert.True(emptyCollection.Equals(&testEquality))
	assert.Equal(0, emptyCollection.Count(false /*includeDefaultScope*/, false /*includeDefaultCollection*/))
	assert.Equal(0, emptyCollection.Count(true /*includeDefaultScope*/, false /*includeDefaultCollection*/))
	assert.Equal(1, emptyCollection.Count(true /*includeDefaultScope*/, true /*includeDefaultCollection*/))

	data, err = ioutil.ReadFile(provisionedFile)
	data2, err2 := ioutil.ReadFile(provisionedFileCustom)
	assert.Nil(err)
	assert.Nil(err2)
	var provisionedManifest CollectionsManifest
	err = provisionedManifest.LoadBytes(data)
	assert.Nil(err)
	var provisionedManifestCustom CollectionsManifest
	err = provisionedManifestCustom.LoadBytes(data2)
	assert.Nil(err)
	assert.Equal(uint64(7), provisionedManifest.Uid())
	assert.Equal(uint64(10), provisionedManifestCustom.Uid())
	// should have default scope
	defaultScope, ok = provisionedManifest.Scopes()[base.DefaultScopeCollectionName]
	assert.True(ok)
	assert.Equal(uint64(0), defaultScope.Uid)
	// S1 and S2 scope
	s1Scope, exists := provisionedManifest.Scopes()["S1"]
	assert.True(exists)
	_, exists = provisionedManifest.Scopes()["S2"]
	assert.True(exists)
	s1CustomScope, exists := provisionedManifestCustom.Scopes()["S2"]
	assert.True(exists)
	assert.Equal(uint64(11), s1CustomScope.Uid)
	s1Col1, exists := s1Scope.Collections["col1"]
	assert.True(exists)
	assert.Equal("col1", s1Col1.Name)
	s1Col2, exists := s1Scope.Collections["col2"]
	assert.True(exists)
	assert.Equal("col2", s1Col2.Name)
	// Test marshal
	marshalledBytes, _ = provisionedManifest.MarshalJSON()
	err = testEquality.LoadBytes(marshalledBytes)
	assert.Nil(err)
	assert.True(provisionedManifest.Equals(&testEquality))

	fmt.Println("============== Test case end: TestUnmarshalCollectionManifest =================")
}

func TestCollectionsManifestDiff(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestCollectionsManifestDiff =================")
	data, err := ioutil.ReadFile(emptyManifest)
	assert.Nil(err)
	emptyCollectionManifest, err := NewCollectionsManifestFromBytes(data)
	assert.Nil(err)

	data, err = ioutil.ReadFile(provisionedFile)
	assert.Nil(err)
	provisionedManifest, err := NewCollectionsManifestFromBytes(data)
	assert.Nil(err)

	assert.False(emptyCollectionManifest.Equals(&provisionedManifest))

	addedOrModified, removed, err := provisionedManifest.Diff(&emptyCollectionManifest)
	assert.Nil(err)
	assert.Equal(0, len(removed))
	assert.NotEqual(0, len(addedOrModified))
	// Since comparing against empty, all non-defaults are new
	provisionedCnt := provisionedManifest.Count(false /*includeDefaultScope*/, false /*includeDefaultCollection*/)
	addedCnt := addedOrModified.Count(true /*includeDefaultScope*/, true /*includeDefaultCollection*/)
	assert.Equal(provisionedCnt, addedCnt)

	// Change 2 collections uid and see those as modified
	changedManifest := provisionedManifest.Clone()
	changedManifest.uid++
	colVar := changedManifest.scopes["S1"].Collections["col1"]
	colVar.Uid++
	changedManifest.scopes["S1"].Collections["col1"] = colVar
	assert.NotEqual(changedManifest.Scopes()["S1"].Collections["col1"].Uid, provisionedManifest.Scopes()["S1"].Collections["col1"].Uid)
	colVar = changedManifest.scopes["S2"].Collections["col1"]
	colVar.Uid++
	changedManifest.scopes["S2"].Collections["col1"] = colVar
	assert.NotEqual(changedManifest.Scopes()["S2"].Collections["col1"].Uid, provisionedManifest.Scopes()["S2"].Collections["col1"].Uid)
	// Add a new collection
	changedManifest.scopes["S2"].Collections["col1Clone"] = changedManifest.scopes["S2"].Collections["col1"].Clone()
	addedOrModified, removed, err = changedManifest.Diff(&provisionedManifest)
	assert.Nil(err)
	assert.Equal(3, addedOrModified.Count(true, true))

	// Prevent diffing against newer
	_, _, err = emptyCollectionManifest.Diff(&provisionedManifest)
	assert.NotNil(err)

	// Hack it so we can diff against an empty one
	emptyCollectionManifest.uid = 10000
	addedOrModified, removed, err = emptyCollectionManifest.Diff(&provisionedManifest)
	assert.Nil(err)
	assert.NotEqual(0, len(removed))
	assert.Equal(0, len(addedOrModified))
	removedCnt := removed.Count(true, true)
	assert.Equal(provisionedCnt, removedCnt)

	fmt.Println("============== Test case end: TestCollectionsManifestDiff =================")
}

func TestManifestHashing(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestManifestHashing =================")

	data, err := ioutil.ReadFile(provisionedFile)
	assert.Nil(err)
	provisionedManifest, err := NewCollectionsManifestFromBytes(data)
	assert.Nil(err)
	provisionedManifest2, err := NewCollectionsManifestFromBytes(data)
	assert.Nil(err)
	provisionedManifest3, err := NewCollectionsManifestFromBytes(data)
	assert.Nil(err)
	provisionedManifest3b, err := NewCollectionsManifestFromBytes(data)
	assert.Nil(err)
	provisionedManifest3b.uid++

	shaHash1, err := provisionedManifest.Sha256()
	assert.Nil(err)
	shaHash2, err := provisionedManifest2.Sha256()
	assert.Nil(err)
	assert.True(reflect.DeepEqual(shaHash1, shaHash2))
	provisionedManifest2.uid++
	shaHash2b, err := provisionedManifest2.Sha256()
	assert.Nil(err)
	assert.False(reflect.DeepEqual(shaHash1, shaHash2b))

	var list1 ManifestsList
	var list2 ManifestsList
	list1 = append(list1, &provisionedManifest)
	list2 = append(list2, &provisionedManifest)
	list1 = append(list1, &provisionedManifest2)
	list2 = append(list2, &provisionedManifest2)

	list1Hash, err := list1.Sha256()
	assert.Nil(err)
	list2Hash, err := list2.Sha256()
	assert.Nil(err)
	assert.True(reflect.DeepEqual(list1Hash, list2Hash))

	list1 = append(list1, &provisionedManifest3)
	list2 = append(list2, &provisionedManifest3b)

	list1Hash, err = list1.Sha256()
	assert.Nil(err)
	list2Hash, err = list2.Sha256()
	assert.Nil(err)
	assert.False(reflect.DeepEqual(list1Hash, list2Hash))

	// Weird nil test
	var list3 ManifestsList
	var list4 ManifestsList
	list3Hash, err := list3.Sha256()
	assert.Nil(err)
	list4Hash, err := list4.Sha256()
	assert.Nil(err)
	assert.True(reflect.DeepEqual(list3Hash, list4Hash))

	var emptyCollection CollectionsManifest

	list3 = append(list3, nil)
	list4 = append(list4, &emptyCollection)
	list3Hash, err = list3.Sha256()
	assert.Nil(err)
	list4Hash, err = list4.Sha256()
	assert.Nil(err)
	assert.True(reflect.DeepEqual(list3Hash, list4Hash))

	fmt.Println("============== Test case end: TestManifestHashing =================")
}

func TestManifestsListSort(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestManifestsListSort =================")
	data, _ := ioutil.ReadFile(provisionedFile)
	var list ManifestsList
	provisionedManifest, _ := NewCollectionsManifestFromBytes(data)
	provisionedManifest2, _ := NewCollectionsManifestFromBytes(data)

	provisionedManifest2.uid++

	list = append(list, &provisionedManifest2)
	list = append(list, &provisionedManifest)

	assert.Equal(list[0].uid, provisionedManifest2.uid)
	assert.NotEqual(list[1].uid, provisionedManifest2.uid)
	assert.Equal(list[1].uid, provisionedManifest.uid)
	assert.True(list[0].uid > list[1].uid)

	list.Sort()

	assert.Equal(list[1].uid, provisionedManifest2.uid)
	assert.Equal(list[0].uid, provisionedManifest.uid)

	fmt.Println("============== Test case end: TestManifestsListSort =================")
}

func TestManifestMapping(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestManifestMapping =================")
	data, _ := ioutil.ReadFile(provisionedFile)
	source, _ := NewCollectionsManifestFromBytes(data)
	target, _ := NewCollectionsManifestFromBytes(data)

	// unmapped source
	delete(target.Scopes()["S1"].Collections, "col2")

	// unmapped target
	target.Scopes()["S2"].Collections["colTest"] = Collection{1234, "colTest"}

	_, unmappedSrc, unmappedTgt := source.MapAsSourceToTargetByName(&target)
	assert.Equal(1, len(unmappedSrc))
	assert.Equal(1, len(unmappedTgt))

	fmt.Println("============== Test case end: TestManifestMapping =================")
}
