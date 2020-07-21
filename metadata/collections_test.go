// Copyright (c) 2013-2020 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package metadata

import (
	"encoding/json"
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
var sourcev7 string = testDir + "diffSourcev7.json"
var targetv7 string = testDir + "diffTargetv7.json"
var targetv9 string = testDir + "diffTargetv9.json"

var colRecreatedDir string = "singleCollectionDiff/"
var colRecreatedOrig string = testDir + colRecreatedDir + "orig.json"
var colRecreatedNew string = testDir + colRecreatedDir + "newer.json"

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
	assert.Equal(uint32(0), defaultScope.Uid)
	assert.Equal(base.DefaultScopeCollectionName, defaultScope.Name)
	// should have default collection
	assert.Equal(1, len(defaultScope.Collections))
	defaultCol, ok := defaultScope.Collections[base.DefaultScopeCollectionName]
	assert.True(ok)
	assert.Equal(uint32(0), defaultCol.Uid)
	assert.Equal(base.DefaultScopeCollectionName, defaultCol.Name)
	// Test marshal
	marshalledBytes, _ := emptyCollection.MarshalJSON()
	var testEquality CollectionsManifest
	err = testEquality.LoadBytes(marshalledBytes)
	assert.Nil(err)
	assert.True(emptyCollection.IsSameAs(&testEquality))
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
	assert.Equal(uint32(0), defaultScope.Uid)
	// S1 and S2 scope
	s1Scope, exists := provisionedManifest.Scopes()["S1"]
	assert.True(exists)
	_, exists = provisionedManifest.Scopes()["S2"]
	assert.True(exists)
	s1CustomScope, exists := provisionedManifestCustom.Scopes()["S2"]
	assert.True(exists)
	assert.Equal(uint32(11), s1CustomScope.Uid)
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
	assert.True(provisionedManifest.IsSameAs(&testEquality))

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

	assert.False(emptyCollectionManifest.IsSameAs(&provisionedManifest))

	added, modified, removed, err := provisionedManifest.Diff(&emptyCollectionManifest)
	assert.Nil(err)
	assert.Equal(0, len(removed))
	assert.NotEqual(0, len(added))
	assert.Equal(0, len(modified))
	// Since comparing against empty, all non-defaults are new
	provisionedCnt := provisionedManifest.Count(false /*includeDefaultScope*/, false /*includeDefaultCollection*/)
	addedCnt := added.Count(true /*includeDefaultScope*/, true /*includeDefaultCollection*/)
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
	added, modified, removed, err = changedManifest.Diff(&provisionedManifest)
	assert.Nil(err)
	assert.Equal(2, modified.Count(true, true))
	assert.Equal(1, added.Count(true, true))

	// Prevent diffing against newer
	_, _, _, err = emptyCollectionManifest.Diff(&provisionedManifest)
	assert.NotNil(err)

	// Hack it so we can diff against an empty one
	emptyCollectionManifest.uid = 10000
	added, modified, removed, err = emptyCollectionManifest.Diff(&provisionedManifest)
	assert.Nil(err)
	assert.NotEqual(0, len(removed))
	assert.Equal(0, len(added))
	assert.Equal(0, len(modified))
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

func TestCollectionsNsMapping(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestCollectionsMapping =================")
	mapping := make(CollectionNamespaceMapping)
	implicitEntry := &base.CollectionNamespace{"scope", "collection"}
	implicitEntry2 := &base.CollectionNamespace{"scope", "collection"}
	implicitEntry3 := &base.CollectionNamespace{"scope", "collection1"}

	// This tests to make sure that even though using diff structs, the info inside the map
	// is de-referenced and deduped to work correctly
	mapping.AddSingleMapping(implicitEntry, implicitEntry)
	assert.Equal(1, len(mapping))
	mapping.AddSingleMapping(implicitEntry2, implicitEntry2)
	assert.Equal(1, len(mapping))
	for _, v := range mapping {
		assert.Equal(1, len(v))
	}

	oldMap := mapping.Clone()
	mapping.AddSingleMapping(implicitEntry2, implicitEntry)
	assert.Equal(1, len(mapping))
	for _, v := range mapping {
		assert.Equal(1, len(v))
	}
	assert.True(oldMap.IsSame(mapping))

	oldMap = mapping.Clone()
	mapping.AddSingleMapping(implicitEntry2, implicitEntry3)
	assert.Equal(1, len(mapping))
	for _, v := range mapping {
		assert.Equal(2, len(v))
	}
	assert.False(oldMap.IsSame(mapping))

	//	fmt.Printf("Before Delete MappingOutput: %v\n", mapping.String())
	// Test delete
	mapping = mapping.Delete(oldMap)
	assert.Equal(1, len(mapping))
	for _, v := range mapping {
		assert.Equal(1, len(v))
	}

	//	fmt.Printf("After Delete MappingOutput: %v\n", mapping.String())
	confirmMapping := make(CollectionNamespaceMapping)
	confirmMapping.AddSingleMapping(implicitEntry2, implicitEntry3)
	assert.True(confirmMapping.IsSame(mapping))

	fmt.Println("============== Test case end: TestCollectionsMapping =================")
}

func TestCollectionRecreatedDiff(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestCollectionRecreatedDiff =================")
	data, err := ioutil.ReadFile(colRecreatedOrig)
	assert.Nil(err)

	var origManifest CollectionsManifest
	err = origManifest.LoadBytes(data)
	assert.Nil(err)

	//	 Manifest version 7
	//	 ScopeName "S2" (UID 9)
	//	 ScopeName "S2" CollectionName "col3" (UID 12)
	//	 ScopeName "S2" CollectionName "col2" (UID 11)
	//	 ScopeName "S2" CollectionName "col1" (UID 10)
	//	 ScopeName "S1" (UID 8)
	//	 ScopeName "S1" CollectionName "col2" (UID 9)
	//	 ScopeName "S1" CollectionName "col1" (UID 8) <--- Delete this one
	//	 ScopeName "_default" (UID 0)
	//	 ScopeName "_default" CollectionName "_default" (UID 0)

	data, err = ioutil.ReadFile(colRecreatedNew)
	assert.Nil(err)

	var newerManifest CollectionsManifest
	err = newerManifest.LoadBytes(data)
	assert.Nil(err)

	// Manifest version 9
	// ScopeName "S2" (UID 9)
	// ScopeName "S2" CollectionName "col3" (UID 12)
	// ScopeName "S2" CollectionName "col2" (UID 11)
	// ScopeName "S2" CollectionName "col1" (UID 10)
	// ScopeName "S1" (UID 8)
	// ScopeName "S1" CollectionName "col1" (UID 13) <----- Recreated one
	// ScopeName "S1" CollectionName "col2" (UID 9)
	// ScopeName "_default" (UID 0)
	// ScopeName "_default" CollectionName "_default" (UID 0)

	added, modified, removed, err := newerManifest.Diff(&origManifest)
	assert.Nil(err)
	assert.Equal(0, len(added))
	assert.Equal(0, len(removed))
	assert.Equal(1, len(modified))
	col, found := modified.GetCollection(13)
	assert.True(found)
	assert.Equal("col1", col.Name)

	fmt.Println("============== Test case end: TestCollectionRecreatedDiff =================")
}

func TestMarshalUnmarshalCollectionsNamespaceMapping(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestMarshalUnmarshalCollectionsNamespaceMapping =================")
	nsMap := make(CollectionNamespaceMapping)
	defaultNamespace := base.CollectionNamespace{"_default", "_default"}
	var nslist CollectionNamespaceList
	nslist = append(nslist, &defaultNamespace)
	nsMap[&defaultNamespace] = nslist

	marshalledData, err := nsMap.MarshalJSON()
	assert.Nil(err)

	checkMap := make(CollectionNamespaceMapping)
	err = checkMap.UnmarshalJSON(marshalledData)
	assert.Nil(err)

	assert.True(checkMap.IsSame(nsMap))
	fmt.Println("============== Test case end: TestMarshalUnmarshalCollectionsNamespaceMapping =================")
}

func TestCollectionsNsConsolidate(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestCollectionsNsConsolidate =================")

	nsMap := make(CollectionNamespaceMapping)
	defaultNamespace := base.CollectionNamespace{"_default", "_default"}
	c1Ns := base.CollectionNamespace{"C1", "C1"}
	c2Ns := base.CollectionNamespace{"C2", "C2"}
	c3Ns := base.CollectionNamespace{"C3", "C3"}
	var nslist CollectionNamespaceList
	nslist = append(nslist, &c1Ns)
	nslist = append(nslist, &c3Ns)
	nsMap[&defaultNamespace] = nslist
	assert.Equal(1, len(nsMap))
	assert.Equal(2, len(nsMap[&defaultNamespace]))

	nsMap2 := make(CollectionNamespaceMapping)
	var nslist2 CollectionNamespaceList
	nslist2 = append(nslist2, &c2Ns)
	nslist2 = append(nslist2, &c3Ns)
	nsMap2[&defaultNamespace] = nslist2
	var nslist3 CollectionNamespaceList
	nslist3 = append(nslist3, &defaultNamespace)
	nsMap2[&c1Ns] = nslist3
	assert.Equal(2, len(nsMap2))
	assert.Equal(2, len(nsMap2[&defaultNamespace]))
	assert.Equal(1, len(nsMap2[&c1Ns]))

	//nsmap1
	//SOURCE ||Scope: _default Collection: _default|| -> |Scope: C1 Collection: C1| |Scope: C3 Collection: C3| |Scope: C2 Collection: C2|
	//SOURCE ||Scope: C1 Collection: C1|| -> |Scope: _default Collection: _default|
	//
	//nsmap2
	//SOURCE ||Scope: _default Collection: _default|| -> |Scope: C2 Collection: C2| |Scope: C3 Collection: C3|
	//SOURCE ||Scope: C1 Collection: C1|| -> |Scope: _default Collection: _default|
	//
	//Added:
	//
	//Removed:
	//SOURCE ||Scope: _default Collection: _default|| -> |Scope: C1 Collection: C1|

	nsMap.Consolidate(nsMap2)
	assert.Equal(2, len(nsMap))
	assert.Equal(3, len(nsMap[&defaultNamespace]))
	assert.Equal(1, len(nsMap[&c1Ns]))

	added, removed := nsMap.Diff(nsMap2)
	assert.Equal(0, len(added))
	assert.Equal(1, len(removed))
	assert.Equal(1, len(removed[&defaultNamespace]))
	fmt.Println("============== Test case end: TestCollectionsNsConsolidate =================")
}

func TestCollectionNsMappingsDocMarshaller(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestCollectionNsMappingsDocMarshaller =================")
	// Test out of order list will have the same sha
	nsMap := make(CollectionNamespaceMapping)
	nsMapPrime := make(CollectionNamespaceMapping)

	defaultNamespace := base.CollectionNamespace{"_default", "_default"}
	c1Ns := base.CollectionNamespace{"C1", "C1"}
	c3Ns := base.CollectionNamespace{"C3", "C3"}

	var nslist CollectionNamespaceList
	nslist = append(nslist, &c1Ns)
	nslist = append(nslist, &c3Ns)

	var nslistPrime CollectionNamespaceList
	nslistPrime = append(nslistPrime, &c3Ns)
	nslistPrime = append(nslistPrime, &c1Ns)

	nsMap[&defaultNamespace] = nslist
	nsMapPrime[&defaultNamespace] = nslistPrime

	// Two map's snappy compressed content are different but sha should be the same
	snappy1, err := nsMap.ToSnappyCompressed()
	assert.Nil(err)
	snappy2, err := nsMapPrime.ToSnappyCompressed()
	assert.Nil(err)
	assert.False(reflect.DeepEqual(snappy1, snappy2))
	sha1, err := nsMap.Sha256()
	assert.Nil(err)
	sha2, err := nsMapPrime.Sha256()
	assert.Nil(err)
	assert.True(reflect.DeepEqual(sha1[:], sha2[:]))

	// Load from compressed data
	validate1, err := NewCollectionNamespaceMappingFromSnappyData(snappy1)
	assert.Nil(err)
	validate2, err := NewCollectionNamespaceMappingFromSnappyData(snappy2)
	assert.Nil(err)
	assert.True(validate1.IsSame(*validate2))

	oneMapping := &CompressedColNamespaceMapping{nil, "dummySha"}
	var oneList CompressedColNamespaceMappingList
	oneList = append(oneList, oneMapping)

	brokenMappingDoc := &CollectionNsMappingsDoc{oneList, "dummySpec", nil}

	_, err = json.Marshal(brokenMappingDoc)
	assert.Nil(err)
	fmt.Println("============== Test case end: TestCollectionNsMappingsDocMarshaller =================")
}

func TestManifestsDocMarshalling(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestManifestsDocMarshalling =================")

	data, err := ioutil.ReadFile(emptyManifest)
	assert.Nil(err)

	var emptyCollection CollectionsManifest
	err = emptyCollection.LoadBytes(data)
	assert.Nil(err)
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

	doc := &ManifestsDoc{}
	doc.collectionsManifests = append(doc.collectionsManifests, &emptyCollection)
	doc.collectionsManifests = append(doc.collectionsManifests, &provisionedManifest)
	doc.collectionsManifests = append(doc.collectionsManifests, &provisionedManifestCustom)

	assert.Nil(doc.PreMarshal())
	assert.NotEqual(0, len(doc.CompressedCollectionsManifests))

	serializedData, err := json.Marshal(doc)
	assert.Nil(err)

	doc.ClearCompressedData()

	newDoc := &ManifestsDoc{}
	assert.Nil(json.Unmarshal(serializedData, newDoc))
	assert.Nil(newDoc.PostUnmarshal())
	newDoc.ClearCompressedData()

	assert.Equal(3, len(newDoc.collectionsManifests))
	for i := 0; i < 3; i++ {
		assert.True(doc.collectionsManifests[i].IsSameAs(newDoc.collectionsManifests[i]))
	}
	fmt.Println("============== Test case end: TestManifestsDocMarshalling =================")
}

func TestManifestMapping(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestManifestMapping =================")
	data, _ := ioutil.ReadFile(provisionedFile)
	source, _ := NewCollectionsManifestFromBytes(data)
	target, _ := NewCollectionsManifestFromBytes(data)

	// unmapped source
	delete(target.scopes["S1"].Collections, "col2")

	// unmapped target
	target.scopes["S2"].Collections["colTest"] = Collection{1234, "colTest"}

	_, unmappedSrc, unmappedTgt := source.ImplicitMap(&target)
	assert.Equal(1, len(unmappedSrc))
	assert.Equal(1, len(unmappedTgt))

	added, removed, modified := unmappedSrc.Diff(unmappedTgt)
	assert.Equal(1, len(added))
	assert.Equal(1, len(removed))
	assert.Equal(0, len(modified))

	fmt.Println("============== Test case end: TestManifestMapping =================")
}

func TestManifestMappingReal(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestManifestMappingReal =================")
	data, _ := ioutil.ReadFile(sourcev7)
	source, _ := NewCollectionsManifestFromBytes(data)
	data, _ = ioutil.ReadFile(targetv7)
	target, _ := NewCollectionsManifestFromBytes(data)

	mappedSrcToTarget, unmappedSrc, unmappedTgt := source.ImplicitMap(&target)
	assert.Equal(0, len(unmappedSrc))
	assert.Equal(0, len(unmappedTgt))
	assert.Equal(6, len(mappedSrcToTarget))

	fmt.Println("============== Test case end: TestManifestMappingReal =================")
}

func TestManifestFindBackfill(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestManifestFindBackfill =================")
	data, err := ioutil.ReadFile(provisionedFile)
	if err != nil {
		panic(err)
	}
	source, _ := NewCollectionsManifestFromBytes(data)
	target, _ := NewCollectionsManifestFromBytes(data)
	target2, _ := NewCollectionsManifestFromBytes(data)

	newCol := target.scopes["S1"].Collections["col1"].Clone()
	newCol.Uid++
	target2.uid++
	target2.scopes["S1"].Collections["col1"] = newCol

	output, err := target2.ImplicitGetBackfillCollections(&target, &source)
	assert.Nil(err)
	assert.Equal(1, len(output))

	// target 3 is target 2 but with one more collection
	target3 := target2.Clone()
	testCol := Collection{Uid: 1234, Name: "Extraneous"}
	target3.scopes["S1"].Collections["testNewCol"] = testCol

	// However, there should not be a backfill YET because source doesn't have a matching "Extraneous" collection
	output, err = target3.ImplicitGetBackfillCollections(&target, &source)
	assert.Nil(err)
	assert.Equal(1, len(output))

	// Source creates the same collection but diff UID, now it should be backfilled
	source2 := source.Clone()
	testSrcCol := Collection{Uid: 2345, Name: "Extraneous"}
	source2.scopes["S1"].Collections["testNewCol"] = testSrcCol

	output, err = target3.ImplicitGetBackfillCollections(&target, &source2)
	assert.Nil(err)
	assert.Equal(2, len(output))

	fmt.Println("============== Test case end: TestManifestFindBackfill =================")
}

func TestNoIntersectionDiff(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestNoIntersectionDiff =================")
	defer fmt.Println("============== Test case start: TestNoIntersectionDiff =================")

	oldMapping := make(CollectionNamespaceMapping)
	src := base.CollectionNamespace{
		ScopeName:      "S1",
		CollectionName: "col1",
	}
	tgt0 := base.CollectionNamespace{
		ScopeName:      "S3",
		CollectionName: "col3",
	}
	oldMapping.AddSingleMapping(&src, &tgt0)

	newMapping := make(CollectionNamespaceMapping)
	tgt1 := base.CollectionNamespace{
		ScopeName:      "S3",
		CollectionName: "col2",
	}
	newMapping.AddSingleMapping(&src, &tgt1)

	added, removed := oldMapping.Diff(newMapping)
	assert.Equal(1, len(added))
	assert.Equal(1, len(removed))
}
