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
	"io/ioutil"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/couchbase/goxdcr/base"
	"github.com/stretchr/testify/assert"
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
	nsMap[NewSourceCollectionNamespace(&defaultNamespace)] = nslist

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
	c1NsSrc := NewSourceCollectionNamespace(&c1Ns)
	c2Ns := base.CollectionNamespace{"C2", "C2"}
	c3Ns := base.CollectionNamespace{"C3", "C3"}
	var nslist CollectionNamespaceList
	nslist = append(nslist, &c1Ns)
	nslist = append(nslist, &c3Ns)
	newDefaultSourceNs := NewSourceCollectionNamespace(&defaultNamespace)
	nsMap[newDefaultSourceNs] = nslist
	assert.Equal(1, len(nsMap))
	assert.Equal(2, len(nsMap[newDefaultSourceNs]))

	nsMap2 := make(CollectionNamespaceMapping)
	var nslist2 CollectionNamespaceList
	nslist2 = append(nslist2, &c2Ns)
	nslist2 = append(nslist2, &c3Ns)
	defaultSourceNs := NewSourceCollectionNamespace(&defaultNamespace)
	nsMap2[defaultSourceNs] = nslist2
	var nslist3 CollectionNamespaceList
	nslist3 = append(nslist3, &defaultNamespace)
	nsMap2[c1NsSrc] = nslist3
	assert.Equal(2, len(nsMap2))
	assert.Equal(2, len(nsMap2[defaultSourceNs]))
	assert.Equal(1, len(nsMap2[c1NsSrc]))

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
	_, _, tgtList, exists := nsMap.Get(&defaultNamespace, nil)
	assert.True(exists)
	assert.Equal(3, len(tgtList))
	_, _, tgtList, exists = nsMap.Get(&c1Ns, nil)
	assert.True(exists)
	assert.Equal(1, len(nsMap[c1NsSrc]))

	added, removed := nsMap.Diff(nsMap2)
	assert.Equal(0, len(added))
	assert.Equal(1, len(removed))
	_, _, tgtList, exists = removed.Get(&defaultNamespace, nil)
	assert.True(exists)
	assert.Equal(1, len(tgtList))
	fmt.Println("============== Test case end: TestCollectionsNsConsolidate =================")
}

func TestCollectionNsMappingsDocMarshaller(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestCollectionNsMappingsDocMarshaller =================")
	// Test out of order list will have the same sha
	nsMap := make(CollectionNamespaceMapping)
	nsMapPrime := make(CollectionNamespaceMapping)

	defaultNamespace := base.CollectionNamespace{"_default", "_default"}
	defaultSrcNamespace := NewSourceCollectionNamespace(&defaultNamespace)
	c1Ns := base.CollectionNamespace{"C1", "C1"}
	c3Ns := base.CollectionNamespace{"C3", "C3"}

	var nslist CollectionNamespaceList
	nslist = append(nslist, &c1Ns)
	nslist = append(nslist, &c3Ns)

	var nslistPrime CollectionNamespaceList
	nslistPrime = append(nslistPrime, &c3Ns)
	nslistPrime = append(nslistPrime, &c1Ns)

	nsMap[defaultSrcNamespace] = nslist
	nsMapPrime[defaultSrcNamespace] = nslistPrime

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
	target2.generateReverseLookupMap()

	output, err := target2.ImplicitGetBackfillCollections(&target, &source)
	assert.Nil(err)
	assert.Equal(1, len(output))

	var oneMoreCollectionName = "Extraneous"

	// target 3 is target 2 but with one more collection
	target3 := target2.Clone()
	testCol := Collection{Uid: 1234, Name: oneMoreCollectionName}
	target3.scopes["S1"].Collections[oneMoreCollectionName] = testCol
	target3.generateReverseLookupMap()

	// However, there should not be a backfill YET because source doesn't have a matching "Extraneous" collection
	output, err = target3.ImplicitGetBackfillCollections(&target, &source)
	assert.Nil(err)
	assert.Equal(1, len(output))

	// Source creates the same collection but diff UID, now it should be backfilled
	source2 := source.Clone()
	testSrcCol := Collection{Uid: 2345, Name: oneMoreCollectionName}
	source2.scopes["S1"].Collections[oneMoreCollectionName] = testSrcCol
	source2.generateReverseLookupMap()

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

//      "name": "S2",
//          "name": "col3",
//          "name": "col2",
//          "name": "col1",
//      "name": "S1",
//          "name": "col2",
//          "name": "col1",
//      "name": "_default",
//          "name": "_default",
func TestExplicitMapping(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestExplicitMapping =================")
	defer fmt.Println("============== Test case start: TestExplicitMapping =================")

	data, err := ioutil.ReadFile(provisionedFile)
	assert.Nil(err)
	source, _ := NewCollectionsManifestFromBytes(data)
	target, _ := NewCollectionsManifestFromBytes(data)
	manifestPair := CollectionsManifestPair{
		Source: &source,
		Target: &target,
	}
	var mappingMode base.CollectionsMgtType
	mappingMode.SetExplicitMapping(true)

	rules := make(CollectionsMappingRulesType)
	rules["S2"] = "S1"
	explicitMap, err := NewCollectionNamespaceMappingFromRules(manifestPair, mappingMode, rules, false, false)
	assert.Nil(err)
	assert.NotNil(explicitMap)
	explicitMapIdx := explicitMap.CreateLookupIndex()

	assert.Equal(2, len(explicitMap))
	sourceNamespace := base.CollectionNamespace{
		ScopeName:      "S2",
		CollectionName: "col1",
	}
	targetNamespace := base.CollectionNamespace{
		ScopeName:      "S1",
		CollectionName: "col1",
	}
	_, _, tgtCheckList, ok := explicitMap.Get(&sourceNamespace, nil)
	assert.True(ok)
	assert.Equal(1, len(tgtCheckList))
	assert.True(tgtCheckList[0].IsSameAs(targetNamespace))

	// Using index
	_, _, tgtCheckList, ok = explicitMap.Get(&sourceNamespace, explicitMapIdx)
	assert.True(ok)
	assert.Equal(1, len(tgtCheckList))
	assert.True(tgtCheckList[0].IsSameAs(targetNamespace))

	// Check a diff collection
	sourceNamespace.CollectionName = "col2"
	targetNamespace.CollectionName = "col2"
	_, _, tgtCheckList, ok = explicitMap.Get(&sourceNamespace, nil)
	assert.True(ok)
	assert.True(tgtCheckList[0].IsSameAs(targetNamespace))

	// using index
	_, _, tgtCheckList, ok = explicitMap.Get(&sourceNamespace, explicitMapIdx)
	assert.True(ok)
	assert.True(tgtCheckList[0].IsSameAs(targetNamespace))

	// Last one is unmapped
	sourceNamespace.CollectionName = "col3"
	_, _, tgtCheckList, ok = explicitMap.Get(&sourceNamespace, nil)
	assert.False(ok)
	// using index
	_, _, tgtCheckList, ok = explicitMap.Get(&sourceNamespace, explicitMapIdx)
	assert.False(ok)

	// Test blacklist
	rules = make(CollectionsMappingRulesType)
	rules["S1"] = "S2"
	rules["S1.col2"] = nil
	explicitMap, err = NewCollectionNamespaceMappingFromRules(manifestPair, mappingMode, rules, false, false)
	assert.Nil(err)
	assert.NotNil(explicitMap)
	explicitMapIdx = explicitMap.CreateLookupIndex()

	assert.Equal(2, len(explicitMap))
	sourceNamespace.ScopeName = "S1"
	sourceNamespace.CollectionName = "col1"
	targetNamespace.ScopeName = "S2"
	targetNamespace.CollectionName = "col1"
	_, _, tgtCheckList, ok = explicitMap.Get(&sourceNamespace, nil)
	assert.True(ok)
	assert.True(tgtCheckList[0].IsSameAs(targetNamespace))
	// with index
	_, _, tgtCheckList, ok = explicitMap.Get(&sourceNamespace, explicitMapIdx)
	assert.True(ok)
	assert.True(tgtCheckList[0].IsSameAs(targetNamespace))

	// Src: s1.col1
	// Tgt: s2.col1
	//      s2.col2
	// rule: s1.col1 -> s2.col3
	// In this case, s1.col1 should *not* have a corresponding target
	customSrcColMap := make(CollectionsMap)
	customSrcColMap["col1"] = Collection{
		Uid:  1,
		Name: "col1",
	}
	customSrcScopes := make(ScopesMap)
	customSrcScopes["s1"] = Scope{
		Uid:         1,
		Name:        "s1",
		Collections: customSrcColMap,
	}
	customSrcManifest := CollectionsManifest{
		uid:    1,
		scopes: customSrcScopes,
	}

	customTgtColMap := make(CollectionsMap)
	customTgtColMap["col1"] = Collection{
		Uid:  2,
		Name: "col1",
	}
	customTgtColMap["col2"] = Collection{
		Uid:  3,
		Name: "col2",
	}
	customTgtScopes := make(ScopesMap)
	customTgtScopes["s2"] = Scope{
		Uid:         3,
		Name:        "s2",
		Collections: customTgtColMap,
	}
	customTgtManifest := CollectionsManifest{
		uid:    2,
		scopes: customTgtScopes,
	}

	rules = make(CollectionsMappingRulesType)
	rules["s1"] = "s2"
	rules["s1.col1"] = "s2.col3"
	customManifestPair := CollectionsManifestPair{
		Source: &customSrcManifest,
		Target: &customTgtManifest,
	}
	explicitMap, err = NewCollectionNamespaceMappingFromRules(customManifestPair, mappingMode, rules, false, false)
	assert.Nil(err)
	assert.NotNil(explicitMap)
	explicitMapIdx = explicitMap.CreateLookupIndex()

	checkNamespace := &base.CollectionNamespace{
		ScopeName:      "s1",
		CollectionName: "col1",
	}
	_, _, _, exists := explicitMap.Get(checkNamespace, nil)
	assert.False(exists)
	_, _, _, exists = explicitMap.Get(checkNamespace, explicitMapIdx)
	assert.False(exists)
}

func TestListIsSubset(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestListIsSubset =================")
	defer fmt.Println("============== Test case end: TestListIsSubset =================")

	namespace1 := &base.CollectionNamespace{
		ScopeName:      "S1",
		CollectionName: "Col1",
	}

	namespace2 := &base.CollectionNamespace{
		ScopeName:      "S2",
		CollectionName: "Col2",
	}

	var list1 CollectionNamespaceList
	var list2 CollectionNamespaceList
	list1 = append(list1, namespace1)
	list1 = append(list1, namespace2)
	list2 = append(list2, namespace1)

	assert.True(list2.IsSubset(list1))
	assert.False(list1.IsSubset(list2))
	assert.False(list1.IsSame(list2))
}

//      "name": "S2",
//          "name": "col3",
//          "name": "col2",
//          "name": "col1",
//      "name": "S1",
//          "name": "col2",
//          "name": "col1",
//      "name": "_default",
//          "name": "_default",
func TestMigrationMapping(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestMigrationMapping =================")
	defer fmt.Println("============== Test case end: TestMigrationMapping =================")

	data, err := ioutil.ReadFile(provisionedFile)
	assert.Nil(err)
	target, _ := NewCollectionsManifestFromBytes(data)
	source := NewDefaultCollectionsManifest()
	manifestPair := CollectionsManifestPair{
		Source: &source,
		Target: &target,
	}

	var mappingMode base.CollectionsMgtType
	mappingMode.SetMigration(true)

	rules := make(CollectionsMappingRulesType)
	// Make a rule that says if the doc key starts with "S1_"
	rules["REGEXP_CONTAINS(META().id, \"^S1_\")"] = "S1.col1"
	rules["REGEXP_CONTAINS(META().id, \"^S2_\")"] = "S2.col1"

	explicitMap, err := NewCollectionNamespaceMappingFromRules(manifestPair, mappingMode, rules, false, false)
	assert.Nil(err)
	assert.NotNil(explicitMap)
	assert.Equal(2, len(explicitMap))

	for src, _ := range explicitMap {
		assert.Equal(SourceDefaultCollectionFilter, src.GetType())
	}
}
func TestMigrationDiff(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestMigrationDiff =================")
	defer fmt.Println("============== Test case end: TestMigrationDiff =================")

	defaultManifest := NewDefaultCollectionsManifest()
	rules := make(CollectionsMappingRulesType)
	// Make a rule that says if the doc key starts with "S1_"
	rules["REGEXP_CONTAINS(META().id, \"^S1_\")"] = "S1.col1"
	rules["REGEXP_CONTAINS(META().id, \"^S2_\")"] = "S2.col1"

	manifestPair := CollectionsManifestPair{
		Source: &defaultManifest,
		Target: &defaultManifest,
	}

	var mappingMode base.CollectionsMgtType
	mappingMode.SetMigration(true)

	explicitMap, err := NewCollectionNamespaceMappingFromRules(manifestPair, mappingMode, rules, false, true)
	assert.Nil(err)
	assert.NotNil(explicitMap)
	assert.Equal(0, len(explicitMap))

	// Now target has all the collections made
	data, err := ioutil.ReadFile(provisionedFile)
	assert.Nil(err)
	target, _ := NewCollectionsManifestFromBytes(data)

	newPair := CollectionsManifestPair{
		Source: &defaultManifest,
		Target: &target,
	}
	newExplicitMap, err := NewCollectionNamespaceMappingFromRules(newPair, mappingMode, rules, false, true)
	assert.Nil(err)
	assert.NotNil(explicitMap)
	assert.Equal(2, len(newExplicitMap))
	assert.False(newExplicitMap.IsSame(explicitMap))

	added, removed := explicitMap.Diff(newExplicitMap)
	assert.Equal(0, len(removed))
	assert.Equal(2, len(added))
}

func TestCollectionNsMappingsDocMarshallerWithMigration(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestCollectionNsMappingsDocMarshallerWithMigration =================")
	defer fmt.Println("============== Test case end: TestCollectionNsMappingsDocMarshallerWithMigration =================")
	// Test out of order list will have the same sha
	nsMap := make(CollectionNamespaceMapping)
	nsMapPrime := make(CollectionNamespaceMapping)

	expr := "REGEXP_CONTAINS(META().id, \"^S1_\")"
	defaultSrcNamespace, err := NewSourceMigrationNamespace(expr, nil)
	assert.Nil(err)

	c1Ns := base.CollectionNamespace{"C1", "C1"}
	c3Ns := base.CollectionNamespace{"C3", "C3"}

	var nslist CollectionNamespaceList
	nslist = append(nslist, &c1Ns)
	nslist = append(nslist, &c3Ns)

	var nslistPrime CollectionNamespaceList
	nslistPrime = append(nslistPrime, &c3Ns)
	nslistPrime = append(nslistPrime, &c1Ns)

	nsMap[defaultSrcNamespace] = nslist
	nsMapPrime[defaultSrcNamespace] = nslistPrime

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

	// This is just running through "getUpsertMap() from checkpoints_service
	newUpsertingMap := make(CollectionNamespaceMapping)
	newUpsertingMap[defaultSrcNamespace] = nslist
	newUpsertingMap[defaultSrcNamespace] = nslistPrime
	checkLen := len(newUpsertingMap)
	shaForNewUpsertMap, err := newUpsertingMap.Sha256()
	assert.Nil(err)

	compressedMap, err := newUpsertingMap.ToSnappyCompressed()
	assert.Nil(err)

	mappingRecord := CompressedColNamespaceMapping{
		CompressedMapping: compressedMap,
		Sha256Digest:      fmt.Sprintf("%x", shaForNewUpsertMap[:]),
	}

	var mappingRecords CompressedColNamespaceMappingList
	mappingRecords.SortedInsert(&mappingRecord)
	mappingDoc := &CollectionNsMappingsDoc{
		NsMappingRecords: mappingRecords,
		SpecInternalId:   "random",
	}
	mappingsDocMarshalled, err := json.Marshal(mappingDoc)
	assert.Nil(err)

	var checkDoc CollectionNsMappingsDoc
	err = json.Unmarshal(mappingsDocMarshalled, &checkDoc)
	assert.Nil(err)

	shaMap, err := checkDoc.ToShaMap()
	assert.Nil(err)
	assert.Equal(checkLen, len(shaMap))

	for _, nsMapping := range shaMap {
		for source, _ := range *nsMapping {
			assert.Equal(SourceDefaultCollectionFilter, source.GetType())
			assert.Equal(base.DefaultScopeCollectionName, source.ScopeName)
			assert.NotEqual(base.DefaultScopeCollectionName, source.CollectionName)
			assert.NotNil(source.filter)
			assert.NotEqual(0, len(source.filterString))
		}
	}
}

func TestCollectionMigrateRuleValidation(t *testing.T) {
	fmt.Println("============== Test case start: TestCollectionMigrateRuleValidation =================")
	defer fmt.Println("============== Test case end: TestCollectionMigrateRuleValidation =================")
	assert := assert.New(t)

	validRules := make(map[string]interface{})
	validRules[fmt.Sprintf("REGEXP_CONTAINS(META().id, %v%v%v)", "\"", "^_abc", "\"")] = "targetScope1.targetCol1"
	validRules[fmt.Sprintf("doc.Value == %v%v%v AND doc.Value2 != %v%v%v", "\"", "abc", "\"", "\"", "def", "\"")] = "targetScope2.targetCol2"

	rules, err := ValidateAndConvertJsonMapToRuleType(validRules)
	assert.Nil(err)
	err = rules.ValidateMigrateRules()
	assert.Nil(err)
	assert.False(rules.IsExplicitMigrationRule())

	invalidRules := make(map[string]interface{})
	// Incorrect target namespace
	invalidRules[fmt.Sprintf("REGEXP_CONTAINS(META().id, %v%v%v)", "\"", "^_abc", "\"")] = "targetScope1*"
	// Incorrect filter
	invalidRules[fmt.Sprintf("WRONGREGEXP_CONTAINS(META().id, %v%v%v)", "\"", "^_abc", "\"")] = "targetScope1*"
	rules, err = ValidateAndConvertJsonMapToRuleType(invalidRules)
	assert.Nil(err)
	err = rules.ValidateMigrateRules()
	assert.NotNil(err)

	doubleKey := "{\"key\":\"val\",\"key\":\"val2\"}"
	rules, err = ValidateAndConvertStringToMappingRuleType(doubleKey)
	assert.NotNil(err)

	perfKeyWSpaces := "{\"scope-1.collection-1\" : \"scope-2.collection-2\"}"
	rules, err = ValidateAndConvertStringToMappingRuleType(perfKeyWSpaces)
	assert.Nil(err)
}

func TestDenylistMapping(t *testing.T) {
	fmt.Println("============== Test case start: TestDenylistMapping =================")
	defer fmt.Println("============== Test case end: TestDenylistMapping =================")
	assert := assert.New(t)

	validRules := make(map[string]interface{})
	validRules["S1"] = "S1"
	validRules["S1.col1"] = nil

	data, err := ioutil.ReadFile(provisionedFile)
	assert.Nil(err)
	provisionedManifest, err := NewCollectionsManifestFromBytes(data)
	assert.Nil(err)
	manifestsPair := CollectionsManifestPair{
		Source: &provisionedManifest,
		Target: &provisionedManifest,
	}

	rules, err := ValidateAndConvertJsonMapToRuleType(validRules)
	assert.Nil(err)

	var mode base.CollectionsMgtType
	mode.SetExplicitMapping(true)

	mapping, err := rules.GetOutputMapping(manifestsPair, mode, true)
	assert.Nil(err)

	assert.Equal(2, len(mapping))
	for k, v := range mapping {
		assert.Equal(1, len(v))
		if k.CollectionName == "col1" {
			assert.True(v[0].IsEmpty())
		}
	}

}

func TestPerfWeirdMappingChange(t *testing.T) {
	fmt.Println("============== Test case start: TestPerfWeirdMappingChange =================")
	defer fmt.Println("============== Test case end: TestPerfWeirdMappingChange =================")
	assert := assert.New(t)

	oldRules := make(map[string]interface{})
	oldRules["S2.col1"] = "S2.col1"
	oldRules["S2.col2"] = "S2.col2"

	newRules := make(map[string]interface{})
	newRules["S2.col3"] = "S2.col3"

	data, err := ioutil.ReadFile(provisionedFile)
	assert.Nil(err)
	var provisionedManifest CollectionsManifest
	err = provisionedManifest.LoadBytes(data)
	assert.Nil(err)
	manifestPair := CollectionsManifestPair{
		Source: &provisionedManifest,
		Target: &provisionedManifest,
	}

	mode := base.CollectionsMgtType(0)
	mode.SetExplicitMapping(true)

	oldMapping, err := NewCollectionNamespaceMappingFromRules(manifestPair, mode, oldRules, false, false)
	assert.Nil(err)
	newMapping, err := NewCollectionNamespaceMappingFromRules(manifestPair, mode, newRules, false, false)
	assert.Nil(err)

	added, removed := oldMapping.Diff(newMapping)
	assert.Equal(1, len(added))
	assert.Equal(2, len(removed))

	var testBackfillSpec BackfillReplicationSpec
	testBackfillSpec.VBTasksMap = NewVBTasksMap()

	testBackfillSpec.VBTasksMap.RemoveNamespaceMappings(removed)
	assert.False(testBackfillSpec.VBTasksMap.ContainsAtLeastOneTask())

	var vbs []uint16
	vbs = append(vbs, 0)
	endSeqos := make(map[uint16]uint64)
	endSeqos[0] = 100

	newTasks, err := NewBackfillVBTasksMap(added, vbs, endSeqos)
	assert.Nil(err)

	testBackfillSpec.MergeNewTasks(newTasks, true)
	checkMapping := testBackfillSpec.VBTasksMap.GetAllCollectionNamespaceMappings()
	assert.Equal(1, len(checkMapping))

	actualTask, exists, unlockFunc := testBackfillSpec.VBTasksMap.VBTasksMap[0].GetRO(0)
	assert.True(exists)
	_, filter, err := actualTask.ToDcpNozzleTask(&provisionedManifest)
	unlockFunc()
	assert.Nil(err)
	assert.False(filter.UseManifestUid)
	assert.Equal(1, len(filter.CollectionsList))
}

func TestGetDeduplicatedSourceNamespaces(t *testing.T) {
	fmt.Println("============== Test case start: TestGetDeduplicatedSourceNamespaces =================")
	defer fmt.Println("============== Test case end: TestGetDeduplicatedSourceNamespaces =================")
	assert := assert.New(t)

	testMap := NewVBTasksMap()
	for i := uint16(1); i < uint16(10); i++ {
		backfillTasks := NewBackfillTasks()
		for j := uint16(0); j < i; j++ {
			var namespaceMapping []CollectionNamespaceMapping
			oneNamespace := make(CollectionNamespaceMapping)
			srcNs := NewSourceCollectionNamespace(&base.CollectionNamespace{
				ScopeName:      "S1",
				CollectionName: fmt.Sprintf("Col%v", j),
			})
			oneNamespace.AddSingleSourceNsMapping(srcNs, &base.CollectionNamespace{})
			namespaceMapping = append(namespaceMapping, oneNamespace)

			timestamps := BackfillVBTimestamps{
				StartingTimestamp: &base.VBTimestamp{},
				EndingTimestamp:   &base.VBTimestamp{},
			}
			backfillTask := &BackfillTask{
				Timestamps:               &timestamps,
				requestedCollections_:    namespaceMapping,
				RequestedCollectionsShas: []string{"test"},
				mutex:                    &sync.RWMutex{},
			}
			backfillTasks.List = append(backfillTasks.List, backfillTask)
		}
		testMap.VBTasksMap[i] = &backfillTasks
	}

	assert.NotNil(testMap)
	deduplicatedSources := testMap.GetDeduplicatedSourceNamespaces()
	assert.Equal(9, len(deduplicatedSources))
}

func TestIsMigrationExplicitRule(t *testing.T) {
	fmt.Println("============== Test case start: TestIsMigrationExplicitRule =================")
	defer fmt.Println("============== Test case end: TestIsMigrationExplicitRule =================")
	assert := assert.New(t)

	rule := make(CollectionsMappingRulesType)
	rule[fmt.Sprintf("%v%v%v", base.DefaultScopeCollectionName, base.ScopeCollectionDelimiter, base.DefaultScopeCollectionName)] = fmt.Sprintf("%v%v%v", "S1", base.ScopeCollectionDelimiter, "col1")
	assert.True(rule.IsExplicitMigrationRule())
	assert.Nil(rule.ValidateMigrateRules())

	ruleNeg1 := rule.Clone()
	ruleNeg2 := rule.Clone()

	ruleNeg1["EXISTS(key)"] = "S2.col2"
	assert.False(ruleNeg1.IsExplicitMigrationRule())
	assert.Equal(ErrorMigrationExplicitOnlyOneAllowed, ruleNeg1.ValidateMigrateRules())

	ruleNeg2[fmt.Sprintf("%v%v%v", base.DefaultScopeCollectionName, base.ScopeCollectionDelimiter, base.DefaultScopeCollectionName)] = nil
	assert.False(ruleNeg2.IsExplicitMigrationRule())
	assert.Equal(ErrorDenyRuleMigrationModeNotAllowed, ruleNeg2.ValidateMigrateRules())

	ruleNeg3 := make(CollectionsMappingRulesType)
	ruleNeg3["EXISTS(key)"] = "S2.col2"
	ruleNeg3["scopeName"] = "targetScopeName"
	validateErr := ruleNeg3.ValidateMigrateRules()
	assert.NotNil(validateErr)
	assert.True(strings.Contains(validateErr.Error(), ErrorMigrationExplicitScopeToScopeNotAllowedStr))

	ruleNeg4 := make(CollectionsMappingRulesType)
	ruleNeg4["scopeName"] = nil
	validateErr = ruleNeg4.ValidateMigrateRules()
	assert.Equal(ErrorDenyRuleMigrationModeNotAllowed, ruleNeg4.ValidateMigrateRules())
}

func TestSpecialMigrationNamespaceFromRules(t *testing.T) {
	fmt.Println("============== Test case start: TestSpecialMigrationNamespaceFromRules =================")
	defer fmt.Println("============== Test case end: TestSpecialMigrationNamespaceFromRules =================")
	assert := assert.New(t)

	oldRules := make(CollectionsMappingRulesType)
	oldRules[fmt.Sprintf("%v%v%v", base.DefaultScopeCollectionName, base.ScopeCollectionDelimiter, base.DefaultScopeCollectionName)] = fmt.Sprintf("%v%v%v", "S1", base.ScopeCollectionDelimiter, "col1")
	assert.True(oldRules.IsExplicitMigrationRule())

	defaultManifest := NewDefaultCollectionsManifest()
	data, err := ioutil.ReadFile(provisionedFile)
	assert.Nil(err)
	var provisionedManifest CollectionsManifest
	err = provisionedManifest.LoadBytes(data)
	assert.Nil(err)
	manifestPair := CollectionsManifestPair{
		Source: &defaultManifest,
		Target: &provisionedManifest,
	}

	mode := base.CollectionsMgtType(0)
	mode.SetMigration(true)

	oldMapping, err := NewCollectionNamespaceMappingFromRules(manifestPair, mode, oldRules, false, false)
	assert.Nil(err)
	assert.Len(oldMapping, 1)

	newRules := make(CollectionsMappingRulesType)
	newRules[fmt.Sprintf("%v%v%v", base.DefaultScopeCollectionName, base.ScopeCollectionDelimiter, base.DefaultScopeCollectionName)] = fmt.Sprintf("%v%v%v", "S1", base.ScopeCollectionDelimiter, "col2")

	newMapping, err := NewCollectionNamespaceMappingFromRules(manifestPair, mode, newRules, false, false)
	assert.Nil(err)
	assert.Len(newMapping, 1)

	added, removed := oldMapping.Diff(newMapping)
	assert.Len(added, 1)
	assert.Len(removed, 1)
}

func TestTwoToOneExplicitMapping(t *testing.T) {
	fmt.Println("============== Test case start: TestTwoToOneExplicitMapping =================")
	defer fmt.Println("============== Test case end: TestTwoToOneExplicitMapping =================")
	assert := assert.New(t)

	// Test 2 collections to 1 collection
	rule := make(CollectionsMappingRulesType)
	rule[fmt.Sprintf("%v%v%v", "S1", base.ScopeCollectionDelimiter, "c1")] = fmt.Sprintf("%v%v%v", "S1", base.ScopeCollectionDelimiter, "col1")
	rule[fmt.Sprintf("%v%v%v", "S1", base.ScopeCollectionDelimiter, "c2")] = fmt.Sprintf("%v%v%v", "S1", base.ScopeCollectionDelimiter, "col1")
	assert.False(rule.IsExplicitMigrationRule())
	assert.NotNil(rule.ValidateExplicitMapping())

	// Test 2 scopes to 1 scope
	rule = make(CollectionsMappingRulesType)
	rule[fmt.Sprintf("%v", "S1")] = fmt.Sprintf("%v", "S1")
	rule[fmt.Sprintf("%v", "S2")] = fmt.Sprintf("%v", "S1")
	assert.False(rule.IsExplicitMigrationRule())
	assert.NotNil(rule.ValidateExplicitMapping())
}
