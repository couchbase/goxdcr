/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package parts

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/crMeta"
	"github.com/stretchr/testify/assert"
)

/**
 * The tests here are live tests that expect
 *  - two clusters C1/C2 running
 *  - Administrator/wewewe can login
 *  - and remote cluster reference already created
 * It will create buckets/replication before starting each test
 */
const sourceConnStr = "couchbase://127.0.0.1:12000"
const targetConnStr = "couchbase://127.0.0.1:12002"
const targetCluster = "C2"
const urlCreateReplicationFmt = "http://127.0.0.1:%s/controller/createReplication"
const urlFunctionsFmt = "http://127.0.0.1:13000/evaluator/v1/libraries/%v"
const bucketPath = "/pools/default/buckets"

type SubdocInternal struct {
	DocFlags gocb.SubdocDocFlag
	User     []byte
}

func createBucket(connStr, bucketName string) (cluster *gocb.Cluster, bucket *gocb.Bucket, err error) {
	cluster, err = gocb.Connect(connStr, gocb.ClusterOptions{Authenticator: gocb.PasswordAuthenticator{
		Username: username,
		Password: password,
	}})
	if err != nil {
		return
	}
	err = cluster.WaitUntilReady(15*time.Second, nil)
	if err != nil {
		return
	}
	setting := gocb.BucketSettings{
		Name:                 bucketName,
		RAMQuotaMB:           100,
		BucketType:           "membase",
		NumReplicas:          0,
		FlushEnabled:         true,
		ReplicaIndexDisabled: true,
	}
	mgr := cluster.Buckets()
	// Don't check error since the bucket may already exist
	mgr.CreateBucket(gocb.CreateBucketSettings{
		BucketSettings:         setting,
		ConflictResolutionType: "custom"},
		&gocb.CreateBucketOptions{Timeout: 10 * time.Second})
	// Wait for bucket ready
	bucket = cluster.Bucket(bucketName)
	err = bucket.WaitUntilReady(20*time.Second, &gocb.WaitUntilReadyOptions{DesiredState: gocb.ClusterStateOnline})
	return
}

func createReplication(t *testing.T, bucketName string, mergeFunction string, timeout int, sourceToTarget bool) {
	assert := assert.New(t)
	client := &http.Client{}
	data := url.Values{}
	data.Set("fromBucket", bucketName)
	var toCluster string
	var urlCreateReplication string
	if sourceToTarget {
		toCluster = targetClusterName
		urlCreateReplication = fmt.Sprintf(urlCreateReplicationFmt, sourcePort)
	} else { // reverse direction
		toCluster = sourceClusterName
		urlCreateReplication = fmt.Sprintf(urlCreateReplicationFmt, targetPort)
	}
	data.Add("toCluster", toCluster)
	data.Add("toBucket", bucketName)
	data.Add("replicationType", "continuous")
	data.Add("mergeFunctionMapping", "{\""+base.BucketMergeFunctionKey+"\":\""+mergeFunction+"\"}")
	//data.Add("logLevel", "Debug")
	data.Add(base.JSFunctionTimeoutKey, fmt.Sprintf("%v", timeout))
	req, err := http.NewRequest(base.MethodPost, urlCreateReplication, bytes.NewBufferString(data.Encode()))
	assert.Nil(err)
	req.Header.Set(base.ContentType, base.DefaultContentType)
	i := 0
	var bodyBytes []byte
	for ; i <= 5; i++ {
		req.SetBasicAuth(username, password)
		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("client.Do failed for req: %v, err: %v\n", req, err)
			t.FailNow()
		}
		assert.NotNil(resp)
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		assert.Nil(err)
		resp.Body.Close()
		created := !strings.Contains(string(bodyBytes), "error") || strings.Contains(string(bodyBytes), "already exists")
		if created {
			fmt.Printf("Created replication for bucket %v with merge function %v\n", bucketName, mergeFunction)
			return
		}
		time.Sleep(2 * time.Second)
	}
	fmt.Printf("Failed to create replication after %d retries. Error '%s'", i, string(bodyBytes))
}

// This routine will wait for replication and verify source/target CAS are the same
func waitForReplication(key string, cas gocb.Cas, target *gocb.Bucket) (err error) {
	var i int
	for i = 0; i < 120; i++ {
		value, err := getPathValue(key, []string{base.XATTR_HLV}, target)
		if err == nil && value.Cas() == cas {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("Document '%s' with source cas %v has not replicated to target after %v seconds.\n", key, cas, i)
}

// Verify that document cas has changed from the cas passed in.
func waitForCasChange(t *testing.T, key string, cas gocb.Cas, bucket *gocb.Bucket) {
	var err error
	var doc *gocb.GetResult
	newCas := cas
	start := time.Now()
	for i := 0; i < 120 && (err != nil || cas == newCas); i++ {
		doc, err = bucket.DefaultCollection().Get(key, nil)
		newCas = doc.Cas()
		time.Sleep(1 * time.Second)
	}
	if newCas == cas {
		t.Errorf("Document '%s' with cas %v has not changed in %v, err=%v\n", key, cas, time.Since(start), err)
		t.FailNow()
	}
}

func waitForMV(key string, expectedMV map[string]string, bucket *gocb.Bucket) (cas gocb.Cas, err error) {
	var mv []byte
	for i := 0; i < 120; i++ {
		value, err := bucket.DefaultCollection().LookupIn(key,
			[]gocb.LookupInSpec{gocb.GetSpec(crMeta.XATTR_MV_PATH, &gocb.GetSpecOptions{IsXattr: true})}, nil)
		if err != nil {
			return 0, err
		}
		value.ContentAt(0, &mv)
		if len(mv) > 0 {
			it, err := base.NewCCRXattrFieldIterator(mv)
			if err != nil {
				return 0, err
			}
			mvMap := make(map[string]string)
			for it.HasNext() {
				k, v, err := it.Next()
				if err != nil {
					return 0, err
				}
				mvMap[string(k)] = string(v)
			}
			if reflect.DeepEqual(mvMap, expectedMV) {
				return value.Cas(), nil
			}
		}
		time.Sleep(1 * time.Second)
	}
	return 0, fmt.Errorf("MV %s is not expected %s\n", mv, expectedMV)
}

// Verify _vv.cv == CAS and _vv.cvCas == CAS
func verifyCv(key string, target *gocb.Bucket) (err error) {
	var cvCasHex, cvHex string
	paths := []string{crMeta.XATTR_CVCAS_PATH, crMeta.XATTR_VER_PATH}
	value, err := getPathValue(key, paths, target)
	if err != nil {
		return
	}
	value.ContentAt(0, &cvCasHex)
	value.ContentAt(1, &cvHex)
	cvCas, err := base.HexLittleEndianToUint64([]byte(cvCasHex))
	if err != nil {
		fmt.Printf("verifyCv for Key: %v, retrieving cvCas: error: %v\n", key, err)
		return
	}
	cv, err := base.HexLittleEndianToUint64([]byte(cvHex))
	if err != nil {
		fmt.Printf("verifyCv for Key: %v, retrieving ver: error: %v\n", key, err)
		return
	}
	if value.Cas() != gocb.Cas(cv) {
		return fmt.Errorf("%v %v does not equal to CAS value %v", crMeta.XATTR_VER_PATH, cv, value.Cas())
	}
	if value.Cas() != gocb.Cas(cvCas) {
		return fmt.Errorf("%v %v does not equal to CAS value %v", crMeta.XATTR_CVCAS_PATH, cvCas, value.Cas())
	}
	return nil
}

func getPathValue(key string, paths []string, target *gocb.Bucket) (value *gocb.LookupInResult, err error) {
	specs := make([]gocb.LookupInSpec, len(paths))
	for i, path := range paths {
		spec := gocb.GetSpec(path, &gocb.GetSpecOptions{IsXattr: true})
		specs[i] = spec
	}
	value, err = target.DefaultCollection().LookupIn(key, specs,
		&gocb.LookupInOptions{Internal: SubdocInternal{DocFlags: gocb.SubdocDocFlagAccessDeleted}})
	return
}

func createMergeFunction(t *testing.T, mergeFunction string) {
	assert := assert.New(t)
	urlFunctions := fmt.Sprintf(urlFunctionsFmt, mergeFunction)
	fileName := fmt.Sprintf("../tools/testScripts/customConflict/%v.js", mergeFunction)
	f, err := ioutil.ReadFile(fileName)
	assert.Nil(err)
	f1 := strings.Replace(string(f), "\n", "", -1)
	req, err := http.NewRequest(base.MethodPost, urlFunctions, bytes.NewBufferString(f1))
	assert.Nil(err)
	req.Header.Set(base.ContentType, base.JsonContentType)
	req.SetBasicAuth(username, password)
	response, err := http.DefaultClient.Do(req)
	assert.Nil(err)
	defer response.Body.Close()
	if response.StatusCode == http.StatusOK {
		fmt.Printf("Created merge function %v\n", mergeFunction)
	} else {
		fmt.Printf("create merge function returned %v for request %v\n", response.Status, req)
		t.FailNow()
	}
}

func TestCustomCrXattrAfterRep(t *testing.T) {
	fmt.Println("============== Test case start: TestCustomCrXattrAfterRep =================")
	defer fmt.Println("============== Test case end: TestCustomCrXattrAfterRep =================")
	if !targetXmemIsUpAndCorrectSetupExists(xmemBucket) {
		fmt.Println("Skipping since live cluster_run setup has not been detected")
		return
	}
	bucketName := "TestCustomCrXattrAfterRep"
	assert := assert.New(t)
	srcCluster, sourceBucket, err := createBucket(sourceConnStr, bucketName)
	if err != nil {
		fmt.Printf("TestCustomCrXattrAfterRep skipped because source cluster is not ready. Error: %v\n", err)
		return
	}
	defer srcCluster.Close(nil)
	trgCluster, targetBucket, err := createBucket(targetConnStr, bucketName)
	if err != nil {
		fmt.Printf("TestCustomCrXattrAfterRep skipped because target cluster is not ready. Error: %v\n", err)
		return
	}
	defer trgCluster.Close(nil)
	assert.NotNil(sourceBucket)
	assert.NotNil(targetBucket)
	createReplication(t, bucketName, base.DefaultMergeFunc, base.JSFunctionTimeoutDefault, true)
	createReplication(t, bucketName, base.DefaultMergeFunc, base.JSFunctionTimeoutDefault, false) // reverse direction to test pruning
	expire := 1 * time.Hour
	/*
	 * Test 1: New doc at source. Expect to format _vv at target with cv and id.
	 */
	key := time.Now().Format(time.RFC3339)
	fmt.Printf("Test 1: Insert %v and expect target to have %v and %v\n", key, crMeta.XATTR_VER_PATH, crMeta.XATTR_SRC_PATH)
	upsOut, err := sourceBucket.DefaultCollection().Upsert(key,
		User{Id: "kingarthur",
			Email:     "kingarthur@couchbase.com",
			Interests: []string{"Holy Grail", "African Swallows"}}, &gocb.UpsertOptions{Expiry: expire})
	if err != nil {
		assert.FailNow("Upsert failed with errror %v", err)
	}
	err = waitForReplication(key, upsOut.Cas(), targetBucket)
	assert.Nil(err)
	err = verifyCv(key, targetBucket)
	assert.Nil(err)
	_, err = getPathValue(key, []string{crMeta.XATTR_SRC_PATH}, targetBucket)
	assert.Nil(err)

	/*
	* Test 2: Update the previous document to add two XATTRs. Make sure we don't mess it up
	 */
	fmt.Println("Test 2: Update to add two XATTRs")
	mutOut, err := sourceBucket.DefaultCollection().MutateIn(key,
		[]gocb.MutateInSpec{
			gocb.InsertSpec("list", [2]uint32{11, 12}, &gocb.InsertSpecOptions{IsXattr: true, CreatePath: true})},
		&gocb.MutateInOptions{Expiry: expire})
	assert.Nil(err)
	err = waitForReplication(key, mutOut.Cas(), targetBucket)
	assert.Nil(err)
	mutOut, err = sourceBucket.DefaultCollection().MutateIn(key,
		[]gocb.MutateInSpec{
			gocb.InsertSpec("aKey", "some values", &gocb.InsertSpecOptions{IsXattr: true, CreatePath: true})},
		&gocb.MutateInOptions{Expiry: expire})
	assert.Nil(err)
	err = waitForReplication(key, mutOut.Cas(), targetBucket)
	assert.Nil(err)
	err = verifyCv(key, targetBucket)
	assert.Nil(err)
	value, err := getPathValue(key, []string{"list"}, targetBucket)
	assert.Nil(err)
	var l interface{}
	value.ContentAt(0, &l)
	fmt.Printf("list value: %v\n", l)
	assert.NotNil(value)
	value, err = getPathValue(key, []string{"aKey"}, targetBucket)
	assert.Nil(err)
	var k interface{}
	value.ContentAt(0, &k)
	fmt.Printf("akey value: %v\n", k)
	assert.NotNil(value)
	var id string
	value, err = getPathValue(key, []string{crMeta.XATTR_SRC_PATH}, targetBucket)
	assert.Nil(err)
	value.ContentAt(0, &id)

	/*
	* Test 3: Simulate a merged document at source that dominates the previous version
	* Verify that the document is replicated without change
	 */
	fmt.Println("Test 3: Simulate a merged document at source that dominates the previous version")
	// First get the cv so we can use it to build the MV
	var cv string
	value, err = getPathValue(key, []string{crMeta.XATTR_VER_PATH}, targetBucket)
	assert.Nil(err)
	value.ContentAt(0, &cv)
	cas, err := base.HexLittleEndianToUint64([]byte(cv))
	assert.Nil(err)
	mvMap := make(map[string]string)
	mvMap["Cluster1"] = "FhSITdr4AAA"
	mvMap["Cluster2"] = "FhSITdr4AAA"
	mvMap[id] = string(base.Uint64ToBase64(cas))
	mutOut, err = sourceBucket.DefaultCollection().MutateIn(key,
		[]gocb.MutateInSpec{
			gocb.InsertSpec(crMeta.XATTR_CVCAS_PATH, gocb.MutationMacroCAS, &gocb.InsertSpecOptions{IsXattr: true, CreatePath: true}),
			gocb.InsertSpec(crMeta.XATTR_VER_PATH, gocb.MutationMacroCAS, &gocb.InsertSpecOptions{IsXattr: true, CreatePath: true}),
			gocb.InsertSpec(crMeta.XATTR_SRC_PATH, id, &gocb.InsertSpecOptions{IsXattr: true, CreatePath: true}),
			gocb.InsertSpec(crMeta.XATTR_MV_PATH, mvMap, &gocb.InsertSpecOptions{IsXattr: true, CreatePath: true})},
		&gocb.MutateInOptions{Expiry: expire})
	assert.Nil(err)
	err = waitForReplication(key, mutOut.Cas(), targetBucket)
	assert.Nil(err)

	// CV should not change.
	var sourceCv, targetCv string
	value, err = getPathValue(key, []string{crMeta.XATTR_VER_PATH}, targetBucket)
	assert.Nil(err)
	value.ContentAt(0, &targetCv)
	value, err = getPathValue(key, []string{crMeta.XATTR_VER_PATH}, sourceBucket)
	assert.Nil(err)
	value.ContentAt(0, &sourceCv)
	assert.Equal(targetCv, sourceCv)

	// SRC should not change
	var sourceId string
	value, err = getPathValue(key, []string{crMeta.XATTR_SRC_PATH}, sourceBucket)
	assert.Nil(err)
	value.ContentAt(0, &sourceId)
	assert.Equal(id, sourceId)

	// MV should not change
	var mv, sourceMv map[string]interface{}
	value, err = getPathValue(key, []string{crMeta.XATTR_MV_PATH}, targetBucket)
	assert.Nil(err)
	value.ContentAt(0, &mv)
	value, err = getPathValue(key, []string{crMeta.XATTR_MV_PATH}, sourceBucket)
	assert.Nil(err)
	value.ContentAt(0, &sourceMv)
	assert.Equal(mv, sourceMv)

	/*
	* Test 4: Update the previous merged doc and mv should be fold into pv
	 */
	fmt.Println("Test 4: Update a merged doc")
	mutOut, err = sourceBucket.DefaultCollection().MutateIn(key,
		[]gocb.MutateInSpec{
			gocb.InsertSpec("new", "doc field", &gocb.InsertSpecOptions{CreatePath: true})},
		&gocb.MutateInOptions{Expiry: expire})
	assert.Nil(err)
	err = waitForReplication(key, mutOut.Cas(), targetBucket)
	assert.Nil(err)
	err = verifyCv(key, targetBucket)
	assert.Nil(err)
	value, err = getPathValue(key, []string{crMeta.XATTR_SRC_PATH}, targetBucket)
	assert.Nil(err)
	value.ContentAt(0, &id)
	value, err = getPathValue(key, []string{crMeta.XATTR_PV_PATH}, targetBucket)
	assert.Nil(err)
	var pv4 map[string]interface{}
	value.ContentAt(0, &pv4)
	// There are 3 items in MV, 2 moved to PV and got pruned, 1 is dedupped since its ID is in the cv
	assert.Nil(pv4)
	mv = nil
	value, err = getPathValue(key, []string{crMeta.XATTR_MV_PATH}, targetBucket)
	assert.Nil(err)
	value.ContentAt(0, &mv)
	assert.Nil(mv)

	/*
	 * Test 5: Update at target, The previous version from source cluster is added to PV.
	 */
	fmt.Println("Test 5: Update at target, The previous version from source cluster is added to PV")
	upsOut, err = targetBucket.DefaultCollection().Upsert(key,
		User{Id: "kingarthur",
			Email:     "kingarthur@couchbase.com",
			Interests: []string{"Holy Grail", "African Swallows", "Target item"}},
		&gocb.UpsertOptions{Expiry: expire})
	assert.Nil(err)
	err = waitForReplication(key, upsOut.Cas(), sourceBucket) // replicate from C2 to C1
	assert.Nil(err)
	err = verifyCv(key, sourceBucket)
	assert.Nil(err)
	_, err = getPathValue(key, []string{crMeta.XATTR_SRC_PATH}, sourceBucket)
	assert.Nil(err)
	value, err = getPathValue(key, []string{crMeta.XATTR_PV_PATH}, sourceBucket)
	assert.Nil(err)
	var pv map[string]interface{}
	value.ContentAt(0, &pv)
	assert.Equal(1, len(pv), fmt.Sprintf("Unexpected pv=%v\n", pv))

	/*
	* Test 6: Do an upsert without subdoc flags and check that xattrs are preserved
	 */
	fmt.Println("Test 6: Upsert without subdoc flags and check that xattrs are preserved")
	upsOut, err = sourceBucket.DefaultCollection().Upsert(key,
		User{Id: "kingarthur",
			Email:     "kingarthur@couchbase.com",
			Interests: []string{"Holy Grail", "African Swallows", "New item"}},
		&gocb.UpsertOptions{Expiry: expire})
	assert.Nil(err)
	err = waitForReplication(key, upsOut.Cas(), targetBucket)
	assert.Nil(err)
	err = verifyCv(key, targetBucket)
	assert.Nil(err)
	_, err = getPathValue(key, []string{crMeta.XATTR_SRC_PATH}, targetBucket)
	assert.Nil(err)
	pv = nil
	value, err = getPathValue(key, []string{crMeta.XATTR_PV_PATH}, sourceBucket)
	assert.Nil(err)
	value.ContentAt(0, &pv)
	assert.Equal(1, len(pv), fmt.Sprintf("key=%q Expect pv len to be 1 but got pv=%q", key, pv))

	/*
	* Test 7. Delete the document and _vv is intact
	 */
	fmt.Println("Test 7: Delete the document and _vv is intact")
	rmOut, err := sourceBucket.DefaultCollection().Remove(key, nil)
	assert.Nil(err)
	err = waitForReplication(key, rmOut.Cas(), targetBucket)
	assert.Nil(err)
	value, err = getPathValue(key, []string{base.XATTR_HLV}, targetBucket)
	assert.Nil(err)
	var vv map[string]interface{}
	value.ContentAt(0, &vv)
	assert.Equal(4, len(vv))

	/*
	* Test 8. Recreate the document and old _vv is lost, unfortunately
	 */
	fmt.Println("Test 8: Recreate the document and old _vv is lost")
	upsOut, err = sourceBucket.DefaultCollection().Upsert(key,
		User{Id: "kingarthur",
			Email:     "kingarthur@couchbase.com",
			Interests: []string{"Holy Grail"}},
		&gocb.UpsertOptions{Expiry: expire})
	assert.Nil(err)
	err = waitForReplication(key, upsOut.Cas(), targetBucket)
	assert.Nil(err)
	value, err = getPathValue(key, []string{base.XATTR_HLV}, targetBucket)
	assert.Nil(err)
	vv = nil
	value.ContentAt(0, &vv)
	assert.Equal(3, len(vv))
}

func MB_58490_TestCustomCRDeletedDocs(t *testing.T) {
	fmt.Println("============== Test case start: TestCustomCRDeletedDocs =================")
	defer fmt.Println("============== Test case end: TestCustomCRDeletedDocs =================")
	if !targetXmemIsUpAndCorrectSetupExists(xmemBucket) {
		fmt.Println("Skipping since live cluster_run setup has not been detected")
		return
	}
	bucketName := "TestCustomCRDeletedDocs"
	assert := assert.New(t)
	srcCluster, sourceBucket, err := createBucket(sourceConnStr, bucketName)
	if err != nil {
		fmt.Printf("TestCustomCRDeletedDocs skipped because source cluster is not ready. Error: %v\n", err)
		return
	}
	defer srcCluster.Close(nil)
	trgCluster, targetBucket, err := createBucket(targetConnStr, bucketName)
	if err != nil {
		fmt.Printf("TestCustomCRDeletedDocs skipped because target cluster is not ready. Error: %v\n", err)
		return
	}
	defer trgCluster.Close(nil)
	assert.NotNil(sourceBucket)
	assert.NotNil(targetBucket)
	createReplication(t, bucketName, base.DefaultMergeFunc, base.JSFunctionTimeoutDefault, true)

	key := time.Now().Format(time.RFC3339)
	fmt.Printf("Test with key %q\n", key)
	expire := 1 * time.Hour
	_, err = sourceBucket.DefaultCollection().Upsert(key,
		User{Id: "kingarthur",
			Email:     "kingarthur@couchbase.com",
			Interests: []string{"Holy Grail", "African Swallows", "Source"}},
		&gocb.UpsertOptions{Expiry: expire})
	assert.Nil(err)
	// Target dominate. Nothing should happen when replicating from source to target
	_, err = targetBucket.DefaultCollection().Upsert(key,
		User{Id: "kingarthur",
			Email:     "kingarthur@couchbase.com",
			Interests: []string{"Holy Grail", "African Swallows", "Target"}},
		&gocb.UpsertOptions{Expiry: expire})
	assert.Nil(err)

	rmOut, err := sourceBucket.DefaultCollection().Remove(key, nil)
	// The deleted document should be replicated
	err = waitForReplication(key, rmOut.Cas(), targetBucket)
	assert.Nil(err)
	// verify deleted document has the expected XATTR
	value, err := getPathValue(key, []string{base.XATTR_HLV}, targetBucket)
	assert.Nil(err)
	var xdcr map[string]interface{}
	value.ContentAt(0, &xdcr)
	if _, ok := xdcr[crMeta.HLV_SRC_FIELD]; !ok {
		fmt.Printf("XATTRS %s does not contain the expected %v field", xdcr, crMeta.HLV_SRC_FIELD)
		t.FailNow()
	}
	if _, ok := xdcr[crMeta.HLV_VER_FIELD]; !ok {
		fmt.Printf("XATTRS %s does not contain the expected %v field", xdcr, crMeta.HLV_VER_FIELD)
		t.FailNow()
	}
	err = verifyCv(key, targetBucket)
	assert.Nil(err)
}

// TODO: MB-58490: Need to revisit this.
// Besides the issues listed in the MB, currently gocb.v2 does not seem to have a way to insert
// a non-json document. Inserting a document with the content ""Source document" turns into a json document
// {"0":"S","1":"o","2":"u","3":"r","4":"c","5":"e","6":" ","7":"d","8":"o","9":"c","10":"u","11":"m","12":"e","13":"n","14":"t"}
//func TestCustomCRBinaryDocs(t *testing.T) {
//	fmt.Println("============== Test case start: TestCustomCRBinaryDocs =================")
//	defer fmt.Println("============== Test case end: TestCustomCRBinaryDocs =================")
//	bucketName := "TestCustomCRBinaryDocs"
//	assert := assert.New(t)
//	srcCluster, sourceBucket, err := createBucket(sourceConnStr, bucketName)
//	if err != nil {
//		fmt.Printf("TestCustomCRBinaryDocs skipped because source cluster is not ready. Error: %v\n", err)
//		return
//	}
//	defer srcCluster.Close(nil)
//	trgCluster, targetBucket, err := createBucket(targetConnStr, bucketName)
//	if err != nil {
//		fmt.Printf("TestCustomCRBinaryDocs skipped because target cluster is not ready. Error: %v\n", err)
//		return
//	}
//	defer trgCluster.Close(nil)
//	assert.NotNil(sourceBucket)
//	assert.NotNil(targetBucket)
//	createReplication(t, bucketName, "simpleMerge", base.JSFunctionTimeoutDefault, true)
//
//	fmt.Println("Test 1. Create target binary doc, create source binary doc. Source wins.")
//	key := "sourceAndTargetBinary2"
//	expire := 1 * time.Hour
//	_, err = targetBucket.DefaultCollection().Upsert(key, "Target document",
//		&gocb.UpsertOptions{Expiry: expire})
//	assert.Nil(err)
//	upsOut, err := sourceBucket.DefaultCollection().Upsert(key, "Source document",
//		&gocb.UpsertOptions{Expiry: expire})
//	assert.Nil(err)
//	err = waitForReplication(key, upsOut.Cas(), targetBucket)
//	assert.Nil(err)
// TODO: Failing!
//fmt.Println("Test 2. Create target json doc, create source binary doc. Source wins.")
//key = "sourceBinaryTargetJson"
//_, err = targetBucket.DefaultCollection().Upsert(key,
//	User{Id: "kingarthur",
//		Email:     "kingarthur@couchbase.com",
//		Interests: []string{"Holy Grail", "African Swallows", "target"}},
//	&gocb.UpsertOptions{Expiry: expire})
//assert.Nil(err)
//upsOut, err = sourceBucket.DefaultCollection().Upsert(key, fmt.Sprintf("Source document for key %v", key),
//	&gocb.UpsertOptions{Expiry: expire})
//assert.Nil(err)
//err = waitForReplication(key, upsOut.Cas(), targetBucket)
//assert.Nil(err)

// TODO: Failing!
//fmt.Println("Test 3. Create target binary doc, create source json doc. Source wins.")
//key = "sourceJsonTargetBinary"
//trgOut, err := targetBucket.DefaultCollection().Upsert(key, "target document",
//	&gocb.UpsertOptions{Expiry: expire})
//assert.Nil(err)
//srcOut, err := sourceBucket.DefaultCollection().Upsert(key,
//	User{Id: "kingarthur",
//		Email:     "kingarthur@couchbase.com",
//		Interests: []string{"Holy Grail", "African Swallows", "Source"}},
//	&gocb.UpsertOptions{Expiry: expire})
//assert.True(srcOut.Cas() > trgOut.Cas())
//err = waitForReplication(key, upsOut.Cas(), targetBucket)
//assert.Nil(err)
//}

func TestCustomCrXattrAfterMerge(t *testing.T) {
	fmt.Println("============== Test case start: TestCustomCrXattrAfterMerge =================")
	defer fmt.Println("============== Test case end: TestCustomCrXattrAfterMerge =================")
	if !targetXmemIsUpAndCorrectSetupExists(xmemBucket) {
		fmt.Println("Skipping since live cluster_run setup has not been detected")
		return
	}
	bucketName := "TestCustomCrXattrAfterMerge"
	assert := assert.New(t)
	srcCluster, sourceBucket, err := createBucket(sourceConnStr, bucketName)
	if err != nil {
		fmt.Printf("TestCustomCrXattrAfterMerge skipped because source cluster is not ready. Error: %v\n", err)
		return
	}
	defer srcCluster.Close(nil)
	trgCluster, targetBucket, err := createBucket(targetConnStr, bucketName)
	if err != nil {
		fmt.Printf("TestCustomCrXattrAfterMerge skipped because target cluster is not ready. Error: %v\n", err)
		return
	}
	defer trgCluster.Close(nil)
	assert.NotNil(sourceBucket)
	assert.NotNil(targetBucket)
	mergeFunc := "simpleMerge"
	createMergeFunction(t, mergeFunc)
	createReplication(t, bucketName, mergeFunc, base.JSFunctionTimeoutDefault, true)

	// Create documents at target and then at source to get conflicts
	keyTime := time.Now().Format(time.RFC3339)
	numDoc := 20
	cas := make([]gocb.Cas, numDoc)
	key := make([]string, numDoc)
	expire := 1 * time.Hour
	for i := 0; i < numDoc; i++ {
		key[i] = fmt.Sprintf("%v_%v", keyTime, i)
	}
	for i := 0; i < numDoc; i++ {
		_, err := targetBucket.DefaultCollection().Upsert(key[i],
			User{Id: "kingarthur",
				Email:     "kingarthur@couchbase.com",
				Interests: []string{"Holy Grail", "African Swallows", "Target"}},
			&gocb.UpsertOptions{Expiry: expire})
		assert.Nil(err)
	}
	fmt.Printf("Created %v target documents\n", numDoc)
	for i := 0; i < numDoc; i++ {
		value, err := sourceBucket.DefaultCollection().Upsert(key[i],
			User{Id: "kingarthur",
				Email:     "kingarthur@couchbase.com",
				Interests: []string{"Holy Grail", "African Swallows", "Source"}},
			&gocb.UpsertOptions{Expiry: expire})
		assert.Nil(err)
		cas[i] = value.Result.Cas()
	}
	fmt.Printf("Created %v source documents\n", numDoc)
	fmt.Println("Wait for merge to finish")
	for i := 0; i < numDoc; i++ {
		waitForCasChange(t, key[i], cas[i], sourceBucket)
	}
	fmt.Printf("Verifying merge and replication of merged doc for %v documents\n", numDoc)
	for i := 0; i < numDoc; i++ {
		value, err := getPathValue(key[i], []string{crMeta.XATTR_MV_PATH}, sourceBucket)
		assert.Nil(err, "%v lookup failed for key %v", crMeta.XATTR_MV_PATH, key[i])
		err = verifyCv(key[i], sourceBucket)
		assert.Nil(err)
		cas[i] = value.Cas()
		err = waitForReplication(key[i], cas[i], targetBucket)
		assert.Nil(err)
		err = verifyCv(key[i], targetBucket)
		assert.Nil(err)
	}
}

func TestCustomCrXattrSetBack(t *testing.T) {
	fmt.Println("============== Test case start: TestCustomCrXattrSetBack =================")
	defer fmt.Println("============== Test case end: TestCustomCrXattrSetBack =================")
	if !targetXmemIsUpAndCorrectSetupExists(xmemBucket) {
		fmt.Println("Skipping since live cluster_run setup has not been detected")
		return
	}
	assert := assert.New(t)
	bucketName := "TestCustomCrXattrSetBack"
	srcCluster, sourceBucket, err := createBucket(sourceConnStr, bucketName)
	if err != nil {
		fmt.Printf("TestCustomCrXattrSetBack skipped because source cluster is not ready. Error: %v\n", err)
		return
	}
	defer srcCluster.Close(nil)
	trgCluster, targetBucket, err := createBucket(targetConnStr, bucketName)
	if err != nil {
		fmt.Printf("TestCustomCrXattrSetBack skipped because target cluster is not ready. Error: %v\n", err)
		return
	}
	defer trgCluster.Close(nil)
	assert.NotNil(sourceBucket)
	assert.NotNil(targetBucket)
	createReplication(t, bucketName, base.DefaultMergeFunc, base.JSFunctionTimeoutDefault, true)

	key := time.Now().Format(time.RFC3339) + "_setback"
	expire := 1 * time.Hour

	// Create documents at target that looks like merge from 3 clusters
	_, err = targetBucket.DefaultCollection().Upsert(key,
		User{Id: "kingarthur",
			Email:     "kingarthur@couchbase.com",
			Interests: []string{"Holy Grail", "African Swallows", "Target"}},
		&gocb.UpsertOptions{Expiry: expire})
	assert.Nil(err)

	mvTarget := make(map[string]string, 3)
	mvTarget["Cluster1"] = "FhSITdr4AAA"
	mvTarget["Cluster2"] = "FhSITdr4ABU"
	mvTarget["Cluster3"] = "FhSITdr4ACA"
	_, err = targetBucket.DefaultCollection().MutateIn(key,
		[]gocb.MutateInSpec{
			gocb.InsertSpec(crMeta.XATTR_CVCAS_PATH, gocb.MutationMacroCAS, &gocb.InsertSpecOptions{IsXattr: true, CreatePath: true}),
			gocb.InsertSpec(crMeta.XATTR_VER_PATH, gocb.MutationMacroCAS, &gocb.InsertSpecOptions{IsXattr: true, CreatePath: true}),
			gocb.InsertSpec(crMeta.XATTR_SRC_PATH, "C2", &gocb.InsertSpecOptions{IsXattr: true, CreatePath: true}),
			gocb.InsertSpec(crMeta.XATTR_MV_PATH, mvTarget, &gocb.InsertSpecOptions{IsXattr: true, CreatePath: true})},
		&gocb.MutateInOptions{Expiry: expire})
	assert.Nil(err)
	fmt.Printf("Created target document %v\n", key)

	// Create documents at source that looks like merge from 2 clusters
	_, err = sourceBucket.DefaultCollection().Upsert(key,
		User{Id: "kingarthur",
			Email:     "kingarthur@couchbase.com",
			Interests: []string{"Holy Grail", "African Swallows", "Source"}},
		&gocb.UpsertOptions{Expiry: expire})
	assert.Nil(err)
	mvSource := make(map[string]string, 3)
	mvSource["Cluster1"] = "FhSITdr4AAA"
	mvSource["Cluster2"] = "FhSITdr4ABU"
	_, err = sourceBucket.DefaultCollection().MutateIn(key,
		[]gocb.MutateInSpec{
			gocb.InsertSpec(crMeta.XATTR_CVCAS_PATH, gocb.MutationMacroCAS, &gocb.InsertSpecOptions{IsXattr: true, CreatePath: true}),
			gocb.InsertSpec(crMeta.XATTR_VER_PATH, gocb.MutationMacroCAS, &gocb.InsertSpecOptions{IsXattr: true, CreatePath: true}),
			gocb.InsertSpec(crMeta.XATTR_SRC_PATH, "C1", &gocb.InsertSpecOptions{IsXattr: true, CreatePath: true}),
			gocb.InsertSpec(crMeta.XATTR_MV_PATH, mvSource, &gocb.InsertSpecOptions{IsXattr: true, CreatePath: true})},
		&gocb.MutateInOptions{Expiry: expire})
	assert.Nil(err)
	fmt.Printf("Created source document %v\n", key)
	fmt.Println("Wait for target document set back to source.")
	cas, err := waitForMV(key, mvTarget, sourceBucket)
	assert.Nil(err)
	err = verifyCv(key, sourceBucket)
	assert.Nil(err)

	err = waitForReplication(key, cas, targetBucket)
	assert.Nil(err)
	err = verifyCv(key, targetBucket)
	assert.Nil(err)
}

// TODO: Javascript engine timeout not working
//func TestCustomCRFunctionTimeout(t *testing.T) {
//	fmt.Println("============== Test case start: TestCustomCRFunctionTimeout =================")
//	defer fmt.Println("============== Test case end: TestCustomCRFunctionTimeout =================")
//	bucketName := "TestCustomCRTimeout"
//	assert := assert.New(t)
//	srcCluster, sourceBucket, err := createBucket(sourceConnStr, bucketName)
//	if err != nil {
//		fmt.Printf("TestCustomCRFunctionTimeout skipped because source cluster is not ready. Error: %v\n", err)
//		return
//	}
//	defer srcCluster.Close(nil)
//	trgCluster, targetBucket, err := createBucket(targetConnStr, bucketName)
//	if err != nil {
//		fmt.Printf("TestCustomCRFunctionTimeout skipped because target cluster is not ready. Error: %v\n", err)
//		return
//	}
//	defer trgCluster.Close(nil)
//	assert.NotNil(sourceBucket)
//	assert.NotNil(targetBucket)
//	funcName := "loopForever"
//	createMergeFunction(t, funcName)
//	timeout := 4000
//	createReplication(t, bucketName, funcName, timeout, true)
//
//	// Create a document at target and then at source to get conflicts
//	key := time.Now().Format(time.RFC3339) + "timeout"
//	expire := 1 * time.Hour
//	_, err = targetBucket.DefaultCollection().Upsert(key,
//		User{Id: "kingarthur",
//			Email:     "kingarthur@couchbase.com",
//			Interests: []string{"Holy Grail", "African Swallows", "Target"}},
//		&gocb.UpsertOptions{Expiry: expire})
//	assert.Nil(err)
//	_, err = sourceBucket.DefaultCollection().Upsert(key,
//		User{Id: "kingarthur",
//			Email:     "kingarthur@couchbase.com",
//			Interests: []string{"Holy Grail", "African Swallows", "Target"}},
//		&gocb.UpsertOptions{Expiry: expire})
//	assert.Nil(err)
//	time.Sleep(40 * time.Second)
//	// Expect timeout message
//	filename := "../../../../../../ns_server/logs/n_0/goxdcr.log"
//	b, err := ioutil.ReadFile(filename)
//	assert.Nil(err)
//	s := string(b)
//	expected := fmt.Sprintf("loopForever stopped after running beyond %v ms", timeout)
//	assert.Contains(s, expected, fmt.Sprintf("%v does not contain expected message '%v'", filename, expected))
//}

//func TestBinaryDoc(t *testing.T) {
//	bucketName := "B0"
//	assert := assert.New(t)
//	cluster, bucket, err := createBucket(sourceConnStr, bucketName)
//	assert.Nil(err)
//	defer cluster.Close(nil)
//	key := "binaryDoc2"
//	_, err = bucket.DefaultCollection().Upsert(key, "Target document",
//		&gocb.UpsertOptions{Expiry: 1 * time.Hour})
//	assert.Nil(err)
//	res, err := bucket.DefaultCollection().LookupIn(key, []gocb.LookupInSpec{
//		gocb.GetSpec("$document.datatype", &gocb.GetSpecOptions{IsXattr: true}),
//	}, nil)
//	assert.Nil(err)
//	var value []byte
//	res.ContentAt(0, &value)
//	fmt.Printf("Lookup result for %v is %s\n", key, value)
//}
