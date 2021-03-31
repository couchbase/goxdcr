/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software
will be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package parts

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/goxdcr/base"
	"github.com/stretchr/testify/assert"
	gocb "gopkg.in/couchbase/gocb.v1"
)

/**
 * The tests here are live tests that expect
 *  - two clusters C1/C2 running
 *  - Administrator/wewewe can login
 *  - and remote cluster reference already created
 * It will create buckets/replication before starting each test
 */
const sourceConnStr = "http://127.0.0.1:9000"
const targetConnStr = "http://127.0.0.1:9001"
const targetCluster = "C2"
const urlCreateReplicationFmt = "http://127.0.0.1:%s/controller/createReplication"
const urlFunctionsFmt = "http://127.0.0.1:13000/functions/v1/libraries/xdcr/functions/%v"
const bucketPath = "/pools/default/buckets"

func createBucket(connStr, bucketName string) (bucket *gocb.Bucket, err error) {
	cluster, err := gocb.Connect(connStr)
	if err != nil {
		return
	}
	cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: username,
		Password: password,
	})
	// Create a bucket if it doesn't exist. Custom CR bucket can only be created using rest API
	// curl -X POST http://localhost:9000/pools/default/buckets -u Administrator:asdasd -d 'name=CCRBucket&bucketType=membase&conflictResolutionType=custom&ramQuotaMB=100'
	client := &http.Client{}
	data := url.Values{}
	data.Set("name", bucketName)
	data.Add("bucketType", "membase")
	data.Add("conflictResolutionType", "custom")
	data.Add("ramQuotaMB", "100")
	req, err := http.NewRequest(base.MethodPost, connStr+bucketPath, bytes.NewBufferString(data.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set(base.ContentType, base.DefaultContentType)
	req.SetBasicAuth(username, password)
	_, err = client.Do(req)
	if err != nil {
		return nil, err
	}
	// Don't check the response here since it may fail because it was created before
	//if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
	//	return nil, fmt.Errorf("create bucket returned status %v", resp.Status)
	//}
	for i := 0; i < 6; i++ {
		bucket, err = cluster.OpenBucket(bucketName, "")
		if err == nil {
			fmt.Printf("Created bucket %v\n", bucketName)
			return
		}
		time.Sleep(1 * time.Second)
	}
	return
}

func waitForBucketReady(t *testing.T, bucket *gocb.Bucket) {
	var err error
	key := "waitForBucket"
	for i := 0; i < 60; i++ {
		_, err := bucket.Upsert(key,
			User{Id: "kingarthur",
				Email:     "kingarthur@couchbase.com",
				Interests: []string{"Holy Grail", "African Swallows"}}, 0)
		if err != gocb.ErrTmpFail && err != gocb.ErrTimeout {
			break
		}
		time.Sleep(1 * time.Second)
	}
	bucket.Remove(key, 0)
	if err != nil {
		t.FailNow()
	}
	fmt.Println("Buckets are ready")
}
func createReplication(t *testing.T, bucketName string, mergeFunction string, sourceToTarget bool) {
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
	data.Add("logLevel", "Debug")
	data.Add(base.HlvPruningWindowKey, "360") // 1 hour
	req, err := http.NewRequest(base.MethodPost, urlCreateReplication, bytes.NewBufferString(data.Encode()))
	assert.Nil(err)
	req.Header.Set(base.ContentType, base.DefaultContentType)
	i := 0
	var bodyBytes []byte
	for ; i <= 5; i++ {
		req.SetBasicAuth(username, password)
		resp, err := client.Do(req)
		assert.Nil(err)
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
	t.FailNow()
}

// This routine will wait for replication and verify source/target CAS are the same
// This routine will wait for replication and verify source/target CAS are the same
func waitForReplication(key string, source, target *gocb.Bucket) (err error) {
	sourceDoc, err := source.LookupInEx(key, gocb.SubdocDocFlagAccessDeleted).GetEx("_xdcr", gocb.SubdocFlagXattr).Execute()
	if sourceDoc == nil {
		fmt.Printf("source lookup failed for key")
		return
	}
	targetDoc, err := target.LookupInEx(key, gocb.SubdocDocFlagAccessDeleted).GetEx("_xdcr", gocb.SubdocFlagXattr).Execute()

	var i int
	for i = 0; i < 120; i++ {
		if targetDoc != nil {
			if sourceDoc.Cas() == targetDoc.Cas() {
				return nil
			}
		}
		time.Sleep(1 * time.Second)
		targetDoc, err = target.LookupInEx(key, gocb.SubdocDocFlagAccessDeleted).GetEx("_xdcr", gocb.SubdocFlagXattr).Execute()
	}
	if targetDoc != nil && sourceDoc.Cas() == targetDoc.Cas() {
		return nil
	} else {
		return fmt.Errorf("Document '%s' with source cas %v has not replicated to target after %v seconds. Target cas %v\n", key, sourceDoc.Cas(), i, targetDoc.Cas())
	}
}

// Verify that document cas has changed from the cas passed in.
func waitForCasChange(t *testing.T, key string, cas gocb.Cas, bucket *gocb.Bucket) {
	var value interface{}
	var err error
	newCas := cas
	start := time.Now()
	for i := 0; i < 120 && (err != nil || cas == newCas); i++ {
		newCas, err = bucket.Get(key, value)
		time.Sleep(1 * time.Second)
	}
	if newCas == cas {
		t.Errorf("Document '%s' with cas %v has not changed in %v\n", key, cas, time.Since(start))
		t.FailNow()
	}
}

func waitForMV(key string, expectedMV []byte, bucket *gocb.Bucket) (err error) {
	mv := make([]byte, 2*len(expectedMV))
	for i := 0; i < 120; i++ {
		frag, err := bucket.LookupIn(key).GetEx("_xdcr.mv", gocb.SubdocFlagXattr).Execute()
		if err != nil {
			return err
		}
		if err = frag.Content("_xdcr.mv", &mv); err != nil {
			return err
		}
		if bytes.Equal(mv, expectedMV) {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("MV %s is not expected %s\n", mv, expectedMV)
}

// Verify _xdcr.cv == CAS
func verifyCv(key string, target *gocb.Bucket) (err error) {
	var cvHex string
	frag, err := target.LookupInEx(key, gocb.SubdocDocFlagAccessDeleted).GetEx("_xdcr.cv", gocb.SubdocFlagXattr).Execute()
	if err != nil && err != gocb.ErrSubDocSuccessDeleted {
		return
	}
	if err = frag.Content("_xdcr.cv", &cvHex); err != nil {
		return
	}
	cv, err := base.HexLittleEndianToUint64([]byte(cvHex))
	if err != nil {
		return
	}
	if frag.Cas() != gocb.Cas(cv) {
		return fmt.Errorf("_xdcr.cv %v does not equal to CAS value %v", cv, frag.Cas())
	}
	return nil
}
func getPathValue(key string, path string, target *gocb.Bucket) (value interface{}, err error) {
	frag, err := target.LookupInEx(key, gocb.SubdocDocFlagAccessDeleted).GetEx(path, gocb.SubdocFlagXattr).Execute()
	if err != nil && !strings.Contains(err.Error(), "document is soft-deleted") {
		return
	}
	err = frag.Content(path, &value)
	return value, err
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
func TestCcrXattrAfterRep(t *testing.T) {
	fmt.Println("============== Test case start: TestCcrXattrAfterRep =================")
	defer fmt.Println("============== Test case end: TestCcrXattrAfterRep =================")
	bucketName := "TestCcrXattrAfterRep"
	assert := assert.New(t)
	sourceBucket, err := createBucket(sourceConnStr, bucketName)
	if err != nil {
		fmt.Printf("TestCcrXattrAfterRep skipped because source cluster is not ready. Error: %v\n", err)
		return
	}
	targetBucket, err := createBucket(targetConnStr, bucketName)
	if err != nil {
		fmt.Printf("TestCcrXattrAfterRep skipped because target cluster is not ready. Error: %v\n", err)
		return
	}
	assert.NotNil(sourceBucket)
	assert.NotNil(targetBucket)
	waitForBucketReady(t, targetBucket)
	createReplication(t, bucketName, base.DefaultMergeFunc, true)
	createReplication(t, bucketName, base.DefaultMergeFunc, false) // reverse direction to test pruning

	/*
	 * Test 1: New doc at source. Expect to format _xdcr at target with cv and id.
	 */
	key := time.Now().Format(time.RFC3339)
	var expire uint32 = 60 * 60 * 24 // expires in 1 day
	_, err = sourceBucket.Upsert(key,
		User{Id: "kingarthur",
			Email:     "kingarthur@couchbase.com",
			Interests: []string{"Holy Grail", "African Swallows"}}, expire)
	assert.Nil(err)
	err = waitForReplication(key, sourceBucket, targetBucket)
	assert.Nil(err)
	err = verifyCv(key, targetBucket)
	assert.Nil(err)
	_, err = getPathValue(key, "_xdcr.id", targetBucket)
	assert.Nil(err)

	/*
	 * Test 2: Update the previous document to add two XATTRs. Make sure we don't mess it up
	 */
	_, err = sourceBucket.MutateIn(key, 0, expire).
		UpsertEx("list", [2]uint32{11, 12}, gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath).
		Execute()
	assert.Nil(err)
	err = waitForReplication(key, sourceBucket, targetBucket)
	assert.Nil(err)
	_, err = sourceBucket.MutateIn(key, 0, expire).
		UpsertEx("aKey", "some values.", gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath).
		Execute()
	assert.Nil(err)
	err = waitForReplication(key, sourceBucket, targetBucket)
	assert.Nil(err)
	err = verifyCv(key, targetBucket)
	assert.Nil(err)
	_, err = getPathValue(key, "list", targetBucket)
	assert.Nil(err)
	_, err = getPathValue(key, "aKey", targetBucket)
	assert.Nil(err)
	id, err := getPathValue(key, "_xdcr.id", targetBucket)
	assert.Nil(err)

	/*
	 * Test 3: Simulate a merged document at source that dominates the the the previous version
	 * Verify that the document is replicated without change
	 */
	// First get the cv so we can use it to build the MV
	cv, err := getPathValue(key, "_xdcr.cv", targetBucket)
	assert.Nil(err)
	cas, err := base.HexLittleEndianToUint64([]byte(cv.(string)))
	assert.Nil(err)
	mvMap := make(map[string]string)
	mvMap["Cluster1"] = "FhSITdr4AAA"
	mvMap["Cluster2"] = "FhSITdr4AAA"
	mvMap[id.(string)] = string(base.Uint64ToBase64(cas))
	_, err = sourceBucket.MutateIn(key, 0, expire).
		UpsertEx("_xdcr.cv", "${Mutation.CAS}", gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath|0x10).
		UpsertEx("_xdcr.id", id.(string), gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath).
		UpsertEx("_xdcr.mv", mvMap, gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath).
		Execute()
	err = waitForReplication(key, sourceBucket, targetBucket)
	assert.Nil(err)
	// CV should not change.
	cv, err = getPathValue(key, "_xdcr.cv", targetBucket)
	sourceCv, err := getPathValue(key, "_xdcr.cv", sourceBucket)
	assert.Nil(err)
	assert.Equal(cv, sourceCv)
	// ID should not change
	sourceId, err := getPathValue(key, "_xdcr.id", sourceBucket)
	assert.Nil(err)
	assert.Equal(id, sourceId)
	// MV should not change
	mv, err := getPathValue(key, "_xdcr.mv", targetBucket)
	assert.Nil(err)
	sourceMv, err := getPathValue(key, "_xdcr.mv", sourceBucket)
	assert.Nil(err)
	assert.Equal(mv, sourceMv)

	/*
	 * Test 4: Update the previous merged doc and mv should be fold into pcas
	 */
	_, err = sourceBucket.MutateIn(key, 0, expire).
		UpsertEx("new", "doc field", gocb.SubdocFlagCreatePath).
		Execute()
	assert.Nil(err)
	err = waitForReplication(key, sourceBucket, targetBucket)
	assert.Nil(err)
	err = verifyCv(key, targetBucket)
	assert.Nil(err)
	id, err = getPathValue(key, "_xdcr.id", targetBucket)
	assert.Nil(err)
	pc, err := getPathValue(key, "_xdcr.pc", targetBucket)
	assert.Nil(err)
	// There are 3 items in MV and they all move to PV
	assert.Equal(3, len(pc.(map[string]interface{})), fmt.Sprintf("Document %s, Unexpected pc: %v\n", key, pc))
	mv, err = getPathValue(key, "_xdcr.mv", targetBucket)
	assert.NotNil(err)
	assert.Nil(mv)

	/*
	 * Test 5: Update at target, PV should be pruned.
	 */
	_, err = targetBucket.Upsert(key,
		User{Id: "kingarthur",
			Email:     "kingarthur@couchbase.com",
			Interests: []string{"Holy Grail", "African Swallows", "Target item"}}, expire)
	assert.Nil(err)
	err = waitForReplication(key, targetBucket, sourceBucket) // replicate from C2 to C1
	assert.Nil(err)
	err = verifyCv(key, sourceBucket)
	assert.Nil(err)
	_, err = getPathValue(key, base.XATTR_ID_PATH, sourceBucket)
	assert.Nil(err)
	pv, err := getPathValue(key, base.XATTR_PCAS_PATH, sourceBucket)
	assert.Nil(err)
	// PV had 3 items, now 1
	assert.Equal(1, len(pv.(map[string]interface{})), fmt.Sprintf("Unexpected pv=%v\n", pv))

	/*
	 * Test 6: Do an upsert without subdoc flags and check that xattrs are preserved
	 */
	_, err = sourceBucket.Upsert(key,
		User{Id: "kingarthur",
			Email:     "kingarthur@couchbase.com",
			Interests: []string{"Holy Grail", "African Swallows", "New item"}}, expire)
	assert.Nil(err)
	err = waitForReplication(key, sourceBucket, targetBucket)
	assert.Nil(err)
	err = verifyCv(key, targetBucket)
	assert.Nil(err)
	_, err = getPathValue(key, "_xdcr.id", targetBucket)
	assert.Nil(err)
	pv, err = getPathValue(key, base.XATTR_PCAS_PATH, sourceBucket)
	assert.Nil(err)
	assert.Equal(1, len(pv.(map[string]interface{})))

	/*
	 * Test 7. Delete the document and _xdcr is intact
	 */
	_, err = sourceBucket.Remove(key, 0)
	assert.Nil(err)
	err = waitForReplication(key, sourceBucket, targetBucket)
	assert.Nil(err)
	xdcr, err := getPathValue(key, "_xdcr", targetBucket)
	assert.Nil(err)
	assert.Equal(3, len(xdcr.(map[string]interface{})))

	/*
	* Test 7. Recreate the document and old _xdcr is lost, unfortunately
	 */
	_, err = sourceBucket.Upsert(key,
		User{Id: "kingarthur",
			Email:     "kingarthur@couchbase.com",
			Interests: []string{"Holy Grail"}}, expire)
	assert.Nil(err)
	err = waitForReplication(key, sourceBucket, targetBucket)
	assert.Nil(err)
	xdcr, err = getPathValue(key, "_xdcr", targetBucket)
	assert.Nil(err)
	assert.Equal(2, len(xdcr.(map[string]interface{})))
}

func TestCustomCRDeletedDocs(t *testing.T) {
	fmt.Println("============== Test case start: TestCustomCRDeletedDocs =================")
	defer fmt.Println("============== Test case end: TestCustomCRDeletedDocs =================")
	bucketName := "TestCustomCRDeletedDocs"
	assert := assert.New(t)
	sourceBucket, err := createBucket(sourceConnStr, bucketName)
	if err != nil {
		fmt.Printf("TestCustomCRDeletedDocs skipped because source cluster is not ready. Error: %v\n", err)
	}
	targetBucket, err := createBucket(targetConnStr, bucketName)
	if err != nil {
		fmt.Printf("TestCustomCRDeletedDocs skipped because target cluster is not ready. Error: %v\n", err)
		return
	}
	assert.NotNil(sourceBucket)
	assert.NotNil(targetBucket)
	waitForBucketReady(t, targetBucket)
	createReplication(t, bucketName, base.DefaultMergeFunc, true)

	key := time.Now().Format(time.RFC3339)
	var expire uint32 = 24 * 60 * 60
	srcInsCas, err := sourceBucket.Upsert(key,
		User{Id: "kingarthur",
			Email:     "kingarthur@couchbase.com",
			Interests: []string{"Holy Grail", "African Swallows", "Source"}}, expire)
	assert.Nil(err)
	// Target dominate. Nothing should happen when replicating from source to target
	_, err = targetBucket.Upsert(key,
		User{Id: "kingarthur",
			Email:     "kingarthur@couchbase.com",
			Interests: []string{"Holy Grail", "African Swallows", "Target"}}, expire)
	assert.Nil(err)
	_, err = sourceBucket.Remove(key, srcInsCas)
	// The deleted document should be replicated
	err = waitForReplication(key, sourceBucket, targetBucket)
	assert.Nil(err)
	// verify deleted document has the expected XATTR
	xdcr, err := getPathValue(key, "_xdcr", targetBucket)
	assert.Nil(err)
	xdcrMap := xdcr.(map[string]interface{})
	if _, ok := xdcrMap["id"]; !ok {
		fmt.Printf("XATTRS %s does not contain the expected id field", xdcrMap)
		t.FailNow()
	}
	if _, ok := xdcrMap["cv"]; !ok {
		fmt.Printf("XATTRS %s does not contain the expected cv field", xdcrMap)
		t.FailNow()
	}
	err = verifyCv(key, targetBucket)
	assert.Nil(err)
}

func TestCustomCRBinaryDocs(t *testing.T) {
	fmt.Println("============== Test case start: TestCustomCRBinaryDocs =================")
	defer fmt.Println("============== Test case end: TestCustomCRBinaryDocs =================")
	bucketName := "TestCustomCRBinaryDocs"
	assert := assert.New(t)
	sourceBucket, err := createBucket(sourceConnStr, bucketName)
	if err != nil {
		fmt.Printf("TestCustomCRBinaryDocs skipped because source cluster is not ready. Error: %v\n", err)
		return
	}
	targetBucket, err := createBucket(targetConnStr, bucketName)
	if err != nil {
		fmt.Printf("TestCustomCRBinaryDocs skipped because target cluster is not ready. Error: %v\n", err)
		return
	}
	assert.NotNil(sourceBucket)
	assert.NotNil(targetBucket)
	waitForBucketReady(t, targetBucket)
	createReplication(t, bucketName, base.DefaultMergeFunc, true)

	fmt.Println("Test 1. Create target binary doc, create source binary doc. Source wins.")
	key := "sourceAndTargetBinary"
	var expire uint32 = 24 * 60 * 60
	_, err = targetBucket.Upsert(key, "Target document", expire)
	assert.Nil(err)
	_, err = sourceBucket.Upsert(key, fmt.Sprintf("Source document for key %v", key), expire)
	assert.Nil(err)
	err = waitForReplication(key, sourceBucket, targetBucket)
	assert.Nil(err)

	fmt.Println("Test 2. Create target json doc, create source binary doc. Source wins.")
	key = "sourceBinaryTargetJson"
	_, err = targetBucket.Upsert(key,
		User{Id: "kingarthur",
			Email:     "kingarthur@couchbase.com",
			Interests: []string{"Holy Grail", "African Swallows", "target"}}, expire)
	assert.Nil(err)
	_, err = sourceBucket.Upsert(key, fmt.Sprintf("Source document for key %v", key), expire)
	assert.Nil(err)
	err = waitForReplication(key, sourceBucket, targetBucket)
	assert.Nil(err)

	fmt.Println("Test 3. Create target binary doc, create source json doc. Source wins.")
	key = "sourceJsonTargetBinary"
	_, err = targetBucket.Upsert(key, "target document", expire)
	assert.Nil(err)
	_, err = sourceBucket.Upsert(key,
		User{Id: "kingarthur",
			Email:     "kingarthur@couchbase.com",
			Interests: []string{"Holy Grail", "African Swallows", "Source"}}, expire)
	err = waitForReplication(key, sourceBucket, targetBucket)
	assert.Nil(err)
}

func TestCcrXattrAfterMerge(t *testing.T) {
	fmt.Println("============== Test case start: TestCcrXattrAfterMerge =================")
	defer fmt.Println("============== Test case end: TestCcrXattrAfterMerge =================")
	bucketName := "TestCcrXattrAfterMerge"
	assert := assert.New(t)
	sourceBucket, err := createBucket(sourceConnStr, bucketName)
	if err != nil {
		fmt.Printf("TestCcrXattrAfterMerge skipped because source cluster is not ready. Error: %v\n", err)
		return
	}
	targetBucket, err := createBucket(targetConnStr, bucketName)
	if err != nil {
		fmt.Printf("TestCcrXattrAfterMerge skipped because target cluster is not ready. Error: %v\n", err)
		return
	}
	assert.NotNil(sourceBucket)
	assert.NotNil(targetBucket)
	waitForBucketReady(t, targetBucket)
	mergeFunc := "simpleMerge"
	createMergeFunction(t, mergeFunc)
	createReplication(t, bucketName, mergeFunc, true)

	// Create documents at target and then at source to get conflicts
	key := time.Now().Format(time.RFC3339)
	var expire uint32 = 60 * 60 * 24 // expires in 1 day
	numDoc := 100
	var cas1 uint64
	var cas100 uint64
	for i := 0; i < numDoc; i++ {
		cas, err := targetBucket.Upsert(fmt.Sprintf("%v_%v", key, i),
			User{Id: "kingarthur",
				Email:     "kingarthur@couchbase.com",
				Interests: []string{"Holy Grail", "African Swallows", "Target"}}, expire)
		assert.Nil(err)
		if i == 0 {
			cas1 = uint64(cas)
		} else if i == 99 {
			cas100 = uint64(cas)
		}
	}

	fmt.Printf("cas1=%v,cas100=%v,diff=%v, diff2=%v\n", base.CasToTime(cas1), base.CasToTime(cas100), base.CasDuration(cas1, cas100), base.CasDuration(cas100, cas1))
	fmt.Printf("Created %v target documents\n", numDoc)
	cas := make([]gocb.Cas, numDoc)
	for i := 0; i < numDoc; i++ {
		cas[i], err = sourceBucket.Upsert(fmt.Sprintf("%v_%v", key, i),
			User{Id: "kingarthur",
				Email:     "kingarthur@couchbase.com",
				Interests: []string{"Holy Grail", "African Swallows", "Source"}}, expire)
		assert.Nil(err)
	}
	fmt.Printf("Created %v source documents\n", numDoc)
	fmt.Println("Wait for merge to finish")
	lastKey := fmt.Sprintf("%v_%v", key, numDoc-1)
	waitForCasChange(t, lastKey, cas[numDoc-1], sourceBucket)
	fmt.Printf("Verifying merge and replication of merged doc for %v documents\n", numDoc)
	for i := 0; i < numDoc; i++ {
		key_i := fmt.Sprintf("%v_%v", key, i)
		_, err = sourceBucket.LookupIn(key_i).GetEx("_xdcr.mv", gocb.SubdocFlagXattr).Execute()
		assert.Nil(err, "_xdcr.mv lookup failed for key %v", key_i)
		err = verifyCv(key_i, sourceBucket)
		assert.Nil(err)

		err = waitForReplication(key_i, sourceBucket, targetBucket)
		assert.Nil(err)
		err = verifyCv(key_i, targetBucket)
		assert.Nil(err)
	}
}

func TestCcrXattrSetBack(t *testing.T) {
	fmt.Println("============== Test case start: TestCcrXattrSetBack =================")
	defer fmt.Println("============== Test case end: TestCcrXattrSetBack =================")
	assert := assert.New(t)
	bucketName := "CCRSetBack"
	sourceBucket, err := createBucket(sourceConnStr, bucketName)
	if err != nil {
		fmt.Printf("TestCcrXattrSetBack skipped because source cluster is not ready. Error: %v\n", err)
		return
	}
	targetBucket, err := createBucket(targetConnStr, bucketName)
	if err != nil {
		fmt.Printf("TestCcrXattrSetBack skipped because target cluster is not ready. Error: %v\n", err)
		return
	}
	assert.NotNil(sourceBucket)
	assert.NotNil(targetBucket)
	waitForBucketReady(t, targetBucket)
	createReplication(t, bucketName, base.DefaultMergeFunc, true)

	key := time.Now().Format(time.RFC3339) + "_setback"
	var expire uint32 = 60 * 60 * 24 // expires in 1 day

	// Create documents at target that looks like merge from 3 clusters
	_, err = targetBucket.Upsert(key,
		User{Id: "kingarthur",
			Email:     "kingarthur@couchbase.com",
			Interests: []string{"Holy Grail", "African Swallows", "Target"}}, expire)
	assert.Nil(err)

	pcasTarget := make(map[string]string, 3)
	pcasTarget["Cluster1"] = "FhSITdr4AAA"
	pcasTarget["Cluster2"] = "FhSITdr4ABU"
	pcasTarget["Cluster3"] = "FhSITdr4ACA"
	_, err = targetBucket.MutateIn(key, 0, 0).
		UpsertEx("_xdcr.cv", "${Mutation.CAS}", gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath|0x10).
		UpsertEx("_xdcr.id", "SourceCluster", gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath).
		UpsertEx("_xdcr.mv", pcasTarget, gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath).
		Execute()
	assert.Nil(err)
	fmt.Printf("Created target document\n")

	// Create documents at target that looks like merge from 2 clusters
	_, err = sourceBucket.Upsert(key,
		User{Id: "kingarthur",
			Email:     "kingarthur@couchbase.com",
			Interests: []string{"Holy Grail", "African Swallows", "Source"}}, expire)
	assert.Nil(err)
	pcasSource := make(map[string]string, 3)
	pcasSource["Cluster1"] = "FhSITdr4AAA"
	pcasSource["Cluster2"] = "FhSITdr4ABU"
	_, err = sourceBucket.MutateIn(key, 0, 0).
		UpsertEx("_xdcr.cv", "${Mutation.CAS}", gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath|0x10).
		UpsertEx("_xdcr.id", "SourceCluster", gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath).
		UpsertEx("_xdcr.mv", pcasSource, gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath).
		Execute()
	assert.Nil(err)
	fmt.Printf("Created source document\n")
	fmt.Println("Wait for target document set back to source.")
	mv := []byte("{\"Cluster1\":\"FhSITdr4AAA\",\"Cluster2\":\"FhSITdr4ABU\",\"Cluster3\":\"FhSITdr4ACA\"}")
	err = waitForMV(key, mv, sourceBucket)
	assert.Nil(err)
	err = verifyCv(key, sourceBucket)
	assert.Nil(err)

	err = waitForReplication(key, sourceBucket, targetBucket)
	assert.Nil(err)
	err = verifyCv(key, targetBucket)
	assert.Nil(err)

}

// TODO: Error test, create a function with typo, like "function bucketName (a, b) return b}" and make sure we have a way to recover
