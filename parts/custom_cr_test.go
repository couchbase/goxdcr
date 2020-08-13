package parts

import (
	"bytes"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/stretchr/testify/assert"
	gocb "gopkg.in/couchbase/gocb.v1"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
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
const urlCreateReplication = "http://127.0.0.1:9000/controller/createReplication"

func createBucket(connStr, bucketName string) (bucket *gocb.Bucket, err error) {
	cluster, err := gocb.Connect(connStr)
	if err != nil {
		return
	}
	cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: username,
		Password: password,
	})
	// Create a bucket if it doesn't exist
	cm := cluster.Manager(username, password)
	bucketSettings := gocb.BucketSettings{false, false, bucketName, "", 100, 0, gocb.Couchbase}
	_ = cm.InsertBucket(&bucketSettings)

	bucket, err = cluster.OpenBucket(bucketName, "")
	for i := 0; i < 5 && err != nil; i++ {
		time.Sleep(1 * time.Second)
		bucket, err = cluster.OpenBucket(bucketName, "")
	}
	fmt.Printf("Created bucket %v\n", bucketName)
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
func createReplication(t *testing.T, bucketName string) {
	assert := assert.New(t)
	client := &http.Client{}
	data := url.Values{}
	data.Set("fromBucket", bucketName)
	data.Add("toCluster", targetCluster)
	data.Add("toBucket", bucketName)
	data.Add("replicationType", "continuous")
	req, err := http.NewRequest("POST", urlCreateReplication, bytes.NewBufferString(data.Encode()))
	assert.Nil(err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; param=value")
	i := 0
	var bodyBytes []byte
	for ; i <= 5; i++ {
		req.SetBasicAuth(username, password)
		resp, err := client.Do(req)
		assert.Nil(err)
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		assert.Nil(err)
		resp.Body.Close()
		created := !strings.Contains(string(bodyBytes), "error") || strings.Contains(string(bodyBytes), "already exists")
		if created {
			fmt.Printf("Created replication for bucket %v\n", bucketName)
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
	//var value interface{}

	sourceDoc, err := source.LookupInEx(key, gocb.SubdocDocFlagAccessDeleted).GetEx("_xdcr", gocb.SubdocFlagXattr).Execute()
	if sourceDoc == nil {
		fmt.Printf("source lookup failed for key")
		return
	}
	targetDoc, err := target.LookupInEx(key, gocb.SubdocDocFlagAccessDeleted).GetEx("_xdcr", gocb.SubdocFlagXattr).Execute()

	var i int
	for i = 0; i < 60 && (targetDoc == nil || sourceDoc.Cas() != targetDoc.Cas()); i++ {
		time.Sleep(1 * time.Second)
		targetDoc, err = target.LookupInEx(key, gocb.SubdocDocFlagAccessDeleted).GetEx("_xdcr", gocb.SubdocFlagXattr).Execute()
	}
	if sourceDoc.Cas() != targetDoc.Cas() {
		return fmt.Errorf("Document '%s' with cas %v has not replicated to target after %v seconds. Target cas %v\n", key, sourceDoc.Cas(), i, targetDoc.Cas())
	}
	return nil
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
	frag, err := target.LookupIn(key).GetEx(path, gocb.SubdocFlagXattr).Execute()
	if err != nil {
		return
	}
	err = frag.Content(path, &value)
	return value, err
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
	createReplication(t, bucketName)

	/*
	 * Test 1: New doc at source. Expect to format _xdcr at target with cv and id.
	 */
	key := time.Now().String()
	var expire uint32 = 60 * 60 * 24 // expires in 1 day
	sourceBucket.Upsert(key,
		User{Id: "kingarthur",
			Email:     "kingarthur@couchbase.com",
			Interests: []string{"Holy Grail", "African Swallows"}}, expire)
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
	_, err = getPathValue(key, "_xdcr.pc", targetBucket)
	assert.Nil(err)
	assert.Nil(err)
	_, err = getPathValue(key, "_xdcr.mv", targetBucket)
	assert.NotNil(err)
}

func TestCustomCRDeletedDocs(t *testing.T) {
	fmt.Println("============== Test case start: TestCustomCRDeletedDocs =================")
	defer fmt.Println("============== Test case end: TestCustomCRDeletedDocs =================")
	bucketName := "TestCustomCRDeletedDocs"
	assert := assert.New(t)
	sourceBucket, err := createBucket(sourceConnStr, bucketName)
	if err != nil {
		fmt.Printf("TestCustomCRDeletedDocs skipped because source cluster is not ready. Error: %v\n", err)
		return
	}
	targetBucket, err := createBucket(targetConnStr, bucketName)
	if err != nil {
		fmt.Printf("TestCustomCRDeletedDocs skipped because target cluster is not ready. Error: %v\n", err)
		return
	}
	assert.NotNil(sourceBucket)
	assert.NotNil(targetBucket)
	waitForBucketReady(t, targetBucket)
	createReplication(t, bucketName)
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
	targetDoc, err := targetBucket.LookupInEx(key, gocb.SubdocDocFlagAccessDeleted).GetEx("_xdcr", gocb.SubdocFlagXattr).Execute()
	assert.True(targetDoc.Exists("id"))
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
	createReplication(t, bucketName)

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
