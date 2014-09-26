// Test for KVFeed, source nozzle in XDCR
package main

import (
	"flag"
	"fmt"
	"os"
	"time"
	"net/http"
	"bytes"
	"strconv"
	ap "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/adminport"
	base "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/base"
	rm "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/replication_manager"
	c "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/tests/common"
)

const (
	DefaultContentType = "application/x-www-form-urlencoded"

	TestTopic      = "test"
	NumSourceConn = 2
	NumTargetConn = 3
	FilterExpression = "testExpr"
	BatchCount = 20
	BatchSize = 30
	Active = false
)

var options struct {
	sourceBucket    string // source bucket
	targetBucket    string //target bucket
	connectStr      string //connect string
	numConnPerKV    int    // number of connections per source KV node
	numOutgoingConn int    // number of connections to target cluster
	username        string //username
	password        string //password
	maxVbno         int    // maximum number of vbuckets
	kvaddr    string // kv addr. may not need
}

func argParse() {
	flag.StringVar(&options.sourceBucket, "source_bucket", "default",
		"bucket to replicate from")
	flag.IntVar(&options.maxVbno, "maxvb", 8,
		"maximum number of vbuckets")
	flag.StringVar(&options.targetBucket, "target_bucket", "default",
		"bucket to replicate to")
	flag.IntVar(&options.numConnPerKV, "numConnPerKV", NumSourceConn,
		"number of connections per kv node")
	flag.IntVar(&options.numOutgoingConn, "numOutgoingConn", NumTargetConn,
		"number of outgoing connections to target")
	flag.StringVar(&options.username, "username", "Admin", "username to cluster admin console")
	flag.StringVar(&options.password, "password", "justam00", "password to Cluster admin console")
	flag.StringVar(&options.kvaddr, "kvaddr", "localhost", "kv address")

	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		usage()
		os.Exit(1)
	}
	options.connectStr = args[0]
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] <cluster-addr> \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	fmt.Println("Start Testing adminport...")
	argParse()
	fmt.Printf("kvaddr=%s\n", options.kvaddr)
	fmt.Println("Done with parsing the arguments")
	startAdminport()
}

func startAdminport() {
	c.SetTestOptions(options.sourceBucket, options.targetBucket, options.connectStr, options.username, options.password, options.numConnPerKV, options.numOutgoingConn)
	rm.Initialize(new(c.MockMetadataSvc), new(c.MockClusterInfoSvc), new(c.MockXDCRTopologySvc), new(c.MockReplicationSettingsSvc))

	go ap.MainAdminPort(options.kvaddr)
	//wait for server to start
	time.Sleep(time.Second * 3)

	// this one is failing now since replication is not yet running end to end	
	testCreateReplication()

	// TODO get replicationId from testCreateReplication()
	testViewReplicationSettings("test")
	
	testChangeReplicationSettings("test")
	
	testGetStatistics()
		
	testDeleteReplication("test")

}

func getUrlPrefix() string {
	return "http://" + ap.GetHostAddr(options.kvaddr, base.AdminportNumber) + base.AdminportUrlPrefix
}

func testCreateReplication() error {
	url := getUrlPrefix() + ap.CreateReplicationPath
	
	params := make(map[string]string)
	params[ap.FromBucket] = options.sourceBucket
	params[ap.ToCluster] = options.connectStr
	params[ap.ToBucket] = options.targetBucket
	params[ap.FilterExpression] = FilterExpression
	params[ap.BatchCount] = strconv.FormatInt(int64(BatchCount), base.ParseIntBase)
		
	paramsBytes := constructRequestBodyFromParams(params)
	paramsBuf := bytes.NewBuffer(paramsBytes)
	
	request, err := http.NewRequest(ap.MethodPost, url, paramsBuf)
	if err != nil {
		return err
	}
	request.Header.Set(ap.ContentType, DefaultContentType)
	
	fmt.Println("request", request)
	
	response, err := http.DefaultClient.Do(request)
	
	validateResponse("CreateReplication", response, err)
	
	return err
}

func testDeleteReplication(replicationId string) error {
	url := getUrlPrefix() + ap.DeleteReplicationPrefix + ap.UrlDelimiter + replicationId
	
	request, err := http.NewRequest(ap.MethodPost, url, nil)
	if err != nil {
		return err
	}
	request.Header.Set(ap.ContentType, DefaultContentType)
	
	fmt.Println("request", request)
	
	response, err := http.DefaultClient.Do(request)
	
	validateResponse("DeleteReplication", response, err)
	
	return err
}

func testViewReplicationSettings(replicationId string) error {
	url := getUrlPrefix() + ap.SettingsReplicationsPath + ap.UrlDelimiter + replicationId
	
	request, err := http.NewRequest(ap.MethodGet, url, nil)
	if err != nil {
		return err
	}
	request.Header.Set(ap.ContentType, DefaultContentType)
	
	fmt.Println("request", request)
	
	response, err := http.DefaultClient.Do(request)
	
	validateResponse("ViewReplicationSettings", response, err)
	
	return err
}

func testChangeReplicationSettings(replicationId string) error {
	url := getUrlPrefix() + ap.SettingsReplicationsPath + ap.UrlDelimiter + replicationId
	
	params := make(map[string]string)
	params[ap.Active] = strconv.FormatBool(Active)
	params[ap.BatchSize] = strconv.FormatInt(int64(BatchSize), base.ParseIntBase)
		
	paramsBytes := constructRequestBodyFromParams(params)
	paramsBuf := bytes.NewBuffer(paramsBytes)
	
	request, err := http.NewRequest(ap.MethodPost, url, paramsBuf)
	if err != nil {
		return err
	}
	request.Header.Set(ap.ContentType, DefaultContentType)
	
	fmt.Println("request", request)
	
	response, err := http.DefaultClient.Do(request)
	
	validateResponse("ChangeReplicationSettings", response, err)
	
	return err
}

func testGetStatistics() error {
	url := getUrlPrefix() + ap.StatisticsPath
	
	request, err := http.NewRequest(ap.MethodGet, url, nil)
	if err != nil {
		return err
	}
	request.Header.Set(ap.ContentType, DefaultContentType)
	
	fmt.Println("request", request)
	
	response, err := http.DefaultClient.Do(request)
	
	validateResponse("GetStatistics", response, err)
	
	return err
}

func constructRequestBodyFromParams(params map[string]string) []byte{
	var result string
	for key, val := range params {
		result = result + key + ap.KeyValueDelimiter + val + ap.ValuesDelimiter		
	}
	if len(result) > 0 {
		result = result[:len(result)-len(ap.ValuesDelimiter)]
	}
	return []byte (result)
}

func validateResponse(testName string, response *http.Response, err error) {
	if err != nil || response.StatusCode != 200 {
		fmt.Println("!!!", testName, " failed. err : ", err, " response status: ", response.Status)
	} else{
		fmt.Println("!!!", testName, " succeeded.")
	}
}




