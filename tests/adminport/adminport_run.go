// Test for KVFeed, source nozzle in XDCR
package main

import (
	"flag"
	"fmt"
	"os"
	"time"
	"net/http"
	"bytes"
	"errors"
	"net/url"
	ap "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/adminport"
	base "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/base"
	rm "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/replication_manager"
	c "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/mock_services"
	pm "github.com/Xiaomei-Zhang/couchbase_goxdcr/pipeline_manager"
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
	filterName      string //filter name
	numConnPerKV    int    // number of connections per source KV node
	numOutgoingConn int    // number of connections to target cluster
	username        string //username
	password        string //password
	maxVbno         int    // maximum number of vbuckets
	kvaddr    string // kv addr. may not need
}

func argParse() {
	flag.StringVar(&options.sourceBucket, "sourceBucket", "default",
		"bucket to replicate from")
	flag.IntVar(&options.maxVbno, "maxvb", 8,
		"maximum number of vbuckets")
	flag.StringVar(&options.targetBucket, "targetBucket", "target",
		"bucket to replicate to")
	flag.StringVar(&options.filterName, "filterName", "myActive",
		"name of filter to use for replication")
	flag.IntVar(&options.numConnPerKV, "numConnPerKV", NumSourceConn,
		"number of connections per kv node")
	flag.IntVar(&options.numOutgoingConn, "numOutgoingConn", NumTargetConn,
		"number of outgoing connections to target")
	flag.StringVar(&options.username, "username", "Administrator", "username to cluster admin console")
	flag.StringVar(&options.password, "password", "welcome", "password to Cluster admin console")
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
	c.SetTestOptions(options.sourceBucket, options.targetBucket, options.connectStr, options.connectStr, options.username, options.password, options.numConnPerKV, options.numOutgoingConn)
	rm.Initialize(c.NewMockMetadataSvc(), new(c.MockClusterInfoSvc), new(c.MockXDCRTopologySvc), new(c.MockReplicationSettingsSvc))

	go ap.MainAdminPort(options.kvaddr)
	//wait for server to finish starting
	time.Sleep(time.Second * 3)

	replicationId, err := testCreateReplication()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	
	escapedReplId := url.QueryEscape(replicationId)
	fmt.Println("replicationId: ", replicationId, " escaped replication id: " + escapedReplId)

	if err := testViewReplicationSettings(escapedReplId); err != nil {
		fmt.Println(err.Error())
		return
	}
	
	if err := testChangeReplicationSettings(escapedReplId); err != nil {
		fmt.Println(err.Error())
		return
	}
	
	if err := testPauseReplication(replicationId, escapedReplId); err != nil {
		fmt.Println(err.Error())
		return
	}
	
	if err := testResumeReplication(replicationId, escapedReplId); err != nil {
		fmt.Println(err.Error())
		return
	}
	
	if err := testGetStatistics(); err != nil {
		fmt.Println(err.Error())
		return
	}
	
	if err := testDeleteReplication(replicationId, escapedReplId); err != nil {
		fmt.Println(err.Error())
		return
	}
	
	fmt.Println("All tests passed.")

}

func getUrlPrefix() string {
	return "http://" + ap.GetHostAddr(options.kvaddr, base.AdminportNumber) + base.AdminportUrlPrefix
}

func testCreateReplication() (string, error) {
	url := getUrlPrefix() + ap.CreateReplicationPath
	
	params := make(map[string]interface{})
	params[ap.FromBucket] = options.sourceBucket
	params[ap.ToClusterUuid] = options.connectStr
	params[ap.ToBucket] = options.targetBucket
	params[ap.FilterName] = options.filterName
	params[ap.FilterExpression] = FilterExpression
	params[ap.BatchCount] = BatchCount
		
	paramsBytes, _ := ap.EncodeMapIntoByteArray(params)
	paramsBuf := bytes.NewBuffer(paramsBytes)
	
	request, err := http.NewRequest(ap.MethodPost, url, paramsBuf)
	if err != nil {
		return "", err
	}
	request.Header.Set(ap.ContentType, DefaultContentType)
	
	fmt.Println("request", request)
	
	response, err := http.DefaultClient.Do(request)
	
	err = validateResponse("CreateReplication", response, err)
	if err != nil {
		return "", err
	}
	
	replicationId, err := ap.DecodeCreateReplicationResponse(response)
	
	// verify that the replication is created and started and is being
	// managed by pipeline manager
	return replicationId, validatePipeline("CreateReplication", replicationId, true)
}

func testPauseReplication(replicationId, escapedReplId string) error {
	url := getUrlPrefix() + ap.PauseReplicationPrefix + ap.UrlDelimiter + escapedReplId
	
	request, err := http.NewRequest(ap.MethodPost, url, nil)
	if err != nil {
		return err
	}
	request.Header.Set(ap.ContentType, DefaultContentType)
	
	fmt.Println("request", request)
	
	response, err := http.DefaultClient.Do(request)
	
	err = validateResponse("PauseReplication", response, err)
	if err != nil {
		return err
	}
	
	return validatePipeline("PauseReplication", replicationId, false)
}

func testResumeReplication(replicationId, escapedReplId string) error {
	url := getUrlPrefix() + ap.ResumeReplicationPrefix + ap.UrlDelimiter + escapedReplId
	
	request, err := http.NewRequest(ap.MethodPost, url, nil)
	if err != nil {
		return err
	}
	request.Header.Set(ap.ContentType, DefaultContentType)
	
	fmt.Println("request", request)
	
	response, err := http.DefaultClient.Do(request)
	
	err = validateResponse("ResumeReplication", response, err)
	if err != nil {
		return err
	}
	
	return validatePipeline("ResumeReplication", replicationId, true)
}

func testDeleteReplication(replicationId, escapedReplId string) error {
	url := getUrlPrefix() + ap.DeleteReplicationPrefix + ap.UrlDelimiter + escapedReplId
	
	request, err := http.NewRequest(ap.MethodPost, url, nil)
	if err != nil {
		return err
	}
	request.Header.Set(ap.ContentType, DefaultContentType)
	
	fmt.Println("request", request)
	
	response, err := http.DefaultClient.Do(request)
	
	err = validateResponse("DeleteReplication", response, err)
	if err != nil {
		return err
	}
	
	return validatePipeline("DeleteReplication", replicationId, false)
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
	
	return validateResponse("ViewReplicationSettings", response, err)
}

func testChangeReplicationSettings(replicationId string) error {
	url := getUrlPrefix() + ap.SettingsReplicationsPath + ap.UrlDelimiter + replicationId
	
	params := make(map[string]interface{})
	params[ap.Active] = Active
	params[ap.BatchSize] = BatchSize
		
	paramsBytes, _ := ap.EncodeMapIntoByteArray(params)
	paramsBuf := bytes.NewBuffer(paramsBytes)
	
	request, err := http.NewRequest(ap.MethodPost, url, paramsBuf)
	if err != nil {
		return err
	}
	request.Header.Set(ap.ContentType, DefaultContentType)
	
	fmt.Println("request", request)
	
	response, err := http.DefaultClient.Do(request)
	return validateResponse("ChangeReplicationSettings", response, err)
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
	
	return validateResponse("GetStatistics", response, err)
}


func validateResponse(testName string, response *http.Response, err error) error {
	if err != nil || response.StatusCode != 200 {
		return errors.New(fmt.Sprintf("Test %v failed. err=%v; response status=%v\n", testName, err, response.Status))
	} 
	return nil
}

func validatePipeline(testName string, replicationId string, pipelineRunning bool) error {
	if (pm.Pipeline(replicationId) == nil) == pipelineRunning {
		var errMsg string
		if pipelineRunning {
			errMsg = ", should be running but was not"
		} else {
			errMsg = ", should not be running but was"
		}
		return errors.New(fmt.Sprintf("Test %v failed. Pipeline, %v%v\n", testName, replicationId, errMsg))
	} else{
		fmt.Println("Test ", testName, " passed.")
		return nil
	}
}





