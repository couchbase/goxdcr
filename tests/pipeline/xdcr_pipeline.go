package main

import (
	"flag"
	"fmt"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr/pipeline_manager"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/metadata"
	c "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/mock_services"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/parts"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/replication_manager"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/utils"
	"github.com/couchbaselabs/go-couchbase"
	"log"
	"net/http"
	"os"
	"time"
)

import _ "net/http/pprof"

var options struct {
	source_bucket           string // source bucket
	target_bucket           string //target bucket
	source_cluster_addr     string //source connect string
	target_cluster_addr     string //target connect string
	source_cluster_username string //source cluster username
	source_cluster_password string //source cluster password
	target_cluster_username string //target cluster username
	target_cluster_password string //target cluster password
	target_bucket_password  string //target bucket password
	nozzles_per_node_source int    // number of nozzles per source node
	nozzles_per_node_target int    // number of nozzles per target node
	//	username        string //username
	//	password        string //password
	//	maxVbno         int    // maximum number of vbuckets
}

const (
	NUM_SOURCE_CONN = 2
	NUM_TARGET_CONN = 3
)

func argParse() {

	flag.StringVar(&options.source_cluster_addr, "source_cluster_addr", "127.0.0.1:9000",
		"source cluster address")
	flag.StringVar(&options.source_bucket, "source_bucket", "default",
		"bucket to replicate from")
	flag.StringVar(&options.target_cluster_addr, "target_cluster_addr", "127.0.0.1:9000",
		"target cluster address")
	flag.StringVar(&options.target_bucket, "target_bucket", "target",
		"bucket to replicate to")
	flag.StringVar(&options.source_cluster_username, "source_cluster_username", "Administrator",
		"user name to use for logging into source cluster")
	flag.StringVar(&options.source_cluster_password, "source_cluster_password", "welcome",
		"password to use for logging into source cluster")
	flag.StringVar(&options.target_cluster_username, "target_cluster_username", "Administrator",
		"user name to use for logging into target cluster")
	flag.StringVar(&options.target_cluster_password, "target_cluster_password", "welcome",
		"password to use for logging into target cluster")
	flag.StringVar(&options.target_bucket_password, "target_bucket_password", "welcome",
		"password to use for accessing target bucket")
	flag.IntVar(&options.nozzles_per_node_source, "nozzles_per_node_source", NUM_SOURCE_CONN,
		"number of nozzles per source node")
	flag.IntVar(&options.nozzles_per_node_target, "nozzles_per_node_target", NUM_TARGET_CONN,
		"number of nozzles per target node")

	flag.Parse()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS]\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	go func() {
		log.Println("Try to start pprof...")
		err := http.ListenAndServe("localhost:7000", nil)
		if err != nil {
			panic(err)
		} else {
			log.Println("Http server for pprof is started")
		}
	}()

	//	c.SetLogLevel(c.LogLevelTrace)
	fmt.Println("Start Testing ...")
	argParse()
	setup()
	test()
	verify()
}

func setup() {
	flushTargetBkt()
	fmt.Println("Finish setup")
	c.SetTestOptions(options.source_bucket, options.target_bucket, options.source_cluster_addr, options.target_cluster_addr, options.source_cluster_username, options.source_cluster_password, options.nozzles_per_node_source, options.nozzles_per_node_target)
	replication_manager.Initialize(c.NewMockMetadataSvc(), new(c.MockClusterInfoSvc), new(c.MockXDCRTopologySvc), new(c.MockReplicationSettingsSvc))
	return
}

func test() {
	settings := make(map[string]interface{})
	settings[metadata.PipelineLogLevel] = "Info"
	settings[metadata.SourceNozzlePerNode] = 1
	settings[metadata.TargetNozzlePerNode] = 4
	settings[metadata.BatchCount] = 500
	
	topic, err := replication_manager.CreateReplication(options.source_cluster_addr, options.source_bucket, options.target_cluster_addr, options.target_bucket, options.target_bucket_password, settings)
	if err != nil {
		fail(fmt.Sprintf("%v", err))
	}
	time.Sleep(1 * time.Second)
	
	replication_manager.PauseReplication(topic)

	err = replication_manager.SetPipelineLogLevel(topic, "Error")
	if err != nil {
		fail(fmt.Sprintf("%v", err))
	}
	fmt.Printf("Replication %s is paused\n", topic)
	time.Sleep(100 * time.Millisecond)
	err = replication_manager.ResumeReplication(topic)
	if err != nil {
		fail(fmt.Sprintf("%v", err))
	}
	fmt.Printf("Replication %s is resumed\n", topic)
	time.Sleep(2 * time.Second)
	summary(topic)

	err = replication_manager.DeleteReplication(topic)
	if err != nil {
		fail(fmt.Sprintf("%v", err))
	}
	fmt.Printf("Replication %s is deleted\n", topic)
	time.Sleep(3 * time.Minute)

}

func summary(topic string) {
	pipeline := pipeline_manager.Pipeline(topic)
	targetNozzles := pipeline.Targets()
	for _, targetNozzle := range targetNozzles {
		fmt.Println(targetNozzle.(*parts.XmemNozzle).StatusSummary())
	}
}
func fail(msg string) {
	panic(fmt.Sprintf("TEST FAILED - %s", msg))
}

func verify() {
	sourceDocCount := getDocCounts(options.source_cluster_addr, options.source_bucket, "")
	targetDocCount := getDocCounts(options.target_cluster_addr, options.target_bucket, options.target_bucket_password)

	if sourceDocCount == targetDocCount {
		fmt.Println("TEST SUCCESS")
	} else {
		fmt.Printf("TEST FAILED\n")
		fmt.Printf("Source doc count=%d; target doc count=%d\n", sourceDocCount, targetDocCount)
	}
}

func getDocCounts(clusterAddress string, bucketName string, password string) int {
	output := &utils.CouchBucket{}
	baseURL, err := couchbase.ParseURL("http://" + clusterAddress)

	if err == nil {
		err = utils.QueryRestAPI(baseURL,
			"/pools/default/buckets/"+bucketName,
			bucketName,
			password,
			"GET",
			output, nil)
	}
	if err != nil {
		panic(err)
	}
	log.Printf("name=%s itemCount=%d\n, out=%v\n", output.Name, output.Stat.ItemCount, output)
	return output.Stat.ItemCount

}

func flushTargetBkt() {
	//flush the target bucket
	baseURL, err := couchbase.ParseURL("http://" + options.target_bucket + ":" + options.target_bucket_password + "@" + options.target_cluster_addr)

	if err == nil {
		err = utils.QueryRestAPI(baseURL,
			"/pools/default/buckets/"+options.target_bucket+"/controller/doFlush",
			options.target_cluster_username,
			options.target_cluster_password,
			"POST",
			nil, nil)
	}

	if err != nil {
		log.Printf("Setup error=%v\n", err)
	} else {
		log.Println("Setup is done")
	}

}
