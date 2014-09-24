// Test for KVFeed, source nozzle in XDCR
package main

import (
	"flag"
	"fmt"
	connector "github.com/Xiaomei-Zhang/couchbase_goxdcr/connector"
	mcc "github.com/couchbase/gomemcached/client"
	protobuf "github.com/couchbase/indexing/secondary/protobuf"
	"github.com/ysui6888/indexing/secondary/common"
	sp "github.com/ysui6888/indexing/secondary/projector"
	"log"
	"os"
	"time"
)

var options struct {
	source_bucket string // source bucket
	connectStr    string //connect string
	kvaddr        string //kvaddr
	maxVbno       int    // maximum number of vbuckets
}

const (
	NUM_DATA = 30 // number of data points to collect before test ends
)

var count int

func argParse() {

	flag.StringVar(&options.source_bucket, "source_bucket", "default",
		"bucket to replicate from")
	flag.IntVar(&options.maxVbno, "maxvb", 8,
		"maximum number of vbuckets")
	flag.StringVar(&options.kvaddr, "kvaddr", "127.0.0.1:12000",
		"kv server address")

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
	fmt.Println("Start Testing Router...")
	argParse()
	fmt.Printf("connectStr=%s\n", options.connectStr)
	fmt.Println("Done with parsing the arguments")
	startKVFeed(options.connectStr, options.kvaddr, options.source_bucket)

}

func mf(err error, msg string) {
	if err != nil {
		log.Fatalf("%v: %v", msg, err)
	}
}

func startKVFeed(cluster, kvaddr, bucketn string) {
	b, err := common.ConnectBucket(cluster, "default", bucketn)
	mf(err, "bucket")

	kvfeed, err := sp.NewKVFeed(kvaddr, "test", "", b, nil, 0)
	kvfeed.SetConnector(NewTestConnector())
	kvfeed.Start(sp.ConstructStartSettingsForKVFeed(constructTimestamp(bucketn)))
	fmt.Println("KVFeed is started")

	timeChan := time.NewTimer(time.Second * 20).C
loop:
	for {
		select {
		case <-timeChan:
			fmt.Println("Timer expired")
			break loop
		default:
			if count >= NUM_DATA {
				break loop
			}
		}
	}
	kvfeed.Stop()
	fmt.Println("KVFeed is stopped")

	if count < NUM_DATA {
		fmt.Printf("Test failed. Only %v data was received before timer expired.\n", count)
	} else {
		fmt.Println("Test passed. All test data was received as expected before timer expired.")
	}
}

func constructTimestamp(bucketn string) *protobuf.TsVbuuid {
	ts := protobuf.NewTsVbuuid(bucketn, options.maxVbno)
	for i := 0; i < options.maxVbno; i++ {
		ts.Append(uint16(i), 0, 0, 0, 0)
	}
	return ts
}

type TestConnector struct {
	*connector.SimpleConnector
}

func NewTestConnector() *TestConnector {
	tc := new(TestConnector)
	tc.SimpleConnector = new(connector.SimpleConnector)
	return tc
}

func (tc *TestConnector) Forward(data interface{}) error {
	uprEvent := data.(*mcc.UprEvent)
	count++
	fmt.Printf("received %vth upr event with opcode %v and vbno %v\n", count, uprEvent.Opcode, uprEvent.VBucket)
	return nil
}
