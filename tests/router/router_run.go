// Tool receives raw events from go-couchbase UPR client.
package main

import (
	"flag"
	"fmt"
	parts "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/parts"
	part "github.com/Xiaomei-Zhang/couchbase_goxdcr/part"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbaselabs/go-couchbase"
	"log"
	"os"
	"sync"
	"strconv"
	pc "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	mc "github.com/couchbase/gomemcached"
)

var options struct {
	source_bucket string // source bucket
	target_bucket string //target bucket
	connectStr    string //connect string
	username      string //username
	password      string //password
	maxVbno       int    // maximum number of vbuckets
}

var done = make(chan bool, 16)
var rch = make(chan []interface{}, 10000)
var uprFeed *couchbase.UprFeed = nil
var router *parts.Router = nil

const (
    // total number of parts to route data to
	NUM_PARTS = 3
	// total number of data points to be routed
	NUM_DATA = 100 
	// prefix for part id
	PART_ID_PREFIX = "part"
)


func argParse() {

	flag.StringVar(&options.source_bucket, "source_bucket", "default",
		"bucket to replicate from")
	flag.IntVar(&options.maxVbno, "maxvb", 8,
		"maximum number of vbuckets")
	flag.StringVar(&options.target_bucket, "target_bucket", "target",
		"bucket to replicate to")

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
	startRouter()
	fmt.Println("Router is started")
	waitGrp := &sync.WaitGroup{}
	waitGrp.Add(1)
	go startUpr(options.connectStr, options.source_bucket, waitGrp)
	waitGrp.Wait()
}

func startUpr(cluster, bucketn string, waitGrp *sync.WaitGroup) {
	b, err := common.ConnectBucket(cluster, "default", bucketn)
	mf(err, "bucket")

	uprFeed, err = b.StartUprFeed("rawupr", uint32(0))
	mf(err, "- upr")

	flogs := failoverLogs(b)
	fmt.Print("Got failover log successfully")

	// list of vbuckets
	vbnos := make([]uint16, 0, options.maxVbno)
	for i := 0; i < options.maxVbno; i++ {
		vbnos = append(vbnos, uint16(i))
	}

	startStream(uprFeed, flogs)
	fmt.Print("Upr stream is started")

	count := 0
	for {
		e, ok := <-uprFeed.C
		if ok == false {
			fmt.Println("Closing for bucket", b.Name)
		}

		// let router process the stream
		count++
		fmt.Println("Number of upr event received so far is %d", count)
		err := router.Forward(e)
		mf(err, " - route")
		

		if count >= NUM_DATA {
			break
		}

	}

	//close the upr stream
	uprFeed.Close()
	waitGrp.Done()
}

func startStream(uprFeed *couchbase.UprFeed, flogs couchbase.FailoverLog) {
	start, end := uint64(0), uint64(0xFFFFFFFFFFFFFFFF)
	snapStart, snapEnd := uint64(0), uint64(0)
	for vbno, flog := range flogs {
		x := flog[len(flog)-1] // map[uint16][][2]uint64
		flags, vbuuid := uint32(0), x[0]
		err := uprFeed.UprRequestStream(
			vbno, flags, vbuuid, start, end, snapStart, snapEnd)
		mf(err, fmt.Sprintf("stream-req for %v failed", vbno))
	}
}

func failoverLogs(b *couchbase.Bucket) couchbase.FailoverLog {
	// list of vbuckets
	vbnos := make([]uint16, 0, options.maxVbno)
	for i := 0; i < options.maxVbno; i++ {
		vbnos = append(vbnos, uint16(i))
	}

	flogs, err := b.GetFailoverLogs(vbnos)
	mf(err, "- upr failoverlogs")
	return flogs
}

func mf(err error, msg string) {
	if err != nil {
		log.Fatalf("%v: %v", msg, err)
	}
}

func startRouter() {
	
	partMaps := make(map[string]pc.Part)
	for i := 0; i < NUM_PARTS; i++ {
		partId := PART_ID_PREFIX + strconv.FormatInt(int64(i), 10)
		partMaps[partId] = NewFakePart(partId)
	}
	
	router, _ = parts.NewRouter(partMaps, uint16(options.maxVbno))
}

type FakePart struct {
	part.AbstractPart
}

func NewFakePart(id string) *FakePart {
	fp := new(FakePart)
	var isStarted_callback_func part.IsStarted_Callback_Func = fp.IsStarted
	fp.AbstractPart = part.NewAbstractPart(id, &isStarted_callback_func)
	return fp
}

func (fp *FakePart) Start (settings map[string]interface{} ) error {
	return nil
}

func (fp *FakePart) Stop () error {
	return nil
}

func (fp *FakePart) Receive (data interface {}) error {
	fmt.Println("Part %v received data with vbno %v", fp.Id(), data.(*mc.MCRequest).VBucket)

	return nil
}

func (fp *FakePart) IsStarted () bool {
	return false
}