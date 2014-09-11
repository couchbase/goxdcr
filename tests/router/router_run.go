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
	"math"
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
	
	partMap := make(map[string]pc.Part)
	for i := 0; i < NUM_PARTS; i++ {
		partId := PART_ID_PREFIX + strconv.FormatInt(int64(i), 10)
		partMap[partId] = NewTestPart(partId)
	}
	
	router, _ = parts.NewRouter(partMap, buildVbMap(partMap))
}

func buildVbMap(downStreamParts map[string]pc.Part) (map[uint16]string) {
	vbMap := make(map[uint16]string)

	numOfNodes := len(downStreamParts)
	
	numOfVbPerNode := uint16(math.Ceil(float64(options.maxVbno)/float64(numOfNodes)))
	
	var indexOfNode uint16
	for partId := range downStreamParts {
	    var j uint16
		for j = 0; j < numOfVbPerNode; j++ {
			vbno := indexOfNode * numOfVbPerNode + j
			if vbno < uint16(options.maxVbno) {
				vbMap[vbno] = partId
			} else {
				// no more vbs to process 
				break
			}
		}
		indexOfNode ++
	}
	return vbMap
	
}


type TestPart struct {
	part.AbstractPart
}

func NewTestPart(id string) *TestPart {
	tp := new(TestPart)
	var isStarted_callback_func part.IsStarted_Callback_Func = tp.IsStarted
	tp.AbstractPart = part.NewAbstractPart(id, &isStarted_callback_func)
	return tp
}

func (tp *TestPart) Start (settings map[string]interface{} ) error {
	return nil
}

func (tp *TestPart) Stop () error {
	return nil
}

func (tp *TestPart) Receive (data interface {}) error {
	fmt.Println("Part %v received data with vbno %v", tp.Id(), data.(*mc.MCRequest).VBucket)

	return nil
}

func (tp *TestPart) IsStarted () bool {
	return false
}