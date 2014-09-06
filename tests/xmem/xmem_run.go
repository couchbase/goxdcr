// Tool receives raw events from go-couchbase UPR client.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	parts "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/parts"
	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/indexing/secondary/common"
	"github.com/couchbaselabs/go-couchbase"
	"log"
	"os"
	"sync"
	"time"
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
var xmem *parts.XmemNozzle = nil

func argParse() {

	flag.StringVar(&options.source_bucket, "source_bucket", "default",
		"bucket to replicate from")
	flag.IntVar(&options.maxVbno, "maxvb", 1024,
		"maximum number of vbuckets")
	flag.StringVar(&options.target_bucket, "target_bucket", "target",
		"bucket to replicate to")
	flag.StringVar(&options.username, "username", "",
		"username")
	flag.StringVar(&options.password, "password", "",
		"password")

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
	fmt.Println("Start Testing Xmem...")
	argParse()
	fmt.Printf("connectStr=%s, username=%s, password=%s\n", options.connectStr, options.username, options.password)
	fmt.Println("Done with parsing the arguments")
	startXmem()
	fmt.Println("XMEM is started")
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

		//transfer UprEvent to MCRequest
		mcReq := composeMCRequest(e)
		count++
		fmt.Printf("Number of upr event received so far is %d", count)
		xmem.Receive(mcReq)

		if count > 100 {
			goto Done
		}

	}
Done:
	//close the upr stream
	uprFeed.Close()
	xmem.Stop()
	waitGrp.Done()
}

func composeMCRequest(event *mcc.UprEvent) *mc.MCRequest {
	req := &mc.MCRequest{Cas: event.Cas,
		Opaque:  0,
		VBucket: event.VBucket,
		Key:     event.Key,
		Body:    event.Value,
		Extras:	 make ([]byte, 224)}
	//opCode
	switch event.Opcode {
	case mcc.UprStreamRequest:
		req.Opcode = mc.UPR_STREAMREQ
	case mcc.UprMutation:
		req.Opcode = mc.UPR_MUTATION
	case mcc.UprDeletion:
		req.Opcode = mc.UPR_DELETION
	case mcc.UprExpiration:
		req.Opcode = mc.UPR_EXPIRATION
	case mcc.UprCloseStream:
		req.Opcode = mc.UPR_CLOSESTREAM
	case mcc.UprSnapshot:
		req.Opcode = mc.UPR_SNAPSHOT
	case mcc.UprFlush:
		req.Opcode = mc.UPR_FLUSH
	}

	//extra
	if event.Opcode == mcc.UprMutation || event.Opcode == mcc.UprDeletion ||
		event.Opcode == mcc.UprExpiration {
		binary.BigEndian.PutUint64(req.Extras, event.Seqno)
		binary.BigEndian.PutUint32(req.Extras, event.Flags)
		binary.BigEndian.PutUint32(req.Extras, event.Expiry)
	} else if event.Opcode == mcc.UprSnapshot {
		fmt.Printf("event.Seqno=%v\n", event.Seqno)
		binary.BigEndian.PutUint64(req.Extras, event.Seqno)
		binary.BigEndian.PutUint64(req.Extras, event.SnapstartSeq)
		binary.BigEndian.PutUint64(req.Extras, event.SnapendSeq)
		binary.BigEndian.PutUint32(req.Extras, event.SnapshotType)
	}

	return req
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

func startXmem() {

	xmem = parts.NewXmemNozzle("xmem")
	var configs map[string]interface{} = map[string]interface{}{parts.XMEM_SETTING_BATCHCOUNT: 10,
		parts.XMEM_SETTING_TIMEOUT:    time.Millisecond * 10,
		parts.XMEM_SETTING_NUMOFRETRY: 3,
		parts.XMEM_SETTING_MODE:       parts.Batch_XMEM,
		parts.XMEM_SETTING_CONNECTSTR: options.connectStr,
		parts.XMEM_SETTING_BUCKETNAME: options.target_bucket,
		parts.XMEM_SETTING_USERNAME:   options.username,
		parts.XMEM_SETTING_PASSWORD:   options.password}

	xmem.Start(configs)
}
