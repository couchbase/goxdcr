// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

// test for xdcr router
package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/couchbase/go-couchbase"
	mc "github.com/couchbase/gomemcached"
	"github.com/couchbase/goxdcr/v8/base"
	pc "github.com/couchbase/goxdcr/v8/common"
	couchlog "github.com/couchbase/goxdcr/v8/log"
	parts "github.com/couchbase/goxdcr/v8/parts"
	utils "github.com/couchbase/goxdcr/v8/utils"
	"log"
	"math"
	"os"
	"regexp"
	"strconv"
	"sync"
)

var options struct {
	source_bucket     string // source bucket
	target_bucket     string //target bucket
	connectStr        string //connect string
	username          string //username
	password          string //password
	maxVbno           int    // maximum number of vbuckets
	filter_expression string //filter expression
}

var done = make(chan bool, 16)
var rch = make(chan []interface{}, 10000)
var uprFeed *couchbase.UprFeed = nil
var router *parts.Router = nil
var filterRegExp *regexp.Regexp
var routedCount int

const (
	// total number of parts to route data to
	NumParts = 3
	// total number of data points to be routed
	NumData = 100
	// prefix for part id
	PartIdPrefix = "part"
)

func argParse() {
	flag.StringVar(&options.connectStr, "connectStr", "127.0.0.1:9000",
		"connection string to source cluster")
	flag.StringVar(&options.source_bucket, "source_bucket", "default",
		"bucket to replicate from")
	flag.IntVar(&options.maxVbno, "maxvb", 8,
		"maximum number of vbuckets")
	flag.StringVar(&options.target_bucket, "target_bucket", "target",
		"bucket to replicate to")
	// example filter_expression to use: "default-1-1.*"
	flag.StringVar(&options.filter_expression, "filter_expression", "",
		"filter expression")

	flag.Parse()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	fmt.Println("Start Testing Router...")
	argParse()
	fmt.Printf("connectStr=%s\n", options.connectStr)
	fmt.Println("Done with parsing the arguments")

	// compile filter expression if needed
	var err error
	if len(options.filter_expression) > 0 {
		filterRegExp, err = regexp.Compile(options.filter_expression)
		if err != nil {
			fmt.Println("Error compiling filter expression. ", err.Error())
			os.Exit(1)
		}
	}

	startRouter()
	fmt.Println("Router is started")
	waitGrp := &sync.WaitGroup{}
	waitGrp.Add(1)
	go startUpr(options.connectStr, options.source_bucket, waitGrp)
	waitGrp.Wait()
}

func startUpr(cluster, bucketn string, waitGrp *sync.WaitGroup) {
	b, err := utils.LocalBucket(cluster, bucketn)
	mf(err, "bucket")

	uprFeed, err = b.StartUprFeedWithConfig("rawupr", uint32(0), 1000, 1024*1024)
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
		err := router.Forward(e)
		mf(err, " - route")

		if count >= NumData {
			break
		}

	}

	fmt.Printf("Number of upr event routed is %d\n", routedCount)

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
			vbno, vbno, flags, vbuuid, start, end, snapStart, snapEnd)
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

	utils := utils.NewUtilities()
	partMap := make(map[string]pc.Part)
	for i := 0; i < NumParts; i++ {
		partId := PartIdPrefix + strconv.FormatInt(int64(i), 10)
		partMap[partId] = NewTestPart(partId)
	}

	router, _ = parts.NewRouter("router1", "router1", options.filter_expression, partMap, buildVbMap(partMap), base.CRMode_RevId, couchlog.DefaultLoggerContext, nil, utils, nil, nil, nil, 0, nil)
}

func buildVbMap(downStreamParts map[string]pc.Part) map[uint16]string {
	vbMap := make(map[uint16]string)

	numOfNodes := len(downStreamParts)

	numOfVbPerNode := uint16(math.Ceil(float64(options.maxVbno) / float64(numOfNodes)))

	var indexOfNode uint16
	for partId := range downStreamParts {
		var j uint16
		for j = 0; j < numOfVbPerNode; j++ {
			vbno := indexOfNode*numOfVbPerNode + j
			if vbno < uint16(options.maxVbno) {
				vbMap[vbno] = partId
			} else {
				// no more vbs to process
				break
			}
		}
		indexOfNode++
	}
	return vbMap

}

type TestPart struct {
	parts.AbstractPart
}

func NewTestPart(id string) *TestPart {
	tp := new(TestPart)
	tp.AbstractPart = parts.NewAbstractPart(id)
	return tp
}

func (tp *TestPart) Start(settings metadata.ReplicationSettingsMap) error {
	return nil
}

func (tp *TestPart) Stop() error {
	return nil
}

func (tp *TestPart) Receive(data interface{}) error {
	request := data.(*mc.MCRequest)
	routedCount++
	fmt.Printf("Part %v received data with vbno=%v, key=%v\n", tp.Id(), request.VBucket, string(request.Key))
	if filterRegExp != nil && !filterRegExp.Match(request.Key) {
		return errors.New("Data with key=" + string(request.Key) + " has not been filtered out as expected by filter expression=" + options.filter_expression)
	}

	return nil
}

func (tp *TestPart) IsStarted() bool {
	return false
}
