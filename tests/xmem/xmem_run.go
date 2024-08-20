// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

// Tool receives raw events from go-couchbase UPR client.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/couchbase/go-couchbase"
	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	parts "github.com/couchbase/goxdcr/v8/parts"
	utils "github.com/couchbase/goxdcr/v8/utils"

	_ "net/http/pprof"
)

var logger *log.CommonLogger = log.NewLogger("Xmem_run", log.DefaultLoggerContext)

var options struct {
	source_bucket       string // source bucket
	target_bucket       string //target bucket
	source_cluster_addr string //source connect string
	target_cluster_addr string //target connect string
	username            string //username
	password            string //password
	maxVbno             int    // maximum number of vbuckets
}

var done = make(chan bool, 16)
var rch = make(chan []interface{}, 10000)
var uprFeed *couchbase.UprFeed = nil
var xmem *parts.XmemNozzle = nil
var target_bk *couchbase.Bucket

func argParse() {
	flag.StringVar(&options.source_cluster_addr, "source_cluster_addr", "127.0.0.1:9000",
		"source cluster address")
	flag.StringVar(&options.target_cluster_addr, "target_cluster_addr", "127.0.0.1:9000",
		"target cluster address")
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
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] \n", os.Args[0])
	flag.PrintDefaults()
}

func setup() (err error) {

	logger.Info("Start Testing Xmem...")
	logger.Infof("target_clusterAddr=%s, username=%s, password=%s\n", options.target_cluster_addr, options.username, options.password)
	logger.Info("Done with parsing the arguments")

	//flush the target bucket
	if err == nil {
		err, _ = utils.QueryRestApiWithAuth(options.target_cluster_addr,
			"/pools/default/buckets/target/controller/doFlush",
			false,
			options.target_bucket,
			options.password,
			nil, false,
			"POST", "", nil,
			0, nil, nil, false, logger)
	}

	if err != nil {
		logger.Infof("Setup error=%v\n", err)
	} else {
		logger.Info("Setup is done")
	}

	return
}

func verify(data_count int) bool {
	output := &utils.CouchBucket{}

	err, _ := utils.QueryRestApiWithAuth(options.target_cluster_addr,
		"/pools/default/buckets/target",
		false,
		options.target_bucket,
		options.password,
		nil, false,
		"GET", "", nil,
		0, output, nil, false, logger)
	if err != nil {
		panic(err)
	}
	logger.Infof("name=%s itemCount=%d\n", output.Name, output.Stat.ItemCount)

	return output.Stat.ItemCount == data_count
}
func main() {
	//start http server for pprof
	go func() {
		logger.Info("Try to start pprof...")
		err := http.ListenAndServe("localhost:7000", nil)
		if err != nil {
			panic(err)
		} else {
			logger.Info("Http server for pprof is started")
		}
	}()
	argParse()

	test(500, 1000)
	//	test(5, 50, parts.Batch_XMEM)

}

func test(batch_count int, data_count int) {
	logger.Info("------------START testing Xmem-------------------")
	logger.Infof("batch_count=%d, data_count=%d\n", batch_count, data_count)
	//	err := setup()

	//	if err != nil {
	//		panic(err)
	//	}
	startXmem(batch_count)
	logger.Info("XMEM is started")
	waitGrp := &sync.WaitGroup{}
	waitGrp.Add(1)
	go startUpr(options.source_cluster_addr, options.source_bucket, waitGrp, data_count)
	waitGrp.Wait()

	time.Sleep(100 * time.Second)
	bSuccess := verify(data_count)
	if bSuccess {
		logger.Info("----------TEST SUCCEED------------")
	} else {
		logger.Info("----------TEST FAILED-------------")
	}
}
func startUpr(cluster, bucketn string, waitGrp *sync.WaitGroup, data_count int) {
	logger.Info("Start Upr...")
	b, err := utils.LocalBucket(cluster, bucketn)
	mf(err, "bucket")

	uprFeed, err = b.StartUprFeedWithConfig("rawupr", uint32(0), 1000, 1024*1024)
	mf(err, "- upr")

	flogs := failoverLogs(b)
	logger.Info("Got failover log successfully")

	// list of vbuckets
	vbnos := make([]uint16, 0, options.maxVbno)
	for i := 0; i < options.maxVbno; i++ {
		vbnos = append(vbnos, uint16(i))
	}

	startStream(uprFeed, flogs)
	logger.Info("Upr stream is started")

	count := 0
	for {
		e, ok := <-uprFeed.C
		if ok == false {
			logger.Infof("Closing for bucket %v\n", b.Name)
		}

		//transfer UprEvent to MCRequest
		switch e.Opcode {
		case mc.UPR_MUTATION, mc.UPR_DELETION, mc.UPR_EXPIRATION:
			mcReq := composeMCRequest(e)
			count++
			logger.Infof("Number of upr event received so far is %d\n", count)

			xmem.Receive(mcReq)
		}
		if count >= data_count {
			goto Done
		}

	}
Done:
	//close the upr stream
	logger.Info("Done.........")
	uprFeed.Close()
	xmem.Stop()
	waitGrp.Done()
}

func getVBucket(source_vbId uint16) uint16 {
	return uint16(1023)
}

func composeMCRequest(event *mcc.UprEvent) *mc.MCRequest {
	req := &mc.MCRequest{Cas: event.Cas,
		Opaque:  0,
		VBucket: event.VBucket,
		Key:     event.Key,
		Body:    event.Value,
		Extras:  make([]byte, 224)}

	//opCode
	req.Opcode = event.Opcode
	//	switch event.Opcode {
	//	case mcc.UprStreamRequest:
	//		req.Opcode = mc.UPR_STREAMREQ
	//	case mcc.UprMutation:
	//		req.Opcode = mc.UPR_MUTATION
	//	case mcc.UprDeletion:
	//		req.Opcode = mc.UPR_DELETION
	//	case mcc.UprExpiration:
	//		req.Opcode = mc.UPR_EXPIRATION
	//	case mcc.UprCloseStream:
	//		req.Opcode = mc.UPR_CLOSESTREAM
	//	case mcc.UprSnapshot:
	//		req.Opcode = mc.UPR_SNAPSHOT
	//	case mcc.UprFlush:
	//		req.Opcode = mc.UPR_FLUSH
	//	}

	//extra
	if event.Opcode == mc.UPR_MUTATION || event.Opcode == mc.UPR_DELETION ||
		event.Opcode == mc.UPR_EXPIRATION {
		binary.BigEndian.PutUint64(req.Extras, event.Seqno)
		binary.BigEndian.PutUint32(req.Extras, event.Flags)
		binary.BigEndian.PutUint32(req.Extras, event.Expiry)
	} else if event.Opcode == mc.UPR_SNAPSHOT {
		logger.Infof("event.Seqno=%v\n", event.Seqno)
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
		logger.Errorf("%v: %v", msg, err)
	}
}

func getConnectStr(clusterAddr string, poolName string, bucketName string, username string, password string) (string, error) {
	var c string
	if username != "" && password != "" {
		c = "http://" + username + ":" + password + "@" + clusterAddr
	} else {
		c = "http://" + clusterAddr
	}
	var err error
	target_bk, err = couchbase.GetBucket(c, poolName, bucketName)
	if err == nil && target_bk != nil {
		addrs := target_bk.NodeAddresses()

		if addrs != nil && len(addrs) > 0 {
			for _, add := range addrs {
				logger.Infof("node_address=%v\n", add)
			}
			return addrs[0], nil
		}
	} else {
		panic(fmt.Sprintf("failed to instantiate target bucket - %v, err=%v", c, err))
	}
	return "", err
}

func startXmem(batch_count int) {
	target_connectStr, err := getConnectStr(options.target_cluster_addr, "default", options.target_bucket, options.username, options.password)
	if err != nil || target_connectStr == "" {
		panic(err)
	}
	logger.Infof("target_connectStr=%s\n", target_connectStr)

	xmem = parts.NewXmemNozzle("xmem", "abc", "abc", 10, target_connectStr, options.source_bucket, options.target_bucket, options.target_bucket, options.password, base.CRMode_RevId, logger.LoggerContext(), nil, "")
	var configs map[string]interface{} = map[string]interface{}{parts.SETTING_BATCHCOUNT: batch_count,
		parts.SETTING_RESP_TIMEOUT: time.Millisecond * 10,
		parts.SETTING_NUMOFRETRY:   3}

	xmem.Start(configs)
}
