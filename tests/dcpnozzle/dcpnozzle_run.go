/*
Copyright 2014-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

// Test for DcpNozzle, source nozzle in XDCR
package main

import (
	"flag"
	"fmt"
	"github.com/couchbase/go-couchbase"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/v8/base"
	connector "github.com/couchbase/goxdcr/v8/connector"
	"github.com/couchbase/goxdcr/v8/parts"
	"github.com/couchbase/goxdcr/v8/utils"
	"log"
	"math"
	"net/http"
	"os"
	"time"
)

import _ "net/http/pprof"

var options struct {
	source_bucket string // source bucket
	connectStr    string //connect string
	kvaddr        string //kvaddr
}

const (
	NUM_DATA = 3000 // number of data points to collect before test ends
)

var count int

func argParse() {
	flag.StringVar(&options.connectStr, "connectStr", "127.0.0.1:9000",
		"connection string to source cluster")
	flag.StringVar(&options.source_bucket, "source_bucket", "default",
		"bucket to replicate from")
	flag.StringVar(&options.kvaddr, "kvaddr", "127.0.0.1:12000",
		"kv server address")

	flag.Parse()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] \n", os.Args[0])
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

	fmt.Println("Start Testing DcpNozzle...")
	argParse()
	fmt.Printf("connectStr=%s\n", options.connectStr)
	fmt.Println("Done with parsing the arguments")

	bucket, err := utils.LocalBucket(options.connectStr, options.source_bucket)
	mf(err, "bucket")

	vblist, err := getVBListFromBucket(bucket, options.kvaddr)
	mf(err, "vblist")
	fmt.Printf("vblist in b=%v\n", vblist)

	for i := 0; i < 1; i++ {
		go startDcpNozzle(options.kvaddr, bucket, vblist, 16, i)
	}

	time.Sleep(1 * time.Minute)
}

func mf(err error, msg string) {
	if err != nil {
		log.Fatalf("%v: %v", msg, err)
	}
}

func startDcpNozzle(kvaddr string, bucket *couchbase.Bucket, vbList []uint16, numNozzle, i int) {

	numVbs := len(vbList)
	numVbPerNozzle := int(math.Ceil(float64(numVbs) / float64(numNozzle)))
	dcpVbList := make([]uint16, 0)
	for j := numVbPerNozzle * i; j < numVbPerNozzle*(i+1); j++ {
		if j >= numVbs {
			break
		}
		dcpVbList = append(dcpVbList, uint16(j))
	}
	dcpNozzle := parts.NewDcpNozzle("test_dcp", bucket.Name, bucket.Password, dcpVbList, nil, false, nil)
	dcpNozzle.SetConnector(NewTestConnector())
	dcpNozzle.Start(constructStartSettings(dcpNozzle))
	fmt.Println("DcpNozzle is started")

	timeChan := time.NewTimer(time.Second * 1000).C
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
	dcpNozzle.Stop()
	fmt.Println("DcpNozzle is stopped")

	if count < NUM_DATA {
		fmt.Printf("Test failed. Only %v data was received before timer expired.\n", count)
	} else {
		fmt.Println("Test passed. All test data was received as expected before timer expired.")
	}
}

func constructStartSettings(dcpNozzle *parts.DcpNozzle) map[string]interface{} {
	settings := make(map[string]interface{})
	vblist := dcpNozzle.GetVBList()
	fmt.Printf("vblist in dcp =%v\n", vblist)
	ts := make(map[uint16]*base.VBTimestamp)
	for _, vb := range vblist {
		ts[vb] = &base.VBTimestamp{}
		ts[vb].Vbno = vb
	}
	settings[parts.DCP_VBTimestamp] = ts
	return settings
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

// get all vbs owned by specied node from bucket
func getVBListFromBucket(bucket *couchbase.Bucket, kvaddr string) ([]uint16, error) {

	m, err := bucket.GetVBmap([]string{kvaddr})
	if err != nil {
		return nil, err
	}
	return m[kvaddr], nil
}
