// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package main

import (
	//	"errors"
	"flag"
	"fmt"
	"github.com/couchbase/go-couchbase"
	mc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/v8/base"
	xdcrlog "github.com/couchbase/goxdcr/v8/log"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
	//	 "io/ioutil"
)

import _ "net/http/pprof"

var quit bool = false
var logger_latency *xdcrlog.CommonLogger = xdcrlog.NewLogger("LatencyTest", xdcrlog.DefaultLoggerContext)
var source_rest_server_addr string //source rest server address
var num_worker int                 // number of read worker used by the test

var options struct {
	source_bucket           string // source bucket
	target_bucket           string //target bucket
	source_cluster_addr     string //source connect string
	target_cluster_addr     string //target connect string
	target_cluster_username string //target cluster username
	target_cluster_password string //target cluster password
	target_bucket_password  string //target bucket password
	doc_size                int    //doc_size
	doc_count               int    //doc_count
	num_write               int    // number of concurrent write routines
	sample_frequency        int    // frequency of sampling - sample one in every sample_frequency data points
}

//type docInfo struct {
//	key         string
//	update_time time.Time
//	duration    time.Duration
//}

type appWriter struct {
	cluster    string
	bucket     string
	key_prefix string
	doc_size   int
	doc_count  int
	doc        []byte
	reader     *appReader
}

func newAppWriter(cluster, bucket, key_prefix string, doc_size, doc_count int, reader *appReader) *appWriter {
	return &appWriter{cluster: cluster,
		bucket:     bucket,
		key_prefix: key_prefix,
		doc_size:   doc_size,
		doc_count:  doc_count,
		reader:     reader}
}

func (w *appWriter) run() (err error) {
	u, err := url.Parse("http://" + w.cluster)
	if err != nil {
		logger_latency.Errorf("Failed to parse cluster %v\n", w.cluster)
		return
	}

	c, err := couchbase.Connect(u.String())
	if err != nil {
		logger_latency.Errorf("connect - %v", u.String())
		return
	}

	p, err := c.GetPool("default")
	if err != nil {
		logger_latency.Error("Failed to get 'default' pool")
		return
	}

	num_write := options.num_write
	if num_write > w.doc_count {
		num_write = w.doc_count
	}

	couchbase.PoolSize = num_write
	b, err := p.GetBucket(w.bucket)
	if err != nil {
		logger_latency.Errorf("Failed to get bucket %v", w.bucket)
		return
	}

	docs_per_write := int(math.Ceil(float64(w.doc_count) / float64(num_write)))
	logger_latency.Infof("docs_per_write=%v\n", docs_per_write)
	for start_index := 0; start_index < num_write; start_index++ {
		logger_latency.Infof("Starting write routine #%v\n", start_index)
		go w.write(b, start_index, num_write, docs_per_write)
	}

	logger_latency.Infof("--------DONE WITH CREATING %v Items------------\n", w.doc_count)
	return
}

// Each write routine writes docs at start_index, start_index+num_write, start_index+2*num_write, etc.
// This way the larger the doc index, the later it will be written.
// This, coupled with the current sampling algorithm based on doc index, can achieve approximate uniform sampling in the time dimension.
func (w *appWriter) write(b *couchbase.Bucket, start_index, num_write, docs_per_write int) error {
	doc := w.genDoc(start_index)

	var err error
	for i := 0; i < docs_per_write; i++ {
		index := start_index + i*num_write
		if index >= options.doc_count {
			break
		}
		doc_key := w.key_prefix + "_" + fmt.Sprintf("%v", index)
		err = b.SetRaw(doc_key, 0, doc)
		if err != nil {
			return err
		}

		if err == nil {
			if math.Mod(float64(index), float64(options.sample_frequency)) == 0 {
				logger_latency.Infof("Record doc %v in write routine #%v in the %vth iteration\n", doc_key, start_index, i)
				write_time := time.Now()
				w.reader.read(index/options.sample_frequency, doc_key, write_time)
			}
		}
	}
	return err
}

func (w *appWriter) genDoc(index int) []byte {
	if w.doc == nil {
		w.doc = []byte{}
		for i := 0; i < w.doc_size; i++ {
			w.doc = append(w.doc, byte(i))
		}
	}
	return w.doc
}

func getDoc(b *couchbase.Bucket, key string) error {
	return b.Do(key, func(mc *mc.Client, vb uint16) error {
		var err error
		_, err = mc.Get(vb, key)
		return err
	})
}

//func recordWriteTime(id string, key string, write_time time.Time) {
//	logger_latency.Infof("Record (%v, %v)--\n", id, write_time)
//	if key_map == nil {
//		key_map = make(map[string]*docInfo)
//	}
//
//	info := &docInfo{key: key,
//		update_time: write_time}
//	key_map[id] = info
//	logger_latency.Infof("key_map has %v elements\n", len(key_map))
//}

type appReader struct {
	cluster     string
	bucket      string
	password    string
	b           *couchbase.Bucket
	worker_pool []*appReadWorker
}

func (r *appReader) init() (err error) {

	u, err := url.Parse("http://" + r.bucket + ":" + r.password + "@" + r.cluster)
	if err != nil {
		logger_latency.Errorf("Failed to parse cluster %v, err=%v\n", r.cluster, err)
		os.Exit(1)
		return
	}

	c, err := couchbase.Connect(u.String())
	if err != nil {
		logger_latency.Errorf("Failed to connect - %v, err=%v\n", u.String(), err)
		os.Exit(1)
		return
	}

	p, err := c.GetPool("default")
	if err != nil {
		logger_latency.Errorf("Failed to get 'default' pool, err=%v\n", err)
		os.Exit(1)
		return
	}

	r.b, err = p.GetBucket(r.bucket)
	if err != nil {
		logger_latency.Errorf("Failed to get bucket %v, err=%v\n", r.bucket, err)
		os.Exit(1)
		return
	}

	r.worker_pool = make([]*appReadWorker, num_worker)

	for i := 0; i < num_worker; i++ {
		r.worker_pool[i] = &appReadWorker{}
	}
	return
}

type appReadWorker struct {
	key      string
	duration time.Duration
}

func (w *appReadWorker) run(write_time time.Time, r *appReader) {
	defer func() {
		if r := recover(); r != nil {
			logger_latency.Infof("Recovered in function read ", r)
			logger_latency.Infof("Received %v\n", w.key)
		}
	}()

	logger_latency.Infof("Try to read doc key=%v\n", w.key)
	for {
		err := getDoc(r.b, w.key)
		if err == nil {
			w.duration = time.Since(write_time)
			return
		} else {
			// sleep to avoid taking up too much CPU
			time.Sleep(time.Millisecond * 50)
		}
	}
	return

}
func (r *appReader) read(index int, key string, write_time time.Time) {
	worker := r.worker_pool[index]
	worker.key = key

	go worker.run(write_time, r)
}

func parseArgs() {
	flag.StringVar(&options.source_cluster_addr, "source_cluster_addr", "127.0.0.1:9000",
		"source cluster address")
	flag.StringVar(&options.source_bucket, "source_bucket", "default",
		"bucket to replicate from")
	flag.StringVar(&options.target_cluster_addr, "target_cluster_addr", "127.0.0.1:9000",
		"target cluster address")
	flag.StringVar(&options.target_bucket, "target_bucket", "target",
		"bucket to replicate to")
	flag.StringVar(&options.target_cluster_username, "target_cluster_username", "Administrator",
		"user name to use for logging into target cluster")
	flag.StringVar(&options.target_cluster_password, "target_cluster_password", "welcome",
		"password to use for logging into target cluster")
	flag.StringVar(&options.target_bucket_password, "target_bucket_password", "",
		"password to use for accessing target bucket")
	flag.IntVar(&options.doc_size, "doc_size", 1000, "size (in byte) of the documents app writer generates")
	flag.IntVar(&options.doc_count, "doc_count", 100000, "the number of documents app writer generates")
	flag.IntVar(&options.num_write, "num_write", 100, "number of concurrent write routines")
	flag.IntVar(&options.sample_frequency, "sample_frequency", 50, "frequency of sampling - sample one in every sample_frequency data points")
	flag.Parse()

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

	setup()

	logger_latency.Info("Start testing...")
	//start the replication
	err := startGoXDCRReplicationByRest()
	if err != nil {
		os.Exit(1)
	}
	defer func() {
		stopGoXDCRReplicationByRest()
	}()

	// wait for replication to finish initializing
	time.Sleep(time.Second * 20)

	appR := &appReader{cluster: options.target_cluster_addr,
		bucket: options.target_bucket, password: options.target_bucket_password}
	appR.init()

	//start app writer
	appW := newAppWriter(options.source_cluster_addr, options.source_bucket, "TEST-", options.doc_size, options.doc_count, appR)
	go appW.run()

	//start app reader

	//let it run for 3 minutes
	time.Sleep(time.Second * 120)

	quit = true

	verify(appR.worker_pool)
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage :  [OPTIONS]\n")
	flag.PrintDefaults()
}

func setup() error {
	parseArgs()

	// set source rest server address
	hostName := strings.Split(options.source_cluster_addr, ":")[0]
	source_rest_server_addr = hostName + ":" + strconv.FormatInt(int64(base.AdminportNumber), 10)

	// set number of read workers
	num_worker = int(math.Ceil(float64(options.doc_count) / float64(options.sample_frequency)))

	logger_latency.Infof("Setup is done")
	return nil
}

func startGoXDCRReplicationByRest() error {
	go func() {
		cmd := exec.Command("curl", "-X", "POST", "http://"+source_rest_server_addr+"/controller/createReplication", "-d", "fromBucket="+options.source_bucket, "-d", "uuid="+options.target_cluster_addr,
			"-d", "toBucket="+options.target_bucket, "-d", "xdcrSourceNozzlePerNode=4", "-d", "xdcrTargetNozzlePerNode=4", "-d", "xdcrLogLevel=Error")
		logger_latency.Infof("cmd =%v, path=%v\n", cmd.Args, cmd.Path)
		bytes, err := cmd.Output()
		if err != nil {
			logger_latency.Errorf("Failed to start goxdcr replication, err=%v\n", err)
			logger_latency.Infof("err=%v, out=%v\n", err, bytes)

			quit = true
			return

		}
		return
	}()

	return nil
}

func stopGoXDCRReplicationByRest() (err error) {
	replicationId := options.source_cluster_addr + "_" + options.source_bucket + "_" + options.target_cluster_addr + "_" + options.target_bucket
	cmd := exec.Command("curl", "-X", "POST", "http://"+source_rest_server_addr+"/controller/cancelXDCR/"+replicationId)
	logger_latency.Infof("cmd =%v, path=%v\n", cmd.Args, cmd.Path)
	bytes, err := cmd.Output()
	if err != nil {
		logger_latency.Errorf("Failed to pause goxdcr replication, err=%v\n", err)
		logger_latency.Infof("err=%v, out=%v\n", err, bytes)

		quit = true
		return

	}
	return
}

func verify(readWorkers []*appReadWorker) {
	logger_latency.Infof("----------VERIFY------------")
	outlier := []string{}
	outlier_count := 0
	normals_count := 0
	normals_total := 0 * time.Millisecond
	normals_min := 0 * time.Millisecond
	normals_max := 0 * time.Millisecond

	for _, readWorker := range readWorkers {
		if readWorker.duration != 0 {
			normals_count++
			normals_total = normals_total + readWorker.duration
			if normals_min == 0*time.Millisecond || readWorker.duration < normals_min {
				normals_min = readWorker.duration
			}

			if normals_min == 0*time.Millisecond || readWorker.duration > normals_max {
				normals_max = readWorker.duration
			}
		} else {
			outlier = append(outlier, readWorker.key)
			outlier_count++
		}
	}

	logger_latency.Info("------TEST RESULT-----")
	logger_latency.Infof("outlier_count=%v\n", outlier_count)
	logger_latency.Infof("outlier=%v\n", outlier)
	logger_latency.Infof("normal latency item count=%v\n", normals_count)
	logger_latency.Infof("normal latency max=%v sec\n", normals_max.Seconds())
	logger_latency.Infof("normal latency min=%v sec\n", normals_min.Seconds())
	logger_latency.Infof("normal latency average=%v sec\n", (normals_total.Seconds() / float64(normals_count)))

	//write key_map and key_map_recv to files
	//	   w := bufio.NewWriter(f)
	//  for _,
	//	err := ioutil.WriteFile("/tmp/"+time.Now()+"/"+"key_map.dat", d1, 0644)
	//    check(err)
}

//func write
