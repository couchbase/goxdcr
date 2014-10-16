package main

import (
	//	"errors"
	"flag"
	"fmt"
	xdcrlog "github.com/Xiaomei-Zhang/couchbase_goxdcr/log"
	//	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/adminport"
	//	couchdoc "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/couchdoc_metadata"
	//	c "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/mock_services"
	//	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/replication_manager"
	//	s "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/services"
	"github.com/couchbase/gomemcached"
	mc "github.com/couchbase/gomemcached/client"
	"github.com/couchbaselabs/go-couchbase"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"time"
	//	 "io/ioutil"
)

import _ "net/http/pprof"

var quit bool = false
var logger_latency *xdcrlog.CommonLogger = xdcrlog.NewLogger("LatencyTest", xdcrlog.DefaultLoggerContext)

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
	doc_size                int    //doc_size
	doc_count               int    //doc_count
}

type docInfo struct {
	key         string
	update_time time.Time
	duration    time.Duration
}

var key_map map[string]*docInfo
var key_recv_ch chan *docInfo

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

	b, err := p.GetBucket(w.bucket)
	if err != nil {
		logger_latency.Errorf("Failed to get bucket %v", w.bucket)
		return
	}

	for i := 0; i < w.doc_count; i++ {
		logger_latency.Infof("Write doc #%v\n", i)
		err = w.write(b, i)
		if err != nil {
			logger_latency.Errorf("Failed to write item %v\n", i)
			return
		}
	}

	logger_latency.Infof("--------DONE WITH CREATING %v Items------------\n", w.doc_count)
	return
}

func (w *appWriter) write(b *couchbase.Bucket, index int) error {
	doc_key := w.key_prefix + "_" + fmt.Sprintf("%v", index)
	doc := w.genDoc(doc_key, index)
	err := b.SetRaw(doc_key, 0, doc)
	if err != nil {
		return err
	}

	if err == nil {
		logger_latency.Infof("Record doc %v", index)
		write_time := time.Now()
		id := doc_key
		recordWriteTime(id, doc_key, write_time)
		go w.reader.read(doc_key)
	}
	return err
}

func (w *appWriter) genDoc(doc_key string, index int) []byte {
	if w.doc == nil {
		w.doc = []byte{}
		for i := 0; i < w.doc_size; i++ {
			w.doc = append(w.doc, byte(i))
		}
	}
	return w.doc
}

func getDoc(b *couchbase.Bucket, key string, res *gomemcached.MCResponse) error {
	return b.Do(key, func(mc *mc.Client, vb uint16) error {
		var err error
		res, err = mc.Get(vb, key)
		return err
	})
}
func recordWriteTime(id string, key string, write_time time.Time) {
	logger_latency.Infof("Record (%v, %v)--\n", id, write_time)
	if key_map == nil {
		key_map = make(map[string]*docInfo)
	}

	info := &docInfo{key: key,
		update_time: write_time}
	key_map[id] = info
	logger_latency.Infof("key_map has %v elements\n", len(key_map))
}

type appReader struct {
	cluster  string
	bucket   string
	password string
	id_ch    chan string
	b        *couchbase.Bucket
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

	return
}

func (r *appReader) read(key string) {
	defer func() {
		if r := recover(); r != nil {
			logger_latency.Infof("Recovered in f", r)
			logger_latency.Infof("Received %v\n", key)
		}
	}()

	docinfo := key_map[key]
	logger_latency.Infof("Try to read doc key=%v\n", key)
	_, err := r.b.Observe(key)
	if err == nil {
		logger_latency.Infof("Observed changes for %v\n", key)
		newInfo := &docInfo{key: key,
			duration: time.Since(docinfo.update_time)}
		key_recv_ch <- newInfo
		return
	} else {
	}

	return

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
	flag.IntVar(&options.doc_size, "doc_size", 1000, "size (in byte) of the documents app writer generates")
	flag.IntVar(&options.doc_count, "doc_count", 100000, "the number of documents app writer generates")
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

	ch := make(chan string, options.doc_count)

	appR := &appReader{cluster: options.target_cluster_addr,
		bucket: options.target_bucket, password: options.target_bucket_password,
		id_ch: ch}
	appR.init()

	//start app writer
	appW := newAppWriter(options.source_cluster_addr, options.source_bucket, "TEST-", options.doc_size, options.doc_count, appR)
	go appW.run()

	//start app reader

	//let it run for 3 minutes
	time.Sleep(time.Minute * 2)

	quit = true

	verify()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage :  [OPTIONS]\n")
	flag.PrintDefaults()
}

func setup() error {
	parseArgs()
	key_recv_ch = make(chan *docInfo, options.doc_count)

	logger_latency.Infof("Setup is done")
	return nil
}

func startGoXDCRReplicationByRest() error {

	go func() {
		cmd := exec.Command("curl", "-X", "POST", "http://localhost:12100/controller/createReplication", "-d", "fromBucket="+options.source_bucket, "-d", "uuid="+options.source_cluster_addr,
			"-d", "toBucket="+options.target_bucket, "-d", "xdcrSourceNozzlePerNode=2", "-d", "xdcrTargetNozzlePerNode=2", "-d", "xdcrLogLevel=Error")
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

	cmd := exec.Command("curl", "-X", "POST", "http://localhost:12100/controller/pauseXDCR/127.0.0.1:9000_default_127.0.0.1:9000_target")
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

func verify() {
	logger_latency.Infof("----------VERIFY------------")
	outliner := []string{}
	outliner_count := 0
	normals_count := 0
	normals_total := 0 * time.Millisecond
	normals_min := 0 * time.Millisecond
	normals_max := 0 * time.Millisecond

	recvmap := make(map[string]*docInfo)
	close(key_recv_ch)
	for ent := range key_recv_ch {
		recvmap[ent.key] = ent
		logger_latency.Infof("key=%v, duration=%v\n", ent.key, ent.duration)
	}

	for _, docinfo := range key_map {
		entry, ok := recvmap[docinfo.key]
		if ok {
			normals_count++
			normals_total = normals_total + entry.duration
			if normals_min == 0*time.Millisecond || entry.duration < normals_min {
				normals_min = entry.duration
			}

			if normals_min == 0*time.Millisecond || entry.duration > normals_max {
				normals_max = entry.duration
			}
		} else {
			outliner = append(outliner, docinfo.key)
			outliner_count++
		}
	}

	logger_latency.Info("------TEST RESULT-----")
	logger_latency.Infof("outliner_count=%v\n", outliner_count)
	logger_latency.Infof("outliner=%v\n", outliner)
	logger_latency.Infof("normal latency item count=%v\n", normals_count)
	logger_latency.Infof("normal latency max=%v\n", normals_max.Seconds())
	logger_latency.Infof("normal latency min=%v\n", normals_min.Seconds())
	logger_latency.Infof("normal latency average=%v\n", (normals_total.Seconds() / float64(normals_count)))

	//write key_map and key_map_recv to files
	//	   w := bufio.NewWriter(f)
	//  for _,
	//	err := ioutil.WriteFile("/tmp/"+time.Now()+"/"+"key_map.dat", d1, 0644)
	//    check(err)
}

//func write
