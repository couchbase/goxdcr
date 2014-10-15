package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr/log"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/adminport"
	couchdoc "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/couchdoc_metadata"
	c "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/mock_services"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/replication_manager"
	s "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/services"
	"github.com/couchbase/gomemcached"
	mc "github.com/couchbase/gomemcached/client"
	"github.com/couchbaselabs/go-couchbase"
	"net/url"
	"os"
	"os/exec"
	"time"
)

var quit bool = false
var logger_latency *log.CommonLogger = log.NewLogger("LatencyTest", log.DefaultLoggerContext)

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
	done        bool
}

var key_map map[string]*docInfo

type appWriter struct {
	cluster    string
	bucket     string
	key_prefix string
	doc_size   int
	doc_count  int
}

func newAppWriter(cluster, bucket, key_prefix string, doc_size, doc_count int) *appWriter {
	return &appWriter{cluster: cluster,
		bucket:     bucket,
		key_prefix: key_prefix,
		doc_size:   doc_size,
		doc_count:  doc_count}
}

func (w *appWriter) run() (err error) {

	u, err := url.Parse("http://" + w.cluster)
	logger_latency.Errorf("Failed to parse cluster %v\n", w.cluster)

	c, err := couchbase.Connect(u.String())
	logger_latency.Errorf("connect - %v", u.String())

	p, err := c.GetPool("default")
	logger_latency.Error("Failed to get 'default' pool")

	b, err := p.GetBucket(w.bucket)
	logger_latency.Errorf("Failed to get bucket %v", w.bucket)

	for i := 0; i < w.doc_count; i++ {
		err = w.write(b, i)
		if err != nil {
			logger_latency.Errorf("Failed to write item %v\n", i)
			return
		}
	}
	return
}

func (w *appWriter) write(b *couchbase.Bucket, index int) error {
	doc_key := w.key_prefix + "_" + string(index)
	doc := w.genDoc(doc_key, index)
	err := b.SetRaw(doc_key, 0, doc)
	res := &gomemcached.MCResponse{}
	err = getDoc(b, doc_key, res)

	if err != nil {
		write_time := time.Now()
		metainfo := couchdoc.GetDocMetadataFromResp(res)
		recordWriteTime(string(doc_key)+"_"+string(metainfo.RevSeqno), doc_key, write_time)
	}
	return err
}

func (w *appWriter) genDoc(doc_key string, index int) []byte {
	doc := []byte{}
	for i := 0; i < w.doc_size; i++ {
		doc = append(doc, byte(i))
	}
	return doc
}

func getDoc(b *couchbase.Bucket, key string, res *gomemcached.MCResponse) error {
	return b.Do(key, func(mc *mc.Client, vb uint16) error {
		var err error
		res, err = mc.Get(vb, key)
		return err
	})
}
func recordWriteTime(id string, key string, write_time time.Time) {
	if key_map == nil {
		key_map = make(map[string]*docInfo)
	}

	info := &docInfo{key: key,
		update_time: write_time,
		done:        false}
	key_map[id] = info
}

type appReader struct {
	cluster string
	bucket  string
}

func (r *appReader) run() (err error) {
	u, err := url.Parse("http://" + r.cluster)
	logger_latency.Errorf("Failed to parse cluster %v\n", r.cluster)

	c, err := couchbase.Connect(u.String())
	logger_latency.Errorf("connect - %v", u.String())

	p, err := c.GetPool("default")
	logger_latency.Error("Failed to get 'default' pool")

	b, err := p.GetBucket(r.bucket)
	logger_latency.Errorf("Failed to get bucket %v", r.bucket)

	for {
		for id, docinfo := range key_map {
			if !docinfo.done {
				key := docinfo.key
				res := &gomemcached.MCResponse{}
				err := getDoc(b, key, res)
				if err == nil {
					metainfo := couchdoc.GetDocMetadataFromResp(res)
					if id == key+"_"+string(metainfo.RevSeqno) {
						docinfo.duration = time.Since(docinfo.update_time)
						docinfo.done = true
					}
				}

			}
		}
		if quit {
			return
		}
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
	options.doc_size = *flag.Int("doc_size", 10000, "size (in byte) of the documents app writer generates")
	options.doc_count = *flag.Int("doc_count", 100000, "the number of documents app writer generates")
	flag.Parse()

}

func main() {
	setup()

	xdcrTopologyService := new(c.MockXDCRTopologySvc)
	hostAddr, err := xdcrTopologyService.MyHost()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting host address \n")
		os.Exit(1)
	}
	
	go adminport.MainAdminPort(hostAddr)
	time.Sleep(time.Second * 30)

	logger_latency.Info("Start testing...")
	//start the replication
	go startGoXDCRReplicationByRest()

	if err != nil {
		logger_latency.Errorf("Failed to start goxdcr replication, err=%v\n", err)
	}

	//start app writer
	appW := newAppWriter(options.source_cluster_addr, options.source_bucket, "TEST-", 10000, 100000)
	go appW.run()

	//start app reader
	appR := &appReader{cluster: options.target_cluster_addr,
		bucket: options.target_bucket}
	go appR.run()

	//let it run for 3 minutes
	time.Sleep(time.Minute * 3)

	quit = true

	verify()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage :  [OPTIONS]\n")
	flag.PrintDefaults()
}

func setup() error {
	parseArgs()

	cmd, err := s.StartGometaService()
	if err != nil {
		fmt.Println("Failed to start gometa service. err: ", err)
		os.Exit(1)
	}
	defer s.KillGometaService(cmd)

	c.SetTestOptions(options.source_bucket, options.target_bucket, options.source_cluster_addr, options.target_cluster_addr, options.source_cluster_username, options.source_cluster_password, 2, 2)

	metadata_svc, err := s.DefaultMetadataSvc()
	if err != nil {
		return err
	}
	replication_manager.Initialize(metadata_svc, new(c.MockClusterInfoSvc), new(c.MockXDCRTopologySvc), new(c.MockReplicationSettingsSvc))
	logger_latency.Infof("Setup is done")
	return nil
}

func startGoXDCRReplicationByRest() error {

	cmd := exec.Command("curl", "-X POST http://localhost:12100/controller/createReplication -d fromBucket="+options.source_bucket+" -d uuid="+options.source_cluster_addr+" -d toBucket="+options.target_bucket+" -d xdcrSourceNozzlePerNode=2 -d xdcrTargetNozzlePerNode=2 -d xdcrLogLevel=Error")
	logger_latency.Infof("cmd =%v\n", cmd.Args)
	bytes, err := cmd.Output()
	if err != nil {
		logger_latency.Infof("err=%v, out=%v\n", err, bytes)
		return errors.New(err.Error() + " " + string(bytes))
	}

	return nil
}

func verify() {
	outliner := []string{}
	outliner_count := 0
	normals_count := 0
	normals_total := 0 * time.Millisecond
	normals_min := 0 * time.Millisecond
	normals_max := 0 * time.Millisecond

	for _, docinfo := range key_map {
		if docinfo.done {
			normals_count++
			normals_total = normals_total + docinfo.duration
			if normals_min == 0*time.Millisecond || docinfo.duration < normals_min {
				normals_min = docinfo.duration
			}

			if docinfo.duration > normals_max {
				normals_max = docinfo.duration
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
}
