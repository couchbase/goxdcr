// unit test for xdcr pipeline factory.
package main

import (
	"flag"
	"fmt"
	factory "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/factory"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/parts"
	sp "github.com/ysui6888/indexing/secondary/projector"
	//	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/base"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/metadata"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/utils"
	//	"github.com/couchbaselabs/go-couchbase"
	"errors"
	"github.com/couchbaselabs/go-couchbase"
	"os"
)

var options struct {
	sourceBucket    string // source bucket
	targetBucket    string //target bucket
	connectStr      string //connect string
	numConnPerKV    int    // number of connections per source KV node
	numOutgoingConn int    // number of connections to target cluster
	username        string //username
	password        string //password
	maxVbno         int    // maximum number of vbuckets
}

const (
	TEST_TOPIC      = "test"
	NUM_SOURCE_CONN = 2
	NUM_TARGET_CONN = 3
)

func argParse() {

	flag.StringVar(&options.sourceBucket, "source_bucket", "default",
		"bucket to replicate from")
	flag.IntVar(&options.maxVbno, "maxvb", 8,
		"maximum number of vbuckets")
	flag.StringVar(&options.targetBucket, "target_bucket", "target",
		"bucket to replicate to")
	flag.IntVar(&options.numConnPerKV, "numConnPerKV", NUM_SOURCE_CONN,
		"number of connections per kv node")
	flag.IntVar(&options.numOutgoingConn, "numOutgoingConn", NUM_TARGET_CONN,
		"number of outgoing connections to target")

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
	fmt.Println("Start Testing ...")
	argParse()
	fmt.Printf("connectStr=%s\n", options.connectStr)
	fmt.Println("Done with parsing the arguments")
	err := invokeFactory()
	if err == nil {
		fmt.Println("Test passed.")
	} else {
		fmt.Println(err)
	}
}

func invokeFactory() error {
	msvc := &mockMetadataSvc{}
	mcisvc := &mockClusterInfoSvc{}
	mxtsvc := &mockXDCRTopologySvc{}

	fac := factory.NewXDCRFactory(msvc, mcisvc, mxtsvc)

	pl, err := fac.NewPipeline(TEST_TOPIC)
	if err != nil {
		return err
	}

	sources := pl.Sources()
	targets := pl.Targets()

	if len(sources) != NUM_SOURCE_CONN {
		return errors.New(fmt.Sprintf("incorrect source nozzles. expected %v; actual %v", NUM_SOURCE_CONN, len(sources)))
	}
	if len(targets) != NUM_TARGET_CONN {
		return errors.New(fmt.Sprintf("incorrect target nozzles. expected %v; actual %v", NUM_TARGET_CONN, len(targets)))
	}
	for sourceId, source := range sources {
		_, ok := source.(*sp.KVFeed)
		if !ok {
			return errors.New(fmt.Sprintf("incorrect nozzle type for source nozzle %v.", sourceId))
		}

		// validate connector in source nozzles
		connector := source.Connector()
		if connector == nil {
			return errors.New(fmt.Sprintf("no connector defined in source nozzle %v.", sourceId))
		}
		_, ok = connector.(*parts.Router)
		if !ok {
			return errors.New(fmt.Sprintf("incorrect connector type in source nozzle %v.", sourceId))
		}
		downStreamParts := source.Connector().DownStreams()
		if len(downStreamParts) != NUM_TARGET_CONN {
			return errors.New(fmt.Sprintf("incorrect number of downstream parts for source nozzle %v. expected %v; actual %v", sourceId, NUM_TARGET_CONN, len(downStreamParts)))
		}
		for partId := range downStreamParts {
			if _, ok := targets[partId]; !ok {
				return errors.New(fmt.Sprintf("invalid downstream part %v for source nozzle %v.", partId, sourceId))
			}
		}
	}
	//validate that target nozzles do not have connectors
	for targetId, target := range targets {
		_, ok := target.(*parts.XmemNozzle)
		if !ok {
			return errors.New(fmt.Sprintf("incorrect nozzle type for target nozzle %v.", targetId))
		}
		if target.Connector() != nil {
			return errors.New(fmt.Sprintf("target nozzle %v has connector, which is invalid.", targetId))
		}
	}

	return nil
}

type mockMetadataSvc struct {
}

func (mock_meta_svc *mockMetadataSvc) ReplicationSpec(replicationId string) (*metadata.ReplicationSpecification, error) {
	spec := metadata.NewReplicationSpecification(options.connectStr, options.sourceBucket, options.connectStr, options.targetBucket, "")
	settings := spec.Settings()
	settings.SetTargetNozzlesPerNode(options.numOutgoingConn)
	settings.SetSourceNozzlesPerNode(options.numConnPerKV)
	return spec, nil
}

func (mock_meta_svc *mockMetadataSvc) AddReplicationSpec(spec metadata.ReplicationSpecification) error {
	return nil
}

func (mock_meta_svc *mockMetadataSvc) SetReplicationSpec(spec metadata.ReplicationSpecification) error {
	return nil
}

func (mock_meta_svc *mockMetadataSvc) DelReplicationSpec(replicationId string) error {
	return nil
}

type mockClusterInfoSvc struct {
}

func (mock_ci_svc *mockClusterInfoSvc) GetClusterConnectionStr(ClusterUUID string) (string, error) {
	return options.connectStr, nil
}

func (mock_ci_svc *mockClusterInfoSvc) GetMyActiveVBuckets(ClusterUUID string, bucketName string, NodeId string) ([]uint16, error) {
	sourceCluster, err := mock_ci_svc.GetClusterConnectionStr(ClusterUUID)
	if err != nil {
		return nil, err
	}
	b, err := utils.Bucket(sourceCluster, bucketName, options.username, options.password)
	if err != nil {
		return nil, err
	}

	// in test env, there should be only one kv in bucket server list
	kvaddr := b.VBServerMap().ServerList[0]

	m, err := b.GetVBmap([]string{kvaddr})
	if err != nil {
		return nil, err
	}

	vbList := m[kvaddr]

	return vbList, nil
}

func (mock_ci_svc *mockClusterInfoSvc) GetServerList(ClusterUUID string, bucketName string) ([]string, error) {
	cluster, err := mock_ci_svc.GetClusterConnectionStr(ClusterUUID)
	if err != nil {
		return nil, err
	}
	bucket, err := utils.Bucket(cluster, bucketName, options.username, options.password)
	if err != nil {
		return nil, err
	}

	// in test env, there should be only one kv in bucket server list
	serverlist := bucket.VBServerMap().ServerList

	return serverlist, nil
}

func (mock_ci_svc *mockClusterInfoSvc) GetServerVBucketsMap(ClusterUUID string, bucketName string) (map[string][]uint16, error) {
	cluster, err := mock_ci_svc.GetClusterConnectionStr(ClusterUUID)
	fmt.Printf("cluster=%s\n", cluster)
	if err != nil {
		return nil, err
	}
	bucket, err := utils.Bucket(cluster, bucketName, options.username, options.password)
	if err != nil {
		return nil, err
	}
	fmt.Printf("ServerList=%v\n", bucket.VBServerMap().ServerList)
	serverVBMap, err := bucket.GetVBmap(bucket.VBServerMap().ServerList)
	fmt.Printf("ServerVBMap=%v\n", serverVBMap)
	return serverVBMap, err
}

func (mock_ci_svc *mockClusterInfoSvc) IsNodeCompatible(node string, version string) (bool, error) {
	return true, nil
}

func (mock_ci_svc *mockClusterInfoSvc) GetBucket(clusterUUID, bucketName string) (*couchbase.Bucket, error) {
	clusterConnStr, err := mock_ci_svc.GetClusterConnectionStr(clusterUUID)
	if err != nil {
		return nil, err
	}
	return utils.Bucket(clusterConnStr, bucketName, options.username, options.password)
}


type mockXDCRTopologySvc struct {
}

func (mock_top_svc *mockXDCRTopologySvc) MyHost() (string, error) {
	return options.connectStr, nil
}

func (mock_top_svc *mockXDCRTopologySvc) MyAdminPort() (uint16, error) {
	return 0, nil
}

func (mock_top_svc *mockXDCRTopologySvc) MyKVNodes() ([]string, error) {
	mock_ci_svc := &mockClusterInfoSvc{}
	nodes, err := mock_ci_svc.GetServerList(options.connectStr, "default")
	return nodes, err
}

func (mock_top_svc *mockXDCRTopologySvc) XDCRTopology() (map[string]uint16, error) {
	retmap := make(map[string]uint16)
	return retmap, nil
}

func (mock_top_svc *mockXDCRTopologySvc) XDCRCompToKVNodeMap() (map[string][]string, error) {
	retmap := make(map[string][]string)
	return retmap, nil
}

func (mock_top_svc *mockXDCRTopologySvc) MyCluster() (string, error) {
	return options.connectStr, nil
}
