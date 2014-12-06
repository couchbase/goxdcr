couchbase_goxdcr_impl
=====================

To build:
1. go to project root dir
2. sh ./build.sh

This will build the entire project, including the test dirs, and put an executable for xdcr rest service named "xdcr" under root/bin

To start xdcr rest service:
1. go to root/bin
2. ./xdcr 

This will start xdcr rest service on the local machine at address 127.0.0.1:12100

To send requests to xdcr rest service:
1. To create remote cluster reference: "curl -X POST http://127.0.0.1:12100/pools/default/remoteClusters -d name=... -d hostname=... -d username=... -d password=... -d demandEncryption=... -d certificate=...
2. To create replication: "curl -X POST http://127.0.0.1:12100/controller/createReplication -d fromBucket=default -d toBucket=beer-sample - -d toCluster=remoteName -d sourceNozzlePerNode=... -d targetNozzlePerNode=... -d workerBatchSize=... -d logLevel=Info -d statsInterval=1000"
	note: -toCluster is the remote cluster reference name you passed in as "name" when creating remote cluster reference
		  -statsInterval is the stats collection interval in millisecond
4. To delete replication: "curl -X DELETE http://127.0.0.1:12100/controller/cancelXDCR/..."
5. To view replication settings: "curl -X GET http://127.0.0.1:12100/settings/replications/..."
6. To change replication settings: "curl -X POST http://127.0.0.1:12100/settings/replications/... -d xdcrWorkerBatchSize=... ..."
7. To get statistics: "curl -X GET http://127.0.0.1:12100/stats/buckets/<bucket name>"
8. To get detail stats and additional debugging information "curl -X GET http://127.0.0.1:12100/debug/vars"

If the xdcr instance is to be started on AWS instances, the source cluster addr and source KV host need to be explicitly specified: 
For instance:
./xdcr -sourceClusterAddr=ec2-54-160-164-226.compute-1.amazonaws.com:8091 -sourceKVHost=ec2-54-160-164-226.compute-1.amazonaws.com


