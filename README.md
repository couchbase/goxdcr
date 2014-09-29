couchbase_goxdcr_impl
=====================

To build:
1. go to project root dir
2. sh ./build.sh

This will build the entire project, including the test dirs, and put an executable for xdcr rest service named "xdcr" under root/bin

To start xdcr rest service:
1. go to root/bin
2. ./xdcr localhost

This will start xdcr rest service at address localhost:12100

To send requests to xdcr rest service:
1. To create replication: "curl -X POST http://localhost:12100/controller/createReplication -d fromBucket=... -d toCluster=... -d toBucket=..." 
2. To delete replication: "curl -X DELETE http://localhost:12100/controller/cancelXDCR/..."
3. To view replication settings: "curl -X GET http://localhost:12100/settings/replications/..."
4. To change replication settings: "curl -X POST http://localhost:12100/settings/replications/... -d xdcrBatchCount=... ..."
5. To get statistics: "curl -X GET http://localhost:12100/stats"

