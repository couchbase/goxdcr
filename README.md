couchbase_goxdcr_impl
=====================

To build:
1. go to project root dir
2. sh ./build.sh

This will build the entire project, including the test dirs, and put an executable for xdcr rest service named "xdcr" under root/bin

To start xdcr rest service:
1. go to root/bin
2. ./xdcr 

This will start xdcr rest service on the local machine at address localhost:12100

Optional input arguments to xdcr:
1. sourceKVAdminPort - adminport for source KV where xdcr is running on. Defaulted to 9000. 
If the xdcr instance is running on AWS instances, the source KV adminport may be different and need to be explicitly specified: 
./xdcr -sourceKVAdminPort=8091	
2. username - admin username of the cluster that xdcr is running. Defaulted to "Aministrator".
3. password - password of the admin user of the cluster that xdcr is running. Defaulted to "welcome".
4. xdcrRestPort - port number for xdcr rest server. Defaulted to 12100. Generally there is no need to change it.
5. gometaRequestPort - request port number for gometa service. Defaulted to 11000. Generally there is no need to change it.
6. isEnterprise - whether couchbase is of enterprise edition. Defaulted to true.
7. isConvert - whether xdcr is running in convert/upgrade mode. Defaulted to false.


To send requests to xdcr rest service:
1. To create remote cluster reference: "curl -X POST http://localhost:12100/pools/default/remoteClusters -d ..."
	(1) required parameters: 
		(a) name, string, e.g., "remote"
		(b) hostname, string, e.g., "127.0.0.1:9000"
		(c) username, string, e.g., "Administrator"
		(d) password, string, e.g., "welcome"
	(2) optional parameters: 
		(a) demandEncryption, bool; when demandEncryption is set to true, a certificate must be provided as follows:
		    -d demandEncryption=1 --data-urlencode "certificate=$(cat remoteCert.pem)"
2. To look up existsing remote cluster references:  "curl -X GET http://localhost:12100/pools/default/remoteClusters"
3. To delete remote cluster reference: "curl -X Delete  http://localhost:12100/pools/default/remoteClusters/<remote cluster name>"

4. To create replication: "curl -X POST http://localhost:12100/controller/createReplication -d ..."
If the replication is created successfully, a replication id will be returned, which can be used to access the same replication in replication specific rest apis.
	(1) required parameters: 
		(a) toCluster, string, name of the remote cluster reference, e.g., "remote"
		(b) fromBucket, string, e.g., "default"
		(c) toBucket, string, e.g., "target"
		(d) filterName, string, e.g., "myActive"
	(2) optional parameters. Optionally, the following replication settings can be passed in to fine tune replication behavior
		(a) type, string, type of replication protocol, i.e., "xmem"/"capi"
		(b) filterExpression, string, e.g., "default-1.*"
		(c) pausedRequested, bool, whether the replications needs to be paused
		(d) checkpointInterval, int, the interval for checkpointing in seconds, range: 60-14400
		(e) workerBatchSize, int, the number of mutations in a batch, range: 500-10000
		(f) docBatchSizeKb, int, the size of a batch in KB, range: 10-10000
		(g) failureRestartInterval, int, the number of seconds to wait after failure before restarting replication, range: 1-300
		(h) optimisticReplicationThreshold, int, documents with size less than this threshold (in KB) will be replicated optimistically, range: 0-20*1024
 		(i) httpConnections, int, the number of maximum simultaneous HTTP connections, range: 1-100
 		(j) sourceNozzlePerNode, int, the number of source nozzles per source node, range: 1-10
 		(k) targetNozzlePerNode, int, the number of outgoing nozzles per target node, range: 1-10
 		(l) maxExpectedReplicationLag, int, the maximum replication lag (in millisecond) that can be tolerated before it is considered timeout
 		(m) timeoutPercentageCap, int, the maximum allowed timeout percentage. If this limit is exceeded, replication is considered as not healthy and may be restarted.
 		(n) logLevel, string, the level of logging, i.e., "Error"/"Info"/"Debug"/"Trace"
 		(o) statsInterval, int, the interval (in milliseconds) for statistics updates
 
5. To view replication settings for a replication: "curl -X GET http://localhost:12100/settings/replications/<replication id>"
6. To change replication settings for a replication: "curl -X POST http://localhost:12100/settings/replications/<replication id> -d ..."
Any of the replication settings in 4.(2) can be specified.
7. To pause a replication: "curl -X POST http://localhost:12100/settings/replications/<replication id> -d pauseRequested=true"
8. To resume a replication: "curl -X POST http://localhost:12100/settings/replications/<replication id> -d pauseRequested=false"
9. To delete a replication: "curl -X Delete http://localhost:12100/settings/replications/<replication id>"


10. To view default settings for all replications: "curl -X GET http://localhost:12100/settings/replications"
11. To change default settings for all replications: "curl -X POST http://localhost:12100/settings/replications -d ..."
Any of the replication settings in 4.(2) can be specified.


12. To view internal settings for all replications: "curl -X GET http://localhost:12100/internalSettings"
13. To change internal settings for all replications: "curl -X POST http://localhost:12100/internalSettings -d ..."
Any of the replication settings in 4.(2) can be specified. 
Note that the name of each replication setting in 4.(2) will have to take a new form here, i.e., by changing the first letter to capital letter and then prefixing with "xdcr". 
For example, for checkpointInterval, use xdcrCheckpointInterval instead. => "curl -X POST http://localhost:12100/internalSettings -d xdcrCheckpointInterval=1000"
The reason for this is that internalSettings/ is a generic purpose API which is supposed to return both xdcr and non-xdcr settings. The "xdcr" prefix for 
xdcr settings are useful in separating them from non-xdcr settings. /settings/replications, on the other hand, is a xdcr specific API. The "xdcr" prefix
would have been redundant there.

14. To get statistics: "curl -X GET http://localhost:12100/stats/buckets/<bucket name>"
15. To get detail stats and additional debugging information "curl -X GET http://localhost:12100/debug/vars"

