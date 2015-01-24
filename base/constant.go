// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package base

import (
	"errors"
)

//constants

var DefaultConnectionSize = 5
var DefaultCAPIConnectionSize = 5
var DefaultPoolName = "default"
var AuditServicePoolName = "auditService"

var LocalHostName = "127.0.0.1"

// URL Paths for retrieving cluster info
var PoolsPath = "/pools"
var DefaultPoolPath = "/pools/default"
var NodesPath = "/pools/nodes"
var NodesSelfPath = "/nodes/self"
var SSLPortsPath = "/nodes/self/xdcrSSLPorts"
var NodeServicesPath = "/pools/default/nodeServices"


// constants for CAPI nozzle
var RevsDiffPath = "/_revs_diff"
var BulkDocsPath = "/_bulk_docs"

//keys in the map which /nodes/self returns
var CouchApiBase = "couchApiBase"
var CouchApiBaseHttps = "couchApiBaseHTTPS"

// keys used in parsing cluster info
var NodesKey = "nodes"
var HostNameKey = "hostname"
var ThisNodeKey = "thisNode"
var SSLPortKey = "httpsMgmt" 
var PortsKey = "ports"
var DirectPortKey = "direct"
var ProxyPortKey ="proxy"
var SSLProxyPortKey = "sslProxy"
var NodeExtKey = "nodesExt"
var KVPortKey = "kv"
var KVSSLPortKey = "kvSSL"
var ServicesKey= "services"

// URL related constants
var UrlDelimiter = "/"
var UrlPortNumberDelimiter = ":"

// http request method types
const (
	MethodGet = "GET"
	MethodPost = "POST"
	MethodDelete = "DELETE"
)

// delimiter for multiple parts in a key
var KeyPartsDelimiter = "/"

//constants for adminport
var AdminportUrlPrefix = UrlDelimiter

// used as default value for tests
var AdminportNumber uint16 = 13000
var GometaRequestPortNumber uint16 = 11000

// AdminportReadTimeout timeout, in milliseconds, is read timeout for
// golib's http server.
var AdminportReadTimeout = 0
// AdminportWriteTimeout timeout, in milliseconds, is write timeout for
// golib's http server.
var AdminportWriteTimeout = 0

//outgoing nozzle type
type XDCROutgoingNozzleType int

const (
	Xmem XDCROutgoingNozzleType = iota
	Capi XDCROutgoingNozzleType = iota
)

const (
	PIPELINE_SUPERVISOR_SVC string = "PipelineSupervisor"
	CHECKPOINT_MGR_SVC string = "CheckpointManager"
	STATISTICS_MGR_SVC string = "StatisticsManager"
	TOPOLOGY_CHANGE_DETECT_SVC string = "TopologyChangeDetectSvc"
)

// supervisor related constants
const(
	ReplicationManagerSupervisorId = "ReplicationManagerSupervisor"
	PipelineMasterSupervisorId = "PipelineMasterSupervisor"
	AdminportSupervisorId = "AdminportSupervisor"
	PipelineSupervisorIdPrefix = "PipelineSupervisor_"
)

// constants for integer parsing
var ParseIntBase    = 10
var ParseIntBitSize = 64

var ErrorNotMyVbucket = errors.New("NOT_MY_VBUCKET")

var InvalidStateTransitionErrMsg = "Can't move to state %v - %v's current state is %v, can only move to state [%v]"


// constants used for remote cluster references
const (
	RemoteClustersPath  = "pools/default/remoteClusters"
	
	RemoteClusterUuid   = "uuid"
	RemoteClusterName  = "name"
	RemoteClusterHostName = "hostname"
	RemoteClusterUserName = "username"
	RemoteClusterPassword = "password"
	RemoteClusterDemandEncryption = "demandEncryption"
	RemoteClusterCertificate = "certificate"
	RemoteClusterUri = "uri"
	RemoteClusterValidateUri = "validateURI"
	RemoteClusterDeleted = "deleted"
	IsEnterprise = "isEnterprise"
)

// constant used by more than one rest apis
const (
 	JustValidate = "just_validate"
 	JustValidatePostfix = "?" + JustValidate + "=1"
)

// Key used for general validation errors that are not related to any specific http request parameters
var PlaceHolderFieldKey = "_"  

// http request related constants
const (
	ContentType = "Content-Type"
	DefaultContentType = "application/x-www-form-urlencoded"
	JsonContentType = "application/json"
	ContentLength = "Content-Length"
)

//constant for replication tasklist status
const (
	Pending = "Pending"
	Replicating = "Replicating"
	Paused = "Paused"
)

const (
	//Bucket sequence number statistics
	VBUCKET_SEQNO_STAT_NAME            = "vbucket-seqno"
	VBUCKET_HIGH_SEQNO_STAT_KEY_FORMAT = "vb_%v:high_seqno"

)

var ErrorsStatsKey = "Errors"

// ui log related constants
var UILogPath = "_log"
var UILogRetry = 3
var UILogMessageKey = "message"
var UILogLogLevelKey = "logLevel"
var UILogComponentKey = "component"
// so far all xdcr logs are of info level, thus we can hardcode it here
var UILogXDCRLogLevel = "info"
var UILogXDCRComponent = "xdcr"

var CouchApiBaseUriDelimiter = "%2f"

var XDCR_EXPVAR_ROOT = "XDCR_Replications"
//constants for replication docs
const (
	RemoteClustersForReplicationDoc = "remoteClusters"
	BucketsPath                     = "buckets"

	ReplicationDocType           = "type"
	ReplicationDocId             = "id"
	ReplicationDocSource         = "source"
	ReplicationDocTarget         = "target"
	ReplicationDocContinuous     = "continuous"
	ReplicationDocPauseRequested = "pause_requested"
	ReplicationDocPauseRequestedOutput = "pauseRequested"

	ReplicationDocTypeXmem = "xdc-xmem"
	ReplicationDocTypeCapi = "xdc"
)

// constant used in replication info to ensure compatibility with erlang xdcr
var MaxVBReps = "max_vbreps"
