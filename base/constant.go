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
	mc "github.com/couchbase/gomemcached"
	"time"
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
var DefaultPoolBucketsPath = "/pools/default/buckets/"
var NodesPath = "/pools/nodes"
var NodesSelfPath = "/nodes/self"
var SSLPortsPath = "/nodes/self/xdcrSSLPorts"
var NodeServicesPath = "/pools/default/nodeServices"
var BPath = "/pools/default/b"

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
var ProxyPortKey = "proxy"
var SSLProxyPortKey = "sslProxy"
var NodeExtKey = "nodesExt"
var KVPortKey = "kv"
var KVSSLPortKey = "kvSSL"
var ServicesKey = "services"

// URL related constants
var UrlDelimiter = "/"
var UrlPortNumberDelimiter = ":"

// http request method types
const (
	MethodGet    = "GET"
	MethodPost   = "POST"
	MethodDelete = "DELETE"
)

// delimiter for multiple parts in a key
var KeyPartsDelimiter = "/"

//constants for adminport
var AdminportUrlPrefix = UrlDelimiter

// used as default value for tests
var AdminportNumber uint16 = 13000
var GometaRequestPortNumber uint16 = 11000

// read timeout for golib's http server.
var AdminportReadTimeout = 1 * time.Second

// write timeout for golib's http server.
var AdminportWriteTimeout = 180 * time.Second

// default time out for outgoing http requests if it is not explicitly specified
var DefaultHttpTimeout = 180 * time.Second

// short timeout
// when we need to make a rest call when processing a XDCR rest request, the time out of the second rest call needs
// to be shorter than that of the first one, which is currently 30 seconds.
var ShortHttpTimeout = 20 * time.Second

//outgoing nozzle type
type XDCROutgoingNozzleType int

const (
	Xmem XDCROutgoingNozzleType = iota
	Capi XDCROutgoingNozzleType = iota
)

const (
	PIPELINE_SUPERVISOR_SVC    string = "PipelineSupervisor"
	CHECKPOINT_MGR_SVC         string = "CheckpointManager"
	STATISTICS_MGR_SVC         string = "StatisticsManager"
	TOPOLOGY_CHANGE_DETECT_SVC string = "TopologyChangeDetectSvc"
)

// supervisor related constants
const (
	ReplicationManagerSupervisorId = "ReplicationManagerSupervisor"
	PipelineMasterSupervisorId     = "PipelineMasterSupervisor"
	AdminportSupervisorId          = "AdminportSupervisor"
	PipelineSupervisorIdPrefix     = "PipelineSupervisor_"
)

// metadata change listener related constants
const (
	ReplicationSpecChangeListener  = "ReplicationSpecChangeListener"
	RemoteClusterChangeListener    = "RemoteClusterChangeListener"
	GlobalSettingChangeListener    = "GlobalSettingChangeListener"
	BucketSettingsChangeListener   = "BucketSettingsChangeListener"
	InternalSettingsChangeListener = "InternalSettingsChangeListener"
)

// constants for integer parsing
var ParseIntBase = 10
var ParseIntBitSize = 64

var ErrorNotMyVbucket = errors.New("NOT_MY_VBUCKET")

var InvalidStateTransitionErrMsg = "Can't move to state %v - %v's current state is %v, can only move to state [%v]"

// constants used for remote cluster references
const (
	RemoteClustersPath = "pools/default/remoteClusters"

	RemoteClusterUuid             = "uuid"
	RemoteClusterName             = "name"
	RemoteClusterHostName         = "hostname"
	RemoteClusterUserName         = "username"
	RemoteClusterPassword         = "password"
	RemoteClusterDemandEncryption = "demandEncryption"
	RemoteClusterCertificate      = "certificate"
	RemoteClusterUri              = "uri"
	RemoteClusterValidateUri      = "validateURI"
	RemoteClusterDeleted          = "deleted"
	IsEnterprise                  = "isEnterprise"
	Pools                         = "pools"
)

// constants used for create replication request
const (
	FromBucket = "fromBucket"
	ToCluster  = "toCluster"
	ToBucket   = "toBucket"
)

// constant used by more than one rest apis
const (
	JustValidate        = "just_validate"
	JustValidatePostfix = "?" + JustValidate + "=1"
)

//const used by block profile
const (
	BlockProfileRate = "block_profile_rate"
)

// Key used for general validation errors that are not related to any specific http request parameters
var PlaceHolderFieldKey = "_"

// http request related constants
const (
	ContentType        = "Content-Type"
	DefaultContentType = "application/x-www-form-urlencoded"
	JsonContentType    = "application/json"
	ContentLength      = "Content-Length"
)

//constant for replication tasklist status
const (
	Pending     = "Pending"
	Replicating = "Replicating"
	Paused      = "Paused"
)

const (
	//Bucket sequence number statistics
	VBUCKET_SEQNO_STAT_NAME            = "vbucket-seqno"
	VBUCKET_HIGH_SEQNO_STAT_KEY_FORMAT = "vb_%v:high_seqno"
	DCP_STAT_NAME                      = "dcp"
	DCP_XDCR_STATS_PREFIX              = "eq_dcpq:xdcr:"
	DCP_XDCR_ITEMS_REMAINING_SUFFIX    = ":items_remaining"
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

	ReplicationDocType                 = "type"
	ReplicationDocId                   = "id"
	ReplicationDocSource               = "source"
	ReplicationDocTarget               = "target"
	ReplicationDocContinuous           = "continuous"
	ReplicationDocPauseRequested       = "pause_requested"
	ReplicationDocPauseRequestedOutput = "pauseRequested"

	ReplicationDocTypeXmem = "xdc-xmem"
	ReplicationDocTypeCapi = "xdc"
)

// constant used in replication info to ensure compatibility with erlang xdcr
var MaxVBReps = "max_vbreps"

var InvalidCerfiticateError = errors.New("certificate must be a single, PEM-encoded x509 certificate and nothing more (failed to parse given certificate)")

const (
	GET_WITH_META    = mc.CommandCode(0xa0)
	SET_WITH_META    = mc.CommandCode(0xa2)
	DELETE_WITH_META = mc.CommandCode(0xa8)
	SET_TIME_SYNC    = mc.CommandCode(0xc1)
)

const (
	PipelineSetting_RequestPool     = "RequestPool"
	DefaultRequestPoolSize          = 10000
	HTTP_RETRIES                int = 5
)

var UprFeedDataChanLength = 1000

var EventChanSize = 10000

// number of async listeners [for an event type]
var MaxNumberOfAsyncListeners = 4

// names of async component event listeners
const (
	DataReceivedEventListener    = "DataReceivedEventListener"
	DataProcessedEventListener   = "DataProcessedEventListener"
	DataFilteredEventListener    = "DataFilteredEventListener"
	DataSentEventListener        = "DataSentEventListener"
	DataFailedCREventListener    = "DataFailedCREventListener"
	GetMetaReceivedEventListener = "GetMetaReceivedEventListener"
)

const (
	OutNozzleStatsCollector  = "OutNozzleStatsCollector"
	DcpStatsCollector        = "DcpStatsCollector"
	RouterStatsCollector     = "RouterStatsCollector"
	CheckpointStatsCollector = "CheckpointStatsCollector"
	ThroughSeqnoTracker      = "ThroughSeqnoTracker"
)

var CouchbaseBucketType = "membase"

// keys used in pipeline.settings
const (
	ProblematicVBSource = "ProblematicVBSource"
	ProblematicVBTarget = "ProblematicVBTarget"
	VBTimestamps        = "VBTimestamps"
)

// version of extended metadata to look for
var ExtendedMetadataVersion = 1

// id of conflict resolution mode in extended metadata
var ConflictResolutionModeId = 2

// permissions for RBAC
const (
	PermissionRemoteClusterRead       = "cluster.xdcr.remote_clusters!read"
	PermissionRemoteClusterWrite      = "cluster.xdcr.remote_clusters!write"
	PermissionXDCRSettingsRead        = "cluster.xdcr.settings!read"
	PermissionXDCRSettingsWrite       = "cluster.xdcr.settings!write"
	PermissionBucketPrefix            = "cluster.bucket["
	PermissionBucketXDCRReadSuffix    = "].xdcr!read"
	PermissionBucketXDCRWriteSuffix   = "].xdcr!write"
	PermissionBucketXDCRExecuteSuffix = "].xdcr!execute"
	PermissionXDCRInternalRead        = "cluster.admin.internal.xdcr!read"
	PermissionXDCRInternalWrite       = "cluster.admin.internal.xdcr!write"
)

// constants for parsing time synchronization setting in bucket metadata
const (
	TimeSynchronizationKey       = "timeSynchronization"
	TimeSynchronization_Disabled = "disabled"
)

// --------------- Constants that are configurable -----------------
var TopologyChangeCheckInterval = 10 * time.Second

// the maximum number of topology change checks to wait before pipeline is restarted
// to elaborate:
// 1. topology change has happened  - the current topology is not the same as the topology when pipeline was first started
// 2. we have performed max_topology_change_count_before_restart topology change checks since the topology change was first seen
// then we restart the pipeline.
// this puts an upper bound on the delay on pipeline restart
var MaxTopologyChangeCountBeforeRestart = 30

// the maximum number of consecutive stable topology seen before pipeline is restarted
// to elaborate:
// 1. topology change has happened before  - the current topology is not the same as the topology when pipeline was first started
// 2. there has been no topology change in the past max_topology_stable_count_before_restart topology change checks
// then we assume that the topology change has completed, and restart the pipeline
var MaxTopologyStableCountBeforeRestart = 30

// the max number of concurrent workers for checkpointing
var MaxWorkersForCheckpointing = 5

// timeout for checkpointing attempt due to topology changes - to put an upper bound on the delay of pipeline restartx
var TopologyChangeCheckpointTimeout = 10 * time.Minute

func InitConstants(topologyChangeCheckInterval time.Duration, maxTopologyChangeCountBeforeRestart,
	maxTopologyStableCountBeforeRestart, maxWorkersForCheckpointing int, topologyChangeCheckpointTimeout time.Duration) {
	TopologyChangeCheckInterval = topologyChangeCheckInterval
	MaxTopologyChangeCountBeforeRestart = maxTopologyChangeCountBeforeRestart
	MaxTopologyStableCountBeforeRestart = maxTopologyStableCountBeforeRestart
	MaxWorkersForCheckpointing = maxWorkersForCheckpointing
	TopologyChangeCheckpointTimeout = topologyChangeCheckpointTimeout
}
