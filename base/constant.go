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
var BPath = "/pools/default/b/"

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
var ClusterCompatibilityKey = "clusterCompatibility"
var ImplementationVersionKey = "implementationVersion"
var ServerListKey = "serverList"
var VBucketServerMapKey = "vBucketServerMap"
var VBucketMapKey = "vBucketMap"
var URIKey = "uri"
var SASLPasswordKey = "saslPassword"
var UUIDKey = "uuid"
var BucketCapabilitiesKey = "bucketCapabilities"
var BucketTypeKey = "bucketType"
var BucketsKey = "buckets"
var EvictionPolicyKey = "evictionPolicy"

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
	BANDWIDTH_THROTTLER_SVC    string = "BandwidthThrottlerSvc"
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
	RemoteClusterEncryptionType   = "encryptionType"
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
	UserAgent          = "User-Agent"
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
	VBUCKET_UUID_STAT_KEY_FORMAT       = "vb_%v:uuid"
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

// In order for dcp flow control to work correctly, the number of mutations in dcp buffer
// should be no larger than the size of the dcp data channel.
// This way we can ensure that gomemcached is never blocked on writing to data channel,
// and thus can always respond to dcp commands such as NOOP
// As of now, mutations that can fit into data chan have collective size of
// size > MinimumMutationSize * UprFeedDataChanLength = 60*20000 = 1.2M > UprFeedBufferSize
// This gives us the aforementioned assurance

// minimum size of a SetMeta/DelMeta mutation, which affects DCP buffer size to allocate
// a DCP mutation has size 54 + key + body. 60 should be a safe value to use
var MinimumMutationSize = 60

// length of data channel between dcp nozzle and gomemcached
var UprFeedDataChanLength = 20000

// dcp flow control buffer size
var UprFeedBufferSize uint32 = 1024 * 1024

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
	DataThrottledEventListener   = "DataThrottledEventListener"
)

const (
	OutNozzleStatsCollector  = "OutNozzleStatsCollector"
	DcpStatsCollector        = "DcpStatsCollector"
	RouterStatsCollector     = "RouterStatsCollector"
	CheckpointStatsCollector = "CheckpointStatsCollector"
	ThroughSeqnoTracker      = "ThroughSeqnoTracker"
)

var CouchbaseBucketType = "membase"
var EphemeralBucketType = "ephemeral"

// NRU eviction policy for ephemeral bucket
var EvictionPolicyNRU = "nruEviction"

// keys used in pipeline.settings
const (
	ProblematicVBSource = "ProblematicVBSource"
	ProblematicVBTarget = "ProblematicVBTarget"
	VBTimestamps        = "VBTimestamps"
)

// flag for requesting datatype in GetMeta request
var ReqExtMetaDataType = 0x02

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

// constants for parsing conflict resolution type setting in bucket metadata
const (
	ConflictResolutionTypeKey    = "conflictResolutionType"
	ConflictResolutionType_Seqno = "seqno"
	ConflictResolutionType_Lww   = "lww"
)

var UnexpectedEOF = "unexpected EOF"

// flag for memcached to enable lww to lww bucket replication
var FORCE_ACCEPT_WITH_META_OPS uint32 = 0x02

// read/write timeout for helo command to memcached
var HELOTimeout time.Duration = time.Duration(120) * time.Second

// minimum versions where various features are supported
var VersionForXmemSupport = []int{2, 2}
var VersionForSSLOverMemSupport = []int{3, 0}
var VersionForSANInCertificateSupport = []int{4, 0}
var VersionForRBACAndXattrSupport = []int{5, 0}

var GoxdcrUserAgentPrefix = "couchbase-goxdcr"
var GoxdcrUserAgent = ""

var RetryIntervalForPipelineUpdaterRefresh = 100 * time.Millisecond

// value representing tcp no delay feature in helo request/response
var HELO_FEATURE_TCP_NO_DELAY uint16 = 0x03

// value representing xattr feature in helo request/response
var HELO_FEATURE_XATTR uint16 = 0x06

// new XATTR bit in data type field in dcp mutations
var PROTOCOL_BINARY_DATATYPE_XATTR uint8 = 0x04

// number of time slots [in one second] to track for bandwidth throttling computation
var NumberOfSlotsForBandwidthThrottling = 10

// When doing bandwith throttling in xmem, set minNumberOfBytes = NumberOfBytes * FractionOfNumberOfBytesToSendAsMin
var FractionOfNumberOfBytesToSendAsMin float32 = 0.30

// max retry for metakv related operations
var MaxRetryMetakvOps = 5

// length of random id
var LengthOfRandomId = 16

// max retry for random id generation
var MaxRetryForRandomIdGeneration = 5

// --------------- Constants that are configurable -----------------

// timeout for checkpointing attempt before pipeline is stopped - to put an upper bound on the delay of pipeline stop/restart
var TimeoutCheckpointBeforeStop = 180 * time.Second

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
var MaxTopologyStableCountBeforeRestart = 20

// the max number of concurrent workers for checkpointing
var MaxWorkersForCheckpointing = 5

// capi nozzle data chan size is defined as batchCount*CapiDataChanSizeMultiplier
var CapiDataChanSizeMultiplier = 1

// interval for refreshing remote cluster references
var RefreshRemoteClusterRefInterval = 15 * time.Second

// max retry for capi batchUpdateDocs operation
var CapiMaxRetryBatchUpdateDocs = 6

// timeout for batch processing in capi
// 1. http timeout in revs_diff, i.e., batchGetMeta, call to target
// 2. overall timeout for batchUpdateDocs operation
var CapiBatchTimeout = 180 * time.Second

// timeout for tcp write operation in capi
var CapiWriteTimeout = 10 * time.Second

// timeout for tcp read operation in capi
var CapiReadTimeout = 60 * time.Second

func InitConstants(topologyChangeCheckInterval time.Duration, maxTopologyChangeCountBeforeRestart,
	maxTopologyStableCountBeforeRestart, maxWorkersForCheckpointing int,
	timeoutCheckpointBeforeStop time.Duration, capiDataChanSizeMultiplier int,
	refreshRemoteClusterRefInterval time.Duration, clusterVersion string,
	capiMaxRetryBatchUpdateDocs int, capiBatchTimeout time.Duration,
	capiWriteTimeout time.Duration, capiReadTimeout time.Duration) {
	TopologyChangeCheckInterval = topologyChangeCheckInterval
	MaxTopologyChangeCountBeforeRestart = maxTopologyChangeCountBeforeRestart
	MaxTopologyStableCountBeforeRestart = maxTopologyStableCountBeforeRestart
	MaxWorkersForCheckpointing = maxWorkersForCheckpointing
	TimeoutCheckpointBeforeStop = timeoutCheckpointBeforeStop
	CapiDataChanSizeMultiplier = capiDataChanSizeMultiplier
	RefreshRemoteClusterRefInterval = refreshRemoteClusterRefInterval
	if len(clusterVersion) > 0 {
		GoxdcrUserAgent = GoxdcrUserAgentPrefix + KeyPartsDelimiter + clusterVersion
	} else {
		GoxdcrUserAgent = GoxdcrUserAgentPrefix
	}
	CapiMaxRetryBatchUpdateDocs = capiMaxRetryBatchUpdateDocs
	CapiBatchTimeout = capiBatchTimeout
	CapiWriteTimeout = capiWriteTimeout
	CapiReadTimeout = capiReadTimeout
}
