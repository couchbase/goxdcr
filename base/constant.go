// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package base

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	mc "github.com/couchbase/gomemcached"
)

//constants

var DefaultConnectionSize = 5
var DefaultCAPIConnectionSize = 5
var DefaultPoolName = "default"
var AuditServicePoolName = "auditService"

const AuditWrongCertificateErr = "certificate signed by unknown authority, statusCode=0"
const AuditStatusFmt = "statusCode=%v"

var LocalHostName = "127.0.0.1"
var LocalHostNameIpv6 = "[::1]"

var IpFamilyOnlyErrorMessage = fmt.Sprintf("The cluster is %v only. ", IpFamilyStr)
var AddressNotAllowedErrorMessageFmt = "The address %v is not allowed."
var IpFamilyAddressNotFoundMessageFmt = "Cannot find address in the ip family for %v."

var DefaultAdminPort uint16 = 8091
var DefaultAdminPortSSL uint16 = 18091

const RESTInvalidPath = "Invalid path"
const RESTHttpReq = "in http request"
const RESTNoSuchHost = "no such host"
const RESTHttpChunkedEncoding = "chunked"
const RESTNsServerNotFound = "Not found"

// Exponential backoff factor
var MetaKvBackoffFactor = 2

// URL Paths for retrieving cluster info
const XDCRPrefix = "xdcr"
const PoolsPath = "/pools"
const DefaultPoolPath = "/pools/default"
const DefaultPoolBucketsPath = "/pools/default/buckets/"
const NodesSelfPath = "/nodes/self"
const SSLPortsPath = "/nodes/self/xdcrSSLPorts"
const NodeServicesPath = "/pools/default/nodeServices"
const XDCRPeerToPeerPath = XDCRPrefix + "/p2pCommunications"
const BPath = "/pools/default/b/"
const DocsPath = "/docs/"
const CollectionsManifestPath = "/scopes"
const ScopesPath = "/scopes/"
const CollectionsPath = "/collections/"
const ClientCertAuthPath = "/settings/clientCertAuth"
const TerseClusterInfoPath = "/pools/default/terseClusterInfo"
const XDCRClusterToClusterPath = XDCRPrefix + "/c2cCommunications"

// Streaming API paths. They are used for source clusters only
const ObservePoolPath = "/poolsStreaming/default"
const ObserveBucketPath = "/pools/default/bucketsStreaming/" // + bucketName

// constants for CAPI nozzle
var RevsDiffPath = "/_revs_diff"
var BulkDocsPath = "/_bulk_docs"

// keys in the map which /nodes/self returns
var CouchApiBase = "couchApiBase"
var CouchApiBaseHttps = "couchApiBaseHTTPS"

// REST endpoint for connection pre-check
const XDCRConnectionPreCheckPath = XDCRPrefix + "/connectionPreCheck"

// ns_server REST endpoint to perform quick user authentication check (user can check their id (or username), domain, roles, and other details)
const WhoAmIPath = "/whoami"

// keys used in parsing cluster info
var NodesKey = "nodes"
var HostNameKey = "hostname"
var ClusterNameKey = "clusterName"
var ThisNodeKey = "thisNode"
var SSLPortKey = "httpsMgmt"
var SSLMgtPortKey = "mgmtSSL"
var MgtPortKey = "mgmt"
var PortsKey = "ports"
var DirectPortKey = "direct"
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
var UUIDKey = "uuid"
var BucketCapabilitiesKey = "bucketCapabilities"
var BucketTypeKey = "bucketType"
var BucketsKey = "buckets"
var EvictionPolicyKey = "evictionPolicy"
var BucketNameKey = "name"
var ClusterMembershipKey = "clusterMembership"
var AlternateKey = "alternateAddresses"
var ExternalKey = "external"
var CapiPortKey = "capi"
var CapiSSLPortKey = "capiSSL"
var DeveloperPreviewKey = "isDeveloperPreview"
var StatusKey = "status"
var NumberOfReplicas = "numReplicas"
var StorageBackendKey = "storageBackend"
var OrchestratorNodeKey = "orchestrator"
var OtpNodeKey = "otpNode"
var NumVBucketsKey = "numVBuckets"

// Value for StorageBackendKey
var Magma = "magma"
var Couchstore = "couchstore"

// Collection consts
const UIDKey = "uid"
const NameKey = "name"
const CollectionsKey = "collections"
const DefaultScopeCollectionName = "_default"
const ScopeCollectionDelimiter = "."
const JsonDelimiter = ":"
const ManualBackfillKey = "manualBackfill"
const CollectionsDelAllBackfillKey = "delAllBackfills"
const CollectionsDelSingleVBBackfillKey = "delSpecificBackfillForVb"
const CollectionsManifestUidKey = "collectionsManifestUid"
const SystemScopeName = "_system"
const SystemCollectionMobile = "_mobile"

// This is for when FilterSystemScope == true
var FilterSystemScopePassthruCollections = []string{SystemCollectionMobile}

// From KV design doc:
// A user’s collection can only contain characters A-Z, a-z, 0-9 and the following symbols _ - %
// The prefix character of a user’s collection name however is restricted. It cannot be _ or %
// System scope/collections always start with _
// Note that XDCR doesn't care if it is system vs user. It just replicates
const CollectionValidNameCharClass = "[0-9A-Za-z-_%]"
const CollectionValidPrefixNameClass = "[0-9A-Za-z-_]"

// OptionalCollectionNamespaceRegexExpr matches pattern with collection name being optional
// E.g S1.C1, S1 but not S1.
var OptionalCollectionNamespaceRegexExpr = fmt.Sprintf("^(?P<scope>%v+)(?:[%v](?P<collection>%v+))?$", CollectionValidNameCharClass, ScopeCollectionDelimiter, CollectionValidNameCharClass)
var CollectionNamespaceRegexExpr = fmt.Sprintf("^(?P<scope>%v+)[%v](?P<collection>%v+)$", CollectionValidNameCharClass, ScopeCollectionDelimiter, CollectionValidNameCharClass)
var CollectionNamespaceRegex, _ = regexp.Compile(CollectionNamespaceRegexExpr)
var OptionalCollectionNamespaceRegex, _ = regexp.Compile(OptionalCollectionNamespaceRegexExpr)

var CollectionNameValidationRegex, _ = regexp.Compile(fmt.Sprintf("^%v%v*$", CollectionValidPrefixNameClass, CollectionValidNameCharClass))

const BasicFQDNCharClass = "[0-9A-Za-z-_]"

var BasicFQDNRegex, _ = regexp.Compile(fmt.Sprintf("^(%v+\\.)+(%v+)$", BasicFQDNCharClass, BasicFQDNCharClass))

var WhiteSpaceCharsRegex, _ = regexp.Compile(`\s`)

var DefaultCollectionId uint32 = 0

var CollectionsUidBase int = 16

// URL related constants
const UrlDelimiter = "/"

var UrlPortNumberDelimiter = ":"

// Custom conflict resolution related constants
var JSEngineWorkers = DefaultGoMaxProcs
var JSWorkerQuota = 1572864 // 1.5MB

// constants for ipv6 addresses
const Ipv6AddressSeparator = ":"
const LeftBracket = "["
const RightBracket = "]"

var ClusterMembership_Active = "active"

// http request method types
const (
	MethodGet    = "GET"
	MethodPost   = "POST"
	MethodDelete = "DELETE"
)

const (
	RMDistHighRepl = 0
	RMDistLowRepl  = 1
	RMDistCLog     = 2
)

// delimiter for multiple parts in a key
var KeyPartsDelimiter = "/"

// constants for adminport
var AdminportUrlPrefix = UrlDelimiter

// used as default value for tests
var AdminportNumber uint16 = 13000
var GometaRequestPortNumber uint16 = 11000

// read timeout for golib's http server.
var AdminportReadTimeout = 60 * time.Second

// write timeout for golib's http server.
var AdminportWriteTimeout = 180 * time.Second

// outgoing nozzle type
type XDCROutgoingNozzleType int

const (
	Xmem XDCROutgoingNozzleType = iota
	Capi XDCROutgoingNozzleType = iota
)

// Last element is invalid and is there to keep consistency with the EndMarker
var CompressionTypeStrings = [...]string{"Invalid", "None", "Snappy", "Auto", "ForceUncompress", "Invalid"}

const (
	// Start and End markers are considered invalid values
	CompressionTypeStartMarker = iota
	// None means try to establish memcached with snappy HELO. KV will send document down as they are stored
	// If target doesn't support receiving snappy data, fallback to ForceUncompress
	CompressionTypeNone = iota
	// Snappy is for internal use only. Users could set it with older server. Now they cannot.
	CompressionTypeSnappy = iota
	// Auto means try to establish snappy if possible. If not, fall back to None
	CompressionTypeAuto = iota // XDCR only
	// ForceUncompress means request source KV to send decompressed data if they are compressed
	// This is needed for replicating to pre-snappy legacy clusters
	CompressionTypeForceUncompress = iota // XDCR only
	CompressionTypeEndMarker       = iota
)

const CompressionTypeREST = "compressionType"

const CompressionTypeKey = "compression_type"

// DataType fields of MCRequest
// kv_engine/include/mcbp/protocol/datatype.h
const (
	RawDataType    uint8 = 0 // "unknown" datatype. Eg: subdoc commands with body containing operational specs
	JSONDataType   uint8 = 1
	SnappyDataType uint8 = 2
	XattrDataType  uint8 = 4
	// In subdoc lookup for vxattr $document, KV returns an array of the following strings instead
	JsonDataTypeStr   string = "json"
	SnappyDataTypeStr string = "snappy"
	XattrDataTypeStr  string = "xattr"
)

const (
	PIPELINE_SUPERVISOR_SVC    string = "PipelineSupervisor"
	CHECKPOINT_MGR_SVC         string = "CheckpointManager"
	STATISTICS_MGR_SVC         string = "StatisticsManager"
	TOPOLOGY_CHANGE_DETECT_SVC string = "TopologyChangeDetectSvc"
	BANDWIDTH_THROTTLER_SVC    string = "BandwidthThrottlerSvc"
	BACKFILL_MGR_SVC           string = "BackfillMgrSvc"
	CONFLICT_MANAGER_SVC       string = "ConflictManager"

	// Conflict logger is technically not a typical pipeline service.
	// It may or may not exist based on the replication setting.
	// But it is attached to the pipeline to blend in with the existing design of the pipeline.
	CONFLICT_LOGGER string = "ConflictLogger"
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

const CouchbaseDnsServiceName = "couchbase"
const CouchbaseSecureDnsServiceName = "couchbases"

var CouchbaseUri = fmt.Sprintf("%v://", CouchbaseDnsServiceName)
var CouchbaseSecureUri = fmt.Sprintf("%v://", CouchbaseSecureDnsServiceName)

var CapellaHostnameSuffix = ".cloud.couchbase.com"

const ImportDetectedStr = "Import mutations detected when mobile is Off. This is not supported."

const FinClosureStr = "because of finch closure"

const StringTargetCollectionMappingErr = "Target node unable to find"

// the full error as of now is : "x509: cannot validate certificate for xxx because it doesn't contain any IP SANs"
// use a much shorter version for matching to reduce the chance of false negatives - the error message may be changed by golang in the future
var NoIpSANErrMsg = "IP SANs"

// constants used for remote cluster references
const (
	RemoteClustersPath = "pools/default/remoteClusters"

	RemoteClusterUuid     = "uuid"
	RemoteClusterName     = "name"
	RemoteClusterHostName = "hostname"
	RemoteClusterUserName = "username"
	RemoteClusterPassword = "password"
	// To be deprecated
	RemoteClusterDemandEncryption = "demandEncryption"
	// To be deprecated
	RemoteClusterEncryptionType = "encryptionType"
	// New parameter that should be used in the place of RemoteClusterDemandEncryption
	// and RemoteClusterEncryptionType starting at 5.5
	RemoteClusterSecureType        = "secureType"
	RemoteClusterCertificate       = "certificate"
	RemoteClusterClientCertificate = "clientCertificate"
	RemoteClusterClientKey         = "clientKey"
	RemoteClusterUri               = "uri"
	RemoteClusterValidateUri       = "validateURI"
	RemoteClusterDeleted           = "deleted"
	IsEnterprise                   = "isEnterprise"
	Pools                          = "pools"
	RemoteClusterHostnameMode      = "network_type"
	ConnectivityStatus             = "connectivityStatus"
	ConnectivityErrors             = "connectivityErrors"
	RemoteBucketManifest           = "remoteBucketManifest"
	RedactRequested                = "redactRequested"
	RestrictHostnameReplace        = "restrictHostnameReplace"
)

// secure type for remote cluster reference
const (
	SecureTypeNone = "none"
	SecureTypeHalf = "half"
	SecureTypeFull = "full"
)

// constants used for create replication request
const (
	Type       = "type"
	FromBucket = "fromBucket"
	ToCluster  = "toCluster"
	ToBucket   = "toBucket"
)

const RemoteClusterAuthErrString = "Authentication failed"

// constant used by more than one rest apis
const (
	JustValidate        = "just_validate"
	IncludeWarnings     = "include_warnings"
	JustValidatePostfix = "?" + JustValidate + "=1"
)

// const used by block profile
const (
	BlockProfileRate = "block_profile_rate"
)

// Key used for general validation errors that are not related to any specific http request parameters
var PlaceHolderFieldKey = "_"

// http request related constants
const (
	ContentType               = "Content-Type"
	DefaultContentType        = "application/x-www-form-urlencoded"
	JsonContentType           = "application/json"
	PlainTextContentType      = "text/plain"
	PrometheusTextContentType = "; version=0.0.4"
	ContentLength             = "Content-Length"
	UserAgent                 = "User-Agent"
)

// constant for replication tasklist status
const (
	Pending     = "Pending"
	Replicating = "Replicating"
	Paused      = "Paused"
)

const (
	//Bucket sequence number statistics
	VBUCKET_SEQNO_STAT_NAME            = "vbucket-seqno"
	VBUCKET_PREFIX                     = "vb_"
	HIGH_SEQNO_CONST                   = ":high_seqno"
	VBUCKET_HIGH_SEQNO_STAT_KEY_FORMAT = VBUCKET_PREFIX + "%v" + HIGH_SEQNO_CONST
	VBUCKET_UUID_STAT_KEY_FORMAT       = "vb_%v:uuid"
	VBUCKET_DETAILS_NAME               = "vbucket-details"
	MAXCAS_CONST                       = ":max_cas"
	VBUCKET_MAXCAS_STAT_KEY_FORMAT     = VBUCKET_PREFIX + "%v" + MAXCAS_CONST
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

const ChangesLeftStats = "changes_left"
const DocsFromDcpStats = "docs_received_from_dcp"
const DocsRepQueueStats = "docs_rep_queue"
const ConflictDocsWritten = "clog_docs_written"

// constants for replication docs
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

	VBTaskMap      = "vbTasks"
	SourceManifest = "sourceManifest"
)

// constant used in replication info to ensure compatibility with erlang xdcr
var MaxVBReps = "max_vbreps"

const (
	GET                   = mc.CommandCode(0x00)
	GET_WITH_META         = mc.CommandCode(0xa0)
	SET_WITH_META         = mc.CommandCode(0xa2)
	ADD_WITH_META         = mc.CommandCode(0xa4)
	DELETE_WITH_META      = mc.CommandCode(0xa8)
	SET_TIME_SYNC         = mc.CommandCode(0xc1)
	SUBDOC_DICT_UPSERT    = mc.CommandCode(0xc8)
	SUBDOC_DELETE         = mc.CommandCode(0xc9)
	SUBDOC_MULTI_MUTATION = mc.CommandCode(0xd1)
)

// Flags for SUBDOC commands
const (
	// Path level flag
	SUBDOC_FLAG_MKDIR_P       = 0x01
	SUBDOC_FLAG_XATTR         = 0x04
	SUBDOC_FLAG_EXPAND_MACROS = 0x10
)

const (
	PipelineSetting_RequestPool = "RequestPool"
	DefaultRequestPoolSize      = 10000
)

var EventChanSize = 10000

// names of async component event listeners
const (
	DataReceivedEventListener            = "DataReceivedEventListener"
	DataProcessedEventListener           = "DataProcessedEventListener"
	DataFilteredEventListener            = "DataFilteredEventListener"
	DataSentEventListener                = "DataSentEventListener"
	DataSentCasChangedEventListener      = "DataSentCasChangedEventListener"
	DataFailedCREventListener            = "DataFailedCREventListener"
	OutNozzleDataSkippedEventListener    = "OutNozzleDataSkippedEventListener"
	GetReceivedEventListener             = "GetReceivedEventListener"
	DataThrottledEventListener           = "DataThrottledEventListener"
	DataThroughputThrottledEventListener = "DataThroughputThrottledEventListener"
	CollectionRoutingEventListener       = "CollectionRoutingEventListener"
	DataClonedEventListener              = "DataClonedEventListener"
	DataSentFailedListener               = "DataSentFailedListener"
	DataMergedEventListener              = "DataMergedEventListener"
	MergeCasChangedEventListener         = "MergeCasChangedEventListener"
	MergeFailedEventListener             = "MergeFailedEventListener"
	SrcSyncXattrRemovedEventListener     = "SourceSyncXattrRemovedEventListener"
	TgtSyncXattrPreservedEventListener   = "TargetSyncXattrPreservedEventListenr"
	HlvUpdatedEventListener              = "HlvUpdatedEventListener"
	HlvPrunedEventListener               = "HlvPrunedEventListener"
	HlvPrunedAtMergeEventListener        = "HlvPrunedAtMergeEventListener"
	CLogDocsWrittenEventListener         = "CLogDocsWrittenEventListener"
	CLogWriteStatusEventListener         = "CLogWriteStatusEventListener"
	TrueConflictsEventListener           = "TrueConflictsEventListener"
)

const (
	OutNozzleStatsCollector  = "OutNozzleStatsCollector"
	DcpStatsCollector        = "DcpStatsCollector"
	RouterStatsCollector     = "RouterStatsCollector"
	CheckpointStatsCollector = "CheckpointStatsCollector"
	ThroughSeqnoTracker      = "ThroughSeqnoTracker"
	ConflictMgrCollector     = "ConflictManagerCollector"
	CLogStatsCollector       = "CLogStatsCollector"
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
	CollectionNsMapping = "CollectionNsMapping"
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
	PermissionBucketDataReadSuffix    = "].data!read"
	PermissionXDCRAdminInternalRead   = "cluster.admin.internal.xdcr!read"
	PermissionXDCRAdminInternalWrite  = "cluster.admin.internal.xdcr!write"
	PermissionXDCRPrometheusRead      = "cluster.admin.internal.stats!read"
	PermissionC2cCommunications       = "cluster.xdcr.c2c_communications!all"

	//  XDCR Admin has following permissions but not cluster.admin.internal.xdcr
	PermissionXDCRInternalRead  = "cluster.xdcr.internal!read"
	PermissionXDCRInternalWrite = "cluster.xdcr.internal!write"
)

// constants for parsing conflict resolution type setting in bucket metadata
const (
	ConflictResolutionTypeKey     = "conflictResolutionType"
	ConflictResolutionType_Seqno  = "seqno"
	ConflictResolutionType_Lww    = "lww"
	ConflictResolutionType_Custom = "custom"
)

const EOFString = "EOF"

// flag for memcached to enable lww to lww bucket replication
var FORCE_ACCEPT_WITH_META_OPS uint32 = 0x02
var SKIP_CONFLICT_RESOLUTION_FLAG uint32 = 0x08

// https://github.com/couchbase/kv_engine/blob/master/engines/ep/docs/protocol/del_with_meta.md
var IS_EXPIRATION uint32 = 0x10

// read/write timeout for helo command to memcached
var HELOTimeout time.Duration = time.Duration(120) * time.Second

// For utilities to try to get a working remote connection
var MaxRemoteMcRetry = 5
var RemoteMcRetryWaitTime = 200 * time.Millisecond
var RemoteMcRetryFactor = 2

// The following will result in a max of 3 seconds of retry time before declaring failure
var BucketInfoOpMaxRetry = 5
var BucketInfoOpWaitTime = 100 * time.Millisecond
var BucketInfoOpRetryFactor = 2

// Retry for serializer - should be relatively quick
var PipelineSerializerMaxRetry = 3
var PipelineSerializerRetryWaitTime = 100 * time.Millisecond
var PipelineSerializerRetryFactor = 2

// For Requesting Remote Bucket monitoring - max of 8 seconds
var RemoteBucketMonitorMaxRetry = 4
var RemoteBucketMonitorWaitTime = 200 * time.Millisecond
var RemoteBucketMonitorRetryFactor = 3

// DefaultHttpTimeout is 180 seconds
var DefaultHttpTimeoutWaitTime = 200 * time.Millisecond
var DefaultHttpTimeoutMaxRetry = 10
var DefaultHttpTimeoutRetryFactor = 2

// For peer-to-peer communication retry - max of ~51 seconds
var PeerToPeerMaxRetry = 8
var PeerToPeerRetryWaitTime = 200 * time.Millisecond
var PeerToPeerRetryFactor = 2
var PeerToPeerNonExponentialWaitTime = 51 * time.Second

// minimum versions where various features are supported
type ServerVersion []int

const ServerVersionSeparator = "."

var VersionForCompressionSupport = ServerVersion{5, 5}
var VersionForClientCertSupport = ServerVersion{5, 5}
var VersionForHttpScramShaSupport = ServerVersion{5, 5}
var VersionForCollectionSupport = ServerVersion{7, 0}
var VersionForAdvErrorMapSupport = ServerVersion{7, 5}
var VersionForHeartbeatSupport = ServerVersion{8, 0}

// ns_server and support would like to start seeing 3 digits for versions
var VersionForAdvFilteringSupport = ServerVersion{6, 5, 0}
var VersionForPrometheusSupport = ServerVersion{7, 0, 0}
var VersionForCcrDpSupport = ServerVersion{7, 0, 0}
var VersionForPeerToPeerSupport = ServerVersion{7, 1, 0}
var Version7_2_1 = ServerVersion{7, 2, 1}
var VersionForConnectionPreCheckSupport = ServerVersion{7, 6, 0}
var VersionForSupportability = ServerVersion{7, 6, 0}
var VersionForP2PManifestSharing = ServerVersion{7, 6, 0}
var VersionForMobileSupport = ServerVersion{7, 6, 6}
var VersionForCasPoisonDetection = ServerVersion{8, 0, 0}
var VersionForSrcHeartbeatSupport = ServerVersion{8, 0, 0}
var VersionForCLoggerSupport = ServerVersion{8, 0, 0}
var VersionForVariableVBSupport = ServerVersion{8, 0, 0}
var VersionForPipelineReinitHashSupport = ServerVersion{8, 0, 0}

func (s ServerVersion) String() string {
	builder := strings.Builder{}
	for i := 0; i < len(s); i++ {
		builder.WriteString(strconv.Itoa(s[i]))
		if i != len(s)-1 {
			builder.WriteString(ServerVersionSeparator)
		}
	}
	return builder.String()
}

func (s ServerVersion) SameAs(other ServerVersion) bool {
	if len(s) != len(other) {
		return false
	}
	for i, c := range s {
		if other[i] != c {
			return false
		}
	}
	return true
}

func NewServerVersionFromString(str string) (ServerVersion, error) {
	if str == "" {
		return ServerVersion{}, nil
	}

	versions := strings.Split(str, ServerVersionSeparator)
	versionsInt := ServerVersion{}

	for i := 0; i < len(versions); i++ {
		parsedInt, err := strconv.ParseInt(versions[i], 10, 0)
		if err != nil {
			return nil, err
		}
		versionsInt = append(versionsInt, int(parsedInt))
	}

	return versionsInt, nil
}

// User agent for REST calls.
var GoxdcrUserAgentPrefix = "couchbase-goxdcr"
var GoxdcrUserAgent = ""

// Refer https://github.com/couchbase/kv_engine/blob/977c08f26f9826a8b0e21dd9c1c20a7b2be26d3a/docs/BinaryProtocol.md#0x1f-helo
// HELO user agent name ("a") will be truncated only upto the 32nd byte by kv_engine.
// So use a short name with a server version suffix.
var GoxdcrHELOUserAgentPrefix = "goxdcr"
var GoxdcrHELOUserAgent = ""
var HELOUserAgentNameMaxLen int = 32
var HELOConnIdMaxLen int = 33

func ComposeHELOMsgKey(connectionId string) string {
	if len(connectionId) > HELOConnIdMaxLen {
		// In an odd case wherein the connectionId is too long (it's a possibility if a bucket name is too long),
		// we will use the md5 hash of it (32 bytes) to fit the connectionId to a 33 bytes limit.
		connectionId = fmt.Sprintf("%x", md5.Sum([]byte(connectionId)))
	}

	key, err := json.Marshal(struct {
		UserAgentName string `json:"a"`
		ConnectionId  string `json:"i"`
	}{
		UserAgentName: GoxdcrHELOUserAgent,
		ConnectionId:  connectionId,
	})
	if err != nil {
		// non-JSON objects HELO message keys are also fine, but hinders debuggability
		// on the kv_engine side.
		return GoxdcrHELOUserAgent
	}

	return string(key)
}

// Used to calculate the number of bytes to allocate for sending the HELO messages
var HELO_BYTES_PER_FEATURE int = 2

// value representing tcp no delay feature in helo request/response
var HELO_FEATURE_TCP_NO_DELAY uint16 = 0x03

// value representing xattr feature in helo request/response
var HELO_FEATURE_XATTR uint16 = 0x06

// value representing snappy compression
var HELO_FEATURE_SNAPPY uint16 = 0x0a

// value representing XERROR
var HELO_FEATURE_XERROR uint16 = 0x07

// new XATTR bit in data type field in dcp mutations
var PROTOCOL_BINARY_DATATYPE_XATTR uint8 = 0x04

// Collections Feature
var HELO_FEATURE_COLLECTIONS uint16 = 0x12

// SnappyEverywhere Feature. The client has to
// honour the snappy datatype bit even in the response packets.
var HELO_FEATURE_SNAPPYEVERYWHERE uint16 = 0x13

// JSON bit will be set in the response packets.
var HELO_FEATURE_JSON uint16 = 0x0b

// length of random id
var LengthOfRandomId = 16

// 8 bytes put the approx. hash collision probability of 'MaxCheckpointRecordsToKeep' (100) items at ~0%
const PipelineReinitHashLength = 8

// max retry for random id generation
var MaxRetryForRandomIdGeneration = 5

var TimeoutRuntimeContextStart = 30 * time.Second
var TimeoutRuntimeContextStop = 15 * time.Second
var TimeoutPartsStart = 30 * time.Second
var TimeoutPartsStop = 10 * time.Second
var TimeoutConnectorsStop = 5 * time.Second
var TimeoutDcpCloseUprStreams = 3 * time.Second
var TimeoutDcpCloseUprFeed = 3 * time.Second
var TimeoutP2PProtocol = 60 * time.Second // Default if not specified

// This is for enforcing remote connection network type.
const TCP = "tcp"   // ipv4/ipv6 are both supported
const TCP4 = "tcp4" // ipv4 only
const TCP6 = "tcp6" // ipv6 only

var NetTCP = TCP
var IpFamilyStr = "tcp4/tcp6"

var CurrentTime = "CurrentTime"

// Limit imposed by memcached
const MaxDcpConnectionNameLength = 200

/**
 * Log Redaction section
 * Spectrum 1 - User Data redaction (ud) tags
 * Spectrum 2 - Metadata redaction tags (TBD)
 * Spectrum 3 - System Data redaction tags (TBD)
 */
const (
	UdTagBegin = "<ud>"
	UdTagEnd   = "</ud>"
)

var UdTagBeginBytes = []byte(UdTagBegin)
var UdTagEndBytes = []byte(UdTagEnd)

const (
	HttpReqUserKey   = "Menelaus-Auth-User"
	AuthorizationKey = "Authorization"
	CBOnBehalfOfKey  = "Cb-On-Behalf-Of"
)

var HttpRedactKeys = []string{HttpReqUserKey, AuthorizationKey, CBOnBehalfOfKey}

const AuthorizationKeyRedactPrefix = "Basic "

// retry interval for setDerivedObj op
var RetryIntervalSetDerivedObj = 100 * time.Millisecond

// max number of retries for setDerivedObj op
var MaxNumOfRetriesSetDerivedObj = 8

var NumberOfWorkersForCheckpointing = 5

const NumberOfVbs = 1024

type FilterVersionType int

const (
	// Note the default is KeyOnly because from a version that did not have this key, that is the
	// expected version so we can do proper handling.
	// AdvInMemory version means that the filter itself is advanced, but in metakv it is still stored as KeyOnly
	FilterVersionKeyOnly  FilterVersionType = iota
	FilterVersionAdvanced FilterVersionType = iota
)

type FilterFlagType int

const (
	FilterFlagSkipXattr FilterFlagType = 0x1
	FilterFlagSkipKey   FilterFlagType = 0x2
	FilterFlagKeyOnly   FilterFlagType = 0x4
	FilterFlagXattrOnly FilterFlagType = 0x8
)

var DefaultGoMaxProcs int = 4

var BacklogThresholdDefault = 50

var MaxDocSizeByte uint32 = 20 << 20

var MaxCollectionNameBytes int = 251

const MaxCas = 0xFFFFFFFFFFFFFFFF

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

// the maximum number of checkpoint records to keep in the checkpoint doc - for traditional replications
var MaxCheckpointRecordsToKeepTraditional int = 5

// the maximum number of checkpoint records to read from the checkpoint doc - for traditional replications
var MaxCheckpointRecordsToReadTraditional int = 5

// the maximum number of checkpoint records to keep in the checkpoint doc - for variableVB replications
var MaxCheckpointRecordsToKeepVariableVB int = 12

// the maximum number of checkpoint records to read from the checkpoint doc - for variableVB replications
var MaxCheckpointRecordsToReadVariableVB int = 12

// default time out for outgoing http requests if it is not explicitly specified (seconds)
var DefaultHttpTimeout = 180 * time.Second

// when we need to make a rest call when processing a XDCR rest request, the time out of the second rest call needs
// to be shorter than that of the first one, which is currently 30 seconds. (seconds)
var ShortHttpTimeout = 20 * time.Second

// When contacting the remote cluster's NS server for getting HTTPS ports, it should not take long. But if necessary,
// this can be changed
var HttpsPortLookupTimeout = 2 * time.Second

// max retry for live updating of pipelines
var MaxRetryForLiveUpdatePipeline = 5

// wait time between retries for live updating of pipelines (milliseconds)
var WaitTimeForLiveUpdatePipeline = 2000 * time.Millisecond

// interval for replication spec validity check (seconds)
// This should be *less* than statsInterval, otherwise it may trigger too many timer resets
var ReplSpecCheckInterval = 15 * time.Second

// interval for mem stats logging (seconds)
var MemStatsLogInterval = 120 * time.Second

// max number of retries for metakv ops
var MaxNumOfMetakvRetries = 5

// interval between metakv retries
var RetryIntervalMetakv = 500 * time.Millisecond

// In order for dcp flow control to work correctly, the number of mutations in dcp buffer
// should be no larger than the size of the dcp data channel.
// This way we can ensure that gomemcached is never blocked on writing to data channel,
// and thus can always respond to dcp commands such as NOOP
// In other words, the following three parameters should be selected such that
// MinimumMutationSize * UprFeedDataChanLength >= UprFeedBufferSize
// where MinimumMutationSize is the minimum size of a SetMeta/DelMeta mutation,
// a DCP mutation has size 54 + key + body. 60 should be a safe value to use

// length of data channel between dcp nozzle and gomemcached
var UprFeedDataChanLength = 20000

// dcp flow control buffer size (number of bytes)
var UprFeedBufferSize uint32 = 1024 * 1024

// max retry for xmem operations like batch send, resend, etc.
var XmemMaxRetry = 5

// xmem write time out for writing to network connection (seconds)
var XmemWriteTimeout = 120 * time.Second

// xmem read time out when reading from network connection (seconds)
var XmemReadTimeout = 120 * time.Second

// network connection will be repaired if its down time (the time that it receives
// continuous network error responses from read or write) exceeds max down time (seconds)
var XmemMaxReadDownTime = 60 * time.Second

// wait time between writes is backoff_factor*XmemBackoffWaitTime
var XmemBackoffWaitTime = 10 * time.Millisecond

// max backoff factor
var XmemMaxBackoffFactor = 10

// max retry for new xmem connection
var XmemMaxRetryNewConn = 10

// initial backoff time between retries for new xmem connection (milliseconds)
var XmemBackoffTimeNewConn = 1000 * time.Millisecond

// interval for xmem self monitoring (seconds)
var XmemSelfMonitorInterval = 6 * time.Second

// initial max idle count;
// it is dynamically adjusted at runtime by factor = actual response wait time / previous response wait time
// if xmem idle count exceeds this max, it will be declared to be stuck
var XmemMaxIdleCount = 60

// lower bound for xmem max idle count
var XmemMaxIdleCountLowerBound = 10

// upper bound for xmem max idle count
var XmemMaxIdleCountUpperBound = 120

// //the maximum amount of data (in bytes) xmem data channel can hold
var XmemMaxDataChanSize = 10 * 1024 * 1024

// max batch size that can be sent in one writeToClient() op
var XmemMaxBatchSize = 50

// interval between retries on batchUpdateDocs
var CapiRetryInterval = 500 * time.Millisecond

// maximum number of snapshot markers to store for each vb
// once the maximum is reached, the oldest snapshot marker is dropped to make room for the new one
var MaxLengthSnapshotHistory = 200

// max retry for target stats retrieval.
var MaxRetryTargetStats = 6

// base wait time between retries for target stats retrieval (milliseconds)
var RetryIntervalTargetStats = 1000 * time.Millisecond

// number of time slots [in one second] to track for bandwidth throttling computation
var NumberOfSlotsForBandwidthThrottling = 10

// When doing bandwith throttling in xmem, set minNumberOfBytes = TotalNumberOfBytes * PercentageOfBytesToSendAsMin / 100
var PercentageOfBytesToSendAsMin = 30

// write time out for audit service (seconds)
var AuditWriteTimeout = 1 * time.Second

// read time out for audit service (seconds)
var AuditReadTimeout = 1 * time.Second

// number of retries for CAPI calls, e.g., pre_replicate and commit_for_checkpoint
var MaxRetryCapiService = 5

// max number of async listeners [for an event type]
var MaxNumberOfAsyncListeners = 4

// max interval between retries when resending docs  (seconds)
var XmemMaxRetryInterval = 300 * time.Second

// max retry for xmem resend operation on mutation locked error
var XmemMaxRetryMutationLocked = 20

// max interval between retries when resending docs on mutation locked errors  (seconds)
// the upper limit on lock period is as of now 30 seconds
var XmemMaxRetryIntervalMutationLocked = 30 * time.Second

var WaitTimeBetweenMetadataChangeListeners = 500 * time.Millisecond

// Keep alive period for tcp connections
var KeepAlivePeriod = 30 * time.Second

// actual size of data chan is logged when it exceeds ThresholdForEventChanSizeLogging
var ThresholdForEventChanSizeLogging = EventChanSize * 9 / 10

// if through seqno computation takes longer than the threshold, it will be logged
var ThresholdForThroughSeqnoComputation = 100 * time.Millisecond

// interval for printing replication runtime stats to log file
var StatsLogInterval = 30 * time.Second

// default resp timeout, which is used as the interval for checkAndRepairBufferMonitor
var XmemDefaultRespTimeout time.Duration = 1000 * time.Millisecond

// Number of retries for RemoteClusterService to do any metaKV operations
var MaxRCSMetaKVOpsRetry int = 5

// Time to wait between metakv get ops
var TimeBetweenMetaKVGetOps = time.Duration(500) * time.Millisecond

// when set to true, bypass san in certificate check in ssl connections
var BypassSanInCertificateCheck bool = false

// Number of times to verify bucket is missing before removing an invalid replicationSpec
var ReplicationSpecGCCnt int = 6

// interval for cpu stats collection
var CpuCollectionInterval = 2000 * time.Millisecond

// interval for resource management actions
var ResourceManagementInterval = 1000 * time.Millisecond

// interval for logging resource management stats
var ResourceManagementStatsInterval = 10000 * time.Millisecond

// once changes_left becomes smaller than this threshold, replication will be classified as ongoing replication
var ChangesLeftThresholdForOngoingReplication = 200000

// all the ratio related constants are defined as multiples of 1/ResourceManagementRatioBase
// so that we can do integer arithmetic
var ResourceManagementRatioBase = 100

// upper bound for ratio of throughput of high priority replications to throughput of all replications
// this is to ensure that low priority replications will not be completely starved
var ResourceManagementRatioUpperBound = 90

// when the number of consecutive terms where there have been backlog reaches the threshold, set DCP priorities
var MaxCountBacklogForSetDcpPriority = 5

// when the number of consecutive terms where there have been no backlog reaches the threshold, reset DCP priorities to normal
var MaxCountNoBacklogForResetDcpPriority = 300

// extra quota given to replications when cpu is not yet maximized
var ExtraQuotaForUnderutilizedCPU = 10

// interval for printing throughput throttler stats to log file
var ThroughputThrottlerLogInterval = 10000 * time.Millisecond

// interval for clearing tokens in throughput throttler
var ThroughputThrottlerClearTokensInterval = 3000 * time.Millisecond

// number of time slots [per measurement interval] for throughput throttling
var NumberOfSlotsForThroughputThrottling = 10

// interval for throttler calibration, i.e., for stopping reassigning tokens to low priority replications, as number of time slots
var IntervalForThrottlerCalibration = 4

// number of throughput samples to keep
var ThroughputSampleSize = 1028

// alpha for exponential decay sampling. Actual alpha = ThroughputSampleAlpha / 1000
var ThroughputSampleAlpha = 15

// when actual process cpu usage exceeds maxProcessCpu * ThresholdRatioForProcessCpu/100, process cpu is considered to have maxed out
var ThresholdRatioForProcessCpu = 95

// when actual total cpu usage exceeds totalCpu * ThresholdRatioForTotalCpu/100, total cpu is considered to have maxed out
var ThresholdRatioForTotalCpu = 95

// max count of consecutive terms where cpu has not been maxed out
// an extra quota period will be started when the max count is reached
var MaxCountCpuNotMaxed = 3

// max count of consecutive terms where throughput dropped from previous high
// if we are in extra quota period, the period will be ended when the max count is reached
var MaxCountThroughputDrop = 3

// Internal key to wrap around incoming document's xattributes for advanced filtering
// Customers may change it if a specific key conflicts with this
var InternalKeyXattr = "[$%XDCRInternalMeta*%$]"

// Internal key to wrap around incoming key for advanced filtering
var InternalKeyKey = "[$%XDCRInternalKey*%$]"

// Cached variables
var CachedInternalKeyKeyByteSlice = []byte(InternalKeyKey)
var CachedInternalKeyKeyByteSize = len(CachedInternalKeyKeyByteSlice)
var CachedInternalKeyXattrByteSlice = []byte(InternalKeyXattr)
var CachedInternalKeyXattrByteSize = len(CachedInternalKeyXattrByteSlice)

// From end user's perspective, they will see the reserved word they entered
// However, internally, XDCR will insert more obscure internal keys to prevent collision with actual
// user's data
var ReservedWordsMap = map[string]string{
	ExternalKeyKey:   InternalKeyKey, /* if this entry changes, CachedInternalKeyKeyByteSlice needs to change too */
	ExternalKeyXattr: InternalKeyXattr,
}
var ReverseReservedWordsMap = map[string]string{
	InternalKeyKey:   ExternalKeyKeyContains,
	InternalKeyXattr: ExternalKeyXattrContains,
}

// The regexp here returns true if the specified values are not escaped (enclosed by backticks)
var ReservedWordsReplaceMap = map[string]PcreWrapperInterface{}

// Used to make sure the pcre's are initialized only once, when needed
var ReservedWordsReplaceMapOnce sync.Once

// Number of times for a remote cluster to consistently change from using internal interface to
// external interface, and vice versa
var RemoteClusterAlternateAddrChangeCnt = 5

// How often in seconds to pull manifests from ns_server
var ManifestRefreshSrcInterval = 2
var ManifestRefreshTgtInterval = 60

// Amount of time between each backfill metakv persist operation
// Be careful when changing the unit - need corresponding base.InitConstants to change
var BackfillPersistInterval = 1000 * time.Millisecond

var ResourceMgrKVDetectionRetryInterval = 60 * time.Second

var ReplStatusLoadBrokenMapTimeout = 5 * time.Second
var ReplStatusExportBrokenMapTimeout = 5 * time.Second

var TopologySvcCoolDownPeriod = 60 * time.Second
var TopologySvcErrCoolDownPeriod = 120 * time.Second
var TopologySvcStatusNotFoundCoolDownPeriod = 10 * time.Second

var BucketTopologyWatcherChanLen = 1000
var BucketTopologyGCScanTime = 1 * time.Minute
var BucketTopologyGCPruneTime = 24 * time.Hour
var BucketTopologyWatcherErrChanLen = 1

var P2PCommTimeout = 15 * time.Second
var MaxP2PReceiveChLen = 10000
var P2POpaqueCleanupInterval = 5 * time.Second
var P2PVBRelatedGCInterval = 24 * time.Hour
var P2PReplicaReplicatorReloadChSize = 10
var P2PManifestsCacheCleanupInterval = 5 * time.Minute

var ThroughSeqnoBgScannerFreq = 5 * time.Second
var ThroughSeqnoBgScannerLogFreq = 60 * time.Second

// Factor multiplied with an xmem nozzle's `selfMonitorInterval` to set the duration
// of the delay after which temporary memcached errors like `ETMPFAIL` are displayed on UI
var TempMCErrorDisplayDelayFactor = 20

func InitConstants(topologyChangeCheckInterval time.Duration, maxTopologyChangeCountBeforeRestart,
	maxTopologyStableCountBeforeRestart, maxWorkersForCheckpointing int,
	timeoutCheckpointBeforeStop time.Duration, capiDataChanSizeMultiplier int,
	refreshRemoteClusterRefInterval time.Duration, clusterVersion string,
	capiMaxRetryBatchUpdateDocs int, capiBatchTimeout time.Duration,
	capiWriteTimeout time.Duration, capiReadTimeout time.Duration,
	maxCheckpointRecordsToKeepTraditional int, maxCheckpointRecordsToReadTraditional int,
	defaultHttpTimeout time.Duration, shortHttpTimeout time.Duration,
	maxRetryForLiveUpdatePipeline int, waitTimeForLiveUpdatePipeline time.Duration,
	replSpecCheckInterval time.Duration, memStatsLogInterval time.Duration,
	maxNumOfMetakvRetries int, retryIntervalMetakv time.Duration,
	uprFeedDataChanLength int, uprFeedBufferSize int,
	xmemMaxRetry int, xmemWriteTimeout time.Duration,
	xmemReadTimeout time.Duration, xmemMaxReadDownTime time.Duration,
	xmemBackoffWaitTime time.Duration, xmemMaxBackoffFactor int, xmemMaxRetryNewConn int,
	xmemBackoffTimeNewConn time.Duration, xmemSelfMonitorInterval time.Duration,
	xmemMaxIdleCount int, xmemMaxIdleCountLowerBound int, xmemMaxIdleCountUpperBound int,
	xmemMaxDataChanSize int, xmemMaxBatchSize int,
	capiRetryInterval time.Duration, maxLengthSnapshotHistory int,
	maxRetryTargetStats int, retryIntervalTargetStats time.Duration,
	numberOfSlotsForBandwidthThrottling int, percentageOfBytesToSendAsMin int,
	auditWriteTimeout time.Duration, auditReadTimeout time.Duration,
	maxRetryCapiService int, maxNumberOfAsyncListeners int,
	xmemMaxRetryInterval time.Duration, xmemMaxRetryMutationLocked int,
	xmemMaxRetryIntervalMutationLocked time.Duration, heloTimeout time.Duration,
	waitTimeBetweenMetadataChangeListeners time.Duration, keepAlivePeriod time.Duration,
	thresholdPercentageForEventChanSizeLogging int, thresholdForThroughSeqnoComputation time.Duration,
	statsLogInterval time.Duration, xmemDefaultRespTimeout time.Duration,
	bypassSanInCertificateCheck int, replicationSpecGCCnt int, timeoutRuntimeContextStart time.Duration,
	timeoutRuntimeContextStop time.Duration, timeoutPartsStart time.Duration,
	timeoutPartsStop time.Duration, timeoutDcpCloseUprStreams time.Duration,
	timeoutDcpCloseUprFeed time.Duration, cpuCollectionInterval time.Duration,
	resourceManagementInterval time.Duration, resourceManagementStatsInterval time.Duration,
	changesLeftThresholdForOngoingReplication int, resourceManagementRatioBase int,
	resourceManagementRatioUpperBound int, maxCountBacklogForSetDcpPriority int,
	maxCountNoBacklogForResetDcpPriority int, extraQuotaForUnderutilizedCPU int,
	throughputThrottlerLogInterval time.Duration,
	throughputThrottlerClearTokensInterval time.Duration,
	numberOfSlotsForThroughputThrottling int, intervalForThrottlerCalibration int,
	throughputSampleSize int, throughputSampleAlpha int,
	thresholdRatioForProcessCpu int, thresholdRatioForTotalCpu int,
	maxCountCpuNotMaxed int, maxCountThroughputDrop int,
	filteringInternalKey string, filteringInternalXattr string,
	remoteClusterAlternateAddrChangeCnt int,
	manifestRefreshSrcInterval int, manifestRefreshTgtInterval int,
	backfillPersistInterval time.Duration,
	httpsPortLookupTimeout time.Duration,
	jsEngineWorkers int,
	jsWorkerQuota int,
	maxCountDcpStreamsInactive int, resourceMgrKVDetectionRetryInterval time.Duration,
	utilsStopwatchDiagInternal time.Duration, utilsStopwatchDiagExternal time.Duration,
	replStatusLoadBrokenMapTimeout, replStatusExportBrokenMapTimeout time.Duration,
	topologyCooldownPeriod time.Duration, topologyErrCooldownPeriod time.Duration,
	blockedIpv4 bool, blockedIpv6 bool,
	peerToPeerTimeout, bucketTopologyGCScanTime, bucketTopologyGCPruneTime time.Duration,
	maxP2PReceiveChLen int,
	p2pOpaqueCleanupInterval, p2pVBRelatedGCInterval,
	throughSeqnoBgScannerFreq, throughSeqnoBgScannerLogFreq,
	timeoutP2PProtocol time.Duration,
	ckptCacheCtrlLen, ckptCacheReqLen int,
	humanRecoveryThreshold time.Duration,
	dnsSrvReBootstrap bool,
	p2pReplicaReplicatorReloadSize int, globalOSOMode int,
	connectionPreCheckGCTimeout time.Duration, connectionPreCheckRPCTimeout time.Duration,
	connErrsListMaxEntries, P2PRetryFactor int,
	P2PRetryWaitTimeMilliSec time.Duration,
	p2pManifestsGetterSleepTimeSecs int, p2pManifestsGetterMaxRetry int,
	datapoolLogFrequency int, capellaHostNameSuffix string,
	nwLatencyToleranceMilliSec time.Duration, casPoisoningPreCheckEnabled int,
	srcHeartbeatEnabled bool, srcHeartbeatIgnoreIncoming bool,
	srcHeartbeatSkipIntraCluster bool, srcHeartbeatSkipCapellaTarget bool,
	srcHeartbeatMinInterval time.Duration, srcHeartbeatMaxIntervalFactor int,
	rmTokenDistribution string, cLogSkipTlsVerify bool, cLogRMBoost int, cLogStatsMaxFreq time.Duration,
	tempMCErrorDisplayDelayFactor int,
	maxCheckpointRecordsToKeepVariableVB int, maxCheckpointRecordsToReadVariableVB int,
	buildVersion string) {
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

	if len(buildVersion) > 0 {
		GoxdcrHELOUserAgent = GoxdcrHELOUserAgentPrefix + KeyPartsDelimiter + buildVersion
		if len(GoxdcrHELOUserAgent) > HELOUserAgentNameMaxLen {
			GoxdcrHELOUserAgent = GoxdcrHELOUserAgentPrefix
		}
	} else {
		GoxdcrHELOUserAgent = GoxdcrHELOUserAgentPrefix
	}

	CapiMaxRetryBatchUpdateDocs = capiMaxRetryBatchUpdateDocs
	CapiBatchTimeout = capiBatchTimeout
	CapiWriteTimeout = capiWriteTimeout
	CapiReadTimeout = capiReadTimeout
	MaxCheckpointRecordsToKeepTraditional = maxCheckpointRecordsToKeepTraditional
	MaxCheckpointRecordsToReadTraditional = maxCheckpointRecordsToReadTraditional
	DefaultHttpTimeout = defaultHttpTimeout
	ShortHttpTimeout = shortHttpTimeout
	MaxRetryForLiveUpdatePipeline = maxRetryForLiveUpdatePipeline
	WaitTimeForLiveUpdatePipeline = waitTimeForLiveUpdatePipeline
	ReplSpecCheckInterval = replSpecCheckInterval
	MemStatsLogInterval = memStatsLogInterval
	MaxNumOfMetakvRetries = maxNumOfMetakvRetries
	RetryIntervalMetakv = retryIntervalMetakv
	UprFeedDataChanLength = uprFeedDataChanLength
	UprFeedBufferSize = uint32(uprFeedBufferSize)
	XmemMaxRetry = xmemMaxRetry
	XmemWriteTimeout = xmemWriteTimeout
	XmemReadTimeout = xmemReadTimeout
	XmemMaxReadDownTime = xmemMaxReadDownTime
	XmemBackoffWaitTime = xmemBackoffWaitTime
	XmemMaxBackoffFactor = xmemMaxBackoffFactor
	XmemMaxRetryNewConn = xmemMaxRetryNewConn
	XmemBackoffTimeNewConn = xmemBackoffTimeNewConn
	XmemSelfMonitorInterval = xmemSelfMonitorInterval
	XmemMaxIdleCount = xmemMaxIdleCount
	XmemMaxIdleCountLowerBound = xmemMaxIdleCountLowerBound
	XmemMaxIdleCountUpperBound = xmemMaxIdleCountUpperBound
	XmemMaxDataChanSize = xmemMaxDataChanSize
	XmemMaxBatchSize = xmemMaxBatchSize
	CapiRetryInterval = capiRetryInterval
	MaxLengthSnapshotHistory = maxLengthSnapshotHistory
	MaxRetryTargetStats = maxRetryTargetStats
	RetryIntervalTargetStats = retryIntervalTargetStats
	NumberOfSlotsForBandwidthThrottling = numberOfSlotsForBandwidthThrottling
	PercentageOfBytesToSendAsMin = percentageOfBytesToSendAsMin
	AuditWriteTimeout = auditWriteTimeout
	AuditReadTimeout = auditReadTimeout
	MaxRetryCapiService = maxRetryCapiService
	MaxNumberOfAsyncListeners = maxNumberOfAsyncListeners
	XmemMaxRetryInterval = xmemMaxRetryInterval
	XmemMaxRetryMutationLocked = xmemMaxRetryMutationLocked
	XmemMaxRetryIntervalMutationLocked = xmemMaxRetryIntervalMutationLocked
	HELOTimeout = heloTimeout
	WaitTimeBetweenMetadataChangeListeners = waitTimeBetweenMetadataChangeListeners
	KeepAlivePeriod = keepAlivePeriod
	ThresholdForEventChanSizeLogging = EventChanSize * thresholdPercentageForEventChanSizeLogging / 100
	ThresholdForThroughSeqnoComputation = thresholdForThroughSeqnoComputation
	StatsLogInterval = statsLogInterval
	XmemDefaultRespTimeout = xmemDefaultRespTimeout
	if !BypassSanInCertificateCheck /* If set to true in the source code, honor it */ {
		BypassSanInCertificateCheck = (bypassSanInCertificateCheck != 0)
	}
	ReplicationSpecGCCnt = replicationSpecGCCnt
	TimeoutRuntimeContextStart = timeoutRuntimeContextStart
	TimeoutRuntimeContextStop = timeoutRuntimeContextStop
	TimeoutPartsStart = timeoutPartsStart
	TimeoutPartsStop = timeoutPartsStop
	TimeoutDcpCloseUprStreams = timeoutDcpCloseUprStreams
	TimeoutDcpCloseUprFeed = timeoutDcpCloseUprFeed
	CpuCollectionInterval = cpuCollectionInterval
	ResourceManagementInterval = resourceManagementInterval
	ResourceManagementStatsInterval = resourceManagementStatsInterval
	ChangesLeftThresholdForOngoingReplication = changesLeftThresholdForOngoingReplication
	ResourceManagementRatioBase = resourceManagementRatioBase
	ResourceManagementRatioUpperBound = resourceManagementRatioUpperBound
	MaxCountBacklogForSetDcpPriority = maxCountBacklogForSetDcpPriority
	MaxCountNoBacklogForResetDcpPriority = maxCountNoBacklogForResetDcpPriority
	ExtraQuotaForUnderutilizedCPU = extraQuotaForUnderutilizedCPU
	ThroughputThrottlerLogInterval = throughputThrottlerLogInterval
	ThroughputThrottlerClearTokensInterval = throughputThrottlerClearTokensInterval
	NumberOfSlotsForThroughputThrottling = numberOfSlotsForThroughputThrottling
	IntervalForThrottlerCalibration = intervalForThrottlerCalibration
	ThroughputSampleSize = throughputSampleSize
	ThroughputSampleAlpha = throughputSampleAlpha
	ThresholdRatioForProcessCpu = thresholdRatioForProcessCpu
	ThresholdRatioForTotalCpu = thresholdRatioForTotalCpu
	MaxCountCpuNotMaxed = maxCountCpuNotMaxed
	MaxCountThroughputDrop = maxCountThroughputDrop
	InternalKeyKey = filteringInternalKey
	InternalKeyXattr = filteringInternalXattr
	CachedInternalKeyKeyByteSlice = []byte(InternalKeyKey)
	CachedInternalKeyKeyByteSize = len(CachedInternalKeyKeyByteSlice)
	CachedInternalKeyXattrByteSlice = []byte(InternalKeyXattr)
	CachedInternalKeyXattrByteSize = len(CachedInternalKeyXattrByteSlice)
	ReservedWordsMap = map[string]string{
		ExternalKeyKey:   InternalKeyKey, /* if this entry changes, CachedInternalKeyKeyByteSlice needs to change too */
		ExternalKeyXattr: InternalKeyXattr,
	}
	ReverseReservedWordsMap = map[string]string{
		InternalKeyKey:   ExternalKeyKeyContains,
		InternalKeyXattr: ExternalKeyXattrContains,
	}
	RemoteClusterAlternateAddrChangeCnt = remoteClusterAlternateAddrChangeCnt
	ManifestRefreshSrcInterval = manifestRefreshSrcInterval
	ManifestRefreshTgtInterval = manifestRefreshTgtInterval
	BackfillPersistInterval = backfillPersistInterval
	HttpsPortLookupTimeout = httpsPortLookupTimeout
	JSEngineWorkers = jsEngineWorkers
	JSWorkerQuota = jsWorkerQuota
	MaxCountStreamsInactive = maxCountDcpStreamsInactive
	ResourceMgrKVDetectionRetryInterval = resourceMgrKVDetectionRetryInterval
	DiagInternalThreshold = utilsStopwatchDiagInternal
	DiagNetworkThreshold = utilsStopwatchDiagExternal
	ReplStatusLoadBrokenMapTimeout = replStatusLoadBrokenMapTimeout
	ReplStatusExportBrokenMapTimeout = replStatusExportBrokenMapTimeout
	TopologySvcCoolDownPeriod = topologyCooldownPeriod
	TopologySvcErrCoolDownPeriod = topologyErrCooldownPeriod
	if blockedIpv4 {
		NetTCP = TCP6
		IpFamilyStr = "ipv6"
	} else if blockedIpv6 {
		NetTCP = TCP4
		IpFamilyStr = "ipv4"
	}
	IpFamilyOnlyErrorMessage = fmt.Sprintf("The cluster is %v only. ", IpFamilyStr)
	P2PCommTimeout = peerToPeerTimeout
	BucketTopologyGCScanTime = bucketTopologyGCScanTime
	BucketTopologyGCPruneTime = bucketTopologyGCPruneTime
	MaxP2PReceiveChLen = maxP2PReceiveChLen
	P2POpaqueCleanupInterval = p2pOpaqueCleanupInterval
	P2PVBRelatedGCInterval = p2pVBRelatedGCInterval
	ThroughSeqnoBgScannerFreq = throughSeqnoBgScannerFreq
	ThroughSeqnoBgScannerLogFreq = throughSeqnoBgScannerLogFreq
	TimeoutRuntimeContextStart = timeoutRuntimeContextStart
	TimeoutRuntimeContextStop = timeoutRuntimeContextStop
	TimeoutPartsStart = timeoutPartsStart
	TimeoutPartsStop = timeoutPartsStop
	TimeoutP2PProtocol = timeoutP2PProtocol
	CkptCacheCtrlChLen = ckptCacheReqLen
	CkptCacheReqChLen = ckptCacheReqLen
	HumanRecoveryThreshold = humanRecoveryThreshold
	DNSSrvReBootstrap = dnsSrvReBootstrap
	P2PReplicaReplicatorReloadChSize = p2pReplicaReplicatorReloadSize
	GlobalOSOSetting = GlobalOSOMode(globalOSOMode)
	ConnectionPreCheckGCTimeout = connectionPreCheckGCTimeout
	ConnectionPreCheckRPCTimeout = connectionPreCheckRPCTimeout
	ConnErrorsListMaxEntries = connErrsListMaxEntries
	PeerToPeerRetryWaitTime = P2PRetryWaitTimeMilliSec
	PeerToPeerRetryFactor = P2PRetryFactor
	ManifestsGetterSleepTimeSecs = p2pManifestsGetterSleepTimeSecs
	ManifestsGetterMaxRetry = p2pManifestsGetterMaxRetry
	DatapoolLogFrequency = datapoolLogFrequency
	CapellaHostnameSuffix = capellaHostNameSuffix
	NWLatencyToleranceMilliSec = nwLatencyToleranceMilliSec
	CasPoisoningPreCheckEnabled = casPoisoningPreCheckEnabled
	SrcHeartbeatEnabled = srcHeartbeatEnabled
	SrcHeartbeatIgnoreIncoming = srcHeartbeatIgnoreIncoming
	SrcHeartbeatSkipIntraCluster = srcHeartbeatSkipIntraCluster
	SrcHeartbeatSkipCapellaTarget = srcHeartbeatSkipCapellaTarget
	SrcHeartbeatMinInterval = srcHeartbeatMinInterval
	SrcHeartbeatMaxIntervalFactor = srcHeartbeatMaxIntervalFactor
	RMTokenDistributionStr = rmTokenDistribution

	// We ignore the error here because it is assumed that it will be validated
	// in the settings rest api
	RMTokenDistribution, _ = ParseRMTokenDistStr(RMTokenDistributionStr)

	CLogSkipTlsVerify = cLogSkipTlsVerify
	CLogResourceManagerBoost = cLogRMBoost
	CLogStatsLoggingMaxFreqInterval = cLogStatsMaxFreq

	TempMCErrorDisplayDelayFactor = tempMCErrorDisplayDelayFactor

	MaxCheckpointRecordsToKeepVariableVB = maxCheckpointRecordsToKeepVariableVB
	MaxCheckpointRecordsToReadVariableVB = maxCheckpointRecordsToReadVariableVB
}

// XDCR Dev hidden replication settings
const DevMainPipelineSendDelay = "xdcrDevMainSendDelayMs"
const DevBackfillPipelineSendDelay = "xdcrDevBackfillSendDelayMs"
const DevMainPipelineRollbackTo0VB = "xdcrDevMainRollbackTo0VB"
const DevBackfillRollbackTo0VB = "xdcrDevBackfillRollbackTo0VB"
const DevCkptMgrForceGCWaitSec = "xdcrDevCkptMgrForceGCWaitSec"
const DevColManifestSvcDelaySec = "xdcrDevColManifestSvcDelaySec"
const DevNsServerPortSpecifier = "xdcrDevNsServerPort" // Certain injection may apply to a specific node using this
const DevBackfillReplUpdateDelay = "xdcrDevBackfillReplUpdateDelayMs"
const DevCasDriftForceDocKey = "xdcrDevCasDriftInjectDocKey"
const DevPreCheckCasDriftForceVbKey = "xdcrDevPreCheckCasDriftInjectVb"
const DevPreCheckMaxCasErrorInjection = "xdcrDevPreCheckMaxCasErrorInjection"
const DevBackfillReqHandlerStartOnceDelay = "xdcrDevBackfillReqHandlerStartOnceDelaySec"
const DevBackfillReqHandlerHandleVBTaskDoneHang = "xdcrDevBackfillReqHandlerHandleVBTaskDoneHang"
const DevBackfillUnrecoverableErrorInj = "xdcrDevBackfillUnrecoverableErrorInj"
const DevBackfillMgrVbsTasksDoneNotifierDelay = "xdcrDevBackfillMgrVbsTasksDoneNotifierDelay"

// Need to escape the () to result in "META().xattrs" literal
const ExternalKeyXattr = "META\\(\\).xattrs"
const ExternalKeyKey = "META\\(\\).id"
const ExternalKeyKeyContains = "META().id"
const ExternalKeyXattrContains = "META().xattrs"

// This constant is used when communicating with KV to retrieve a list of all the available xattr keys
const XattributeToc = "$XTOC"

const FilterExpDelKey = "filter_exp_del"

const FilterExpKey = "filterExpiration"
const FilterDelKey = "filterDeletion"
const BypassExpiryKey = "filterBypassExpiry"
const BypassUncommittedTxnKey = "filterBypassUncommittedTxn"

const MergeFunctionMappingKey = "mergeFunctionMapping"

const JSFunctionTimeoutKey = "jsFunctionTimeoutMs"
const JSFunctionTimeoutDefault = 20000 // 20s. 10s is not enough in evaluator unit tests

const RetryOnRemoteAuthErrMaxWaitDefault = 3600   // seconds, 1 hour
const RetryOnErrExceptAuthErrMaxWaitDefault = 360 // seconds, 6 minutes

// UI+ns_server returned document content, keyed by special keys
const (
	BucketDocBodyKey  = "json"
	BucketDocMetaKey  = "meta"
	BucketDocXattrKey = "xattrs"
)

const TransactionClientRecordKey = "_txn:client-record"
const ActiveTransactionRecordPrefix = "^_txn:atr-"
const ValidVbucketRangeRegexpGroup = "([0-9]|[1-9][0-9]|[1-9][0-9][0-9]|[1][0][0-1][0-9]|[1][0][2][0-3])"
const ActiveTransactionRecordSuffix = "-#[0-9a-f]+$"

var ActiveTxnRecordRegexp *regexp.Regexp = regexp.MustCompile(fmt.Sprintf("%v%v%v", ActiveTransactionRecordPrefix, ValidVbucketRangeRegexpGroup, ActiveTransactionRecordSuffix))

const TransactionXattrKey = "txn"

const (
	ConflictLoggingXattrKey string = "_xdcr_conflict"
	ConflictLoggingXattrVal string = "true"
)

var ConflictLoggingXattrKeyBytes []byte = []byte(ConflictLoggingXattrKey)
var ConflictLoggingXattrValBytes []byte = []byte(ConflictLoggingXattrVal)

const BackfillPipelineTopicPrefix = "backfill_"

const MobileCompatibleKey = "mobile"

// Last element is invalid and is there to keep consistency with the EndMarker
var MobileCompatibilityStrings = [...]string{"Invalid", "Off", "Active", "Invalid"}

const (
	MobileCompatibilityStartMarker = iota
	// Off means no mobile running or mobile is on the source only
	MobileCompatibilityOff
	// Active means we have mobile on the source as well as target in active/active mode
	MobileCompatibilityActive
	MobileCompatibilityEndMarker
)

var (
	MobileDocPrefixSync    = []byte("_sync:")
	MobileDocPrefixSyncAtt = []byte("_sync:att")
)

// Conflict logging replication setting and associated keys
// The feature setting will look like the following:
//
//	"conflictLogging": {
//		"bucket": "bucketname",
//		"collection": "[scope].[collection]",
//		"loggingRules": { ... },
//		"disabled": [true|false]
//	}
const (
	CLogKey                  string = "conflictLogging"
	CLogBucketKey            string = "bucket"
	CLogCollectionKey        string = "collection"
	CLogLoggingRulesKey      string = "loggingRules"
	CLogDisabledKey          string = "disabled"
	CLogSetMetaTimeout       string = "cLogSetTimeoutMs"
	CLogPoolGetTimeout       string = "cLogPoolGetTimeoutMs"
	CLogNetworkRetryCount    string = "cLogNetworkRetryCount"
	CLogNetworkRetryInterval string = "cLogNetworkRetryIntervalMs"
	CLogWorkerCount          string = "cLogWorkerCount"
	CLogQueueCapacity        string = "cLogQueueCapacity"
	CLogMaxErrorCount        string = "cLogMaxErrorCount"
	CLogErrorTimeWindow      string = "cLogErrorTimeWindowMs"
	CLogReattemptDuration    string = "cLogReattemptDurationMs"
	CLogConnPoolLimit        string = "cLogConnPoolLimit"
	CLogConnPoolGCInterval   string = "cLogConnPoolGCIntervalMs"
	CLogConnPoolReapInterval string = "cLogConnPoolReapIntervalMs"
)

// simple keys inside conflict logging mapping. It excludes loggingRules key.
var SimpleCLogKeys map[string]reflect.Kind = map[string]reflect.Kind{
	CLogBucketKey:     reflect.String,
	CLogCollectionKey: reflect.String,
	CLogDisabledKey:   reflect.Bool,
}

// simple keys inside loggingRules mapping.
var SimpleConflictLoggingRulesKeys map[string]reflect.Kind = map[string]reflect.Kind{
	CLogBucketKey:     reflect.String,
	CLogCollectionKey: reflect.String,
}

const (
	// conflict logger default consts
	DefaultCLogSetMetaTimeoutMs       int = 5000 // in milliseconds
	DefaultCLogPoolGetTimeoutMs       int = 5000 // in milliseconds
	DefaultCLogNetworkRetryCount      int = 5
	DefaultCLogNetworkRetryIntervalMs int = 2000 // in milliseconds
	DefaultCLogWorkerCount            int = 20
	// Assuming a default of 3 node capella cluster, and a default of 2 xmem nozzles
	// per target node and 1000 is the default max buffer size for a given xmem nozzle.
	DefaultCLogQueueCapacity int = 3 * 2 * 1000

	// The following means that the logger will have a tolerance for less than 10 timeout/throttling/mapping errors in the last 2 minutes.
	// If we see 10 errors, the conflict logger will hibernate and stop accepting requests for the next 10 minutes.
	DefaultCLogMaxErrorCount       int = 10
	DefaultCLogErrorTimeWindowMs   int = 2 * 60 * 1000  // in milliseconds (2 minutes)
	DefaultCLogReattemptDurationMs int = 10 * 60 * 1000 // in milliseconds (10 minutes)

	// conflict logger's Connection Pool consts
	// maximum number of connection objects for a given bucket in the pool.
	// It is assumed that an average of 3 replications with conflict logging to the same bucket could exist.
	DefaultCLogPoolConnLimit = 3 * 10
	// DefaultPoolGCInterval is the GC frequency for connection pool
	DefaultCLogConnPoolGCIntervalMs = 60000 // in milliseconds
	// DefaultPoolReapInterval is the last used threshold for reaping unused connections
	DefaultCLogConnPoolReapIntervalMs = 120000 // in milliseconds
)

// Used for internal testing with self-signed certs.
// Exposed as an internal setting.
var CLogSkipTlsVerify bool
var CLogResourceManagerBoost int
var CLogStatsLoggingMaxFreqInterval time.Duration = 30 * 60 * time.Second

// Required for conflict resolution
const (
	PERIOD      = "."
	IMPORTCAS   = "cas"
	PREVIOUSREV = "pRev"

	// This is for subdoc set operation
	CAS_MACRO_EXPANSION = "\"${Mutation.CAS}\"" // The value for the cv field when setting back to source
	// These are for subdoc get operations
	VXATTR_REVID    = "$document.revid"
	VXATTR_FLAGS    = "$document.flags"
	VXATTR_EXPIRY   = "$document.exptime"
	VXATTR_DATATYPE = "$document.datatype"
	VXATTR_VBUUID   = "$document.vbucket_uuid"
	VXATTR_SEQNO    = "$document.seqno"
	// The leading "_" indicates a system XATTR
	XATTR_MOBILE = "_sync"
	// This is the HLV XATTR name.
	XATTR_HLV = "_vv"  // The HLV XATTR
	XATTR_MOU = "_mou" // The Metadata Only Update XATTR

	// nested xattrs
	XATTR_IMPORTCAS   = XATTR_MOU + PERIOD + IMPORTCAS
	XATTR_PREVIOUSREV = XATTR_MOU + PERIOD + PREVIOUSREV

	FunctionUrlFmt         = "http://%v:%v/evaluator/v1/libraries"
	DefaultMergeFunc       = "defaultLWW"
	DefaultMergeFuncBodyCC = "function " + DefaultMergeFunc + "(key, sourceDoc, sourceCas, sourceId, targetDoc, targetCas, targetId) {" +
		"if (sourceCas >= targetCas) {return sourceDoc; } else {return targetDoc; } } "
	BucketMergeFunctionKey = "default"

	CCRKVRestCallRetryInterval = 2 * time.Second
)

var MouXattrValuesForCR []string = []string{
	IMPORTCAS,
	PREVIOUSREV,
}

const (
	// Bucket setting for version vector pruning
	VersionPruningWindowHrsKey = "versionPruningWindowHrs"
	// Bucket setting for enable versioning when CR mode is not custom CR
	EnableCrossClusterVersioningKey = "enableCrossClusterVersioning"
	// Bucket setting for starting timestamp (CAS) to eanble versioning
	HlvVbMaxCasKey = "vbucketsMaxCas"
	// The target cluster counterparts for above.
	TargetHlvVbMaxCasKey = "targetVbucketsMaxCas"
)

const DcpSeqnoEnd = uint64(0xFFFFFFFFFFFFFFFF)

const RetryOnRemoteAuthErrKey = "retryOnRemoteAuthErr"
const RetryOnRemoteAuthErrMaxWaitSecKey = "retryOnRemoteAuthErrMaxWaitSec"
const RetryOnErrExceptAuthErrMaxWaitSecKey = "retryOnErrExceptAuthErrMaxWaitSec"

// DCP inactive stream monitor will sleep every "dcp_inactive_stream_check_interval" (30sec)
// Once this max is hit, it'll retry streamReq with DCP
var MaxCountStreamsInactive = 10

// Stopwatch timers
var DiagNetworkThreshold = 5000 * time.Millisecond
var DiagInternalThreshold = 2000 * time.Millisecond
var DiagVBMasterHandleThreshold = 20 * time.Second
var DiagCkptMergeThreshold = 30 * time.Second
var DiagCkptStopTheWorldThreshold = 10 * time.Second
var DiagStopTheWorldAndMergeCkptThreshold = DiagCkptMergeThreshold + DiagCkptStopTheWorldThreshold
var DiagTopologyMonitorThreshold = 5 * time.Second

// Pprof goroutines dump types
type PprofLookupTypes string

const (
	PprofAllGoroutines PprofLookupTypes = "goroutine"
	PprofHeapProfile   PprofLookupTypes = "heap"
	PprofthreadCreate  PprofLookupTypes = "threadcreate"
	PprofBlocking      PprofLookupTypes = "block"
)

func (p PprofLookupTypes) String() string {
	return string(p)
}

const (
	IpFamilyRequiredOption = "required"
	IpFamilyOptionalOption = "optional"
	IpFamilyOffOption      = "off"
)

type IpFamilySupport int

const (
	IpFamilyRequired IpFamilySupport = 1
	IpFamilyOptional IpFamilySupport = 2
	IpFamilyOff      IpFamilySupport = 3
)

const PreReplicateVBMasterCheckKey = "preReplicateVBMasterCheck"

const ReplicateCkptIntervalKey = "replicateCkptIntervalMin"

var ReplicateCkptInterval = 20 * time.Minute

const (
	SourceNozzlePerNode = "sourceNozzlePerNode"
	TargetNozzlePerNode = "targetNozzlePerNode"
)

var CkptCacheCtrlChLen = 10
var CkptCacheReqChLen = 1000

const CkptSvcCacheEnabled = "ckptSvcCacheEnabled"

var HumanRecoveryThreshold = 5 * time.Minute

const FilterSystemScope = "filterSystemScope"

var DNSSrvReBootstrap = true

const EnableDcpPurgeRollback = "dcpEnablePurgeRollback"

const TargetTopologyLogFreqKey = "targetTopologyLogFrequency"

// Each iteration of monitorTarget will be exectued every 10 seconds by default.
// So for the logging to take place once every 5 hours (18000 seconds), we need to wait for 1800 iteration of monitorTarget
var TargetTopologyLogFreqVal = 1800

type GlobalOSOMode int

const (
	GlobalOSONoOp GlobalOSOMode = iota // Let individual replication's setting take effect
	GlobalOSOOff  GlobalOSOMode = iota // Override individual replication to force OSO off
	GlobalOSOMax  GlobalOSOMode = iota // Boundary checking - Invalid for usage
)

var GlobalOSOSetting = GlobalOSONoOp

const FilterBinaryDocs = "filterBinary"

/* Connection Pre-Check */
type ConnPreChkMsgType int

const (
	ConnPreChkInCompatibleVersion ConnPreChkMsgType = iota
	ConnPreChkUnableToFetchUUID
	ConnPreChkIsIntraClusterReplication
	ConnPreChkSendingRequest
	ConnPreChkResponseWait
	ConnPreChkResponseObtained
	ConnPreChkP2PSuccessful
	ConnPreChkSuccessful
	ConnPreChkWarnForMissingHostname
)

var ConnectionPreCheckMsgs = map[ConnPreChkMsgType]string{
	ConnPreChkInCompatibleVersion:       "This version of some or all the nodes doesn't support the connection pre-check",
	ConnPreChkUnableToFetchUUID:         "Unable to fetch source cluster's UUID",
	ConnPreChkIsIntraClusterReplication: "Intra-cluster replication detected, skipping connection pre-check",
	ConnPreChkSendingRequest:            "Sending requests to the peer",
	ConnPreChkResponseWait:              "P2PSend was successful, waiting for the node's response",
	ConnPreChkResponseObtained:          "Response obtained from the node, storing the results",
	ConnPreChkP2PSuccessful:             "P2P protocol successfully executed, no errors",
	ConnPreChkSuccessful:                "Connection check was successful, no errors",
	ConnPreChkWarnForMissingHostname:    "WARNING: The result might be inaccurate because the target cluster has only one node, with hostname most likely configured as localhost",
}

const (
	KVIdxForConnPreChk      = iota
	KVSSLIdxForConnPreChk   = iota
	MgmtIdxForConnPreChk    = iota
	MgmtSSLIdxForConnPreChk = iota
)

var PortsKeysForConnectionPreCheck = map[PortType]string{
	KVIdxForConnPreChk:      KVPortKey,
	KVSSLIdxForConnPreChk:   KVSSLPortKey,
	MgmtIdxForConnPreChk:    MgtPortKey,
	MgmtSSLIdxForConnPreChk: SSLMgtPortKey,
}

var ConnectionPreCheckGCTimeout = 120 * time.Second
var ConnectionPreCheckRPCTimeout = 15 * time.Second

const ConnectionPreCheckTaskId string = "taskId"

var ConnErrorsListMaxEntries = 20
var ManifestsGetterSleepTimeSecs = 1
var ManifestsGetterMaxRetry = 8

var DatapoolLogFrequency = 10

const PipelineFullTopic string = "pipelineFullTopic"

const (
	DocsFiltered                       = "docs_filtered"
	DocsUnableToFilter                 = "docs_unable_to_filter"
	ExpiryFiltered                     = "expiry_filtered"
	DeletionFiltered                   = "deletion_filtered"
	SetFiltered                        = "set_filtered"
	BinaryFiltered                     = "binary_filtered"
	ExpiryStripped                     = "expiry_stripped"
	AtrTxnDocsFiltered                 = "atr_txn_docs_filtered"
	ClientTxnDocsFiltered              = "client_txn_docs_filtered"
	DocsFilteredOnTxnXattr             = "docs_filtered_on_txn_xattr"
	DocsFilteredOnUserDefinedFilter    = "docs_filtered_on_user_defined_filter"
	MobileDocsFiltered                 = "mobile_docs_filtered"
	GuardrailResidentRatio             = "guardrail_resident_ratio"
	GuardrailDataSize                  = "guardrail_data_size"
	GuardrailDiskSpace                 = "guardrail_disk_space"
	DocsSentWithSubdocSet              = "docs_sent_with_subdoc_set"
	DocsSentWithSubdocDelete           = "docs_sent_with_subdoc_delete"
	DocsSentWithPoisonedCasErrorMode   = "docs_sent_with_poisonedCas_errorMode"
	DocsSentWithPoisonedCasReplaceMode = "docs_sent_with_poisonedCas_replaceMode"
	DocsCasPoisoned                    = "docs_cas_poisoned"
	ConflictDocsFiltered               = "clog_docs_filtered"
	SrcConflictDocsWritten             = "clog_src_docs_written"
	TgtConflictDocsWritten             = "clog_tgt_docs_written"
	CRDConflictDocsWritten             = "clog_crd_docs_written"
	TrueConflictsDetected              = "true_conflicts_detected"
	CLogHibernatedCount                = "clog_hibernated_count"
	GetDocsCasChangedCount             = "get_docs_cas_changed"
	SubdocCmdsSkippedCount             = "subdoc_cmd_docs_skipped"
)

var ValidJsonEnds []byte = []byte{
	'}', ']',
}

const (
	EmptyJsonObject string = "{}"
	EmptyJsonArray  string = "[]"
)

const (
	CASDriftThresholdSecsKey          = "casDriftThresholdSecs"
	PreCheckCasDriftThresholdHoursKey = "preCheckCasDriftThresholdHours"
)

const CASDriftLiveDetected = "One or more documents are not replicated because their CAS values are beyond the acceptable drift threshold"
const PreCheckCASDriftDetected = "The following VBs have time drift (nanoSecs) beyond acceptable threshold"

var NWLatencyToleranceMilliSec = 10000 * time.Millisecond

// names of services to be used for setting loggerContext's
// this list also contains some of the single ton loggers declared at package level
const (
	UtilsKey                  = "UtilsService"
	SecuritySvcKey            = "SecurityService"
	TopoSvcKey                = "TopologyService"
	MetadataSvcKey            = "MetaKVMetadataService"
	IntSettSvcKey             = "InternalSettingsService"
	AuditSvcKey               = "AuditService"
	GlobalSettSvcKey          = "GlobalSettingsService"
	RemClusterSvcKey          = "RemoteClusterService"
	ReplSpecSvcKey            = "ReplicationSpecService"
	CheckpointSvcKey          = "CheckpointService"
	MigrationSvcKey           = "MigrationService"
	ReplSettSvcKey            = "ReplicationSettingService"
	BucketTopologySvcKey      = "BucketTopologyService"
	ManifestServiceKey        = "ManifestService"
	CollectionsManifestSvcKey = "CollectionsManifestService"
	BackfillReplSvcKey        = "BackfillReplicationService"
	P2PManagerKey             = "P2PManagerService"
	CapiSvcKey                = "CapiService"
	TpThrottlerSvcKey         = "ThroughputThrottlerService"
	GenericSupervisorKey      = "GenericSupervisor"
	XDCRFactoryKey            = "XDCRFactory"
	PipelineMgrKey            = "PipelineManager"
	ResourceMgrKey            = "ResourceManager"
	BackfillMgrKey            = "BackfillManager"
	DefaultKey                = "Default"
	AdminPortKey              = "AdminPort"
	HttpServerKey             = "HttpServer"
	MsgUtilsKey               = "MessageUtils"
	CLogManagerKey            = "CLogManager"
)

// This is exposed as an internal setting (which triggers process restart which is necessary),
// which needs to be turned on if new pipeline cas poisoning check is required.
// 0 means disabled, 1 means enabled.
var CasPoisoningPreCheckEnabled int = 0

func IsCasPoisoningPreCheckEnabled() bool {
	return CasPoisoningPreCheckEnabled > 0
}

var BodySpec SubdocLookupPathSpec = SubdocLookupPathSpec{
	Opcode: GET,
	Flags:  0,
	Path:   nil,
}

// Client Cert related consts
const (
	ErrorStringClientCertMandatory = "tls: certificate required"
	StateKey                       = "state"
	MandatoryVal                   = "mandatory"
	HybridVal                      = "hybrid"
)

// Target Awareness (Heartbeat) related Internal Setting
var (
	SrcHeartbeatEnabled           = true             // to enable/disable sending heartbeats as a Source cluster
	SrcHeartbeatIgnoreIncoming    = false            // to accept or ignore heartbeats from remote Source clusters
	SrcHeartbeatSkipIntraCluster  = true             // to skip sending heartbeats for intra-cluster replications
	SrcHeartbeatSkipCapellaTarget = true             // to skip sending heartbeats to a 'Capella' Target cluster
	SrcHeartbeatMinInterval       = 60 * time.Second // upper bound on heartbeat frequency
	SrcHeartbeatMaxIntervalFactor = 5                // factor to set lower bound on heartbeat frequency
)

const (
	XDCRSourceClustersPath      = XDCRPrefix + "/sourceClusters"
	SrcHeartbeatExpiryFactor    = 2
	SrcHeartbeatSummaryInterval = 5 * time.Minute
	UnknownSourceClusterName    = "<Unknown>"
)

func SrcHeartbeatMaxInterval() time.Duration { // lower bound on heartbeat frequency
	return time.Duration(SrcHeartbeatMaxIntervalFactor) * SrcHeartbeatMinInterval
}

// The default resource manager token distribution ratio:
// 89 for high priority replications,
// 9 for low priority replications,
// 3 for all the conflict loggers in the system.
var RMTokenDistributionStr string = "89:9:3"

// RMTokenDistributionPartsCount is the number of parts in a token distribution string
// The reason this is not derived from RMTokenDistribution is because it self
// can be modified when the RMTokenDistributionStr is parsed
var RMTokenDistributionPartsCount int = 3

// RMTokenDistribution for different resource consumers
var RMTokenDistribution = []int{
	89, // High priority replication %
	8,  // Low priority replication %
	3,  // Conflict log %
}

const SkipReplSpecAutoGcKey = "skipReplSpecAutoGc"

const ROUTER_NAME_PREFIX = "Router"

var RouterVBMetricKeys = []string{DocsFiltered, DocsUnableToFilter, ExpiryFiltered,
	DeletionFiltered, SetFiltered, BinaryFiltered, ExpiryStripped,
	AtrTxnDocsFiltered, ClientTxnDocsFiltered, DocsFilteredOnTxnXattr,
	MobileDocsFiltered, DocsFilteredOnUserDefinedFilter, DocsCasPoisoned, ConflictDocsFiltered}

var OutNozzleVBMetricKeys = []string{GuardrailResidentRatio, GuardrailDataSize, GuardrailDiskSpace,
	DocsSentWithSubdocSet, DocsSentWithSubdocDelete,
	DocsSentWithPoisonedCasErrorMode, DocsSentWithPoisonedCasReplaceMode,
	GetDocsCasChangedCount, TrueConflictsDetected, SubdocCmdsSkippedCount}

var CLogVBMetricKeys = []string{SrcConflictDocsWritten, TgtConflictDocsWritten, CRDConflictDocsWritten, CLogHibernatedCount}

// After a successful pre_replicate call, how long the information stays around for
var GlobalPreReplicateCacheExpireTimeSecs = 10

// After a failed pre_replicate call that returns an error, how long the error stays around for before a retry
// Generally speaking, pre_replicate should not fail. If it fails, ckpt resume/rollback
// would generally error out and XDCR should avoid hammering ns_server
var GlobalPreReplicateCacheErrorExpireTimeSecs = 30

// from https://github.com/couchbase/kv_engine/blob/master/docs/SubDocument.md#limits
const SUBDOC_MULTI_MAX_PATHS int = 16

const ExternalAddressSetup = "externalAddressSetup"

const PipelineReinitHash = "pipelineReinitHash"

// constant used for paring get all replications request
const Backfill = "backfill"

const DisableHlvBasedShortCircuitKey string = "disableHlvBasedShortCircuit"
