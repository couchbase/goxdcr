// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package replication_manager

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"sync/atomic"

	ap "github.com/couchbase/goxdcr/adminport"
	"github.com/couchbase/goxdcr/base"
	base2 "github.com/couchbase/goxdcr/base/helpers"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
)

// xdcr prefix for internal settings keys
var ErrorsKey = "errors"

// constants used for parsing url path
const (
	CreateReplicationPath       = "controller/createReplication"
	StatisticsPrefix            = "stats/buckets"
	RegexpValidationPrefix      = "controller/regexpValidation"
	AllReplicationsPath         = "pools/default/replications"
	AllReplicationInfosPath     = "pools/default/replicationInfos"
	DeleteReplicationPrefix     = "controller/cancelXDCR"
	SettingsReplicationsPath    = "settings/replications"
	MemStatsPath                = "stats/mem"
	BlockProfileStartPath       = "profile/block/start"
	BlockProfileStopPath        = "profile/block/stop"
	XDCRInternalSettingsPath    = base.XDCRPrefix + "/internalSettings"
	XDCRPrometheusStatsPath     = "_prometheusMetrics"
	XDCRPrometheusStatsHighPath = "_prometheusMetricsHigh"

	// Some url paths are not static and have variable contents, e.g., settings/replications/$replication_id
	// The message keys for such paths are constructed by appending the dynamic suffix below to the static portion of the path.
	// e.g., settings/replications/dynamic
	DynamicSuffix = "/dynamic"
)

// constants used for parsing replication settings
const (
	ReplicationType                   = "replicationType"
	FilterExpression                  = "filterExpression"
	PauseRequested                    = "pauseRequested"
	CheckpointInterval                = "checkpointInterval"
	BatchCount                        = "workerBatchSize"
	BatchSize                         = "docBatchSizeKb"
	FailureRestartInterval            = "failureRestartInterval"
	OptimisticReplicationThreshold    = "optimisticReplicationThreshold"
	SourceNozzlePerNode               = base.SourceNozzlePerNode
	TargetNozzlePerNode               = base.TargetNozzlePerNode
	MaxExpectedReplicationLag         = "maxExpectedReplicationLag"
	TimeoutPercentageCap              = "timeoutPercentageCap"
	LogLevel                          = "logLevel"
	StatsInterval                     = "statsInterval"
	BandwidthLimit                    = "networkUsageLimit"
	ReplicationTypeValue              = "continuous"
	GoMaxProcs                        = "goMaxProcs"
	GoGC                              = "goGC"
	FilterVersionKey                  = "filterVersion"
	FilterSkipRestreamKey             = "filterSkipRestream"
	Priority                          = "priority"
	BacklogThreshold                  = "desiredLatency" // desired latency is the parameter exposed to UI and CLI
	FilterExpKey                      = "filterExpiration"
	FilterDelKey                      = "filterDeletion"
	BypassExpiryKey                   = "filterBypassExpiry" // bypass sounds better to external, translates into strip internally
	BypassUncommittedTxnKey           = base.BypassUncommittedTxnKey
	CollectionsMappingKey             = base.CollectionsMappingKey
	CollectionsMirrorKey              = base.CollectionsMirrorKey
	CollectionsMigrateKey             = base.CollectionsMigrateKey
	CollectionsOsoKey                 = base.CollectionsOsoKey
	CollectionsMappingRulesKey        = base.CollectionsMappingRulesKey
	ManualBackfillRequested           = base.ManualBackfillKey
	CollectionsDelAllBackfillKey      = base.CollectionsDelAllBackfillKey
	CollectionsDelVbBackfillKey       = base.CollectionsDelSingleVBBackfillKey
	RetryOnRemoteAuthErrKey           = base.RetryOnRemoteAuthErrKey
	RetryOnRemoteAuthErrMaxWaitKey    = base.RetryOnRemoteAuthErrMaxWaitSecKey
	RetryOnErrExceptAuthErrMaxWaitKey = base.RetryOnErrExceptAuthErrMaxWaitSecKey
	MergeFunctionMappingKey           = base.MergeFunctionMappingKey
	JSFunctionTimeoutKey              = base.JSFunctionTimeoutKey
	DismissEventKey                   = metadata.DismissEventKey
	PreReplicateVBMasterCheckKey      = base.PreReplicateVBMasterCheckKey
	ReplicateCkptIntervalKey          = base.ReplicateCkptIntervalKey
	CkptSvcCacheEnabledKey            = metadata.CkptSvcCacheEnabledKey
	FilterSystemScopeKey              = base.FilterSystemScope
	FilterBinaryDocsKey               = metadata.FilterBinaryDocsKey
	MobileCompatibleKey               = base.MobileCompatibleKey
	EnableDcpPurgeRollback            = base.EnableDcpPurgeRollback
)

// constants for parsing create/change/view replication response
const (
	ReplicationId = "id"
	Warnings      = "warnings"
	Result        = "result"
)

// constants for RegexpValidation request
const (
	// Input
	Expression = "expression"
	DocID      = "docId"
	Bucket     = "bucket"
	Scope      = "scope"
	Collection = "collection"
	SkipDoc    = "skipDoc"
	// Output
	MatchResult = "result"
	MatchError  = "error"
)

// constants for stats names
const (
	DocsWritten          = "docs_written"
	DataReplicated       = "data_replicated"
	ChangesLeft          = "changes_left"
	DocsChecked          = "docs_checked"
	NumCheckpoints       = "num_checkpoints"
	NumFailedCheckpoints = "num_failedckpts"
	SizeRepQueue         = "size_rep_queue"
	TimeCommiting        = "time_committing"
	BandWidthUsage       = "bandwidth_usage"
	DocsLatencyAppr      = "docs_latency_aggr"
	DocsLatencyWt        = "docs_latency_wt"
	DocsRepQueue         = "docs_req_queue"
	MetaLatencyAggr      = "meta_latency_aggr"
	MetaLatencyWt        = "meta_latency_wt"
	RateReplication      = "rate_replication"
	DocsOptRepd          = "docs_opt_repd"
	ActiveVbreps         = "active_vbreps"
	WaitingVbreps        = "waiting_vbreps"
	TimeWorking          = "time_working"
	TimeoutPercentageMap = "timeout_percentage_map"
)

const (
	TASKID   string = "taskId"
	RESULT   string = "result"
	DONE     string = "done"
	HOSTNAME string = "hostname"
	USERNAME string = "username"
)

// errors
var ErrorParsingForm = errors.New("Error parsing http request")
var MissingSettingsInRequest = errors.New("Invalid http request. No replication setting parameters have been supplied.")
var MissingOldSettingsInRequest = errors.New("Invalid http request. No old replication settings have been supplied.")

// replication settings key in rest api -> internal replication settings key
var RestKeyToSettingsKeyMap = map[string]string{
	base.DevMainPipelineSendDelay:     metadata.DevMainPipelineSendDelay,
	base.DevBackfillPipelineSendDelay: metadata.DevBackfillPipelineSendDelay,
	base.DevMainPipelineRollbackTo0VB: metadata.DevMainPipelineRollbackTo0VB,
	base.DevBackfillRollbackTo0VB:     metadata.DevBackfillRollbackTo0VB,
	base.DevCkptMgrForceGCWaitSec:     metadata.DevCkptMgrForceGCWaitSec,
	base.DevColManifestSvcDelaySec:    metadata.DevColManifestSvcDelaySec,
	base.DevNsServerPortSpecifier:     metadata.DevNsServerPortSpecifier,
	base.DevBucketTopologyLegacyDelay: metadata.DevBucketTopologyLegacyDelay,
	base.DevBackfillReplUpdateDelay:   metadata.DevBackfillReplUpdateDelay,
	base.Type:                         metadata.ReplicationTypeKey,
	FilterExpression:                  metadata.FilterExpressionKey,
	PauseRequested:                    metadata.ActiveKey,
	CheckpointInterval:                metadata.CheckpointIntervalKey,
	BatchCount:                        metadata.BatchCountKey,
	BatchSize:                         metadata.BatchSizeKey,
	FailureRestartInterval:            metadata.FailureRestartIntervalKey,
	OptimisticReplicationThreshold:    metadata.OptimisticReplicationThresholdKey,
	SourceNozzlePerNode:               metadata.SourceNozzlePerNodeKey,
	TargetNozzlePerNode:               metadata.TargetNozzlePerNodeKey,
	LogLevel:                          metadata.PipelineLogLevelKey,
	StatsInterval:                     metadata.PipelineStatsIntervalKey,
	BandwidthLimit:                    metadata.BandwidthLimitKey,
	GoMaxProcs:                        metadata.GoMaxProcsKey,
	GoGC:                              metadata.GoGCKey,
	base.CompressionTypeREST:          metadata.CompressionTypeKey,
	FilterVersionKey:                  metadata.FilterVersionKey,
	FilterSkipRestreamKey:             metadata.FilterSkipRestreamKey,
	Priority:                          metadata.PriorityKey,
	BacklogThreshold:                  metadata.BacklogThresholdKey,
	FilterExpKey:                      metadata.FilterExpKey,
	FilterDelKey:                      metadata.FilterDelKey,
	BypassExpiryKey:                   metadata.BypassExpiryKey,
	BypassUncommittedTxnKey:           metadata.BypassUncommittedTxnKey,
	CollectionsMappingKey:             metadata.CollectionsMgtMappingKey,
	CollectionsMigrateKey:             metadata.CollectionsMgtMigrateKey,
	CollectionsMirrorKey:              metadata.CollectionsMgtMirrorKey,
	CollectionsOsoKey:                 metadata.CollectionsMgtOsoKey,
	CollectionsMappingRulesKey:        metadata.CollectionsMappingRulesKey,
	ManualBackfillRequested:           metadata.CollectionsManualBackfillKey,
	CollectionsDelAllBackfillKey:      metadata.CollectionsDelAllBackfillKey,
	CollectionsDelVbBackfillKey:       metadata.CollectionsDelVbBackfillKey,
	RetryOnRemoteAuthErrKey:           metadata.RetryOnRemoteAuthErrKey,
	RetryOnRemoteAuthErrMaxWaitKey:    metadata.RetryOnRemoteAuthErrMaxWaitSecKey,
	RetryOnErrExceptAuthErrMaxWaitKey: metadata.RetryOnErrExceptAuthErrMaxWaitSecKey,
	MergeFunctionMappingKey:           metadata.MergeFunctionMappingKey,
	JSFunctionTimeoutKey:              metadata.JSFunctionTimeoutKey,
	DismissEventKey:                   metadata.DismissEventKey,
	PreReplicateVBMasterCheckKey:      metadata.PreReplicateVBMasterCheckKey,
	ReplicateCkptIntervalKey:          metadata.ReplicateCkptIntervalKey,
	CkptSvcCacheEnabledKey:            metadata.CkptSvcCacheEnabledKey,
	FilterSystemScopeKey:              metadata.FilterSystemScopeKey,
	FilterBinaryDocsKey:               metadata.FilterBinaryDocsKey,
	MobileCompatibleKey:               metadata.MobileCompatibleKey,
	EnableDcpPurgeRollback:            metadata.EnableDcpPurgeRollback,
}

// internal replication settings key -> replication settings key in rest api
var SettingsKeyToRestKeyMap = map[string]string{
	metadata.DevMainPipelineSendDelay:             base.DevMainPipelineSendDelay,
	metadata.DevBackfillPipelineSendDelay:         base.DevBackfillPipelineSendDelay,
	metadata.DevMainPipelineRollbackTo0VB:         base.DevMainPipelineRollbackTo0VB,
	metadata.DevBackfillRollbackTo0VB:             base.DevBackfillRollbackTo0VB,
	metadata.DevCkptMgrForceGCWaitSec:             base.DevCkptMgrForceGCWaitSec,
	metadata.DevColManifestSvcDelaySec:            base.DevColManifestSvcDelaySec,
	metadata.DevNsServerPortSpecifier:             base.DevNsServerPortSpecifier,
	metadata.DevBucketTopologyLegacyDelay:         base.DevBucketTopologyLegacyDelay,
	metadata.DevBackfillReplUpdateDelay:           base.DevBackfillReplUpdateDelay,
	metadata.ReplicationTypeKey:                   base.Type,
	metadata.FilterExpressionKey:                  FilterExpression,
	metadata.ActiveKey:                            PauseRequested,
	metadata.CheckpointIntervalKey:                CheckpointInterval,
	metadata.BatchCountKey:                        BatchCount,
	metadata.BatchSizeKey:                         BatchSize,
	metadata.FailureRestartIntervalKey:            FailureRestartInterval,
	metadata.OptimisticReplicationThresholdKey:    OptimisticReplicationThreshold,
	metadata.SourceNozzlePerNodeKey:               SourceNozzlePerNode,
	metadata.TargetNozzlePerNodeKey:               TargetNozzlePerNode,
	metadata.PipelineLogLevelKey:                  LogLevel,
	metadata.PipelineStatsIntervalKey:             StatsInterval,
	metadata.BandwidthLimitKey:                    BandwidthLimit,
	metadata.GoMaxProcsKey:                        GoMaxProcs,
	metadata.GoGCKey:                              GoGC,
	metadata.CompressionTypeKey:                   base.CompressionTypeREST,
	metadata.FilterVersionKey:                     FilterVersionKey,
	metadata.FilterSkipRestreamKey:                FilterSkipRestreamKey,
	metadata.PriorityKey:                          Priority,
	metadata.BacklogThresholdKey:                  BacklogThreshold,
	metadata.FilterExpKey:                         FilterExpKey,
	metadata.FilterDelKey:                         FilterDelKey,
	metadata.BypassExpiryKey:                      BypassExpiryKey,
	metadata.BypassUncommittedTxnKey:              BypassUncommittedTxnKey,
	metadata.CollectionsMgtMappingKey:             CollectionsMappingKey,
	metadata.CollectionsMgtMigrateKey:             CollectionsMigrateKey,
	metadata.CollectionsMgtMirrorKey:              CollectionsMirrorKey,
	metadata.CollectionsMgtOsoKey:                 CollectionsOsoKey,
	metadata.CollectionsMappingRulesKey:           CollectionsMappingRulesKey,
	metadata.CollectionsManualBackfillKey:         ManualBackfillRequested,
	metadata.CollectionsDelAllBackfillKey:         CollectionsDelAllBackfillKey,
	metadata.CollectionsDelVbBackfillKey:          CollectionsDelVbBackfillKey,
	metadata.RetryOnRemoteAuthErrKey:              RetryOnRemoteAuthErrKey,
	metadata.RetryOnRemoteAuthErrMaxWaitSecKey:    RetryOnRemoteAuthErrMaxWaitKey,
	metadata.RetryOnErrExceptAuthErrMaxWaitSecKey: RetryOnErrExceptAuthErrMaxWaitKey,
	metadata.MergeFunctionMappingKey:              MergeFunctionMappingKey,
	metadata.JSFunctionTimeoutKey:                 JSFunctionTimeoutKey,
	metadata.DismissEventKey:                      DismissEventKey,
	metadata.PreReplicateVBMasterCheckKey:         PreReplicateVBMasterCheckKey,
	metadata.ReplicateCkptIntervalKey:             ReplicateCkptIntervalKey,
	metadata.CkptSvcCacheEnabledKey:               CkptSvcCacheEnabledKey,
	metadata.FilterSystemScopeKey:                 FilterSystemScopeKey,
	metadata.FilterBinaryDocsKey:                  FilterBinaryDocsKey,
	metadata.MobileCompatibleKey:                  MobileCompatibleKey,
	metadata.EnableDcpPurgeRollback:               EnableDcpPurgeRollback,
}

// Conversion to REST for user -> pauseRequested - Pretty much a NOT operation
// User wants to know if the replication is paused. Internally, we store it as Active or not active
var ReplicationPauseRequestsedValuesMap = map[interface{}]interface{}{
	// Replication is active? Then pause is not Requested
	true: false,
	// Replication is paused - Then pause is requested
	false: true,
}

// For outside world via REST, they would prefer to see descriptive names of compression settings instead of 0 or 1
var CompressionTypeRESTValuesMap = map[interface{}]interface{}{
	base.CompressionTypeNone:   base.CompressionTypeStrings[base.CompressionTypeNone],
	base.CompressionTypeSnappy: base.CompressionTypeStrings[base.CompressionTypeSnappy],
	base.CompressionTypeAuto:   base.CompressionTypeStrings[base.CompressionTypeAuto],
}

var MobileCompatibilityTypeToRESTValueMap = map[interface{}]interface{}{
	base.MobileCompatibilityOff:    base.MobileCompatibilityStrings[base.MobileCompatibilityOff],
	base.MobileCompatibilityActive: base.MobileCompatibilityStrings[base.MobileCompatibilityActive],
}

/**
 * In cases where internally we store values differently from what the REST API requires, this
 * map performs the translation. The key is the Rest Key
 */
var SettingsValueToRestValueMap = map[string]map[interface{}]interface{}{
	base.CompressionTypeREST: CompressionTypeRESTValuesMap,
	PauseRequested:           ReplicationPauseRequestsedValuesMap,
	MobileCompatibleKey:      MobileCompatibilityTypeToRESTValueMap,
}

var logger_msgutil *log.CommonLogger = log.NewLogger("MsgUtils", log.DefaultLoggerContext)

func NewGetRemoteClustersResponse(remoteClusters map[string]*metadata.RemoteClusterReference) (*ap.Response, error) {
	remoteClusterArr := make([]map[string]interface{}, 0)
	for _, remoteCluster := range remoteClusters {
		remoteClusterArr = append(remoteClusterArr, remoteCluster.ToMap())
	}
	return EncodeObjectIntoResponseSensitive(remoteClusterArr)
}

func NewGetAllReplicationsResponse(replSpecs map[string]*metadata.ReplicationSpecification) (*ap.Response, error) {
	// UI requires that the specs are in sorted order to avoid flicking
	specIds := make([]string, 0)
	for specId, _ := range replSpecs {
		specIds = append(specIds, specId)
	}
	sort.Strings(specIds)

	replArr := make([]map[string]interface{}, 0)
	for _, specId := range specIds {
		replArr = append(replArr, getReplicationDocMap(replSpecs[specId]))
	}
	return EncodeObjectIntoResponseSensitive(replArr)
}

func NewGetAllReplicationInfosResponse(replInfos []base.ReplicationInfo) (*ap.Response, error) {
	return EncodeObjectIntoResponse(replInfos)
}

func getReplicationDocMap(replSpec *metadata.ReplicationSpecification) map[string]interface{} {
	replDocMap := make(map[string]interface{})
	if replSpec != nil {
		replDocMap[base.ReplicationDocId] = replSpec.Id
		replDocMap[base.ReplicationDocContinuous] = true
		replDocMap[base.ReplicationDocSource] = replSpec.SourceBucketName
		replDocMap[base.ReplicationDocTarget] = base.UrlDelimiter + base.RemoteClustersForReplicationDoc + base.UrlDelimiter + replSpec.TargetClusterUUID + base.UrlDelimiter + base.BucketsPath + base.UrlDelimiter + replSpec.TargetBucketName

		// special transformation for replication type and active flag
		replDocMap[base.ReplicationDocPauseRequestedOutput] = !replSpec.Settings.Active
		if replSpec.Settings.RepType == metadata.ReplicationTypeXmem {
			replDocMap[base.ReplicationDocType] = base.ReplicationDocTypeXmem
		} else {
			replDocMap[base.ReplicationDocType] = base.ReplicationDocTypeCapi
		}

		// copy other replication settings into replication doc
		for key, value := range replSpec.Settings.ToRESTMap(false /*defaultSettings*/) {
			if key != metadata.ReplicationTypeKey && key != metadata.ActiveKey {
				replDocMap[key] = value
			}
		}
	}
	return replDocMap
}

// this func assumes that the request.ParseForm() has already been called, which
// should be the case since justValidate always come with some other required parameters
// As a result, the error returned by this func is always a validation error
func DecodeJustValidateFromRequest(request *http.Request) (bool, error) {
	for key, valArr := range request.Form {
		switch key {
		case base.JustValidate:
			justValidate, err := getBoolFromValArr(valArr, false)
			if err != nil {
				return false, err
			} else {
				return justValidate, nil
			}
		default:
			// ignore other parameters
		}
	}
	return false, nil
}

func DecodeIncludeWarningsFromRequest(request *http.Request) (bool, error) {
	for key, valArr := range request.Form {
		switch key {
		case base.IncludeWarnings:
			includeWarnings, err := getBoolFromValArr(valArr, false)
			if err != nil {
				return false, err
			} else {
				return includeWarnings, nil
			}
		default:
			// ignore other parameters
		}
	}
	return false, nil
}

// decode parameters from create remote cluster request
func DecodeRemoteClusterRequest(request *http.Request) (justValidate bool, remoteClusterRef *metadata.RemoteClusterReference, errorsMap map[string]error, err error) {
	errorsMap = make(map[string]error)
	var err1 error
	var demandEncryption, restrictHostnameReplace bool
	var name, hostName, userName, password, secureType, encryptionType, hostnameMode string
	var certificate, clientCertificate, clientKey []byte

	if err1 = request.ParseForm(); err1 != nil {
		errorsMap[base.PlaceHolderFieldKey] = ErrorParsingForm
		return
	}

	for key, valArr := range request.Form {
		switch key {
		case base.JustValidate:
			justValidate, err1 = getBoolFromValArr(valArr, false)
			if err1 != nil {
				errorsMap[base.JustValidate] = err1
			}
		case base.RemoteClusterName:
			name = getStringFromValArr(valArr)
			base.ValidateRemoteClusterName(name, errorsMap)
		case base.RemoteClusterHostName:
			hostName = getStringFromValArr(valArr)
		case base.RemoteClusterUserName:
			userName = getStringFromValArr(valArr)
		case base.RemoteClusterPassword:
			password = getStringFromValArr(valArr)
		case base.RemoteClusterSecureType:
			secureType = getStringFromValArr(valArr)
		case base.RemoteClusterDemandEncryption:
			demandEncryption = getDemandEncryptionFromValArr(valArr)
		case base.RemoteClusterEncryptionType:
			encryptionType = getStringFromValArr(valArr)
		case base.RemoteClusterCertificate:
			certificateStr := getStringFromValArr(valArr)
			certificate = []byte(certificateStr)
		case base.RemoteClusterClientCertificate:
			clientCertificateStr := getStringFromValArr(valArr)
			clientCertificate = []byte(clientCertificateStr)
		case base.RemoteClusterClientKey:
			clientKeyStr := getStringFromValArr(valArr)
			clientKey = []byte(clientKeyStr)
		case base.RemoteClusterHostnameMode:
			hostnameMode = getStringFromValArr(valArr)
		case base.RestrictHostnameReplace:
			restrictHostnameReplace = getRestrictHostnameReplaceFromValArr(valArr)
		default:
			// ignore other parameters
		}
	}

	if len(secureType) > 0 {
		// if secureType is specified, use it
		// if demandEncryption and encryptionType are also specified, they will get overwritten
		demandEncryption, encryptionType, err1 = convertSecureTypeToEncryptionType(secureType)
		if err1 != nil {
			errorsMap[base.RemoteClusterSecureType] = err1
			// cannot proceed if secureType is invalid
			return
		}
	} else {
		// if secureType is not specified, use demandEncryption and encryptionType
		// if demandEncryption and encryptionType are not specified either,
		// demandEncryption will be defaulted to false and that is all we need to derive other settings

		if demandEncryption && len(encryptionType) == 0 {
			encryptionType = metadata.EncryptionType_Full
		}

		secureType, err1 = convertEncryptionTypeToSecureType(demandEncryption, encryptionType)
		if err1 != nil {
			errorsMap[base.RemoteClusterEncryptionType] = err1
			// cannot proceed if encryptionType is invalid
			return
		}
	}

	validateRemoteClusterParameters(name, hostName, secureType, userName, password, hostnameMode, certificate, clientCertificate, clientKey, errorsMap)

	hostAddr, err1 := base.ValidateHostAddr(hostName)
	if err1 != nil {
		errorsMap[base.RemoteClusterHostName] = err1
	}

	if len(errorsMap) > 0 {
		return
	}

	remoteClusterRef, err = metadata.NewRemoteClusterReference("", name, hostAddr, userName, password, hostnameMode, demandEncryption, encryptionType, certificate, clientCertificate, clientKey, &base2.DnsSrvHelper{})
	if err != nil {
		return
	}

	if remoteClusterRef.IsCapellaHostname() && !remoteClusterRef.IsFullEncryption() {
		// Don't even try non-TLS as Capella won't allow any non-TLS ports opened to the outside world
		// Even with VPC peering, Capella control-plane will always create fully secure replication
		errorsMap[base.RemoteClusterHostName] = metadata.ErrorCapellaNeedsTLS
	}

	remoteClusterRef.SetRestrictHostnameReplace(restrictHostnameReplace)

	return
}

func validateRemoteClusterParameters(name, hostName, secureType, userName, password, hostnameMode string, certificate, clientCertificate, clientKey []byte, errorsMap map[string]error) {
	// check required parameters
	if len(name) == 0 {
		errorsMap[base.RemoteClusterName] = base.MissingParameterError("cluster name")
	}
	if len(hostName) == 0 {
		errorsMap[base.RemoteClusterHostName] = base.MissingParameterError("hostname (ip)")
	}
	if secureType != base.SecureTypeFull && len(userName) == 0 {
		errorsMap[base.RemoteClusterUserName] = errors.New("username must be given when secure type is not full")
	}
	if secureType != base.SecureTypeFull && len(password) == 0 {
		errorsMap[base.RemoteClusterPassword] = errors.New("password must be given when secure type is not full")
	}
	if secureType == base.SecureTypeFull && len(certificate) == 0 && !metadata.IsCapellaHostname(hostName) {
		errorsMap[base.RemoteClusterCertificate] = errors.New("certificate must be given when secure type is full")
	}

	if secureType == base.SecureTypeNone && len(certificate) > 0 {
		errorsMap[base.RemoteClusterCertificate] = errors.New("certificate cannot be given when secure type is none")
	}

	if secureType != base.SecureTypeFull && len(clientCertificate) > 0 {
		errorsMap[base.RemoteClusterClientCertificate] = errors.New("client certificate cannot be given when secure type is not full")
	}

	if secureType != base.SecureTypeFull && len(clientKey) > 0 {
		errorsMap[base.RemoteClusterClientKey] = errors.New("client key cannot be given when secure type is not full")
	}

	if hostnameMode != metadata.HostnameMode_None && hostnameMode != metadata.HostnameMode_External && hostnameMode != metadata.HostnameMode_Internal {
		errorsMap[base.RemoteClusterHostnameMode] = fmt.Errorf("%v specified is invalid", base.RemoteClusterHostnameMode)
	}

	// full secure type is special in that it is the only mode where
	// 1. client certificate / client key can be given
	// 2. username / password can be omited
	// either clientCert or username needs to be given, but not both
	if secureType == base.SecureTypeFull {
		if len(userName) == 0 && len(clientCertificate) == 0 {
			errorsMap[base.PlaceHolderFieldKey] = errors.New("either username or client certificate must be given when secure type is full")
		} else if len(userName) > 0 && len(clientCertificate) > 0 {
			errorsMap[base.PlaceHolderFieldKey] = errors.New("username and client certificate cannot both be given when secure type is full")
		} else {
			if len(userName) > 0 {
				// userName is given
				if len(password) == 0 {
					errorsMap[base.RemoteClusterPassword] = errors.New("password must be given when username is specified")
				}
				if len(clientKey) > 0 {
					errorsMap[base.RemoteClusterClientKey] = errors.New("client key cannot be given when client certificate is not specified")
				}
			} else {
				// client certificate is given
				if len(clientKey) == 0 {
					errorsMap[base.RemoteClusterClientKey] = errors.New("client key must be given when client certificate is specified")
				}
				if len(password) > 0 {
					errorsMap[base.RemoteClusterPassword] = errors.New("password cannot be given when username is not specified")
				}
			}
		}
	}
}

// convert secureType parameter to demandEncryption and encrytionType
func convertSecureTypeToEncryptionType(secureType string) (demandEncryption bool, encryptionType string, err error) {
	switch secureType {
	case base.SecureTypeNone:
		demandEncryption = false
	case base.SecureTypeHalf:
		demandEncryption = true
		encryptionType = metadata.EncryptionType_Half
	case base.SecureTypeFull:
		demandEncryption = true
		encryptionType = metadata.EncryptionType_Full
	default:
		err = errors.New("Invalid value")
	}
	return
}

// convert demandEncryption and encrytionType to secureType
func convertEncryptionTypeToSecureType(demandEncryption bool, encryptionType string) (secureType string, err error) {
	if len(encryptionType) > 0 {
		if encryptionType != metadata.EncryptionType_Full && encryptionType != metadata.EncryptionType_Half {
			return "", errors.New("invalid value")
		} else if !demandEncryption {
			return "", errors.New("encryptionType cannot be given if demand encryption is not on")
		}
	}

	if !demandEncryption {
		return base.SecureTypeNone, nil
	}

	if encryptionType == metadata.EncryptionType_Full {
		return base.SecureTypeFull, nil
	}

	return base.SecureTypeHalf, nil
}

func NewCreateRemoteClusterResponse(remoteClusterRef *metadata.RemoteClusterReference) (*ap.Response, error) {
	return EncodeObjectIntoResponseSensitive(remoteClusterRef.ToMap())
}

func NewOKResponse() (*ap.Response, error) {
	// return "ok" in success case
	return EncodeByteArrayIntoResponse([]byte("\"ok\""))
}

func NewEmptyArrayResponse() (*ap.Response, error) {
	// return empty array in success case
	return EncodeObjectIntoResponse(make([]map[string]interface{}, 0))
}

// decode parameters from create replication request
func DecodeCreateReplicationRequest(request *http.Request) (justValidate bool, fromBucket, toCluster, toBucket string, settings metadata.ReplicationSettingsMap, errorsMap map[string]error, err error) {
	errorsMap = make(map[string]error)
	var replicationType string

	if err = request.ParseForm(); err != nil {
		errorsMap[base.PlaceHolderFieldKey] = ErrorParsingForm
		err = nil
		return
	}

	// default isCapi to false if replication type is not explicitly specified in request
	isCapi := false

	for key, valArr := range request.Form {
		switch key {
		case ReplicationType:
			replicationType = getStringFromValArr(valArr)
			if replicationType != ReplicationTypeValue {
				errorsMap[ReplicationType] = base.GenericInvalidValueError(ReplicationType)
			}
		case base.FromBucket:
			fromBucket = getStringFromValArr(valArr)
		case base.ToCluster:
			toCluster = getStringFromValArr(valArr)
		case base.ToBucket:
			toBucket = getStringFromValArr(valArr)
		case base.JustValidate:
			justValidate, err = getBoolFromValArr(valArr, false)
			if err != nil {
				errorsMap[base.JustValidate] = err
			}
		case base.Type:
			replType := getStringFromValArr(valArr)
			isCapi = (replType == metadata.ReplicationTypeCapi)
		default:
			// ignore other parameters
		}
	}

	if len(replicationType) == 0 {
		errorsMap[ReplicationType] = base.MissingValueError("replication type")
	}

	if len(fromBucket) == 0 {
		errorsMap[base.FromBucket] = base.MissingValueError("source bucket")
	}
	if len(toCluster) == 0 {
		errorsMap[base.ToCluster] = base.MissingValueError("target cluster")
	}
	if len(toBucket) == 0 {
		errorsMap[base.ToBucket] = base.MissingValueError("target bucket")
	}

	settings, settingsErrorsMap := DecodeSettingsFromRequest(request, false, false, isCapi)
	for key, value := range settingsErrorsMap {
		errorsMap[key] = value
	}

	err = filterExpressionVariousChecks(settings, true /*creation*/, false /*isDefaultSettings*/)
	if err != nil {
		errorsMap[FilterExpression] = err
	}

	err = validateCollectionsMappingRule(settings, base.CollectionsMgtDefault, fromBucket, toBucket, toCluster)
	if err != nil {
		errorsMap[CollectionsMappingRulesKey] = err
	}

	err = validateMergeFunctionMapping(settings)
	if err != nil {
		errorsMap[MergeFunctionMappingKey] = err
	}
	cleanupTempReplicationSettingKeys(settings)
	return
}

func filterExpressionVariousChecks(settings metadata.ReplicationSettingsMap, creation bool, isDefaultSettings bool) error {
	isEnterprise, err := XDCRCompTopologyService().IsMyClusterEnterprise()
	if err != nil {
		return err
	}

	filterExpression, ok := settings[metadata.FilterExpressionKey]
	if !ok {
		// No filter means do not check anything else
		return nil
	} else if len(filterExpression.(string)) > 0 {
		if !isEnterprise {
			return base.ErrorFilterEnterpriseOnly
		}
		// Anytime user edits the filter from this point forward (6.5), the filter version expected is now advanced
		if !isDefaultSettings {
			settings[metadata.FilterVersionKey] = base.FilterVersionAdvanced
		}
	} else if len(filterExpression.(string)) == 0 {
		// Explicitly setting filter to be empty string means to clear a filter
		// The Version shouldn't exist if there is no filter
		delete(settings, metadata.FilterVersionKey)
	}

	_, ok = settings[metadata.FilterSkipRestreamKey]
	if creation {
		settings[metadata.FilterSkipRestreamKey] = false
	} else if !ok && !isDefaultSettings {
		return base.ErrorFilterSkipRestreamRequired
	}

	return nil
}

func validateCollectionsMappingRule(settings metadata.ReplicationSettingsMap, currentMode base.CollectionsMgtType, srcBucket, targetBucketName, targetClusterName string) error {
	mappingRulesObj, exists := settings[metadata.CollectionsMappingRulesKey]
	if !exists {
		return nil
	}

	mappingRules, ok := mappingRulesObj.(metadata.CollectionsMappingRulesType)
	if !ok {
		return fmt.Errorf("Invalid mapping rule type: %v", reflect.TypeOf(mappingRules))
	}

	// If input does not contain any traces of mode changes, then use the currentMode to drive rule validation
	var rulesToCheck = currentMode

	if multiHelper, ok := settings[metadata.CollectionsMgtMultiKey].(metadata.ReplicationMultiValueHelper); ok {
		baselineSetting := metadata.EmptyReplicationSettings()
		baselineSetting.Values[metadata.CollectionsMgtMultiKey] = currentMode
		peekMap := multiHelper.PeekValues(baselineSetting)
		rulesToCheck = peekMap[metadata.CollectionsMgtMultiKey].(base.CollectionsMgtType)
	}

	if rulesToCheck.IsMigrationOn() {
		isEnterprise, err := XDCRCompTopologyService().IsMyClusterEnterprise()
		if err != nil {
			return err
		}
		if !isEnterprise {
			return base.ErrorColMigrationEnterpriseOnly
		}
		return mappingRules.ValidateMigrateRules()
	} else {
		err := mappingRules.ValidateExplicitMapping()
		if err != nil {
			return err
		}
		if skipSrcCheck, ok := settings[metadata.CollectionsSkipSourceCheckKey].(bool); ok && skipSrcCheck {
			return nil
		}

		err = validateSrcNamespacesExist(targetClusterName, srcBucket, targetBucketName, rulesToCheck, mappingRules)
		if err != nil {
			return err
		}
	}
	return nil
}

// Validate source mapping - which means we need to get manifests
func validateSrcNamespacesExist(targetClusterName string, srcBucket string, targetBucketName string, rulesToCheck base.CollectionsMgtType, mappingRules metadata.CollectionsMappingRulesType) error {
	if targetBucketName == "" || srcBucket == "" || targetClusterName == "" {
		return fmt.Errorf("One of the following is empty: tgtBucketName: %v srcBucket %v targetClusterName %v",
			targetBucketName, srcBucket, targetClusterName)
	}

	var err error
	ref, err := RemoteClusterService().RemoteClusterByRefName(targetClusterName, false)
	if err != nil {
		return fmt.Errorf("unable to retrieve remote cluster reference by name %v to validate source collection mapping", err)
	}
	getterSpec, err := metadata.NewReplicationSpecification(srcBucket, "", ref.Uuid(), targetBucketName, "")
	if err != nil {
		return fmt.Errorf("unable to generate spec for validating source mapping: %v", err)
	}
	srcManifest, tgtManifest, err := CollectionsManifestService().GetLatestManifests(getterSpec, true)
	if err != nil {
		return err
	}
	manifestsPair := metadata.CollectionsManifestPair{
		Source: srcManifest,
		Target: tgtManifest,
	}
	_, err = metadata.NewCollectionNamespaceMappingFromRules(manifestsPair, rulesToCheck, mappingRules, true, false)
	if err != nil {
		return err
	}
	return nil
}

func validateMergeFunctionMapping(settings metadata.ReplicationSettingsMap) error {
	mergeFunctionMapping, exists := settings[metadata.MergeFunctionMappingKey]
	if !exists {
		return nil
	}
	mergeFunctions, ok := mergeFunctionMapping.(base.MergeFunctionMappingType)
	if !ok {
		return fmt.Errorf("Invalid merge function mapping %v", mergeFunctionMapping)
	}
	if len(mergeFunctions) > 1 || mergeFunctions[base.BucketMergeFunctionKey] == "" {
		return fmt.Errorf("Only bucket level merge function can be specified.")
	}
	return nil
}

func DecodeChangeReplicationSettings(request *http.Request, replicationId string) (justValidate bool, settings metadata.ReplicationSettingsMap, errorsMap base.ErrorMap, includeWarnings bool) {
	errorsMap = make(base.ErrorMap)

	if err := request.ParseForm(); err != nil {
		errorsMap[base.PlaceHolderFieldKey] = ErrorParsingForm
		return
	}

	justValidate, err := DecodeJustValidateFromRequest(request)
	if err != nil {
		errorsMap[base.JustValidate] = err
	}

	includeWarnings, err = DecodeIncludeWarningsFromRequest(request)
	if err != nil {
		errorsMap[base.IncludeWarnings] = err
	}

	var isDefaultSettings bool
	var isCapi bool
	var currentCollectionMode = base.CollectionsMgtDefault
	var spec *metadata.ReplicationSpecification
	if len(replicationId) == 0 {
		// empty replicationId indicates that we are handling default settings that are not replication specific
		isDefaultSettings = true
		// no need for isCapi check in this case
		isCapi = false
	} else {
		isDefaultSettings = false
		spec, err = ReplicationSpecService().ReplicationSpec(replicationId)
		if err != nil {
			// if err is not nil, replicationId is likely invalid
			// simply return. validation error will be raised downstream
			logger_msgutil.Warnf("Error retrieving spec for %v. err=%v\n", replicationId, err)
			return
		}
		isCapi = spec.Settings.IsCapi()
		currentSetting, ok := spec.Settings.Values[metadata.CollectionsMgtMultiKey].(base.CollectionsMgtType)
		if ok {
			currentCollectionMode = currentSetting
		}
	}

	settings, settingsErrorsMap := DecodeSettingsFromRequest(request, isDefaultSettings, true, isCapi)
	for key, value := range settingsErrorsMap {
		errorsMap[key] = value
	}

	err = filterExpressionVariousChecks(settings, false /*creation*/, isDefaultSettings)
	if err != nil {
		errorsMap[FilterExpression] = err
	}

	var srcBucket string
	var tgtBucket string
	var tgtClusterName string
	if !isDefaultSettings {
		if spec == nil {
			panic("Should not be nil")
		}
		srcBucket = spec.SourceBucketName
		tgtBucket = spec.TargetBucketName
		ref, err := RemoteClusterService().RemoteClusterByUuid(spec.TargetClusterUUID, false)
		if err != nil {
			errorsMap[base.PlaceHolderFieldKey] = err
			return
		}
		tgtClusterName = ref.Name()

		err = validateCollectionsMappingRule(settings, currentCollectionMode, srcBucket, tgtBucket, tgtClusterName)
		if err != nil {
			errorsMap[CollectionsMappingRulesKey] = err
		}
	}
	err = validateMergeFunctionMapping(settings)
	if err != nil {
		errorsMap[MergeFunctionMappingKey] = err
	}
	return
}

// Temporary keys are used to interact with XDCR and not meant to be visible to the user
func cleanupTempReplicationSettingKeys(settings metadata.ReplicationSettingsMap) {
	for key, _ := range settings {
		if metadata.IsSettingValueTemporary(key) {
			delete(settings, key)
		}
	}
}

// decode replicationId from create replication response
func DecodeCreateReplicationResponse(response *http.Response) (string, error) {
	defer response.Body.Close()

	bodyBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return "", err
	}

	var paramsMap map[string]interface{}
	err = json.Unmarshal(bodyBytes, &paramsMap)
	if err != nil {
		return "", err
	}

	replicationId, ok := paramsMap[ReplicationId]

	if !ok {
		return "", base.MissingParameterInHttpResponseError(ReplicationId)
	}

	replicationIdStr, ok := replicationId.(string)
	if !ok {
		return "", base.IncorrectValueTypeInHttpResponseError(ReplicationId, replicationId, "string")
	}

	return replicationIdStr, nil

}

// decode replication settings related parameters from http request
func DecodeSettingsFromRequest(request *http.Request, isDefaultSettings bool, isUpdate bool, isCapi bool) (metadata.ReplicationSettingsMap, map[string]error) {
	settings := make(metadata.ReplicationSettingsMap)
	errorsMap := make(map[string]error)
	mvHelper := metadata.NewMultiValueHelper()

	if err := request.ParseForm(); err != nil {
		errorsMap[base.PlaceHolderFieldKey] = ErrorParsingForm
		return nil, errorsMap
	}

	isEnterprise, err := XDCRCompTopologyService().IsMyClusterEnterprise()
	if err != nil {
		errorsMap[base.PlaceHolderFieldKey] = err
		return nil, errorsMap
	}

	for key, valArr := range request.Form {
		key, valArr, err := mvHelper.CheckAndConvertMultiValue(key, valArr, isEnterprise)
		if err != nil {
			errorsMap[key] = err
			continue
		}
		err = processKey(key, valArr, &settings, isDefaultSettings, isUpdate, isEnterprise, isCapi)
		if err != nil {
			errorsMap[key] = err
		}
	}

	err = filterExpressionVariousChecks(settings, !isUpdate, isDefaultSettings)
	if err != nil {
		errorsMap[FilterExpression] = err
	}

	if len(errorsMap) > 0 {
		return nil, errorsMap
	}

	mvHelper.ExportToSettingsMap(settings)
	if logger_msgutil.GetLogLevel() >= log.LogLevelDebug {
		logger_msgutil.Debugf("settings decoded from request: %v\n", settings.CloneAndRedact())
	}
	return settings, nil
}

func DecodeSettingsFromXDCRInternalSettingsRequest(request *http.Request) (metadata.ReplicationSettingsMap, map[string]error) {
	settings := make(metadata.ReplicationSettingsMap)
	errorsMap := make(map[string]error)

	if err := request.ParseForm(); err != nil {
		errorsMap[base.PlaceHolderFieldKey] = ErrorParsingForm
		return nil, errorsMap
	}

	for key, valArr := range request.Form {
		convertedValue, err := metadata.ValidateAndConvertXDCRInternalSettingsValue(key, valArr[0])
		if err != nil {
			errorsMap[key] = err
		} else {
			settings[key] = convertedValue
		}
	}

	if len(errorsMap) > 0 {
		return nil, errorsMap
	}

	if logger_msgutil.GetLogLevel() >= log.LogLevelDebug {
		logger_msgutil.Debugf("settings decoded from request: %v\n", settings.CloneAndRedact())
	}
	return settings, nil
}

func DecodeRegexpValidationRequest(request *http.Request) (expression, docId, bucket string, collectionNamespace *base.CollectionNamespace, skipDocSpecified bool, err error) {
	if err = request.ParseForm(); err != nil {
		return
	}

	collectionNamespace = base.NewDefaultCollectionNamespace()
	var scopeSpecified bool
	var collectionSpecified bool

	for key, valArr := range request.Form {
		switch key {
		case Expression:
			expression = getStringFromValArr(valArr)
		case DocID:
			docId = getStringFromValArr(valArr)
		case Bucket:
			bucket = getStringFromValArr(valArr)
		case Scope:
			scopeSpecified = true
			collectionNamespace.ScopeName = getStringFromValArr(valArr)
		case Collection:
			collectionSpecified = true
			collectionNamespace.CollectionName = getStringFromValArr(valArr)
		case SkipDoc:
			skipDocSpecified, err = getBoolFromValArr(valArr, false)
		default:
			// ignore other parameters
		}
	}

	if err != nil {
		return
	}

	if len(expression) == 0 {
		err = base.MissingParameterError(Expression)
	} else if len(docId) == 0 && !skipDocSpecified {
		err = base.MissingParameterError(DocID)
	} else if len(bucket) == 0 {
		err = base.MissingParameterError(Bucket)
	} else if scopeSpecified && !collectionSpecified {
		err = base.MissingParameterError(Collection)
	} else if collectionSpecified && !scopeSpecified {
		err = base.MissingParameterError(Scope)
	}
	return
}

func NewCreateReplicationResponse(replicationId string, warnings service_def.UIWarnings, justValidate bool) (*ap.Response, error) {
	params := make(map[string]interface{})
	params[ReplicationId] = replicationId
	if warnings != nil && warnings.Len() > 0 {
		if justValidate {
			params[Warnings] = warnings.GetFieldWarningsOnly()
		} else {
			params[Warnings] = warnings.GetSuccessfulWarningStrings()
		}
	}
	return EncodeObjectIntoResponse(params)
}

func NewReplicationSettingsResponse(settings *metadata.ReplicationSettings, warnings service_def.UIWarnings, includeWarnings bool) (*ap.Response, error) {
	if settings == nil {
		if warnings == nil || warnings.Len() == 0 {
			return NewEmptyArrayResponse()
		} else {
			params := make(map[string]interface{})
			params[Warnings] = warnings.GetFieldWarningsOnly()
			return EncodeObjectIntoResponse(params)
		}
	} else {
		restMap := convertSettingsToRestSettingsMap(settings, false)
		if includeWarnings && warnings != nil && warnings.Len() > 0 {
			params := make(map[string]interface{})
			params[Result] = restMap
			params[Warnings] = warnings.GetSuccessfulWarningStrings()
			return EncodeObjectIntoResponseSensitive(params)
		} else {
			return EncodeObjectIntoResponseSensitive(restMap)
		}
	}
}

func NewDefaultReplicationSettingsResponse(settings *metadata.ReplicationSettings, globalSettings *metadata.GlobalSettings) (*ap.Response, error) {
	if settings == nil || globalSettings == nil {
		return NewEmptyArrayResponse()
	} else {
		globalSettingsMap := convertGlobalSettingsToRestSettingsMap(globalSettings)
		replicationSettingMap := convertSettingsToRestSettingsMap(settings, true)
		for key, value := range globalSettingsMap {
			replicationSettingMap[key] = value
		}

		return EncodeObjectIntoResponseSensitive(replicationSettingMap)
	}
}

func NewXDCRInternalSettingsResponse(settings *metadata.InternalSettings) (*ap.Response, error) {
	if settings == nil {
		return NewEmptyArrayResponse()
	} else {
		return EncodeObjectIntoResponse(settings.ToMap())
	}
}

func NewRegexpValidationResponse(result bool, err error) (*ap.Response, error) {
	returnMap := make(map[string]interface{})

	returnMap[MatchResult] = result
	if err != nil {
		returnMap[MatchError] = err.Error()
	}

	return EncodeObjectIntoResponseSensitive(returnMap)
}

// decode dynamic paramater from the path of http request
func DecodeDynamicParamInURL(request *http.Request, pathPrefix string, paramName string) (string, error) {
	// length of prefix preceding replicationId in request url path
	prefixLength := len(base.AdminportUrlPrefix) + len(pathPrefix) + len(base.UrlDelimiter)

	if len(request.URL.Path) <= prefixLength {
		return "", base.MissingParameterInHttpRequestUrlError(paramName, request.URL.Path)
	}

	paramValue := request.URL.Path[prefixLength:]

	logger_msgutil.Debugf("param value decoded from request: %v\n", paramValue)
	return paramValue, nil
}

func convertGlobalSettingsToRestSettingsMap(settings *metadata.GlobalSettings) map[string]interface{} {
	restSettingsMap := make(map[string]interface{})
	var settingsMap map[string]interface{}

	settingsMap = settings.ToMap()
	for key, value := range settingsMap {
		restKey := SettingsKeyToRestKeyMap[key]
		restSettingsMap[restKey] = value
	}
	return restSettingsMap
}

func convertSettingsToRestSettingsMap(settings *metadata.ReplicationSettings, isDefaultSettings bool) map[string]interface{} {
	restSettingsMap := make(map[string]interface{})
	settingsMap := settings.ToRESTMap(isDefaultSettings)
	for key, value := range settingsMap {
		restKey := SettingsKeyToRestKeyMap[key]
		convertedValue := convertSettingsInternalValuesToRESTValues(restKey, value)
		restSettingsMap[restKey] = convertedValue
	}
	return restSettingsMap
}

func convertSettingsInternalValuesToRESTValues(restKey string, origValue interface{}) interface{} {
	if valuesMap, ok := SettingsValueToRestValueMap[restKey]; ok {
		if newValue, valueOk := valuesMap[origValue]; valueOk {
			return newValue
		}
	}
	return origValue
}

func getStringFromValArr(valArr []string) string {
	if len(valArr) == 0 {
		return ""
	} else {
		return valArr[0]
	}
}

func getBoolFromValArr(valArr []string, defaultValue bool) (bool, error) {
	boolStr := getStringFromValArr(valArr)
	if boolStr != "" {
		result, err := strconv.ParseBool(boolStr)
		if err != nil {
			return defaultValue, base.IncorrectValueTypeError("a boolean")
		}
		return result, nil
	}

	return defaultValue, nil
}

func EncodeRemoteClusterErrorsMapIntoResponse(errorsMap map[string]error) (*ap.Response, error) {
	return EncodeErrorsMapIntoResponse(errorsMap, false)
}

// encode a map of validation errors into a Response object, which can then be returned in a http response
// when withErrorsWrapper is false, simply return a map of error messages -- this is needed by remote cluster rest APIs
// when withErrorsWrapper is true, wrap the map of error messages in an outer map -- this is needed by replication rest APIs
func EncodeErrorsMapIntoResponse(errorsMap map[string]error, withErrorsWrapper bool) (*ap.Response, error) {
	errorMsgMap := make(map[string]string)
	for key, err := range errorsMap {
		errorMsgMap[key] = err.Error()
	}

	if !withErrorsWrapper {
		// validation errors cause StatusBadRequest to be returned to client
		return EncodeObjectIntoResponseWithStatusCode(errorMsgMap, http.StatusBadRequest)
	}

	result := make(map[string]interface{})
	result[ErrorsKey] = errorMsgMap

	// validation errors cause StatusBadRequest to be returned to client
	return EncodeObjectIntoResponseWithStatusCode(result, http.StatusBadRequest)
}

func EncodeReplicationValidationErrorIntoResponse(err error) (*ap.Response, error) {
	return EncodeValidationErrorIntoResponse(err, true)
}

func EncodeRemoteClusterValidationErrorIntoResponse(err error) (*ap.Response, error) {
	return EncodeValidationErrorIntoResponse(err, false)
}

// encode a validation error into Response object by wrapping it in an errorMap
func EncodeValidationErrorIntoResponse(err error, withErrorsWrapper bool) (*ap.Response, error) {
	errorsMap := make(map[string]error)
	errorsMap[base.PlaceHolderFieldKey] = err
	return EncodeErrorsMapIntoResponse(errorsMap, withErrorsWrapper)
}

// encode the error message of an error, without any wrapping, into Response object.
func EncodeErrorMessageIntoResponse(err error, statusCode int) (*ap.Response, error) {
	if err != nil {
		errMap := make(map[string]interface{})
		errMap[base.PlaceHolderFieldKey] = err.Error()
		return EncodeObjectIntoResponseWithStatusCode(errMap, statusCode)
	} else {
		return NewEmptyArrayResponse()
	}
}

// encode a byte array into Response object with default status code of StatusOK
func EncodeByteArrayIntoResponse(data []byte) (*ap.Response, error) {
	return EncodeByteArrayIntoResponseWithStatusCode(data, http.StatusOK)
}

// encode a byte array into Response object with specified status code
func EncodeByteArrayIntoResponseWithStatusCode(data []byte, statusCode int) (*ap.Response, error) {
	return &ap.Response{StatusCode: statusCode, Body: data}, nil
}

func EncodeByteArrayIntoPrometheusResponseWithStatusCode(data []byte, statusCode int) (*ap.Response, error) {
	return &ap.Response{StatusCode: statusCode, ContentType: ap.ContentTypePrometheusText, Body: data}, nil
}

// encode an arbitrary object into Response object with default status code of StatusOK
func EncodeObjectIntoResponse(object interface{}) (*ap.Response, error) {
	return EncodeObjectIntoResponseWithStatusCode(object, http.StatusOK)
}

func EncodeObjectIntoResponseSensitive(object interface{}) (*ap.Response, error) {
	return EncodeObjectIntoResponseWithStatusCodeSensitive(object, http.StatusOK)
}

// encode an arbitrary object into Response object with specified status code
func EncodeObjectIntoResponseWithStatusCode(object interface{}, statusCode int) (*ap.Response, error) {
	var body []byte
	if object == nil {
		body = []byte{}
	} else {
		var err error
		buf := new(bytes.Buffer)
		encoder := json.NewEncoder(buf)
		// Need to set this to false to keep the redaction tag symbols "<ud>" from being transformed
		encoder.SetEscapeHTML(false)
		err = encoder.Encode(object)
		if err != nil {
			return nil, err
		}
		body = buf.Bytes()
	}
	return EncodeByteArrayIntoResponseWithStatusCode(body, statusCode)
}

func EncodeObjectIntoResponseWithStatusCodeSensitive(object interface{}, statusCode int) (*ap.Response, error) {
	res, err := EncodeObjectIntoResponseWithStatusCode(object, statusCode)
	if err == nil && res != nil {
		res.TagPrintingBody = true
	}
	return res, err
}

func EncodeAuthorizationErrorMessageIntoResponse(permission string) (*ap.Response, error) {
	msgMap := make(map[string]interface{})
	// use the same error message as that produced by ns_server
	msgMap["message"] = "Forbidden. User needs one of the following permissions"
	msgMap["permissions"] = []string{permission}
	return EncodeObjectIntoResponseWithStatusCode(msgMap, http.StatusForbidden)
}

func EncodeAuthorizationErrorMessageIntoResponse2(permissions []string) (*ap.Response, error) {
	msgMap := make(map[string]interface{})
	msgMap["message"] = "Forbidden. User needs all of the following permissions"
	msgMap["permissions"] = permissions
	return EncodeObjectIntoResponseWithStatusCode(msgMap, http.StatusForbidden)
}

// Remote cluster related errors can be internal server error or less servere invalid/unknown remote cluster errors,
// return different Response for them
func EncodeRemoteClusterErrorIntoResponse(err error) (*ap.Response, error) {
	if err != nil {
		isValidationError, unwrapperError := RemoteClusterService().CheckAndUnwrapRemoteClusterError(err)
		if isValidationError {
			return EncodeRemoteClusterValidationErrorIntoResponse(unwrapperError)
		} else {
			return nil, err
		}
	} else {
		return NewEmptyArrayResponse()
	}
}

// Replication spec related errors can be internal server error or less servere replication spec not found/already exists errors,
// return different Response for them
func EncodeReplicationSpecErrorIntoResponse(err error) (*ap.Response, error) {
	if err != nil {
		if ReplicationSpecService().IsReplicationValidationError(err) {
			return EncodeReplicationValidationErrorIntoResponse(err)
		} else {
			return nil, err
		}
	} else {
		return NewEmptyArrayResponse()
	}

}

func processKey(restKey string, valArr []string, settingsPtr *metadata.ReplicationSettingsMap, isDefaultSettings bool, isUpdate bool, isEnterprise bool, isCapi bool) error {
	settingsKey, ok := RestKeyToSettingsKeyMap[restKey]
	if !ok {
		// ignore non-settings key
		return nil
	}

	if isDefaultSettings && !metadata.IsSettingDefaultValueMutable(settingsKey) {
		// ignore settings whose default values cannot be changed
		return nil
	}

	if isUpdate && !metadata.IsSettingValueMutable(settingsKey) {
		return errors.New("Setting value cannot be modified after replication is created.")
	}

	convertedValue, err := validateAndConvertAllSettingValue(settingsKey, valArr[0], restKey, isEnterprise, isCapi)
	if err == nil {
		(*settingsPtr)[settingsKey] = convertedValue
	}
	return err
}

func validateAndConvertAllSettingValue(key, value, restKey string, isEnterprise bool, isCapi bool) (convertedValue interface{}, err error) {
	//check if key is a replication setting
	convertedValue, err = metadata.ValidateAndConvertReplicationSettingsValue(key, value, restKey, isEnterprise, isCapi)

	//if key is not a valid replication settings,  check if key is a global setting
	if err == base.ErrorInvalidSettingsKey {
		convertedValue, err = metadata.ValidateAndConvertGlobalSettingsValue(key, value)
	}
	return
}

// check if encryption is enabled from the valArr of demandEncryption parameter in http request
func getDemandEncryptionFromValArr(valArr []string) bool {
	demandEncryptionStr := getStringFromValArr(valArr)
	demandEncryptionInt, err := strconv.ParseInt(demandEncryptionStr, base.ParseIntBase, base.ParseIntBitSize)
	if err == nil && demandEncryptionInt == 0 {
		// int value of 0 indicates that encryption is not enabled
		return false
	} else {
		// any other value, e.g., "", 1, "on", "true", "false", etc., indicates that encryption is enabled
		return true
	}
}

// check if RestrictHostnameReplace is enabled from the valArr in http request
func getRestrictHostnameReplaceFromValArr(valArr []string) bool {
	restrictHostnameReplaceStr := getStringFromValArr(valArr)
	restrictHostnameReplaceInt, err := strconv.ParseInt(restrictHostnameReplaceStr, base.ParseIntBase, base.ParseIntBitSize)
	if err == nil && restrictHostnameReplaceInt == 0 {
		// int value of 0 indicates that RestrictHostnameReplace is not enabled
		return false
	} else {
		// any other value, e.g., "", 1, "on", "true", "false", etc., indicates that RestrictHostnameReplace is enabled
		return true
	}
}

/* Connection Pre-Check feature */

// Generate taskId for the connection pre-check feature
var taskIdCounter uint64 = 0

const maxTaskIdCounter uint64 = ^uint64(0)

func generateTaskId(host string) string {
	atomic.AddUint64(&taskIdCounter, 1)
	if taskIdCounter == maxTaskIdCounter {
		taskIdCounter = 0
	}
	taskId := fmt.Sprintf("task/%s/%v", host, taskIdCounter)
	return taskId
}

func DecodeGetConnectionPreCheckResultRequest(request *http.Request) (taskId string, err error) {
	if request == nil {
		err = errors.New("Nil request found")
		return
	}
	taskId = ""
	query := request.URL.Query()
	if query == nil || len(query) == 0 {
		err = errors.New("1 query parameter, taskId expected, got 0")
		return
	}

	for key, valArr := range query {
		if key == TASKID && len(valArr) == 1 {
			taskId = getStringFromValArr(valArr)
		} else {
			err = errors.New("No taskID provided to get the pre-check results")
		}
	}
	return
}

// similiar to DecodeRemoteClusterRequest but without justValidate
func DecodePostConnectionPreCheckRequest(request *http.Request) (remoteClusterRef *metadata.RemoteClusterReference, errorsMap map[string]error, err error) {
	_, ref, errMap, err := DecodeRemoteClusterRequest(request)
	return ref, errMap, err
}

func NewConnectionPreCheckPostResponse(hostname string, username string, taskId string) (*ap.Response, error) {
	resMap := make(map[string]interface{})
	resMap[TASKID] = taskId
	resMap[HOSTNAME] = hostname
	resMap[USERNAME] = username
	res, err := EncodeObjectIntoResponseSensitive(resMap)
	return res, err
}

func NewConnectionPreCheckGetResponse(taskId string, res base.ConnectionErrMapType, done bool) (*ap.Response, error) {
	resMap := make(map[string]interface{})
	resMap[TASKID] = taskId
	resMap[RESULT] = res
	resMap[DONE] = done
	response, err := EncodeObjectIntoResponseSensitive(resMap)
	return response, err
}
