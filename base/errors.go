/*
Copyright 2025-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"errors"
	"fmt"
	"strings"
)

// Various error messages
var (
	ErrorNotResponding                           = errors.New("Not responding")
	ErrorNotOK                                   = errors.New("Not OK")
	ErrorNotMyVbucket                            = errors.New("NOT_MY_VBUCKET")
	InvalidStateTransitionErrMsg                 = "Can't move to state %v - %v's current state is %v, can only move to state [%v]"
	InvalidCerfiticateError                      = errors.New("Failed to parse given certificates. Certificates must be one or more, PEM-encoded x509 certificate and nothing more.")
	ErrorNoSourceNozzle                          = errors.New("Invalid configuration. No source nozzle can be constructed since the source kv nodes are not the master for any vbuckets.")
	ErrorNoTargetNozzle                          = errors.New("Invalid configuration. No target nozzle can be constructed.")
	ErrorMasterNegativeIndex                     = errors.New("Master index is negative. ")
	ErrorFailedAfterRetry                        = errors.New("Operation failed after max retries. ")
	ErrorDoesNotExistString                      = "does not exist"
	ErrorResourceDoesNotExist                    = fmt.Errorf("Specified resource %v.", ErrorDoesNotExistString)
	ErrorResourceDoesNotMatch                    = errors.New("Specified resource does not match the item to which is being compared.")
	ErrorInvalidType                             = errors.New("Specified type is invalid")
	ErrorInvalidInput                            = errors.New("Invalid input given")
	ErrorNoPortNumber                            = errors.New("No port number")
	ErrorInvalidPortNumber                       = errors.New("Port number is not a valid integer")
	ErrorUnauthorized                            = errors.New("unauthorized")
	ErrorForbidden                               = errors.New("forbidden")
	ErrorCompressionNotSupported                 = errors.New("Specified compression type is not supported.")
	ErrorCompressionUnableToConvert              = errors.New("Unable to translate user input to internal compression Type")
	ErrorCompressionDcpInvalidHandshake          = errors.New("DCP connection is established as compressed even though compression is not requested.")
	ErrorCompressionUnableToInflate              = errors.New("Unable to properly uncompress data from DCP")
	ErrorMaxReached                              = errors.New("Maximum entries has been reached")
	ErrorNilPtr                                  = errors.New("Nil pointer given")
	ErrorNilPipeline                             = errors.New("Nil pipeline")
	ErrorNoHostName                              = errors.New("hostname is missing")
	ErrorInvalidSettingsKey                      = errors.New("Invalid settings key")
	ErrorSizeExceeded                            = errors.New("Size is larger than maximum allowed")
	ErrorLengthExceeded                          = errors.New("Length is longer than maximum allowed")
	ErrorNoMatcher                               = errors.New("Internal error - unable to establish GoJsonsm Matcher")
	ErrorNoDataPool                              = errors.New("Internal error - unable to establish GoXDCR datapool")
	ErrorFilterEnterpriseOnly                    = errors.New("Filter expression can be specified in Enterprise edition only")
	ErrorFilterInvalidVersion                    = errors.New("Filter version specified is deprecated")
	ErrorFilterInvalidFormat                     = errors.New("Filter specified using key-only regex is deprecated")
	ErrorFilterInvalidExpression                 = errors.New("Filter expression is invalid")
	ErrorFilterParsingError                      = errors.New("Filter unable to parse DCP packet")
	ErrorFilterSkipRestreamRequired              = errors.New("Filter skip restream flag is required along with a filter")
	ErrorNotSupported                            = errors.New("Not supported")
	ErrorInvalidJSONMap                          = errors.New("Retrieved value is not a valid JSON key-value map")
	ErrorInvalidCAS                              = errors.New("Invalid CAS")
	ErrorNoSourceKV                              = errors.New("Invalid configuration. No source kv node is found.")
	ErrorExecutionTimedOut                       = errors.New("Execution timed out")
	ErrorPipelineStartTimedOutUI                 = errors.New("Pipeline did not start in a timely manner, possibly due to busy source or target. Will try again...")
	ErrorRemoteClusterUninit                     = errors.New("Remote cluster has not been successfully contacted to figure out user intent for alternate address yet. Will try again next refresh cycle")
	ErrorTargetNoAltHostName                     = errors.New("Alternate hostname is not set up on at least one node of the remote cluster")
	ErrorPipelineRestartDueToClusterConfigChange = errors.New("Pipeline needs to update due to remote cluster configuration change")
	ErrorPipelineRestartDueToEncryptionChange    = errors.New("Pipeline needs to update due to cluster encryption level change")
	ErrorRemoteClusterFullEncryptionRequired     = errors.New("Current cluster encryption level requires the remote-cluster-reference to be of 'Full' encryption-type.")
	ErrorNotFound                                = errors.New("Specified entity is not found")
	ErrorTargetCollectionsNotSupported           = errors.New("Target cluster does not support collections")
	ErrorSourceCollectionsNotSupported           = errors.New("Source cluster collections critical error")
	ErrorInvalidOperation                        = errors.New("Invalid operation")
	ErrorRouterRequestRetry                      = errors.New("Request is in retry queue")
	ErrorIgnoreRequest                           = errors.New("Request should be ignored")
	ErrorXmemCollectionSubErr                    = errors.New(StringTargetCollectionMappingErr)
	ErrorRequestAlreadyIgnored                   = errors.New("Request has been marked ignored")
	ErrorInvalidSRVFormat                        = errors.New("hostname format is not SRV")
	ErrorSdkUriNotSupported                      = fmt.Errorf("XDCR currently does not support %v or %v URI. If using DNS SRV, remove the URI prefix", CouchbaseUri, CouchbaseSecureUri)
	ErrorColMigrationEnterpriseOnly              = errors.New("Collections migration is supported in Enterprise edition only")
	ErrorInvalidColNamespaceFormat               = fmt.Errorf("Invalid CollectionNamespace format")
	ErrorCAPIDeprecated                          = errors.New("CAPI replication mode is now deprecated")
	ReplicationSpecNotFoundErrorMessage          = "requested resource not found"
	ReplNotFoundErr                              = errors.New(ReplicationSpecNotFoundErrorMessage)
	ErrorExplicitMappingEnterpriseOnly           = errors.New("Explicit Mapping is supported in Enterprise Edition only")
	ErrorChunkedEncodingNotSupported             = errors.New("Chunked encoding is not supported")
	BrokenMappingUIString                        = "Found following destination collection(s) missing (and will not get replicated to):\n"
	ErrorSourceBucketTopologyNotReady            = errors.New("Local bucket topology does not have any cached data yet")
	ErrorTargetBucketTopologyNotReady            = errors.New("Target bucket topology does not have any cached data yet")
	ErrorNoBackfillNeeded                        = errors.New("No backfill needed")
	ErrorNilCertificate                          = errors.New("Nil certificate")
	ErrorNilCertificateStrictMode                = errors.New("cluster encryption is set to strict mode and unable to retrieve a valid certificate")
	ErrorOpInterrupted                           = errors.New("Operation interrupted")
	ErrorNoVbSpecified                           = errors.New("No vb being specified")
	ErrorCollectionManifestNotChanged            = errors.New("Collection manifest has not changed")
	ErrorSystemScopeMapped                       = errors.New("System scope is mapped")
	ErrorAdvFilterMixedModeUnsupported           = errors.New("Not all nodes support advanced filtering so adv filtering editing is not allowed")
	ErrorJSONReEncodeFailed                      = errors.New("JSON string passed in did not pass re-encode test. Potentially duplicated keys or characters except: A-Z a-z 0-9 _ - %")
	ErrorDocumentNotFound                        = errors.New("Document not found")
	ErrorSubdocLookupPathNotFound                = errors.New("SUBDOC_MULTI_LOOKUP does not include the path")
	ErrorUnexpectedSubdocOp                      = errors.New("Unexpected subdoc op was observed")
	ErrorCasPoisoningDetected                    = errors.New("Document CAS is stamped with a time beyond allowable drift threshold")
	ErrorHostNameEmpty                           = errors.New("Hostname is empty")
	ErrorReplicationSpecNotActive                = errors.New("replication specification not found or no longer active")
	ErrorCLoggingMixedModeUnsupported            = errors.New("not all nodes support conflict logging feature")
	ErrorSubdocMaxPathLimitBreached              = fmt.Errorf("subdoc max path limit breached")
	ErrorSeamlessCredsChangeMixedModeUnsupported = errors.New("Seamless credentials change is not supported in mixed-mode clusters. Upgrade all nodes to version >=8.1 to enable credential staging.")
)

// Various non-error internal msgs
var FilterForcePassThrough = errors.New("No data is to be filtered, should allow passthrough")

func GetBackfillFatalDataLossError(specId string) error {
	return fmt.Errorf("%v experienced fatal error when trying to create backfill request. To prevent data loss, the pipeline must restream from the beginning", specId)
}

func BypassUIErrorCodes(errStr string) bool {
	if strings.Contains(errStr, ErrorNoSourceNozzle.Error()) {
		return true
	} else if strings.Contains(errStr, ErrorMasterNegativeIndex.Error()) {
		return true
	}
	return false
}
