// Copyright (c) 2013-2018 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the refific language governing permissions
// and limitations under the License.

// metadata service implementation leveraging gometa
package metadata_svc

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	utilities "github.com/couchbase/goxdcr/utils"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// the key to the metadata that stores the keys of all remote clusters
	RemoteClustersCatalogKey = metadata.RemoteClusterKeyPrefix
)

var InvalidRemoteClusterOperationErrorMessage = "Invalid remote cluster operation. "
var InvalidRemoteClusterErrorMessage = "Invalid remote cluster. "
var UnknownRemoteClusterErrorMessage = "unknown remote cluster"
var BootStrapNodeHasMovedError = errors.New("Bootstrap node in reference has been moved")
var UUIDMismatchError = errors.New("UUID does not match")
var RemoteSyncInProgress = errors.New("A RPC request is currently underway")
var InitInProgress = errors.New("Initialization is already in progress")
var SetInProgress = errors.New("An user-driven setRemoteClusterReference event is already in progress")
var HostNameEmpty = errors.New("Hostname is empty")
var RefreshNotEnabledYet = errors.New("The initial reference update hasn't finished yet, refresh is not enabled and pipelines cannot be started yet")
var SetDisabledUntilInit = errors.New("The reference has not finished initializing yet. SetRemoteCluster is disabled")
var RefreshAlreadyActive = errors.New("There is a refresh that is ongoing")
var RefreshAborted = errors.New("Refresh instance was called to be aborted")
var DeleteAlreadyIssued = errors.New("Underlying remote cluster reference has been already deleted manually")
var WriteToMetakvErrString = "Error writing to metakv"
var NoSuchHostRecommendationString = " Check to see if firewall config is incorrect, or if Couchbase Cloud, check to see if source IP is allowed"

func IsRefreshError(err error) bool {
	return err != nil && strings.Contains(err.Error(), RefreshNotEnabledYet.Error())
}

// Whether or not to use internal or external (alternate) addressing for communications
type AddressType int

const (
	Uninitialized AddressType = iota
	Internal      AddressType = iota
	External      AddressType = iota
)

func (a AddressType) String() string {
	switch a {
	case Uninitialized:
		return "Uninitialized"
	case Internal:
		return "Internal"
	case External:
		return "External"
	default:
		return "?? (AddressType)"
	}
}

func (a *AddressType) Set(external bool) {
	if external {
		*a = External
	} else {
		*a = Internal
	}
}

type ConnectivityStatus int

const (
	// Nothing wrong yet
	ConnValid ConnectivityStatus = iota
	// If any remote cluster node returned authentication error
	ConnAuthErr ConnectivityStatus = iota
	// If this node experienced connectivity issues to a least one remote cluster nodes
	ConnDegraded ConnectivityStatus = iota
	// If this node cannot contact every single remote cluster nodes
	ConnError ConnectivityStatus = iota
)

func (c ConnectivityStatus) String() string {
	switch c {
	case ConnValid:
		return "RC_OK"
	case ConnAuthErr:
		return "RC_AUTH_ERR"
	case ConnDegraded:
		return "RC_DEGRADED"
	case ConnError:
		return "RC_ERROR"
	default:
		return "?? (ConnectivityStatus)"
	}
}

func NewConnectivityHelper() *ConnectivityHelper {
	return &ConnectivityHelper{
		nodeStatus: make(map[string]ConnectivityStatus),
	}
}

type ConnectivityHelper struct {
	nodeStatus map[string]ConnectivityStatus
	mtx        sync.RWMutex
}

func (c *ConnectivityHelper) String() string {
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	var output []string
	for node, status := range c.nodeStatus {
		output = append(output, fmt.Sprintf("Node: %v Status: %v |", node, status.String()))
	}
	return strings.Join(output, " ")
}

// Returns true if node has existed and state has been changed
func (c *ConnectivityHelper) MarkNode(nodeName string, status ConnectivityStatus) (changedState, authErrFixed bool) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if curStatus, exists := c.nodeStatus[nodeName]; exists && curStatus != status {
		changedState = true
		if curStatus == ConnAuthErr && status == ConnValid {
			authErrFixed = true
		}
	}
	c.nodeStatus[nodeName] = status
	return
}

func (c *ConnectivityHelper) SyncWithValidList(nodeList base.StringPairList) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	var keysToDelete []string
	for _, nodePair := range nodeList {
		var found bool
		var nodeName string
		for nodeName, _ = range c.nodeStatus {
			if nodeName == nodePair.GetFirstString() {
				found = true
				break
			}
		}
		if !found {
			keysToDelete = append(keysToDelete, nodeName)
		}
	}

	for _, key := range keysToDelete {
		delete(c.nodeStatus, key)
	}

	// The Given the list, only set to connValid if it never existed before
	for _, nodePair := range nodeList {
		_, exists := c.nodeStatus[nodePair.GetFirstString()]
		if !exists {
			c.nodeStatus[nodePair.GetFirstString()] = ConnValid
		}
	}

	// For the rest, if this func is called, it means that target has returned a valid nodeList
	// Reset any auth errors
	for key, status := range c.nodeStatus {
		if status == ConnAuthErr {
			c.nodeStatus[key] = ConnValid
		}
	}

	// If a node is in the ConnError state, leave them be. Let the refresh() take care of
	// fixing the connError state when the node is contacted for refresh and returned successfully
}

func (c *ConnectivityHelper) GetOverallStatus() ConnectivityStatus {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	totalCount := len(c.nodeStatus)
	var connErrCount int

	for _, status := range c.nodeStatus {
		if status == ConnAuthErr {
			// Any auth error means the whole cluster is auth errored
			return ConnAuthErr
		}
		if status == ConnError {
			connErrCount++
		}
	}

	if connErrCount == totalCount {
		return ConnError
	} else if connErrCount > 0 {
		return ConnDegraded
	} else {
		return ConnValid
	}
}

/**
 * A RemoteClusterAgent is responsible for handling all operations related to a specific RemoteClusterReference.
 * RemoteClusterService's job is to wrap around them and provide APIs to other components that require info
 * or operation regarding a specific Remote Cluster Reference.
 */
type RemoteClusterAgentIface interface {
	/* Modifier ops */
	Start(newRef *metadata.RemoteClusterReference) error
	Stop()
	// This call will set the RemoteClusterAgent's internal reference with new information from newRef
	UpdateReferenceFrom(newRef *metadata.RemoteClusterReference, writeToMetaKv bool) error
	// This call will queue the update and run it in the background without needing to wait for success
	UpdateReferenceFromAsync(newRef *metadata.RemoteClusterReference, writeToMetaKv bool) error
	DeleteReference(delFromMetaKv bool) (*metadata.RemoteClusterReference, error)
	Refresh() error

	/* Getter ops */
	// To be used for RemoteClusterService for any caller requesting a copy of the RC Reference
	GetReferenceClone() *metadata.RemoteClusterReference
	GetReferenceAndStatusClone() *metadata.RemoteClusterReference
	GetConnectionStringForCAPIRemoteCluster() string
	UsesAlternateAddress() (bool, error)
	GetCapability() (metadata.Capability, error)
}

type RemoteClusterAgent struct {
	/** Members protected by refMutex */
	// Mutex used to protect any internal data structure that may be modified
	refMtx sync.RWMutex
	// The offical local copy of the RemoteClusterReference. Use Clone() method to make a copy.
	reference metadata.RemoteClusterReference
	// The most up-to-date cached list of nodes in pairs of [httpAddr, httpsAddr]
	refNodesList base.StringPairList
	// Flag to state that metakv deletes have occured. Any concurrent refresh() taking place
	// when delete occures should NOT write to metakv after this is set
	deletedFromMetakv bool
	// For this remote cluster, use the following addressing scheme
	addressPreference AddressType
	/* refreshContext persisted information - only used by instances of refreshContext */
	pendingAddressPreference AddressType
	pendingAddressPrefCnt    int
	configurationChanged     bool
	// As the remote cluster is upgraded, certain features will automatically become enabled
	currentCapability *metadata.Capability

	// function pointer to callback
	metadataChangeCallback base.MetadataChangeHandlerCallback
	metadataChangeCbMtx    sync.RWMutex

	// Wait group for making sure we exit synchronized
	agentWaitGrp sync.WaitGroup
	// finChannel for refresher
	agentFinCh chan bool
	// Make sure we call stop only once
	stopOnce sync.Once
	stopped  uint32

	// When agent started asynchronously, prevent a refresh from happening until the first update
	// finishes
	initDone uint32
	// It is possible that there may be concurrent calls to refresh()
	refreshActive uint32
	refreshResult []chan error

	// Refresh() operation needs synchronization primatives in case abort is needed
	refreshMtx        sync.Mutex
	refreshCv         *sync.Cond
	refreshAbortState int
	// As part of refresh() it launches a bg task to do the RPC
	refreshRPCState int
	// The opaque is to prevent a stale refresh RPC from modifying the state
	refreshRPCOpaque uint64
	// If a user-induced set is taking place, disable any refresh
	temporaryDisableRefresh bool

	// for logging
	logger *log.CommonLogger
	// Metadata service reference
	metakvSvc service_def.MetadataSvc
	// uilog svc for printing
	uiLogSvc service_def.UILogSvc
	// utilites service
	utils utilities.UtilsIface

	// Each bucket on one remote cluster will have one centralized getter
	bucketManifestGetters map[string]*BucketManifestGetter
	// bucket refcounts
	bucketRefCnt map[string]uint32
	// protects the map
	bucketMtx sync.RWMutex

	/* Staging changes area */
	pendingRef      metadata.RemoteClusterReference
	pendingRefNodes base.StringPairList

	/* Post processing */
	oldRef *metadata.RemoteClusterReference

	// For certain unit tests, bypass metakv
	unitTestBypassMetaKV bool

	// To be able to return remote cluster status
	connectivityHelper *ConnectivityHelper
}

const (
	refreshRPCNotInit  = 0
	refreshRPCUnderway = 1
	refreshRPCDone     = 2
)

const (
	refreshAbortNotRequested = 0
	refreshAbortRequested    = 1
	refreshAbortAcknowledged = 2
)

func (agent *RemoteClusterAgent) GetReferenceClone() *metadata.RemoteClusterReference {
	agent.refMtx.RLock()
	defer agent.refMtx.RUnlock()
	return agent.reference.Clone()
}

func (agent *RemoteClusterAgent) GetReferenceAndStatusClone() *metadata.RemoteClusterReference {
	ref := agent.GetReferenceClone()
	ref.SetConnectivityStatus(agent.connectivityHelper.GetOverallStatus().String())
	return ref
}

func (agent *RemoteClusterAgent) ConfigurationHasChanged() bool {
	agent.refMtx.RLock()
	defer agent.refMtx.RUnlock()

	return agent.configurationChanged
}

func (agent *RemoteClusterAgent) GetConnectionStringForCAPIRemoteCluster() (string, error) {
	agent.refMtx.RLock()
	defer agent.refMtx.RUnlock()

	if len(agent.refNodesList) == 0 {
		// if host name list is empty, which could be the case when goxdcr process is first started
		// fall back to using reference.activeHostName
		return agent.reference.MyConnectionStr()
	}
	// we only need the string1/hostname part for capi
	// since capi replication is always non-ssl type, and there is no need for https addr
	toBeSortedList := agent.refNodesList.GetListOfFirstString()
	sort.Strings(toBeSortedList)
	return toBeSortedList[0], nil
}

func (agent *RemoteClusterAgent) clearAddressModeAccounting() {
	agent.refMtx.Lock()
	defer agent.refMtx.Unlock()

	agent.pendingAddressPreference = Uninitialized
	agent.pendingAddressPrefCnt = 0
	agent.configurationChanged = false
}

func (agent *RemoteClusterAgent) initializeNewRefreshContext() (*refreshContext, chan error, error) {
	if !agent.InitDone() {
		return nil, nil, RefreshNotEnabledYet
	}

	var err error
	var resultCh chan error
	agent.refreshMtx.Lock()
	if agent.temporaryDisableRefresh {
		err = SetInProgress
	} else {
		if agent.refreshActive == 0 {
			// This is the first Refresh() call and will get the honor of executing
			agent.refreshActive = 1
		} else {
			// A Refresh() is currently ongoing - piggy back onto that running process for the result
			resultCh = make(chan error, 1)
			agent.refreshResult = append(agent.refreshResult, resultCh)
			err = RefreshAlreadyActive
		}
	}
	agent.refreshMtx.Unlock()

	if err != nil {
		return nil, resultCh, err
	}

	rctx := &refreshContext{agent: agent}
	rctx.initialize()
	return rctx, nil, nil
}

func (agent *RemoteClusterAgent) cleanupRefreshContext(rctx *refreshContext, result error) {
	agent.refreshMtx.Lock()
	if rctx != nil {
		switch agent.refreshAbortState {
		case refreshAbortNotRequested:
			break
		case refreshAbortRequested:
			// too late
			defer agent.refreshCv.Broadcast()
			agent.refreshAbortState = refreshAbortAcknowledged
		case refreshAbortAcknowledged:
			// too late anyway
			break
		}
	}

	for _, ch := range agent.refreshResult {
		ch <- result
		close(ch)
	}

	agent.refreshResult = agent.refreshResult[:0]
	agent.refreshActive = 0
	agent.refreshRPCState = refreshRPCNotInit
	agent.refreshMtx.Unlock()
}

// This is used as a helper context during each refresh operation
type refreshContext struct {
	// For comparison and editing
	refOrig            *metadata.RemoteClusterReference
	refCache           *metadata.RemoteClusterReference
	cachedRefNodesList base.StringPairList
	origCapability     metadata.Capability
	cachedCapability   metadata.Capability
	capabilityLoaded   bool
	nodeListUpdated    bool

	// connection related
	connStr       string
	hostName      string
	httpsHostName string

	// iterator related
	index           int
	atLeastOneValid bool

	// agent shortcut
	agent *RemoteClusterAgent

	// addressType refresh
	origAddressPref           AddressType
	addressPrefUpdate         bool
	cachedNodeListWithMinInfo []interface{}

	// non-empty if the refresh context needs to raise an UI error message when committing
	uiErrorMsg string
}

// Initializes the context and also populates the credentials for connecting to nodes
func (rctx *refreshContext) initialize() {
	// First cache the info
	rctx.agent.refMtx.RLock()
	// For comparison
	rctx.refOrig = rctx.agent.reference.Clone()
	// for editing
	rctx.refCache = rctx.agent.reference.Clone()
	rctx.cachedRefNodesList = base.DeepCopyStringPairList(rctx.agent.refNodesList)
	// addressType
	rctx.origAddressPref = rctx.agent.addressPreference
	if rctx.agent.currentCapability != nil {
		rctx.origCapability = rctx.agent.currentCapability.Clone()
	}
	rctx.agent.refMtx.RUnlock()

	rctx.index = 0
	rctx.atLeastOneValid = false
	if len(rctx.cachedRefNodesList) == 0 {
		// target node list may be empty if goxdcr process has been restarted. populate it with ActiveHostName or HostName
		activeHostName := rctx.refOrig.ActiveHostName()
		if len(activeHostName) == 0 {
			activeHostName = rctx.refOrig.HostName()
		}
		activeHttpsHostName := rctx.refOrig.ActiveHttpsHostName()
		if len(activeHttpsHostName) == 0 {
			activeHttpsHostName = rctx.refOrig.HttpsHostName()
		}
		rctx.cachedRefNodesList = append(rctx.cachedRefNodesList, base.StringPair{activeHostName, activeHttpsHostName})
	} else if len(rctx.cachedRefNodesList) > 1 {
		// Randomize the list of hosts to walk through
		base.ShuffleStringPairList(rctx.cachedRefNodesList)
	}

}

func (rctx *refreshContext) setHostNamesAndConnStr(pair base.StringPair) {
	rctx.hostName = pair.GetFirstString()
	rctx.httpsHostName = pair.GetSecondString()

	if rctx.refCache.IsHttps() {
		rctx.connStr = rctx.httpsHostName
	} else {
		rctx.connStr = rctx.hostName
	}
}

func (rctx *refreshContext) checkAndUpdateActiveHost() {
	if rctx.refCache.ActiveHostName() != rctx.hostName {
		// update ActiveHostName to the new selected node if needed
		rctx.refCache.SetActiveHostName(rctx.hostName)
		rctx.refCache.SetActiveHttpsHostName(rctx.httpsHostName)
		rctx.agent.logger.Infof("Replaced ActiveHostName in ref %v with %v and ActiveHttpsHostName with %v\n", rctx.refCache.Id(), rctx.hostName, rctx.httpsHostName)
	}
}

// Updates the agent reference if changes are made
// Will also update the reference's boostrap hostname if necessary
func (rctx *refreshContext) checkAndUpdateAgentReference() error {
	err := rctx.checkIfAbortRequested()
	if err != nil {
		return err
	}

	if !rctx.refOrig.IsSame(rctx.refCache) || rctx.nodeListUpdated || rctx.addressPrefUpdate || rctx.capabilityChanged() {
		// 1. when refOrig.IsSame(refCache) is true, i.e., when there have been no changes to refCache,
		//    updateReferenceFromNoLock is not called
		// 2. when refOrig.IsSame(refCache) is false, and refOrig.IsEssentiallySame(refCache) is true,
		//    i.e., when there have been changes to transient fields like ActiveHostName in refCache,
		//    updateReferenceFromNoLock is called to get the transient fields updated.
		//    there are no metakv update or metadata change callback, though
		// 3. when refOrig.IsEssentiallySame(refCache) is false,
		//    i.e., when there have been changes to essential fields in refCache,
		//    updateReferenceFromNoLock is called with metakv update and metadata change callback
		// 4. When refresh context has shown that the address preference count needs to be updated
		//    only if there is not another concurrent refresh context updating at the same time
		//    and that it isn't waiting for a consumer to poll
		if !rctx.refOrig.IsSame(rctx.refCache) || rctx.addressPrefUpdate || rctx.capabilityChanged() {
			isEssentiallySame := rctx.refOrig.IsEssentiallySame(rctx.refCache)
			updateErr := rctx.agent.updateReferenceFromInternal(rctx.refCache, !isEssentiallySame, /*updateMetaKv*/
				!isEssentiallySame /*shouldCallCb*/, true /*synchronous*/, rctx)
			if updateErr != nil {
				if updateErr != DeleteAlreadyIssued {
					rctx.agent.logger.Warnf(updateErr.Error())
				}
				return updateErr
			}
			if rctx.uiErrorMsg != "" {
				rctx.agent.uiLogSvc.Write(rctx.uiErrorMsg)
			}
		} else {
			rctx.agent.refMtx.Lock()
			rctx.agent.refNodesList = base.DeepCopyStringPairList(rctx.cachedRefNodesList)
			rctx.agent.refMtx.Unlock()
		}

		rctx.agent.logger.Infof(populateRefreshSuccessMsg(rctx.refOrig.CloneAndRedact(), rctx.agent.reference.CloneAndRedact(), rctx.agent.refNodesList))
	}

	return nil
}

func (rctx *refreshContext) finalizeRefCacheListFrom(listToBeUsed base.StringPairList, nodeList []interface{}) {
	sort.Sort(listToBeUsed)
	sort.Sort(rctx.cachedRefNodesList)
	rctx.nodeListUpdated = !reflect.DeepEqual(rctx.cachedRefNodesList, listToBeUsed)

	rctx.cachedRefNodesList = listToBeUsed
	if !rctx.atLeastOneValid {
		rctx.atLeastOneValid = true
	}
	rctx.cachedNodeListWithMinInfo = nodeList
}

func (rctx *refreshContext) verifyNodeAndGetList(connStr string, updateSecuritySettings bool) ([]interface{}, error) {
	username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, err := rctx.refCache.MyCredentials()
	if err != nil {
		rctx.agent.logger.Warnf("When refreshing remote cluster reference %v, skipping node %v because of error retrieving user credentials from reference. err=%v\n", rctx.refCache.Id(), connStr, err)
		return nil, err
	}

	var defaultPoolInfo map[string]interface{}
	var bgErr error
	var statusCode int
	// This backgroundRPC is launched with an opaque
	// Because it is possible that an Refresh is aborted - in which this function call will need to bail out
	// quickly and leave this RPC call hanging.
	// The RPC call may lag dramatically and by the time signalFunc() is called, another backgroundRPC is underway
	// So the opaque is to ensure that only the right incarnation of backgroundRPC can modify the data and send broadcasts
	backgroundRpc := func(opaque uint64) {
		signalFunc := func() {
			rctx.agent.refreshMtx.Lock()
			defer rctx.agent.refreshMtx.Unlock()
			if rctx.agent.refreshRPCOpaque == opaque && rctx.agent.refreshRPCState == refreshRPCUnderway {
				rctx.agent.refreshRPCState = refreshRPCDone
				rctx.agent.refreshCv.Broadcast()
			}
		}
		defer signalFunc()
		if updateSecuritySettings && rctx.refCache.IsEncryptionEnabled() {
			// if updateSecuritySettings is true, get up to date security settings from target
			sanInCertificate, httpAuthMech, defaultPoolInfo, statusCode, bgErr = rctx.agent.utils.GetSecuritySettingsAndDefaultPoolInfo(rctx.hostName, rctx.httpsHostName, username, password, certificate, clientCertificate, clientKey, rctx.refCache.IsHalfEncryption(), rctx.agent.logger)
			rctx.markNodeWithStatus(statusCode)
			if bgErr != nil {
				rctx.agent.logger.Warnf("When refreshing remote cluster reference %v, skipping node %v because of error retrieving security settings from target. err=%v\n", rctx.refCache.Id(), connStr, bgErr)
				return
			}
		} else {
			defaultPoolInfo, bgErr, statusCode = rctx.agent.utils.GetClusterInfoWStatusCode(connStr, base.DefaultPoolPath, username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, rctx.agent.logger)
			rctx.markNodeWithStatus(statusCode)
			if bgErr != nil {
				rctx.agent.logger.Warnf("When refreshing remote cluster reference %v, skipping node %v because of error retrieving default pool info from target. statusCode=%v err=%v\n", rctx.refCache.Id(), connStr,
					statusCode, bgErr)
				return
			}
		}
	}
	// This operation can take a while. Let it run in the bg and watch for any abort signals
	rctx.agent.refreshMtx.Lock()
	// Before launching bg go-routine, check for abort signals
	switch rctx.agent.refreshAbortState {
	case refreshAbortRequested:
		rctx.agent.refreshAbortState = refreshAbortAcknowledged
		defer rctx.agent.refreshCv.Broadcast()
		err = RefreshAborted
	case refreshAbortAcknowledged:
		// Don't launch the goroutine
		err = RefreshAborted
	case refreshAbortNotRequested:
		rctx.agent.refreshRPCState = refreshRPCUnderway
		// Assign a new ID for the background task
		rctx.agent.refreshRPCOpaque++
		go backgroundRpc(rctx.agent.refreshRPCOpaque)
		for {
			rctx.agent.refreshCv.Wait()
			// When this wakes up, can be potential spurious wakeup, RPC Done, or abort
			if rctx.agent.refreshAbortState == refreshAbortRequested || rctx.agent.refreshAbortState == refreshAbortAcknowledged {
				// Revoke permission from the background RPC function
				rctx.agent.refreshRPCOpaque++
				err = RefreshAborted
				if rctx.agent.refreshAbortState == refreshAbortRequested {
					defer rctx.agent.refreshCv.Broadcast()
				}
				break
			} else if rctx.agent.refreshRPCState == refreshRPCDone {
				err = bgErr
				break
			}
		}
	}
	rctx.agent.refreshMtx.Unlock()
	if err != nil {
		return nil, err
	}

	capabilityErr := rctx.cachedCapability.LoadFromDefaultPoolInfo(defaultPoolInfo, rctx.agent.logger)
	if capabilityErr == nil {
		rctx.capabilityLoaded = true
	}

	clusterUUID, nodeList, err := rctx.agent.utils.GetClusterUUIDAndNodeListWithMinInfoFromDefaultPoolInfo(defaultPoolInfo, rctx.agent.logger)
	if err != nil {
		rctx.agent.logger.Warnf("When refreshing remote cluster reference %v, skipping node %v because of error parsing default pool info. err=%v\n", rctx.refCache.Id(), connStr, err)
		return nil, err
	}
	// selected node is accessible
	refCacheUuid := rctx.refCache.Uuid()
	if len(refCacheUuid) == 0 {
		rctx.agent.logger.Warnf("Current reference has empty UUID, setting to %v - this is likely due to a failed node-pull when the reference was first added", clusterUUID)
	} else if clusterUUID != refCacheUuid {
		rctx.agent.logger.Warnf("Cluster UUID: %v and refCache UUID: %v", clusterUUID, refCacheUuid)
		return nil, UUIDMismatchError
	}

	if updateSecuritySettings {
		// update security settings only if the target node is still in the same target cluster
		if rctx.refCache.IsEncryptionEnabled() {
			if rctx.refCache.SANInCertificate() != sanInCertificate {
				rctx.agent.logger.Infof("Preparing to update sanInCertificate in remote cluster reference %v to %v\n", rctx.refCache.Id(), sanInCertificate)
				rctx.refCache.SetSANInCertificate(sanInCertificate)
			}
			refCacheAuthMech := rctx.refCache.HttpAuthMech()
			if refCacheAuthMech != httpAuthMech {
				rctx.agent.logger.Infof("Preparing to update httpAuthMech in remote cluster reference %v from %v to %v\n", rctx.refCache.Id(), refCacheAuthMech, httpAuthMech)
				rctx.refCache.SetHttpAuthMech(httpAuthMech)
			}
		}
	}

	return nodeList, err
}

func (rctx *refreshContext) markNodeWithStatus(statusCode int) {
	markedHostname := rctx.hostName

	if statusCode == http.StatusUnauthorized {
		changed, _ := rctx.agent.connectivityHelper.MarkNode(markedHostname, ConnAuthErr)
		if changed {
			// This rctx will not get to be able to update the agent's reference, so log the error now
			rctx.agent.uiLogSvc.Write(fmt.Sprintf("The remote cluster reference node %v returned authentication error. Please check the remote cluster credentials", markedHostname))
		}
	} else if statusCode == http.StatusOK {
		_, authErrFixed := rctx.agent.connectivityHelper.MarkNode(markedHostname, ConnValid)
		if authErrFixed {
			rctx.agent.uiLogSvc.Write(fmt.Sprintf("The remote cluster credentials that includes node %v have now been fixed", markedHostname))
		}
	} else {
		// Any non-OK return code
		rctx.agent.connectivityHelper.MarkNode(markedHostname, ConnError)
	}
}

func (rctx *refreshContext) checkUserIntent(nodeList []interface{}) {
	if rctx.refOrig.HostnameMode() != metadata.HostnameMode_None {
		// No need to check intent
		return
	}

	isExternal, err := rctx.agent.checkIfHostnameIsAlternate(nodeList, rctx.hostName, rctx.refCache.IsFullEncryption())
	if err != nil {
		rctx.agent.logger.Warnf("Unable to figure out if hostname %v is alternate or not", rctx.hostName)
		return
	}
	useExternal, err := rctx.getAddressPreference()
	if err != nil {
		rctx.agent.logger.Warnf("Unable to figure out if should use external")
		return
	}

	if useExternal && !isExternal || !useExternal && isExternal {
		// Change is expected
		rctx.addressPrefUpdate = true
	}
	rctx.cachedNodeListWithMinInfo = nodeList
}

func (rctx *refreshContext) capabilityChanged() bool {
	return rctx.capabilityLoaded && !rctx.origCapability.IsSameAs(rctx.cachedCapability)
}

func (agent *RemoteClusterAgent) Refresh() error {
	rctx, refreshErrCh, err := agent.initializeNewRefreshContext()
	if err != nil {
		if err == RefreshAlreadyActive {
			// Just wait until the err is returned from the active result
			err = <-refreshErrCh
			agent.logger.Warnf("Concurrent refresh was ongoing. Using the same result... %v", err)
			return err
		} else {
			agent.logger.Errorf("Refresh err %v\n", err)
			return err
		}
	}

	// At this point, everything below can only be executed by a single refresh context
	defer agent.cleanupRefreshContext(rctx, err)

	useExternal, err := rctx.getAddressPreference()
	if err != nil {
		// Do not allow refresh to continue - wait until next refresh cycle to try again
		return err
	}

	var nodeAddressesList base.StringPairList
	for rctx.index = 0; rctx.index < len(rctx.cachedRefNodesList /*already shuffled*/); rctx.index++ {
		hostnamePair := rctx.cachedRefNodesList[rctx.index]
		rctx.setHostNamesAndConnStr(hostnamePair)

		nodeList, err := rctx.verifyNodeAndGetList(rctx.connStr, true /*updateSecuritySettings*/)
		if err != nil {
			if err == UUIDMismatchError {
				if rctx.hostName == rctx.refOrig.HostName() && len(rctx.cachedRefNodesList) == 1 {
					// If this is the only node to be checked AND this is the bootstrap node
					// then there's nothing to do now as there is no more nodes in the list to walk
					return BootStrapNodeHasMovedError
				}
			} else if err == RefreshAborted {
				return err
			}
		} else {
			// rctx.hostname is in the cluster and is available - make it the activeHost
			rctx.checkAndUpdateActiveHost()

			rctx.checkUserIntent(nodeList)

			nodeAddressesList, err = agent.utils.GetRemoteNodeAddressesListFromNodeList(nodeList, rctx.connStr, rctx.refCache.IsEncryptionEnabled(), agent.logger, useExternal)
			if err == nil {
				// This node is an acceptable replacement for active node - and sets atLeastOneValid
				rctx.finalizeRefCacheListFrom(nodeAddressesList, nodeList)

				if rctx.refOrig.IsDnsSRV() {
					rctx.checkDnsSRVEntries(nodeAddressesList)
				} else {
					rctx.checkIfBootstrapNodeIsValidAndReplaceIfNot(nodeAddressesList)
				}

				// We are done
				break
			} else {
				// Look for another node
				agent.logger.Warnf("Error getting node name list for remote cluster reference %v using connection string %v. err=%v\n", rctx.refCache.Id(), rctx.connStr, err)
			}
		}
	} // end for

	if !rctx.atLeastOneValid {
		errMsg := fmt.Sprintf("Failed to refresh remote cluster reference %v since none of the nodes in target node list is accessible. node list = %v\n", rctx.refCache.Id(), rctx.cachedRefNodesList)
		agent.logger.Error(errMsg)
		return errors.New(errMsg)
	}

	// If there's anything that needs to be persisted to agent, update it
	err = rctx.checkAndUpdateAgentReference()
	return err
}

func (rctx *refreshContext) checkDnsSRVEntries(nodeAddressesList base.StringPairList) {
	_, _, totalEntries, err := rctx.refCache.RefreshSRVEntries()
	if err != nil || totalEntries == 0 {
		oldHostName := rctx.refCache.HostName()
		rctx.replaceHostNameUsingList(nodeAddressesList)
		rctx.uiErrorMsg = rctx.getSRVReplacementMsg(oldHostName)
		if err != nil {
			rctx.agent.logger.Errorf(err.Error())
		}
	} else {
		err = rctx.refCache.CheckSRVValidityUsingNodeAddressesList(nodeAddressesList)
		if err != nil {
			if err == metadata.ErrorNoBootableSRVEntryFound {
				// Do a second level validation
				err = rctx.refCache.CheckSRVValidityByUUID(rctx.agent.utils.GetClusterUUID,
					rctx.agent.logger)
				if err != nil {
					rctx.agent.logger.Errorf(err.Error())
					if err == metadata.ErrorNoBootableSRVEntryFound {
						oldHostName := rctx.refCache.HostName()
						rctx.replaceHostNameUsingList(nodeAddressesList)
						rctx.uiErrorMsg = rctx.getSRVReplacementMsg(oldHostName)
					}
				}
			}
		}
	}
}

func (rctx *refreshContext) getSRVReplacementMsg(oldHostName string) string {
	return fmt.Sprintf("Remote Cluster reference was given DNS SRV record with %v, but the DNS SRV entries are no longer valid for bootstrap. The hostname will now be replaced. To restore, re-edit the remote cluster reference manually.",
		oldHostName)
}

func (rctx *refreshContext) checkIfBootstrapNodeIsValidAndReplaceIfNot(nodeAddressesList base.StringPairList) {
	//  so check the list to make sure that the bootstrap node is valid
	hostNameInCluster := false
	for _, pair := range nodeAddressesList {
		// refCache.HostName() could be http addr or https addr
		if pair.GetFirstString() == rctx.refCache.HostName() || pair.GetSecondString() == rctx.refCache.HostName() {
			hostNameInCluster = true
			break
		}
	}
	if !hostNameInCluster {
		// Bootstrap mode is NOT in the node list - find a replace node if possible, from the already pulled list
		rctx.replaceHostNameUsingList(nodeAddressesList)
	}
}

// Returns true if this call successfully disabled refresh, and need to re-enable it
func (agent *RemoteClusterAgent) AbortAnyOngoingRefresh() (needToReEnable bool) {
	agent.refreshMtx.Lock()
	defer agent.refreshMtx.Unlock()
	// This call could be called concurrently, only the first call should do the honor of restoration
	if agent.temporaryDisableRefresh == false {
		agent.temporaryDisableRefresh = true
		needToReEnable = true
	}
	if agent.refreshActive > 0 {
		if agent.refreshAbortState == refreshAbortNotRequested {
			// The first call should set the state
			agent.refreshAbortState = refreshAbortRequested
		}
		for agent.refreshAbortState != refreshAbortAcknowledged {
			agent.refreshCv.Wait()
		}
	}
	return
}

func (agent *RemoteClusterAgent) ReenableRefresh() {
	agent.refreshMtx.Lock()
	defer agent.refreshMtx.Unlock()
	agent.temporaryDisableRefresh = false
	agent.refreshAbortState = refreshAbortNotRequested
}

func (rctx *refreshContext) checkIfAbortRequested() error {
	rctx.agent.refreshMtx.Lock()
	defer rctx.agent.refreshMtx.Unlock()
	switch rctx.agent.refreshAbortState {
	case refreshAbortNotRequested:
		return nil
	case refreshAbortRequested:
		rctx.agent.refreshAbortState = refreshAbortAcknowledged
		defer rctx.agent.refreshCv.Broadcast()
		return RefreshAborted
	case refreshAbortAcknowledged:
		return RefreshAborted
	}
	return fmt.Errorf("Not implemented")
}

func (rctx *refreshContext) getAddressPreference() (useExternal bool, err error) {
	addressPref := rctx.origAddressPref
	useExternal = addressPref == External
	err = rctx.checkIfAbortRequested()
	if err != nil {
		return
	}
	if addressPref == Uninitialized {
		// Uninitialized means that when the agent started up, it had trouble determining user's intent
		// is external or interna. Take this refresh cycle to figure that out again
		useExternal, err = rctx.initAlternateAddress()
	}
	return
}

// Agent does not have its preference set yet
func (rctx *refreshContext) initAlternateAddress() (bool, error) {
	var useExternal bool

	username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, err := rctx.refCache.MyCredentials()
	if err != nil {
		return useExternal, err
	}
	connStr, err := rctx.refCache.MyConnectionStr()
	if err != nil {
		return useExternal, err
	}
	nodeList, err := rctx.agent.utils.GetNodeListWithMinInfo(connStr, username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, rctx.agent.logger)
	if err != nil {
		return useExternal, err
	}

	useExternal, err = rctx.agent.checkIfHostnameIsAlternate(nodeList, rctx.refCache.HostName(), rctx.refCache.IsHttps())
	if err != nil {
		return useExternal, err
	}

	rctx.agent.setAddressPreference(useExternal, true /*lock*/, true /*setOnlyIfUninit*/)

	rctx.origAddressPref = rctx.agent.getAddressPreference()
	return rctx.origAddressPref == External, nil
}

func (rctx *refreshContext) replaceHostNameUsingList(nodeAddressesList base.StringPairList) {
	// sort the node list, so that the selection of the replacement node will be deterministic
	// in other words, if two source nodes performs the selection at the same time,
	// they will get the same replacement node. this way less strain is put on metakv
	sortedList := base.DeepCopyStringPairList(nodeAddressesList)
	sort.Sort(sortedList)

	for i := 0; i < len(sortedList); i++ {
		rctx.setHostNamesAndConnStr(sortedList[i])

		// updateSecuritySettings is set to false since security settings should have been updated shortly before in Refresh()
		_, err := rctx.verifyNodeAndGetList(rctx.connStr, false /*updateSecuritySettings*/)

		if err == nil {
			// this is the node to set
			oldHostName := rctx.refCache.HostName()
			if rctx.refCache.IsFullEncryption() {
				// in full encryption mode, set hostname in ref to https address
				rctx.refCache.SetHostName(rctx.httpsHostName)
			} else {
				rctx.refCache.SetHostName(rctx.hostName)
			}
			rctx.refCache.SetHttpsHostName(rctx.httpsHostName)
			rctx.agent.logger.Infof("Pending update hostname in remote cluster reference %v from %v to %v.\n", rctx.refCache.Id(), oldHostName, rctx.refCache.HostName())
			return
		}
	}
	rctx.agent.logger.Warnf("Error: Unable to replace bootstrap node in RemoteClusterReference. It may be invalid if XDCR restarts")
}

/**
 * Starts a RemoteClusterAgent and associate it with the incoming new reference.
 * This agent will be responsible for any information regarding the specific cluster.
 * If user initiated this start, then take charge and create the metakv entry.
 * NOTE: If returned error is non-nil, this method must not have spawned any rouge go-routines.
 */
func (agent *RemoteClusterAgent) Start(newRef *metadata.RemoteClusterReference, userInitiated bool) error {
	var err error

	if userInitiated {
		// If user initiated, it means that no other reference should have existed
		// It is safer to do a synchronous update
		err = agent.UpdateReferenceFrom(newRef, true /*writeToMetakv*/)
	} else {
		// This is a startup time and loading from metakv, not going to update metakv
		// Quickly get the agent up and running and let any error be fixed by refresh op
		err = agent.UpdateReferenceFromAsync(newRef, false /*WriteToMetakv*/)
	}

	if err == nil {
		agent.refMtx.RLock()
		agentId := agent.pendingRef.Id()
		agentName := agent.pendingRef.Name()
		agentUuid := agent.pendingRef.Uuid()
		agent.refMtx.RUnlock()
		agent.logger.Infof("Agent %v %v started for cluster: %v synchronously? %v", agentId, agentName, agentUuid, userInitiated)
		agent.agentWaitGrp.Add(1)
		go agent.runPeriodicRefresh()
	} else {
		agent.logger.Warnf("Agent %v starting resulted in error: %v", newRef.Id(), err)
	}

	if userInitiated {
		// When userInitiated, meaning that someone called AddRemoteCluster, then only allow Refresh
		// operations to continue.
		// Async call above would have set this automatically
		atomic.StoreUint32(&agent.initDone, 1)
	}
	return err
}

func (agent *RemoteClusterAgent) stopAllGoRoutines() {
	close(agent.agentFinCh)

	// Wait for all go routines to stop before clean up
	agent.agentWaitGrp.Wait()
}

func (agent *RemoteClusterAgent) IsStopped() bool {
	return atomic.LoadUint32(&agent.stopped) > 0
}

// Once it's been Stopped, an agent *must* be deleted and not reused due to the stopOnce here
func (agent *RemoteClusterAgent) Stop() {
	agent.stopOnce.Do(func() {
		atomic.StoreUint32(&agent.stopped, 1)

		var cachedId string
		var cachedName string
		var cachedUuid string
		agent.refMtx.RLock()
		if !agent.reference.IsEmpty() {
			cachedId = agent.reference.Id()
			cachedName = agent.reference.Name()
			cachedUuid = agent.reference.Uuid()
		} else {
			cachedId = agent.oldRef.Id()
			cachedName = agent.oldRef.Name()
			cachedUuid = agent.oldRef.Uuid()
		}
		agent.refMtx.RUnlock()

		agent.logger.Infof("Agent %v %v stopping for cluster: %v", cachedId, cachedName, cachedUuid)
		// Stop all go-routines here
		agent.stopAllGoRoutines()
	})
}

// Cleans up. Returns a copy of the old reference back as part of the stoppage
// If err occurred at any point, then the reference may still exist in metaKV
func (agent *RemoteClusterAgent) DeleteReference(delFromMetaKv bool) (*metadata.RemoteClusterReference, error) {
	var err error

	// When deleting reference, the clonedCopy is used for logging
	agent.refMtx.RLock()
	clonedCopy := agent.reference.Clone()
	agent.refMtx.RUnlock()

	if delFromMetaKv {
		err = agent.deleteFromMetaKV()
	}

	if service_def.DelOpConsideredPass(err) {
		agent.clearReference()
		agent.callMetadataChangeCb()
	}
	return clonedCopy, err
}

/**
 * Given the reference being staged in the reference, use the bare bone information to get more
 * information for caching purposes
 */
func (agent *RemoteClusterAgent) syncInternalsFromStagedReference(rctx *refreshContext) error {
	var nodeList []interface{}
	_, capabilityErr := agent.GetCapability()
	needToInitCapability := capabilityErr != nil
	if rctx != nil && !needToInitCapability {
		nodeList = rctx.cachedNodeListWithMinInfo
	}

	username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey,
		connStr, refNodesList, encryptionEnabled, err := agent.getCredentialsAndMisc()
	if err != nil {
		return err
	}

	// If refresh context has already done the heavy lifting work of reaching the remote node to get the list
	// Then just use it to avoid duplicate work
	if len(nodeList) == 0 {
		clusterInfo, err := agent.utils.GetClusterInfo(connStr, base.DefaultPoolPath, username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, agent.logger)
		if err != nil {
			agent.logger.Infof("Remote cluster reference %v has a bad connectivity, didn't populate alternative connection strings. err=%v", agent.pendingRef.Id(), err)
			return err
		}

		if needToInitCapability {
			var newCapability metadata.Capability
			capabilitiesErr := newCapability.LoadFromDefaultPoolInfo(clusterInfo, agent.logger)
			if capabilitiesErr != nil {
				agent.logger.Errorf("Unable to initialize capabilities: %v - replications may not start yet. Will try again next refresh cycle...", err)
			} else {
				agent.SetCapability(newCapability)
			}
		}

		nodeList, err = agent.utils.GetNodeListFromInfoMap(clusterInfo, agent.logger)
		if err != nil {
			agent.logger.Infof("Unable to parse default/pool data to populate alternative connection strings. err=%v", err)
			agent.logger.Infof("nodes_connStrs from old cache =%v", agent.refNodesList)
			agent.refMtx.Lock()
			agent.pendingRefNodes = base.DeepCopyStringPairList(refNodesList)
			agent.refMtx.Unlock()
			return err
		}
	}

	useExternal, err := agent.updatePendingAddress(nodeList, rctx)
	if err != nil {
		return err
	}

	// Prepare pending refNodeList for committing
	nodeAddressesList, err := agent.utils.GetRemoteNodeAddressesListFromNodeList(nodeList, connStr, encryptionEnabled, agent.logger, useExternal)
	agent.refMtx.Lock()
	if err != nil {
		agent.logger.Warnf("Error getting nodes from target cluster. skipping alternative node computation. ref=%v\n", agent.pendingRef.HostName())
		agent.pendingRefNodes = base.DeepCopyStringPairList(agent.refNodesList)
	} else {
		agent.pendingRefNodes = base.DeepCopyStringPairList(nodeAddressesList)
	}
	agent.refMtx.Unlock()

	return nil
}

func (agent *RemoteClusterAgent) getCredentialsAndMisc() (username, password string, httpAuthMech base.HttpAuthMech,
	certificate []byte, sanInCertificate bool, clientCertificate []byte, clientKey []byte,
	connStr string, curRefNodesList base.StringPairList, encryptionEnabled bool, err error) {
	agent.refMtx.RLock()
	defer agent.refMtx.RUnlock()
	if agent.pendingRef.IsEmpty() {
		err = fmt.Errorf("Agent pendingRef is empty when doing sync to %v", agent.reference.Id())
		return
	}

	agent.pendingRef.PopulateDnsSrvIfNeeded(true)

	username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, err = agent.pendingRef.MyCredentials()
	if err != nil {
		return
	}

	// misc...
	connStr, err = agent.pendingRef.MyConnectionStr()
	if err != nil {
		return
	}
	curRefNodesList = base.DeepCopyStringPairList(agent.refNodesList)
	encryptionEnabled = agent.pendingRef.IsEncryptionEnabled()
	return
}

func (agent *RemoteClusterAgent) updatePendingAddress(nodeList []interface{}, rctx *refreshContext) (useExternal bool, err error) {
	agent.refMtx.RLock()
	agentPref := agent.addressPreference
	pendingHostName := agent.pendingRef.HostName()
	pendingRefIsHttps := agent.pendingRef.IsHttps()
	hostnameMode := agent.pendingRef.HostnameMode()
	skipPrefUpdate := agent.shouldSkipAddressPrefNoLock(rctx)
	agent.refMtx.RUnlock()

	useExternal = agentPref == External
	if agentPref == Uninitialized {
		err = agent.initAddressPreference(nodeList, pendingHostName, pendingRefIsHttps, hostnameMode)
		if err != nil {
			return
		}
		useExternal = agent.getAddressPreference() == External
	} else if !skipPrefUpdate {
		var isExternal bool
		isExternal, err = agent.checkIfHostnameIsAlternate(nodeList, agent.pendingRef.HostName(), agent.pendingRef.IsHttps())
		if err != nil {
			return
		}
		if !isExternal && useExternal || isExternal && !useExternal {
			// User intent has changed
			agent.refMtx.Lock()
			if agent.pendingAddressPreference == Uninitialized {
				// first time
				if isExternal {
					agent.pendingAddressPreference = External
				} else {
					agent.pendingAddressPreference = Internal
				}
			}
			agent.pendingAddressPrefCnt++
			agent.refMtx.Unlock()
		} else {
			var upgraded bool
			agent.refMtx.RLock()
			if agent.pendingAddressPrefCnt > 0 {
				agent.refMtx.RUnlock()
				agent.refMtx.Lock()
				upgraded = true
				// Just re-check to be safe
				if agent.pendingAddressPrefCnt > 0 {
					// Either mixed-mode or flip flop
					agent.pendingAddressPreference = Uninitialized
					agent.pendingAddressPrefCnt = 0
				}
				agent.refMtx.Unlock()
			}
			if !upgraded {
				agent.refMtx.RUnlock()
			}
		}
	}
	return
}

// This function checks the user's "intent" to use internal vs external address
// This means that the node user specified when declaring remote cluster
// should be the node of intent for checking. In situations where users misconfigure
// the cluster (i.e. missing setting up alt address/port for certain nodes in the cluster)
// as long as the specified node still belongs in the cluster, we can use that node's alternate
// address setup as the source for the user's intent
func (agent *RemoteClusterAgent) initAddressPreference(nodeList []interface{}, hostname string, isHttps bool, hostnameMode string) error {
	isExternal := hostnameMode == metadata.HostnameMode_External
	var err error

	if hostnameMode == metadata.HostnameMode_None {
		isExternal, err = agent.checkIfHostnameIsAlternate(nodeList, hostname, isHttps)
		if err != nil {
			return err
		}
	}

	agent.setAddressPreference(isExternal, true /*lock*/, true /*setOnlyIfUninit*/)
	return nil
}

func (agent *RemoteClusterAgent) getAddressPreference() AddressType {
	agent.refMtx.RLock()
	defer agent.refMtx.RUnlock()

	return agent.addressPreference
}

func (agent *RemoteClusterAgent) setAddressPreference(external bool, lock bool, setOnlyIfUninitialized bool) {
	if lock {
		agent.refMtx.Lock()
		defer agent.refMtx.Unlock()
	}

	if setOnlyIfUninitialized {
		if agent.addressPreference == Uninitialized {
			agent.addressPreference.Set(external)
		} else {
			return
		}
	} else {
		if agent.addressPreference != Uninitialized {
			agentIsExternal := agent.addressPreference == External
			if agentIsExternal && !external || !agentIsExternal && external {
				agent.configurationChanged = true
			}
		}
		agent.addressPreference.Set(external)
	}
}

func (agent *RemoteClusterAgent) checkIfHostnameIsAlternate(nodeList []interface{}, hostname string, isHttps bool) (bool, error) {
	var isExternal bool

	if len(hostname) == 0 {
		return false, HostNameEmpty
	}

	pendingHostname := base.GetHostName(hostname)
	pendingPort, portErr := base.GetPortNumber(hostname)

	for _, node := range nodeList {
		nodeInfoMap, ok := node.(map[string]interface{})
		if !ok {
			return isExternal, fmt.Errorf("node info is not of map type. type of node info=%v", reflect.TypeOf(node))
		}

		extHost, extPort, extErr := agent.utils.GetExternalMgtHostAndPort(nodeInfoMap, isHttps)

		var matched bool

		switch extErr {
		case base.ErrorResourceDoesNotExist:
			// No alternate address section for this node
			continue

		case base.ErrorNoPortNumber:
			// Alternate address did not provide port number
			switch portErr {
			case base.ErrorNoPortNumber:
				// User did not enter port number
				// This means that the external hostname sitting on default port is
				// the user's intention, since user did not specify a non-default port
				if extHost == pendingHostname {
					matched = true
				}
			case nil:
				// Since alternate address did not provide mgmt port number, and user provided
				// port number, it means that the user did not intend to use external address
				// Special case - if user specified 8091, then it is the same as didn't specifying
				if pendingPort == base.DefaultAdminPort {
					matched = true
				}
			default:
				return isExternal, fmt.Errorf("Unable to parse pendingRef's hostname port number")
			}

		case nil:
			// Alternate address provided port number
			// Now, check user entry:
			switch portErr {
			case base.ErrorNoPortNumber:
				// User did not enter port number
				// User's intention is to use the original default internal port
				if extHost == pendingHostname && extPort == int(base.DefaultAdminPort) {
					// Only allow this if the alternate port is also the default admin port
					matched = true
				}
			case nil:
				// User entered port number
				// User's intention to use external iff both match
				if extHost == pendingHostname && extPort == int(pendingPort) {
					matched = true
				}
			default:
				return isExternal, fmt.Errorf("Unable to parse pendingRef's hostname port number")
			}

		default:
			return isExternal, fmt.Errorf("Unable to parse GetExternalMgtHostAndPort")
		}

		if matched {
			isExternal = true
			break
		}
	}

	return isExternal, nil
}

func (agent *RemoteClusterAgent) UsesAlternateAddress() (bool, error) {
	agentPref := agent.getAddressPreference()
	if agentPref == Uninitialized {
		return false, base.ErrorRemoteClusterUninit
	} else {
		usesAlt := agentPref == External
		return usesAlt, nil
	}
}

func (agent *RemoteClusterAgent) GetCapability() (capability metadata.Capability, err error) {
	agent.refMtx.RLock()
	defer agent.refMtx.RUnlock()
	if agent.currentCapability == nil {
		err = fmt.Errorf("Remote cluster %v has not initialized capability yet", agent.reference.Name())
		return
	}

	capability = agent.currentCapability.Clone()
	return
}

func (agent *RemoteClusterAgent) SetCapability(capability metadata.Capability) {
	agent.refMtx.Lock()
	defer agent.refMtx.Unlock()

	if agent.currentCapability == nil {
		agent.currentCapability = &capability
	} else {
		agent.currentCapability.LoadFromOther(capability)
	}
}

func (agent *RemoteClusterAgent) runPeriodicRefresh() {
	defer agent.agentWaitGrp.Done()

	agent.refMtx.RLock()
	cachedId := agent.reference.Id()
	agent.refMtx.RUnlock()

	ticker := time.NewTicker(base.RefreshRemoteClusterRefInterval)
	defer ticker.Stop()

	for {
		select {
		case <-agent.agentFinCh:
			agent.logger.Infof("Agent %v is stopped", cachedId)
			return
		case <-ticker.C:
			err := agent.Refresh()
			if err != nil {
				agent.logger.Warnf("Agent %v periodic refresher encountered error while doing a refresh: %v", cachedId, err.Error())
			}
		}
	}
}

// Prepare a staging area to populate run-time data for the incoming reference
func (agent *RemoteClusterAgent) stageNewReferenceNoLock(newRef *metadata.RemoteClusterReference, userInitiated bool) error {
	if !agent.pendingRef.IsEmpty() {
		if !agent.InitDone() {
			return InitInProgress
		} else {
			return RemoteSyncInProgress
		}
	}

	agent.pendingRef.LoadFrom(newRef)
	agent.pendingRefNodes = make(base.StringPairList, 0)
	if !agent.reference.IsEmpty() {
		agent.pendingRef.SetId(agent.reference.Id())
		if userInitiated {
			agent.pendingRef.SetRevision(agent.reference.Revision())
		}
	}

	return nil
}

// operation to commit the staged changes into the reference
func (agent *RemoteClusterAgent) commitStagedChanges(rctx *refreshContext) {
	agent.refMtx.Lock()
	defer agent.refMtx.Unlock()

	if agent.deletedFromMetakv {
		// Bail
		return
	}

	if !agent.pendingRef.IsEmpty() {
		agent.oldRef = agent.reference.Clone()
		agent.reference.LoadFrom(&agent.pendingRef)
		agent.refNodesList = base.DeepCopyStringPairList(agent.pendingRefNodes)
		agent.connectivityHelper.SyncWithValidList(agent.refNodesList)
	}

	// Only set once to prevent constant locking
	if agent.pendingAddressPrefCnt == base.RemoteClusterAlternateAddrChangeCnt {
		agent.setAddressPreference(agent.pendingAddressPreference == External, false /*lock*/, false /*setOnlyIfUninit*/)
		agent.configurationChanged = true
	}

	// Update capability if it was successfully initialized and it has changed
	if agent.currentCapability != nil && rctx != nil && rctx.capabilityChanged() {
		agent.currentCapability.LoadFromOther(rctx.cachedCapability)
		agent.configurationChanged = true
	}
}

func (agent *RemoteClusterAgent) clearStagedReference() {
	agent.refMtx.Lock()
	defer agent.refMtx.Unlock()
	agent.pendingRef.Clear()
}

func (agent *RemoteClusterAgent) IsSame(ref *metadata.RemoteClusterReference) bool {
	agent.refMtx.RLock()
	defer agent.refMtx.RUnlock()
	return agent.reference.IsSame(ref)
}

func (agent *RemoteClusterAgent) clearReference() {
	agent.refMtx.Lock()
	defer agent.refMtx.Unlock()
	agent.oldRef = agent.reference.Clone()
	agent.reference.Clear()
	agent.refNodesList = nil
	agent.deletedFromMetakv = true
}

// Retrieves the ref from metakv to be able to get the latest revision, stores into pendingRef
// Write lock must be held
// Returns non-nil if the reference in metakv is different from locally stored (less revision differences)
func (agent *RemoteClusterAgent) updateRevisionFromMetaKV() error {
	if len(agent.pendingRef.Id()) == 0 {
		return base.ErrorResourceDoesNotExist
	}

	if agent.unitTestBypassMetaKV {
		return nil
	}

	var value []byte
	var rev interface{}
	var err error = errors.New("NotNil")
	for i := 0; i < base.MaxRCSMetaKVOpsRetry && err != nil; i++ {
		value, rev, err = agent.metakvSvc.Get(agent.pendingRef.Id())
		if err != nil {
			time.Sleep(base.TimeBetweenMetaKVGetOps)
		}
	}

	agent.refMtx.Lock()
	defer agent.refMtx.Unlock()
	if agent.deletedFromMetakv {
		return DeleteAlreadyIssued
	}

	if err == nil {
		refInMetaKv, err := constructRemoteClusterReference(value, rev)
		if err != nil {
			return err
		}
		// Do a sanity check to make sure there has not been any other writer who updated this reference after we've written.
		if !agent.pendingRef.IsEssentiallySame(refInMetaKv) {
			// If someone did change from underneath, discard everything and wait until the metakv callback to handle
			return base.ErrorResourceDoesNotMatch
		}
		// Loads revision minus the ActiveHostName and ActiveHttpsHostName
		agent.pendingRef.LoadNonActivesFrom(refInMetaKv)
	} else {
		// Any type of error getting the revision means that we will have a nil revision in this ref
		// And we'll depend upon the metakv callback to set the revision correctly
		// Errors are ignorable once we have set Revision to nil
		agent.pendingRef.ClearRevision()
	}

	agent.logger.Infof("Updating remote cluster %v in cache after metadata store update. revision after update=%v\n", agent.pendingRef.Id(), agent.pendingRef.Revision())

	return nil
}

func (agent *RemoteClusterAgent) shouldSkipAddressPrefNoLock(rctx *refreshContext) bool {
	agent.refMtx.RLock()
	defer agent.refMtx.RUnlock()

	if agent.configurationChanged ||
		agent.reference.HostnameMode() == metadata.HostnameMode_External ||
		agent.reference.HostnameMode() == metadata.HostnameMode_Internal {
		// forced internal or external mode means always skip addr pref change
		return true
	}
	return false
}

/**
 * The agent will update its information from the incoming newRef.
 * If updateMetaKv is set to true, it'll write the information to metakv.
 *
 * This operation performs RPC call and can take a while, and can be a synchronous
 * or async call.
 * Sync callers will care about the result of the queueing and the RPC
 * Async callers will only care about the queueing, such as when the remote cluster service
 * starts up and it needs to quickly get going without blocking due to RPCs
 *
 * This function could potentially be called concurrently
 */
func (agent *RemoteClusterAgent) updateReferenceFromInternal(newRef *metadata.RemoteClusterReference, updateMetaKv, shouldCallCb, synchronous bool, rctx *refreshContext) error {
	var err error
	if newRef == nil {
		return base.ErrorResourceDoesNotExist
	}

	capabilityChanged := rctx != nil && rctx.capabilityChanged()
	agent.refMtx.RLock()
	//	No need to update if they are the same, or we're not transitioning, or capability is the same
	if agent.reference.IsSame(newRef) && agent.pendingAddressPrefCnt == 0 && !capabilityChanged {
		agent.refMtx.RUnlock()
		return nil
	}
	agent.refMtx.RUnlock()

	var needToReenable bool
	if rctx != nil {
		err = rctx.checkIfAbortRequested()
		if err != nil {
			return err
		}
	} else {
		// This call is one of the following non-Refresh calls:
		// 1. User calls AddRemoteCluster
		// 2. User calls SetRemoteCluster
		// 3. Metakv callback to create a new remote cluster
		// 4. Metakv callback to set remote cluster based on user induced action
		// All of the above must win over any potential Refresh()'s going on
		needToReenable = agent.AbortAnyOngoingRefresh()
	}

	agent.refMtx.Lock()

	if agent.deletedFromMetakv {
		agent.refMtx.Unlock()
		// No need to do the following
		if rctx == nil && needToReenable {
			agent.ReenableRefresh()
		}
		return DeleteAlreadyIssued
	}

	err = agent.stageNewReferenceNoLock(newRef, updateMetaKv)
	if err != nil {
		agent.refMtx.Unlock()
		// The stage call will filter out concurrent callers
		// Whoever goes through and staged the reference successfully will be responsible
		// for re-enabling refresh
		if rctx == nil && err == InitInProgress && needToReenable {
			agent.ReenableRefresh()
		}
		return err
	}
	agent.refMtx.Unlock()
	// At this point, safe to return and perform async task
	// If refresh is the one running here, no more path to bail out of aborting refresh
	// Also, async task is given the task to clean up the stagedReference above

	var errCh chan error
	if synchronous {
		errCh = make(chan error)
	}

	agent.agentWaitGrp.Add(1)
	go agent.executeBgSyncTask(updateMetaKv, shouldCallCb, synchronous, errCh, rctx)

	if synchronous {
		err = <-errCh
	}
	return err
}

// Only one instance at once allowed
// The sync task needs to be in charge of cleaning up the pendingRefs for the next Stage() call
func (agent *RemoteClusterAgent) executeBgSyncTask(updateMetaKv, shouldCallCb, synchronous bool, errCh chan error, rctx *refreshContext) {
	defer agent.agentWaitGrp.Done()

	select {
	case <-agent.agentFinCh:
		// Bail out
		if synchronous {
			errCh <- fmt.Errorf("Agent already stopped")
			close(errCh)
		}
		return
	default:
		defer agent.clearStagedReference()
		if rctx == nil {
			defer agent.ReenableRefresh()
		}
		// Populate staged runtime information from the staged metadata information.
		syncErr := agent.syncInternalsFromStagedReference(rctx)

		if syncErr != nil {
			if agent.getAddressPreference() != Uninitialized {
				// Because as part of validateRemoteCluster, we already checked the remoteCluster status
				// At this point, this error should be innocuous to pass through.
				agent.logger.Warnf(fmt.Sprintf("Error: Issues populating runtime info: %v", syncErr.Error()))
			} else {
				// Cannot figure out user intent - let refresh take care of error handling
				agent.logger.Errorf(fmt.Sprintf("Error: Issues populating runtime info: %v and unable to set preference", syncErr.Error()))
			}
		}

		/**
		 * Update procedure:
		 * First write the pending changes to metaKV. Once they are persisted, then commit the staged changes
		 * permanently by loading it to agent.reference.
		 * If unable to successfully operate on metakv, then discard the staged changes.
		 */
		var err error
		if updateMetaKv {
			err = agent.writeToMetaKV()
			if err == nil {
				// After writing, try to get the revision
				err = agent.updateRevisionFromMetaKV()
			} else {
				err = fmt.Errorf("%v: %v", WriteToMetakvErrString, err)
			}
		}

		if err == nil {
			agent.commitStagedChanges(rctx)
			if shouldCallCb {
				agent.callMetadataChangeCb()
			}
		}
		if synchronous {
			errCh <- err
			close(errCh)
		} else {
			// Not sending err means that this was called asynchronously
			// An asynchronous call is possible when the service starts up and the reference
			// is loaded from the metakv. In this case, enable refresh path
			atomic.StoreUint32(&agent.initDone, 1)
		}
		return
	}
}

func (agent *RemoteClusterAgent) UpdateReferenceFrom(newRef *metadata.RemoteClusterReference, updateMetaKv bool) error {
	return agent.updateReferenceFromInternal(newRef, updateMetaKv, true /*shouldCallCallback*/, true /*synchronous*/, nil /*refreshContext*/)
}

func (agent *RemoteClusterAgent) UpdateReferenceFromAsync(newRef *metadata.RemoteClusterReference, updateMetaKv bool) error {
	return agent.updateReferenceFromInternal(newRef, updateMetaKv, true /*shouldCallCallback*/, false /*synchronous*/, nil /*refreshContext*/)
}

func (agent *RemoteClusterAgent) callMetadataChangeCb() {
	var id string

	agent.refMtx.RLock()
	if agent.reference.IsEmpty() && agent.oldRef != nil {
		id = agent.oldRef.Id()
	} else {
		id = agent.reference.Id()
	}
	oldRef := agent.oldRef.Clone()
	ref := agent.reference.Clone()
	agent.refMtx.RUnlock()

	agent.metadataChangeCbMtx.RLock()
	defer agent.metadataChangeCbMtx.RUnlock()
	if agent.metadataChangeCallback != nil {
		callbackErr := agent.metadataChangeCallback(id, oldRef, ref)
		if callbackErr != nil {
			agent.logger.Error(callbackErr.Error())
		}
	}
}

func (agent *RemoteClusterAgent) deleteFromMetaKV() error {
	agent.refMtx.Lock()
	agent.deletedFromMetakv = true
	referenceId := agent.reference.Id()
	referenceName := agent.reference.Name()
	agent.refMtx.Unlock()
	// Delete should always succeed in a reference's life cycle, use nil revision
	return agent.deleteFromMetaKVNoLock(referenceId, referenceName, nil)
}

// Delete the reference information from metakv
func (agent *RemoteClusterAgent) deleteFromMetaKVNoLock(id, name string, revision interface{}) error {
	err := agent.metakvSvc.DelWithCatalog(RemoteClustersCatalogKey, id, revision)
	if err != nil {
		agent.logger.Errorf(fmt.Sprintf("Error occured when deleting reference %v from metakv: %v\n", name, err.Error()))
	} else {
		agent.logger.Infof("Remote cluster %v deleted from metadata store\n", name)
	}
	return err
}

/**
 * Writes the staged reference to metakv.
 * There are 2 types of writes: Add and Set.
 * Add is used when it is the first time this agent is writing to metakv to create a new kv.
 * Otherwise, Set should be used to update the existing kv.
 */
func (agent *RemoteClusterAgent) writeToMetaKV() error {
	var err error

	agent.refMtx.RLock()
	defer agent.refMtx.RUnlock()

	refForMetaKv := agent.pendingRef.CloneForMetakvUpdate()
	referenceIsEmpty := agent.reference.IsEmpty()
	revision := agent.pendingRef.Revision()

	if agent.deletedFromMetakv {
		return base.ErrorResourceDoesNotExist
	}

	key := refForMetaKv.Id()
	value, err := refForMetaKv.Marshal()
	if err != nil {
		return err
	}

	if referenceIsEmpty {
		err = agent.metakvSvc.AddSensitiveWithCatalog(RemoteClustersCatalogKey, key, value)
	} else {
		err = agent.metakvSvc.SetSensitive(key, value, revision)
	}

	return err
}

func (agent *RemoteClusterAgent) setMetadataChangeCb(newCb base.MetadataChangeHandlerCallback) {
	agent.metadataChangeCbMtx.Lock()
	defer agent.metadataChangeCbMtx.Unlock()
	agent.metadataChangeCallback = newCb
}

func (agent *RemoteClusterAgent) RegisterBucketRequest(bucketName string) error {
	agent.bucketMtx.Lock()
	defer agent.bucketMtx.Unlock()

	manifestGetter, ok := agent.bucketManifestGetters[bucketName]
	if !ok {
		// Use TopologyChangeCheckInterval as min interval between pulls, while agent refreshes at a longer interval
		manifestGetter = NewBucketManifestGetter(bucketName, agent, time.Duration(base.ManifestRefreshTgtInterval)*time.Second)
		agent.bucketManifestGetters[bucketName] = manifestGetter
	}

	_, ok = agent.bucketRefCnt[bucketName]
	if !ok {
		agent.bucketRefCnt[bucketName] = uint32(0)
	}
	agent.bucketRefCnt[bucketName]++

	return nil
}

func (agent *RemoteClusterAgent) UnRegisterBucketRefresh(bucketName string) error {
	agent.bucketMtx.Lock()
	defer agent.bucketMtx.Unlock()

	_, ok := agent.bucketRefCnt[bucketName]
	if !ok {
		return base.ErrorInvalidInput
	}

	if agent.bucketRefCnt[bucketName] > uint32(0) {
		agent.bucketRefCnt[bucketName]--
	}

	if agent.bucketRefCnt[bucketName] == uint32(0) {
		delete(agent.bucketManifestGetters, bucketName)
	}
	return nil
}

// Implements CollectionsManifestOps interface
func (agent *RemoteClusterAgent) CollectionManifestGetter(bucketName string) (*metadata.CollectionsManifest, error) {
	agent.refMtx.RLock()
	connStr, err := agent.reference.MyConnectionStr()
	if err != nil {
		agent.refMtx.RUnlock()
		agent.logger.Errorf("Unable to get connectionStr with err: %v\n", err)
		return nil, err
	}
	username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate,
		clientKey, err := agent.reference.MyCredentials()
	agent.refMtx.RUnlock()

	if err != nil {
		agent.logger.Errorf("Unable to get credentials with err: %v\n", err)
		return nil, err
	}

	return agent.utils.GetCollectionsManifest(connStr, bucketName, username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, agent.logger)
}

// refreshIfPossible to prevent overwhelming target outside of refresh interval
func (agent *RemoteClusterAgent) GetManifest(bucketName string, refreshIfPossible bool) *metadata.CollectionsManifest {
	agent.bucketMtx.RLock()
	getter, ok := agent.bucketManifestGetters[bucketName]
	if !ok {
		agent.logger.Warnf("Unable to find manifest getter for bucket %v", bucketName)
		agent.bucketMtx.RUnlock()
		return nil
	}
	agent.bucketMtx.RUnlock()

	return getter.GetManifest()
}

func (agent *RemoteClusterAgent) refreshBucketsManifests() {
	var waitGrp sync.WaitGroup
	agent.bucketMtx.RLock()
	defer agent.bucketMtx.RUnlock()

	for _, getter := range agent.bucketManifestGetters {
		waitGrp.Add(1)
		refreshFunc := func() {
			getter.GetManifest()
			waitGrp.Done()
		}
		go refreshFunc()
	}

	waitGrp.Wait()
}

func (agent *RemoteClusterAgent) InitDone() bool {
	return atomic.LoadUint32(&agent.initDone) > 0
}

type RemoteClusterService struct {
	metakv_svc        service_def.MetadataSvc
	uilog_svc         service_def.UILogSvc
	xdcr_topology_svc service_def.XDCRCompTopologySvc
	cluster_info_svc  service_def.ClusterInfoSvc
	logger            *log.CommonLogger
	// key = hostname; value = https address of hostname
	httpsAddrMap             map[string]string
	httpsAddrMap_lock        sync.Mutex
	metadata_change_callback base.MetadataChangeHandlerCallback
	utils                    utilities.UtilsIface
	// agent related members
	// a hashmap with key == refId. Rest are dynamically populated for O(1) lookups
	agentMap             map[string]*RemoteClusterAgent
	agentCacheRefNameMap map[string]*RemoteClusterAgent
	agentCacheUuidMap    map[string]*RemoteClusterAgent
	agentMutex           sync.RWMutex

	// When adding or setting a remote cluster reference, the metakv callback will be called even though
	// the particular active node already has add or set ongoing
	// The followings are needed to ensure add/set on the active-node do not get double-called
	metakvCbAddMtx sync.RWMutex
	metakvCbAddMap map[string]bool // refId
	metakvCbSetMtx sync.RWMutex
	metakvCbSetMap map[string]bool // refId
	metakvCbDelMtx sync.RWMutex
	metakvCbDelMap map[string]bool // refId
}

func NewRemoteClusterService(uilog_svc service_def.UILogSvc, metakv_svc service_def.MetadataSvc,
	xdcr_topology_svc service_def.XDCRCompTopologySvc, cluster_info_svc service_def.ClusterInfoSvc,
	logger_ctx *log.LoggerContext, utilsIn utilities.UtilsIface) (*RemoteClusterService, error) {
	logger := log.NewLogger("RemClusterSvc", logger_ctx)
	svc := &RemoteClusterService{
		metakv_svc:           metakv_svc,
		uilog_svc:            uilog_svc,
		xdcr_topology_svc:    xdcr_topology_svc,
		cluster_info_svc:     cluster_info_svc,
		logger:               logger,
		httpsAddrMap:         make(map[string]string),
		utils:                utilsIn,
		agentMap:             make(map[string]*RemoteClusterAgent),
		agentCacheRefNameMap: make(map[string]*RemoteClusterAgent),
		agentCacheUuidMap:    make(map[string]*RemoteClusterAgent),
		metakvCbAddMap:       make(map[string]bool),
		metakvCbSetMap:       make(map[string]bool),
		metakvCbDelMap:       make(map[string]bool),
	}

	return svc, svc.loadFromMetaKV()
}

func (service *RemoteClusterService) loadFromMetaKV() error {
	var KVsFromMetaKV []*service_def.MetadataEntry
	var KVsFromMetaKVErr error

	getAllKVsOpFunc := func() error {
		KVsFromMetaKV, KVsFromMetaKVErr = service.metakv_svc.GetAllMetadataFromCatalog(RemoteClustersCatalogKey)
		return KVsFromMetaKVErr
	}
	err := service.utils.ExponentialBackoffExecutor("GetAllMetadataFromCatalogRemoteCluster", base.RetryIntervalMetakv,
		base.MaxNumOfMetakvRetries, base.MetaKvBackoffFactor, getAllKVsOpFunc)
	if err != nil {
		service.logger.Errorf("Unable to get all the KVs from metakv: %v", err)
		return err
	}

	var ref *metadata.RemoteClusterReference
	for _, KVentry := range KVsFromMetaKV {
		ref, err = constructRemoteClusterReference(KVentry.Value, KVentry.Rev)

		if err != nil {
			service.logger.Errorf("Unable to construct remote cluster %v from metaKV's data. err: %v. value: %v\n", KVentry.Key, base.TagUDBytes(KVentry.Value), err)
			continue
		}
		_, _, err = service.getOrStartNewAgent(ref, false /*user initiated*/, true /*updateFromRef*/)
		if err != nil {
			service.logger.Errorf("Failed to start new agent for remote cluster %v. err: %v\n", KVentry.Key, err)
			continue
		}
	}
	return nil
}

func (service *RemoteClusterService) SetMetadataChangeHandlerCallback(call_back base.MetadataChangeHandlerCallback) {
	service.metadata_change_callback = call_back
	// Need to update all the agents' callbacks as well
	service.agentMutex.RLock()
	defer service.agentMutex.RUnlock()
	for _, agent := range service.agentMap {
		agent.setMetadataChangeCb(service.metadata_change_callback)
	}
}

func getRefreshErrorMsg(customStr string, err error) string {
	return fmt.Sprintf("Error occured while doing refresh during getting remote cluster reference for %v: %v\n", customStr, err.Error())
}

func getBootStrapNodeHasMovedErrorMsg(reference string) string {
	return fmt.Sprintf("Error: The bootstrap node listed in the reference: %v is not valid as it has been moved to a different cluster than the original target cluster.",
		reference)
}

func getUnknownCluster(customType string, customStr string) error {
	return errors.New(fmt.Sprintf("%v : %v - %v", UnknownRemoteClusterErrorMessage, customType, customStr))
}

func (service *RemoteClusterService) RemoteClusterByRefId(refId string, refresh bool) (*metadata.RemoteClusterReference, error) {
	service.agentMutex.RLock()
	agent := service.agentMap[refId]
	if agent == nil {
		service.agentMutex.RUnlock()
		return nil, getUnknownCluster("refId", refId)
	}
	service.agentMutex.RUnlock()

	if refresh {
		err := agent.Refresh()
		if err != nil {
			if err == BootStrapNodeHasMovedError {
				service.logger.Errorf(getBootStrapNodeHasMovedErrorMsg(refId))
				return nil, errors.New(getBootStrapNodeHasMovedErrorMsg(refId))
			} else if IsRefreshError(err) {
				return nil, RefreshNotEnabledYet
			} else {
				service.logger.Warnf(getRefreshErrorMsg(refId, err))
			}
		}
	}

	return agent.GetReferenceClone(), nil
}
func (service *RemoteClusterService) RemoteClusterByRefName(refName string, refresh bool) (*metadata.RemoteClusterReference, error) {
	ref, _, err := service.remoteClusterByRefNameWithAgent(refName, refresh)
	return ref, err
}

func (service *RemoteClusterService) remoteClusterByRefNameWithAgent(refName string, refresh bool) (*metadata.RemoteClusterReference, *RemoteClusterAgent, error) {
	service.agentMutex.RLock()
	agent := service.agentCacheRefNameMap[refName]
	if agent == nil {
		service.agentMutex.RUnlock()
		return nil, nil, getUnknownCluster("refName", refName)
	}
	service.agentMutex.RUnlock()

	if refresh {
		err := agent.Refresh()
		if err != nil {
			if err == BootStrapNodeHasMovedError {
				service.logger.Errorf(getBootStrapNodeHasMovedErrorMsg(refName))
				return nil, agent, errors.New(getBootStrapNodeHasMovedErrorMsg(refName))
			} else if IsRefreshError(err) {
				return nil, agent, RefreshNotEnabledYet
			} else {
				service.logger.Warnf(getRefreshErrorMsg(refName, err))
			}
		}
	}

	return agent.GetReferenceClone(), agent, nil
}

func (service *RemoteClusterService) RemoteClusterByUuid(uuid string, refresh bool) (*metadata.RemoteClusterReference, error) {
	service.agentMutex.RLock()
	agent := service.agentCacheUuidMap[uuid]
	if agent == nil {
		service.agentMutex.RUnlock()
		return nil, getUnknownCluster("uuid", uuid)
	}
	service.agentMutex.RUnlock()

	if refresh {
		err := agent.Refresh()
		if err != nil {
			if err == BootStrapNodeHasMovedError {
				service.logger.Errorf(getBootStrapNodeHasMovedErrorMsg(uuid))
				return nil, errors.New(getBootStrapNodeHasMovedErrorMsg(uuid))
			} else if IsRefreshError(err) {
				return nil, RefreshNotEnabledYet
			} else {
				service.logger.Warnf(getRefreshErrorMsg(uuid, err))
			}
		}
	}

	return agent.GetReferenceClone(), nil
}

func (service *RemoteClusterService) AddRemoteCluster(ref *metadata.RemoteClusterReference, skipConnectivityValidation bool) error {
	service.logger.Infof("Adding remote cluster with referenceId %v\n", ref.Id())
	err := service.validateAddRemoteCluster(ref, skipConnectivityValidation)
	if err != nil {
		return err
	}

	err = service.addRemoteCluster(ref)
	if err != nil {
		return err
	}

	if service.uilog_svc != nil {
		uiLogMsg := fmt.Sprintf("Created remote cluster reference \"%s\" via %s.", ref.Name(), ref.HostName())
		service.uilog_svc.Write(uiLogMsg)
	}
	return nil
}

func (service *RemoteClusterService) SetRemoteCluster(refName string, ref *metadata.RemoteClusterReference) error {
	return service.setRemoteCluster(refName, ref)
}

// Set ops are synchronous
func (service *RemoteClusterService) setRemoteCluster(refName string, newRef *metadata.RemoteClusterReference) error {
	service.logger.Infof("Setting remote cluster with refName %v. ref=%v\n", refName, newRef)

	agent, err := service.validateSetRemoteClusterWithAgent(refName, newRef)
	if err != nil {
		return err
	}

	if agent == nil {
		return errors.New(fmt.Sprintf("Error: refName %v not found in the cluster service\n", refName))
	} else {
		// In case things change and need to update maps
		oldRef := agent.reference.Clone()

		service.registerSet(newRef.Name())
		err := agent.UpdateReferenceFrom(newRef, true)

		if err == nil {
			service.checkAndUpdateAgentMaps(oldRef, newRef, agent)

			if service.uilog_svc != nil {
				var hostnameChangeMsg string
				newRefHostName := newRef.HostName()
				if oldRef.HostName() != newRefHostName {
					hostnameChangeMsg = fmt.Sprintf(" New contact point is %s.", newRefHostName)
				}
				uiLogMsg := fmt.Sprintf("Remote cluster reference \"%s\" updated.%s", oldRef.Name(), hostnameChangeMsg)
				service.uilog_svc.Write(uiLogMsg)
			}
		} else {
			service.deregisterSet(newRef.Name())
		}
		return err
	}
}

// The entry point for REST iface for when an user wants to delete a remote cluster reference
func (service *RemoteClusterService) DelRemoteCluster(refName string) (*metadata.RemoteClusterReference, error) {
	if len(refName) == 0 {
		return nil, errors.New("No refName is given")
	}
	service.logger.Infof("Deleting remote cluster with reference name=%v\n", refName)

	ref, err := service.delRemoteClusterAgent(refName, true)
	if err != nil {
		if !service_def.DelOpConsideredPass(err) {
			service.deregisterDel(ref.Id())
		}
		return nil, err
	}

	if service.uilog_svc != nil {
		uiLogMsg := fmt.Sprintf("Remote cluster reference \"%s\" known via %s removed.", ref.Name(), ref.HostName())
		service.uilog_svc.Write(uiLogMsg)
	}
	return ref, nil
}

func (service *RemoteClusterService) RemoteClusters() (map[string]*metadata.RemoteClusterReference, error) {
	service.logger.Debugf("Getting remote clusters references")

	remoteClusterReferencesMap := make(map[string]*metadata.RemoteClusterReference)

	service.agentMutex.RLock()
	defer service.agentMutex.RUnlock()
	for refId, agent := range service.agentMap {
		// Used by external REST call, so get the status to be returned
		remoteClusterReferencesMap[refId] = agent.GetReferenceAndStatusClone()
	}

	return remoteClusterReferencesMap, nil
}

// validate that the remote cluster ref itself is valid, and that it does not collide with any of the existing remote clusters.
func (service *RemoteClusterService) ValidateAddRemoteCluster(ref *metadata.RemoteClusterReference) error {
	return service.validateAddRemoteCluster(ref, false)
}

func (service *RemoteClusterService) validateAddRemoteCluster(ref *metadata.RemoteClusterReference, skipConnectivityValidation bool) error {
	oldRef, _ := service.RemoteClusterByRefName(ref.Name(), false)

	if oldRef != nil {
		return wrapAsInvalidRemoteClusterOperationError("Duplicate cluster names are not allowed")
	}

	// skip connectivity validation if so specified, e.g., when called from migration service
	if !skipConnectivityValidation {
		err := service.validateRemoteCluster(ref, true)
		if err != nil {
			return err
		}
	}

	refUuid := ref.Uuid()
	if refUuid != "" {
		oldRef, _ = service.RemoteClusterByUuid(refUuid, false)
		if oldRef != nil {
			return wrapAsInvalidRemoteClusterOperationError(fmt.Sprintf("Cluster reference to the same cluster already exists under the name `%v`", oldRef.Name()))
		}
	}

	return nil
}
func (service *RemoteClusterService) ValidateSetRemoteCluster(refName string, ref *metadata.RemoteClusterReference) error {
	_, err := service.validateSetRemoteClusterWithAgent(refName, ref)
	return err
}

func (service *RemoteClusterService) validateSetRemoteClusterWithAgent(refName string, ref *metadata.RemoteClusterReference) (*RemoteClusterAgent, error) {
	oldRef, agent, err := service.remoteClusterByRefNameWithAgent(refName, false)
	if err != nil {
		return agent, err
	}

	if !agent.InitDone() {
		// If a set is called before the first RPC call has finished, then do not allow the set to continue
		return agent, SetDisabledUntilInit
	}

	err = service.validateRemoteCluster(ref, true)
	if err != nil {
		return agent, err
	}

	if oldRef.Uuid() != ref.Uuid() {
		return agent, wrapAsInvalidRemoteClusterOperationError(fmt.Sprintf("The new hostname points to a different remote cluster %v, which is not allowed with old cluster being %v.", ref.Uuid(), oldRef.Uuid()))
	}

	return agent, nil
}

// validate remote cluster info
func (service *RemoteClusterService) ValidateRemoteCluster(ref *metadata.RemoteClusterReference) error {
	// do not update ref when we are merely validating existing remote cluster ref
	return service.validateRemoteCluster(ref, false /*updateRef*/)
}

// validate remote cluster info
// when updateRef is true, update internal fields in ref such as ActiveHostName
// this is the case when ref is being created or updated by user
func (service *RemoteClusterService) validateRemoteCluster(ref *metadata.RemoteClusterReference, updateRef bool) error {
	if ref.IsEncryptionEnabled() {
		isEnterprise, err := service.xdcr_topology_svc.IsMyClusterEnterprise()
		if err != nil {
			return err
		}

		if !isEnterprise {
			return wrapAsInvalidRemoteClusterError("Encryption can only be used in enterprise edition when the entire cluster is running at least 2.5 version of Couchbase Server")
		}

		err = service.validateCertificates(ref)
		if err != nil {
			return wrapAsInvalidRemoteClusterError(err.Error())
		}
	}

	ref.PopulateDnsSrvIfNeeded(false)

	refHostName, _ := ref.MyConnectionStr()
	hostName := base.GetHostName(refHostName)
	port, err := base.GetPortNumber(refHostName)
	if err != nil {
		return wrapAsInvalidRemoteClusterError(fmt.Sprintf("Failed to resolve address for \"%v\". The hostname may be incorrect or not resolvable.", refHostName))
	}

	if updateRef {
		err = service.setHostNamesAndSecuritySettings(ref)
		if err != nil {
			return wrapAsInvalidRemoteClusterError(err.Error())
		}
	}

	startTime := time.Now()

	hostAddr, err := ref.MyConnectionStr()
	if err != nil {
		return err
	}
	clusterInfo, err, statusCode := service.utils.GetClusterInfoWStatusCode(hostAddr, base.PoolsPath, ref.UserName(), ref.Password(), ref.HttpAuthMech(), ref.Certificate(), ref.SANInCertificate(), ref.ClientCertificate(), ref.ClientKey(), service.logger)
	service.logger.Infof("Result from validate remote cluster call: err=%v, statusCode=%v. time taken=%v\n", err, statusCode, time.Since(startTime))
	if err != nil || statusCode != http.StatusOK {
		if statusCode == http.StatusUnauthorized {
			return wrapAsInvalidRemoteClusterError(fmt.Sprintf("Authentication failed. Verify username and password. Got HTTP status %v from REST call get to %v%v. Body was: []", statusCode, hostAddr, base.PoolsPath))
		} else {
			if err == nil {
				err = fmt.Errorf("Received non-OK HTTP status %v from %v%v", statusCode, hostAddr, base.PoolsPath)
			}
			return service.formErrorFromValidatingRemotehost(ref, hostName, port, err)
		}
	}

	// check if remote cluster has been initialized, i.e., has non-empty pools
	pools, ok := clusterInfo[base.Pools].([]interface{})
	if !ok {
		return wrapAsInvalidRemoteClusterError("Could not get cluster info from remote cluster. Remote cluster may be invalid.")
	}
	if len(pools) == 0 {
		return wrapAsInvalidRemoteClusterError("Remote node is not initialized.")
	}

	if ref.IsEncryptionEnabled() {
		// check if target cluster supports SSL when SSL is specified

		//get isEnterprise from the map
		isEnterprise_remote, ok := clusterInfo[base.IsEnterprise].(bool)
		if !ok {
			isEnterprise_remote = false
		}

		if !isEnterprise_remote {
			return wrapAsInvalidRemoteClusterError("Remote cluster is not enterprise version and does not support SSL.")
		}

		// if ref is half secured, validate that target clusters is spock and up
		if ref.IsHalfEncryption() {
			rbacCompatible, err := service.cluster_info_svc.IsClusterCompatible(ref, base.VersionForRBACAndXattrSupport)
			if err != nil {
				return wrapAsInvalidRemoteClusterError("Failed to get target cluster version information")
			}
			if !rbacCompatible {
				return wrapAsInvalidRemoteClusterError("Remote cluster has a version lower than 5.0 and does not support half-SSL type remote cluster references.")
			}
		}
	}

	// get remote cluster uuid from the map
	if updateRef {
		actualUuid, ok := clusterInfo[base.RemoteClusterUuid]
		if !ok {
			// should never get here
			return wrapAsInvalidRemoteClusterError("Could not get uuid of remote cluster. Remote cluster may be invalid.")
		}
		actualUuidStr, ok := actualUuid.(string)
		if !ok {
			// should never get here
			service.logger.Errorf("Uuid of remote cluster is of wrong type. Expected type: string; Actual type: %s", reflect.TypeOf(actualUuid))
			return wrapAsInvalidRemoteClusterError("Could not get uuid of remote cluster. Remote cluster may be invalid.")
		}

		// update uuid in ref to real value
		ref.SetUuid(actualUuidStr)
	}

	return nil
}

func (service *RemoteClusterService) getUserIntentFromNodeList(ref *metadata.RemoteClusterReference, nodeList []interface{}) (useExternal bool, err error) {
	checkHostName := base.GetHostName(ref.HostName())
	checkPortNo, checkPortErr := base.GetPortNumber(ref.HostName())

	for _, node := range nodeList {
		nodeInfoMap, ok := node.(map[string]interface{})
		if !ok {
			err = fmt.Errorf("node info is not of map type")
			return
		}
		extHost, extPort, extErr := service.utils.GetExternalMgtHostAndPort(nodeInfoMap, ref.IsHttps())
		if ref.IsFullEncryption() {
			// Calling this from full-encryption means user may have entered a SSL port already
			if extErr == nil && checkPortErr == nil && checkHostName == extHost && int(checkPortNo) == extPort {
				// 1. alternateHostname:alternateSSLPort
				useExternal = true
				break
			}
			// It is possible that the user is asking for full encryption but contacting the nonSSL port:
			extHost2, extPort2, extErr2 := service.utils.GetExternalMgtHostAndPort(nodeInfoMap, false /*isHttps*/)
			if extErr2 == base.ErrorNoPortNumber && checkPortErr == base.ErrorNoPortNumber && checkHostName == extHost2 {
				// 2. alternateHostname (8091 is implied)
				useExternal = true
				break
			}
			if extErr2 == nil && checkPortErr == nil && checkHostName == extHost2 && int(checkPortNo) == extPort2 {
				// 3. alternateHostname:9000 (8091 non-SSL alternate equivalent)
				useExternal = true
				break
			}
		} else {
			// Calling this from half-encryption means user already entered non-encrypted port
			// Thus, as long as the user entered hostname matches an alternate hostname, then
			// user's intent is to use alternate address
			if (extErr == nil || extErr == base.ErrorNoPortNumber) && checkHostName == extHost {
				useExternal = true
				break
			}
		}
	}
	return
}

func (service *RemoteClusterService) setHostNamesAndSecuritySettings(ref *metadata.RemoteClusterReference) error {
	if !ref.IsEncryptionEnabled() {
		if ref.IsDnsSRV() {
			srvHosts := ref.GetSRVHostNames()
			if len(srvHosts) > 0 {
				ref.SetActiveHostName(srvHosts[0])
			} else {
				ref.SetActiveHostName(ref.HostName())
			}
		} else {
			ref.SetActiveHostName(ref.HostName())
		}
		// nothing more needs to be done if encryption is not enabled
		return nil
	}

	refHostName := ref.HostName()
	refHttpsHostName := ref.HttpsHostName()
	if ref.IsDnsSRV() {
		// We will overwrite both with DNS SRV entries
		// When user set up DNS SRV with encryption, they should have coded the correct port
		srvHosts := ref.GetSRVHostNames()
		if len(srvHosts) > 0 {
			refHostName = srvHosts[0]
		}
	}
	var err error

	if refHttpsHostName == "" {
		if !ref.IsFullEncryption() {
			// half encryption mode
			// refHostName is always a http address
			// we will need to retrieve https port from target and compute https address
			refHttpsHostName, _, err = service.getHttpsRemoteHostAddr(refHostName)
			if err != nil {
				if strings.Contains(err.Error(), base.EOFString) {
					err = fmt.Errorf("%v; %v", err.Error(),
						"This could be due to the target cluster does not allow half-encryption. Try with full encryption instead")
				}
				return err
			}
		} else {
			// in full encryption mode, customer may optionally put hostName:httpsPort in hostname field of remote cluster reference
			// in this case there is no need to make a http call to target to retrieve https port
			// we assume this is the case, and will try other cases later if this does not work
			refHttpsHostName = refHostName
		}
	}

	refSANInCertificate, refHttpAuthMech, defaultPoolInfo, err, refHttpsHostName := service.getDefaultPoolInfoAndAuthMech(ref, refHostName, refHttpsHostName)
	if err != nil {
		if strings.Contains(err.Error(), base.RESTNoSuchHost) {
			err = wrapNoSuchHostRecommendationError(err.Error())
		}
		return err
	}

	// by now defaultPoolInfo contains valid info
	// Set this now so isHttps() call is correct
	ref.SetHttpAuthMech(refHttpAuthMech)

	// compute http address based on the returned defaultPoolInfo
	// even though http address is needed only by half secure type reference as of now,
	// always compute and populate http address to be more consistent and less error prone
	nodeList, err := service.utils.GetNodeListFromInfoMap(defaultPoolInfo, service.logger)
	if err != nil {
		err = fmt.Errorf("Can't get nodes information for cluster %v for ref %v, err=%v", refHostName, ref.Id(), err)
		return wrapAsInvalidRemoteClusterError(err.Error())
	}

	useExternal, err := service.getUserIntentFromNodeList(ref, nodeList)
	if err != nil {
		err = fmt.Errorf("Can't get user intent from node list, err=%v", err)
		return wrapAsInvalidRemoteClusterError(err.Error())
	}

	nodeAddressesList, err := service.utils.GetRemoteNodeAddressesListFromNodeList(nodeList, refHostName, true /*needHttps*/, service.logger, useExternal)
	if err != nil {
		err = fmt.Errorf("Can't get node addresses from node info for cluster %v for cluster reference %v, err=%v", refHostName, ref.Id(), err)
		return wrapAsInvalidRemoteClusterError(err.Error())
	}

	refHttpHostName := ""
	for _, pair := range nodeAddressesList {
		// need both checks to cover all scenarios
		// first check is for the half encryption mode where refHostName is http address and refHttpsHostName may not have been populated
		// second check is for the full encryption mode, where refHostName may be a https address
		if pair.GetFirstString() == refHostName || pair.GetSecondString() == refHttpsHostName {
			refHttpHostName = pair.GetFirstString()
			break
		}
	}

	if len(refHttpHostName) == 0 {
		// this should not happen in production
		// if it does happen, leave refHttpHostName empty for now.
		// hopefully remote cluster refresh will get ref.ActiveHostName refreshed/populated
		service.logger.Warnf("Can't get http address for cluster %v for cluster reference %v", refHostName, ref.Id())
	}

	ref.SetActiveHostName(refHttpHostName)
	ref.SetHttpsHostName(refHttpsHostName)
	ref.SetActiveHttpsHostName(refHttpsHostName)

	ref.SetSANInCertificate(refSANInCertificate)
	service.logger.Infof("Set hostName=%v, httpsHostName=%v, SANInCertificate=%v HttpAuthMech=%v for remote cluster reference %v\n", refHttpHostName, refHttpsHostName, refSANInCertificate, refHttpAuthMech, ref.Id())

	return nil
}

// For full encryption mode, uses can either enter hostname:<nonSecurePort> or hostname:<securePort>
// The procedure is try to establish conn and get defaultPoolInfo using whatever has been entered
// If the user entered securePort, all is well. However, if they entered a non-secure port (or didn't enter anything at all)
// then the call below will fail, and we'll need to figure out the SSL port by using getHttpsRemoteHostAddr(), if it's possible
// The second part of "figuring out https" if the first fails, can be done in parallel to save time
// This method lets both go at the same time, and whoever comes back first with a valid result wins
func (service *RemoteClusterService) getDefaultPoolInfoAndAuthMech(ref *metadata.RemoteClusterReference, refHostName string, refHttpsHostNameIn string) (bool, base.HttpAuthMech, map[string]interface{}, error, string) {
	// Synchronization primitives for racing
	firstWinnerCh := make(chan bool)
	secondWinnerCh := make(chan bool)
	finCh := make(chan bool)

	shouldBail := func() bool {
		select {
		case <-finCh:
			return true
		default:
			return false
		}
	}

	// First go-routine
	var refSANInCertificate bool
	var refHttpAuthMech base.HttpAuthMech
	var defaultPoolInfo map[string]interface{}
	var refHttpsHostName = refHttpsHostNameIn
	var err error

	// second go-routine
	var bgSANInCertificate bool
	var bgRefHttpAuthMech base.HttpAuthMech
	var bgDefaultPoolInfo map[string]interface{}
	var bgRefHttpsHostName = refHttpsHostNameIn
	var bgExternalRefHttpsHostName string
	var bgErr error

	go func() {
		defer close(firstWinnerCh)
		// Attempt to retrieve defaultPoolInfo with what the user initially entered
		refSANInCertificate, refHttpAuthMech, defaultPoolInfo, _, err = service.utils.GetSecuritySettingsAndDefaultPoolInfo(refHostName, refHttpsHostName, ref.UserName(), ref.Password(), ref.Certificate(), ref.ClientCertificate(), ref.ClientKey(), ref.IsHalfEncryption(), service.logger)
	}()

	// If half-mode, no need to do the following to look up ports, etc
	if ref.IsFullEncryption() {
		portNo, err := base.GetPortNumber(refHostName)
		tryDefaultSSLAdminPort := err == nil && portNo == base.DefaultAdminPort
		go func() {
			defer close(secondWinnerCh)
			// in full encryption mode, the error could have been caused by refHostName, and hence refHttpsHostName, containing a http address,
			// try treating refHostName as a http address and compute the corresponding https address by retrieving tls port from target
			var err1 error
			bgRefHttpsHostName, bgExternalRefHttpsHostName, err1 = service.getHttpsRemoteHostAddr(refHostName)
			if err1 != nil && !tryDefaultSSLAdminPort {
				// if the attempt to treat refHostName as a http address also fails, return all errors and let user decide what to do
				bgErr = err1
				return
			}
			if tryDefaultSSLAdminPort {
				bgExternalRefHttpsHostName = ""
				bgRefHttpsHostName = base.GetHostAddr(base.GetHostName(refHostName), base.DefaultAdminPortSSL)
			}

			if shouldBail() {
				return
			}

			// now we potentially have valid https address, re-do security settings retrieval
			bgSANInCertificate, bgRefHttpAuthMech, bgDefaultPoolInfo, _, bgErr = service.utils.GetSecuritySettingsAndDefaultPoolInfo(refHostName, bgRefHttpsHostName, ref.UserName(), ref.Password(), ref.Certificate(), ref.ClientCertificate(), ref.ClientKey(), ref.IsHalfEncryption(), service.logger)
			if bgErr != nil {
				if len(bgExternalRefHttpsHostName) > 0 {
					if shouldBail() {
						return
					}
					// If the https address doesn't work, and remote cluster has set-up an alternate SSL port,
					// as a last resort, try that for a third time
					bgRefHttpsHostName = bgExternalRefHttpsHostName
					bgSANInCertificate, bgRefHttpAuthMech, bgDefaultPoolInfo, _, bgErr = service.utils.GetSecuritySettingsAndDefaultPoolInfo(refHostName, bgRefHttpsHostName, ref.UserName(), ref.Password(), ref.Certificate(), ref.ClientCertificate(), ref.ClientKey(), ref.IsHalfEncryption(), service.logger)
				}
			}
			return
		}()
	}

	// When returning, always close finCh
	defer close(finCh)

	select {
	case <-firstWinnerCh:
		if err != nil {
			if !ref.IsFullEncryption() {
				// There is no second go-routine
				close(secondWinnerCh)
				return false, 0, nil, wrapAsInvalidRemoteClusterError(err.Error()), ""
			}
			select {
			case <-secondWinnerCh:
				if bgErr != nil {
					bgErr = getCombinedError(ref, err, bgErr)
				}
				return bgSANInCertificate, bgRefHttpAuthMech, bgDefaultPoolInfo, bgErr, bgRefHttpsHostName
			}
		} else {
			// The original entered reference hostname was successful
			return refSANInCertificate, refHttpAuthMech, defaultPoolInfo, err, refHttpsHostName
		}
	case <-secondWinnerCh:
		if bgErr != nil {
			select {
			case <-firstWinnerCh:
				if err != nil {
					bgErr = getCombinedError(ref, err, bgErr)
					return bgSANInCertificate, bgRefHttpAuthMech, bgDefaultPoolInfo, bgErr, bgRefHttpsHostName
				} else {
					return refSANInCertificate, refHttpAuthMech, defaultPoolInfo, err, refHttpsHostName
				}
			}
		} else {
			// Second path succeeded - return immediately and don't wait for the first path
			return bgSANInCertificate, bgRefHttpAuthMech, bgDefaultPoolInfo, bgErr, bgRefHttpsHostName
		}
	}
}

func getCombinedError(ref *metadata.RemoteClusterReference, err error, bgErr error) error {
	errMsg := fmt.Sprintf("cannot use HostName, %v, as a https address or a http address. Error when using it as a http address=%v, and https address=%v\n", ref.HostName(), err, bgErr)
	bgErr = wrapAsInvalidRemoteClusterError(errMsg)
	return bgErr
}

func (service *RemoteClusterService) getHttpsRemoteHostAddr(hostName string) (string, string, error) {
	internalHttpsHostname, externalHttpsHostname, err := service.utils.HttpsRemoteHostAddr(hostName, service.logger)
	if err != nil {
		if err.Error() == base.ErrorUnauthorized.Error() {
			return "", "", wrapAsInvalidRemoteClusterError(fmt.Sprintf("Could not get ssl port for %v. Remote cluster could be an Elasticsearch cluster that does not support ssl encryption. Please double check remote cluster configuration or turn off ssl encryption.", hostName))
		} else {
			return "", "", wrapAsInvalidRemoteClusterError(fmt.Sprintf("Could not get ssl port. err=%v", err))
		}
	}

	return internalHttpsHostname, externalHttpsHostname, nil
}

// validate certificates in remote cluster ref
func (service *RemoteClusterService) validateCertificates(ref *metadata.RemoteClusterReference) error {
	refCertificate := ref.Certificate()
	if len(refCertificate) == 0 {
		return nil
	}

	// check validity of server root certificate
	block, _ := pem.Decode(refCertificate)
	if block == nil {
		return base.InvalidCerfiticateError
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return fmt.Errorf("Failed to parse certificate. err=%v", err)
	}

	// check the signature of certificate
	err = certificate.CheckSignature(certificate.SignatureAlgorithm, certificate.RawTBSCertificate, certificate.Signature)
	if err != nil {
		return fmt.Errorf("Error validating the signature of certificate. err=%v", err)
	}

	// check validity of client certificate if it has been provided
	refClientCertificate := ref.ClientCertificate()
	if len(refClientCertificate) == 0 {
		return nil
	}

	clientCert, err := tls.X509KeyPair(refClientCertificate, ref.ClientKey())
	if err != nil {
		return fmt.Errorf("Error parsing client certificate. err=%v", err)
	}

	parentCert := certificate

	// clientCert.Certificate contains a chain of certificates, leaf first
	// e.g., LeafCert, IntermediateCert1, IntermediateCert2
	// we will be verifying these certificates in the reverse order
	// first we check IntermediateCert2 is signed by its parent, the server root certificate
	// then we check IntermediateCert1 is signed by IntermediateCert2
	// then we check LeafCert is signed by IntermediateCert1
	// if any of the certificates has been tempered with, the corresponding check should fail
	for index := len(clientCert.Certificate) - 1; index >= 0; index-- {
		curCert, err := x509.ParseCertificate(clientCert.Certificate[index])
		if err != nil {
			return fmt.Errorf("Error parsing certificate chain in client certificate. err=%v", err)
		} else {
			err = curCert.CheckSignatureFrom(parentCert)
			if err != nil {
				return fmt.Errorf("Error validating the signature of client certficate. err=%v", err)
			}
		}

		parentCert = curCert
	}

	return nil
}

func (service *RemoteClusterService) formErrorFromValidatingRemotehost(ref *metadata.RemoteClusterReference, hostName string, port uint16, err error) error {
	if !ref.IsEncryptionEnabled() {
		// if encryption is not on, most likely the error is caused by incorrect hostname or firewall.
		return wrapAsInvalidRemoteClusterError(fmt.Sprintf("Could not connect to \"%v\" on port %v. This could be due to an incorrect host/port combination or a firewall in place between the servers.", hostName, port))
	} else {
		// if encryption is on, several different errors could be returned here, e.g., invalid hostname, invalid certificate, certificate by unknown authority, etc.
		// just return the err
		// Error passed in should not be nil. But add this here just to be safe
		if err == nil {
			err = fmt.Errorf("refName: %v hostname: %v port: %v", ref.Name(), hostName, port)
		}
		return wrapAsInvalidRemoteClusterError(err.Error())
	}
}

func (service *RemoteClusterService) NewRemoteClusterAgent() *RemoteClusterAgent {
	newAgent := &RemoteClusterAgent{metakvSvc: service.metakv_svc,
		uiLogSvc:               service.uilog_svc,
		utils:                  service.utils,
		logger:                 service.logger,
		metadataChangeCallback: service.metadata_change_callback,
		bucketRefCnt:           make(map[string]uint32),
		bucketManifestGetters:  make(map[string]*BucketManifestGetter),
		agentFinCh:             make(chan bool, 1),
		connectivityHelper:     NewConnectivityHelper(),
	}
	newAgent.refreshCv = &sync.Cond{L: &newAgent.refreshMtx}
	return newAgent
}

// Should return as soon as metakv is updated (if needed)
func (service *RemoteClusterService) delRemoteAgent(agent *RemoteClusterAgent, delFromMetaKv bool) (*metadata.RemoteClusterReference, error) {
	if agent == nil {
		return nil, errors.New("Nil agent provided")
	}

	clonedCopy, err := agent.DeleteReference(delFromMetaKv)
	if service_def.DelOpConsideredPass(err) {
		// This is do-able in the background
		go agent.Stop()
		service.deleteAgentFromMaps(clonedCopy)
	}

	return clonedCopy, err
}

// Returns a cloned copy of the reference being deleted
func (service *RemoteClusterService) delRemoteClusterAgent(refName string, delFromMetaKv bool) (*metadata.RemoteClusterReference, error) {
	if len(refName) == 0 {
		return nil, errors.New("No refName is given")
	}

	service.agentMutex.RLock()
	agent := service.agentCacheRefNameMap[refName]
	refId := agent.reference.Id()
	service.agentMutex.RUnlock()
	service.registerDel(refId)
	if agent == nil {
		return nil, errors.New(fmt.Sprintf("Cannot find local reference given the name: %v", refName))
	}
	return service.delRemoteAgent(agent, delFromMetaKv)
}

// Returns a cloned copy of the reference being deleted
func (service *RemoteClusterService) delRemoteClusterAgentById(id string, delFromMetaKv bool) (*metadata.RemoteClusterReference, error) {
	if len(id) == 0 {
		return nil, errors.New("No id given")
	}

	service.agentMutex.RLock()
	agent := service.agentMap[id]
	service.agentMutex.RUnlock()
	if agent == nil {
		return nil, errors.New(fmt.Sprintf("Cannot find local reference given the Id: %v", id))
	}
	return service.delRemoteAgent(agent, delFromMetaKv)
}

/**
 * Get or Creates an agent given a specific reference and looks up by ID only.
 * If updateFromRef is set, then it'll update the agent's data with the incoming reference
 * Returns the agent pointer and a boolean that is true if the agent already existed.
 */
func (service *RemoteClusterService) getOrStartNewAgent(ref *metadata.RemoteClusterReference, userInitiated, updateFromRef bool) (*RemoteClusterAgent, bool, error) {
	var err error
	if ref == nil {
		return nil, false, base.ErrorResourceDoesNotExist
	}

	service.agentMutex.RLock()
	if agent, ok := service.agentMap[ref.Id()]; ok {
		defer service.agentMutex.RUnlock()
		if updateFromRef {
			err = agent.UpdateReferenceFrom(ref, userInitiated)
		}
		return agent, true, err
	} else {
		service.agentMutex.RUnlock()
		service.agentMutex.Lock()
		if agent, ok := service.agentMap[ref.Id()]; ok {
			service.agentMutex.Unlock()
			// someone jumped ahead of us
			if updateFromRef {
				err = agent.UpdateReferenceFrom(ref, userInitiated)
			}
			return agent, true, err
		} else {
			// empty for now - create a new agent and attempt to start it
			newAgent := service.NewRemoteClusterAgent()
			service.addAgentToAgentMapNoLock(ref, newAgent)
			service.agentMutex.Unlock()

			err := newAgent.Start(ref, userInitiated)
			if err != nil && strings.Contains(err.Error(), WriteToMetakvErrString) {
				// Error writing to metakv with a brand new agent means that the reference
				// wasn't written correctly - handle cleanup here
				go newAgent.Stop()
				service.deleteAgentFromMaps(ref)
				newAgent = nil
			}
			return newAgent, false, err
		}
	}
}

// this internal api differs from AddRemoteCluster in that it does not perform validation
func (service *RemoteClusterService) addRemoteCluster(ref *metadata.RemoteClusterReference) error {
	if ref == nil {
		return base.ErrorInvalidInput
	}
	/**
	 * Check to see if there is a local existance of this copy of the reference.
	 * This is still subjected to conflict if user adds a same reference in >1 locations simultaneously,
	 * but XDCR will have to do its best to resolve it if that's the case, as it's not a usual use case.
	 * If it doesn't exist, a new agent will be created and started at this time.
	 */
	service.registerAdd(ref.Name())
	_, exist, err := service.getOrStartNewAgent(ref, true /*userinitiated*/, false /*updateFromRef*/)
	if exist {
		return errors.New(fmt.Sprintf("Reference %v already exists on this node, potentially created from another node in the cluster. Please refresh the UI.", ref.Id()))
	}

	if err != nil && strings.Contains(err.Error(), WriteToMetakvErrString) {
		service.deregisterAdd(ref.Name())
	}

	return err
}

// These registerAdd/dregisterAdd are needed to prevent innocuous errors from being shown in the logs
func (service *RemoteClusterService) registerAdd(name string) {
	service.metakvCbAddMtx.Lock()
	defer service.metakvCbAddMtx.Unlock()

	service.metakvCbAddMap[name] = true
}

func (service *RemoteClusterService) deregisterAdd(name string) {
	service.metakvCbAddMtx.Lock()
	defer service.metakvCbAddMtx.Unlock()

	delete(service.metakvCbAddMap, name)
}

func (service *RemoteClusterService) checkIfAddingIsActive(name string) bool {
	service.metakvCbAddMtx.RLock()
	defer service.metakvCbAddMtx.RUnlock()
	_, exists := service.metakvCbAddMap[name]
	return exists
}

func (service *RemoteClusterService) registerSet(name string) {
	service.metakvCbSetMtx.Lock()
	defer service.metakvCbSetMtx.Unlock()

	service.metakvCbSetMap[name] = true
}

func (service *RemoteClusterService) deregisterSet(name string) {
	service.metakvCbSetMtx.Lock()
	defer service.metakvCbSetMtx.Unlock()

	delete(service.metakvCbSetMap, name)
}

func (service *RemoteClusterService) checkIfSettingIsActive(name string) bool {
	service.metakvCbSetMtx.RLock()
	defer service.metakvCbSetMtx.RUnlock()
	_, exists := service.metakvCbSetMap[name]
	return exists
}

func (service *RemoteClusterService) registerDel(refId string) {
	service.metakvCbDelMtx.Lock()
	defer service.metakvCbDelMtx.Unlock()

	service.metakvCbDelMap[refId] = true
}

func (service *RemoteClusterService) deregisterDel(refId string) {
	service.metakvCbDelMtx.Lock()
	defer service.metakvCbDelMtx.Unlock()

	delete(service.metakvCbDelMap, refId)
}

func (service *RemoteClusterService) checkIfDeletingIsActive(refId string) bool {
	service.metakvCbDelMtx.RLock()
	defer service.metakvCbDelMtx.RUnlock()
	_, exists := service.metakvCbDelMap[refId]
	return exists
}

func constructRemoteClusterReference(value []byte, rev interface{}) (*metadata.RemoteClusterReference, error) {
	ref := &metadata.RemoteClusterReference{}
	err := ref.Unmarshal(value)
	if err != nil {
		return nil, err
	}
	ref.SetRevision(rev)
	ref.PopulateDnsSrvIfNeeded(true /*retryOnErr*/)

	return ref, err
}

//get remote cluster name from remote cluster uuid. Return unknown if remote cluster cannot be found
func (service *RemoteClusterService) GetRemoteClusterNameFromClusterUuid(uuid string) string {
	remoteClusterRef, err := service.RemoteClusterByUuid(uuid, false)
	if err != nil || remoteClusterRef == nil {
		errMsg := fmt.Sprintf("Error getting the name of the remote cluster with uuid=%v.", uuid)
		if err != nil {
			errMsg += fmt.Sprintf(" err=%v", err)
		} else {
			errMsg += " The remote cluster may have been deleted."
		}
		service.logger.Error(errMsg)
		return service_def.UnknownRemoteClusterName
	}
	return remoteClusterRef.Name()
}

// wrap/mark an error as invalid remote cluster error - by adding "invalid remote cluster" message to the front
func wrapAsInvalidRemoteClusterError(errMsg string) error {
	return errors.New(InvalidRemoteClusterErrorMessage + errMsg)
}

// wrap/mark an error as invalid remote cluster operation error - by adding "invalid remote cluster operation" message to the front
func wrapAsInvalidRemoteClusterOperationError(errMsg string) error {
	return errors.New(InvalidRemoteClusterOperationErrorMessage + errMsg)
}

func wrapNoSuchHostRecommendationError(errMsg string) error {
	return errors.New(errMsg + NoSuchHostRecommendationString)
}

func (service *RemoteClusterService) CheckAndUnwrapRemoteClusterError(err error) (bool, error) {
	if err != nil {
		errMsg := err.Error()
		if strings.HasPrefix(errMsg, InvalidRemoteClusterErrorMessage) {
			return true, errors.New(errMsg[len(InvalidRemoteClusterErrorMessage):])
		} else if strings.HasPrefix(errMsg, InvalidRemoteClusterOperationErrorMessage) {
			return true, errors.New(errMsg[len(InvalidRemoteClusterOperationErrorMessage):])
		} else if strings.HasPrefix(err.Error(), UnknownRemoteClusterErrorMessage) {
			return true, err
		} else {
			return false, err
		}
	} else {
		return false, nil
	}
}

// Implement callback function for metakv
func (service *RemoteClusterService) RemoteClusterServiceCallback(path string, value []byte, rev interface{}) error {
	service.logger.Infof("metakvCallback called on path = %v\n", path)

	var newRef *metadata.RemoteClusterReference
	var err error
	if len(value) != 0 {
		newRef, err = constructRemoteClusterReference(value, rev)
		if err != nil {
			service.logger.Errorf("Error marshaling remote cluster. value=%v, err=%v\n", base.TagUDBytes(value), err)
			return err
		}
	}

	var refId string
	if len(path) > 0 {
		refId = GetKeyFromPath(path)
	}

	if newRef == nil || newRef.IsEmpty() {
		if len(path) == 0 {
			err = errors.New(fmt.Sprintf("%v - newRef is nil and refId %v from path %v is not given. Cannot proceed.\n",
				base.ErrorResourceDoesNotExist, refId, path))
			service.logger.Errorf(err.Error())
		} else {
			if service.checkIfDeletingIsActive(refId) {
				service.deregisterDel(refId)
			} else {
				// newRef was null - need to remove agent
				_, err = service.delRemoteClusterAgentById(refId, false)
			}
		}
	} else {
		if service.checkIfAddingIsActive(newRef.Name()) {
			// When user executes an addRemoteCluster, the issuing node's metakv callback is also called
			// Prevent duplicate call from happening
			service.deregisterAdd(newRef.Name())
		} else if service.checkIfSettingIsActive(newRef.Name()) {
			// When user executes an setRemoteCluster, the issuing node's metakv callback is also called
			// Prevent duplicate call from happening
			service.deregisterSet(newRef.Name())
		} else {
			_, _, err = service.getOrStartNewAgent(newRef, false /*userInitiated*/, true /*updateFromRef*/)
		}
	}

	return err
}

func (service *RemoteClusterService) GetConnectionStringForRemoteCluster(ref *metadata.RemoteClusterReference, isCapiReplication bool) (string, error) {
	if !isCapiReplication {
		// for xmem replication, return ref.activeHostName, which is rotated among target nodes for load balancing
		return ref.MyConnectionStr()
	} else {
		// for capi replication, return the lexicographically smallest hostname in hostname list of ref
		// this ensures that the same hostname is returned consistently (in lieu of hostname changes, which is very rare,
		// and target topology changes, which require replication restart anyway)
		// otherwise target may return different server vb maps due to an issue in elastic search plugin
		// and cause unnecessary replication restart
		service.agentMutex.RLock()
		defer service.agentMutex.RUnlock()
		agent := service.agentMap[ref.Id()]
		if agent == nil {
			service.logger.Warnf("Error retrieving %v from Remote Cluster Service. It may have been deleted by others\n", ref.Id())
			return "", service_def.MetadataNotFoundErr
		}
		return agent.GetConnectionStringForCAPIRemoteCluster()
	}
}

/**
 * Helper functions
 */
func populateRefreshSuccessMsg(origRef *metadata.RemoteClusterReference, newRef *metadata.RemoteClusterReference, newList base.StringPairList) string {
	return fmt.Sprintf("Refresher has successfully committed staged changes:\n Original: %v\n Actual staged changes now in memory: %v %v\n",
		origRef, newRef, newList)
}

func (service *RemoteClusterService) addAgentToAgentMapNoLock(ref *metadata.RemoteClusterReference, newAgent *RemoteClusterAgent) {
	service.agentMap[ref.Id()] = newAgent
	service.agentCacheRefNameMap[ref.Name()] = newAgent
	service.agentCacheUuidMap[ref.Uuid()] = newAgent
}

func (service *RemoteClusterService) deleteAgentFromMaps(clonedCopy *metadata.RemoteClusterReference) {
	service.agentMutex.Lock()
	delete(service.agentMap, clonedCopy.Id())
	delete(service.agentCacheRefNameMap, clonedCopy.Name())
	delete(service.agentCacheUuidMap, clonedCopy.Uuid())
	service.agentMutex.Unlock()
}

// If agentMaps are updated, return true
func (service *RemoteClusterService) checkAndUpdateAgentMaps(oldRef *metadata.RemoteClusterReference, newRef *metadata.RemoteClusterReference, agent *RemoteClusterAgent) bool {
	var retVal bool
	// UUID and ID both cannot change
	oldRefName := oldRef.Name()
	newRefName := newRef.Name()
	if oldRefName != newRefName {
		service.agentMutex.Lock()
		service.agentCacheRefNameMap[newRefName] = agent
		delete(service.agentCacheRefNameMap, oldRefName)
		service.agentMutex.Unlock()
		retVal = true
	}
	return retVal
}

func (service *RemoteClusterService) ShouldUseAlternateAddress(ref *metadata.RemoteClusterReference) (bool, error) {
	if ref == nil {
		return false, base.ErrorInvalidInput
	}

	service.agentMutex.RLock()
	defer service.agentMutex.RUnlock()
	agent := service.agentMap[ref.Id()]
	if agent == nil {
		return false, errors.New(fmt.Sprintf("Cannot find local reference given the Id: %v", ref.Id()))
	}

	return agent.UsesAlternateAddress()
}

func (service *RemoteClusterService) GetRefListForRestartAndClearState() (list []*metadata.RemoteClusterReference, err error) {
	service.agentMutex.RLock()
	defer service.agentMutex.RUnlock()

	for _, agent := range service.agentMap {
		if agent.ConfigurationHasChanged() {
			list = append(list, agent.GetReferenceClone())
			agent.clearAddressModeAccounting()
		}
	}
	return
}

func (service *RemoteClusterService) GetCapability(ref *metadata.RemoteClusterReference) (metadata.Capability, error) {
	if ref == nil {
		err := base.ErrorInvalidInput
		var emptyCapability metadata.Capability
		return emptyCapability, err
	}

	service.agentMutex.RLock()
	defer service.agentMutex.RUnlock()
	agent := service.agentMap[ref.Id()]
	if agent == nil {
		var emptyCapability metadata.Capability
		return emptyCapability, errors.New(fmt.Sprintf("Cannot find local reference given the Id: %v", ref.Id()))
	}

	return agent.GetCapability()
}

/**
 * Unit test helper functions
 */
func (service *RemoteClusterService) getNumberOfAgents() int {
	service.agentMutex.RLock()
	defer service.agentMutex.RUnlock()
	return len(service.agentMap)
}

func (service *RemoteClusterService) agentCacheMapsAreSynced() bool {
	service.agentMutex.RLock()
	defer service.agentMutex.RUnlock()
	if len(service.agentMap) != len(service.agentCacheRefNameMap) {
		fmt.Printf("AgentMap: %v\nAgentCacheRefNameMap: %v\n", service.agentMap, service.agentCacheRefNameMap)
		return false
	}
	if len(service.agentMap) != len(service.agentCacheUuidMap) {
		fmt.Printf("AgentMap: %v\nAgentCacheUuidMap: %v\n", service.agentMap, service.agentCacheUuidMap)
		return false
	}

	for _, agent := range service.agentMap {
		if service.agentCacheRefNameMap[agent.reference.Name()] != agent {
			fmt.Printf("AgentCacheRefNameMap mismatch name: %v\n", agent.reference.Name())
			return false
		}
		if service.agentCacheUuidMap[agent.reference.Uuid()] != agent {
			fmt.Printf("AgentCacheUuidMap mismatch\n")
			return false
		}
	}

	return true
}

func (service *RemoteClusterService) updateUtilities(utilsIn utilities.UtilsIface) {
	service.agentMutex.RLock()
	defer service.agentMutex.RUnlock()

	service.utils = utilsIn
	for _, agent := range service.agentMap {
		agent.utils = utilsIn
	}
}

func (service *RemoteClusterService) updateMetaSvc(metaSvc service_def.MetadataSvc) {
	service.agentMutex.RLock()
	defer service.agentMutex.RUnlock()

	service.metakv_svc = metaSvc
	for _, agent := range service.agentMap {
		agent.metakvSvc = metaSvc
	}
}

func (service *RemoteClusterService) GetManifestByUuid(uuid, bucketName string, forceRefresh bool) (*metadata.CollectionsManifest, error) {
	service.agentMutex.RLock()
	agent, ok := service.agentCacheUuidMap[uuid]
	service.agentMutex.RUnlock()

	if !ok {
		return nil, fmt.Errorf("Unable to find remote cluster agent given cluster UUID: %v\n", uuid)
	}

	manifest := agent.GetManifest(bucketName, forceRefresh)
	return manifest, nil
}

func (service *RemoteClusterService) RequestRemoteMonitoring(spec *metadata.ReplicationSpecification) error {
	if spec == nil {
		return base.ErrorInvalidInput
	}

	agent, err := service.getAgentByReplSpec(spec)
	if err != nil {
		return err
	}
	return agent.RegisterBucketRequest(spec.TargetBucketName)
}

func (service *RemoteClusterService) UnRequestRemoteMonitoring(spec *metadata.ReplicationSpecification) error {
	if spec == nil {
		return base.ErrorInvalidInput
	}

	agent, err := service.getAgentByReplSpec(spec)
	if err != nil {
		return err
	}
	return agent.UnRegisterBucketRefresh(spec.TargetBucketName)
}

func (service *RemoteClusterService) getAgentByReplSpec(spec *metadata.ReplicationSpecification) (*RemoteClusterAgent, error) {
	service.agentMutex.RLock()
	agent := service.agentCacheUuidMap[spec.TargetClusterUUID]
	defer service.agentMutex.RUnlock()
	if agent == nil {
		return nil, getUnknownCluster("uuid", spec.TargetClusterUUID)
	}
	return agent, nil
}

func (service *RemoteClusterService) waitForRefreshEnabled(ref *metadata.RemoteClusterReference) {
	service.agentMutex.RLock()
	agent := service.agentMap[ref.Id()]
	service.agentMutex.RUnlock()

	agent.waitForRefreshEnabled()
}

func (agent *RemoteClusterAgent) waitForRefreshEnabled() {
	for initDone := atomic.LoadUint32(&agent.initDone) == 1; !initDone; initDone = atomic.LoadUint32(&agent.initDone) == 1 {
		time.Sleep(10 * time.Millisecond)
	}
}

func (agent *RemoteClusterAgent) waitForRefreshOngoing() {
	var refreshOngoing bool
	for !refreshOngoing {
		agent.refreshMtx.Lock()
		refreshOngoing = agent.refreshActive > 0
		agent.refreshMtx.Unlock()
		time.Sleep(50 * time.Nanosecond)
	}
}
