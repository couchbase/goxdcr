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
	"time"
)

const (
	// the key to the metadata that stores the keys of all remote clusters
	RemoteClustersCatalogKey = metadata.RemoteClusterKeyPrefix
)

var InvalidRemoteClusterOperationErrorMessage = "Invalid remote cluster operation. "
var InvalidRemoteClusterErrorMessage = "Invalid remote cluster. "
var UnknownRemoteClusterErrorMessage = "unknown remote cluster"
var InvalidConnectionStrError = errors.New("invalid connection string")
var BootStrapNodeHasMovedError = errors.New("Bootstrap node in reference has been moved")
var UUIDMismatchError = errors.New("UUID does not match")

/**
 * A RemoteClusterAgent is responsible for handling all operations related to a specific RemoteClusterReference.
 * RemoteClusterService's job is to wrap around them and provide APIs to other components that require info
 * or operation regarding a specific Remote Cluster Reference.
 */
type RemoteClusterAgentIface interface {
	service_def.CollectionsManifestOps

	/* Modifier ops */
	Start(newRef *metadata.RemoteClusterReference) error
	Stop()
	// This call will set the RemoteClusterAgent's internal reference with new information from newRef
	UpdateReferenceFrom(newRef *metadata.RemoteClusterReference, writeToMetaKv bool) error
	DeleteReference(delFromMetaKv bool) (*metadata.RemoteClusterReference, error)
	Refresh() error

	// Buckets that agents need to monitor
	RegisterBucketRequest(bucketName string) error
	UnRegisterBucketRefresh(bucketName string) error

	/* Getter ops */
	// To be used for RemoteClusterService for any caller requesting a copy of the RC Reference
	GetReferenceClone() *metadata.RemoteClusterReference
	GetConnectionStringForCAPIRemoteCluster() string

	// Manifest Ops
	GetManifest(bucketName string, forceRefresh bool) *metadata.CollectionsManifest
}

type RemoteClusterAgent struct {
	/** Members protected by refMutex */
	// Mutex used to protect any internal data structure that may be modified
	refMtx sync.RWMutex
	// The offical local copy of the RemoteClusterReference. Use Clone() method to make a copy.
	reference metadata.RemoteClusterReference
	// The most up-to-date cached list of nodes in pairs of [httpAddr, httpsAddr]
	refNodesList base.StringPairList
	// function pointer to callback
	metadataChangeCallback base.MetadataChangeHandlerCallback
	// Flag to state that metakv deletes have occured. Any concurrent refresh() taking place
	// when delete occures should NOT write to metakv after this is set
	deletedFromMetakv bool

	// Wait group for making sure we exit synchronized
	agentWaitGrp sync.WaitGroup
	// finChannel for refresher
	refresherFinCh chan bool
	// Make sure we call stop only once
	stopOnce sync.Once

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
}

func (agent *RemoteClusterAgent) GetReferenceClone() *metadata.RemoteClusterReference {
	agent.refMtx.RLock()
	defer agent.refMtx.RUnlock()
	return agent.reference.Clone()
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

func (agent *RemoteClusterAgent) initializeNewRefreshContext() (*refreshContext, error) {
	rctx := &refreshContext{agent: agent}
	err := rctx.initialize()
	if err != nil {
		return nil, err
	} else {
		return rctx, nil
	}
}

// This is used as a helper context during each refresh operation
type refreshContext struct {
	// For comparison and editing
	refOrig            *metadata.RemoteClusterReference
	refCache           *metadata.RemoteClusterReference
	origRefNodesList   base.StringPairList
	cachedRefNodesList base.StringPairList

	// connection related
	connStr       string
	hostName      string
	httpsHostName string

	// iterator related
	index           int
	atLeastOneValid bool

	// agent shortcut
	agent *RemoteClusterAgent
}

// Initializes the context and also populates the credentials for connecting to nodes
func (rctx *refreshContext) initialize() error {
	var err error
	// First cache the info
	rctx.agent.refMtx.RLock()
	// For comparison
	rctx.refOrig = rctx.agent.reference.Clone()
	// for editing
	rctx.refCache = rctx.agent.reference.Clone()
	// For comparison
	rctx.origRefNodesList = base.DeepCopyStringPairList(rctx.agent.refNodesList)
	// for editing
	rctx.cachedRefNodesList = base.DeepCopyStringPairList(rctx.agent.refNodesList)
	rctx.agent.refMtx.RUnlock()

	if err != nil {
		return err
	}

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

	return nil
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
	sort.Sort(rctx.origRefNodesList)
	sort.Sort(rctx.cachedRefNodesList)
	nodesListUpdated := !reflect.DeepEqual(rctx.origRefNodesList, rctx.cachedRefNodesList)

	if !rctx.refOrig.IsSame(rctx.refCache) || nodesListUpdated {
		rctx.agent.refMtx.Lock()
		defer rctx.agent.refMtx.Unlock()
		// First see if anyone has changed the reference from underneath us
		sortedAgentList := base.DeepCopyStringPairList(rctx.agent.refNodesList)
		sort.Sort(sortedAgentList)
		if !rctx.agent.reference.IsSame(rctx.refOrig) || !reflect.DeepEqual(sortedAgentList, rctx.origRefNodesList) {
			return populateRefreshDataInconsistentError(rctx.refOrig.CloneAndRedact(), rctx.agent.reference.CloneAndRedact(), rctx.origRefNodesList, sortedAgentList)
		}

		// 1. when refOrig.IsSame(refCache) is true, i.e., when there have been no changes to refCache,
		//    updateReferenceFromNoLock is not called
		// 2. when refOrig.IsSame(refCache) is false, and refOrig.IsEssentiallySame(refCache) is true,
		//    i.e., when there have been changes to transient fields like ActiveHostName in refCache,
		//    updateReferenceFromNoLock is called to get the transient fields updated.
		//    there are no metakv update or metadata change callback, though
		// 3. when refOrig.IsEssentiallySame(refCache) is false,
		//    i.e., when there have been changes to essential fields in refCache,
		//    updateReferenceFromNoLock is called with metakv update and metadata change callback
		if !rctx.refOrig.IsSame(rctx.refCache) {
			isEssentiallySame := rctx.refOrig.IsEssentiallySame(rctx.refCache)
			updateErr := rctx.agent.updateReferenceFromNoLock(rctx.refCache, !isEssentiallySame /*updateMetaKv*/, !isEssentiallySame /*shouldCallCb*/)
			if updateErr != nil {
				rctx.agent.logger.Warnf(updateErr.Error())
				return updateErr
			}
		} else {
			rctx.agent.refNodesList = base.DeepCopyStringPairList(rctx.cachedRefNodesList)
		}
		rctx.agent.logger.Infof(populateRefreshSuccessMsg(rctx.refOrig.CloneAndRedact(), rctx.agent.reference.CloneAndRedact(), rctx.origRefNodesList, rctx.agent.refNodesList))
	}

	return nil
}

func (rctx *refreshContext) finalizeRefCacheListFrom(listToBeUsed base.StringPairList) {
	rctx.cachedRefNodesList = listToBeUsed
	if !rctx.atLeastOneValid {
		rctx.atLeastOneValid = true
	}
}

func (rctx *refreshContext) verifyNodeAndGetList(connStr string, updateSecuritySettings bool) ([]interface{}, error) {
	username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, err := rctx.refCache.MyCredentials()
	if err != nil {
		rctx.agent.logger.Warnf("When refreshing remote cluster reference %v, skipping node %v because of error retrieving user credentials from reference. err=%v\n", rctx.refCache.Id(), connStr, err)
		return nil, err
	}

	var defaultPoolInfo map[string]interface{}
	if updateSecuritySettings && rctx.refCache.IsEncryptionEnabled() {
		// if updateSecuritySettings is true, get up to date security settings from target
		sanInCertificate, httpAuthMech, defaultPoolInfo, err = rctx.agent.utils.GetSecuritySettingsAndDefaultPoolInfo(rctx.hostName, rctx.httpsHostName, username, password, certificate, clientCertificate, clientKey, rctx.refCache.IsHalfEncryption(), rctx.agent.logger)
		if err != nil {
			rctx.agent.logger.Warnf("When refreshing remote cluster reference %v, skipping node %v because of error retrieving security settings from target. err=%v\n", rctx.refCache.Id(), connStr, err)
			return nil, err
		}
	} else {
		defaultPoolInfo, err = rctx.agent.utils.GetClusterInfo(connStr, base.DefaultPoolPath, username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, rctx.agent.logger)
		if err != nil {
			rctx.agent.logger.Warnf("When refreshing remote cluster reference %v, skipping node %v because of error retrieving default pool info from target. err=%v\n", rctx.refCache.Id(), connStr, err)
			return nil, err
		}
	}

	clusterUUID, nodeList, err := rctx.agent.utils.GetClusterUUIDAndNodeListWithMinInfoFromDefaultPoolInfo(defaultPoolInfo, rctx.agent.logger)
	if err != nil {
		rctx.agent.logger.Warnf("When refreshing remote cluster reference %v, skipping node %v because of error parsing default pool info. err=%v\n", rctx.refCache.Id(), connStr, err)
		return nil, err
	}
	// selected node is accessible
	refCacheUuid := rctx.refCache.Uuid()
	if clusterUUID != refCacheUuid {
		rctx.agent.logger.Warnf("Cluster UUID: %v and refCache UUID: %v", clusterUUID, refCacheUuid)
		return nil, UUIDMismatchError
	} else {
		// update security settings only if the target node is still in the same target cluster
		if updateSecuritySettings && rctx.refCache.IsEncryptionEnabled() {
			if rctx.refCache.SANInCertificate() != sanInCertificate {
				rctx.agent.logger.Infof("Updating sanInCertificate in remote cluster reference %v to %v\n", rctx.refCache.Id(), sanInCertificate)
				rctx.refCache.SetSANInCertificate(sanInCertificate)
			}
			refCacheAuthMech := rctx.refCache.HttpAuthMech()
			if refCacheAuthMech != httpAuthMech {
				rctx.agent.logger.Infof("Updating httpAuthMech in remote cluster reference %v from %v to %v\n", rctx.refCache.Id(), refCacheAuthMech, httpAuthMech)
				rctx.refCache.SetHttpAuthMech(httpAuthMech)
			}
		}
		return nodeList, nil
	}
}

func (agent *RemoteClusterAgent) Refresh() error {
	rctx, err := agent.initializeNewRefreshContext()
	if err != nil {
		return err
	}

	var nodeAddressesList base.StringPairList
	for rctx.index = 0; rctx.index < len(rctx.cachedRefNodesList /*already shuffled*/); rctx.index++ {
		rctx.setHostNamesAndConnStr(rctx.cachedRefNodesList[rctx.index])

		nodeList, err := rctx.verifyNodeAndGetList(rctx.connStr, true /*updateSecuritySettings*/)
		if err != nil {
			if err == UUIDMismatchError {
				if rctx.hostName == rctx.refOrig.HostName() && len(rctx.cachedRefNodesList) == 1 {
					// If this is the only node to be checked AND this is the bootstrap node
					// then there's nothing to do now as there is no more nodes in the list to walk
					return BootStrapNodeHasMovedError
				}
			}
		} else {
			// rctx.hostname is in the cluster and is available - make it the activeHost
			rctx.checkAndUpdateActiveHost()

			nodeAddressesList, err = agent.utils.GetRemoteNodeAddressesListFromNodeList(nodeList, rctx.connStr, rctx.refCache.IsEncryptionEnabled(), agent.logger)
			if err == nil {
				// This node is an acceptable replacement for active node - and sets atLeastOneValid
				rctx.finalizeRefCacheListFrom(nodeAddressesList)

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
	if err != nil {
		return err
	}

	agent.refreshBucketsManifests()
	return nil
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

	err := agent.UpdateReferenceFrom(newRef, userInitiated)

	if err == nil {
		agent.logger.Infof("Agent %v %v started for cluster: %v", agent.reference.Id(), agent.reference.Name(), agent.reference.Uuid())
		agent.agentWaitGrp.Add(1)
		go agent.runPeriodicRefresh()
	} else {
		agent.logger.Warnf("Agent %v starting resulted in error: %v", agent.reference.Id(), err)
	}
	return err
}

func (agent *RemoteClusterAgent) stopAllGoRoutines() {
	close(agent.refresherFinCh)

	// Wait for all go routines to stop before clean up
	agent.agentWaitGrp.Wait()
}

// Once it's been Stopped, an agent *must* be deleted and not reused due to the stopOnce here
func (agent *RemoteClusterAgent) Stop() {
	agent.stopOnce.Do(func() {
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

	agent.refMtx.Lock()
	defer agent.refMtx.Unlock()
	var err error

	// When deleting reference, the clonedCopy is used for logging
	clonedCopy := agent.reference.Clone()

	if delFromMetaKv {
		err = agent.deleteFromMetaKVNoLock()
	}

	if service_def.DelOpConsideredPass(err) {
		agent.clearReferenceNoLock()
		agent.callMetadataChangeCbNoLock()
		agent.deletedFromMetakv = true
	}
	return clonedCopy, err
}

/**
 * Given the reference being staged in the reference, use the bare bone information to get more
 * information for caching purposes
 * Write lock needs to be held
 */
func (agent *RemoteClusterAgent) syncInternalsFromStagedReferenceNoLock() error {
	username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, err := agent.pendingRef.MyCredentials()
	if err != nil {
		return err
	}
	connStr, err := agent.pendingRef.MyConnectionStr()
	if err != nil {
		return err
	}

	// use GetNodeListWithMinInfo API to ensure that it is supported by target cluster, which could be an elastic search cluster
	nodeList, err := agent.utils.GetNodeListWithMinInfo(connStr, username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, agent.logger)
	if err == nil {
		agent.logger.Debugf("connStr=%v, nodeList=%v\n", connStr, nodeList)

		nodeAddressesList, err := agent.utils.GetRemoteNodeAddressesListFromNodeList(nodeList, connStr, agent.pendingRef.IsEncryptionEnabled(), agent.logger)
		if err != nil {
			agent.logger.Errorf("Error getting nodes from target cluster. skipping alternative node computation. ref=%v\n", agent.pendingRef.HostName())
			agent.pendingRefNodes = base.DeepCopyStringPairList(agent.refNodesList)
		} else {
			agent.pendingRefNodes = base.DeepCopyStringPairList(nodeAddressesList)
		}
		agent.logger.Debugf("agent.pendingRefNodes after internal sync =%v", agent.pendingRefNodes)

	} else {
		agent.logger.Infof("Remote cluster reference %v has a bad connectivity, didn't populate alternative connection strings. err=%v", agent.pendingRef.Id(), err)
		err = InvalidConnectionStrError
		agent.logger.Infof("nodes_connStrs from old cache =%v", agent.refNodesList)
		agent.pendingRefNodes = base.DeepCopyStringPairList(agent.refNodesList)
	}

	return err
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
		case <-agent.refresherFinCh:
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
func (agent *RemoteClusterAgent) stageNewReferenceNoLock(newRef *metadata.RemoteClusterReference, userInitiated bool) {
	agent.pendingRef.LoadFrom(newRef)
	agent.pendingRefNodes = make(base.StringPairList, 0)
	if !agent.reference.IsEmpty() {
		agent.pendingRef.SetId(agent.reference.Id())
		if userInitiated {
			agent.pendingRef.SetRevision(agent.reference.Revision())
		}
	}
}

// operation to commit the staged changes into the reference
func (agent *RemoteClusterAgent) commitStagedChangesNoLock() {
	if !agent.pendingRef.IsEmpty() {
		agent.oldRef = agent.reference.Clone()
		agent.reference.LoadFrom(&agent.pendingRef)
		agent.refNodesList = base.DeepCopyStringPairList(agent.pendingRefNodes)
	}
}

func (agent *RemoteClusterAgent) IsSame(ref *metadata.RemoteClusterReference) bool {
	agent.refMtx.RLock()
	defer agent.refMtx.RUnlock()
	return agent.reference.IsSame(ref)
}

func (agent *RemoteClusterAgent) clearReferenceNoLock() {
	agent.oldRef = agent.reference.Clone()
	agent.reference.Clear()
	agent.refNodesList = nil
}

// Retrieves the ref from metakv to be able to get the latest revision, stores into pendingRef
// Write lock must be held
// Returns non-nil if the reference in metakv is different from locally stored (less revision differences)
func (agent *RemoteClusterAgent) updateRevisionFromMetaKVNoLock() error {
	if len(agent.pendingRef.Id()) == 0 {
		return base.ErrorResourceDoesNotExist
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

/**
 * The agent will update its information from the incoming newRef.
 * If updateMetaKv is set to true, it'll write the information to metakv.
 * Returns an error code if any non-recoverable metakv operation failed.
 */
func (agent *RemoteClusterAgent) updateReferenceFromNoLock(newRef *metadata.RemoteClusterReference, updateMetaKv bool, shouldCallCb bool) error {
	var err error
	if newRef == nil {
		return base.ErrorResourceDoesNotExist
	}
	// No need to update if they are the same
	if agent.reference.IsSame(newRef) {
		return nil
	}

	agent.stageNewReferenceNoLock(newRef, updateMetaKv)

	// Populate staged runtime information from the staged metadata information.
	syncErr := agent.syncInternalsFromStagedReferenceNoLock()
	if syncErr != nil {
		// Because as part of validateRemoteCluster, we already checked the remoteCluster status
		// At this point, this error should be innocuous to pass through.
		agent.logger.Warnf(fmt.Sprintf("Error: Issues populating runtime info: %v", syncErr.Error()))
	}

	/**
	 * Update procedure:
	 * First write the pending changes to metaKV. Once they are persisted, then commit the staged changes
	 * permanently by loading it to agent.reference.
	 * If unable to successfully operate on metakv, then discard the staged changes.
	 */
	if updateMetaKv {
		err = agent.writeToMetaKVNoLock()
		if err == nil {
			// After writing, try to get the revision
			err = agent.updateRevisionFromMetaKVNoLock()
		}
	}

	if err == nil {
		agent.commitStagedChangesNoLock()
		if shouldCallCb {
			agent.callMetadataChangeCbNoLock()
		}
	}
	return err
}

func (agent *RemoteClusterAgent) UpdateReferenceFrom(newRef *metadata.RemoteClusterReference, updateMetaKv bool) error {
	agent.refMtx.Lock()
	defer agent.refMtx.Unlock()
	return agent.updateReferenceFromNoLock(newRef, updateMetaKv, true)
}

func (agent *RemoteClusterAgent) callMetadataChangeCbNoLock() {
	var id string
	if agent.reference.IsEmpty() && agent.oldRef != nil {
		id = agent.oldRef.Id()
	} else {
		id = agent.reference.Id()
	}

	if agent.metadataChangeCallback != nil {
		callbackErr := agent.metadataChangeCallback(id, agent.oldRef.Clone(), agent.reference.Clone())
		if callbackErr != nil {
			agent.logger.Error(callbackErr.Error())
		}
	}
}

func (agent *RemoteClusterAgent) deleteFromMetaKV() error {
	agent.refMtx.Lock()
	defer agent.refMtx.Unlock()
	return agent.deleteFromMetaKVNoLock()
}

// Delete the reference information from metakv
func (agent *RemoteClusterAgent) deleteFromMetaKVNoLock() error {
	err := agent.metakvSvc.DelWithCatalog(RemoteClustersCatalogKey, agent.reference.Id(), agent.reference.Revision())
	if err != nil {
		agent.logger.Errorf(fmt.Sprintf("Error occured when deleting reference %v from metakv: %v\n", agent.reference.Name(), err.Error()))
	} else {
		agent.logger.Infof("Remote cluster %v deleted from metadata store\n", agent.reference.Name())
	}
	return err
}

/**
 * Writes the staged reference to metakv.
 * There are 2 types of writes: Add and Set.
 * Add is used when it is the first time this agent is writing to metakv to create a new kv.
 * Otherwise, Set should be used to update the existing kv.
 */
func (agent *RemoteClusterAgent) writeToMetaKVNoLock() error {
	var err error
	refForMetaKv := agent.pendingRef.CloneForMetakvUpdate()

	if agent.deletedFromMetakv {
		return base.ErrorResourceDoesNotExist
	}

	key := refForMetaKv.Id()
	value, err := refForMetaKv.Marshal()
	if err != nil {
		return err
	}

	if agent.reference.IsEmpty() {
		err = agent.metakvSvc.AddSensitiveWithCatalog(RemoteClustersCatalogKey, key, value)
	} else {
		err = agent.metakvSvc.SetSensitive(key, value, agent.pendingRef.Revision())
	}

	return err
}

func (agent *RemoteClusterAgent) setMetadataChangeCb(newCb base.MetadataChangeHandlerCallback) {
	agent.refMtx.Lock()
	defer agent.refMtx.Unlock()
	agent.metadataChangeCallback = newCb
}

func (agent *RemoteClusterAgent) RegisterBucketRequest(bucketName string) error {
	agent.bucketMtx.Lock()
	defer agent.bucketMtx.Unlock()

	manifestGetter, ok := agent.bucketManifestGetters[bucketName]
	if !ok {
		// Use TopologyChangeCheckInterval as min interval between pulls, while agent refreshes at a longer interval
		manifestGetter = NewBucketManifestGetter(bucketName, agent, base.TopologyChangeCheckInterval)
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
		return nil, err
	}
	username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate,
		clientKey, err := agent.reference.MyCredentials()
	agent.refMtx.RUnlock()

	if err != nil {
		return nil, err
	}

	return agent.utils.GetCollectionsManifest(connStr, bucketName, username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, agent.logger)
}

// refreshIfPossible to prevent overwhelming target outside of refresh interval
func (agent *RemoteClusterAgent) GetManifest(bucketName string, refreshIfPossible bool) *metadata.CollectionsManifest {
	agent.bucketMtx.RLock()
	getter, ok := agent.bucketManifestGetters[bucketName]
	if !ok {
		agent.bucketMtx.RUnlock()
		return nil
	}
	agent.bucketMtx.RUnlock()

	if refreshIfPossible {
		return getter.GetManifest()
	} else {
		return getter.lastStoredManifest
	}
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
		_, _, err = service.getOrStartNewAgent(ref, false, true)
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
			} else {
				service.logger.Warnf(getRefreshErrorMsg(refId, err))
			}
		}
	}

	return agent.GetReferenceClone(), nil
}

func (service *RemoteClusterService) RemoteClusterByRefName(refName string, refresh bool) (*metadata.RemoteClusterReference, error) {
	service.agentMutex.RLock()
	agent := service.agentCacheRefNameMap[refName]
	if agent == nil {
		service.agentMutex.RUnlock()
		return nil, getUnknownCluster("refName", refName)
	}
	service.agentMutex.RUnlock()

	if refresh {
		err := agent.Refresh()
		if err != nil {
			if err == BootStrapNodeHasMovedError {
				service.logger.Errorf(getBootStrapNodeHasMovedErrorMsg(refName))
				return nil, errors.New(getBootStrapNodeHasMovedErrorMsg(refName))
			} else {
				service.logger.Warnf(getRefreshErrorMsg(refName, err))
			}
		}
	}

	return agent.GetReferenceClone(), nil
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

func (service *RemoteClusterService) setRemoteCluster(refName string, newRef *metadata.RemoteClusterReference) error {
	service.logger.Infof("Setting remote cluster with refName %v. ref=%v\n", refName, newRef)

	err := service.ValidateSetRemoteCluster(refName, newRef)
	if err != nil {
		return err
	}

	service.agentMutex.Lock()
	defer service.agentMutex.Unlock()

	agent := service.agentCacheRefNameMap[refName]

	if agent == nil {
		return errors.New(fmt.Sprintf("Error: refName %v not found in the cluster service\n", refName))
	} else {
		// In case things change and need to update maps
		oldRef := agent.reference.Clone()

		err := agent.UpdateReferenceFrom(newRef, true)

		if err == nil {
			service.checkAndUpdateAgentMapsNoLock(oldRef, newRef, agent)

			if service.uilog_svc != nil {
				var hostnameChangeMsg string
				newRefHostName := newRef.HostName()
				if oldRef.HostName() != newRefHostName {
					hostnameChangeMsg = fmt.Sprintf(" New contact point is %s.", newRefHostName)
				}
				uiLogMsg := fmt.Sprintf("Remote cluster reference \"%s\" updated.%s", oldRef.Name(), hostnameChangeMsg)
				service.uilog_svc.Write(uiLogMsg)
			}
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
		remoteClusterReferencesMap[refId] = agent.GetReferenceClone()
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
	oldRef, err := service.RemoteClusterByRefName(refName, false)
	if err != nil {
		return err
	}

	err = service.validateRemoteCluster(ref, true)
	if err != nil {
		return err
	}

	if oldRef.Uuid() != ref.Uuid() {
		return wrapAsInvalidRemoteClusterOperationError("The new hostname points to a different remote cluster, which is not allowed.")
	}

	return nil
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

	refHostName := ref.HostName()
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

func (service *RemoteClusterService) setHostNamesAndSecuritySettings(ref *metadata.RemoteClusterReference) error {
	if !ref.IsEncryptionEnabled() {
		ref.SetActiveHostName(ref.HostName())
		// nothing more needs to be done if encryption is not enabled
		return nil
	}

	refHostName := ref.HostName()
	refHttpsHostName := ref.HttpsHostName()
	var err error
	var err1 error

	if refHttpsHostName == "" {
		if !ref.IsFullEncryption() {
			// half encryption mode
			// refHostName is always a http address
			// we will need to retrieve https port from target and compute https address
			refHttpsHostName, err = service.getHttpsRemoteHostAddr(refHostName)
			if err != nil {
				return err
			}
		} else {
			// in full encryption mode, customer may optionally put hostName:httpsPort in hostname field of remote cluster reference
			// in this case there is no need to make a http call to target to retrieve https port
			// we assume this is the case, and will try other cases later if this does not work
			refHttpsHostName = refHostName
		}
	}

	refSANInCertificate, refHttpAuthMech, defaultPoolInfo, err := service.utils.GetSecuritySettingsAndDefaultPoolInfo(refHostName, refHttpsHostName, ref.UserName(), ref.Password(), ref.Certificate(), ref.ClientCertificate(), ref.ClientKey(), ref.IsHalfEncryption(), service.logger)
	if err != nil {
		if !ref.IsFullEncryption() {
			return wrapAsInvalidRemoteClusterError(err.Error())
		}
		// in full encryption mode, the error could have been caused by refHostName, and hence refHttpsHostName, containing a http address,
		// try treating refHostName as a http address and compute the corresponding https address by retrieving tls port from target
		refHttpsHostName, err1 = service.getHttpsRemoteHostAddr(refHostName)
		if err1 != nil {
			// if the attempt to treat refHostName as a http address also fails, return all errors and let user decide what to do
			errMsg := fmt.Sprintf("Cannot use HostName, %v, as a https address or a http address. Error when using it as a https address=%v\n. Error when using it as a http address=%v\n", ref.HostName(), err, err1)
			return wrapAsInvalidRemoteClusterError(errMsg)
		}

		// now we have valid https address, re-do security settings retrieval
		refSANInCertificate, refHttpAuthMech, defaultPoolInfo, err = service.utils.GetSecuritySettingsAndDefaultPoolInfo(refHostName, refHttpsHostName, ref.UserName(), ref.Password(), ref.Certificate(), ref.ClientCertificate(), ref.ClientKey(), ref.IsHalfEncryption(), service.logger)
		if err != nil {
			return wrapAsInvalidRemoteClusterError(err.Error())
		}
	}

	// by now defaultPoolInfo contains valid info
	// compute http address based on the returned defaultPoolInfo
	// even though http address is needed only by half secure type reference as of now,
	// always compute and populate http address to be more consistent and less error prone
	nodeList, err := service.utils.GetNodeListFromInfoMap(defaultPoolInfo, service.logger)
	if err != nil {
		err = fmt.Errorf("Can't get nodes information for cluster %v for ref %v, err=%v", refHostName, ref.Id(), err)
		return wrapAsInvalidRemoteClusterError(err.Error())
	}

	nodeAddressesList, err := service.utils.GetRemoteNodeAddressesListFromNodeList(nodeList, refHostName, true /*needHttps*/, service.logger)
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
	ref.SetHttpAuthMech(refHttpAuthMech)
	service.logger.Infof("Set hostName=%v, httpsHostName=%v, SANInCertificate=%v HttpAuthMech=%v for remote cluster reference %v\n", refHttpHostName, refHttpsHostName, refSANInCertificate, refHttpAuthMech, ref.Id())
	return nil
}

func (service *RemoteClusterService) getHttpsRemoteHostAddr(hostName string) (string, error) {
	refHttpsHostName, err := service.utils.HttpsRemoteHostAddr(hostName, service.logger)
	if err != nil {
		if err.Error() == base.ErrorUnauthorized.Error() {
			return "", wrapAsInvalidRemoteClusterError(fmt.Sprintf("Could not get ssl port for %v. Remote cluster could be an Elasticsearch cluster that does not support ssl encryption. Please double check remote cluster configuration or turn off ssl encryption.", hostName))
		} else {
			return "", wrapAsInvalidRemoteClusterError(fmt.Sprintf("Could not get ssl port for %v. err=%v", hostName, err))
		}
	}
	return refHttpsHostName, nil
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
		refresherFinCh:         make(chan bool, 1),
		bucketRefCnt:           make(map[string]uint32),
		bucketManifestGetters:  make(map[string]*BucketManifestGetter),
	}
	return newAgent
}

func (service *RemoteClusterService) delRemoteAgentNoLock(agent *RemoteClusterAgent, delFromMetaKv bool) (*metadata.RemoteClusterReference, error) {
	if agent == nil {
		return nil, errors.New("Nil agent provided")
	}

	clonedCopy, err := agent.DeleteReference(delFromMetaKv)
	if service_def.DelOpConsideredPass(err) {
		agent.Stop()
		service.deleteAgentFromMapsNoLock(clonedCopy)
	}

	return clonedCopy, err
}

// Returns a cloned copy of the reference being deleted
func (service *RemoteClusterService) delRemoteClusterAgent(refName string, delFromMetaKv bool) (*metadata.RemoteClusterReference, error) {
	if len(refName) == 0 {
		return nil, errors.New("No refName is given")
	}

	service.agentMutex.Lock()
	defer service.agentMutex.Unlock()
	agent := service.agentCacheRefNameMap[refName]
	if agent == nil {
		return nil, errors.New(fmt.Sprintf("Cannot find local reference given the name: %v", refName))
	}
	return service.delRemoteAgentNoLock(agent, delFromMetaKv)
}

// Returns a cloned copy of the reference being deleted
func (service *RemoteClusterService) delRemoteClusterAgentById(id string, delFromMetaKv bool) (*metadata.RemoteClusterReference, error) {
	if len(id) == 0 {
		return nil, errors.New("No id given")
	}

	service.agentMutex.Lock()
	defer service.agentMutex.Unlock()
	agent := service.agentMap[id]
	if agent == nil {
		return nil, errors.New(fmt.Sprintf("Cannot find local reference given the Id: %v", id))
	}
	return service.delRemoteAgentNoLock(agent, delFromMetaKv)
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
		defer service.agentMutex.Unlock()
		if agent, ok := service.agentMap[ref.Id()]; ok {
			// someone jumped ahead of us
			if updateFromRef {
				err = agent.UpdateReferenceFrom(ref, userInitiated)
			}
			return agent, true, err
		} else {
			// empty for now - create a new agent and attempt to start it
			newAgent := service.NewRemoteClusterAgent()
			err := newAgent.Start(ref, userInitiated)
			if err == nil {
				service.addAgentToAgentMapNoLock(ref, newAgent)
			} else {
				newAgent = nil
			}
			return newAgent, false, err
		}
	}
}

// this internal api differs from AddRemoteCluster in that it does not perform validation
func (service *RemoteClusterService) addRemoteCluster(ref *metadata.RemoteClusterReference) error {
	/**
	 * Check to see if there is a local existance of this copy of the reference.
	 * This is still subjected to conflict if user adds a same reference in >1 locations simultaneously,
	 * but XDCR will have to do its best to resolve it if that's the case, as it's not a usual use case.
	 * If it doesn't exist, a new agent will be created and started at this time.
	 */
	_, exist, err := service.getOrStartNewAgent(ref, true, false)
	if exist {
		return errors.New(fmt.Sprintf("Reference %v already exists on this node, potentially created from another node in the cluster. Please refresh the UI.", ref.Id()))
	}

	return err
}

func constructRemoteClusterReference(value []byte, rev interface{}) (*metadata.RemoteClusterReference, error) {
	ref := &metadata.RemoteClusterReference{}
	err := ref.Unmarshal(value)
	if err != nil {
		return nil, err
	}
	ref.SetRevision(rev)

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
			// newRef was null - need to remove agent
			_, err = service.delRemoteClusterAgentById(refId, false)
		}
	} else {
		_, _, err = service.getOrStartNewAgent(newRef, false, true)
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
func populateRefreshDataInconsistentError(origRef *metadata.RemoteClusterReference, newRef *metadata.RemoteClusterReference, origList base.StringPairList, newList base.StringPairList) error {
	return errors.New(fmt.Sprintf("Refresher has experienced someone updating the reference from underneath it while it was populating data.\n Expected Original: %v %v\n Actual Reference in memory: %v %v\n Will skip updating the actual reference this time around.",
		origRef, origList, newRef, newList))
}

func populateRefreshSuccessMsg(origRef *metadata.RemoteClusterReference, newRef *metadata.RemoteClusterReference, origList base.StringPairList, newList base.StringPairList) string {
	return fmt.Sprintf("Refresher has successfully committed staged changes:\n Original: %v %v\n Actual staged changes now in memory: %v %v\n",
		origRef, origList, newRef, newList)
}

func (service *RemoteClusterService) addAgentToAgentMapNoLock(ref *metadata.RemoteClusterReference, newAgent *RemoteClusterAgent) {
	service.agentMap[ref.Id()] = newAgent
	service.agentCacheRefNameMap[ref.Name()] = newAgent
	service.agentCacheUuidMap[ref.Uuid()] = newAgent
}

func (service *RemoteClusterService) deleteAgentFromMapsNoLock(clonedCopy *metadata.RemoteClusterReference) {
	delete(service.agentMap, clonedCopy.Id())
	delete(service.agentCacheRefNameMap, clonedCopy.Name())
	delete(service.agentCacheUuidMap, clonedCopy.Uuid())
}

// If agentMaps are updated, return true
func (service *RemoteClusterService) checkAndUpdateAgentMapsNoLock(oldRef *metadata.RemoteClusterReference, newRef *metadata.RemoteClusterReference, agent *RemoteClusterAgent) bool {
	var retVal bool
	// UUID and ID both cannot change
	oldRefName := oldRef.Name()
	newRefName := newRef.Name()
	if oldRefName != newRefName {
		service.agentCacheRefNameMap[newRefName] = agent
		delete(service.agentCacheRefNameMap, oldRefName)
		retVal = true
	}
	return retVal
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
		return false
	}
	if len(service.agentMap) != len(service.agentCacheUuidMap) {
		return false
	}

	for _, agent := range service.agentMap {
		if service.agentCacheRefNameMap[agent.reference.Name()] != agent {
			return false
		}
		if service.agentCacheUuidMap[agent.reference.Uuid()] != agent {
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

func (service *RemoteClusterService) getAgentByReplSpec(spec *metadata.ReplicationSpecification) (*RemoteClusterAgent, error) {
	service.agentMutex.RLock()
	agent := service.agentCacheUuidMap[spec.TargetClusterUUID]
	defer service.agentMutex.RUnlock()
	if agent == nil {
		return nil, getUnknownCluster("uuid", spec.TargetClusterUUID)
	}
	return agent, nil
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
