// Copyright (c) 2013 Couchbase, Inc.
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
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/pipeline_utils"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/simple_utils"
	"github.com/couchbase/goxdcr/utils"
	"math/rand"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// the key to the metadata that stores the keys of all remote clusters
	RemoteClustersCatalogKey = metadata.RemoteClusterKeyPrefix
)

// cas value indicating that the entry is supposed to be a new entry in cache
var CAS_NEW_ENTRY int64 = -1

var InvalidRemoteClusterOperationErrorMessage = "Invalid remote cluster operation. "
var InvalidRemoteClusterErrorMessage = "Invalid remote cluster. "
var UnknownRemoteClusterErrorMessage = "unknown remote cluster"
var InvalidConnectionStrError = errors.New("invalid connection string")

// This is used for caching and the cas is used only for the updateCache comparison
type remoteClusterVal struct {
	key                 string
	nodes_connectionstr []string
	ref                 *metadata.RemoteClusterReference
	cas                 int64
}

func (rcv *remoteClusterVal) CAS(obj CacheableMetadataObj) bool {
	if rcv == nil {
		// delete always wins
		return true
	} else if obj == nil {
		if rcv.cas == CAS_NEW_ENTRY {
			// this indicates that we are trying to perform an insert operation
			// the op should be allowed since "obj==nil" indicates that there is indeed no old entry in cache
			rcv.cas = 0
			return true
		} else {
			// this indicates that we are trying to perform an update operation.
			// this is inconsistent with the fact "obj==nil", indicating that this is no old entry in cache
			// the entry to be updated may bave been deleted due to racing
			// do not allow the update operation
			return false
		}
	} else if rcv2, ok := obj.(*remoteClusterVal); ok {
		if rcv.cas == rcv2.cas {
			if !rcv.ref.SameRef(rcv2.ref) {
				// increment cas only when the metadata portion of remoteClusterVal has been changed
				// in other words, concurrent updates to the metadata portion is not allowed -- later write fails
				// while concurrent updates to the runtime portion is allowed -- later write wins
				rcv.cas++
			}
			return true
		} else {
			return false
		}
	} else {
		return false
	}
}

type RemoteClusterService struct {
	metakv_svc        service_def.MetadataSvc
	uilog_svc         service_def.UILogSvc
	xdcr_topology_svc service_def.XDCRCompTopologySvc
	cluster_info_svc  service_def.ClusterInfoSvc
	logger            *log.CommonLogger
	cache             *MetadataCache
	cache_lock        sync.Mutex
	// key = hostname; value = https address of hostname
	httpsAddrMap      map[string]string
	httpsAddrMap_lock sync.Mutex

	metadata_change_callback base.MetadataChangeHandlerCallback
}

func NewRemoteClusterService(uilog_svc service_def.UILogSvc, metakv_svc service_def.MetadataSvc,
	xdcr_topology_svc service_def.XDCRCompTopologySvc, cluster_info_svc service_def.ClusterInfoSvc,
	logger_ctx *log.LoggerContext) (*RemoteClusterService, error) {
	logger := log.NewLogger("RemClusterSvc", logger_ctx)
	svc := &RemoteClusterService{
		metakv_svc:        metakv_svc,
		uilog_svc:         uilog_svc,
		xdcr_topology_svc: xdcr_topology_svc,
		cluster_info_svc:  cluster_info_svc,
		cache:             nil,
		logger:            logger,
		httpsAddrMap:      make(map[string]string),
	}

	svc.initCache()
	return svc, nil
}

func (service *RemoteClusterService) SetMetadataChangeHandlerCallback(call_back base.MetadataChangeHandlerCallback) {
	service.metadata_change_callback = call_back
}

//fill the cache the first time
func (service *RemoteClusterService) initCache() {
	service.cache = NewMetadataCache(service.logger)
}

func (service *RemoteClusterService) getCache() *MetadataCache {
	if service.cache == nil {
		panic("cache is not initialized for RemoteClusterService")
	}
	return service.cache
}

func (service *RemoteClusterService) getCacheVal(refId string) (*remoteClusterVal, error) {
	val, ok := service.getCache().Get(refId)
	if !ok || val == nil {
		return nil, errors.New(UnknownRemoteClusterErrorMessage)
	}

	return val.(*remoteClusterVal), nil
}

func (service *RemoteClusterService) RemoteClusterByRefId(refId string, refresh bool) (*metadata.RemoteClusterReference, error) {
	var err error
	val, err := service.getCacheVal(refId)
	if err != nil && val == nil {
		return nil, errors.New(UnknownRemoteClusterErrorMessage)
	}

	ref := val.ref.Clone()
	if refresh {
		service.refresh(ref)
	}

	return ref, nil
}

func (service *RemoteClusterService) RemoteClusterByRefName(refName string, refresh bool) (*metadata.RemoteClusterReference, error) {
	var ref *metadata.RemoteClusterReference

	ref_map := service.RemoteClusterMap()
	for _, ref_val := range ref_map {
		if ref_val.ref.Name == refName {
			ref = ref_val.ref.Clone()
			break
		}
	}

	if ref == nil {
		return nil, errors.New(UnknownRemoteClusterErrorMessage)
	} else {
		if refresh {
			service.refresh(ref)
		}
		return ref, nil
	}
}

func (service *RemoteClusterService) RemoteClusterByUuid(uuid string, refresh bool) (*metadata.RemoteClusterReference, error) {
	var ref *metadata.RemoteClusterReference

	remote_cluster_map := service.RemoteClusterMap()
	for _, ref_val := range remote_cluster_map {
		if ref_val.ref.Uuid == uuid {
			ref = ref_val.ref.Clone()
			break
		}
	}

	if ref == nil {
		return nil, errors.New(UnknownRemoteClusterErrorMessage)
	} else {
		if refresh {
			service.refresh(ref)
		}
		return ref, nil
	}
}

func (service *RemoteClusterService) AddRemoteCluster(ref *metadata.RemoteClusterReference, skipConnectivityValidation bool) error {
	service.logger.Infof("Adding remote cluster with referenceId %v\n", ref.Id)

	err := service.validateAddRemoteCluster(ref, skipConnectivityValidation)
	if err != nil {
		return err
	}

	err = service.addRemoteCluster(ref)
	if err != nil {
		return err
	}

	if service.uilog_svc != nil {
		uiLogMsg := fmt.Sprintf("Created remote cluster reference \"%s\" via %s.", ref.Name, ref.HostName)
		service.uilog_svc.Write(uiLogMsg)
	}
	return nil
}

func (service *RemoteClusterService) updateRemoteCluster(ref *metadata.RemoteClusterReference) error {

	revision := ref.Revision
	// unset all internal fields before saving to metakv
	ref_clone := ref.CloneForMetakvUpdate()

	key := ref.Id
	value, err := json.Marshal(ref_clone)
	if err != nil {
		return err
	}

	err = service.metakv_svc.SetSensitive(key, value, revision)
	if err != nil {
		return err
	}

	service.logger.Infof("Remote cluster %v in metadata store updated. value=%v, revision before update=%v\n", key, ref_clone, revision)

	return service.updateCacheForRefId(ref.Id)
}

func (service *RemoteClusterService) updateCacheForRefId(refId string) error {
	value, rev, err := service.metakv_svc.Get(refId)
	if err != nil {
		return err
	}
	refInMetakv, err := service.constructRemoteClusterReference(value, rev)
	if err != nil {
		return err
	}

	service.logger.Infof("Updating remote cluster %v in cache after metadata store update. revision after update=%v\n", refId, refInMetakv.Revision)

	return service.updateCache(refId, refInMetakv)
}

func (service *RemoteClusterService) SetRemoteCluster(refName string, ref *metadata.RemoteClusterReference) error {
	oldRef, err := service.RemoteClusterByRefName(refName, false)
	if err != nil {
		return err
	}

	// when SetRemoteCluster is called, ref is a newly constructed ref object with its own id and revision
	// in order for metakv update to succeed, the id and revision needs to be updated to those of the ref in cache
	ref.Id = oldRef.Id
	ref.Revision = oldRef.Revision

	return service.setRemoteCluster(refName, ref)
}

func (service *RemoteClusterService) setRemoteCluster(refName string, ref *metadata.RemoteClusterReference) error {
	service.logger.Infof("Setting remote cluster with refName %v. ref=%v\n", refName, ref)

	oldRef, err := service.RemoteClusterByRefName(refName, false)
	if err != nil {
		return err
	}

	err = service.ValidateSetRemoteCluster(refName, ref)
	if err != nil {
		return err
	}

	err = service.updateRemoteCluster(ref)
	if err != nil {
		return err
	}

	if service.uilog_svc != nil {
		hostnameChangeMsg := ""
		if oldRef.HostName != ref.HostName {
			hostnameChangeMsg = fmt.Sprintf(" New contact point is %s.", ref.HostName)
		}
		uiLogMsg := fmt.Sprintf("Remote cluster reference \"%s\" updated.%s", oldRef.Name, hostnameChangeMsg)
		service.uilog_svc.Write(uiLogMsg)
	}
	return nil
}

func (service *RemoteClusterService) DelRemoteCluster(refName string) (*metadata.RemoteClusterReference, error) {
	service.logger.Infof("Deleting remote cluster with reference name=%v\n", refName)
	ref, err := service.RemoteClusterByRefName(refName, false)
	if err != nil {
		return nil, err
	}

	key := ref.Id

	err = service.metakv_svc.DelWithCatalog(RemoteClustersCatalogKey, key, ref.Revision)
	if err != nil {
		return nil, err
	}

	service.logger.Infof("Remote cluster %v deleted from metadata store\n", key)

	err = service.updateCache(ref.Id, nil)
	if err != nil {
		return nil, err
	}

	if service.uilog_svc != nil {
		uiLogMsg := fmt.Sprintf("Remote cluster reference \"%s\" known via %s removed.", ref.Name, ref.HostName)
		service.uilog_svc.Write(uiLogMsg)
	}
	return ref, nil
}

func (service *RemoteClusterService) RemoteClusters(refresh bool) (map[string]*metadata.RemoteClusterReference, error) {
	service.logger.Debugf("Getting remote clusters")

	remote_cluster_map := service.RemoteClusterMap()

	remote_cluster_map_out := make(map[string]*metadata.RemoteClusterReference)
	for key, ref_val := range remote_cluster_map {
		remote_cluster_map_out[key] = ref_val.ref.Clone()
	}

	if refresh {
		for _, ref := range remote_cluster_map_out {
			service.refresh(ref)
		}
	}

	return remote_cluster_map_out, nil
}

func (service *RemoteClusterService) RemoteClusterMap() map[string]*remoteClusterVal {
	ret := make(map[string]*remoteClusterVal)
	values_map := service.getCache().GetMap()
	for key, val := range values_map {
		ret[key] = val.(*remoteClusterVal)
	}
	return ret
}

// validate that the remote cluster ref itself is valid, and that it does not collide with any of the existing remote clusters.
func (service *RemoteClusterService) ValidateAddRemoteCluster(ref *metadata.RemoteClusterReference) error {
	return service.validateAddRemoteCluster(ref, false)
}

func (service *RemoteClusterService) validateAddRemoteCluster(ref *metadata.RemoteClusterReference, skipConnectivityValidation bool) error {
	oldRef, _ := service.RemoteClusterByRefName(ref.Name, false)

	if oldRef != nil {
		return wrapAsInvalidRemoteClusterOperationError("Duplicate cluster names are not allowed")
	}

	// skip connectivity validation if so specified, e.g., when called from migration service
	if !skipConnectivityValidation {
		bUpdateUuid := (ref.Uuid == "")
		err := service.validateRemoteCluster(ref, bUpdateUuid)
		if err != nil {
			return err
		}
	}

	if ref.Uuid != "" {
		oldRef, _ = service.RemoteClusterByUuid(ref.Uuid, false)
		if oldRef != nil {
			return wrapAsInvalidRemoteClusterOperationError(fmt.Sprintf("Cluster reference to the same cluster already exists under the name `%v`", oldRef.Name))
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

	if oldRef.Uuid != ref.Uuid {
		return wrapAsInvalidRemoteClusterOperationError("The new hostname points to a different remote cluster, which is not allowed.")
	}

	return nil
}

// validate remote cluster info
func (service *RemoteClusterService) ValidateRemoteCluster(ref *metadata.RemoteClusterReference) error {
	return service.validateRemoteCluster(ref, false /*updateUuid*/)
}

// validate remote cluster info and update actual uuid
func (service *RemoteClusterService) validateRemoteCluster(ref *metadata.RemoteClusterReference, updateUUid bool) error {
	if ref.IsEncryptionEnabled() {
		// check if source cluster supports SSL when SSL is specified
		isEnterprise, err := service.xdcr_topology_svc.IsMyClusterEnterprise()
		if err != nil {
			return err
		}

		sourceSSLCompatible, err := service.cluster_info_svc.IsClusterCompatible(service.xdcr_topology_svc, []int{2, 5})
		if err != nil {
			return fmt.Errorf("Failed to get source cluster version information, err=%v\n", err)
		}

		if !isEnterprise || !sourceSSLCompatible {
			return wrapAsInvalidRemoteClusterError("Encryption can only be used in enterprise edition when the entire cluster is running at least 2.5 version of Couchbase Server")
		}
	}

	hostName := utils.GetHostName(ref.HostName)
	port, err := utils.GetPortNumber(ref.HostName)
	if err != nil {
		return wrapAsInvalidRemoteClusterError(fmt.Sprintf("Failed to resolve address for \"%v\". The hostname may be incorrect or not resolvable.", ref.HostName))
	}

	var hostAddr string
	if ref.IsEncryptionEnabled() {
		if ref.HttpsHostName == "" {
			httpsHostAddr, err, isInternalError := utils.HttpsHostAddr(ref.HostName, service.logger)
			if err != nil {
				if isInternalError {
					return err
				} else {
					return wrapAsInvalidRemoteClusterError(fmt.Sprintf("Could not connect to \"%v\" on port %v. This could be due to an incorrect host/port combination or a firewall in place between the servers.", hostName, port))
				}
			}
			// store https host name in ref for later re-use
			ref.HttpsHostName = httpsHostAddr

			// check if target cluster supports SAN certificate and store the info in remote cluster reference
			// note that this check itelf requires a https connection to target, which uses "false" as the value
			// of hasSANInCertificateSupport. After the check hasSANInCertificateSupport will be updated with
			// the correct value and all subsequent https calls will use the correct value
			hasSANInCertificateSupport, err := pipeline_utils.HasSANInCertificateSupport(service.cluster_info_svc, ref)
			if err != nil {
				return wrapAsInvalidRemoteClusterError(fmt.Sprintf("Error checking if target cluster supports SANs in cerificates. err=%v", err))
			}
			ref.SANInCertificate = hasSANInCertificateSupport
		}
		hostAddr = ref.HttpsHostName
	} else {
		hostAddr = ref.HostName
	}

	ref.ActiveHostName = ref.HostName
	ref.ActiveHttpsHostName = ref.HttpsHostName

	var poolsInfo map[string]interface{}
	startTime := time.Now()
	err, statusCode := utils.QueryRestApiWithAuth(hostAddr, base.PoolsPath, false, ref.UserName, ref.Password, ref.Certificate, ref.SANInCertificate, base.MethodGet, "", nil, base.ShortHttpTimeout, &poolsInfo, nil, false, service.logger)
	service.logger.Infof("Result from validate remote cluster call: err=%v, statusCode=%v. time taken=%v\n", err, statusCode, time.Since(startTime))
	if err != nil || statusCode != http.StatusOK {
		if statusCode == http.StatusUnauthorized {
			return wrapAsInvalidRemoteClusterError(fmt.Sprintf("Authentication failed. Verify username and password. Got HTTP status %v from REST call get to %v%v. Body was: []", statusCode, hostAddr, base.PoolsPath))
		} else {
			return service.formErrorFromValidatingRemotehost(ref, hostName, port, err)
		}
	}

	// check if remote cluster has been initialized, i.e., has non-empty pools
	pools, ok := poolsInfo[base.Pools].([]interface{})
	if !ok {
		return wrapAsInvalidRemoteClusterError("Could not get cluster info from remote cluster. Remote cluster may be invalid.")
	}
	if len(pools) == 0 {
		return wrapAsInvalidRemoteClusterError("Remote node is not initialized.")
	}

	if ref.IsEncryptionEnabled() {
		// check if target cluster supports SSL when SSL is specified

		//get isEnterprise from the map
		isEnterprise_remote, ok := poolsInfo[base.IsEnterprise].(bool)
		if !ok {
			isEnterprise_remote = false
		}

		if !isEnterprise_remote {
			return wrapAsInvalidRemoteClusterError("Remote cluster is not enterprise version and does not support SSL.")
		}

		remoteSSLCompatible, err := service.cluster_info_svc.IsClusterCompatible(ref, []int{2, 5})
		if err != nil {
			return wrapAsInvalidRemoteClusterError("Failed to get target cluster version information")
		}

		if !remoteSSLCompatible {
			return wrapAsInvalidRemoteClusterError("Remote cluster has a version lower than 2.5 and does not support SSL.")
		}

		// if ref is half-ssl, validate that target clusters is spock and up
		if !ref.IsFullEncryption() {
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
	if updateUUid {
		actualUuid, ok := poolsInfo[base.RemoteClusterUuid]
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
		ref.Uuid = actualUuidStr
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
		return wrapAsInvalidRemoteClusterError(err.Error())
	}
}

// this internal api differs from AddRemoteCluster in that it does not perform validation
func (service *RemoteClusterService) addRemoteCluster(ref *metadata.RemoteClusterReference) error {
	// unset all internal fields before saving to metakv
	ref_clone := ref.CloneForMetakvUpdate()

	key := ref.Id
	value, err := json.Marshal(ref_clone)
	if err != nil {
		return err
	}

	err = service.metakv_svc.AddSensitiveWithCatalog(RemoteClustersCatalogKey, key, value)
	if err != nil {
		return err
	}

	service.logger.Infof("Remote cluster %v has been added to metadata store. value=%v\n", key, ref_clone)

	return service.updateCacheForRefId(ref.Id)
}

func (service *RemoteClusterService) constructRemoteClusterReference(value []byte, rev interface{}) (*metadata.RemoteClusterReference, error) {
	ref := &metadata.RemoteClusterReference{}
	err := json.Unmarshal(value, ref)
	if err != nil {
		return nil, err
	}
	ref.Revision = rev

	return ref, err
}

func (service *RemoteClusterService) cacheRef(ref *metadata.RemoteClusterReference, old_cas int64) error {
	var nodes_connStrs []string
	var err error

	cache := service.getCache()

	username, password, certificate, sanInCertificate, err := ref.MyCredentials()
	if err != nil {
		return err
	}
	connStr, err := ref.MyConnectionStr()
	if err != nil {
		return err
	}

	// use GetNodeListWithMinInfo API to ensure that it is supported by target cluster, which could be an elastic search cluster
	nodeList, err := utils.GetNodeListWithMinInfo(connStr, username, password, certificate, sanInCertificate, service.logger)
	if err == nil {
		service.logger.Debugf("connStr=%v, nodeList=%v\n", connStr, nodeList)

		nodeNameList, err := utils.GetNodeNameListFromNodeList(nodeList, connStr, service.logger)
		if err != nil {
			service.logger.Errorf("Error getting nodes from target cluster. skipping alternative node computation. ref=%v\n", ref.HostName)
		} else {
			for _, nodeName := range nodeNameList {
				nodes_connStrs = append(nodes_connStrs, nodeName)
			}
		}
		service.logger.Debugf("nodes_connStrs after refresh =%v", nodes_connStrs)

	} else {
		service.logger.Infof("Remote cluster reference %v has a bad connectivity, didn't populate alternative connection strings. err=%v", ref.Id, err)

		err = InvalidConnectionStrError
		//keep the old connection string if it is in cache
		old_cache_obj, ok := cache.Get(ref.Id)
		if ok && old_cache_obj != nil {
			old_cache_val := old_cache_obj.(*remoteClusterVal)
			old_ref := old_cache_val.ref
			if ref.HostName == old_ref.HostName {
				nodes_connStrs = old_cache_val.nodes_connectionstr
			}
		}
		service.logger.Infof("nodes_connStrs from old cache =%v", nodes_connStrs)
	}

	ref_cache := &remoteClusterVal{key: ref.Id,
		nodes_connectionstr: nodes_connStrs,
		ref:                 ref,
		cas:                 old_cas}

	err1 := cache.Upsert(ref.Id, ref_cache)
	if err1 != nil {
		service.logger.Errorf("Error caching remote cluster reference %v. err=%v\n", ref.Id, err1)
		return err1
	}

	service.logger.Debugf("Remote cluster reference %v is cached\n", ref.Id)
	return err
}

func (service *RemoteClusterService) refresh(ref *metadata.RemoteClusterReference) error {
	service.logger.Debugf("Refresh remote cluster reference %v\n", ref.Id)

	ref_cache, _ := service.getCacheVal(ref.Id)
	if ref_cache == nil {
		service.logger.Infof("Stop refreshing ref %v. Ref in cache has been deleted by others\n", ref.Id)
		return service_def.MetadataNotFoundErr
	}

	username, password, certificate, sanInCertificate, err := ref.MyCredentials()
	if err != nil {
		return err
	}

	var connStr, hostName, httpsHostName string
	numberOfNodes := len(ref_cache.nodes_connectionstr)
	if numberOfNodes == 0 {
		// target node list may be empty if goxdcr process has been restarted. populate it with ActiveHostName or HostName
		activeHostName := ref.ActiveHostName
		if len(activeHostName) == 0 {
			activeHostName = ref.HostName
		}
		ref_cache.nodes_connectionstr = append(ref_cache.nodes_connectionstr, activeHostName)
		numberOfNodes = 1
	}

	// randomly pick one node from ref_cache.nodes_connectionstr as the starting point
	// iterate through ref_cache.nodes_connectionstr till we find one connection string that works
	startingIndex := rand.Intn(numberOfNodes)
	index := startingIndex
	first := true
	var nodeNameList []string

	for {
		if !first {
			// increment index
			index++
			if index >= numberOfNodes {
				index = 0
			}
			if index == startingIndex {
				// we have exhausted the entire node list. return error
				errMsg := fmt.Sprintf("Failed to refresh remote cluster reference %v since none of the nodes in target node list is accessible. node list = %v\n", ref.Id, ref_cache.nodes_connectionstr)
				service.logger.Error(errMsg)
				return errors.New(errMsg)
			}
		}

		first = false

		hostName = ref_cache.nodes_connectionstr[index]
		connStr = hostName
		if ref.IsEncryptionEnabled() {
			httpsHostName, err = service.getHttpsAddrFromMap(hostName)
			if err != nil {
				service.logger.Warnf("When refreshing remote cluster reference %v, skipping node %v since received error getting https address. err=%v\n", ref.Id, hostName, err)
				continue
			}
			connStr = httpsHostName
		}

		service.logger.Debugf("Selected node %v to refresh remote cluster reference %v\n", connStr, ref.Id)

		// connect to selected node to retrieve nodes info
		clusterUUID, nodeList, err := utils.GetClusterUUIDAndNodeListWithMinInfo(connStr, username, password, certificate, sanInCertificate, service.logger)
		if err != nil {
			service.logger.Warnf("When refreshing remote cluster reference %v, skipping node %v since it is not accessible. err=%v\n", ref.Id, connStr, err)
			continue
		} else {
			// selected node is accessible
			if clusterUUID != ref.Uuid {
				// if selected node is no longer in target cluster, remove selected node from ref_cache.nodes_connectionstr and update cache
				service.logger.Warnf("Node %v is in a different cluster %v now. Removing it from node list in cache. ref=%v, ref cluster=%v\n", connStr, clusterUUID, ref.Id, ref.Uuid)

				new_nodes_connectionstr := append(ref_cache.nodes_connectionstr[:index], ref_cache.nodes_connectionstr[index+1:]...)
				new_ref_cache := &remoteClusterVal{key: ref.Id,
					nodes_connectionstr: new_nodes_connectionstr,
					ref:                 ref,
					cas:                 ref_cache.cas}

				err = service.getCache().Upsert(ref.Id, new_ref_cache)
				if err != nil {
					service.logger.Warnf("Error removing node %v from node list in cache for remote cluster reference %v. err=%v\n", connStr, ref.Id, err)
				}
				// since we modified ref_cache, it may not be safe to continue processing the old ref_cache.
				// return error to abort the current refresh operation
				return fmt.Errorf("Skipping refreshing remote cluster reference %v since target node %v has been moved to a different cluster\n", ref.Id, connStr)
			}

			if ref.ActiveHostName != hostName {
				// update ActiveHostName to the new selected node if needed
				ref.ActiveHostName = hostName
				ref.ActiveHttpsHostName = httpsHostName
				service.logger.Infof("Replaced ActiveHostName in ref %v with %v and ActiveHttpsHostName with %v\n", ref.Id, hostName, httpsHostName)
			}

			nodeNameList, err = utils.GetNodeNameListFromNodeList(nodeList, connStr, service.logger)
			if err != nil {
				service.logger.Warnf("Error getting node name list for remote cluster reference %v using connection string %v. err=%v\n", ref.Id, connStr, err)
				continue
			}

			new_ref_cache := &remoteClusterVal{key: ref.Id,
				nodes_connectionstr: nodeNameList,
				ref:                 ref,
				cas:                 ref_cache.cas}

			err = service.getCache().Upsert(ref.Id, new_ref_cache)
			if err != nil {
				errMsg := fmt.Sprintf("Skipping refreshing remote cluster reference %v since received error updating cache. err=%v\n", ref.Id, err)
				service.logger.Warn(errMsg)
				// if we receive error updating cache, the remote cluster ref in cache may have been updated by another go-routine
				// additional ref operation is likely not effective or safe. abort the current refresh operation
				return errors.New(errMsg)
			}

			// if cache update succeeds, stop iteration
			break
		}
	}

	// after we are done with cache update, if we have a valid nodeNameList, check if ref.HostName is still in target node list
	if len(nodeNameList) > 0 {
		hostNameStillInTarget := false
		for _, nodeName := range nodeNameList {
			if nodeName == ref.HostName {
				hostNameStillInTarget = true
				break
			}
		}

		if !hostNameStillInTarget {
			err = service.replaceRefHostName(ref.Id)
			if err != nil {
				service.logger.Warnf(err.Error())
			}
		}
	}

	return nil
}

// this method is called when ref.HostName is no longer in target cluster and needs to be replaced
// by an alternative working node in target cluster
func (service *RemoteClusterService) replaceRefHostName(refId string) error {
	service.logger.Infof("Replacing HostName in remote cluster reference %v\n", refId)

	ref_cache, _ := service.getCacheVal(refId)
	if ref_cache == nil {
		service.logger.Infof("Stop replacing HostName in remote cluster reference %v. Ref in cache has been deleted by others\n", refId)
		return service_def.MetadataNotFoundErr
	}

	ref, err := service.RemoteClusterByRefId(refId, false)
	if err != nil {
		return err
	}

	username, password, certificate, sanInCertificate, err := ref.MyCredentials()
	if err != nil {
		return err
	}

	hostNames := simple_utils.DeepCopyStringArray(ref_cache.nodes_connectionstr)
	// sort the node list, so that the selection of the replacement node will be deterministic
	// in other words, if two source nodes performs the selection at the same time,
	// they will get the same replacement node. this way less strain is put on metakv
	sort.Strings(hostNames)

	for _, hostName := range hostNames {
		if hostName == ref.HostName {
			// skip ref.HostName since we would not have come here if it was usable
			continue
		}
		connStr := hostName
		httpsHostName := ""
		if ref.IsEncryptionEnabled() {
			httpsHostName, err = service.getHttpsAddrFromMap(hostName)
			if err != nil {
				service.logger.Warnf("When replacing hostname in remote cluster reference %v, skipping node %v since received error getting https address. err=%v\n", ref.Id, hostName, err)
				continue
			}
			connStr = httpsHostName
		}

		service.logger.Debugf("Selected node %v to refresh remote cluster reference %v\n", connStr, ref.Id)

		// connect to selected node to retrieve nodes info
		clusterUUID, _, err := utils.GetClusterUUIDAndNodeListWithMinInfo(connStr, username, password, certificate, sanInCertificate, service.logger)
		if err != nil {
			service.logger.Warnf("When replacing hostname in remote cluster reference %v, skipping node %v since it is not accessible. err=%v\n", ref.Id, connStr, err)
			continue
		} else {
			// selected node is accessible
			if clusterUUID != ref.Uuid {
				// if the node is not in the target cluster, skip it
				service.logger.Warnf("When replacing hostname in remote cluster reference %v, skipping node %v since it is not in target cluster.\n", ref.Id, connStr)
				continue
			} else {
				// this node can be used as the replacement node
				oldHostName := ref.HostName
				ref.HostName = hostName
				ref.HttpsHostName = httpsHostName
				// changes to ref.HostName needs to be saved to metakv, so that it can be propagated to other nodes
				// call setRemoteCluster() instead of SetRemoteCluster(), so that ref.Revision is preserved
				// if reference in metakv has been updated (e.g., due to update to reference from other source nodes)
				// after ref is retrieved from cache, setRemoteCluster() will fail due to mismatching revision
				// this way we can avoid unnecessary update to metakv
				err = service.setRemoteCluster(ref.Name, ref)
				if err != nil {
					service.logger.Warnf("Error updating hostname in remote cluster reference %v. err=%v\n", ref.Id, err)
				} else {
					service.logger.Infof("Updated hostname in remote cluster reference %v from %v to %v.\n", ref.Id, oldHostName, hostName)
				}
				//  do not continue iteration even if setRemoteCluster fails, since reference in metakv may have been updated.
				return err
			}
		}
	}

	// if we get here, we did not find a usable replacement node and we did not update remote cluster ref
	return fmt.Errorf("Could not find usable node for %v\n", ref.Id)

}

// for ssl enabled ref, working_conn_str is in the form of hostname:sslport:kvport.
// parse out the three components
func parseWorkingConnStr(working_conn_str string) (string, uint16, uint16, error) {
	parts := strings.Split(working_conn_str, base.UrlPortNumberDelimiter)
	if len(parts) != 3 {
		return "", 0, 0, fmt.Errorf("alternative connStr %v is of wrong format", working_conn_str)
	}

	hostname := parts[0]

	sslPort, err := strconv.ParseUint(parts[1], 10, 16)
	if err != nil {
		return "", 0, 0, fmt.Errorf("alternative connStr %v is of wrong format", working_conn_str)
	}

	kvPort, err := strconv.ParseUint(parts[2], 10, 16)
	if err != nil {
		return "", 0, 0, fmt.Errorf("alternative connStr %v is of wrong format", working_conn_str)
	}

	return hostname, uint16(sslPort), uint16(kvPort), nil
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
		return base.UnknownRemoteClusterName
	}
	return remoteClusterRef.Name
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
		newRef, err = service.constructRemoteClusterReference(value, rev)
		if err != nil {
			service.logger.Errorf("Error marshaling remote cluster. value=%v, err=%v\n", string(value), err)
			return err
		}
	}

	refId := GetKeyFromPath(path)

	return service.updateCache(refId, newRef)

}

func (service *RemoteClusterService) updateCache(refId string, newRef *metadata.RemoteClusterReference) error {
	//this ensures that all accesses to the cache in this method are a single atomic operation,
	// this is needed because this method can be called concurrently
	service.cache_lock.Lock()
	defer service.cache_lock.Unlock()

	var oldRef *metadata.RemoteClusterReference
	var oldCas int64
	val, err := service.getCacheVal(refId)
	if err == nil && val != nil {
		oldRef = val.ref
		oldCas = val.cas
	} else {
		oldCas = CAS_NEW_ENTRY
	}

	updated := false
	if newRef == nil {
		if oldRef != nil {
			// remote cluster has been deleted
			service.getCache().Delete(refId)
			updated = true
		}
	} else {
		// remote cluster has been created or updated

		// no need to update cache if newRef is the same as the one already in cache
		if !newRef.SameRef(oldRef) {
			err = service.cacheRef(newRef, oldCas)
			if err == nil {
				updated = true
			} else {
				if err != InvalidConnectionStrError && err != CASMisMatchError {
					return err
				} else {
					// ignore InvalidConnectionStrError and CASMisMatchError
					// since cache is still in valid state in spite of them
					return nil
				}
			}
		}
	}

	if updated && service.metadata_change_callback != nil {
		err = service.metadata_change_callback(refId, oldRef, newRef)
		if err != nil {
			service.logger.Error(err.Error())
		}
	}

	return nil
}

func (service *RemoteClusterService) getHttpsAddrFromMap(hostName string) (string, error) {
	service.httpsAddrMap_lock.Lock()
	defer service.httpsAddrMap_lock.Unlock()

	var httpsHostName string
	var ok bool
	var err error
	httpsHostName, ok = service.httpsAddrMap[hostName]
	if !ok {
		httpsHostName, err, _ = utils.HttpsHostAddr(hostName, service.logger)
		if err != nil {
			return "", err
		}
		service.httpsAddrMap[hostName] = httpsHostName
	}
	return httpsHostName, nil
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
		ref_cache, _ := service.getCacheVal(ref.Id)
		if ref_cache == nil {
			service.logger.Warnf("Error retrieving ref %v from cache. It may have been deleted by others\n", ref.Id)
			return "", service_def.MetadataNotFoundErr
		}
		if len(ref_cache.nodes_connectionstr) == 0 {
			// if host name list is empty, which could be the case when goxdcr process is first started
			// fall back to using ref.activeHostName
			return ref.MyConnectionStr()
		}
		sort.Strings(ref_cache.nodes_connectionstr)
		// capi replication is always non-ssl type, there is no need to construct https addr
		return ref_cache.nodes_connectionstr[0], nil
	}
}
