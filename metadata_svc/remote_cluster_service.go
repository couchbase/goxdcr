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
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/utils"
	"net/http"
	"reflect"
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
var InvalidConnectionStrErrorMessage = "invalid connection string"

type remoteClusterVal struct {
	key                 string
	nodes_connectionstr []string
	ref                 *metadata.RemoteClusterReference
}

type RemoteClusterService struct {
	metakv_svc               service_def.MetadataSvc
	uilog_svc                service_def.UILogSvc
	xdcr_topology_svc        service_def.XDCRCompTopologySvc
	cluster_info_svc         service_def.ClusterInfoSvc
	logger                   *log.CommonLogger
	cache                    *MetadataCache
	cache_lock               *sync.Mutex
	metadata_change_callback base.MetadataChangeHandlerCallback
}

func NewRemoteClusterService(uilog_svc service_def.UILogSvc, metakv_svc service_def.MetadataSvc,
	xdcr_topology_svc service_def.XDCRCompTopologySvc, cluster_info_svc service_def.ClusterInfoSvc,
	logger_ctx *log.LoggerContext) (*RemoteClusterService, error) {
	logger := log.NewLogger("RemoteClusterService", logger_ctx)
	svc := &RemoteClusterService{
		metakv_svc:        metakv_svc,
		uilog_svc:         uilog_svc,
		xdcr_topology_svc: xdcr_topology_svc,
		cluster_info_svc:  cluster_info_svc,
		cache:             nil,
		cache_lock:        &sync.Mutex{},
		logger:            logger,
	}

	err := svc.initCache()
	if err != nil {
		return nil, err
	}
	return svc, nil
}

func (service *RemoteClusterService) SetMetadataChangeHandlerCallback(call_back base.MetadataChangeHandlerCallback) {
	service.metadata_change_callback = call_back
}

//fill the cache the first time
func (service *RemoteClusterService) initCache() error {
	service.cache = NewMetadataCache(service.logger)

	entries, err := service.metakv_svc.GetAllMetadataFromCatalog(RemoteClustersCatalogKey)
	if err != nil {
		service.logger.Errorf("Failed to get all entries, err=%v\n", err)
		service.cache = nil
		return err
	}

	for _, entry := range entries {
		ref, err := service.constructRemoteClusterReference(entry.Value, entry.Rev)
		if err != nil {
			service.cache = nil
			return err
		}
		service.cacheRef(ref)
	}
	return nil

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
		ref, err = service.refresh(ref)
	}

	return ref, err
}

func (service *RemoteClusterService) RemoteClusterByRefName(refName string, refresh bool) (*metadata.RemoteClusterReference, error) {
	var ref *metadata.RemoteClusterReference
	ref_map, err := service.RemoteClusterMap()
	if err != nil {
		return nil, err
	}
	for _, ref_in_cache := range ref_map {
		if ref_in_cache.Name == refName {
			ref = ref_in_cache.Clone()
			break
		}
	}

	if ref == nil {
		return nil, errors.New(UnknownRemoteClusterErrorMessage)
	} else {
		var err error
		if refresh {
			ref, err = service.refresh(ref)
		}
		return ref, err
	}
}

func (service *RemoteClusterService) RemoteClusterByUuid(uuid string, refresh bool) (*metadata.RemoteClusterReference, error) {
	var ref *metadata.RemoteClusterReference
	remote_cluster_map, err := service.RemoteClusterMap()
	if err != nil {
		return nil, err
	}

	for _, ref_in_cache := range remote_cluster_map {
		if ref_in_cache.Uuid == uuid {
			ref = ref_in_cache.Clone()
			break
		}
	}

	if ref == nil {
		return nil, errors.New(UnknownRemoteClusterErrorMessage)
	} else {
		var err error
		if refresh {
			ref, err = service.refresh(ref)
		}
		return ref, err
	}
}

func (service *RemoteClusterService) AddRemoteCluster(ref *metadata.RemoteClusterReference) error {
	service.logger.Infof("Adding remote cluster with referenceId %v\n", ref.Id)

	err := service.ValidateAddRemoteCluster(ref)
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

func (service *RemoteClusterService) updateRemoteCluster(ref *metadata.RemoteClusterReference, revision interface{}) error {
	key := ref.Id
	value, err := json.Marshal(ref)
	if err != nil {
		return err
	}
	service.logger.Debugf("Remote cluster being changed: key=%v, value=%v\n", key, string(value))

	err = service.metakv_svc.SetSensitive(key, value, revision)
	if err != nil {
		return err
	}

	service.logger.Infof("Remote cluster %v in metadata store updated. value=%v, revision=%v\n", key, value, revision)

	// update ref.Revision
	_, rev, err := service.metakv_svc.Get(key)
	if err != nil {
		return err
	}
	ref.Revision = rev

	service.updateCache(ref.Id, ref)

	return nil
}

func (service *RemoteClusterService) SetRemoteCluster(refName string, ref *metadata.RemoteClusterReference) error {
	service.logger.Infof("Setting remote cluster with refName %v\n", refName)

	err := service.ValidateSetRemoteCluster(refName, ref)
	if err != nil {
		return err
	}

	oldRef, err := service.RemoteClusterByRefName(refName, false)
	if err != nil {
		return err
	}

	// these are critical to ensure a successful update operation on oldRef
	ref.Id = oldRef.Id

	err = service.updateRemoteCluster(ref, oldRef.Revision)
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

	service.updateCache(ref.Id, nil)

	if service.uilog_svc != nil {
		uiLogMsg := fmt.Sprintf("Remote cluster reference \"%s\" known via %s removed.", ref.Name, ref.HostName)
		service.uilog_svc.Write(uiLogMsg)
	}
	return ref, nil
}

func (service *RemoteClusterService) RemoteClusters(refresh bool) (map[string]*metadata.RemoteClusterReference, error) {
	service.logger.Debugf("Getting remote clusters")

	remote_cluster_map, err := service.RemoteClusterMap()
	if err != nil {
		return nil, err
	}

	remote_cluster_map_out := make(map[string]*metadata.RemoteClusterReference)
	for key, ref := range remote_cluster_map {
		remote_cluster_map_out[key] = ref.Clone()
	}

	if refresh {
		for key, ref := range remote_cluster_map_out {
			ref, err := service.refresh(ref)
			if err != nil {
				// log the error
				service.logger.Errorf("could not refresh remote cluster reference %v.\n", ref.Name)
			} else {
				remote_cluster_map_out[key] = ref
			}
		}
	}

	return remote_cluster_map_out, nil
}

func (service *RemoteClusterService) RemoteClusterMap() (map[string]*metadata.RemoteClusterReference, error) {
	ret := make(map[string]*metadata.RemoteClusterReference)
	values_map := service.getCache().GetMap()
	for key, val := range values_map {
		cachedVal := val.(*remoteClusterVal)
		ret[key] = cachedVal.ref
	}
	return ret, nil
}

// validate that the remote cluster ref itself is valid, and that it does not collide with any of the existing remote clusters.
func (service *RemoteClusterService) ValidateAddRemoteCluster(ref *metadata.RemoteClusterReference) error {
	oldRef, _ := service.RemoteClusterByRefName(ref.Name, false)

	if oldRef != nil {
		return wrapAsInvalidRemoteClusterOperationError(errors.New("duplicate cluster names are not allowed"))
	}

	bUpdateUuid := (ref.Uuid == "")
	err := service.validateRemoteCluster(ref, bUpdateUuid)
	if err != nil {
		return err
	}

	oldRef, err = service.RemoteClusterByUuid(ref.Uuid, false)
	if oldRef != nil {
		return wrapAsInvalidRemoteClusterOperationError(fmt.Errorf("Cluster reference to the same cluster already exists under the name `%v`", oldRef.Name))
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
		return wrapAsInvalidRemoteClusterOperationError(errors.New("The new hostname points to a different remote cluster, which is not allowed."))
	}

	return nil
}

// validate remote cluster info and retrieve actual uuid
func (service *RemoteClusterService) validateRemoteCluster(ref *metadata.RemoteClusterReference, updateUUid bool) error {
	isEnterprise, err := service.xdcr_topology_svc.IsMyClusterEnterprise()
	if err != nil {
		return err
	}
	if ref.DemandEncryption && !isEnterprise {
		return wrapAsInvalidRemoteClusterError(errors.New("encryption can only be used in enterprise edition when the entire cluster is running at least 2.5 version of Couchbase Server"))
	}

	hostName := utils.GetHostName(ref.HostName)
	port, err := utils.GetPortNumber(ref.HostName)
	if err != nil {
		return wrapAsInvalidRemoteClusterError(errors.New(fmt.Sprintf("Failed to resolve address for \"%v\". The hostname may be incorrect or not resolvable.", ref.HostName)))
	}

	var hostAddr string
	var isInternalError bool
	if ref.DemandEncryption {
		hostAddr, err, isInternalError = service.httpsHostAddress(ref.HostName, ref.UserName, ref.Password)
		if err != nil {
			if isInternalError {
				return err
			} else {
				return wrapAsInvalidRemoteClusterError(errors.New(fmt.Sprintf("Could not connect to \"%v\" on port %v. This could be due to an incorrect host/port combination or a firewall in place between the servers.", hostName, port)))
			}
		}
	} else {
		hostAddr = ref.HostName
	}
	var poolsInfo map[string]interface{}

	if ref.DemandEncryption {
		hostAddr = utils.EnforcePrefix("https://", hostAddr)
	} else {
		hostAddr = utils.EnforcePrefix("http://", hostAddr)
	}

	startTime := time.Now()
	err, statusCode, _ := utils.InvokeRestWithRetryWithAuth(hostAddr, base.PoolsPath, false, ref.UserName, ref.Password, ref.Certificate, false, base.MethodGet, "", nil, 0, &poolsInfo, nil, false, service.logger, base.HTTP_RETRIES)
	service.logger.Infof("Result from validate remote cluster call: err=%v, statusCode=%v. time taken=%v\n", err, statusCode, time.Since(startTime))
	if err != nil || statusCode != http.StatusOK {
		if statusCode == http.StatusUnauthorized {
			return wrapAsInvalidRemoteClusterError(errors.New(fmt.Sprintf("Authentication failed. Verify username and password. Got HTTP status %v from REST call get to %v%v. Body was: []", statusCode, hostAddr, base.PoolsPath)))
		} else {
			return service.formErrorFromValidatingRemotehost(ref, hostName, port, err)
		}
	}

	//get isEnterprise from the map
	isEnterprise_remote, ok := poolsInfo[base.IsEnterprise].(bool)
	if !ok {
		isEnterprise_remote = false
	}

	if ref.DemandEncryption && !isEnterprise_remote {
		return wrapAsInvalidRemoteClusterError(fmt.Errorf("Remote cluster %v is not enterprise version, so no SSL support", hostAddr))
	}
	// get remote cluster uuid from the map
	if updateUUid {
		actualUuid, ok := poolsInfo[base.RemoteClusterUuid]
		if !ok {
			// should never get here
			return errors.New("Could not get uuid of remote cluster.")
		}
		actualUuidStr, ok := actualUuid.(string)
		if !ok {
			// should never get here
			return errors.New(fmt.Sprintf("uuid of remote cluster is of wrong type. Expected type: string; Actual type: %s", reflect.TypeOf(actualUuid)))
		}

		// update uuid in ref to real value
		ref.Uuid = actualUuidStr
	}
	return nil
}

func (service *RemoteClusterService) formErrorFromValidatingRemotehost(ref *metadata.RemoteClusterReference, hostName string, port uint16, err error) error {
	if !ref.DemandEncryption {
		// if encryption is not on, most likely the error is caused by incorrect hostname or firewall.
		return wrapAsInvalidRemoteClusterError(errors.New(fmt.Sprintf("Could not connect to \"%v\" on port %v. This could be due to an incorrect host/port combination or a firewall in place between the servers.", hostName, port)))
	} else {
		// if encryption is on, several different errors could be returned here, e.g., invalid hostname, invalid certificate, certificate by unknown authority, etc.
		// just return the err
		return wrapAsInvalidRemoteClusterError(err)
	}
}

func (service *RemoteClusterService) httpsHostAddress(hostName, userName, password string) (string, error, bool) {
	sslPort, err, isInternalError := utils.GetXDCRSSLPort(hostName, userName, password, service.logger)
	if err != nil {
		return "", err, isInternalError
	}

	hostNode := strings.Split(hostName, base.UrlPortNumberDelimiter)[0]
	newHostName := utils.GetHostAddr(hostNode, sslPort)
	return newHostName, nil, false
}

// this internal api differs from AddRemoteCluster in that it does not perform validation
func (service *RemoteClusterService) addRemoteCluster(ref *metadata.RemoteClusterReference) error {
	key := ref.Id
	value, err := json.Marshal(ref)
	if err != nil {
		return err
	}

	err = service.metakv_svc.AddSensitiveWithCatalog(RemoteClustersCatalogKey, key, value)
	if err != nil {
		return err
	}

	service.logger.Infof("Remote cluster %v added to metadata store. value=%v\n", key, value)

	service.updateCache(ref.Id, ref)

	return nil
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

func (service *RemoteClusterService) cacheRef(ref *metadata.RemoteClusterReference) error {
	var old_cache_val *remoteClusterVal
	var nodes_connStrs []string
	var err error

	cache := service.getCache()

	username, password, err := ref.MyCredentials()
	if err != nil {
		return err
	}
	connStr, err := ref.MyConnectionStr()
	if err != nil {
		return err
	}

	pool, err := utils.RemotePool(connStr, username, password)
	if err == nil {
		service.logger.Debugf("Pool.Nodes=%v\n", pool.Nodes)

		for _, node := range pool.Nodes {
			if node.Hostname != connStr {
				nodes_connStrs = append(nodes_connStrs, node.Hostname)
			}
		}
	} else {
		err = errors.New(InvalidConnectionStrErrorMessage)
		//keep the old connection string if it is in cache
		old_cache_obj, ok := cache.Get(ref.Id)
		if ok && old_cache_obj != nil {
			old_cache_val = old_cache_obj.(*remoteClusterVal)
		}
		if old_cache_val != nil {
			old_ref := old_cache_val.ref
			if ref.HostName == old_ref.HostName {
				nodes_connStrs = old_cache_val.nodes_connectionstr
			}
		}

		service.logger.Infof("Remote cluster reference %v has a bad connectivity, didn't populate alternative connection strings. err=%v", ref.Id, err)
	}

	ref_cache := &remoteClusterVal{key: ref.Id,
		nodes_connectionstr: nodes_connStrs,
		ref:                 ref}

	cache.Upsert(ref.Id, ref_cache)
	service.logger.Debugf("Remote cluster reference %v is cached\n", ref.Id)

	return err
}

func (service *RemoteClusterService) refresh(ref *metadata.RemoteClusterReference) (*metadata.RemoteClusterReference, error) {
	service.logger.Debugf("Refresh remote cluster reference %v\n", ref.Id)

	err := service.cacheRef(ref)
	if err == nil {
		return ref, nil
	}

	if err.Error() != InvalidConnectionStrErrorMessage {
		return nil, err
	}

	connStr, err := ref.MyConnectionStr()
	if err != nil {
		return nil, err
	}

	username, password, err := ref.MyCredentials()
	if err != nil {
		return nil, err
	}

	service.logger.Infof("Connstr %v in remote cluster reference failed to connect. Try to use alternative connStr", connStr)

	ref_cache, err := service.getCacheVal(ref.Id)
	if err != nil {
		service.logger.Errorf("Failed to connect to cluster reference %v - %v doesn't work, no alternative connStr\n", ref.Id, connStr)
		return nil, errors.New(fmt.Sprintf("Failed to connect to cluster reference %v\n", ref.Id))

	}

	service.logger.Debugf("ref_cache=%v\n", ref_cache)

	var working_conn_str string = ""
	for _, alt_conn_str := range ref_cache.nodes_connectionstr {
		_, err = utils.RemotePool(alt_conn_str, username, password)
		if err == nil {
			working_conn_str = alt_conn_str
			break
		}
	}

	if working_conn_str != "" {
		service.logger.Infof("Found a working alternative connStr %v", working_conn_str)
		//update the ref in cache
		ref.HostName = working_conn_str
		//persist
		err = service.updateRemoteCluster(ref, ref.Revision)
		if err != nil {
			return nil, err
		}
		return ref, nil
	}

	return nil, errors.New(fmt.Sprintf("Failed to connect to cluster reference %v\n", ref.Id))

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
func wrapAsInvalidRemoteClusterError(err error) error {
	return errors.New(InvalidRemoteClusterErrorMessage + err.Error())
}

// wrap/mark an error as invalid remote cluster operation error - by adding "invalid remote cluster operation" message to the front
func wrapAsInvalidRemoteClusterOperationError(err error) error {
	return errors.New(InvalidRemoteClusterOperationErrorMessage + err.Error())
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

	service.updateCache(refId, newRef)

	return nil
}

func (service *RemoteClusterService) updateCache(refId string, newRef *metadata.RemoteClusterReference) {
	//this ensures that all accesses to the cache in this method are a single atomic operation,
	// this is needed because this method can be called concurrently
	service.cache_lock.Lock()
	defer service.cache_lock.Unlock()

	var oldRef *metadata.RemoteClusterReference
	val, err := service.getCacheVal(refId)
	if err == nil && val != nil {
		oldRef = val.ref
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
			service.cacheRef(newRef)
			updated = true
		}
	}

	if updated && service.metadata_change_callback != nil {
		err := service.metadata_change_callback(refId, oldRef, newRef)
		if err != nil {
			service.logger.Error(err.Error())
		}
	}
}
