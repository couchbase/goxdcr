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
	"github.com/couchbase/goxdcr/utils"
	"net/http"
	"reflect"
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
	cache_lock        *sync.Mutex

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
		service.cacheRef(ref, CAS_NEW_ENTRY)
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
		ref, err = service.refresh(ref, val.cas)
	}

	return ref, err
}

func (service *RemoteClusterService) RemoteClusterByRefName(refName string, refresh bool) (*metadata.RemoteClusterReference, error) {
	var ref *metadata.RemoteClusterReference
	var old_cas int64

	ref_map := service.RemoteClusterMap()
	for _, ref_val := range ref_map {
		if ref_val.ref.Name == refName {
			ref = ref_val.ref.Clone()
			old_cas = ref_val.cas
			break
		}
	}

	if ref == nil {
		return nil, errors.New(UnknownRemoteClusterErrorMessage)
	} else {
		var err error
		if refresh {
			ref, err = service.refresh(ref, old_cas)
		}
		return ref, err
	}
}

func (service *RemoteClusterService) RemoteClusterByUuid(uuid string, refresh bool) (*metadata.RemoteClusterReference, error) {
	var ref *metadata.RemoteClusterReference
	var old_cas int64

	remote_cluster_map := service.RemoteClusterMap()
	for _, ref_val := range remote_cluster_map {
		if ref_val.ref.Uuid == uuid {
			ref = ref_val.ref.Clone()
			old_cas = ref_val.cas
			break
		}
	}

	if ref == nil {
		return nil, errors.New(UnknownRemoteClusterErrorMessage)
	} else {
		var err error
		if refresh {
			ref, err = service.refresh(ref, old_cas)
		}
		return ref, err
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

func (service *RemoteClusterService) updateRemoteCluster(ref *metadata.RemoteClusterReference, revision interface{}) error {
	key := ref.Id
	value, err := json.Marshal(ref)
	if err != nil {
		return err
	}
	service.logger.Debugf("Remote cluster is being updated: key=%v, value=%v\n", key, string(value))

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

	return service.updateCache(ref.Id, ref)
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
	cas_map := make(map[string]int64)
	for key, ref_val := range remote_cluster_map {
		remote_cluster_map_out[key] = ref_val.ref.Clone()
		cas_map[key] = ref_val.cas
	}

	if refresh {
		for key, ref := range remote_cluster_map_out {
			ref, err := service.refresh(ref, cas_map[key])
			if err != nil {
				// log the error
				service.logger.Errorf("could not refresh remote cluster reference %v. err=%v\n", ref.Name, err)
			} else {
				remote_cluster_map_out[key] = ref
			}
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
	if ref.DemandEncryption {
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
	if ref.DemandEncryption {
		if ref.HttpsHostName == "" {
			httpsHostAddr, err, isInternalError := service.httpsHostAddr(ref.HostName)
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

	if ref.DemandEncryption {
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
	if !ref.DemandEncryption {
		// if encryption is not on, most likely the error is caused by incorrect hostname or firewall.
		return wrapAsInvalidRemoteClusterError(fmt.Sprintf("Could not connect to \"%v\" on port %v. This could be due to an incorrect host/port combination or a firewall in place between the servers.", hostName, port))
	} else {
		// if encryption is on, several different errors could be returned here, e.g., invalid hostname, invalid certificate, certificate by unknown authority, etc.
		// just return the err
		return wrapAsInvalidRemoteClusterError(err.Error())
	}
}

func (service *RemoteClusterService) httpsHostAddr(hostAddr string) (string, error, bool) {
	hostName := utils.GetHostName(hostAddr)
	sslPort, err, isInternalError := utils.GetSSLPort(hostAddr, service.logger)
	if err != nil {
		return "", err, isInternalError
	}
	return utils.GetHostAddr(hostName, sslPort), nil, false
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

	service.logger.Infof("Remote cluster %v has been added to metadata store.\n", key)

	return service.updateCache(ref.Id, ref)
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

		nodeNameList, err := service.getNodeNameList(nodeList, connStr)
		if err != nil {
			service.logger.Errorf("Error getting nodes from target cluster. skipping alternative node computation. ref=%v\n", ref.HostName)
		} else {
			for _, nodeName := range nodeNameList {
				if nodeName != ref.HostName {
					nodes_connStrs = append(nodes_connStrs, nodeName)
				}
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

func (service *RemoteClusterService) getNodeNameList(nodeList []interface{}, connStr string) ([]string, error) {
	nodeNameList := make([]string, 0)

	for _, node := range nodeList {
		nodeInfoMap, ok := node.(map[string]interface{})
		if !ok {
			errMsg := fmt.Sprintf("node info is not of map type. type of node info=%v", reflect.TypeOf(node))
			service.logger.Error(errMsg)
			return nil, errors.New(errMsg)
		}

		hostAddr, err := utils.GetHostAddrFromNodeInfo(connStr, nodeInfoMap, service.logger)
		if err != nil {
			errMsg := fmt.Sprintf("cannot get hostname from node info %v", nodeInfoMap)
			service.logger.Error(errMsg)
			return nil, errors.New(errMsg)
		}

		nodeNameList = append(nodeNameList, hostAddr)
	}
	return nodeNameList, nil
}

func (service *RemoteClusterService) refresh(ref *metadata.RemoteClusterReference, old_cas int64) (*metadata.RemoteClusterReference, error) {
	service.logger.Debugf("Refresh remote cluster reference %v\n", ref.Id)

	err := service.cacheRef(ref, old_cas)
	if err == nil {
		return ref, nil
	}

	if err == CASMisMatchError {
		// cache has been updated by some other routines. stop refreshing and reload ref
		ref_cache, _ := service.getCacheVal(ref.Id)
		if ref_cache != nil {
			service.logger.Infof("Stop refreshing ref %v. Ref in cache has been updated by others\n", ref.Id)
			return ref_cache.ref, nil
		} else {
			service.logger.Infof("Stop refreshing ref %v. Ref in cache has been deleted by others\n", ref.Id)
			return nil, service_def.MetadataNotFoundErr
		}
	}

	if err != InvalidConnectionStrError {
		return nil, err
	}

	connStr, err := ref.MyConnectionStr()
	if err != nil {
		return nil, err
	}

	username, password, certificate, sanInCertificate, err := ref.MyCredentials()
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
		if ref.DemandEncryption {
			kvPort, err := utils.GetPortNumber(alt_conn_str)
			if err != nil {
				continue
			}
			sslPort, err, _ := utils.GetSSLPort(alt_conn_str, service.logger)
			if err != nil {
				continue
			}

			alt_https_conn_str := utils.GetHostAddr(utils.GetHostName(alt_conn_str), sslPort)
			// even if we could get sslport from the cluster with alt_conn_str, it does not mean that the cluster
			// has been initialized. make another call to /pools/default to make sure
			_, err = utils.GetClusterInfo(alt_https_conn_str, base.DefaultPoolPath, username, password, certificate, sanInCertificate, service.logger)
			if err == nil {
				// for ssl enabled ref, we need both kvport and sslport, we contantenate them
				// in the form of hostname:sslport:kvport
				working_conn_str = utils.GetHostAddr(alt_https_conn_str, kvPort)
				break
			}
		} else {
			_, err := utils.GetClusterInfo(alt_conn_str, base.DefaultPoolPath, username, password, certificate, sanInCertificate, service.logger)
			if err == nil {
				working_conn_str = alt_conn_str
				break
			}
		}
	}

	if working_conn_str != "" {
		service.logger.Infof("Found a working alternative connStr %v", working_conn_str)
		//update the ref in cache
		if ref.DemandEncryption {
			hostname, sslPort, kvPort, err := parseWorkingConnStr(working_conn_str)
			if err != nil {
				return nil, err
			}
			oldHostName := ref.HostName
			ref.HostName = utils.GetHostAddr(hostname, kvPort)
			ref.HttpsHostName = utils.GetHostAddr(hostname, sslPort)
			service.logger.Infof("After refresh, old host name=%v, new host name = %v, new https host name = %v\n", oldHostName, ref.HostName, ref.HttpsHostName)
		} else {
			ref.HostName = working_conn_str
		}
		//persist
		err = service.updateRemoteCluster(ref, ref.Revision)
		if err != nil {
			return nil, err
		}
		return ref, nil
	}

	return nil, errors.New(fmt.Sprintf("Failed to connect to cluster reference %v\n", ref.Id))

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
