// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

// metadata service implementation leveraging gometa
package service_impl

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/utils"
	"strings"
)

const (
	// parent dir of all Replication Specs
	ReplicationSpecsCatalogKey = "replicationSpec"
)

var ReplicationSpecAlreadyExistErrorMessage = "Replication to the same remote cluster and bucket already exists"
var ReplicationSpecNotFoundErrorMessage = "Requested resource not found"
var InvalidReplicationSpecError = errors.New("Invalid Replication spec")

type ReplicationSpecService struct {
	xdcr_comp_topology_svc service_def.XDCRCompTopologySvc
	metadata_svc           service_def.MetadataSvc
	uilog_svc              service_def.UILogSvc
	remote_cluster_svc     service_def.RemoteClusterSvc
	logger                 *log.CommonLogger
}

func NewReplicationSpecService(uilog_svc service_def.UILogSvc, remote_cluster_svc service_def.RemoteClusterSvc, metadata_svc service_def.MetadataSvc, xdcr_comp_topology_svc service_def.XDCRCompTopologySvc, logger_ctx *log.LoggerContext) *ReplicationSpecService {
	return &ReplicationSpecService{
		metadata_svc:           metadata_svc,
		uilog_svc:              uilog_svc,
		remote_cluster_svc:     remote_cluster_svc,
		xdcr_comp_topology_svc: xdcr_comp_topology_svc,
		logger:                 log.NewLogger("ReplicationSpecService", logger_ctx),
	}
}

func (service *ReplicationSpecService) ReplicationSpec(replicationId string) (*metadata.ReplicationSpecification, error) {
	result, rev, err := service.metadata_svc.Get(getKeyFromReplicationId(replicationId))
	if err != nil {
		return nil, err
	}
	return constructReplicationSpec(result, rev)
}

func (service *ReplicationSpecService) ValidateNewReplicationSpec(sourceBucket, targetCluster, targetBucket string) map[string]error {
	service.logger.Infof("Start ValidateAddReplicationSpec, sourceBucket=%v, targetCluster=%v, targetBucket=%v\n", sourceBucket, targetCluster, targetBucket)

	errorMap := make(map[string]error)

	//validate the existence of source bucket
	local_connStr, _ := service.xdcr_comp_topology_svc.MyConnectionStr()
	if local_connStr == "" {
		panic("XDCRTopologySvc.MyConnectionStr() should not return empty string")
	}
	_, err_source := utils.LocalBucketUUID(local_connStr, sourceBucket)

	if err_source == utils.NonExistentBucketError {
		service.logger.Errorf("Spec [sourceBucket=%v, targetClusterUuid=%v, targetBucket=%v] refers to non-existent bucket\n", sourceBucket, targetCluster, targetBucket)
		errorMap[base.ReplicationDocSource] = utils.NonExistentBucketError
	}

	// validate remote cluster ref
	targetClusterRef, err := service.remote_cluster_svc.RemoteClusterByRefName(targetCluster, false)
	if err != nil {
		errorMap["remote cluster"] = utils.NewEnhancedError("cannot find remote cluster", err)
		return errorMap
	}

	remote_connStr, err := targetClusterRef.MyConnectionStr()
	if err != nil {
		errorMap["remote cluster"] = utils.NewEnhancedError("invalid remote cluter, MyConnectionStr() failed.", err)
		return errorMap
	}
	remote_userName, remote_password, err := targetClusterRef.MyCredentials()
	if err != nil {
		errorMap["remote cluster"] = utils.NewEnhancedError("invalid remote cluter, MyCredentials() failed.", err)
		return errorMap
	}

	//validate target bucket
	_, err_target := utils.RemoteBucketUUID(remote_connStr, remote_userName, remote_password, targetBucket)
	if err_target == utils.NonExistentBucketError {
		service.logger.Errorf("Spec [sourceBucket=%v, targetClusterUuid=%v, targetBucket=%v] refers to non-existent target bucket\n", sourceBucket, targetCluster, targetBucket)
		errorMap[base.ReplicationDocTarget] = utils.NonExistentBucketError
	}

	repId := metadata.ReplicationId(sourceBucket, targetClusterRef.Uuid, targetBucket)
	_, err = service.ReplicationSpec(repId)
	if err == nil {
		errorMap[base.PlaceHolderFieldKey] = errors.New(ReplicationSpecAlreadyExistErrorMessage)
	}

	return errorMap
}

func (service *ReplicationSpecService) AddReplicationSpec(spec *metadata.ReplicationSpecification) error {
	service.logger.Infof("Start AddReplicationSpec, spec=%v\n", spec)

	value, err := json.Marshal(spec)
	if err != nil {
		return err
	}

	service.logger.Info("Adding it to metadata store...")
	err = service.metadata_svc.AddWithCatalog(ReplicationSpecsCatalogKey, getKeyFromReplicationId(spec.Id), value)
	if err != nil {
		return err
	}
	service.logger.Info("log it with ale logger...")
	service.writeUiLog(spec, "created")
	service.logger.Info("Done with logging...")
	return nil
}

func (service *ReplicationSpecService) SetReplicationSpec(spec *metadata.ReplicationSpecification) error {
	value, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	return service.metadata_svc.Set(getKeyFromReplicationId(spec.Id), value, spec.Revision)
}

func (service *ReplicationSpecService) DelReplicationSpec(replicationId string) (*metadata.ReplicationSpecification, error) {
	spec, err := service.ReplicationSpec(replicationId)
	if err != nil {
		return nil, errors.New(ReplicationSpecNotFoundErrorMessage)
	}

	err = service.metadata_svc.DelWithCatalog(ReplicationSpecsCatalogKey, getKeyFromReplicationId(replicationId), spec.Revision)
	if err != nil {
		return nil, err
	}

	service.writeUiLog(spec, "removed")
	return spec, nil
}

func (service *ReplicationSpecService) AllReplicationSpecs() (map[string]*metadata.ReplicationSpecification, error) {
	specs := make(map[string]*metadata.ReplicationSpecification, 0)

	entries, err := service.metadata_svc.GetAllMetadataFromCatalog(ReplicationSpecsCatalogKey)
	if err != nil {
		service.logger.Errorf("Failed to get all entries, err=%v\n", err)
		return nil, err
	}

	for _, entry := range entries {
		spec, err := constructReplicationSpec(entry.Value, entry.Rev)
		if err != nil {
			return nil, err
		}
		specs[entry.Key] = spec
	}

	return specs, nil
}

func (service *ReplicationSpecService) AllReplicationSpecIds() ([]string, error) {
	repIds := make([]string, 0)
	keys, err := service.metadata_svc.GetAllKeysFromCatalog(ReplicationSpecsCatalogKey)
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		repIds = append(repIds, service.getReplicationIdFromKey(key))
	}
	return repIds, nil
}

func (service *ReplicationSpecService) AllReplicationSpecIdsForBucket(bucket string) ([]string, error) {
	var repIds []string
	allRepIds, err := service.AllReplicationSpecIds()
	if err != nil {
		return nil, err
	}

	if allRepIds != nil {
		for _, repId := range allRepIds {
			if metadata.IsReplicationIdForSourceBucket(repId, bucket) {
				repIds = append(repIds, repId)
			}
		}
	}
	return repIds, nil
}

func constructReplicationSpec(value []byte, rev interface{}) (*metadata.ReplicationSpecification, error) {
	if value == nil {
		return nil, nil
	}

	spec := &metadata.ReplicationSpecification{}
	err := json.Unmarshal(value, spec)
	if err != nil {
		return nil, err
	}
	spec.Revision = rev
	return spec, nil
}

// Implement callback function for metakv
func (service *ReplicationSpecService) ReplicationSpecServiceCallback(path string, value []byte, rev interface{}) (string, interface{}, interface{}, error) {
	service.logger.Infof("ReplicationSpecServiceCallback called on path = %v\n", path)

	spec, err := constructReplicationSpec(value, rev)
	if err != nil {
		service.logger.Errorf("Error marshaling replication spec. value=%v, err=%v\n", string(value), err)
		return "", nil, nil, err
	}

	return service.getReplicationIdFromKey(GetKeyFromPath(path)), nil, spec, nil

}

func (service *ReplicationSpecService) writeUiLog(spec *metadata.ReplicationSpecification, action string) {
	if service.uilog_svc != nil {
		remoteClusterName := service.remote_cluster_svc.GetRemoteClusterNameFromClusterUuid(spec.TargetClusterUUID)

		uiLogMsg := fmt.Sprintf("Replication from bucket \"%s\" to bucket \"%s\" on cluster \"%s\" %s.", spec.SourceBucketName, spec.TargetBucketName, remoteClusterName, action)
		service.uilog_svc.Write(uiLogMsg)
	}
}

func (service *ReplicationSpecService) IsReplicationValidationError(err error) bool {
	if err != nil {
		return strings.HasPrefix(err.Error(), ReplicationSpecAlreadyExistErrorMessage) || strings.HasPrefix(err.Error(), ReplicationSpecNotFoundErrorMessage)
	} else {
		return false
	}
}

func getKeyFromReplicationId(replicationId string) string {
	return ReplicationSpecsCatalogKey + base.KeyPartsDelimiter + replicationId
}

func (service *ReplicationSpecService) getReplicationIdFromKey(key string) string {
	prefix := ReplicationSpecsCatalogKey + base.KeyPartsDelimiter
	if !strings.HasPrefix(key, prefix) {
		// should never get here.
		panic(fmt.Sprintf("Got unexpected key %v for replication spec", key))
	}
	return key[len(prefix):]
}

func (service *ReplicationSpecService) ValidateExistingReplicationSpec(spec *metadata.ReplicationSpecification) error {
	//validate the existence of source bucket
	local_connStr, _ := service.xdcr_comp_topology_svc.MyConnectionStr()
	if local_connStr == "" {
		panic("XDCRTopologySvc.MyConnectionStr() should not return empty string")
	}
	sourceBucketUuid, err_source := utils.LocalBucketUUID(local_connStr, spec.SourceBucketName)

	if err_source == utils.NonExistentBucketError {
		service.logger.Errorf("Spec %v refers to non-existent bucket %v\n", spec.Id, spec.SourceBucketName)
		return InvalidReplicationSpecError
	}

	if spec.SourceBucketUUID != "" && spec.SourceBucketUUID != sourceBucketUuid {
		//spec is referring to a deleted bucket
		service.logger.Errorf("Spec %v refers to bucket %v which was deleted and recreated\n", spec.Id, spec.SourceBucketName)
		return InvalidReplicationSpecError
	}

	//validate target cluster
	targetClusterRef, err := service.remote_cluster_svc.RemoteClusterByUuid(spec.TargetClusterUUID, false)
	if err == service_def.MetadataNotFoundErr {
		//remote cluster is no longer valid
		service.logger.Errorf("Spec %v refers to non-existent remote cluster reference %v\n", spec.Id, spec.TargetClusterUUID)
		return InvalidReplicationSpecError
	} else if err != nil {
		return err
	}

	remote_connStr, err := targetClusterRef.MyConnectionStr()
	if err != nil {
		service.logger.Errorf("Spec %v refers to an invalid remote cluster reference %v, as RemoteClusterRef.MyConnectionStr() returns err=%v\n", spec.Id, spec.TargetClusterUUID, err)
		return InvalidReplicationSpecError
	}
	remote_userName, remote_password, err := targetClusterRef.MyCredentials()
	if err != nil {
		service.logger.Errorf("Spec %v refers to an invalid remote cluster reference %v, as RemoteClusterRef.MyCredentials() returns err=%v\n", spec.Id, spec.TargetClusterUUID, err)
		return InvalidReplicationSpecError
	}

	//validate target bucket
	targetBucketUuid, err_target := utils.RemoteBucketUUID(remote_connStr, remote_userName, remote_password, spec.TargetBucketName)
	if err_target == utils.NonExistentBucketError {
		service.logger.Errorf("Spec %v refers to non-existent bucket %v\n", spec.Id, spec.TargetBucketName)
		return InvalidReplicationSpecError
	}

	if spec.TargetBucketUUID != "" && spec.TargetBucketUUID != targetBucketUuid {
		//spec is referring to a deleted bucket
		service.logger.Errorf("Spec %v refers to bucket %v which was deleted and recreated\n", spec.Id, spec.TargetBucketName)
		return InvalidReplicationSpecError
	}
	return nil
}

func (service *ReplicationSpecService) ValidateAndGC(spec *metadata.ReplicationSpecification) {
	err := service.ValidateExistingReplicationSpec(spec)
	if err == InvalidReplicationSpecError {
		service.logger.Errorf("Replication specification %v is no longer valid, garbage collect it", spec.Id)
		_, err1 := service.DelReplicationSpec(spec.Id)
		if err1 != nil {
			service.logger.Infof("Failed to garbage collect spec %v, err=%v\n", spec.Id, err1)
		}
	}
}

func (service *ReplicationSpecService) sourceBucketUUID(bucketName string) (string, error) {
	local_connStr, _ := service.xdcr_comp_topology_svc.MyConnectionStr()
	if local_connStr == "" {
		panic("XDCRTopologySvc.MyConnectionStr() should not return empty string")
	}
	return utils.LocalBucketUUID(local_connStr, bucketName)
}

func (service *ReplicationSpecService) targetBucketUUID(targetClusterUUID, bucketName string) (string, error) {
	ref, err_target := service.remote_cluster_svc.RemoteClusterByUuid(targetClusterUUID, false)
	if err_target != nil {
		return "", err_target
	}
	remote_connStr, err_target := ref.MyConnectionStr()
	if err_target != nil {
		return "", err_target
	}
	remote_userName, remote_password, err_target := ref.MyCredentials()
	if err_target != nil {
		return "", err_target
	}

	return utils.RemoteBucketUUID(remote_connStr, remote_userName, remote_password, bucketName)
}

func (service *ReplicationSpecService) ConstructNewReplicationSpec(sourceBucketName string, targetClusterUUID string, targetBucketName string) (*metadata.ReplicationSpecification, error) {
	sourceBucketUUID, err := service.sourceBucketUUID(sourceBucketName)
	if err != nil {
		return nil, err
	}

	targetBucketUUID, err := service.targetBucketUUID(targetClusterUUID, targetBucketName)
	if err != nil {
		return nil, err
	}

	spec := metadata.NewReplicationSpecification(sourceBucketName, sourceBucketUUID, targetClusterUUID, targetBucketName, targetBucketUUID)
	return spec, nil
}
