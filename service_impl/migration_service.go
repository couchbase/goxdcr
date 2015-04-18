// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package service_impl

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	base "github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/utils"
	"os"
	"reflect"
	"strconv"
	"strings"
)

const (
	// keys used in json map
	RemoteClustersKey      = "remoteClusters"
	ReplicationDocsKey     = "replicationDocs"
	CheckpointsKey         = "checkpoints"
	ReplicationSettingsKey = "replicationSettings"
)

const (
	// metadata type names
	TypeRemoteCluster       = "remote cluster"
	TypeReplicationDoc      = "replication document"
	TypeCheckpoint          = "checkpoint"
	TypeReplicationSettings = "replication settings"
)

const (
	// erlang xdcr specific replication settings
	SettingWorkerProcesses   = "worker_processes"
	SettingMaxConcurrentReps = "max_concurrent_reps"

	// in the unlikely event that default settings are not migrated,
	// the following default values will be used
	SettingWorkerProcessesDefault   = 4
	SettingMaxConcurrentRepsDefault = 16
)

// checkpoint field names
const (
	CheckpointDocId               = "checkpointDocId"
	CheckpointCommitOpaque        = "commitopaque"
	CheckpointFailoverUuid        = "failover_uuid"
	CheckpointSeqno               = "seqno"
	CheckpointDCPSnapshotSeqno    = "dcp_snapshot_seqno"
	CheckpointDCPSnapshotEndSeqno = "dcp_snapshot_end_seqno"
	CheckpointTargetVbUuid        = "target_vb_uuid"
)

var CheckpointDocIdPrefix = "_local/30-ck-"

// the value indicates that the passed in value is not a valid int
var InvalidIntValue int = -1

// the value indicates that no value is passed in at all
var NonExistentIntValue int = -2

type MigrationSvc struct {
	xdcr_comp_topology_svc   service_def.XDCRCompTopologySvc
	remote_cluster_svc       service_def.RemoteClusterSvc
	repl_spec_svc            service_def.ReplicationSpecSvc
	replication_settings_svc service_def.ReplicationSettingsSvc
	checkpoints_svc          service_def.CheckpointsService
	logger                   *log.CommonLogger
}

func NewMigrationSvc(xdcr_comp_topology_svc service_def.XDCRCompTopologySvc, remote_cluster_svc service_def.RemoteClusterSvc, repl_spec_svc service_def.ReplicationSpecSvc,
	replication_settings_svc service_def.ReplicationSettingsSvc, checkpoints_svc service_def.CheckpointsService,
	loggerCtx *log.LoggerContext) *MigrationSvc {
	service := &MigrationSvc{
		xdcr_comp_topology_svc:   xdcr_comp_topology_svc,
		remote_cluster_svc:       remote_cluster_svc,
		repl_spec_svc:            repl_spec_svc,
		replication_settings_svc: replication_settings_svc,
		checkpoints_svc:          checkpoints_svc,
		logger:                   log.NewLogger("MigrationService", loggerCtx),
	}

	service.logger.Infof("Created Migration service.\n")
	return service
}

func (service *MigrationSvc) Migrate() error {
	data, err := service.readMetadataFromStdin()
	if err != nil {
		return err
	}

	errorList := service.migrate_internal(data)
	if len(errorList) == 0 {
		service.logger.Info("Metadata migration completed without errors")
		return nil
	} else {
		service.logger.Errorf("Errors from migration: %v\n", errorList)
		return errors.New("Error occured when migration metadata.")
	}
}

// in upgrade mode, ns_server will send metadata that needs to be migrated to
// goxdcr through stdin. read them
func (service *MigrationSvc) readMetadataFromStdin() ([]byte, error) {
	service.logger.Infof("Starting to read upgrade metadata from stdin")

	reader := bufio.NewReader(os.Stdin)
	dataBytes := make([]byte, 0)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			service.logger.Infof("Error reading metadata from stdin. err=%v\n", err)
			return nil, err
		}

		if len(line) == 1 {
			break
		} else {
			dataBytes = append(dataBytes, line[:len(line)-1]...)
		}
	}
	return dataBytes, nil
}

func (service *MigrationSvc) migrate_internal(data []byte) []error {
	service.logger.Info("Starting to migrate xdcr metadata")

	errorList := make([]error, 0)

	if len(data) == 0 {
		return errorList
	}

	dataObj := make(map[string]interface{})
	err := json.Unmarshal(data, &dataObj)
	if err != nil {
		errorList = append(errorList, utils.NewEnhancedError(fmt.Sprintf("Error unmarshaling metadata"), err))
		// fetal error. stop right away
		return errorList
	}

	deletedRemoteClusterUuidList := make([]string, 0)
	remoteClustersData, ok := dataObj[RemoteClustersKey]
	if ok {
		var indErrorList []error
		deletedRemoteClusterUuidList, indErrorList = service.migrateRemoteClusters(remoteClustersData)
		if len(indErrorList) != 0 {
			errorList = append(errorList, indErrorList...)
		}
	}

	defaultWorkerProcesses := InvalidIntValue
	defaultMaxConcurrentReps := InvalidIntValue
	replSettingsData, ok := dataObj[ReplicationSettingsKey]
	if ok {
		var indErrorList []error
		defaultWorkerProcesses, defaultMaxConcurrentReps, indErrorList = service.migrateReplicationSettings(replSettingsData)
		if len(indErrorList) != 0 {
			errorList = append(errorList, indErrorList...)
			// if any error has occured, the erlang default settings have not been saved
			// do not proceed to migrate replications, since they may have incorrect settings
			return errorList
		}
	}

	replDocsData, ok := dataObj[ReplicationDocsKey]
	if ok {
		indErrorList := service.migrateReplicationDocs(replDocsData, deletedRemoteClusterUuidList, defaultWorkerProcesses, defaultMaxConcurrentReps)
		if len(indErrorList) != 0 {
			errorList = append(errorList, indErrorList...)
		}
	}

	return errorList
}

func (service *MigrationSvc) migrateRemoteClusters(remoteClustersData interface{}) ([]string, []error) {
	service.logger.Info("Starting to migrate remote clusters")

	deletedRemoteClusterUuidList := make([]string, 0)
	errorList := make([]error, 0)

	if remoteClustersData == nil {
		return deletedRemoteClusterUuidList, errorList
	}

	remoteClusterArr, ok := remoteClustersData.([]interface{})
	if !ok {
		err := incorrectMetadataValueTypeError(TypeRemoteCluster, remoteClustersData, "[]interface{}")
		errorList = append(errorList, err)
		return deletedRemoteClusterUuidList, errorList
	}

	for _, remoteClusterData := range remoteClusterArr {
		var indErrorList []error
		deletedRemoteClusterUuidList, indErrorList = service.migrateRemoteCluster(remoteClusterData, deletedRemoteClusterUuidList)
		if len(indErrorList) != 0 {
			errorList = append(errorList, indErrorList...)
		}
	}

	return deletedRemoteClusterUuidList, errorList

}

func (service *MigrationSvc) migrateRemoteCluster(remoteClusterData interface{}, deletedRemoteClusterUuidList []string) ([]string, []error) {

	errorList := make([]error, 0)

	remoteCluster, ok := remoteClusterData.(map[string]interface{})
	if !ok {
		err := incorrectMetadataValueTypeError(TypeRemoteCluster, remoteClusterData, "map[string]interface{}")
		errorList = append(errorList, err)
		return deletedRemoteClusterUuidList, errorList
	}
	service.logger.Info("Starting to migrate remote cluster")
	service.logger.Infof("data=%v\n", sanitizeForLogging(remoteCluster))

	name := ""
	uuid := ""
	deleted := false
	hostname := ""
	username := ""
	password := ""
	demandEncryption := false
	var certificate []byte

	nameData, ok := remoteCluster[base.RemoteClusterName]
	if ok {
		name, errorList = getStringValue(base.RemoteClusterName, nameData, TypeRemoteCluster, errorList)
	} else {
		errorList = append(errorList, missingRequiredFieldError(base.RemoteClusterName, TypeRemoteCluster, remoteCluster))
	}

	uuidData, ok := remoteCluster[base.RemoteClusterUuid]
	if ok {
		uuid, errorList = getStringValue(base.RemoteClusterUuid, uuidData, TypeRemoteCluster, errorList)
	} else {
		errorList = append(errorList, missingRequiredFieldError(base.RemoteClusterUuid, TypeRemoteCluster, remoteCluster))
	}

	if deletedData, ok := remoteCluster[base.RemoteClusterDeleted]; ok {
		deleted, errorList = getBoolValue(base.RemoteClusterDeleted, deletedData, TypeRemoteCluster, errorList)
		if deleted {
			// if the remote cluster has been deleted, do not save it and do not return any error
			// instead, remember the cluster uuid, so that we can skip migrating the replications
			// that reference the cluster later
			service.logger.Infof("Skipping migrating remote cluster with name, %v, and uuid, %v, since it has deleted flag set to true", name, uuid)
			if uuid != "" {
				deletedRemoteClusterUuidList = append(deletedRemoteClusterUuidList, uuid)
			}
			return deletedRemoteClusterUuidList, make([]error, 0)
		}
	}

	hostnameData, ok := remoteCluster[base.RemoteClusterHostName]
	if ok {
		hostname, errorList = getStringValue(base.RemoteClusterHostName, hostnameData, TypeRemoteCluster, errorList)
	} else {
		errorList = append(errorList, missingRequiredFieldError(base.RemoteClusterHostName, TypeRemoteCluster, remoteCluster))
	}

	usernameData, ok := remoteCluster[base.RemoteClusterUserName]
	if ok {
		username, errorList = getStringValue(base.RemoteClusterUserName, usernameData, TypeRemoteCluster, errorList)
	} else {
		errorList = append(errorList, missingRequiredFieldError(base.RemoteClusterUserName, TypeRemoteCluster, remoteCluster))
	}

	passwordData, ok := remoteCluster[base.RemoteClusterPassword]
	if ok {
		password, errorList = getStringValue(base.RemoteClusterPassword, passwordData, TypeRemoteCluster, errorList)
	} else {
		errorList = append(errorList, missingRequiredFieldError(base.RemoteClusterPassword, TypeRemoteCluster, remoteCluster))
	}

	if demandEncryptionData, ok := remoteCluster[base.RemoteClusterDemandEncryption]; ok {
		demandEncryption = getDemandEncryptionValue(demandEncryptionData)
	}
	if certificateData, ok := remoteCluster[base.RemoteClusterCertificate]; ok {
		var certificateStr string
		certificateStr, errorList = getStringValue(base.RemoteClusterCertificate, certificateData, TypeRemoteCluster, errorList)
		certificate = []byte(certificateStr)
	}

	if demandEncryption && len(certificate) == 0 {
		errorList = append(errorList, errors.New(fmt.Sprintf("Certificate of remote cluster is required when demandEncryption is enabled. data=%v", sanitizeForLogging(remoteCluster))))
	}

	if len(errorList) != 0 {
		return deletedRemoteClusterUuidList, errorList
	}

	// save remote cluster if there are no validation errors
	ref := metadata.NewRemoteClusterReference(uuid, name, hostname, username, password, demandEncryption, certificate)

	service.logger.Infof("Remote cluster constructed = %v\n", ref)

	// delete remote cluster if it already exists
	_, err := service.remote_cluster_svc.DelRemoteCluster(name)
	if err == nil {
		service.logger.Infof("Deleted existing remote cluster with name=%v\n", name)
	}

	err = service.remote_cluster_svc.AddRemoteCluster(ref)
	// may need to ignore keyAlreadyExist error here if migration is to be done in multiple nodes concurrently
	if err != nil {
		errorList = append(errorList, err)
	}

	service.logger.Infof("Done with migrating remote cluster with name=%v. errorList=%v\n", name, errorList)

	return deletedRemoteClusterUuidList, errorList
}

func (service *MigrationSvc) migrateReplicationSettings(replicationSettingsData interface{}) (int, int, []error) {
	service.logger.Info("Starting to migrate default replication settings")
	service.logger.Infof("data=%v\n", replicationSettingsData)

	//var workerProcesses int
	//var maxConcurrentReps int

	errorList := make([]error, 0)

	if replicationSettingsData == nil {
		return InvalidIntValue, InvalidIntValue, errorList
	}

	oldSettingsMap, ok := replicationSettingsData.(map[string]interface{})
	if !ok {
		err := incorrectMetadataValueTypeError(TypeReplicationSettings, replicationSettingsData, "map[string]interface{}")
		errorList = append(errorList, err)
		return InvalidIntValue, InvalidIntValue, errorList
	}

	settingsMap, errorList, workerProcesses, maxConcurrentReps := service.getGoxdcrSettingsMap(oldSettingsMap, errorList, SettingWorkerProcessesDefault, SettingMaxConcurrentRepsDefault)

	if len(errorList) > 0 {
		return InvalidIntValue, InvalidIntValue, errorList
	}

	defaultSettings, err := service.replication_settings_svc.GetDefaultReplicationSettings()
	if err != nil {
		errorList = append(errorList, err)
		return InvalidIntValue, InvalidIntValue, errorList
	}

	changedSettingsMap, errorMap := defaultSettings.UpdateSettingsFromMap(settingsMap)

	errorList = addErrorMapToErrorList(errorMap, errorList)
	if len(errorList) > 0 {
		return InvalidIntValue, InvalidIntValue, errorList
	}

	if len(changedSettingsMap) == 0 {
		// no op if no real changes
		return workerProcesses, maxConcurrentReps, errorList
	}

	err = service.replication_settings_svc.SetDefaultReplicationSettings(defaultSettings)
	if err != nil {
		errorList = append(errorList, err)
		return InvalidIntValue, InvalidIntValue, errorList
	}

	service.logger.Infof("Done with migrating default replication settings. errorList=%v\n", errorList)

	return workerProcesses, maxConcurrentReps, errorList

}

func (service *MigrationSvc) migrateReplicationDocs(replicationDocsData interface{}, deletedRemoteClusterUuidList []string,
	defaultWorkerProcesses, defaultMaxConcurrentReps int) []error {
	service.logger.Info("Starting to migrate replication docs")
	service.logger.Infof("data=%v\n", replicationDocsData)

	errorList := make([]error, 0)

	if replicationDocsData == nil {
		return errorList
	}

	replicationDocArr, ok := replicationDocsData.([]interface{})
	if !ok {
		err := incorrectMetadataValueTypeError(TypeReplicationDoc, replicationDocsData, "[]interface{}")
		errorList = append(errorList, err)
		return errorList
	}

	for _, replicationDocData := range replicationDocArr {
		indErrorList := service.migrateReplicationDoc(replicationDocData, deletedRemoteClusterUuidList, defaultWorkerProcesses, defaultMaxConcurrentReps)
		if len(indErrorList) != 0 {
			errorList = append(errorList, indErrorList...)
		}
	}

	return errorList

}

func (service *MigrationSvc) migrateReplicationDoc(replicationDocData interface{}, deletedRemoteClusterUuidList []string,
	defaultWorkerProcesses, defaultMaxConcurrentReps int) []error {
	service.logger.Info("Starting to migrate replication doc")
	service.logger.Infof("data=%v\n", replicationDocData)

	errorList := make([]error, 0)

	replicationDoc, ok := replicationDocData.(map[string]interface{})
	if !ok {
		err := incorrectMetadataValueTypeError(TypeRemoteCluster, replicationDocData, "map[string]interface{}")
		errorList = append(errorList, err)
		return errorList
	}

	id := ""
	sourceBucket := ""
	target := ""
	targetClusterUuid := ""
	targetBucket := ""

	idData, ok := replicationDoc[base.ReplicationDocId]
	if ok {
		id, errorList = getStringValue(base.ReplicationDocId, idData, TypeReplicationDoc, errorList)
	} else {
		errorList = append(errorList, missingRequiredFieldError(base.ReplicationDocId, TypeReplicationDoc, replicationDoc))
	}

	sourceData, ok := replicationDoc[base.ReplicationDocSource]
	if ok {
		sourceBucket, errorList = getStringValue(base.ReplicationDocSource, sourceData, TypeReplicationDoc, errorList)
	} else {
		errorList = append(errorList, missingRequiredFieldError(base.ReplicationDocSource, TypeReplicationDoc, replicationDoc))
	}

	targetData, ok := replicationDoc[base.ReplicationDocTarget]
	if ok {
		target, errorList = getStringValue(base.ReplicationDocTarget, targetData, TypeReplicationDoc, errorList)
	} else {
		errorList = append(errorList, missingRequiredFieldError(base.ReplicationDocTarget, TypeReplicationDoc, replicationDoc))
	}

	// get target cluster uuid and target bucket from targetStr
	if target != "" {
		parts := strings.Split(target, base.KeyPartsDelimiter)
		if len(parts) == 5 {
			targetClusterUuid = parts[2]
			targetBucket = parts[4]
		} else {
			errorList = append(errorList, invalidFieldValueError(target, base.ReplicationDocTarget, TypeReplicationDoc))
		}
	}

	if sourceBucket != "" && targetBucket != "" && targetClusterUuid != "" {
		// check if the remote cluster referenced exists
		_, err := service.remote_cluster_svc.RemoteClusterByUuid(targetClusterUuid, true)
		if err != nil {
			clusterDeleted := false
			for _, uuid := range deletedRemoteClusterUuidList {
				if uuid == targetClusterUuid {
					clusterDeleted = true
					break
				}
			}
			if clusterDeleted {
				// if the referenced remote cluster has been deleted, do not migrate the current replication doc and do not return any errors
				service.logger.Infof("Skipping migrating replication doc with id, %v, since the remote cluster referenced, %v, has already been deleted", id, targetClusterUuid)
				return make([]error, 0)
			} else {
				// otherwise, this is an error that needs to be reported
				errorList = append(errorList, errors.New(fmt.Sprintf("Error migrating replication doc with id, %v, since cannot find the referenced remote cluster with uuid, %v", id, targetClusterUuid)))
			}
		}
	}

	// get replication settings
	settingsMap, errorList, _, _ := service.getGoxdcrSettingsMap(replicationDoc, errorList, defaultWorkerProcesses, defaultMaxConcurrentReps)

	if len(errorList) > 0 {
		return errorList
	}

	// save replication spec if there are no validation errors
	sourceBucketUUID, err_source := service.sourceBucketUUID(sourceBucket)
	targetBucketUUID, err_target := service.targetBucketUUID(targetClusterUuid, targetBucket)
	if err_source != nil && err_source != utils.NonExistentBucketError &&
		err_target != nil && err_target != utils.NonExistentBucketError {
		if err_source != nil {
			errorList = append(errorList, err_source)
		}
		if err_target != nil {
			errorList = append(errorList, err_target)
		}
		return errorList
	}

	if err_source == utils.NonExistentBucketError || err_target == utils.NonExistentBucketError {
		if err_source == utils.NonExistentBucketError && err_target == utils.NonExistentBucketError {
			service.logger.Infof("replication doc with sourceBucket=%v, targetClusterUuid=%v, targetBucket=%v has non-existent source and target bucket, skip it.\n", sourceBucket, targetClusterUuid, targetBucket)
		} else if err_source == utils.NonExistentBucketError {
			service.logger.Infof("replication doc with sourceBucket=%v, targetClusterUuid=%v, targetBucket=%v has non-existent source bucket, skip it.\n", sourceBucket, targetClusterUuid, targetBucket)
		} else {
			service.logger.Infof("replication doc with sourceBucket=%v, targetClusterUuid=%v, targetBucket=%v has non-existent target bucket, skip it.\n", sourceBucket, targetClusterUuid, targetBucket)
		}
	} else {
		spec := metadata.NewReplicationSpecification(sourceBucket, sourceBucketUUID, targetClusterUuid, targetBucket, targetBucketUUID)
		_, errorMap := spec.Settings.UpdateSettingsFromMap(settingsMap)

		service.logger.Infof("Replication spec constructed = %v\n", spec)

		errorList = addErrorMapToErrorList(errorMap, errorList)

		if len(errorList) > 0 {
			return errorList
		}

		// delete replication spec if it already exists
		_, err := service.repl_spec_svc.DelReplicationSpec(spec.Id)
		if err == nil {
			service.logger.Infof("Deleted existing replication spec with id=%v\n", spec.Id)
		}

		err = service.repl_spec_svc.AddReplicationSpec(spec)
		if err != nil {
			errorList = append(errorList, err)
		}

		service.logger.Infof("Done with migrating replication doc with id=%v. errorList=%v\n", id, errorList)

	}
	return errorList

}

func (service *MigrationSvc) sourceBucketUUID(bucketName string) (string, error) {
	local_connStr, _ := service.xdcr_comp_topology_svc.MyConnectionStr()
	if local_connStr == "" {
		panic("XDCRTopologySvc.MyConnectionStr() should not return empty string")
	}
	return utils.LocalBucketUUID(local_connStr, bucketName)
}

func (service *MigrationSvc) targetBucketUUID(targetClusterUUID, bucketName string) (string, error) {
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

func addErrorMapToErrorList(errorMap map[string]error, errorList []error) []error {
	if len(errorMap) > 0 {
		for _, indError := range errorMap {
			errorList = append(errorList, indError)
		}
	}

	return errorList
}

// get goxdcr settings from replication doc or erlang settings map
func (service *MigrationSvc) getGoxdcrSettingsMap(oldSettingsMap map[string]interface{}, errorList []error, defaultWorkerProcesses, defaultMaxConcurrentReps int) (map[string]interface{}, []error, int, int) {
	settingsMap := make(map[string]interface{})
	if replType, ok := oldSettingsMap[base.ReplicationDocType]; ok {
		replTypeStr, errorList := getStringValue(base.ReplicationDocType, replType, TypeReplicationDoc, errorList)
		if replTypeStr != "" {
			switch replTypeStr {
			case base.ReplicationDocTypeCapi:
				settingsMap[metadata.ReplicationType] = metadata.ReplicationTypeCapi
			case base.ReplicationDocTypeXmem:
				settingsMap[metadata.ReplicationType] = metadata.ReplicationTypeXmem
			default:
				errorList = append(errorList, invalidFieldValueError(replTypeStr, base.ReplicationDocType, TypeReplicationDoc))
			}
		}

	}
	if pauseRequested, ok := oldSettingsMap[base.ReplicationDocPauseRequested]; ok {
		var boolPauseRequested bool
		boolPauseRequested, errorList = getBoolValue(metadata.CheckpointInterval, pauseRequested, TypeReplicationDoc, errorList)
		settingsMap[metadata.Active] = !boolPauseRequested
	}
	if checkpointInterval, ok := oldSettingsMap[metadata.CheckpointInterval]; ok {
		var intCheckpointInterval int
		intCheckpointInterval, errorList = getIntValue(metadata.CheckpointInterval, checkpointInterval, TypeReplicationDoc, errorList)
		settingsMap[metadata.CheckpointInterval] = intCheckpointInterval
	}
	if batchCount, ok := oldSettingsMap[metadata.BatchCount]; ok {
		var intBatchCount int
		intBatchCount, errorList = getIntValue(metadata.BatchCount, batchCount, TypeReplicationDoc, errorList)
		settingsMap[metadata.BatchCount] = intBatchCount
	}
	if batchSize, ok := oldSettingsMap[metadata.BatchSize]; ok {
		var intBatchSize int
		intBatchSize, errorList = getIntValue(metadata.BatchSize, batchSize, TypeReplicationDoc, errorList)
		settingsMap[metadata.BatchSize] = intBatchSize
	}
	if failureRestartInterval, ok := oldSettingsMap[metadata.FailureRestartInterval]; ok {
		var intFailureRestartInterval int
		intFailureRestartInterval, errorList = getIntValue(metadata.FailureRestartInterval, failureRestartInterval, TypeReplicationDoc, errorList)
		settingsMap[metadata.FailureRestartInterval] = intFailureRestartInterval
	}
	if optimisticReplicationThreshold, ok := oldSettingsMap[metadata.OptimisticReplicationThreshold]; ok {
		var intOptimisticReplicationThreshold int
		intOptimisticReplicationThreshold, errorList = getIntValue(metadata.OptimisticReplicationThreshold, optimisticReplicationThreshold, TypeReplicationDoc, errorList)
		settingsMap[metadata.OptimisticReplicationThreshold] = intOptimisticReplicationThreshold
	}

	workerProcesses := NonExistentIntValue
	maxConcurrentReps := NonExistentIntValue
	if workerProcessesData, ok := oldSettingsMap[SettingWorkerProcesses]; ok {
		workerProcesses, errorList = getIntValue(SettingWorkerProcesses, workerProcessesData, TypeReplicationDoc, errorList)
	}
	if maxConcurrentRepsData, ok := oldSettingsMap[SettingMaxConcurrentReps]; ok {
		maxConcurrentReps, errorList = getIntValue(SettingMaxConcurrentReps, maxConcurrentRepsData, TypeReplicationDoc, errorList)
	}

	if workerProcesses == NonExistentIntValue || workerProcesses == InvalidIntValue {
		workerProcesses = defaultWorkerProcesses
	}
	if maxConcurrentReps == NonExistentIntValue || maxConcurrentReps == InvalidIntValue {
		maxConcurrentReps = defaultMaxConcurrentReps
	}

	settingsMap[metadata.TargetNozzlePerNode] = workerProcesses * maxConcurrentReps

	service.logger.Infof("Done with converting replication settings to goxdcr settings. old settings=%v\n new settings=%v\n errorList=%v\n", oldSettingsMap, settingsMap, errorList)

	return settingsMap, errorList, workerProcesses, maxConcurrentReps
}

func getWorkerProcessAndMaxConcurrentReps(oldSettingsMap map[string]interface{}, errorList []error) (int, int, []error) {
	workerProcesses := NonExistentIntValue
	maxConcurrentReps := NonExistentIntValue
	if workerProcessesData, ok := oldSettingsMap[SettingWorkerProcesses]; ok {
		workerProcesses, errorList = getIntValue(SettingWorkerProcesses, workerProcessesData, TypeReplicationDoc, errorList)
	}
	if maxConcurrentRepsData, ok := oldSettingsMap[SettingMaxConcurrentReps]; ok {
		maxConcurrentReps, errorList = getIntValue(SettingMaxConcurrentReps, maxConcurrentRepsData, TypeReplicationDoc, errorList)
	}
	return workerProcesses, maxConcurrentReps, errorList
}

// helper method that gets string value from fieldValue.
// it adds validation error to errorList if fieldValue is not of string type
func getStringValue(fieldName string, fieldValue interface{}, metadataType string, errorList []error) (string, []error) {
	strValue, ok := fieldValue.(string)
	if !ok {
		errorList = append(errorList, incorrectFieldValueTypeError(fieldName, fieldValue, metadataType, "string"))
		return "", errorList
	}
	return strValue, errorList
}

func getBoolValue(fieldName string, fieldValue interface{}, metadataType string, errorList []error) (bool, []error) {
	boolValue, ok := fieldValue.(bool)
	if !ok {
		errorList = append(errorList, incorrectFieldValueTypeError(fieldName, fieldValue, metadataType, "bool"))
		return false, errorList
	}
	return boolValue, errorList
}

func getIntValue(fieldName string, fieldValue interface{}, metadataType string, errorList []error) (int, []error) {
	floatValue, ok := fieldValue.(float64)
	if !ok {
		errorList = append(errorList, incorrectFieldValueTypeError(fieldName, fieldValue, metadataType, "int"))
		return InvalidIntValue, errorList
	}
	return int(floatValue), errorList
}

func getUint64Value(fieldName string, fieldValue interface{}, metadataType string, errorList []error) (uint64, []error) {
	intValue, errorList := getIntValue(fieldName, fieldValue, metadataType, errorList)
	return uint64(intValue), errorList
}

// demandEncryption needs special handling
func getDemandEncryptionValue(fieldValue interface{}) bool {
	floatValue, ok := fieldValue.(float64)
	if !ok {
		return true
	}
	if int(floatValue) == 0 {
		// only an int value of 0 indicates that encryption is not enabled
		return false
	} else {
		return true
	}
}

func incorrectMetadataValueTypeError(metadataType string, data interface{}, expectedType string) error {
	return errors.New(fmt.Sprintf("The value, %v, for %v is of incorrect type. expected type=%v, actual type=%v\n", data, metadataType, expectedType, reflect.TypeOf(data)))
}

func missingRequiredFieldError(fieldName string, metadataType string, data interface{}) error {
	return errors.New(fmt.Sprintf("Required field, %v, in %v is missing. Data=%v", fieldName, metadataType, data))
}

func incorrectFieldValueTypeError(fieldName string, fieldValue interface{}, metadataType string, expectedType string) error {
	return errors.New(fmt.Sprintf("The field value, %v, for field, %v, in %v is of incorrect type. Expected type=%v, actual type=%v", fieldValue, fieldName, metadataType, expectedType, reflect.TypeOf(fieldValue)))
}

func invalidFieldValueError(fieldName string, fieldValue interface{}, metadataType string) error {
	return errors.New(fmt.Sprintf("The field value, %v, for field, %v, in %v is invalid.", fieldValue, fieldName, metadataType))
}

func getReplicationIdAndVBFromCheckpointId(checkpointDocId string) (string, uint16, error) {
	if strings.HasPrefix(checkpointDocId, CheckpointDocIdPrefix) {
		checkpointDocId2 := checkpointDocId[len(CheckpointDocIdPrefix):]
		index := strings.LastIndex(checkpointDocId2, base.KeyPartsDelimiter)
		if index > 0 {
			var newReplicationId string

			oldReplicationId := checkpointDocId2[:index]
			// old replication id has the form of targetClusterUuid/sourceBucket/targetBucket
			parts := strings.Split(oldReplicationId, base.KeyPartsDelimiter)
			if len(parts) == 3 {
				newReplicationId = metadata.ReplicationId(parts[1], parts[0], parts[2])
			} else {
				return "", 0, invalidFieldValueError(CheckpointDocId, checkpointDocId, TypeCheckpoint)
			}

			vbnoStr := checkpointDocId2[index+len(base.KeyPartsDelimiter):]
			vbno, err := strconv.ParseInt(vbnoStr, base.ParseIntBase, base.ParseIntBitSize)
			if err == nil {
				return newReplicationId, uint16(vbno), nil
			} else {
				return "", 0, invalidFieldValueError(CheckpointDocId, checkpointDocId, TypeCheckpoint)
			}
		} else {
			return "", 0, invalidFieldValueError(CheckpointDocId, checkpointDocId, TypeCheckpoint)
		}
	} else {
		return "", 0, invalidFieldValueError(CheckpointDocId, checkpointDocId, TypeCheckpoint)
	}
}

func sanitizeForLogging (data map[string]interface{}) map[string]interface{}{
	ret_map := make(map[string]interface{})
	for key, value := range data {
		if key == "password" {
			ret_map[key] = "xxxx"
		}else {
			ret_map[key] = value
		}
	}
	return ret_map
}

