// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_impl

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	base "github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
	utilities "github.com/couchbase/goxdcr/v8/utils"
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

type migrationDataMap map[string]interface{}

// Map of key and bool to indicate the value is a stream of bytes
var migrationDataMapRedactKeys = map[string]bool{"password": false,
	"userName":    false,
	"certificate": true,
}

func (mdm migrationDataMap) Clone() migrationDataMap {
	if mdm != nil {
		clone := make(migrationDataMap)
		for k, v := range mdm {
			if isByteArray, exists := migrationDataMapRedactKeys[k]; exists && isByteArray {
				clone[k] = base.DeepCopyByteArray(v.([]byte))
			} else {
				clone[k] = v
			}
		}
		return clone
	}
	return mdm
}

func (mdm migrationDataMap) Redact() migrationDataMap {
	for key, redactBytes := range migrationDataMapRedactKeys {
		if v, ok := mdm[key]; ok {
			if redactBytes {
				mdm[key] = base.TagUDBytes(v.([]byte))
			} else {
				mdm[key] = base.TagUD(v)
			}
		}
	}
	return mdm
}

func (mdm migrationDataMap) CloneAndRedact() migrationDataMap {
	return mdm.Clone().Redact()
}

type MigrationSvc struct {
	xdcr_comp_topology_svc   service_def.XDCRCompTopologySvc
	remote_cluster_svc       service_def.RemoteClusterSvc
	repl_spec_svc            service_def.ReplicationSpecSvc
	replication_settings_svc service_def.ReplicationSettingsSvc
	checkpoints_svc          service_def.CheckpointsService
	logger                   *log.CommonLogger
	utils                    utilities.UtilsIface
}

func NewMigrationSvc(xdcr_comp_topology_svc service_def.XDCRCompTopologySvc, remote_cluster_svc service_def.RemoteClusterSvc, repl_spec_svc service_def.ReplicationSpecSvc,
	replication_settings_svc service_def.ReplicationSettingsSvc, checkpoints_svc service_def.CheckpointsService,
	loggerCtx *log.LoggerContext, utilsIn utilities.UtilsIface) *MigrationSvc {
	service := &MigrationSvc{
		xdcr_comp_topology_svc:   xdcr_comp_topology_svc,
		remote_cluster_svc:       remote_cluster_svc,
		repl_spec_svc:            repl_spec_svc,
		replication_settings_svc: replication_settings_svc,
		checkpoints_svc:          checkpoints_svc,
		logger:                   log.NewLogger("MigrationSvc", loggerCtx),
		utils:                    utilsIn,
	}

	service.logger.Infof("Created Migration service.\n")
	return service
}

func (service *MigrationSvc) Migrate() error {
	data, err := service.readMetadataFromStdin()
	if err != nil {
		// this error is a fatal error and should have been logged prior
		return err
	}

	fatalErrorList, mildErrorList := service.migrate_internal(data)
	if len(fatalErrorList) == 0 && len(mildErrorList) == 0 {
		service.logger.Info("Metadata migration completed without errors")
		return nil
	} else if len(fatalErrorList) == 0 {
		// print mild errors/warnings.
		// Migration service will not be retried for these errors, since retrying most likely would not help
		service.logger.Infof("Metadata migration completed with warnings. Some migrated metadata may be invalid. warnings = %v\n", mildErrorList)
		return nil
	} else {
		// fatal errors will get migration service restarted
		service.logger.Errorf("Metadata migration failed. fatalErrorList=%v, mildErrorList=%v\n", fatalErrorList, mildErrorList)
		return fatalErrorList[0]
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
			service.logger.Errorf("Error reading metadata from stdin. err=%v\n", err)
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

func (service *MigrationSvc) migrate_internal(data []byte) ([]error, []error) {
	service.logger.Info("Starting to migrate xdcr metadata")

	fatalErrorList := make([]error, 0)
	mildErrorList := make([]error, 0)

	if len(data) == 0 {
		return fatalErrorList, mildErrorList
	}

	dataObj := make(map[string]interface{})
	err := json.Unmarshal(data, &dataObj)
	if err != nil {
		fatalErrorList = append(fatalErrorList, service.utils.NewEnhancedError(fmt.Sprintf("Error unmarshaling metadata"), err))
		return fatalErrorList, mildErrorList
	}

	deletedRemoteClusterUuidList := make([]string, 0)
	remoteClustersData, ok := dataObj[RemoteClustersKey]
	if ok {
		var rcFatalErrorList []error
		var rcMildErrorList []error
		deletedRemoteClusterUuidList, rcFatalErrorList, rcMildErrorList = service.migrateRemoteClusters(remoteClustersData)
		if len(rcMildErrorList) != 0 {
			mildErrorList = append(mildErrorList, rcMildErrorList...)
		}
		if len(rcFatalErrorList) != 0 {
			fatalErrorList = append(fatalErrorList, rcFatalErrorList...)
			return fatalErrorList, mildErrorList
		}
	}

	defaultWorkerProcesses := InvalidIntValue
	defaultMaxConcurrentReps := InvalidIntValue
	replSettingsData, ok := dataObj[ReplicationSettingsKey]
	if ok {
		var rsFatalErrorList []error
		// The following errors may be returned from migrateReplicationSettings:
		// 1. error reading a particular setting, e.g., because the value passed in from ns_server is of wrong data type
		//    (1) it is highly unlikely. If it happens the data from ns_server is probably corrupted.
		//    (2) it is not safe to migrate the setting in this case, e.g. by using some default value, since replications would
		//    run "successfully" with wrong settings and customers may not know about it
		// 2. error reading settings from or writing settings to metakv
		// Treat all these errors as fatal
		defaultWorkerProcesses, defaultMaxConcurrentReps, rsFatalErrorList = service.migrateReplicationSettings(replSettingsData)
		if len(rsFatalErrorList) != 0 {
			fatalErrorList = append(fatalErrorList, rsFatalErrorList...)
			return fatalErrorList, mildErrorList
		}
	}

	replDocsData, ok := dataObj[ReplicationDocsKey]
	if ok {
		rdFatalErrorList, rdMildErrorList := service.migrateReplicationDocs(replDocsData, deletedRemoteClusterUuidList, defaultWorkerProcesses, defaultMaxConcurrentReps)
		if len(rdMildErrorList) != 0 {
			mildErrorList = append(mildErrorList, rdMildErrorList...)
		}
		if len(rdFatalErrorList) != 0 {
			fatalErrorList = append(fatalErrorList, rdFatalErrorList...)
			return fatalErrorList, mildErrorList
		}
	}

	return fatalErrorList, mildErrorList
}

func (service *MigrationSvc) migrateRemoteClusters(remoteClustersData interface{}) ([]string, []error, []error) {
	service.logger.Info("Starting to migrate remote clusters")

	deletedRemoteClusterUuidList := make([]string, 0)
	fatalErrorList := make([]error, 0)
	mildErrorList := make([]error, 0)

	if remoteClustersData == nil {
		return deletedRemoteClusterUuidList, fatalErrorList, mildErrorList
	}

	remoteClusterArr, ok := remoteClustersData.([]interface{})
	if !ok {
		err := incorrectMetadataValueTypeError(TypeRemoteCluster, remoteClustersData, "[]interface{}")
		fatalErrorList = append(fatalErrorList, err)
		return deletedRemoteClusterUuidList, fatalErrorList, mildErrorList
	}

	for _, remoteClusterData := range remoteClusterArr {
		var indFatalErrorList []error
		var indMildErrorList []error
		deletedRemoteClusterUuidList, indFatalErrorList, indMildErrorList = service.migrateRemoteCluster(remoteClusterData, deletedRemoteClusterUuidList)
		if len(indMildErrorList) != 0 {
			mildErrorList = append(mildErrorList, indMildErrorList...)
		}
		if len(indFatalErrorList) != 0 {
			fatalErrorList = append(fatalErrorList, indFatalErrorList...)
			return deletedRemoteClusterUuidList, fatalErrorList, mildErrorList
		}
	}

	return deletedRemoteClusterUuidList, fatalErrorList, mildErrorList

}

func (service *MigrationSvc) migrateRemoteCluster(remoteClusterData interface{}, deletedRemoteClusterUuidList []string) ([]string, []error, []error) {
	fatalErrorList := make([]error, 0)
	mildErrorList := make([]error, 0)

	remoteCluster, ok := remoteClusterData.(migrationDataMap)
	if !ok {
		err := incorrectMetadataValueTypeError(TypeRemoteCluster, remoteClusterData, "map[string]interface{}")
		fatalErrorList = append(fatalErrorList, err)
		return deletedRemoteClusterUuidList, fatalErrorList, mildErrorList
	}
	service.logger.Info("Starting to migrate remote cluster")
	redactedClusterData := remoteCluster.CloneAndRedact()
	service.logger.Infof("data=%v\n", redactedClusterData)

	name := ""
	uuid := ""
	deleted := false
	hostname := ""
	username := ""
	password := ""
	demandEncryption := false
	var certificate []byte
	var err error

	nameData, ok := remoteCluster[base.RemoteClusterName]
	if ok {
		name, mildErrorList = getStringValue(base.RemoteClusterName, nameData, TypeRemoteCluster, mildErrorList)
	} else {
		mildErrorList = append(mildErrorList, missingRequiredFieldError(base.RemoteClusterName, TypeRemoteCluster, redactedClusterData))
	}

	if name == "" {
		// the "Name" attribute is especially critical since some remote cluster rest APIs do not work without it
		// if we migrate the remote cluster reference, we will not be able to edit it or delete it
		// in the unlikely event that Name is missing, skip the remote cluster reference
		// in comparison, if other required fields are missing, we can still migrate the remote cluster reference
		// and let users decide what to do with the migrated and invalid reference later
		mildErrorList = append(mildErrorList, fmt.Errorf("Skipping migrating remote cluster %v since it does not have Name specified", redactedClusterData))
		return deletedRemoteClusterUuidList, fatalErrorList, mildErrorList
	}

	uuidData, ok := remoteCluster[base.RemoteClusterUuid]
	if ok {
		uuid, mildErrorList = getStringValue(base.RemoteClusterUuid, uuidData, TypeRemoteCluster, mildErrorList)
	} else {
		mildErrorList = append(mildErrorList, missingRequiredFieldError(base.RemoteClusterUuid, TypeRemoteCluster, redactedClusterData))
	}

	if deletedData, ok := remoteCluster[base.RemoteClusterDeleted]; ok {
		deleted, mildErrorList = getBoolValue(base.RemoteClusterDeleted, deletedData, TypeRemoteCluster, mildErrorList)
		if deleted {
			// if the remote cluster has been deleted, do not save it and do not return any error
			// instead, remember the cluster uuid, so that we can skip migrating the replications
			// that reference the cluster later
			service.logger.Infof("Skipping migrating remote cluster with name, %v, and uuid, %v, since it has deleted flag set to true", name, uuid)
			if uuid != "" {
				deletedRemoteClusterUuidList = append(deletedRemoteClusterUuidList, uuid)
			}
			// if skipping, return empty fatal error and mild error list
			if len(mildErrorList) > 0 {
				mildErrorList = make([]error, 0)
			}
			return deletedRemoteClusterUuidList, fatalErrorList, mildErrorList
		}
	}

	hostnameData, ok := remoteCluster[base.RemoteClusterHostName]
	if ok {
		hostname, mildErrorList = getStringValue(base.RemoteClusterHostName, hostnameData, TypeRemoteCluster, mildErrorList)
	} else {
		mildErrorList = append(mildErrorList, missingRequiredFieldError(base.RemoteClusterHostName, TypeRemoteCluster, redactedClusterData))
	}

	usernameData, ok := remoteCluster[base.RemoteClusterUserName]
	if ok {
		username, mildErrorList = getStringValue(base.RemoteClusterUserName, usernameData, TypeRemoteCluster, mildErrorList)
	} else {
		mildErrorList = append(mildErrorList, missingRequiredFieldError(base.RemoteClusterUserName, TypeRemoteCluster, redactedClusterData))
	}

	passwordData, ok := remoteCluster[base.RemoteClusterPassword]
	if ok {
		password, mildErrorList = getStringValue(base.RemoteClusterPassword, passwordData, TypeRemoteCluster, mildErrorList)
	} else {
		mildErrorList = append(mildErrorList, missingRequiredFieldError(base.RemoteClusterPassword, TypeRemoteCluster, redactedClusterData))
	}

	if demandEncryptionData, ok := remoteCluster[base.RemoteClusterDemandEncryption]; ok {
		demandEncryption, err = getDemandEncryptionValue(demandEncryptionData, name)
		if err != nil {
			mildErrorList = append(mildErrorList, err)
		}
	}
	if certificateData, ok := remoteCluster[base.RemoteClusterCertificate]; ok {
		var certificateStr string
		certificateStr, mildErrorList = getStringValue(base.RemoteClusterCertificate, certificateData, TypeRemoteCluster, mildErrorList)
		certificate = []byte(certificateStr)
	}

	if demandEncryption && len(certificate) == 0 {
		mildErrorList = append(mildErrorList, errors.New(fmt.Sprintf("Certificate of remote cluster is required when demandEncryption is enabled. data=%v", redactedClusterData)))
	}

	if !demandEncryption && len(certificate) > 0 {
		mildErrorList = append(mildErrorList, errors.New(fmt.Sprintf("Certificate of remote cluster has been specified when demandEncryption is not enabled. data=%v", redactedClusterData)))
	}

	encryptionType := ""
	if demandEncryption {
		encryptionType = metadata.EncryptionType_Full
	}

	// save remote cluster  - even if there are validation errors
	ref, err := metadata.NewRemoteClusterReference(uuid, name, hostname, username, password, "", demandEncryption, encryptionType, certificate, nil, nil, nil)
	if err != nil {
		// err here comes from random number generation, which is promised to always be nil by golang
		// handle it anyways
		fatalErrorList = append(fatalErrorList, err)
		return deletedRemoteClusterUuidList, fatalErrorList, mildErrorList
	}

	service.logger.Infof("Remote cluster constructed = %v\n", ref.CloneAndRedact())

	// delete remote cluster if it already exists
	_, err = service.remote_cluster_svc.DelRemoteCluster(name)
	if err == nil {
		service.logger.Infof("Deleted existing remote cluster with name=%v\n", name)
	}

	// Since skipConnectivityValidation is set to true, errors are not returned in the following scenarios:
	// 1. ref points to an invalid remote cluster  -- ref would still be migrated
	// 2. ref has SSL specified and current cluster and remote cluster are not Enterprise -- ref would still
	// be migrated and the error condition will be detected and handled at replication runtime
	// The following errors can still be returned:
	// 1. ref conflicts with other refs (e.g., having the same name, pointing to the same remote cluster).
	//    this should not happen in practice since pre-4.x had the same validation to avoid these conflicts.
	// 2. error writing to metakv service
	// 3. error updating remote cluster service cache
	// Let migration fail in these cases
	err = service.remote_cluster_svc.AddRemoteCluster(ref, true /*skipConnectivityValidation*/)
	if err != nil {
		fatalErrorList = append(fatalErrorList, err)
	}

	service.logger.Infof("Done with migrating remote cluster with name=%v. fatalErrorList=%v, mildErrorList=%v\n", name, fatalErrorList, mildErrorList)

	return deletedRemoteClusterUuidList, fatalErrorList, mildErrorList
}

func (service *MigrationSvc) migrateReplicationSettings(replicationSettingsData interface{}) (int, int, []error) {
	service.logger.Info("Starting to migrate default replication settings")
	service.logger.Infof("data=%v\n", (replicationSettingsData.(metadata.ReplicationSettingsMap)).CloneAndRedact())

	fatalErrorList := make([]error, 0)

	if replicationSettingsData == nil {
		return SettingWorkerProcessesDefault, SettingMaxConcurrentRepsDefault, fatalErrorList
	}

	oldSettingsMap, ok := replicationSettingsData.(metadata.ReplicationSettingsMap)
	if !ok {
		err := incorrectMetadataValueTypeError(TypeReplicationSettings, replicationSettingsData, "map[string]interface{}")
		fatalErrorList = append(fatalErrorList, err)
		return InvalidIntValue, InvalidIntValue, fatalErrorList
	}

	settingsMap, fatalErrorList, workerProcesses, maxConcurrentReps := service.getGoxdcrSettingsMap(oldSettingsMap, fatalErrorList, SettingWorkerProcessesDefault, SettingMaxConcurrentRepsDefault)

	if len(fatalErrorList) > 0 {
		return InvalidIntValue, InvalidIntValue, fatalErrorList
	}

	_, errorMap, err := service.replication_settings_svc.UpdateDefaultReplicationSettings(settingsMap)
	if err != nil {
		fatalErrorList = append(fatalErrorList, err)
		return InvalidIntValue, InvalidIntValue, fatalErrorList
	}

	if len(errorMap) > 0 {
		fatalErrorList = addErrorMapToErrorList(errorMap, fatalErrorList)
		return InvalidIntValue, InvalidIntValue, fatalErrorList
	}

	service.logger.Infof("Done with migrating default replication settings. fatalErrorList=%v\n", fatalErrorList)

	return workerProcesses, maxConcurrentReps, fatalErrorList

}

func (service *MigrationSvc) migrateReplicationDocs(replicationDocsData interface{}, deletedRemoteClusterUuidList []string,
	defaultWorkerProcesses, defaultMaxConcurrentReps int) ([]error, []error) {
	service.logger.Info("Starting to migrate replication docs")
	service.logger.Infof("data=%v\n", replicationDocsData)

	fatalErrorList := make([]error, 0)
	mildErrorList := make([]error, 0)

	if replicationDocsData == nil {
		return fatalErrorList, mildErrorList
	}

	replicationDocArr, ok := replicationDocsData.([]interface{})
	if !ok {
		err := incorrectMetadataValueTypeError(TypeReplicationDoc, replicationDocsData, "[]interface{}")
		fatalErrorList = append(fatalErrorList, err)
		return fatalErrorList, mildErrorList
	}

	for _, replicationDocData := range replicationDocArr {
		indFatalErrorList, indMildErrorList := service.migrateReplicationDoc(replicationDocData, deletedRemoteClusterUuidList, defaultWorkerProcesses, defaultMaxConcurrentReps)
		if len(indMildErrorList) != 0 {
			mildErrorList = append(mildErrorList, indMildErrorList...)
		}
		if len(indFatalErrorList) != 0 {
			fatalErrorList = append(fatalErrorList, indFatalErrorList...)
			return fatalErrorList, mildErrorList
		}
	}

	return fatalErrorList, mildErrorList

}

func (service *MigrationSvc) migrateReplicationDoc(replicationDocData interface{}, deletedRemoteClusterUuidList []string,
	defaultWorkerProcesses, defaultMaxConcurrentReps int) ([]error, []error) {
	service.logger.Info("Starting to migrate replication doc")
	service.logger.Infof("data=%v\n", replicationDocData)

	fatalErrorList := make([]error, 0)
	mildErrorList := make([]error, 0)
	var err error

	replicationDoc, ok := replicationDocData.(map[string]interface{})
	if !ok {
		err = incorrectMetadataValueTypeError(TypeRemoteCluster, replicationDocData, "map[string]interface{}")
		fatalErrorList = append(fatalErrorList, err)
		return fatalErrorList, mildErrorList
	}

	id := ""
	sourceBucket := ""
	sourceBucketUUID := ""
	target := ""
	targetBucket := ""
	targetBucketUUID := ""
	targetClusterUuid := ""

	idData, ok := replicationDoc[base.ReplicationDocId]
	if ok {
		id, mildErrorList = getStringValue(base.ReplicationDocId, idData, TypeReplicationDoc, mildErrorList)
	} else {
		mildErrorList = append(mildErrorList, missingRequiredFieldError(base.ReplicationDocId, TypeReplicationDoc, replicationDoc))
	}

	sourceData, ok := replicationDoc[base.ReplicationDocSource]
	if ok {
		sourceBucket, mildErrorList = getStringValue(base.ReplicationDocSource, sourceData, TypeReplicationDoc, mildErrorList)
	} else {
		mildErrorList = append(mildErrorList, missingRequiredFieldError(base.ReplicationDocSource, TypeReplicationDoc, replicationDoc))
	}

	targetData, ok := replicationDoc[base.ReplicationDocTarget]
	if ok {
		target, mildErrorList = getStringValue(base.ReplicationDocTarget, targetData, TypeReplicationDoc, mildErrorList)
	} else {
		mildErrorList = append(mildErrorList, missingRequiredFieldError(base.ReplicationDocTarget, TypeReplicationDoc, replicationDoc))
	}

	// get target cluster uuid and target bucket from targetStr
	if target != "" {
		parts := strings.Split(target, base.KeyPartsDelimiter)
		if len(parts) == 5 {
			targetClusterUuid = parts[2]
			targetBucket = parts[4]
		} else {
			mildErrorList = append(mildErrorList, invalidFieldValueError(target, base.ReplicationDocTarget, TypeReplicationDoc))
		}
	}

	if sourceBucket == "" || targetBucket == "" || targetClusterUuid == "" {
		// these three attributes are especially critical since replication spec cannot be saved without them
		// in the unlikely event that any of them is missing, skip the replication spec
		mildErrorList = append(mildErrorList, fmt.Errorf("Skipping migrating replication doc %v since some of the required fields, sourceBucket, targetBucket, and targetClusterUuid, are missing", replicationDoc))
		return fatalErrorList, mildErrorList
	}

	// check if the remote cluster referenced exists
	_, err = service.remote_cluster_svc.RemoteClusterByUuid(targetClusterUuid, true)
	if err != nil {
		clusterDeleted := false
		for _, uuid := range deletedRemoteClusterUuidList {
			if uuid == targetClusterUuid {
				clusterDeleted = true
				break
			}
		}
		if clusterDeleted {
			// if the referenced remote cluster has been deleted, do not migrate the current replication doc
			mildErrorList = append(mildErrorList, fmt.Errorf("Skipping migrating replication doc with id, %v, since the remote cluster referenced, %v, has already been deleted", id, targetClusterUuid))
			return fatalErrorList, mildErrorList
		} else {
			// otherwise, log and ignore the error and continue with replication doc migration
			mildErrorList = append(mildErrorList, fmt.Errorf("Cannot find referenced remote cluster with uuid, %v, for replication doc with id, %v", targetClusterUuid, id))
		}
	}

	sourceBucketUUID, err = service.sourceBucketUUID(sourceBucket)
	if err != nil {
		err = fmt.Errorf("Error retrieving source bucket uuid for replication doc with sourceBucket=%v, targetClusterUuid=%v, targetBucket=%v. err=%v\n", sourceBucket, targetClusterUuid, targetBucket, err)
		mildErrorList = append(mildErrorList, err)
	}

	targetBucketUUID, err = service.targetBucketUUID(targetClusterUuid, targetBucket)
	if err != nil {
		err = fmt.Errorf("Error retrieving target bucket uuid for replication doc with sourceBucket=%v, targetClusterUuid=%v, targetBucket=%v. err=%v\n", targetBucket, targetClusterUuid, targetBucket, err)
		mildErrorList = append(mildErrorList, err)
	}

	// save replication spec
	spec, err := metadata.NewReplicationSpecification(sourceBucket, sourceBucketUUID, targetClusterUuid, targetBucket, targetBucketUUID)
	if err != nil {
		fatalErrorList = append(fatalErrorList, err)
		return fatalErrorList, mildErrorList
	}

	// again, treat all errors from settings processing as fatal
	// 1. they are highly unlikely to occur, unless there are bugs
	// 2. if they do occur, we do not want to migrate the replications since the wrong settings are hard to notice
	settingsMap, fatalErrorList, _, _ := service.getGoxdcrSettingsMap(replicationDoc, fatalErrorList, defaultWorkerProcesses, defaultMaxConcurrentReps)
	if len(fatalErrorList) > 0 {
		return fatalErrorList, mildErrorList
	}
	_, errorMap := spec.Settings.UpdateSettingsFromMap(settingsMap)
	fatalErrorList = addErrorMapToErrorList(errorMap, fatalErrorList)
	if len(fatalErrorList) > 0 {
		return fatalErrorList, mildErrorList
	}

	service.logger.Infof("Replication spec constructed = %v\n", spec)

	// delete replication spec if it already exists
	_, err = service.repl_spec_svc.DelReplicationSpec(spec.Id)
	if err == nil {
		service.logger.Infof("Deleted existing replication spec with id=%v\n", spec.Id)
	}

	// TODO we should add non-empty additionalInfo here to differentiate the repl spec created by migration
	// from repl spec created by user
	err = service.repl_spec_svc.AddReplicationSpec(spec, "" /*addtionalInfo*/)
	if err != nil {
		fatalErrorList = append(fatalErrorList, err)
	}

	service.logger.Infof("Done with migrating replication doc with id=%v. fatalErrorList=%v, mildErrorList=%v\n", id, fatalErrorList, mildErrorList)

	return fatalErrorList, mildErrorList

}

func (service *MigrationSvc) sourceBucketUUID(bucketName string) (string, error) {
	local_connStr, _ := service.xdcr_comp_topology_svc.MyConnectionStr()
	if local_connStr == "" {
		return "", errors.New("XDCRTopologySvc.MyConnectionStr() returned empty string")
	}
	return service.utils.LocalBucketUUID(local_connStr, bucketName, service.logger)
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
	remote_userName, remote_password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, err_target := ref.MyCredentials()
	if err_target != nil {
		return "", err_target
	}

	return service.utils.BucketUUID(remote_connStr, bucketName, remote_userName, remote_password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, service.logger)
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
func (service *MigrationSvc) getGoxdcrSettingsMap(oldSettingsMap metadata.ReplicationSettingsMap, errorList []error, defaultWorkerProcesses, defaultMaxConcurrentReps int) (metadata.ReplicationSettingsMap, []error, int, int) {
	settingsMap := make(metadata.ReplicationSettingsMap)
	if replType, ok := oldSettingsMap[base.ReplicationDocType]; ok {
		replTypeStr, errorList := getStringValue(base.ReplicationDocType, replType, TypeReplicationDoc, errorList)
		if replTypeStr != "" {
			switch replTypeStr {
			case base.ReplicationDocTypeCapi:
				settingsMap[metadata.ReplicationTypeKey] = metadata.ReplicationTypeCapi
			case base.ReplicationDocTypeXmem:
				settingsMap[metadata.ReplicationTypeKey] = metadata.ReplicationTypeXmem
			default:
				errorList = append(errorList, invalidFieldValueError(replTypeStr, base.ReplicationDocType, TypeReplicationDoc))
			}
		}

	}
	if pauseRequested, ok := oldSettingsMap[base.ReplicationDocPauseRequested]; ok {
		var boolPauseRequested bool
		boolPauseRequested, errorList = getBoolValue(metadata.CheckpointIntervalKey, pauseRequested, TypeReplicationDoc, errorList)
		settingsMap[metadata.ActiveKey] = !boolPauseRequested
	}
	if checkpointInterval, ok := oldSettingsMap[metadata.CheckpointIntervalKey]; ok {
		var intCheckpointInterval int
		intCheckpointInterval, errorList = getIntValue(metadata.CheckpointIntervalKey, checkpointInterval, TypeReplicationDoc, errorList)
		settingsMap[metadata.CheckpointIntervalKey] = intCheckpointInterval
	}
	if batchCount, ok := oldSettingsMap[metadata.BatchCountKey]; ok {
		var intBatchCount int
		intBatchCount, errorList = getIntValue(metadata.BatchCountKey, batchCount, TypeReplicationDoc, errorList)
		settingsMap[metadata.BatchCountKey] = intBatchCount
	}
	if batchSize, ok := oldSettingsMap[metadata.BatchSizeKey]; ok {
		var intBatchSize int
		intBatchSize, errorList = getIntValue(metadata.BatchSizeKey, batchSize, TypeReplicationDoc, errorList)
		settingsMap[metadata.BatchSizeKey] = intBatchSize
	}
	if failureRestartInterval, ok := oldSettingsMap[metadata.FailureRestartIntervalKey]; ok {
		var intFailureRestartInterval int
		intFailureRestartInterval, errorList = getIntValue(metadata.FailureRestartIntervalKey, failureRestartInterval, TypeReplicationDoc, errorList)
		settingsMap[metadata.FailureRestartIntervalKey] = intFailureRestartInterval
	}
	if optimisticReplicationThreshold, ok := oldSettingsMap[metadata.OptimisticReplicationThresholdKey]; ok {
		var intOptimisticReplicationThreshold int
		intOptimisticReplicationThreshold, errorList = getIntValue(metadata.OptimisticReplicationThresholdKey, optimisticReplicationThreshold, TypeReplicationDoc, errorList)
		settingsMap[metadata.OptimisticReplicationThresholdKey] = intOptimisticReplicationThreshold
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

	// make sure that targetNozzlePerNode stays within pre-defined bounds
	targetNozzlePerNode := workerProcesses * maxConcurrentReps
	if targetNozzlePerNode < metadata.TargetNozzlePerNodeConfig.MinValue {
		targetNozzlePerNode = metadata.TargetNozzlePerNodeConfig.MinValue
	}
	if targetNozzlePerNode > metadata.TargetNozzlePerNodeConfig.MaxValue {
		targetNozzlePerNode = metadata.TargetNozzlePerNodeConfig.MaxValue
	}
	settingsMap[metadata.TargetNozzlePerNodeKey] = targetNozzlePerNode

	service.logger.Infof("Done with converting replication settings to goxdcr settings. old settings=%v\n new settings=%v\n errorList=%v\n", oldSettingsMap.CloneAndRedact(), settingsMap.CloneAndRedact(), errorList)

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
func getDemandEncryptionValue(fieldValue interface{}, name string) (bool, error) {
	boolValue, ok := fieldValue.(bool)
	if !ok {
		// the presence of some value for demandEncryption indicates that demandEncryption has been enabled
		// in the weird case that the value is not of bool type, return true and a warning
		return true, fmt.Errorf("DemandEncryption in remote cluster reference %v is not of boolean type. Defaulting it to true. value=%v", name, fieldValue)
	}
	return boolValue, nil
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
