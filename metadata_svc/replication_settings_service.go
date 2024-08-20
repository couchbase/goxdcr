// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package metadata_svc

import (
	"encoding/json"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
	"sync"
)

var DefaultReplicationSettingsKey = "DefaultReplicationSettings"

type ReplicationSettingsSvc struct {
	metadata_svc           service_def.MetadataSvc
	logger                 *log.CommonLogger
	xdcr_topology_svc      service_def.XDCRCompTopologySvc
	replicationSettingsMtx sync.Mutex
}

func NewReplicationSettingsSvc(metadata_svc service_def.MetadataSvc, logger_ctx *log.LoggerContext, top_svc service_def.XDCRCompTopologySvc) *ReplicationSettingsSvc {
	return &ReplicationSettingsSvc{
		metadata_svc:      metadata_svc,
		xdcr_topology_svc: top_svc,
		logger:            log.NewLogger("ReplSettSvc", logger_ctx),
	}
}

func (repl_settings_svc *ReplicationSettingsSvc) GetDefaultReplicationSettings() (*metadata.ReplicationSettings, error) {
	repl_settings_svc.replicationSettingsMtx.Lock()
	defer repl_settings_svc.replicationSettingsMtx.Unlock()

	return repl_settings_svc.getReplicationSettings(true /*populateDefault*/)
}

// getter for replication settings
// if populateDefault is false, it simple returns the settings values in metakv, which could be empty
// if populateDefault is true, it ensures that the values for all settings keys are populated
func (repl_settings_svc *ReplicationSettingsSvc) getReplicationSettings(populateDefault bool) (*metadata.ReplicationSettings, error) {
	replicationSettings, err := repl_settings_svc.getReplicationSettingsFromMetakv()
	if err != nil {
		return nil, err
	}
	if populateDefault {
		replicationSettings.PopulateDefault()
	}
	isEnterprise, err := repl_settings_svc.xdcr_topology_svc.IsMyClusterEnterprise()
	if err != nil {
		return nil, err
	}
	if !isEnterprise {
		replicationSettings.Values[base.CompressionTypeKey] = base.CompressionTypeNone
	} else if replicationSettings.Values[base.CompressionTypeKey] == base.CompressionTypeSnappy {
		replicationSettings.Values[base.CompressionTypeKey] = base.CompressionTypeAuto
	}

	return replicationSettings, nil
}

// retrieves replication settings from metakv
// returns empty replication settings if not found in metakv
func (repl_settings_svc *ReplicationSettingsSvc) getReplicationSettingsFromMetakv() (*metadata.ReplicationSettings, error) {
	// first try to read v2 replication settings from metakv
	bytes, rev, err := repl_settings_svc.metadata_svc.Get(DefaultReplicationSettingsKey)
	if err != nil && err != service_def.MetadataNotFoundErr {
		return nil, err
	}
	if err == service_def.MetadataNotFoundErr {
		repl_settings_svc.logger.Info("Default replication settings not found in metakv")
		return metadata.EmptyReplicationSettings(), nil
	}

	return repl_settings_svc.constructReplicationSettingsObject(bytes, rev)
}

func (repl_settings_svc *ReplicationSettingsSvc) UpdateDefaultReplicationSettings(settingsMap map[string]interface{}) (metadata.ReplicationSettingsMap, map[string]error, error) {
	repl_settings_svc.replicationSettingsMtx.Lock()
	defer repl_settings_svc.replicationSettingsMtx.Unlock()

	replicationSettings, err := repl_settings_svc.getReplicationSettings(false /*populateDefault*/)
	if err != nil {
		repl_settings_svc.logger.Warnf("Skipped update to default replication settings because of error retrieving current default replication settings. err=%v", err)
		return nil, nil, err
	}

	changedSettingMap, errorsMap := replicationSettings.UpdateSettingsFromMap(settingsMap)
	if len(errorsMap) > 0 {
		return nil, errorsMap, nil
	}

	if len(changedSettingMap) > 0 {
		err := repl_settings_svc.setDefaultReplicationSettings(replicationSettings)
		if err != nil {
			repl_settings_svc.logger.Warnf("Error updating default replication settings. err=%v\n", err)
			return nil, nil, err
		}
	} else {
		repl_settings_svc.logger.Infof("Skipped update to default replication settings since there have been no real changes.")
	}
	return changedSettingMap, nil, nil
}

func (repl_settings_svc *ReplicationSettingsSvc) setDefaultReplicationSettings(settings *metadata.ReplicationSettings) error {
	bytes, err := json.Marshal(settings)
	if err != nil {
		return err
	}
	if settings.Revision != nil {
		return repl_settings_svc.metadata_svc.Set(DefaultReplicationSettingsKey, bytes, settings.Revision)
	} else {
		return repl_settings_svc.metadata_svc.Add(DefaultReplicationSettingsKey, bytes)
	}
}

func (repl_settings_svc *ReplicationSettingsSvc) constructReplicationSettingsObject(value []byte, rev interface{}) (*metadata.ReplicationSettings, error) {
	settings := &metadata.ReplicationSettings{}
	err := json.Unmarshal(value, settings)
	if err != nil {
		return nil, err
	}
	settings.Revision = rev

	settings.PostProcessAfterUnmarshalling()
	return settings, nil
}
