// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_impl

import (
	"github.com/couchbase/cbauth"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/service_def"
	"sync"
)

type EncryptionSetting struct {
	EncrytionEabled  bool
	StrictEncryption bool // This disables non SSL port and remote cluster ref must use full encryption
	TlsConfig        cbauth.TLSConfig
}

func (setting *EncryptionSetting) IsStrictEncryption() bool {
	return setting.StrictEncryption
}

type SecurityService struct {
	encrytionSetting        EncryptionSetting
	securityChangeCallbacks map[string]service_def.SecChangeCallback
	settingMtx              sync.RWMutex
	callbackMtx             sync.RWMutex
	logger                  *log.CommonLogger
}

func NewSecurityService(logger_ctx *log.LoggerContext) *SecurityService {
	return &SecurityService{
		securityChangeCallbacks: make(map[string]service_def.SecChangeCallback),
		settingMtx:              sync.RWMutex{},
		callbackMtx:             sync.RWMutex{},
		logger:                  log.NewLogger("SecuritySvc", logger_ctx),
	}
}

func (sec *SecurityService) Start() {
	cbauth.RegisterConfigRefreshCallback(sec.refresh)
	sec.logger.Infof("Security service started.")
}

func (sec *SecurityService) GetEncryptionSetting() EncryptionSetting {
	sec.settingMtx.RLock()
	defer sec.settingMtx.RUnlock()
	return sec.encrytionSetting
}
func (sec *SecurityService) IsClusterEncryptionLevelStrict() bool {
	sec.settingMtx.RLock()
	defer sec.settingMtx.RUnlock()
	return sec.encrytionSetting.IsStrictEncryption()
}

func (sec *SecurityService) refreshClusterEncryption() error {
	clusterSetting, err := cbauth.GetClusterEncryptionConfig()
	if err != nil {
		sec.logger.Errorf("GetClusterEncryptionConfig returned error: %v", err)
		return err
	}

	sec.settingMtx.Lock()
	defer sec.settingMtx.Unlock()
	sec.encrytionSetting.EncrytionEabled = clusterSetting.EncryptData
	sec.encrytionSetting.StrictEncryption = clusterSetting.DisableNonSSLPorts
	sec.logger.Infof("Cluster Encryption is changed to enabled=%v, strict=%v", sec.encrytionSetting.EncrytionEabled, sec.encrytionSetting.StrictEncryption)
	return nil
}

// Currently GetTLSConfig will return error "TLSConfig is not present for this service".
// This will be updated after ns_server pass the key/certificate from the commandline and xdcr can get TLSConfig.
func (sec *SecurityService) refreshTLSConfig() error {
	return nil
	//newConfig, err := cbauth.GetTLSConfig()
	//if err != nil {
	//	sec.logger.Errorf("Failed to call cbauth.GetTLSConfig. Error: %v", err)
	//	return err
	//}
	//sec.mutex.Lock()
	//defer sec.mutex.Unlock()
	//sec.encrytionSetting.TlsConfig = newConfig
	//sec.logger.Infof("Cluster TLS Config changed.")
	//return nil
}

// We can refresh the key/certificate once we have the file locations from the commandline
func (sec *SecurityService) refreshCert() error {
	return nil
}
func (sec *SecurityService) refresh(code uint64) error {
	sec.logger.Infof("Received security change notification. code %v", code)
	oldSetting := sec.GetEncryptionSetting()

	if code&cbauth.CFG_CHANGE_CERTS_TLSCONFIG != 0 {
		if err := sec.refreshTLSConfig(); err != nil {
			return err
		}

		if err := sec.refreshCert(); err != nil {
			return err
		}
	}

	if code&cbauth.CFG_CHANGE_CLUSTER_ENCRYPTION != 0 {
		err := sec.refreshClusterEncryption()
		if err != nil {
			return err
		}
	}
	newSetting := sec.GetEncryptionSetting()
	sec.callbackMtx.RLock()
	defer sec.callbackMtx.RUnlock()
	for _, funcName := range sec.securityChangeCallbacks {
		go funcName(&oldSetting, &newSetting)
	}
	return nil
}

func (sec *SecurityService) SetEncryptionLevelChangeCallback(key string, callback service_def.SecChangeCallback) {
	sec.callbackMtx.Lock()
	defer sec.callbackMtx.Unlock()
	sec.securityChangeCallbacks[key] = callback
}
