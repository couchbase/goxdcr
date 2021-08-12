// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_impl

import (
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"github.com/couchbase/cbauth"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/service_def"
	"io/ioutil"
	"sync"
)

type EncryptionSetting struct {
	EncrytionEabled  bool
	StrictEncryption bool // This disables non SSL port and remote cluster ref must use full encryption
	TlsConfig        cbauth.TLSConfig
	certificates     [][]byte
}

func (setting *EncryptionSetting) IsStrictEncryption() bool {
	return setting.StrictEncryption
}

type SecurityService struct {
	encrytionSetting        EncryptionSetting
	securityChangeCallbacks map[string]service_def.SecChangeCallback
	settingMtx              sync.RWMutex
	callbackMtx             sync.RWMutex
	certFile                string
	logger                  *log.CommonLogger
}

func NewSecurityService(certFile string, logger_ctx *log.LoggerContext) *SecurityService {
	return &SecurityService{
		securityChangeCallbacks: make(map[string]service_def.SecChangeCallback),
		settingMtx:              sync.RWMutex{},
		callbackMtx:             sync.RWMutex{},
		certFile:                certFile,
		logger:                  log.NewLogger("SecuritySvc", logger_ctx),
	}
}

func (sec *SecurityService) Start() {
	cbauth.RegisterConfigRefreshCallback(sec.refresh)
	sec.logger.Infof("Security service started.")
}

func (sec *SecurityService) getEncryptionSetting() EncryptionSetting {
	sec.settingMtx.RLock()
	defer sec.settingMtx.RUnlock()
	return sec.encrytionSetting
}

func (sec *SecurityService) GetCertificates() [][]byte {
	sec.settingMtx.RLock()
	defer sec.settingMtx.RUnlock()
	certificates := sec.encrytionSetting.certificates
	if len(certificates) == 0 {
		return nil
	}
	return certificates
}

func (sec *SecurityService) IsClusterEncryptionLevelStrict() bool {
	sec.settingMtx.RLock()
	defer sec.settingMtx.RUnlock()
	return sec.encrytionSetting.IsStrictEncryption()
}

func (sec *SecurityService) EncryptData() bool {
	sec.settingMtx.RLock()
	defer sec.settingMtx.RUnlock()
	return sec.encrytionSetting.EncrytionEabled
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

func (sec *SecurityService) refreshCert() error {
	if len(sec.certFile) == 0 {
		sec.logger.Warnf("Certificate location is missing. Cannot refresh certificate.")
		return nil
	}
	certPEMBlock, err := ioutil.ReadFile(sec.certFile)
	if err != nil {
		return err
	}

	certs, err := sec.processCerts(certPEMBlock)
	if err != nil {
		return err
	}
	sec.settingMtx.Lock()
	sec.encrytionSetting.certificates = certs
	sec.settingMtx.Unlock()
	sec.logger.Infof("Certificates are updated.")
	return nil
}

func (sec *SecurityService) processCerts(certPEMBlock []byte) (certs [][]byte, err error) {
	var skippedBlockTypes []string
	for {
		var certDERBlock *pem.Block
		certDERBlock, certPEMBlock = pem.Decode(certPEMBlock)
		if certDERBlock == nil {
			break
		}
		if certDERBlock.Type == "CERTIFICATE" {
			_, err := x509.ParseCertificate(certDERBlock.Bytes)
			if err != nil {
				return nil, err
			}
			certs = append(certs, certDERBlock.Bytes)
		} else {
			skippedBlockTypes = append(skippedBlockTypes, certDERBlock.Type)
		}
	}
	if len(certs) == 0 {
		if len(skippedBlockTypes) == 0 {
			return nil, errors.New("Failed to find any PEM data in certificate file.")
		} else {
			return nil, fmt.Errorf("Failed to find certificate PEM data in certificate file after skipping the following types: %v", skippedBlockTypes)
		}
	}
	return certs, nil
}

func (sec *SecurityService) refresh(code uint64) error {
	sec.logger.Infof("Received security change notification. code %v", code)
	oldSetting := sec.getEncryptionSetting()

	if code&cbauth.CFG_CHANGE_CERTS_TLSCONFIG != 0 {
		if err := sec.refreshTLSConfig(); err != nil {
			sec.logger.Error(err.Error())
			return err
		}

		if err := sec.refreshCert(); err != nil {
			sec.logger.Error(err.Error())
			return err
		}
	}

	if code&cbauth.CFG_CHANGE_CLUSTER_ENCRYPTION != 0 {
		err := sec.refreshClusterEncryption()
		if err != nil {
			sec.logger.Error(err.Error())
			return err
		}
	}
	newSetting := sec.getEncryptionSetting()
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
