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
	"github.com/couchbase/cbauth"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/service_def"
	"io/ioutil"
	"sync"
)

type EncryptionSetting struct {
	EncrytionEnabled bool
	StrictEncryption bool // This disables non SSL port and remote cluster ref must use full encryption
	TlsConfig        cbauth.TLSConfig
	caPool           *x509.CertPool // This is generated from the certificates field.
	certificates     []byte         // This is the content of ca.pem
	initializer      sync.Once
	initializedCh    chan bool
}

func (setting *EncryptionSetting) IsStrictEncryption() bool {
	return setting.StrictEncryption
}

type SecurityService struct {
	encrytionSetting        EncryptionSetting
	securityChangeCallbacks map[string]service_def.SecChangeCallback
	settingMtx              sync.RWMutex
	callbackMtx             sync.RWMutex
	caFile                  string
	logger                  *log.CommonLogger
}

func NewSecurityService(caFile string, logger_ctx *log.LoggerContext) *SecurityService {
	return &SecurityService{
		encrytionSetting: EncryptionSetting{
			initializedCh: make(chan bool),
		},
		securityChangeCallbacks: make(map[string]service_def.SecChangeCallback),
		settingMtx:              sync.RWMutex{},
		callbackMtx:             sync.RWMutex{},
		caFile:                  caFile,
		logger:                  log.NewLogger("SecuritySvc", logger_ctx),
	}
}

func (sec *SecurityService) Start() {
	cbauth.RegisterConfigRefreshCallback(sec.refresh)
	sec.logger.Infof("Security service started. Waiting for security context to be initialized")
}

func (sec *SecurityService) getEncryptionSetting() EncryptionSetting {
	sec.settingMtx.RLock()
	defer sec.settingMtx.RUnlock()
	return sec.encrytionSetting
}

func (sec *SecurityService) GetCaPool() *x509.CertPool {
	<-sec.encrytionSetting.initializedCh
	sec.settingMtx.RLock()
	defer sec.settingMtx.RUnlock()
	return sec.encrytionSetting.caPool
}

func (sec *SecurityService) GetCACertificates() []byte {
	<-sec.encrytionSetting.initializedCh
	sec.settingMtx.RLock()
	defer sec.settingMtx.RUnlock()
	return sec.encrytionSetting.certificates
}
func (sec *SecurityService) IsClusterEncryptionLevelStrict() bool {
	<-sec.encrytionSetting.initializedCh
	sec.settingMtx.RLock()
	defer sec.settingMtx.RUnlock()
	return sec.encrytionSetting.IsStrictEncryption()
}

func (sec *SecurityService) EncryptData() bool {
	<-sec.encrytionSetting.initializedCh
	sec.settingMtx.RLock()
	defer sec.settingMtx.RUnlock()
	return sec.encrytionSetting.EncrytionEnabled
}

func (sec *SecurityService) refreshClusterEncryption() error {
	clusterSetting, err := cbauth.GetClusterEncryptionConfig()
	if err != nil {
		sec.logger.Errorf("GetClusterEncryptionConfig returned error: %v", err)
		return err
	}

	sec.settingMtx.Lock()
	defer sec.settingMtx.Unlock()
	sec.encrytionSetting.EncrytionEnabled = clusterSetting.EncryptData
	sec.encrytionSetting.StrictEncryption = clusterSetting.DisableNonSSLPorts
	sec.logger.Infof("Cluster Encryption is changed to enabled=%v, strict=%v", sec.encrytionSetting.EncrytionEnabled, sec.encrytionSetting.StrictEncryption)
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
	if len(sec.caFile) == 0 {
		sec.logger.Warnf("Certificate location is missing. Cannot refresh certificate.")
		return nil
	}
	certPEMBlock, err := ioutil.ReadFile(sec.caFile)
	if err != nil {
		return err
	}

	caPool := x509.NewCertPool()
	if ok := caPool.AppendCertsFromPEM(certPEMBlock); !ok {
		return base.InvalidCerfiticateError
	}
	sec.settingMtx.Lock()
	sec.encrytionSetting.caPool = caPool
	sec.encrytionSetting.certificates = certPEMBlock
	sec.settingMtx.Unlock()
	sec.logger.Infof("Certificates are updated.")
	return nil
}

func (sec *SecurityService) refresh(code uint64) error {
	sec.logger.Infof("Received security change notification. code %v", code)
	oldSetting := sec.getEncryptionSetting()

	// Update encryption level first.
	if code&cbauth.CFG_CHANGE_CLUSTER_ENCRYPTION != 0 {
		err := sec.refreshClusterEncryption()
		if err != nil {
			sec.logger.Error(err.Error())
			return err
		}
	}

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

	sec.encrytionSetting.initializer.Do(func() {
		close(sec.encrytionSetting.initializedCh)
		sec.logger.Infof("Security context is initialized.")
	})
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
