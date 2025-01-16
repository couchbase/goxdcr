// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_impl

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/service_def"
	tlsUtils "github.com/couchbase/tools-common/http/tls"
)

type EncryptionSetting struct {
	EncryptionEnabled bool
	StrictEncryption  bool // Corresponds to clusterEncryptionLevel "strict". This disables non-SSL ports and remote cluster ref must use "full" encryption.
	AllEncryption     bool // Corresponds to clusterEncryptionLevel "all"
	CbAuthTlsConfig   cbauth.TLSConfig
	caPool            *x509.CertPool // This is generated from the certificates field.
	certificates      []byte         // This is the content of ca.pem
	initializer       sync.Once
	initializedCh     chan bool

	// used to contact other peer nodes when client certificate setting on this cluster
	// is mandatory
	clientCertKeyPair []tls.Certificate
}

func (setting *EncryptionSetting) IsEncryptionLevelStrict() bool {
	return setting.StrictEncryption
}

func (setting *EncryptionSetting) IsEncryptionLevelAll() bool {
	return setting.AllEncryption
}

func (setting *EncryptionSetting) IsEncryptionLevelStrictOrAll() bool {
	return setting.EncryptionEnabled
}

type SecurityService struct {
	encryptionSetting       EncryptionSetting
	securityChangeCallbacks map[string]service_def.SecChangeCallback
	settingMtx              sync.RWMutex
	callbackMtx             sync.RWMutex
	// babysitter starts up and passes these file locations to goxdcr to read as needed
	caFile         string
	clientKeyFile  string
	clientCertFile string
	logger         *log.CommonLogger

	clientCertSettingChangeCbMtx sync.RWMutex
	clientCertSettingChangeCb    func()
}

func NewSecurityService(caFile string, logger_ctx *log.LoggerContext) *SecurityService {
	return &SecurityService{
		encryptionSetting: EncryptionSetting{
			initializedCh: make(chan bool),
		},
		securityChangeCallbacks: make(map[string]service_def.SecChangeCallback),
		settingMtx:              sync.RWMutex{},
		callbackMtx:             sync.RWMutex{},
		caFile:                  caFile,
		logger:                  log.NewLogger("SecuritySvc", logger_ctx),
	}
}

func (sec *SecurityService) SetClientKeyFile(clientKey string) *SecurityService {
	sec.clientKeyFile = clientKey
	return sec
}

func (sec *SecurityService) SetClientCertFile(clientCert string) *SecurityService {
	sec.clientCertFile = clientCert
	return sec
}

func (sec *SecurityService) Start() error {
	cbauth.RegisterConfigRefreshCallback(sec.refresh)
	sec.logger.Infof("Security service started. Waiting for security context to be initialized")
	return nil
}

func (sec *SecurityService) getEncryptionSetting() EncryptionSetting {
	sec.settingMtx.RLock()
	defer sec.settingMtx.RUnlock()
	return sec.encryptionSetting
}

func (sec *SecurityService) GetCaPool() *x509.CertPool {
	<-sec.encryptionSetting.initializedCh
	sec.settingMtx.RLock()
	defer sec.settingMtx.RUnlock()
	return sec.encryptionSetting.caPool
}

func (sec *SecurityService) GetCACertificates() []byte {
	<-sec.encryptionSetting.initializedCh
	sec.settingMtx.RLock()
	defer sec.settingMtx.RUnlock()
	return sec.encryptionSetting.certificates
}

func (sec *SecurityService) GetClientPassphrase() []byte {
	<-sec.encryptionSetting.initializedCh
	sec.settingMtx.RLock()
	defer sec.settingMtx.RUnlock()
	return sec.encryptionSetting.CbAuthTlsConfig.ClientPrivateKeyPassphrase
}

func (sec *SecurityService) IsClusterEncryptionLevelStrict() bool {
	<-sec.encryptionSetting.initializedCh
	sec.settingMtx.RLock()
	defer sec.settingMtx.RUnlock()
	return sec.encryptionSetting.StrictEncryption
}

func (sec *SecurityService) IsClusterEncryptionLevelAll() bool {
	<-sec.encryptionSetting.initializedCh
	sec.settingMtx.RLock()
	defer sec.settingMtx.RUnlock()
	return sec.encryptionSetting.AllEncryption
}

func (sec *SecurityService) IsClusterEncryptionStrictOrAll() bool {
	return sec.EncryptData() // based on ns_server's should_cluster_data_be_encrypted() method, see MB-42373
}

func (sec *SecurityService) EncryptData() bool {
	<-sec.encryptionSetting.initializedCh
	sec.settingMtx.RLock()
	defer sec.settingMtx.RUnlock()
	return sec.encryptionSetting.EncryptionEnabled
}

func (sec *SecurityService) refreshClusterEncryption() error {
	clusterSetting, err := cbauth.GetClusterEncryptionConfig()
	if err != nil {
		sec.logger.Errorf("GetClusterEncryptionConfig returned error: %v", err)
		return err
	}

	sec.settingMtx.Lock()
	defer sec.settingMtx.Unlock()
	sec.encryptionSetting.EncryptionEnabled = clusterSetting.EncryptData
	sec.encryptionSetting.StrictEncryption = clusterSetting.DisableNonSSLPorts
	sec.encryptionSetting.AllEncryption = (clusterSetting.EncryptData && !clusterSetting.DisableNonSSLPorts) // based on ns_server's should_cluster_data_be_encrypted() and disable_non_ssl_ports() methods, see MB-32256 & MB-42373
	sec.logger.Infof("Cluster Encryption is changed to enabled=%v, strict=%v, all=%v", sec.encryptionSetting.EncryptionEnabled, sec.encryptionSetting.StrictEncryption, sec.encryptionSetting.AllEncryption)
	return nil
}

func (sec *SecurityService) refreshTLSConfig() error {
	newConfig, err := cbauth.GetTLSConfig()
	if err != nil {
		sec.logger.Errorf("Failed to call cbauth.GetTLSConfig. Error: %v", err)
		return err
	}
	sec.settingMtx.Lock()
	defer sec.settingMtx.Unlock()
	sec.encryptionSetting.CbAuthTlsConfig = newConfig
	sec.logger.Infof("Cluster TLS Config refreshed with client passphrase len=%v.", len(newConfig.ClientPrivateKeyPassphrase))
	return nil
}

func (sec *SecurityService) refreshCert() error {
	if len(sec.caFile) == 0 {
		sec.logger.Warnf("Certificate location is missing. Cannot refresh certificate.")
		return nil
	}
	certPEMBlock, err := os.ReadFile(sec.caFile)
	if err != nil {
		return err
	}

	caPool := x509.NewCertPool()
	if ok := caPool.AppendCertsFromPEM(certPEMBlock); !ok {
		return base.InvalidCerfiticateError
	}
	sec.settingMtx.Lock()
	sec.encryptionSetting.caPool = caPool
	sec.encryptionSetting.certificates = certPEMBlock
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
			sec.logger.Errorf("error refreshing for CFG_CHANGE_CLUSTER_ENCRYPTION change, err=%v", err)
			return err
		}
	}

	if code&cbauth.CFG_CHANGE_CLIENT_CERTS_TLSCONFIG != 0 {
		if err := sec.refreshTLSConfig(); err != nil {
			sec.logger.Errorf("error refreshing tlsConfig for CFG_CHANGE_CLIENT_CERTS_TLSCONFIG change, err=%v", err)
			return err
		}

		// This is sent down whenever client certs are changed (or regenerated)
		// See https://docs.couchbase.com/server/current/rest-api/rest-regenerate-all-certs.html#description
		// which will regenerate both client certs and other types of certs
		if err := sec.refreshClientCertConfig(); err != nil {
			sec.logger.Errorf("error refreshing client certs for CFG_CHANGE_CLIENT_CERTS_TLSCONFIG change, err=%v", err)
			return err
		}
	}

	if code&cbauth.CFG_CHANGE_CERTS_TLSCONFIG != 0 {
		if err := sec.refreshTLSConfig(); err != nil {
			sec.logger.Errorf("error refreshing tlsConfig for CFG_CHANGE_CERTS_TLSCONFIG change, err=%v", err)
			return err
		}

		// When a user changes the client cert config (i.e. off -> mandatory, or mandatory -> off)
		// CFG_CHANGE_CERTS_TLSCONFIG is sent down but not CFG_CHANGE_CLIENT_CERTS_TLSCONFIG
		// because the client certs themselves did not change
		// XDCR needs to ensure that any decision-making based on mandatory cert or not should be refreshed
		go func() {
			sec.clientCertSettingChangeCbMtx.RLock()
			if sec.clientCertSettingChangeCb != nil {
				sec.clientCertSettingChangeCb()
			}
			sec.clientCertSettingChangeCbMtx.RUnlock()
		}()

		if err := sec.refreshCert(); err != nil {
			sec.logger.Errorf("error refreshing certs for CFG_CHANGE_CERTS_TLSCONFIG change, err=%v", err)
			return err
		}
	}

	sec.encryptionSetting.initializer.Do(func() {
		if err := sec.refreshTLSConfig(); err != nil {
			err = fmt.Errorf("error in security context initializing TLS config: %v", err)
			sec.logger.Error(err.Error())
		}

		// When goXDCR starts up, ns_server will automatically have client key and cert generated already
		// However, cbauth will not call ConfigRefreshCallback with CFG_CHANGE_CLIENT_CERTS_TLSCONFIG flag set
		// because it did not change (it is the originally generated cert/key)
		// So, call the same method here to at a minimum load the cert and key during initialization
		if err := sec.refreshClientCertConfig(); err != nil {
			err = fmt.Errorf("error in security context initializing client cert: %v", err)
			sec.logger.Error(err.Error())
		}

		close(sec.encryptionSetting.initializedCh)
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

func (sec *SecurityService) refreshClientCertConfig() error {
	if len(sec.clientCertFile) == 0 {
		// Warn because there is nothing XDCR can do if ns_server did not provide correct credentials
		sec.logger.Warnf("Client Certificate location is missing. Cannot refresh certificate.")
		return nil
	}

	if len(sec.clientKeyFile) == 0 {
		// Warn because there is nothing XDCR can do if ns_server did not provide correct credentials
		// Also note from tlsUtils package:
		// For PKCS#12 format we don't expect a client ca and a client key, they're both stored in the same file.
		sec.logger.Warnf("Client Key location is missing. Cannot refresh key.")
		return nil
	}

	certPEMBlock, err := os.ReadFile(sec.clientCertFile)
	if err != nil {
		err = fmt.Errorf("reading client cert file: %v", err)
		return err
	}

	clientKey, err := os.ReadFile(sec.clientKeyFile)
	if err != nil {
		err = fmt.Errorf("reading client key file: %v", err)
		return err
	}

	sec.settingMtx.Lock()
	defer sec.settingMtx.Unlock()

	caCerts := sec.encryptionSetting.certificates
	passphrase := sec.encryptionSetting.CbAuthTlsConfig.ClientPrivateKeyPassphrase
	tlsConfig, err := tlsUtils.NewConfig(tlsUtils.ConfigOptions{
		ClientCert:           certPEMBlock,
		ClientKey:            clientKey,
		ClientAuthType:       tls.VerifyClientCertIfGiven,
		RootCAs:              caCerts,
		Password:             passphrase,
		IgnoreUnusedPassword: true,
	})
	if err != nil {
		err = fmt.Errorf("error parsing client certs and keys to actual tls config: %v", err)
		return err
	}

	sec.encryptionSetting.clientCertKeyPair = tlsConfig.Certificates
	sec.logger.Infof("refreshed client certs, client key and passphrase with len=%v", len(passphrase))
	return nil
}

func (sec *SecurityService) GetClientCertAndKeyPair() []tls.Certificate {
	<-sec.encryptionSetting.initializedCh
	sec.settingMtx.RLock()
	defer sec.settingMtx.RUnlock()
	return sec.encryptionSetting.clientCertKeyPair
}

func (sec *SecurityService) SetClientCertSettingChangeCb(cb func()) {
	sec.clientCertSettingChangeCbMtx.Lock()
	defer sec.clientCertSettingChangeCbMtx.Unlock()

	sec.clientCertSettingChangeCb = cb
}
