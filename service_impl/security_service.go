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
	"github.com/couchbase/cbauth"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/service_def"
	"io/ioutil"
	"os"
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

	// used to contact other peer nodes when client certificate setting on this cluster
	// is mandatory
	clientCert []byte
	clientKey  []byte
}

func (setting *EncryptionSetting) IsStrictEncryption() bool {
	return setting.StrictEncryption
}

type SecurityService struct {
	encrytionSetting        EncryptionSetting
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
// This is because XDCR does not have its own TLS server, as it uses ns_server's TLS https proxy
func (sec *SecurityService) refreshTLSConfig() error {
	newConfig, err := cbauth.GetTLSConfig()
	if err != nil {
		sec.logger.Errorf("Failed to call cbauth.GetTLSConfig. Error: %v", err)
		return err
	}
	sec.settingMtx.Lock()
	defer sec.settingMtx.Unlock()
	sec.encrytionSetting.TlsConfig = newConfig
	sec.logger.Infof("Cluster TLS Config changed.")
	return nil
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

	if code&cbauth.CFG_CHANGE_CLIENT_CERTS_TLSCONFIG != 0 {
		// This is sent down whenever client certs are changed (or regenerated)
		// See https://docs.couchbase.com/server/current/rest-api/rest-regenerate-all-certs.html#description
		// which will regenerate both client certs and other types of certs
		if err := sec.refreshClientCertConfig(); err != nil {
			sec.logger.Error(err.Error())
			return err
		}
	}

	if code&cbauth.CFG_CHANGE_CERTS_TLSCONFIG != 0 {
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
			sec.logger.Error(err.Error())
			return err
		}
	}

	sec.encrytionSetting.initializer.Do(func() {
		// When goXDCR starts up, ns_server will automatically have client key and cert generated already
		// However, cbauth will not call ConfigRefreshCallback with CFG_CHANGE_CLIENT_CERTS_TLSCONFIG flag set
		// because it did not change (it is the originally generated cert/key)
		// So, call the same method here to at a minimum load the cert and key during initialization
		if err := sec.refreshClientCertConfig(); err != nil {
			err = fmt.Errorf("security context initializing client cert: %v", err)
			sec.logger.Error(err.Error())
		}

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

func (sec *SecurityService) refreshClientCertConfig() error {
	if len(sec.clientCertFile) == 0 {
		// Warn because there is nothing XDCR can do if ns_server did not provide correct credentials
		sec.logger.Warnf("Client Certificate location is missing. Cannot refresh certificate.")
		return nil
	}

	if len(sec.clientKeyFile) == 0 {
		// Warn because there is nothing XDCR can do if ns_server did not provide correct credentials
		sec.logger.Warnf("Client Key location is missing. Cannot refresh key.")
		return nil
	}

	certPEMBlock, err := os.ReadFile(sec.clientCertFile)
	if err != nil {
		err = fmt.Errorf("reading client cert file: %v", err)
		return err
	}

	certPool := x509.NewCertPool()
	if ok := certPool.AppendCertsFromPEM(certPEMBlock); !ok {
		return base.InvalidCerfiticateError
	}

	clientKey, err := os.ReadFile(sec.clientKeyFile)
	if err != nil {
		err = fmt.Errorf("reading client key file: %v", err)
		return err
	}

	// Do validation
	_, err = tls.X509KeyPair(certPEMBlock, clientKey)
	if err != nil {
		err = fmt.Errorf("unable to validate x509KeyPair from cbauth: %v", err)
		return err
	}

	sec.settingMtx.Lock()
	defer sec.settingMtx.Unlock()
	sec.encrytionSetting.clientCert = certPEMBlock
	sec.encrytionSetting.clientKey = clientKey
	return nil
}

func (sec *SecurityService) GetClientCertAndKey() (clientCert, clientKey []byte) {
	sec.settingMtx.RLock()
	defer sec.settingMtx.RUnlock()
	clientCert = sec.encrytionSetting.clientCert
	clientKey = sec.encrytionSetting.clientKey
	return
}

func (sec *SecurityService) SetClientCertSettingChangeCb(cb func()) {
	sec.clientCertSettingChangeCbMtx.Lock()
	defer sec.clientCertSettingChangeCbMtx.Unlock()

	sec.clientCertSettingChangeCb = cb
}
