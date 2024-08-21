// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_def

import "crypto/x509"

type EncryptionSettingIface interface {
	IsStrictEncryption() bool
}

type SecuritySvc interface {
	Start() error
	IsClusterEncryptionLevelStrict() bool
	EncryptData() bool
	GetCACertificates() []byte
	GetCaPool() *x509.CertPool
	SetEncryptionLevelChangeCallback(key string, callback SecChangeCallback)

	GetClientCertAndKey() ([]byte, []byte)
	SetClientCertSettingChangeCb(func())
}

type SecChangeCallback func(old, new EncryptionSettingIface)
