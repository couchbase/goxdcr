// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package metadata

import (
	"bytes"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"reflect"
	"strings"
)

const (
	// ids of remote cluster refs are used as keys in gometa service.
	// the following prefix distinguishes the remote cluster refs from other entries
	// and reduces the chance of naming conflicts
	RemoteClusterKeyPrefix = "remoteCluster"
)

var SizeOfRemoteClusterRefId = 32
var MaxRetryForIdGeneration = 5

const (
	EncryptionType_Full string = "full"
	EncryptionType_Half string = "half"
)

/************************************
/* struct RemoteClusterReference
 * NOTE - if adding/removing new members, need to also modify LoadFrom(), etc.
*************************************/
type RemoteClusterReference struct {
	Id       string `json:"id"`
	Uuid     string `json:"uuid"`
	Name     string `json:"name"`
	HostName string `json:"hostName"`
	UserName string `json:"userName"`
	Password string `json:"password"`

	DemandEncryption bool   `json:"demandEncryption"`
	EncryptionType   string `json:"encryptionType"`
	Certificate      []byte `json:"certificate"`
	// hostname to use when making https connection
	HttpsHostName    string            `json:"httpsHostName"`
	SANInCertificate bool              `json:"SANInCertificate"`
	HttpAuthMech     base.HttpAuthMech `json:"httpAuthMech"`

	ClientCertificate []byte `json:"clientCertificate"`
	ClientKey         []byte `json:"clientKey"`
	// client cert auth setting on target
	ClientCertAuthSetting base.ClientCertAuth `json:"clientCertAuthSetting"`

	// these are hostname actually used to connect to target
	// they are rotated among nodes in target cluster to achieve load balancing on target
	// they are used to update HostName/HttpsHostName when HostName has been removed from the target cluster
	// they are not exposed to users, e.g., through UI or rest api
	ActiveHostName      string `json:"activeHostName"`
	ActiveHttpsHostName string `json:"activeHttpsHostName"`

	// revision number to be used by metadata service. not included in json
	// Revision should only be passed along and should never be modified
	Revision interface{}
}

func NewRemoteClusterReference(uuid, name, hostName, userName, password string,
	demandEncryption bool, encryptionType string, certificate, clientCertificate, clientKey []byte) (*RemoteClusterReference, error) {
	refId, err := RemoteClusterRefId()
	if err != nil {
		return nil, err
	}

	return &RemoteClusterReference{Id: refId,
		Uuid:              uuid,
		Name:              name,
		HostName:          hostName,
		UserName:          userName,
		Password:          password,
		DemandEncryption:  demandEncryption,
		EncryptionType:    encryptionType,
		Certificate:       certificate,
		ClientCertificate: clientCertificate,
		ClientKey:         clientKey,
	}, nil
}

func RemoteClusterRefId() (string, error) {
	refUuid, err := base.GenerateRandomId(SizeOfRemoteClusterRefId, MaxRetryForIdGeneration)
	if err != nil {
		return "", err
	}
	parts := []string{RemoteClusterKeyPrefix, refUuid}
	return strings.Join(parts, base.KeyPartsDelimiter), nil
}

// implements base.ClusterConnectionInfoProvider
func (ref *RemoteClusterReference) MyConnectionStr() (string, error) {
	if ref.IsHttps() {
		if len(ref.ActiveHttpsHostName) > 0 {
			return ref.ActiveHttpsHostName, nil
		} else {
			return ref.HttpsHostName, nil
		}
	} else {
		if len(ref.ActiveHostName) > 0 {
			return ref.ActiveHostName, nil
		} else {
			return ref.HostName, nil
		}
	}
}

func (ref *RemoteClusterReference) Redact() *RemoteClusterReference {
	if ref != nil {
		if len(ref.UserName) > 0 && !base.IsStringRedacted(ref.UserName) {
			ref.UserName = base.TagUD(ref.UserName)
		}
		if len(ref.Password) > 0 && !base.IsStringRedacted(ref.Password) {
			ref.Password = base.TagUD(ref.Password)
		}
		if len(ref.Certificate) > 0 && !base.IsByteSliceRedacted(ref.Certificate) {
			ref.Certificate = base.TagUDBytes(ref.Certificate)
		}
		if len(ref.ClientCertificate) > 0 && !base.IsByteSliceRedacted(ref.ClientCertificate) {
			ref.ClientCertificate = base.TagUDBytes(ref.ClientCertificate)
		}
		// no need to redact ClientKey since it is always nil in this ref for redact
	}
	return ref
}

func (ref *RemoteClusterReference) CloneAndRedact() *RemoteClusterReference {
	if ref != nil {
		return ref.CloneForRedact().Redact()
	}
	return ref
}

func (ref *RemoteClusterReference) MyCredentials() (string, string, base.HttpAuthMech, []byte, bool, []byte, []byte, base.ClientCertAuth, error) {
	return ref.UserName, ref.Password, ref.HttpAuthMech, ref.Certificate, ref.SANInCertificate, ref.ClientCertificate, ref.ClientKey, ref.ClientCertAuthSetting, nil
}

// convert to a map for output
// do not include password or client private key
func (ref *RemoteClusterReference) ToMap() map[string]interface{} {
	uri := base.UrlDelimiter + base.RemoteClustersPath + base.UrlDelimiter + ref.Name
	validateUri := uri + base.JustValidatePostfix
	outputMap := make(map[string]interface{})
	outputMap[base.RemoteClusterUuid] = ref.Uuid
	outputMap[base.RemoteClusterName] = ref.Name
	outputMap[base.RemoteClusterUri] = uri
	outputMap[base.RemoteClusterValidateUri] = validateUri
	outputMap[base.RemoteClusterHostName] = ref.HostName
	outputMap[base.RemoteClusterUserName] = ref.UserName
	outputMap[base.RemoteClusterDeleted] = false
	outputMap[base.RemoteClusterSecureType] = ref.GetSecureTypeString()
	if len(ref.Certificate) > 0 {
		outputMap[base.RemoteClusterCertificate] = string(ref.Certificate)
	}
	if len(ref.ClientCertificate) > 0 {
		outputMap[base.RemoteClusterClientCertificate] = string(ref.ClientCertificate)
	}

	return outputMap
}

// checks if the passed in ref is the same as the current ref
func (ref *RemoteClusterReference) IsSame(ref2 *RemoteClusterReference) bool {
	if ref == nil {
		return ref2 == nil
	}
	if ref2 == nil {
		return false
	}
	if !ref.IsEssentiallySame(ref2) {
		return false
	} else {
		return reflect.DeepEqual(ref.Revision, ref2.Revision) && ref.HttpsHostName == ref2.HttpsHostName &&
			ref.ActiveHostName == ref2.ActiveHostName && ref.ActiveHttpsHostName == ref2.ActiveHttpsHostName
	}
}

func (ref *RemoteClusterReference) AreUserSecurityCredentialsTheSame(ref2 *RemoteClusterReference) bool {
	if ref == nil {
		return ref2 == nil
	}
	if ref2 == nil {
		return false
	}
	return ref.UserName == ref2.UserName && ref.Password == ref2.Password && ref.DemandEncryption == ref2.DemandEncryption &&
		ref.EncryptionType == ref2.EncryptionType && bytes.Equal(ref.Certificate, ref2.Certificate) &&
		bytes.Equal(ref.ClientCertificate, ref2.ClientCertificate) && bytes.Equal(ref.ClientKey, ref2.ClientKey)
}

func (ref *RemoteClusterReference) AreSecuritySettingsTheSame(ref2 *RemoteClusterReference) bool {
	if ref == nil {
		return ref2 == nil
	}
	if ref2 == nil {
		return false
	}
	return ref.SANInCertificate == ref2.SANInCertificate && ref.ClientCertAuthSetting == ref2.ClientCertAuthSetting &&
		ref.HttpAuthMech == ref2.HttpAuthMech
}

// checks if they are the same minus changable fields
func (ref *RemoteClusterReference) IsEssentiallySame(ref2 *RemoteClusterReference) bool {
	if ref == nil {
		return ref2 == nil
	}
	if ref2 == nil {
		return false
	}
	if !ref.AreUserSecurityCredentialsTheSame(ref2) || !ref.AreSecuritySettingsTheSame(ref2) {
		return false
	} else {
		return ref.Id == ref2.Id && ref.Uuid == ref2.Uuid && ref.Name == ref2.Name && ref.HostName == ref2.HostName
	}
}

// Caller of this function should be wary of the need for redaction. Recommended to use it on an already redacted object
func (ref *RemoteClusterReference) String() string {
	if ref == nil {
		return "nil"
	}

	// redact password and client private key
	var password string
	if len(ref.Password) > 0 {
		password = "xxxx"
	}
	var clientKey string
	// cannot do "if len(ref.ClientKey) > 0" since ClientKey may have been changed to nil due to redaction.
	// check ClientCertificate instead
	if len(ref.ClientCertificate) > 0 {
		clientKey = "xxxx"
	}

	return fmt.Sprintf("id:%v; uuid:%v; name:%v; hostName:%v; userName:%v; password:%v; secureType:%v; certificate:%v; clientCertificate:%v; clientKey:%v; SanInCertificate:%v; ClientCertAuthSetting:%v; HttpAuthMech:%v, revision:%v",
		ref.Id, ref.Uuid, ref.Name, ref.HostName, ref.UserName, password, ref.GetSecureTypeString(), ref.Certificate, ref.ClientCertificate, clientKey, ref.SANInCertificate, ref.ClientCertAuthSetting, ref.HttpAuthMech, ref.Revision)
}

func (ref *RemoteClusterReference) LoadFrom(inRef *RemoteClusterReference) {
	if ref == nil {
		return
	}
	ref.LoadNonActivesFrom(inRef)
	ref.ActiveHostName = inRef.ActiveHostName
	ref.ActiveHttpsHostName = inRef.ActiveHttpsHostName
}

func (ref *RemoteClusterReference) LoadNonActivesFrom(inRef *RemoteClusterReference) {
	if ref == nil {
		return
	}
	ref.Id = inRef.Id
	ref.Uuid = inRef.Uuid
	ref.Name = inRef.Name
	ref.HostName = inRef.HostName
	ref.UserName = inRef.UserName
	ref.Password = inRef.Password
	ref.DemandEncryption = inRef.DemandEncryption
	ref.Certificate = base.DeepCopyByteArray(inRef.Certificate)
	ref.ClientCertificate = base.DeepCopyByteArray(inRef.ClientCertificate)
	ref.ClientKey = base.DeepCopyByteArray(inRef.ClientKey)
	ref.HttpsHostName = inRef.HttpsHostName
	ref.EncryptionType = inRef.EncryptionType
	ref.SANInCertificate = inRef.SANInCertificate
	ref.ClientCertAuthSetting = inRef.ClientCertAuthSetting
	ref.HttpAuthMech = inRef.HttpAuthMech
	// !!! shallow copy of revision.
	// ref.Revision should only be passed along and should never be modified
	ref.Revision = inRef.Revision
}

func (ref *RemoteClusterReference) Clone() *RemoteClusterReference {
	if ref == nil {
		return nil
	}

	cloneRef := ref.CloneForRedact()
	cloneRef.ClientKey = base.DeepCopyByteArray(ref.ClientKey)
	return cloneRef
}

func (ref *RemoteClusterReference) CloneForRedact() *RemoteClusterReference {
	if ref == nil {
		return nil
	}
	cloneRef := ref.cloneCommonFields()
	cloneRef.ActiveHostName = ref.ActiveHostName
	cloneRef.ActiveHttpsHostName = ref.ActiveHttpsHostName
	return cloneRef
}

// clone for metakv update, i.e., clone without any internal fields
func (ref *RemoteClusterReference) CloneForMetakvUpdate() *RemoteClusterReference {
	if ref == nil {
		return nil
	}

	cloneRef := ref.cloneCommonFields()
	cloneRef.ClientKey = base.DeepCopyByteArray(ref.ClientKey)
	// no need for Revision in metakv.
	// cloneRef.Revision is a shallow copy, hence it is not a big waste to copy and then set it to nil
	cloneRef.Revision = nil
	return cloneRef
}

// clone of common fields needed by all Clonexxx() APIs
func (ref *RemoteClusterReference) cloneCommonFields() *RemoteClusterReference {
	if ref == nil {
		return nil
	}
	return &RemoteClusterReference{Id: ref.Id,
		Uuid:                  ref.Uuid,
		Name:                  ref.Name,
		HostName:              ref.HostName,
		HttpsHostName:         ref.HttpsHostName,
		UserName:              ref.UserName,
		Password:              ref.Password,
		DemandEncryption:      ref.DemandEncryption,
		Certificate:           base.DeepCopyByteArray(ref.Certificate),
		ClientCertificate:     base.DeepCopyByteArray(ref.ClientCertificate),
		EncryptionType:        ref.EncryptionType,
		SANInCertificate:      ref.SANInCertificate,
		ClientCertAuthSetting: ref.ClientCertAuthSetting,
		HttpAuthMech:          ref.HttpAuthMech,
		// !!! shallow copy of revision.
		// ref.Revision should only be passed along and should never be modified
		Revision: ref.Revision,
	}
}

func (ref *RemoteClusterReference) IsEncryptionEnabled() bool {
	return ref.DemandEncryption
}

var remoteClusterReferenceSampleEmptyRef *RemoteClusterReference = &RemoteClusterReference{}

func (ref *RemoteClusterReference) IsEmpty() bool {
	return ref.IsSame(remoteClusterReferenceSampleEmptyRef)
}

func (ref *RemoteClusterReference) IsFullEncryption() bool {
	// ref.EncryptionType may be empty for unupgraded remote cluster refs. treat it as "full" in this case
	return ref.DemandEncryption && (len(ref.EncryptionType) == 0 || ref.EncryptionType == EncryptionType_Full)
}

func (ref *RemoteClusterReference) IsHalfEncryption() bool {
	return ref.DemandEncryption && ref.EncryptionType == EncryptionType_Half
}

func (ref *RemoteClusterReference) IsHttps() bool {
	return ref.HttpAuthMech == base.HttpAuthMechHttps
}

func (ref *RemoteClusterReference) Clear() {
	ref.LoadFrom(remoteClusterReferenceSampleEmptyRef)
}

func (ref *RemoteClusterReference) GetSecureTypeString() string {
	if !ref.IsEncryptionEnabled() {
		return base.SecureTypeNone
	} else if ref.IsFullEncryption() {
		return base.SecureTypeFull
	} else {
		return base.SecureTypeHalf
	}
}
