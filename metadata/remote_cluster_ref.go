// Copyright (c) 2013-2020 Couchbase, Inc.
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
	"encoding/json"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"reflect"
	"strings"
	"sync"
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
	Id_       string `json:"Id"`
	Uuid_     string `json:"Uuid"`
	Name_     string `json:"Name"`
	HostName_ string `json:"HostName"`
	UserName_ string `json:"UserName"`
	Password_ string `json:"Password"`

	DemandEncryption_ bool   `json:"DemandEncryption"`
	EncryptionType_   string `json:"EncryptionType"`
	Certificate_      []byte `json:"Certificate"`
	// hostname to use when making https connection
	HttpsHostName_    string            `json:"HttpsHostName"`
	SANInCertificate_ bool              `json:"SANInCertificate"`
	HttpAuthMech_     base.HttpAuthMech `json:"HttpAuthMech"`

	ClientCertificate_ []byte `json:"ClientCertificate"`
	ClientKey_         []byte `json:"ClientKey"`

	// these are hostname actually used to connect to target
	// they are rotated among nodes in target cluster to achieve load balancing on target
	// they are used to update HostName/HttpsHostName when HostName has been removed from the target cluster
	// they are not exposed to users, e.g., through UI or rest api
	ActiveHostName_      string `json:"ActiveHostName"`
	ActiveHttpsHostName_ string `json:"ActiveHttpsHostName"`

	// revision number to be used by metadata service. not included in json
	// Revision should only be passed along and should never be modified
	revision interface{}

	// Internal RW Mutex to prevent concurrent data race
	mutex sync.RWMutex
}

func NewRemoteClusterReference(uuid, name, hostName, userName, password string,
	demandEncryption bool, encryptionType string, certificate, clientCertificate, clientKey []byte) (*RemoteClusterReference, error) {
	refId, err := RemoteClusterRefId()
	if err != nil {
		return nil, err
	}

	return &RemoteClusterReference{Id_: refId,
		Uuid_:              uuid,
		Name_:              name,
		HostName_:          hostName,
		UserName_:          userName,
		Password_:          password,
		DemandEncryption_:  demandEncryption,
		EncryptionType_:    encryptionType,
		Certificate_:       certificate,
		ClientCertificate_: clientCertificate,
		ClientKey_:         clientKey,
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
		activeHttpsHostName := ref.ActiveHttpsHostName()
		if len(activeHttpsHostName) > 0 {
			return activeHttpsHostName, nil
		} else {
			return ref.HttpsHostName(), nil
		}
	} else {
		activeHostName := ref.ActiveHostName()
		if len(activeHostName) > 0 {
			return activeHostName, nil
		} else {
			return ref.HostName(), nil
		}
	}
}

func (ref *RemoteClusterReference) redactNoLock() *RemoteClusterReference {
	if ref != nil {
		if len(ref.UserName_) > 0 && !base.IsStringRedacted(ref.UserName_) {
			ref.UserName_ = base.TagUD(ref.UserName_)
		}
		if len(ref.Password_) > 0 && !base.IsStringRedacted(ref.Password_) {
			ref.Password_ = base.TagUD(ref.Password_)
		}
		if len(ref.Certificate_) > 0 && !base.IsByteSliceRedacted(ref.Certificate_) {
			ref.Certificate_ = base.TagUDBytes(ref.Certificate_)
		}
		if len(ref.ClientCertificate_) > 0 && !base.IsByteSliceRedacted(ref.ClientCertificate_) {
			ref.ClientCertificate_ = base.TagUDBytes(ref.ClientCertificate_)
		}
		// no need to redact ClientKey since it is always nil in this ref for redact
	}
	return ref
}

func (ref *RemoteClusterReference) CloneAndRedact() *RemoteClusterReference {
	if ref != nil {
		ref.mutex.RLock()
		defer ref.mutex.RUnlock()
		return ref.cloneForRedactNoLock().redactNoLock()
	}
	return ref
}

func (ref *RemoteClusterReference) MyCredentials() (string, string, base.HttpAuthMech, []byte, bool, []byte, []byte, error) {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.UserName_, ref.Password_, ref.HttpAuthMech_, ref.Certificate_, ref.SANInCertificate_, ref.ClientCertificate_, ref.ClientKey_, nil
}

// convert to a map for output
// do not include password or client private key
func (ref *RemoteClusterReference) ToMap() map[string]interface{} {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	uri := base.UrlDelimiter + base.RemoteClustersPath + base.UrlDelimiter + ref.Name_
	validateUri := uri + base.JustValidatePostfix
	outputMap := make(map[string]interface{})
	outputMap[base.RemoteClusterUuid] = ref.Uuid_
	outputMap[base.RemoteClusterName] = ref.Name_
	outputMap[base.RemoteClusterUri] = uri
	outputMap[base.RemoteClusterValidateUri] = validateUri
	outputMap[base.RemoteClusterHostName] = ref.HostName_
	outputMap[base.RemoteClusterUserName] = ref.UserName_
	outputMap[base.RemoteClusterDeleted] = false
	outputMap[base.RemoteClusterSecureType] = ref.SecureTypeString()
	// To be deprecated
	if ref.IsEncryptionEnabled() {
		outputMap[base.RemoteClusterDemandEncryption] = ref.DemandEncryption_
		outputMap[base.RemoteClusterEncryptionType] = ref.EncryptionType_
	}
	if len(ref.Certificate_) > 0 {
		outputMap[base.RemoteClusterCertificate] = string(ref.Certificate_)
	}
	if len(ref.ClientCertificate_) > 0 {
		outputMap[base.RemoteClusterClientCertificate] = string(ref.ClientCertificate_)
	}

	return outputMap
}

// checks if the passed in ref is the same as the current ref
func (ref *RemoteClusterReference) IsSame(ref2 *RemoteClusterReference) bool {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	if ref == nil {
		return ref2 == nil
	}
	if ref2 == nil {
		return false
	}
	if !ref.isEssentiallySameNoLock(ref2) {
		return false
	} else {
		ref2.mutex.RLock()
		defer ref2.mutex.RUnlock()
		return reflect.DeepEqual(ref.revision, ref2.revision) && ref.HttpsHostName_ == ref2.HttpsHostName_ &&
			ref.ActiveHostName_ == ref2.ActiveHostName_ && ref.ActiveHttpsHostName_ == ref2.ActiveHttpsHostName_
	}
}

func (ref *RemoteClusterReference) AreUserSecurityCredentialsTheSame(ref2 *RemoteClusterReference) bool {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.areUserSecurityCredentialsTheSameNoLock(ref2)
}

func (ref *RemoteClusterReference) areUserSecurityCredentialsTheSameNoLock(ref2 *RemoteClusterReference) bool {
	if ref == nil {
		return ref2 == nil
	}
	if ref2 == nil {
		return false
	}
	ref2.mutex.RLock()
	defer ref2.mutex.RUnlock()
	return ref.UserName_ == ref2.UserName_ && ref.Password_ == ref2.Password_ && ref.DemandEncryption_ == ref2.DemandEncryption_ &&
		ref.EncryptionType_ == ref2.EncryptionType_ && bytes.Equal(ref.Certificate_, ref2.Certificate_) &&
		bytes.Equal(ref.ClientCertificate_, ref2.ClientCertificate_) && bytes.Equal(ref.ClientKey_, ref2.ClientKey_)
}

func (ref *RemoteClusterReference) AreSecuritySettingsTheSame(ref2 *RemoteClusterReference) bool {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.areSecuritySettingsTheSameNoLock(ref2)
}

func (ref *RemoteClusterReference) areSecuritySettingsTheSameNoLock(ref2 *RemoteClusterReference) bool {
	if ref == nil {
		return ref2 == nil
	}
	if ref2 == nil {
		return false
	}
	ref2.mutex.RLock()
	defer ref2.mutex.RUnlock()
	return ref.SANInCertificate_ == ref2.SANInCertificate_ &&
		ref.HttpAuthMech_ == ref2.HttpAuthMech_
}

// checks if they are the same minus changable fields
func (ref *RemoteClusterReference) IsEssentiallySame(ref2 *RemoteClusterReference) bool {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.isEssentiallySameNoLock(ref2)
}

func (ref *RemoteClusterReference) isEssentiallySameNoLock(ref2 *RemoteClusterReference) bool {
	if ref == nil {
		return ref2 == nil
	}
	if ref2 == nil {
		return false
	}
	if !ref.areUserSecurityCredentialsTheSameNoLock(ref2) || !ref.areSecuritySettingsTheSameNoLock(ref2) {
		return false
	} else {
		ref2.mutex.RLock()
		defer ref2.mutex.RUnlock()
		return ref.Id_ == ref2.Id_ && ref.Uuid_ == ref2.Uuid_ && ref.Name_ == ref2.Name_ && ref.HostName_ == ref2.HostName_
	}
}

// Caller of this function should be wary of the need for redaction. Recommended to use it on an already redacted object
func (ref *RemoteClusterReference) String() string {
	if ref == nil {
		return "nil"
	}

	ref.mutex.RLock()
	defer ref.mutex.RUnlock()

	// redact password and client private key
	var password string
	if len(ref.Password_) > 0 {
		password = "xxxx"
	}
	var clientKey string
	// cannot do "if len(ref.ClientKey) > 0" since ClientKey may have been changed to nil due to redaction.
	// check ClientCertificate instead
	if len(ref.ClientCertificate_) > 0 {
		clientKey = "xxxx"
	}

	return fmt.Sprintf("id:%v; uuid:%v; name:%v; hostName:%v; userName:%v; password:%v; secureType:%v; certificate:%v; clientCertificate:%v; clientKey:%v; SanInCertificate:%v; HttpAuthMech:%v, revision:%v",
		ref.Id_, ref.Uuid_, ref.Name_, ref.HostName_, ref.UserName_, password, ref.SecureTypeString(), ref.Certificate_, ref.ClientCertificate_, clientKey, ref.SANInCertificate_, ref.HttpAuthMech_, ref.revision)
}

func (ref *RemoteClusterReference) LoadFrom(inRef *RemoteClusterReference) {
	if ref == nil {
		return
	}
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.loadNonActivesFromNoLock(inRef)
	ref.ActiveHostName_ = inRef.ActiveHostName()
	ref.ActiveHttpsHostName_ = inRef.ActiveHttpsHostName()
}

func (ref *RemoteClusterReference) LoadNonActivesFrom(inRef *RemoteClusterReference) {
	if ref == nil {
		return
	}
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.loadNonActivesFromNoLock(inRef)
}

func (ref *RemoteClusterReference) loadNonActivesFromNoLock(inRef *RemoteClusterReference) {
	inRef.mutex.RLock()
	defer inRef.mutex.RUnlock()
	ref.Id_ = inRef.Id_
	ref.Uuid_ = inRef.Uuid_
	ref.Name_ = inRef.Name_
	ref.HostName_ = inRef.HostName_
	ref.UserName_ = inRef.UserName_
	ref.Password_ = inRef.Password_
	ref.DemandEncryption_ = inRef.DemandEncryption_
	ref.Certificate_ = base.DeepCopyByteArray(inRef.Certificate_)
	ref.ClientCertificate_ = base.DeepCopyByteArray(inRef.ClientCertificate_)
	ref.ClientKey_ = base.DeepCopyByteArray(inRef.ClientKey_)
	ref.HttpsHostName_ = inRef.HttpsHostName_
	ref.EncryptionType_ = inRef.EncryptionType_
	ref.SANInCertificate_ = inRef.SANInCertificate_
	ref.HttpAuthMech_ = inRef.HttpAuthMech_
	// !!! shallow copy of revision.
	// ref.Revision should only be passed along and should never be modified
	ref.revision = inRef.revision
}

func (ref *RemoteClusterReference) Clone() *RemoteClusterReference {
	if ref == nil {
		return nil
	}

	ref.mutex.RLock()
	defer ref.mutex.RUnlock()

	cloneRef := ref.cloneForRedactNoLock()
	cloneRef.ClientKey_ = base.DeepCopyByteArray(ref.ClientKey_)
	return cloneRef
}

func (ref *RemoteClusterReference) cloneForRedactNoLock() *RemoteClusterReference {
	cloneRef := ref.cloneCommonFieldsNoLock()
	cloneRef.ActiveHostName_ = ref.ActiveHostName_
	cloneRef.ActiveHttpsHostName_ = ref.ActiveHttpsHostName_
	return cloneRef
}

// clone for metakv update, i.e., clone without any internal fields
func (ref *RemoteClusterReference) CloneForMetakvUpdate() *RemoteClusterReference {
	if ref == nil {
		return nil
	}

	ref.mutex.RLock()
	defer ref.mutex.RUnlock()

	cloneRef := ref.cloneCommonFieldsNoLock()
	cloneRef.ClientKey_ = base.DeepCopyByteArray(ref.ClientKey_)
	// no need for Revision in metakv.
	// cloneRef.Revision is a shallow copy, hence it is not a big waste to copy and then set it to nil
	cloneRef.revision = nil
	return cloneRef
}

// clone of common fields needed by all Clonexxx() APIs
func (ref *RemoteClusterReference) cloneCommonFieldsNoLock() *RemoteClusterReference {
	if ref == nil {
		return nil
	}
	return &RemoteClusterReference{Id_: ref.Id_,
		Uuid_:              ref.Uuid_,
		Name_:              ref.Name_,
		HostName_:          ref.HostName_,
		HttpsHostName_:     ref.HttpsHostName_,
		UserName_:          ref.UserName_,
		Password_:          ref.Password_,
		DemandEncryption_:  ref.DemandEncryption_,
		Certificate_:       base.DeepCopyByteArray(ref.Certificate_),
		ClientCertificate_: base.DeepCopyByteArray(ref.ClientCertificate_),
		EncryptionType_:    ref.EncryptionType_,
		SANInCertificate_:  ref.SANInCertificate_,
		HttpAuthMech_:      ref.HttpAuthMech_,
		// !!! shallow copy of revision.
		// ref.Revision should only be passed along and should never be modified
		revision: ref.revision,
	}
}

func (ref *RemoteClusterReference) IsEncryptionEnabled() bool {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.DemandEncryption_
}

var remoteClusterReferenceSampleEmptyRef *RemoteClusterReference = &RemoteClusterReference{}

func (ref *RemoteClusterReference) IsEmpty() bool {
	return ref.IsSame(remoteClusterReferenceSampleEmptyRef)
}

func (ref *RemoteClusterReference) IsFullEncryption() bool {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	// ref.EncryptionType may be empty for unupgraded remote cluster refs. treat it as "full" in this case
	return ref.DemandEncryption_ && (len(ref.EncryptionType_) == 0 || ref.EncryptionType_ == EncryptionType_Full)
}

func (ref *RemoteClusterReference) IsHalfEncryption() bool {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.DemandEncryption_ && ref.EncryptionType_ == EncryptionType_Half
}

func (ref *RemoteClusterReference) IsHttps() bool {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.HttpAuthMech_ == base.HttpAuthMechHttps
}

func (ref *RemoteClusterReference) Clear() {
	ref.LoadFrom(remoteClusterReferenceSampleEmptyRef)
}

func (ref *RemoteClusterReference) ClearRevision() {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.revision = nil
}

// Getters and setters
func (ref *RemoteClusterReference) SecureTypeString() string {
	if !ref.IsEncryptionEnabled() {
		return base.SecureTypeNone
	} else if ref.IsFullEncryption() {
		return base.SecureTypeFull
	} else {
		return base.SecureTypeHalf
	}
}

func (ref *RemoteClusterReference) Id() string {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.Id_
}

func (ref *RemoteClusterReference) SetId(id string) {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.Id_ = id
}

func (ref *RemoteClusterReference) Name() string {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.Name_
}

func (ref *RemoteClusterReference) SetName(name string) {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.Name_ = name
}

func (ref *RemoteClusterReference) Uuid() string {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.Uuid_
}

func (ref *RemoteClusterReference) SetUuid(uuid string) {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.Uuid_ = uuid
}

func (ref *RemoteClusterReference) HostName() string {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.HostName_
}

func (ref *RemoteClusterReference) SetHostName(name string) {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.HostName_ = name
}

func (ref *RemoteClusterReference) ActiveHostName() string {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.ActiveHostName_
}

func (ref *RemoteClusterReference) SetActiveHostName(activeHostName string) {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.ActiveHostName_ = activeHostName
}

func (ref *RemoteClusterReference) ActiveHttpsHostName() string {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.ActiveHttpsHostName_
}

func (ref *RemoteClusterReference) SetActiveHttpsHostName(activeHttpsHostName string) {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.ActiveHttpsHostName_ = activeHttpsHostName
}

func (ref *RemoteClusterReference) HttpsHostName() string {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.HttpsHostName_
}

func (ref *RemoteClusterReference) SetHttpsHostName(name string) {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.HttpsHostName_ = name
}

func (ref *RemoteClusterReference) UserName() string {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.UserName_
}

func (ref *RemoteClusterReference) SetUserName(userName string) {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.UserName_ = userName
}

func (ref *RemoteClusterReference) Password() string {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.Password_
}

func (ref *RemoteClusterReference) DemandEncryption() bool {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.DemandEncryption_
}

func (ref *RemoteClusterReference) EncryptionType() string {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.EncryptionType_
}

func (ref *RemoteClusterReference) SetEncryptionType(encryptionType string) {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.EncryptionType_ = encryptionType
}

func (ref *RemoteClusterReference) SANInCertificate() bool {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.SANInCertificate_
}

func (ref *RemoteClusterReference) SetSANInCertificate(sanInCertificate bool) {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.SANInCertificate_ = sanInCertificate
}

func (ref *RemoteClusterReference) HttpAuthMech() base.HttpAuthMech {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.HttpAuthMech_
}

func (ref *RemoteClusterReference) SetHttpAuthMech(authMech base.HttpAuthMech) {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.HttpAuthMech_ = authMech
}

func (ref *RemoteClusterReference) Certificate() []byte {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.Certificate_
}

func (ref *RemoteClusterReference) ClientCertificate() []byte {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.ClientCertificate_
}

func (ref *RemoteClusterReference) ClientKey() []byte {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.ClientKey_
}

func (ref *RemoteClusterReference) Revision() interface{} {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.revision
}

func (ref *RemoteClusterReference) SetRevision(rev interface{}) {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.revision = rev
}

func (ref *RemoteClusterReference) Marshal() ([]byte, error) {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return json.Marshal(ref)
}

func (ref *RemoteClusterReference) Unmarshal(value []byte) error {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	return json.Unmarshal(value, ref)
}
