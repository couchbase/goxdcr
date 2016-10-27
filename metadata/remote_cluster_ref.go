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
	"github.com/couchbase/goxdcr/simple_utils"
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
	HttpsHostName    string `json:"httpsHostName"`
	SANInCertificate bool   `json:"SANInCertificate"`

	// these are hostname actually used to connect to target
	// they are rotated among nodes in target cluster to achieve load balancing on target
	// they are used to update HostName/HttpsHostName when HostName has been removed from the target cluster
	// they are not exposed to users, e.g., through UI or rest api
	ActiveHostName      string `json:"activeHostName"`
	ActiveHttpsHostName string `json:"activeHttpsHostName"`

	// revision number to be used by metadata service. not included in json
	Revision interface{}
}

func NewRemoteClusterReference(uuid, name, hostName, userName, password string,
	demandEncryption bool, encryptionType string, certificate []byte) (*RemoteClusterReference, error) {
	refId, err := RemoteClusterRefId()
	if err != nil {
		return nil, err
	}

	return &RemoteClusterReference{Id: refId,
		Uuid:             uuid,
		Name:             name,
		HostName:         hostName,
		UserName:         userName,
		Password:         password,
		DemandEncryption: demandEncryption,
		EncryptionType:   encryptionType,
		Certificate:      certificate,
	}, nil
}

func RemoteClusterRefId() (string, error) {
	refUuid, err := simple_utils.GenerateRandomId(SizeOfRemoteClusterRefId, MaxRetryForIdGeneration)
	if err != nil {
		return "", err
	}
	parts := []string{RemoteClusterKeyPrefix, refUuid}
	return strings.Join(parts, base.KeyPartsDelimiter), nil
}

// implements base.ClusterConnectionInfoProvider
func (ref *RemoteClusterReference) MyConnectionStr() (string, error) {
	if ref.IsEncryptionEnabled() {
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

func (ref *RemoteClusterReference) MyCredentials() (string, string, []byte, bool, error) {
	return ref.UserName, ref.Password, ref.Certificate, ref.SANInCertificate, nil
}

// convert to a map for output
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
	if ref.IsEncryptionEnabled() {
		outputMap[base.RemoteClusterDemandEncryption] = ref.DemandEncryption
		outputMap[base.RemoteClusterEncryptionType] = ref.EncryptionType
		outputMap[base.RemoteClusterCertificate] = string(ref.Certificate)
	}
	return outputMap
}

// checks if the passed in ref is the same as the current ref
func (ref *RemoteClusterReference) SameRef(ref2 *RemoteClusterReference) bool {
	if ref == nil {
		return ref2 == nil
	}
	if ref2 == nil {
		return false
	}
	return ref.Id == ref2.Id && ref.Uuid == ref2.Uuid && ref.Name == ref2.Name &&
		ref.HostName == ref2.HostName && ref.UserName == ref2.UserName &&
		ref.Password == ref2.Password && reflect.DeepEqual(ref.Revision, ref2.Revision) &&
		ref.DemandEncryption == ref2.DemandEncryption && ref.EncryptionType == ref2.EncryptionType &&
		bytes.Equal(ref.Certificate, ref2.Certificate)
}

func (ref *RemoteClusterReference) String() string {
	if ref == nil {
		return "nil"
	}
	return fmt.Sprintf("id:%v; uuid:%v; name:%v; hostName:%v; userName:%v; password:xxxx; demandEncryption:%v: encryptionType:%v; certificate:%v; revision:%v", ref.Id, ref.Uuid, ref.Name, ref.HostName, ref.UserName, ref.DemandEncryption, ref.EncryptionType, ref.Certificate, ref.Revision)
}

func (ref *RemoteClusterReference) Clone() *RemoteClusterReference {
	if ref == nil {
		return nil
	}
	return &RemoteClusterReference{Id: ref.Id,
		Uuid:                ref.Uuid,
		Name:                ref.Name,
		HostName:            ref.HostName,
		UserName:            ref.UserName,
		Password:            ref.Password,
		DemandEncryption:    ref.DemandEncryption,
		Certificate:         ref.Certificate,
		HttpsHostName:       ref.HttpsHostName,
		ActiveHostName:      ref.ActiveHostName,
		ActiveHttpsHostName: ref.ActiveHttpsHostName,
		EncryptionType:      ref.EncryptionType,
		SANInCertificate:    ref.SANInCertificate,
	}
}

func (ref *RemoteClusterReference) IsEncryptionEnabled() bool {
	return ref.DemandEncryption
}

func (ref *RemoteClusterReference) IsFullEncryption() bool {
	// ref.EncryptionType may be empty for unupgraded remote cluster refs. treat it as "full" in this case
	return ref.DemandEncryption && (len(ref.EncryptionType) == 0 || ref.EncryptionType == EncryptionType_Full)
}
