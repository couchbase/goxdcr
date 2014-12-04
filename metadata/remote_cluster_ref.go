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
	"strings"
	"github.com/couchbase/goxdcr/base"
)

const (
	// ids of remote cluster refs are used as keys in gometa service. 
	// the following prefix distinguishes the remote cluster refs from other entries
	// and reduces the chance of naming conflicts
	RemoteClusterKeyPrefix = "remoteCluster"
)

/************************************
/* struct RemoteClusterReference
*************************************/
type RemoteClusterReference struct {
	Id  string `json:"id"`
	Uuid string `json:"uuid"`
	Name string `json:"name"`
    HostName string `json:"hostName"`
	UserName string `json:"userName"`
    Password string `json:"password"`
    
    // ignore the following fields during json serialization
    DemandEncryption  bool `json:"demandEncryption"`
    Certificate  []byte  `json:"certificate"`
}

func NewRemoteClusterReference(uuid, name, hostName, userName, password string, 
	demandEncryption  bool, certificate  []byte) *RemoteClusterReference {
	return &RemoteClusterReference{Id:  RemoteClusterRefId(name),
		Uuid:  uuid,
		Name:  name,
		HostName:  hostName,
		UserName:  userName,
		Password:  password,
		DemandEncryption: demandEncryption,
		Certificate:  certificate,
	}
}

func RemoteClusterRefId(name string) string {
	// name alone should guarantee uniqueness of the remote cluster reference
	parts := []string{RemoteClusterKeyPrefix, name}
	return strings.Join(parts, base.KeyPartsDelimiter)
}
