// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package service_def

import (
	"github.com/couchbase/goxdcr/base"
)

//XDCRCompTopologySvc abstracts the service interface that has the knowledge
//of xdcr component topology - xdcr components in a cluster running on which nodes;
//what are the port numbers for the admin port of xdcr component; which kv node that
//a xdcr component is responsible for.
//
//This interface is trying to be deployment agnostic. It doesn't assume if xdcr component
//and kv node are coexist on the same physical host or not.
//
//An implementation of this interface is likely to be deployment dependent. Base on the
//deployment mode, xdcr solution can choose to use the proper implementation class
type XDCRCompTopologySvc interface {
	//the host name that this xdcr comp is running on
	MyHost() (string, error)

	// the address, ip:couchbaseAdminport, of the host that this xdcr comp is running on
	// note that ip is the public ip of the host, which may not be the same as what MyHost() returns.
	// this is used as local cluster address by audit
	MyHostAddr() (string, error)

	//the address, ip:memcachedport, of the kv that this xdcr comp is running on
	MyMemcachedAddr() (string, error)

	//the admin port number of this xdcr comp
	MyAdminPort() (uint16, error)

	//the list of kv nodes (hostname:port) that this xdcr comp is responsible for
	MyKVNodes() ([]string, error)

	//number of kv nodes in the cluster that this xdcr comp is running in
	NumberOfKVNodes() (int, error)

	// the uuid of the cluster that this xdcr comp is running on
	MyClusterUuid() (string, error)

	// the version of the cluster that this xdcr comp is running on
	MyClusterVersion() (string, error)

	//is the cluster XDCR is serving of enterprise edition
	IsMyClusterEnterprise() (bool, error)

	//is the cluster XDCR is serving ipv6 enabled
	IsMyClusterIpv6() bool

	IsMyClusterDeveloperPreview() bool
	// get local host name
	GetLocalHostName() string

	//return a map with the key to be the host name that a xdcr comp is running on
	// and the value to be an array of kv node address which the xdcr component would
	//be responsible for
	XDCRCompToKVNodeMap() (map[string][]string, error)

	// implements base.ClusterConnectionInfoProvider
	MyConnectionStr() (string, error)
	MyCredentials() (string, string, base.HttpAuthMech, []byte, bool, []byte, []byte, error)
	IsKVNode() (bool, error)
}
