// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package service_def

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

	//the admin port number of this xdcr comp
	MyAdminPort() (uint16, error)

	//the list of kv nodes (hostname:port) that this xdcr comp is responsible for
	MyKVNodes() ([]string, error)
	
	//is the cluster XDCR is serving of enterprise edition
	IsMyClusterEnterprise() (bool, error)

	//return a map with the key to be the host name that a xdcr comp is
	//running on and the value to be the admin port number on that host
	XDCRTopology() (map[string]uint16, error)

	//return a map with the key to be the host name that a xdcr comp is running on
	// and the value to be an array of kv node address which the xdcr component would
	//be responsible for
	XDCRCompToKVNodeMap() (map[string][]string, error)
	
	// implements base.ClusterConnectionInfoProvider
	MyConnectionStr() string
	MyUsername()  string
	MyPassword()  string
}
