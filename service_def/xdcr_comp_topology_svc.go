// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_def

import (
	"github.com/couchbase/goxdcr/v8/base"
)

const UnknownPoolStr = "statusCode=404"

// XDCRCompTopologySvc abstracts the service interface that has the knowledge
// of xdcr component topology - xdcr components in a cluster running on which nodes;
// what are the port numbers for the admin port of xdcr component; which kv node that
// a xdcr component is responsible for.
//
// This interface is trying to be deployment agnostic. It doesn't assume if xdcr component
// and kv node are coexist on the same physical host or not.
//
// An implementation of this interface is likely to be deployment dependent. Base on the
// deployment mode, xdcr solution can choose to use the proper implementation class
type XDCRCompTopologySvc interface {
	//the host name that this xdcr comp is running on
	MyHost() (string, error)

	// the address, ip:couchbaseAdminport, of the host that this xdcr comp is running on
	// note that ip is the public ip of the host, which may not be the same as what MyHost() returns.
	// this is used as local cluster address by audit
	// If cluster encryption mode is strict, then the host address will contain a TLS port
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
	MyNodeVersion() (string, error)

	// Cluster Compatibility number for local cluster
	MyClusterCompatibility() (int, error)

	//is the cluster XDCR is serving of enterprise edition
	IsMyClusterEnterprise() (bool, error)

	//is the cluster XDCR is serving ipv6 enabled
	IsMyClusterIpv6() bool
	IsIpv4Blocked() bool
	IsIpv6Blocked() bool

	IsMyClusterDeveloperPreview() bool
	IsMyClusterEncryptionLevelStrict() bool

	// get local host name
	GetLocalHostName() string

	//return a map with the key to be the host name that a xdcr comp is running on
	// and the value to be an array of kv node address which the xdcr component would
	//be responsible for
	XDCRCompToKVNodeMap() (map[string][]string, error)

	// Returns a list of peer admin addresses
	// If security requires strict mode, the address returned will contain a TLS port
	PeerNodesAdminAddrs() ([]string, error)

	// implements base.ClusterConnectionInfoProvider
	MyConnectionStr() (string, error)
	MyCredentials() (string, string, base.HttpAuthMech, []byte, bool, []byte, []byte, error)
	IsKVNode() (bool, error)
}
