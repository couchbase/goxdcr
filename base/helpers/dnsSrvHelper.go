// Copyright (c) 2020 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

// simple utility functions with minimum dependencies on other goxdcr packages
package base

import "net"

type DnsSrvHelperIface interface {
	DnsSrvLookup(hostname string) (srvEntries []*net.SRV, err error)
}

type DnsSrvHelper struct{}

// NOTE - SRV entries for "name" will end with a .
// i.e. 192.168.0.1.
func (dsh *DnsSrvHelper) DnsSrvLookup(hostname string) (srvEntries []*net.SRV, err error) {
	_, addrs, err := net.LookupSRV("", "", hostname)
	srvEntries = addrs
	return
}
