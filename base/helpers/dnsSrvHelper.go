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

import (
	"net"
	"sync"
)

type DnsSrvHelperIface interface {
	DnsSrvLookup(hostname string) (srvEntries []*net.SRV, secureType SrvRecordsType, err error)
}

type DnsSrvHelper struct{}

const CouchbaseService = "couchbase"
const CouchbaseSecureService = "couchbases"
const TCPProto = "tcp"

type SrvRecordsType int

const (
	SrvRecordsInvalid   SrvRecordsType = iota
	SrvRecordsNonSecure SrvRecordsType = iota
	SrvRecordsSecure    SrvRecordsType = iota
	SrvRecordsBoth      SrvRecordsType = iota
)

// NOTE - SRV entries for "name" will end with a .
// i.e. 192.168.0.1.
func (dsh *DnsSrvHelper) DnsSrvLookup(hostname string) (srvEntries []*net.SRV, secureType SrvRecordsType, err error) {
	// Perform both lookups in parallel
	var waitGroup sync.WaitGroup
	var nonSecureAddrs []*net.SRV
	var nonSecureErr error
	var secureAddrs []*net.SRV
	var secureErr error

	waitGroup.Add(2)
	go func() {
		defer waitGroup.Done()
		_, nonSecureAddrs, nonSecureErr = net.LookupSRV(CouchbaseService, TCPProto, hostname)
	}()

	go func() {
		defer waitGroup.Done()
		_, secureAddrs, secureErr = net.LookupSRV(CouchbaseSecureService, TCPProto, hostname)
	}()
	waitGroup.Wait()

	if nonSecureErr == nil {
		srvEntries = append(srvEntries, nonSecureAddrs...)
		secureType = SrvRecordsNonSecure
	}

	if secureErr == nil {
		srvEntries = append(srvEntries, secureAddrs...)
		if secureType == SrvRecordsNonSecure {
			secureType = SrvRecordsBoth
		} else {
			secureType = SrvRecordsSecure
		}
	}

	// As long as one worked, returned error should be nil
	if secureErr != nil && nonSecureErr != nil {
		err = nonSecureErr
	}
	return
}
