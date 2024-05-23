// Copyright 2020-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

// simple utility functions with minimum dependencies on other goxdcr packages
package base

import (
	"fmt"
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
		err = fmt.Errorf("secureErr:%s, nonSecureErr:%s", secureErr.Error(), nonSecureErr.Error())
	}
	return
}
