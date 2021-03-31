/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software
will be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
)

// HOW TO SET UP LOCAL ENVIRONMENT FOR UNIT TEST
// 1. Set up local DNS server using dnsmasq as described here:
//    https://medium.com/@zhimin.wen/setup-local-dns-server-on-macbook-82ad22e76f2a
// 2. For me, I set up the conf file  /usr/local/etc/dnsmasq.d/xdcr.conf - with the 2 entries:
//    address=/_couchbase._tcp.xdcr.local/192.168.0.1
//    srv-host=_couchbase._tcp.xdcr.local,192.168.0.1,9001
// 3. For MacOS - the /etc/resolver/ doesn't really work. I had to actually go to the Network Preference, Wifi,
//    Advanced, and manually add a DNS server of 127.0.0.1 entry
// 4. Validate it by running nslookup as well as dig:
//    neil.huang@NeilsMacbookPro:~$ dig master _couchbase._tcp.xdcr.local @localhost +short
//    192.168.0.1
//
//    neil.huang@NeilsMacbookPro:~$ nslookup -type=srv _couchbase._tcp.xdcr.local
//    Server:		127.0.0.1
//    Address:	127.0.0.1#53
//
//    _couchbase._tcp.xdcr.local	service = 0 0 9001 192.168.0.1.
var dnsSrvHostname string = "xdcr.local"

// NOTE - SRV entries for "name" will end with a .
// See above actual output
func TestDNSHelper(t *testing.T) {
	// Test to see if above set up has been done correctly
	_, addrs, err := net.LookupSRV(CouchbaseService, TCPProto, dnsSrvHostname)
	if err != nil {
		fmt.Printf("Local DNS look up failed for %v - %v, skipping DNSSRV unit test\n", dnsSrvHostname, err)
		return
	}
	for _, addr := range addrs {
		fmt.Printf("Address: %v port: %v, priority: %v weight: %v\n", addr.Target, addr.Port, addr.Priority, addr.Weight)
	}

	assert := assert.New(t)
	assert.Nil(err)
	assert.NotEqual(0, len(addrs))
	assert.Equal("192.168.0.1.", addrs[0].Target)
	assert.Equal(uint16(9001), addrs[0].Port)

	helper := &DnsSrvHelper{}
	entries, secureMode, err := helper.DnsSrvLookup(dnsSrvHostname)
	assert.Nil(err)
	assert.Equal(SrvRecordsNonSecure, secureMode)
	assert.Equal(1, len(entries))
	assert.Equal("192.168.0.1.", entries[0].Target)
	assert.Equal(uint16(9001), addrs[0].Port)
}
