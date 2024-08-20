//go:build !pcre
// +build !pcre

/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package metadata

import (
	"fmt"
	"net"
	"testing"

	"github.com/couchbase/goxdcr/v8/base"
	base2 "github.com/couchbase/goxdcr/v8/base/helpers"
	baseH "github.com/couchbase/goxdcr/v8/base/helpers/mocks"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/stretchr/testify/assert"
)

var dnsSrvHostname string = "xdcr.couchbase.target.local"
var invalidHostName = fmt.Sprintf("%v:%v", dnsSrvHostname, 12345)

const localhostARecord = "localhostFqdn"
const localhostArecord2 = "localhostFqdn2"

func setupDNSMocks() *baseH.DnsSrvHelperIface {
	helper := &baseH.DnsSrvHelperIface{}
	oneEntry := &net.SRV{
		// NOTE - SRV entries for "name" will end with a .
		// See actual output in DnsSrvHelper in base package
		Target: localhostARecord,
		Port:   9001,
	}
	var entryList []*net.SRV
	var emptyList []*net.SRV
	entryList = append(entryList, oneEntry)
	helper.On("DnsSrvLookup", dnsSrvHostname).Return(entryList, base2.SrvRecordsNonSecure, nil)
	helper.On("DnsSrvLookup", invalidHostName).Return(emptyList, base2.SrvRecordsInvalid, fmt.Errorf("Invalid"))
	return helper
}

func setupDNS2Nodes() *baseH.DnsSrvHelperIface {
	helper := &baseH.DnsSrvHelperIface{}
	oneEntry := &net.SRV{
		// NOTE - SRV entries for "name" will end with a .
		// See actual output in DnsSrvHelper in base package
		Target: localhostARecord,
		Port:   9001,
	}
	secondEntry := &net.SRV{
		Target: localhostARecord,
		Port:   9002,
	}
	var entryList []*net.SRV
	entryList = append(entryList, oneEntry)
	entryList = append(entryList, secondEntry)
	helper.On("DnsSrvLookup", dnsSrvHostname).Return(entryList, base2.SrvRecordsNonSecure, nil)
	return helper
}

func setupEmptyDNS() *baseH.DnsSrvHelperIface {
	helper := &baseH.DnsSrvHelperIface{}
	var emptyList []*net.SRV
	helper.On("DnsSrvLookup", dnsSrvHostname).Return(emptyList, base2.SrvRecordsNonSecure, nil)
	return helper
}

func setupErrDNS() *baseH.DnsSrvHelperIface {
	helper := &baseH.DnsSrvHelperIface{}
	var emptyList []*net.SRV
	helper.On("DnsSrvLookup", dnsSrvHostname).Return(emptyList, base2.SrvRecordsInvalid, fmt.Errorf("Dummy"))
	return helper
}

func setupDNSSecureMocks() *baseH.DnsSrvHelperIface {
	helper := &baseH.DnsSrvHelperIface{}
	oneEntry := &net.SRV{
		// NOTE - SRV entries for "name" will end with a .
		// See actual output in DnsSrvHelper in base package
		Target: localhostARecord,
		Port:   19001,
	}
	var entryList []*net.SRV
	var emptyList []*net.SRV
	entryList = append(entryList, oneEntry)
	helper.On("DnsSrvLookup", dnsSrvHostname).Return(entryList, base2.SrvRecordsSecure, nil)
	helper.On("DnsSrvLookup", invalidHostName).Return(emptyList, base2.SrvRecordsInvalid, fmt.Errorf("Invalid"))
	return helper
}

func TestNewRefWithDNSSrv(t *testing.T) {
	fmt.Println("============== Test case start: TestNewRefWithDNSSrv =================")

	assert := assert.New(t)

	helper := setupDNSMocks()

	ref, _ := NewRemoteClusterReference("testsUuid", "testName", dnsSrvHostname, "testUserName", "testPassword",
		"", false, "", nil, nil, nil, helper)

	ref.PopulateDnsSrvIfNeeded(nil)

	// Test SameAs
	ref0 := ref.Clone()
	ref1 := ref.Clone()

	dummySRV := &net.SRV{Target: "testTarget"}
	dummyEntry := SrvEntryType{dummySRV}
	ref0.srvEntries = append(ref0.srvEntries, dummyEntry)
	assert.False(ref0.IsSame(ref1))
	assert.False(ref0.srvEntries.SameAs(ref1.srvEntries))
	ref1.srvEntries = append(ref1.srvEntries, dummyEntry)
	copy(ref1.srvEntries[1:], ref.srvEntries[0:])
	ref1.srvEntries[0] = dummyEntry
	assert.True(ref0.IsSame(ref1))
	assert.True(ref0.srvEntries.SameAs(ref1.srvEntries))

	assert.True(ref.IsDnsSRV())
	assert.NotEqual(0, len(ref.srvEntries))

	hostnameList := ref.GetSRVHostNames()
	//assert.Equal("192.168.0.1:9001", hostnameList[0])
	// Ensure that 8091 is returned because Couchbase DNS SRV record ports are KV ports (MB-41083)
	assert.Equal(fmt.Sprintf("%v:%v", localhostARecord, base.DefaultAdminPort), hostnameList[0])

	// test GetTargetConnectionString() to return the correct one
	for _, entry := range ref.srvEntries {
		assert.NotEqual(uint16(8091), entry.srv.Port)
		targetConnStr, err := entry.GetTargetConnectionString(HostNameSRV, false)
		assert.Nil(err)
		portNo, err := base.GetPortNumber(targetConnStr)
		assert.Nil(err)
		assert.Equal(base.DefaultAdminPort, portNo)
		targetHostname := base.GetHostName(targetConnStr)
		assert.Equal(localhostARecord, targetHostname)
		targetConnStr, err = entry.GetTargetConnectionString(HostNameSecureSRV, false)
		assert.Nil(err)
		portNo, err = base.GetPortNumber(targetConnStr)
		assert.Nil(err)
		assert.Equal(base.DefaultAdminPortSSL, portNo)
	}

	added, removed, totalCnt, err := ref.RefreshSRVEntries()
	assert.Equal(0, len(removed))
	assert.Equal(0, len(added))
	assert.Equal(1, totalCnt)
	assert.Nil(err)

	// Pretend that SRV now points to something else totally wrong
	ref.UnitTestSetSRVHelper(setupDNS2Nodes())
	added, removed, totalCnt, err = ref.RefreshSRVEntries()
	assert.Equal(0, len(removed))
	assert.Equal(1, len(added))
	assert.Equal(2, totalCnt)
	assert.Nil(err)

	refCopy := ref.Clone()

	// Now pretend all 2 nodes are gone
	assert.Equal(2, len(refCopy.srvEntries))
	refCopy.UnitTestSetSRVHelper(setupEmptyDNS())
	added, removed, totalCnt, err = refCopy.RefreshSRVEntries()
	assert.Equal(2, len(removed))
	assert.Equal(0, len(added))
	assert.Equal(0, totalCnt)
	assert.Nil(err)

	// If SRV look up returns error, entries should be gone
	refCopy.UnitTestSetSRVHelper(setupErrDNS())
	assert.True(refCopy.IsDnsSRV())
	added, removed, totalCnt, err = refCopy.RefreshSRVEntries()
	assert.Equal(0, totalCnt)
	assert.NotNil(err)
	assert.False(refCopy.IsDnsSRV())

	goodUuidFunc := func(hostAddr, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte, logger *log.CommonLogger) (string, error) {
		return "testsUuid", nil
	}
	err = ref.CheckSRVValidityByUUID(goodUuidFunc, nil)
	assert.Nil(err)

	badUuidFunc := func(hostAddr, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte, logger *log.CommonLogger) (string, error) {
		return "badUuid", nil
	}
	err = ref.CheckSRVValidityByUUID(badUuidFunc, nil)
	assert.Equal(ErrorNoBootableSRVEntryFound, err)

	fmt.Println("============== Test case end: TestDNSSrv =================")
}

func TestNewRefWithDNSSrvInvalidPort(t *testing.T) {
	fmt.Println("============== Test case start: TestNewRefWithDNSSrvInvalidPort =================")

	assert := assert.New(t)

	helper := setupDNSMocks()

	ref, _ := NewRemoteClusterReference("testsUuid", "testName", invalidHostName, "testUserName", "testPassword",
		"", false, "", nil, nil, nil, helper)

	ref.PopulateDnsSrvIfNeeded(nil)

	assert.False(ref.IsDnsSRV())
	assert.Equal(0, len(ref.srvEntries))
	fmt.Println("============== Test case end: TestDNSSrvInvalidPort =================")
}

func TestNewRefWithDNSSrvSecure(t *testing.T) {
	fmt.Println("============== Test case start: TestNewRefWithDNSSrvSecure =================")
	defer fmt.Println("============== Test case done: TestNewRefWithDNSSrvSecure =================")

	assert := assert.New(t)

	helper := setupDNSSecureMocks()

	ref, _ := NewRemoteClusterReference("testsUuid", "testName", dnsSrvHostname, "testUserName", "testPassword",
		"", false, "", nil, nil, nil, helper)

	ref.PopulateDnsSrvIfNeeded(nil)

	// Test SameAs
	assert.True(ref.IsDnsSRV())
	assert.NotEqual(0, len(ref.srvEntries))

	hostnameList := ref.GetSRVHostNames()
	assert.Equal(fmt.Sprintf("%v:%v", localhostARecord, base.DefaultAdminPortSSL), hostnameList[0])
	assert.Equal(HostNameSecureSRV, ref.hostnameSRVType)

}

func setupDNSBothMocks() *baseH.DnsSrvHelperIface {
	helper := &baseH.DnsSrvHelperIface{}
	oneEntry := &net.SRV{
		// NOTE - SRV entries for "name" will end with a .
		// See actual output in DnsSrvHelper in base package
		Target: localhostARecord,
		Port:   19001,
	}
	var entryList []*net.SRV
	entryList = append(entryList, oneEntry)

	oneEntry.Target = localhostArecord2
	entryList = append(entryList, oneEntry)

	helper.On("DnsSrvLookup", dnsSrvHostname).Return(entryList, base2.SrvRecordsBoth, nil)
	return helper
}

// When DNS SRV records contain both secure and non secure, then the behavior should depend upon the reference's
// half or full encryption mode
func TestDNSSRVBoth(t *testing.T) {
	fmt.Println("============== Test case start: TestNewRefWithDNSSrvSecure =================")
	defer fmt.Println("============== Test case done: TestNewRefWithDNSSrvSecure =================")

	assert := assert.New(t)

	helper := setupDNSBothMocks()

	// Non-Full encryption reference
	nonSecureRef, _ := NewRemoteClusterReference("testsUuid", "testName", dnsSrvHostname, "testUserName", "testPassword",
		"", false, "", nil, nil, nil, helper)
	nonSecureRef.PopulateDnsSrvIfNeeded(nil)

	assert.Equal(HostNameBothSRV, nonSecureRef.hostnameSRVType)
	assert.False(nonSecureRef.IsFullEncryption())

	// Non secure reference should give back a non-secure port
	for _, entry := range nonSecureRef.srvEntries {
		connStr, err := entry.GetTargetConnectionString(nonSecureRef.hostnameSRVType, nonSecureRef.IsFullEncryption())
		assert.Nil(err)
		portNo, err := base.GetPortNumber(connStr)
		assert.Nil(err)
		assert.Equal(base.DefaultAdminPort, portNo)
	}

	// Full encryption reference
	fullEncryptionRef, _ := NewRemoteClusterReference("testsUuid", "testName", dnsSrvHostname, "testUserName", "testPassword",
		"", true, EncryptionType_Full, nil, nil, nil, helper)
	fullEncryptionRef.PopulateDnsSrvIfNeeded(nil)

	assert.Equal(HostNameBothSRV, fullEncryptionRef.hostnameSRVType)
	assert.True(fullEncryptionRef.IsFullEncryption())

	// Secure reference should give back a secure port
	for _, entry := range fullEncryptionRef.srvEntries {
		connStr, err := entry.GetTargetConnectionString(fullEncryptionRef.hostnameSRVType, fullEncryptionRef.IsFullEncryption())
		assert.Nil(err)
		portNo, err := base.GetPortNumber(connStr)
		assert.Nil(err)
		assert.Equal(base.DefaultAdminPortSSL, portNo)
	}
}

func TestEnforceIP(t *testing.T) {
	fmt.Println("============== Test case start: TestEnforceIP =================")
	defer fmt.Println("============== Test case done: TestEnforceIP =================")

	assert := assert.New(t)

	hostname := "localhost"
	// Non-Full encryption reference
	nonSecureRef, _ := NewRemoteClusterReference("testsUuid", "testName", hostname, "testUserName", "testPassword",
		"", false, "", nil, nil, nil, nil)

	// Set IPv6 to be blocked
	base.NetTCP = base.TCP4
	assert.True(base.IsIpV6Blocked())
	// Set it back after test
	defer func() {
		base.NetTCP = base.TCP
	}()

	connStr, connErr := nonSecureRef.MyConnectionStr()
	assert.Nil(connErr)
	assert.Equal("127.0.0.1", connStr)

	// Set IPv4 to be blocked
	base.NetTCP = base.TCP6
	assert.True(base.IsIpV4Blocked())

	connStr, connErr = nonSecureRef.MyConnectionStr()
	assert.Nil(connErr)
	assert.Equal("[::1]", connStr)

	// Set none blocked - should return original "hostname"
	base.NetTCP = base.TCP
	assert.False(base.IsIpV4Blocked())
	assert.False(base.IsIpV6Blocked())
	connStr, connErr = nonSecureRef.MyConnectionStr()
	assert.Nil(connErr)
	assert.Equal(hostname, connStr)
}

func TestEnforceIP_TLS(t *testing.T) {
	fmt.Println("============== Test case start: TestEnforceIP_TLS =================")
	defer fmt.Println("============== Test case done: TestEnforceIP_TLS =================")

	assert := assert.New(t)

	// Non-Full encryption reference - use mock DNS SRV to populate the lookup to a strict IPV4
	helper := setupDNSBothMocks()
	fullEncryptionRef, _ := NewRemoteClusterReference("testsUuid", "testName", dnsSrvHostname, "testUserName", "testPassword",
		"", true, EncryptionType_Full, nil, nil, nil, helper)
	fullEncryptionRef.PopulateDnsSrvIfNeeded(nil)

	// Mock the NetParseIP function
	base.NetParseIP = func(input string) net.IP {
		return net.ParseIP("127.0.0.1")
	}
	defer func() {
		base.NetParseIP = func(input string) net.IP {
			return net.ParseIP(input)
		}
	}()

	// Set IPv4 to be blocked fake SRV lookup translates to IPv4
	base.NetTCP = base.TCP6
	assert.True(base.IsIpV4Blocked())
	// Set it back after test
	defer func() {
		base.NetTCP = base.TCP
	}()
	connStr, connErr := fullEncryptionRef.MyConnectionStr()
	assert.NotNil(connErr)

	// Set IPv6 to be blocked... SRV IP lookup is IPv4 ... so should return FQDN after SRV lookup
	base.NetTCP = base.TCP4
	assert.True(base.IsIpV6Blocked())
	connStr, connErr = fullEncryptionRef.MyConnectionStr()
	assert.Nil(connErr)
	assert.True(connStr == base.GetHostAddr(localhostARecord, 18091) || connStr == base.GetHostAddr(localhostArecord2, 18091))

	// Set none blocked - should return FQDN after SRV lookup
	base.NetTCP = base.TCP
	assert.False(base.IsIpV4Blocked())
	assert.False(base.IsIpV6Blocked())
	connStr, connErr = fullEncryptionRef.MyConnectionStr()
	assert.Nil(connErr)
	assert.True(connStr == base.GetHostAddr(localhostARecord, 18091) || connStr == base.GetHostAddr(localhostArecord2, 18091))
}

func TestCapellaNoTLS(t *testing.T) {
	fmt.Println("============== Test case start: TestCapellaNoTLS =================")
	defer fmt.Println("============== Test case done: TestCapellaNoTLS =================")
	assert := assert.New(t)

	// Dummy cloud name
	hostname := "abc.xyz.cloud.couchbase.com"
	// Non-Full encryption reference
	nonSecureRef, _ := NewRemoteClusterReference("testsUuid", "testName", hostname, "testUserName", "testPassword",
		"", false, "", nil, nil, nil, nil)

	assert.Equal(ErrorCapellaNeedsTLS, nonSecureRef.ValidateCertificates())
}

func TestCapellaTLS(t *testing.T) {
	fmt.Println("============== Test case start: TestCapellaTLS =================")
	defer fmt.Println("============== Test case done: TestCapellaTLS =================")
	assert := assert.New(t)

	// Dummy cloud name
	hostname := "abc.xyz.cloud.couchbase.com"
	// Full encryption reference
	secureRef, _ := NewRemoteClusterReference("testsUuid", "testName", hostname, "testUserName", "testPassword",
		"", true, EncryptionType_Full, nil, nil, nil, nil)

	assert.Equal([]byte(CapellaCert), secureRef.Certificates())
	assert.Nil(secureRef.ValidateCertificates())
	_, _, _, validateCert, _, _, _, _ := secureRef.MyCredentials()
	assert.NotNil(validateCert)
}
