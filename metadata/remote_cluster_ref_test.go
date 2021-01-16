// +build !pcre

package metadata

import (
	"fmt"
	"github.com/couchbase/goxdcr/base"
	base2 "github.com/couchbase/goxdcr/base/helpers"
	baseH "github.com/couchbase/goxdcr/base/helpers/mocks"
	"github.com/couchbase/goxdcr/log"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
)

var dnsSrvHostname string = "xdcr.couchbase.target.local"
var invalidHostName = fmt.Sprintf("%v:%v", dnsSrvHostname, 12345)

const localhostIP = "192.168.0.1"
const localhostIP2 = "192.168.0.2"

func setupDNSMocks() *baseH.DnsSrvHelperIface {
	helper := &baseH.DnsSrvHelperIface{}
	oneEntry := &net.SRV{
		// NOTE - SRV entries for "name" will end with a .
		// See actual output in DnsSrvHelper in base package
		Target: localhostIP,
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
		Target: localhostIP,
		Port:   9001,
	}
	secondEntry := &net.SRV{
		Target: localhostIP,
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
		Target: localhostIP,
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

	ref.PopulateDnsSrvIfNeeded()

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
	assert.Equal(fmt.Sprintf("%v:%v", localhostIP, base.DefaultAdminPort), hostnameList[0])

	// test GetTargetConnectionString() to return the correct one
	for _, entry := range ref.srvEntries {
		assert.NotEqual(uint16(8091), entry.srv.Port)
		targetConnStr, err := entry.GetTargetConnectionString(HostNameSRV, false)
		assert.Nil(err)
		portNo, err := base.GetPortNumber(targetConnStr)
		assert.Nil(err)
		assert.Equal(base.DefaultAdminPort, portNo)
		targetHostname := base.GetHostName(targetConnStr)
		assert.Equal(localhostIP, targetHostname)
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

	ref.PopulateDnsSrvIfNeeded()

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

	ref.PopulateDnsSrvIfNeeded()

	// Test SameAs
	assert.True(ref.IsDnsSRV())
	assert.NotEqual(0, len(ref.srvEntries))

	hostnameList := ref.GetSRVHostNames()
	assert.Equal(fmt.Sprintf("%v:%v", localhostIP, base.DefaultAdminPortSSL), hostnameList[0])
	assert.Equal(HostNameSecureSRV, ref.hostnameSRVType)

}

func setupDNSBothMocks() *baseH.DnsSrvHelperIface {
	helper := &baseH.DnsSrvHelperIface{}
	oneEntry := &net.SRV{
		// NOTE - SRV entries for "name" will end with a .
		// See actual output in DnsSrvHelper in base package
		Target: localhostIP,
		Port:   19001,
	}
	var entryList []*net.SRV
	entryList = append(entryList, oneEntry)

	oneEntry.Target = localhostIP2
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
	nonSecureRef.PopulateDnsSrvIfNeeded()

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
	fullEncryptionRef.PopulateDnsSrvIfNeeded()

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
