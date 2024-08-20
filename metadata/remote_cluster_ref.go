// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package metadata

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	baseH "github.com/couchbase/goxdcr/v8/base/helpers"
	"github.com/couchbase/goxdcr/v8/log"
)

const (
	// ids of remote cluster refs are used as keys in gometa service.
	// the following prefix distinguishes the remote cluster refs from other entries
	// and reduces the chance of naming conflicts
	RemoteClusterKeyPrefix = "remoteCluster"
)

var SizeOfRemoteClusterRefId = 32
var MaxRetryForIdGeneration = 5

const (
	EncryptionType_Full string = "full"
	EncryptionType_Half string = "half"
)

// Hostname mode is specific to how XDCR would handle the hostname field
const (
	HostnameMode_None     string = ""         // XDCR Heuristic mode
	HostnameMode_External string = "external" // used by K8 operator/DBAS
	HostnameMode_Internal string = "default"  // To be consistent as goCBv2
)

// As a way to provide user a seemless experience to migrate data from on-prem
// to Capella, this trusted root CA for signing capella clusters is provided here
// The certificate expires in 2029
const CapellaCert = `
-----BEGIN CERTIFICATE-----
MIIDFTCCAf2gAwIBAgIRANLVkgOvtaXiQJi0V6qeNtswDQYJKoZIhvcNAQELBQAw
JDESMBAGA1UECgwJQ291Y2hiYXNlMQ4wDAYDVQQLDAVDbG91ZDAeFw0xOTEyMDYy
MjEyNTlaFw0yOTEyMDYyMzEyNTlaMCQxEjAQBgNVBAoMCUNvdWNoYmFzZTEOMAwG
A1UECwwFQ2xvdWQwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCfvOIi
enG4Dp+hJu9asdxEMRmH70hDyMXv5ZjBhbo39a42QwR59y/rC/sahLLQuNwqif85
Fod1DkqgO6Ng3vecSAwyYVkj5NKdycQu5tzsZkghlpSDAyI0xlIPSQjoORA/pCOU
WOpymA9dOjC1bo6rDyw0yWP2nFAI/KA4Z806XeqLREuB7292UnSsgFs4/5lqeil6
rL3ooAw/i0uxr/TQSaxi1l8t4iMt4/gU+W52+8Yol0JbXBTFX6itg62ppb/Eugmn
mQRMgL67ccZs7cJ9/A0wlXencX2ohZQOR3mtknfol3FH4+glQFn27Q4xBCzVkY9j
KQ20T1LgmGSngBInAgMBAAGjQjBAMA8GA1UdEwEB/wQFMAMBAf8wHQYDVR0OBBYE
FJQOBPvrkU2In1Sjoxt97Xy8+cKNMA4GA1UdDwEB/wQEAwIBhjANBgkqhkiG9w0B
AQsFAAOCAQEARgM6XwcXPLSpFdSf0w8PtpNGehmdWijPM3wHb7WZiS47iNen3oq8
m2mm6V3Z57wbboPpfI+VEzbhiDcFfVnK1CXMC0tkF3fnOG1BDDvwt4jU95vBiNjY
xdzlTP/Z+qr0cnVbGBSZ+fbXstSiRaaAVcqQyv3BRvBadKBkCyPwo+7svQnScQ5P
Js7HEHKVms5tZTgKIw1fbmgR2XHleah1AcANB+MAPBCcTgqurqr5G7W2aPSBLLGA
fRIiVzm7VFLc7kWbp7ENH39HVG6TZzKnfl9zJYeiklo5vQQhGSMhzBsO70z4RRzi
DPFAN/4qZAgD5q3AFNIq2WWADFQGSwVJhg==
-----END CERTIFICATE-----`

func IsCapellaHostname(hostname string) bool {
	return strings.Contains(hostname, base.CapellaHostnameSuffix)
}

/*
***********************************
/* struct RemoteClusterReference
  - NOTE - if adding/removing new members, need to also modify LoadFrom(), etc.

************************************
*/
type RemoteClusterReference struct {
	Id_       string `json:"Id"`
	Uuid_     string `json:"Uuid"`
	Name_     string `json:"Name"`
	HostName_ string `json:"HostName"`
	UserName_ string `json:"UserName"`
	Password_ string `json:"Password"`

	DemandEncryption_ bool   `json:"DemandEncryption"`
	EncryptionType_   string `json:"EncryptionType"`
	// Certificate_ contains user entered data only. Readers should call Certificates() or certificatesNoLock() to
	// ensure that access to Capella will not be interrupted. This way, if the Capella default's cert is updated, this
	// field shall continue to remain empty and no metadata upgrade needs to take place
	Certificate_ []byte `json:"Certificate"`
	// hostname to use when making https connection
	HttpsHostName_    string            `json:"HttpsHostName"`
	SANInCertificate_ bool              `json:"SANInCertificate"`
	HttpAuthMech_     base.HttpAuthMech `json:"HttpAuthMech"`

	ClientCertificate_ []byte `json:"ClientCertificate"`
	ClientKey_         []byte `json:"ClientKey"`

	// these are hostname actually used to connect to target
	// they are rotated among nodes in target cluster to achieve load balancing on target
	// they are used to update HostName/HttpsHostName when HostName has been removed from the target cluster
	// they are not exposed to users, e.g., through UI or rest api
	ActiveHostName_      string `json:"ActiveHostName"`
	ActiveHttpsHostName_ string `json:"ActiveHttpsHostName"`

	HostnameMode_ string `json:"HostnameMode"`

	// revision number to be used by metadata service. not included in json
	// Revision should only be passed along and should never be modified
	revision interface{}

	// Internal RW Mutex to prevent concurrent data race
	mutex sync.RWMutex

	// In-memory flag of whether or not the HostName_ field is a DNS SRV entry
	hostnameSRVType HostNameSrvType
	// srv related things that need to be used by remote cluster agent
	dnsSrvHelper baseH.DnsSrvHelperIface
	srvEntries   SrvEntriesType

	// If requested by REST, this field will be non-Empty
	connectivityStatus string
	// latest unique connectivity error messages until the status is RC_OK
	connectivityErrors ConnErrs

	// If specifically requested, this will contain fetched remote bucket manifest
	TargetBucketManifest map[string]*CollectionsManifest

	// if specified true and using non-DNSSRV hostname, bootstrap hostname will not be replaced. Eg: For users using a LB
	RestrictHostnameReplace bool `json:"RestrictHostnameReplace"`
}

type ConnErr struct {
	FirstOccurence time.Time `json:"firstOccuredOn"`
	TargetNode     string    `json:"targetNode"`
	Cause          string    `json:"causeOfError"`
	Occurences     uint      `json:"numOccurences"`
}

type ConnErrs []ConnErr

func (ces ConnErrs) String() string {
	bytes, err := json.Marshal(ces)
	if err != nil {
		return ""
	}

	return string(bytes)
}

func (ce *ConnErr) SameAs(cein ConnErr) bool {
	return ce.Cause == cein.Cause && ce.TargetNode == cein.TargetNode
}

func (ref *RemoteClusterReference) InsertConnError(ce ConnErr) {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()

	if ref.connectivityErrors == nil {
		ref.connectivityErrors = make([]ConnErr, 0)
	}

	// skip if duplicate
	for i, conErr := range ref.connectivityErrors {
		if conErr.SameAs(ce) {
			ref.connectivityErrors[i].Occurences++
			return
		}
	}

	// insert at the beginning
	end := len(ref.connectivityErrors)
	if end > base.ConnErrorsListMaxEntries-1 {
		end = base.ConnErrorsListMaxEntries - 1
	}
	ref.connectivityErrors = append(ref.connectivityErrors[:end], ConnErr{})
	for i := len(ref.connectivityErrors) - 1; i > 0; i-- {
		ref.connectivityErrors[i] = ref.connectivityErrors[i-1]
	}
	ref.connectivityErrors[0] = ce
}

func (ref *RemoteClusterReference) ClearConnErrs() {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.connectivityErrors = nil
}

func (ref *RemoteClusterReference) PrintConnErrs(logger *log.CommonLogger) {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	if ref.connectivityErrors != nil && len(ref.connectivityErrors) != 0 {
		logger.Errorf("RC %v Connectivity Errors = %v", ref.Name_, ref.connectivityErrors)
	}
}

func (ref *RemoteClusterReference) CloneConnErrsNoLock() []ConnErr {
	if ref.connectivityErrors == nil {
		return nil
	}
	deepCopy := make([]ConnErr, 0)
	for _, ce := range ref.connectivityErrors {
		deepCopy = append(deepCopy, ce)
	}

	return deepCopy
}

func (ref *RemoteClusterReference) SetRestrictHostnameReplaceNoLock(val bool) {
	ref.RestrictHostnameReplace = val
}

func (ref *RemoteClusterReference) GetRestrictHostnameReplaceNoLock() bool {
	return ref.RestrictHostnameReplace
}

func (ref *RemoteClusterReference) SetRestrictHostnameReplace(val bool) {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.SetRestrictHostnameReplaceNoLock(val)
}

func (ref *RemoteClusterReference) GetRestrictHostnameReplace() bool {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.GetRestrictHostnameReplaceNoLock()
}

func (ref *RemoteClusterReference) RestrictHostnameReplaceAtRefresh() bool {
	return !ref.IsDnsSRV() && ref.GetRestrictHostnameReplace()
}

type HostNameSrvType int

const (
	HostNameNonSRV    HostNameSrvType = iota
	HostNameSRV       HostNameSrvType = iota
	HostNameSecureSRV HostNameSrvType = iota
	HostNameBothSRV   HostNameSrvType = iota
)

func NewHostNameSrvType(srvRecordsType baseH.SrvRecordsType) HostNameSrvType {
	switch srvRecordsType {
	case baseH.SrvRecordsInvalid:
		return HostNameNonSRV
	case baseH.SrvRecordsNonSecure:
		return HostNameSRV
	case baseH.SrvRecordsSecure:
		return HostNameSecureSRV
	case baseH.SrvRecordsBoth:
		return HostNameBothSRV
	}
	return HostNameNonSRV
}

func (h *HostNameSrvType) SetSRV(secure baseH.SrvRecordsType) {
	switch secure {
	case baseH.SrvRecordsInvalid:
		*h = HostNameNonSRV
	case baseH.SrvRecordsNonSecure:
		*h = HostNameSRV
	case baseH.SrvRecordsSecure:
		*h = HostNameSecureSRV
	case baseH.SrvRecordsBoth:
		*h = HostNameBothSRV
	}
}

func (h *HostNameSrvType) ClearSRV() {
	*h = HostNameNonSRV
}

type SrvEntriesType []SrvEntryType

type SrvEntryType struct {
	srv *net.SRV
}

// Note - this method will return the port
// For couchbase/couchbases service, the port is KV port
// For now, return standard ports until MB-41083 is implemented
func (s SrvEntryType) GetTargetConnectionString(srvType HostNameSrvType, fullEncryption bool) (string, error) {
	if s.srv == nil {
		return "", base.ErrorInvalidSRVFormat
	}
	// SRV target always end with an extra period
	hostname := strings.TrimSuffix(s.srv.Target, ".")
	switch srvType {
	case HostNameNonSRV:
		return "", base.ErrorInvalidSRVFormat
	case HostNameSRV:
		return fmt.Sprintf("%v:%v", hostname, base.DefaultAdminPort), nil
	case HostNameSecureSRV:
		return fmt.Sprintf("%v:%v", hostname, base.DefaultAdminPortSSL), nil
	case HostNameBothSRV:
		if fullEncryption {
			return fmt.Sprintf("%v:%v", hostname, base.DefaultAdminPortSSL), nil
		} else {
			return fmt.Sprintf("%v:%v", hostname, base.DefaultAdminPort), nil
		}
	default:
		return "", base.ErrorInvalidSRVFormat
	}
}

// Because Remote Cluster Reference doesn't care about weight or priority
// as long as both lists contain the same information, they are same
// even if the ordering is off
func (s SrvEntriesType) SameAs(other SrvEntriesType) bool {
	if len(s) != len(other) {
		return false
	}

	for _, oneEntry := range s {
		var found bool
		for _, oneOtherEntry := range other {
			if *oneEntry.srv == *oneOtherEntry.srv {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (s SrvEntriesType) Clone() SrvEntriesType {
	var cloned SrvEntriesType
	for _, entry := range s {
		clonedSrvPtr := &net.SRV{}
		*clonedSrvPtr = *(entry.srv)
		cloned = append(cloned, SrvEntryType{clonedSrvPtr})
	}
	return cloned
}

func NewRemoteClusterReference(uuid, name, hostName, userName, password, hostnameMode string,
	demandEncryption bool, encryptionType string, certificate, clientCertificate, clientKey []byte,
	dnsSrvHelper baseH.DnsSrvHelperIface) (*RemoteClusterReference, error) {
	refId, err := RemoteClusterRefId()
	if err != nil {
		return nil, err
	}

	ref := &RemoteClusterReference{Id_: refId,
		Uuid_:              uuid,
		Name_:              name,
		HostName_:          hostName,
		UserName_:          userName,
		Password_:          password,
		DemandEncryption_:  demandEncryption,
		EncryptionType_:    encryptionType,
		Certificate_:       certificate,
		ClientCertificate_: clientCertificate,
		ClientKey_:         clientKey,
		HostnameMode_:      hostnameMode,
		dnsSrvHelper:       dnsSrvHelper,
	}

	return ref, nil
}

func RemoteClusterRefId() (string, error) {
	refUuid, err := base.GenerateRandomId(SizeOfRemoteClusterRefId, MaxRetryForIdGeneration)
	if err != nil {
		return "", err
	}
	parts := []string{RemoteClusterKeyPrefix, refUuid}
	return strings.Join(parts, base.KeyPartsDelimiter), nil
}

// SmallString is String() but much more consise and logging friendly
func (ref *RemoteClusterReference) SmallString() string {
	return fmt.Sprintf("isHTTPS:%v,isEncEnabled:%v,isFullEnc:%v,isDNSSrv:%v,host:[%s,%s],activeHost:[%s,%s]",
		ref.IsHttps(),
		ref.IsEncryptionEnabled(),
		ref.IsFullEncryption(),
		ref.IsDnsSRV(),
		ref.HostName(), ref.HttpsHostName(), ref.ActiveHostName(), ref.ActiveHttpsHostName())
}

// implements base.ClusterConnectionInfoProvider
func (ref *RemoteClusterReference) MyConnectionStr() (connStr string, err error) {
	if ref.IsDnsSRV() && ref.ActiveHostName() == "" && ref.ActiveHttpsHostName() == "" {
		srvHostnames := ref.GetSRVHostNames()
		if len(srvHostnames) == 0 {
			// Shouldn't be the case
			connStr = ref.HostName()
		}
		randIdx := rand.Int() % len(srvHostnames)
		connStr = srvHostnames[randIdx]
	} else if ref.IsHttps() {
		activeHttpsHostName := ref.ActiveHttpsHostName()
		if len(activeHttpsHostName) > 0 {
			connStr = activeHttpsHostName
		} else {
			connStr = ref.HttpsHostName()
		}
	} else {
		activeHostName := ref.ActiveHostName()
		if len(activeHostName) > 0 {
			connStr = activeHostName
		} else {
			connStr = ref.HostName()
		}
	}
	connStr, err = base.MapToSupportedIpFamily(connStr, ref.IsFullEncryption())
	return
}

func (ref *RemoteClusterReference) redactNoLock() *RemoteClusterReference {
	if ref != nil {
		if len(ref.UserName_) > 0 && !base.IsStringRedacted(ref.UserName_) {
			ref.UserName_ = base.TagUD(ref.UserName_)
		}
		if len(ref.Password_) > 0 && !base.IsStringRedacted(ref.Password_) {
			ref.Password_ = base.TagUD(ref.Password_)
		}
		if len(ref.Certificate_) > 0 && !base.IsByteSliceRedacted(ref.Certificate_) {
			ref.Certificate_ = base.TagUDBytes(ref.Certificate_)
		}
		if len(ref.ClientCertificate_) > 0 && !base.IsByteSliceRedacted(ref.ClientCertificate_) {
			ref.ClientCertificate_ = base.TagUDBytes(ref.ClientCertificate_)
		}
		// no need to redact ClientKey since it is always nil in this ref for redact
	}
	return ref
}

func (ref *RemoteClusterReference) CloneAndRedact() *RemoteClusterReference {
	if ref != nil {
		ref.mutex.RLock()
		defer ref.mutex.RUnlock()
		return ref.cloneForRedactNoLock().redactNoLock()
	}
	return ref
}

func (ref *RemoteClusterReference) MyCredentials() (string, string, base.HttpAuthMech, []byte, bool, []byte, []byte, error) {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.UserName_, ref.Password_, ref.HttpAuthMech_, ref.certificatesNoLock(), ref.SANInCertificate_, ref.ClientCertificate_, ref.ClientKey_, nil
}

// convert to a map for output
// do not include password or client private key
func (ref *RemoteClusterReference) ToMap() map[string]interface{} {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	uri := base.UrlDelimiter + base.RemoteClustersPath + base.UrlDelimiter + ref.Name_
	validateUri := uri + base.JustValidatePostfix
	outputMap := make(map[string]interface{})
	outputMap[base.RemoteClusterUuid] = ref.Uuid_
	outputMap[base.RemoteClusterName] = ref.Name_
	outputMap[base.RemoteClusterUri] = uri
	outputMap[base.RemoteClusterValidateUri] = validateUri
	outputMap[base.RemoteClusterHostName] = ref.getHostNameForOutputNoLock()
	outputMap[base.RemoteClusterUserName] = ref.UserName_
	outputMap[base.RemoteClusterDeleted] = false
	outputMap[base.RemoteClusterSecureType] = ref.SecureTypeString()
	outputMap[base.RemoteClusterHostnameMode] = ref.HostnameMode()
	// To be deprecated
	if ref.IsEncryptionEnabled() {
		outputMap[base.RemoteClusterDemandEncryption] = ref.DemandEncryption_
		outputMap[base.RemoteClusterEncryptionType] = ref.EncryptionType_
	}
	if len(ref.Certificate_) > 0 {
		outputMap[base.RemoteClusterCertificate] = string(ref.Certificate_)
	}
	if len(ref.ClientCertificate_) > 0 {
		outputMap[base.RemoteClusterClientCertificate] = string(ref.ClientCertificate_)
	}
	if ref.connectivityStatus != "" {
		outputMap[base.ConnectivityStatus] = ref.connectivityStatus
	}
	if len(ref.TargetBucketManifest) > 0 {
		outputMap[base.RemoteBucketManifest] = ref.TargetBucketManifest
	}
	outputMap[base.ConnectivityErrors] = ref.CloneConnErrsNoLock()

	return outputMap
}

// checks if the passed in ref is the same as the current ref
func (ref *RemoteClusterReference) IsSame(ref2 *RemoteClusterReference) bool {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	if ref == nil {
		return ref2 == nil
	}
	if ref2 == nil {
		return false
	}
	if !ref.isEssentiallySameNoLock(ref2) {
		return false
	} else {
		ref2.mutex.RLock()
		defer ref2.mutex.RUnlock()
		return reflect.DeepEqual(ref.revision, ref2.revision) && ref.HttpsHostName_ == ref2.HttpsHostName_ &&
			ref.ActiveHostName_ == ref2.ActiveHostName_ && ref.ActiveHttpsHostName_ == ref2.ActiveHttpsHostName_ &&
			ref.hostnameSRVType == ref2.hostnameSRVType && ref.srvEntries.SameAs(ref2.srvEntries)
	}
}

func (ref *RemoteClusterReference) AreUserSecurityCredentialsTheSame(ref2 *RemoteClusterReference) bool {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.areUserSecurityCredentialsTheSameNoLock(ref2)
}

func (ref *RemoteClusterReference) areUserSecurityCredentialsTheSameNoLock(ref2 *RemoteClusterReference) bool {
	if ref == nil {
		return ref2 == nil
	}
	if ref2 == nil {
		return false
	}
	ref2.mutex.RLock()
	defer ref2.mutex.RUnlock()
	return ref.UserName_ == ref2.UserName_ && ref.Password_ == ref2.Password_ && ref.DemandEncryption_ == ref2.DemandEncryption_ &&
		ref.EncryptionType_ == ref2.EncryptionType_ && bytes.Equal(ref.Certificate_, ref2.Certificate_) &&
		bytes.Equal(ref.ClientCertificate_, ref2.ClientCertificate_) && bytes.Equal(ref.ClientKey_, ref2.ClientKey_)
}

func (ref *RemoteClusterReference) AreSecuritySettingsTheSame(ref2 *RemoteClusterReference) bool {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.areSecuritySettingsTheSameNoLock(ref2)
}

func (ref *RemoteClusterReference) areSecuritySettingsTheSameNoLock(ref2 *RemoteClusterReference) bool {
	if ref == nil {
		return ref2 == nil
	}
	if ref2 == nil {
		return false
	}
	ref2.mutex.RLock()
	defer ref2.mutex.RUnlock()
	return ref.SANInCertificate_ == ref2.SANInCertificate_ &&
		ref.HttpAuthMech_ == ref2.HttpAuthMech_
}

// checks if they are the same minus changable fields
func (ref *RemoteClusterReference) IsEssentiallySame(ref2 *RemoteClusterReference) bool {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.isEssentiallySameNoLock(ref2)
}

func (ref *RemoteClusterReference) isEssentiallySameNoLock(ref2 *RemoteClusterReference) bool {
	if ref == nil {
		return ref2 == nil
	}
	if ref2 == nil {
		return false
	}
	if !ref.areUserSecurityCredentialsTheSameNoLock(ref2) || !ref.areSecuritySettingsTheSameNoLock(ref2) {
		return false
	} else {
		ref2.mutex.RLock()
		defer ref2.mutex.RUnlock()
		return ref.Id_ == ref2.Id_ && ref.Uuid_ == ref2.Uuid_ && ref.Name_ == ref2.Name_ && ref.HostName_ == ref2.HostName_
	}
}

// Caller of this function should be wary of the need for redaction. Recommended to use it on an already redacted object
func (ref *RemoteClusterReference) String() string {
	if ref == nil {
		return "nil"
	}

	ref.mutex.RLock()
	defer ref.mutex.RUnlock()

	// redact password and client private key
	var password string
	if len(ref.Password_) > 0 {
		password = "xxxx"
	}
	var clientKey string
	// cannot do "if len(ref.ClientKey) > 0" since ClientKey may have been changed to nil due to redaction.
	// check ClientCertificate instead
	if len(ref.ClientCertificate_) > 0 {
		clientKey = "xxxx"
	}

	return fmt.Sprintf("id:%v; uuid:%v; name:%v; hostName:%v; userName:%v; password:%v; secureType:%v; certificate:%v; clientCertificate:%v; clientKey:%v; SanInCertificate:%v; HttpAuthMech:%v, revision:%v",
		ref.Id_, ref.Uuid_, ref.Name_, ref.HostName_, ref.UserName_, password, ref.SecureTypeString(), ref.Certificate_, ref.ClientCertificate_, clientKey, ref.SANInCertificate_, ref.HttpAuthMech_, ref.revision)
}

func (ref *RemoteClusterReference) LoadFrom(inRef *RemoteClusterReference) {
	if ref == nil {
		return
	}
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.loadNonActivesFromNoLock(inRef)
	ref.ActiveHostName_ = inRef.ActiveHostName()
	ref.ActiveHttpsHostName_ = inRef.ActiveHttpsHostName()
	ref.hostnameSRVType = inRef.hostnameSRVType
	ref.srvEntries = inRef.srvEntries.Clone()
	ref.dnsSrvHelper = inRef.dnsSrvHelper
}

func (ref *RemoteClusterReference) LoadNonActivesFrom(inRef *RemoteClusterReference) {
	if ref == nil {
		return
	}
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.loadNonActivesFromNoLock(inRef)
}

func (ref *RemoteClusterReference) loadNonActivesFromNoLock(inRef *RemoteClusterReference) {
	inRef.mutex.RLock()
	defer inRef.mutex.RUnlock()
	ref.Id_ = inRef.Id_
	ref.Uuid_ = inRef.Uuid_
	ref.Name_ = inRef.Name_
	ref.HostName_ = inRef.HostName_
	ref.UserName_ = inRef.UserName_
	ref.Password_ = inRef.Password_
	ref.DemandEncryption_ = inRef.DemandEncryption_
	ref.Certificate_ = base.DeepCopyByteArray(inRef.Certificate_)
	ref.ClientCertificate_ = base.DeepCopyByteArray(inRef.ClientCertificate_)
	ref.ClientKey_ = base.DeepCopyByteArray(inRef.ClientKey_)
	ref.HttpsHostName_ = inRef.HttpsHostName_
	ref.EncryptionType_ = inRef.EncryptionType_
	ref.SANInCertificate_ = inRef.SANInCertificate_
	ref.HttpAuthMech_ = inRef.HttpAuthMech_
	ref.HostnameMode_ = inRef.HostnameMode_
	ref.RestrictHostnameReplace = inRef.RestrictHostnameReplace
	// !!! shallow copy of revision.
	// ref.Revision should only be passed along and should never be modified
	ref.revision = inRef.revision
}

func (ref *RemoteClusterReference) Clone() *RemoteClusterReference {
	if ref == nil {
		return nil
	}

	ref.mutex.RLock()
	defer ref.mutex.RUnlock()

	cloneRef := ref.cloneForRedactNoLock()
	cloneRef.ClientKey_ = base.DeepCopyByteArray(ref.ClientKey_)
	return cloneRef
}

func (ref *RemoteClusterReference) cloneForRedactNoLock() *RemoteClusterReference {
	cloneRef := ref.cloneCommonFieldsNoLock()
	cloneRef.ActiveHostName_ = ref.ActiveHostName_
	cloneRef.ActiveHttpsHostName_ = ref.ActiveHttpsHostName_
	return cloneRef
}

// clone for metakv update, i.e., clone without any internal fields
func (ref *RemoteClusterReference) CloneForMetakvUpdate() *RemoteClusterReference {
	if ref == nil {
		return nil
	}

	ref.mutex.RLock()
	defer ref.mutex.RUnlock()

	cloneRef := ref.cloneCommonFieldsNoLock()
	cloneRef.ClientKey_ = base.DeepCopyByteArray(ref.ClientKey_)
	// no need for Revision in metakv.
	// cloneRef.Revision is a shallow copy, hence it is not a big waste to copy and then set it to nil
	cloneRef.revision = nil
	return cloneRef
}

// clone of common fields needed by all Clonexxx() APIs
func (ref *RemoteClusterReference) cloneCommonFieldsNoLock() *RemoteClusterReference {
	if ref == nil {
		return nil
	}
	return &RemoteClusterReference{Id_: ref.Id_,
		Uuid_:              ref.Uuid_,
		Name_:              ref.Name_,
		HostName_:          ref.HostName_,
		HttpsHostName_:     ref.HttpsHostName_,
		UserName_:          ref.UserName_,
		Password_:          ref.Password_,
		DemandEncryption_:  ref.DemandEncryption_,
		Certificate_:       base.DeepCopyByteArray(ref.Certificate_),
		ClientCertificate_: base.DeepCopyByteArray(ref.ClientCertificate_),
		EncryptionType_:    ref.EncryptionType_,
		SANInCertificate_:  ref.SANInCertificate_,
		HttpAuthMech_:      ref.HttpAuthMech_,
		HostnameMode_:      ref.HostnameMode_,
		// !!! shallow copy of revision.
		// ref.Revision should only be passed along and should never be modified
		revision:                ref.revision,
		dnsSrvHelper:            ref.dnsSrvHelper,
		hostnameSRVType:         ref.hostnameSRVType,
		srvEntries:              ref.srvEntries.Clone(),
		connectivityErrors:      ref.connectivityErrors, // shallow copy to ensure ClearConnErrs() works
		RestrictHostnameReplace: ref.GetRestrictHostnameReplaceNoLock(),
	}
}

func (ref *RemoteClusterReference) IsEncryptionEnabled() bool {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.DemandEncryption_
}

var remoteClusterReferenceSampleEmptyRef *RemoteClusterReference = &RemoteClusterReference{}

func (ref *RemoteClusterReference) IsEmpty() bool {
	return ref.IsSame(remoteClusterReferenceSampleEmptyRef)
}

func (ref *RemoteClusterReference) IsFullEncryption() bool {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.isFullEncryptionNoLock()
}

func (ref *RemoteClusterReference) isFullEncryptionNoLock() bool {
	// ref.EncryptionType may be empty for unupgraded remote cluster refs. treat it as "full" in this case
	return ref.DemandEncryption_ && (len(ref.EncryptionType_) == 0 || ref.EncryptionType_ == EncryptionType_Full)
}

func (ref *RemoteClusterReference) IsHalfEncryption() bool {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.DemandEncryption_ && ref.EncryptionType_ == EncryptionType_Half
}

func (ref *RemoteClusterReference) IsHttps() bool {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.HttpAuthMech_ == base.HttpAuthMechHttps
}

func (ref *RemoteClusterReference) Clear() {
	ref.LoadFrom(remoteClusterReferenceSampleEmptyRef)
}

func (ref *RemoteClusterReference) ClearRevision() {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.revision = nil
}

// Getters and setters
func (ref *RemoteClusterReference) SecureTypeString() string {
	if !ref.IsEncryptionEnabled() {
		return base.SecureTypeNone
	} else if ref.IsFullEncryption() {
		return base.SecureTypeFull
	} else {
		return base.SecureTypeHalf
	}
}

func (ref *RemoteClusterReference) Id() string {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.Id_
}

func (ref *RemoteClusterReference) SetId(id string) {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.Id_ = id
}

func (ref *RemoteClusterReference) Name() string {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.Name_
}

func (ref *RemoteClusterReference) SetName(name string) {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.Name_ = name
}

func (ref *RemoteClusterReference) Uuid() string {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.Uuid_
}

func (ref *RemoteClusterReference) SetUuid(uuid string) {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.Uuid_ = uuid
}

func (ref *RemoteClusterReference) HostName() string {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.HostName_
}

func (ref *RemoteClusterReference) IsCapellaHostname() bool {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.isCapellaHostnameNoLock()
}

func (ref *RemoteClusterReference) isCapellaHostnameNoLock() bool {
	return strings.Contains(ref.HostName_, base.CapellaHostnameSuffix) ||
		strings.Contains(ref.ActiveHostName_, base.CapellaHostnameSuffix) ||
		strings.Contains(ref.ActiveHttpsHostName_, base.CapellaHostnameSuffix)
}

func (ref *RemoteClusterReference) SetHostName(name string) {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.HostName_ = name
}

func (ref *RemoteClusterReference) ActiveHostName() string {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.ActiveHostName_
}

func (ref *RemoteClusterReference) SetActiveHostName(activeHostName string) {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.ActiveHostName_ = activeHostName
}

func (ref *RemoteClusterReference) ActiveHttpsHostName() string {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.ActiveHttpsHostName_
}

func (ref *RemoteClusterReference) SetActiveHttpsHostName(activeHttpsHostName string) {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.ActiveHttpsHostName_ = activeHttpsHostName
}

func (ref *RemoteClusterReference) HttpsHostName() string {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.HttpsHostName_
}

func (ref *RemoteClusterReference) SetHttpsHostName(name string) {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.HttpsHostName_ = name
}

func (ref *RemoteClusterReference) UserName() string {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.UserName_
}

func (ref *RemoteClusterReference) SetUserName(userName string) {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.UserName_ = userName
}

func (ref *RemoteClusterReference) Password() string {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.Password_
}

func (ref *RemoteClusterReference) DemandEncryption() bool {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.DemandEncryption_
}

func (ref *RemoteClusterReference) EncryptionType() string {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.EncryptionType_
}

func (ref *RemoteClusterReference) SetEncryptionType(encryptionType string) {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.EncryptionType_ = encryptionType
}

func (ref *RemoteClusterReference) SANInCertificate() bool {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.SANInCertificate_
}

func (ref *RemoteClusterReference) SetSANInCertificate(sanInCertificate bool) {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.SANInCertificate_ = sanInCertificate
}

func (ref *RemoteClusterReference) HttpAuthMech() base.HttpAuthMech {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.HttpAuthMech_
}

func (ref *RemoteClusterReference) SetHttpAuthMech(authMech base.HttpAuthMech) {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.HttpAuthMech_ = authMech
}

func (ref *RemoteClusterReference) Certificates() []byte {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.certificatesNoLock()
}

func (ref *RemoteClusterReference) certificatesNoLock() []byte {
	// As part of making transition to Capella simple, CBServer is now going to
	// allow remote cluster to be created to a Capella Cluster even if the user did not provide
	// a specific root cert
	if len(ref.Certificate_) == 0 && ref.isCapellaHostnameNoLock() {
		return []byte(CapellaCert)
	}

	return ref.Certificate_
}

func (ref *RemoteClusterReference) ClientCertificate() []byte {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.ClientCertificate_
}

func (ref *RemoteClusterReference) ClientKey() []byte {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.ClientKey_
}

func (ref *RemoteClusterReference) Revision() interface{} {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.revision
}

func (ref *RemoteClusterReference) SetRevision(rev interface{}) {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.revision = rev
}

func (ref *RemoteClusterReference) HostnameMode() string {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.HostnameMode_
}

func (ref *RemoteClusterReference) Marshal() ([]byte, error) {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return json.Marshal(ref)
}

func (ref *RemoteClusterReference) Unmarshal(value []byte) error {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	return json.Unmarshal(value, ref)
}

func (ref *RemoteClusterReference) IsDnsSRV() bool {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.isDnsSrvNoLock()
}

func (ref *RemoteClusterReference) isDnsSrvNoLock() bool {
	return ref.hostnameSRVType != HostNameNonSRV
}

func (ref *RemoteClusterReference) GetSRVHostNames() (hostnameList []string) {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()

	if !ref.isDnsSrvNoLock() || len(ref.srvEntries) == 0 {
		return
	}

	for _, oneEntry := range ref.srvEntries {
		connStr, err := oneEntry.GetTargetConnectionString(ref.hostnameSRVType, ref.isFullEncryptionNoLock())
		if err != nil {
			continue
		}
		hostnameList = append(hostnameList, connStr)
	}
	return
}

// DNS SRV look up should be quick
// In the case of failure, if it is a user induced action, then retryOnErr should be false
// because the user can fix any DNS SRV look up error if it isn't right
// If it is a cold start-up or metakv callback, retry on error before giving up
// because there is no way to manually intervene before the system corrects itself
func (ref *RemoteClusterReference) PopulateDnsSrvIfNeeded(logger *log.CommonLogger) error {
	// hostname may have port
	hostNameWithoutPort := base.GetHostName(ref.HostName())
	if net.ParseIP(hostNameWithoutPort) != nil {
		// If it is IPv4 or IPv6, it is not going to be a DNS SRV
		return nil
	}

	ref.mutex.RLock()
	dnsSrvHelper := ref.dnsSrvHelper
	ref.mutex.RUnlock()

	if dnsSrvHelper == nil {
		ref.mutex.Lock()
		if ref.dnsSrvHelper == nil {
			ref.dnsSrvHelper = &baseH.DnsSrvHelper{}
		}
		dnsSrvHelper = ref.dnsSrvHelper
		ref.mutex.Unlock()
	}

	var entries []*net.SRV
	var srvRecordsType baseH.SrvRecordsType
	ref.mutex.RLock()
	lookupName, err := ref.getSRVLookupHostnameNoLock()
	ref.mutex.RUnlock()
	if err != nil {
		if logger != nil {
			logger.Errorf("%s dnsSrv failed while getting lookup name err=%v", ref.Id(), err)
		}
		ref.mutex.Lock()
		ref.hostnameSRVType.ClearSRV()
		ref.mutex.Unlock()
		return err
	}

	entries, srvRecordsType, err = dnsSrvHelper.DnsSrvLookup(lookupName)

	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	if err != nil {
		if logger != nil {
			logger.Errorf("%s dnsSrv failed while lookup lookupHost=%s, err=%v", ref.Name_, lookupName, err)
		}
		ref.hostnameSRVType.ClearSRV()
		return err
	}
	ref.hostnameSRVType.SetSRV(srvRecordsType)
	ref.srvEntries = ref.srvEntries[:0]
	for _, entry := range entries {
		ref.srvEntries = append(ref.srvEntries, SrvEntryType{entry})
	}

	return nil
}

func (ref *RemoteClusterReference) getSRVLookupHostnameNoLock() (string, error) {
	lookupHostname := ref.HostName_

	// Replication Manager should have called validate already, so hostname should be valid IPv4/IPv6/FQDN
	portNumber, err := base.GetPortNumber(ref.HostName_)
	if err == nil {
		if portNumber != base.DefaultAdminPort && portNumber != base.DefaultAdminPortSSL {
			// If port number is non-8091, not going to be a valid DnsSRV
			return "", base.ErrorInvalidSRVFormat
		} else {
			// If 8091, strip it before DNS-SRV lookup. We assume that the user has set up DNS SRV correctly
			// i.e. if they specified 18091, they should have DNS SRV entry pointing to the SSL adminport of the target
			lookupHostname = base.GetHostName(lookupHostname)
		}
	}
	return lookupHostname, nil
}

var ErrorNonSRV error = fmt.Errorf("Cannot call RefreshSRVEntries on a non-SRV entry")
var ErrorNoLongerSRV error = fmt.Errorf("Hostname was an SRV entry, but now is no longer an SRV entry")
var ErrorCapellaNeedsTLS error = fmt.Errorf("References to Capella requires Full Encryption to be enabled")

// When entries are refreshed, the remote cluster's reference, which should be a SRV
// is re-pulled. All the cached DNS SRV entries targets status's are cleared
func (ref *RemoteClusterReference) RefreshSRVEntries() (added, removed []*net.SRV, totalSRVEntries int, err error) {
	if !ref.IsDnsSRV() {
		err = ErrorNonSRV
		return
	}

	var pulledEntries []*net.SRV
	var latestLookup baseH.SrvRecordsType

	ref.mutex.RLock()
	lookupName, err := ref.getSRVLookupHostnameNoLock()
	if err == nil {
		pulledEntries, latestLookup, err = ref.dnsSrvHelper.DnsSrvLookup(lookupName)
	}
	ref.mutex.RUnlock()
	if err != nil && err != base.ErrorInvalidSRVFormat {
		for i := 0; i < 5; i++ {
			ref.mutex.RLock()
			pulledEntries, _, err = ref.dnsSrvHelper.DnsSrvLookup(lookupName)
			ref.mutex.RUnlock()
			if err == nil {
				break
			}
		}
	}

	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	if ref.isDnsSrvNoLock() && err != nil {
		ref.hostnameSRVType.ClearSRV()
		ref.srvEntries = ref.srvEntries[:0]
		return
	}

	// Check if secure type changed - if changed, refresh all the entries
	checkType := NewHostNameSrvType(latestLookup)
	if checkType != ref.hostnameSRVType {
		ref.hostnameSRVType.SetSRV(latestLookup)
		// Replace all the entries
		for _, existingEntry := range ref.srvEntries {
			removed = append(removed, existingEntry.srv)
		}
		ref.srvEntries = ref.srvEntries[:0]
		for _, pulledEntry := range pulledEntries {
			added = append(added, pulledEntry)
			ref.srvEntries = append(ref.srvEntries, SrvEntryType{srv: pulledEntry})
		}
		totalSRVEntries = len(ref.srvEntries)
		return
	}

	added = ref.populateAddedEntries(pulledEntries, added)

	removed = ref.populateRemovedEntries(pulledEntries, removed)

	totalSRVEntries = len(ref.srvEntries)
	return
}

func (ref *RemoteClusterReference) populateRemovedEntries(pulledEntries []*net.SRV, removed []*net.SRV) []*net.SRV {
	var toRemove []*SrvEntryType
	for _, existingEntry := range ref.srvEntries {
		var found bool
		for _, oneEntry := range pulledEntries {
			if *oneEntry == *existingEntry.srv {
				found = true
				break
			}
		}
		if !found {
			removed = append(removed, existingEntry.srv)
			toRemove = append(toRemove, &existingEntry)
		}
	}

	for _, ptr := range toRemove {
		var index int = -1
		var curEntry SrvEntryType
		for index, curEntry = range ref.srvEntries {
			if &curEntry == ptr {
				break
			}
		}
		if index >= 0 {
			ref.srvEntries = append(ref.srvEntries[:index], ref.srvEntries[index+1:]...)
		}
	}
	return removed
}

func (ref *RemoteClusterReference) populateAddedEntries(pulledEntries []*net.SRV, added []*net.SRV) []*net.SRV {
	for _, oneEntry := range pulledEntries {
		var found bool
		for _, existingEntry := range ref.srvEntries {
			if *oneEntry == *existingEntry.srv {
				found = true
				break
			}
		}
		if !found {
			added = append(added, oneEntry)
			ref.srvEntries = append(ref.srvEntries, SrvEntryType{srv: oneEntry})
		}
	}
	return added
}

var ErrorNoBootableSRVEntryFound = fmt.Errorf("None of the SRV entries are bootstrappable")

// Given the reference's cluster's UUID, these are the nodes that are part of the target cluster
// that target ns_server has returned
// If the user set up SRV such that it forwards to the same FQDN as what the ns_server list returns,
// i.e. user sets up a.abc.com to point to a node: 10.10.1.1 and that node exists in the pulled list
// from ns_server, then it is good enough to validate that the SRV entry is still bootable
// Under Autonomous Operator though, it most likely is not how it will be set up. Regardless,
// this mechanism is a "shortcut" for bootable check without the need to reach out to target cluster again
func (ref *RemoteClusterReference) CheckSRVValidityUsingNodeAddressesList(nodeAddressesList base.StringPairList) error {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()

	var bootable bool
OUTERFOR:
	for _, entry := range ref.srvEntries {
		for _, pair := range nodeAddressesList {
			regHostName := base.GetHostName(pair.GetFirstString())
			sslHostName := base.GetHostName(pair.GetSecondString())
			srvRegEntry, err := entry.GetTargetConnectionString(HostNameSRV, false)
			srvSecureEntry, err2 := entry.GetTargetConnectionString(HostNameSecureSRV, false)
			if err != nil || err2 != nil {
				continue
			}

			// The nodeAddressesList should contain the port #, so does GetTargetConnectionString()
			if srvRegEntry == regHostName || srvSecureEntry == sslHostName {
				bootable = true
				break OUTERFOR
			}
		}
	}
	if !bootable {
		return ErrorNoBootableSRVEntryFound
	}
	return nil
}

type GetUUIDFunc func(hostAddr, username, password string, authMech base.HttpAuthMech, certificate []byte, sanInCertificate bool, clientCertificate, clientKey []byte, logger *log.CommonLogger) (string, error)

// In the case where user sets up the SRV entries such that they do not reflect the pulled ns_server list,
// then this method is used to validate that the SRV is actually still valid for bootstrap.
// This is more expensive, as it requires connecting to at least one SRV entry to see if the UUID of the target
// matches the UUID of the reference
// If none match, it means the srv record is no longer valid for bootstrap
func (ref *RemoteClusterReference) CheckSRVValidityByUUID(getter GetUUIDFunc, logger *log.CommonLogger) error {
	if ref.Uuid() == "" {
		// Uninitialized UUID - weird but means assume SRV entry is valid
		return nil
	}

	var valid bool
	atLeastOneErr := make(base.ErrorMap)
	// Randomize SRV entry hit, since we don't really take in account for weight
	clonedEntries := ref.srvEntries.Clone()
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(clonedEntries), func(i, j int) { clonedEntries[i], clonedEntries[j] = clonedEntries[j], clonedEntries[i] })
	for _, entry := range clonedEntries {
		hostAddr, err := entry.GetTargetConnectionString(ref.HostNameSrvType(), ref.IsFullEncryption())
		if err != nil {
			continue
		}
		checkUuid, err := getter(hostAddr, ref.UserName(), ref.Password(), ref.HttpAuthMech(), ref.Certificates(), ref.SANInCertificate(), ref.ClientCertificate(), ref.ClientKey(), logger)
		if err != nil {
			// Skip and check a next one
			continue
		}
		if checkUuid == ref.Uuid_ {
			valid = true
			break
		} else {
			atLeastOneErr[hostAddr] = fmt.Errorf("this SRV entry is pointing to a different cluster UUID: %v", checkUuid)
		}
	}

	if !valid {
		return ErrorNoBootableSRVEntryFound
	} else if len(atLeastOneErr) > 0 {
		return fmt.Errorf(base.FlattenErrorMap(atLeastOneErr))
	}
	return nil
}

func (ref *RemoteClusterReference) HostNameSrvType() HostNameSrvType {
	ref.mutex.RLock()
	defer ref.mutex.RUnlock()
	return ref.hostnameSRVType
}

func (ref *RemoteClusterReference) SetConnectivityStatus(status string) {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.connectivityStatus = status
}

func (ref *RemoteClusterReference) UnitTestSetSRVHelper(helper baseH.DnsSrvHelperIface) {
	ref.mutex.Lock()
	defer ref.mutex.Unlock()
	ref.dnsSrvHelper = helper
}

func (ref *RemoteClusterReference) getHostNameForOutputNoLock() interface{} {
	if ref.isDnsSrvNoLock() {
		portNumber, err := base.GetPortNumber(ref.HostName_)
		if err != nil {
			return ref.HostName_
		}
		return strings.TrimSuffix(ref.HostName_, fmt.Sprintf("%v%v", base.UrlPortNumberDelimiter, portNumber))
	} else {
		return ref.HostName_
	}
}

func (ref *RemoteClusterReference) ValidateCertificates() error {
	refCertificates := ref.Certificates()
	if len(refCertificates) == 0 {
		return nil
	}

	// If the certs being returned are all part of the Capella trusted CA, and they do not enable TLS
	// We should not let this through to prevent unsecure data leakage
	if ref.IsCapellaHostname() && !ref.IsFullEncryption() {
		return ErrorCapellaNeedsTLS
	}

	// check validity of server root certificates
	var rootCerts []*x509.Certificate
	for {
		var block *pem.Block
		block, refCertificates = pem.Decode(refCertificates)
		if block == nil {
			break
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return fmt.Errorf("Failed to parse certificate. err=%v", err)
		}

		// check the signature of certificate
		err = certificate.CheckSignature(certificate.SignatureAlgorithm, certificate.RawTBSCertificate, certificate.Signature)
		if err != nil {
			return fmt.Errorf("Error validating the signature of certificate. err=%v", err)
		}
		rootCerts = append(rootCerts, certificate)
	}
	// We should have at least one root certificate
	if len(rootCerts) == 0 {
		return base.InvalidCerfiticateError
	}
	// check validity of client certificate if it has been provided
	refClientCertificate := ref.ClientCertificate()
	if len(refClientCertificate) == 0 {
		return nil
	}

	_, err := tls.X509KeyPair(refClientCertificate, ref.ClientKey())
	if err != nil {
		return fmt.Errorf("Error parsing client certificate. err=%v", err)
	}

	return nil
}

type ConnectivityStatus int

const (
	// If the node has not been contacted yet
	ConnIniting ConnectivityStatus = iota
	// Nothing wrong yet
	ConnValid ConnectivityStatus = iota
	// If any remote cluster node returned authentication error
	ConnAuthErr ConnectivityStatus = iota
	// If this node experienced connectivity issues to a least one remote cluster nodes
	ConnDegraded ConnectivityStatus = iota
	// If this node cannot contact every single remote cluster nodes
	ConnError ConnectivityStatus = iota
)

func (c ConnectivityStatus) String() string {
	switch c {
	case ConnIniting:
		return "RC_INIT"
	case ConnValid:
		return "RC_OK"
	case ConnAuthErr:
		return "RC_AUTH_ERR"
	case ConnDegraded:
		return "RC_DEGRADED"
	case ConnError:
		return "RC_ERROR"
	default:
		return "?? (ConnectivityStatus)"
	}
}
