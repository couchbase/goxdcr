// Copyright (c) 2013-2020 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package base

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	mrand "math/rand"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
)

type SettingDef struct {
	Data_type reflect.Type
	Required  bool
}

func NewSettingDef(data_type reflect.Type, bReq bool) *SettingDef {
	return &SettingDef{Data_type: data_type, Required: bReq}
}

type SettingDefinitions map[string]*SettingDef

type ErrorMap map[string]error

// Not thread safe - callers need to sync
func (errMap *ErrorMap) AddErrors(otherMap ErrorMap) {
	MergeErrorMaps(*errMap, otherMap, true /*overwrite*/)
}

func (errMap *ErrorMap) HasError(err error) bool {
	for _, v := range *errMap {
		if v == err {
			return true
		}
	}
	return false
}

type SSLPortMap map[string]uint16

type SettingsError struct {
	err_map ErrorMap
}

func (se SettingsError) Error() string {
	var buffer bytes.Buffer
	for key, err := range se.err_map {
		errStr := fmt.Sprintf("setting=%s; err=%s\n", key, err.Error())
		buffer.WriteString(errStr)
	}
	return buffer.String()
}

func NewSettingsError() *SettingsError {
	return &SettingsError{make(ErrorMap)}
}

func (se SettingsError) Add(key string, err error) {
	se.err_map[key] = err
}

type CollectionsManifestIdPair struct {
	SourceManifestId uint64
	TargetManifestId uint64
}

// Ensure that this current c can accomodate the other range
func (c *CollectionsManifestIdPair) Accomodate(other CollectionsManifestIdPair) {
	if c.SourceManifestId > other.SourceManifestId {
		c.SourceManifestId = other.SourceManifestId
	}
	if c.TargetManifestId < other.TargetManifestId {
		c.TargetManifestId = other.TargetManifestId
	}
}

// timestamp for a specific vb
type VBTimestamp struct {
	Vbno          uint16
	Vbuuid        uint64
	Seqno         uint64
	SnapshotStart uint64
	SnapshotEnd   uint64
	ManifestIDs   CollectionsManifestIdPair
}

var emptyVBts VBTimestamp

func (vbts *VBTimestamp) String() string {
	return fmt.Sprintf("[vbno=%v, uuid=%v, seqno=%v, sn_start=%v, sn_end=%v, srcManId=%v, tgtManId=%v]",
		vbts.Vbno, vbts.Vbuuid, vbts.Seqno, vbts.SnapshotStart, vbts.SnapshotEnd, vbts.ManifestIDs.SourceManifestId, vbts.ManifestIDs.TargetManifestId)
}

func (vbts VBTimestamp) IsEmpty() bool {
	return vbts == emptyVBts
}

func (vbts *VBTimestamp) SameAs(other *VBTimestamp) bool {
	if vbts == nil && other == nil {
		return true
	} else if vbts == nil && other != nil {
		return false
	} else if vbts != nil && other == nil {
		return false
	}

	return vbts.Vbno == other.Vbno && vbts.Vbuuid == other.Vbuuid && vbts.Seqno == other.Seqno &&
		vbts.SnapshotStart == other.SnapshotStart && vbts.SnapshotEnd == other.SnapshotEnd &&
		vbts.ManifestIDs.SourceManifestId == other.ManifestIDs.SourceManifestId &&
		vbts.ManifestIDs.TargetManifestId == other.ManifestIDs.TargetManifestId
}

func (vbts VBTimestamp) Clone() VBTimestamp {
	var clonedTs VBTimestamp
	clonedTs.Vbno = vbts.Vbno
	clonedTs.Vbuuid = vbts.Vbuuid
	clonedTs.Seqno = vbts.Seqno
	clonedTs.SnapshotStart = vbts.SnapshotStart
	clonedTs.SnapshotEnd = vbts.SnapshotEnd
	clonedTs.ManifestIDs.SourceManifestId = vbts.ManifestIDs.SourceManifestId
	clonedTs.ManifestIDs.TargetManifestId = vbts.ManifestIDs.TargetManifestId
	return clonedTs
}

type ClusterConnectionInfoProvider interface {
	MyConnectionStr() (string, error)
	// returns username, password, http auth mechanism, certificate, whether certificate contains SAN, client certificate, client key
	MyCredentials() (string, string, HttpAuthMech, []byte, bool, []byte, []byte, error)
}

type ReplicationInfo struct {
	Id        string
	StatsMap  map[string]interface{}
	ErrorList []ErrorInfo
}

type ErrorInfo struct {
	// Time is the number of nano seconds elapsed since 1/1/1970 UTC
	Time     int64
	ErrorMsg string
}

// These should be RO once they are created
// because they are potentially used as constant keys for CollectionNamespaceMapping
type CollectionNamespace struct {
	ScopeName      string
	CollectionName string
}

var DefaultCollectionNamespace = CollectionNamespace{
	ScopeName:      DefaultScopeCollectionName,
	CollectionName: DefaultScopeCollectionName,
}

// The specificStr should follow the format of: "<scope>:<collection>"
func NewCollectionNamespaceFromString(specificStr string) (CollectionNamespace, error) {
	if !CollectionNamespaceRegex.MatchString(specificStr) {
		return CollectionNamespace{}, fmt.Errorf("Invalid CollectionNamespace format")
	} else {
		names := CollectionNamespaceRegex.FindStringSubmatch(specificStr)
		if len(names) != 3 {
			return CollectionNamespace{}, fmt.Errorf("Invalid capture for CollectionNameSpaces")
		}
		return CollectionNamespace{
			ScopeName:      names[1],
			CollectionName: names[2],
		}, nil
	}
}

func (c CollectionNamespace) ToIndexString() string {
	return fmt.Sprintf("%v%v%v", c.ScopeName, ScopeCollectionDelimiter, c.CollectionName)
}

func (c *CollectionNamespace) IsDefault() bool {
	return c.ScopeName == DefaultScopeCollectionName && c.CollectionName == DefaultScopeCollectionName
}

func (c *CollectionNamespace) IsEmpty() bool {
	return c.ScopeName == "" && c.CollectionName == ""
}

func (c CollectionNamespace) LessThan(other CollectionNamespace) bool {
	if c.ScopeName == other.ScopeName {
		return c.CollectionName < other.CollectionName
	} else {
		return c.ScopeName < other.ScopeName
	}
}

func (c CollectionNamespace) IsSameAs(other CollectionNamespace) bool {
	return c.ScopeName == other.ScopeName && c.CollectionName == other.CollectionName
}

func (c CollectionNamespace) Clone() CollectionNamespace {
	return CollectionNamespace{
		ScopeName:      c.ScopeName,
		CollectionName: c.CollectionName,
	}
}

type CollectionNamespacePtrList []*CollectionNamespace

func (c CollectionNamespacePtrList) Len() int           { return len(c) }
func (c CollectionNamespacePtrList) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c CollectionNamespacePtrList) Less(i, j int) bool { return (*(c[i])).LessThan(*(c[j])) }

func SortCollectionNamespacePtrList(list CollectionNamespacePtrList) CollectionNamespacePtrList {
	sort.Sort(list)
	return list
}

type WrappedUprEvent struct {
	UprEvent     *mcc.UprEvent
	ColNamespace *CollectionNamespace
	Flags        WrappedUprEventFlag
}

type WrappedUprEventFlag uint64

const (
	WrappedUprCollectionDNE = 0x1
)

func (w WrappedUprEventFlag) IsSet() bool {
	return uint64(w) > uint64(0)
}

func (w WrappedUprEventFlag) CollectionDNE() bool {
	return w&WrappedUprCollectionDNE > 0
}

func (w *WrappedUprEventFlag) SetCollectionDNE() {
	*w |= WrappedUprCollectionDNE
}

type TargetCollectionInfo struct {
	// The manifestID used when retrying
	ManifestId uint64
	// Temporary storage space to avoid memmove
	ColIDPrefixedKey []byte
	// The len is needed because slice returned from datapool can have garbage
	ColIDPrefixedKeyLen int
}

type WrappedMCRequest struct {
	Seqno              uint64
	Req                *gomemcached.MCRequest
	Start_time         time.Time
	UniqueKey          string
	ColNamespace       *CollectionNamespace
	ColInfo            *TargetCollectionInfo
	SlicesToBeReleased [][]byte

	// If a single source mutation is translated to multiple target requests, the additional ones are listed here
	SiblingReqs []*WrappedMCRequest
}

func (req *WrappedMCRequest) ConstructUniqueKey() {
	var buffer bytes.Buffer
	buffer.Write(req.Req.Key)
	buffer.Write(req.Req.Extras[8:16])
	req.UniqueKey = buffer.String()

	if len(req.SiblingReqs) > 0 {
		for _, sibling := range req.SiblingReqs {
			if sibling != nil {
				sibling.ConstructUniqueKey()
			}
		}
	}
}

// Returns 0 if no collection is used
func (req *WrappedMCRequest) GetManifestId() uint64 {
	if req != nil && req.ColInfo != nil {
		return req.ColInfo.ManifestId
	} else {
		return 0
	}
}

type McRequestMap map[string]*WrappedMCRequest

type MCResponseMap map[string]*gomemcached.MCResponse

type MetadataChangeListener interface {
	Id() string
	Start() error
}

type ObjectWithLock struct {
	Object interface{}
	Lock   *sync.RWMutex
}

type SeqnoWithLock struct {
	seqno uint64
	lock  *sync.RWMutex
}

func NewSeqnoWithLock() *SeqnoWithLock {
	return &SeqnoWithLock{0, &sync.RWMutex{}}
}

// the following methods should be sufficient for typical get/set operations
func (seqno_obj *SeqnoWithLock) GetSeqno() uint64 {
	return seqno_obj.getSeqno(true)
}

func (seqno_obj *SeqnoWithLock) SetSeqno(seqno uint64) {
	seqno_obj.setSeqno(seqno, true)
}

// the following methods allow more than one get/set operations to be done in one transation
func (seqno_obj *SeqnoWithLock) Lock() {
	seqno_obj.lock.Lock()
}

func (seqno_obj *SeqnoWithLock) Unlock() {
	seqno_obj.lock.Unlock()
}

func (seqno_obj *SeqnoWithLock) GetSeqnoWithoutLock() uint64 {
	return seqno_obj.getSeqno(false)
}

func (seqno_obj *SeqnoWithLock) SetSeqnoWithoutLock(seqno uint64) {
	seqno_obj.setSeqno(seqno, false)
}

func (seqno_obj *SeqnoWithLock) getSeqno(lock bool) uint64 {
	if lock {
		seqno_obj.lock.RLock()
		defer seqno_obj.lock.RUnlock()
	}
	return seqno_obj.seqno
}

func (seqno_obj *SeqnoWithLock) setSeqno(seqno uint64, lock bool) {
	if lock {
		seqno_obj.lock.Lock()
		defer seqno_obj.lock.Unlock()
	}
	seqno_obj.seqno = seqno
}

// Callback function, which typically is a method in metadata service, which translates metakv call back parameters into metadata objects
// The function may do something addtional that is specific to the metadata service, e.g., caching the new metadata
type MetadataServiceCallback func(path string, value []byte, rev interface{}) error

// Callback function for the handling of metadata changed event
type MetadataChangeHandlerCallback func(metadataId string, oldMetadata interface{}, newMetadata interface{}) error

type DataObjRecycler func(topic string, dataObj *WrappedMCRequest)

type VBErrorType int

const (
	// vb error caused by source topology change
	VBErrorType_Source VBErrorType = iota
	// vb error caused by target topology change
	VBErrorType_Target VBErrorType = iota
)

type VBErrorEventAdditional struct {
	Vbno      uint16
	Error     error
	ErrorType VBErrorType
}

type ConflictResolutionMode int

const (
	CRMode_RevId  ConflictResolutionMode = iota
	CRMode_LWW    ConflictResolutionMode = iota
	CRMode_Custom ConflictResolutionMode = iota
)

// stack implementation
type Stack []interface{}

func (s *Stack) Empty() bool        { return len(*s) == 0 }
func (s *Stack) Peek() interface{}  { return (*s)[len(*s)-1] }
func (s *Stack) Push(i interface{}) { (*s) = append((*s), i) }
func (s *Stack) Pop() interface{} {
	d := (*s)[len(*s)-1]
	(*s) = (*s)[:len(*s)-1]
	return d
}

type InterfaceMap map[string]interface{}

// Shallow clone
func (in InterfaceMap) Clone() InterfaceMap {
	clonedMap := make(InterfaceMap)
	for k, v := range in {
		clonedMap[k] = v
	}
	return clonedMap
}

// Redact only works on keys
func (in InterfaceMap) Redact() InterfaceMap {
	for k, v := range in {
		if !IsStringRedacted(k) {
			in[TagUD(k)] = v
			delete(in, k)
		}
	}
	return in
}

// bucket info map
type BucketKVVbMap map[string][]uint16

// Usually, server vbucket maps are consisted of host:directPort
// To support External hosts, we need to manually look up external hosts if they exist, as well as
// the external direct port if they exist.
func (kvVbMap BucketKVVbMap) ReplaceInternalWithExternalHosts(translationMap map[string]string) {
	var keysToDelete []string
	// bucketKVVBMap's key is usually [internalHostname:internalPort]
	// The translated map will look like [internalHostname:internalPort] -> [externalHostName:externalPort/internalPort]
	for internalHostAndPort, vbSlice := range kvVbMap {
		var externalHostAndPort string
		externalHostAndPort, internalExists := translationMap[internalHostAndPort]
		if internalExists && internalHostAndPort != externalHostAndPort {
			// replace the internal key with external key and the same value
			kvVbMap[externalHostAndPort] = vbSlice
			keysToDelete = append(keysToDelete, internalHostAndPort)
		}
	}
	for _, key := range keysToDelete {
		delete(kvVbMap, key)
	}
}

// Compression Section
type CompressionType int

type UserAuthMode int

const (
	// no user auth
	UserAuthModeNone UserAuthMode = iota
	// use implicit local user auth
	UserAuthModeLocal UserAuthMode = iota
	// basic user auth using passed in username and password - so far this applies to user in remote clusters only
	UserAuthModeBasic UserAuthMode = iota
)

// http authentication mode
type HttpAuthMech int

const (
	// plain http authentication
	HttpAuthMechPlain HttpAuthMech = iota
	// scram sha authentication
	HttpAuthMechScramSha HttpAuthMech = iota
	// https authentication
	HttpAuthMechHttps HttpAuthMech = iota
)

// for logging
func (httpAuthMech HttpAuthMech) String() string {
	switch httpAuthMech {
	case HttpAuthMechPlain:
		return "Plain"
	case HttpAuthMechScramSha:
		return "ScramSha"
	case HttpAuthMechHttps:
		return "Https"
	default:
		return "Unknown"
	}
}

// StringPair holds a pair of strings
type StringPair [2]string

func (sp StringPair) GetFirstString() string {
	return sp[0]
}

func (sp StringPair) GetSecondString() string {
	return sp[1]
}

// StringPairList is used mainly to facilitate sorting
// StringPairList can be sorted by first string through sort.Sort(list)
type StringPairList []StringPair

func (spl StringPairList) Len() int           { return len(spl) }
func (spl StringPairList) Swap(i, j int)      { spl[i], spl[j] = spl[j], spl[i] }
func (spl StringPairList) Less(i, j int) bool { return spl[i][0] < spl[j][0] }

// get a list of string1
func (spl StringPairList) GetListOfFirstString() []string {
	result := make([]string, len(spl))

	for i := 0; i < len(spl); i++ {
		result[i] = spl[i][0]
	}

	return result
}

func ShuffleStringPairList(list StringPairList) {
	r := mrand.New(mrand.NewSource(time.Now().Unix()))
	// Start at the end of the slice, go backwards and scramble
	for i := len(list); i > 1; i-- {
		randIndex := r.Intn(i)
		// Swap values and continue until we're done
		if (i - 1) != randIndex {
			list[i-1], list[randIndex] = list[randIndex], list[i-1]
		}
	}
}

func DeepCopyStringPairList(list StringPairList) StringPairList {
	if list == nil {
		return nil
	}

	out := make([]StringPair, len(list))
	copy(out, list)
	return out
}

type ComponentError struct {
	ComponentId string
	Err         error
}

func FormatErrMsg(errCh chan ComponentError) map[string]error {
	// use -1 to indicate no upper limit on number of errors to return
	return FormatErrMsgWithUpperLimit(errCh, -1)
}

func FormatErrMsgWithUpperLimit(errCh chan ComponentError, maxNumberOfErrorsToTrack int) map[string]error {
	errMap := make(map[string]error)
	for {
		select {
		case componentErr := <-errCh:
			errMap[componentErr.ComponentId] = componentErr.Err
			if maxNumberOfErrorsToTrack > 0 && len(errMap) >= maxNumberOfErrorsToTrack {
				return errMap
			}
		default:
			return errMap
		}
	}
	return errMap
}

type PriorityType int

const (
	PriorityTypeHigh   PriorityType = iota
	PriorityTypeMedium PriorityType = iota
	PriorityTypeLow    PriorityType = iota
)

// priority shown on UI and rest api
const (
	PriorityHighString   string = "High"
	PriorityMediumString string = "Medium"
	PriorityLowString    string = "Low"
)

func PriorityTypeFromStr(priorityStr string) (PriorityType, error) {
	var priority PriorityType
	switch priorityStr {
	case PriorityHighString:
		priority = PriorityTypeHigh
	case PriorityMediumString:
		priority = PriorityTypeMedium
	case PriorityLowString:
		priority = PriorityTypeLow
	default:
		return -1, errors.New(fmt.Sprintf("%v is not a valid priority", priorityStr))
	}
	return priority, nil
}

func (priority PriorityType) String() string {
	switch priority {
	case PriorityTypeHigh:
		return PriorityHighString
	case PriorityTypeMedium:
		return PriorityMediumString
	case PriorityTypeLow:
		return PriorityLowString
	}
	return "Unknown"
}

type DpGetterFunc func(uint64) ([]byte, error)

// atomic boolean type which uses a uint32 integer to store boolean value
type AtomicBooleanType struct {
	val uint32
}

const (
	AtomicBooleanTrue  uint32 = 1
	AtomicBooleanFalse uint32 = 0
)

func NewAtomicBooleanType(value bool) *AtomicBooleanType {
	atomicBoolean := &AtomicBooleanType{}
	atomicBoolean.Set(value)
	return atomicBoolean
}

func (a *AtomicBooleanType) Set(value bool) {
	if value {
		a.SetTrue()
	} else {
		a.SetFalse()
	}
}

func (a *AtomicBooleanType) SetTrue() {
	atomic.StoreUint32(&a.val, AtomicBooleanTrue)
}

func (a *AtomicBooleanType) SetFalse() {
	atomic.StoreUint32(&a.val, AtomicBooleanFalse)
}

func (a *AtomicBooleanType) Get() bool {
	return atomic.LoadUint32(&a.val) == AtomicBooleanTrue
}

type FilterExpDelType int

const (
	filterExpDelStripN   = 0
	filterExpDelSkipDelN = 1
	filterExpDelSkipExpN = 2
)

var FilterExpDelNone FilterExpDelType = 0x0
var FilterExpDelStripExpiration FilterExpDelType = 1 << filterExpDelStripN  // 0x1
var FilterExpDelSkipDeletes FilterExpDelType = 1 << filterExpDelSkipDelN    // 0x2
var FilterExpDelSkipExpiration FilterExpDelType = 1 << filterExpDelSkipExpN // 0x4
var FilterExpDelAll FilterExpDelType = FilterExpDelStripExpiration | FilterExpDelSkipDeletes | FilterExpDelSkipExpiration

func (a *FilterExpDelType) IsStripExpirationSet() bool {
	return *a&FilterExpDelStripExpiration > 0
}

func (a *FilterExpDelType) IsSkipDeletesSet() bool {
	return *a&FilterExpDelSkipDeletes > 0
}

func (a *FilterExpDelType) IsSkipExpirationSet() bool {
	return *a&FilterExpDelSkipExpiration > 0
}

func (a *FilterExpDelType) SetStripExpiration(setVal bool) {
	curValue := *a&FilterExpDelStripExpiration > 0
	if curValue != setVal {
		*a ^= 1 << filterExpDelStripN
	}
}

func (a *FilterExpDelType) SetSkipDeletes(setVal bool) {
	curValue := *a&FilterExpDelSkipDeletes > 0
	if curValue != setVal {
		*a ^= 1 << filterExpDelSkipDelN
	}
}

func (a *FilterExpDelType) SetSkipExpiration(setVal bool) {
	curValue := *a&FilterExpDelSkipExpiration > 0
	if curValue != setVal {
		*a ^= 1 << filterExpDelSkipExpN
	}
}

func (a FilterExpDelType) String() string {
	return fmt.Sprintf("%d", a)
}

func (a FilterExpDelType) LogString() string {
	return fmt.Sprintf("StripTTL(%v), SkipDeletes(%v), SkipExpiration(%v)",
		a&FilterExpDelStripExpiration > 0, a&FilterExpDelSkipDeletes > 0, a&FilterExpDelSkipExpiration > 0)
}

type CollectionsMgtType int

const (
	CollectionsMappingKey = "collectionsExplicitMapping"
	CollectionsMirrorKey  = "collectionsMirroringMode"
	CollectionsMigrateKey = "collectionsMigrationMode"
)

const (
	colMgtMappingN   = 0 // Non-Set bit if implicit, Set bit if explicit
	colMgtMirroringN = 1 // Non-Set bit if mirrirng off, set bit if mirroring on
	colMgtMigrateN   = 2 // Non-set if traditional, set bit if migration mode
)

var CollectionsMgtDefault CollectionsMgtType = 0 // Implicit, no mirror, no migration
var CollectionsExplicitBit CollectionsMgtType = 1 << colMgtMappingN
var CollectionsMirroringBit CollectionsMgtType = 1 << colMgtMirroringN
var CollectionsMigrationBit CollectionsMgtType = 1 << colMgtMigrateN

var CollectionsMgtMax = CollectionsExplicitBit | CollectionsMirroringBit | CollectionsMigrationBit

func (c CollectionsMgtType) String() string {
	var output []string
	output = append(output, "ExplicitMapping:")
	output = append(output, fmt.Sprintf("%v", c.IsExplicitMapping()))
	output = append(output, "Mirroring:")
	output = append(output, fmt.Sprintf("%v", c.IsMirroringOn()))
	output = append(output, "Migration:")
	output = append(output, fmt.Sprintf("%v", c.IsMigrationOn()))
	return strings.Join(output, " ")
}

func (c *CollectionsMgtType) IsExplicitMapping() bool {
	return *c&CollectionsExplicitBit > 0
}

func (c *CollectionsMgtType) SetExplicitMapping(val bool) {
	if c.IsExplicitMapping() != val {
		*c ^= 1 << colMgtMappingN
	}
}

func (c *CollectionsMgtType) IsMirroringOn() bool {
	return *c&CollectionsMirroringBit > 0
}

func (c *CollectionsMgtType) SetMirroring(val bool) {
	if c.IsMirroringOn() != val {
		*c ^= 1 << colMgtMirroringN
	}
}

func (c *CollectionsMgtType) IsMigrationOn() bool {
	return *c&CollectionsMigrationBit > 0
}

func (c *CollectionsMgtType) SetMigration(val bool) {
	if c.IsMigrationOn() != val {
		*c ^= 1 << colMgtMigrateN
	}
}

const CollectionsMappingRulesKey = "colMappingRules"

type XattrIterator struct {
	body []byte
	// end position of xattrs
	endPos uint32
	// current cursor position
	pos uint32
}

func NewXattrIterator(body []byte) (*XattrIterator, error) {
	if len(body) < 4 {
		return nil, fmt.Errorf("body is too short to contain valid xattrs")
	}
	xattrSize := binary.BigEndian.Uint32(body[0:4])
	// Couchbase doc size is max of 20MB. Xattribute count against this limit.
	// So if total xattr size is greater than this limit, then something is wrong
	if xattrSize > MaxDocSizeByte {
		return nil, fmt.Errorf("xattrs size %v exceeds max doc size", xattrSize)
	}
	return &XattrIterator{
		body:   body,
		endPos: xattrSize + 4,
		// jump to the beginning of the first xattr
		pos: 4,
	}, nil
}

func (xi *XattrIterator) HasNext() bool {
	return xi.pos < xi.endPos
}

func (xi *XattrIterator) Next() ([]byte, []byte, error) {
	// xattrs pattern: uint32 -> key -> NUL -> value -> NUL (repeat)

	xi.pos += 4
	var separator uint32

	// Search for end of key
	for separator = xi.pos; xi.body[separator] != '\x00'; separator++ {
		if separator >= xi.endPos {
			return nil, nil, fmt.Errorf("Error parsing xattr key")
		}
	}

	key := xi.body[xi.pos:separator]

	xi.pos = separator + 1

	// Search for end of value
	for separator = xi.pos; xi.body[separator] != '\x00'; separator++ {
		if separator >= xi.endPos {
			return nil, nil, fmt.Errorf("Error parsing xattr value")
		}
	}

	value := xi.body[xi.pos:separator]

	xi.pos = separator + 1

	return key, value, nil
}

type CCRXattrFieldIterator struct {
	xattr []byte
	pos   int
}

func NewCCRXattrFieldIterator(xattr []byte) (*CCRXattrFieldIterator, error) {
	length := len(xattr)
	if xattr[0] != '{' || xattr[length-1] != '}' {
		return nil, fmt.Errorf("Invalid format for XATTR: %v", xattr)
	}
	return &CCRXattrFieldIterator{
		xattr: xattr,
		pos:   1,
	}, nil
}

func (xfi *CCRXattrFieldIterator) HasNext() bool {
	return xfi.pos < len(xfi.xattr)
}

// Expected format is: {"key":"value","key":"value","key":{...},key:{...}}
func (xfi *CCRXattrFieldIterator) Next() (key, value []byte, err error) {
	beginQuote := xfi.pos
	colon := bytes.Index(xfi.xattr[beginQuote:], []byte{':'})
	endQuote := beginQuote + colon - 1
	if colon == -1 || xfi.xattr[beginQuote] != '"' || xfi.xattr[endQuote] != '"' {
		err = fmt.Errorf("XATTR %s invalid format at pos=%v, char '%c,, endQuote =%v", xfi.xattr, xfi.pos, xfi.xattr[xfi.pos], endQuote)
		return
	}
	key = xfi.xattr[beginQuote+1 : endQuote]

	beginValue := endQuote + 2
	var endValue int
	if xfi.xattr[beginValue] == '"' {
		endValue = beginValue + 1 + bytes.Index(xfi.xattr[beginValue+1:], []byte{'"'})
		if endValue < beginValue {
			err = fmt.Errorf("XATTR %s invalid format searching for key '%s' at pos=%v, beginValue pos %v, char '%c', endValue pos %v, char '%c'", xfi.xattr, key, xfi.pos, beginValue, xfi.xattr[beginValue], endValue, xfi.xattr[endValue])
		}
		// The value is a string
		value = xfi.xattr[beginValue+1 : endValue]
	} else if xfi.xattr[beginValue] == '{' {
		endValue = beginValue + 1 + bytes.Index(xfi.xattr[beginValue+1:], []byte{'}'})
		if endValue < beginValue {
			err = fmt.Errorf("XATTR %s invalid format searching for key '%s' at pos=%v, beginValue pos %v, char '%c', endValue pos %v, char '%c'", xfi.xattr, key, xfi.pos, beginValue, xfi.xattr[beginValue], endValue, xfi.xattr[endValue])
		}
		value = xfi.xattr[beginValue : endValue+1]
	} else {
		err = fmt.Errorf("XATTR %s invalid format searching for key '%s' at pos=%v, beginValue pos %v, char '%c', endValue pos %v, char '%c'", xfi.xattr, key, xfi.pos, beginValue, xfi.xattr[beginValue], endValue, xfi.xattr[endValue])
	}
	xfi.pos = endValue + 2
	return
}

type SortedSeqnoListWithLock struct {
	seqno_list []uint64
	lock       *sync.RWMutex
}

func NewSortedSeqnoListWithLock() *SortedSeqnoListWithLock {
	return &SortedSeqnoListWithLock{make([]uint64, 0), &sync.RWMutex{}}
}

// when needToSort is true, sort the internal seqno_list before returning it
// sorting is needed only when seqno_list is not already sorted, which is the case only for sent_seqno_list
// in other words, needToSort should be set to true only when operating on sent_seqno_list
func (list_obj *SortedSeqnoListWithLock) GetSortedSeqnoList(needToSort bool) []uint64 {
	if needToSort {
		list_obj.lock.Lock()
		defer list_obj.lock.Unlock()

		SortUint64List(list_obj.seqno_list)
		return DeepCopyUint64Array(list_obj.seqno_list)
	} else {
		list_obj.lock.RLock()
		defer list_obj.lock.RUnlock()

		return DeepCopyUint64Array(list_obj.seqno_list)
	}
}

func (list_obj *SortedSeqnoListWithLock) GetLengthOfSeqnoList() int {
	list_obj.lock.RLock()
	defer list_obj.lock.RUnlock()

	return len(list_obj.seqno_list)
}

// append seqno to the end of seqno_list
func (list_obj *SortedSeqnoListWithLock) AppendSeqno(seqno uint64) {
	list_obj.lock.Lock()
	defer list_obj.lock.Unlock()
	list_obj.seqno_list = append(list_obj.seqno_list, seqno)
}

// truncate all seqnos that are no larger than passed in through_seqno
func (list_obj *SortedSeqnoListWithLock) TruncateSeqnos(through_seqno uint64) {
	list_obj.lock.Lock()
	defer list_obj.lock.Unlock()
	seqno_list := list_obj.seqno_list
	index, found := SearchUint64List(seqno_list, through_seqno)
	if found {
		list_obj.seqno_list = seqno_list[index+1:]
	} else if index > 0 {
		list_obj.seqno_list = seqno_list[index:]
	}
}

type Uleb128 []byte

type ExplicitMappingValidator struct {
	// S:C -> S:C || S:C -> nil
	oneToOneRules map[string]interface{}
	// S -> S || S -> nil
	scopeToScopeRules map[string]interface{}
}

type explicitValidatingType int

const (
	explicitRuleInvalid      explicitValidatingType = iota
	explicitRuleOneToOne     explicitValidatingType = iota
	explicitRuleScopeToScope explicitValidatingType = iota
)

func NewExplicitMappingValidator() *ExplicitMappingValidator {
	return &ExplicitMappingValidator{
		oneToOneRules:     make(map[string]interface{}),
		scopeToScopeRules: make(map[string]interface{}),
	}
}

func (e *ExplicitMappingValidator) parseRule(k string, v interface{}) explicitValidatingType {
	if k == "" {
		return explicitRuleInvalid
	}
	vStr, vIsString := v.(string)

	_, err := NewCollectionNamespaceFromString(k)
	if err == nil {
		// value must be the same or nil
		if v == nil {
			return explicitRuleOneToOne
		}
		if !vIsString {
			return explicitRuleInvalid
		}
		_, err = NewCollectionNamespaceFromString(vStr)
		if err != nil {
			return explicitRuleInvalid
		}
		return explicitRuleOneToOne
	}

	// At this point, name has to be a scope name
	matched := CollectionNameValidationRegex.MatchString(k)
	if !matched {
		return explicitRuleInvalid
	}
	if vIsString {
		matched = CollectionNameValidationRegex.MatchString(vStr)
		if !matched {
			return explicitRuleInvalid
		}
	} else if v != nil {
		// Can only be string type or nil type
		return explicitRuleInvalid
	}
	return explicitRuleScopeToScope
}

// Rules in descending priority:
// 1. S:C -> S':C'
// 2. S:C -> null
// 3. S -> S'
// 4. S -> null
//
// Given the same scope:
// 1 can coexist with 3 if C' != C
// 1 can coexist with 4
// 2 can coexist with 3
func (e *ExplicitMappingValidator) ValidateKV(k string, v interface{}) error {
	ruleType := e.parseRule(k, v)
	switch ruleType {
	case explicitRuleInvalid:
		return fmt.Errorf("invalid rule: %v:%v", k, v)
	case explicitRuleOneToOne:
		// Shouldn't have duplicated keys, but check anyway
		_, exists := e.oneToOneRules[k]
		if exists {
			return fmt.Errorf("Key already exists: %v", k)
		}
		e.oneToOneRules[k] = v

		// Check for duplicity
		submatches := CollectionNamespaceRegex.FindStringSubmatch(k)
		sourceScopeName := submatches[1]
		sourceCollectionName := submatches[2]
		//sourceNamespace, _ := NewCollectionNamespaceFromString(k)
		targetScope, exists := e.scopeToScopeRules[sourceScopeName]
		if v == nil {
			if exists && targetScope == nil {
				// S -> null already exists. S:C -> null is redundant
				return fmt.Errorf("The rule %v:%v is redundant", k, v)
			}
		} else {
			submatches2 := CollectionNamespaceRegex.FindStringSubmatch(v.(string))
			targetCollectionName := submatches2[2]
			if exists && targetScope != nil {
				if sourceCollectionName == targetCollectionName {
					// S -> S2 already exists, S:C -> S2:C is redundant
					return fmt.Errorf("The rule %v:%v is redundant", k, v)
				} else {
					// S -> S2 exists, but S:C -> S2:C2 will have higher priority
					// (S:C -> S2:C will not take place as the more specific rule takes higher precedence)
				}
			}
		}
	case explicitRuleScopeToScope:
		// Shouldn't have duplicated keys, but check anyway
		_, exists := e.scopeToScopeRules[k]
		if exists {
			return fmt.Errorf("Key already exists: %v", k)
		}
		e.scopeToScopeRules[k] = v

		// Check for duplicity
		for checkK, checkV := range e.oneToOneRules {
			submatches := CollectionNamespaceRegex.FindStringSubmatch(checkK)
			sourceScopeName := submatches[1]
			sourceCollectionName := submatches[2]
			if sourceScopeName == checkK {
				if v == nil && checkV == nil {
					// S1:C1 -> nil already exists
					return fmt.Errorf("The rule %v:%v is redundant", checkK, checkV)
				} else if v != nil && checkV != nil {
					//targetNamespace, _ := NewCollectionNamespaceFromString(checkV.(string))
					submatches2 := CollectionNamespaceRegex.FindStringSubmatch(checkV.(string))
					targetColName := submatches2[2]
					if sourceCollectionName == targetColName {
						// S1:C1 -> S2:C1 exists, and trying to enter rule S1 -> S2
						return fmt.Errorf("The rule %v:%v is redundant", checkK, checkV)
					} else {
						// S1:C1 -> S2:C3 exists, and can coexist with S1 -> S2
					}
				}
			}
		}
	default:
		panic(fmt.Sprintf("Unhandled rule type: %v", ruleType))
	}
	return nil
}

type StoppedPipelineCallback func() error
type StoppedPipelineErrCallback func(err error)

type PipelineMgrStopCbType func(string, StoppedPipelineCallback, StoppedPipelineErrCallback) error

type CustomCRMeta struct {
	senderId []byte // The source cluster that sent the document to XDCR
	cas      uint64 // The Cas in the document metadata
	cvId     []byte // _xdcr.id in XATTR
	cv       uint64 // _xdcr.cv in XATTR
	pcas     []byte // _xdcr.pc in XATTR
	mv       []byte // _xdcr.mv in XATTR
}

func NewCustomCRMeta(sourceId []byte, cas uint64, id, cvHex, pcas, mv []byte) (*CustomCRMeta, error) {
	if len(cvHex) == 0 {
		return &CustomCRMeta{
			senderId: sourceId,
			cas:      cas,
			cvId:     id,
			cv:       0,
			pcas:     pcas,
			mv:       mv,
		}, nil
	}
	cv, err := HexLittleEndianToUint64(cvHex)
	if err != nil {
		return nil, err
	}
	return &CustomCRMeta{
		senderId: sourceId,
		cas:      cas,
		cvId:     id,
		cv:       cv,
		pcas:     pcas,
		mv:       mv,
	}, nil
}

func (meta *CustomCRMeta) GetCas() uint64 {
	return meta.cas
}

func (meta *CustomCRMeta) GetCv() uint64 {
	return meta.cv
}
func (meta *CustomCRMeta) GetMv() []byte {
	return meta.mv
}

func (meta *CustomCRMeta) GetPcas() []byte {
	return meta.pcas
}

func (meta *CustomCRMeta) GetCvId() []byte {
	return meta.cvId
}

// Returns the id of the cluster that last updated this document
func (meta *CustomCRMeta) Updater() []byte {
	if meta.cas > meta.cv {
		// New change in sending cluster so that's the id
		return meta.senderId
	} else {
		return meta.cvId
	}
}

func (meta *CustomCRMeta) IsMergedDoc() bool {
	if meta.mv != nil && meta.cas == meta.cv {
		return true
	} else {
		return false
	}
}

func (meta CustomCRMeta) ContainsVersion(targetId []byte, targetCas uint64) (bool, error) {
	if meta.cas > meta.cv && bytes.Equal(meta.senderId, targetId) {
		if meta.cas >= targetCas {
			return true, nil
		} else {
			return false, nil
		}
	}
	if bytes.Equal(meta.cvId, targetId) {
		if meta.cv >= targetCas {
			return true, nil
		} else {
			return false, nil
		}
	}
	if meta.pcas != nil {
		casInVV, err := findItemInVV(meta.pcas, targetId)
		if err != nil {
			return false, err
		}
		if casInVV >= targetCas {
			return true, nil
		}
	}
	if meta.mv != nil {
		casInVV, err := findItemInVV(meta.mv, targetId)
		if err != nil {
			return false, err
		}
		if casInVV >= targetCas {
			return true, nil
		}
	}
	return false, nil
}

// This routine checks if meta contains the version vector passed in.
func (meta *CustomCRMeta) ContainsVV(vv []byte) (bool, error) {
	it, err := NewCCRXattrFieldIterator(vv)
	if err != nil {
		return false, err
	}
	for it.HasNext() {
		var key, value []byte
		key, value, err = it.Next()
		if err != nil {
			return false, err
		}
		cas, err := Base64ToUint64(value)
		if err != nil {
			return false, err
		}
		if res, err := meta.ContainsVersion(key, cas); err != nil || res == false {
			return false, err
		}
	}
	// Looped through all items in vv and meta contains all of them
	return true, nil
}

// This routine returns a version vector but in the form map[clusterId]CAS.
// If the document is a merge, the map will have a length > 1 where all source of the merge will be included. This is the MV
// If the document is not a merge, the map will have a length of 1 with the cluster ID/CAS of the last update.
func (meta *CustomCRMeta) currentVersions() (map[string]uint64, error) {
	currentVersions := make(map[string]uint64)
	if meta.cas > meta.cv {
		currentVersions[string(meta.senderId)] = meta.cas
	} else if meta.mv == nil {
		currentVersions[string(meta.cvId)] = meta.cv
	} else {
		it, err := NewCCRXattrFieldIterator(meta.mv)
		if err != nil {
			return nil, err
		}
		for it.HasNext() {
			k, v, err := it.Next()
			if err != nil {
				return nil, err
			}
			cas, err := Base64ToUint64(v)
			if err != nil {
				return nil, err
			}
			currentVersions[string(k)] = cas
		}
	}
	return currentVersions, nil
}

// This routine returns the updated PCAS in the form map[clusterId]CAS
func (meta *CustomCRMeta) previousVersions() (map[string]uint64, error) {
	if meta.cv == 0 {
		return nil, nil
	}
	previousVersions := make(map[string]uint64)
	if meta.pcas != nil {
		it, err := NewCCRXattrFieldIterator(meta.pcas)
		if err != nil {
			return nil, err
		}
		for it.HasNext() {
			k, v, err := it.Next()
			if err != nil {
				return nil, err
			}
			cas, err := Base64ToUint64(v)
			if err != nil {
				return nil, err
			}
			previousVersions[string(k)] = cas
		}
	}
	if meta.cas > meta.cv {
		if meta.mv == nil {
			// Update since cv was set. Need to add it to previous versions
			v, ok := previousVersions[string(meta.cvId)]
			if !ok || meta.cv > v {
				previousVersions[string(meta.cvId)] = meta.cv
			}
		} else {
			// Update since last merge. Need to add mv to previous versions
			it, err := NewCCRXattrFieldIterator(meta.mv)
			if err != nil {
				return nil, err
			}
			for it.HasNext() {
				k, v, err := it.Next()
				if err != nil {
					return nil, err
				}
				cas, err := Base64ToUint64(v)
				if err != nil {
					return nil, err
				}
				i, ok := previousVersions[string(k)]
				if !ok || cas > i {
					previousVersions[string(k)] = cas
				}
			}
		}
	}
	return previousVersions, nil
}

// This routine construct XATTR _xdcr{...} if there is new change (meta.cas > meta.cv)
func (meta *CustomCRMeta) ConstructCustomCRXattr(body []byte, startPos int) (pos int, err error) {
	if meta.cas <= meta.cv {
		return startPos, errors.New("ConstructCustomCRXattr cannot be called when there has been no change")
	}
	pos = startPos + 4
	copy(body[pos:], []byte(XATTR_XDCR))
	pos = pos + len(XATTR_XDCR)
	body[pos] = '\x00'
	pos++
	if len(meta.senderId) == 0 {
		return startPos, errors.New("Cannot format XATTR for custom CR because the sender ID is empty")
	}
	body, pos = WriteJsonRawMsg(body, []byte(XATTR_ID), pos, WriteJsonKey, len(XATTR_ID), true /*firstKey*/)
	body, pos = WriteJsonRawMsg(body, meta.senderId, pos, WriteJsonValue, len(meta.senderId), false /*firstKey*/)

	cvHex := Uint64ToHexLittleEndian(meta.cas)
	body, pos = WriteJsonRawMsg(body, []byte(XATTR_CV), pos, WriteJsonKey, len(XATTR_CV), false /*firstKey*/)
	body, pos = WriteJsonRawMsg(body, cvHex, pos, WriteJsonValue, len(cvHex), false /*firstKey*/)

	pcas := make(map[string][]byte)
	if meta.cvId != nil {
		pcas[string(meta.cvId)] = Uint64ToBase64(meta.cv)
	}
	if meta.pcas != nil {
		it, err := NewCCRXattrFieldIterator(meta.pcas)
		if err != nil {
			// This should never happen unless there is software bug where we formatted the _xdcr.pcas badly.
			return startPos, fmt.Errorf("Error '%v' calling NewCCRXattrFieldIterator() for pcas '%s'. Will truncate it from new pcas.", err, meta.pcas)
		} else {
			for it.HasNext() {
				key, value, err := it.Next()
				if err != nil {
					return startPos, fmt.Errorf("Error '%v' iterating through '%s'. Will truncate it from new pcas.", err, meta.pcas)
				}
				pcas[string(key)] = value
			}
		}
	}
	if meta.mv != nil {
		it, err := NewCCRXattrFieldIterator(meta.mv)
		if err != nil {
			return startPos, fmt.Errorf("Error '%v' calling NewCCRXattrFieldIterator() for mv '%s'. Will truncate it from new pcas.", err, meta.mv)
		} else {
			for it.HasNext() {
				key, value, err := it.Next()
				if err != nil {
					return startPos, fmt.Errorf("Error '%v' iterating through '%s'. Will truncate it from new pcas.", err, meta.mv)
				}
				// pcas should not have any entry that's in mv
				pcas[string(key)] = value
			}
		}
	}

	if len(pcas) > 0 {
		body, pos = WriteJsonRawMsg(body, []byte(XATTR_PCAS), pos, WriteJsonKey, len(XATTR_PCAS), false /*firstKey*/)

		first := true
		for k, v := range pcas {
			if Equals(meta.senderId, k) == false { // sender is already in _xdcr.id
				body, pos = WriteJsonRawMsg(body, []byte(k), pos, WriteJsonKey, len(k), first /*firstKey*/)
				body, pos = WriteJsonRawMsg(body, v, pos, WriteJsonValue, len(v), false /*firstKey*/)
				first = false
			}
		}
		body[pos] = '}'
		pos++
	}
	body[pos] = '}'
	pos++
	body[pos] = '\x00'
	pos++
	binary.BigEndian.PutUint32(body[startPos:startPos+4], uint32(pos-(startPos+4)))
	return pos, nil
}

// This routine combines source and target meta and puts the new MV/PCAS in the mergedMvSlice/mergedPcasSlice
//   It finds the current versions (MV) of the two documents, combine them into new MV.
//   It finds the previous versions (PCAS) of the two documents, combine them into new PCAS
//   It then remove any duplicates and format them in the provided slices
func (meta *CustomCRMeta) MergeMeta(targetMeta *CustomCRMeta, mergedMvSlice, mergedPcasSlice []byte) (mvlen int, pcaslen int, err error) {
	currentVersions, err := meta.currentVersions()
	if err != nil {
		return 0, 0, err
	}
	targetCurrentVersions, err := targetMeta.currentVersions()
	if err != nil {
		return 0, 0, err
	}
	// merge current versions
	for key, value := range targetCurrentVersions {
		v, ok := currentVersions[key]
		if !ok || value > v {
			currentVersions[key] = value
		}
	}
	previousVersions, err := meta.previousVersions()
	if err != nil {
		return 0, 0, err
	}
	targetPreviousVersions, err := targetMeta.previousVersions()
	if err != nil {
		return 0, 0, err
	}
	// merge previous versions
	for key, value := range targetPreviousVersions {
		v, ok := previousVersions[key]
		if !ok || value > v {
			previousVersions[key] = value
		}
	}
	// Remove any redundant entries
	for key, value := range currentVersions {
		v, ok := previousVersions[key]
		if ok {
			if value > v {
				delete(previousVersions, key)
			} else {
				delete(currentVersions, key)
			}
		}
	}
	// Construct MV
	firstKey := true
	for key, cas := range currentVersions {
		value := Uint64ToBase64(cas)
		mergedMvSlice, mvlen = WriteJsonRawMsg(mergedMvSlice, []byte(key), mvlen, WriteJsonKey, len(key), firstKey /*firstKey*/)
		mergedMvSlice, mvlen = WriteJsonRawMsg(mergedMvSlice, value, mvlen, WriteJsonValue, len(value), false /*firstKey*/)
		firstKey = false
	}
	if mvlen > 0 {
		// We have MV. Finish it with '}'
		mergedMvSlice[mvlen] = '}'
		mvlen++
	}
	// Construct PCAS
	firstKey = true
	for key, cas := range previousVersions {
		value := Uint64ToBase64(cas)
		mergedPcasSlice, pcaslen = WriteJsonRawMsg(mergedPcasSlice, []byte(key), pcaslen, WriteJsonKey, len(key), firstKey /*firstKey*/)
		mergedPcasSlice, pcaslen = WriteJsonRawMsg(mergedPcasSlice, value, pcaslen, WriteJsonValue, len(value), false /*firstKey*/)
		firstKey = false
	}
	if pcaslen > 0 {
		// We have PCAS. Finish it with '}
		mergedPcasSlice[pcaslen] = '}'
		pcaslen++
	}
	return mvlen, pcaslen, nil
}
func (meta *CustomCRMeta) String() string {
	return fmt.Sprintf("senderId: %s, cas: %v, cvId: %s, cv: %v, pcas: %s, mv: %s",
		meta.senderId, meta.cas, meta.cvId, meta.cv, meta.pcas, meta.mv)
}

type ConflictParams struct {
	Source         *WrappedMCRequest
	Target         *SubdocLookupResponse
	SourceId       []byte
	TargetId       []byte
	BucketName     string
	ResultNotifier MergeResultNotifier
}

type MergeInputAndResult struct {
	Input  *ConflictParams
	Result interface{}
	Err    error
}

type MergeResultNotifier interface {
	NotifyMergeResult(input *ConflictParams, mergeResult interface{}, mergeError error)
}
