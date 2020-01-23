// Copyright (c) 2013 Couchbase, Inc.
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
	"github.com/couchbase/gomemcached"
	mrand "math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
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

// timestamp for a specific vb
type VBTimestamp struct {
	Vbno          uint16
	Vbuuid        uint64
	Seqno         uint64
	SnapshotStart uint64
	SnapshotEnd   uint64
}

func (vbts *VBTimestamp) String() string {
	return fmt.Sprintf("[vbno=%v, uuid=%v, seqno=%v, sn_start=%v, sn_end=%v]", vbts.Vbno, vbts.Vbuuid, vbts.Seqno, vbts.SnapshotStart, vbts.SnapshotEnd)
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

type WrappedMCRequest struct {
	Seqno      uint64
	Req        *gomemcached.MCRequest
	Start_time time.Time
	UniqueKey  string
}

func (req *WrappedMCRequest) ConstructUniqueKey() {
	var buffer bytes.Buffer
	buffer.Write(req.Req.Key)
	buffer.Write(req.Req.Extras[8:16])
	req.UniqueKey = buffer.String()
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
	CRMode_RevId ConflictResolutionMode = iota
	CRMode_LWW   ConflictResolutionMode = iota
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
		return nil, fmt.Errorf("xattrs size exceeds max doc size")
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
