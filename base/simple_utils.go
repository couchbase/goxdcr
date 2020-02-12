// Copyright (c) 2013-2019 Couchbase, Inc.
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
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"github.com/couchbase/gojsonsm"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/log"
	"io/ioutil"
	"math"
	mrand "math/rand"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Action func() error
type Action2 func(input interface{}) (interface{}, error)

func ExecWithTimeout(action Action, timeout_duration time.Duration, logger *log.CommonLogger) error {
	ret := make(chan error, 1)
	go func(finch chan error) {
		err1 := action()
		finch <- err1
	}(ret)

	var retErr error
	timeoutticker := time.NewTicker(timeout_duration)
	defer timeoutticker.Stop()
	for {
		select {
		case retErr = <-ret:
			return retErr
		case <-timeoutticker.C:
			logger.Infof("Executing Action timed out")
			logger.Info("****************************")
			return ErrorExecutionTimedOut
		}
	}

}

func ExecWithTimeout2(action Action2, input interface{}, timeout_duration time.Duration, logger *log.CommonLogger) (interface{}, error) {
	ret := make(chan interface{}, 1)
	go func(finch chan interface{}) {
		output, err := action(input)
		outputs := []interface{}{output, err}
		finch <- outputs
	}(ret)

	var output interface{}
	var errObj interface{}
	timeoutticker := time.NewTicker(timeout_duration)
	defer timeoutticker.Stop()
	for {
		select {
		case outputs := <-ret:
			output = outputs.([]interface{})[0]
			errObj = outputs.([]interface{})[1]
			if errObj != nil {
				return output, errObj.(error)
			} else {
				return output, nil
			}
		case <-timeoutticker.C:
			logger.Info("Executing Action2 timed out")
			logger.Info("****************************")
			return nil, ErrorExecutionTimedOut
		}
	}
}

func GetVbListFromKvVbMap(kv_vb_map map[string][]uint16) []uint16 {
	vb_list := make([]uint16, 0)
	for _, kv_vb_list := range kv_vb_map {
		vb_list = append(vb_list, kv_vb_list...)
	}
	return vb_list
}

// type to facilitate the sorting of uint16 lists
type Uint16List []uint16

func (u Uint16List) Len() int           { return len(u) }
func (u Uint16List) Swap(i, j int)      { u[i], u[j] = u[j], u[i] }
func (u Uint16List) Less(i, j int) bool { return u[i] < u[j] }

func SortUint16List(list []uint16) []uint16 {
	sort.Sort(Uint16List(list))
	return list
}

func AreSortedUint16ListsTheSame(sorted_list_1, sorted_list_2 []uint16) bool {
	if len(sorted_list_1) != len(sorted_list_2) {
		return false
	}

	if len(sorted_list_1) == 0 {
		return true
	}

	isSame := true
	for i := 0; i < len(sorted_list_1); i++ {
		if sorted_list_1[i] != sorted_list_2[i] {
			isSame = false
			break
		}
	}

	return isSame
}

func IsVbInList(vbno uint16, vb_list []uint16) bool {
	for _, vb_in_list := range vb_list {
		if vb_in_list == vbno {
			return true
		}
	}

	return false
}

func SearchVBInSortedList(vbno uint16, vb_list []uint16) (int, bool) {
	index := sort.Search(len(vb_list), func(i int) bool {
		return vb_list[i] >= vbno
	})
	if index < len(vb_list) && vb_list[index] == vbno {
		return index, true
	} else {
		return index, false
	}
}

func AddVbToSortedList(vbno uint16, vb_list []uint16) []uint16 {
	index, found := SearchVBInSortedList(vbno, vb_list)
	if found {
		return vb_list
	} else {
		new_vb_list := make([]uint16, 0)
		new_vb_list = append(new_vb_list, vb_list[:index]...)
		new_vb_list = append(new_vb_list, vbno)
		new_vb_list = append(new_vb_list, vb_list[index:len(vb_list)]...)
		return new_vb_list
	}
}

// randomized ordering of elements in passed in list
func RandomizeUint16List(list []uint16) {
	length := len(list)
	for i := 0; i < length; i++ {
		j := mrand.Intn(length-i) + i
		if j != i {
			list[i], list[j] = list[j], list[i]
		}
	}

}

// "sort" needs to be true if list_1 and list_2 are not sorted 	136
func ComputeDeltaOfUint16Lists(list_1, list_2 []uint16, sort bool) ([]uint16, []uint16) {
	if sort {
		SortUint16List(list_1)
		SortUint16List(list_2)
	}

	vblist_removed := make([]uint16, 0)
	vblist_new := make([]uint16, 0)

	i := 0
	j := 0
	for {
		if i >= len(list_1) || j >= len(list_2) {
			if j < len(list_2) {
				vblist_new = append(vblist_new, list_2[j:]...)
			} else if i < len(list_1) {
				vblist_removed = append(vblist_removed, list_1[i:]...)
			}
			break
		} else {
			if list_1[i] == list_2[j] {
				// vb not changed
				i++
				j++
			} else if list_1[i] > list_2[j] {
				// vb in list_2 is new
				vblist_new = append(vblist_new, list_2[j])
				j++
			} else {
				//  vb in list_1 has been removed
				vblist_removed = append(vblist_removed, list_1[i])
				i++
			}
		}
	}

	return vblist_removed, vblist_new
}

// type to facilitate the sorting of uint64 lists
type Uint64List []uint64

func (u Uint64List) Len() int           { return len(u) }
func (u Uint64List) Swap(i, j int)      { u[i], u[j] = u[j], u[i] }
func (u Uint64List) Less(i, j int) bool { return u[i] < u[j] }

func SortUint64List(list []uint64) []uint64 {
	sort.Sort(Uint64List(list))
	return list
}

func SearchUint64List(seqno_list []uint64, seqno uint64) (int, bool) {
	index := sort.Search(len(seqno_list), func(i int) bool {
		return seqno_list[i] >= seqno
	})
	if index < len(seqno_list) && seqno_list[index] == seqno {
		return index, true
	} else {
		return index, false
	}
}

type StringList []string

func (s StringList) Len() int           { return len(s) }
func (s StringList) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s StringList) Less(i, j int) bool { return s[i] < s[j] }

func SortStringList(list []string) []string {
	sort.Sort(StringList(list))
	return list
}

func MissingValueError(param string) error {
	return errors.New(fmt.Sprintf("%v cannot be empty", param))
}

func GenericInvalidValueError(param string) error {
	return errors.New(fmt.Sprintf("%v is invalid", param))
}

func MissingParameterError(param string) error {
	return errors.New(fmt.Sprintf("%v is missing", param))
}

func MissingParameterInHttpRequestUrlError(paramName, path string) error {
	return errors.New(fmt.Sprintf("%v is missing from request url, %v.", paramName, path))
}

func MissingParameterInHttpResponseError(param string) error {
	return errors.New(fmt.Sprintf("Parameter, %v, is missing in http response.", param))
}

func IncorrectValueTypeInHttpResponseError(key string, val interface{}, expectedType string) error {
	return errors.New(fmt.Sprintf("Value, %v, for key, %v, in http response has incorrect data type. Expected type: %v. Actual type: %v", val, key, expectedType, reflect.TypeOf(val)))
}

func IncorrectValueTypeInMapError(key string, val interface{}, expectedType string) error {
	return errors.New(fmt.Sprintf("Value, %v, with key, %v, in map has incorrect data type. Expected type: %v. Actual type: %v", val, key, expectedType, reflect.TypeOf(val)))
}

func IncorrectValueTypeInHttpRequestError(key string, val interface{}, expectedType string) error {
	return errors.New(fmt.Sprintf("Value, %v, for key, %v, in http request has incorrect data type. Expected type: %v. Actual type: %v", val, key, expectedType, reflect.TypeOf(val)))
}

func IncorrectValueTypeError(expectedType string) error {
	return errors.New(fmt.Sprintf("The value must be %v", expectedType))
}

func InvalidValueError(expectedType string, minVal, maxVal interface{}) error {
	return errors.New(fmt.Sprintf("The value must be %v between %v and %v", expectedType, minVal, maxVal))
}

func InvalidPathInHttpRequestError(path string) error {
	return errors.New(fmt.Sprintf("Invalid path, %v, in http request.", path))
}

func DeepCopyUint64Array(in []uint64) []uint64 {
	if in == nil {
		return nil
	}

	out := make([]uint64, len(in))
	copy(out, in)
	return out
}

func DeepCopyUint16Array(in []uint16) []uint16 {
	if in == nil {
		return nil
	}

	out := make([]uint16, len(in))
	copy(out, in)
	return out
}

func DeepCopyByteArray(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

func DeepCopyStringArray(in []string) []string {
	if in == nil {
		return nil
	}

	out := make([]string, len(in))
	copy(out, in)
	return out
}

func DeepCopyErrorMap(inMap ErrorMap) ErrorMap {
	newMap := make(ErrorMap)
	for k, v := range inMap {
		newMap[k] = v
	}
	return newMap
}

func IsJSON(in []byte) bool {
	var out interface{}
	err := json.Unmarshal(in, &out)
	return err == nil
}

func GenerateRandomId(length, maxRetry int) (string, error) {
	numOfRetry := 0
	var err error
	for {
		rb := make([]byte, length)
		_, err := rand.Read(rb)

		if err != nil {
			if numOfRetry < maxRetry {
				numOfRetry++
			} else {
				break
			}
		} else {
			id := base64.URLEncoding.EncodeToString(rb)
			return id, nil
		}
	}

	return "", fmt.Errorf("Error generating Id after %v retries. err=%v", numOfRetry, err)

}

// translate conflict resolution type bucket metadata into ConflictResolutionMode
func GetCRModeFromConflictResolutionTypeSetting(conflictResolutionType string) ConflictResolutionMode {
	if conflictResolutionType == ConflictResolutionType_Lww {
		return CRMode_LWW
	}
	return CRMode_RevId
}

// get the subset of vbs in vbList that point to different servers in the two vb server maps
func GetDiffVBList(vbList []uint16, vbServerMap1, vbServerMap2 map[uint16]string) []uint16 {
	diffVBList := make([]uint16, 0)
	for _, vb := range vbList {
		if vbServerMap1[vb] != vbServerMap2[vb] {
			diffVBList = append(diffVBList, vb)
		}
	}

	SortUint16List(diffVBList)
	return diffVBList
}

// check if two vb server maps are the same
func AreVBServerMapsTheSame(vbServerMap1, vbServerMap2 map[uint16]string) bool {
	if len(vbServerMap1) != len(vbServerMap2) {
		return false
	}

	for vb, server := range vbServerMap1 {
		if server != vbServerMap2[vb] {
			return false
		}
	}

	return true
}

// evenly distribute load across workers
// assumes that num_of_worker <= num_of_load
// returns load_distribution [][]int, where
//     load_distribution[i][0] is the start index, inclusive, of load for ith worker
//     load_distribution[i][1] is the end index, exclusive, of load for ith worker
// note that load is zero indexed, i.e., indexed as 0, 1, .. N-1 for N loads
func BalanceLoad(num_of_worker int, num_of_load int) [][]int {
	load_distribution := make([][]int, 0)

	max_load_per_worker := int(math.Ceil(float64(num_of_load) / float64(num_of_worker)))
	num_of_worker_with_max_load := num_of_load - (max_load_per_worker-1)*num_of_worker

	index := 0
	var num_of_load_per_worker int
	for i := 0; i < num_of_worker; i++ {
		if i < num_of_worker_with_max_load {
			num_of_load_per_worker = max_load_per_worker
		} else {
			num_of_load_per_worker = max_load_per_worker - 1
		}

		load_for_worker := make([]int, 2)
		load_for_worker[0] = index
		index += num_of_load_per_worker
		load_for_worker[1] = index

		load_distribution = append(load_distribution, load_for_worker)
	}

	if index != num_of_load {
		panic(fmt.Sprintf("number of load processed %v does not match total number of load %v", index, num_of_load))
	}

	return load_distribution
}

// check if a cluster (with specified clusterCompatibility) is compatible with version
func IsClusterCompatible(clusterCompatibility int, version []int) bool {
	return clusterCompatibility >= EncodeVersionToEffectiveVersion(version)
}

// encode version into an integer
func EncodeVersionToEffectiveVersion(version []int) int {
	majorVersion := 0
	minorVersion := 0
	if len(version) > 0 {
		majorVersion = version[0]
	}
	if len(version) > 1 {
		minorVersion = version[1]
	}

	effectiveVersion := majorVersion*0x10000 + minorVersion
	return effectiveVersion
}

func ComposeUserAgentWithBucketNames(prefix, sourceBucketName, targetBucketName string) string {
	var buffer bytes.Buffer
	buffer.WriteString(prefix)
	buffer.WriteString(" SourceBucket:")
	buffer.WriteString(sourceBucketName)
	buffer.WriteString(" TargetBucket:")
	buffer.WriteString(targetBucketName)
	return buffer.String()
}

// wait till the specified time or till finish_ch is closed
func WaitForTimeoutOrFinishSignal(wait_time time.Duration, finish_ch chan bool) {
	ticker := time.NewTicker(wait_time)
	defer ticker.Stop()
	select {
	case <-finish_ch:
	case <-ticker.C:
	}
}

// check if a specified data type contains xattr
func HasXattr(dataType uint8) bool {
	return dataType&PROTOCOL_BINARY_DATATYPE_XATTR > 0
}

// flatten an array of byte array into a byte array
func FlattenBytesList(bytesList [][]byte, size int64) []byte {
	flattenedBytes := make([]byte, 0, size)
	for _, bytes := range bytesList {
		flattenedBytes = append(flattenedBytes, bytes...)
	}
	return flattenedBytes
}

// return host address in the form of hostName:port
// hostName could be
// 1. ipv4 address, which is always without brackets
// 2. ipv6 address without brackets
// 3. ipv6 address with brackets
// in case 1 and 2, net.JoinHostPort() can be called directly
// in case 3, the brackets need to be stripped before net.JoinHostPort() can be called
func GetHostAddr(hostName string, port uint16) string {
	return net.JoinHostPort(StripBracketsFromHostName(hostName), fmt.Sprintf("%v", port))
}

// extract host name from hostAddr, which is in the form of hostName:port
func GetHostName(hostAddr string) string {
	index := strings.LastIndex(hostAddr, UrlPortNumberDelimiter)
	if index < 0 {
		// host addr does not contain ":". treat host addr as host name
		return hostAddr
	}
	return hostAddr[0:index]
}

func GetPortNumber(hostAddr string) (uint16, error) {
	index := strings.LastIndex(hostAddr, UrlPortNumberDelimiter)
	if index < 0 {
		// host addr does not contain ":".
		// this could happen only in remote cluster ref creation scenario,
		// where hostAddr is specified by user and user may choose not to provide a port number
		return 0, ErrorNoPortNumber
	}

	port_str := hostAddr[index+1:]
	port, err := strconv.ParseUint(port_str, 10, 16)
	if err == nil {
		return uint16(port), nil
	} else {
		// check for the special case when hostAddr is an ipv6 address without port number, e.g., [FC00::11]
		// in this case the last ":" found before was not really a port number delimiter
		// and we need to return ErrorNoPortNumber instead of ErrorInvalidPortNumber
		if strings.HasPrefix(hostAddr, LeftBracket) && strings.HasSuffix(hostAddr, RightBracket) {
			return 0, ErrorNoPortNumber
		} else {
			return 0, ErrorInvalidPortNumber
		}
	}
}

// validate host address [provided by user at remote cluster reference creation time]
func ValidateHostAddr(hostAddr string) (string, error) {
	// validate port number
	_, err := GetPortNumber(hostAddr)
	if err != nil {
		if err == ErrorNoPortNumber {
			// this could happen if host address provided by user doesn't contain port number
			// append default port number 8091
			hostAddr = GetHostAddr(hostAddr, DefaultAdminPort)
		} else {
			return "", err
		}
	}

	// get the host name portion of the host address
	hostName := GetHostName(hostAddr)
	if strings.Contains(hostName, Ipv6AddressSeparator) {
		// host name contains ":" and has to be an ipv6 address. validate that it is enclosed in "[]"
		if !IsIpAddressEnclosedInBrackets(hostName) {
			return "", errors.New("ipv6 address needs to be enclosed in square brackets")
		}
	}

	return hostAddr, nil

}

func IsIpAddressEnclosedInBrackets(hostName string) bool {
	return strings.HasPrefix(hostName, LeftBracket) && strings.HasSuffix(hostName, RightBracket)
}

func StripBracketsFromHostName(hostName string) string {
	if !IsIpAddressEnclosedInBrackets(hostName) {
		return hostName
	}

	return hostName[len(LeftBracket) : len(hostName)-len(RightBracket)]
}

func ShuffleStringsList(list []string) {
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

func ShuffleVbList(list []uint16) {
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

func StringListContains(list []string, checkStr string) bool {
	for _, str := range list {
		if str == checkStr {
			return true
		}
	}
	return false
}

// Linearly combine two lists into one and also deduplicates duplicated entries, returns the result
func StringListsDedupAndCombine(list1 []string, list2 []string) []string {
	combineMap := make(map[string]bool)
	var combineSlice []string
	for _, element := range list1 {
		combineMap[element] = true
	}
	for _, element := range list2 {
		if _, ok := combineMap[element]; !ok {
			combineMap[element] = true
		}
	}
	for element, _ := range combineMap {
		combineSlice = append(combineSlice, element)
	}
	return combineSlice
}

// Given two lists, return a slice that contains elements from list1 that are not found in list2
func StringListsFindMissingFromFirst(list1 []string, list2 []string) []string {
	list2Map := make(map[string]bool)
	var missingSlice []string
	for _, element := range list2 {
		list2Map[element] = true
	}
	for _, element := range list1 {
		if _, ok := list2Map[element]; !ok {
			missingSlice = append(missingSlice, element)
		}
	}
	return missingSlice
}

// flatten a string array into a string. each string element in string array is separated by line breaks
func FlattenStringArray(input []string) string {
	var output string
	if len(input) > 0 {
		var buffer bytes.Buffer
		for index, str := range input {
			if index != 0 {
				buffer.WriteString("\n")
			}
			buffer.WriteString(str)
		}
		output = buffer.String()
	}
	return output
}

// Returns true iff the specific MCRequest is of a compression type that is being checked (Note: None is also a type)
func IsRequestCompressedType(req *WrappedMCRequest, checkType CompressionType) (retVal bool, err error) {
	if req == nil || req.Req == nil {
		retVal = false
		// Do not return error if item is nil, since xmem nozzle should handle it fine
		return
	}
	switch checkType {
	case CompressionTypeNone:
		if req.Req.DataType&SnappyDataType == 0 {
			retVal = true
		}
	case CompressionTypeSnappy:
		if req.Req.DataType&SnappyDataType > 0 {
			retVal = true
		}
	default:
		retVal = false
		// Only return error if user of this function fed in an invalid value
		err = ErrorInvalidType
	}
	return
}

// Merge two maps into one - not go-routine/thread safe
func MergeErrorMaps(dest ErrorMap, src ErrorMap, overwrite bool) {
	for sk, sv := range src {
		if !overwrite {
			if _, ok := dest[sk]; ok {
				continue
			}
		}
		dest[sk] = sv
	}
}

func ConcatenateStringSlices(dest []string, src []string) {
	for _, str := range src {
		dest = append(dest, str)
	}
}

// Efficiently concatenate and flatten errmap into a presentable single error string
// Note map coming in should be read-protected
func FlattenErrorMap(errMap ErrorMap) string {
	var buffer bytes.Buffer
	var first bool = true
	for k, v := range errMap {
		if !first {
			buffer.WriteByte('\n')
		} else {
			first = false
		}
		buffer.WriteString(k)
		buffer.WriteString(" : ")
		buffer.WriteString(v.Error())
	}
	return buffer.String()
}

// Map should be protected for read
func CheckErrorMapForError(errMap ErrorMap, toCheck error, exactMatch bool) bool {
	for _, v := range errMap {
		if exactMatch {
			if v == toCheck {
				return true
			}
		} else {
			if strings.Contains(v.Error(), toCheck.Error()) {
				return true
			}
		}
	}
	return false
}

func CompressionStringToCompressionTypeConverter(userInput string) (int, error) {
	// First and Last element of the CompressionTypeStrings is not to be used
	for i := 1; i < len(CompressionTypeStrings)-1; i++ {
		if i == CompressionTypeSnappy {
			// Users cannot specify Snappy
			continue
		}
		if strings.ToLower(CompressionTypeStrings[i]) == strings.ToLower(userInput) {
			return i, nil
		}
	}
	return (int)(CompressionTypeNone), ErrorCompressionUnableToConvert
}

func ConcatenateErrors(errorMap ErrorMap, incomingErrorMap ErrorMap, maxNumberOfErrors int, logger *log.CommonLogger) {
	overflowErrorMap := make(ErrorMap)
	for errorKey, err := range incomingErrorMap {
		if _, ok := errorMap[errorKey]; !ok {
			if len(errorMap) < maxNumberOfErrors {
				errorMap[errorKey] = err
			} else {
				overflowErrorMap[errorKey] = err
			}
		}
	}

	if len(overflowErrorMap) > 0 {
		logger.Warnf("Failed to add all errors to error map since the error map is full. errors discarded = %v", overflowErrorMap)
	}
}

// Given two maps, find the total number of elements in an union should these maps be combined
func GetUnionOfErrorMapsSize(map1, map2 ErrorMap) (counter int) {
	var mapToBeWalked ErrorMap
	var mapToBeChecked ErrorMap

	if len(map1) > len(map2) {
		counter = len(map1)
		mapToBeWalked = map2
		mapToBeChecked = map1
	} else {
		counter = len(map2)
		mapToBeWalked = map1
		mapToBeChecked = map2
	}

	for k, _ := range mapToBeWalked {
		if _, ok := mapToBeChecked[k]; !ok {
			counter++
		}
	}
	return
}

// Redaction User Data tag - cbcollect will hash these values externally
// Makes a copy of the data coming in
// Expensive to call, so ONLY call on DEBUG or calls that do not happen regularly
func TagUD(constData interface{}) string {
	return fmt.Sprintf("%s%v%s", UdTagBegin, constData, UdTagEnd)
}

// Modifies data coming in
func TagUDBytes(data []byte) []byte {
	if len(data) > 0 {
		data = append(data, UdTagEndBytes...)
		data = append(data, UdTagBeginBytes...)
		copy(data[len(UdTagBeginBytes):], data)
		copy(data[0:], UdTagBeginBytes)
	}
	return data
}

// During debug mode, a bunch of HTTP requests will have user identifiable information. Redact them.
func CloneAndTagHttpRequest(r *http.Request) *http.Request {
	if r != nil {
		clonedReq := &http.Request{}
		*clonedReq = *r

		// Take care of URL user if it is there
		if r.URL != nil && r.URL.User != nil {
			origUsername := r.URL.User.Username()
			origPassword, pwSet := r.URL.User.Password()
			redactedUsername := TagUD(origUsername)
			var redactedPassword string
			if pwSet {
				redactedPassword = TagUD(origPassword)
			}

			var redactedUserInfo *url.Userinfo
			if pwSet {
				redactedUserInfo = url.UserPassword(redactedUsername, redactedPassword)
			} else {
				redactedUserInfo = url.User(redactedUsername)
			}
			clonedReq.URL.User = redactedUserInfo
		}

		// Take care of Header that may have user information
		v := r.Header.Get(HttpReqUserKey)
		if len(v) > 0 {
			// First make a clone
			h2 := make(http.Header, len(r.Header))
			for k, vv := range r.Header {
				vv2 := make([]string, len(vv))
				copy(vv2, vv)
				h2[k] = vv2
			}

			valueSlice := make([]string, len(v))
			for i := 0; i < len(v); i++ {
				valueSlice[i] = TagUD(v[i])
			}
			h2[HttpReqUserKey] = valueSlice
			clonedReq.Header = h2
		} else {
			clonedReq.Header = r.Header
		}

		return clonedReq
	}
	return r
}

func IsStringRedacted(input string) bool {
	// Currently, only user tag
	return strings.HasPrefix(input, UdTagBegin) && strings.HasSuffix(input, UdTagEnd)
}

func IsByteSliceRedacted(constData []byte) bool {
	return bytes.HasPrefix(constData, UdTagBeginBytes) && bytes.HasSuffix(constData, UdTagEndBytes)
}

// A pointer function to lookup Auto with the actual compression type - right now it's Snappy
func GetCompressionType(inType int) CompressionType {
	if (CompressionType)(inType) == CompressionTypeAuto {
		return CompressionTypeSnappy
	} else {
		return (CompressionType)(inType)
	}
}

func IntMin(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func IntMax(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func Uint64ToInt64(x uint64) int64 {
	if x >= math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(x)
}

// construct vb->server map for the vbs in vbList using server->vbList map
func ConstructVbServerMap(vbList []uint16, serverVbMap map[string][]uint16) map[uint16]string {
	vbServerMap := make(map[uint16]string)
	for server, serverVbList := range serverVbMap {
		for _, vb := range serverVbList {
			if _, found := SearchVBInSortedList(vb, vbList); found {
				vbServerMap[vb] = server
			}
		}
	}
	return vbServerMap
}

func UpgradeFilter(oldFilter string) string {
	return fmt.Sprintf("%v(%v, \"%v\")", gojsonsm.FuncRegexp, ExternalKeyKeyContains, oldFilter)
}

func GoJsonsmGetFilterExprMatcher(filter string) (matcher gojsonsm.Matcher, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("Error from FilterExpressionMatcher: %v", r)
		}
	}()

	return gojsonsm.GetFilterExpressionMatcher(filter)
}

func ValidateAdvFilter(filter string) error {
	// Empty filter means to unset a filter, and that's valid
	if len(filter) == 0 {
		return nil
	}

	_, err := ValidateAndGetAdvFilter(filter)
	return err
}

func ValidateAndGetAdvFilter(filter string) (gojsonsm.Matcher, error) {
	// From XDCR's perspective, instead of calling and creating gojson filter obj, this function call provides a simple and cheap
	// way to verify whether or not the filter can potentially be valid by finding operators in the filter
	// Once it's valid, spend the resource and do the actual matcher parsing
	var operatorFound bool
	for _, operator := range gojsonsm.GojsonsmOperators {
		if strings.Contains(filter, operator) {
			operatorFound = true
			break
		}
	}
	if !operatorFound {
		if !strings.Contains(filter, " ") {
			// Having no white-space most likely means user entered a single word, and most likely meant as a key-only regex
			return nil, ErrorFilterInvalidFormat
		}
		return nil, ErrorFilterInvalidExpression
	}

	internalStr, err := ReplaceKeyWordsForExpressionWErr(filter)
	if err != nil {
		err = fmt.Errorf("Error validating advanced filter keywords: %v", err.Error())
	}
	matcher, err := GoJsonsmGetFilterExprMatcher(internalStr)
	if err != nil {
		err = fmt.Errorf("%v", err.Error())
	}
	return matcher, err
}

// Given a destination, insert the "ins" at pos
// This insert should not generate garbage unless a contiguous block of memory cannot be found
func CleanInsert(dest, ins []byte, pos int) ([]byte, error) {
	insLen := len(ins)

	if pos < 0 || pos >= len(dest) {
		return nil, ErrorInvalidInput
	}

	dest = append(dest, ins...)
	copy(dest[pos+insLen:], dest[pos:])
	copy(dest[pos:], ins[:])
	return dest, nil
}

var AddFilterKeyExtraBytes int = 6 + len(ReservedWordsMap[ExternalKeyKey])

var AddFilterXattrExtraBytes int = 4 + len(ReservedWordsMap[ExternalKeyXattr])

func appendKVWriteKeyValue(dpSlice, key, value []byte, pos, keySize, valueSize int, valueNeedsQuotes bool) ([]byte, int) {
	dpSlice, pos = WriteJsonRawMsg(dpSlice, key, pos, WriteJsonKey, keySize, pos == 0)
	valueMode := WriteJsonValue
	if !valueNeedsQuotes {
		valueMode = WriteJsonValueNoQuotes
	}
	dpSlice, pos = WriteJsonRawMsg(dpSlice, value, pos, valueMode, valueSize, pos == 0)
	// pos points to the last "}"
	return dpSlice, pos
}

func AppendSingleKVToAllocatedBody(currentValue, key, value []byte, dpGetter DpGetterFunc, toBeReleased *[][]byte, sizeToGet uint64, keySize, valueSize int, valueNeedsQuotes bool, currentValueEndBracket int) ([]byte, error, int) {
	var dpSlice []byte
	var err error
	var pos int
	if currentValueEndBracket <= 0 {
		// Since the dpSlice is retrieved from datapool, it is more than likely that it contains older data
		// If we do not know the end bracket location, looking backwards is too risky as it most likely
		// will have invalid data from previous mutations. Do not process
		err = ErrorInvalidInput
		return nil, err, pos
	}

	dpSlice, err = dpGetter(sizeToGet)
	if err != nil {
		return nil, err, -1
	}
	if toBeReleased != nil {
		*toBeReleased = append(*toBeReleased, dpSlice)
	}
	copy(dpSlice, currentValue)
	pos = currentValueEndBracket
	dpSlice, pos = appendKVWriteKeyValue(dpSlice, key, value, pos, keySize, valueSize, valueNeedsQuotes)

	return dpSlice, err, pos
}

func AddXattrToBeFilteredWithoutDP(currentValue, xAttr []byte) ([]byte, error) {
	// Always insert Xattr at the end, no need for comma at the end
	xattrBytesToBeInserted := json.RawMessage(fmt.Sprintf(",\"%v\":%v", ReservedWordsMap[ExternalKeyXattr], string(xAttr)))
	// Look for reverse pos, for the last }
	var i int
	for i = len(currentValue) - 1; currentValue[i] != '}' && i >= 0; i-- {
	}
	if i < 0 {
		return currentValue, ErrorInvalidInput
	}
	return CleanInsert(currentValue, xattrBytesToBeInserted, i)
}

func getSizeToGetFromDP(currentValue, xAttr []byte) (sizeToGet, xAttrSize int) {
	// { "bodyKey":bodyValue...
	// 	,"XattrKey":<xattr> 	<- sizeToGet
	// }
	xAttrSize = len(xAttr)
	sizeToGet = CachedInternalKeyXattrByteSize + xAttrSize + len(currentValue) + 4
	return
}

func GetLastBracketPos(slice []byte, size int) (valueSize int) {
	for valueSize = size - 1; valueSize > 0 && slice[valueSize] != '}'; valueSize-- {
	}
	if valueSize == 0 && slice[valueSize] != '}' {
		valueSize = -1
	}
	return
}

func RetrieveUprJsonAndConvert(fileName string) (*mcc.UprEvent, error) {
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	var uprEvent mcc.UprEvent
	err = json.Unmarshal(data, &uprEvent)
	if err != nil {
		return nil, err
	}

	return &uprEvent, nil
}

func ReplaceKeyWordsForExpression(expression string) string {
	str, _ := ReplaceKeyWordsForExpressionWErr(expression)
	return str
}

func ReplaceKeyWordsForExpressionWErr(expression string) (string, error) {
	err := InitPcreVars()
	if err != nil {
		return "", err
	}
	expressionBytes := []byte(expression)

	// Replace all backtick with quotes
	for k, regexIface := range ReservedWordsReplaceMap {
		expressionBytes = regexIface.ReplaceAll(expressionBytes, []byte(fmt.Sprintf("`%v`", ReservedWordsMap[k])), 0 /*flags*/)
	}

	return string(expressionBytes), nil
}

func ReplaceKeyWordsForOutput(expression string) string {
	for internal, external := range ReverseReservedWordsMap {
		expression = strings.Replace(expression, fmt.Sprintf("`%v`", internal), external, -1)
	}
	return expression
}

func FilterContainsXattrExpression(expression string) bool {
	return strings.Contains(expression, ExternalKeyXattrContains)
}

func FilterContainsKeyExpression(expression string) bool {
	return strings.Contains(expression, ExternalKeyKeyContains)
}

// Checks for at least one of the valid key expression connected by one or more valid key expression connected by AND or OR
// Logic explained:
// Either a single instance that starts and ends with one of: "REGEXP_CONTAINS(key, \".*\")" OR "key op \"alphanumeric\"*"
// Note that escaped " can be replaced with '
// OR
// One single instance  instance of the above (that does not start and end) followed by one or more instances of (AND|OR [statement above])
// A = {NOT }* Key Op "Value"
// B = {NOT }* REGEXP_CONTAINS(Key, "regex")
// ^((A|B) ((AND|OR) (A|B))*)$
const compareOp = "=|==|!=|<>|>|>=|<|<="

// Do not allow single or double quotes to be part of the regex set
const regexValidCharSet = `[^'"]`

var singleAwesomeKeyOnlyRegex *regexp.Regexp = regexp.MustCompile(fmt.Sprintf("^(((((NOT *)*%v *(%v) *(\"|')[a-zA-Z0-9_-]*(\"|')) *|((NOT *)*REGEXP_CONTAINS\\( *%v *, *(\"|')%v+(\"|') *\\)) *))((AND|OR) *(((NOT *)*%v *(%v) *(\"|')[a-zA-Z0-9_-]*(\"|')) *|((NOT *)*REGEXP_CONTAINS\\( *%v *, *(\"|')%v+(\"|') *\\)) *))*)$", ExternalKeyKey, compareOp, ExternalKeyKey, regexValidCharSet, ExternalKeyKey, compareOp, ExternalKeyKey, regexValidCharSet))

// Almost the same as key but replace it with xattr
// For xattr, instead of matching META().xattr we need to match it to "META().xattrs." + "[identifier]*"
var singleAwesomeXattrOnlyRegex *regexp.Regexp = regexp.MustCompile(fmt.Sprintf("^(((((NOT *)*%v[.][.a-zA-Z0-9_-]+ *(%v) *(\"|')[a-zA-Z0-9_-]*(\"|')) *|((NOT *)*REGEXP_CONTAINS\\( *%v[.][a-zA-Z0-9_-]+ *, *(\"|')%v+(\"|') *\\)) *))((AND|OR) *(((NOT *)*%v[.][.a-zA-Z0-9_-]+ *(%v) *(\"|')[a-zA-Z0-9_-]*(\"|')) *|((NOT *)*REGEXP_CONTAINS\\( *%v[.][.a-zA-Z0-9_-]+ *, *(\"|')%v+(\"|') *\\)) *))*)$", ExternalKeyXattr, compareOp, ExternalKeyXattr, regexValidCharSet, ExternalKeyXattr, compareOp, ExternalKeyXattr, regexValidCharSet))

// NOTE - takes in user entered META().id as key
func FilterOnlyContainsKeyExpression(expression string) bool {
	// Valid key operations only are:
	// 1. META().id {=/</<=/>/>=} somethingExact
	// 2. NOT 1.
	// 3. REGEXP_CONTAINS(META().id, "regex")
	// 4. NOT 4.
	// 5. Any combination of the above connected with AND or OR
	// If any parenthesis are used, then too bad...
	return singleAwesomeKeyOnlyRegex.MatchString(expression)
}

// Xattr counterpart of the key above
func FilterOnlyContainsXattrExpression(expression string) bool {
	return singleAwesomeXattrOnlyRegex.MatchString(expression)
}

func InitPcreVars() (err error) {
	ReservedWordsReplaceMapOnce.Do(func() {
		ReservedWordsReplaceMap = make(map[string]PcreWrapperInterface)

		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("Error from PCRE: %v", r)
			}
		}()

		var externalKeyKeyReplace PcreWrapperInterface
		externalKeyKeyReplace, err = MakePcreRegex(fmt.Sprintf("(?<!`)%v(?!`)", ExternalKeyKey))
		if err == nil {
			ReservedWordsReplaceMap[ExternalKeyKey] = externalKeyKeyReplace
		}

		var externalXattrReplace PcreWrapperInterface
		externalXattrReplace, err = MakePcreRegex(fmt.Sprintf("(?<!`)%v(?!`)", ExternalKeyXattr))
		if err == nil {
			ReservedWordsReplaceMap[ExternalKeyXattr] = externalXattrReplace
		}
	})
	return
}

// parse stats value from stats map
func ParseStats(statsMap *expvar.Map, statsName string) (int64, error) {
	statsVal := statsMap.Get(statsName)
	if statsVal == nil {
		return 0, fmt.Errorf("Cannot find value for stats %v", statsName)
	}
	return strconv.ParseInt(statsVal.String(), ParseIntBase, ParseIntBitSize)
}

func FilterErrorIsRecoverable(err error) bool {
	return err == ErrorCompressionUnableToInflate
}

type WriteJsonRawMsgType int

const (
	WriteJsonKey           WriteJsonRawMsgType = iota
	WriteJsonValue         WriteJsonRawMsgType = iota
	WriteJsonValueNoQuotes WriteJsonRawMsgType = iota
)

// Given a correctly allocatedBytes slice that can contain the whole rawJSON message, write the given information without
// the use of any data structures on the heap
// Input:
// 	 allocatedBytes - finalized byte slice that will contain the specific marshalled JSON
//   bytesToWrite - This can be either key or value in []byte form
//   mode - mode above
//   pos - current position to continue the write
//   size - In cases where bytesToWrite originated from dataPool, the length may not reflect the actual
//			length of the data (i.e. string). Use this field to avoid calling len and avoid out of bounds error
//   isFirstKey - if the key is the first one, then add an open bracket. Otherwise, convert prev } to a comma for appending a new key
// Returns:
// 	 original byte slice reference
// 	 updated position
func WriteJsonRawMsg(allocatedBytes, bytesToWrite []byte, pos int, mode WriteJsonRawMsgType, size int, isFirstKey bool) ([]byte, int) {
	if mode == WriteJsonKey {
		if isFirstKey {
			// Need to do an open bracket
			allocatedBytes[pos] = '{'
			pos++
		} else {
			allocatedBytes[pos] = ','
			pos++
		}
		allocatedBytes[pos] = '"'
		pos++
		copy(allocatedBytes[pos:], bytesToWrite[:])
		pos += size
		allocatedBytes[pos] = '"'
		pos++
		allocatedBytes[pos] = ':'
		pos++
	} else {
		if mode == WriteJsonValue {
			allocatedBytes[pos] = '"'
			pos++
		}
		copy(allocatedBytes[pos:], bytesToWrite[:])
		pos += size
		if mode == WriteJsonValue {
			allocatedBytes[pos] = '"'
			pos++
		}
		allocatedBytes[pos] = '}'
		// Do not increment pos here because if there is a next key, it will replace this } with a ,
	}
	return allocatedBytes, pos
}

// get number of vbuckets in a cluster based on kv vb map
func GetNumberOfVbs(kvVBMap map[string][]uint16) int {
	numberOfVBs := 0
	for _, vbList := range kvVBMap {
		numberOfVBs += len(vbList)
	}
	return numberOfVBs
}

// check whether source byte array contains the same string as target string
// this impl avoids converting byte array to string
func Equals(source []byte, target string) bool {
	if len(source) != len(target) {
		return false
	}
	for i := 0; i < len(target); i++ {
		if target[i] != source[i] {
			return false
		}
	}
	return true
}

func MatchWrapper(matcher gojsonsm.Matcher, slice []byte) (matched bool, status int, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("Error from matcher: %v\n", r)
		}
	}()
	return matcher.MatchWithStatus(slice)
}

func GetClusterCompatibilityFromNodeList(nodeList []interface{}) (int, error) {
	if len(nodeList) > 0 {
		firstNode, ok := nodeList[0].(map[string]interface{})
		if !ok {
			return 0, fmt.Errorf("node info is of wrong type. node info=%v", nodeList[0])
		}
		clusterCompatibility, ok := firstNode[ClusterCompatibilityKey]
		if !ok {
			return 0, fmt.Errorf("Can't get cluster compatibility info. node info=%v\n If replicating to ElasticSearch node, use XDCR v1.", nodeList[0])
		}
		clusterCompatibilityFloat, ok := clusterCompatibility.(float64)
		if !ok {
			return 0, fmt.Errorf("cluster compatibility is not of int type. type=%v", reflect.TypeOf(clusterCompatibility))
		}
		return int(clusterCompatibilityFloat), nil
	}
	return 0, fmt.Errorf("node list is empty")
}

func GetNodeListFromInfoMap(infoMap map[string]interface{}, logger *log.CommonLogger) ([]interface{}, error) {
	// get node list from the map
	nodes, ok := infoMap[NodesKey]
	if !ok {
		errMsg := fmt.Sprintf("info map contains no nodes. info map=%v", infoMap)
		logger.Error(errMsg)
		return nil, errors.New(errMsg)
	}

	nodeList, ok := nodes.([]interface{})
	if !ok {
		errMsg := fmt.Sprintf("nodes is not of list type. type of nodes=%v", reflect.TypeOf(nodes))
		logger.Error(errMsg)
		return nil, errors.New(errMsg)
	}

	// only return the nodes that are active
	activeNodeList := make([]interface{}, 0)
	for _, node := range nodeList {
		nodeInfoMap, ok := node.(map[string]interface{})
		if !ok {
			errMsg := fmt.Sprintf("node info is not of map type. type=%v", reflect.TypeOf(node))
			logger.Error(errMsg)
			return nil, errors.New(errMsg)
		}
		clusterMembershipObj, ok := nodeInfoMap[ClusterMembershipKey]
		if !ok {
			// this could happen when target is elastic search cluster (or maybe very old couchbase cluster?)
			// consider the node to be "active" to be safe
			errMsg := fmt.Sprintf("node info map does not contain cluster membership. node info map=%v ", nodeInfoMap)
			logger.Debug(errMsg)
			activeNodeList = append(activeNodeList, node)
			continue
		}
		clusterMembership, ok := clusterMembershipObj.(string)
		if !ok {
			// play safe and return the node as active
			errMsg := fmt.Sprintf("cluster membership is not string type. type=%v ", reflect.TypeOf(clusterMembershipObj))
			logger.Warn(errMsg)
			activeNodeList = append(activeNodeList, node)
			continue
		}
		if clusterMembership == "" || clusterMembership == ClusterMembership_Active {
			activeNodeList = append(activeNodeList, node)
		}
	}

	return activeNodeList, nil
}
