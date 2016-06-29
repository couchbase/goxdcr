// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

// simple utility functions with minimum dependencies on other goxdcr packages
package simple_utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/log"
	"math"
	mrand "math/rand"
	"reflect"
	"sort"
	"time"
)

type ExecutionTimeoutError struct {
}

func (te *ExecutionTimeoutError) Error() string {
	return "Execution timed out"
}

var ErrorNoSourceKV = errors.New("Invalid configuration. No source kv node is found.")

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
			retErr = &ExecutionTimeoutError{}
			logger.Infof("Executing Action timed out")
			logger.Info("****************************")
			return retErr
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
			err := &ExecutionTimeoutError{}
			logger.Info("Executing Action2 timed out")
			logger.Info("****************************")
			return nil, err
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

	out := make([]uint64, 0)
	out = append(out, in...)
	return out
}

func IsJSON(in []byte) bool {
	var out interface{}
	err := json.Unmarshal(in, &out)
	return err == nil
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
