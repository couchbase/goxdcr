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

func ExecWithTimeout(action Action, timeout_duration time.Duration, logger *log.CommonLogger) error {
	ret := make(chan error, 1)
	go func(finch chan error) {
		logger.Info("About to call function")
		err1 := action()
		ret <- err1
	}(ret)

	var retErr error
	timeoutticker := time.NewTicker(timeout_duration)
	defer timeoutticker.Stop()
	for {
		select {
		case retErr = <-ret:
			logger.Infof("Finish executing %v\n", action)
			return retErr
		case <-timeoutticker.C:
			retErr = &ExecutionTimeoutError{}
			logger.Infof("Executing %v timed out", action)
			logger.Info("****************************")
			return retErr
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

func IsVbInList(vbno uint16, vb_list []uint16) bool {
	for _, vb_in_list := range vb_list {
		if vb_in_list == vbno {
			return true
		}
	}

	return false
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
