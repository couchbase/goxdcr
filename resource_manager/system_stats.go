// Copyright (c) 2019 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package resource_manager

// This file retrieves cpu information using sigar library
// This impl is duplicated from indexing

//#cgo LDFLAGS: -lsigar
//#include <sigar.h>
import "C"

import (
	"errors"
	"fmt"
)

type SystemStats struct {
	handle *C.sigar_t
	pid    C.sigar_pid_t
}

//
// Open a new handle
//
func NewSystemStats() (*SystemStats, error) {

	var handle *C.sigar_t

	if err := C.sigar_open(&handle); err != C.SIGAR_OK {
		return nil, errors.New(fmt.Sprintf("Fail to open sigar.  Error code = %v", err))
	}

	h := &SystemStats{}
	h.handle = handle
	h.pid = C.sigar_pid_get(handle)

	return h, nil
}

//
// Close handle
//
func (h *SystemStats) Close() {
	C.sigar_close(h.handle)
}

//
// Get CPU percentage
//
func (h *SystemStats) ProcessCpuPercent() (C.sigar_pid_t, int64, error) {

	// Sigar returns a ratio of (system_time + user_time) / elapsed time
	var cpu C.sigar_proc_cpu_t
	if err := C.sigar_proc_cpu_get(h.handle, h.pid, &cpu); err != C.SIGAR_OK {
		return C.sigar_pid_t(0), 0, errors.New(fmt.Sprintf("Fail to get process CPU.  Err=%v", C.sigar_strerror(h.handle, err)))
	}

	return h.pid, int64(cpu.percent * 100), nil
}

// returns total cpu and idle cpu
func (h *SystemStats) OverallCpu() (int64, int64, error) {

	// Sigar returns a ratio of (system_time + user_time) / elapsed time
	var cpu C.sigar_cpu_t
	if err := C.sigar_cpu_get(h.handle, &cpu); err != C.SIGAR_OK {
		return 0, 0, errors.New(fmt.Sprintf("Fail to get CPU.  Err=%v", C.sigar_strerror(h.handle, err)))
	}

	return int64(cpu.total), int64(cpu.idle), nil
}
