/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package evaluator

//#cgo LDFLAGS: -lsigar
//#include <sigar.h>
import "C"
import "fmt"
import "errors"

// This and the following sigar routines are for tests.
// The have to be outside of test files since cgo in test is not supported
var sigarHandle *C.sigar_t

func InitSigar() error {
	if err := C.sigar_open(&sigarHandle); err != C.SIGAR_OK {
		return errors.New(fmt.Sprintf("Fail to open sigar.  Error code = %v", err))
	}
	return nil
}

func GetProcessThread() (int, error) {
	var state C.sigar_proc_state_t

	if err := C.sigar_proc_state_get(sigarHandle, C.sigar_pid_get(sigarHandle), &state); err != C.SIGAR_OK {
		return int(0), fmt.Errorf("Fail to get num threads.  Err=%v", C.sigar_strerror(sigarHandle, err))
	}
	return int(state.threads), nil
}

type ProcMemStats struct {
	ProcMemSize uint64
	ProcMemRSS  uint64
}

func GetProcessRSS() (ProcMemStats, error) {
	var stats ProcMemStats
	var v C.sigar_proc_mem_t

	if C.sigar_proc_mem_get(sigarHandle,
		C.sigar_pid_get(sigarHandle), &v) != C.SIGAR_OK {
		return stats, fmt.Errorf("Unable to fetch process memory")
	}
	stats.ProcMemSize = uint64(v.size)
	stats.ProcMemRSS = uint64(v.resident)
	return stats, nil
}

func GetProcessCpu() (float64, error) {

	// Sigar returns a ratio of (system_time + user_time) / elapsed time
	var cpu C.sigar_proc_cpu_t
	if err := C.sigar_proc_cpu_get(sigarHandle, C.sigar_pid_get(sigarHandle), &cpu); err != C.SIGAR_OK {
		return float64(0), fmt.Errorf("Fail to get CPU.  Err=%v", C.sigar_strerror(sigarHandle, err))
	}

	return float64(cpu.percent), nil
}
