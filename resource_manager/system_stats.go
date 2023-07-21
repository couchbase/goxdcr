// Copyright 2019-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package resource_manager

// This file retrieves cpu information using sigar library
// This impl is duplicated from indexing

//#cgo LDFLAGS: -lsigar
//#include <sigar.h>
//#include <sigar_control_group.h>
import "C"

import (
	"errors"
	"fmt"
	"time"
)

const SIGAR_CGROUP_SUPPORTED = 1

type SystemStats struct {
	handle  *C.sigar_t
	pid     C.sigar_pid_t
	cgState cgroupState

	lastCPU     float64
	lastCPUTime time.Time
}

type cgroupState struct {
	prevCPUUsageSec uint64
	prevTS          int64
	cgroupSupported bool
}

// SigarControlGroupInfo holds just the subset of C.sigar_control_group_info_t XDCR uses. There are
// many more fields available at time of writing.
type SigarControlGroupInfo struct {
	Supported uint8 // "1" if cgroup info is supprted, "0" otherwise
	Version   uint8 // "1" for cgroup v1, "2" for cgroup v2

	// The number of CPUs available in the cgroup (in % where 100% represents 1 full core)
	// Derived from (cpu.cfs_quota_us/cpu.cfs_period_us) or COUCHBASE_CPU_COUNT env variable
	NumCpuPrc uint16

	// Maximum memory available in the group in bytes. Derived from memory.max
	// Can be 0 if the the path /sys/fs/cgroup/memory.max contains string "max"
	MemoryMax uint64

	// Current memory usage by this cgroup in bytes. Derived from memory.usage_in_bytes
	MemoryCurrent uint64

	// UsageUsec gives the total microseconds of CPU used from sigar start across all available
	// cores, so this can increase at a rate of N times real time if there are N cores in use
	UsageUsec uint64
}

// Open a new handle
func NewSystemStats() (*SystemStats, error) {

	var handle *C.sigar_t

	if err := C.sigar_open(&handle); err != C.SIGAR_OK {
		return nil, errors.New(fmt.Sprintf("Fail to open sigar.  Error code = %v", err))
	}

	h := &SystemStats{}
	h.handle = handle
	h.pid = C.sigar_pid_get(handle)
	cgInfo := h.GetControlGroupInfo()
	h.cgState.cgroupSupported = cgInfo.Supported == SIGAR_CGROUP_SUPPORTED

	var cpu C.sigar_proc_cpu_t
	if err := C.sigar_proc_cpu_get(h.handle, h.pid, &cpu); err != C.SIGAR_OK {
		return nil, fmt.Errorf("failed to get CPU, err: %w",
			C.sigar_strerror(h.handle, err))
	}
	h.lastCPU = float64(cpu.user + cpu.sys)
	h.lastCPUTime = time.Now()

	return h, nil
}

func (h *SystemStats) IsCGroupSupported() bool {
	return h.cgState.cgroupSupported
}

// Close handle
func (h *SystemStats) Close() {
	C.sigar_close(h.handle)
}

// Get CPU percentage
func (h *SystemStats) ProcessCpuPercent() (C.sigar_pid_t, int64, error) {

	// Sigar returns a ratio of (system_time + user_time) / elapsed time
	var cpu C.sigar_proc_cpu_t
	if err := C.sigar_proc_cpu_get(h.handle, h.pid, &cpu); err != C.SIGAR_OK {
		return C.sigar_pid_t(0), 0, errors.New(fmt.Sprintf("Fail to get process CPU.  Err=%v", C.sigar_strerror(h.handle, err)))
	}

	totalCPU := float64(cpu.user + cpu.sys)
	currentTime := time.Now()
	timeDiffInMilliseconds := float64(currentTime.Sub(h.lastCPUTime).Milliseconds())
	if timeDiffInMilliseconds <= 0 {
		// Avoid divide by zero.
		timeDiffInMilliseconds = 1
	}

	cpuPercent := (totalCPU - h.lastCPU) / timeDiffInMilliseconds
	h.lastCPU = totalCPU
	h.lastCPUTime = currentTime

	return h.pid, int64(cpuPercent * 100), nil
}

// returns total cpu and idle cpu
// This not thread-safe and it is assumed to be called in a single thread only
func (h *SystemStats) OverallCpu() (int64, int64, error) {

	if h.cgState.cgroupSupported {
		return h.overallCpuCgroup()
	}

	// Sigar returns a ratio of (system_time + user_time) / elapsed time
	var cpu C.sigar_cpu_t
	if err := C.sigar_cpu_get(h.handle, &cpu); err != C.SIGAR_OK {
		return 0, 0, errors.New(fmt.Sprintf("Fail to get CPU.  Err=%v", C.sigar_strerror(h.handle, err)))
	}

	return int64(cpu.total), int64(cpu.idle), nil
}

// overallCpuCgroup is not thread-safe. Caller needs to ensure of thread-safety
func (h *SystemStats) overallCpuCgroup() (totalCpu, idleCpu int64, err error) {
	cgInfo := h.GetControlGroupInfo()
	now := (time.Now().UnixNano() + 500) / 1000 // rounded to nearest microsecond

	if h.cgState.prevTS == 0 {
		h.cgState.prevTS = now
		h.cgState.prevCPUUsageSec = cgInfo.UsageUsec
		return 0, 0, fmt.Errorf("CGroup prev timestamp not set")
	}

	// The idea is that if totalCpu == potentialCpu then it means CPU (all cores) were fully
	// occupied
	/*
		CPU usage is tracked in terms of time units. But it is not wall clock.
		If CPU usage is 0 then this metric will not advance. This measures how
		much duration CPU spent for all cores. On the other hand we have wall
		clock which ticks no matter what. So during the base.CpuCollectionInterval
		(which is every 2 sec) the max CPU usage cannot exceed this. The wall clock
		ticks for each CPU. So if there 4 cpus then total maximum cpu usage can
		be 4 * 2 sec = 8 sec.

		Example: Let's say a single CPU usage at 10:00:00 was 10 sec. At 10:00:02 CPU time used
		was 10.40 sec. So in 2 sec, CPU was busy for 0.40 sec duration. So this
		cannot exceed 2 sec, hence this potential max CPU.
	*/
	potentialMaxCpu := (now - h.cgState.prevTS) * int64(cgInfo.NumCpuPrc/100)
	totalCpu = int64(cgInfo.UsageUsec - h.cgState.prevCPUUsageSec)
	idleCpu = potentialMaxCpu - totalCpu
	if idleCpu < 0 {
		idleCpu = 0
	}

	h.cgState.prevTS = now
	h.cgState.prevCPUUsageSec = cgInfo.UsageUsec

	return
}

// GetControlGroupInfo returns the fields of C.sigar_control_group_info_t XDCR uses. These reflect
// Linux control group settings, which are used by Kubernetes to set pod memory and CPU limits.
func (h *SystemStats) GetControlGroupInfo() *SigarControlGroupInfo {
	var info C.sigar_control_group_info_t
	C.sigar_get_control_group_info(&info)

	return &SigarControlGroupInfo{
		Supported:     uint8(info.supported),
		Version:       uint8(info.version),
		NumCpuPrc:     uint16(info.num_cpu_prc),
		MemoryMax:     uint64(info.memory_max),
		MemoryCurrent: uint64(info.memory_current),
		UsageUsec:     uint64(info.usage_usec),
	}
}
