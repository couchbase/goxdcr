// Copyright 2019-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package resource_manager

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/v8/base"
	baseclog "github.com/couchbase/goxdcr/v8/base/conflictlog"
	"github.com/couchbase/goxdcr/v8/common"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/parts"
	"github.com/couchbase/goxdcr/v8/pipeline"
	"github.com/couchbase/goxdcr/v8/pipeline_manager"
	"github.com/couchbase/goxdcr/v8/service_def"
	"github.com/couchbase/goxdcr/v8/service_def/throttlerSvc"
	utilities "github.com/couchbase/goxdcr/v8/utils"
	"github.com/rcrowley/go-metrics"
)

var pipelineNotRunning = fmt.Errorf("pipeline is not running yet")

// runtime stats collected from active replications
type ReplStats struct {
	changesLeft         int64
	docsReceivedFromDcp int64
	docsRepQueue        int64
	clogDocsWritten     int64
	clogQueueSize       int64
	// timestamp that the stats is updated
	timestamp int64
	// stats derived from other stats
	throughput int64
}

type ThrottlerCalibrationAction int

const (
	// no op. keep current throttler calibration option
	ThrottlerCalibrationActionNone ThrottlerCalibrationAction = iota
	// enable thottler calibration
	ThrottlerCalibrationActionEnable
	// disable thottler calibration
	ThrottlerCalibrationActionDisable
)

func (tca ThrottlerCalibrationAction) String() string {
	switch tca {
	case ThrottlerCalibrationActionNone:
		return "None"
	case ThrottlerCalibrationActionEnable:
		return "Enable"
	case ThrottlerCalibrationActionDisable:
		return "Disable"
	default:
		return "Unknown"
	}
}

type DcpPriorityAction int

const (
	// no op
	DcpPriorityActionNone DcpPriorityAction = iota
	// set dcp priorities to high/low
	DcpPriorityActionSet
	// reset dcp priorities to med
	DcpPriorityActionReset
)

func (da DcpPriorityAction) String() string {
	switch da {
	case DcpPriorityActionNone:
		return "None"
	case DcpPriorityActionSet:
		return "Set"
	case DcpPriorityActionReset:
		return "Reset"
	default:
		return "Unknown"
	}
}

// state of resource manager, which changes at each management interval
type State struct {
	// goxdcr cpu usage percentage
	cpu int64
	// total cpu of the entire machine
	totalCpu int64
	// idle cpu of the entire machine
	idleCpu int64
	// throughput limit for low priority replications
	throughputLimit int64
	// tokens given to high priority replications
	highTokens int64
	// conflictLog tokens
	conflictLogTokens int64
	// max high tokens that can be reassigned to low priority replications
	maxReassignableTokens int64
	// throughput of all replications
	overallThroughput int64
	// throughput of high priority replications
	// not used for control, just for informational purpose
	highThroughput int64
	// max throughput that system can sustain
	maxThroughput int64
	// throughput considering all reaplications needed to satisfy QOS
	totalThroughputNeeded int64
	// throughput needed to satisfy QOS for high priority replications
	throughputNeededByHighRepl int64
	// whether high priority replications exist
	highPriorityReplExist bool
	// whether low priority replications exist
	lowPriorityReplExist bool
	// whether high priority replications with backlog exist
	backlogReplExist bool

	// indicator whether conflict logging is enabled or not
	isConflictLoggingEnabled bool

	// runtime stats of active replications
	replStatsMap map[string]*ReplStats

	throttlerCalibrationAction ThrottlerCalibrationAction
	dcpPriorityAction          DcpPriorityAction
}

func newState() *State {
	return &State{
		replStatsMap: make(map[string]*ReplStats),
	}
}

func (s *State) String() string {
	var buffer bytes.Buffer
	buffer.WriteString("isClogEnabled: ")
	buffer.WriteString(strconv.FormatBool(s.isConflictLoggingEnabled))
	buffer.WriteString(" overallTP: ")
	buffer.WriteString(strconv.FormatInt(s.overallThroughput, base.ParseIntBase))
	buffer.WriteString(" highTP: ")
	buffer.WriteString(strconv.FormatInt(s.highThroughput, base.ParseIntBase))
	buffer.WriteString(" highExist: ")
	buffer.WriteString(strconv.FormatBool(s.highPriorityReplExist))
	buffer.WriteString(" lowExist: ")
	buffer.WriteString(strconv.FormatBool(s.lowPriorityReplExist))
	buffer.WriteString(" backlogExist: ")
	buffer.WriteString(strconv.FormatBool(s.backlogReplExist))
	buffer.WriteString(" maxTP: ")
	buffer.WriteString(strconv.FormatInt(s.maxThroughput, base.ParseIntBase))
	buffer.WriteString(" highTPNeeded: ")
	buffer.WriteString(strconv.FormatInt(s.throughputNeededByHighRepl, base.ParseIntBase))
	buffer.WriteString(" highTokens: ")
	buffer.WriteString(strconv.FormatInt(s.highTokens, base.ParseIntBase))
	buffer.WriteString(" maxTokens: ")
	buffer.WriteString(strconv.FormatInt(s.maxReassignableTokens, base.ParseIntBase))
	buffer.WriteString(" lowTPLimit: ")
	buffer.WriteString(fmt.Sprintf("%v", s.throughputLimit))
	buffer.WriteString(" calibration: ")
	buffer.WriteString(s.throttlerCalibrationAction.String())
	buffer.WriteString(" dcpAction: ")
	buffer.WriteString(s.dcpPriorityAction.String())
	buffer.WriteString(" processCpu: ")
	buffer.WriteString(fmt.Sprintf("%v", s.cpu))
	buffer.WriteString(" idleCpu: ")
	if s.totalCpu != 0 {
		buffer.WriteString(fmt.Sprintf("%v", s.idleCpu*100/s.totalCpu))
	} else {
		buffer.WriteString("0")
	}
	return buffer.String()
}

type ResourceManager struct {
	pipelineMgr            pipeline_manager.PipelineMgrIface
	repl_spec_svc          service_def.ReplicationSpecSvc
	topoSvc                service_def.XDCRCompTopologySvc
	replStatsGetter        ReplStatsGetter
	throughputThrottlerSvc throttlerSvc.ThroughputThrottlerSvc
	logger                 *log.CommonLogger
	utils                  utilities.UtilsIface
	waitGrp                sync.WaitGroup
	finch                  chan bool

	// count of consecutive terms where there has been backlog
	backlogCount uint32
	// count of consecutive terms where there has been no backlog
	noBacklogCount uint32

	// count of consecutive terms where cpu has not been maxed out
	cpuNotMaxedCount uint32
	// boolean indicating whether we are currently in extra quota period
	// This is activated when CPU hasn't maxed out for base.MaxCountCpuNotMaxed times
	// to enable the system to push the limits a bit more
	inExtraQuotaPeriod *base.AtomicBooleanType

	// if overall throughput starts to drop in extra quota period, this captures the throughput before drop
	throughputBeforeDrop int64
	// count of consecutive terms where overall throughput stays below throughputBeforeDrop
	throughputDropCount uint32

	// replications with ongoing flags set
	ongoingReplMap map[string]bool
	// replications with dcp priorities set
	replDcpPriorityMap map[string]mcc.PriorityType
	mapLock            sync.RWMutex

	// state in previous resource management interval
	// it will be used to compute the state in the next resource management interval
	previousState *State
	stateLock     sync.RWMutex

	systemStats unsafe.Pointer //*SystemStats

	// max cpu usage as a percentage, as defined by goMaxProcs
	maxCpu int64
	cpu    int64
	// total cpu of the entire machine
	totalCpu int64
	// accumulative total cpu of the entire machine
	accumulativeTotalCpu int64
	// idle cpu of the entire machine
	idleCpu int64
	// previous accumulative idle cpu of the entire machine
	accumulativeIdleCpu int64

	// historical samples of overall throughputs, which can be used as an estimate of max throughput system can sustain
	overallThroughputSamples metrics.Sample
	// historical samples of high priority replication throughputs
	highThroughputSamples metrics.Sample

	backfillReplSvc service_def.BackfillReplSvc

	managedResourceOnceSpecMap map[string]*metadata.GenericSpecification

	isKVNode uint32

	// tracks if throttler limits have been reset or not
	resettedThrottler bool
}

type ResourceMgrIface interface {
	Start() error
	Stop() error
	GetThroughputThrottler() throttlerSvc.ThroughputThrottlerSvc
	IsReplHighPriority(replId string, priority base.PriorityType) bool
	HandlePipelineDeletion(replId string)
	HandleGoMaxProcsChange(goMaxProcs int)
}

type ReplStatsGetter interface {
	CollectReplStats() map[*metadata.GenericSpecification]*ReplStats
}

func NewResourceManager(pipelineMgr pipeline_manager.PipelineMgrIface, repl_spec_svc service_def.ReplicationSpecSvc, xdcr_topology_svc service_def.XDCRCompTopologySvc,
	throughput_throttler_svc throttlerSvc.ThroughputThrottlerSvc,
	logger_context *log.LoggerContext, backfillReplSvc service_def.BackfillReplSvc) *ResourceManager {

	resourceMgrRetVar := &ResourceManager{
		pipelineMgr:                pipelineMgr,
		repl_spec_svc:              repl_spec_svc,
		topoSvc:                    xdcr_topology_svc,
		logger:                     log.NewLogger(base.ResourceMgrKey, logger_context),
		finch:                      make(chan bool),
		ongoingReplMap:             make(map[string]bool),
		replDcpPriorityMap:         make(map[string]mcc.PriorityType),
		throughputThrottlerSvc:     throughput_throttler_svc,
		maxCpu:                     int64(base.DefaultGoMaxProcs * 100),
		overallThroughputSamples:   metrics.NewExpDecaySample(base.ThroughputSampleSize, float64(base.ThroughputSampleAlpha)/1000),
		highThroughputSamples:      metrics.NewExpDecaySample(base.ThroughputSampleSize, float64(base.ThroughputSampleAlpha)/1000),
		accumulativeTotalCpu:       -1,
		accumulativeIdleCpu:        -1,
		inExtraQuotaPeriod:         &base.AtomicBooleanType{},
		backfillReplSvc:            backfillReplSvc,
		managedResourceOnceSpecMap: make(map[string]*metadata.GenericSpecification),
	}

	resourceMgrRetVar.replStatsGetter = resourceMgrRetVar

	resourceMgrRetVar.logger.Info("Resource Manager is initialized")

	return resourceMgrRetVar
}

// SetReplStatsGetter sets the stats getter.
// Note: this was added mainly for unit testing
func (rm *ResourceManager) SetReplStatsGetter(getter ReplStatsGetter) {
	rm.replStatsGetter = getter
}

func (rm *ResourceManager) Start() error {
	rm.logger.Infof("%v starting ....\n", base.ResourceMgrKey)
	defer rm.logger.Infof("%v started\n", base.ResourceMgrKey)

	// this could take a while when ns_server starts up, run in bg
	go rm.checkForKVService()

	// ignore error
	rm.getSystemStats()

	// this does not return error as of now
	rm.throughputThrottlerSvc.Start()

	rm.waitGrp.Add(1)
	go rm.collectCpuUsage()

	rm.waitGrp.Add(1)
	go rm.manageResources()

	rm.waitGrp.Add(1)
	go rm.logStats()

	return nil
}

func (rm *ResourceManager) checkForKVService() {
	// When a node first starts up and before it is a "cluster" IsKVNode() will return 404
	// XDCR must retry until it gets a successful lookup of services
	for {
		isKVNode, err := rm.topoSvc.IsKVNode()
		if err != nil {
			time.Sleep(base.ResourceMgrKVDetectionRetryInterval)
		} else {
			if isKVNode {
				atomic.StoreUint32(&rm.isKVNode, 1)
			}
			rm.logger.Infof("Finished retrieving node's information - isKVNode: %v", isKVNode)
			return
		}
	}
}

func (rm *ResourceManager) Stop() error {
	rm.logger.Infof("%v stopping ....\n", base.ResourceMgrKey)
	defer rm.logger.Infof("%v stopped\n", base.ResourceMgrKey)

	close(rm.finch)
	rm.waitGrp.Wait()

	rm.closeSystemStats()

	err := rm.throughputThrottlerSvc.Stop()
	if err != nil {
		rm.logger.Errorf("%v Error stopping throughput throttler service. err=%v\n", base.ResourceMgrKey, err)
	}
	return err
}

func (rm *ResourceManager) GetThroughputThrottler() throttlerSvc.ThroughputThrottlerSvc {
	return rm.throughputThrottlerSvc
}

func (rm *ResourceManager) IsReplHighPriority(replId string, priority base.PriorityType) bool {
	return rm.isReplHighPriority(replId, priority, true)
}

func (rm *ResourceManager) isReplHighPriority(replId string, priority base.PriorityType, lock bool) bool {
	switch priority {
	case base.PriorityTypeHigh:
		return true
	case base.PriorityTypeLow:
		return false
	case base.PriorityTypeMedium:
		return rm.isReplOngoing(replId, lock)
	}
	// should never get here
	return false
}

func (rm *ResourceManager) HandlePipelineDeletion(replId string) {
	rm.mapLock.Lock()
	defer rm.mapLock.Unlock()
	// without this obselete entries may cause issues for recreated replications
	delete(rm.ongoingReplMap, replId)
	delete(rm.replDcpPriorityMap, replId)
}

func (rm *ResourceManager) HandleGoMaxProcsChange(goMaxProcs int) {
	atomic.StoreInt64(&rm.maxCpu, int64(goMaxProcs*100))
}

func (rm *ResourceManager) getMaxCpu() int64 {
	return atomic.LoadInt64(&rm.maxCpu)
}

func (rm *ResourceManager) getCpu() int64 {
	return atomic.LoadInt64(&rm.cpu)
}

func (rm *ResourceManager) setCpu(cpu int64) {
	atomic.StoreInt64(&rm.cpu, cpu)
}

func (rm *ResourceManager) getTotalCpu() int64 {
	return atomic.LoadInt64(&rm.totalCpu)
}

func (rm *ResourceManager) setTotalCpu(accumulativeTotalCpu int64) {
	if accumulativeTotalCpu < 0 {
		// did not get valid value
		atomic.StoreInt64(&rm.totalCpu, -1)
		// leave rm.accumulativeTotalCpu alone
		return
	}

	previousAccumulativeTotalCpu := atomic.LoadInt64(&rm.accumulativeTotalCpu)
	if previousAccumulativeTotalCpu < 0 {
		// cannot compute totalCpu without previousAccumulativeTotalCpu
		atomic.StoreInt64(&rm.totalCpu, -1)
	} else {
		atomic.StoreInt64(&rm.totalCpu, accumulativeTotalCpu-previousAccumulativeTotalCpu)
	}
	atomic.StoreInt64(&rm.accumulativeTotalCpu, accumulativeTotalCpu)
}

func (rm *ResourceManager) getIdleCpu() int64 {
	return atomic.LoadInt64(&rm.idleCpu)
}

func (rm *ResourceManager) setIdleCpu(accumulativeIdleCpu int64) {
	if accumulativeIdleCpu < 0 {
		// did not get valid value
		atomic.StoreInt64(&rm.idleCpu, -1)
		// leave rm.accumulativeIdleCpu alone
		return
	}

	previousAccumulativeIdleCpu := atomic.LoadInt64(&rm.accumulativeIdleCpu)
	if previousAccumulativeIdleCpu < 0 {
		// cannot compute idleCpu without previousAccumulativeIdleCpu
		atomic.StoreInt64(&rm.idleCpu, -1)
	} else {
		atomic.StoreInt64(&rm.idleCpu, accumulativeIdleCpu-previousAccumulativeIdleCpu)
	}
	atomic.StoreInt64(&rm.accumulativeIdleCpu, accumulativeIdleCpu)
}

func (rm *ResourceManager) cpuMaxedout(previousState *State, state *State) bool {
	return rm.processCpuMaxedout(previousState, state) || rm.overallCpuMaxedout(previousState, state)
}

// returns whether goxdcr process cpu has been maxed out
// if state.cpu has value of -1 because of cpu collection failure, check for previousState.cpu instead
// if previousState is nil, or previousState.cpu is also -1, then this method returns false
// in such cases we are effectively reverting back to the algorithm where cpu was not a factor
func (rm *ResourceManager) processCpuMaxedout(previousState *State, state *State) bool {
	cpu := state.cpu
	if cpu < 0 && previousState != nil {
		cpu = previousState.cpu
	}
	return cpu >= rm.getMaxCpu()*int64(base.ThresholdRatioForProcessCpu)/100
}

// returns whether the cpu on the current node has been maxed out
func (rm *ResourceManager) overallCpuMaxedout(previousState *State, state *State) bool {
	totalCpu := state.totalCpu
	if totalCpu < 0 && previousState != nil {
		totalCpu = previousState.totalCpu
	}
	idleCpu := state.idleCpu
	if idleCpu < 0 && previousState != nil {
		idleCpu = previousState.totalCpu
	}

	if totalCpu >= 0 && idleCpu >= 0 && idleCpu < totalCpu*int64(100-base.ThresholdRatioForTotalCpu)/100 {
		return true
	}

	return false
}

func (rm *ResourceManager) getSystemStats() (*SystemStats, error) {
	systemStatsPtr := atomic.LoadPointer(&rm.systemStats)
	if systemStatsPtr != nil {
		return (*SystemStats)(systemStatsPtr), nil
	}
	systemStats, err := NewSystemStats()
	if err != nil {
		return nil, err
	}

	rm.logger.Infof("cgroup supported = %t", systemStats.IsCGroupSupported())
	atomic.StorePointer(&rm.systemStats, unsafe.Pointer(systemStats))
	return systemStats, nil
}

func (rm *ResourceManager) closeSystemStats() {
	systemStatsPtr := atomic.LoadPointer(&rm.systemStats)
	if systemStatsPtr != nil {
		(*SystemStats)(systemStatsPtr).Close()
	}
}

func (rm *ResourceManager) collectCpuUsage() {
	rm.logger.Info("collectCpuUsage starting ....\n")
	defer rm.logger.Info("collectCpuUsage exiting\n")

	defer rm.waitGrp.Done()
	ticker := time.NewTicker(base.CpuCollectionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rm.finch:
			return
		case <-ticker.C:
			rm.collectCpuUsageOnce()
		}
	}
}

func (rm *ResourceManager) manageResources() {
	rm.logger.Info("manageResources starting ....\n")
	defer rm.logger.Info("manageResources exiting\n")

	defer rm.waitGrp.Done()
	ticker := time.NewTicker(base.ResourceManagementInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rm.finch:
			return
		case <-ticker.C:
			rm.manageResourcesOnce()
		}
	}
}

func (rm *ResourceManager) manageResourcesOnce() error {
	specs, err := rm.repl_spec_svc.AllActiveReplicationSpecsReadOnly()
	if err != nil {
		rm.logger.Infof("Skipping resource management actions because of err = %v\n", err)
		return err
	}
	backfillSpecs, err := rm.backfillReplSvc.AllActiveBackfillSpecsReadOnly()
	rm.managedResourceOnceSpecMap = make(map[string]*metadata.GenericSpecification)
	for _, spec := range specs {
		genSpec := metadata.GenericSpecification(spec)
		rm.managedResourceOnceSpecMap[spec.GetFullId()] = &genSpec
	}
	for _, spec := range backfillSpecs {
		vblist, err := rm.backfillReplSvc.GetMyVBs(spec.ReplicationSpec())
		if err != nil {
			rm.logger.Warnf("failed to fetch responsible vbs for spec %s. err=%v", spec.Id, err)
		} else {
			if !spec.VBTasksMap.ContainsAtLeastOneTaskForVBs(vblist) {
				// There's no active backfill pipeline if the spec has no responsible Vbs. Hence skip the spec
				continue
			}
		}
		genSpec := metadata.GenericSpecification(spec)
		rm.managedResourceOnceSpecMap[spec.GetFullId()] = &genSpec
	}
	rm.RemoveAnyStaleMapValues()

	specReplStatsMap := rm.CollectReplStats()

	previousState := rm.getPreviousState()

	state := rm.computeState(specReplStatsMap, previousState)

	if rm.needResourceManagement(state) {
		rm.resettedThrottler = false
		rm.computeActionsToTake(previousState, state)
		rm.takeActions(previousState, state)
	} else {
		if !rm.resettedThrottler {
			// reset throttler limits if we are not managing resources
			// this is to ensure that throttler limits are reset when we are not managing resources
			rm.resetThrottlerLimits(state)
			rm.resettedThrottler = true
		}
	}

	rm.setPreviousState(state)

	return nil
}

func (rm *ResourceManager) areAllSpecSamePriority(specMap map[string]*metadata.GenericSpecification) bool {
	var priority base.PriorityType
	priorityCounts := [base.TotalPriorityTypes]int{}

	for _, genericSpecPtr := range specMap {
		spec := *genericSpecPtr
		priority = spec.GetReplicationSpec().Settings.GetPriority()
		priorityCounts[priority]++
	}

	specCount := len(specMap)
	for i := 0; i < base.TotalPriorityTypes; i++ {
		if priorityCounts[i] == specCount {
			return true
		}
	}

	return false
}

// needResourceManagement determines if resource management is needed or not
// returns true if all replications are of the same priority and conflict logging is not enabled
func (rm *ResourceManager) needResourceManagement(s *State) bool {
	if s.isConflictLoggingEnabled {
		return true
	}

	if rm.areAllSpecSamePriority(rm.managedResourceOnceSpecMap) {
		return false
	}

	return true
}

func (rm *ResourceManager) collectCpuUsageOnce() {
	systemStats, err := rm.getSystemStats()
	if err != nil {
		rm.logger.Warnf("Error retrieving system stats. err=%v\n", err)
		// use a negative value to indicate invalid cpu value
		rm.setCpu(-1)
		return
	}

	_, cpu, err := systemStats.ProcessCpuPercent()
	if err != nil {
		rm.logger.Warnf("Error retrieving cpu usage. err=%v\n", err)
		// use a negative value to indicate invalid cpu value
		rm.setCpu(-1)
	} else {
		// use the integer portion of cpu, which is a percentage
		rm.setCpu(cpu)
	}

	accumulativeTotalCpu, accumulativeIdleCpu, err := systemStats.OverallCpu()
	if err != nil {
		rm.logger.Warnf("Error retrieving overall cpu. err=%v\n", err)
		// use a negative value to indicate invalid cpu value
		rm.setTotalCpu(-1)
		rm.setIdleCpu(-1)
	} else {
		rm.logger.Debugf("totalCPU = %d, idleCPU = %d\n", accumulativeTotalCpu, accumulativeIdleCpu)
		rm.setTotalCpu(accumulativeTotalCpu)
		rm.setIdleCpu(accumulativeIdleCpu)
	}
}

func (rm *ResourceManager) logStats() {
	rm.logger.Info("logStats starting ....\n")
	defer rm.logger.Info("logStats exiting\n")

	defer rm.waitGrp.Done()
	ticker := time.NewTicker(base.ResourceManagementStatsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rm.finch:
			return
		case <-ticker.C:
			rm.logStatsOnce()
		}
	}
}

func (rm *ResourceManager) logStatsOnce() {
	rm.logState()
	rm.logCounters()
	rm.logMaps()
}

func (rm *ResourceManager) logState() {
	rm.stateLock.RLock()
	defer rm.stateLock.RUnlock()

	highTPMean := int64(rm.highThroughputSamples.Mean())
	overallTPMean := int64(rm.overallThroughputSamples.Mean())
	rm.logger.Infof("Resource Manager State highTPMean=%d, overallTPMean=%d, state = %v\n",
		highTPMean,
		overallTPMean,
		rm.previousState)
}

func (rm *ResourceManager) logCounters() {
	rm.logger.Infof("backlogCount=%v, noBacklogCount=%v extraQuota=%v cpuNotMaxedCount=%v throughputDropCount=%v\n",
		atomic.LoadUint32(&rm.backlogCount), atomic.LoadUint32(&rm.noBacklogCount), rm.inExtraQuotaPeriod.Get(),
		atomic.LoadUint32(&rm.cpuNotMaxedCount), atomic.LoadUint32(&rm.throughputDropCount))
}

func (rm *ResourceManager) logMaps() {
	rm.mapLock.RLock()
	defer rm.mapLock.RUnlock()
	rm.logger.Infof("DcpPriorityMap=%v\nongoingReplMap=%v\n", rm.replDcpPriorityMap, rm.ongoingReplMap)
}

func (rm *ResourceManager) getPreviousState() *State {
	rm.stateLock.RLock()
	defer rm.stateLock.RUnlock()
	return rm.previousState
}

func (rm *ResourceManager) setPreviousState(state *State) {
	rm.stateLock.Lock()
	defer rm.stateLock.Unlock()
	rm.previousState = state
}

func (rm *ResourceManager) CollectReplStats() map[*metadata.GenericSpecification]*ReplStats {
	specReplStatsMap := make(map[*metadata.GenericSpecification]*ReplStats)

	for _, specPtr := range rm.managedResourceOnceSpecMap {
		spec := *specPtr
		replStats, err := rm.getStatsFromReplication(spec)
		if err != nil {
			if err != pipelineNotRunning && spec.Type() == metadata.MainReplication {
				rm.logger.Warnf("Could not retrieve runtime stats for %v. err=%v\n", spec.GetFullId(), err)
			}
		} else {
			specReplStatsMap[specPtr] = replStats
		}
	}

	return specReplStatsMap
}

func (rm *ResourceManager) computeState(specReplStatsMap map[*metadata.GenericSpecification]*ReplStats, previousState *State) (state *State) {
	state = newState()
	state.cpu = rm.getCpu()
	state.totalCpu = rm.getTotalCpu()
	state.idleCpu = rm.getIdleCpu()

	for genericSpecPtr, replStats := range specReplStatsMap {
		spec := *genericSpecPtr
		isReplHighPriority := rm.IsReplHighPriority(spec.GetReplicationSpec().Id, spec.GetReplicationSpec().Settings.GetPriority())
		if isReplHighPriority {
			state.highPriorityReplExist = true
		} else {
			state.lowPriorityReplExist = true
		}

		if !state.isConflictLoggingEnabled {
			if rspec := spec.GetReplicationSpec(); rspec != nil {
				if !rspec.Settings.GetConflictLoggingMapping().Disabled() {
					state.isConflictLoggingEnabled = true
				}
			}
		}

		state.replStatsMap[spec.GetFullId()] = replStats

		if replStats.changesLeft <= int64(base.ChangesLeftThresholdForOngoingReplication) {
			rm.setReplOngoing(spec)
		}

		var previousReplStats *ReplStats
		var statsChanged bool = true
		var throughput int64
		var ok bool

		if previousState != nil {
			previousReplStats, ok = previousState.replStatsMap[spec.GetFullId()]
			if ok {
				statsChanged = replStats.timestamp != previousReplStats.timestamp
			}
		}

		if !statsChanged {
			// if stats has not changed for a replication, use throughput from last interval as a best effort estimate
			// previousReplStats cannot be nil in this case
			throughput = previousReplStats.throughput
		} else {
			if previousReplStats != nil {
				clogDocsWritten := replStats.clogDocsWritten - previousReplStats.clogDocsWritten
				if clogDocsWritten < 0 {
					clogDocsWritten = 0
				}

				clogQueueSizeDiff := previousReplStats.clogQueueSize - replStats.clogQueueSize
				clogDocsWritten += clogQueueSizeDiff

				docsProcessed := replStats.docsReceivedFromDcp - previousReplStats.docsReceivedFromDcp + previousReplStats.docsRepQueue - replStats.docsRepQueue
				docsProcessed += clogDocsWritten
				throughput = int64(float64(docsProcessed) / (float64(replStats.timestamp-previousReplStats.timestamp) / float64(1000000000)))
				rm.logger.Tracef("clogCount=%d, docsProcessed=%d, stats:%#v, prevStats=%#v",
					clogDocsWritten, docsProcessed, replStats, previousReplStats)
			} else {
				docsProcessed := replStats.docsReceivedFromDcp - replStats.docsRepQueue
				docsProcessed += replStats.clogDocsWritten
				docsProcessed -= replStats.clogQueueSize
				throughput = int64(float64(docsProcessed) / base.ResourceManagementInterval.Seconds())
			}
			if throughput < 0 {
				// this could happen when replication is starting up, and stats are not yet up to date
				throughput = 0
			}
		}

		replStats.throughput = throughput
		state.overallThroughput += throughput

		throughPutNeeded := replStats.changesLeft * 1000 / int64(spec.GetReplicationSpec().Settings.GetDesiredLatencyMs())

		state.totalThroughputNeeded += throughPutNeeded

		if isReplHighPriority {
			state.highThroughput += throughput

			// for high priority replications, compute throughputNeededByHighRepl
			// This is the throughput desired to clear the changesLeft in whatever time specified
			state.throughputNeededByHighRepl += throughPutNeeded

			// If we cannot clear the changesLeft within specified time, this is considered backlog
			if throughput < throughPutNeeded {
				state.backlogReplExist = true
			}
		}
	}

	rm.overallThroughputSamples.Update(state.overallThroughput)

	if state.highPriorityReplExist {
		// update high throughput sample only when high replications exist
		rm.highThroughputSamples.Update(state.highThroughput)
	}

	state.maxThroughput = state.overallThroughput

	return state
}

func (rm *ResourceManager) computeActionsToTake(previousState, state *State) {
	rm.computeThrottlingActions(previousState, state)
	rm.computeDcpActions(state)
}

func (rm *ResourceManager) computeThrottlingActions(previousState, state *State) {

	if !rm.inExtraQuotaPeriod.Get() {
		// not in extra quota period
		if !rm.cpuMaxedout(previousState, state) {
			newCount := atomic.AddUint32(&rm.cpuNotMaxedCount, 1)
			if newCount > uint32(base.MaxCountCpuNotMaxed) {
				// start extra quota period
				rm.inExtraQuotaPeriod.SetTrue()
				state.throttlerCalibrationAction = ThrottlerCalibrationActionDisable

				rm.applyExtraQuota(state)
			}
		} else {
			atomic.StoreUint32(&rm.cpuNotMaxedCount, 0)
		}
	} else {
		// in extra quota period

		stopExtraQuota := false
		if rm.cpuMaxedout(previousState, state) {
			// stop extra quota period when cpu is maxed out
			stopExtraQuota = true
		}

		if atomic.LoadUint32(&rm.throughputDropCount) == 0 {
			// have not seen throughput drop before
			if previousState != nil && state.overallThroughput < previousState.overallThroughput {
				// first drop in throughput
				atomic.StoreUint32(&rm.throughputDropCount, 1)
				// remember throughput before drop
				atomic.StoreInt64(&rm.throughputBeforeDrop, previousState.overallThroughput)
			}
		} else {
			// already seen throughput drop before
			if state.overallThroughput < atomic.LoadInt64(&rm.throughputBeforeDrop) {
				newCount := atomic.AddUint32(&rm.throughputDropCount, 1)
				if newCount > uint32(base.MaxCountThroughputDrop) {
					// stop extra quota period if throughput stayed below previous max for a number of terms
					stopExtraQuota = true
				}
			} else {
				// throughput got back to previous max. reset counter and stay in extra quota period
				atomic.StoreUint32(&rm.throughputDropCount, 0)
			}
		}

		if stopExtraQuota {
			// stop extra quota period
			atomic.StoreUint32(&rm.throughputDropCount, 0)
			atomic.StoreUint32(&rm.cpuNotMaxedCount, 0)
			rm.inExtraQuotaPeriod.SetFalse()
			state.throttlerCalibrationAction = ThrottlerCalibrationActionEnable

			// do not apply extra quota

		} else {
			// stay in extra quota period
			rm.applyExtraQuota(state)
		}
	}

	var rmTokens []int64
	rmTokens, state.maxReassignableTokens = rm.computeTokens(state, state.maxThroughput, state.throughputNeededByHighRepl)

	rm.logger.Tracef("totalTPut=%d, totalTPutNeeded=%d, maxRA=%d, rmTokens=%v",
		int64(rm.overallThroughputSamples.Mean()), state.totalThroughputNeeded, state.maxReassignableTokens, rmTokens)

	state.highTokens = rmTokens[0]
	state.throughputLimit = rmTokens[1]
	state.conflictLogTokens = rmTokens[2]
}

func (rm *ResourceManager) applyExtraQuota(state *State) {
	// set maxThroughput as max of current throughput and historical mean throughput
	meanHistoricalThroughput := int64(rm.overallThroughputSamples.Mean())
	if state.maxThroughput < meanHistoricalThroughput {
		state.maxThroughput = meanHistoricalThroughput
	}

	// give extra quota to max throughput to allow cpu utilization to go up
	state.maxThroughput += state.maxThroughput * int64(base.ExtraQuotaForUnderutilizedCPU) / int64(base.ResourceManagementRatioBase)
}

// computeTokensWithoutClog reverts back to token distribution that existed before conflict logging
func (rm *ResourceManager) computeTokensWithoutClog(maxThroughput, throughputNeededByHighRepl int64) (highTokens, throughputLimit, maxReassignableTokens int64) {
	// this is the max throughput high priority replications are allowed, after reserving minimum quota for low priority replications
	maxThroughputAllowedForHighRepl := maxThroughput * int64(base.ResourceManagementRatioUpperBound) / int64(base.ResourceManagementRatioBase)

	// high tokens = min(throughputNeeded, throughputAllowed)
	highTokens = throughputNeededByHighRepl
	if highTokens > maxThroughputAllowedForHighRepl {
		highTokens = maxThroughputAllowedForHighRepl
	}

	maxReassignableTokens = highTokens - int64(rm.highThroughputSamples.Mean())
	if maxReassignableTokens < 0 {
		maxReassignableTokens = 0
	}

	// assign remaining tokens, which serves as a throughput limit, to low priority replications
	throughputLimit = maxThroughput - highTokens

	return
}

func (rm *ResourceManager) computeTokens(state *State, maxThroughput, throughputNeededByHighRepl int64) (rmTokens []int64, maxReassignableTokens int64) {
	if !state.isConflictLoggingEnabled {
		highTokens, lowTokens, commonTokens := rm.computeTokensWithoutClog(state.maxThroughput, throughputNeededByHighRepl)
		rmTokens = []int64{highTokens, lowTokens, 0}
		maxReassignableTokens = commonTokens
		return
	}

	// this is the max throughput high priority replications are allowed, after reserving minimum quota for low priority replications
	highTokens := maxThroughput * int64(base.RMTokenDistribution[base.RMDistHighRepl]) / int64(base.ResourceManagementRatioBase)

	// assign remaining tokens, which serves as a throughput limit, to low priority replications
	lowPriorityTokens := maxThroughput * int64(base.RMTokenDistribution[base.RMDistLowRepl]) / int64(base.ResourceManagementRatioBase)

	// totalThroughputNeeded is a theoretical throughput needed based on total changes left to clear the backlog.
	// maxReassignableTokens = min(HighTokens + LowTokens, totalThroughputNeeded)
	maxReassignableTokens = state.totalThroughputNeeded - int64(rm.overallThroughputSamples.Mean())
	if maxReassignableTokens < 0 {
		maxReassignableTokens = 0
	}

	replThroughPut := highTokens + lowPriorityTokens

	if maxReassignableTokens > replThroughPut {
		maxReassignableTokens = replThroughPut
	}

	clogTokens := maxThroughput * int64(base.RMTokenDistribution[base.RMDistCLog]) / int64(base.ResourceManagementRatioBase)
	if base.CLogResourceManagerBoost > 0 && clogTokens < int64(base.CLogResourceManagerBoost) {
		clogTokens = (maxThroughput + int64(base.CLogResourceManagerBoost)) * int64(base.RMTokenDistribution[base.RMDistCLog]) / int64(base.ResourceManagementRatioBase)
	}

	if !state.highPriorityReplExist {
		maxReassignableTokens += highTokens
		highTokens = 0
	}

	if !state.lowPriorityReplExist {
		maxReassignableTokens += lowPriorityTokens
		lowPriorityTokens = 0
	}

	rmTokens = []int64{highTokens, lowPriorityTokens, clogTokens}

	return
}

func (rm *ResourceManager) computeDcpActions(state *State) {
	if !state.backlogReplExist {
		rm.computeDcpActionsWithoutBacklog(state)
	} else {
		rm.computeDcpActionsWithBacklog(state)
	}
}

func (rm *ResourceManager) computeDcpActionsWithoutBacklog(state *State) {
	noBacklogCount := atomic.AddUint32(&rm.noBacklogCount, 1)
	atomic.StoreUint32(&rm.backlogCount, 0)

	if !state.highPriorityReplExist || !state.lowPriorityReplExist {
		state.dcpPriorityAction = DcpPriorityActionReset
		return
	}

	if noBacklogCount >= uint32(base.MaxCountNoBacklogForResetDcpPriority) {
		state.dcpPriorityAction = DcpPriorityActionReset
		return
	}
}

func (rm *ResourceManager) computeDcpActionsWithBacklog(state *State) {
	backlogCount := atomic.AddUint32(&rm.backlogCount, 1)
	atomic.StoreUint32(&rm.noBacklogCount, 0)

	if !state.lowPriorityReplExist {
		state.dcpPriorityAction = DcpPriorityActionReset
		return
	}

	if backlogCount >= uint32(base.MaxCountBacklogForSetDcpPriority) {
		state.dcpPriorityAction = DcpPriorityActionSet
		return
	}

	return
}

func (rm *ResourceManager) takeActions(previousState *State, state *State) {
	rm.setThrottlerActions(previousState, state)

	switch state.dcpPriorityAction {
	case DcpPriorityActionSet:
		rm.setDcpPriorities(state)
	case DcpPriorityActionReset:
		rm.resetDcpPriorities(state)
	default:
		// no op for ActionNone
	}
}

// resetThrottlerLimits sets the throttler limits to 0 (essentially disabling the throttler)
func (rm *ResourceManager) resetThrottlerLimits(state *State) {
	settings := map[string]interface{}{
		throttlerSvc.HighTokensKey:                int64(0),
		throttlerSvc.LowTokensKey:                 int64(0),
		throttlerSvc.MaxReassignableHighTokensKey: int64(0),
		throttlerSvc.ConflictLogTokensKey:         int64(0),
		throttlerSvc.ConflictLogEnabledKey:        state.isConflictLoggingEnabled,
		throttlerSvc.NeedToCalibrateKey:           false,
	}

	errMap := rm.throughputThrottlerSvc.UpdateSettings(settings)
	if len(errMap) > 0 {
		rm.logger.Warnf("Error resetting tokens. err=%v\n", errMap)
	}
}

func (rm *ResourceManager) setThrottlerActions(previousState, state *State) {
	// -1 indicates that there are no previous tokens
	var previousHighTokens int64 = -1
	var previousMaxReassignableTokens int64 = -1
	var previousThroughputLimit int64 = -1
	var previousClogTokens int64 = -1
	if previousState != nil {
		previousHighTokens = previousState.highTokens
		previousMaxReassignableTokens = previousState.maxReassignableTokens
		previousThroughputLimit = previousState.throughputLimit
		previousClogTokens = previousState.conflictLogTokens
	}

	settings := rm.constructSettings(state, previousHighTokens, previousMaxReassignableTokens, previousThroughputLimit, previousClogTokens)
	errMap := rm.throughputThrottlerSvc.UpdateSettings(settings)
	if len(errMap) > 0 {
		if err, ok := errMap[throttlerSvc.HighTokensKey]; ok {
			rm.logger.Warnf("Error setting tokens for high priority replications to %v. err=%v", state.highTokens, err)
			state.highTokens = previousHighTokens
		}
		if err, ok := errMap[throttlerSvc.MaxReassignableHighTokensKey]; ok {
			rm.logger.Warnf("Error setting max reassignable tokens for high priority replications to %v. err=%v", state.maxReassignableTokens, err)
			state.maxReassignableTokens = previousMaxReassignableTokens
		}
		if err, ok := errMap[throttlerSvc.LowTokensKey]; ok {
			rm.logger.Warnf("Error setting tokens for low priority replications to %v. err=%v", state.throughputLimit, err)
			state.throughputLimit = previousThroughputLimit
		}
	}
}

// construct settings map for throttler service
func (rm *ResourceManager) constructSettings(state *State, previousHighTokens, previousMaxReassignableTokens, previousThroughputLimit int64, previousClogTokens int64) map[string]interface{} {
	settings := make(map[string]interface{})

	if state.highTokens != previousHighTokens {
		settings[throttlerSvc.HighTokensKey] = state.highTokens
	}

	if state.maxReassignableTokens != previousMaxReassignableTokens {
		settings[throttlerSvc.MaxReassignableHighTokensKey] = state.maxReassignableTokens
	}

	if state.throughputLimit != previousThroughputLimit {
		settings[throttlerSvc.LowTokensKey] = state.throughputLimit
	}

	if state.conflictLogTokens != previousClogTokens {
		settings[throttlerSvc.ConflictLogTokensKey] = state.conflictLogTokens
	}

	settings[throttlerSvc.ConflictLogEnabledKey] = state.isConflictLoggingEnabled

	switch state.throttlerCalibrationAction {
	case ThrottlerCalibrationActionEnable:
		settings[throttlerSvc.NeedToCalibrateKey] = true
	case ThrottlerCalibrationActionDisable:
		settings[throttlerSvc.NeedToCalibrateKey] = false
	default:
		// no op
	}

	return settings
}

func (rm *ResourceManager) setDcpPriorities(state *State) {
	rm.mapLock.Lock()
	defer rm.mapLock.Unlock()

	for fullSpecId, _ := range state.replStatsMap {
		var targetPriority mcc.PriorityType
		specPtr, ok := rm.managedResourceOnceSpecMap[fullSpecId]
		if !ok {
			// should never get here
			rm.logger.Warnf("Skipping setting dcp priority for %v because of error retrieving replication spec", fullSpecId)
			continue
		}
		spec := *specPtr
		if rm.isReplHighPriority(spec.GetFullId(), spec.GetReplicationSpec().Settings.GetPriority(), false /*lock*/) {
			targetPriority = mcc.PriorityHigh
		} else {
			targetPriority = mcc.PriorityLow
		}

		if currentPriority, ok := rm.replDcpPriorityMap[spec.GetFullId()]; ok && currentPriority == targetPriority {
			// no op if the current set priority is the same as the target value
			continue
		}

		err := rm.setDcpPriority(spec, targetPriority)
		if err == nil {
			rm.replDcpPriorityMap[spec.GetFullId()] = targetPriority
		} else {
			rm.logger.Warnf("Error setting dcp priority for %v to %v. err=%v\n", spec.GetFullId(), targetPriority, err)
			continue
		}
	}

}

func (rm *ResourceManager) resetDcpPriorities(state *State) {
	rm.mapLock.Lock()
	defer rm.mapLock.Unlock()

	if len(rm.replDcpPriorityMap) == 0 {
		// if dcp priority has not been set for any replications yet, nothing to do
		return
	}

	targetPriority := mcc.PriorityMed

	for fullReplId, _ := range state.replStatsMap {
		if currentPriority, ok := rm.replDcpPriorityMap[fullReplId]; !ok || currentPriority == targetPriority {
			// no op if dcp priority has not been set before, or the set value is the same as the target value
			continue
		}
		specPtr, ok := rm.managedResourceOnceSpecMap[fullReplId]
		if !ok || specPtr == nil {
			rm.logger.Warnf("Unable to find generic spec given full replId: %v", fullReplId)
			continue
		}
		spec := *specPtr

		err := rm.setDcpPriority(spec, targetPriority)
		if err == nil {
			rm.replDcpPriorityMap[fullReplId] = targetPriority
		} else {
			rm.logger.Warnf("Error setting dcp priority for %v to %v. err=%v\n", fullReplId, targetPriority, err)
			continue
		}
	}
}

func (rm *ResourceManager) isReplOngoing(replId string, lock bool) bool {
	if lock {
		rm.mapLock.RLock()
		defer rm.mapLock.RUnlock()
	}

	if ongoing, ok := rm.ongoingReplMap[replId]; ok && ongoing {
		return true
	} else {
		return false
	}
}

func (rm *ResourceManager) setReplOngoing(spec metadata.GenericSpecification) error {
	if rm.isReplOngoing(spec.GetFullId(), true) {
		// replication is already ongoing. no op
		return nil
	}

	if spec.GetReplicationSpec().Settings.GetPriority() == base.PriorityTypeMedium {
		// change "isHighReplication" flag on Medium replication
		settings := make(map[string]interface{})
		settings[parts.IsHighReplicationKey] = true

		err := rm.applySettingsToPipeline(spec, settings)
		if err != nil {
			// do not add repl to ongoingReplMap
			// we will get another chance to repeat this op in the next interval
			rm.logger.Warnf("Skipping changing needToThrottle setting for %v due to err=%v.", spec.GetFullId(), err)
			return err
		}
	}

	rm.addReplToOngoingReplMap(spec.GetFullId())
	return nil
}

// returns true if repl has been changed to ongoing
// returns false if repl is already in ongoing state and no action has been taken
func (rm *ResourceManager) addReplToOngoingReplMap(replId string) {
	rm.mapLock.Lock()
	defer rm.mapLock.Unlock()
	rm.ongoingReplMap[replId] = true
	rm.logger.Infof("Set ongoing flag for %v\nongoingReplMap=%v\n", replId, rm.ongoingReplMap)
}

func (rm *ResourceManager) RemoveAnyStaleMapValues() {
	rm.mapLock.Lock()
	defer rm.mapLock.Unlock()
	for key := range rm.ongoingReplMap {
		_, exists := rm.managedResourceOnceSpecMap[key]
		if !exists {
			delete(rm.ongoingReplMap, key)
		}
	}
	for key := range rm.replDcpPriorityMap {
		_, exists := rm.managedResourceOnceSpecMap[key]
		if !exists {
			delete(rm.replDcpPriorityMap, key)
		}
	}
}

func (rm *ResourceManager) applySettingsToPipeline(spec metadata.GenericSpecification, settings map[string]interface{}) error {
	replId := spec.GetReplicationSpec().Id
	rs, err := rm.pipelineMgr.ReplicationStatus(replId)
	if err != nil {
		return fmt.Errorf("Skipping updating settings for %v because of error retrieving replication status. settings=%v, err=%v\n", replId, settings, err)
	}

	if spec.Type() == metadata.MainReplication {
		// set custom settings on replStatus so that settings can be automatically re-applied after pipeline restart
		// Backfill pipelines do not reapply when pipeline restarts
		rs.SetCustomSettings(settings)
	}

	var pipeline common.Pipeline
	switch spec.Type() {
	case metadata.MainReplication:
		pipeline = rs.Pipeline()
	case metadata.BackfillReplication:
		pipeline = rs.BackfillPipeline()
	default:
		panic(fmt.Sprintf("Invalid type: %v", spec.Type().String()))
	}

	if pipeline == nil {
		return fmt.Errorf("Skipping updating settings for %v because of nil pipeline. err=%v\n", replId, err)
	}
	// apply the setting to the live pipeline
	return pipeline.UpdateSettings(settings)
}

func (rm *ResourceManager) setDcpPriority(spec metadata.GenericSpecification, priority mcc.PriorityType) error {
	rm.logger.Infof("Setting dcp priority to %v for %v", priority, spec.GetFullId())

	settings := make(map[string]interface{})
	settings[parts.DCP_Priority] = priority

	return rm.applySettingsToPipeline(spec, settings)
}

func (rm *ResourceManager) getStatsFromReplication(spec metadata.GenericSpecification) (*ReplStats, error) {
	if atomic.LoadUint32(&rm.isKVNode) == 0 {
		return &ReplStats{
			timestamp: time.Now().UnixNano(),
		}, nil
	}

	var pipelineType common.PipelineType
	switch spec.Type() {
	case metadata.MainReplication:
		pipelineType = common.MainPipeline
	case metadata.BackfillReplication:
		pipelineType = common.BackfillPipeline
	default:
		panic(fmt.Sprintf("Unknown type: %v", spec.Type().String()))
	}
	rs, err := rm.pipelineMgr.ReplicationStatus(spec.GetReplicationSpec().Id)
	if err != nil {
		return nil, err
	}

	curProgress := rs.GetProgress()
	if curProgress != common.ProgressPipelineRunning {
		return nil, pipelineNotRunning
	}

	statsMap := rs.GetOverviewStats(pipelineType)
	if statsMap == nil {
		// this is possible when replication is starting up
		return nil, fmt.Errorf("Cannot find overview stats for %v", spec.GetFullId())
	}

	changesLeft, err := base.ParseStats(statsMap, base.ChangesLeftStats)
	if err != nil {
		return nil, err
	}
	docsFromDcp, err := base.ParseStats(statsMap, base.DocsFromDcpStats)
	if err != nil {
		return nil, err
	}
	docsRepQueue, err := base.ParseStats(statsMap, base.DocsRepQueueStats)
	if err != nil {
		return nil, err
	}
	timestamp, err := base.ParseStats(statsMap, base.CurrentTime)
	if err != nil {
		return nil, err
	}

	clogDocsWritten, err := base.ParseStats(statsMap, base.ConflictDocsWritten)
	if err != nil {
		// we ignore if the stat is not found because the stat is dependent on
		// whether conflict logging is enabled or not.
		if !strings.Contains(err.Error(), "Cannot find value for stats") {
			return nil, err
		}
		clogDocsWritten = 0
	}

	clogQueueSize := rm.getClogQueueSize(rs)

	// throughput will be computer later and is temporarily set to 0 for now
	return &ReplStats{
		changesLeft:         changesLeft,
		docsReceivedFromDcp: docsFromDcp,
		docsRepQueue:        docsRepQueue,
		clogDocsWritten:     clogDocsWritten,
		clogQueueSize:       clogQueueSize,
		timestamp:           timestamp,
	}, nil
}

func (rm *ResourceManager) getClogQueueSize(rs pipeline.ReplicationStatusIface) int64 {
	pipe := rs.Pipeline()
	if pipe == nil {
		return 0
	}

	logger := baseclog.LoggerFromService(pipe.RuntimeContext().Service(base.CONFLICT_LOGGER))
	if logger == nil {
		return 0
	}

	sz := int64(logger.QueueSize())

	return sz
}
