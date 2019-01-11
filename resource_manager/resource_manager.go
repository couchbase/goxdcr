// Copyright (c) 2019 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package resource_manager

import (
	"bytes"
	"fmt"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/parts"
	"github.com/couchbase/goxdcr/pipeline_manager"
	"github.com/couchbase/goxdcr/service_def"
	utilities "github.com/couchbase/goxdcr/utils"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const ResourceManagerName = "ResourceMgr"

// runtime stats collected from active replications
type ReplStats struct {
	changesLeft         int64
	docsReceivedFromDcp int64
	docsRepQueue        int64
	// timestamp that the stats is updated
	timestamp int64
	// stats derived from other stats
	throughput int64
}

type ThrottleAction int

const (
	// no throttling
	ThrottleActionNone ThrottleAction = iota
	// increase throttling
	ThrottleActionIncrease
	// decrease throttling
	ThrottleActionDecrease
	// wait action/period before we switch to decrease action when increase action is not effective
	ThrottleActionWait
)

func (ta ThrottleAction) String() string {
	switch ta {
	case ThrottleActionNone:
		return "None"
	case ThrottleActionIncrease:
		return "Inc"
	case ThrottleActionDecrease:
		return "Dec"
	case ThrottleActionWait:
		return "Wait"
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
	// cpu usage percentage
	cpu int32
	// ratio of throughput of high priority replications to throughput of all replications
	// computed as "highThroughput * 100/ overallThroughput"
	currentRatio int
	// target ratio after resource management actions
	targetRatio int
	// throughput limit for low priority replications
	// computed based on targetRatio
	throughputLimit int64
	// throughput of all replications
	overallThroughput int64
	// throughput of high priority replications
	highThroughput int64
	// throughput of high priority replications with backlog
	backlogThroughput int64
	// whether highThroughput stats has changed since last interval
	highThroughputStatsChanged bool
	// whether backlogThroughput stats has changed since last interval
	backlogThroughputStatsChanged bool
	// whether high priority replications exist
	highPriorityReplExist bool
	// whether low priority replications exist
	lowPriorityReplExist bool
	// whether high priority replications with backlog exist
	backlogReplExist bool
	// runtime stats of active replications
	replStatsMap map[string]*ReplStats

	throttleAction    ThrottleAction
	dcpPriorityAction DcpPriorityAction
}

func newState() *State {
	return &State{
		replStatsMap: make(map[string]*ReplStats),
	}
}

func (s *State) String() string {
	var buffer bytes.Buffer
	buffer.WriteString("cpu: ")
	buffer.WriteString(fmt.Sprintf("%v", s.cpu))
	buffer.WriteString(" currentRatio: ")
	buffer.WriteString(strconv.Itoa(s.currentRatio))
	buffer.WriteString(" targetRatio: ")
	buffer.WriteString(strconv.Itoa(s.targetRatio))
	buffer.WriteString(" overallTP: ")
	buffer.WriteString(strconv.FormatInt(s.overallThroughput, base.ParseIntBase))
	buffer.WriteString(" highTP: ")
	buffer.WriteString(strconv.FormatInt(s.highThroughput, base.ParseIntBase))
	buffer.WriteString(" backlogTP: ")
	buffer.WriteString(strconv.FormatInt(s.backlogThroughput, base.ParseIntBase))
	buffer.WriteString(" highTPChanged: ")
	buffer.WriteString(strconv.FormatBool(s.highThroughputStatsChanged))
	buffer.WriteString(" backlogTPChanged: ")
	buffer.WriteString(strconv.FormatBool(s.backlogThroughputStatsChanged))
	buffer.WriteString(" highReplExist: ")
	buffer.WriteString(strconv.FormatBool(s.highPriorityReplExist))
	buffer.WriteString(" lowReplExist: ")
	buffer.WriteString(strconv.FormatBool(s.lowPriorityReplExist))
	buffer.WriteString(" backlogReplExist: ")
	buffer.WriteString(strconv.FormatBool(s.backlogReplExist))
	buffer.WriteString(" throughputLimit: ")
	buffer.WriteString(fmt.Sprintf("%v", s.throughputLimit))
	buffer.WriteString(" throttleAction: ")
	buffer.WriteString(s.throttleAction.String())
	buffer.WriteString(" dcpAction: ")
	buffer.WriteString(s.dcpPriorityAction.String())
	return buffer.String()
}

type ResourceManager struct {
	pipelineMgr            pipeline_manager.Pipeline_mgr_iface
	repl_spec_svc          service_def.ReplicationSpecSvc
	xdcr_topology_svc      service_def.XDCRCompTopologySvc
	remote_cluster_svc     service_def.RemoteClusterSvc
	cluster_info_svc       service_def.ClusterInfoSvc
	checkpoint_svc         service_def.CheckpointsService
	uilog_svc              service_def.UILogSvc
	throughputThrottlerSvc service_def.ThroughputThrottlerSvc
	logger                 *log.CommonLogger
	utils                  utilities.UtilsIface
	waitGrp                sync.WaitGroup
	finch                  chan bool

	// count of consecutive terms where there has been backlog
	backlogCount int32
	// count of consecutive terms where there has been no backlog
	noBacklogCount int32
	// count of consecutive ThrottleActionIncrease actions which have not been effective
	ineffectiveIncreaseThrottlingCount int32
	// count of consecutive ThrottleActionDecrease actions
	decreaseThrottlingCount int32
	// count of consecutive wait actions
	waitCount int32

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
	maxCpu int32
}

type ResourceMgrIface interface {
	Start() error
	Stop() error
	GetThroughputThrottler() service_def.ThroughputThrottlerSvc
	IsReplHighPriority(replId string, priority base.PriorityType) bool
	HandlePipelineDeletion(replId string)
	HandleGoMaxProcsChange(goMaxProcs int)
}

func NewResourceManager(pipelineMgr pipeline_manager.Pipeline_mgr_iface, repl_spec_svc service_def.ReplicationSpecSvc, xdcr_topology_svc service_def.XDCRCompTopologySvc,
	remote_cluster_svc service_def.RemoteClusterSvc, cluster_info_svc service_def.ClusterInfoSvc, checkpoint_svc service_def.CheckpointsService,
	uilog_svc service_def.UILogSvc, throughput_throttler_svc service_def.ThroughputThrottlerSvc,
	logger_context *log.LoggerContext, utilsIn utilities.UtilsIface) ResourceMgrIface {

	resourceMgrRetVar := &ResourceManager{
		pipelineMgr:            pipelineMgr,
		repl_spec_svc:          repl_spec_svc,
		xdcr_topology_svc:      xdcr_topology_svc,
		remote_cluster_svc:     remote_cluster_svc,
		checkpoint_svc:         checkpoint_svc,
		logger:                 log.NewLogger(ResourceManagerName, logger_context),
		cluster_info_svc:       cluster_info_svc,
		uilog_svc:              uilog_svc,
		utils:                  utilsIn,
		finch:                  make(chan bool),
		ongoingReplMap:         make(map[string]bool),
		replDcpPriorityMap:     make(map[string]mcc.PriorityType),
		throughputThrottlerSvc: throughput_throttler_svc,
		maxCpu:                 int32(base.DefaultGoMaxProcs * 100),
	}

	resourceMgrRetVar.logger.Info("Resource Manager is initialized")

	return resourceMgrRetVar
}

func (rm *ResourceManager) Start() error {
	rm.logger.Infof("%v starting ....\n", ResourceManagerName)
	defer rm.logger.Infof("%v started\n", ResourceManagerName)

	// igore error
	rm.getSystemStats()

	// this does not return error as of now
	rm.throughputThrottlerSvc.Start()

	rm.waitGrp.Add(1)
	go rm.manageResources()

	rm.waitGrp.Add(1)
	go rm.logStats()

	return nil
}

func (rm *ResourceManager) Stop() error {
	rm.logger.Infof("%v stopping ....\n", ResourceManagerName)
	defer rm.logger.Infof("%v stopped\n", ResourceManagerName)

	close(rm.finch)
	rm.waitGrp.Wait()

	rm.closeSystemStats()

	err := rm.throughputThrottlerSvc.Stop()
	if err != nil {
		rm.logger.Errorf("%v Error stopping throughput throttler service. err=%v\n", ResourceManagerName, err)
	}
	return err
}

func (rm *ResourceManager) GetThroughputThrottler() service_def.ThroughputThrottlerSvc {
	return rm.throughputThrottlerSvc
}

func (rm *ResourceManager) IsReplHighPriority(replId string, priority base.PriorityType) bool {
	switch priority {
	case base.PriorityTypeHigh:
		return true
	case base.PriorityTypeLow:
		return false
	case base.PriorityTypeMedium:
		return rm.isReplOngoing(replId)
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
	atomic.StoreInt32(&rm.maxCpu, int32(goMaxProcs*100))
}

func (rm *ResourceManager) getMaxCpu() int32 {
	return atomic.LoadInt32(&rm.maxCpu)
}

// returns whether cpu has been maxed out
// if state.cpu has value of -1 because of cpu collection failure, use previousState.cpu instead
// if previousState.cpu is also -1, this method returns false
// in such cases we are effectively reverting back to the algorithm where cpu was not a factor
func (rm *ResourceManager) cpuMaxedout(previousState *State, state *State) bool {
	cpu := state.cpu
	if cpu < 0 {
		cpu = previousState.cpu
	}
	return cpu >= rm.getMaxCpu()*int32(base.ThresholdRatioForMaxCpu)/100
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

	atomic.StorePointer(&rm.systemStats, unsafe.Pointer(systemStats))
	return systemStats, nil
}

func (rm *ResourceManager) closeSystemStats() {
	systemStatsPtr := atomic.LoadPointer(&rm.systemStats)
	if systemStatsPtr != nil {
		(*SystemStats)(systemStatsPtr).Close()
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

	cpu := rm.collectCpuUsage()

	specReplStatsMap := rm.collectReplStats(specs)

	previousState := rm.getPreviousState()

	state := rm.computeState(specReplStatsMap, previousState, cpu)

	rm.computeActionsToTake(previousState, state)

	rm.takeActions(specs, previousState, state)

	rm.setPreviousState(state)

	return nil
}

func (rm *ResourceManager) collectCpuUsage() int32 {
	systemStats, err := rm.getSystemStats()
	if err != nil {
		rm.logger.Warnf("Error retrieving system stats. err=%v\n", err)
		// use a negative value to indicate invalid cpu value
		return -1
	}

	_, cpu, err := systemStats.ProcessCpuPercent()
	if err != nil {
		rm.logger.Warnf("Error retrieving cpu usage. err=%v\n", err)
		// use a negative value to indicate invalid cpu value
		return -1
	}
	// return the integer portion of cpu, which is a percentage
	return int32(cpu)
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
	rm.logger.Infof("Resource Manager State = %v\n", rm.previousState)
}

func (rm *ResourceManager) logCounters() {
	rm.logger.Infof("backlogCount=%v, noBacklogCount=%v ineffectiveIncCount=%v, decCount=%v, waitCount=%v\n", atomic.LoadInt32(&rm.backlogCount), atomic.LoadInt32(&rm.noBacklogCount),
		atomic.LoadInt32(&rm.ineffectiveIncreaseThrottlingCount), atomic.LoadInt32(&rm.decreaseThrottlingCount), atomic.LoadInt32(&rm.waitCount))
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

func (rm *ResourceManager) collectReplStats(specs map[string]*metadata.ReplicationSpecification) map[*metadata.ReplicationSpecification]*ReplStats {
	specReplStatsMap := make(map[*metadata.ReplicationSpecification]*ReplStats)

	for _, spec := range specs {
		replStats, err := rm.getStatsFromReplication(spec)
		if err != nil {
			rm.logger.Warnf("Could not retrieve runtime stats for %v. err=%v\n", spec.Id, err)
		} else {
			specReplStatsMap[spec] = replStats
		}
	}

	return specReplStatsMap
}

func (rm *ResourceManager) computeState(specReplStatsMap map[*metadata.ReplicationSpecification]*ReplStats,
	previousState *State, cpu int32) (state *State) {
	state = newState()
	state.cpu = cpu

	for spec, replStats := range specReplStatsMap {
		isReplHighPriority := rm.IsReplHighPriority(spec.Id, spec.Settings.GetPriority())
		if isReplHighPriority {
			state.highPriorityReplExist = true
		} else {
			state.lowPriorityReplExist = true
		}

		state.replStatsMap[spec.Id] = replStats

		if replStats.changesLeft <= int64(base.ChangesLeftThresholdForOngoingReplication) {
			rm.setReplOngoing(spec)
		}

		var previousReplStats *ReplStats
		var statsChanged bool = true
		var throughput int64
		var ok bool

		if previousState != nil {
			previousReplStats, ok = previousState.replStatsMap[spec.Id]
			if ok {
				statsChanged = (replStats.timestamp != previousReplStats.timestamp)
			}
		}

		if !statsChanged {
			// if stats has not changed for a replication, use throughput from last interval as a best effort estimate
			// previousReplStats cannot be nil in this case
			throughput = previousReplStats.throughput
		} else {
			if previousReplStats != nil {
				docsProcessed := replStats.docsReceivedFromDcp - previousReplStats.docsReceivedFromDcp + previousReplStats.docsRepQueue - replStats.docsRepQueue
				throughput = int64(float64(docsProcessed) / (float64(replStats.timestamp-previousReplStats.timestamp) / float64(1000000000)))
			} else {
				docsProcessed := replStats.docsReceivedFromDcp - replStats.docsRepQueue
				throughput = int64(float64(docsProcessed) / base.ResourceManagementInterval.Seconds())
			}
			if throughput < 0 {
				// this could happen when replication is starting up, and stats are not yet up to date
				throughput = 0
			}
		}

		replStats.throughput = throughput
		state.overallThroughput += throughput

		if isReplHighPriority {
			state.highThroughput += throughput
			state.highThroughputStatsChanged = statsChanged

			if (throughput == 0 && replStats.changesLeft > 0) ||
				// backlogThreshold is in ms, hence we need to divide throughput by 1000 to convert it to mutations per ms
				// we are multiplying changesLeft by 1000 to acheive the same effect
				(throughput > 0 && replStats.changesLeft*1000/throughput > int64(spec.Settings.GetBacklogThreshold())) {
				state.backlogReplExist = true
				state.backlogThroughput += throughput
				state.backlogThroughputStatsChanged = statsChanged
			}
		}
	}

	state.currentRatio = base.ResourceManagementRatioBase
	if state.overallThroughput != 0 {
		state.currentRatio = int(state.highThroughput * int64(base.ResourceManagementRatioBase) / state.overallThroughput)
	}

	return state
}

func (rm *ResourceManager) computeActionsToTake(previousState, state *State) {
	if !state.backlogReplExist {
		rm.computeActionsToTakeWithoutBacklog(previousState, state)
	} else {
		rm.computeActionsToTakeWithBacklog(previousState, state)
	}
}

func (rm *ResourceManager) computeActionsToTakeWithoutBacklog(previousState, state *State) {
	noBacklogCount := atomic.AddInt32(&rm.noBacklogCount, 1)

	// reset all counters used in backlog scenario
	atomic.StoreInt32(&rm.backlogCount, 0)
	atomic.StoreInt32(&rm.ineffectiveIncreaseThrottlingCount, 0)
	atomic.StoreInt32(&rm.waitCount, 0)
	atomic.StoreInt32(&rm.decreaseThrottlingCount, 0)

	if !state.highPriorityReplExist || !state.lowPriorityReplExist {
		rm.computeActionsToTakeWithOneGroup(state)
		return
	}

	if noBacklogCount >= int32(base.MaxCountNoBacklogForResetDcpPriority) {
		state.dcpPriorityAction = DcpPriorityActionReset
	}

	state.throttleAction = ThrottleActionDecrease

	if previousState == nil {
		// this can happen only when goxdcr process is first started
		// without a previous state to consult, decrease throttling
		state.targetRatio = decrementRatio(state.currentRatio, -1, base.ResourceManagementRatioDecrement)
		computeThroughputLimit(state)
	} else {
		state.targetRatio = decrementRatio(state.currentRatio, previousState.targetRatio, base.ResourceManagementRatioDecrement)
		computeThroughputLimit(state)

		// make sure that throughput limit does not decrease
		// otherwise we may have serious problems when throughput of high priority replications drops
		// suddenly, e.g., because of backfill completion
		if state.throughputLimit < previousState.throughputLimit {
			state.throughputLimit = previousState.throughputLimit
		}
	}

}

func (rm *ResourceManager) computeActionsToTakeWithBacklog(previousState, state *State) {
	atomic.StoreInt32(&rm.noBacklogCount, 0)
	backlogCount := atomic.AddInt32(&rm.backlogCount, 1)

	if !state.lowPriorityReplExist {
		rm.computeActionsToTakeWithOneGroup(state)
		return
	}

	if backlogCount >= int32(base.MaxCountBacklogForSetDcpPriority) {
		state.dcpPriorityAction = DcpPriorityActionSet
	}

	currentRatio := state.currentRatio

	if previousState == nil {
		// this can happen only when goxdcr process is first started
		// increase throttling
		state.targetRatio = incrementRatio(currentRatio, -1 /*previousTargetRatio*/, base.ResourceManagementRatioFirstIncrement)
		computeThroughputLimit(state)
		state.throttleAction = ThrottleActionIncrease
		return
	}

	// tracks if this is the first time we need to throttle
	//  1. when this is the first time we see backlog
	// or 2. when there was backlog before, and there is the first time low priority replications are present
	firstThrottle := (backlogCount == 1) || !previousState.lowPriorityReplExist

	switch previousState.throttleAction {
	case ThrottleActionNone:
		deltaRatio := base.ResourceManagementRatioIncrement
		if firstThrottle {
			deltaRatio = base.ResourceManagementRatioFirstIncrement
		}
		state.targetRatio = incrementRatio(currentRatio, previousState.targetRatio, deltaRatio)
		state.throttleAction = ThrottleActionIncrease
	case ThrottleActionIncrease:
		// if last action was increase, no need to check for firstThrottle since it cannot be true

		if throughputImproved(previousState, state) || rm.cpuMaxedout(previousState, state) {
			// keep increasing throttling if
			// 1.  throughput of high priority replications improved
			// or 2. cpu is currently maxed out
			atomic.StoreInt32(&rm.ineffectiveIncreaseThrottlingCount, 0)
			state.targetRatio = incrementRatio(currentRatio, previousState.targetRatio, base.ResourceManagementRatioIncrement)
			state.throttleAction = ThrottleActionIncrease
		} else if !throughputStatsChanged(state) {
			// stats has not changed. we do not know whether throttling has been effective
			// keep increasing throttling for now to be conservative.
			// keep ineffectiveIncreaseThrottlingCount as is
			state.targetRatio = incrementRatio(currentRatio, previousState.targetRatio, base.ResourceManagementRatioIncrement)
			state.throttleAction = ThrottleActionIncrease
		} else {
			// previous increase throttling action is not effective
			ineffectiveIncreaseThrottlingCount := atomic.AddInt32(&rm.ineffectiveIncreaseThrottlingCount, 1)
			if ineffectiveIncreaseThrottlingCount > int32(base.MaxCountIneffectiveThrottlingIncrease) {
				// start wait period. keep throughput limit as is
				atomic.StoreInt32(&rm.ineffectiveIncreaseThrottlingCount, 0)
				atomic.StoreInt32(&rm.waitCount, 1)
				state.targetRatio = previousState.targetRatio
				state.throughputLimit = previousState.throughputLimit
				state.throttleAction = ThrottleActionWait
				return
			} else {
				// keep increasing throttling
				state.targetRatio = incrementRatio(currentRatio, previousState.targetRatio, base.ResourceManagementRatioIncrement)
				state.throttleAction = ThrottleActionIncrease
			}
		}
	case ThrottleActionWait:
		// if last action was Wait, no need to check for fistThrottle since it cannot be true

		if throughputImproved(previousState, state) || rm.cpuMaxedout(previousState, state) {
			// stop waiting and start increasing throttling
			atomic.StoreInt32(&rm.waitCount, 0)
			state.targetRatio = incrementRatio(currentRatio, previousState.targetRatio, base.ResourceManagementRatioIncrement)
			state.throttleAction = ThrottleActionIncrease
		} else {
			// throughput stats has not changed or throughput still has not improved

			var waitCount int32
			if throughputStatsChanged(state) {
				// increment waitCount only if stats has changed
				waitCount = atomic.AddInt32(&rm.waitCount, 1)
			} else {
				waitCount = atomic.LoadInt32(&rm.waitCount)
			}

			if waitCount > int32(base.MaxCountWaitPeriod) {
				// max count for wait period reached. start decreasing throttling
				// note that this cannot happen when throughput stats did not change
				atomic.StoreInt32(&rm.waitCount, 0)
				atomic.StoreInt32(&rm.decreaseThrottlingCount, 1)
				state.targetRatio = decrementRatio(currentRatio, previousState.targetRatio, base.ResourceManagementRatioDecrement)
				state.throttleAction = ThrottleActionDecrease
			} else {
				// continue to wait. leave throughput limit as is
				state.targetRatio = previousState.targetRatio
				state.throughputLimit = previousState.throughputLimit
				state.throttleAction = ThrottleActionWait
				return
			}

		}
	case ThrottleActionDecrease:
		// last action could be decrease in two scenarios:
		// 1. there was no backlog before
		// 2. there was backlog, and throttling had not been effective, and we were trying decreasing throttling
		// These two scenarios can be differentiated by firstThrottle flag

		if firstThrottle {
			// in scenario 1, increase throttling [by a lot]
			state.targetRatio = incrementRatio(currentRatio, previousState.targetRatio, base.ResourceManagementRatioFirstIncrement)
			state.throttleAction = ThrottleActionIncrease
		} else {
			// in scenario 2, check whether decreasing throttling had had negative impact
			if throughputDegraded(previousState, state) || rm.cpuMaxedout(previousState, state) {
				// stop decreasing throttling and start increasing throttling
				atomic.StoreInt32(&rm.decreaseThrottlingCount, 0)
				state.targetRatio = incrementRatio(currentRatio, previousState.targetRatio, base.ResourceManagementRatioIncrement)
				state.throttleAction = ThrottleActionIncrease
			} else {
				if !throughputStatsChanged(state) {
					// stats has not changed. we do not know whether decreasing throttling had negative impact
					// keep previous throughput limit as is
					// keep decreaseThrottlingCount as is
					state.targetRatio = previousState.targetRatio
					state.throughputLimit = previousState.throughputLimit
					state.throttleAction = ThrottleActionDecrease
					return
				} else {
					// continue decreasing throttling
					decreaseThrottlingCount := atomic.AddInt32(&rm.decreaseThrottlingCount, 1)
					if decreaseThrottlingCount > int32(base.MaxCountThrottlingDecrease) {
						// reached threshold for decreasing throttling
						// go back to increasing throttling again
						atomic.StoreInt32(&rm.decreaseThrottlingCount, 0)
						state.targetRatio = incrementRatio(currentRatio, previousState.targetRatio, base.ResourceManagementRatioIncrement)
						state.throttleAction = ThrottleActionIncrease
					} else {
						// continue descreasing throttling
						state.targetRatio = decrementRatio(currentRatio, previousState.targetRatio, base.ResourceManagementRatioDecrement)
						state.throttleAction = ThrottleActionDecrease
					}
				}
			}
		}
	}

	computeThroughputLimit(state)

	return
}

func (rm *ResourceManager) computeActionsToTakeWithOneGroup(state *State) {
	// when there is at most one group of replications
	// 1. there should be no throttling actions. throughput limit will be set to 0
	// 2. there should be no dcp priority setting. set dcp priority back to medium

	// use "-1" to indicate that targetRatio is not applicable
	state.targetRatio = -1
	state.throughputLimit = 0
	state.throttleAction = ThrottleActionNone
	state.dcpPriorityAction = DcpPriorityActionReset
}

func incrementRatio(currentRatio, previousTargetRatio, delta int) int {
	return computeNewRatio(currentRatio, previousTargetRatio, delta, true /*incrementing*/, base.ResourceManagementRatioLowerBound, base.ResourceManagementRatioUpperBound)
}

func decrementRatio(currentRatio, previousTargetRatio, delta int) int {
	return computeNewRatio(currentRatio, previousTargetRatio, delta, false /*incrementing*/, base.ResourceManagementRatioLowerBound, base.ResourceManagementRatioUpperBound)
}

// change ratio to min(currentRatio, previousTargetRatio) +/- delta
// make sure that the resulting ratio stays within [lowerBound, upperBound]
func computeNewRatio(currentRatio, previousTargetRatio, delta int, incrementing bool, lowerBound int, upperBound int) int {
	newRatio := currentRatio

	// set base ratio as the larger of currentRatio and previousTargetRatio to be conservative
	if previousTargetRatio > 0 && newRatio < previousTargetRatio {
		newRatio = previousTargetRatio
	}

	if incrementing {
		newRatio += delta
	} else {
		newRatio -= delta
	}
	if newRatio > upperBound {
		newRatio = upperBound
	}
	// lowerBound <= 0 indicates that lowerBound does not exist
	// Note that newRatio could be < 0 if lowerBound does not exist
	// When throughputs of high priority replications are really low,
	// it is necessary for newRatio to be negative for throughput limit to be effectively increased
	if lowerBound > 0 && newRatio < lowerBound {
		newRatio = lowerBound
	}

	return newRatio
}

// returns whether throughput stats has changed since last interval
func throughputStatsChanged(state *State) bool {
	return state.highThroughputStatsChanged || state.backlogThroughputStatsChanged
}

func throughputImproved(previousState *State, state *State) bool {
	return state.highThroughput > previousState.highThroughput ||
		state.backlogThroughput > previousState.backlogThroughput
}

func throughputDegraded(previousState *State, state *State) bool {
	return state.highThroughput < previousState.highThroughput ||
		state.backlogThroughput < previousState.backlogThroughput
}

func computeThroughputLimit(state *State) {
	state.throughputLimit = state.overallThroughput - state.overallThroughput*int64(state.targetRatio)/int64(base.ResourceManagementRatioBase)
}

func (rm *ResourceManager) takeActions(specs map[string]*metadata.ReplicationSpecification,
	previousState *State, state *State) {
	rm.setThroughputLimit(previousState, state)

	switch state.dcpPriorityAction {
	case DcpPriorityActionSet:
		rm.setDcpPriorities(specs, state)
	case DcpPriorityActionReset:
		rm.resetDcpPriorities(state)
	default:
		// no op for ActionNone
	}
}

func (rm *ResourceManager) setThroughputLimit(previousState *State, state *State) {
	// -1 indicates that there is no previous limit
	var previousThroughputLimit int64 = -1
	if previousState != nil {
		previousThroughputLimit = previousState.throughputLimit
	}

	if state.throughputLimit == previousThroughputLimit {
		// skip setting throughput limit if it is the same as the previous limit
		// we will never get here when previous limit is -1
		return
	}

	err := rm.throughputThrottlerSvc.SetThroughputLimit(state.throughputLimit)
	if err != nil {
		rm.logger.Warnf("Error setting throughput limit to %v. err=%v", state.throughputLimit, err)
		// keep the previous limit
		// previous limit could be -1, which would still work
		state.throughputLimit = previousThroughputLimit
	}
}

func (rm *ResourceManager) setDcpPriorities(specs map[string]*metadata.ReplicationSpecification, state *State) {
	rm.mapLock.Lock()
	defer rm.mapLock.Unlock()

	for replId, _ := range state.replStatsMap {
		var targetPriority mcc.PriorityType
		spec, ok := specs[replId]
		if !ok {
			// should never get here
			rm.logger.Warnf("Skipping setting dcp priority for %v because of error retrieving replication spec", replId)
			continue
		}
		if rm.IsReplHighPriority(spec.Id, spec.Settings.GetPriority()) {
			targetPriority = mcc.PriorityHigh
		} else {
			targetPriority = mcc.PriorityLow
		}

		if currentPriority, ok := rm.replDcpPriorityMap[replId]; ok && currentPriority == targetPriority {
			// no op if the current set priority is the same as the target value
			continue
		}

		err := rm.setDcpPriority(replId, targetPriority)
		if err == nil {
			rm.replDcpPriorityMap[replId] = targetPriority
		} else {
			rm.logger.Warnf("Error setting dcp priority for %v to %v. err=%v\n", replId, targetPriority, err)
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

	for replId, _ := range state.replStatsMap {
		if currentPriority, ok := rm.replDcpPriorityMap[replId]; !ok || currentPriority == targetPriority {
			// no op if dcp priority has not been set before, or the set value is the same as the target value
			continue
		}

		err := rm.setDcpPriority(replId, targetPriority)
		if err == nil {
			rm.replDcpPriorityMap[replId] = targetPriority
		} else {
			rm.logger.Warnf("Error setting dcp priority for %v to %v. err=%v\n", replId, targetPriority, err)
			continue
		}
	}
}

func (rm *ResourceManager) isReplOngoing(replId string) bool {
	rm.mapLock.RLock()
	defer rm.mapLock.RUnlock()
	if ongoing, ok := rm.ongoingReplMap[replId]; ok && ongoing {
		return true
	} else {
		return false
	}
}

func (rm *ResourceManager) setReplOngoing(spec *metadata.ReplicationSpecification) error {
	if rm.isReplOngoing(spec.Id) {
		// replication is already ongoing. no op
		return nil
	}

	if spec.Settings.GetPriority() == base.PriorityTypeMedium {
		// change "needToThrottle" flag on Medium replication
		settings := make(map[string]interface{})
		settings[parts.NeedToThrottleKey] = false

		err := rm.applySettingsToPipeline(spec.Id, settings)
		if err != nil {
			// do not add repl to ongoingReplMap
			// we will get another chance to repeat this op in the next interval
			rm.logger.Warnf("Skipping changing needToThrottle setting for %v due to err=%v.", spec.Id, err)
			return err
		}
	}

	rm.addReplToOngoingReplMap(spec.Id)
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

func (rm *ResourceManager) applySettingsToPipeline(replId string, settings map[string]interface{}) error {
	rs, err := rm.pipelineMgr.ReplicationStatus(replId)
	if err != nil {
		return fmt.Errorf("Skipping updating settings for %v because of error retrieving replication status. settings=%v, err=%v\n", replId, settings, err)
	}

	// set custom settings on replStatus so that settings can be automatically re-applied after pipeline restart
	rs.SetCustomSettings(settings)

	pipeline := rs.Pipeline()
	if pipeline == nil {
		return fmt.Errorf("Skipping updating settings for %v because of nil pipeline. err=%v\n", replId)
	}
	// apply the setting to the live pipeline
	return pipeline.UpdateSettings(settings)
}

func (rm *ResourceManager) setDcpPriority(replId string, priority mcc.PriorityType) error {
	rm.logger.Infof("Setting dcp priority to %v for %v", priority, replId)

	settings := make(map[string]interface{})
	settings[parts.DCP_Priority] = priority

	return rm.applySettingsToPipeline(replId, settings)
}

func (rm *ResourceManager) getStatsFromReplication(spec *metadata.ReplicationSpecification) (*ReplStats, error) {
	rs, err := rm.pipelineMgr.ReplicationStatus(spec.Id)
	if err != nil {
		return nil, err
	}
	statsMap := rs.GetOverviewStats()
	if statsMap == nil {
		// this is possible when replication is starting up
		return nil, fmt.Errorf("Cannot find overview stats for %v", spec.Id)
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
	// throughput will be computer later and is temporarily set to 0 for now
	return &ReplStats{changesLeft, docsFromDcp, docsRepQueue, timestamp, 0}, nil
}
