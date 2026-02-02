/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package conflictlog

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	baseclog "github.com/couchbase/goxdcr/v8/base/conflictlog"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
)

var _ baseclog.Monitor = (*monitorImpl)(nil)

// monitorImpl is the concrete implementation of the Monitor interface.
type monitorImpl struct {
	// Monitor takes care of the core monitoring functionalaties and takes actions based
	// of conflicts detected by all replications in the system during conflict logging.
	// The Monitor will only be initialised after pipeline manager is initialised.
	baseclog.Monitor

	// monitorOnce helps to start the monitoring exactly once.
	monitorOnce sync.Once

	// replSpecSvc enables the monitor to have a global view of all replications
	// in the process.
	replSpecSvc service_def.ReplicationSpecReader

	// statsToMonitor tracks the latest monitoring stats observed
	// across all active replications in the system.
	// The key should be the unique ID of a replication specification
	// to ensure that the right stats are tracked accross replication recreations.
	// It's access is not currently thread-safe as there is only one
	// monitoring thread per process, sequentially accessing it at the moment.
	statsToMonitor map[string]*stats

	// logger helps log necessary information needed for debugging.
	logger *log.CommonLogger
}

// stats represents the statistics of a given replication to be monitored
// by the conflict manager.
type stats struct {
	// timestamp is the last updated timestamp.
	timestamp int64

	// conflicts is the total number of conflicts detected.
	conflicts int64

	// lastErr is the last seen error while monitoring for a
	// given replication.
	lastErr error

	// errCnt is the total number of errors seen while monitoring
	// for a given replication.
	errCnt uint64

	// conflictRateBreached represents if the conflict rate for the given
	// replication has exceeded threshold, across the lifecycle of an active
	// replication pipeline.
	// When conflictRateBreached is true, autopaused should be true. If it's not
	// true, it means that we have breached the threshold, but there's some trouble
	// autopausing the replication.
	conflictRateBreached bool

	// autopaused represents that the replication is already autopaused
	// successfully. conflictRateBreached will always be true if autopaused
	// is true.
	autopaused bool

	// descTime represents the number of monitoring cycles in which the timestamp
	// read was non-monotonic.
	descTime uint64

	// descConflicts represents the number of monitoring cycles in which the number of
	// conflicts stat was non-monotonic.
	descConflicts uint64
}

// String returns the stringer representation of s.
func (s *stats) String() string {
	if s == nil {
		return "<nil>"
	}

	return fmt.Sprintf("{conflicts=%v,errCnt=%v,lastErr=%v,breach=%v,paused=%v,desc=(%v,%v)} ",
		s.conflicts, s.errCnt, s.lastErr, s.conflictRateBreached, s.autopaused, s.descTime, s.descConflicts)
}

// resetError resets s.lastErr to be set to nil.
func (s *stats) resetError() {
	if s == nil {
		return
	}

	s.lastErr = nil
}

// Start initiates the monitoring process.
func (m *monitorImpl) Start() {
	m.monitorOnce.Do(func() {
		// This is a ever running go-routine.
		go m.monitorConflictRate()
	})
}

// monitorStatsLogStr returns a string representation of monitored stats for
// logging purposes.
func (m *monitorImpl) monitorStatsLogStr() string {
	if len(m.statsToMonitor) == 0 {
		return "{}"
	}

	sb := strings.Builder{}
	for fullTopic, stat := range m.statsToMonitor {
		sb.WriteString(fullTopic)
		sb.WriteByte(':')
		sb.WriteString(stat.String())
	}

	return sb.String()
}

// resetErrors resets the last error seen for monitoring each replication.
func (m *monitorImpl) resetErrors() {
	for _, stat := range m.statsToMonitor {
		stat.resetError()
	}
}

// monitorConflictRate monitors the conflict rate of all replications in the process, which have conflict logging on
// and takes action based on it.
func (m *monitorImpl) monitorConflictRate() {
	m.logger.Info("starting conflict manager monitor routine")
	defer m.logger.Fatal("stopping conflict manager monitor routine")

	var (
		monitored uint64
		lastErr   error
		errCnt    uint64
	)

	ticker := time.NewTicker(base.ResourceManagementInterval)
	defer ticker.Stop()

	for range ticker.C {
		monitored++
		if monitored%MonitorCleanupFreq == 0 {
			// Take this opportunity to log some information for debugging before cleanup.
			m.logger.Infof("monitored: %v, errCnt: %v, lastErr: %v, details: %s",
				monitored, errCnt, lastErr, m.monitorStatsLogStr())
			m.resetErrors()
			lastErr = nil

			// Also cleanup unwanted (replications not active anymore) monitoring
			// stats which are still in the map.
			lastErr = m.cleanupMonitoredStats()
			if lastErr != nil {
				m.logger.Debugf("error cleaning up monitoring conflict rate: %v", lastErr)
				errCnt++
			}
		}

		lastErr = m.monitorConflictRateOnce()
		if lastErr != nil {
			m.logger.Debugf("error monitoring conflict rate: %v", lastErr)
			errCnt++
		}
	}
}

// updateStat updates the monitoring stats map for the given fullTopic, with the input stats.
func (m *monitorImpl) updateStat(fullTopic string, timestamp int64, conflicts int64, conflictRateBreached bool, autopaused bool, err error) {
	stat, ok := m.statsToMonitor[fullTopic]
	if !ok || stat == nil {
		stat = &stats{
			timestamp: timestamp,
			conflicts: conflicts,
		}
		m.statsToMonitor[fullTopic] = stat
	}

	switch {
	case err == nil:
		stat.timestamp = timestamp
		stat.conflicts = conflicts
		stat.conflictRateBreached = conflictRateBreached
		stat.autopaused = autopaused
	case errors.Is(err, baseclog.ErrDescConflictCounter):
		// record timestamp and conflicts as a baseline for next monitoring cycle.
		stat.timestamp = timestamp
		stat.conflicts = conflicts
		stat.descConflicts++
	case errors.Is(err, baseclog.ErrDescTime):
		// record timestamp and conflicts as a baseline for next monitoring cycle.
		stat.timestamp = timestamp
		stat.conflicts = conflicts
		stat.descTime++
	default:
		stat.lastErr = err
		stat.errCnt++
	}
}

// monitorConflictRateOnce is a helper function which represents a single cycle of monitorConflictRate.
// For each active replication, it computes the conflict rate in this cycle and autopauses the replication
// if the conflict rate computed exceeds the set threshold.
func (m *monitorImpl) monitorConflictRateOnce() error {
	specs, err := m.replSpecSvc.AllActiveReplicationSpecsReadOnly()
	if err != nil {
		return fmt.Errorf("error getting all specs: %w", err)
	}

	for _, spec := range specs {
		m.monitorOneReplicationOnce(spec)
	}

	return nil
}

// monitorOneReplicationOnce executes once cycle of monitorConflictRateOnce for one single
// replication with the input spec. This function doesn't return any error because any errors
// are recorded in the monitoring stats state.
func (m *monitorImpl) monitorOneReplicationOnce(spec *metadata.ReplicationSpecification) {
	if spec == nil {
		return
	}

	conflictLoggingOff := spec.Settings.GetConflictLoggingMapping().Disabled()
	autoPauseOff := spec.Settings.GetConflictRateToPauseRepl() == 0
	if conflictLoggingOff || autoPauseOff {
		return
	}

	var (
		newTimestamp = time.Now().UnixNano()
		threshold    = float64(spec.Settings.GetConflictRateToPauseRepl())
		fullTopic    = spec.UniqueId()
		topic        = spec.Id

		deltaSeconds   float64
		deltaConflicts float64
	)

	newConflictCnt, err := m.Monitor.GetConflictsCount(topic)
	if err != nil {
		err = fmt.Errorf("error getting conflict count: %w", err)
		m.updateStat(fullTopic, newTimestamp, 0, false, false, err)
		return
	}

	oldStats, ok := m.statsToMonitor[fullTopic]
	if !ok || oldStats == nil {
		// This is probably the first cycle for this replication.
		m.updateStat(fullTopic, newTimestamp, newConflictCnt, false, false, nil)
		return
	}

	delta := time.Duration(newTimestamp - oldStats.timestamp).Seconds()
	if delta > 0 {
		// delta of 0 is not an acceptable value for the denominator.
		deltaSeconds = delta
	} else {
		// Time went backwards or is the same as the last cycle. Do not consider this
		// sample for the current cycle.
		m.updateStat(fullTopic, newTimestamp, newConflictCnt, oldStats.conflictRateBreached, oldStats.autopaused, baseclog.ErrDescTime)
		return
	}

	delta = float64(newConflictCnt - oldStats.conflicts)
	if delta >= 0 {
		// delta of 0 is an acceptable value for the numerator.
		deltaConflicts = delta
	} else {
		// Monotonic counter went backwards. Do not consider this sample for the current cycle.
		m.updateStat(fullTopic, newTimestamp, newConflictCnt, oldStats.conflictRateBreached, oldStats.autopaused, baseclog.ErrDescConflictCounter)
		return
	}

	var (
		conflictRate         = deltaConflicts / deltaSeconds
		autopaused           = oldStats.autopaused
		conflictRateBreached = oldStats.conflictRateBreached || (conflictRate >= threshold)
	)

	m.logger.Tracef("%s: conflictRate=%v, autopaused=%v, breached=%v", topic, conflictRate, autopaused, conflictRateBreached)
	if conflictRateBreached && !autopaused {
		m.logger.Infof("%s: The replication will be autopaused due to high conflict rate, which is greater than the set threshold, conflictRate=%v, threshold=%v",
			fullTopic, conflictRate, threshold)
		err = m.Monitor.AutoPauseReplication(topic)
		if err != nil {
			// could not pause in this cycle, will retry in next cycle
			err = fmt.Errorf("error autopausing the replication %v: %w", fullTopic, err)
			m.updateStat(fullTopic, newTimestamp, newConflictCnt, true, false, err)
			return
		}

		autopaused = true
		uiMsg := fmt.Sprintf(AutoPauseErrUIStr, topic, int(conflictRate), int(threshold))
		_, err = m.Monitor.RaiseUIError(topic, uiMsg)
		if err != nil {
			err = fmt.Errorf("error displaying ui message after autopausing %v: %w", fullTopic, err)
			m.updateStat(fullTopic, newTimestamp, newConflictCnt, true, true, err)
			return
		}
	}

	m.updateStat(fullTopic, newTimestamp, newConflictCnt, conflictRateBreached, autopaused, nil)
}

// cleanupMonitoredStats removes all the replication entries from the monitoring stats map, which
// are not active anymore in the system.
func (m *monitorImpl) cleanupMonitoredStats() error {
	if len(m.statsToMonitor) == 0 {
		// nothing to clean
		return nil
	}

	specs, err := m.replSpecSvc.AllActiveReplicationSpecsReadOnly()
	if err != nil {
		return fmt.Errorf("error getting all specs for gc: %w", err)
	}

	activeReplIds := map[string]bool{}
	for _, spec := range specs {
		activeReplIds[spec.UniqueId()] = true
	}

	for specId := range m.statsToMonitor {
		_, ok := activeReplIds[specId]
		if !ok {
			delete(m.statsToMonitor, specId)
		}
	}

	return nil
}
