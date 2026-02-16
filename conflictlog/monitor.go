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

// monitorState represents the current state of conflict monitoring for a replication.
type monitorState int

const (
	// monitorStateNormal indicates normal operation with no conflict rate breach
	monitorStateNormal monitorState = iota

	// monitorStateConflictRateBreached indicates the conflict rate threshold has been exceeded
	monitorStateConflictRateBreached
)

// String returns the string representation of the monitor state.
func (s monitorState) String() string {
	switch s {
	case monitorStateNormal:
		return "normal"
	case monitorStateConflictRateBreached:
		return "conflictRateBreached"
	default:
		return "unknown"
	}
}

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
//
// Note:
// A "window" is defined as an interval [windowStartTimestamp, windowStartTimestamp + windowMonitorDuration]. The number of
// conflicts observed at any instant in the current window is stored in windowConflicts.
// A "cycle" is defined as one execution of monitorConflictRateOnce. It has a frequency of 1 seconds (CLogMonitorCycleInterval).
// The idea is that a replication is eligible to be autopaused after every 1 second, eventhough the threshold number of conflicts
// is configured for a possible larger window duration. This is because we don't want to wait until the end of a window to autopause
// the replication if the threshold was breached at any instant of the window to ensure immediate QoS and reduce the chances of conflicts or
// data loss.
type stats struct {
	// timestamp is when the last cycle was executed.
	timestamp int64

	// conflicts is the total number of conflicts detected in the last cycle.
	conflicts int64

	// windowConflicts tracks the accumulated conflicts in the current monitoring window.
	windowConflicts int64

	// windowStartTimestamp marks the start time of the current monitoring window (in nanoseconds).
	windowStartTimestamp int64

	// windowThreshold is the threshold captured at the start of the current monitoring window. This
	// is so that if the corresponsing replication setting is updated in middle of an ongoing window,
	// the update will only come into effect from the next window.
	windowThreshold int

	// windowMonitorDuration is the monitor duration captured at the start of the current monitoring window.
	// This is so that if the corresponsing replication setting is updated in middle of an ongoing window,
	// the update will only come into effect from the next window.
	windowMonitorDuration int

	// state represents the current monitoring state for the replication.
	// State transitions: normal -> conflictRateBreached -> autoPaused
	// - normal: No conflict rate breach detected
	// - conflictRateBreached: Conflict rate has exceeded threshold, autopause pending/in-progress
	// - autoPaused: Replication has been successfully autopaused
	// The state spans accross the lifetime of a replication and is not restricted to a specific
	// window.
	state monitorState

	// descTime represents the number of monitoring cycles in which the timestamp
	// read was non-monotonic.
	descTime uint64

	// descConflicts represents the number of monitoring cycles in which the number of
	// conflicts stat was non-monotonic.
	descConflicts uint64

	// lastErr is the last seen error while monitoring for a
	// given replication.
	lastErr error

	// errCnt is the total number of errors seen while monitoring
	// for a given replication.
	errCnt uint64
}

// String returns the stringer representation of s.
func (s *stats) String() string {
	if s == nil {
		return "<nil>"
	}

	return fmt.Sprintf("{conflicts=%v,windowStart=%v,currentWindow=%v,errCnt=%v,lastErr=%v,state=%v,desc=(%v,%v),threshold=%v,duration=%v} ",
		s.conflicts, s.windowStartTimestamp, s.windowConflicts, s.errCnt, s.lastErr, s.state, s.descTime, s.descConflicts, s.windowThreshold, s.windowMonitorDuration)
}

// resetError resets s.lastErr to be set to nil.
func (s *stats) resetError() {
	if s == nil {
		return
	}

	s.lastErr = nil
}

// startNewWindow initializes or resets the monitoring window of size monitorDuration.
func (s *stats) startNewWindow(startTimestamp int64, initialConflicts int64, threshold int, monitorDuration int) {
	if s == nil {
		return
	}

	s.windowStartTimestamp = startTimestamp
	s.windowConflicts = initialConflicts
	s.windowThreshold = threshold
	s.windowMonitorDuration = monitorDuration
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
	for uniqueTopic, stat := range m.statsToMonitor {
		sb.WriteString(uniqueTopic)
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

	ticker := time.NewTicker(base.CLogMonitorCycleInterval)
	defer ticker.Stop()

	for range ticker.C {
		monitored++
		if monitored%base.CLogMonitorCleanupFreq == 0 {
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

// newStat is to be called when a replication with uniqueTopic doesn't have a stat
// initialised yet. It initializes both the basic stats and the monitoring window.
// If uniqueTopic already has an entry, this function is a no-op.
func (m *monitorImpl) newStat(uniqueTopic string, timestamp int64, conflicts int64, threshold int, monitorDuration int) {
	stat, ok := m.statsToMonitor[uniqueTopic]
	if !ok || stat == nil {
		stat = &stats{
			timestamp: timestamp,
			conflicts: conflicts,
			state:     monitorStateNormal,
		}

		// Initialise a window of size monitorDuration.
		stat.startNewWindow(timestamp, 0, threshold, monitorDuration)
		m.statsToMonitor[uniqueTopic] = stat
	}
}

// handleErr sets the stats when a non-nil err is passed.
// uniqueTopic should already have an entry in m.statsToMonitor and err should be non nil.
func (m *monitorImpl) handleErr(uniqueTopic string, timestamp int64, conflicts int64, err error) {
	if err == nil {
		// Breaks the pre-requisite. Don't continue.
		return
	}

	stat, ok := m.statsToMonitor[uniqueTopic]
	if !ok || stat == nil {
		// Breaks the pre-requisite. Don't continue.
		return
	}

	switch {
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

	var (
		timestamp   = time.Now().UnixNano()
		uniqueTopic = spec.UniqueId()
		topic       = spec.Id
	)

	conflictLoggingOff := spec.Settings.GetConflictLoggingMapping().Disabled()
	threshold := spec.Settings.GetCLogPauseReplThreshold()
	monitorDuration := spec.Settings.GetCLogMonitorDuration()
	autoPauseOff := threshold == 0 || monitorDuration == 0
	if conflictLoggingOff || autoPauseOff {
		// Ensure that old monitoring stat if any is deleted.
		// If it's not present delete is a no-op anyways.
		delete(m.statsToMonitor, uniqueTopic)
		return
	}

	conflicts, err := m.Monitor.GetConflictsCount(topic)
	switch {
	case err == nil:
	case errors.Is(err, base.ErrorReplicationSpecNotActive):
		// Ensure the previous monitoring states are deleted for new
		// monitoring sessions in the future.
		delete(m.statsToMonitor, uniqueTopic)
		return
	default:
		err = fmt.Errorf("error getting conflict count: %w", err)
		m.handleErr(uniqueTopic, timestamp, 0, err)
		return
	}

	stat, ok := m.statsToMonitor[uniqueTopic]
	if !ok || stat == nil {
		// This is the first cycle of monitoring for this replication.
		m.newStat(uniqueTopic, timestamp, conflicts, threshold, monitorDuration)
		return
	}

	if stat.state == monitorStateNormal {
		// Validate time monotonicity and conflict counter monotonicity as we have
		// monitored for this replication in the past.
		if timestamp <= stat.timestamp {
			// Time went backwards or is the same as the last cycle. Do not consider this
			// sample for the current cycle.
			m.handleErr(uniqueTopic, timestamp, conflicts, baseclog.ErrDescTime)
			return
		}

		// Add this cycle's conflicts to the current window
		delta := conflicts - stat.conflicts
		if delta < 0 {
			// Monotonic counter went backwards. Do not consider this sample for the current cycle.
			// delta of 0 is an acceptable value though.
			m.handleErr(uniqueTopic, timestamp, conflicts, baseclog.ErrDescConflictCounter)
			return
		}

		stat.windowConflicts += delta

		// Use the threshold that was captured at window start. If the state is
		// already monitorStateConflictRateBreached, we don't need to transition
		// to a new state based on conflicts count.
		if stat.windowConflicts >= int64(stat.windowThreshold) {
			stat.state = monitorStateConflictRateBreached
			m.logger.Infof("%s: The replication will be autopaused due to high conflict count, which is greater than the set threshold, windowConflicts=%v, windowThreshold=%v, windowMonitorDuration=%v",
				uniqueTopic, stat.windowConflicts, stat.windowThreshold, stat.windowMonitorDuration)
		}
	}

	m.logger.Tracef("%s: windowConflicts=%v, windowThreshold=%v, state=%v", topic, stat.windowConflicts, stat.windowThreshold, stat.state)
	if stat.state == monitorStateConflictRateBreached {
		uiMsg := fmt.Sprintf(AutoPauseErrUIStr, topic, stat.windowConflicts, stat.windowThreshold, stat.windowMonitorDuration)
		err = m.autopauseReplication(topic, uniqueTopic, uiMsg)
		if err != nil {
			m.handleErr(uniqueTopic, timestamp, conflicts, err)
			return
		}
	} else {
		// Check if monitoring window has expired.
		windowElapsed := time.Duration(timestamp - stat.windowStartTimestamp).Seconds()
		if windowElapsed >= float64(stat.windowMonitorDuration) {
			// Window expired. Start a new window from the last successful monitoring point
			// to ensure no time gaps between windows.
			// Next cycle will be a fresh window for this replication.
			stat.startNewWindow(stat.timestamp, 0, threshold, monitorDuration)
		}

		// This cycle was successful and conflict count is below threshold.
		// Store the stats for baseline of next cycle.
		stat.timestamp = timestamp
		stat.conflicts = conflicts
	}
}

// autopauseReplication pauses the replication with input topic and notifies the frontend
// with the reason for autopause.
func (m *monitorImpl) autopauseReplication(topic, uniqueTopic string, reason string) error {
	err := m.Monitor.AutoPauseReplication(topic)
	if err != nil {
		// could not pause in this cycle, will retry in next cycle
		return fmt.Errorf("error autopausing the replication %v: %w", uniqueTopic, err)
	}

	// Delete the monitoring metadata for this replication to indicate that
	// the replication has been paused. This way if the replication is then resumed
	// a new monitoring session will start.
	delete(m.statsToMonitor, uniqueTopic)

	// Raise an error to alert the frontend. We will try this only once and if it fails
	// we will ignore it.
	_, err = m.Monitor.RaiseUIError(topic, reason)
	if err != nil {
		return fmt.Errorf("error displaying ui message after autopausing %v: %w", uniqueTopic, err)
	}

	return nil
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
