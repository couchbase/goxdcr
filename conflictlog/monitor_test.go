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
	"testing"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	baseclogmocks "github.com/couchbase/goxdcr/v8/base/conflictlog/mocks"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	service_def "github.com/couchbase/goxdcr/v8/service_def/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// createTestSpec creates a test replication specification with the given parameters
func createTestSpec(topic string, conflictThreshold int, monitorDuration int, conflictLoggingEnabled bool) *metadata.ReplicationSpecification {
	spec := &metadata.ReplicationSpecification{}
	spec.Id = topic
	spec.InternalId = "test-internal-id"

	settings := metadata.DefaultReplicationSettings()
	if conflictLoggingEnabled {
		// Explicitly enable conflict logging by setting a valid mapping
		settings.Values[base.CLogKey] = map[string]interface{}{
			"enabled": true,
		}
	} else {
		// Set conflict logging to disabled
		settings.Values[base.CLogKey] = false
	}
	// Using new count-based settings: both threshold and duration must be > 0 for autopause
	settings.Values[metadata.CLogPauseReplThresholdKey] = conflictThreshold
	settings.Values[metadata.CLogMonitorDurationKey] = monitorDuration
	spec.Settings = settings

	return spec
}

// requireStat retrieves a stat and fails the test if not found
func requireStat(t *testing.T, m *monitorImpl, key string) *stats {
	stat := m.statsToMonitor[key]
	if !assert.NotNil(t, stat, "stat should not be nil for key: %s", key) {
		t.FailNow()
	}
	return stat
}

// dontRequireStat retrieves a stat and fails the test if found
func dontRequireStat(t *testing.T, m *monitorImpl, key string) {
	stat := m.statsToMonitor[key]
	if !assert.Nil(t, stat, "stat should be nil (not present) for key: %s", key) {
		t.FailNow()
	}
}

// setupMonitorMocks creates and configures the mocks needed for testing monitorOneReplicationOnce
func setupMonitorMocks() (*baseclogmocks.Monitor, *log.CommonLogger) {
	mockMon := &baseclogmocks.Monitor{}
	logger := log.NewLogger(ConflictManagerLoggerName, log.DefaultLoggerContext)
	return mockMon, logger
}

func TestMonitorOneReplicationOnce(t *testing.T) {
	tests := []struct {
		name                  string
		spec                  *metadata.ReplicationSpecification
		existingStats         *stats
		mockGetConflicts      int64
		mockGetConflictsErr   error
		mockAutopauseErr      error
		mockUIErr             error
		expectAutopauseCalled bool
		expectUIErrorCalled   bool
		validateFunc          func(t *testing.T, m *monitorImpl, topic string)
	}{
		// Early returns - spec validation
		{
			name:             "NilSpec_NoProcessing",
			spec:             nil,
			mockGetConflicts: 0,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				assert.Empty(t, m.statsToMonitor)
			},
		},
		{
			name:             "ConflictLoggingDisabled_NoProcessing",
			spec:             createTestSpec("repl1", 10, 60, false),
			mockGetConflicts: 0,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				assert.Empty(t, m.statsToMonitor)
			},
		},
		{
			name:             "ZeroThreshold_AutopauseDisabled",
			spec:             createTestSpec("repl1", 0, 60, true),
			mockGetConflicts: 0,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				assert.Empty(t, m.statsToMonitor)
			},
		},
		{
			name:             "ZeroDuration_AutopauseDisabled",
			spec:             createTestSpec("repl1", 100, 0, true),
			mockGetConflicts: 0,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				assert.Empty(t, m.statsToMonitor)
			},
		},

		// GetConflictsCount error handling
		{
			name:                "GetConflictsError_FirstCycle_NoStatCreated",
			spec:                createTestSpec("repl1", 100, 60, true),
			mockGetConflicts:    0,
			mockGetConflictsErr: errors.New("connection error"),
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				assert.Empty(t, m.statsToMonitor)
			},
		},
		{
			name: "GetConflictsError_ExistingStat_ErrorRecorded",
			spec: createTestSpec("repl1", 100, 60, true),
			existingStats: &stats{
				timestamp:             time.Now().Add(-1 * time.Second).UnixNano(),
				conflicts:             100,
				windowStartTimestamp:  time.Now().Add(-5 * time.Second).UnixNano(),
				windowConflicts:       20,
				windowThreshold:       100,
				windowMonitorDuration: 60,
			},
			mockGetConflicts:    0,
			mockGetConflictsErr: errors.New("timeout"),
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				stat := requireStat(t, m, topic)
				assert.NotNil(t, stat.lastErr)
				assert.Contains(t, stat.lastErr.Error(), "error getting conflict count")
				assert.Equal(t, uint64(1), stat.errCnt)
			},
		},

		// First cycle - stat initialization
		{
			name:             "FirstCycle_InitializesStatAndWindow",
			spec:             createTestSpec("repl1", 50, 30, true),
			mockGetConflicts: 25,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				stat := requireStat(t, m, topic)
				assert.Equal(t, int64(25), stat.conflicts)
				assert.Greater(t, stat.timestamp, int64(0))
				assert.Equal(t, monitorStateNormal, stat.state)
				assert.Equal(t, 50, stat.windowThreshold)
				assert.Equal(t, 30, stat.windowMonitorDuration)
				// windowConflicts should be set to initial count on first cycle
				assert.Equal(t, int64(25), stat.windowConflicts)
				assert.Equal(t, stat.windowStartTimestamp, stat.timestamp)
			},
		},

		// Time monotonicity validation
		{
			name: "NonMonotonicTime_ErrorRecorded_NoUpdate",
			spec: createTestSpec("repl1", 100, 60, true),
			existingStats: &stats{
				timestamp:             time.Now().Add(1 * time.Hour).UnixNano(),
				conflicts:             100,
				windowStartTimestamp:  time.Now().Add(-10 * time.Second).UnixNano(),
				windowConflicts:       30,
				windowThreshold:       100,
				windowMonitorDuration: 60,
			},
			mockGetConflicts: 110,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				stat := requireStat(t, m, topic)
				assert.Equal(t, uint64(1), stat.descTime)
				assert.Equal(t, int64(110), stat.conflicts)
				assert.Equal(t, int64(30), stat.windowConflicts) // Not updated
			},
		},

		// Conflict counter monotonicity validation
		{
			name: "NonMonotonicConflicts_ErrorRecorded_NoUpdate",
			spec: createTestSpec("repl1", 100, 60, true),
			existingStats: &stats{
				timestamp:             time.Now().Add(-1 * time.Second).UnixNano(),
				conflicts:             200,
				windowStartTimestamp:  time.Now().Add(-10 * time.Second).UnixNano(),
				windowConflicts:       50,
				windowThreshold:       100,
				windowMonitorDuration: 60,
			},
			mockGetConflicts: 150,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				stat := requireStat(t, m, topic)
				assert.Equal(t, uint64(1), stat.descConflicts)
				assert.Equal(t, int64(150), stat.conflicts)
				assert.Equal(t, int64(50), stat.windowConflicts) // Not updated
			},
		},

		// Normal operation - within window
		{
			name: "WithinWindow_ConflictsAccumulate",
			spec: createTestSpec("repl1", 100, 60, true),
			existingStats: &stats{
				timestamp:             time.Now().Add(-2 * time.Second).UnixNano(),
				conflicts:             100,
				windowStartTimestamp:  time.Now().Add(-10 * time.Second).UnixNano(),
				windowConflicts:       30,
				windowThreshold:       100,
				windowMonitorDuration: 60,
				state:                 monitorStateNormal,
			},
			mockGetConflicts: 125,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				stat := requireStat(t, m, topic)
				assert.Equal(t, int64(125), stat.conflicts)
				assert.Equal(t, int64(55), stat.windowConflicts) // 30 + 25
				assert.Equal(t, monitorStateNormal, stat.state)
			},
		},
		{
			name: "WithinWindow_ZeroDelta_NoChange",
			spec: createTestSpec("repl1", 50, 60, true),
			existingStats: &stats{
				timestamp:             time.Now().Add(-1 * time.Second).UnixNano(),
				conflicts:             100,
				windowStartTimestamp:  time.Now().Add(-5 * time.Second).UnixNano(),
				windowConflicts:       20,
				windowThreshold:       50,
				windowMonitorDuration: 60,
			},
			mockGetConflicts: 100,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				stat := requireStat(t, m, topic)
				assert.Equal(t, int64(20), stat.windowConflicts)
			},
		},

		// Window expiry - reset and start new window
		{
			name: "WindowExpires_ThresholdBreached_Autopaused",
			spec: createTestSpec("repl1", 100, 5, true),
			existingStats: &stats{
				timestamp:             time.Now().Add(-6 * time.Second).UnixNano(),
				conflicts:             100,
				windowStartTimestamp:  time.Now().Add(-6 * time.Second).UnixNano(),
				windowConflicts:       80,
				windowThreshold:       100,
				windowMonitorDuration: 5,
			},
			mockGetConflicts:      150, // delta=50, total=80+50=130 >= 100, triggers autopause
			expectAutopauseCalled: true,
			expectUIErrorCalled:   true,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				// Should autopause because 130 >= 100, then delete stat
				dontRequireStat(t, m, topic)
			},
		},
		{
			name: "WindowExpires_BelowThreshold_ResetsToNewWindow",
			spec: createTestSpec("repl1", 200, 5, true),
			existingStats: &stats{
				timestamp:             time.Now().Add(-6 * time.Second).UnixNano(),
				conflicts:             100,
				windowStartTimestamp:  time.Now().Add(-6 * time.Second).UnixNano(),
				windowConflicts:       80,
				windowThreshold:       200, // Higher threshold
				windowMonitorDuration: 5,
			},
			mockGetConflicts: 150, // delta=50, total=80+50=130 < 200
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				stat := requireStat(t, m, topic)
				// Window expired below threshold - new window starts fresh at 0
				assert.Equal(t, int64(0), stat.windowConflicts)
				assert.Equal(t, 200, stat.windowThreshold)
				assert.Equal(t, 5, stat.windowMonitorDuration)
			},
		},
		{
			name: "WindowExpires_CapturesNewSettings",
			spec: createTestSpec("repl1", 200, 10, true),
			existingStats: &stats{
				timestamp:             time.Now().Add(-11 * time.Second).UnixNano(),
				conflicts:             100,
				windowStartTimestamp:  time.Now().Add(-11 * time.Second).UnixNano(),
				windowConflicts:       150,
				windowThreshold:       100, // Old
				windowMonitorDuration: 5,   // Old
			},
			mockGetConflicts:      180,  // delta=80, total=150+80=230 >= 100 (old threshold) but we capture new settings in new window
			expectAutopauseCalled: true, // Breaches old threshold of 100
			expectUIErrorCalled:   true,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				// Should autopause with old threshold, stat deleted
				dontRequireStat(t, m, topic)
			},
		},
		{
			name: "WindowExpires_OldNearThreshold_NewBelowThreshold_NoAutopause",
			spec: createTestSpec("repl1", 10, 5, true),
			existingStats: &stats{
				timestamp:             time.Now().Add(-6 * time.Second).UnixNano(), // Window expired (6s > 5s)
				conflicts:             100,
				windowStartTimestamp:  time.Now().Add(-6 * time.Second).UnixNano(),
				windowConflicts:       9, // Near threshold (9 out of 10)
				windowThreshold:       10,
				windowMonitorDuration: 5,
				state:                 monitorStateNormal,
			},
			mockGetConflicts:      103, // delta=3, total=9+3=12 >= 10, should autopause
			expectAutopauseCalled: true,
			expectUIErrorCalled:   true,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				// Should autopause because 12 >= 10, stat deleted
				dontRequireStat(t, m, topic)
			},
		},

		// Threshold breach detection
		{
			name: "ThresholdBreached_StateTransition",
			spec: createTestSpec("repl1", 50, 60, true),
			existingStats: &stats{
				timestamp:             time.Now().Add(-1 * time.Second).UnixNano(),
				conflicts:             100,
				windowStartTimestamp:  time.Now().Add(-10 * time.Second).UnixNano(),
				windowConflicts:       30,
				windowThreshold:       50,
				windowMonitorDuration: 60,
				state:                 monitorStateNormal,
			},
			mockGetConflicts:      130, // delta=30, total=60 >= 50
			expectAutopauseCalled: true,
			expectUIErrorCalled:   true,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				dontRequireStat(t, m, topic)
			},
		},
		{
			name: "ExactThreshold_Autopaused",
			spec: createTestSpec("repl1", 100, 60, true),
			existingStats: &stats{
				timestamp:             time.Now().Add(-1 * time.Second).UnixNano(),
				conflicts:             50,
				windowStartTimestamp:  time.Now().Add(-10 * time.Second).UnixNano(),
				windowConflicts:       75,
				windowThreshold:       100,
				windowMonitorDuration: 60,
				state:                 monitorStateNormal,
			},
			mockGetConflicts:      75, // delta=25, total=100 == threshold
			expectAutopauseCalled: true,
			expectUIErrorCalled:   true,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				dontRequireStat(t, m, topic)
			},
		},
		{
			name: "BelowThreshold_NoAutopause",
			spec: createTestSpec("repl1", 100, 60, true),
			existingStats: &stats{
				timestamp:             time.Now().Add(-1 * time.Second).UnixNano(),
				conflicts:             100,
				windowStartTimestamp:  time.Now().Add(-10 * time.Second).UnixNano(),
				windowConflicts:       50,
				windowThreshold:       100,
				windowMonitorDuration: 60,
				state:                 monitorStateNormal,
			},
			mockGetConflicts: 140, // delta=40, total=90 < 100
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				stat := requireStat(t, m, topic)
				assert.Equal(t, int64(90), stat.windowConflicts)
				assert.Equal(t, monitorStateNormal, stat.state)
			},
		},
		{
			name: "ThresholdBreached_LastCycleBeforeWindowExpiry",
			spec: createTestSpec("repl1", 100, 5, true), // threshold=100, 5 second window
			existingStats: &stats{
				timestamp:             time.Now().Add(-4500 * time.Millisecond).UnixNano(), // 4.5s into 5s window
				conflicts:             200,
				windowStartTimestamp:  time.Now().Add(-4500 * time.Millisecond).UnixNano(),
				windowConflicts:       90, // Just below threshold (90 out of 100)
				windowThreshold:       100,
				windowMonitorDuration: 5,
				state:                 monitorStateNormal,
			},
			mockGetConflicts:      215, // delta=15, total=90+15=105 >= 100
			expectAutopauseCalled: true,
			expectUIErrorCalled:   true,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				dontRequireStat(t, m, topic)
				// Demonstrates autopause triggered on last cycle before window would expire
			},
		},

		// Autopause error handling
		{
			name: "AutopauseError_StateBreached_ErrorRecorded",
			spec: createTestSpec("repl1", 50, 60, true),
			existingStats: &stats{
				timestamp:             time.Now().Add(-1 * time.Second).UnixNano(),
				conflicts:             100,
				windowStartTimestamp:  time.Now().Add(-10 * time.Second).UnixNano(),
				windowConflicts:       40,
				windowThreshold:       50,
				windowMonitorDuration: 60,
				state:                 monitorStateNormal,
			},
			mockGetConflicts:      120, // delta=20, total=60 >= 50
			mockAutopauseErr:      errors.New("autopause failed"),
			expectAutopauseCalled: true,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				stat := requireStat(t, m, topic)
				assert.Equal(t, monitorStateConflictRateBreached, stat.state)
				assert.NotNil(t, stat.lastErr)
				assert.Contains(t, stat.lastErr.Error(), "error autopausing")
				assert.Equal(t, uint64(1), stat.errCnt)
			},
		},
		{
			name: "UIError_AutopauseSucceeds_ErrorRecorded",
			spec: createTestSpec("repl1", 50, 60, true),
			existingStats: &stats{
				timestamp:             time.Now().Add(-1 * time.Second).UnixNano(),
				conflicts:             100,
				windowStartTimestamp:  time.Now().Add(-10 * time.Second).UnixNano(),
				windowConflicts:       40,
				windowThreshold:       50,
				windowMonitorDuration: 60,
			},
			mockGetConflicts:      120,
			mockUIErr:             errors.New("ui error"),
			expectAutopauseCalled: true,
			expectUIErrorCalled:   true,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				dontRequireStat(t, m, topic)
			},
		},

		// State machine - already breached/paused
		{
			name: "AlreadyBreached_RetryAutopause",
			spec: createTestSpec("repl1", 50, 60, true),
			existingStats: &stats{
				timestamp:             time.Now().Add(-1 * time.Second).UnixNano(),
				conflicts:             100,
				windowStartTimestamp:  time.Now().Add(-10 * time.Second).UnixNano(),
				windowConflicts:       60,
				windowThreshold:       50,
				windowMonitorDuration: 60,
				state:                 monitorStateConflictRateBreached,
			},
			mockGetConflicts:      110,
			expectAutopauseCalled: true,
			expectUIErrorCalled:   true,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				dontRequireStat(t, m, topic)
			},
		},

		// First cycle threshold breach - verifies conflicts in cycle #1 are not ignored
		{
			name:                  "FirstCycle_ThresholdBreached_ImmediateAutopause",
			spec:                  createTestSpec("repl1", 10, 60, true), // threshold=10
			mockGetConflicts:      15,                                    // 15 conflicts on first cycle
			expectAutopauseCalled: true,
			expectUIErrorCalled:   true,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				// Stat should be deleted after successful autopause
				dontRequireStat(t, m, topic)
				// This proves that conflicts from the 0th to 1st second are NOT ignored
			},
		},
		{
			name:             "FirstCycle_BelowThreshold_NoAutopause",
			spec:             createTestSpec("repl1", 100, 60, true), // threshold=100
			mockGetConflicts: 50,                                     // 50 conflicts, below threshold
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				stat := requireStat(t, m, topic)
				assert.Equal(t, int64(50), stat.conflicts)
				// windowConflicts set to initial count on first cycle
				assert.Equal(t, int64(50), stat.windowConflicts)
				assert.Equal(t, monitorStateNormal, stat.state)
			},
		},

		// 1-second window edge cases
		{
			name: "OneSecondWindow_ThresholdBreachedBeforeExpiry_Autopaused",
			spec: createTestSpec("repl1", 10, 1, true), // threshold=10, 1 second window
			existingStats: &stats{
				timestamp:             time.Now().Add(-500 * time.Millisecond).UnixNano(), // 0.5s into window
				conflicts:             5,
				windowStartTimestamp:  time.Now().Add(-500 * time.Millisecond).UnixNano(),
				windowConflicts:       5,
				windowThreshold:       10,
				windowMonitorDuration: 1,
				state:                 monitorStateNormal,
			},
			mockGetConflicts:      12, // delta=7, total=5+7=12 >= 10
			expectAutopauseCalled: true,
			expectUIErrorCalled:   true,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				// Should autopause and remove stat
				dontRequireStat(t, m, topic)
			},
		},
		{
			name: "OneSecondWindow_WindowExpiresBeforeThreshold_NewWindowStarts",
			spec: createTestSpec("repl1", 100, 1, true), // threshold=100, 1 second window
			existingStats: &stats{
				timestamp:             time.Now().Add(-1100 * time.Millisecond).UnixNano(), // 1.1s ago
				conflicts:             50,
				windowStartTimestamp:  time.Now().Add(-1100 * time.Millisecond).UnixNano(),
				windowConflicts:       50, // Near but below threshold
				windowThreshold:       100,
				windowMonitorDuration: 1,
				state:                 monitorStateNormal,
			},
			mockGetConflicts: 60, // delta=10, total would be 60, but window expired
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				stat := requireStat(t, m, topic)
				// Window expired below threshold - new window starts fresh at 0
				assert.Equal(t, int64(0), stat.windowConflicts)
				assert.Equal(t, monitorStateNormal, stat.state)
			},
		},

		// ErrorReplicationSpecNotActive handling
		{
			name:                "ReplicationNotActive_FirstCycle_StatNotCreated",
			spec:                createTestSpec("repl1", 10, 60, true),
			mockGetConflicts:    0,
			mockGetConflictsErr: base.ErrorReplicationSpecNotActive,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				// No stat should be created
				assert.Empty(t, m.statsToMonitor)
			},
		},
		{
			name: "ReplicationNotActive_ExistingStat_StatDeleted",
			spec: createTestSpec("repl1", 10, 60, true),
			existingStats: &stats{
				timestamp:             time.Now().Add(-1 * time.Second).UnixNano(),
				conflicts:             50,
				windowStartTimestamp:  time.Now().Add(-5 * time.Second).UnixNano(),
				windowConflicts:       30,
				windowThreshold:       10,
				windowMonitorDuration: 60,
				state:                 monitorStateNormal,
			},
			mockGetConflicts:    0,
			mockGetConflictsErr: base.ErrorReplicationSpecNotActive,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				// Stat should be deleted when replication becomes inactive
				assert.Empty(t, m.statsToMonitor)
			},
		},
		{
			name: "ReplicationNotActive_BreachedState_StatDeleted",
			spec: createTestSpec("repl1", 10, 60, true),
			existingStats: &stats{
				timestamp:             time.Now().Add(-1 * time.Second).UnixNano(),
				conflicts:             50,
				windowStartTimestamp:  time.Now().Add(-5 * time.Second).UnixNano(),
				windowConflicts:       30,
				windowThreshold:       10,
				windowMonitorDuration: 60,
				state:                 monitorStateConflictRateBreached,
			},
			mockGetConflicts:    0,
			mockGetConflictsErr: base.ErrorReplicationSpecNotActive,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				// Even in breached state, stat should be deleted
				assert.Empty(t, m.statsToMonitor)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockMon, logger := setupMonitorMocks()

			m := &monitorImpl{
				Monitor:        mockMon,
				statsToMonitor: make(map[string]*stats),
				logger:         logger,
			}

			// Set up existing stats if provided
			if tt.existingStats != nil && tt.spec != nil {
				uniqueTopic := tt.spec.UniqueId()
				m.statsToMonitor[uniqueTopic] = tt.existingStats
			}

			// Set up mock expectations
			if tt.spec != nil && !tt.spec.Settings.GetConflictLoggingMapping().Disabled() && tt.spec.Settings.GetCLogPauseReplThreshold() > 0 && tt.spec.Settings.GetCLogMonitorDuration() > 0 {
				topic := tt.spec.Id
				mockMon.On("GetConflictsCount", topic).Return(tt.mockGetConflicts, tt.mockGetConflictsErr).Once()

				if tt.expectAutopauseCalled {
					mockMon.On("AutoPauseReplication", topic).Return(tt.mockAutopauseErr).Once()
				}
				if tt.expectUIErrorCalled {
					mockMon.On("RaiseUIError", topic, mock.AnythingOfType("string")).Return(int64(0), tt.mockUIErr).Once()
				}
			}

			// Execute
			m.monitorOneReplicationOnce(tt.spec)

			// Validate
			var topic string
			if tt.spec != nil {
				topic = tt.spec.UniqueId()
			}
			tt.validateFunc(t, m, topic)

			// Assert mock expectations
			mockMon.AssertExpectations(t)
		})
	}
}

// TestCleanupMonitoredStats tests the cleanupMonitoredStats function using table-driven tests
func TestCleanupMonitoredStats(t *testing.T) {
	tests := []struct {
		name           string
		existingStats  map[string]*stats
		activeSpecs    []*metadata.ReplicationSpecification
		getAllSpecsErr error
		expectedStats  []string // expected remaining stats keys
		expectedErr    bool
	}{
		{
			name:          "EmptyStatsMap",
			existingStats: map[string]*stats{},
			activeSpecs: []*metadata.ReplicationSpecification{
				createTestSpec("repl1", 100, 60, true),
			},
			expectedStats: []string{},
			expectedErr:   false,
		},
		{
			name: "AllStatsMatchActiveSpecs",
			existingStats: map[string]*stats{
				"repl1-test-internal-id": {conflicts: 100},
				"repl2-test-internal-id": {conflicts: 200},
			},
			activeSpecs: []*metadata.ReplicationSpecification{
				createTestSpec("repl1", 100, 60, true),
				createTestSpec("repl2", 100, 60, true),
			},
			expectedStats: []string{"repl1-test-internal-id", "repl2-test-internal-id"},
			expectedErr:   false,
		},
		{
			name: "SomeStatsNoLongerActive",
			existingStats: map[string]*stats{
				"repl1-test-internal-id": {conflicts: 100},
				"repl2-test-internal-id": {conflicts: 200},
				"repl3-test-internal-id": {conflicts: 300},
			},
			activeSpecs: []*metadata.ReplicationSpecification{
				createTestSpec("repl1", 100, 60, true),
			},
			expectedStats: []string{"repl1-test-internal-id"},
			expectedErr:   false,
		},
		{
			name: "AllStatsNoLongerActive",
			existingStats: map[string]*stats{
				"repl1-test-internal-id": {conflicts: 100},
				"repl2-test-internal-id": {conflicts: 200},
			},
			activeSpecs:   []*metadata.ReplicationSpecification{},
			expectedStats: []string{},
			expectedErr:   false,
		},
		{
			name: "ErrorGettingActiveSpecs",
			existingStats: map[string]*stats{
				"repl1-test-internal-id": {conflicts: 100},
			},
			activeSpecs:    nil,
			getAllSpecsErr: errors.New("service unavailable"),
			expectedStats:  []string{"repl1-test-internal-id"}, // Stats unchanged on error
			expectedErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockMon, logger := setupMonitorMocks()

			// Create mock ReplicationSpecSvc
			mockReplSpecSvc := &service_def.ReplicationSpecSvc{}

			// Only set up mock expectation if stats map is not empty
			// (function returns early when empty)
			if len(tt.existingStats) > 0 {
				if tt.getAllSpecsErr != nil {
					mockReplSpecSvc.On("AllActiveReplicationSpecsReadOnly").Return(
						map[string]*metadata.ReplicationSpecification{}, tt.getAllSpecsErr).Once()
				} else {
					specsMap := make(map[string]*metadata.ReplicationSpecification)
					for _, spec := range tt.activeSpecs {
						specsMap[spec.UniqueId()] = spec
					}
					mockReplSpecSvc.On("AllActiveReplicationSpecsReadOnly").Return(specsMap, nil).Once()
				}
			}

			m := &monitorImpl{
				Monitor:        mockMon,
				statsToMonitor: tt.existingStats,
				logger:         logger,
				replSpecSvc:    mockReplSpecSvc,
			}

			// Execute
			err := m.cleanupMonitoredStats()

			// Validate error
			if tt.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			// Validate remaining stats
			assert.Equal(t, len(tt.expectedStats), len(m.statsToMonitor))
			for _, expectedKey := range tt.expectedStats {
				assert.Contains(t, m.statsToMonitor, expectedKey)
			}

			// Assert mock expectations
			mockReplSpecSvc.AssertExpectations(t)
		})
	}
}
