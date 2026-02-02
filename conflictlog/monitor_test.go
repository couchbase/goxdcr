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
func createTestSpec(topic string, conflictRateThreshold int, conflictLoggingEnabled bool) *metadata.ReplicationSpecification {
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
	if conflictRateThreshold >= 0 {
		settings.Values[metadata.ConflictRateToPauseReplKey] = conflictRateThreshold
	}
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
		{
			name:             "NilSpec",
			spec:             nil,
			mockGetConflicts: 0,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				assert.Empty(t, m.statsToMonitor)
			},
		},
		{
			name:             "ConflictLoggingDisabled",
			spec:             createTestSpec("repl1", 10, false),
			mockGetConflicts: 0,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				assert.Empty(t, m.statsToMonitor)
			},
		},
		{
			name:             "AutopauseDisabled",
			spec:             createTestSpec("repl1", 0, true),
			mockGetConflicts: 0,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				assert.Empty(t, m.statsToMonitor)
			},
		},
		{
			name:             "FirstCycle",
			spec:             createTestSpec("repl1", 10, true),
			mockGetConflicts: 100,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				stat := requireStat(t, m, topic)
				assert.Equal(t, int64(100), stat.conflicts)
				assert.Greater(t, stat.timestamp, int64(0))
				assert.False(t, stat.conflictRateBreached)
				assert.False(t, stat.autopaused)
				assert.Nil(t, stat.lastErr)
			},
		},
		{
			name:                "GetConflictsCountError",
			spec:                createTestSpec("repl1", 10, true),
			mockGetConflicts:    0,
			mockGetConflictsErr: errors.New("connection error"),
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				stat := requireStat(t, m, topic)
				assert.NotNil(t, stat.lastErr)
				assert.Contains(t, stat.lastErr.Error(), "error getting conflict count")
				assert.Equal(t, uint64(1), stat.errCnt)
			},
		},
		{
			name: "NonMonotonicTime",
			spec: createTestSpec("repl1", 10, true),
			existingStats: &stats{
				timestamp: time.Now().Add(1 * time.Hour).UnixNano(),
				conflicts: 100,
			},
			mockGetConflicts: 150,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				stat := requireStat(t, m, topic)
				assert.Equal(t, uint64(1), stat.descTime)
				assert.Equal(t, int64(150), stat.conflicts)
			},
		},
		{
			name: "NonMonotonicConflicts",
			spec: createTestSpec("repl1", 10, true),
			existingStats: &stats{
				timestamp: time.Now().Add(-1 * time.Hour).UnixNano(),
				conflicts: 200,
			},
			mockGetConflicts: 150,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				stat := requireStat(t, m, topic)
				assert.Equal(t, uint64(1), stat.descConflicts)
				assert.Equal(t, int64(150), stat.conflicts)
			},
		},
		{
			name: "LowConflictRate",
			spec: createTestSpec("repl1", 100, true),
			existingStats: &stats{
				timestamp: time.Now().Add(-10 * time.Second).UnixNano(),
				conflicts: 100,
			},
			mockGetConflicts: 600,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				stat := requireStat(t, m, topic)
				assert.False(t, stat.conflictRateBreached)
				assert.False(t, stat.autopaused)
				assert.Equal(t, int64(600), stat.conflicts)
			},
		},
		{
			name: "ZeroConflictDelta",
			spec: createTestSpec("repl1", 50, true),
			existingStats: &stats{
				timestamp: time.Now().Add(-10 * time.Second).UnixNano(),
				conflicts: 100,
			},
			mockGetConflicts: 100,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				stat := requireStat(t, m, topic)
				assert.False(t, stat.conflictRateBreached)
				assert.False(t, stat.autopaused)
			},
		},
		{
			name: "HighConflictRate",
			spec: createTestSpec("repl1", 50, true),
			existingStats: &stats{
				timestamp: time.Now().Add(-10 * time.Second).UnixNano(),
				conflicts: 100,
			},
			mockGetConflicts:      700,
			expectAutopauseCalled: true,
			expectUIErrorCalled:   true,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				stat := requireStat(t, m, topic)
				assert.True(t, stat.conflictRateBreached)
				assert.True(t, stat.autopaused)
				assert.Equal(t, int64(700), stat.conflicts)
				assert.Nil(t, stat.lastErr)
			},
		},
		{
			name: "AutopauseError",
			spec: createTestSpec("repl1", 50, true),
			existingStats: &stats{
				timestamp: time.Now().Add(-10 * time.Second).UnixNano(),
				conflicts: 100,
			},
			mockGetConflicts:      700,
			mockAutopauseErr:      errors.New("autopause failed"),
			expectAutopauseCalled: true,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				stat := requireStat(t, m, topic)
				assert.False(t, stat.conflictRateBreached)
				assert.False(t, stat.autopaused)
				assert.NotNil(t, stat.lastErr)
				assert.Contains(t, stat.lastErr.Error(), "error autopausing")
				assert.Equal(t, uint64(1), stat.errCnt)
			},
		},
		{
			name: "UIErrorAfterAutopause",
			spec: createTestSpec("repl1", 50, true),
			existingStats: &stats{
				timestamp: time.Now().Add(-10 * time.Second).UnixNano(),
				conflicts: 100,
			},
			mockGetConflicts:      700,
			mockUIErr:             errors.New("ui error"),
			expectAutopauseCalled: true,
			expectUIErrorCalled:   true,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				stat := requireStat(t, m, topic)
				assert.False(t, stat.conflictRateBreached)
				assert.False(t, stat.autopaused)
				assert.NotNil(t, stat.lastErr)
				assert.Contains(t, stat.lastErr.Error(), "error displaying ui message")
				assert.Equal(t, uint64(1), stat.errCnt)
			},
		},
		{
			name: "AlreadyAutopaused",
			spec: createTestSpec("repl1", 50, true),
			existingStats: &stats{
				timestamp:            time.Now().Add(-10 * time.Second).UnixNano(),
				conflicts:            100,
				conflictRateBreached: true,
				autopaused:           true,
			},
			mockGetConflicts: 700,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				stat := requireStat(t, m, topic)
				assert.True(t, stat.conflictRateBreached)
				assert.True(t, stat.autopaused)
				assert.Nil(t, stat.lastErr)
			},
		},
		{
			name: "BreachPersistence",
			spec: createTestSpec("repl1", 100, true),
			existingStats: &stats{
				timestamp:            time.Now().Add(-10 * time.Second).UnixNano(),
				conflicts:            100,
				conflictRateBreached: true,
				autopaused:           true,
			},
			mockGetConflicts: 300,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				stat := requireStat(t, m, topic)
				assert.True(t, stat.conflictRateBreached)
				assert.True(t, stat.autopaused)
				assert.Equal(t, int64(300), stat.conflicts)
			},
		},
		{
			name: "RetryAutopauseAfterFailure",
			spec: createTestSpec("repl1", 50, true),
			existingStats: &stats{
				timestamp:            time.Now().Add(-10 * time.Second).UnixNano(),
				conflicts:            600,
				conflictRateBreached: true,
				autopaused:           false,
				lastErr:              errors.New("previous failure"),
			},
			mockGetConflicts:      1200,
			expectAutopauseCalled: true,
			expectUIErrorCalled:   true,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				stat := requireStat(t, m, topic)
				assert.True(t, stat.conflictRateBreached)
				assert.True(t, stat.autopaused)
				assert.NotNil(t, stat.lastErr)
			},
		},
		{
			name: "ExactThreshold",
			spec: createTestSpec("repl1", 100, true),
			existingStats: &stats{
				timestamp: time.Now().Add(-100 * time.Second).UnixNano(),
				conflicts: 0,
			},
			mockGetConflicts:      10001,
			expectAutopauseCalled: true,
			expectUIErrorCalled:   true,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				stat := requireStat(t, m, topic)
				assert.True(t, stat.conflictRateBreached)
				assert.True(t, stat.autopaused)
			},
		},
		{
			name: "VeryHighRate",
			spec: createTestSpec("repl1", 10, true),
			existingStats: &stats{
				timestamp: time.Now().Add(-1 * time.Second).UnixNano(),
				conflicts: 0,
			},
			mockGetConflicts:      5000,
			expectAutopauseCalled: true,
			expectUIErrorCalled:   true,
			validateFunc: func(t *testing.T, m *monitorImpl, topic string) {
				stat := requireStat(t, m, topic)
				assert.True(t, stat.conflictRateBreached)
				assert.True(t, stat.autopaused)
				assert.Equal(t, int64(5000), stat.conflicts)
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
				fullTopic := tt.spec.UniqueId()
				m.statsToMonitor[fullTopic] = tt.existingStats
			}

			// Set up mock expectations
			if tt.spec != nil && !tt.spec.Settings.GetConflictLoggingMapping().Disabled() && tt.spec.Settings.GetConflictRateToPauseRepl() > 0 {
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
				createTestSpec("repl1", 100, true),
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
				createTestSpec("repl1", 100, true),
				createTestSpec("repl2", 100, true),
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
				createTestSpec("repl1", 100, true),
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
