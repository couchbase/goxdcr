/*
Copyright 2024-Present Couchbase, Inc.

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
	"testing"

	"github.com/couchbase/gomemcached"
	mccmocks "github.com/couchbase/gomemcached/client/mocks"
	baseclog "github.com/couchbase/goxdcr/v8/base/conflictlog"
	"github.com/couchbase/goxdcr/v8/common"
	commonmocks "github.com/couchbase/goxdcr/v8/common/mocks"
	component "github.com/couchbase/goxdcr/v8/component"
	"github.com/couchbase/goxdcr/v8/log"
	utils "github.com/couchbase/goxdcr/v8/utils/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// TestSetMeta_TMPFAIL_PreservesSentinel tests that setMeta preserves the sentinel
// baseclog.ErrTMPFAIL when handling a TMPFAIL response from the memcached server.
// This is a regression test for MB-64316 where %v formatting was losing sentinel identity.
func TestSetMeta_TMPFAIL_PreservesSentinel(t *testing.T) {
	// Build a minimal MemcachedConn directly (avoid calling NewMemcachedConn which
	// requires cbauth.GetMemcachedServiceAuth)
	m := &MemcachedConn{
		logger: log.NewLogger("test", log.DefaultLoggerContext),
		addr:   "127.0.0.1:12000",
		opaque: 0,
	}

	// Create a mocked ClientIface that returns a TMPFAIL response
	mockClient := mccmocks.NewClientIface(t)
	mockClient.EXPECT().Send(mock.AnythingOfType("*gomemcached.MCRequest")).
		RunAndReturn(func(req *gomemcached.MCRequest) (*gomemcached.MCResponse, error) {
			return &gomemcached.MCResponse{
				Opcode: req.Opcode,
				Status: gomemcached.TMPFAIL,
				Opaque: req.Opaque, // MUST echo opaque; handleResponse validates this
			}, nil
		})

	// Call setMeta with valid parameters
	err := m.setMeta(mockClient, "k1", 0, []byte("{}"), 0, 0)

	// After the fix (changing %v to %w in gomemcached.go:295 or using errors.Is in
	// loggerimpl.go:670), the error should preserve the sentinel
	assert.Error(t, err)
	assert.True(t, errors.Is(err, baseclog.ErrTMPFAIL),
		"setMeta must preserve baseclog.ErrTMPFAIL sentinel so processWriteError can categorize it; got %v", err)
}

// TestSetMeta_OtherSentinels_PreservedOrCategorized tests that other sentinel errors
// from handleResponse are also preserved correctly by setMeta.
func TestSetMeta_OtherSentinels_PreservedOrCategorized(t *testing.T) {
	tests := []struct {
		name          string
		status        gomemcached.Status
		wantSentinel  error
		shouldBeError bool
	}{
		{
			name:          "TMPFAIL",
			status:        gomemcached.TMPFAIL,
			wantSentinel:  baseclog.ErrTMPFAIL,
			shouldBeError: true,
		},
		{
			name:          "EACCESS",
			status:        gomemcached.EACCESS,
			wantSentinel:  baseclog.ErrEACCESS,
			shouldBeError: true,
		},
		{
			name:          "SUCCESS",
			status:        gomemcached.SUCCESS,
			wantSentinel:  nil,
			shouldBeError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MemcachedConn{
				logger: log.NewLogger("test", log.DefaultLoggerContext),
				addr:   "127.0.0.1:12000",
				opaque: 0,
			}

			mockClient := mccmocks.NewClientIface(t)
			mockClient.EXPECT().Send(mock.AnythingOfType("*gomemcached.MCRequest")).
				RunAndReturn(func(req *gomemcached.MCRequest) (*gomemcached.MCResponse, error) {
					return &gomemcached.MCResponse{
						Opcode: req.Opcode,
						Status: tt.status,
						Opaque: req.Opaque,
					}, nil
				})

			err := m.setMeta(mockClient, "k1", 0, []byte("{}"), 0, 0)

			if tt.shouldBeError {
				assert.Error(t, err)
				if tt.wantSentinel != nil {
					assert.True(t, errors.Is(err, tt.wantSentinel),
						"expected sentinel %v, got %v", tt.wantSentinel, err)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestProcessWriteError_WrappedTMPFAIL_CategorizesAsRetry tests that processWriteError
// correctly categorizes a wrapped TMPFAIL error as needing retry.
// This test deliberately constructs the wrapped-error shape that the current (buggy) setMeta produces,
// then verifies it's handled correctly.
func TestProcessWriteError_WrappedTMPFAIL_CategorizesAsRetry(t *testing.T) {
	// Setup mocks for a minimal LoggerImpl
	mockUtils := &utils.UtilsIface{}
	mockUtils.On("IsSeriousNetError", mock.Anything).Return(false)

	// Create a mock events producer
	eventsProducer := &commonmocks.PipelineEventsProducer{}

	// Create a minimal LoggerImpl with only the fields processWriteError touches
	l := &LoggerImpl{
		utils:              mockUtils,
		AbstractComponent:  component.NewAbstractComponent("test"),
		eventsProducer:     eventsProducer,
	}

	// Construct the wrapped error exactly as setMeta does today (with %v, losing sentinel identity)
	wrapped := fmt.Errorf("error in setMeta: err=%v, err2=%v", nil, baseclog.ErrTMPFAIL)

	// Call processWriteError with a test pipeline type
	writeErr, _ := l.processWriteError(wrapped, common.MainPipeline)

	// After the fix (either %w in gomemcached.go:295 or errors.Is in loggerimpl.go:670),
	// wrapped TMPFAIL must be categorized as requiring retry, not as unknownErr
	assert.Equal(t, needRetryErr, writeErr,
		"wrapped TMPFAIL must be categorized as retryable, not unknownErr")
}

