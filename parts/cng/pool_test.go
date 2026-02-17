package cng

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWithConnRetryBackoff(t *testing.T) {
	tests := []struct {
		name       string
		retryCount uint64
		expected   time.Duration
	}{
		{
			name:       "retryCount 0 uses initial",
			retryCount: 0,
			expected:   WithConnRetryBackoffInitial,
		},
		{
			name:       "retryCount 1 uses initial",
			retryCount: 1,
			expected:   WithConnRetryBackoffInitial,
		},
		{
			name:       "retryCount 2 scales by factor",
			retryCount: 2,
			expected:   3 * time.Second,
		},
		{
			name:       "retryCount 3 scales by factor twice",
			retryCount: 3,
			expected:   4500 * time.Millisecond,
		},
		{
			name:       "retryCount large is capped",
			retryCount: 10,
			expected:   WithConnRetryBackoffMax,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, withConnRetryBackoff(tt.retryCount))
		})
	}
}
