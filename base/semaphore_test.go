package base

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSemaphore(t *testing.T) {
	acquireN := func(t *testing.T, s *Semaphore, n int, shouldFail bool) {
		timeout := 1 * time.Millisecond

		for i := 0; i < n; i++ {
			if shouldFail {
				require.False(t, s.AcquireWithTimeout(timeout))
			} else {
				require.True(t, s.AcquireWithTimeout(timeout))
			}
		}
	}

	releaseN := func(t *testing.T, s *Semaphore, n int) {
		for i := 0; i < n; i++ {
			s.Release()
		}
	}

	basicAcqAndRel := func(t *testing.T, s *Semaphore) {
		limit := s.Limit()
		timeout := 10 * time.Millisecond

		acquireN(t, s, limit, false)

		require.False(t, s.AcquireWithTimeout(timeout))

		s.Release()
		require.True(t, s.AcquireWithTimeout(timeout))

		releaseN(t, s, limit-1)
		acquireN(t, s, limit-1, false)
		releaseN(t, s, limit)
	}

	t.Run("basic acquire & release", func(t *testing.T) {
		limit := 5
		s := NewSemaphore(limit)

		basicAcqAndRel(t, s)
	})

	/*
	   NLim > OLim
	       Scenario       OLim  OLen  NLim  Expected
	       OLim == OLen   10    10    15    No active uses. New channel len=15.
	       OLim > OLen    10    7     15    Active uses=3. New channel len=12.
	   NLim < OLim
	       Scenario       OLim  OLen  NLim  Expected
	       OLim == OLen   15    15    10    Active uses=0. New channel len=10.
	       OLim > OLen    15    12    10    Active uses=3. New channel len=7.
	       OLim > OLen    15    7     10    Active uses=8. New channel len=2.
	       OLim > OLen    15    1     10    Active uses=14. New channel len=0.
	*/

	t.Run("old limit == new limit", func(t *testing.T) {
		limit := 10
		newLimit := 10

		s := NewSemaphore(limit)
		s.SetLimit(newLimit)

		basicAcqAndRel(t, s)
	})

	t.Run("increase limit, active_use=0", func(t *testing.T) {
		limit := 10
		newLimit := 15

		s := NewSemaphore(limit)
		s.SetLimit(newLimit)

		basicAcqAndRel(t, s)
	})

	t.Run("increase limit, active_use=3", func(t *testing.T) {
		limit := 10
		newLimit := 15
		activeUse := 3

		s := NewSemaphore(limit)

		acquireN(t, s, activeUse, false)

		s.SetLimit(newLimit)

		acquireN(t, s, newLimit-activeUse, false)

		acquireN(t, s, 1, true)
	})

	t.Run("decrease limit, active_use=0", func(t *testing.T) {
		limit := 15
		newLimit := 10

		s := NewSemaphore(limit)
		s.SetLimit(newLimit)

		basicAcqAndRel(t, s)
		acquireN(t, s, newLimit, false)
		acquireN(t, s, 1, true)
	})

	t.Run("decrease limit, active_use < new limit", func(t *testing.T) {
		limit := 15
		newLimit := 10
		activeUse := 3

		s := NewSemaphore(limit)
		acquireN(t, s, activeUse, false)
		s.SetLimit(newLimit)

		acquireN(t, s, newLimit-activeUse, false)
		acquireN(t, s, 1, true)

		releaseN(t, s, newLimit)
		basicAcqAndRel(t, s)
	})

	t.Run("decrease limit, active_use >= new limit", func(t *testing.T) {
		limit := 15
		newLimit := 10
		activeUse := 14

		s := NewSemaphore(limit)
		acquireN(t, s, activeUse, false)
		s.SetLimit(newLimit)

		acquireN(t, s, newLimit, true)

		releaseN(t, s, activeUse)
		basicAcqAndRel(t, s)
	})
}
