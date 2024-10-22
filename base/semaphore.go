package base

import (
	"sync"
	"time"
)

// Semaphore allows at max N number of concurrent access to a resource
// See https://en.wikipedia.org/wiki/Semaphore_(programming)
// This implementation allows changing of the limit which
// the standard lib version does not allow
type Semaphore struct {
	// rw lock is mainly to protect the channel when it needs to be replaced by a new one
	rw sync.RWMutex

	// ch contains tokens. len != 0 implies that the next acquire request will be allowed
	ch chan bool
}

// NewSemaphore creates a new semaphore with a given limit.
// A limit of <= 0 will be converted to 1.
func NewSemaphore(limit int) *Semaphore {
	if limit <= 0 {
		limit = 1
	}

	s := &Semaphore{
		rw: sync.RWMutex{},
		ch: make(chan bool, limit),
	}

	s.addTokens(limit)

	return s
}

// addTokens adds 'n' tokens to the channel. This means next 'n' acquire calls (without release) will be allowed.
func (s *Semaphore) addTokens(n int) {
	for i := 0; i < n; i++ {
		s.ch <- true
	}
}

// AcquireWithTimeout attempts to acquire the semaphore and returns true if it succeeds.
// The timeout 'd' is duration after which false will be returned.
// Internally a channel is used to keep track of available tokens.
// Channel cap - channel len indicates tokens in active use and channel len indicates the
// count of remaining tokens
func (s *Semaphore) AcquireWithTimeout(d time.Duration) bool {
	s.rw.RLock()
	defer s.rw.RUnlock()

	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-s.ch:
		return true
	case <-t.C:
		return false
	}
}

// Limit returns the current limit
func (s *Semaphore) Limit() int {
	s.rw.RLock()
	defer s.rw.RUnlock()

	return cap(s.ch)
}

// Release releases the semaphore. Attempt to call Release more than the limit will result into a no-op.
// This can happen when the limit is decreased while some callers have already acquired the tokens.
func (s *Semaphore) Release() {
	s.rw.RLock()
	defer s.rw.RUnlock()

	select {
	case s.ch <- true:
	default:
	}
}

// SetLimit sets the limit. A limit of <= 0 will be converted to 1. Nothing happens when old & new limit are the same.
// A new channel is created and tokens are added into it. The count depends on the old and new limit. It also takes into
// account the tokens which are already acquired (called activeUse). So, the limit-activeUse count of tokens will be added.
//
//	 Scenarios (NLim=New Limit, OLim=Old Limit, OLen=Old channel len):
//		NLim > OLim
//		    Scenario       OLim  OLen  NLim  Expected
//		    OLim == OLen   10    10    15    No active uses. New channel len=15.
//		    OLim > OLen    10    7     15    Active uses=3. New channel len=12.
//		NLim < OLim
//		    Scenario       OLim  OLen  NLim  Expected
//		    OLim == OLen   15    15    10    Active uses=0. New channel len=10.
//		    OLim > OLen    15    12    10    Active uses=3. New channel len=7.
//		    OLim > OLen    15    7     10    Active uses=8. New channel len=2.
//		    OLim > OLen    15    1     10    Active uses=14. New channel len=0.
func (s *Semaphore) SetLimit(limit int) {
	if limit <= 0 {
		limit = 1
	}

	s.rw.Lock()
	defer s.rw.Unlock()

	oldLimit := cap(s.ch)

	if oldLimit == limit {
		return
	}

	oldLen := len(s.ch)
	activeUse := oldLimit - oldLen

	s.ch = make(chan bool, limit)

	// the subtraction below can be -ve and this fine.
	// the addTokens will be a no-op if this happens
	s.addTokens(limit - activeUse)
}
