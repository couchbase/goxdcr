package cng

import (
	"fmt"
	"sync/atomic"
)

// Stats holds various statistics for a single the CNG nozzle instance
type Stats struct {
	// Number of documents sent (irrespective of success/failure)
	docsSent uint64

	// Number of bytes sent (irrespective of success/failure)
	bytesSent uint64

	// Number of documents failed to be sent
	docsFailed uint64

	// Number of connection retries
	connRetryCount uint64

	// Number of items in queue
	itemsInQueue uint64

	// Number of items enqueuing was blocked due to full queue
	enqueueBlocked uint64

	// Number of times upstream error reporter was missing for a vb
	errUpstreamReporterMissingCount uint64
}

func NewStats() *Stats {
	return &Stats{}
}

func (s *Stats) IncDocsSent(count uint64) {
	atomic.AddUint64(&s.docsSent, count)
}

func (s *Stats) IncBytesSent(count uint64) {
	atomic.AddUint64(&s.bytesSent, count)
}

func (s *Stats) IncDocsFailed(count uint64) {
	atomic.AddUint64(&s.docsFailed, count)
}

func (s *Stats) IncConnRetryCount(count uint64) {
	atomic.AddUint64(&s.connRetryCount, count)
}

func (s *Stats) SetItemsInQueue(count uint64) {
	atomic.StoreUint64(&s.itemsInQueue, count)
}

func (s *Stats) IncEnqueueBlocked(count uint64) {
	atomic.AddUint64(&s.enqueueBlocked, count)
}

func (s *Stats) IncErrReporterMissingCount(count uint64) {
	atomic.AddUint64(&s.errUpstreamReporterMissingCount, count)
}

func (s *Stats) String() string {
	docsSent := atomic.LoadUint64(&s.docsSent)
	bytesSent := atomic.LoadUint64(&s.bytesSent)
	docsFailed := atomic.LoadUint64(&s.docsFailed)
	connRetryCount := atomic.LoadUint64(&s.connRetryCount)
	itemsInQueue := atomic.LoadUint64(&s.itemsInQueue)
	enqueuedBlocked := atomic.LoadUint64(&s.enqueueBlocked)
	errUpstreamReporterMissingCount := atomic.LoadUint64(&s.errUpstreamReporterMissingCount)

	return fmt.Sprintf("DocsSent: %v, BytesSent: %v, DocsFailed: %v, ConnRetryCount: %v, ItemsInQueue: %v, EnqueueBlocked: %v, ErrUpRptMissCount: %v",
		docsSent, bytesSent, docsFailed, connRetryCount, itemsInQueue, enqueuedBlocked, errUpstreamReporterMissingCount)
}

// handleNozzleStats updates the nozzle stats based on the transfer result
// Almost all stats are handled here except few.
func (n *Nozzle) handleNozzleStats(trace *Trace, err error) {
	if err != nil {
		n.stats.IncDocsFailed(1)
		return
	}

	n.stats.IncDocsSent(1)
	n.stats.IncBytesSent(uint64(trace.pushRsp.bytesReplicated))
	n.stats.IncConnRetryCount(trace.retryCount)
}
