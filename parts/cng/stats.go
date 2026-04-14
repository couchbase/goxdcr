package cng

import (
	"fmt"
	"strings"
	"sync/atomic"
)

// Stats holds various statistics for a single the CNG nozzle instance
type Stats struct {
	// Docs received from upstream
	docsReceived uint64

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

	// Number of WrappedMCRequest recycled in the nozzle.
	requestsRecycled uint64

	// Number of non-retryable errors encountered during processing of requests
	nonRetryableErrorCount uint64
}

func NewStats() *Stats {
	return &Stats{}
}

func (s *Stats) IncDocsReceived(count uint64) {
	atomic.AddUint64(&s.docsReceived, count)
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

func (s *Stats) IncRequestsRecycled(count uint64) {
	atomic.AddUint64(&s.requestsRecycled, count)
}

func (s *Stats) IncNonRetryableErrorCount(count uint64) {
	atomic.AddUint64(&s.nonRetryableErrorCount, count)
}

func (s *Stats) String() string {
	docsReceived := atomic.LoadUint64(&s.docsReceived)
	docsSent := atomic.LoadUint64(&s.docsSent)
	bytesSent := atomic.LoadUint64(&s.bytesSent)
	docsFailed := atomic.LoadUint64(&s.docsFailed)
	connRetryCount := atomic.LoadUint64(&s.connRetryCount)
	itemsInQueue := atomic.LoadUint64(&s.itemsInQueue)
	enqueuedBlocked := atomic.LoadUint64(&s.enqueueBlocked)
	errUpstreamReporterMissingCount := atomic.LoadUint64(&s.errUpstreamReporterMissingCount)
	requestsRecycled := atomic.LoadUint64(&s.requestsRecycled)
	nonRetryableErrorCount := atomic.LoadUint64(&s.nonRetryableErrorCount)

	w := strings.Builder{}

	fmt.Fprintf(&w, "DocsReceived: %d", docsReceived)
	fmt.Fprintf(&w, ",DocsSent: %d", docsSent)
	fmt.Fprintf(&w, ",BytesSent: %d", bytesSent)
	fmt.Fprintf(&w, ",DocsFailed: %d", docsFailed)
	fmt.Fprintf(&w, ",ConnRetryCount: %d", connRetryCount)
	fmt.Fprintf(&w, ",ItemsInQueue: %d", itemsInQueue)
	fmt.Fprintf(&w, ",EnqueueBlocked: %d", enqueuedBlocked)
	fmt.Fprintf(&w, ",ErrUpRptMissCount: %d", errUpstreamReporterMissingCount)
	fmt.Fprintf(&w, ",RequestsRecycled: %d", requestsRecycled)
	fmt.Fprintf(&w, ",NonRetryableErrorCount: %d", nonRetryableErrorCount)

	return w.String()
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
