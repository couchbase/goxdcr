package utils

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDataTrackingCtx(t *testing.T) {
	ctx := NewDataTrackingCtx()
	assert.True(t, ctx.TrackDataSentAndReceived)
	assert.Equal(t, int64(0), ctx.GetDataSent())
	assert.Equal(t, int64(0), ctx.GetDataReceived())
}

func TestNewDefaultContext(t *testing.T) {
	ctx := NewDefaultContext()
	assert.False(t, ctx.TrackDataSentAndReceived)
}

func TestLoadDataTransferInfo_Accumulates(t *testing.T) {
	ctx := NewDataTrackingCtx()

	ctx.LoadDataTransferInfo(100, 200)
	assert.Equal(t, int64(100), ctx.GetDataSent())
	assert.Equal(t, int64(200), ctx.GetDataReceived())

	ctx.LoadDataTransferInfo(50, 75)
	assert.Equal(t, int64(150), ctx.GetDataSent())
	assert.Equal(t, int64(275), ctx.GetDataReceived())
}

func TestLoadDataTransferInfo_NilContext(t *testing.T) {
	var ctx *Context
	// Should not panic
	ctx.LoadDataTransferInfo(100, 200)
}

func TestLoadDataTransferInfo_TrackingDisabled(t *testing.T) {
	ctx := NewDefaultContext()

	ctx.LoadDataTransferInfo(100, 200)
	assert.Equal(t, int64(0), ctx.GetDataSent())
	assert.Equal(t, int64(0), ctx.GetDataReceived())
}

func TestLoadDataTransferInfo_ConcurrentAccumulation(t *testing.T) {
	ctx := NewDataTrackingCtx()

	const goroutines = 100
	const perGoroutineSent = int64(10)
	const perGoroutineRecv = int64(20)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			ctx.LoadDataTransferInfo(perGoroutineSent, perGoroutineRecv)
		}()
	}
	wg.Wait()

	assert.Equal(t, int64(goroutines)*perGoroutineSent, ctx.GetDataSent())
	assert.Equal(t, int64(goroutines)*perGoroutineRecv, ctx.GetDataReceived())
}

func TestLoadDataTransferInfo_ConcurrentWithDirectAtomicAdd(t *testing.T) {
	// Simulates the mixed usage pattern: LoadDataTransferInfo from memcached stats calls
	// and atomic.AddInt64 from onetimeTrackedTransport / CNG proto.Size tracking
	ctx := NewDataTrackingCtx()

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	// Half use LoadDataTransferInfo (memcached path)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			ctx.LoadDataTransferInfo(10, 20)
		}()
	}

	// Half use direct atomic.AddInt64 (CNG/transport path)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			atomic.AddInt64(&ctx.DataSent, 10)
			atomic.AddInt64(&ctx.DataReceived, 20)
		}()
	}

	wg.Wait()

	assert.Equal(t, int64(goroutines*2)*10, ctx.GetDataSent())
	assert.Equal(t, int64(goroutines*2)*20, ctx.GetDataReceived())
}
