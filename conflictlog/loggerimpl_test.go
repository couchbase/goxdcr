package conflictlog

import (
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	baseclog "github.com/couchbase/goxdcr/v8/base/conflictlog"
	"github.com/couchbase/goxdcr/v8/log"

	"github.com/couchbase/goxdcr/v8/base/iopool"
	"github.com/couchbase/goxdcr/v8/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeConnection struct {
	sleep *time.Duration
	id    int64
}

func newFakeConnection(bucketName string, params interface{}) (io.Closer, error) {
	return &fakeConnection{
		id: NewConnId(),
	}, nil
}

func (f *fakeConnection) Id() int64 {
	return f.id
}

func (f *fakeConnection) Close() error {
	return nil
}

func (f *fakeConnection) SetMeta(key string, val []byte, dataType uint8, target baseclog.Target) (err error) {
	if f.sleep != nil {
		time.Sleep(*f.sleep)
	}
	return
}

func TestLoggerImpl_closeWithOutstandingRequest(t *testing.T) {
	utils := utils.NewUtilities()

	var fakeConnectionSleep time.Duration

	pool := iopool.NewConnPool(nil, 10,
		time.Duration(base.DefaultCLogConnPoolGCIntervalMs)*time.Millisecond,
		time.Duration(base.DefaultCLogConnPoolReapIntervalMs)*time.Millisecond,
		func(bucketName string, params interface{}) (io.Closer, error) {
			return &fakeConnection{
				sleep: &fakeConnectionSleep,
				id:    NewConnId(),
			}, nil
		},
		nil)

	fakeConnectionSleep = 1 * time.Second

	mcache := NewManifestCache()
	l, err := newLoggerImpl(log.NewLogger("test", log.DefaultLoggerContext), "1234", utils, nil, nil, nil, pool, nil,
		mcache,
		WithCapacity(20))
	require.Nil(t, err)
	assert.Nil(t, l.Start(nil))

	l.UpdateRules(&baseclog.Rules{
		Target: baseclog.NewTarget("B1", "S1", "C1"),
	})

	handles := []base.ConflictLoggerHandle{}
	for i := 0; i < 10; i++ {
		h, err := l.Log(&ConflictRecord{})
		require.Nil(t, err)

		handles = append(handles, h)
	}

	assert.Nil(t, l.Stop())

	_, err = l.Log(&ConflictRecord{})
	require.Equal(t, baseclog.ErrLoggerClosed, err)
	assert.Nil(t, l.Stop())

	wg := &sync.WaitGroup{}
	for _, h := range handles {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err = h.Wait(nil)
			require.Nil(t, err)
		}()
	}
	wg.Wait()
}

// the workers will be shutdown to test the number of workers.
// so the caller should expect 0 workers in the loggers after this routine exits.
func testNumWorkers(t *testing.T, l *LoggerImpl, num int) {
	assert.Equal(t, l.opts.workerCount, num)

	for i := 0; i < num; i++ {
		select {
		case l.shutdownWorkerCh <- true:
			time.Sleep(time.Second)
			if len(l.shutdownWorkerCh) > 0 {
				assert.FailNow(t, fmt.Sprintf("lesser workers than %v", num))
			}
		default:
			assert.FailNow(t, fmt.Sprintf("lesser workers than %v", num))
		}
	}

	select {
	case l.shutdownWorkerCh <- true:
		time.Sleep(time.Second)
		if len(l.shutdownWorkerCh) == 0 {
			assert.FailNow(t, fmt.Sprintf("more workers than %v", num))
		}
	default:
	}

}

func TestLoggerImpl_basicClose(t *testing.T) {
	utils := utils.NewUtilities()

	pool := iopool.NewConnPool(nil, 10,
		time.Duration(base.DefaultCLogConnPoolGCIntervalMs)*time.Millisecond,
		time.Duration(base.DefaultCLogConnPoolReapIntervalMs)*time.Millisecond,
		newFakeConnection, nil)

	mcache := NewManifestCache()
	l, err := newLoggerImpl(nil, "1234", utils, nil, nil, nil, pool, nil, mcache)
	require.Nil(t, err)
	assert.Nil(t, l.Start(nil))

	assert.Nil(t, l.Stop())
	assert.Nil(t, l.Stop())
}

func TestLoggerImpl_UpdateWorker(t *testing.T) {
	utils := utils.NewUtilities()

	var fakeConnectionSleep time.Duration

	pool := iopool.NewConnPool(nil, 10,
		time.Duration(base.DefaultCLogConnPoolGCIntervalMs)*time.Millisecond,
		time.Duration(base.DefaultCLogConnPoolReapIntervalMs)*time.Millisecond,
		func(bucketName string, params interface{}) (io.Closer, error) {
			return &fakeConnection{
				sleep: &fakeConnectionSleep,
				id:    NewConnId(),
			}, nil
		},
		nil)

	fakeConnectionSleep = 1 * time.Second

	mcache := NewManifestCache()
	// 1. update with same value
	l, err := newLoggerImpl(nil, "1234", utils, nil, nil, nil, pool, nil, mcache, WithCapacity(20), WithWorkerCount(2))
	assert.Nil(t, err)
	assert.Nil(t, l.Start(nil))

	// same as before
	err = l.UpdateWorkerCount(2)
	assert.Equal(t, err, baseclog.ErrNoChange)
	testNumWorkers(t, l, 2)
	assert.Nil(t, l.Stop())

	// 2. update with a higher value
	l, err = newLoggerImpl(nil, "1234", utils, nil, nil, nil, pool, nil, mcache, WithCapacity(20), WithWorkerCount(2))
	assert.Nil(t, err)
	assert.Nil(t, l.Start(nil))

	// more than before
	err = l.UpdateWorkerCount(4)
	assert.Nil(t, err)
	assert.Equal(t, l.opts.workerCount, 4)
	testNumWorkers(t, l, 4)
	assert.Nil(t, l.Stop())

	// 3. update with a lower value
	l, err = newLoggerImpl(nil, "1234", utils, nil, nil, nil, pool, nil, mcache, WithCapacity(20), WithWorkerCount(3))
	assert.Nil(t, err)
	assert.Nil(t, l.Start(nil))

	// less than before
	err = l.UpdateWorkerCount(1)
	assert.Nil(t, err)
	testNumWorkers(t, l, 1)
	assert.Nil(t, l.Stop())
}

func TestLoggerImpl_UpdateCapacity(t *testing.T) {
	utils := utils.NewUtilities()

	var fakeConnectionSleep time.Duration

	pool := iopool.NewConnPool(nil, 10,
		time.Duration(base.DefaultCLogConnPoolGCIntervalMs)*time.Millisecond,
		time.Duration(base.DefaultCLogConnPoolReapIntervalMs)*time.Millisecond,
		func(bucketName string, params interface{}) (io.Closer, error) {
			return &fakeConnection{
				sleep: &fakeConnectionSleep,
				id:    NewConnId(),
			}, nil
		},
		nil)

	fakeConnectionSleep = 1 * time.Second

	testCapacity := func(l *LoggerImpl, num int) {
		assert.Equal(t, l.opts.logQueueCap, num)
		assert.Equal(t, cap(l.logReqCh), num)
	}

	mcache := NewManifestCache()
	// 1. update with same value
	l, err := newLoggerImpl(nil, "1234", utils, nil, nil, nil, pool, nil, mcache, WithCapacity(20))
	assert.Nil(t, err)
	assert.Equal(t, l.opts.logQueueCap, 20)
	assert.Nil(t, l.Start(nil))

	// same as before
	err = l.UpdateQueueCapcity(20)
	assert.Equal(t, err, baseclog.ErrNoChange)
	testCapacity(l, 20)
	assert.Nil(t, l.Stop())

	// 2. update with a higher value
	l, err = newLoggerImpl(nil, "1234", utils, nil, nil, nil, pool, nil, mcache, WithCapacity(20))
	assert.Nil(t, err)
	assert.Equal(t, l.opts.logQueueCap, 20)
	assert.Nil(t, l.Start(nil))

	// more than before
	err = l.UpdateQueueCapcity(40)
	assert.Nil(t, err)
	testCapacity(l, 40)
	assert.Nil(t, l.Stop())

	// 3. update with a lower value
	l, err = newLoggerImpl(nil, "1234", utils, nil, nil, nil, pool, nil, mcache, WithCapacity(20))
	assert.Nil(t, err)
	assert.Equal(t, l.opts.logQueueCap, 20)
	assert.Nil(t, l.Start(nil))

	// less than before - cannot be done
	err = l.UpdateQueueCapcity(10)
	assert.NotNil(t, err)
	assert.Nil(t, l.Stop())

	// 4. non-empty queue - test conflicts are not lost
	numItems := 100000
	readCount := 0
	l, err = newLoggerImpl(nil, "1234", utils, nil, nil, nil, pool, nil, mcache, WithWorkerCount(0), WithCapacity(numItems))
	assert.Nil(t, err)
	assert.Equal(t, l.opts.logQueueCap, numItems)
	assert.Nil(t, l.Start(nil))

	// simulate conflict writes
	for i := 0; i < numItems; i++ {
		l.logReqCh <- logRequest{
			conflictRec: &ConflictRecord{},
			// have a buffered channel because we don't have a reader of this channel.
			// i.e. no Wait() will be called.
			ackCh: make(chan error, 1),
		}
	}

	finCh := make(chan bool)

	// pseudo worker to test that conflicts are not lost
	go func() {
		for {
			select {
			case <-finCh:
				return
			case req := <-l.logReqCh:
				if req.conflictRec == nil {
					continue
				}
				readCount++
				time.Sleep(200 * time.Millisecond)
			}
		}
	}()

	go func() {
		// increase the capacity
		err = l.UpdateQueueCapcity(numItems + 1)
		assert.Nil(t, err)
	}()

	// sleep 2 seconds for mu to be acquired by UpdateQueueCapcity
	// this way testCapacity will only be called after UpdateQueueCapcity is done.
	time.Sleep(2 * time.Second)

	l.lock.Lock()
	close(finCh)
	testCapacity(l, numItems+1)
	l.lock.Unlock()
	assert.Equal(t, len(l.logReqCh)+readCount, numItems)

	// have actual workers and check if the queue is read from
	err = l.UpdateWorkerCount(len(l.logReqCh))
	time.Sleep(5 * time.Second)
	assert.Equal(t, len(l.logReqCh), 0)
	assert.Nil(t, l.Stop())

	// 5. test that the worker count remains the same
	l, err = newLoggerImpl(nil, "1234", utils, nil, nil, nil, pool, nil, mcache, WithWorkerCount(10))
	assert.Nil(t, err)
	assert.Equal(t, l.opts.workerCount, 10)
	assert.Nil(t, l.Start(nil))

	err = l.UpdateQueueCapcity(numItems + 1) // increase the capacity
	testNumWorkers(t, l, 10)
	assert.Equal(t, l.opts.workerCount, 10)
	assert.Nil(t, l.Stop())
}
