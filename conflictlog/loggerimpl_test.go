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
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	baseclog "github.com/couchbase/goxdcr/v8/base/conflictlog"
	"github.com/couchbase/goxdcr/v8/base/iopool"
	common "github.com/couchbase/goxdcr/v8/common/mocks"
	"github.com/couchbase/goxdcr/v8/log"
	service_def "github.com/couchbase/goxdcr/v8/service_def/mocks"
	utils "github.com/couchbase/goxdcr/v8/utils/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func setupMocks() (*service_def.XDCRCompTopologySvc, *service_def.SecuritySvc, *utils.UtilsIface, *common.PipelineEventsProducer) {
	compSvc := &service_def.XDCRCompTopologySvc{}
	securitySvc := &service_def.SecuritySvc{}
	utils := &utils.UtilsIface{}
	producer := &common.PipelineEventsProducer{}
	compSvc.On("MyConnectionStr").Return("localhost:9000", nil)
	securitySvc.On("IsClusterEncryptionLevelStrict").Return(false)
	bucketMap := make(map[string]interface{})
	bucketMap["numVBuckets"] = float64(1024)
	bucketMap["uuid"] = "loremepsum"
	utils.On("GetBucketInfo", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(bucketMap, nil)
	utils.On("IsSeriousNetError", mock.Anything).Return(false)
	producer.On("DismissEvent", mock.Anything).Return(nil)
	producer.On("AddEvent", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(int64(0))

	return compSvc, securitySvc, utils, producer
}

type fakeConnection struct {
	sleep *time.Duration
	id    int64
}

func newFakeConnection(string, interface{}) (io.Closer, error) {
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
	topoSvc, securitySvc, utils, _ := setupMocks()
	rules := &baseclog.Rules{Target: baseclog.NewTarget("B1", "S1", "C1")}

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
	l, err := newLoggerImpl(log.NewLogger("test", log.DefaultLoggerContext), "1234", utils, securitySvc, nil, topoSvc, pool, nil,
		mcache,
		WithCapacity(20), WithRules(rules))
	require.Nil(t, err)
	assert.Nil(t, l.Start(nil))

	l.UpdateRules(&baseclog.Rules{
		Target: baseclog.NewTarget("B1", "S1", "C1"),
	})

	for i := 0; i < 10; i++ {
		err := l.Log(&ConflictRecord{})
		require.Nil(t, err)
	}

	assert.Nil(t, l.Stop())

	err = l.Log(&ConflictRecord{})
	require.Equal(t, baseclog.ErrLoggerClosed, err)
	assert.Nil(t, l.Stop())
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
	topo, security, utils, _ := setupMocks()
	rules := &baseclog.Rules{Target: baseclog.NewTarget("B1", "S1", "C1")}

	pool := iopool.NewConnPool(nil, 10,
		time.Duration(base.DefaultCLogConnPoolGCIntervalMs)*time.Millisecond,
		time.Duration(base.DefaultCLogConnPoolReapIntervalMs)*time.Millisecond,
		newFakeConnection, nil)

	mcache := NewManifestCache()
	l, err := newLoggerImpl(nil, "1234", utils, security, nil, topo, pool, nil, mcache, WithRules(rules))
	require.Nil(t, err)
	assert.Nil(t, l.Start(nil))

	assert.Nil(t, l.Stop())
	assert.Nil(t, l.Stop())
}

func TestLoggerImpl_UpdateWorker(t *testing.T) {
	topoSvc, securitySvc, utils, _ := setupMocks()
	rules := &baseclog.Rules{Target: baseclog.NewTarget("B1", "S1", "C1")}

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
	l, err := newLoggerImpl(nil, "1234", utils, securitySvc, nil, topoSvc, pool, nil, mcache, WithCapacity(20), WithWorkerCount(2), WithRules(rules))
	assert.Nil(t, err)
	assert.Nil(t, l.Start(nil))

	// same as before
	err = l.UpdateWorkerCount(2)
	assert.Equal(t, err, baseclog.ErrNoChange)
	testNumWorkers(t, l, 2)
	assert.Nil(t, l.Stop())

	// 2. update with a higher value
	l, err = newLoggerImpl(nil, "1234", utils, securitySvc, nil, topoSvc, pool, nil, mcache, WithCapacity(20), WithWorkerCount(2), WithRules(rules))
	assert.Nil(t, err)
	assert.Nil(t, l.Start(nil))

	// more than before
	err = l.UpdateWorkerCount(4)
	assert.Nil(t, err)
	assert.Equal(t, l.opts.workerCount, 4)
	testNumWorkers(t, l, 4)
	assert.Nil(t, l.Stop())

	// 3. update with a lower value
	l, err = newLoggerImpl(nil, "1234", utils, securitySvc, nil, topoSvc, pool, nil, mcache, WithCapacity(20), WithWorkerCount(3), WithRules(rules))
	assert.Nil(t, err)
	assert.Nil(t, l.Start(nil))

	// less than before
	err = l.UpdateWorkerCount(1)
	assert.Nil(t, err)
	testNumWorkers(t, l, 1)
	assert.Nil(t, l.Stop())
}

func TestLoggerImpl_UpdateCapacity(t *testing.T) {
	topo, security, utils, producer := setupMocks()
	rules := &baseclog.Rules{Target: baseclog.NewTarget("B1", "S1", "C1")}
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
	l, err := newLoggerImpl(nil, "1234", utils, security, nil, topo, pool, nil, mcache, WithCapacity(20), WithRules(rules))
	assert.Nil(t, err)
	assert.Equal(t, l.opts.logQueueCap, 20)
	assert.Nil(t, l.Start(nil))

	// same as before
	err = l.UpdateQueueCapcity(20)
	assert.Equal(t, err, baseclog.ErrNoChange)
	testCapacity(l, 20)
	assert.Nil(t, l.Stop())

	// 2. update with a higher value
	l, err = newLoggerImpl(nil, "1234", utils, security, nil, topo, pool, nil, mcache, WithCapacity(20), WithRules(rules))
	assert.Nil(t, err)
	assert.Equal(t, l.opts.logQueueCap, 20)
	assert.Nil(t, l.Start(nil))

	// more than before
	err = l.UpdateQueueCapcity(40)
	assert.Nil(t, err)
	testCapacity(l, 40)
	assert.Nil(t, l.Stop())

	// 3. update with a lower value
	l, err = newLoggerImpl(nil, "1234", utils, security, nil, topo, pool, nil, mcache, WithCapacity(20), WithRules(rules))
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
	l, err = newLoggerImpl(log.NewLogger("logger", log.DefaultLoggerContext), "1234", utils, security, nil, topo, pool, producer, mcache, WithWorkerCount(0), WithCapacity(numItems), WithRules(rules))
	assert.Nil(t, err)
	assert.Equal(t, l.opts.logQueueCap, numItems)
	assert.Nil(t, l.Start(nil))

	// simulate conflict writes
	for i := 0; i < numItems; i++ {
		l.logReqCh <- logRequest{
			conflictRec: &ConflictRecord{},
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
	l, err = newLoggerImpl(nil, "1234", utils, security, nil, topo, pool, nil, mcache, WithWorkerCount(10), WithRules(rules))
	assert.Nil(t, err)
	assert.Equal(t, l.opts.workerCount, 10)
	assert.Nil(t, l.Start(nil))

	err = l.UpdateQueueCapcity(numItems + 1) // increase the capacity
	testNumWorkers(t, l, 10)
	assert.Equal(t, l.opts.workerCount, 10)
	assert.Nil(t, l.Stop())
}
