package iopool

import (
	"io"
	"testing"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/stretchr/testify/require"
)

type testConn struct {
	count *int
}

func (conn *testConn) Close() error {
	(*conn.count)++
	return nil
}

func TestPool_EmptyPool(t *testing.T) {
	logger := log.NewLogger("testlogger", log.DefaultLoggerContext)

	buckets := map[string]*int{
		"B1": new(int),
		"B2": new(int),
	}

	newConnFn := func(bucketName string, params interface{}) (io.Closer, error) {
		count := buckets[bucketName]
		return &testConn{
			count: count,
		}, nil
	}

	pool := NewConnPool(logger, 10,
		time.Duration(base.DefaultCLogConnPoolGCIntervalMs)*time.Millisecond,
		time.Duration(base.DefaultCLogConnPoolReapIntervalMs)*time.Millisecond,
		newConnFn, nil)
	pool.UpdateGCInterval(1 * time.Second)
	pool.UpdateReapInterval(2 * time.Second)

	pool.Close()
}

func TestPool_GC(t *testing.T) {
	logger := log.NewLogger("testlogger", log.DefaultLoggerContext)

	buckets := map[string]*int{
		"B1": new(int),
		"B2": new(int),
	}

	newConnFn := func(bucketName string, params interface{}) (io.Closer, error) {
		count := buckets[bucketName]
		return &testConn{
			count: count,
		}, nil
	}

	pool := NewConnPool(logger, 10,
		time.Duration(base.DefaultCLogConnPoolGCIntervalMs)*time.Millisecond,
		time.Duration(base.DefaultCLogConnPoolReapIntervalMs)*time.Millisecond,
		newConnFn, nil)
	pool.UpdateGCInterval(1 * time.Second)
	pool.UpdateReapInterval(3 * time.Second)

	connCount := 10
	bucket := "B1"
	connList := []io.Closer{}
	for i := 0; i < connCount; i++ {
		conn, err := pool.Get(bucket, 10*time.Millisecond, nil)
		require.Nil(t, err)
		connList = append(connList, conn)
	}

	for _, conn := range connList {
		pool.Put(bucket, conn, false)
	}

	time.Sleep(8 * time.Second)
	require.Equal(t, connCount, *(buckets[bucket]))
}
