package conflictlog

import (
	"io"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/base/iopool"
	"github.com/couchbase/goxdcr/v8/utils"
	"github.com/stretchr/testify/require"
)

var fakeConnectionSleep time.Duration

type fakeConnection struct {
	sleep *time.Duration
	id    int64
}

func newFakeConnection(bucketName string) (io.Closer, error) {
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

func (f *fakeConnection) SetMeta(key string, val []byte, dataType uint8, target base.ConflictLogTarget) (err error) {
	if f.sleep != nil {
		time.Sleep(*f.sleep)
	}
	return
}

func TestLoggerImpl_closeWithOutstandingRequest(t *testing.T) {
	utils := utils.NewUtilities()

	var fakeConnectionSleep time.Duration

	pool := iopool.NewConnPool(nil, 10, func(bucketName string) (io.Closer, error) {
		return &fakeConnection{
			sleep: &fakeConnectionSleep,
			id:    NewConnId(),
		}, nil
	})

	fakeConnectionSleep = 1 * time.Second

	l, err := newLoggerImpl(nil, "1234", utils, nil, pool, WithCapacity(20))
	require.Nil(t, err)

	l.UpdateRules(&base.ConflictLogRules{
		Target: base.NewConflictLogTarget("B1", "S1", "C1"),
	})

	handles := []base.ConflictLoggerHandle{}
	for i := 0; i < 10; i++ {
		h, err := l.Log(&ConflictRecord{})
		require.Nil(t, err)

		handles = append(handles, h)
	}

	l.Close()

	_, err = l.Log(&ConflictRecord{})
	require.Equal(t, ErrLoggerClosed, err)
	l.Close()

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

func TestLoggerImpl_basicClose(t *testing.T) {
	utils := utils.NewUtilities()

	pool := iopool.NewConnPool(nil, 10, newFakeConnection)

	l, err := newLoggerImpl(nil, "1234", utils, nil, pool)
	require.Nil(t, err)

	l.Close()
	l.Close()
}
