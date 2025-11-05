package cng

import (
	"net"
	"time"

	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/service_def"
)

// BandwidthLimitedConn is a net.Conn wrapper that limits bandwidth usage
type BandwidthLimitedConn struct {
	conn    net.Conn
	limiter service_def.BandwidthThrottlerSvc
	logger  *log.CommonLogger
}

func (b *BandwidthLimitedConn) Read(p []byte) (n int, err error) {
	n, err = b.conn.Read(p)
	return n, err
}

func (b *BandwidthLimitedConn) Write(p []byte) (n int, err error) {
	allowedBytes := int64(0)
	toSent := int64(len(p))

	// The function does not return error it returns (bytesCanSend int64, bytesAllowed int64).
	// In this context we only need the first value.
	// This is because we are not sending partial buffer.
	allowedBytes, _ = b.limiter.Throttle(toSent, toSent, toSent)
	for allowedBytes < toSent {
		b.logger.Debugf("throttling write, allowedBytes=%v, toSent=%v\n", allowedBytes, toSent)
		err = b.limiter.Wait()
		if err != nil {
			return 0, err
		}
		allowedBytes, _ = b.limiter.Throttle(toSent, toSent, toSent)
	}

	n, err = b.conn.Write(p)
	return n, err
}

func (b *BandwidthLimitedConn) Close() error {
	return b.conn.Close()
}

func (b *BandwidthLimitedConn) LocalAddr() net.Addr {
	return b.conn.LocalAddr()
}

func (b *BandwidthLimitedConn) RemoteAddr() net.Addr {
	return b.conn.RemoteAddr()
}

func (b *BandwidthLimitedConn) SetDeadline(t time.Time) error {
	return b.conn.SetDeadline(t)
}

func (b *BandwidthLimitedConn) SetReadDeadline(t time.Time) error {
	return b.conn.SetReadDeadline(t)
}

func (b *BandwidthLimitedConn) SetWriteDeadline(t time.Time) error {
	return b.conn.SetWriteDeadline(t)
}
