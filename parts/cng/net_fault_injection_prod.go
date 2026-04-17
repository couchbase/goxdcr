//go:build !dev

package cng

type cngNetFaultInjector struct{}

func (n *Nozzle) wrapConn(conn cngConn) cngConn {
	return conn
}
