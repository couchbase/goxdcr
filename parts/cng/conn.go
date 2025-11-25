package cng

import (
	"context"
	"net"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
	"google.golang.org/grpc"
)

func (n *Nozzle) getDialOpts() (dialOpts []grpc.DialOption, err error) {
	if n.cfg.Services.BWThrottler != nil {
		dialOpts = append(dialOpts, grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			var dialer net.Dialer
			conn, err := dialer.DialContext(ctx, "tcp", addr)
			if err != nil {
				return nil, err
			}

			return &BandwidthLimitedConn{
				conn:    conn,
				limiter: n.cfg.Services.BWThrottler,
				logger:  n.Logger(),
			}, nil
		}))
	}
	return
}

// newCNGClient creates a new gRPC client connection to CNG
// Note: grpc.NewClient does not actually create the TCP connection.
// It gets lazily created when the first RPC is made.
func (n *Nozzle) newCNGClient() (conn *base.CngConn, err error) {
	ref, err := n.getRemoteRef()
	if err != nil {
		n.Logger().Errorf("error getting remote cluster reference: %v", err)
		return nil, err
	}
	cngHost := ref.HostName()

	n.Logger().Infof("connecting at %v\n", cngHost)
	dialOpts, err := n.getDialOpts()
	if err != nil {
		return nil, err
	}
	conn, err = ref.NewCNGConn(dialOpts...)
	return
}

func (n *Nozzle) getRemoteRef() (ref *metadata.RemoteClusterReference, err error) {
	ref, err = n.cfg.Services.RemoteClusterSvc.RemoteClusterByUuid(n.cfg.Replication.TargetClusterUUID, false)
	return
}
