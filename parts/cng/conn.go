package cng

import (
	"context"
	"net"
	"os"

	"github.com/couchbase/goxdcr/v8/base"
	"google.golang.org/grpc"
)

// CNG TODO: Remove this file once remote cluster reference is implemented for CNG
func (n *Nozzle) getCNGCredentials() (creds base.Credentials, err error) {
	return base.Credentials{
		// CNG TODO: HARDCODED
		UserName_: "Administrator",
		Password_: "aaaaaa",
	}, nil
}

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
	creds, err := n.getCNGCredentials()
	if err != nil {
		return nil, err
	}

	cngCert, err := n.getCNGCert()
	if err != nil {
		n.Logger().Errorf("error getting CNG cert: %v", err)
		return nil, err
	}

	n.Logger().Infof("connecting at %v\n", n.cfg.Replication.SNGAddr)
	opts, err := base.NewGrpcOptions(n.cfg.Replication.SNGAddr, creds, cngCert, n.cfg.Tunables.InsecureSkipVerify)
	if err != nil {
		return nil, err
	}

	dialOpts, err := n.getDialOpts()
	if err != nil {
		return nil, err
	}
	conn, err = base.NewCngConn(opts, dialOpts...)
	return
}

// getCNGCert reads the CNG certificate from a file
// CNG TODO: This is a temporary implementation for testing purpose only
// CNG TODO: remove this function
func (n *Nozzle) getCNGCert() (buf []byte, err error) {
	return os.ReadFile("/tmp/cngcert.crt")
}
