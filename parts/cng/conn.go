package cng

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"net"

	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type CNGConn struct {
	client XDCRClient
	conn   *grpc.ClientConn
}

func (c *CNGConn) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

type grpcBasicAuth struct {
	EncodedData string
}

func (j grpcBasicAuth) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": "Basic " + j.EncodedData,
	}, nil
}

func (j grpcBasicAuth) RequireTransportSecurity() bool {
	return false
}

func newGrpcBasicAuth(username, password string) credentials.PerRPCCredentials {
	basicAuth := username + ":" + password
	authValue := base64.StdEncoding.EncodeToString([]byte(basicAuth))
	return grpcBasicAuth{authValue}
}

func (n *Nozzle) getCNGCredentials() (username, password string, err error) {
	// CNG TODO: get from remote cluster reference
	return "Administrator", "aaaaaa", nil
}

func (n *Nozzle) getDialOpts() (dialOpts []grpc.DialOption, err error) {
	username, password, err := n.getCNGCredentials()
	if err != nil {
		return nil, err
	}

	//svgConfig := grpc.WithDefaultServiceConfig(``)
	auth := grpc.WithPerRPCCredentials(newGrpcBasicAuth(username, password))
	tlsConfig := grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		// TODO: change this for production use
		InsecureSkipVerify: n.cfg.Tunables.InsecureSkipVerify,
	}))

	dialOpts = []grpc.DialOption{
		//svgConfig,
		tlsConfig,
		auth,
	}

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
func (n *Nozzle) newCNGClient() (conn *CNGConn, err error) {
	dialOpts, err := n.getDialOpts()
	if err != nil {
		return nil, err
	}

	n.Logger().Infof("connecting at %v\n", n.cfg.Replication.SNGAddr)
	grpcConn, err := grpc.NewClient(n.cfg.Replication.SNGAddr, dialOpts...)
	if err != nil {
		return nil, err
	}

	conn = &CNGConn{
		conn:   grpcConn,
		client: internal_xdcr_v1.NewXdcrServiceClient(grpcConn),
	}

	return
}
