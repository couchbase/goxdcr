package base

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// GrpcCredentials implements gRPC PerRPCCredentials interface using basic authentication.
type GrpcCredentials struct {
	// Base64EncodedAuth contains the base64-encoded "username:password" string for gRPC authentication
	Base64EncodedAuth string
	// RequireTLS when set to true ensures the credentials are sent across only on a connection that is TLS/SSL secured
	RequireTLS bool
	// IsMTLS indicates if mutual TLS is being used for authentication
	IsMTLS bool
}

// NewGrpcCredentials creates a new GrpcCredentials instance based on the provided client credentials.
func NewGrpcCredentials(clientCreds Credentials, requireTLS bool) (*GrpcCredentials, error) {
	if len(clientCreds.ClientCertificate_) > 0 {
		// In mTLS mode, PerRPCCredentials are not required.
		// The grpc framework on server side(CNG) automatically extracts the client certificate
		// from the underlying established TLS session and makes it available in the RPC context.
		// Therefore, the certificate does not need to be attached to each outgoing RPC.
		return &GrpcCredentials{
			RequireTLS: requireTLS,
			IsMTLS:     true,
		}, nil
	}

	// For non-mTLS, username and password must be provided
	if clientCreds.UserName_ == "" || clientCreds.Password_ == "" {
		return nil, fmt.Errorf("username and password must not be empty. username empty? %t, password empty? %t", len(clientCreds.UserName_) == 0, len(clientCreds.Password_) == 0)
	}

	basicAuth := fmt.Sprintf("%s%s%s", clientCreds.UserName_, JsonDelimiter, clientCreds.Password_)
	base64EncodedAuth := base64.StdEncoding.EncodeToString([]byte(basicAuth))

	return &GrpcCredentials{
		Base64EncodedAuth: base64EncodedAuth,
		RequireTLS:        requireTLS,
		IsMTLS:            false,
	}, nil
}

// GetRequestMetadata attaches authentication headers to outgoing RPCs.
func (grpc *GrpcCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		AuthorizationKey: BasicAuthorizationKey + grpc.Base64EncodedAuth,
	}, nil
}

// RequireTransportSecurity reports whether the credentials require a secure
// transport channel (TLS). It returns true if the credentials must be
// transmitted only over TLS, and false if they can be sent over an insecure
// connection.
func (grpc *GrpcCredentials) RequireTransportSecurity() bool {
	return grpc.RequireTLS
}

// UseWithPerRPCCredentials returns true if these credentials should be attached to each outgoing RPC.
// Returns false for mTLS
func (grpc *GrpcCredentials) UseWithPerRPCCredentials() bool {
	return !grpc.IsMTLS
}

// TLSConfigOptions holds options for configuring TLS.
type TLSConfigOptions struct {
	// ServerName specifies the hostname for TLS handshake verification.
	ServerName string
	// Creds holds the user credentials for authentication.
	Creds Credentials
	// CACert contains the CA certificate for server certificate verification.
	CACert []byte
	// Insecure, when true, skips server certificate and hostname validation.
	// To be used only in dev or local envs.
	Insecure bool
}

// IsCapella checks if the ServerName indicates a Capella deployment.
func (opts *TLSConfigOptions) IsCapella() bool {
	return strings.Contains(opts.ServerName, CapellaHostnameSuffix)
}

// configureTLS creates a TLS configuration based on the provided options.
func configureTLS(opts TLSConfigOptions) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: opts.Insecure,
	}
	if opts.Insecure {
		return tlsConfig, nil
	}

	tlsConfig.ServerName = opts.ServerName

	certPool, err := configureCACertPool(opts.CACert, opts.IsCapella())
	if err != nil {
		return nil, fmt.Errorf("failed to configure CA certificate pool: %w", err)
	}
	tlsConfig.RootCAs = certPool

	// mTLS: attach client cert/key if provided
	// CNG currently does not support mtls
	// Tracking ticket - https://jira.issues.couchbase.com/browse/ING-662
	if len(opts.Creds.ClientCertificate_) > 0 && len(opts.Creds.ClientKey_) > 0 {
		cert, tlsErr := tls.X509KeyPair(opts.Creds.ClientCertificate_, opts.Creds.ClientKey_)
		if tlsErr != nil {
			return nil, fmt.Errorf("failed to load client certificate/key pair: %w", tlsErr)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

// configureCACertPool creates a certificate pool for server cert verification.
// For Capella, we can rely on the system's default trust store unless a CA certificate is provided explicitly.
// For non-Capella, a valid CA certificate is required.
func configureCACertPool(caCert []byte, isCapella bool) (*x509.CertPool, error) {
	if isCapella && len(caCert) == 0 {
		// Use the system's trust store to validate the server cert.
		return nil, nil
	}

	if len(caCert) == 0 {
		// caCert is required in non-capella deployments
		return nil, errors.New("CA certificate is required for non-Capella deployments")
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caCert) {
		return nil, errors.New("failed to parse CA certificate")
	}
	return certPool, nil
}

// CngConn encapsulates a gRPC connection and the associated XdcrService client.
type CngConn struct {
	conn   *grpc.ClientConn
	client internal_xdcr_v1.XdcrServiceClient
}

// Close closes the underlying gRPC connection.
func (c *CngConn) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// Client returns the XdcrServiceClient for making gRPC calls.
func (c *CngConn) Client() internal_xdcr_v1.XdcrServiceClient {
	return c.client
}

// NewCngConn establishes a gRPC connection using the provided GrpcOptions and dial options,
// and returns a CngConn that wraps both the connection and the protostellar-defined XDCR client.
func NewCngConn(grpcOptions *GrpcOptions, dialOptions ...grpc.DialOption) (*CngConn, error) {
	// Configure per-RPC credentials.
	grpcCreds, err := NewGrpcCredentials(grpcOptions.Creds, grpcOptions.UseTLS)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC credentials: %w", err)
	}

	// Configure TLS.
	tlsOpts := TLSConfigOptions{
		ServerName: GetHostName(grpcOptions.ConnStr),
		CACert:     grpcOptions.CaCert,
		Creds:      grpcOptions.Creds,
		Insecure:   !grpcOptions.UseTLS,
	}
	tlsConfig, err := configureTLS(tlsOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to configure TLS: %w", err)
	}

	// Set up gRPC dial options.
	dialOptions = append(dialOptions,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		grpc.WithUserAgent(UserAgent+CngUserAgentSuffix),
	)
	if grpcCreds.UseWithPerRPCCredentials() {
		dialOptions = append(dialOptions, grpc.WithPerRPCCredentials(grpcCreds))
	}

	// Establish the gRPC connection.
	grpcConn, err := grpc.NewClient(grpcOptions.ConnStr, dialOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to establish gRPC connection: %v", err)
	}

	return &CngConn{conn: grpcConn,
		client: internal_xdcr_v1.NewXdcrServiceClient(grpcConn)}, nil
}
