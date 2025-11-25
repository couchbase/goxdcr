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
	// GetCredentials is a function pointer that retrieves connection credentials dynamically.
	GetCredentials func() *Credentials
	// RequireTLS when set to true ensures the credentials are sent across only on a connection that is TLS/SSL secured
	RequireTLS bool
	// IsMTLS indicates if mutual TLS is being used for authentication
	IsMTLS bool

	// headers hold the pre-computed authentication headers
	headers map[string]string
}

// NewGrpcCredentials creates a new GrpcCredentials instance based on the provided client credentials.
// This computes the headers once and caches it. Incase the creds are invalid then the pipeline is
// expected to recreate n/w connection and retry.
func NewGrpcCredentials(getCredentials func() *Credentials, requireTLS bool) (*GrpcCredentials, error) {
	// Get the client credentials to determine if mTLS is required.
	clientCreds := getCredentials()

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

	g := &GrpcCredentials{
		GetCredentials: getCredentials,
		RequireTLS:     requireTLS,
		IsMTLS:         false,
	}

	err := g.prepRequestMetadata()
	if err != nil {
		return nil, fmt.Errorf("failed to prepare request metadata: %w", err)
	}

	return g, nil
}

// prepare request metadata with authentication headers once and reuse it
func (grpc *GrpcCredentials) prepRequestMetadata() error {
	credentials := grpc.GetCredentials()

	if err := credentials.Validate(); err != nil {
		return err
	}

	basicAuth := fmt.Sprintf("%s%s%s", credentials.UserName_, JsonDelimiter, credentials.Password_)
	base64EncodedAuth := base64.StdEncoding.EncodeToString([]byte(basicAuth))

	grpc.headers = map[string]string{
		AuthorizationKey: BasicAuthorizationKey + base64EncodedAuth,
	}
	return nil
}

// GetRequestMetadata attaches authentication headers to outgoing RPCs.
func (grpc *GrpcCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return grpc.headers, nil
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
	// GetCredentials is a function pointer that retrieves connection credentials dynamically.
	GetCredentials func() *Credentials
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

// GetClientCertificate is a function pointer that retrieves the client certificate for the connection.
// This is used to support dynamic client certificate refresh.
func (opts *TLSConfigOptions) GetClientCertificate(cri *tls.CertificateRequestInfo) (*tls.Certificate, error) {
	clientCreds := opts.GetCredentials()
	if len(clientCreds.ClientCertificate_) == 0 {
		// No client certificate to provide. (i.e., no mTLS)
		// return an empty certificate to indicate that no client certificate is available.
		// Go's tls package will not send any client certificate in this case.
		return new(tls.Certificate), nil
	}
	cert, err := tls.X509KeyPair(clientCreds.ClientCertificate_, clientCreds.ClientKey_)
	if err != nil {
		return nil, fmt.Errorf("failed to load client certificate/key pair: %w", err)
	}
	return &cert, nil
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
	tlsConfig.GetClientCertificate = opts.GetClientCertificate

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
	client CngClient
}

// Close closes the underlying gRPC connection.
func (c *CngConn) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// Client returns the XdcrServiceClient for making gRPC calls.
func (c *CngConn) Client() CngClient {
	return c.client
}

// NewCngConn establishes a gRPC connection using the provided GrpcOptions and dial options,
// and returns a CngConn that wraps both the connection and the protostellar-defined XDCR client.
func NewCngConn(grpcOptions *GrpcOptions, dialOptions ...grpc.DialOption) (*CngConn, error) {
	// Configure per-RPC credentials.
	grpcCreds, err := NewGrpcCredentials(grpcOptions.GetCredentials, grpcOptions.UseTLS)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC credentials: %w", err)
	}

	// Configure TLS.
	tlsOpts := TLSConfigOptions{
		ServerName:     GetHostName(grpcOptions.ConnStr),
		CACert:         grpcOptions.CaCert,
		GetCredentials: grpcOptions.GetCredentials,
		Insecure:       !grpcOptions.UseTLS,
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
