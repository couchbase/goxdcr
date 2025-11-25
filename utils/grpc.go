package utils

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/couchbase/goxdcr/v8/base"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GrpcStreamHandler defines the interface for handling streamed messages.
type GrpcStreamHandler[Resp any] interface {
	// OnMessage is called for each message received from the stream.
	OnMessage(msg Resp)
	// OnError is called if an error occurs during streaming.
	OnError(err error)
	// OnComplete is called when the stream completes successfully.
	OnComplete()
}

// grpcCall handles simple unary gRPC calls.
func grpcCall[Req, Resp any](request *base.GrpcRequest[Req], rpc func(ctx context.Context, request Req, opts ...grpc.CallOption) (Resp, error)) *base.GrpcResponse[Resp] {
	resp, err := rpc(request.Context, request.Request)
	st, _ := status.FromError(err)
	return &base.GrpcResponse[Resp]{
		Resp:   resp,
		Status: st,
		Error:  err,
	}
}

// isUserTriggeredCancellation checks if the cancellation was caused by user action
func isUserTriggeredCancellation(cause error) bool {
	if cause == nil {
		return false
	}
	return errors.Is(cause, base.ErrorUserInitiatedStreamRpcCancellation)
}

// grpcServerStreamCall handles server-side streaming gRPC calls.
func grpcServerStreamCall[Req, Resp any](
	request *base.GrpcRequest[Req],
	handler GrpcStreamHandler[*Resp],
	rpc func(ctx context.Context, request Req, opts ...grpc.CallOption) (grpc.ServerStreamingClient[Resp], error),
) {
	stream, err := rpc(request.Context, request.Request)
	if err != nil {
		err = fmt.Errorf("error calling rpc: %w", err)
		handler.OnError(err)
		return
	}

	// Wait for the header to be received
	// A receipt of the header indicates that the server has accepted the request and
	// is ready to send messages
	if _, err := stream.Header(); err != nil {
		err = fmt.Errorf("error reading stream header: %w", err)
		handler.OnError(err)
		return
	}

	// Get the context for the stream
	streamCtx := stream.Context()

	// Receive messages from the stream
	for {
		resp, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				// Stream completed successfully
				handler.OnComplete()
				return
			}
			// Check for cancellation - gRPC wraps context.Canceled in a status error
			st, _ := status.FromError(err)
			if strings.Contains(st.Message(), context.Canceled.Error()) || st.Code() == codes.Canceled {
				// Check if the cancellation was caused by user action
				if isUserTriggeredCancellation(context.Cause(streamCtx)) {
					// A user-initiated cancellation occurs when XDCR intentionally ends the stream—for example,
					//  when all replications for the target bucket are deleted and the stream is no longer needed.
					// This should be treated as a successful completion of the stream.
					handler.OnComplete()
					return
				}
			}
			// any other errors (timeout, connection breakdown, application errors etc.) should be treated as errors on the client side and retried.
			handler.OnError(err)
			return
		}
		handler.OnMessage(resp)
	}
}
