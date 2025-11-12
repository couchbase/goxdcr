package cng

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrCollectionNotFound = errors.New("collection not found")
	ErrScopeNotFound      = errors.New("scope not found")
	ErrProcessPanic       = errors.New("panic occurred in processing of mutation")
)

// CNGErrorCode defines error codes for CNG nozzle related errors
// This is suppossed to be all encompassing error codes for CNG nozzle
type CNGErrorCode int

const (
	// Note: did not want to use iota here to make sure the values are stable

	ERR_UNKNOWN CNGErrorCode = 1

	ERR_COLLECTION_NOT_FOUND CNGErrorCode = 2
	ERR_SCOPE_NOT_FOUND      CNGErrorCode = 3

	// GRPC Errors
	ERR_GRPC_CANCELLED           CNGErrorCode = 10
	ERR_GRPC_UNKNOWN             CNGErrorCode = 11
	ERR_GRPC_INVALID_ARGUMENT    CNGErrorCode = 12
	ERR_GRPC_DEADLINE_EXCEEDED   CNGErrorCode = 13
	ERR_GRPC_NOT_FOUND           CNGErrorCode = 14
	ERR_GRPC_ALREADY_EXISTS      CNGErrorCode = 15
	ERR_GRPC_PERMISSION_DENIED   CNGErrorCode = 16
	ERR_GRPC_RESOURCE_EXHAUSTED  CNGErrorCode = 17
	ERR_GRPC_FAILED_PRECONDITION CNGErrorCode = 18
	ERR_GRPC_ABORTED             CNGErrorCode = 19
	ERR_GRPC_OUT_OF_RANGE        CNGErrorCode = 20
	ERR_GRPC_UNIMPLEMENTED       CNGErrorCode = 21
	ERR_GRPC_INTERNAL            CNGErrorCode = 22
	ERR_GRPC_UNAVAILABLE         CNGErrorCode = 23
	ERR_GRPC_DATA_LOSS           CNGErrorCode = 24
	ERR_GRPC_UNAUTHENTICATED     CNGErrorCode = 25
)

// grpcErrorInfo holds metadata about a gRPC error code
type grpcErrorInfo struct {
	// CNG error code to map to
	cngErrCode CNGErrorCode

	// whether the error is retryable
	shouldSkipRetry bool
}

// 1. Errors every goroutine sees but transient e.g. Unavailable
// 2. Errors every goroutine sees but not transient e.g. PermissionDenied
// 3. Errors only some goroutines see but transient e.g. ResourceExhausted
//    - the same goroutine will retry
// 4. Errors only some goroutines see but non-transient (e.g. InvalidArgument, Unimplemented)
//    - let goroutine handle next mutation (atleast the data is flowing)

var grpcErrors = map[codes.Code]grpcErrorInfo{
	codes.Canceled:           {cngErrCode: ERR_GRPC_CANCELLED},
	codes.Unknown:            {cngErrCode: ERR_GRPC_UNKNOWN},
	codes.DeadlineExceeded:   {cngErrCode: ERR_GRPC_DEADLINE_EXCEEDED},
	codes.NotFound:           {cngErrCode: ERR_GRPC_NOT_FOUND},
	codes.AlreadyExists:      {cngErrCode: ERR_GRPC_ALREADY_EXISTS},
	codes.PermissionDenied:   {cngErrCode: ERR_GRPC_PERMISSION_DENIED},
	codes.ResourceExhausted:  {cngErrCode: ERR_GRPC_RESOURCE_EXHAUSTED},
	codes.FailedPrecondition: {cngErrCode: ERR_GRPC_FAILED_PRECONDITION},
	codes.Aborted:            {cngErrCode: ERR_GRPC_ABORTED},
	codes.Internal:           {cngErrCode: ERR_GRPC_INTERNAL},
	codes.Unavailable:        {cngErrCode: ERR_GRPC_UNAVAILABLE},
	codes.DataLoss:           {cngErrCode: ERR_GRPC_DATA_LOSS},
	codes.Unauthenticated:    {cngErrCode: ERR_GRPC_UNAUTHENTICATED},

	// Non-retryable errors
	// These errors should not happen in normal course of operation
	codes.OutOfRange:      {cngErrCode: ERR_GRPC_OUT_OF_RANGE, shouldSkipRetry: true},
	codes.InvalidArgument: {cngErrCode: ERR_GRPC_INVALID_ARGUMENT, shouldSkipRetry: true},
	codes.Unimplemented:   {cngErrCode: ERR_GRPC_UNIMPLEMENTED, shouldSkipRetry: true},
}

// mapErrorToCode maps a an error to a CNGErrorCode
// the function assumes err != nil
func mapErrorToCode(err error) CNGErrorCode {
	var st *status.Status
	st, ok := status.FromError(err)
	if ok {
		return mapGRPCStatusToCode(st)
	}

	if errors.Is(err, ErrCollectionNotFound) {
		return ERR_COLLECTION_NOT_FOUND
	}

	if errors.Is(err, ErrScopeNotFound) {
		return ERR_SCOPE_NOT_FOUND
	}

	return ERR_UNKNOWN
}

// mapGRPCStatusToCode maps a gRPC status to a CNGErrorCode
// the function assumes st != nil
func mapGRPCStatusToCode(st *status.Status) CNGErrorCode {
	code := st.Code()
	if grpcErrInfo, exists := grpcErrors[code]; exists {
		return grpcErrInfo.cngErrCode
	}

	return ERR_UNKNOWN
}

// isErrorRetryable checks if an error is retryable or not
// the function assumes err != nil
// The function is designed to return true unless the error
// is known to be non-retryable
func isErrorRetryable(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return true
	}

	code := st.Code()
	if grpcErrInfo, exists := grpcErrors[code]; exists {
		return !grpcErrInfo.shouldSkipRetry
	}

	return true
}
