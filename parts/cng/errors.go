package cng

import (
	"errors"
	"fmt"
	"strings"

	"github.com/couchbase/goxdcr/v8/base"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrProcessPanic = errors.New("panic occurred in processing of mutation")
)

// CNGErrorCode defines error codes for CNG nozzle related errors
// This is suppossed to be all encompassing error codes for CNG nozzle
type CNGErrorCode int

// CNGError defines an error type for CNG nozzle related errors
// It wraps grpc status if applicable
type CNGError struct {
	// Code is the CNG nozzle specific error code
	Code CNGErrorCode

	// grpcStatus is the gRPC status if applicable. Can be nil.
	grpcStatus *status.Status

	// msg is additional message (can be empty)
	msg string
}

func (c *CNGError) Error() string {
	ret := strings.Builder{}

	fmt.Fprintf(&ret, "CNGError Code: %d", c.Code)

	if c.msg != "" {
		ret.WriteString(",msg=")
		ret.WriteString(c.msg)
	}

	if c.grpcStatus != nil {
		ret.WriteString(",grpc=")
		// Special handling for certain error codes to extract more details or redact sensitive info
		switch c.grpcStatus.Code() {
		case codes.NotFound:
			for _, d := range c.grpcStatus.Details() {
				resInfo, ok := d.(*errdetails.ResourceInfo)
				if !ok {
					continue
				}

				// CNG follows the following format for ResourceName in error details:
				//    ResourceName: `fmt.Sprintf("%s/%s/%s", bucketName, scopeName, collectionName),`
				fmt.Fprintf(&ret, "NotFound ResourceType=%s ResourceName=%s%s%s",
					resInfo.ResourceType, base.UdTagBegin, resInfo.ResourceName, base.UdTagEnd)
			}
		default:
			ret.WriteString(c.grpcStatus.Message())
		}
	}

	return ret.String()
}

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

// mapToCNGError maps a an error to a CNGError
// the function assumes err != nil
func mapToCNGError(err error) (cngErr *CNGError) {
	cngErr, ok := err.(*CNGError)
	if ok {
		// Already a CNGError, so no need to do anything
		return cngErr
	}

	var st *status.Status
	st, ok = status.FromError(err)
	if ok {
		cngErr = mapGrpcStatusToCNGError(st)
		return
	}

	cngErr = &CNGError{
		Code: ERR_UNKNOWN,
		msg:  err.Error(),
	}

	return
}

// maps grpc Status to CNGError
// the function assumes st != nil
func mapGrpcStatusToCNGError(st *status.Status) (cngErr *CNGError) {
	code := mapGRPCStatusToCode(st)
	cngErr = &CNGError{
		Code:       code,
		grpcStatus: st,
		msg:        st.Message(),
	}

	// We further categorize certain errors based on error details
	switch st.Code() {
	case codes.NotFound:
		for _, d := range st.Details() {
			resInfo, ok := d.(*errdetails.ResourceInfo)
			if !ok {
				continue
			}

			switch resInfo.ResourceType {
			case ResourceTypeCollection:
				cngErr.Code = ERR_COLLECTION_NOT_FOUND
			case ResourceTypeScope:
				cngErr.Code = ERR_SCOPE_NOT_FOUND
			}
		}
		return
	default:
		return
	}
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

// isMutationRetryable checks if an error is retryable or not
// the err is expected of type *CNGError
// The function is designed to return true unless the error
// is known to be non-retryable
func isMutationRetryable(err error) bool {
	if err == nil {
		return false
	}

	cngErr, ok := err.(*CNGError)
	if !ok {
		return true // Not a CNGError, treat as retryable
	}

	if cngErr.grpcStatus == nil {
		return true // Unknown error, treat as retryable
	}

	code := cngErr.grpcStatus.Code()
	if grpcErrInfo, exists := grpcErrors[code]; exists {
		return !grpcErrInfo.shouldSkipRetry
	}

	return true
}

// isErrorUpstreamReportable determines whether the given error is retryable
// Raise error to upstream only if err is:
// 1. Collection Not Found
// 2. Scope Not Found
func isErrorUpstreamReportable(err error) bool {
	if err == nil {
		return false
	}

	cngErr, ok := err.(*CNGError)
	if !ok {
		// Not a CNGError, cannot determine, so do not report upstream
		// Ideally this code should not be reached
		return false
	}

	return cngErr.Code == ERR_COLLECTION_NOT_FOUND || cngErr.Code == ERR_SCOPE_NOT_FOUND
}
