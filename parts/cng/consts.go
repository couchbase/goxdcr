package cng

import "time"

const (

	// ProcessRetryInterval is the interval to wait before retrying processing a request
	// This is different from the backoff time for retryable errors
	ProcessRetryInterval = 5000 * time.Millisecond

	WithConnRetryBackoffInitial = 2 * time.Second
	WithConnRetryBackoffMax     = 60 * time.Second
	WithConnRetryBackoffFactor  = 1.5

	ConflictReasonSuccess    = "SUCCESS"
	ConflictReasonDocMissing = "DOC_MISSING"
	ConflictReasonDocNewer   = "DOC_NEWER"

	ResourceTypeDocument   = "document"
	ResourceTypeCollection = "collection"
	ResourceTypeScope      = "scope"

	// CNGAbortReasonVbuuidMismatch indicates that the vbuuid from source and target do not match
	// This is stamped in the details of grpc error for code == Aborted
	CNGAbortReasonVbuuidMismatch = "VBUUID_MISMATCH"
)
