package cng

import "time"

const (
	// MaxProcessRetryInterval caps the maximum wait time between retries when processing
	// a request hits a non-network retryable error.
	MaxProcessRetryInterval = 40 * time.Second
	// JitterDuration controls the max additional random delay (1s..JitterDuration)
	// added to retry waits to avoid synchronized retries across workers.
	JitterDuration = 5 * time.Second

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
