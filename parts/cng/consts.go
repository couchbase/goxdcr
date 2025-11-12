package cng

import "time"

const (

	// ProcessRetryInterval is the interval to wait before retrying processing a request
	// This is different from the backoff time for retryable errors
	ProcessRetryInterval = 1000 * time.Millisecond

	ConflictReasonSuccess    = "SUCCESS"
	ConflictReasonDocMissing = "DOC_MISSING"
	ConflictReasonDocNewer   = "DOC_NEWER"

	ResourceTypeDocument   = "document"
	ResourceTypeCollection = "collection"
	ResourceTypeScope      = "scope"
)
