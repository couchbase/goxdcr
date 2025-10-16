package cng

const (
	// CNG TODO: change this number
	DefaultDataChanSize  = 1000
	DefaultWorkerCount   = 1
	DefaultRetryInterval = 5000 // in milliseconds
	DefaultPoolSize      = 1    // Default number of connections in pool

	ConflictReasonDocMissing = "DOC_MISSING"
	ConflictReasonDocNewer   = "DOC_NEWER"
)
