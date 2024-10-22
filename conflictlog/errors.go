package conflictlog

import "errors"

var (
	ErrManagerNotInitialized error = errors.New("conflict manager not initialized")
	ErrWriterTimeout         error = errors.New("conflict writer timed out")
	ErrWriterClosed          error = errors.New("conflict writer closed")
	ErrQueueFull             error = errors.New("conflict log is full")
	ErrLoggerClosed          error = errors.New("conflict logger is closed")
	ErrLogWaitAborted        error = errors.New("conflict log handle received abort")
	ErrEmptyRules            error = errors.New("empty conflict rules")
	ErrUnknownCollection     error = errors.New("unknown collection")
	ErrClosedConnPool        error = errors.New("use of closed connection pool")
	ErrNotMyBucket           error = errors.New("not my bucket")
	ErrConnPoolGetTimeout    error = errors.New("conflict logging pool get timedout")
	ErrSetMetaTimeout        error = errors.New("conflict logging SetMeta timedout")
	ErrConflictLoggingIsOff  error = errors.New("conflict logging is off")
)
