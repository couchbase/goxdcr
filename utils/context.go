package utils

import "sync/atomic"

// Context struct points to a bunch of optional context that could be used in utils methods
type Context struct {
	// Data Tracking based context
	// They are meant to be used once and thrown away
	DataSent                 int64
	DataReceived             int64
	TrackDataSentAndReceived bool
}

func NewDataTrackingCtx() *Context {
	return &Context{
		TrackDataSentAndReceived: true,
	}
}

func NewDefaultContext() *Context {
	return &Context{}
}

// LoadDataTransferInfo atomically accumulates the data sent and received info into the context.
func (c *Context) LoadDataTransferInfo(dataSent, dataReceived int64) {
	if c == nil {
		return
	}

	if c.TrackDataSentAndReceived {
		atomic.AddInt64(&c.DataSent, dataSent)
		atomic.AddInt64(&c.DataReceived, dataReceived)
	}
}

// GetDataSent atomically loads the total data sent.
func (c *Context) GetDataSent() int64 {
	return atomic.LoadInt64(&c.DataSent)
}

// GetDataReceived atomically loads the total data received.
func (c *Context) GetDataReceived() int64 {
	return atomic.LoadInt64(&c.DataReceived)
}
