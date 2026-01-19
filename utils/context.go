package utils

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
