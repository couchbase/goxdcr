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

// LoadDataTransferInfo loads the data sent and received info into the context.
func (c *Context) LoadDataTransferInfo(dataSent, dataReceived int64) {
	if c == nil {
		return
	}

	if c.TrackDataSentAndReceived {
		c.DataSent = dataSent
		c.DataReceived = dataReceived
	}
}
