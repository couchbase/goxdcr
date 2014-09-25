package base

import ()

//constants
var DefaultConnectionSize = 20
var DefaultPoolName = "default"

//constants for adminport
var AdminportUrlPrefix = "/"
var AdminportNumber = 12100
// AdminportReadTimeout timeout, in milliseconds, is read timeout for
// golib's http server.
var AdminportReadTimeout = 0
// AdminportWriteTimeout timeout, in milliseconds, is write timeout for
// golib's http server.
var AdminportWriteTimeout = 0

//outgoing nozzle type
type XDCROutgoingNozzleType int

const (
	XMEM XDCROutgoingNozzleType = iota
	CAPI XDCROutgoingNozzleType = iota
)

// constants for integer parsing
var ParseIntBase    = 10
var ParseIntBitSize = 64

