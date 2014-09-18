package base

import (

)

//constants
var DefaultConnectionSize = 20
var AdminportURLPrefix = "/xdcr_adminport/"
var AdminportNumber = 12100
var DefaultPoolName = "default"


//outgoing nozzle type
type XDCROutgoingNozzleType int
const (
	XMEM XDCROutgoingNozzleType = iota
	CAPI XDCROutgoingNozzleType = iota
)
