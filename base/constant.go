package base

import ()

//constants
var DefaultConnectionSize = 20
var AdminportUrlPrefix = "/"
var AdminportNumber = 12100
var DefaultPoolName = "default"

//outgoing nozzle type
type XDCROutgoingNozzleType int

const (
	XMEM XDCROutgoingNozzleType = iota
	CAPI XDCROutgoingNozzleType = iota
)

const (
	PIPELINE_SUPERVISOR_SVC string = "PipelineSupervisor"
	CHECKPOINT_MGR_SVC string = "CheckpointManager"
)