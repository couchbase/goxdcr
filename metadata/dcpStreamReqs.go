package metadata

import ()

type DcpStreamType int

const (
	DcpStreamTypeLegacy   DcpStreamType = iota
	DcpStreamTypeBackfill DcpStreamType = iota
)

type DcpStreamReqs interface {
	GetStreamType() DcpStreamType
}

type DcpStreamLegacy interface {
	// Nothing
}

type DcpStreamBackfill interface {
	GetHighestEndSeqno() (uint64, error)
}
