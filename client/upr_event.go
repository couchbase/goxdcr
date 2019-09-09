package memcached

import (
	"encoding/binary"
	"fmt"
	"github.com/couchbase/gomemcached"
	"github.com/couchbase/goutils/logging"
)

type SystemEventType int

const (
	CollectionCreate SystemEventType = iota
	CollectionDrop   SystemEventType = iota
	CollectionFlush  SystemEventType = iota
	ScopeCreate      SystemEventType = iota
	ScopeDrop        SystemEventType = iota
)

type ScopeCreateEvent interface {
	GetName() (string, error)
	GetId() (uint32, error) // Gets the SID of the created scope
	GetManifestId() (uint64, error)
}

type CollectionCreateEvent interface {
	GetName() (string, error)
	GetId() (uint32, error) // Gets the CID of the created collection
	GetManifestId() (uint64, error)
}

type CollectionDropEvent interface {
	GetId() (uint32, error) // Gets the CID of the dropped collection
	GetManifestId() (uint64, error)
}

type CollectionFlushEvent interface {
	GetId() (uint32, error) // Gets the CID of the flush collection
	GetManifestId() (uint64, error)
}

type ScopeDropEvent interface {
	GetId() (uint32, error) // Gets the SID of the dropped scope
	GetManifestId() (uint64, error)
}

var InvalidOp error = fmt.Errorf("Invalid Operation")

// UprEvent memcached events for UPR streams.
type UprEvent struct {
	Opcode          gomemcached.CommandCode // Type of event
	Status          gomemcached.Status      // Response status
	VBucket         uint16                  // VBucket this event applies to
	DataType        uint8                   // data type
	Opaque          uint16                  // 16 MSB of opaque
	VBuuid          uint64                  // This field is set by downstream
	Flags           uint32                  // Item flags
	Expiry          uint32                  // Item expiration time
	Key, Value      []byte                  // Item key/value
	OldValue        []byte                  // TODO: TBD: old document value
	Cas             uint64                  // CAS value of the item
	Seqno           uint64                  // sequence number of the mutation
	RevSeqno        uint64                  // rev sequence number : deletions
	LockTime        uint32                  // Lock time
	MetadataSize    uint16                  // Metadata size
	SnapstartSeq    uint64                  // start sequence number of this snapshot
	SnapendSeq      uint64                  // End sequence number of the snapshot
	SnapshotType    uint32                  // 0: disk 1: memory
	FailoverLog     *FailoverLog            // Failover log containing vvuid and sequnce number
	Error           error                   // Error value in case of a failure
	ExtMeta         []byte                  // Extended Metadata
	AckSize         uint32                  // The number of bytes that can be Acked to DCP
	SystemEvent     SystemEventType         // Only valid if IsSystemEvent() is true
	SysEventVersion uint8                   // Based on the version, the way Extra bytes is parsed is different
	ValueLen        int                     // Cache it to avoid len() calls for performance
}

// FailoverLog containing vvuid and sequnce number
type FailoverLog [][2]uint64

func makeUprEvent(rq gomemcached.MCRequest, stream *UprStream, bytesReceivedFromDCP int) *UprEvent {
	event := &UprEvent{
		Opcode:   rq.Opcode,
		VBucket:  stream.Vbucket,
		VBuuid:   stream.Vbuuid,
		Key:      rq.Key,
		Value:    rq.Body,
		Cas:      rq.Cas,
		ExtMeta:  rq.ExtMeta,
		DataType: rq.DataType,
		ValueLen: len(rq.Body),
	}

	// set AckSize for events that need to be acked to DCP,
	// i.e., events with CommandCodes that need to be buffered in DCP
	if _, ok := gomemcached.BufferedCommandCodeMap[rq.Opcode]; ok {
		event.AckSize = uint32(bytesReceivedFromDCP)
	}

	// 16 LSBits are used by client library to encode vbucket number.
	// 16 MSBits are left for application to multiplex on opaque value.
	event.Opaque = appOpaque(rq.Opaque)

	if len(rq.Extras) >= uprMutationExtraLen &&
		event.Opcode == gomemcached.UPR_MUTATION {

		event.Seqno = binary.BigEndian.Uint64(rq.Extras[:8])
		event.RevSeqno = binary.BigEndian.Uint64(rq.Extras[8:16])
		event.Flags = binary.BigEndian.Uint32(rq.Extras[16:20])
		event.Expiry = binary.BigEndian.Uint32(rq.Extras[20:24])
		event.LockTime = binary.BigEndian.Uint32(rq.Extras[24:28])
		event.MetadataSize = binary.BigEndian.Uint16(rq.Extras[28:30])

	} else if len(rq.Extras) >= uprDeletetionWithDeletionTimeExtraLen &&
		event.Opcode == gomemcached.UPR_DELETION {

		event.Seqno = binary.BigEndian.Uint64(rq.Extras[:8])
		event.RevSeqno = binary.BigEndian.Uint64(rq.Extras[8:16])
		event.Expiry = binary.BigEndian.Uint32(rq.Extras[16:20])

	} else if len(rq.Extras) >= uprDeletetionExtraLen &&
		event.Opcode == gomemcached.UPR_DELETION ||
		event.Opcode == gomemcached.UPR_EXPIRATION {

		event.Seqno = binary.BigEndian.Uint64(rq.Extras[:8])
		event.RevSeqno = binary.BigEndian.Uint64(rq.Extras[8:16])
		event.MetadataSize = binary.BigEndian.Uint16(rq.Extras[16:18])

	} else if len(rq.Extras) >= uprSnapshotExtraLen &&
		event.Opcode == gomemcached.UPR_SNAPSHOT {

		event.SnapstartSeq = binary.BigEndian.Uint64(rq.Extras[:8])
		event.SnapendSeq = binary.BigEndian.Uint64(rq.Extras[8:16])
		event.SnapshotType = binary.BigEndian.Uint32(rq.Extras[16:20])
	} else if event.IsSystemEvent() {
		event.PopulateEvent(rq.Extras)
	}

	return event
}

func (event *UprEvent) String() string {
	name := gomemcached.CommandNames[event.Opcode]
	if name == "" {
		name = fmt.Sprintf("#%d", event.Opcode)
	}
	return name
}

func (event *UprEvent) IsSnappyDataType() bool {
	return event.Opcode == gomemcached.UPR_MUTATION && (event.DataType&SnappyDataType > 0)
}

func (event *UprEvent) IsCollectionType() bool {
	return event.IsSystemEvent()
}

func (event *UprEvent) IsSystemEvent() bool {
	return event.Opcode == gomemcached.DCP_SYSTEM_EVENT
}

func (event *UprEvent) PopulateEvent(extras []byte) {
	if len(extras) < dcpSystemEventExtraLen {
		// Wrong length, don't parse
		return
	}
	event.Seqno = binary.BigEndian.Uint64(extras[:8])
	event.SystemEvent = SystemEventType(binary.BigEndian.Uint32(extras[8:12]))
	var versionTemp uint16 = binary.BigEndian.Uint16(extras[12:14])
	event.SysEventVersion = uint8(versionTemp >> 8)
}

func (event *UprEvent) GetName() (string, error) {
	switch event.SystemEvent {
	case CollectionCreate:
		fallthrough
	case ScopeCreate:
		return string(event.Key), nil
	default:
		logging.Errorf("Unable to handle unknown event")
		return "", InvalidOp
	}
}

func (event *UprEvent) GetManifestId() (uint64, error) {
	switch event.SystemEvent {
	case CollectionCreate:
		fallthrough
	case CollectionDrop:
		fallthrough
	case CollectionFlush:
		fallthrough
	case ScopeCreate:
		fallthrough
	case ScopeDrop:
		if event.SysEventVersion != 0 {
			return 0, fmt.Errorf("Invalid version for parsing createScope")
		}
		if event.ValueLen < 8 {
			return 0, fmt.Errorf("Value is too short")
		}
		return binary.BigEndian.Uint64(event.Value[0:8]), nil
	default:
		logging.Errorf("Unable to handle unknown event")
		return 0, InvalidOp
	}
}

func (event *UprEvent) GetId() (uint32, error) {
	switch event.SystemEvent {
	case CollectionCreate:
		fallthrough
	case CollectionDrop:
		fallthrough
	case CollectionFlush:
		fallthrough
	case ScopeCreate:
		fallthrough
	case ScopeDrop:
		if event.SysEventVersion != 0 {
			return 0, fmt.Errorf("Invalid version for parsing createScope")
		}
		if event.ValueLen < 12 {
			return 0, fmt.Errorf("Value is too short")
		}
		return binary.BigEndian.Uint32(event.Value[8:12]), nil
	default:
		logging.Errorf("Unable to handle unknown event")
		return 0, InvalidOp
	}
}
