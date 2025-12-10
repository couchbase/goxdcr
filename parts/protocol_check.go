package parts

import (
	"encoding/binary"
	"fmt"

	mc "github.com/couchbase/gomemcached"
)

func parseMCBodyLen(buf []byte) (bodylen int, err error) {
	// Note: only a subset of memcached protocol used by XDCR is checked
	// Options like framing extras, flex magic are out of scope
	switch buf[0] {
	case mc.RES_MAGIC:
		fallthrough
	case mc.REQ_MAGIC:
		bodylen = int(binary.BigEndian.Uint32(buf[8:]))
	default:
		err = fmt.Errorf("bad magic: 0x%02x", buf[0])
	}

	return
}

// protocolCheck checks if the given byte slice conforms to the Memcached protocol
// The buffer can be a concatenation of multiple Memcached packets.
// The scan stops at the first error encountered and its agnostic to the document content.
// For each packet:
//  1. Read the header
//  2. Parse the header to get the body length
//  3. Move the offset by header length + body length
//
// If the final offset does not match the buffer length, it indicates truncated bytes.
//
// Time complexity:
//
//	O(n) where n is the number of packets in the buffer not the size of the buffer
//
// Why this specific check and not check the content?
//
//	There have been multiple cases where the packet received by KV was malformed
//	The nature of corruption indicated that somehow the data got truncated in transit
//	As of now, it is not clear whether it is a nerwork issue or a bug in XDCR
//	So this check (if it passes) at least ensures that the packets are well formed
//	and any corruption is likely in transit or at the receiver side.
func (xmem *XmemNozzle) protocolCheck(buf []byte) (err error) {
	if !xmem.devReplOpts.EnableXmemProtocolCheck {
		return
	}

	off := 0
	for off < len(buf) {
		if len(buf)-off < mc.HDR_LEN {
			return fmt.Errorf("less bytes for header, off=%d, buflen=%d", off, len(buf))
		}
		header := buf[off : off+mc.HDR_LEN]
		bodylen, err := parseMCBodyLen(header)
		if err != nil {
			return fmt.Errorf("mc packet failed at off=%d, err=%w", off, err)
		}
		off += mc.HDR_LEN + bodylen
	}

	if off != len(buf) {
		return fmt.Errorf("truncated bytes off=%d, buflen=%d", off, len(buf))
	}

	return
}
