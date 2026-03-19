/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

/*
	Refer https://github.com/couchbase/kv_engine/blob/master/docs/mutate-with-meta.md for more information.
*/

const (
	// mutateWithMetaCasOffsetMaxLen represents the max length of value of cas_offsets list in the extras section
	// of a mutateWithMeta command.
	// 20 bytes can fit the base 10 representation of a number as high as uint64_max + 2 bytes for [ & ].
	mutateWithMetaCasOffsetMaxLen = 22

	// mutateWithMetaFlagsValLen denotes the size of flags value in extras of mutateWithMeta command.
	// Eg: 0xdeadbeef
	mutateWithMetaFlagsValLen = 10

	// mutateWithMetaOptionsValLen denotes the size of options of mutateWithMeta command.
	// Eg: 0x0000000e
	mutateWithMetaOptionsValLen = 10

	// mutateWithMetaExpirationValLen denotes the size of expiration value in of mutateWithMeta command.
	// Eg: 0x00000010
	mutateWithMetaExpirationValLen = 10

	// mutateWithMetaQuotesAndSepLen denotes number of bytes need for quotes and separators
	// per options key-val pair in mutateWithMeta command
	// They comprise of 2 pairs of quotes (i.e. 4 bytes), one colon (:) and one comma (,)
	mutateWithMetaQuotesAndSepLen = 6
)

var (
	// mutateWithMetaCommandKey denotes the command key in JSON extras of the mutateWithMeta command,
	// whose value can either be "add", "set" or "delete"
	mutateWithMetaCommandKey []byte = []byte("command")

	// mutateWithMetaSetValue is the value for extras.command, to ensure the command follows
	// SET semantics.
	mutateWithMetaSetValue []byte = []byte("set")

	// mutateWithMetaDelValue is the value for extras.command, to ensure the command follows
	// DELETE semantics.
	mutateWithMetaDelValue []byte = []byte("delete")

	// mutateWithMetaMaxCommandValLen represents the max value out of all possible extras.command values in
	// mutateWithMeta.
	// Currently, delete will need the maximum space.
	mutateWithMetaMaxCommandValLen = max(len(mutateWithMetaSetValue), len(mutateWithMetaDelValue))

	// mutateWithMetaCasOffsetsKey is the key in JSON list in the extras of mutateWithMeta command,
	// which contains the position or offset in the xattrs section, from which kv_engine will perform
	// CAS macros expansion. In goxdcr's case, CASMacroExpansionMarker will be macro expanded with the
	// new CAS.
	mutateWithMetaCasOffsetsKey []byte = []byte("cas_offsets")

	// mutateWithMetaExpirationKey is the key in JSON extras of mutateWithMeta command, whose value
	// will be the expiration value to be stored with the document.
	mutateWithMetaExpirationKey []byte = []byte("expiration")

	// mutateWithMetaOptionsKey is the key in JSON extras of mutateWithMeta command, whose value
	// will be the bitmask for different options (like REGENERATE_CAS, SKIP_CONFLICT_RESOLUTION_FLAG...)
	// to be used by the command.
	mutateWithMetaOptionsKey []byte = []byte("options")

	// mutateWithMetaFlagsKey is the key in JSON extras of mutateWithMeta command, whose value
	// will be the user set flags value to be stored with the document.
	mutateWithMetaFlagsKey []byte = []byte("flags")

	// mutateWithMetaRevSeqnoKey is the key in JSON extras of mutateWithMeta command, whose value
	// will represent the revSeqno value used for conflict resolution. Eventhough REGENERATE_CAS
	// is set in options, seqno will not be regenerated and the value passed as command.extras.seqno
	// will be used as revSeqno of the document to be stored on target.
	mutateWithMetaRevSeqnoKey []byte = []byte("rev_seqno")

	// DisableMutateWithMeta is a switch via internal settings to only use subdoc set/delete.
	DisableMutateWithMeta = false

	// MutateWithMetaExtrasMaxLen denotes the maximum length needed to encode mutateWithMeta
	// command extras.
	// Refer https://github.com/couchbase/kv_engine/blob/master/docs/mutate-with-meta.md for more information.
	MutateWithMetaExtrasMaxLen = 2 /* { and } */ +
		len(mutateWithMetaCommandKey) + mutateWithMetaMaxCommandValLen /* command value */ + mutateWithMetaQuotesAndSepLen /* punctuations */ +
		len(mutateWithMetaExpirationKey) + mutateWithMetaExpirationValLen /* expiration value */ + mutateWithMetaQuotesAndSepLen /* punctuations */ +
		len(mutateWithMetaFlagsKey) + mutateWithMetaFlagsValLen /* flags value */ + mutateWithMetaQuotesAndSepLen /* punctuations */ +
		len(mutateWithMetaRevSeqnoKey) + MaxHexDecodedLength /* seqno value */ + mutateWithMetaQuotesAndSepLen /* punctuations */ +
		len(mutateWithMetaOptionsKey) + mutateWithMetaOptionsValLen /* options bitmask */ + mutateWithMetaQuotesAndSepLen /* punctuations */ +
		len(mutateWithMetaCasOffsetsKey) + mutateWithMetaCasOffsetMaxLen + mutateWithMetaQuotesAndSepLen /* punctuations */
)

// composeExtrasForMutateWithMeta composes a JSON object for extras section of a mutateWithMeta command, while
// referencing the input wrappedReq, which should be a set/del/add_with_meta request.
// Extras for mutateWithMeta is encoded as a JSON object and is stored at the end of body section. The extras section
// will contain the size of the JSON object composed.
// The offset/position in the input body after extras section is composed is returned back to the caller.
// Refer https://github.com/couchbase/kv_engine/blob/master/docs/mutate-with-meta.md for more information.
func (wrappedReq *WrappedMCRequest) composeExtrasForMutateWithMeta(body []byte, pos int, cvCasPos int) int {
	if wrappedReq == nil || !wrappedReq.IsMutateWithMetaOp() {
		return pos
	}

	var (
		// req is a set/del/add_with_meta request. The extras section is binary encoded in
		// big endian format of sizes: <Flag:32, Exp:32, SeqNo:64, CAS:64, Options:32>
		req = wrappedReq.Req

		// buffers allocated on stack for temporary calculations.
		uint32BinBuf   [4]byte
		uint32HexBuf   [10]byte
		uint64BinBuf   [8]byte
		uint64HexBuf   [18]byte
		cvCasOffsetBuf [mutateWithMetaCasOffsetMaxLen]byte

		beginPos                     = pos
		mutateWithMetaCvCasOffsetVal = fmt.Appendf(cvCasOffsetBuf[:0], "[%d]", cvCasPos)
	)

	// 0x prefix is mandatory for all hex encodings.
	uint32HexBuf[0] = '0'
	uint32HexBuf[1] = 'x'
	uint64HexBuf[0] = '0'
	uint64HexBuf[1] = 'x'

	// 1. Store the command based on existing opcode. It's either set semantics or delete semantics given that target doc/tombstone
	// exists in our usecase of avoiding CAS rollback.
	command := mutateWithMetaSetValue
	if wrappedReq.GetMemcachedCommand() == DELETE_WITH_META {
		command = mutateWithMetaDelValue
	}
	body, pos = WriteJsonRawMsg(body, mutateWithMetaCommandKey, pos, WriteJsonKey, len(mutateWithMetaCommandKey), true /*firstKey*/)
	body, pos = WriteJsonRawMsg(body, command, pos, WriteJsonValue, len(command), false)

	// 2. cas will be skipped in options since REGENERATE_CAS (and SKIP_CONFLICT_RESOLUTION_FLAG) flags will be set in
	// options, cas can be skipped in options.

	// 3. expiration is stored in big endian binary format in req.extras[4:8] in req. This means
	// that we need to first convert it to big endian hex format, which will have a size
	// of 4*2 + 2 (for 0x prefix) = 10 bytes.
	body, pos = WriteJsonRawMsg(body, mutateWithMetaExpirationKey, pos, WriteJsonKey, len(mutateWithMetaExpirationKey), false)
	hex.Encode(uint32HexBuf[2:], req.Extras[4:8])
	body, pos = WriteJsonRawMsg(body, uint32HexBuf[:], pos, WriteJsonValue, 10, false)

	// 4. flags is stored in big endian binary format in req.extras[0:4] in req. This means
	// that we need to first convert it to big endian hex format, which will have a size
	// of 4*2 + 2 (for 0x prefix) = 10 bytes.
	body, pos = WriteJsonRawMsg(body, mutateWithMetaFlagsKey, pos, WriteJsonKey, len(mutateWithMetaFlagsKey), false)
	hex.Encode(uint32HexBuf[2:], req.Extras[0:4])
	body, pos = WriteJsonRawMsg(body, uint32HexBuf[:], pos, WriteJsonValue, 10, false)

	// 5. revSeqno is stored in big endian binary format in req.extras[8:16] in req. This means
	// that we need to first convert it to big endian hex format, which will have a size
	// of 8*2 + 2 (for 0x prefix) = 18 bytes.
	// Eventhough REGENERATE_CAS will be set in options, seqno will not be regenerated and the
	// value passed as command.extras.seqno will be used as revSeqno of the document to be stored
	// on target.
	body, pos = WriteJsonRawMsg(body, mutateWithMetaRevSeqnoKey, pos, WriteJsonKey, len(mutateWithMetaRevSeqnoKey), false)
	seqno := binary.BigEndian.Uint64(req.Extras[8:16])
	binary.BigEndian.PutUint64(uint64BinBuf[:], seqno)
	hex.Encode(uint64HexBuf[2:], uint64BinBuf[:])
	body, pos = WriteJsonRawMsg(body, uint64HexBuf[:], pos, WriteJsonValue, 18, false)

	// 6. options is stored in big endian binary format in req.extras[24:28] in req. This means
	// that we need to first convert it to big endian hex format, which will have a size
	// of 4*2 + 2 (for 0x prefix) = 10 bytes.
	body, pos = WriteJsonRawMsg(body, mutateWithMetaOptionsKey, pos, WriteJsonKey, len(mutateWithMetaOptionsKey), false)
	options := wrappedReq.setOptsForMutateWithMeta()
	binary.BigEndian.PutUint32(uint32BinBuf[:], options)
	hex.Encode(uint32HexBuf[2:], uint32BinBuf[:])
	body, pos = WriteJsonRawMsg(body, uint32HexBuf[:], pos, WriteJsonValue, 10, false)

	// 7. Finally we set the intent to perform CAS macro expansion. Send the offset of cvCas value in the XATTR
	// section. We don't need seqno_offsets for our usecase since HLV is based on CAS values.
	body, pos = WriteJsonRawMsg(body, mutateWithMetaCasOffsetsKey, pos, WriteJsonKey, len(mutateWithMetaCasOffsetsKey), false)
	copy(body[pos:], mutateWithMetaCvCasOffsetVal)
	pos += len(mutateWithMetaCvCasOffsetVal)

	body[pos] = '}'
	extrasLen := pos - beginPos + 1
	pos++

	// The extras section will contain the size of JSON object encoded above.
	// It is always 4 bytes.
	binary.BigEndian.PutUint32(wrappedReq.Req.Extras[0:4], uint32(extrasLen))
	wrappedReq.Req.Extras = wrappedReq.Req.Extras[:4]

	return pos
}

// setOptsForMutateWithMeta returns options for mutate_with_meta command.
func (req *WrappedMCRequest) setOptsForMutateWithMeta() uint32 {
	var options uint32
	if len(req.Req.Extras) >= 28 {
		// Extras length can be 24 or 28.
		// Extras section of size < 28 means that it's length is 24 and options = 0 until now.
		options = binary.BigEndian.Uint32(req.Req.Extras[24:28])
	}

	// Regeneration of CAS and macro-expansion of cvCAS to newly generated CAS is the
	// primary usecase of mutate_with_meta over set/add/del_with_meta.
	setRegenerateCASOpt(&options)

	if req.HLVModeOptions.NoTargetCR {
		// This path is always be hit in the current mobile mode usecase of mutate_with_meta
		// to avoid CAS rollback on target. Mobile mode only has source side CR.
		setSkipTargetCROpt(&options)
	}

	return options
}
