/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package filter

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	memcached "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
	"github.com/golang/snappy"
)

const InvalidJSONMsg = "seems to be an invalid JSON"

type FilterUtilsImpl struct {
}

func (f *FilterUtilsImpl) NewDataPool() base.DataPool {
	return base.NewDataPool()
}

func (f *FilterUtilsImpl) ProcessUprEventForFiltering(uprEvent *memcached.UprEvent, body []byte, endBodyPos int, dp base.DataPool, flags base.FilterFlagType, slicesToBeReleased *[][]byte) ([]byte, error, string, int64) {
	var err error
	var additionalErrDesc string
	var totalFailedCnt int64

	// Simplify things
	bodyContainsXattr := uprEvent.DataType&memcached.XattrDataType > 0
	dataTypeIsJson := uprEvent.DataType&memcached.JSONDataType > 0
	bodyIsCompressed := uprEvent.DataType&memcached.SnappyDataType > 0
	shouldSkipKey := flags&base.FilterFlagSkipKey > 0
	shouldSkipXattr := flags&base.FilterFlagSkipXattr > 0
	filterIsKeyOnly := flags&base.FilterFlagKeyOnly > 0
	filterIsXattrOnly := flags&base.FilterFlagXattrOnly > 0

	// Second level simplify logic for needToProcessBody
	filterReferencesBody := !filterIsKeyOnly
	filterReferencesXattr := !shouldSkipXattr

	needToProcessBody := (filterReferencesBody && dataTypeIsJson) ||
		(filterReferencesXattr && bodyContainsXattr)

	if needToProcessBody {
		if len(body) == 0 {
			// process/retrieve body only if it has not been passed in
			if bodyIsCompressed {
				body, err, additionalErrDesc, totalFailedCnt, endBodyPos = decompressSnappyBody(uprEvent.Value, uprEvent.Key, dp, slicesToBeReleased, true, dataTypeIsJson)
				if err != nil {
					return nil, err, additionalErrDesc, totalFailedCnt
				}
			} else {
				body, err, additionalErrDesc, totalFailedCnt, endBodyPos = getBodySlice(uprEvent.Value, uprEvent.Key, dp, slicesToBeReleased)
				if err != nil {
					return nil, err, additionalErrDesc, totalFailedCnt
				}
			}
		}

		if bodyContainsXattr {
			var failedCnt int64
			xattrMode := processSkipXattr
			if !shouldSkipXattr {
				xattrMode = processXattrAndBody
				if filterIsXattrOnly || !dataTypeIsJson {
					xattrMode = processXattrOnly
				}
			}
			body, err, failedCnt, endBodyPos = processXattribute(body, uprEvent.Key, xattrMode, dp, slicesToBeReleased, endBodyPos)
			if failedCnt > 0 {
				totalFailedCnt += failedCnt
			}
			if err != nil {
				additionalErrDesc = fmt.Sprintf("For document %v%v%v Unable to parse xattribute: %v", base.UdTagBegin, string(uprEvent.Key), base.UdTagEnd, err)
				return nil, base.ErrorFilterParsingError, additionalErrDesc, totalFailedCnt
			}
		}
	}

	if !shouldSkipKey {
		var failedCnt int64
		if !needToProcessBody {
			// Only thing passing to filter is the document key
			body, failedCnt, endBodyPos = processKeyOnlyForFiltering(uprEvent.Key, dp, slicesToBeReleased)
			if failedCnt > 0 {
				totalFailedCnt += failedCnt
			}
		} else {
			// Add Key to Body
			body, err, failedCnt, endBodyPos = f.AddKeyToBeFiltered(body, uprEvent.Key, dp.GetByteSlice, slicesToBeReleased, endBodyPos)
			if failedCnt > 0 {
				totalFailedCnt += failedCnt
			}
			if err != nil {
				additionalErrDesc = fmt.Sprintf("For document %v%v%v Unable to add key to body as the body may be malformed JSON", base.UdTagBegin, string(uprEvent.Key), base.UdTagEnd)
				return nil, base.ErrorFilterParsingError, additionalErrDesc, totalFailedCnt
			}
		}
	}

	if shouldSkipKey && shouldSkipXattr && !dataTypeIsJson {
		// This means that the UPR Event coming in is a DCP_MUTATION but is not a JSON document
		// In addition, user did not request filter on Xattribute, nor keys.
		// This is a special case and should be allowed to pass through
		return nil, base.FilterForcePassThrough, additionalErrDesc, totalFailedCnt
	}

	if endBodyPos > 0 {
		// Using datapool slices could potentially have garbage at the end from previously used slices
		// "Trim" this body slice to only contain the valid data
		body = body[0 : endBodyPos+1]
	}

	return body, nil, additionalErrDesc, totalFailedCnt
}

// check whether transaction xattrs exist in uprEvent
func (f *FilterUtilsImpl) CheckForTransactionXattrsInUprEvent(uprEvent *memcached.UprEvent, dp base.DataPool,
	slicesToBeReleased *[][]byte, needToFilterBody bool) (hasTxnXattrs bool, body []byte, endBodyPos int, err error,
	additionalErrDesc string, totalFailedCnt int64, uncompressedUprValue []byte) {
	// by default body is nil and endBodyPos is -1
	endBodyPos = -1

	uncompressedUprValue = uprEvent.Value
	if uprEvent.DataType&memcached.SnappyDataType > 0 {
		dataTypeIsJson := uprEvent.DataType&memcached.JSONDataType > 0
		body, err, additionalErrDesc, totalFailedCnt, endBodyPos = decompressSnappyBody(uprEvent.Value, uprEvent.Key, dp, slicesToBeReleased, needToFilterBody, dataTypeIsJson)
		if err != nil {
			return
		}
		uncompressedUprValue = body
	}

	hasTxnXattrs, err = f.hasTransactionXattrs(uncompressedUprValue)

	if body != nil && !needToFilterBody {
		// if needToFilterBody is false, body does not contain extra bytes for key and cannot be shared with advanced filtering
		// pass a nil body back to be absolutely sure that body won't somehow be used by advanced filtering
		body = nil
		endBodyPos = -1
	}
	return
}

// returns
// 1. whether body has transaction xattrs
// 2. error
func (f *FilterUtilsImpl) hasTransactionXattrs(body []byte) (bool, error) {
	iterator, err := base.NewXattrIterator(body)
	if err != nil {
		return false, err
	}

	for iterator.HasNext() {
		key, _, err := iterator.Next()
		if err != nil {
			return false, err
		}
		if base.Equals(key, base.TransactionXattrKey) {
			// found transaction xattrs.
			return true, nil
		}
	}

	// if we get here, there are no transaction xattrs
	return false, nil
}

// For advanced filtering, need to populate key into the actual data to be filtered
// If we can try not to move data and just use datapool and append to the end, it may be still faster
// to have gojsonsm step through the JSON than to do memory move
func (f *FilterUtilsImpl) AddKeyToBeFiltered(currentValue, key []byte, dpGetter base.DpGetterFunc, toBeReleased *[][]byte, currentValueEndBody int) ([]byte, error, int64, int) {
	if dpGetter != nil && currentValueEndBody > 0 {
		// { "bodyKey":bodyValue...
		// 	,"KeyKey":"<Key>" 	<- sizeToGet
		// }
		sizeToGet := base.CachedInternalKeyKeyByteSize + len(key) + 6 + len(currentValue)
		dpSlice, err, pos := base.AppendSingleKVToAllocatedBody(currentValue, base.CachedInternalKeyKeyByteSlice,
			key, dpGetter, toBeReleased, uint64(sizeToGet), base.CachedInternalKeyKeyByteSize, len(key), true, currentValueEndBody)
		if err != nil {
			retBytes, err, pos := addKeyToBeFilteredWithoutDP(currentValue, key)
			return retBytes, err, int64(len(retBytes)), pos
		}
		return dpSlice, err, 0, pos
	} else {
		retBytes, err, pos := addKeyToBeFilteredWithoutDP(currentValue, key)
		return retBytes, err, int64(len(retBytes)), pos
	}
}

func addKeyToBeFilteredWithoutDP(currentValue, key []byte) ([]byte, error, int) {
	if currentValue[0] != '{' {
		return currentValue, base.ErrorInvalidInput, -1
	}
	lastBracketPos := len(currentValue) - 1
	if currentValue[lastBracketPos] != '}' {
		lastBracketPos = base.GetLastBracketPos(currentValue, lastBracketPos+1)
	}
	keyBytesToBeInserted := json.RawMessage(fmt.Sprintf("\"%v\":\"%v\",", base.ReservedWordsMap[base.ExternalKeyKey], string(key)))
	lastBracketPos += len(keyBytesToBeInserted)
	dataSlice, err := base.CleanInsert(currentValue, keyBytesToBeInserted, 1)
	return dataSlice, err, lastBracketPos
}

func processKeyOnlyForFiltering(key []byte, dp base.DataPool, slicesToBeReleased *[][]byte) ([]byte, int64, int) {
	var body []byte
	var err error
	keyLen := len(key)
	bodySize := +uint64(keyLen + 2 /*quote surrounding key*/ + 5 /*cruft around the special key*/ + len(base.ReservedWordsMap[base.ExternalKeyKey]))
	body, err = dp.GetByteSlice(bodySize)
	if err != nil {
		// If there is any problem using datapool, just use json.RawMessage directly to allocate new byte slice
		body = json.RawMessage(fmt.Sprintf("{\"%v\":\"%v\"}", base.ReservedWordsMap[base.ExternalKeyKey], string(key)))
		return body, int64(len(body)), len(body) - 1
	} else {
		*slicesToBeReleased = append(*slicesToBeReleased, body)
	}
	var bodyPos int
	body, bodyPos = base.WriteJsonRawMsg(body, base.CachedInternalKeyKeyByteSlice, bodyPos, base.WriteJsonKey, base.CachedInternalKeyKeyByteSize, bodyPos == 0)
	body, bodyPos = base.WriteJsonRawMsg(body, key, bodyPos, base.WriteJsonValue /*uprEvent key as value*/, keyLen, false /*firstKey*/)
	return body, 0, bodyPos
}

// Decompresses the input snappy-compressed document body into a buffer slice obtained from the passed data pool.
// Optionally allows adding extra bytes at the end of the buffer slice for appending the document key (as required when filtering on key), to avoid reallocation later on.
func decompressSnappyBody(incomingBody, key []byte, dp base.DataPool, slicesToBeReleased *[][]byte, needExtraBytesInBody, isJson bool) ([]byte, error, string, int64, int) {
	var dpFailedCnt int64
	lenOfDecodedData, err := snappy.DecodedLen(incomingBody)
	lastBodyPos := lenOfDecodedData - 1
	if err != nil {
		return nil, base.ErrorCompressionUnableToInflate, fmt.Sprintf("XDCR for key %v%v%v is unable to decode snappy uncompressed size: %v", base.UdTagBegin, string(key), base.UdTagEnd, err), dpFailedCnt, lastBodyPos
	}

	uncompressedBodySize := uint64(lenOfDecodedData)
	if needExtraBytesInBody {
		uncompressedBodySize += uint64(len(key) + base.AddFilterKeyExtraBytes)
	}
	body, err := dp.GetByteSlice(uncompressedBodySize)
	if err != nil {
		body = make([]byte, 0, uncompressedBodySize)
		dpFailedCnt = int64(uncompressedBodySize)
	} else {
		*slicesToBeReleased = append(*slicesToBeReleased, body)
	}

	body, err = snappy.Decode(body, incomingBody)
	if err != nil {
		return nil, base.ErrorCompressionUnableToInflate, fmt.Sprintf("XDCR for key %v%v%v is unable to snappy decompress body value: %v", base.UdTagBegin, string(key), base.UdTagEnd, err), dpFailedCnt, lastBodyPos
	}

	// Check to make sure the last bracket position is correct
	if isJson && !base.IsJsonEndValid(body, lastBodyPos) {
		return nil, base.ErrorInvalidInput, fmt.Sprintf("XDCR for key %v%v%v after decompression %s", base.UdTagBegin, string(key), base.UdTagEnd, InvalidJSONMsg), dpFailedCnt, lastBodyPos
	}

	return body, nil, "", dpFailedCnt, lastBodyPos
}

// Decompresses the input snappy-compressed document body into a buffer slice obtained from the passed data pool.
// Also allocates extra bytes at the end of the buffer slice for appending the document key (as required when filtering on key), to avoid reallocation later on.
func getBodySlice(incomingBody, key []byte, dp base.DataPool, slicesToBeReleased *[][]byte) ([]byte, error, string, int64, int) {
	var dpFailedCnt int64
	var incomingBodyLen int = len(incomingBody)
	lastBodyPos := incomingBodyLen - 1
	bodySize := uint64(incomingBodyLen + len(key) + base.AddFilterKeyExtraBytes)

	if !base.IsJsonEndValid(incomingBody, lastBodyPos) {
		return nil, base.ErrorInvalidInput, fmt.Sprintf("Document %v%v%v body is not a valid JSON", base.UdTagBegin, string(key), base.UdTagEnd), dpFailedCnt, lastBodyPos
	}

	body, err := dp.GetByteSlice(bodySize)
	if err != nil {
		body = make([]byte, 0, bodySize)
		dpFailedCnt = int64(bodySize)
	} else {
		*slicesToBeReleased = append(*slicesToBeReleased, body)
	}
	copy(body, incomingBody)
	return body, nil, "", dpFailedCnt, lastBodyPos
}

type processXattributeType int

const (
	// Should Process the body but not xattribute
	processSkipXattr processXattributeType = iota
	// Should Process both body and xattribute
	processXattrAndBody processXattributeType = iota
	// Should Process just the xattribute, no body
	processXattrOnly processXattributeType = iota
)

func processXattribute(body, key []byte, processType processXattributeType, dp base.DataPool, slicesToBeReleased *[][]byte, endBodyPos int) ([]byte, error, int64, int) {
	var pos uint32
	//	var separator uint32
	var dpFailedCnt int64
	var err error

	//	first uint32 in the body contains the size of the entire XATTR section
	totalXattrSize := binary.BigEndian.Uint32(body[pos : pos+4])
	// Couchbase doc size is max of 20MB. Xattribute count against this limit.
	// So if total xattr size is greater than this limit, then something is wrong
	if totalXattrSize > base.MaxDocSizeByte {
		return nil, fmt.Errorf("For document %v%v%v, unable to correctly parse xattribute from DCP packet. Xattr size determined to be %v bytes, which is invalid", base.UdTagBegin, string(key), base.UdTagEnd, totalXattrSize), dpFailedCnt, endBodyPos
	}
	// Add 4 bytes here to skip the uint32 that was just parsed
	totalXattrSize += 4

	switch processType {
	case processSkipXattr:
		newBody := body[totalXattrSize:]
		body = newBody
		endBodyPos = endBodyPos - int(totalXattrSize)
	case processXattrAndBody:
		body, err, dpFailedCnt, endBodyPos = stripAndPrependXattribute(body, totalXattrSize, dp, slicesToBeReleased, endBodyPos, false /*xattrOnly*/)
	case processXattrOnly:
		body, err, dpFailedCnt, endBodyPos = stripAndPrependXattribute(body, totalXattrSize, dp, slicesToBeReleased, endBodyPos, true /*xattrOnly*/)
	default:
		return nil, base.ErrorInvalidInput, dpFailedCnt, -1
	}
	return body, err, dpFailedCnt, endBodyPos
}

func stripAndPrependXattribute(body []byte, xattrSize uint32, dp base.DataPool, slicesToBeReleased *[][]byte, endBodyPos int, xattrOnly bool) ([]byte, error, int64, int) {
	var dpFailedCnt int64
	var actualBodySize int
	actualBody := body[xattrSize:]
	endBodyPos = endBodyPos - int(xattrSize)
	if !xattrOnly {
		actualBodySize = len(actualBody)
		// Prereq check
		if actualBody[0] != '{' {
			return nil, base.ErrorInvalidInput, dpFailedCnt, endBodyPos
		}
	}

	// xattrSize is the size of the xAttribute section
	// The xattribute section is consisted of uint32 + key + NUL + value + NUL (repeat)
	// Functions calling this is converting DCP xattribute pairs encoding to the following:
	// {					 <- could be absorbed
	// " key " : value ,
	// " key2 " : value2 }
	// Original DCP stream has 6 extra bytes per KV pair
	// Converted has 4 extra bytes per KV pair, so using xattrSize is sufficient

	// Get a size of body size + xattr value size + internal Xattr KEY and JSON symbol sizes
	bodySize := uint64(int(xattrSize) + actualBodySize + base.AddFilterXattrExtraBytes)
	combinedBody, err := dp.GetByteSlice(bodySize)
	if err != nil {
		combinedBody = make([]byte, 0, bodySize)
		dpFailedCnt += int64(bodySize)
	} else {
		*slicesToBeReleased = append(*slicesToBeReleased, combinedBody)
	}

	// Non-xattrOnly case:
	// ------------------
	// Current body looks like (spaces added for readability):
	// { key : val }
	// Want to insert xattr at the beginning so "combinedBody" looks like:
	// { "XdcrInternalXattrKey" : { xattrKey : xattrVal } , key : val }

	// xattrOnly case: (essentially just returning)
	// --------------------------------------------
	// { "XDCRInternalXattrKey" : <ConvertedXattrSection> }

	// { XdcrInternalXattrKey :
	combinedBodyPos := 0
	combinedBody, combinedBodyPos = base.WriteJsonRawMsg(combinedBody, base.CachedInternalKeyXattrByteSlice, combinedBodyPos, base.WriteJsonKey, base.CachedInternalKeyXattrByteSize, combinedBodyPos == 0 /*firstKey*/)

	// { XdcrInternalXattrKey : { xattrKey : xattrVal }
	// Followed by a uint32, then  -> key -> NUL -> value -> NUL (repeat)
	xattrIter, err := base.NewXattrIterator(body)
	if err != nil {
		return nil, base.ErrorInvalidInput, dpFailedCnt, endBodyPos
	}
	firstKey := true
	for xattrIter.HasNext() == true {
		key, value, err := xattrIter.Next()
		if err != nil {
			return nil, err, dpFailedCnt, endBodyPos
		}
		combinedBody, combinedBodyPos = base.WriteJsonRawMsg(combinedBody, key, combinedBodyPos, base.WriteJsonKey, len(key), firstKey)
		combinedBody, combinedBodyPos = base.WriteJsonRawMsg(combinedBody, value, combinedBodyPos, base.WriteJsonValueNoQuotes, len(value), false)
		if firstKey {
			firstKey = false
		}
	}

	// Currently:                                     v - combinedBodyPos
	// { XdcrInternalXattrKey : { xattrKey : xattrVal }

	if xattrOnly {
		// Targeted:                                       v - combinedBodyPos
		// { XdcrInternalXattrKey : { xattrKey : xattrVal }}
		combinedBodyPos++
		combinedBody[combinedBodyPos] = '}'

		endBodyPos = combinedBodyPos
	} else {
		// Targeted:                                        v - combinedBodyPos
		// { XdcrInternalXattrKey : { xattrKey : xattrVal },
		combinedBodyPos++
		combinedBody[combinedBodyPos] = ','
		// endBodyPos is added instead of combinedBodyPos+1 is because below we are copying actualBody[1:] to skip the first {
		endBodyPos += combinedBodyPos
		combinedBodyPos++

		// ActualBody:
		// { key : val }
		// targeted combinedBody:
		// { XdcrInternalXattrKey : { xattrKey : xattrVal }, key : val }
		copy(combinedBody[combinedBodyPos:], actualBody[1:])
	}

	return combinedBody, nil, dpFailedCnt, endBodyPos
}
