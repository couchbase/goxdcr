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
	"encoding/json"
	"fmt"
	"testing"

	mc "github.com/couchbase/gomemcached"
	"github.com/stretchr/testify/assert"
)

func TestComposeExtrasForMutateWithMeta_NilRequest(t *testing.T) {
	fmt.Println("============== Test case start: TestComposeExtrasForMutateWithMeta_NilRequest =================")
	defer fmt.Println("============== Test case end: TestComposeExtrasForMutateWithMeta_NilRequest =================")

	assert := assert.New(t)

	// Test with nil WrappedMCRequest - should return unchanged position
	var wrappedReq *WrappedMCRequest = nil
	body := make([]byte, 1000)
	initialPos := 100
	cvCasPos := 42

	resultPos := wrappedReq.composeExtrasForMutateWithMeta(body, initialPos, cvCasPos)

	assert.Equal(initialPos, resultPos, "Position should remain unchanged for nil request")
}

func TestComposeExtrasForMutateWithMeta_NonMutateWithMetaOp(t *testing.T) {
	fmt.Println("============== Test case start: TestComposeExtrasForMutateWithMeta_NonMutateWithMetaOp =================")
	defer fmt.Println("============== Test case end: TestComposeExtrasForMutateWithMeta_NonMutateWithMetaOp =================")

	assert := assert.New(t)

	// Test with request that doesn't use MutateWithMeta
	wrappedReq := createTestWrappedMCRequest(mc.SET, false)
	body := make([]byte, 1000)
	initialPos := 100
	cvCasPos := 42

	resultPos := wrappedReq.composeExtrasForMutateWithMeta(body, initialPos, cvCasPos)

	assert.Equal(initialPos, resultPos, "Position should remain unchanged for non-MutateWithMeta operation")
}

func TestComposeExtrasForMutateWithMeta_SetCommand(t *testing.T) {
	fmt.Println("============== Test case start: TestComposeExtrasForMutateWithMeta_SetCommand =================")
	defer fmt.Println("============== Test case end: TestComposeExtrasForMutateWithMeta_SetCommand =================")

	assert := assert.New(t)

	testCases := []struct {
		name        string
		cvCasPos    int
		extrasLen   int
		expectedCas int
	}{
		{"PositiveNumber", 123, 28, 123},
		{"Zero", 0, 28, 0},
		{"LargeNumber", 999999, 28, 999999},
		{"SmallExtras", 42, 24, 42},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wrappedReq := createTestWrappedMCRequest(mc.SET, true)
			
			// Setup extras with proper format: <Flag:32, Exp:32, SeqNo:64, CAS:64, Options:32>
			extras := make([]byte, tc.extrasLen)
			
			// Flags (4 bytes) - 0xdeadbeef
			binary.BigEndian.PutUint32(extras[0:4], 0xdeadbeef)
			
			// Expiration (4 bytes) - 0x12345678
			binary.BigEndian.PutUint32(extras[4:8], 0x12345678)
			
			// Sequence number (8 bytes) - 0xabcdef1234567890
			binary.BigEndian.PutUint64(extras[8:16], 0xabcdef1234567890)
			
			// CAS (8 bytes) - 0x1122334455667788
			binary.BigEndian.PutUint64(extras[16:24], 0x1122334455667788)
			
			// Options (4 bytes) - only present if extrasLen == 28
			if tc.extrasLen == 28 {
				binary.BigEndian.PutUint32(extras[24:28], 0x01) // Some initial options
			}
			
			wrappedReq.Req.Extras = extras

			body := make([]byte, 2000)
			initialPos := 100

			resultPos := wrappedReq.composeExtrasForMutateWithMeta(body, initialPos, tc.cvCasPos)

			// Verify position has advanced
			assert.Greater(resultPos, initialPos, "Position should advance after writing JSON")

			// Extract the JSON from body
			jsonLen := binary.BigEndian.Uint32(wrappedReq.Req.Extras[0:4])
			jsonBytes := body[initialPos : initialPos+int(jsonLen)]

		// Debug: Print the actual JSON being generated
		fmt.Printf("Generated JSON: %s\n", string(jsonBytes))

		// Parse the JSON to verify structure
		var jsonObj map[string]interface{}
		err := json.Unmarshal(jsonBytes, &jsonObj)
		if err != nil {
			fmt.Printf("JSON Parse Error: %v\n", err)
			fmt.Printf("Raw bytes: %v\n", jsonBytes)
		}
			assert.Equal("0x12345678", jsonObj["expiration"], "Expiration should be correctly encoded")
			assert.Equal("0xabcdef1234567890", jsonObj["rev_seqno"], "Seqno should be correctly encoded")
			assert.Equal("set", jsonObj["command"], "Command should be 'set' for SET operations")
			
			// Verify options include required flags
			optionsStr := jsonObj["options"].(string)
			options := parseHexString(optionsStr)
			
			// Check for original options only if extras length includes options field (28 bytes)
			if tc.extrasLen == 28 {
				assert.Equal(uint32(1), options&uint32(1), "Should include original options if present")
			}
			assert.Equal(REGENERATE_CAS, options&REGENERATE_CAS, "Should include REGENERATE_CAS flag")
			assert.Equal(SKIP_CONFLICT_RESOLUTION_FLAG, options&SKIP_CONFLICT_RESOLUTION_FLAG, "Should include SKIP_CONFLICT_RESOLUTION_FLAG")
			
			// Verify cas_offsets - should be a JSON array with single integer
			casOffsetsArray, ok := jsonObj["cas_offsets"].([]interface{})
			assert.True(ok, "cas_offsets should be a JSON array")
			assert.Equal(1, len(casOffsetsArray), "cas_offsets array should have one element")
			casOffset, ok := casOffsetsArray[0].(float64) // JSON numbers are parsed as float64
			assert.True(ok, "cas_offsets[0] should be a number")
			assert.Equal(float64(tc.expectedCas), casOffset, "CAS offset should match expected value")

			// Verify extras section is properly set
			assert.Equal(4, len(wrappedReq.Req.Extras), "Extras should be resized to 4 bytes")
			assert.Equal(jsonLen, binary.BigEndian.Uint32(wrappedReq.Req.Extras), "Extras should contain JSON length")
		})
	}
}

func TestComposeExtrasForMutateWithMeta_DeleteCommand(t *testing.T) {
	fmt.Println("============== Test case start: TestComposeExtrasForMutateWithMeta_DeleteCommand =================")
	defer fmt.Println("============== Test case end: TestComposeExtrasForMutateWithMeta_DeleteCommand =================")

	assert := assert.New(t)

	wrappedReq := createTestWrappedMCRequestWithOpcode(DELETE_WITH_META, true)
	
	// Setup standard extras
	extras := make([]byte, 28)
	binary.BigEndian.PutUint32(extras[0:4], 0xabcdef12)   // flags
	binary.BigEndian.PutUint32(extras[4:8], 0x87654321)   // expiration
	binary.BigEndian.PutUint64(extras[8:16], 0x1111222233334444)  // seqno
	binary.BigEndian.PutUint64(extras[16:24], 0x5555666677778888) // cas
	binary.BigEndian.PutUint32(extras[24:28], 0x02)       // options
	wrappedReq.Req.Extras = extras

	body := make([]byte, 2000)
	initialPos := 50
	cvCasPos := 789

	resultPos := wrappedReq.composeExtrasForMutateWithMeta(body, initialPos, cvCasPos)

	// Verify position has advanced
	assert.Greater(resultPos, initialPos, "Position should advance after writing JSON")

	// Use resultPos to avoid unused variable error
	_ = resultPos

	// Extract and parse JSON
	jsonLen := binary.BigEndian.Uint32(wrappedReq.Req.Extras[0:4])
	jsonBytes := body[initialPos : initialPos+int(jsonLen)]

	var jsonObj map[string]interface{}
	err := json.Unmarshal(jsonBytes, &jsonObj)
	assert.NoError(err, "Should produce valid JSON")

	// Verify command is 'delete' for DELETE operations
	assert.Equal("delete", jsonObj["command"], "Command should be 'delete' for DELETE operations")
	
	// Verify cas_offsets - should be a JSON array with single integer
	casOffsetsArray, ok := jsonObj["cas_offsets"].([]interface{})
	assert.True(ok, "cas_offsets should be a JSON array")
	assert.Equal(1, len(casOffsetsArray), "cas_offsets array should have one element")
	casOffset, ok := casOffsetsArray[0].(float64) // JSON numbers are parsed as float64
	assert.True(ok, "cas_offsets[0] should be a number")
	assert.Equal(float64(789), casOffset, "CAS offset should match input value")
}

func TestComposeExtrasForMutateWithMeta_EdgeCases(t *testing.T) {
	fmt.Println("============== Test case start: TestComposeExtrasForMutateWithMeta_EdgeCases =================")
	defer fmt.Println("============== Test case end: TestComposeExtrasForMutateWithMeta_EdgeCases =================")

	assert := assert.New(t)

	testCases := []struct {
		name       string
		cvCasPos   int
		bodySize   int
		shouldPass bool
	}{
		{"NegativePosition", -1, 2000, true},
		{"VeryLargePosition", 2147483647, 2000, true}, // max int32
		{"ZeroPosition", 0, 2000, true},
		{"SmallBuffer", 42, 1000, true},
		{"LargeBuffer", 123, 10000, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wrappedReq := createTestWrappedMCRequest(mc.SET, true)
			
			// Setup basic extras
			extras := make([]byte, 28)
			binary.BigEndian.PutUint32(extras[0:4], 0x11111111)
			binary.BigEndian.PutUint32(extras[4:8], 0x22222222)
			binary.BigEndian.PutUint64(extras[8:16], 0x3333333344444444)
			binary.BigEndian.PutUint64(extras[16:24], 0x5555555566666666)
			binary.BigEndian.PutUint32(extras[24:28], 0x00)
			wrappedReq.Req.Extras = extras

			body := make([]byte, tc.bodySize)
			initialPos := 100

			if tc.shouldPass {
				resultPos := wrappedReq.composeExtrasForMutateWithMeta(body, initialPos, tc.cvCasPos)
				assert.Greater(resultPos, initialPos, "Should successfully compose JSON for %s", tc.name)
				
				// Verify JSON is valid
				jsonLen := binary.BigEndian.Uint32(wrappedReq.Req.Extras[0:4])
				jsonBytes := body[initialPos : initialPos+int(jsonLen)]
				
				var jsonObj map[string]interface{}
				err := json.Unmarshal(jsonBytes, &jsonObj)
				assert.NoError(err, "Should produce valid JSON for %s", tc.name)
				
				// Verify cas_offsets - should be a JSON array
				casOffsetsArray, ok := jsonObj["cas_offsets"].([]interface{})
				assert.True(ok, "cas_offsets should be a JSON array for %s", tc.name)
				assert.Equal(1, len(casOffsetsArray), "cas_offsets array should have one element for %s", tc.name)
				casOffset, ok := casOffsetsArray[0].(float64)
				assert.True(ok, "cas_offsets[0] should be a number for %s", tc.name)
				assert.Equal(float64(tc.cvCasPos), casOffset, "CAS offsets should be correctly formatted for %s", tc.name)
			}
		})
	}
}

func TestComposeExtrasForMutateWithMeta_JSONStructure(t *testing.T) {
	fmt.Println("============== Test case start: TestComposeExtrasForMutateWithMeta_JSONStructure =================")
	defer fmt.Println("============== Test case end: TestComposeExtrasForMutateWithMeta_JSONStructure =================")

	assert := assert.New(t)

	wrappedReq := createTestWrappedMCRequest(mc.SET, true)
	
	// Setup extras with known values for verification
	extras := make([]byte, 28)
	binary.BigEndian.PutUint32(extras[0:4], 0x12345678)   // flags
	binary.BigEndian.PutUint32(extras[4:8], 0x87654321)   // expiration
	binary.BigEndian.PutUint64(extras[8:16], 0xabcdefabcdefabcd)  // seqno
	binary.BigEndian.PutUint64(extras[16:24], 0x1234567890abcdef) // cas
	binary.BigEndian.PutUint32(extras[24:28], 0x01)       // options
	wrappedReq.Req.Extras = extras

	body := make([]byte, 2000)
	initialPos := 100
	cvCasPos := 42

	resultPos := wrappedReq.composeExtrasForMutateWithMeta(body, initialPos, cvCasPos)

	// Verify position has advanced
	assert.Greater(resultPos, initialPos, "Position should advance after writing JSON")

	// Extract JSON
	jsonLen := binary.BigEndian.Uint32(wrappedReq.Req.Extras[0:4])
	jsonBytes := body[initialPos : initialPos+int(jsonLen)]

	var jsonObj map[string]interface{}
	err := json.Unmarshal(jsonBytes, &jsonObj)
	assert.NoError(err, "Should produce valid JSON")

	// Verify all required keys are present
	requiredKeys := []string{"flags", "expiration", "rev_seqno", "options", "command", "cas_offsets"}
	for _, key := range requiredKeys {
		_, exists := jsonObj[key]
		assert.True(exists, "JSON should contain key: %s", key)
	}

	// Verify specific values
	assert.Equal("0x12345678", jsonObj["flags"])
	assert.Equal("0x87654321", jsonObj["expiration"])
	assert.Equal("0xabcdefabcdefabcd", jsonObj["rev_seqno"])
	assert.Equal("set", jsonObj["command"])
	
	// Verify cas_offsets - should be a JSON array
	casOffsetsArray, ok := jsonObj["cas_offsets"].([]interface{})
	assert.True(ok, "cas_offsets should be a JSON array")
	assert.Equal(1, len(casOffsetsArray), "cas_offsets array should have one element")
	casOffset, ok := casOffsetsArray[0].(float64)
	assert.True(ok, "cas_offsets[0] should be a number")
	assert.Equal(float64(42), casOffset, "CAS offset should be 42")
	
	// Verify options contain the expected flags
	optionsStr := jsonObj["options"].(string)
	options := parseHexString(optionsStr)
	expectedOptions := uint32(0x01) | REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG
	assert.Equal(expectedOptions, options)
}

func TestComposeExtrasForMutateWithMeta_BufferBoundaries(t *testing.T) {
	fmt.Println("============== Test case start: TestComposeExtrasForMutateWithMeta_BufferBoundaries =================")
	defer fmt.Println("============== Test case end: TestComposeExtrasForMutateWithMeta_BufferBoundaries =================")

	assert := assert.New(t)

	wrappedReq := createTestWrappedMCRequest(mc.SET, true)
	
	// Setup minimal extras
	extras := make([]byte, 24) // No options field
	binary.BigEndian.PutUint32(extras[0:4], 0xffffffff)
	binary.BigEndian.PutUint32(extras[4:8], 0xeeeeeeee)
	binary.BigEndian.PutUint64(extras[8:16], 0xddddddddcccccccc)
	binary.BigEndian.PutUint64(extras[16:24], 0xbbbbbbbbaaaaaaaa)
	wrappedReq.Req.Extras = extras

	// Use exact buffer size needed
	estimatedSize := MutateWithMetaExtrasMaxLen
	body := make([]byte, estimatedSize+200) // Add some buffer
	initialPos := 50
	cvCasPos := 999

	resultPos := wrappedReq.composeExtrasForMutateWithMeta(body, initialPos, cvCasPos)

	// Verify it completed successfully
	assert.Greater(resultPos, initialPos)
	assert.Less(resultPos-initialPos, estimatedSize, "Should not exceed maximum estimated size")

	// Verify JSON is valid
	jsonLen := binary.BigEndian.Uint32(wrappedReq.Req.Extras[0:4])
	jsonBytes := body[initialPos : initialPos+int(jsonLen)]
	
	var jsonObj map[string]interface{}
	err := json.Unmarshal(jsonBytes, &jsonObj)
	assert.NoError(err, "Should produce valid JSON")
	
	// For 24-byte extras, options should default to just the required flags
	optionsStr := jsonObj["options"].(string)
	options := parseHexString(optionsStr)
	expectedOptions := REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG
	assert.Equal(expectedOptions, options, "Should set default options when extras length < 28")
}

// Helper functions

func createTestWrappedMCRequest(opcode mc.CommandCode, isMutateWithMeta bool) *WrappedMCRequest {
	return createTestWrappedMCRequestWithOpcode(opcode, isMutateWithMeta)
}

func createTestWrappedMCRequestWithOpcode(opcode mc.CommandCode, isMutateWithMeta bool) *WrappedMCRequest {
	req := &mc.MCRequest{
		Opcode:  opcode,
		Key:     []byte("testkey"),
		Body:    []byte("testbody"),
		Extras:  make([]byte, 28),
	}

	wrapped := &WrappedMCRequest{
		Req:       req,
		UniqueKey: "testkey",
	}

	if isMutateWithMeta {
		// Set up ExCmdOptions to make IsMutateWithMetaOp() return true
		wrapped.ExCmdOptions = &ExCmdOptions{
			ExOp: MutateWithMetaSet, // Use correct constant for SET operation
		}
		
		// If the opcode is DELETE_WITH_META, use MutateWithMetaDel instead
		if opcode == DELETE_WITH_META {
			wrapped.ExCmdOptions.ExOp = MutateWithMetaDel
		}
	}

	return wrapped
}

func TestComposeExtrasForMutateWithMeta_UPR_EXPIRATION(t *testing.T) {
	fmt.Println("============== Test case start: TestComposeExtrasForMutateWithMeta_UPR_EXPIRATION =================")
	defer fmt.Println("============== Test case end: TestComposeExtrasForMutateWithMeta_UPR_EXPIRATION =================")

	assert := assert.New(t)

	testCases := []struct {
		name        string
		sourceCRMode string
		expectedOptions uint32
		expectedExtrasLen int
	}{
		{"RegularBucket", "seqno", REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG | IS_EXPIRATION, 28},
		{"LWWBucket", "lww", REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG | IS_EXPIRATION | FORCE_ACCEPT_WITH_META_OPS, 28},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// UPR_EXPIRATION always maps to DELETE_WITH_META and always uses 28-byte extras
			wrappedReq := createTestWrappedMCRequestWithOpcode(DELETE_WITH_META, true)
			
			// Setup 28-byte extras as UPR_EXPIRATION always requires this
			extras := make([]byte, 28)
			
			// Flags (4 bytes)
			binary.BigEndian.PutUint32(extras[0:4], 0x11111111)
			
			// Expiry time (4 bytes) - critical for expiration events
			binary.BigEndian.PutUint32(extras[4:8], 0x63f12a80) // Unix timestamp for expiry
			
			// Sequence number (8 bytes)
			binary.BigEndian.PutUint64(extras[8:16], 0x2222333344445555)
			
			// CAS (8 bytes)
			binary.BigEndian.PutUint64(extras[16:24], 0x6666777788889999)
			
			// Original options (4 bytes) - simulate router.go logic
			var originalOptions uint32 = IS_EXPIRATION
			if tc.sourceCRMode == "lww" {
				originalOptions |= FORCE_ACCEPT_WITH_META_OPS
			}
			binary.BigEndian.PutUint32(extras[24:28], originalOptions)
			
			wrappedReq.Req.Extras = extras

			body := make([]byte, 2000)
			initialPos := 100
			cvCasPos := 456

			resultPos := wrappedReq.composeExtrasForMutateWithMeta(body, initialPos, cvCasPos)

			// Verify position has advanced
			assert.Greater(resultPos, initialPos, "Position should advance after writing JSON")

			// Extract and parse JSON
			jsonLen := binary.BigEndian.Uint32(wrappedReq.Req.Extras[0:4])
			jsonBytes := body[initialPos : initialPos+int(jsonLen)]

			var jsonObj map[string]interface{}
			err := json.Unmarshal(jsonBytes, &jsonObj)
			assert.NoError(err, "Should produce valid JSON")

			// Verify expiration-specific fields
			assert.Equal("delete", jsonObj["command"], "UPR_EXPIRATION should map to delete command")
			assert.Equal("0x63f12a80", jsonObj["expiration"], "Expiry time should be correctly encoded")
			assert.Equal("0x11111111", jsonObj["flags"], "Flags should be correctly encoded")
			assert.Equal("0x2222333344445555", jsonObj["rev_seqno"], "Seqno should be correctly encoded")
			
			// Verify options include expiration and LWW-specific flags
			optionsStr := jsonObj["options"].(string)
			options := parseHexString(optionsStr)
			assert.Equal(tc.expectedOptions, options, "Should include all expected flags for %s", tc.name)
			
			// Verify cas_offsets
			casOffsetsArray, ok := jsonObj["cas_offsets"].([]interface{})
			assert.True(ok, "cas_offsets should be a JSON array")
			assert.Equal(1, len(casOffsetsArray), "cas_offsets array should have one element")
			casOffset, ok := casOffsetsArray[0].(float64)
			assert.True(ok, "cas_offsets[0] should be a number")
			assert.Equal(float64(456), casOffset, "CAS offset should match input value")
		})
	}
}

func TestComposeExtrasForMutateWithMeta_UPR_DELETION(t *testing.T) {
	fmt.Println("============== Test case start: TestComposeExtrasForMutateWithMeta_UPR_DELETION =================")
	defer fmt.Println("============== Test case end: TestComposeExtrasForMutateWithMeta_UPR_DELETION =================")

	assert := assert.New(t)

	testCases := []struct {
		name             string
		sourceCRMode     string
		extrasLen        int
		expectedOptions  uint32
		deleteTime       uint32
	}{
		{"RegularBucket_24ByteExtras", "seqno", 24, REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG, 0},
		{"LWWBucket_28ByteExtras", "lww", 28, REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG | FORCE_ACCEPT_WITH_META_OPS, 0x63f12b00},
		{"CustomBucket_28ByteExtras", "custom", 28, REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG | FORCE_ACCEPT_WITH_META_OPS, 0x63f12c00},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// UPR_DELETION maps to DELETE_WITH_META
			wrappedReq := createTestWrappedMCRequestWithOpcode(DELETE_WITH_META, true)
			
			// Setup extras based on source CR mode (LWW/Custom use 28 bytes, others use 24)
			extras := make([]byte, tc.extrasLen)
			
			// Flags (4 bytes)
			binary.BigEndian.PutUint32(extras[0:4], 0xaaaaaaaa)
			
			// Delete time in expiry field (4 bytes) - set for LWW/Custom buckets
			binary.BigEndian.PutUint32(extras[4:8], tc.deleteTime)
			
			// Sequence number (8 bytes)
			binary.BigEndian.PutUint64(extras[8:16], 0xbbbbccccddddeeee)
			
			// CAS (8 bytes)
			binary.BigEndian.PutUint64(extras[16:24], 0xffff000011112222)
			
			// Options (4 bytes) - only present for 28-byte extras
			if tc.extrasLen == 28 {
				var originalOptions uint32
				if tc.sourceCRMode == "lww" || tc.sourceCRMode == "custom" {
					originalOptions |= FORCE_ACCEPT_WITH_META_OPS
				}
				binary.BigEndian.PutUint32(extras[24:28], originalOptions)
			}
			
			wrappedReq.Req.Extras = extras

			body := make([]byte, 2000)
			initialPos := 100
			cvCasPos := 789

			resultPos := wrappedReq.composeExtrasForMutateWithMeta(body, initialPos, cvCasPos)

			// Verify position has advanced
			assert.Greater(resultPos, initialPos, "Position should advance after writing JSON")

			// Extract and parse JSON
			jsonLen := binary.BigEndian.Uint32(wrappedReq.Req.Extras[0:4])
			jsonBytes := body[initialPos : initialPos+int(jsonLen)]

			var jsonObj map[string]interface{}
			err := json.Unmarshal(jsonBytes, &jsonObj)
			assert.NoError(err, "Should produce valid JSON")

			// Verify deletion-specific fields
			assert.Equal("delete", jsonObj["command"], "UPR_DELETION should map to delete command")
			assert.Equal("0xaaaaaaaa", jsonObj["flags"], "Flags should be correctly encoded")
			assert.Equal("0xbbbbccccddddeeee", jsonObj["rev_seqno"], "Seqno should be correctly encoded")
			
			// Verify delete time encoding
			if tc.deleteTime > 0 {
				expectedDeleteTime := fmt.Sprintf("0x%08x", tc.deleteTime)
				assert.Equal(expectedDeleteTime, jsonObj["expiration"], "Delete time should be correctly encoded for %s", tc.name)
			} else {
				assert.Equal("0x00000000", jsonObj["expiration"], "Delete time should be zero for regular buckets")
			}
			
			// Verify options based on bucket type
			optionsStr := jsonObj["options"].(string)
			options := parseHexString(optionsStr)
			assert.Equal(tc.expectedOptions, options, "Should include correct flags for %s bucket type", tc.sourceCRMode)
			
			// Verify cas_offsets
			casOffsetsArray, ok := jsonObj["cas_offsets"].([]interface{})
			assert.True(ok, "cas_offsets should be a JSON array")
			assert.Equal(1, len(casOffsetsArray), "cas_offsets array should have one element")
			casOffset, ok := casOffsetsArray[0].(float64)
			assert.True(ok, "cas_offsets[0] should be a number")
			assert.Equal(float64(789), casOffset, "CAS offset should match input value")
		})
	}
}

func TestComposeExtrasForMutateWithMeta_LWWBucketScenarios(t *testing.T) {
	fmt.Println("============== Test case start: TestComposeExtrasForMutateWithMeta_LWWBucketScenarios =================")
	defer fmt.Println("============== Test case end: TestComposeExtrasForMutateWithMeta_LWWBucketScenarios =================")

	assert := assert.New(t)

	testCases := []struct {
		name            string
		operation       string
		command         mc.CommandCode
		expectedOptions uint32
		hasExpiry       bool
		expiryValue     uint32
	}{
		{"LWW_SET_Operation", "set", mc.SET, REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG | FORCE_ACCEPT_WITH_META_OPS, false, 0},
		{"LWW_DELETE_Operation", "delete", DELETE_WITH_META, REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG | FORCE_ACCEPT_WITH_META_OPS, true, 0x63f13000},
		{"LWW_EXPIRATION_Operation", "delete", DELETE_WITH_META, REGENERATE_CAS | SKIP_CONFLICT_RESOLUTION_FLAG | FORCE_ACCEPT_WITH_META_OPS | IS_EXPIRATION, true, 0x63f13100},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wrappedReq := createTestWrappedMCRequestWithOpcode(tc.command, true)
			
			// LWW buckets always use 28-byte extras
			extras := make([]byte, 28)
			
			// Flags (4 bytes) - LWW-specific flags
			binary.BigEndian.PutUint32(extras[0:4], 0x12345abc)
			
			// Expiry/delete time (4 bytes)
			binary.BigEndian.PutUint32(extras[4:8], tc.expiryValue)
			
			// Sequence number (8 bytes)
			binary.BigEndian.PutUint64(extras[8:16], 0x1111222233334444)
			
			// CAS (8 bytes) - LWW uses CAS for conflict resolution
			binary.BigEndian.PutUint64(extras[16:24], 0x5555666677778888)
			
			// Options (4 bytes) - simulate router.go LWW logic
			var originalOptions uint32 = FORCE_ACCEPT_WITH_META_OPS
			if tc.name == "LWW_EXPIRATION_Operation" {
				originalOptions |= IS_EXPIRATION
			}
			binary.BigEndian.PutUint32(extras[24:28], originalOptions)
			
			wrappedReq.Req.Extras = extras

			body := make([]byte, 2000)
			initialPos := 100
			cvCasPos := 321

			resultPos := wrappedReq.composeExtrasForMutateWithMeta(body, initialPos, cvCasPos)

			// Verify position has advanced
			assert.Greater(resultPos, initialPos, "Position should advance after writing JSON")

			// Extract and parse JSON
			jsonLen := binary.BigEndian.Uint32(wrappedReq.Req.Extras[0:4])
			jsonBytes := body[initialPos : initialPos+int(jsonLen)]

			var jsonObj map[string]interface{}
			err := json.Unmarshal(jsonBytes, &jsonObj)
			assert.NoError(err, "Should produce valid JSON for LWW bucket")

			// Verify command type
			assert.Equal(tc.operation, jsonObj["command"], "Command should be correct for %s", tc.name)
			
			// Verify LWW-specific flags and fields
			assert.Equal("0x12345abc", jsonObj["flags"], "Flags should be correctly encoded")
			assert.Equal("0x1111222233334444", jsonObj["rev_seqno"], "Seqno should be correctly encoded")
			
			// Verify expiry encoding for LWW scenarios
			if tc.hasExpiry && tc.expiryValue > 0 {
				expectedExpiry := fmt.Sprintf("0x%08x", tc.expiryValue)
				assert.Equal(expectedExpiry, jsonObj["expiration"], "Expiry should be correctly encoded for %s", tc.name)
			} else {
				assert.Equal("0x00000000", jsonObj["expiration"], "Expiry should be zero when not applicable")
			}
			
			// Verify LWW-specific options flags
			optionsStr := jsonObj["options"].(string)
			options := parseHexString(optionsStr)
			assert.Equal(tc.expectedOptions, options, "Should include all LWW-specific flags for %s", tc.name)
			
			// Verify FORCE_ACCEPT_WITH_META_OPS is always set for LWW
			assert.Equal(FORCE_ACCEPT_WITH_META_OPS, options&FORCE_ACCEPT_WITH_META_OPS, "FORCE_ACCEPT_WITH_META_OPS should be set for LWW buckets")
			
			// Verify IS_EXPIRATION flag for expiration operations
			if tc.name == "LWW_EXPIRATION_Operation" {
				assert.Equal(IS_EXPIRATION, options&IS_EXPIRATION, "IS_EXPIRATION should be set for expiration operations")
			}
			
			// Verify cas_offsets
			casOffsetsArray, ok := jsonObj["cas_offsets"].([]interface{})
			assert.True(ok, "cas_offsets should be a JSON array")
			assert.Equal(1, len(casOffsetsArray), "cas_offsets array should have one element")
			casOffset, ok := casOffsetsArray[0].(float64)
			assert.True(ok, "cas_offsets[0] should be a number")
			assert.Equal(float64(321), casOffset, "CAS offset should match input value")
		})
	}
}

func parseHexString(hexStr string) uint32 {
	// Remove "0x" prefix and parse as hex
	if len(hexStr) > 2 && hexStr[:2] == "0x" {
		hexStr = hexStr[2:]
	}
	
	var result uint32
	for _, c := range hexStr {
		result <<= 4
		if c >= '0' && c <= '9' {
			result += uint32(c - '0')
		} else if c >= 'a' && c <= 'f' {
			result += uint32(c - 'a' + 10)
		} else if c >= 'A' && c <= 'F' {
			result += uint32(c - 'A' + 10)
		}
	}
	return result
}