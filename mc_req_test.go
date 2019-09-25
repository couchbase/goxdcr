package gomemcached

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"io/ioutil"
	"reflect"
	"testing"
)

func TestEncodingRequest(t *testing.T) {
	req := MCRequest{
		Opcode:  SET,
		Cas:     938424885,
		Opaque:  7242,
		VBucket: 824,
		Key:     []byte("somekey"),
		Body:    []byte("somevalue"),
	}
	req.Keylen = len(req.Key)

	got := req.Bytes()

	expected := []byte{
		REQ_MAGIC, byte(SET),
		0x0, 0x7, // length of key
		0x0,       // extra length
		0x0,       // reserved
		0x3, 0x38, // vbucket
		0x0, 0x0, 0x0, 0x10, // Length of value
		0x0, 0x0, 0x1c, 0x4a, // opaque
		0x0, 0x0, 0x0, 0x0, 0x37, 0xef, 0x3a, 0x35, // CAS
		's', 'o', 'm', 'e', 'k', 'e', 'y',
		's', 'o', 'm', 'e', 'v', 'a', 'l', 'u', 'e'}

	if len(got) != req.Size() {
		t.Fatalf("Expected %v bytes, got %v", got,
			len(got))
	}

	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("Expected:\n%#v\n  -- got -- \n%#v",
			expected, got)
	}

	exp := `{MCRequest opcode=SET, bodylen=9, key='somekey'}`
	if req.String() != exp {
		t.Errorf("Expected string=%q, got %q", exp, req.String())
	}
}

func TestEncodingRequestWithExtras(t *testing.T) {
	req := MCRequest{
		Opcode:  SET,
		Cas:     938424885,
		Opaque:  7242,
		VBucket: 824,
		Extras:  []byte{1, 2, 3, 4},
		Key:     []byte("somekey"),
		Body:    []byte("somevalue"),
	}
	req.Keylen = len(req.Key)

	buf := &bytes.Buffer{}
	req.Transmit(buf)
	got := buf.Bytes()

	expected := []byte{
		REQ_MAGIC, byte(SET),
		0x0, 0x7, // length of key
		0x4,       // extra length
		0x0,       // reserved
		0x3, 0x38, // vbucket
		0x0, 0x0, 0x0, 0x14, // Length of remainder
		0x0, 0x0, 0x1c, 0x4a, // opaque
		0x0, 0x0, 0x0, 0x0, 0x37, 0xef, 0x3a, 0x35, // CAS
		1, 2, 3, 4, // extras
		's', 'o', 'm', 'e', 'k', 'e', 'y',
		's', 'o', 'm', 'e', 'v', 'a', 'l', 'u', 'e'}

	if len(got) != req.Size() {
		t.Fatalf("Expected %v bytes, got %v", got,
			len(got))
	}

	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("Expected:\n%#v\n  -- got -- \n%#v",
			expected, got)
	}
}

func TestEncodingRequestWithLargeBody(t *testing.T) {
	req := MCRequest{
		Opcode:  SET,
		Cas:     938424885,
		Opaque:  7242,
		VBucket: 824,
		Extras:  []byte{1, 2, 3, 4},
		Key:     []byte("somekey"),
		Body:    make([]byte, 256),
	}
	req.Keylen = len(req.Key)

	buf := &bytes.Buffer{}
	req.Transmit(buf)
	got := buf.Bytes()

	expected := append([]byte{
		REQ_MAGIC, byte(SET),
		0x0, 0x7, // length of key
		0x4,       // extra length
		0x0,       // reserved
		0x3, 0x38, // vbucket
		0x0, 0x0, 0x1, 0xb, // Length of remainder
		0x0, 0x0, 0x1c, 0x4a, // opaque
		0x0, 0x0, 0x0, 0x0, 0x37, 0xef, 0x3a, 0x35, // CAS
		1, 2, 3, 4, // extras
		's', 'o', 'm', 'e', 'k', 'e', 'y',
	}, make([]byte, 256)...)

	if len(got) != req.Size() {
		t.Fatalf("Expected %v bytes, got %v", got,
			len(got))
	}

	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("Expected:\n%#v\n  -- got -- \n%#v",
			expected, got)
	}
}

func BenchmarkEncodingRequest(b *testing.B) {
	req := MCRequest{
		Opcode:  SET,
		Cas:     938424885,
		Opaque:  7242,
		VBucket: 824,
		Key:     []byte("somekey"),
		Body:    []byte("somevalue"),
	}
	req.Keylen = len(req.Key)

	b.SetBytes(int64(req.Size()))

	for i := 0; i < b.N; i++ {
		req.Bytes()
	}
}

func BenchmarkEncodingRequest0CAS(b *testing.B) {
	req := MCRequest{
		Opcode:  SET,
		Cas:     0,
		Opaque:  7242,
		VBucket: 824,
		Key:     []byte("somekey"),
		Body:    []byte("somevalue"),
	}
	req.Keylen = len(req.Key)

	b.SetBytes(int64(req.Size()))

	for i := 0; i < b.N; i++ {
		req.Bytes()
	}
}

func BenchmarkEncodingRequest1Extra(b *testing.B) {
	req := MCRequest{
		Opcode:  SET,
		Cas:     0,
		Opaque:  7242,
		VBucket: 824,
		Extras:  []byte{1},
		Key:     []byte("somekey"),
		Body:    []byte("somevalue"),
	}
	req.Keylen = len(req.Key)

	b.SetBytes(int64(req.Size()))

	for i := 0; i < b.N; i++ {
		req.Bytes()
	}
}

func TestRequestTransmit(t *testing.T) {
	res := MCRequest{Key: []byte("thekey")}
	res.Keylen = len(res.Key)
	_, err := res.Transmit(ioutil.Discard)
	if err != nil {
		t.Errorf("Error sending small request: %v", err)
	}

	res.Body = make([]byte, 256)
	_, err = res.Transmit(ioutil.Discard)
	if err != nil {
		t.Errorf("Error sending large request thing: %v", err)
	}

}

func TestReceiveRequest(t *testing.T) {
	key := []byte("somekey")
	req := MCRequest{
		Opcode:  SET,
		Cas:     0,
		Opaque:  7242,
		VBucket: 824,
		Extras:  []byte{1},
		Key:     key,
		Body:    []byte("somevalue"),
		ExtMeta: []byte{},
		Keylen:  len(key),
	}

	data := req.Bytes()

	req2 := MCRequest{}
	n, err := req2.Receive(bytes.NewReader(data), nil)
	if err != nil {
		t.Fatalf("Error receiving: %v", err)
	}
	if len(data) != n {
		t.Errorf("Expected to read %v bytes, read %v", len(data), n)
	}

	if !reflect.DeepEqual(req, req2) {
		t.Fatalf("Expected %#v == %#v", req, req2)
	}
}

func TestReceiveRequestNoContent(t *testing.T) {
	req := MCRequest{
		Opcode:  SET,
		Cas:     0,
		Opaque:  7242,
		VBucket: 824,
		Extras:  []byte{},
		Key:     []byte{},
		Body:    []byte{},
		ExtMeta: []byte{},
	}

	data := req.Bytes()

	req2 := MCRequest{
		Extras:  []byte{},
		Key:     []byte{},
		Body:    []byte{},
		ExtMeta: []byte{},
	}
	n, err := req2.Receive(bytes.NewReader(data), nil)
	if err != nil {
		t.Fatalf("Error receiving: %v", err)
	}
	if len(data) != n {
		t.Errorf("Expected to read %v bytes, read %v", len(data), n)
	}

	if fmt.Sprintf("%#v", req) != fmt.Sprintf("%#v", req2) {
		t.Fatalf("Expected %#v == %#v", req, req2)
	}
}

func TestReceiveRequestShortHdr(t *testing.T) {
	req := MCRequest{}
	n, err := req.Receive(bytes.NewReader([]byte{1, 2, 3}), nil)
	if err == nil {
		t.Errorf("Expected error, got %#v", req)
	}
	if n != 3 {
		t.Errorf("Expected to have read 3 bytes, read %v", n)
	}
}

func TestReceiveRequestShortBody(t *testing.T) {
	req := MCRequest{
		Opcode:  SET,
		Cas:     0,
		Opaque:  7242,
		VBucket: 824,
		Extras:  []byte{1},
		Key:     []byte("somekey"),
		Body:    []byte("somevalue"),
	}
	req.Keylen = len(req.Key)

	data := req.Bytes()

	req2 := MCRequest{}
	n, err := req2.Receive(bytes.NewReader(data[:len(data)-3]), nil)
	if err == nil {
		t.Errorf("Expected error %v, got %#v", err, req2)
	}
	if n != len(data)-3 {
		t.Errorf("Expected to have read %v bytes, read %v", len(data)-3, n)
	}
}

func TestReceiveRequestBadMagic(t *testing.T) {
	req := MCRequest{
		Opcode:  SET,
		Cas:     0,
		Opaque:  7242,
		VBucket: 824,
		Extras:  []byte{1},
		Key:     []byte("somekey"),
		Body:    []byte("somevalue"),
	}
	req.Keylen = len(req.Key)

	data := req.Bytes()
	data[0] = 0x83

	req2 := MCRequest{}
	_, err := req2.Receive(bytes.NewReader(data), nil)
	if err == nil {
		t.Fatalf("Expected error, got %#v", req2)
	}
}

func TestReceiveRequestLongBody(t *testing.T) {
	req := MCRequest{
		Opcode:  SET,
		Cas:     0,
		Opaque:  7242,
		VBucket: 824,
		Extras:  []byte{1},
		Key:     []byte("somekey"),
		Body:    make([]byte, MaxBodyLen+5),
	}
	req.Keylen = len(req.Key)

	data := req.Bytes()

	req2 := MCRequest{}
	_, err := req2.Receive(bytes.NewReader(data), nil)
	if err == nil {
		t.Fatalf("Expected error, got %#v", req2)
	}
}

func BenchmarkReceiveRequest(b *testing.B) {
	req := MCRequest{
		Opcode:  SET,
		Cas:     0,
		Opaque:  7242,
		VBucket: 824,
		Extras:  []byte{1},
		Key:     []byte("somekey"),
		Body:    []byte("somevalue"),
	}
	req.Keylen = len(req.Key)

	data := req.Bytes()
	data[0] = REQ_MAGIC
	rdr := bytes.NewReader(data)

	b.SetBytes(int64(len(data)))

	b.ResetTimer()
	buf := make([]byte, HDR_LEN)
	for i := 0; i < b.N; i++ {
		req2 := MCRequest{}
		rdr.Seek(0, 0)
		_, err := req2.Receive(rdr, buf)
		if err != nil {
			b.Fatalf("Error receiving: %v", err)
		}
	}
}

func BenchmarkReceiveRequestNoBuf(b *testing.B) {
	req := MCRequest{
		Opcode:  SET,
		Cas:     0,
		Opaque:  7242,
		VBucket: 824,
		Extras:  []byte{1},
		Key:     []byte("somekey"),
		Body:    []byte("somevalue"),
	}
	req.Keylen = len(req.Key)

	data := req.Bytes()
	data[0] = REQ_MAGIC
	rdr := bytes.NewReader(data)

	b.SetBytes(int64(len(data)))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req2 := MCRequest{}
		rdr.Seek(0, 0)
		_, err := req2.Receive(rdr, nil)
		if err != nil {
			b.Fatalf("Error receiving: %v", err)
		}
	}
}

func TestReceivingTapRequest(t *testing.T) {
	content := []byte{
		REQ_MAGIC, byte(TAP_MUTATION),
		0x0, 0x7, // length of key
		0x2,       // extra length
		0x0,       // reserved
		0x3, 0x38, // vbucket
		0x0, 0x0, 0x0, 0x16, // Length of value
		0x0, 0x0, 0x1c, 0x4a, // opaque
		0x0, 0x0, 0x0, 0x0, 0x37, 0xef, 0x3a, 0x35, // CAS
		0, 4, // extra (describes length of engine specific
		1, 2, 3, 4, // engine specific junk
		's', 'o', 'm', 'e', 'k', 'e', 'y',
		's', 'o', 'm', 'e', 'v', 'a', 'l', 'u', 'e'}

	req := MCRequest{}
	n, err := req.Receive(bytes.NewReader(content), nil)
	if err != nil {
		t.Errorf("Failed to parse response.")
	}
	if n != len(content) {
		t.Errorf("Expected to read %v bytes, read %v", len(content), n)
	}

	exp := `{MCRequest opcode=TAP_MUTATION, bodylen=9, key='somekey'}`
	if req.String() != exp {
		t.Errorf("Expected string=%q, got %q", exp, req.String())
	}
}

func TestReceivingUPRNoop(t *testing.T) {
	content := []byte{
		REQ_MAGIC, byte(UPR_NOOP),
		0x0, 0x0, // length of ley
		0x0,      // extra length
		0x0,      // reserved
		0x0, 0x0, // vbucket
		0x0, 0x0, 0x0, 0x0, // length of value
		0x0, 0x0, 0x1c, 0x4a, // opaque
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // CAS
	}

	req := MCRequest{}

	conn := bytes.NewReader(content)
	n, err := req.Receive(conn, nil)
	if err != nil {
		t.Errorf("Failed to parse response, err: %v", err)
	}

	if n != len(content) {
		t.Errorf("Expected to read %v bytes, read %v", len(content), n)
	}

	exp := `{MCRequest opcode=UPR_NOOP, bodylen=0, key=''}`
	if req.String() != exp {
		t.Errorf("Expected string=%q, got %q", exp, req.String())
	}

	n, err = req.Receive(conn, nil)
	if err != io.EOF {
		t.Errorf("Expected EOF!")
	}
}

func TestStreamIDSnapshotReceive(t *testing.T) {
	assert := assert.New(t)
	nonRegularMutationHeader, err := ioutil.ReadFile("./unitTestData/flexSnapOpHdr.bin")
	if err != nil {
		panic(fmt.Sprintf("%v", err))
	}

	req := &MCRequest{}
	elen, totalbodyLen, framingElen := req.receiveFlexibleFramingHeader(nonRegularMutationHeader)

	assert.Equal(0, req.Keylen)
	assert.Equal(UPR_SNAPSHOT, req.Opcode)
	assert.Equal(3, framingElen)
	assert.Equal(20, elen)
	assert.Equal(925, int(req.VBucket))
	assert.Equal(23, totalbodyLen)
}

func TestStreamIDSnapshotFull(t *testing.T) {
	assert := assert.New(t)
	ssHeader, err := ioutil.ReadFile("./unitTestData/header.0x56.bin")
	assert.Nil(err)
	assert.NotNil(ssHeader)
	assert.Equal(24, len(ssHeader))

	req := &MCRequest{}
	elen, totalBodyLen, framingElen := req.receiveFlexibleFramingHeader(ssHeader)
	assert.Equal(0, req.Keylen)
	assert.Equal(UPR_SNAPSHOT, req.Opcode)
	assert.Equal(3, framingElen)
	assert.Equal(20, elen)
	assert.Equal(23, totalBodyLen)

	ssBody, err := ioutil.ReadFile("./unitTestData/body.0x56.bin")
	assert.Nil(err)
	assert.NotNil(ssBody)
	assert.Equal(totalBodyLen, len(ssBody))
	err = req.populateFlexBodyInternal(ssBody, totalBodyLen, elen, framingElen)
	assert.Nil(err)
	assert.Equal(1, len(req.FramingExtras))
	streamId, err := req.FramingExtras[0].GetStreamId()
	assert.Nil(err)
	assert.Equal(uint16(1), streamId)

	truncatedHeader := req.HeaderBytes()[0:24]
	assert.Equal(ssHeader, truncatedHeader)

	checkSlice := ssHeader
	checkSlice = append(checkSlice, ssBody...)
	assert.Equal(checkSlice, req.Bytes())
}

func TestStreamIDMutation(t *testing.T) {
	assert := assert.New(t)
	ssHeader, err := ioutil.ReadFile("./unitTestData/header.0x57.bin")
	assert.Nil(err)
	assert.NotNil(ssHeader)
	assert.Equal(24, len(ssHeader))

	req := &MCRequest{}
	elen, totalBodyLen, framingElen := req.receiveFlexibleFramingHeader(ssHeader)
	assert.Equal(UPR_MUTATION, req.Opcode)
	assert.Equal(3, framingElen)
	assert.Equal(31, elen)
	assert.Equal(148, totalBodyLen)

	mutBody, err := ioutil.ReadFile("./unitTestData/body.0x57.bin")
	assert.Nil(err)
	assert.NotNil(mutBody)
	assert.Equal(totalBodyLen, len(mutBody))
	err = req.populateFlexBodyInternal(mutBody, totalBodyLen, elen, framingElen)
	assert.Nil(err)
	assert.Equal(1, len(req.FramingExtras))
	streamId, err := req.FramingExtras[0].GetStreamId()
	assert.Nil(err)
	assert.Equal(uint16(1), streamId)

	truncatedHeader := req.HeaderBytes()[0:24]
	assert.Equal(ssHeader, truncatedHeader)

	checkSlice := ssHeader
	checkSlice = append(checkSlice, mutBody...)
	assert.Equal(checkSlice, req.Bytes())
}

func TestSystemEventMutation(t *testing.T) {
	assert := assert.New(t)
	sysHeader, err := ioutil.ReadFile("./unitTestData/header.0x5f.bin")
	assert.Nil(err)
	assert.NotNil(sysHeader)
	assert.Equal(24, len(sysHeader))

	req := &MCRequest{}
	elen, totalBodyLen, framingElen := req.receiveFlexibleFramingHeader(sysHeader)
	assert.Equal(DCP_SYSTEM_EVENT, req.Opcode)
	assert.Equal(3, framingElen)

	sysBody, err := ioutil.ReadFile("./unitTestData/body.0x5f.bin")
	assert.Nil(err)
	assert.NotNil(sysBody)
	assert.Equal(totalBodyLen, len(sysBody))
	err = req.populateFlexBodyInternal(sysBody, totalBodyLen, elen, framingElen)
	assert.Nil(err)
	assert.Equal(1, len(req.FramingExtras))
	streamId, err := req.FramingExtras[0].GetStreamId()
	assert.Nil(err)
	assert.Equal(uint16(1), streamId)

	checkSlice := sysHeader
	checkSlice = append(checkSlice, sysBody...)
	assert.Equal(checkSlice, req.Bytes())
}

func TestSliceShifter(t *testing.T) {
	assert := assert.New(t)
	testSlice := []byte("abc")
	assert.Equal("61", fmt.Sprintf("%x", testSlice[0]))
	assert.Equal("62", fmt.Sprintf("%x", testSlice[1]))
	assert.Equal("63", fmt.Sprintf("%x", testSlice[2]))
	assert.Equal(3, len(testSlice))
	shiftSlice := ShiftByteSliceLeft4Bits(testSlice)
	assert.Equal("16", fmt.Sprintf("%x", shiftSlice[0]))
	assert.Equal("26", fmt.Sprintf("%x", shiftSlice[1]))
	assert.Equal("30", fmt.Sprintf("%x", shiftSlice[2]))
	assert.Equal(3, len(shiftSlice))
	reverseShiftSlice := ShiftByteSliceRight4Bits(shiftSlice)
	assert.Equal("1", fmt.Sprintf("%x", reverseShiftSlice[0]))
	assert.Equal("62", fmt.Sprintf("%x", reverseShiftSlice[1]))
	assert.Equal("63", fmt.Sprintf("%x", reverseShiftSlice[2]))
	assert.Equal(3, len(reverseShiftSlice))

	testSlice = []byte("abc")
	shiftSlice2 := ShiftByteSliceRight4Bits(testSlice)
	assert.Equal("6", fmt.Sprintf("%x", shiftSlice2[0]))
	assert.Equal("16", fmt.Sprintf("%x", shiftSlice2[1]))
	assert.Equal("26", fmt.Sprintf("%x", shiftSlice2[2]))
	assert.Equal("30", fmt.Sprintf("%x", shiftSlice2[3]))

	mergedSlice := Merge2HalfByteSlices(shiftSlice, shiftSlice2)
	assert.Equal("16", fmt.Sprintf("%x", mergedSlice[0]))
	assert.Equal("26", fmt.Sprintf("%x", mergedSlice[1]))
	assert.Equal("36", fmt.Sprintf("%x", mergedSlice[2]))
	assert.Equal("16", fmt.Sprintf("%x", mergedSlice[3]))
	assert.Equal("26", fmt.Sprintf("%x", mergedSlice[4]))
	assert.Equal("30", fmt.Sprintf("%x", mergedSlice[5]))
	assert.Equal(6, len(mergedSlice))

	mergedSlice = Merge2HalfByteSlices(shiftSlice, reverseShiftSlice)
	assert.Equal("16", fmt.Sprintf("%x", mergedSlice[0]))
	assert.Equal("26", fmt.Sprintf("%x", mergedSlice[1]))
	assert.Equal("31", fmt.Sprintf("%x", mergedSlice[2]))
	assert.Equal("62", fmt.Sprintf("%x", mergedSlice[3]))
	assert.Equal("63", fmt.Sprintf("%x", mergedSlice[4]))
	assert.Equal(5, len(mergedSlice))

	shiftSlice = Merge2HalfByteSlices(shiftSlice, reverseShiftSlice)
	assert.Equal("16", fmt.Sprintf("%x", shiftSlice[0]))
	assert.Equal("26", fmt.Sprintf("%x", shiftSlice[1]))
	assert.Equal("31", fmt.Sprintf("%x", shiftSlice[2]))
	assert.Equal("62", fmt.Sprintf("%x", shiftSlice[3]))
	assert.Equal("63", fmt.Sprintf("%x", shiftSlice[4]))
	assert.Equal(5, len(shiftSlice))

}
