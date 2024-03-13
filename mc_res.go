package gomemcached

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

// MCResponse is memcached response
type MCResponse struct {
	// The command opcode of the command that sent the request
	Opcode CommandCode
	// The status of the response
	Status Status
	// The opaque sent in the request
	Opaque uint32
	// The CAS identifier (if applicable)
	Cas uint64
	// Extras, key, and body for this response
	Extras, FlexibleExtras, Key, Body []byte
	// If true, this represents a fatal condition and we should hang up
	Fatal bool
	// Datatype identifier
	DataType uint8

	recycleFunc func()
}

// A debugging string representation of this response
func (res MCResponse) String() string {
	return fmt.Sprintf("{MCResponse status=%v keylen=%d, extralen=%d, bodylen=%d, flexible=%v}",
		res.Status, len(res.Key), len(res.Extras), len(res.Body), res.FlexibleExtras)
}

// Response as an error.
func (res *MCResponse) Error() string {
	return fmt.Sprintf("MCResponse status=%v, opcode=%v, opaque=%v, msg: %s",
		res.Status, res.Opcode, res.Opaque, string(res.Body))
}

func errStatus(e error) Status {
	status := UNKNOWN_STATUS
	if res, ok := e.(*MCResponse); ok {
		status = res.Status
	}
	return status
}

// IsNotFound is true if this error represents a "not found" response.
func IsNotFound(e error) bool {
	return errStatus(e) == KEY_ENOENT
}

// IsFatal is false if this error isn't believed to be fatal to a connection.
func IsFatal(e error) bool {
	if e == nil {
		return false
	}
	_, ok := isFatal[errStatus(e)]
	if ok {
		return true
	}
	return false
}

func IsTenantLimit(e error) bool {
	s := errStatus(e)
	return s >= RATE_LIMITED_NETWORK_INGRESS && s <= BUCKET_SIZE_LIMIT_EXCEEDED
}

// Size is number of bytes this response consumes on the wire.
func (res *MCResponse) Size() int {
	return HDR_LEN + len(res.Extras) + len(res.Key) + len(res.Body)
}

func (res *MCResponse) fillHeaderBytes(data []byte) int {
	pos := 0
	data[pos] = RES_MAGIC
	pos++
	data[pos] = byte(res.Opcode)
	pos++
	binary.BigEndian.PutUint16(data[pos:pos+2],
		uint16(len(res.Key)))
	pos += 2

	// 4
	data[pos] = byte(len(res.Extras))
	pos++
	// Data type
	if res.DataType != 0 {
		data[pos] = byte(res.DataType)
	} else {
		data[pos] = 0
	}
	pos++
	binary.BigEndian.PutUint16(data[pos:pos+2], uint16(res.Status))
	pos += 2

	// 8
	binary.BigEndian.PutUint32(data[pos:pos+4],
		uint32(len(res.Body)+len(res.Key)+len(res.Extras)))
	pos += 4

	// 12
	binary.BigEndian.PutUint32(data[pos:pos+4], res.Opaque)
	pos += 4

	// 16
	binary.BigEndian.PutUint64(data[pos:pos+8], res.Cas)
	pos += 8

	if len(res.Extras) > 0 {
		copy(data[pos:pos+len(res.Extras)], res.Extras)
		pos += len(res.Extras)
	}

	if len(res.Key) > 0 {
		copy(data[pos:pos+len(res.Key)], res.Key)
		pos += len(res.Key)
	}

	return pos
}

// HeaderBytes will get just the header bytes for this response.
func (res *MCResponse) HeaderBytes() []byte {
	data := make([]byte, HDR_LEN+len(res.Extras)+len(res.Key))

	res.fillHeaderBytes(data)

	return data
}

// Bytes will return the actual bytes transmitted for this response.
func (res *MCResponse) Bytes() []byte {
	data := make([]byte, res.Size())

	pos := res.fillHeaderBytes(data)

	copy(data[pos:pos+len(res.Body)], res.Body)

	return data
}

// Transmit will send this response message across a writer.
func (res *MCResponse) Transmit(w io.Writer) (n int, err error) {
	if len(res.Body) < 128 {
		n, err = w.Write(res.Bytes())
	} else {
		n, err = w.Write(res.HeaderBytes())
		if err == nil {
			m := 0
			m, err = w.Write(res.Body)
			m += n
		}
	}
	return
}

// Receive will fill this MCResponse with the data from this reader.
func (res *MCResponse) Receive(r io.Reader, hdrBytes []byte) (n int, err error) {
	return res.ReceiveWithBuf(r, hdrBytes, nil)
}

func (res *MCResponse) ReceiveWithBuf(r io.Reader, hdrbytes, buf []byte) (n int, err error) {
	return res.receiveInternal(r, hdrbytes, buf, nil, nil)
}

func (res *MCResponse) ReceiveWithDatapool(r io.Reader, hdrbytes []byte, getter func(uint64) ([]byte, error), done func([]byte)) (n int, err error) {
	return res.receiveInternal(r, hdrbytes, nil, getter, done)
}

// receiveInternal takes an optional pre-allocated []byte buf which
// will be used if its capacity is large enough, otherwise a new
// []byte slice is allocated
// If a getter is provided, it'll attempt to use it before allocating
func (res *MCResponse) receiveInternal(r io.Reader, hdrBytes, buf []byte, getter func(uint64) ([]byte, error), done func([]byte)) (n int, err error) {
	if len(hdrBytes) < HDR_LEN {
		hdrBytes = []byte{
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0}
	}
	n, err = io.ReadFull(r, hdrBytes)
	if err != nil {
		return n, err
	}

	var klen, flen int
	switch hdrBytes[0] {
	case RES_MAGIC:
		fallthrough
	case REQ_MAGIC:
		klen = int(binary.BigEndian.Uint16(hdrBytes[2:4]))
	case FLEX_RES_MAGIC:
		flen = int(hdrBytes[2])
		klen = int(hdrBytes[3])
	default:
		return n, fmt.Errorf("bad magic: 0x%02x", hdrBytes[0])
	}
	elen := int(hdrBytes[4])

	res.Opcode = CommandCode(hdrBytes[1])
	res.DataType = uint8(hdrBytes[5])
	res.Status = Status(binary.BigEndian.Uint16(hdrBytes[6:8]))
	res.Opaque = binary.BigEndian.Uint32(hdrBytes[12:16])
	res.Cas = binary.BigEndian.Uint64(hdrBytes[16:24])

	bodyLen := int(binary.BigEndian.Uint32(hdrBytes[8:12])) - (klen + elen + flen)

	//defer function to debug the panic seen with MB-15557
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf(`Panic in Receive. Response %v \n
                        key len %v extra len %v bodylen %v`, res, klen, elen, bodyLen)
		}
	}()

	bufNeed := klen + elen + flen + bodyLen
	if buf != nil && cap(buf) >= bufNeed {
		buf = buf[0:bufNeed]
	} else {
		if getter != nil {
			buf, err = getter(uint64(bufNeed))
			if err != nil {
				buf = make([]byte, bufNeed)
			} else {
				buf = buf[0:bufNeed]
				res.recycleFunc = func() {
					done(buf)
				}
			}
		} else {
			buf = make([]byte, bufNeed)
		}
	}

	m, err := io.ReadFull(r, buf)
	if err == nil {
		if flen > 0 {
			res.FlexibleExtras = buf[0:flen]
			buf = buf[flen:]
		}
		res.Extras = buf[0:elen]
		res.Key = buf[elen : klen+elen]
		res.Body = buf[klen+elen:]
	}

	return n + m, err
}

func (res *MCResponse) ComputeUnits() (ru uint64, wu uint64) {
	if res.FlexibleExtras == nil || len(res.FlexibleExtras) == 0 {
		return
	}
	for i := 0; i < len(res.FlexibleExtras); {
		// TODO check: this seems to be the opposite of the documentation?
		l := res.FlexibleExtras[i] & 0x0f
		switch res.FlexibleExtras[i] >> 4 {
		case ComputeUnitsRead:
			ru = uint64(binary.BigEndian.Uint16(res.FlexibleExtras[i+1 : i+3]))
		case ComputeUnitsWrite:
			wu = uint64(binary.BigEndian.Uint16(res.FlexibleExtras[i+1 : i+3]))

		// ID escape: we need to skip the next byte, and ignore
		case 15:
			i++
		}

		// data len is either 1..14, or 15 + next byte
		switch l {
		case 0:
			panic(fmt.Sprintf("Invalid Flexible Extras length received! %v", l))
		case 15:

			// rest of length in next byte, which needs to be skipped too
			i++
			i += int(l + 1 + res.FlexibleExtras[i])
		default:
			i += int(l + 1)
		}
	}
	return
}

func (res *MCResponse) Recycle() {
	if res != nil && res.recycleFunc != nil {
		res.recycleFunc()
	}
}

type MCResponsePool struct {
	pool *sync.Pool
}

func NewMCResponsePool() *MCResponsePool {
	rv := &MCResponsePool{
		pool: &sync.Pool{
			New: func() interface{} {
				return &MCResponse{}
			},
		},
	}

	return rv
}

func (this *MCResponsePool) Get() *MCResponse {
	return this.pool.Get().(*MCResponse)
}

func (this *MCResponsePool) Put(r *MCResponse) {
	if r == nil {
		return
	}

	r.Extras = nil
	r.Key = nil
	r.Body = nil
	r.Fatal = false

	this.pool.Put(r)
}

type StringMCResponsePool struct {
	pool *sync.Pool
	size int
}

func NewStringMCResponsePool(size int) *StringMCResponsePool {
	rv := &StringMCResponsePool{
		pool: &sync.Pool{
			New: func() interface{} {
				return make(map[string]*MCResponse, size)
			},
		},
		size: size,
	}

	return rv
}

func (this *StringMCResponsePool) Get() map[string]*MCResponse {
	return this.pool.Get().(map[string]*MCResponse)
}

func (this *StringMCResponsePool) Put(m map[string]*MCResponse) {
	if m == nil || len(m) > 2*this.size {
		return
	}

	for k := range m {
		m[k] = nil
		delete(m, k)
	}

	this.pool.Put(m)
}
