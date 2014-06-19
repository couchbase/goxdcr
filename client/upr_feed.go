package memcached

import (
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/couchbase/gomemcached"
	"github.com/couchbaselabs/retriever/logger"
	"github.com/couchbaselabs/retriever/stats"
)

// go implementation of upr client.
// See https://github.com/couchbaselabs/cbupr/blob/master/transport-spec.md

// TODO
// 1. Optimize bufferAck messages
// 2. Use a pool allocator to avoid garbage

// UprOpcode is the upr operation type (found in UprEvent)
type UprOpcode uint8

// Upr opcode values.
const (
	UprOpen = UprOpcode(iota)
	UprAddStream
	UprCloseStream
	UprFailoverLog
	UprStreamRequest
	UprSnapshot
	UprMutation
	UprDeletion
	UprExpiration
	UprFlush
	UprControl
	UprBufferAck
	UprNoop
)

const uprMutationExtraLen = 16

var uprOpcodeNames map[UprOpcode]string

//logging and stats
var ul *logger.LogWriter
var us *stats.StatsCollector

func init() {
	uprOpcodeNames = map[UprOpcode]string{
		UprOpen:          "UprOpen",
		UprAddStream:     "AddStream",
		UprCloseStream:   "CloseStream",
		UprFailoverLog:   "FailoverLog",
		UprStreamRequest: "StreamRequest",
		UprSnapshot:      "SnapshotMarker",
		UprMutation:      "Mutation",
		UprDeletion:      "Deletion",
		UprExpiration:    "Expiration",
		UprFlush:         "Flush",
		UprControl:       "Flow Control",
		UprBufferAck:     "Buffer Acknowledgement",
		UprNoop:          "Noop",
	}
	ul, _ = logger.NewLogger("upr_client", logger.LevelInfo)
}

func (event UprEvent) String() string {
	name := uprOpcodeNames[event.Opcode]
	if name == "" {
		name = fmt.Sprintf("#%d", event.Opcode)
	}
	return name
}

type FailoverLog [][2]uint64 // Failover log containing vvuid and sequnce number

type UprEvent struct {
	Opcode       UprOpcode          // Type of event
	Status       gomemcached.Status // Response status
	VBucket      uint16             // VBucket this event applies to
	Flags        uint32             // Item flags
	Expiry       uint32             // Item expiration time
	Key, Value   []byte             // Item key/value
	OldValue     []byte             // TODO: TBD: old document value
	Cas          uint64             // CAS value of the item
	Seqno        uint64             // sequence number of the mutation
	SnapstartSeq uint64             // start sequence number of this snapshot
	SnapendSeq   uint64             // End sequence number of the snapshot
	SnapshotType uint32             // 0: disk 1: memory
	FailoverLog  *FailoverLog       // Failover log containing vvuid and sequnce number
	Error        error              // Error value in case of a failure
}

func makeUprEvent(rq gomemcached.MCRequest) *UprEvent {

	event := &UprEvent{
		VBucket: uint16(rq.VBucket),
		Key:     rq.Key,
		Value:   rq.Body,
		Cas:     rq.Cas,
	}

	if len(rq.Extras) >= tapMutationExtraLen {
		event.Seqno = binary.BigEndian.Uint64(rq.Extras[:8])
	}

	switch rq.Opcode {
	case gomemcached.UPR_MUTATION:
		event.Opcode = UprMutation
	case gomemcached.UPR_DELETION:
		event.Opcode = UprDeletion
	case gomemcached.UPR_EXPIRATION:
		event.Opcode = UprExpiration
	case gomemcached.UPR_CLOSESTREAM:
		event.Opcode = UprCloseStream
	case gomemcached.UPR_SNAPSHOT:
		event.Opcode = UprSnapshot
	case gomemcached.UPR_FLUSH:
		event.Opcode = UprFlush
	}

	if len(rq.Extras) >= tapMutationExtraLen &&
		event.Opcode == UprMutation || event.Opcode == UprDeletion ||
		event.Opcode == UprExpiration {
		event.Flags = binary.BigEndian.Uint32(rq.Extras[8:])
		event.Expiry = binary.BigEndian.Uint32(rq.Extras[12:])

	} else if len(rq.Extras) >= tapMutationExtraLen && event.Opcode == UprSnapshot {
		event.SnapstartSeq = binary.BigEndian.Uint64(rq.Extras[:8])
		event.SnapendSeq = binary.BigEndian.Uint64(rq.Extras[8:16])
		event.SnapshotType = binary.BigEndian.Uint32(rq.Extras[16:20])
	}

	return event
}

type UprStream struct {
	Vbucket   uint16 // Vbucket id
	VbUuid    uint64 // vbucket uuid
	StartSeq  uint64 // start sequence number
	EndSeq    uint64 // end sequence number
	connected bool
}

type FailoverCallback func(uint16, FailoverLog, error)

// Represents a UPR feed. A feed contains a connection to a single host
// and multiple vBuckets
type UprFeed struct {
	C         <-chan *UprEvent      // Exported channel for receiving UPR events
	vbstreams map[uint16]*UprStream // vb->stream mapping
	closer    chan bool             // closer
	conn      *Client               // connection to UPR producer
	Error     error                 // error
	bytesRead uint64                // total bytes read on this connection

	transmitCh chan *gomemcached.MCRequest // transmit command channel
	transmitCl chan bool                   //  closer channel for transmit go-routine
}

const opaqueOpen = 0xBEAF0001
const opaqueFailover = 0xDEADBEEF

func sendCommands(mc *Client, ch chan *gomemcached.MCRequest, closer chan bool) {

loop:
	for {
		select {
		case command := <-ch:
			if err := mc.Transmit(command); err != nil {
				ul.LogError("Failed to transmit command %s. Error %s", command.Opcode.String(), err.Error())
				break loop
			}

		case <-closer:
			ul.LogInfo("", "", "Exiting send command go routine ...")
			break loop
		}

	}
}

// Create a new UPR Feed
func (mc *Client) NewUprFeed() (*UprFeed, error) {

	ul.LogDebug("", "", "New UPR Feed")
	feed := &UprFeed{
		conn:       mc,
		closer:     make(chan bool),
		vbstreams:  make(map[uint16]*UprStream),
		transmitCh: make(chan *gomemcached.MCRequest),
		transmitCl: make(chan bool),
	}

	go sendCommands(mc, feed.transmitCh, feed.transmitCl)
	return feed, nil
}

func doUprOpen(mc *Client, name string, sequence uint32) error {

	rq := &gomemcached.MCRequest{
		Opcode: gomemcached.UPR_OPEN,
		Key:    []byte(name),
		Opaque: opaqueOpen,
	}

	rq.Extras = make([]byte, 8)
	binary.BigEndian.PutUint32(rq.Extras[:4], sequence)

	// flags = 0 for consumer
	binary.BigEndian.PutUint32(rq.Extras[4:], 1)

	if err := mc.Transmit(rq); err != nil {
		return err
	}

	if res, err := mc.Receive(); err != nil {
		return err
	} else if res.Opcode != gomemcached.UPR_OPEN {
		return fmt.Errorf("unexpected #opcode %v", res.Opcode)
	} else if rq.Opaque != res.Opaque {
		return fmt.Errorf("opaque mismatch, %v over %v", res.Opaque, res.Opaque)
	} else if res.Status != gomemcached.SUCCESS {
		return fmt.Errorf("Error %v", res.Status)
	}

	ul.LogDebug("", "", "UPR open success")
	return nil
}

// Connect to a UPR producer
// Name: name of te UPR connection
// sequence: sequence number for the connection
// bufsize: max size of the application
func (feed *UprFeed) UprOpen(name string, sequence uint32, bufSize uint32) error {

	mc := feed.conn

	if err := doUprOpen(mc, name, sequence); err != nil {
		return err
	}
	// send a UPR control message to set the window size for the this connection
	if bufSize > 0 {
		rq := &gomemcached.MCRequest{
			Opcode: gomemcached.UPR_CONTROL,
			Key:    []byte("connection_buffer_size"),
			Body:   []byte(strconv.Itoa(int(bufSize))),
		}
		feed.transmitCh <- rq
	}

	return nil
}

type FailoverLogMap map[uint16]*FailoverLog

// Get failover logs for a given vbucket
func (mc *Client) UprGetFailoverLog(vb []uint16) (map[uint16]*FailoverLog, error) {

	ul.LogDebug("", "", "Get Failover Log")

	rq := &gomemcached.MCRequest{
		Opcode: gomemcached.UPR_FAILOVERLOG,
		Opaque: opaqueFailover,
	}

	if err := doUprOpen(mc, "FailoverLog", 0); err != nil {
		return nil, fmt.Errorf("UPR_OPEN Failed %s", err.Error())
	}

	failoverLogs := make(map[uint16]*FailoverLog)
	for _, vBucket := range vb {
		rq.VBucket = vBucket
		if err := mc.Transmit(rq); err != nil {
			return nil, err
		}
		res, err := mc.Receive()

		if err != nil {
			return nil, fmt.Errorf("Failed to receive %s", err.Error())
		} else if res.Opcode != gomemcached.UPR_FAILOVERLOG || res.Status != gomemcached.SUCCESS {
			return nil, fmt.Errorf("Unexpected #opcode %v", res.Opcode)
		}

		flog, err := parseFailoverLog(res.Body)
		if err != nil {
			return nil, fmt.Errorf("Unable to parse failover logs for vb %d", vb)
		}
		failoverLogs[vBucket] = flog
	}

	return failoverLogs, nil
}

func (feed *UprFeed) UprRequestStream(vb uint16, flags uint32,
	vuuid, startSequence, endSequence, snapStart, snapEnd uint64) error {

	rq := &gomemcached.MCRequest{
		Opcode:  gomemcached.UPR_STREAMREQ,
		VBucket: vb,
		Opaque:  uint32(vb),
	}

	rq.Extras = make([]byte, 48) // #Extras
	binary.BigEndian.PutUint32(rq.Extras[:4], flags)
	binary.BigEndian.PutUint32(rq.Extras[4:8], uint32(0))
	binary.BigEndian.PutUint64(rq.Extras[8:16], startSequence)
	binary.BigEndian.PutUint64(rq.Extras[16:24], endSequence)
	binary.BigEndian.PutUint64(rq.Extras[24:32], vuuid)
	binary.BigEndian.PutUint64(rq.Extras[32:40], snapStart)
	binary.BigEndian.PutUint64(rq.Extras[40:48], snapEnd)

	if err := feed.conn.Transmit(rq); err != nil {
		ul.LogError("", "", "Error in StreamRequest %s", err.Error())
		return err
	}

	stream := &UprStream{
		VbUuid:   vuuid,
		StartSeq: startSequence,
		EndSeq:   endSequence,
	}

	feed.vbstreams[vb] = stream
	return nil
}

// Start the upr feed
func (feed *UprFeed) StartFeed() error {

	ch := make(chan *UprEvent)
	feed.C = ch
	go feed.runFeed(ch)
	return nil
}

func parseFailoverLog(body []byte) (*FailoverLog, error) {

	if len(body)%16 != 0 {
		err := fmt.Errorf("invalid body length %v, in failover-log", len(body))
		return nil, err
	}
	log := make(FailoverLog, len(body)/16)
	for i, j := 0, 0; i < len(body); i += 16 {
		vuuid := binary.BigEndian.Uint64(body[i : i+8])
		seqno := binary.BigEndian.Uint64(body[i+8 : i+16])
		log[j] = [2]uint64{vuuid, seqno}
		j++
	}
	return &log, nil
}

func handleStreamRequest(res *gomemcached.MCResponse) (gomemcached.Status, uint64, *FailoverLog, error) {

	var rollback uint64
	var err error

	switch {
	case res.Status == gomemcached.ROLLBACK && len(res.Extras) != 8:
		err = fmt.Errorf("invalid rollback %v\n", res.Extras)
		return res.Status, 0, nil, err
	case res.Status == gomemcached.ROLLBACK:
		rollback = binary.BigEndian.Uint64(res.Extras)
		ul.LogInfo("", "", "Rollback %v for vb %v\n", rollback, res.Opaque /*vb*/)
		return res.Status, rollback, nil, nil
	case res.Status != gomemcached.SUCCESS:
		err = fmt.Errorf("Unexpected status %v, for %v", res.Status, res.Opaque)
		return res.Status, 0, nil, err
	}

	flog, err := parseFailoverLog(res.Body[:])
	return res.Status, rollback, flog, err
}

func (feed *UprFeed) runFeed(ch chan *UprEvent) {
	defer close(ch)
	var headerBuf [gomemcached.HDR_LEN]byte
	var pkt gomemcached.MCRequest
	var event *UprEvent

	mc := feed.conn.Hijack()

	var mutationCtr uint64
	var snapshotCtr uint64

loop:
	for {
		sendAck := false
		bytes, err := pkt.Receive(mc, headerBuf[:])
		if err != nil {
			ul.LogError("", "", "Error in receive %s", err.Error())
			feed.Error = err
			break loop
		} else {
			event = nil
			res := &gomemcached.MCResponse{
				Opcode: pkt.Opcode,
				Cas:    pkt.Cas,
				Opaque: pkt.Opaque,
				Status: gomemcached.Status(pkt.VBucket),
				Extras: pkt.Extras,
				Key:    pkt.Key,
				Body:   pkt.Body,
			}

			vb := uint16(res.Opaque)
			feed.bytesRead += uint64(bytes)

			switch pkt.Opcode {
			case gomemcached.UPR_STREAMREQ:
				stream := feed.vbstreams[vb]
				if stream == nil {
					ul.LogError("", "", "Fatal Error, Stream not found for vb %d", vb)
					break loop
				}

				status, rb, flog, err := handleStreamRequest(res)
				if status == gomemcached.ROLLBACK {
					// rollback stream
					if err := feed.UprRequestStream(vb, 0, stream.VbUuid, rb,
						stream.EndSeq, 0, 0); err != nil {
						ul.LogError("", "",
							"UPR_STREAMREQ with rollback %d for vb % Failed. Error %s",
							rb, vb, err.Error())
						event = makeUprEvent(pkt)
						// delete the stream from the vbmap for the feed
						delete(feed.vbstreams, vb)
					}
				} else if status == gomemcached.SUCCESS {
					event = makeUprEvent(pkt)
					event.FailoverLog = flog
					stream.connected = true
				} else if err != nil {
					ul.LogError("", "", "UPR_STREAMREQ for vbucket %d erro %s", vb, err.Error())
					event = &UprEvent{Opcode: UprStreamRequest, Status: status, VBucket: vb, Error: err}
				}
			case gomemcached.UPR_MUTATION,
				gomemcached.UPR_DELETION,
				gomemcached.UPR_EXPIRATION:
				event = makeUprEvent(pkt)
				mutationCtr++
				sendAck = true
			case gomemcached.UPR_STREAMEND:
				//stream has ended
				event = makeUprEvent(pkt)
				ul.LogInfo("", "", "Stream Ended for vb %d", vb)
				sendAck = true
				delete(feed.vbstreams, vb)
			case gomemcached.UPR_SNAPSHOT:
				// snapshot marker
				event = makeUprEvent(pkt)
				event.SnapstartSeq = binary.BigEndian.Uint64(pkt.Extras[0:8])
				event.SnapendSeq = binary.BigEndian.Uint64(pkt.Extras[8:16])
				event.SnapshotType = binary.BigEndian.Uint32(pkt.Extras[16:20])
				snapshotCtr++
				sendAck = true
			case gomemcached.UPR_FLUSH:
				// special processing for flush ?
				event = makeUprEvent(pkt)
			case gomemcached.UPR_ADDSTREAM, gomemcached.UPR_CLOSESTREAM:
				ul.LogWarn("", "", "Opcode %v not implemented", pkt.Opcode)
			case gomemcached.UPR_CONTROL, gomemcached.UPR_BUFFERACK:
				if res.Status != gomemcached.SUCCESS {
					ul.LogWarn("", "", "Opcode %v received status %d", pkt.Opcode.String(), res.Status)
				}
			case gomemcached.UPR_NOOP:
				// send a NOOP back
				noop := &gomemcached.MCRequest{
					Opcode: gomemcached.UPR_NOOP,
				}
				feed.transmitCh <- noop
			default:
				ul.LogError("", "", "Recived an unknown response for vbucket %d", vb)
			}
		}

		if event != nil {
			select {
			case ch <- event:
			case <-feed.closer:
				break loop
			}

			if event.Opcode == UprCloseStream && len(feed.vbstreams) == 0 {
				ul.LogInfo("", "", "No more streams")
				break loop
			}

		}

		if sendAck == true {
			bufferAck := &gomemcached.MCRequest{
				Opcode: gomemcached.UPR_BUFFERACK,
			}
			bufferAck.Body = make([]byte, 4)
			binary.BigEndian.PutUint32(bufferAck.Body[:4], uint32(bytes))
			feed.transmitCh <- bufferAck
		}

	}

	feed.transmitCl <- true
}

func (feed *UprFeed) Close() {
	close(feed.closer)
}
