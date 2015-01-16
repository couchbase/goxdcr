// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package parts

import (
	"encoding/binary"
	"errors"
	"fmt"
	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	base "github.com/couchbase/goxdcr/base"
	common "github.com/couchbase/goxdcr/common"
	gen_server "github.com/couchbase/goxdcr/gen_server"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/utils"
	"io"
	"math/rand"
	"net"
	"reflect"
	"sync"
	"time"
)

type XMEM_MODE int

//configuration settings for XmemNozzle
const (
	//configuration param names
	XMEM_SETTING_BATCHCOUNT            = "batch_count"
	XMEM_SETTING_BATCHSIZE             = "batch_size"
	XMEM_SETTING_MODE                  = "mode"
	XMEM_SETTING_NUMOFRETRY            = "max_retry"
	XMEM_SETTING_RESP_TIMEOUT          = "resp_timeout"
	XMEM_SETTING_WRITE_TIMEOUT         = "write_timeout"
	XMEM_SETTING_BATCH_EXPIRATION_TIME = "batch_expiration_time"
	XMEM_SETTING_MAX_RETRY_INTERVAL    = "max_retry_interval"
	XMEM_SETTING_OPTI_REP_THRESHOLD    = "optimistic_replication_threshold"

	XMEM_STATS_QUEUE_SIZE       = "queue_size"
	XMEM_STATS_QUEUE_SIZE_BYTES = "queue_size_bytes"
	XMEM_EVENT_ADDI_SEQNO       = "source_seqno"
	XMEM_EVENT_ADDI_OPT_REPD    = "optimistic_replicated"

	//default configuration
	default_numofretry          int           = 10
	default_resptimeout         time.Duration = 100 * time.Millisecond
	default_dataChannelSize                   = 5000
	default_batchExpirationTime               = 400 * time.Millisecond
	default_maxRetryInterval                  = 30 * time.Second
	default_writeTimeOut        time.Duration = time.Duration(1) * time.Second
)

const (
	GET_WITH_META    = mc.CommandCode(0xa0)
	SET_WITH_META    = mc.CommandCode(0xa2)
	DELETE_WITH_META = mc.CommandCode(0xa8)
)

var xmem_setting_defs base.SettingDefinitions = base.SettingDefinitions{XMEM_SETTING_BATCHCOUNT: base.NewSettingDef(reflect.TypeOf((*int)(nil)), true),
	XMEM_SETTING_BATCHSIZE:             base.NewSettingDef(reflect.TypeOf((*int)(nil)), true),
	XMEM_SETTING_MODE:                  base.NewSettingDef(reflect.TypeOf((*XMEM_MODE)(nil)), false),
	XMEM_SETTING_NUMOFRETRY:            base.NewSettingDef(reflect.TypeOf((*int)(nil)), false),
	XMEM_SETTING_RESP_TIMEOUT:          base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	XMEM_SETTING_WRITE_TIMEOUT:         base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	XMEM_SETTING_MAX_RETRY_INTERVAL:    base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	XMEM_SETTING_BATCH_EXPIRATION_TIME: base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	XMEM_SETTING_OPTI_REP_THRESHOLD:    base.NewSettingDef(reflect.TypeOf((*int)(nil)), true)}

type documentMetadata struct {
	key      []byte
	revSeq   uint64 //Item revision seqno
	cas      uint64 //Item cas
	flags    uint32 // Item flags
	expiry   uint32 // Item expiration time
	deletion bool
}

func (doc_meta documentMetadata) String() string {
	return fmt.Sprintf("[key=%s; revSeq=%v;cas=%v;flags=%v;expiry=%v;deletion=%v]", doc_meta.key, doc_meta.revSeq, doc_meta.cas, doc_meta.flags, doc_meta.expiry, doc_meta.deletion)
}

type ConflictResolver func(doc_metadata_source documentMetadata, doc_metadata_target documentMetadata, logger *log.CommonLogger) bool

/************************************
/* struct bufferedMCRequest
*************************************/

type bufferedMCRequest struct {
	req          *base.WrappedMCRequest
	sent_time    time.Time
	num_of_retry int
	err          error
	reservation  int
}

func newBufferedMCRequest(request *base.WrappedMCRequest, reservationNum int) *bufferedMCRequest {
	return &bufferedMCRequest{req: request,
		sent_time:    time.Now(),
		num_of_retry: 0,
		err:          nil,
		reservation:  reservationNum}
}

/***********************************************************
/* struct requestBuffer
/* This is used to buffer the sent but yet confirmed data
************************************************************/
type requestBuffer struct {
	slots           []*bufferedMCRequest /*slots to store the data*/
	sequences       []uint16
	empty_slots_pos chan uint16 /*empty slot pos in the buffer*/
	size            uint16      /*the size of the buffer*/
	notifych        chan bool   /*notify channel is set when the buffer is below threshold*/
	//	notify_allowed  bool   /*notify is allowed*/
	notify_threshold uint16
	fin_ch           chan bool
	logger           *log.CommonLogger
}

func newReqBuffer(size uint16, threshold uint16, logger *log.CommonLogger) *requestBuffer {
	logger.Debugf("Create a new request buffer of size %d\n", size)
	buf := &requestBuffer{
		make([]*bufferedMCRequest, size, size),
		make([]uint16, size),
		make(chan uint16, size),
		size,
		nil,
		threshold,
		make(chan bool, 1),
		logger}

	logger.Debug("Slots is initialized")

	//initialize the empty_slots_pos
	buf.initializeEmptySlotPos()

	logger.Debugf("new request buffer of size %d is created\n", size)
	return buf
}

func (buf *requestBuffer) close() {
	close(buf.fin_ch)
	buf.logger.Info("request buffer is closed. No blocking on flow control")
}

//not concurrent safe. Caller need to be aware
func (buf *requestBuffer) setNotifyThreshold(threshold uint16) {
	buf.notify_threshold = threshold
}

func (buf *requestBuffer) initializeEmptySlotPos() error {
	for i := 0; i < int(buf.size); i++ {
		buf.empty_slots_pos <- uint16(i)
		buf.sequences[i] = 0
	}

	return nil
}

//blocking until the occupied slots are below threshold
func (buf *requestBuffer) flowControl() {
	ret := buf.size-uint16(len(buf.empty_slots_pos)) <= buf.notify_threshold
	if ret {
		return
	}
	buf.notifych = make(chan bool, 1)
	select {
	case <-buf.notifych:
		buf.notifych = nil
		return
	case <-buf.fin_ch:
		buf.notifych = nil
		return
	}
}

func (buf *requestBuffer) validatePos(pos uint16) (err error) {
	err = nil
	if pos < 0 || int(pos) >= len(buf.slots) {
		buf.logger.Error("Invalid slot index")
		err = errors.New("Invalid slot index")
	}
	return
}

//slot allow caller to get hold of the content in the slot without locking the slot
//@pos - the position of the slot
func (buf *requestBuffer) slot(pos uint16) (uint64, *mc.MCRequest, error) {
	buf.logger.Debugf("Getting the content in slot %d\n", pos)

	err := buf.validatePos(pos)
	if err != nil {
		return 0, nil, err
	}

	req := buf.slots[pos]

	if req == nil || req.req == nil {
		return 0, nil, nil
	} else {
		return req.req.Seqno, req.req.Req, nil
	}

}

//modSlot allow caller to do book-keeping on the slot, like updating num_of_retry, err
//@pos - the position of the slot
//@modFunc - the callback function which is going to update the slot
func (buf *requestBuffer) modSlot(pos uint16, modFunc func(req *bufferedMCRequest, p uint16) (bool, error)) (bool, error) {
	var err error = nil
	err = buf.validatePos(pos)
	if err != nil {
		return false, err
	}

	req := buf.slots[pos]

	var modified bool

	if req != nil && req.req != nil {
		modified, err = modFunc(req, pos)
	} else {
		modified = false
	}
	return modified, err
}

//evictSlot allow caller to empty the slot
//@pos - the position of the slot
//note: not concurrency safe, should be called only in one goroutine
func (buf *requestBuffer) evictSlot(pos uint16) error {

	err := buf.validatePos(pos)
	if err != nil {
		return err
	}
	req := buf.slots[pos]
	buf.slots[pos] = nil

	if req != nil {
		buf.empty_slots_pos <- pos

		//increase sequence
		if buf.sequences[pos]+1 > 65535 {
			buf.sequences[pos] = 0
		} else {
			buf.sequences[pos] = buf.sequences[pos] + 1
		}

		if buf.size-uint16(len(buf.empty_slots_pos)) <= buf.notify_threshold {
			if buf.notifych != nil {
				select {
				case buf.notifych <- true:
					buf.logger.Debugf("buffer's occupied slots is below threshold %v, notify", buf.notify_threshold)
				default:
				}
			} else {
				buf.logger.Debugf("buffer's occupied slots is below threshold %v, no notify channel is specified though", buf.notify_threshold)
			}
		}
	}
	return nil

}

//availableSlotIndex returns a position number of an empty slot
func (buf *requestBuffer) reserveSlot() (error, uint16, int) {
	buf.logger.Debugf("slots chan length=%d\n", len(buf.empty_slots_pos))
	index := <-buf.empty_slots_pos

	var reservation_num int

	//non blocking
	//generate a random number
	reservation_num = rand.Int()
	req := newBufferedMCRequest(nil, reservation_num)
	buf.slots[index] = req
	return nil, uint16(index), reservation_num
}

func (buf *requestBuffer) cancelReservation(index uint16, reservation_num int) error {

	err := buf.validatePos(index)
	if err == nil {
		buf.empty_slots_pos <- index

		req := buf.slots[index]
		var reservation_num int

		//non blocking

		if req.reservation == reservation_num {
			req = nil
		} else {
			err = errors.New("Cancel reservation failed, reservation number doesn't match")
		}
	}
	return err
}

func (buf *requestBuffer) enSlot(pos uint16, req *base.WrappedMCRequest, reservationNum int) error {
	buf.logger.Debugf("enSlot: pos=%d\n", pos)

	err := buf.validatePos(pos)
	if err != nil {
		return err
	}
	r := buf.slots[pos]

	if r == nil {
		buf.slots[pos] = newBufferedMCRequest(nil, 0)
	} else {
		if r.reservation != reservationNum {
			buf.logger.Errorf("Can't enSlot %d, doesn't have the reservation, %v", pos, r)
			return errors.New(fmt.Sprintf("Can't enSlot %d, doesn't have the reservation", pos))
		}
		r.req = req
	}
	buf.logger.Debugf("slot %d is occupied\n", pos)
	return nil
}

func (buf *requestBuffer) bufferSize() uint16 {
	return buf.size
}

/************************************
/* struct xmemConfig
*************************************/
type xmemConfig struct {
	maxCount int
	maxSize  int
	//the duration to wait for the batch-sending to finish
	respTimeout         time.Duration
	maxRetryInterval    time.Duration
	batchExpirationTime time.Duration
	writeTimeout        time.Duration
	maxRetry            int
	optiRepThreshold    int
	connectStr          string
	bucketName          string
	password            string
	logger              *log.CommonLogger
}

func newConfig(logger *log.CommonLogger) xmemConfig {
	return xmemConfig{maxCount: -1,
		maxSize:             -1,
		respTimeout:         default_resptimeout,
		batchExpirationTime: default_batchExpirationTime,
		writeTimeout:        default_writeTimeOut,
		maxRetryInterval:    default_maxRetryInterval,
		maxRetry:            default_numofretry,
		connectStr:          "",
		bucketName:          "",
		password:            "",
	}

}

func (config *xmemConfig) initializeConfig(settings map[string]interface{}) error {
	err := utils.ValidateSettings(xmem_setting_defs, settings, config.logger)

	if err == nil {
		if val, ok := settings[XMEM_SETTING_BATCHSIZE]; ok {
			config.maxSize = val.(int)
		}
		if val, ok := settings[XMEM_SETTING_BATCHCOUNT]; ok {
			config.maxCount = val.(int)
		}
		if val, ok := settings[XMEM_SETTING_RESP_TIMEOUT]; ok {
			config.respTimeout = val.(time.Duration)
		}
		if val, ok := settings[XMEM_SETTING_NUMOFRETRY]; ok {
			config.maxRetry = val.(int)
		}
		if val, ok := settings[XMEM_SETTING_BATCH_EXPIRATION_TIME]; ok {
			config.batchExpirationTime = val.(time.Duration)
		}
		if val, ok := settings[XMEM_SETTING_WRITE_TIMEOUT]; ok {
			config.writeTimeout = val.(time.Duration)
		}
		if val, ok := settings[XMEM_SETTING_MAX_RETRY_INTERVAL]; ok {
			config.maxRetryInterval = val.(time.Duration)
		}
		if val, ok := settings[XMEM_SETTING_OPTI_REP_THRESHOLD]; ok {
			config.optiRepThreshold = val.(int)
		}
	}
	return err
}

/************************************
/* struct xmemBatch
*************************************/
type xmemBatch struct {
	//the document whose size is larger than optimistic replication threshold
	//key of the map is the document key, value is the vbucket number
	bigDoc_key_map map[string]uint16
	//the document metadata retrieved from target system for the big docs
	bigDoc_meta_map   map[string]documentMetadata
	curCount          int
	curSize           int
	capacity_count    int
	capacity_size     int
	start_time        time.Time
	frozen            bool
	logger            *log.CommonLogger
	expiring_duration time.Duration
	expire_ch         <-chan time.Time
}

func newXmemBatch(cap_count int, cap_size int, expiring_duration time.Duration, logger *log.CommonLogger) *xmemBatch {
	return &xmemBatch{
		curCount:          0,
		curSize:           0,
		capacity_count:    cap_count,
		capacity_size:     cap_size,
		expiring_duration: expiring_duration,
		bigDoc_key_map:    make(map[string]uint16),
		bigDoc_meta_map:   make(map[string]documentMetadata),
		logger:            logger}
}

func (b *xmemBatch) accumuBatch(req *base.WrappedMCRequest, classifyFunc func(req *mc.MCRequest) bool) bool {
	var ret bool = true

	if req != nil && req.Req != nil {
		size := req.Req.Size()
		key := string(req.Req.Key)

		b.curCount++
		if b.curCount == 1 {
			b.start_time = time.Now()
			b.expire_ch = time.After(b.expiring_duration)
		}
		if !classifyFunc(req.Req) {
			b.bigDoc_key_map[key] = req.Req.VBucket
		}
		b.curSize += size
		if b.curCount < b.capacity_count && b.curSize < b.capacity_size*1000 {
			ret = false
		}
	}
	return ret
}

func (b *xmemBatch) count() int {
	return b.curCount
}

func (b *xmemBatch) size() int {
	return b.curSize

}

/************************************
/* struct XmemNozzle
*************************************/
type XmemNozzle struct {

	//parent inheritance
	gen_server.GenServer
	AbstractPart

	bOpen      bool
	lock_bOpen sync.RWMutex

	//data channel to accept the incoming data
	dataChan chan *base.WrappedMCRequest
	//the total size of data (in byte) queued in dataChan
	bytes_in_dataChan int

	//memcached client connected to the target bucket
	lock_connection    sync.RWMutex
	memClient          *mcc.Client
	client_for_getMeta *mcc.Client

	//configurable parameter
	config xmemConfig

	//queue for ready batches
	batches_ready chan *xmemBatch

	//batch to be accumulated
	batch *xmemBatch

	//channel to signal if batch can be transitioned to batches_ready
	batch_move_ch chan bool

	childrenWaitGrp sync.WaitGroup

	//buffer for the sent, but not yet confirmed data
	buf *requestBuffer

	//conflict resolover
	conflict_resolver ConflictResolver

	sender_finch   chan bool
	receiver_finch chan bool
	checker_finch  chan bool

	counter_sent     int
	counter_received int
	start_time       time.Time
	handle_error     bool
}

func NewXmemNozzle(id string,
	connectString string,
	bucketName string,
	password string,
	logger_context *log.LoggerContext) *XmemNozzle {

	//callback functions from GenServer
	var msg_callback_func gen_server.Msg_Callback_Func
	var exit_callback_func gen_server.Exit_Callback_Func
	var error_handler_func gen_server.Error_Handler_Func

	var isStarted_callback_func IsStarted_Callback_Func

	server := gen_server.NewGenServer(&msg_callback_func,
		&exit_callback_func, &error_handler_func, logger_context, "XmemNozzle")
	isStarted_callback_func = server.IsStarted
	part := NewAbstractPartWithLogger(id, &isStarted_callback_func, server.Logger())

	xmem := &XmemNozzle{GenServer: server, /*gen_server.GenServer*/
		AbstractPart:       part,           /*part.AbstractPart*/
		bOpen:              true,           /*bOpen	bool*/
		lock_bOpen:         sync.RWMutex{}, /*lock_bOpen	sync.RWMutex*/
		dataChan:           nil,            /*dataChan*/
		memClient:          nil,            /*memClient*/
		client_for_getMeta: nil,
		lock_connection:    sync.RWMutex{},
		config:             newConfig(server.Logger()), /*config	xmemConfig*/
		batches_ready:      nil,                        /*batches_ready chan *xmemBatch*/
		batch:              nil,                        /*batch		  *xmemBatch*/
		batch_move_ch:      nil,
		childrenWaitGrp:    sync.WaitGroup{}, /*childrenWaitGrp sync.WaitGroup*/
		buf:                nil,              /*buf	requestBuffer*/
		receiver_finch:     make(chan bool),  /*receiver_finch chan bool*/
		checker_finch:      make(chan bool),  /*checker_finch chan bool*/
		sender_finch:       make(chan bool),
		//		send_allow_ch:    make(chan bool, 1), /*send_allow_ch chan bool*/
		counter_sent:     0,
		handle_error:     true,
		counter_received: 0}

	xmem.config.connectStr = connectString
	xmem.config.bucketName = bucketName
	xmem.config.password = password

	msg_callback_func = nil
	exit_callback_func = xmem.onExit
	error_handler_func = xmem.handleGeneralError
	return xmem

}

func (xmem *XmemNozzle) Open() error {
	if !xmem.bOpen {
		xmem.bOpen = true

	}
	return nil
}

func (xmem *XmemNozzle) Close() error {
	if xmem.bOpen {
		xmem.bOpen = false
	}
	return nil
}

func (xmem *XmemNozzle) Start(settings map[string]interface{}) error {
	xmem.Logger().Info("Xmem starting ....")
	err := xmem.initialize(settings)
	xmem.Logger().Info("....Finish initializing....")
	if err == nil {
		xmem.childrenWaitGrp.Add(1)
		go xmem.receiveResponse(xmem.receiver_finch, &xmem.childrenWaitGrp)

		xmem.childrenWaitGrp.Add(1)
		go xmem.check(xmem.checker_finch, &xmem.childrenWaitGrp)

		xmem.childrenWaitGrp.Add(1)
		go xmem.processData_batch(xmem.sender_finch, &xmem.childrenWaitGrp)
	}

	//set conflict resolver
	xmem.conflict_resolver = resolveConflictByRevSeq

	xmem.start_time = time.Now()
	if err == nil {
		err = xmem.Start_server()
	}

	xmem.Logger().Info("Xmem nozzle is started")
	return err
}

func (xmem *XmemNozzle) getReadyToShutdown() {
	xmem.Logger().Debug("Waiting for data is drained")

	for {
		if len(xmem.dataChan) == 0 && len(xmem.batches_ready) == 0 {
			xmem.Logger().Debug("Ready to stop")
			break
		} else if len(xmem.batches_ready) == 0 && xmem.batch.count() > 0 {
			xmem.batchReady()

		} else {
			xmem.Logger().Debugf("%d in data channel, %d batches ready, % data in current batch \n", len(xmem.dataChan), len(xmem.batches_ready), xmem.batch.count())
		}
	}

	close(xmem.batches_ready)
}

func (xmem *XmemNozzle) Stop() error {
	xmem.Logger().Infof("Stop XmemNozzle %v\n", xmem.Id())

	conn := xmem.memClient.Hijack()
	conn.(*net.TCPConn).SetReadDeadline(time.Now())

	xmem.Logger().Debugf("XmemNozzle %v processed %v items\n", xmem.Id(), xmem.counter_sent)
	err := xmem.Stop_server()

	conn.(*net.TCPConn).SetReadDeadline(time.Date(1, time.January, 0, 0, 0, 0, 0, time.UTC))
	xmem.Logger().Debugf("XmemNozzle %v is stopped\n", xmem.Id())
	return err
}

func (xmem *XmemNozzle) IsOpen() bool {
	ret := xmem.bOpen
	return ret
}

func (xmem *XmemNozzle) batchReady() error {
	//move the batch to ready batches channel
	<-xmem.batch_move_ch
	defer func() {
		xmem.batch_move_ch <- true
		xmem.Logger().Debugf("%v End moving batch, %v batches ready\n", xmem.Id(), len(xmem.batches_ready))
	}()
	if xmem.batch.count() > 0 {
		xmem.Logger().Debugf("%v move the batch (count=%d) ready queue\n", xmem.Id(), xmem.batch.count())
		meta_map, err := xmem.batchGetMeta(xmem.batch.bigDoc_key_map)
		if err != nil {
			return err
		}
		xmem.batch.bigDoc_meta_map = meta_map

		select {
		case xmem.batches_ready <- xmem.batch:
			xmem.Logger().Debugf("There are %d batches in ready queue\n", len(xmem.batches_ready))

			xmem.initNewBatch()
		}
	}
	return nil

}

func (xmem *XmemNozzle) Receive(data interface{}) error {
	xmem.Logger().Debugf("data key=%v seq=%v is received", data.(*base.WrappedMCRequest).Req.Key, data.(*base.WrappedMCRequest).Seqno)
	xmem.Logger().Debugf("data channel len is %d\n", len(xmem.dataChan))

	request := data.(*base.WrappedMCRequest)

	xmem.dataChan <- request

	xmem.counter_received++
	size := request.Req.Size()
	xmem.bytes_in_dataChan = xmem.bytes_in_dataChan + size

	//accumulate the batchCount and batchSize
	if xmem.batch.accumuBatch(request, xmem.optimisticRep) {
		xmem.batchReady()
	}

	//raise DataReceived event
	additionalInfo := make(map[string]interface{})
	additionalInfo[XMEM_STATS_QUEUE_SIZE] = len(xmem.dataChan)
	additionalInfo[XMEM_STATS_QUEUE_SIZE_BYTES] = xmem.bytes_in_dataChan
	xmem.RaiseEvent(common.DataReceived, request, xmem, nil, additionalInfo)
	xmem.Logger().Debugf("Xmem %v received %v items, queue_size = %v, bytes_in_dataChan=%v\n", xmem.Id(), xmem.counter_received, len(xmem.dataChan), xmem.bytes_in_dataChan)

	return nil
}

func (xmem *XmemNozzle) processData_batch(finch chan bool, waitGrp *sync.WaitGroup) (err error) {
	xmem.Logger().Infof("%v processData starts..........\n", xmem.Id())
	defer waitGrp.Done()
	for {
		xmem.Logger().Debugf("%v processData ....\n", xmem.Id())
		select {
		case <-finch:
			goto done
		case batch := <-xmem.batches_ready:
			select {
			case <-finch:
				goto done
			default:
				if xmem.IsOpen() {
					xmem.buf.flowControl()
					xmem.Logger().Debugf("%v Batch Send..., %v batches ready, %v items in queue, count_recieved=%v, count_sent=%v\n", xmem.Id(), len(xmem.batches_ready), len(xmem.dataChan), xmem.counter_received, xmem.counter_sent)
					err = xmem.sendSetMeta_internal(batch)
					if err != nil {
						xmem.handleGeneralError(err)
					}
				}
			}

		}
	}

done:
	xmem.Logger().Infof("%v processData_batch exits\n", xmem.Id())
	return
}

func (xmem *XmemNozzle) onExit() {
	//in the process of stopping, no need to report any error to replication manager anymore
	xmem.handle_error = false

	xmem.buf.close()

	//notify the data processing routine
	close(xmem.sender_finch)
	close(xmem.receiver_finch)
	close(xmem.checker_finch)
	xmem.childrenWaitGrp.Wait()

	//cleanup
	pool := base.ConnPoolMgr().GetPool(xmem.getPoolName(xmem.config))
	if pool != nil {
		pool.Release(xmem.memClient)
	}

}

func (xmem *XmemNozzle) batchSetMetaWithRetry(batch *xmemBatch, numOfRetry int) error {
	var err error
	count := batch.count()

	for i := 0; i < count; i++ {
		item := <-xmem.dataChan

		if xmem.needSend(item.Req, batch) {
			xmem.bytes_in_dataChan = xmem.bytes_in_dataChan - item.Req.Size()
			//blocking
			err, index, reserv_num := xmem.buf.reserveSlot()
			if err != nil {
				return err
			}
			xmem.adjustRequest(item, index)
			item_byte := item.Req.Bytes()
			err = xmem.sendWithRetry(xmem.memClient, numOfRetry, item_byte)
			if err == nil {
				err = xmem.buf.enSlot(index, item, reserv_num)
			}

			if err != nil {
				xmem.Logger().Errorf("%v Failed to send. err=%v\n", xmem.Id(), err)
				xmem.buf.cancelReservation(index, reserv_num)
			}
		} else {
			//lost on conflict resolution on source side
			//TODO: raise event
		}

	}

	//log the data
	return err
}

func (xmem *XmemNozzle) needSend(req *mc.MCRequest, batch *xmemBatch) bool {
	if req == nil {
		return false
	} else {
		key := string(req.Key)
		if doc_meta_target, ok := batch.bigDoc_meta_map[key]; ok {
			doc_meta_source := xmem.decodeSetMetaReq(req)
			if !xmem.conflict_resolver(doc_meta_source, doc_meta_target, xmem.logger) {
				xmem.Logger().Infof("doc %v (%v)failed on conflict resolution to %v, no need to send\n", key, doc_meta_source, doc_meta_target)
				return false
			}
		}
	}
	return true
}

//return true if doc_meta_source win; false otherwise
func resolveConflictByRevSeq(doc_meta_source documentMetadata,
	doc_meta_target documentMetadata, logger *log.CommonLogger) bool {
	ret := true
	if doc_meta_target.revSeq > doc_meta_source.revSeq {
		ret = false
	} else if doc_meta_target.revSeq == doc_meta_source.revSeq {
		if doc_meta_target.cas > doc_meta_source.cas {
			ret = false
		} else if doc_meta_target.cas == doc_meta_source.cas {
			//if the outgoing mutation is deletion and its revSeq and cas are the
			//same as the target side document, it would lose the conflict resolution
			if doc_meta_source.deletion || (doc_meta_target.expiry > doc_meta_source.expiry) {
				ret = false
			} else if doc_meta_target.expiry == doc_meta_source.expiry {
				if doc_meta_target.flags >= doc_meta_source.flags {
					ret = false
				}
			}
		}
	}
	return ret
}

func (xmem *XmemNozzle) sendWithRetry(client *mcc.Client, numOfRetry int, item_byte []byte) (err error) {
	for j := 0; j < numOfRetry; j++ {
		conn := client.Hijack()
		conn.(*net.TCPConn).SetWriteDeadline(time.Now().Add(xmem.config.writeTimeout * time.Second))
		_, err = conn.Write(item_byte)
		if err == nil {
			return
		} else {
			xmem.Logger().Errorf("%v batchSend: transmit error: %s\n", xmem.Id(), fmt.Sprint(err))

			if !isSeriousError(err) {
				xmem.Logger().Errorf("%v batchSend Failed, retry later\n", xmem.Id())
				time.Sleep(time.Duration(2^(j+1)) * xmem.config.writeTimeout * time.Second)
			} else {
				xmem.repairConn(xmem.memClient)
			}
		}
	}
	return
}

func (xmem *XmemNozzle) sendSetMeta_internal(batch *xmemBatch) error {
	var err error
	if batch != nil {
		count := batch.count()

		xmem.Logger().Infof("Send batch count=%d\n", count)

		xmem.counter_sent = xmem.counter_sent + count
		xmem.Logger().Debugf("So far, xmem %v processed %d items", xmem.Id(), xmem.counter_sent)

		//batch send
		err = xmem.batchSetMetaWithRetry(batch, xmem.config.maxRetry)
		if err != nil {
			xmem.repairConn(xmem.memClient)
			err = xmem.batchSetMetaWithRetry(batch, xmem.config.maxRetry)
		}
	}
	return err
}

//batch call to memcached GetMeta command for document size larger than the optimistic threshold
func (xmem *XmemNozzle) batchGetMeta(key_map map[string]uint16) (map[string]documentMetadata, error) {
	xmem.Logger().Infof("batchGetMeta, key_map length=%v\n", len(key_map))
	ret := make(map[string]documentMetadata)
	respMap := make(map[uint32]*mc.MCResponse)
	waitGrp := &sync.WaitGroup{}
	receiver_fin_ch := make(chan bool, 1)
	var err error = nil

	//if the key_map size is 0, return
	if len(key_map) == 0 {
		return ret, nil
	}

	//launch the receiver
	waitGrp.Add(1)
	go func(count int, finch chan bool, respMap map[uint32]*mc.MCResponse, waitGrp *sync.WaitGroup, logger *log.CommonLogger) {
		defer waitGrp.Done()
		received_count := 0
		read_retry := 0
		var response *mc.MCResponse
		for {
			select {
			case <-finch:
				return
			default:
				conn := xmem.client_for_getMeta.Hijack()
				conn.(*net.TCPConn).SetReadDeadline(time.Now().Add(300 * time.Millisecond))
				response, err = xmem.client_for_getMeta.Receive()

				if err == io.EOF {
					logger.Errorf("%v Quit receive response for GetMeta. err=%v\n", xmem.Id(), err)
					return
				} else if err != nil && isNetError(err) {
					if isSeriousError(err) {
						xmem.repairConn(xmem.client_for_getMeta)
						read_retry = 0
					} else {
						//retry in 100 millisecond
						if read_retry > 10 {
							xmem.repairConn(xmem.memClient)
							read_retry = 0
						}
						logger.Debug("read response timed out")
						time.Sleep(100 * time.Millisecond)
						read_retry++
					}
					return
				} else if err != nil && mc.IsFatal(err) {
					if response.Status == 0x08 {
						//PROTOCOL_BINARY_RESPONSE_NO_BUCKET
						//bucket must be recreated, drop the connection pool
						poolName := xmem.getPoolName(xmem.config)
						pool := base.ConnPoolMgr().GetPool(poolName)
						if pool != nil {
							pool.ReleaseConnections()
						}

					}
					return
				} else {
					//success
					received_count++
					respMap[response.Opaque] = response

					//reset read_retry counter to 0
					read_retry = 0

				}

				if received_count >= count {
					logger.Infof("Expected %v response, got all", count)
					return
				}
			}
		}

	}(len(key_map), receiver_fin_ch, respMap, waitGrp, xmem.Logger())

	var opaque uint32 = 0
	opaque_key_map := make(map[uint32]string)
	for key, vb := range key_map {
		req := xmem.composeRequestForGetMeta(key, vb, opaque)
		err := xmem.sendWithRetry(xmem.client_for_getMeta, xmem.config.maxRetry, req.Bytes())
		if err != nil {
			//kill the receiver and return
			close(receiver_fin_ch)
			waitGrp.Wait()
			return nil, err
		}
		opaque_key_map[opaque] = key
		opaque++
	}

	waitGrp.Wait()
	for opaque, resp := range respMap {
		key, ok := opaque_key_map[opaque]
		if !ok {
			panic("opaque_key_map should contain this opaque")
		}
		if resp.Status == mc.SUCCESS {
			ret[key] = xmem.decodeGetMetaResp([]byte(key), resp)
		} else {
			xmem.Logger().Infof("batchGetMeta: doc %s is not found in target system, send it", key)
		}
	}
	xmem.Logger().Infof("Done with batchGetMeta, ret=%v\n", ret)
	return ret, nil
}

func (xmem *XmemNozzle) decodeGetMetaResp(key []byte, resp *mc.MCResponse) documentMetadata {
	ret := documentMetadata{}
	ret.key = key
	extras := resp.Extras
	ret.flags = binary.BigEndian.Uint32(extras[0:4])
	ret.expiry = binary.BigEndian.Uint32(extras[4:8])
	ret.revSeq = binary.BigEndian.Uint64(extras[8:16])
	ret.cas = resp.Cas

	return ret

}

func (xmem *XmemNozzle) decodeSetMetaReq(req *mc.MCRequest) documentMetadata {
	ret := documentMetadata{}
	ret.key = req.Key
	ret.flags = binary.BigEndian.Uint32(req.Extras[0:4])
	ret.expiry = binary.BigEndian.Uint32(req.Extras[4:8])
	ret.revSeq = binary.BigEndian.Uint64(req.Extras[8:16])
	ret.cas = req.Cas
	ret.deletion = (req.Opcode == DELETE_WITH_META)
	return ret
}

func (xmem *XmemNozzle) composeRequestForGetMeta(key string, vb uint16, opaque uint32) *mc.MCRequest {
	return &mc.MCRequest{VBucket: vb,
		Key:    []byte(key),
		Opaque: opaque,
		Opcode: GET_WITH_META}
}

func (xmem *XmemNozzle) sendSingleSetMeta(adjustRequest bool, item *base.WrappedMCRequest, index uint16) error {
	if xmem.memClient != nil {
		if adjustRequest {
			xmem.adjustRequest(item, index)
			xmem.Logger().Debugf("key=%v\n", item.Req.Key)
			xmem.Logger().Debugf("opcode=%v\n", item.Req.Opcode)
		}
		bytes := item.Req.Bytes()
		conn := xmem.memClient.Hijack()

		conn.(*net.TCPConn).SetWriteDeadline(time.Now().Add(xmem.config.writeTimeout * time.Millisecond))
		_, err := conn.Write(bytes)

		if err != nil {
			xmem.Logger().Errorf("%v sendSingle: transmit error: %s\n", xmem.Id(), fmt.Sprint(err))
			if isSeriousError(err) {

				xmem.repairConn(xmem.memClient)
			}
			return err
		}
	}
	return nil
}

//TODO: who will release the pool? maybe it should be replication manager
//
func (xmem *XmemNozzle) initializeConnection() (err error) {
	xmem.Logger().Debugf("xmem.config= %v", xmem.config.connectStr)
	xmem.Logger().Debugf("poolName=%v", xmem.getPoolName(xmem.config))
	pool, err := base.ConnPoolMgr().GetOrCreatePool(xmem.getPoolName(xmem.config), xmem.config.connectStr, xmem.config.bucketName, xmem.config.bucketName, xmem.config.password, base.DefaultConnectionSize)
	xmem.memClient, err = pool.Get()
	if err != nil {
		return
	}

	xmem.client_for_getMeta, err = pool.Get()
	if err != nil {
		return
	}
	return err
}

func (xmem *XmemNozzle) getPoolName(config xmemConfig) string {
	return "Couch_Xmem_" + config.connectStr + base.KeyPartsDelimiter + config.bucketName
}

func (xmem *XmemNozzle) initNewBatch() {
	xmem.Logger().Debug("init a new batch")
	xmem.batch = newXmemBatch(xmem.config.maxCount, xmem.config.maxSize, xmem.config.batchExpirationTime, xmem.Logger())
}

func (xmem *XmemNozzle) initialize(settings map[string]interface{}) error {
	err := xmem.config.initializeConfig(settings)
	if err != nil {
		return err
	}
	xmem.dataChan = make(chan *base.WrappedMCRequest, xmem.config.maxCount*100)
	xmem.bytes_in_dataChan = 0
	xmem.batches_ready = make(chan *xmemBatch, 100)

	//enable send
	//	xmem.send_allow_ch <- true

	//init a new batch
	xmem.initNewBatch()

	xmem.batch_move_ch = make(chan bool, 1)
	xmem.batch_move_ch <- true

	xmem.buf = newReqBuffer(uint16(xmem.config.maxCount*100), uint16(float64(xmem.config.maxCount)*0.2), xmem.Logger())

	xmem.receiver_finch = make(chan bool, 1)
	xmem.checker_finch = make(chan bool, 1)

	xmem.Logger().Debug("About to start initializing connection")
	if err == nil {
		err = xmem.initializeConnection()
	}

	xmem.Logger().Debug("Initialization is done")

	return err
}

func (xmem *XmemNozzle) repairConn(client *mcc.Client) {
	xmem.lock_connection.Lock()
	defer xmem.lock_connection.Unlock()

	if client == xmem.memClient {
		xmem.Logger().Infof("%v connection is broken, try to repair...\n", xmem.Id())
		pool, err := base.ConnPoolMgr().GetOrCreatePool(xmem.getPoolName(xmem.config), xmem.config.connectStr, xmem.config.bucketName, xmem.config.bucketName, xmem.config.password, base.DefaultConnectionSize)
		xmem.memClient.Close()
		if err == nil {
			xmem.memClient, err = pool.Get()
		}
		if err == nil {
			xmem.Logger().Infof("%v - The connection is repaired\n", xmem.Id())
			size := xmem.buf.bufferSize()
			for i := 0; i < int(size); i++ {
				xmem.buf.modSlot(uint16(i), xmem.resend)
			}
			xmem.Logger().Infof("%v - The unresponded items are resent\n", xmem.Id())
		} else {
			xmem.Logger().Infof("%v - Connection repair failed\n", xmem.Id())
			xmem.handleGeneralError(err)
		}
	}
}

func (xmem *XmemNozzle) receiveResponse(finch chan bool, waitGrp *sync.WaitGroup) {
	defer waitGrp.Done()

	var count = 0
	var read_retry = 0
	for {
		select {
		case <-finch:
			goto done
		default:
			conn := xmem.memClient.Hijack()
			conn.(*net.TCPConn).SetReadDeadline(time.Now().Add(300 * time.Millisecond))
			response, err := xmem.memClient.Receive()
			count++

			if err == io.EOF {
				xmem.Logger().Errorf("%v Quit receiveResponse. err=%v\n", xmem.Id(), err)
				goto done
			} else if err != nil && isNetError(err) {
				if isSeriousError(err) {
					xmem.repairConn(xmem.memClient)
					read_retry =0
				} else {
					//retry in 100 millisecond
					if read_retry > 10 && (int(xmem.buf.bufferSize())-len(xmem.buf.empty_slots_pos)) > 0 {
						xmem.repairConn(xmem.memClient)
						read_retry = 0
					}
					xmem.Logger().Debugf("%v read response timed out\n", xmem.Id())
					time.Sleep(100 * time.Millisecond)
					read_retry++
				}
			} else if err != nil && isRecoverableMCError(response.Status) {
				//reset read_retry
				read_retry = 0
				
				pos := xmem.getPosFromOpaque(response.Opaque)
				xmem.Logger().Infof("%v pos=%d, Received error = %v in response, err = %v, response=%v\n", xmem.Id(), pos, response.Status.String(), err, response.Bytes())
				_, err = xmem.buf.modSlot(pos, xmem.resend)
			} else if err != nil && mc.IsFatal(err) {
				
				if response.Status == 0x08 {
					//PROTOCOL_BINARY_RESPONSE_NO_BUCKET
					//bucket must be recreated, drop the connection pool
					poolName := xmem.getPoolName(xmem.config)
					pool := base.ConnPoolMgr().GetPool(poolName)
					if pool != nil {
						pool.ReleaseConnections()
					}

				}
				xmem.handleGeneralError(err)
				return
			} else {
				//rest read_retry
				read_retry =0
				
				//raiseEvent
				pos := xmem.getPosFromOpaque(response.Opaque)
				seqno, req, _ := xmem.buf.slot(pos)
				if req != nil && req.Opaque == response.Opaque {
					xmem.Logger().Debugf("%v Got the response, response.Opaque=%v, req.Opaque=%v, seqno=%v\n", xmem.Id(), response.Opaque, req.Opaque, seqno)

					if response.Status != mc.SUCCESS {
						xmem.Logger().Debugf("*****%v-%v Got the response, response.Status=%v for doc %s*******\n", xmem.config.bucketName, xmem.Id(), response.Status, req.Key)
						xmem.Logger().Debugf("req.key=%s, req.Extras=%v, req.Cas=%v, req=%v\n", req.Key, req.Extras, req.Cas, req)
					}
					additionalInfo := make(map[string]interface{})
					additionalInfo[XMEM_EVENT_ADDI_SEQNO] = seqno
					additionalInfo[XMEM_EVENT_ADDI_OPT_REPD] = xmem.optimisticRep(req)
					xmem.RaiseEvent(common.DataSent, req, xmem, nil, additionalInfo)
					//empty the slot in the buffer
					if xmem.buf.evictSlot(pos) != nil {
						xmem.Logger().Errorf("Failed to evict slot %d\n", pos)
					}
				} else {
					if req != nil {
						xmem.Logger().Debugf("%v Got the response, response.Opaque=%v, req.Opaque=%v\n", xmem.Id(), response.Opaque, req.Opaque)
					} else {
						xmem.Logger().Debugf("%v Got the response, pos=%v, req in that pos is nil\n", xmem.Id(), pos)
					}
				}
			}
		}
	}

done:
	xmem.Logger().Infof("%v receiveResponse exits\n", xmem.Id())
}

func isNetError(err error) bool {
	_, ok := err.(*net.OpError)
	return ok
}

func isNetTimeoutError(err error) bool {
	netError, ok := err.(*net.OpError)
	return ok && netError.Timeout()
}

func isNetTemporaryError(err error) bool {
	netError, ok := err.(*net.OpError)
	return ok && netError.Temporary()
}

func isSeriousError(err error) bool {
	netError, ok := err.(*net.OpError)
	return err.Error() == "use of closed network connection" || (ok && (!netError.Temporary() && !netError.Timeout()))
}

func isRecoverableMCError(resp_status mc.Status) bool {
	switch resp_status {
	case mc.TMPFAIL:
		return true
	default:
		return false
	}
}

func (xmem *XmemNozzle) check(finch chan bool, waitGrp *sync.WaitGroup) {
	defer waitGrp.Done()

	var count uint64
	ticker := time.Tick(xmem.config.respTimeout)
	for {
		select {
		case <-finch:
			goto done
		case <-ticker:

			select {
			case <-xmem.batch.expire_ch:
				xmem.Logger().Infof("%v batch expired, moving it to ready queue\n", xmem.Id())
				xmem.batchReady()
			default:
			}
			count++
			xmem.Logger().Debugf("%v open=%v checking..., %v item unsent, received %v items, sent %v items, %v items waiting for response, %v batches ready, current batch timeout at %v, current batch expire_ch size is %v\n", xmem.Id(), xmem.IsOpen(), len(xmem.dataChan), xmem.counter_received, xmem.counter_sent, int(xmem.buf.bufferSize())-len(xmem.buf.empty_slots_pos), len(xmem.batches_ready), xmem.batch.start_time.Add(xmem.batch.expiring_duration), len(xmem.batch.expire_ch))
			size := xmem.buf.bufferSize()
			timeoutCheckFunc := xmem.checkTimeout
			for i := 0; i < int(size); i++ {
				_, err := xmem.buf.modSlot(uint16(i), timeoutCheckFunc)
				if err != nil {
					xmem.Logger().Errorf("%v Failed to check timeout this round, try later - %v\n", xmem.Id(), err)
					break
				}
			}

		}
	}
done:
	xmem.Logger().Info("Xmem checking routine exits")
}

func (xmem *XmemNozzle) checkTimeout(req *bufferedMCRequest, pos uint16) (bool, error) {
	if time.Since(req.sent_time) > xmem.timeoutDuration(req.num_of_retry) {
		modified, err := xmem.resend(req, pos)
		return modified, err
	}
	return false, nil
}

func (xmem *XmemNozzle) timeoutDuration(numofRetry int) time.Duration {
	duration := xmem.config.respTimeout
	for i := 1; i <= numofRetry; i++ {
		duration *= 2
		if duration > xmem.config.maxRetryInterval {
			duration = xmem.config.maxRetryInterval
			break
		}
	}
	return duration
}

func (xmem *XmemNozzle) resend(req *bufferedMCRequest, pos uint16) (bool, error) {
	xmem.Logger().Debugf("%v Retry sending ....", xmem.Id())
	err := xmem.sendSingleSetMeta(false, req.req, pos)

	if err != nil {
		req.err = err
	} else {
		req.num_of_retry = req.num_of_retry + 1
	}
	return false, err
}

func (xmem *XmemNozzle) adjustRequest(req *base.WrappedMCRequest, index uint16) {
	mc_req := req.Req
	mc_req.Opcode = xmem.encodeOpCode(mc_req.Opcode)
	mc_req.Cas = 0
	mc_req.Opaque = xmem.getOpaque(index, xmem.buf.sequences[int(index)])
}

func (xmem *XmemNozzle) getOpaque(index, sequence uint16) uint32 {
	result := uint32(sequence)<<16 + uint32(index)
	xmem.Logger().Debugf("uint32(sequence)<<16 = %v", uint32(sequence)<<16)
	xmem.Logger().Debugf("index=%x, sequence=%x, opaque=%x\n", index, sequence, result)
	return result
}

func (xmem *XmemNozzle) getPosFromOpaque(opaque uint32) uint16 {
	result := uint16(0x0000FFFF & opaque)
	xmem.Logger().Debugf("opaque=%x, index=%v\n", opaque, result)
	return result
}

func (xmem *XmemNozzle) encodeOpCode(code mc.CommandCode) mc.CommandCode {
	if code == mc.UPR_MUTATION || code == mc.TAP_MUTATION {
		return SET_WITH_META
	} else if code == mc.TAP_DELETE || code == mc.UPR_DELETION {
		return DELETE_WITH_META
	}
	return code
}

func (xmem *XmemNozzle) StatusSummary() string {
	return fmt.Sprintf("Xmem %v received %v items, sent %v items", xmem.Id(), xmem.counter_received, xmem.counter_sent)
}

func (xmem *XmemNozzle) handleGeneralError(err error) {
	if xmem.handle_error {
		xmem.Logger().Errorf("Raise error condition %v\n", err)
		otherInfo := utils.WrapError(err)
		xmem.RaiseEvent(common.ErrorEncountered, nil, xmem, nil, otherInfo)
	} else {
		xmem.Logger().Debugf("%v in shutdown process, err=%v is ignored\n", xmem.Id(), err)
	}
}

func (xmem *XmemNozzle) optimisticRep(req *mc.MCRequest) bool {
	if req != nil {
		return req.Size() < xmem.config.optiRepThreshold
	}
	return true
}
