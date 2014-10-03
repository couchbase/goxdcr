package parts

import (
	"encoding/binary"
	"errors"
	"fmt"
	common "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr/log"
	part "github.com/Xiaomei-Zhang/couchbase_goxdcr/part"
	base "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/base"
	gen_server "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/gen_server"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/utils"
	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	"io"
	"math/rand"
	"net"
	"reflect"
	"sync"
	"time"
)

type XMEM_MODE int

const (
	Batch_XMEM        XMEM_MODE = iota
	Asynchronous_XMEM XMEM_MODE = iota
)

//configuration settings for XmemNozzle
const (
	//configuration param names
	XMEM_SETTING_BATCHCOUNT            = "batch_count"
	XMEM_SETTING_BATCHSIZE             = "batch_size"
	XMEM_SETTING_MODE                  = "mode"
	XMEM_SETTING_NUMOFRETRY            = "max_retry"
	XMEM_SETTING_TIMEOUT               = "timeout"
	XMEM_SETTING_BATCH_EXPIRATION_TIME = "batch_expiration_time"

	//default configuration
	default_batchcount          int           = 500
	default_batchsize           int           = 2048
	default_mode                XMEM_MODE     = Batch_XMEM
	default_numofretry          int           = 6
	default_timeout             time.Duration = 100 * time.Millisecond
	default_dataChannelSize                   = 50000
	default_batchExpirationTime               = 100 * time.Millisecond
)

const (
	SET_WITH_META    = mc.CommandCode(0xa2)
	DELETE_WITH_META = mc.CommandCode(0xa8)
)

var xmem_setting_defs base.SettingDefinitions = base.SettingDefinitions{XMEM_SETTING_BATCHCOUNT: base.NewSettingDef(reflect.TypeOf((*int)(nil)), false),
	XMEM_SETTING_BATCHSIZE:             base.NewSettingDef(reflect.TypeOf((*int)(nil)), false),
	XMEM_SETTING_MODE:                  base.NewSettingDef(reflect.TypeOf((*XMEM_MODE)(nil)), false),
	XMEM_SETTING_NUMOFRETRY:            base.NewSettingDef(reflect.TypeOf((*int)(nil)), false),
	XMEM_SETTING_TIMEOUT:               base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	XMEM_SETTING_BATCH_EXPIRATION_TIME: base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false)}


/************************************
/* struct bufferedMCRequest
*************************************/

type bufferedMCRequest struct {
	req          *mc.MCRequest
	sent_time    time.Time
	num_of_retry int
	err          error
	reservation  int
}

func newBufferedMCRequest(request *mc.MCRequest, reservationNum int) *bufferedMCRequest {
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
	empty_slots_pos chan int             /*empty slot pos in the buffer*/
	size            int                  /*the size of the buffer*/
	notifych        chan bool            /*notify channel is set when the buffer is empty*/
	logger          *log.CommonLogger
}

func newReqBuffer(size int, notifychan chan bool, logger *log.CommonLogger) *requestBuffer {
	logger.Debugf("Create a new request buffer of size %d\n", size)
	buf := &requestBuffer{
		make([]*bufferedMCRequest, size, size),
		make(chan int, size),
		size,
		notifychan,
		logger}

	logger.Debug("Slots is initialized")

	//initialize the empty_slots_pos
	buf.initializeEmptySlotPos()

	logger.Debugf("new request buffer of size %d is created\n", size)
	return buf
}

func (buf *requestBuffer) initializeEmptySlotPos() error {
	for i := 0; i < buf.size; i++ {
		buf.empty_slots_pos <- i
	}

	return nil
}

func (buf *requestBuffer) validatePos(pos int) (err error) {
	err = nil
	if pos < 0 || pos >= len(buf.slots) {
		buf.logger.Error("Invalid slot index")
		err = errors.New("Invalid slot index")
	}
	return
}

//slot allow caller to get hold of the content in the slot without locking the slot
//@pos - the position of the slot
func (buf *requestBuffer) slot(pos int) (*mc.MCRequest, error) {
	buf.logger.Debugf("Getting the content in slot %d\n", pos)

	err := buf.validatePos(pos)
	if err != nil {
		return nil, err
	}

	req := buf.slots[pos]

	if req == nil {
		return nil, nil
	} else {
		return req.req, nil
	}

}

//modSlot allow caller to do book-keeping on the slot, like updating num_of_retry, err
//@pos - the position of the slot
//@modFunc - the callback function which is going to update the slot
func (buf *requestBuffer) modSlot(pos int, modFunc func(req *bufferedMCRequest, p int) bool) (bool, error) {
	var err error = nil
	err = buf.validatePos(pos)
	if err != nil {
		return false, err
	}

	req := buf.slots[pos]

	var modified bool

	if req != nil && req.req != nil {
		modified = modFunc(req, pos)
	} else {
		modified = false
	}
	return modified, err
}

//evictSlot allow caller to empty the slot
//@pos - the position of the slot
func (buf *requestBuffer) evictSlot(pos int) error {

	err := buf.validatePos(pos)
	if err != nil {
		return err
	}
	req := buf.slots[pos]
	buf.slots[pos] = nil

	if req != nil {
		select {
		case buf.empty_slots_pos <- pos:

		default:
			panic(fmt.Sprintf("Invalid state, the empty slots pos is full. len=%d, pos=%d", len(buf.empty_slots_pos), pos))
		}

		if len(buf.empty_slots_pos) == buf.size {
			if buf.notifych != nil {
				buf.notifych <- true
				buf.logger.Debug("buffer is empty, notify")
			} else {
				buf.logger.Debug("buffer is empty, no notify channel is specified though")
			}
		}
	}
	return nil

}

//availableSlotIndex returns a position number of an empty slot
func (buf *requestBuffer) reserveSlot() (error, int, int) {
	buf.logger.Debugf("slots chan length=%d\n", len(buf.empty_slots_pos))
	index := <-buf.empty_slots_pos

	var reservation_num int

	//non blocking
	//generate a random number
	reservation_num = rand.Int()
	req := newBufferedMCRequest(nil, reservation_num)
	buf.slots[index] = req
	return nil, index, reservation_num
}

func (buf *requestBuffer) cancelReservation(index int, reservation_num int) error {

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

func (buf *requestBuffer) enSlot(pos int, req *mc.MCRequest, reservationNum int) error {
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

func (buf *requestBuffer) bufferSize() int {
	return buf.size
}

/************************************
/* struct xmemConfig
*************************************/
type xmemConfig struct {
	maxCount int
	maxSize  int
	//the duration to wait for the batch-sending to finish
	batchtimeout        time.Duration
	batchExpirationTime time.Duration
	maxRetry            int
	mode                XMEM_MODE
	connectStr          string
	bucketName          string
	password            string
	logger              *log.CommonLogger
}

func newConfig(logger *log.CommonLogger) xmemConfig {
	return xmemConfig{maxCount: default_batchcount,
		maxSize:             default_batchsize,
		batchtimeout:        default_timeout,
		batchExpirationTime: default_batchExpirationTime,
		maxRetry:            default_numofretry,
		mode:                default_mode,
		connectStr:          "",
		bucketName:          "",
		password:            "",
	}

}

func (config *xmemConfig) initializeConfig(settings map[string]interface{}) error {
	err := utils.ValidateSettings(xmem_setting_defs, settings, config.logger)
	if val, ok := settings[XMEM_SETTING_BATCHSIZE]; ok {
		config.maxSize = val.(int)
	}
	if val, ok := settings[XMEM_SETTING_BATCHCOUNT]; ok {
		config.maxCount = val.(int)
	}
	if val, ok := settings[XMEM_SETTING_TIMEOUT]; ok {
		config.batchtimeout = val.(time.Duration)
	}
	if val, ok := settings[XMEM_SETTING_NUMOFRETRY]; ok {
		config.maxRetry = val.(int)
	}
	if val, ok := settings[XMEM_SETTING_MODE]; ok {
		config.mode = val.(XMEM_MODE)
	}
	if val, ok := settings[XMEM_SETTING_BATCH_EXPIRATION_TIME]; ok {
		config.batchExpirationTime = val.(time.Duration)
	}
	return err
}

/************************************
/* struct xmemBatch
*************************************/
type xmemBatch struct {
	curCount       int
	curSize        int
	capacity_count int
	capacity_size  int
	start_time     time.Time
	frozen         bool
	logger         *log.CommonLogger
}

func newXmemBatch(cap_count int, cap_size int, logger *log.CommonLogger) *xmemBatch {
	return &xmemBatch{
		curCount:       0,
		curSize:        0,
		capacity_count: cap_count,
		capacity_size:  cap_size,
		frozen:         false,
		logger:         logger}
}

//turn this batch to be a read only batch
func (b *xmemBatch) frozeBatch() {
	b.frozen = true

	b.logger.Debug("The batch is frozen")
}

func (b *xmemBatch) accumuBatch(size int) bool {
	var ret bool = true

	b.curCount++
	if b.curCount == 1 {
		b.start_time = time.Now()
	}

	b.curSize += size
	if b.curCount < b.capacity_count && b.curSize < b.capacity_size*1000 {
		ret = false
	}
	b.logger.Debugf("The current batch count=%d, size=%d, the batch ready=%v, capacity_count=%d\n", b.curCount, b.curSize, ret, b.capacity_count)
	return ret
}

func (b *xmemBatch) count() int {
	if b == nil {
		panic("lock becomes null")
	}
	return b.curCount
}

func (b *xmemBatch) size() int {
	return b.curSize

}

func (b *xmemBatch) isFrozen() bool {
	return b.frozen
}

/************************************
/* struct XmemNozzle
*************************************/
type XmemNozzle struct {

	//parent inheritance
	gen_server.GenServer
	part.AbstractPart

	bOpen      bool
	lock_bOpen sync.RWMutex

	//data channel to accept the incoming data
	dataChan chan *mc.MCRequest

	//memcached client connected to the target bucket
	memClient *mcc.Client

	//configurable parameter
	config xmemConfig

	//queue for ready batches
	batches_ready chan *xmemBatch

	//batch to be accumulated
	batch *xmemBatch

	//channel to signal if batch can be transitioned to batches_ready
	batch_move_ch chan bool

	//control channel
	sendNowCh chan bool

	childrenWaitGrp sync.WaitGroup

	//buffer for the sent, but not yet confirmed data
	buf *requestBuffer

	receiver_finch chan bool
	checker_finch  chan bool
	send_allow_ch  chan bool

	counter_sent     int
	counter_received int
}

func NewXmemNozzle(id string,
	connectString string,
	bucketName string,
	password string,
	logger_context *log.LoggerContext) *XmemNozzle {

	//callback functions from GenServer
	var msg_callback_func gen_server.Msg_Callback_Func
	var behavior_callback_func gen_server.Behavior_Callback_Func
	var exit_callback_func gen_server.Exit_Callback_Func
	var error_handler_func gen_server.Error_Handler_Func

	var isStarted_callback_func part.IsStarted_Callback_Func

	server := gen_server.NewGenServer(&msg_callback_func,
		&behavior_callback_func, &exit_callback_func, &error_handler_func, logger_context, "XmemNozzle")
	isStarted_callback_func = server.IsStarted
	part := part.NewAbstractPartWithLogger(id, &isStarted_callback_func, server.Logger())

	xmem := &XmemNozzle{GenServer: server, /*gen_server.GenServer*/
		AbstractPart:     part,                       /*part.AbstractPart*/
		bOpen:            true,                       /*bOpen	bool*/
		lock_bOpen:       sync.RWMutex{},             /*lock_bOpen	sync.RWMutex*/
		dataChan:         nil,                        /*dataChan*/
		memClient:        nil,                        /*memClient*/
		config:           newConfig(server.Logger()), /*config	xmemConfig*/
		batches_ready:    make(chan *xmemBatch, 100), /*batches_ready chan *xmemBatch*/
		batch:            nil,                        /*batch		  *xmemBatch*/
		batch_move_ch:    nil,
		sendNowCh:        make(chan bool, 1), /*sendNowCh chan bool*/
		childrenWaitGrp:  sync.WaitGroup{},   /*childrenWaitGrp sync.WaitGroup*/
		buf:              nil,                /*buf	requestBuffer*/
		receiver_finch:   make(chan bool),    /*receiver_finch chan bool*/
		checker_finch:    make(chan bool),    /*checker_finch chan bool*/
		send_allow_ch:    make(chan bool, 1), /*send_allow_ch chan bool*/
		counter_sent:     0,
		counter_received: 0}

	xmem.config.connectStr = connectString
	xmem.config.bucketName = bucketName
	xmem.config.password = password

	msg_callback_func = nil
	behavior_callback_func = xmem.processData
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
		go xmem.receiveResponse(xmem.memClient, xmem.receiver_finch, &xmem.childrenWaitGrp)

		xmem.childrenWaitGrp.Add(1)
		go xmem.check(xmem.checker_finch, &xmem.childrenWaitGrp)
	}
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
	xmem.getReadyToShutdown()

	conn := xmem.memClient.Hijack()
	xmem.memClient = nil
	conn.(*net.TCPConn).SetReadDeadline(time.Now())

	xmem.receiver_finch <- true
	xmem.checker_finch <- true

	xmem.Logger().Debugf("XmemNozzle %v processed %v items\n", xmem.Id(), xmem.counter_sent)
	err := xmem.Stop_server()

	xmem.childrenWaitGrp.Wait()
	conn.(*net.TCPConn).SetReadDeadline(time.Date(1, time.January, 0, 0, 0, 0, 0, time.UTC))
	xmem.Logger().Debugf("XmemNozzle %v is stopped\n", xmem.Id())
	return err
}

func (xmem *XmemNozzle) IsOpen() bool {
	//	xmem.lock_bOpen.RLock()
	//	defer xmem.lock_bOpen.RUnlock()

	ret := xmem.bOpen
	return ret
}

func (xmem *XmemNozzle) batchReady() {
	//move the batch to ready batches channel
	select {
	case <-xmem.batch_move_ch:
		if xmem.batch.count() > 0 {
			xmem.Logger().Infof("move the batch (count=%d) ready queue\n", xmem.batch.count())
			xmem.batch.frozeBatch()
			xmem.batches_ready <- xmem.batch

			xmem.Logger().Debugf("There are %d batches in ready queue\n", len(xmem.batches_ready))
			xmem.initNewBatch()
		}
	default:
	}
}

func (xmem *XmemNozzle) Receive(data interface{}) error {
	xmem.Logger().Errorf(" xmem %v data channel size is %v\n", xmem.Id(), len(xmem.dataChan))
	xmem.Logger().Debugf("data key=%v is received", data.(*mc.MCRequest).Key)
	xmem.Logger().Debugf("data channel len is %d\n", len(xmem.dataChan))

	request := data.(*mc.MCRequest)

	xmem.dataChan <- request

	xmem.counter_received++

	//accumulate the batchCount and batchSize
	if xmem.batch.accumuBatch(data.(*mc.MCRequest).Size()) {
		xmem.batchReady()
	} else {
		select {
		//not enough for a batch, but the xmem.sendNowCh is signaled
		case <-xmem.sendNowCh:
			xmem.Logger().Debug("Need to send now")
			xmem.batchReady()
		default:
		}
	}
	//raise DataReceived event
	xmem.RaiseEvent(common.DataReceived, data.(*mc.MCRequest), xmem, nil, nil)
	xmem.Logger().Debugf("Xmem %v received %v items\n", xmem.Id(), xmem.counter_received)

	return nil
}

func (xmem *XmemNozzle) processData() error {
	if xmem.IsOpen() {
		err := xmem.send()
		if err != nil {
			xmem.Logger().Error(err.Error())
			return err
		}
	} else {
		xmem.Logger().Debug("The nozzle is closed")
	}
	return nil
}

func (xmem *XmemNozzle) onExit() {
	//cleanup
	pool := base.ConnPoolMgr().GetPool(xmem.getPoolName(xmem.config.connectStr))
	if pool != nil {
		pool.Release(xmem.memClient)
	}

	xmem.memClient = nil
}

func (xmem *XmemNozzle) batchSendWithRetry(batch *xmemBatch, conn io.ReadWriteCloser, numOfRetry int) error {
	var err error
	count := batch.count()

	i := 0
	for {
		if i >= count {
			break
		}

		select {
		case item := <-xmem.dataChan:
			//blocking
			err, index, reserv_num := xmem.buf.reserveSlot()
			if err != nil {
				return err
			}
			i++
			xmem.adjustRequest(item, index)
			item_byte := item.Bytes()

			for i := 0; i < numOfRetry; i++ {
				_, err = conn.Write(item_byte)
				if err == nil {
					break
				}
			}

			if err == nil {
				err = xmem.buf.enSlot(index, item, reserv_num)
			}

			if err != nil {
				xmem.buf.cancelReservation(index, reserv_num)
			}
		default:
			panic(fmt.Sprintf("Invalid state - expected %d items in the batch; but there are not that much in the data channel", count-i))
		}

	}

	if conn == nil {
		panic("lost connection")
	}

	//log the data
	return err
}

func (xmem *XmemNozzle) send() error {
	var err error

	//get the batch to process
	select {
	case <-xmem.send_allow_ch:
		select {
		case batch, ok := <-xmem.batches_ready:
			xmem.Logger().Errorf("%v Send..., %v batches ready, %v items in queue\n", xmem.Id(), len(xmem.batches_ready), len(xmem.dataChan))
			if !ok {
				return nil
			}
			err = xmem.send_internal(batch)
		default:
			//didn't use the send allowed token, put it back
			select {
			case xmem.send_allow_ch <- true:
			default:
			}
			if xmem.isCurrentBatchExpiring() {
				xmem.batchReady()
			}
		}
	default:
		//		xmem.Logger().Errorf("xmem %v has %v not yet received\n", xmem.Id(), xmem.buf.bufferSize() - len(xmem.buf.empty_slots_pos))
	}

	return err
}

func (xmem *XmemNozzle) send_internal(batch *xmemBatch) error {
	var err error
	count := batch.count()

	xmem.Logger().Infof("Send batch count=%d\n", count)

	xmem.counter_sent = xmem.counter_sent + count
	xmem.Logger().Debugf("So far, xmem %v processed %d items", xmem.Id(), xmem.counter_sent)

	if count == 1 {
		select {
		case item := <-xmem.dataChan:
			//blocking
			err, index, reserv_num := xmem.buf.reserveSlot()
			if err != nil {
				xmem.Logger().Errorf("Failed to reserve a slot")
				return err
			}
			err = xmem.sendSingleWithRetry(item, xmem.config.maxRetry, index)
			if err != nil {
				xmem.Logger().Errorf("Failed to reserve a slot")
				xmem.buf.cancelReservation(index, reserv_num)
				return err
			}

			//buffer it for now until the receipt is confirmed
			xmem.Logger().Debugf("And sent item to slot=%d\n", index)
			err = xmem.buf.enSlot(index, item, reserv_num)
		default:
			xmem.Logger().Errorf("Invalid state - expected %d items in the batch; but there are not that much in the data channel", 1)
		}
	} else {

		//get the raw connection
		conn := xmem.memClient.Hijack()
		defer func() {
			xmem.memClient, err = mcc.Wrap(conn)

			if err != nil || xmem.memClient == nil {
				conn.Close()
				//failed to recycle the connection
				//reinitialize the connection
				err = xmem.initializeConnection()
				if err != nil {
					xmem.Logger().Errorf("failed to recycle the connection")
				}
				go xmem.receiveResponse(xmem.memClient, xmem.receiver_finch, &xmem.childrenWaitGrp)
			}
		}()

		//batch send
		err = xmem.batchSendWithRetry(batch, conn, xmem.config.maxRetry)
	}
	return err
}

func (xmem *XmemNozzle) sendSingleWithRetry(item *mc.MCRequest, numOfRetry int, index int) (err error) {
	for i := 0; i < numOfRetry; i++ {
		err = xmem.sendSingle(item, index)
		if err == nil {
			break
		}
	}
	return err
}

func (xmem *XmemNozzle) sendSingle(item *mc.MCRequest, index int) error {

	xmem.adjustRequest(item, index)
	xmem.Logger().Debugf("key=%v\n", item.Key)
	xmem.Logger().Debugf("opcode=%v\n", item.Opcode)

	if xmem.memClient == nil {
		panic("memClient is nil")
	}
	err := xmem.memClient.Transmit(item)
	if err != nil {
		xmem.Logger().Errorf("SendBatch: transmit error: %s\n", fmt.Sprint(err))
		return err
	}
	return nil
}

//TODO: who will release the pool? maybe it should be replication manager
//
func (xmem *XmemNozzle) initializeConnection() (err error) {
	xmem.Logger().Debugf("xmem.config= %v", xmem.config.connectStr)
	xmem.Logger().Debugf("poolName=%v", xmem.getPoolName(xmem.config.connectStr))
	pool, err := base.ConnPoolMgr().GetOrCreatePool(xmem.getPoolName(xmem.config.connectStr), xmem.config.connectStr, xmem.config.bucketName, xmem.config.password, base.DefaultConnectionSize)
	if err == nil {
		xmem.memClient, err = pool.Get()
	}
	return err
}

func (xmem *XmemNozzle) getPoolName(connectionStr string) string {
	return "Couch_Xmem_" + connectionStr
}

func (xmem *XmemNozzle) initNewBatch() {
	if xmem.config.mode == Batch_XMEM {
		xmem.batch = newXmemBatch(xmem.config.maxCount, xmem.config.maxSize, xmem.Logger())
	} else {
		xmem.batch = newXmemBatch(1, -1, xmem.Logger())
	}
	xmem.batch_move_ch = make(chan bool, 1)
	xmem.batch_move_ch <- true
}

func (xmem *XmemNozzle) initialize(settings map[string]interface{}) error {
	err := xmem.config.initializeConfig(settings)
	xmem.dataChan = make(chan *mc.MCRequest, xmem.config.maxCount*100)
	xmem.sendNowCh = make(chan bool, 1)

	//enable send
	xmem.send_allow_ch <- true

	//init a new batch
	xmem.initNewBatch()

	//initialize buffer
	if xmem.config.mode == Batch_XMEM {
		xmem.buf = newReqBuffer(xmem.config.maxCount*2, xmem.send_allow_ch, xmem.Logger())
	} else {
		//no send batch control
		close(xmem.send_allow_ch)
		xmem.buf = newReqBuffer(xmem.config.maxCount*2, nil, xmem.Logger())
	}

	xmem.receiver_finch = make(chan bool, 1)
	xmem.checker_finch = make(chan bool, 1)

	xmem.Logger().Debug("About to start initializing connection")
	if err == nil {
		err = xmem.initializeConnection()
	}

	xmem.Logger().Debug("Initialization is done")

	return err
}

func (xmem *XmemNozzle) receiveResponse(client *mcc.Client, finch chan bool, waitGrp *sync.WaitGroup) {

	for {
		select {
		case <-finch:
			{
				goto done
			}
		default:
			{
				response, err := client.Receive()
				if err != io.EOF {
					pos := int(response.Opaque)
					if err != nil && isRealError(response.Status) {
						if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
							xmem.Logger().Debugf("read time out, exiting...")
							goto done
						}
						xmem.Logger().Debugf("pos=%d, Received error = %v in response, err = %v, response=%v\n", pos, response.Status.String(), err, response.Bytes())
						_, err = xmem.buf.modSlot(pos, xmem.resend)
					} else {

						//raiseEvent
						xmem.Logger().Debugf("Received response....")
						req, _ := xmem.buf.slot(pos)
						xmem.RaiseEvent(common.DataSent, req, xmem, nil, nil)
						//empty the slot in the buffer
						if xmem.buf.evictSlot(pos) != nil {
							xmem.Logger().Errorf("Failed to evict slot %d\n", pos)
						}
						xmem.Logger().Debugf("data on slot=%d is received", pos)
					}
				} else {
					goto done
				}
			}
		}
	}

done:
	waitGrp.Done()
}

func isRealError(resp_status mc.Status) bool {
	switch resp_status {
	case mc.KEY_ENOENT, mc.KEY_EEXISTS, mc.NOT_STORED, mc.Status(0x87):
		return false
	default:
		return true
	}
}
func (xmem *XmemNozzle) check(finch chan bool, waitGrp *sync.WaitGroup) {
	for {
		select {
		case <-finch:
			goto done
		default:
			size := xmem.buf.bufferSize()
			timeoutCheckFunc := xmem.checkTimeout
			for i := 0; i < size; i++ {
				xmem.buf.modSlot(i, timeoutCheckFunc)
			}
		}
		time.Sleep(xmem.config.batchtimeout)
	}
done:
	xmem.Logger().Debug("Xmem checking routine exits")
	waitGrp.Done()
}

func (xmem *XmemNozzle) checkTimeout(req *bufferedMCRequest, pos int) bool {
	if time.Since(req.sent_time) > xmem.timeoutDuration(req.num_of_retry) {
		xmem.Logger().Debug("Timeout, resend...")
		modified := xmem.resend(req, pos)
		return modified
	}
	return false
}

func (xmem *XmemNozzle) timeoutDuration(numofRetry int) time.Duration {
	duration := 0 * time.Millisecond
	for i := 0; i <= numofRetry; i++ {
		duration = duration + xmem.config.batchtimeout
	}
	return duration
}

func (xmem *XmemNozzle) resend(req *bufferedMCRequest, pos int) bool {
	if req.num_of_retry < xmem.config.maxRetry-1 {
		xmem.Logger().Debug("Retry sending ....")
		err := xmem.sendSingle(req.req, pos)
		req.sent_time = time.Now()
		if err != nil {
			req.err = err
		} else {
			req.num_of_retry = req.num_of_retry + 1
		}
		return true
	} else {
		//raise error
		err := errors.New("Max number of retry has reached")
		otherInfo := utils.WrapError(err)
		xmem.RaiseEvent(common.ErrorEncountered, req.req, xmem, nil, otherInfo)
		xmem.buf.evictSlot(pos)
	}
	return false
}

func (xmem *XmemNozzle) adjustRequest(mc_req *mc.MCRequest, index int) {
	mc_req.Opcode = xmem.encodeOpCode(mc_req.Opcode)
	mc_req.Cas = 0
	mc_req.Opaque = uint32(index)
	mc_req.Extras = []byte{0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0}
	binary.BigEndian.PutUint64(mc_req.Extras, uint64(0)<<32|uint64(0))

}

func (xmem *XmemNozzle) encodeOpCode(code mc.CommandCode) mc.CommandCode {
	if code == mc.UPR_MUTATION || code == mc.TAP_MUTATION {
		return SET_WITH_META
	} else if code == mc.TAP_DELETE || code == mc.UPR_DELETION {
		return DELETE_WITH_META
	}
	return code
}

func (xmem *XmemNozzle) isCurrentBatchExpiring() bool {
	if !xmem.batch.isFrozen() && xmem.batch.count() >= 1 && time.Since(xmem.batch.start_time) > xmem.config.batchExpirationTime {
		xmem.Logger().Debugf("The current batch count=%v is expiring\n", xmem.batch.count())
		return true
	}
	return false

}

func (xmem *XmemNozzle) StatusSummary() string {
	return fmt.Sprintf("Xmem %v received %v items, sent %v items", xmem.Id(), xmem.counter_received, xmem.counter_sent)
}

func (xmem *XmemNozzle) handleGeneralError(err error) {
	otherInfo := utils.WrapError(err)
	xmem.RaiseEvent(common.ErrorEncountered, nil, xmem, nil, otherInfo)
}

