package parts

import (
	"encoding/binary"
	"errors"
	"fmt"
	common "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	part "github.com/Xiaomei-Zhang/couchbase_goxdcr/part"
	log "github.com/Xiaomei-Zhang/couchbase_goxdcr/util"
	base "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/base"
	gen_server "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/gen_server"
	utils "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/utils"
	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	"io"
	"math/rand"
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
	XMEM_SETTING_VBID       = "vbid"
	XMEM_SETTING_BATCHCOUNT = "batchCount"
	XMEM_SETTING_BATCHSIZE  = "batchSize"
	XMEM_SETTING_MODE       = "mode"
	XMEM_SETTING_NUMOFRETRY = "max_retry"
	XMEM_SETTING_TIMEOUT    = "timeout"

	//default configuration
	default_batchcount      int           = 500
	default_batchsize       int           = 2048
	default_mode            XMEM_MODE     = Batch_XMEM
	default_numofretry      int           = 3
	default_timeout         time.Duration = 100 * time.Millisecond
	default_dataChannelSize               = 200
)

const (
	SET_WITH_META    = mc.CommandCode(0xa2)
	DELETE_WITH_META = mc.CommandCode(0xa8)
)

var xmem_setting_defs base.SettingDefinitions = base.SettingDefinitions{XMEM_SETTING_BATCHCOUNT: base.NewSettingDef(reflect.TypeOf((*int)(nil)), false),
	XMEM_SETTING_BATCHSIZE:  base.NewSettingDef(reflect.TypeOf((*int)(nil)), false),
	XMEM_SETTING_MODE:       base.NewSettingDef(reflect.TypeOf((*XMEM_MODE)(nil)), false),
	XMEM_SETTING_NUMOFRETRY: base.NewSettingDef(reflect.TypeOf((*int)(nil)), false),
	XMEM_SETTING_TIMEOUT:    base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false)}

var logger_xmem *log.CommonLogger = log.NewLogger("XmemNozzle", log.LogLevelDebug)

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
	slots           []chan *bufferedMCRequest /*slots to store the data*/
	empty_slots_pos chan int                  /*empty slot pos in the buffer*/
	size            int                       /*the size of the buffer*/
	notifych        chan bool                 /*notify channel is set when the buffer is empty*/
}

func newReqBuffer(size int, notifychan chan bool) *requestBuffer {
	logger_xmem.Debugf("Create a new request buffer of size %d\n", size)
	buf := &requestBuffer{
		make([]chan *bufferedMCRequest, size, size),
		make(chan int, size),
		size,
		notifychan}

	//initialize the slots
	for i := 0; i < size; i++ {
		slotch := make(chan *bufferedMCRequest, 1)
		slotch <- nil /* nil indicate the slot is an empty slot*/
		buf.slots[i] = slotch
	}

	logger_xmem.Debug("Slots is initialized")

	//initialize the empty_slots_pos
	buf.initializeEmptySlotPos()

	logger_xmem.Debugf("new request buffer of size %d is created\n", size)
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
		logger_xmem.Error("Invalid slot index")
		err = errors.New("Invalid slot index")
	}
	return
}

//slot allow caller to get hold of the content in the slot without locking the slot
//@pos - the position of the slot
func (buf *requestBuffer) slot(pos int) (*mc.MCRequest, error) {
	logger_xmem.Debugf("Getting the content in slot %d\n", pos)

	err := buf.validatePos(pos)
	if err != nil {
		return nil, err
	}

	reqch := buf.slots[pos]

	select {
	case req := <-reqch:
		reqch <- req
		if req == nil {
			return nil, nil
		} else {
			return req.req, nil
		}
	default:
		logger_xmem.Debug("Somebody is holding it")
		return nil, errors.New("Somebody is holding it")
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

	reqch := buf.slots[pos]

	var modified bool

	select {
	case req := <-reqch:
		defer func() {
			reqch <- req
		}()

		if req != nil {
			modified = modFunc(req, pos)
		} else {
			modified = false
		}
	default:
		logger_xmem.Debugf("Can't modified this slot, somebody is holding it.")
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

	reqch := buf.slots[pos]

	req := <-reqch
	defer func() {
		reqch <- nil
	}()

	if req != nil {
		select {
		case buf.empty_slots_pos <- pos:

		default:
			panic(fmt.Sprintf("Invalid state, the empty slots pos is full. len=%d, pos=%d", len(buf.empty_slots_pos), pos))
		}

		if len(buf.empty_slots_pos) == buf.size {
			if buf.notifych != nil {
				buf.notifych <- true
				logger_xmem.Debug("buffer is empty, notify")
			} else {
				logger_xmem.Debug("buffer is empty, no notify channel is specified though")
			}
		}
	}
	return nil

}

//availableSlotIndex returns a position number of an empty slot
func (buf *requestBuffer) reserveSlot() (error, int, int) {
	logger_xmem.Debugf("slots chan length=%d\n", len(buf.empty_slots_pos))
	index := <-buf.empty_slots_pos

	reqch := buf.slots[index]
	var reservation_num int

	//non blocking
	select {
	case req := <-reqch:
		defer func() {
			reqch <- req
		}()
		//generate a random number
		reservation_num = rand.Int()
		req = newBufferedMCRequest(nil, reservation_num)
	default:
		//this index doesn't work
		panic("This slot is supposed to be empty, but somebody hold it.")
	}
	return nil, index, reservation_num
}

func (buf *requestBuffer) cancelReservation(index int, reservation_num int) error {

	err := buf.validatePos(index)
	if err == nil {
		buf.empty_slots_pos <- index

		reqch := buf.slots[index]
		var reservation_num int

		//non blocking

		select {
		case req := <-reqch:
			defer func() {
				reqch <- req
			}()

			if req.reservation == reservation_num {
				req = nil
			} else {
				err = errors.New("Cancel reservation failed, reservation number doesn't match")
			}
		default:
			//this index doesn't work
			panic("This slot is supposed to be empty, but somebody hold it")
		}
	}
	return err
}

func (buf *requestBuffer) enSlot(pos int, req *mc.MCRequest, reservationNum int) error {
	logger_xmem.Debugf("enSlot: pos=%d\n", pos)

	err := buf.validatePos(pos)
	if err != nil {
		return err
	}

	reqch := buf.slots[pos]
	r := <-reqch
	defer func() {
		reqch <- r
	}()

	if r == nil || r.reservation != reservationNum {
		logger_xmem.Errorf("Can't enSlot %d, doesn't have the reservation", pos)
		return errors.New(fmt.Sprintf("Can't enSlot %d, doesn't have the reservation", pos))
	}

	r.req = req

	logger_xmem.Debugf("slot %d is occupied\n", pos)
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
	batchtimeout time.Duration
	maxRetry     int
	mode         XMEM_MODE
	connectStr   string
	bucketName   string
	password     string
}

func newConfig() xmemConfig {
	return xmemConfig{maxCount: default_batchcount,
		maxSize:      default_batchsize,
		batchtimeout: default_timeout,
		maxRetry:     default_numofretry,
		mode:         default_mode,
		connectStr:   "",
		bucketName:   "",
		password:     "",
	}

}

func (config *xmemConfig) initializeConfig(settings map[string]interface{}) error {
	err := utils.ValidateSettings(xmem_setting_defs, settings)
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
	return err
}

/************************************
/* struct xmemBatch
*************************************/
type xmemBatch struct {
	lock           sync.RWMutex
	curCount       int
	curSize        int
	capacity_count int
	capacity_size  int
}

func newXmemBatch(cap_count int, cap_size int) *xmemBatch {
	return &xmemBatch{sync.RWMutex{}, 0, 0, cap_count, cap_size}
}

func (b *xmemBatch) accumuBatch(size int) bool {
	b.lock.Lock()
	defer b.lock.Unlock()
	var ret bool = true
	if b.curCount < b.capacity_count && b.curSize < b.capacity_size {
		b.curCount++
		b.curSize += size
	}
	if b.curCount < b.capacity_count && b.curSize < b.capacity_size {
		ret = false
	}
	logger_xmem.Debugf("The current batch count=%d, size=%d, the batch ready=%v, capacity_count=%d\n", b.curCount, b.curSize, ret, b.capacity_count)
	return ret
}

func (b *xmemBatch) count() int {
	b.lock.RLock()
	defer b.lock.RUnlock()
	return b.curCount
}

func (b *xmemBatch) size() int {
	b.lock.RLock()
	defer b.lock.RUnlock()
	return b.curSize

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

	//control channel
	sendNowCh chan bool

	childrenWaitGrp sync.WaitGroup

	//buffer for the sent, but not yet confirmed data
	buf *requestBuffer

	receiver_finch chan bool
	checker_finch  chan bool
	batch_done     chan bool
}

func NewXmemNozzle(id string, connectString string, bucketName string, password string) *XmemNozzle {
	var msg_callback_func gen_server.Msg_Callback_Func
	var behavior_callback_func gen_server.Behavior_Callback_Func
	var exit_callback_func gen_server.Exit_Callback_Func
	var isStarted_callback_func part.IsStarted_Callback_Func

	server := gen_server.NewGenServer(&msg_callback_func,
		&behavior_callback_func, &exit_callback_func)
	isStarted_callback_func = server.IsStarted
	part := part.NewAbstractPart(id, &isStarted_callback_func)
	xmem := &XmemNozzle{server, /*gen_server.GenServer*/
		part,           /*part.AbstractPart*/
		true,           /*bOpen	bool*/
		sync.RWMutex{}, /*lock_bOpen	sync.RWMutex*/
		nil,            /*dataChan*/
		nil,            /*memClient*/
		//		nil,              /*connPool*/
		newConfig(),                /*config	xmemConfig*/
		make(chan *xmemBatch, 100), /*batches_ready chan *xmemBatch*/
		nil,                /*batch		  *xmemBatch*/
		make(chan bool, 1), /*sendNowChan chan bool*/
		sync.WaitGroup{},   /*childrenWaitGrp sync.WaitGroup*/
		nil,                /*buf	requestBuffer*/
		make(chan bool),    /*receiver_finch chan bool*/
		make(chan bool),    /*checker_finch chan bool*/
		make(chan bool, 1) /*batch_done chan bool*/}
		
	xmem.config.connectStr = connectString
	xmem.config.bucketName = bucketName
	xmem.config.password = password
	
	msg_callback_func = nil
	behavior_callback_func = xmem.processData
	exit_callback_func = xmem.onExit
	return xmem

}

func (xmem *XmemNozzle) Open() error {
	xmem.lock_bOpen.Lock()
	defer xmem.lock_bOpen.Unlock()
	if !xmem.bOpen {
		xmem.bOpen = true
	}
	return nil
}

func (xmem *XmemNozzle) Close() error {
	xmem.lock_bOpen.Lock()
	defer xmem.lock_bOpen.Unlock()
	if xmem.bOpen {
		xmem.bOpen = false
	}
	return nil
}

func (xmem *XmemNozzle) Start(settings map[string]interface{}) error {
	logger_xmem.Info("Xmem starting ....")
	err := xmem.initialize(settings)
	logger_xmem.Info("....Finish initializing....")
	if err == nil {
		xmem.childrenWaitGrp.Add(1)
		go xmem.receiveResponse(xmem.memClient, xmem.receiver_finch, &xmem.childrenWaitGrp)

		xmem.childrenWaitGrp.Add(1)
		go xmem.check(xmem.checker_finch, &xmem.childrenWaitGrp)
	}
	if err == nil {
		err = xmem.Start_server()
	}

	logger_xmem.Info("Xmem nozzle is started")
	return err
}

func (xmem *XmemNozzle) getReadyToShutdown() {
	logger_xmem.Debug("Waiting for data is drained")

	for {
		if len(xmem.dataChan) == 0 && len(xmem.batches_ready) == 0 {
			logger_xmem.Debug("Ready to stop")
			return
		} else if len(xmem.dataChan) == 0 && xmem.batch.count() > 0 {
			xmem.batchReady()
			close(xmem.batches_ready)
		} else {
			logger_xmem.Debugf("%d in data channel, %d batches ready\n", len(xmem.dataChan), len(xmem.batches_ready))
		}
	}

}

func (xmem *XmemNozzle) Stop() error {
	logger_xmem.Info("Stop XmemNozzle")
	xmem.getReadyToShutdown()

	err := xmem.Stop_server()

	//cleanup
	pool := base.ConnPoolMgr().GetPool(xmem.getPoolName(xmem.config.connectStr))
	if pool != nil {
		pool.Release(xmem.memClient)
	}

	xmem.memClient = nil

	return err
}

func (xmem *XmemNozzle) IsOpen() bool {
	xmem.lock_bOpen.RLock()
	defer xmem.lock_bOpen.RUnlock()
	
	ret := xmem.bOpen
	return ret
}

func (xmem *XmemNozzle) batchReady() {
	//move the batch to ready batches channel
	if xmem.batch.count() > 0 {
		logger_xmem.Infof("move the batch (count=%d) ready queue\n", xmem.batch.count())
		xmem.batches_ready <- xmem.batch
		logger_xmem.Debugf("There are %d batches in ready queue\n", len(xmem.batches_ready))
		xmem.initNewBatch()
	} else {
		panic("Nothing in the batch")
		logger_xmem.Debug("Nothing in the batch")
	}
}
func (xmem *XmemNozzle) Receive(data interface{}) error {
	logger_xmem.Debugf("data key=%v is received", data.(*mc.MCRequest).Key)
	logger_xmem.Debugf("data channel len is %d\n", len(xmem.dataChan))
	xmem.dataChan <- data.(*mc.MCRequest)

	//accumulate the batchCount and batchSize
	if xmem.batch.accumuBatch(data.(*mc.MCRequest).Size()) {
		xmem.batchReady()
	} else {
		//not enough for a batch, but the xmem.sendNowCh is signaled
		select {
		case <-xmem.sendNowCh:
			logger_xmem.Debug("Need to send now")
			xmem.batchReady()
		default:
		}
	}
	//raise DataReceived event
	xmem.RaiseEvent(common.DataReceived, data.(*mc.MCRequest), xmem, nil, nil)
	return nil
}

func (xmem *XmemNozzle) processData() {
	if xmem.IsOpen() {
		xmem.send()
	} else {
		logger_xmem.Debug("The nozzle is closed")
	}
}

func (xmem *XmemNozzle) onExit() {
	xmem.receiver_finch <- true
	xmem.checker_finch <- true
	xmem.childrenWaitGrp.Wait()
}

func (xmem *XmemNozzle) batchSendWithRetry(batch *xmemBatch, conn io.ReadWriteCloser, numOfRetry int) error {
	var err error
	count := batch.count()
	var itemIndexMap map[int]*mc.MCRequest = make(map[int]*mc.MCRequest)
	var indexReservMap map[int]int = make(map[int]int)
	var data []byte = make([]byte, 0, batch.curSize)
	for i := 0; i < count; i++ {
		select {
		case item := <-xmem.dataChan:
			//blocking
			err, index, reserv_num := xmem.buf.reserveSlot()
			if err != nil {
				return err
			}

			xmem.adjustRequest(item, index)
			item_byte := item.Bytes()
			data = append(data, item_byte...)
			itemIndexMap[index] = item
			indexReservMap[index] = reserv_num
		default:
			logger_xmem.Errorf("Invalid state - expected %d items in the batch; but there are not that much in the data channel", count-i)
		}
	}

	if conn == nil {
		panic("lost connection")
	}

	var bytes int
	for i := 0; i < numOfRetry; i++ {
		bytes, err = conn.Write(data)
		if err == nil {
			logger_xmem.Debugf("data=%v\n", data)
			logger_xmem.Debugf("%d bytes are sent\n", bytes)

			for ind, it := range itemIndexMap {
				err = xmem.buf.enSlot(ind, it, indexReservMap[ind])
			}

			break
		}
	}
	//log the data
	return err
}

func (xmem *XmemNozzle) send() error {
	var err error

	//get the batch to process
	logger_xmem.Debugf("Send: There are %d batches ready\n", len(xmem.batches_ready))
	select {
	case batch := <-xmem.batches_ready:
		count := batch.count()

		logger_xmem.Infof("Send batch count=%d\n", count)
		if count == 1 {
			select {
			case item := <-xmem.dataChan:
				//blocking
				err, index, reserv_num := xmem.buf.reserveSlot()
				if err != nil {
					logger_xmem.Errorf("Failed to reserve a slot")
					return err
				}
				err = xmem.sendSingleWithRetry(item, xmem.config.maxRetry, index)
				if err != nil {
					xmem.buf.cancelReservation(index, reserv_num)
					return err
				}

				//buffer it for now until the receipt is confirmed
				logger_xmem.Debugf("And sent item to slot=%d\n", index)
				err = xmem.buf.enSlot(index, item, reserv_num)
			default:
				logger_xmem.Errorf("Invalid state - expected %d items in the batch; but there are not that much in the data channel", 1)
			}
		} else {

			//get the raw connection
			conn := xmem.memClient.Hijack()
			defer func() {
				xmem.memClient, err = mcc.Wrap(conn)

				if err != nil || xmem.memClient == nil {
					//failed to recycle the connection
					//reinitialize the connection
					err = xmem.initializeConnection()
					if err != nil {
						panic(fmt.Sprintf("Lost connection. err=%v", err))
					}
				}

			}()

			//batch send
			err = xmem.batchSendWithRetry(batch, conn, xmem.config.maxRetry)

		}
		if err == nil && xmem.config.mode == Batch_XMEM {
			logger_xmem.Debug("Waiting for the confirmation of the batch")
			<-xmem.batch_done
			logger_xmem.Debug("Batch is confirmed")

		}
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
	logger_xmem.Debugf("key=%v\n", item.Key)
	logger_xmem.Debugf("opcode=%v\n", item.Opcode)

	err := xmem.memClient.Transmit(item)
	if err != nil {
		logger_xmem.Errorf("SendBatch: transmit error: %s\n", fmt.Sprint(err))
		return err
	}
	return nil
}

//TODO: who will release the pool? maybe it should be replication manager
//
func (xmem *XmemNozzle) initializeConnection() (err error) {
	logger_xmem.Debugf("xmem.config= %v", xmem.config.connectStr)
	logger_xmem.Debugf("poolName=%v", xmem.getPoolName(xmem.config.connectStr))
	pool, err := base.ConnPoolMgr().GetOrCreatePool(xmem.getPoolName(xmem.config.connectStr), xmem.config.connectStr, xmem.config.bucketName, xmem.config.password, 5)
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
		xmem.batch = newXmemBatch(xmem.config.maxCount, xmem.config.maxSize)
	} else {
		xmem.batch = newXmemBatch(1, -1)
	}
}

func (xmem *XmemNozzle) initialize(settings map[string]interface{}) error {
	err := xmem.config.initializeConfig(settings)
	xmem.dataChan = make(chan *mc.MCRequest, default_dataChannelSize)
	xmem.sendNowCh = make(chan bool, 1)

	//init a new batch
	xmem.initNewBatch()

	//initialize buffer
	if xmem.config.mode == Batch_XMEM {
		xmem.buf = newReqBuffer(xmem.config.maxCount*2, xmem.batch_done)
	} else {
		xmem.buf = newReqBuffer(xmem.config.maxCount*2, nil)
	}

	xmem.receiver_finch = make(chan bool, 1)
	xmem.checker_finch = make(chan bool, 1)

	logger_xmem.Debug("About to start initializing connection")
	if err == nil {
		err = xmem.initializeConnection()
	}

	logger_xmem.Debug("Initialization is done")

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
						logger_xmem.Debugf("pos=%d, Received error = %v in response, err = %v, response=%v\n", pos, response.Status.String(), err, response.Bytes())
						_, err = xmem.buf.modSlot(pos, xmem.resend)
					} else {
						//raiseEvent
						logger_xmem.Debugf("Received response....")
						req, _ := xmem.buf.slot(pos)
						xmem.RaiseEvent(common.DataSent, req, xmem, nil, nil)
						//empty the slot in the buffer
						if xmem.buf.evictSlot(pos) != nil {
							logger_xmem.Errorf("Failed to evict slot %d\n", pos)
						}
						logger_xmem.Debugf("data on slot=%d is received", pos)
					}
				}
			}
		}
	}

done:
	waitGrp.Done()
}

func isRealError(resp_status mc.Status) bool {
	switch resp_status {
	case mc.KEY_ENOENT, mc.KEY_EEXISTS, mc.NOT_STORED:
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
		time.Sleep(time.Millisecond * 10)
	}
done:
	waitGrp.Done()
}

func (xmem *XmemNozzle) checkTimeout(req *bufferedMCRequest, pos int) bool {
	if time.Since(req.sent_time) > xmem.config.batchtimeout {
		logger_xmem.Debug("Timeout, resend...")
		modified := xmem.resend(req, pos)
		return modified
	}
	return false
}

func (xmem *XmemNozzle) resend(req *bufferedMCRequest, pos int) bool {
	if req.num_of_retry < xmem.config.maxRetry-1 {
		logger_xmem.Debug("Retry sending ....")
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
		xmem.RaiseEvent(common.ErrorEncountered, req.req, xmem, nil, nil)
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
