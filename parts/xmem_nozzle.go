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
	XMEM_SETTING_CONNECTSTR = "connectStr"
	XMEM_SETTING_BUCKETNAME = "bucketName"
	XMEM_SETTING_USERNAME   = "userName"
	XMEM_SETTING_PASSWORD   = "password"
	XMEM_SETTING_VBID       = "vbid"
	XMEM_SETTING_BATCHCOUNT = "batchCount"
	XMEM_SETTING_BATCHSIZE  = "batchSize"
	XMEM_SETTING_MODE       = "mode"
	XMEM_SETTING_NUMOFRETRY = "max_retry"
	XMEM_SETTING_TIMEOUT    = "timeout"

	//default configuration
	default_batchcount int           = 500
	default_batchsize  int           = 2048
	default_mode       XMEM_MODE     = Batch_XMEM
	default_numofretry int           = 3
	default_timeout    time.Duration = 100 * time.Millisecond
)

const (
	SET_WITH_META    = mc.CommandCode(0xa2)
	DELETE_WITH_META = mc.CommandCode(0xa8)
)

var xmem_setting_defs base.SettingDefinitions = base.SettingDefinitions{XMEM_SETTING_BATCHCOUNT: base.NewSettingDef(reflect.TypeOf((*int)(nil)), false),
	XMEM_SETTING_BATCHSIZE:  base.NewSettingDef(reflect.TypeOf((*int)(nil)), false),
	XMEM_SETTING_MODE:       base.NewSettingDef(reflect.TypeOf((*XMEM_MODE)(nil)), false),
	XMEM_SETTING_NUMOFRETRY: base.NewSettingDef(reflect.TypeOf((*int)(nil)), false),
	XMEM_SETTING_TIMEOUT:    base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	XMEM_SETTING_CONNECTSTR: base.NewSettingDef(reflect.TypeOf((*string)(nil)), true),
	XMEM_SETTING_BUCKETNAME: base.NewSettingDef(reflect.TypeOf((*string)(nil)), true),
	XMEM_SETTING_USERNAME:   base.NewSettingDef(reflect.TypeOf((*string)(nil)), true),
	XMEM_SETTING_PASSWORD:   base.NewSettingDef(reflect.TypeOf((*string)(nil)), true)}

var logger_xmem *log.CommonLogger = log.NewLogger("XmemNozzle", log.LogLevelInfo)

/************************************
/* struct bufferedMCRequest
*************************************/

type bufferedMCRequest struct {
	req          *mc.MCRequest
	sent_time    time.Time
	num_of_retry int
	err          error
}

func newBufferedMCRequest(request *mc.MCRequest) *bufferedMCRequest {
	return &bufferedMCRequest{req: request,
		sent_time:    time.Now(),
		num_of_retry: 0,
		err:          nil}
}

/************************************
/* struct requestBuffer
*************************************/
type requestBuffer struct {
	slots           []chan *bufferedMCRequest
	empty_slots_pos chan int
	size            int
	empty_slots_num chan int
	notifych        chan bool
}

func newReqBuffer(size int, notifychan chan bool) *requestBuffer {
	logger_xmem.Infof("Create a new request buffer of size %d\n", size)
	buf := &requestBuffer{make([]chan *bufferedMCRequest, 0, size), make(chan int, size), size, make(chan int,1), notifychan}
	buf.initializeOpaqueNumbers()
	logger_xmem.Infof("new request buffer of size %d is created\n", size)
	return buf
}

func (buf *requestBuffer) initializeOpaqueNumbers() error {
	for i := 0; i < buf.size; i++ {
		logger_xmem.Infof("add slot index=%d\n", i)
		buf.empty_slots_pos <- i
	}
	buf.empty_slots_num <- buf.size
	logger_xmem.Infof("empty_slots_num's size is %d\n", len(buf.empty_slots_num))

	return nil
}

func (buf *requestBuffer) slot(pos int) (*mc.MCRequest, error) {
	if pos < 0 || pos >= len(buf.slots) {
		logger_xmem.Error ("Invalid slot index")
		return nil, errors.New("Invalid slot index")
	}
	reqch := buf.slots[pos]

	//blocking to make sure only one caller
	//has the access to the slot at any single time
	select {
	case req := <-reqch:
		reqch <- req
		return req.req, nil
	default:
		return nil, errors.New("Somebody is holding it")
	}

}

func (buf *requestBuffer) modSlot(pos int, modFuc func(req *bufferedMCRequest, p int) bool) (bool, error) {
	if pos < 0 || pos >= len(buf.slots) {
		logger_xmem.Error ("Invalid slot index")
		return false, errors.New("Invalid slot index")
	}
	
	reqch := buf.slots[pos]

	req := <-reqch
	modified := modFuc(req, pos)
	if modified {
		reqch <- req
	}

	return modified, nil
}

func (buf *requestBuffer) evictSlot(pos int) error {
	if pos < 0 || pos >= len(buf.slots) {
		logger_xmem.Error ("Invalid slot index")
		return errors.New("Invalid slot index")
	}

	reqch := buf.slots[pos]

	<-reqch
	buf.empty_slots_pos <- pos
	empty_slots_num := <-buf.empty_slots_num
	empty_slots_num++
	if empty_slots_num == buf.size {
		if buf.notifych != nil {
			buf.notifych <- true
		}
	}
	buf.empty_slots_num <- empty_slots_num
	reqch <- nil

	return nil
}

func (buf *requestBuffer) availableSlotIndex() int {
	index := <-buf.empty_slots_pos
	return index
}

func (buf *requestBuffer) returnSlotIndex(index int) error {
	if index < 0 || index >= len(buf.slots) {
		logger_xmem.Error ("Invalid slot index")
		return errors.New("Invalid slot index")
	}

	buf.empty_slots_pos <- index
	return nil
}

func (buf *requestBuffer) enSlot(pos int, req *mc.MCRequest) error {
	if pos < 0 || pos >= len(buf.slots) {
		logger_xmem.Error ("Invalid slot index")
		return errors.New("Invalid slot index")
	}

	reqch := buf.slots[pos]
	r := <-reqch
	if r != nil {
		//the slot is not empty, error condition
		reqch <- r

		return errors.New("Slot is already occupied")
	}

	empty_slots_num := <-buf.empty_slots_num
	empty_slots_num--
	buf.empty_slots_num <- empty_slots_num

	bufferedReq := newBufferedMCRequest(req)
	reqch <- bufferedReq

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
	username     string
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
		username:     "",
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
	if val, ok := settings[XMEM_SETTING_CONNECTSTR]; ok {
		config.connectStr = val.(string)
	}
	if val, ok := settings[XMEM_SETTING_BUCKETNAME]; ok {
		config.bucketName = val.(string)
	}
	if val, ok := settings[XMEM_SETTING_USERNAME]; ok {
		config.username = val.(string)
	}
	if val, ok := settings[XMEM_SETTING_PASSWORD]; ok {
		config.password = val.(string)
	}
	return err
}

/************************************
/* struct XmemNozzle
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
	if b.curCount < b.capacity_count && b.curSize < b.capacity_size {
		b.curCount++
		b.curSize += size
		return true
	}

	return false
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
	gen_server.GenServer
	part.AbstractPart

	dataChan  chan *mc.MCRequest
	memClient *mcc.Client

	//configurable parameter
	config xmemConfig

	batch *xmemBatch

	sendNowCh chan bool

	childrenWaitGrp sync.WaitGroup

	buf *requestBuffer

	receiver_finch chan bool
	checker_finch  chan bool
	batch_done     chan bool
}

func NewXmemNozzle(id string) *XmemNozzle {

	var msg_callback_func gen_server.Msg_Callback_Func
	var behavior_callback_func gen_server.Behavior_Callback_Func
	var exit_callback_func gen_server.Exit_Callback_Func
	var isStarted_callback_func part.IsStarted_Callback_Func

	server := gen_server.NewGenServer(&msg_callback_func,
		&behavior_callback_func, &exit_callback_func)
	isStarted_callback_func = server.IsStarted
	part := part.NewAbstractPart(id, &isStarted_callback_func)
	xmem := &XmemNozzle{server, /*gen_server.GenServer*/
		part, /*part.AbstractPart*/
		nil,  /*dataChan*/
		nil,  /*memClient*/
		//		nil,              /*connPool*/
		newConfig(),      /*config	xmemConfig*/
		nil,              /*batch *xmemBatch*/
		make(chan bool),  /*sendNowChan chan bool*/
		sync.WaitGroup{}, /*childrenWaitGrp sync.WaitGroup*/
		nil,              /*buf	requestBuffer*/
		make(chan bool),  /*receiver_finch chan bool*/
		make(chan bool),  /*checker_finch chan bool*/
		make(chan bool) /*batch_done chan bool*/}
	msg_callback_func = nil
	behavior_callback_func = xmem.processData
	exit_callback_func = xmem.onExit
	return xmem

}

func (xmem *XmemNozzle) Open() {
}

func (xmem *XmemNozzle) Close() {
}

func (xmem *XmemNozzle) Start(settings map[string]interface{}) error {
	logger_xmem.Info ("Xmem starting ....")
	err := xmem.initialize(settings)
	logger_xmem.Info ("....Finish initializing....")
	if err == nil {
		xmem.childrenWaitGrp.Add(1)
		go xmem.receiveResponse(xmem.memClient, xmem.receiver_finch, &xmem.childrenWaitGrp)

		xmem.childrenWaitGrp.Add(1)
		go xmem.check(xmem.checker_finch, &xmem.childrenWaitGrp)
	}
	if err == nil {
		err = xmem.Start_server()
	}
	
	return err
}

func (xmem *XmemNozzle) Stop() error {
	err := xmem.Stop_server()

	//cleanup
	pool := base.ConnPoolMgr().GetPool(xmem.getPoolName(xmem.config.connectStr))
	if pool != nil {
		pool.Release(xmem.memClient)
	}

	xmem.memClient = nil

	return err
}

func (xmem *XmemNozzle) Receive(data interface{}) error {
	logger_xmem.Infof ("data key=%v is received", data.(*mc.MCRequest).Key)
	xmem.dataChan <- data.(*mc.MCRequest)

	//accumulate the batchCount and batchSize
	if !xmem.batch.accumuBatch(data.(*mc.MCRequest).Size()) {
		//non-blocking
		select {
		case xmem.sendNowCh <- true:
		}
	}
	//raise DataReceived event
	xmem.RaiseEvent(common.DataReceived, data.(*mc.MCRequest), xmem, nil, nil)
	return nil
}

func (xmem *XmemNozzle) processData() {
	select {
	case <-xmem.sendNowCh:
		xmem.send()
	}
}

func (xmem *XmemNozzle) onExit() {
	xmem.receiver_finch <- true
	xmem.checker_finch <- true
	xmem.childrenWaitGrp.Wait()
}

func (xmem *XmemNozzle) send() error {
	var err error
	count := xmem.batch.count()
	if count == 1 {
		select {
		case item := <-xmem.dataChan:
			//blocking
			index := xmem.buf.availableSlotIndex()
			err := xmem.sendSingleWithRetry(item, xmem.config.maxRetry, index)
			if err != nil {
				xmem.buf.returnSlotIndex(index)
				return err
			}

			//buffer it for now until the receipt is confirmed
			xmem.buf.enSlot(index, item)
		default:
			logger_xmem.Errorf("Invalid state - expected %d items in the batch; but there are not that much in the data channel", 1)
		}
	} else {
		var data []byte = make([]byte, 0, xmem.batch.curSize + 64)
		var header []byte = make([]byte, 64)
		binary.BigEndian.PutUint32(header, uint32(xmem.batch.curSize))
		binary.BigEndian.PutUint32(header, uint32(xmem.batch.curCount))
		data = append(data, header...)
		
		for i := 0; i < count; i++ {
			select {
			case item := <-xmem.dataChan:
				//blocking
				index := xmem.buf.availableSlotIndex()
				//				err := xmem.sendSingleWithRetry(item, xmem.config.maxRetry, index)
				//				if err != nil {
				//					xmem.buf.returnSlotIndex(index)
				//					logger_xmem.Errorf("XMmemLoc.sendBatch: remaining item= %d\n", count-i)
				//					return err
				//				}
				//
				//				//buffer it for now until the receipt is confirmed
				//				xmem.buf.enSlot(index, item)
				xmem.adjustRequest(item, index)
				item_byte := item.Bytes()
				data = append(data, item_byte...)

			default:
				logger_xmem.Errorf("Invalid state - expected %d items in the batch; but there are not that much in the data channel", count-i)
				break
			}
		}

		//send it
		conn := xmem.memClient.Hijack()
		conn.Write(data)
		xmem.memClient, err = mcc.Wrap(conn)
	}
	if xmem.config.mode == Batch_XMEM {
		<-xmem.batch_done
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
	err := xmem.memClient.Transmit(item)
	if err != nil {
		logger_xmem.Errorf("XMmemLoc.SendBatch: transmit error: %s\n", fmt.Sprint(err))
		return err
	}
	return nil
}

//TODO: who will release the pool? maybe it should be replication manager
//
func (xmem *XmemNozzle) initializeConnection() (err error) {
	logger_xmem.Infof("xmem.config= %v", xmem.config.connectStr)
	logger_xmem.Infof("poolName=%v", xmem.getPoolName(xmem.config.connectStr))
	pool, err := base.ConnPoolMgr().GetOrCreatePool(xmem.getPoolName(xmem.config.connectStr), xmem.config.connectStr, xmem.config.username, xmem.config.password, 5)
	if err == nil {
		xmem.memClient, err = pool.Get()
	}
	return err
}

func (xmem *XmemNozzle) getPoolName(connectionStr string) string {
	return "Couch_Xmem_" + connectionStr
}

func (xmem *XmemNozzle) initialize(settings map[string]interface{}) error {
	err := xmem.config.initializeConfig(settings)
	xmem.dataChan = make(chan *mc.MCRequest, xmem.config.maxCount)
	xmem.sendNowCh = make(chan bool)

	//initialize batch
	if xmem.config.mode == Batch_XMEM {
		xmem.batch = newXmemBatch(xmem.config.maxCount, xmem.config.maxSize)
		xmem.buf = newReqBuffer(xmem.config.maxCount*2, xmem.batch_done)
	} else {
		xmem.batch = newXmemBatch(1, -1)
		xmem.buf = newReqBuffer(xmem.config.maxCount*2, nil)
	}

	logger_xmem.Info("Here")
	
	xmem.receiver_finch = make(chan bool)
	xmem.checker_finch = make(chan bool)

	logger_xmem.Info("About to start initializing connection")
	if err == nil {
		err = xmem.initializeConnection()
	}

	logger_xmem.Info("Initialization is done")
	
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
						xmem.buf.modSlot(pos, xmem.resend)
					} else {
						//raiseEvent
						req, _ := xmem.buf.slot(pos)
						xmem.RaiseEvent(common.DataSent, req, xmem, nil, nil)
						//empty the slot in the buffer
						xmem.buf.evictSlot(pos)
						logger_xmem.Infof("data on slot=%d is received", pos)
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
		modified := xmem.resend(req, pos)
		return modified
	}
	return false
}

func (xmem *XmemNozzle) resend(req *bufferedMCRequest, pos int) bool {
	if req.num_of_retry < xmem.config.maxRetry-1 {
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
	mc_req.Extras = []byte{0, 0, 0, 0, 0, 0, 0, 0}
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
