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
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/utils"
	"io"
	"math"
	"math/rand"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

//configuration settings for XmemNozzle
const (
	SETTING_RESP_TIMEOUT             = "resp_timeout"
	XMEM_SETTING_DEMAND_ENCRYPTION   = "demandEncryption"
	XMEM_SETTING_CERTIFICATE         = "certificate"
	XMEM_SETTING_INSECURESKIPVERIFY  = "insecureSkipVerify"
	XMEM_SETTING_REMOTE_PROXY_PORT   = "remote_proxy_port"
	XMEM_SETTING_LOCAL_PROXY_PORT    = "local_proxy_port"
	XMEM_SETTING_REMOTE_MEM_SSL_PORT = "remote_ssl_port"

	//default configuration
	default_numofretry          int           = 5
	default_resptimeout         time.Duration = 2000 * time.Millisecond
	default_dataChannelSize                   = 5000
	default_batchExpirationTime               = 300 * time.Millisecond
	default_maxRetryInterval                  = 300 * time.Second
	default_writeTimeOut        time.Duration = time.Duration(1) * time.Second
	default_readTimeout         time.Duration = time.Duration(1) * time.Second
	default_maxIdleCount        int           = 60
	default_selfMonitorInterval time.Duration = 1 * time.Second
	default_demandEncryption    bool          = false
	default_max_downtime        time.Duration = 3 * time.Second
	//wait time between write is default_backoff_wait_time*backoff_factor
	default_backoff_wait_time time.Duration = 10 * time.Millisecond
)

var xmem_setting_defs base.SettingDefinitions = base.SettingDefinitions{SETTING_BATCHCOUNT: base.NewSettingDef(reflect.TypeOf((*int)(nil)), true),
	SETTING_BATCHSIZE:               base.NewSettingDef(reflect.TypeOf((*int)(nil)), true),
	SETTING_NUMOFRETRY:              base.NewSettingDef(reflect.TypeOf((*int)(nil)), false),
	SETTING_RESP_TIMEOUT:            base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	SETTING_WRITE_TIMEOUT:           base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	SETTING_READ_TIMEOUT:            base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	SETTING_MAX_RETRY_INTERVAL:      base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	SETTING_SELF_MONITOR_INTERVAL:   base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	SETTING_BATCH_EXPIRATION_TIME:   base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	SETTING_OPTI_REP_THRESHOLD:      base.NewSettingDef(reflect.TypeOf((*int)(nil)), true),
	XMEM_SETTING_DEMAND_ENCRYPTION:  base.NewSettingDef(reflect.TypeOf((*bool)(nil)), false),
	XMEM_SETTING_CERTIFICATE:        base.NewSettingDef(reflect.TypeOf((*[]byte)(nil)), false),
	XMEM_SETTING_INSECURESKIPVERIFY: base.NewSettingDef(reflect.TypeOf((*bool)(nil)), false),

	//only used for xmem over ssl via ns_proxy for 2.5
	XMEM_SETTING_REMOTE_PROXY_PORT: base.NewSettingDef(reflect.TypeOf((*uint16)(nil)), false),
	XMEM_SETTING_LOCAL_PROXY_PORT:  base.NewSettingDef(reflect.TypeOf((*uint16)(nil)), false)}

var UninitializedReseverationNumber = -1

func (doc_meta documentMetadata) String() string {
	return fmt.Sprintf("[key=%s; revSeq=%v;cas=%v;flags=%v;expiry=%v;deletion=%v]", doc_meta.key, doc_meta.revSeq, doc_meta.cas, doc_meta.flags, doc_meta.expiry, doc_meta.deletion)
}

type ConflictResolver func(doc_metadata_source documentMetadata, doc_metadata_target documentMetadata, logger *log.CommonLogger) bool

/************************************
/* struct bufferedMCRequest
*************************************/

type bufferedMCRequest struct {
	req          *base.WrappedMCRequest
	sent_time    *time.Time
	num_of_retry int
	err          error
	timedout     bool
	reservation  int
	lock         sync.RWMutex
}

func newBufferedMCRequest() *bufferedMCRequest {
	return &bufferedMCRequest{req: nil,
		sent_time:    nil,
		num_of_retry: 0,
		err:          nil,
		timedout:     false,
		reservation:  UninitializedReseverationNumber,
		lock:         sync.RWMutex{}}
}

// once a buffered request reaches the end of its life cycle, e.g., when the underlying mc request has been sent and acknowledged,
// reset it to an empty state to enable it to be reused by future mc requests
func resetBufferedMCRequest(request *bufferedMCRequest) {
	request.req = nil
	request.sent_time = nil
	request.num_of_retry = 0
	request.timedout = false
	request.reservation = UninitializedReseverationNumber
}

/***********************************************************
/* struct requestBuffer
/* This is used to buffer the sent but yet confirmed data
************************************************************/
type requestBuffer struct {
	slots           []*bufferedMCRequest /*slots to store the data*/
	sequences       []uint16
	empty_slots_pos chan uint16 /*empty slot pos in the buffer*/
	occupied_count  int32       /*occupied slot count*/
	size            uint16      /*the size of the buffer*/
	notifych        chan bool   /*notify channel is set when the buffer is below threshold*/
	//	notify_allowed  bool   /*notify is allowed*/
	notify_threshold uint16
	fin_ch           chan bool
	logger           *log.CommonLogger
	notifych_lock    sync.RWMutex
	token_ch         chan int
}

func newReqBuffer(size uint16, threshold uint16, token_ch chan int, logger *log.CommonLogger) *requestBuffer {
	logger.Debugf("Create a new request buffer of size %d\n", size)
	buf := &requestBuffer{
		slots:            make([]*bufferedMCRequest, size, size),
		sequences:        make([]uint16, size),
		empty_slots_pos:  make(chan uint16, size),
		size:             size,
		notifych:         nil,
		notify_threshold: threshold,
		fin_ch:           make(chan bool, 1),
		token_ch:         token_ch,
		logger:           logger,
		notifych_lock:    sync.RWMutex{},
		occupied_count:   0}

	logger.Debug("Slots is initialized")

	buf.initialize()

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

func (buf *requestBuffer) initialize() error {
	for i := 0; i < int(buf.size); i++ {
		// initialize all slots to empty requests
		buf.slots[i] = newBufferedMCRequest()

		//initialize the empty_slots_pos
		buf.empty_slots_pos <- uint16(i)
		buf.sequences[i] = 0
	}

	return nil
}

func (buf *requestBuffer) setNotifyCh() {
	buf.notifych_lock.Lock()
	defer buf.notifych_lock.Unlock()
	buf.notifych = make(chan bool, 1)
}

//blocking until the occupied slots are below threshold
func (buf *requestBuffer) flowControl() {
	buf.setNotifyCh()

	ret := buf.itemCountInBuffer() <= buf.notify_threshold
	if ret {
		return
	}
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
	if pos < 0 || pos >= buf.size {
		buf.logger.Error("Invalid slot index")
		err = errors.New("Invalid slot index")
	}
	return
}

//slot allow caller to get hold of the content in the slot without locking the slot
//@pos - the position of the slot
func (buf *requestBuffer) slot(pos uint16) (*base.WrappedMCRequest, error) {
	buf.logger.Debugf("Getting the content in slot %d\n", pos)

	err := buf.validatePos(pos)
	if err != nil {
		return nil, err
	}

	req := buf.slots[pos]

	req.lock.RLock()
	defer req.lock.RUnlock()

	return req.req, nil
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

	req.lock.Lock()
	defer req.lock.Unlock()

	var modified bool

	if req.req != nil {
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
	req.lock.Lock()
	defer req.lock.Unlock()

	if req.req != nil {
		resetBufferedMCRequest(req)

		buf.empty_slots_pos <- pos

		//decrease the occupied_count
		atomic.AddInt32(&buf.occupied_count, -1)
		<-buf.token_ch

		//increase sequence
		if buf.sequences[pos]+1 > 65535 {
			buf.sequences[pos] = 0
		} else {
			buf.sequences[pos] = buf.sequences[pos] + 1
		}

		buf.notifych_lock.RLock()
		defer buf.notifych_lock.RUnlock()

		if buf.itemCountInBuffer() <= buf.notify_threshold {
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

	req := buf.slots[index]
	req.lock.Lock()
	defer req.lock.Unlock()

	if req.req != nil || req.reservation != UninitializedReseverationNumber {
		panic(fmt.Sprintf("reserveSlot called on non-empty slot. req=%v, reseveration=%v\n", req.req, req.reservation))
	}

	req.reservation = reservation_num

	return nil, uint16(index), reservation_num
}

func (buf *requestBuffer) cancelReservation(index uint16, reservation_num int) error {

	err := buf.validatePos(index)
	if err == nil {
		buf.empty_slots_pos <- index

		req := buf.slots[index]
		var reservation_num int

		req.lock.Lock()
		defer req.lock.Unlock()

		//non blocking
		if req.req != nil {

			if req.reservation == reservation_num {
				resetBufferedMCRequest(req)
			} else {
				err = errors.New("Cancel reservation failed, reservation number doesn't match")
			}
		}
		//increase sequence
		if buf.sequences[index]+1 > 65535 {
			buf.sequences[index] = 0
		} else {
			buf.sequences[index] = buf.sequences[index] + 1
		}

	}
	return err
}

func (buf *requestBuffer) enSlot(pos uint16, req *base.WrappedMCRequest, reservationNum int) error {
	err := buf.validatePos(pos)
	if err != nil {
		return err
	}
	r := buf.slots[pos]

	r.lock.Lock()
	defer r.lock.Unlock()

	if r.reservation == UninitializedReseverationNumber {
		return errors.New("Slot is not initialized, must be reserved first.")
	} else {
		if r.reservation != reservationNum {
			buf.logger.Errorf("Can't enSlot %d, doesn't have the reservation, %v", pos, r)
			return errors.New(fmt.Sprintf("Can't enSlot %d, doesn't have the reservation", pos))
		}
		r.req = req
		now := time.Now()
		r.sent_time = &now
		buf.token_ch <- 1

		//increase the occupied_count
		atomic.AddInt32(&buf.occupied_count, 1)

	}
	buf.logger.Debugf("slot %d is occupied\n", pos)
	return nil
}

func (buf *requestBuffer) bufferSize() uint16 {
	return buf.size
}

func (buf *requestBuffer) itemCountInBuffer() uint16 {
	if buf != nil {
		return uint16(atomic.LoadInt32(&buf.occupied_count))
	} else {
		return 0
	}
}

/************************************
/* struct xmemConfig
*************************************/
type xmemConfig struct {
	baseConfig
	bucketName string
	//the duration to wait for the batch-sending to finish
	certificate        []byte
	demandEncryption   bool
	remote_proxy_port  uint16
	local_proxy_port   uint16
	memcached_ssl_port uint16
	respTimeout        time.Duration
	max_downtime       time.Duration
	logger             *log.CommonLogger
}

func newConfig(logger *log.CommonLogger) xmemConfig {
	return xmemConfig{
		baseConfig: baseConfig{maxCount: -1,
			maxSize:             -1,
			batchExpirationTime: default_batchExpirationTime,
			writeTimeout:        default_writeTimeOut,
			readTimeout:         default_readTimeout,
			maxRetryInterval:    default_maxRetryInterval,
			maxRetry:            default_numofretry,
			selfMonitorInterval: default_selfMonitorInterval,
			maxIdleCount:        default_maxIdleCount,
			connectStr:          "",
			username:            "",
			password:            "",
		},
		bucketName:         "",
		respTimeout:        default_resptimeout,
		demandEncryption:   default_demandEncryption,
		certificate:        []byte{},
		remote_proxy_port:  0,
		local_proxy_port:   0,
		max_downtime:       default_max_downtime,
		memcached_ssl_port: 0,
		logger:             logger,
	}

}

func (config *xmemConfig) initializeConfig(settings map[string]interface{}) error {
	err := utils.ValidateSettings(xmem_setting_defs, settings, config.logger)

	if err == nil {
		config.baseConfig.initializeConfig(settings)
		if val, ok := settings[XMEM_SETTING_DEMAND_ENCRYPTION]; ok {
			config.demandEncryption = val.(bool)
		}
		if config.demandEncryption {
			if val, ok := settings[XMEM_SETTING_CERTIFICATE]; ok {
				config.certificate = val.([]byte)
			} else {
				return errors.New("demandEncryption=true, but certificate is not set in settings")
			}

			if val, ok := settings[XMEM_SETTING_REMOTE_MEM_SSL_PORT]; ok {
				config.memcached_ssl_port = val.(uint16)
			} else {
				if val, ok := settings[XMEM_SETTING_REMOTE_PROXY_PORT]; ok {
					config.remote_proxy_port = val.(uint16)
				} else {
					return errors.New("demandEncryption=true, but neither remote_proxy_port nor remote_ssl_port is set in settings")
				}
				if val, ok := settings[XMEM_SETTING_LOCAL_PROXY_PORT]; ok {
					config.local_proxy_port = val.(uint16)
				} else {
					return errors.New("demandEncryption=true, but neither local_proxy_port nor remote_ssl_port is set in settings")
				}
			}
		}
	}
	return err
}

/************************************
/* struct xmemClient
*************************************/
var badConnectionError = errors.New("Connection is bad")
var connectionTimeoutReachLimitError = errors.New("Retry limit has been reached for this connection")
var connectionClosedError = errors.New("Connection is closed")
var fatalError = errors.New("Fatal")

type xmemClient struct {
	name      string
	memClient *mcc.Client
	//the count of continuous read\write failure on this client
	continuous_failure_counter int
	//the count of continuous successful read\write
	success_counter int
	//the maximum allowed continuous read\write failure on this client
	//exceed this limit, would consider this client's health is ruined.
	max_continuous_failure int
	max_downtime           time.Duration
	downtime_start         time.Time
	lock                   sync.RWMutex
	read_timeout           time.Duration
	write_timeout          time.Duration
	logger                 *log.CommonLogger
	poolName               string
	healthy                bool
	num_of_repairs         int
	last_failure           time.Time
	backoff_factor         int
}

func newXmemClient(name string, read_timeout, write_timeout time.Duration,
	client *mcc.Client, poolName string, max_continuous_failure int, max_downtime time.Duration, logger *log.CommonLogger) *xmemClient {
	logger.Infof("xmem client %v is created with read_timeout=%v, write_timeout=%v, retry_limit=%v", name, read_timeout, write_timeout, max_continuous_failure)
	return &xmemClient{name: name,
		memClient:                  client,
		continuous_failure_counter: 0,
		success_counter:            0,
		logger:                     logger,
		poolName:                   poolName,
		read_timeout:               read_timeout,
		write_timeout:              write_timeout,
		max_downtime:               max_downtime,
		max_continuous_failure:     max_continuous_failure,
		healthy:                    true,
		num_of_repairs:             0,
		lock:                       sync.RWMutex{},
		backoff_factor:             0,
	}
}

func (client *xmemClient) curFailureCounter() int {
	return client.continuous_failure_counter
}

func (client *xmemClient) reportOpFailure() {
	client.lock.Lock()
	defer client.lock.Unlock()

	if client.continuous_failure_counter == 0 {
		client.downtime_start = time.Now()
	}

	client.continuous_failure_counter++
	if client.continuous_failure_counter > client.max_continuous_failure && time.Since(client.downtime_start) > client.max_downtime {
		client.healthy = false
	}
}

func (client *xmemClient) reportOpSuccess() {
	client.lock.Lock()
	defer client.lock.Unlock()

	client.continuous_failure_counter = 0
	client.success_counter++
	if client.success_counter > client.max_continuous_failure && client.backoff_factor > 0 {
		client.backoff_factor--
		client.success_counter = client.success_counter - client.max_continuous_failure
	}
	client.healthy = true

}

func (client *xmemClient) isConnHealthy() bool {
	return client.healthy
}

func (client *xmemClient) getConn(readTimeout bool, writeTimeout bool) (io.ReadWriteCloser, int, error) {
	if client.memClient == nil {
		return nil, client.num_of_repairs, errors.New("memcached client is not set")
	}

	if !client.isConnHealthy() {
		return nil, client.num_of_repairs, badConnectionError
	}

	conn := client.memClient.Hijack()
	if readTimeout {
		conn.(net.Conn).SetReadDeadline(time.Now().Add(client.read_timeout))
	}
	if writeTimeout {
		conn.(net.Conn).SetWriteDeadline(time.Now().Add(client.write_timeout))
	}
	return conn, client.num_of_repairs, nil
}

func (client *xmemClient) repairConn(memClient *mcc.Client, repair_count_at_error int, xmem_id string) {
	client.lock.Lock()
	defer client.lock.Unlock()

	if client.num_of_repairs == repair_count_at_error {
		if time.Since(client.last_failure) < 1*time.Minute {
			client.backoff_factor++
		} else {
			client.backoff_factor = 0
		}

		client.memClient.Close()
		client.memClient = memClient
		client.continuous_failure_counter = 0
		client.num_of_repairs++
		client.healthy = true
	} else {
		client.logger.Infof("client %v for %v has been repaired (num_of_repairs=%v, repair_count_at_error=%v), the repair request is ignored\n", client.name, xmem_id, client.num_of_repairs, repair_count_at_error)
	}
}

func (client *xmemClient) markConnUnhealthy() {
	client.lock.Lock()
	defer client.lock.Unlock()
	client.healthy = false
}

func (client *xmemClient) close() {
	if client.isConnHealthy() {
		pool := base.ConnPoolMgr().GetPool(client.poolName)
		if pool != nil {
			pool.Release(client.memClient)
		}
	}
	client.memClient.Close()
}

func (client *xmemClient) repairCount() int {
	client.lock.RLock()
	defer client.lock.RUnlock()

	return client.num_of_repairs
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
	bytes_in_dataChan int32

	//memcached client connected to the target bucket
	client_for_setMeta *xmemClient
	client_for_getMeta *xmemClient

	//configurable parameter
	config xmemConfig

	//queue for ready batches
	batches_ready chan *dataBatch

	//batch to be accumulated
	batch *dataBatch
	// lock for adding requests to batch and for moving batches to batch ready queue
	batch_lock sync.RWMutex

	childrenWaitGrp sync.WaitGroup

	//buffer for the sent, but not yet confirmed data
	buf *requestBuffer

	//conflict resolover
	conflict_resolver ConflictResolver

	sender_finch      chan bool
	receiver_finch    chan bool
	checker_finch     chan bool
	selfMonitor_finch chan bool

	counter_sent     uint32
	counter_received uint32
	start_time       time.Time

	//the big seqno that is confirmed to be received on the vbucket
	//it is still possible that smaller seqno hasn't been received
	maxseqno_received_map map[uint16]uint64

	receive_token_ch chan int

	connType base.ConnType

	dataObj_recycler DataObjRecycler

	topic string
}

func NewXmemNozzle(id string,
	topic string,
	connPoolNamePrefix string,
	connPoolConnSize int,
	connectString string,
	bucketName string,
	password string,
	dataObj_recycler DataObjRecycler,
	logger_context *log.LoggerContext) *XmemNozzle {

	//callback functions from GenServer
	var msg_callback_func gen_server.Msg_Callback_Func
	var exit_callback_func gen_server.Exit_Callback_Func
	var error_handler_func gen_server.Error_Handler_Func

	server := gen_server.NewGenServer(&msg_callback_func,
		&exit_callback_func, &error_handler_func, logger_context, "XmemNozzle")
	part := NewAbstractPartWithLogger(id, server.Logger())

	xmem := &XmemNozzle{GenServer: server,
		AbstractPart:       part,
		bOpen:              true,
		lock_bOpen:         sync.RWMutex{},
		dataChan:           nil,
		receive_token_ch:   nil,
		client_for_setMeta: nil,
		client_for_getMeta: nil,
		config:             newConfig(server.Logger()),
		batches_ready:      nil,
		batch:              nil,
		batch_lock:         sync.RWMutex{},
		childrenWaitGrp:    sync.WaitGroup{},
		buf:                nil,
		receiver_finch:     make(chan bool, 1),
		checker_finch:      make(chan bool, 1),
		sender_finch:       make(chan bool, 1),
		selfMonitor_finch:  make(chan bool, 1),
		counter_sent:       0,
		counter_received:   0,
		dataObj_recycler:   dataObj_recycler,
		topic:              topic,
		maxseqno_received_map: make(map[uint16]uint64)}

	xmem.config.connectStr = connectString
	xmem.config.bucketName = bucketName
	xmem.config.password = password
	xmem.config.connPoolNamePrefix = connPoolNamePrefix
	xmem.config.connPoolSize = connPoolConnSize

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
	t := time.Now()
	xmem.Logger().Infof("Xmem starting ....settings=%v\n", settings)
	defer xmem.Logger().Infof("%v took %vs to start\n", xmem.Id(), time.Since(t).Seconds())

	err := xmem.SetState(common.Part_Starting)
	if err != nil {
		return err
	}

	err = xmem.initialize(settings)
	xmem.Logger().Info("....Finish initializing....")
	if err == nil {
		xmem.childrenWaitGrp.Add(1)
		go xmem.selfMonitor(xmem.selfMonitor_finch, &xmem.childrenWaitGrp)

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

	xmem.SetState(common.Part_Running)
	xmem.Logger().Info("Xmem nozzle is started")
	return err
}

func (xmem *XmemNozzle) Stop() error {
	xmem.Logger().Infof("Stop XmemNozzle %v\n", xmem.Id())
	err := xmem.SetState(common.Part_Stopping)
	if err != nil {
		return err
	}

	xmem.Logger().Debugf("XmemNozzle %v processed %v items\n", xmem.Id(), xmem.counter_sent)

	//close data channel
	if xmem.dataChan != nil {
		close(xmem.dataChan)
	}

	if xmem.batches_ready != nil {
		close(xmem.batches_ready)
	}

	err = xmem.Stop_server()
	if err != nil {
		return err
	}

	err = xmem.SetState(common.Part_Stopped)
	if err == nil {
		xmem.Logger().Debugf("XmemNozzle %v is stopped\n", xmem.Id())
	}

	//recycle all the bufferred MCRequest to object pool
	if xmem.buf != nil {
		xmem.Logger().Infof("XmemNozzle %v recycle %v objects in buffer\n", xmem.Id(), xmem.buf.itemCountInBuffer())
		for _, bufferredReq := range xmem.buf.slots {
			if bufferredReq != nil && bufferredReq.req != nil {
				xmem.recycleDataObj(bufferredReq.req)
			}
		}
	}

	if xmem.dataChan != nil {
		xmem.Logger().Infof("XmemNozzle %v recycle %v objects in data channel\n", xmem.Id(), len(xmem.dataChan))
		for data := range xmem.dataChan {
			xmem.dataObj_recycler(xmem.topic, data)
		}
	}

	return err
}

func (xmem *XmemNozzle) IsOpen() bool {
	ret := xmem.bOpen
	return ret
}

func (xmem *XmemNozzle) batchReady(lock bool) error {
	if lock {
		xmem.batch_lock.Lock()
		defer xmem.batch_lock.Unlock()
	}

	defer func() {
		if r := recover(); r != nil {
			if xmem.validateRunningState() == nil {
				// report error only when xmem is still in running state
				xmem.handleGeneralError(errors.New(fmt.Sprintf("%v", r)))
			}
		}
		xmem.Logger().Debugf("%v End moving batch, %v batches ready\n", xmem.Id(), len(xmem.batches_ready))
	}()

	//move the batch to ready batches channel
	if xmem.batch.count() > 0 {
		xmem.Logger().Debugf("%v move the batch (count=%d) ready queue\n", xmem.Id(), xmem.batch.count())
		select {
		case xmem.batches_ready <- xmem.batch:
			xmem.Logger().Debugf("There are %d batches in ready queue\n", len(xmem.batches_ready))

			xmem.initNewBatch()
		}
	}
	return nil
}

func (xmem *XmemNozzle) Receive(data interface{}) error {
	defer func() {
		if r := recover(); r != nil {
			xmem.handleGeneralError(errors.New(fmt.Sprintf("%v", r)))
		}
	}()

	err := xmem.validateRunningState()
	if err != nil {
		xmem.Logger().Infof("Part is in %v state, Recieve did no-op", xmem.State())
		return err
	}

	xmem.Logger().Debugf("data key=%v seq=%v is received", data.(*base.WrappedMCRequest).Req.Key, data.(*base.WrappedMCRequest).Seqno)
	xmem.Logger().Debugf("data channel len is %d\n", len(xmem.dataChan))

	request, ok := data.(*base.WrappedMCRequest)

	if !ok {
		err = fmt.Errorf("Got data of unexpected type. data=%v", data)
		xmem.handleGeneralError(errors.New(fmt.Sprintf("%v", err)))
		return err

	}

	xmem.writeToDataChan(request)
	atomic.AddUint32(&xmem.counter_received, 1)

	xmem.accumuBatch(request)

	xmem.Logger().Debugf("Xmem %v received %v items, queue_size = %v\n", xmem.Id(), xmem.counter_received, len(xmem.dataChan))

	return nil
}

func (xmem *XmemNozzle) accumuBatch(request *base.WrappedMCRequest) {
	xmem.batch_lock.Lock()
	defer xmem.batch_lock.Unlock()
	if xmem.batch.accumuBatch(request, xmem.optimisticRep) {
		xmem.batchReady(false)
	}
}

func (xmem *XmemNozzle) batchSize() int {
	xmem.batch_lock.RLock()
	defer xmem.batch_lock.RUnlock()
	return xmem.batch.size()
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
				if xmem.validateRunningState() != nil {
					xmem.Logger().Infof("xmem %v has stopped.", xmem.Id())
					goto done
				}
				if xmem.IsOpen() {
					xmem.buf.flowControl()
					xmem.Logger().Debugf("%v Batch Send..., %v batches ready, %v items in queue, count_recieved=%v, count_sent=%v\n", xmem.Id(), len(xmem.batches_ready), len(xmem.dataChan), xmem.counter_received, xmem.counter_sent)
					err = xmem.sendSetMeta_internal(batch)
					if err != nil {
						if err == PartStoppedError {
							goto done
						}

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
	xmem.buf.close()

	//notify the data processing routine
	close(xmem.sender_finch)
	close(xmem.receiver_finch)
	close(xmem.checker_finch)
	close(xmem.selfMonitor_finch)
	xmem.childrenWaitGrp.Wait()

	//cleanup
	xmem.client_for_setMeta.close()
	xmem.client_for_getMeta.close()

}

func (xmem *XmemNozzle) batchSetMetaWithRetry(batch *dataBatch, numOfRetry int) error {
	var err error
	count := batch.count()
	batch_replicated_count := 0
	reqs_bytes := []byte{}
	index_reservation_map := make(map[uint16]int)

	for i := 0; i < count; i++ {
		//check xmem's state, if it is already in stopping or stopped state, return
		err = xmem.validateRunningState()
		if err != nil {
			return err
		}

		item, err := xmem.readFromDataChan()
		if err != nil {
			return err
		}
		atomic.AddUint32(&xmem.counter_sent, 1)

		if item != nil {
			if needSend(item.Req, batch, xmem.Logger()) {
				//blocking
				err, index, reserv_num := xmem.buf.reserveSlot()
				if err != nil {
					return err
				}
				xmem.adjustRequest(item, index)
				item_byte := item.Req.Bytes()
				batch_replicated_count++

				reqs_bytes = append(reqs_bytes, item_byte...)
				err = xmem.buf.enSlot(index, item, reserv_num)
				index_reservation_map[index] = reserv_num

				//ns_ssl_proxy choke if the batch size is too big
				if batch_replicated_count > 50 {
					//send it
					err = xmem.sendWithRetry(xmem.client_for_setMeta, numOfRetry, xmem.packageRequest(batch_replicated_count, reqs_bytes))

					if err != nil {
						xmem.Logger().Errorf("%v Failed to send. err=%v\n", xmem.Id(), err)
						for index, reserv_num := range index_reservation_map {
							xmem.buf.cancelReservation(index, reserv_num)
						}
						return err
					}

					batch_replicated_count = 0
					reqs_bytes = []byte{}
					index_reservation_map = make(map[uint16]int)
				}
			} else {
				//lost on conflict resolution on source side
				// this still counts as data sent

				additionalInfo := make(map[string]interface{})
				additionalInfo[EVENT_ADDI_SEQNO] = item.Seqno
				xmem.RaiseEvent(common.DataFailedCRSource, item.Req, xmem, nil, additionalInfo)
			}
		}

	}

	//send the batch in one shot
	if batch_replicated_count > 0 {
		err = xmem.sendWithRetry(xmem.client_for_setMeta, numOfRetry, xmem.packageRequest(batch_replicated_count, reqs_bytes))

		if err != nil {
			xmem.Logger().Errorf("%v Failed to send. err=%v\n", xmem.Id(), err)
			for index, reserv_num := range index_reservation_map {
				xmem.buf.cancelReservation(index, reserv_num)
			}
			return err
		}
	}

	//log the data
	return err
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

func (xmem *XmemNozzle) sendWithRetry(client *xmemClient, numOfRetry int, item_byte []byte) error {
	var err error
	for j := 0; j < numOfRetry; j++ {
		err, rev := xmem.writeToClient(client, item_byte)
		if err == nil {
			return nil
		} else if err == badConnectionError {
			xmem.repairConn(client, err.Error(), rev)
		}
	}
	return err
}

func (xmem *XmemNozzle) sendSetMeta_internal(batch *dataBatch) error {
	var err error
	if batch != nil {
		count := batch.count()

		xmem.Logger().Infof("%v send batch count=%d\n", xmem.Id(), count)

		xmem.Logger().Debugf("So far, %v processed %d items", xmem.Id(), xmem.counter_sent)

		bigDoc_noRep_map, err := xmem.batchGetMeta(batch.bigDoc_map)
		if err != nil {
			xmem.Logger().Errorf("%v batchGetMeta failed. err=%v\n", xmem.Id(), err)
		} else {
			batch.bigDoc_noRep_map = bigDoc_noRep_map
		}

		//batch send
		err = xmem.batchSetMetaWithRetry(batch, xmem.config.maxRetry)
		if err != nil && err != PartStoppedError {
			high_level_err := "Error writing documents to memcached in target cluster."
			xmem.Logger().Errorf("%v. err=%v", high_level_err, err)
			xmem.handleGeneralError(errors.New(high_level_err))
		}
	}
	return err
}

//batch call to memcached GetMeta command for document size larger than the optimistic threshold
func (xmem *XmemNozzle) batchGetMeta(bigDoc_map map[string]*base.WrappedMCRequest) (map[string]bool, error) {
	bigDoc_noRep_map := make(map[string]bool)

	//if the bigDoc_map size is 0, return
	if len(bigDoc_map) == 0 {
		return bigDoc_noRep_map, nil
	}

	xmem.Logger().Infof("GetMeta for %v\n", len(bigDoc_map))
	respMap := make(map[string]*mc.MCResponse)
	lock := &sync.RWMutex{}
	opaque_keySeqno_map := make(map[uint32][]interface{})
	waitGrp := &sync.WaitGroup{}
	receiver_fin_ch := make(chan bool, 1)

	err_list := []error{}

	//launch the receiver
	waitGrp.Add(1)
	go func(count int, finch chan bool, opaque_keySeqno_map map[uint32][]interface{}, respMap map[string]*mc.MCResponse, timeout_duration time.Duration, err_list []error, waitGrp *sync.WaitGroup, logger *log.CommonLogger, lock *sync.RWMutex) {
		defer waitGrp.Done()
		ticker := time.NewTicker(timeout_duration)
		for {
			select {
			case <-finch:
				return
			case <-ticker.C:
				err_list = append(err_list, errors.New("batchGetMeta timedout"))
				return
			default:
				if xmem.validateRunningState() != nil {
					xmem.Logger().Infof("xmem %v has stopped, exit from getMeta receiver", xmem.Id())
					return
				}

				response, err, rev := xmem.readFromClient(xmem.client_for_getMeta)
				if err != nil {
					if err == PartStoppedError {
						return
					} else if err == badConnectionError || err == connectionTimeoutReachLimitError || err == connectionClosedError || err == fatalError {
						xmem.repairConn(xmem.client_for_getMeta, err.Error(), rev)
						return
					}
					err_list = append(err_list, err)

				} else {
					lock.RLock()
					keySeqno, ok := opaque_keySeqno_map[response.Opaque]
					lock.RUnlock()
					if ok {
						//success
						key, ok1 := keySeqno[0].(string)
						seqno, ok2 := keySeqno[1].(uint64)
						start_time, ok3 := keySeqno[2].(time.Time)
						if ok1 && ok2 && ok3 {
							respMap[key] = response

							additionalInfo := make(map[string]interface{})
							additionalInfo[EVENT_ADDI_DOC_KEY] = key
							additionalInfo[EVENT_ADDI_SEQNO] = seqno
							additionalInfo[EVENT_ADDI_GETMETA_COMMIT_TIME] = time.Since(start_time)
							xmem.RaiseEvent(common.GetMetaReceived, nil, xmem, nil, additionalInfo)
						} else {
							panic("KeySeqno list is not formated as expected [string, uint64]")
						}
					}
				}

				if len(respMap) >= count {
					logger.Infof("Expected %v response, got all", count)
					return
				}

			}
		}

	}(len(bigDoc_map), receiver_fin_ch, opaque_keySeqno_map, respMap, 1*time.Second, err_list, waitGrp, xmem.Logger(), lock)

	var sequence uint16 = uint16(time.Now().UnixNano())
	reqs_bytes := []byte{}
	counter := 0
	opaque := xmem.getOpaque(0, sequence)
	sent_key_map := make(map[string]bool)
	for _, originalReq := range bigDoc_map {
		docKey := string(originalReq.Req.Key)
		if _, ok := sent_key_map[docKey]; !ok {
			req := xmem.composeRequestForGetMeta(docKey, originalReq.Req.VBucket, opaque)
			reqs_bytes = append(reqs_bytes, req.Bytes()...)
			lock.Lock()
			opaque_keySeqno_map[opaque] = []interface{}{docKey, originalReq.Seqno, time.Now()}
			lock.Unlock()
			opaque++
			counter++
			sent_key_map[docKey] = true

			if counter > 50 {
				err, _ := xmem.writeToClient(xmem.client_for_getMeta, xmem.packageRequest(counter, reqs_bytes))
				if err != nil {
					//kill the receiver and return
					close(receiver_fin_ch)
					waitGrp.Wait()
					return nil, err
				}

				counter = 0
				reqs_bytes = []byte{}
			}
		}
	}

	var err error
	if counter > 0 {
		err, _ = xmem.writeToClient(xmem.client_for_getMeta, xmem.packageRequest(counter, reqs_bytes))
		if err != nil {
			//kill the receiver and return
			close(receiver_fin_ch)
			waitGrp.Wait()
			return nil, err
		}
	}

	waitGrp.Wait()
	if len(err_list) > 0 {
		return nil, err_list[0]
	}

	for _, wrappedReq := range bigDoc_map {
		key := string(wrappedReq.Req.Key)
		resp, ok := respMap[key]
		if ok && resp.Status == mc.SUCCESS {
			doc_meta_target := xmem.decodeGetMetaResp([]byte(key), resp)
			doc_meta_source := decodeSetMetaReq(wrappedReq.Req)
			if !xmem.conflict_resolver(doc_meta_source, doc_meta_target, xmem.Logger()) {
				xmem.Logger().Debugf("doc %v (%v)failed on conflict resolution to %v, no need to send\n", key, doc_meta_source, doc_meta_target)
				bigDoc_noRep_map[string(doc_meta_source.uniqueKey())] = true
			} else {
				xmem.Logger().Debugf("doc %v (%v)succeeded on conflict resolution to %v, sending it to target\n", key, doc_meta_source, doc_meta_target)
			}
		} else {
			xmem.Logger().Debugf("batchGetMeta: doc %s is not found in target system, send it", key)
		}
	}

	xmem.Logger().Debugf("Done with batchGetMeta, bigDoc_noRep_map=%v\n", bigDoc_noRep_map)
	return bigDoc_noRep_map, nil
}

func (xmem *XmemNozzle) decodeGetMetaResp(key []byte, resp *mc.MCResponse) documentMetadata {
	ret := documentMetadata{}
	ret.key = key
	extras := resp.Extras
	ret.deletion = (binary.BigEndian.Uint32(extras[0:4]) != 0)
	ret.flags = binary.BigEndian.Uint32(extras[4:8])
	ret.expiry = binary.BigEndian.Uint32(extras[8:12])
	ret.revSeq = binary.BigEndian.Uint64(extras[12:20])
	ret.cas = resp.Cas

	return ret

}

func (xmem *XmemNozzle) composeRequestForGetMeta(key string, vb uint16, opaque uint32) *mc.MCRequest {
	return &mc.MCRequest{VBucket: vb,
		Key:    []byte(key),
		Opaque: opaque,
		Opcode: base.GET_WITH_META}
}

func (xmem *XmemNozzle) sendSingleSetMeta(adjustRequest bool, item *base.WrappedMCRequest, index uint16, numOfRetry int) error {
	var err error
	if xmem.client_for_setMeta != nil {
		if adjustRequest {
			xmem.adjustRequest(item, index)
			xmem.Logger().Debugf("key=%v\n", item.Req.Key)
			xmem.Logger().Debugf("opcode=%v\n", item.Req.Opcode)
		}
		bytes := item.Req.Bytes()

		for j := 0; j < numOfRetry; j++ {
			err, rev := xmem.writeToClient(xmem.client_for_setMeta, xmem.packageRequest(1, bytes))
			if err == nil {
				return nil
			} else if err == badConnectionError {
				xmem.repairConn(xmem.client_for_setMeta, err.Error(), rev)
			}
		}
		return err

	}
	return nil
}

func (xmem *XmemNozzle) getConnPool() (pool base.ConnPool, err error) {
	poolName := getPoolName(xmem.config)
	if !xmem.config.demandEncryption {
		pool, err = base.ConnPoolMgr().GetOrCreatePool(poolName, xmem.config.connectStr, xmem.config.bucketName, xmem.config.bucketName, xmem.config.password, xmem.config.connPoolSize)
		if err != nil {
			return nil, err
		}
	} else {
		//create a pool of SSL connection
		poolName := getPoolName(xmem.config)
		hostName := utils.GetHostName(xmem.config.connectStr)
		remote_mem_port, err := utils.GetPortNumber(xmem.config.connectStr)
		if err != nil {
			return nil, err
		}

		if xmem.config.memcached_ssl_port != 0 {
			xmem.Logger().Infof("Get or create ssl over memcached connection, memcached_ssl_port=%v\n", int(xmem.config.memcached_ssl_port))
			pool, err = base.ConnPoolMgr().GetOrCreateSSLOverMemPool(poolName, hostName, xmem.config.bucketName, xmem.config.bucketName, xmem.config.password,
				base.DefaultConnectionSize, int(xmem.config.memcached_ssl_port), xmem.config.certificate)

		} else {
			xmem.Logger().Infof("Get or create ssl over proxy connection")
			pool, err = base.ConnPoolMgr().GetOrCreateSSLOverProxyPool(poolName, hostName, xmem.config.bucketName, xmem.config.bucketName, xmem.config.password,
				base.DefaultConnectionSize, int(remote_mem_port), int(xmem.config.local_proxy_port), int(xmem.config.remote_proxy_port), xmem.config.certificate)
		}
		if err != nil {
			return nil, err
		}
	}
	return pool, nil
}

func (xmem *XmemNozzle) initializeConnection() (err error) {
	xmem.Logger().Debugf("xmem.config= %v", xmem.config.connectStr)
	xmem.Logger().Debugf("poolName=%v", getPoolName(xmem.config))
	pool, err := xmem.getConnPool()
	if err != nil && err == base.WrongConnTypeError {
		//there is a stale pool, the remote cluster settings are changed
		base.ConnPoolMgr().RemovePool(getPoolName(xmem.config))
		pool, err = xmem.getConnPool()
	}
	if err != nil {
		return
	}
	xmem.connType = pool.ConnType()

	memClient_setMeta, err := pool.Get()

	if err != nil {
		return
	}
	memClient_getMeta, err := pool.Get()
	if err != nil {
		return
	}

	xmem.client_for_setMeta = newXmemClient("client_setMeta", xmem.config.readTimeout,
		xmem.config.writeTimeout, memClient_setMeta,
		getPoolName(xmem.config), xmem.config.maxRetry, xmem.config.max_downtime, xmem.Logger())
	xmem.client_for_getMeta = newXmemClient("client_getMeta", xmem.config.readTimeout,
		xmem.config.writeTimeout, memClient_getMeta,
		getPoolName(xmem.config), xmem.config.maxRetry, xmem.config.max_downtime, xmem.Logger())

	if err == nil {
		xmem.Logger().Infof("%v done with initializeConnection.", xmem.Id())
	}
	return err
}

func getPoolName(config xmemConfig) string {
	return config.connPoolNamePrefix + base.KeyPartsDelimiter + "Couch_Xmem_" + config.connectStr + base.KeyPartsDelimiter + config.bucketName
}

func (xmem *XmemNozzle) initNewBatch() {
	xmem.Logger().Debug("init a new batch")
	xmem.batch = newBatch(xmem.config.maxCount, xmem.config.maxSize, xmem.config.batchExpirationTime, xmem.Logger())
}

func (xmem *XmemNozzle) initialize(settings map[string]interface{}) error {
	err := xmem.config.initializeConfig(settings)
	if err != nil {
		return err
	}
	xmem.dataChan = make(chan *base.WrappedMCRequest, xmem.config.maxCount*50)
	xmem.bytes_in_dataChan = 0
	xmem.batches_ready = make(chan *dataBatch, 100)

	xmem.counter_received = 0
	xmem.counter_sent = 0

	//init a new batch
	xmem.initNewBatch()

	xmem.receive_token_ch = make(chan int, xmem.config.maxCount*2)
	xmem.buf = newReqBuffer(uint16(xmem.config.maxCount*2), uint16(float64(xmem.config.maxCount)*0.2), xmem.receive_token_ch, xmem.Logger())

	xmem.receiver_finch = make(chan bool, 1)
	xmem.checker_finch = make(chan bool, 1)

	xmem.Logger().Info("About to start initializing connection")
	if err == nil {
		err = xmem.initializeConnection()
	}

	xmem.Logger().Debug("Initialization is done")

	return err
}

func (xmem *XmemNozzle) receiveResponse(finch chan bool, waitGrp *sync.WaitGroup) {
	defer waitGrp.Done()

	for {
		select {
		case <-finch:
			goto done
		case <-xmem.receive_token_ch:
			xmem.receive_token_ch <- 1
			if xmem.validateRunningState() != nil {
				xmem.Logger().Infof("xmem %v has stopped", xmem.Id())
				goto done
			}

			response, err, rev := xmem.readFromClient(xmem.client_for_setMeta)
			if err != nil {
				if err == PartStoppedError || err == fatalError {
					goto done
				} else if err == badConnectionError || err == connectionClosedError {
					xmem.Logger().Error("The connection is ruined. Repair the connection and retry.")
					xmem.repairConn(xmem.client_for_setMeta, err.Error(), rev)
				}

				//read is unsuccessful, put the token back
			} else if response == nil {
				panic("readFromClient returned nil error and nil response")
			} else if response.Status != mc.SUCCESS && !isIgnorableMCError(response.Status) {
				if isRecoverableMCError(response.Status) {
					// err is recoverable. resend doc
					pos := xmem.getPosFromOpaque(response.Opaque)
					xmem.Logger().Errorf("%v Received recoverable error in response. Response status=%v, err = %v, response=%v\n", xmem.Id(), response.Status.String(), err, response.Bytes())
					_, err = xmem.buf.modSlot(pos, xmem.resend)

					//read is unsuccessful, put the token back
				} else {
					// for non-recoverable errors, report failure
					var req *mc.MCRequest = nil
					var seqno uint64
					pos := xmem.getPosFromOpaque(response.Opaque)
					wrappedReq, err := xmem.buf.slot(pos)
					if err == nil && wrappedReq != nil {
						req = wrappedReq.Req
						seqno = wrappedReq.Seqno
						if req != nil && req.Opaque == response.Opaque {
							xmem.Logger().Errorf("%v received a response indicating non-recoverable error from xmem client. response status=%v, opcode=%v, seqno=%v, req.Key=%v, req.Cas=%v, req.Extras=%s\n", xmem.Id(), response.Status, response.Opcode, seqno, req.Key, req.Cas, req.Extras)
							xmem.handleGeneralError(errors.New("Received non-recoverable error from memcached in target cluster"))
							goto done
						}
					}
					if req != nil {
						xmem.Logger().Debugf("%v Got the response, response.Opaque=%v, req.Opaque=%v\n", xmem.Id(), response.Opaque, req.Opaque)
					} else {
						xmem.Logger().Debugf("%v Got the response, pos=%v, req in that pos is nil\n", xmem.Id(), pos)
					}

				}

			} else {
				//raiseEvent
				pos := xmem.getPosFromOpaque(response.Opaque)
				wrappedReq, err := xmem.buf.slot(pos)
				if err != nil {
					xmem.Logger().Errorf("xmem buffer is in invalid state")
					xmem.handleGeneralError(errors.New("xmem buffer is in invalid state"))
					goto done
				}
				var req *mc.MCRequest
				var seqno uint64
				var committing_time time.Duration
				if wrappedReq != nil {
					req = wrappedReq.Req
					seqno = wrappedReq.Seqno
					committing_time = time.Since(wrappedReq.Start_time)
				}

				if req != nil && req.Opaque == response.Opaque {
					xmem.Logger().Debugf("%v Got the response, key=%s, status=%v\n", xmem.Id(), req.Key, response.Status)

					additionalInfo := make(map[string]interface{})
					additionalInfo[EVENT_ADDI_SEQNO] = seqno
					additionalInfo[EVENT_ADDI_OPT_REPD] = xmem.optimisticRep(req)
					additionalInfo[EVENT_ADDI_SETMETA_COMMIT_TIME] = committing_time

					xmem.RaiseEvent(common.DataSent, req, xmem, nil, additionalInfo)

					//feedback the most current commit_time to xmem.config.respTimeout
					xmem.adjustRespTimeout(committing_time)

					//empty the slot in the buffer
					if xmem.buf.evictSlot(pos) != nil {
						panic(fmt.Sprintf("Failed to evict slot %d\n", pos))
					}
					//put the request object back into the pool
					xmem.recycleDataObj(wrappedReq)
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

func (xmem *XmemNozzle) adjustRespTimeout(committing_time time.Duration) {
	xmem.config.respTimeout = committing_time
	//	xmem.Logger().Infof ("xmem.config.respTimeout=%v\n", xmem.config.respTimeout.Seconds())
}

func isNetError(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*net.OpError)
	return ok
}

func isNetTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	netError, ok := err.(*net.OpError)
	return ok && netError.Timeout()
}

func isNetTemporaryError(err error) bool {
	if err == nil {
		return false
	}
	netError, ok := err.(*net.OpError)
	return ok && netError.Temporary()
}

func isRecoverableMCError(resp_status mc.Status) bool {
	switch resp_status {
	case mc.TMPFAIL:
		return true
	default:
		return false
	}
}

func isIgnorableMCError(resp_status mc.Status) bool {
	switch resp_status {
	case mc.KEY_EEXISTS:
		return true
	default:
		return false
	}
}

func (xmem *XmemNozzle) getMaxIdleCount() int {
	backoff_factor := math.Max(float64(xmem.client_for_getMeta.backoff_factor), float64(xmem.client_for_setMeta.backoff_factor))
	backoff_factor = math.Min(float64(10), backoff_factor)

	//if client_for_getMeta.backoff_factor > 0 or client_for_setMeta.backoff_factor > 0, it means the target system is possibly under load, need to be more patient before
	//declare the stuckness.
	if backoff_factor > 0 {
		return xmem.config.maxIdleCount * int(backoff_factor)
	}
	return xmem.config.maxIdleCount
}

func (xmem *XmemNozzle) selfMonitor(finch chan bool, waitGrp *sync.WaitGroup) {
	defer waitGrp.Done()
	ticker := time.Tick(xmem.config.selfMonitorInterval)
	statsTicker := time.Tick(xmem.config.statsInterval)
	var sent_count uint32 = 0
	var resp_waitingConfirm_count int = 0
	var repairCount_setMeta = 0
	var repairCount_getMeta = 0
	var count uint64
	freeze_counter := 0
	for {
		select {
		case <-finch:
			goto done
		case <-ticker:
			if xmem.validateRunningState() != nil {
				xmem.Logger().Infof("xmem %v has stopped.", xmem.Id())
				goto done
			}

			count++
			if xmem.counter_sent == sent_count && int(xmem.buf.itemCountInBuffer()) == resp_waitingConfirm_count &&
				(len(xmem.dataChan) > 0 || xmem.buf.itemCountInBuffer() != 0) &&
				repairCount_setMeta == xmem.client_for_setMeta.repairCount() &&
				repairCount_getMeta == xmem.client_for_getMeta.repairCount() {
				freeze_counter++
			} else {
				freeze_counter = 0
			}
			sent_count = xmem.counter_sent
			resp_waitingConfirm_count = int(xmem.buf.itemCountInBuffer())
			repairCount_setMeta = xmem.client_for_setMeta.repairCount()
			repairCount_getMeta = xmem.client_for_getMeta.repairCount()
			if count == 10 {
				xmem.Logger().Debugf("%v- freeze_counter=%v, xmem.counter_sent=%v, len(xmem.dataChan)=%v, receive_count-%v, cur_batch_count=%v\n", xmem.Id(), freeze_counter, xmem.counter_sent, len(xmem.dataChan), xmem.counter_received, xmem.batch.count())
				xmem.Logger().Debugf("%v open=%v checking..., %v item unsent, received %v items, sent %v items, %v items waiting for response, %v batches ready, current batch timeout at %v, current batch expire_ch size is %v\n", xmem.Id(), xmem.IsOpen(), len(xmem.dataChan), xmem.counter_received, xmem.counter_sent, int(xmem.buf.bufferSize())-len(xmem.buf.empty_slots_pos), len(xmem.batches_ready), xmem.batch.start_time.Add(xmem.batch.expiring_duration), len(xmem.batch.expire_ch))
				count = 0
			}
			if freeze_counter > xmem.getMaxIdleCount() {
				xmem.Logger().Errorf("Xmem hasn't sent any item out for %v ticks, %v data in queue, flowcontrol=%v, con_retry_limit=%v, backoff_factor for client_setMeta is %v, backoff_factor for client_getMeta is %v", xmem.getMaxIdleCount(), len(xmem.dataChan), xmem.buf.itemCountInBuffer() <= xmem.buf.notify_threshold, xmem.client_for_setMeta.continuous_failure_counter, xmem.client_for_setMeta.backoff_factor, xmem.client_for_getMeta.backoff_factor)
				xmem.Logger().Infof("%v open=%v checking..., %v item unsent, received %v items, sent %v items, %v items waiting for response, %v batches ready, current batch timeout at %v, current batch expire_ch size is %v\n", xmem.Id(), xmem.IsOpen(), len(xmem.dataChan), xmem.counter_received, xmem.counter_sent, int(xmem.buf.bufferSize())-len(xmem.buf.empty_slots_pos), len(xmem.batches_ready), xmem.batch.start_time.Add(xmem.batch.expiring_duration), len(xmem.batch.expire_ch))
				//				utils.DumpStack(xmem.Logger())
				//the connection might not be healthy, it should not go back to connection pool
				xmem.client_for_setMeta.markConnUnhealthy()
				xmem.client_for_getMeta.markConnUnhealthy()
				xmem.handleGeneralError(errors.New("Xmem is stuck"))
				goto done
			}
		case <-statsTicker:
			additionalInfo := make(map[string]interface{})
			additionalInfo[STATS_QUEUE_SIZE] = len(xmem.dataChan)
			additionalInfo[STATS_QUEUE_SIZE_BYTES] = xmem.bytesInDataChan()
			xmem.RaiseEvent(common.StatsUpdate, nil, xmem, nil, additionalInfo)
		}
	}
done:
	xmem.Logger().Infof("Xmem %v selfMonitor routine exits", xmem.Id())

}

func (xmem *XmemNozzle) check(finch chan bool, waitGrp *sync.WaitGroup) {
	defer waitGrp.Done()

	ticker := time.Tick(xmem.config.respTimeout)
	for {
		select {
		case <-finch:
			goto done
		case <-ticker:
			if xmem.validateRunningState() != nil {
				goto done
			}

			select {
			case <-xmem.batch.expire_ch:
				xmem.Logger().Debugf("%v batch expired, moving it to ready queue\n", xmem.Id())
				xmem.batchReady(true)
			default:
			}
			xmem.Logger().Debugf("%v open=%v checking..., %v item unsent, received %v items, sent %v items, %v items waiting for response, %v batches ready, current batch timeout at %v, current batch expire_ch size is %v\n", xmem.Id(), xmem.IsOpen(), len(xmem.dataChan), xmem.counter_received, xmem.counter_sent, int(xmem.buf.bufferSize())-len(xmem.buf.empty_slots_pos), len(xmem.batches_ready), xmem.batch.start_time.Add(xmem.batch.expiring_duration), len(xmem.batch.expire_ch))
			size := xmem.buf.bufferSize()
			timeoutCheckFunc := xmem.checkTimeout
			for i := 0; i < int(size); i++ {
				_, err := xmem.buf.modSlot(uint16(i), timeoutCheckFunc)
				if err != nil {
					xmem.Logger().Errorf("%v Failed to check timeout %v\n", xmem.Id(), err)
					break
				}
			}

		}
	}
done:
	xmem.Logger().Info("Xmem checking routine exits")
}

func (xmem *XmemNozzle) checkTimeout(req *bufferedMCRequest, pos uint16) (bool, error) {
	if req.timedout {
		return false, nil
	}

	if req.num_of_retry > xmem.config.maxRetry {
		req.timedout = true
		err := errors.New(fmt.Sprintf("Failed to resend document %s, has tried to resend it %v, maximum retry %v reached",
			req.req.Req.Key, req.num_of_retry, xmem.config.maxRetry))
		xmem.Logger().Error(err.Error())

		xmem.repairConn(xmem.client_for_setMeta, err.Error(), xmem.client_for_setMeta.repairCount())
		return false, nil
	}

	respWaitTime := time.Since(*req.sent_time)
	if respWaitTime > xmem.timeoutDuration(req.num_of_retry) {
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
	xmem.Logger().Debugf("%v Retry sending %s, retry=%v", xmem.Id(), req.req.Req.Key, req.num_of_retry)
	err := xmem.sendSingleSetMeta(false, req.req, pos, xmem.config.maxRetry)

	if err != nil {
		req.err = err

	} else {
		req.num_of_retry = req.num_of_retry + 1
	}

	return false, err
}

func (xmem *XmemNozzle) resendForNewConn(req *bufferedMCRequest, pos uint16) (bool, error) {
	xmem.Logger().Debugf("%v Retry sending %s, retry=%v", xmem.Id(), req.req.Req.Key, req.num_of_retry)
	err := xmem.sendSingleSetMeta(false, req.req, pos, xmem.config.maxRetry)
	req.num_of_retry = 0
	if err != nil {
		req.err = err
		//report error
		xmem.handleGeneralError(err)
	}
	return false, err
}

func (xmem *XmemNozzle) adjustRequest(req *base.WrappedMCRequest, index uint16) {
	mc_req := req.Req
	mc_req.Opcode = encodeOpCode(mc_req.Opcode)
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

func encodeOpCode(code mc.CommandCode) mc.CommandCode {
	if code == mc.UPR_MUTATION || code == mc.TAP_MUTATION {
		return base.SET_WITH_META
	} else if code == mc.TAP_DELETE || code == mc.UPR_DELETION || code == mc.UPR_EXPIRATION {
		return base.DELETE_WITH_META
	}
	return code
}

func (xmem *XmemNozzle) ConnType() base.ConnType {
	return xmem.connType
}

func (xmem *XmemNozzle) StatusSummary() string {
	if xmem.State() == common.Part_Running {
		connType := xmem.connType
		return fmt.Sprintf("Xmem %v state =%v connType=%v received %v items, sent %v items, %v items waiting to confirm, %v in queue, %v in current batch, getMeta's backoff_factor is %v, setMeta's backoff_factor is %v", xmem.Id(), xmem.State(), connType, xmem.counter_received, xmem.counter_sent, xmem.buf.itemCountInBuffer(), len(xmem.dataChan), xmem.batchSize(), xmem.client_for_getMeta.backoff_factor, xmem.client_for_setMeta.backoff_factor)
	} else {
		return fmt.Sprintf("Xmem %v state =%v ", xmem.Id(), xmem.State())
	}
}

func (xmem *XmemNozzle) handleGeneralError(err error) {
	err1 := xmem.SetState(common.Part_Error)
	if err1 == nil {
		otherInfo := utils.WrapError(err)
		xmem.RaiseEvent(common.ErrorEncountered, nil, xmem, nil, otherInfo)
		xmem.Logger().Errorf("Raise error condition %v\n", err)
	} else {
		xmem.Logger().Infof("%v in shutdown process, err=%v is ignored\n", xmem.Id(), err)
	}
}

func (xmem *XmemNozzle) optimisticRep(req *mc.MCRequest) bool {
	if req != nil {
		return req.Size() < xmem.config.optiRepThreshold
	}
	return true
}

func (xmem *XmemNozzle) getConn(client *xmemClient, readTimeout bool, writeTimeout bool) (io.ReadWriteCloser, int, error) {
	err := xmem.validateRunningState()
	if err != nil {
		return nil, client.repairCount(), err
	}
	ret, rev, err := client.getConn(readTimeout, writeTimeout)
	return ret, rev, err
}

func (xmem *XmemNozzle) validateRunningState() error {
	if xmem.State() == common.Part_Stopping || xmem.State() == common.Part_Stopped || xmem.State() == common.Part_Error {
		return PartStoppedError
	}
	return nil
}

func (xmem *XmemNozzle) writeToClient(client *xmemClient, bytes []byte) (error, int) {
	time.Sleep(time.Duration(client.backoff_factor) * default_backoff_wait_time)

	conn, rev, err := xmem.getConn(client, false, true)
	if err != nil {
		return err, rev
	}
	_, err = conn.Write(bytes)
	if err == nil {
		client.reportOpSuccess()
		return err, rev
	} else {
		xmem.Logger().Errorf("%v writeToClient error: %s\n", xmem.Id(), fmt.Sprint(err))

		if utils.IsSeriousNetError(err) {
			//in this case, it is likely the connections in this pool would suffer the same problem, release all the connections
			xmem.releasePool()
			xmem.repairConn(client, err.Error(), rev)

		} else if isNetError(err) {
			client.reportOpFailure()
			wait_time := time.Duration(math.Pow(2, float64(client.curFailureCounter()))) * xmem.config.writeTimeout
			xmem.Logger().Errorf("%v batchSend Failed, retry after %v\n", xmem.Id(), wait_time)
			time.Sleep(wait_time)

		}

		return err, rev
	}
}

func (xmem *XmemNozzle) readFromClient(client *xmemClient) (*mc.MCResponse, error, int) {
	if client.memClient == nil {
		return nil, errors.New("memcached client is not set"), client.repairCount()
	}

	//set the read timeout for the underlying connection
	_, rev, err := xmem.getConn(client, true, false)
	if err != nil {
		client.logger.Errorf("Can't read from client %v, failed to get connection, err=%v", client.name, err)
		return nil, err, rev
	}

	response, err := client.memClient.Receive()
	if err != nil {
		xmem.Logger().Debugf("%v readFromClient: %v\n", xmem.Id(), err)
		isAppErr := false
		var errMsg string = ""
		if err == response {
			isAppErr = true
		} else {
			errMsg = err.Error()
		}

		if !isAppErr {
			if err == io.EOF {
				return nil, connectionClosedError, rev
			} else if utils.IsSeriousNetError(err) {
				//in this case, it is likely the connections in this pool would suffer the same problem, release all the connections
				xmem.releasePool()
				xmem.repairConn(client, errMsg, rev)
				return nil, badConnectionError, rev
			} else if isNetError(err) {
				client.reportOpFailure()
				return response, err, rev
			} else if strings.HasPrefix(errMsg, "bad magic") {
				//the connection to couchbase server is ruined. it is not supposed to return response with bad magic number
				//now the only sensible thing to do is to repair the connection, then retry
				client.logger.Infof("err=%v\n", err)
				xmem.repairConn(client, errMsg, rev)
				return nil, badConnectionError, rev
			}
		} else {
			//need to be called before mc.IsFatal because mc.IsFatal would returns true for ENOMEM
			if response.Status == mc.ENOMEM {
				//this is recoverable, it can succeed when ep-engine evic items out to free up memory
				return response, nil, rev
			} else if mc.IsFatal(err) {

				if response.Status == 0x08 {
					//PROTOCOL_BINARY_RESPONSE_NO_BUCKET
					//bucket must be recreated, drop the connection pool
					xmem.releasePool()
					client.logger.Error("Got PROTOCOL_BINARY_RESPONSE_NO_BUCKET, release the connections in the pool")
				} else if response.Status == mc.NOT_MY_VBUCKET {
					//the original error message is too long, which clogs the log
					err = base.ErrorNotMyVbucket
					xmem.releasePool()
				}
				high_level_err := "Received error response from memcached in target cluster."
				xmem.handleGeneralError(errors.New(high_level_err))
				client.logger.Errorf("%v. err=%v", high_level_err, err)
				return response, fatalError, rev
			} else {
				//response.Status != SUCCESSFUL, in this case, gomemcached return the response as err as well
				//return the err as nil so that caller can differentiate the application error from transport
				//error
				return response, nil, rev
			}
		}
	} else {
		//if no error, reset the client retry counter
		client.reportOpSuccess()
	}
	return response, err, rev
}

func (xmem *XmemNozzle) repairConn(client *xmemClient, reason string, rev int) error {

	if xmem.validateRunningState() != nil {
		xmem.Logger().Infof("%v is not running, no need to repairConn", xmem.Id())
		return nil
	}

	xmem.Logger().Errorf("%v connection %v is broken due to %v, try to repair...\n", xmem.Id(), client.name, reason)
	pool, err := xmem.getConnPool()
	if err != nil {
		return err
	}
	memClient, err := pool.Get()

	if err == nil {
		client.repairConn(memClient, rev, xmem.Id())
		if client == xmem.client_for_setMeta {
			xmem.onSetMetaConnRepaired()
		}

		xmem.Logger().Infof("%v - The connection for %v is repaired\n", xmem.Id(), client.name)

	} else {
		high_level_err := "Failed to repair connections to target cluster."
		xmem.handleGeneralError(errors.New(high_level_err))
		xmem.Logger().Errorf("%v - Failed to repair connections for %v. err=%v\n", xmem.Id(), client.name, err)
		return err
	}
	return nil
}

func (xmem *XmemNozzle) onSetMetaConnRepaired() {
	size := xmem.buf.bufferSize()
	count := 0
	for i := 0; i < int(size); i++ {
		sent, _ := xmem.buf.modSlot(uint16(i), xmem.resendForNewConn)
		if sent {
			count++
		}
	}
	xmem.Logger().Infof("%v - %v unresponded items are resent\n", xmem.Id(), count)

}

func (xmem *XmemNozzle) ConnStr() string {
	return xmem.config.connectStr
}

func (xmem *XmemNozzle) packageRequest(count int, reqs_bytes []byte) []byte {
	if xmem.ConnType() == base.SSLOverProxy {
		bytes := make([]byte, 8+len(reqs_bytes))
		binary.BigEndian.PutUint32(bytes[0:4], uint32(len(reqs_bytes)))
		binary.BigEndian.PutUint32(bytes[4:8], uint32(count))
		xmem.Logger().Infof("batch_size =%v, batch_count=%v\n", len(reqs_bytes), count)
		copy(bytes[8:8+len(reqs_bytes)], reqs_bytes)
		return bytes
	} else {
		return reqs_bytes
	}
}

func (xmem *XmemNozzle) releasePool() {
	xmem.Logger().Infof("release pool %v\n", getPoolName(xmem.config))
	pool := base.ConnPoolMgr().GetPool(getPoolName(xmem.config))
	if pool != nil {
		pool.ReleaseConnections()
	}

}

func (xmem *XmemNozzle) UpdateSettings(settings map[string]interface{}) error {
	optimisticReplicationThreshold, err := utils.GetIntSettingFromSettings(settings, metadata.OptimisticReplicationThreshold)
	if err != nil {
		return err
	}
	xmem.config.optiRepThreshold = optimisticReplicationThreshold
	return nil
}

func (xmem *XmemNozzle) writeToDataChan(item *base.WrappedMCRequest) {
	xmem.dataChan <- item
	atomic.AddInt32(&xmem.bytes_in_dataChan, int32(item.Req.Size()))
}

func (xmem *XmemNozzle) readFromDataChan() (*base.WrappedMCRequest, error) {
	item, ok := <-xmem.dataChan

	if !ok {
		//data channel has been closed, part must have been stopped
		return nil, PartStoppedError
	}

	atomic.AddInt32(&xmem.bytes_in_dataChan, int32(0-item.Req.Size()))

	return item, nil
}

func (xmem *XmemNozzle) bytesInDataChan() int {
	return int(atomic.LoadInt32(&xmem.bytes_in_dataChan))
}

func (xmem *XmemNozzle) recycleDataObj(req *base.WrappedMCRequest) {
	if xmem.dataObj_recycler != nil {
		xmem.dataObj_recycler(xmem.topic, req)
	}
}
