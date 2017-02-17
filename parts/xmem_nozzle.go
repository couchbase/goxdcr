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
	"bytes"
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
	"unsafe"
)

//configuration settings for XmemNozzle
const (
	SETTING_RESP_TIMEOUT             = "resp_timeout"
	XMEM_SETTING_DEMAND_ENCRYPTION   = "demandEncryption"
	XMEM_SETTING_CERTIFICATE         = "certificate"
	XMEM_SETTING_INSECURESKIPVERIFY  = "insecureSkipVerify"
	XMEM_SETTING_SAN_IN_CERITICATE   = "SANInCertificate"
	XMEM_SETTING_REMOTE_PROXY_PORT   = "remote_proxy_port"
	XMEM_SETTING_LOCAL_PROXY_PORT    = "local_proxy_port"
	XMEM_SETTING_REMOTE_MEM_SSL_PORT = "remote_ssl_port"

	//default configuration
	default_numofretry          int           = 5
	default_resptimeout         time.Duration = 6000 * time.Millisecond
	default_maxRetryInterval                  = 300 * time.Second
	default_writeTimeOut        time.Duration = time.Duration(120) * time.Second
	default_readTimeout         time.Duration = time.Duration(120) * time.Second
	default_maxIdleCount        uint32        = 60
	default_selfMonitorInterval time.Duration = default_resptimeout
	default_demandEncryption    bool          = false
	default_max_read_downtime   time.Duration = 60 * time.Second
	//wait time between write is default_backoff_wait_time*backoff_factor
	default_backoff_wait_time    time.Duration = 10 * time.Millisecond
	default_getMeta_readTimeout  time.Duration = time.Duration(1) * time.Second
	default_newconn_backoff_time time.Duration = 1 * time.Second

	//the maximum data (in byte) data channel can hold
	max_datachannelSize = 10 * 1024 * 1024
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
	XMEM_SETTING_SAN_IN_CERITICATE:  base.NewSettingDef(reflect.TypeOf((*bool)(nil)), false),
	XMEM_SETTING_INSECURESKIPVERIFY: base.NewSettingDef(reflect.TypeOf((*bool)(nil)), false),

	//only used for xmem over ssl via ns_proxy for 2.5
	XMEM_SETTING_REMOTE_PROXY_PORT: base.NewSettingDef(reflect.TypeOf((*uint16)(nil)), false),
	XMEM_SETTING_LOCAL_PROXY_PORT:  base.NewSettingDef(reflect.TypeOf((*uint16)(nil)), false)}

var UninitializedReseverationNumber = -1

type ConflictResolver func(doc_metadata_source documentMetadata, doc_metadata_target documentMetadata, source_cr_mode base.ConflictResolutionMode, logger *log.CommonLogger) bool

var GetMetaClientName = "client_getMeta"
var SetMetaClientName = "client_setMeta"

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

	logger.Debug("Slots have been initialized")

	buf.initialize()

	logger.Debugf("New request buffer of size %d has been created\n", size)
	return buf
}

func (buf *requestBuffer) close() {
	close(buf.fin_ch)
	buf.logger.Info("Request buffer has been closed. No blocking on flow control")
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

func (buf *requestBuffer) setNotifyCh() chan bool {
	buf.notifych_lock.Lock()
	defer buf.notifych_lock.Unlock()
	buf.notifych = make(chan bool, 1)
	return buf.notifych
}

func (buf *requestBuffer) unsetNotifyCh() {
	buf.notifych_lock.Lock()
	defer buf.notifych_lock.Unlock()
	buf.notifych = nil
}

//blocking until the occupied slots are below threshold
func (buf *requestBuffer) flowControl() {
	notifych := buf.setNotifyCh()

	ret := buf.itemCountInBuffer() <= buf.notify_threshold
	if ret {
		return
	}

	select {
	case <-notifych:
		buf.unsetNotifyCh()
		return
	case <-buf.fin_ch:
		buf.unsetNotifyCh()
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

func (buf *requestBuffer) cancelReservation(index uint16, reservation_num int) error {

	err := buf.validatePos(index)
	if err == nil {

		req := buf.slots[index]
		var reservation_num int

		req.lock.Lock()
		defer req.lock.Unlock()

		//non blocking
		if req.req != nil {

			if req.reservation == reservation_num {
				resetBufferedMCRequest(req)

				//increase sequence
				if buf.sequences[index]+1 > 65535 {
					buf.sequences[index] = 0
				} else {
					buf.sequences[index] = buf.sequences[index] + 1
				}

				buf.empty_slots_pos <- index
			} else {
				err = errors.New("Cancel reservation failed, reservation number doesn't match")
			}
		}
	}
	return err
}

func (buf *requestBuffer) enSlot(mcreq *base.WrappedMCRequest) (uint16, int, []byte) {
	buf.logger.Debugf("slots chan length=%d\n", len(buf.empty_slots_pos))

	index := <-buf.empty_slots_pos

	//non blocking
	//generate a random number
	reservation_num := rand.Int()

	req := buf.slots[index]
	req.lock.Lock()
	defer req.lock.Unlock()

	if req.req != nil || req.reservation != UninitializedReseverationNumber {
		panic(fmt.Sprintf("reserveSlot called on non-empty slot. req=%v, reseveration=%v\n", req.req, req.reservation))
	}

	req.reservation = reservation_num
	req.req = mcreq
	buf.adjustRequest(mcreq, index)
	item_bytes := mcreq.Req.Bytes()
	now := time.Now()
	req.sent_time = &now
	mcreq.Send_time = now
	buf.token_ch <- 1

	//increase the occupied_count
	atomic.AddInt32(&buf.occupied_count, 1)

	buf.logger.Debugf("slot %d is occupied\n", index)
	return index, reservation_num, item_bytes
}

// always called with lock on buf.slots[index]. no need for separate lock on buf.sequences[index]
func (buf *requestBuffer) adjustRequest(req *base.WrappedMCRequest, index uint16) {
	mc_req := req.Req
	mc_req.Opcode = encodeOpCode(mc_req.Opcode)
	mc_req.Cas = 0
	mc_req.Opaque = getOpaque(index, buf.sequences[int(index)])
}

func getOpaque(index, sequence uint16) uint32 {
	result := uint32(sequence)<<16 + uint32(index)
	return result
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
	// in ssl over mem mode, whether target cluster supports SANs in certificates
	san_in_certificate bool
	respTimeout        unsafe.Pointer // *time.Duration
	max_read_downtime  time.Duration
	logger             *log.CommonLogger
}

func newConfig(logger *log.CommonLogger) xmemConfig {
	config := xmemConfig{
		baseConfig: baseConfig{maxCount: -1,
			maxSize:             -1,
			writeTimeout:        default_writeTimeOut,
			readTimeout:         default_readTimeout,
			maxRetryInterval:    default_maxRetryInterval,
			maxRetry:            default_numofretry,
			selfMonitorInterval: default_selfMonitorInterval,
			connectStr:          "",
			username:            "",
			password:            "",
		},
		bucketName:         "",
		demandEncryption:   default_demandEncryption,
		certificate:        []byte{},
		remote_proxy_port:  0,
		local_proxy_port:   0,
		max_read_downtime:  default_max_read_downtime,
		memcached_ssl_port: 0,
		logger:             logger,
	}

	atomic.StoreUint32(&config.maxIdleCount, default_maxIdleCount)
	resptimeout := default_resptimeout
	atomic.StorePointer(&config.respTimeout, unsafe.Pointer(&resptimeout))

	return config

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

				if val, ok := settings[XMEM_SETTING_SAN_IN_CERITICATE]; ok {
					config.san_in_certificate = val.(bool)
				} else {
					return errors.New("SANInCertificate is not set in settings")
				}
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
var connectionClosedError = errors.New("Connection is closed")
var fatalError = errors.New("Fatal")

type xmemClient struct {
	name      string
	memClient *mcc.Client
	//the count of continuous read\write failure on this client
	continuous_write_failure_counter int
	//the count of continuous successful read\write
	success_counter int
	//the maximum allowed continuous read\write failure on this client
	//exceed this limit, would consider this client's health is ruined.
	max_continuous_write_failure int
	max_downtime                 time.Duration
	downtime_start               time.Time
	lock                         sync.RWMutex
	read_timeout                 time.Duration
	write_timeout                time.Duration
	logger                       *log.CommonLogger
	healthy                      bool
	num_of_repairs               int
	last_failure                 time.Time
	backoff_factor               int
}

func newXmemClient(name string, read_timeout, write_timeout time.Duration,
	client *mcc.Client, max_continuous_failure int, max_downtime time.Duration, logger *log.CommonLogger) *xmemClient {
	logger.Infof("xmem client %v is created with read_timeout=%v, write_timeout=%v, retry_limit=%v", name, read_timeout, write_timeout, max_continuous_failure)
	return &xmemClient{name: name,
		memClient:                        client,
		continuous_write_failure_counter: 0,
		success_counter:                  0,
		logger:                           logger,
		read_timeout:                     read_timeout,
		write_timeout:                    write_timeout,
		max_downtime:                     max_downtime,
		max_continuous_write_failure:     max_continuous_failure,
		healthy:        true,
		num_of_repairs: 0,
		lock:           sync.RWMutex{},
		backoff_factor: 0,
		downtime_start: time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC),
	}
}

func (client *xmemClient) curWriteFailureCounter() int {
	client.lock.RLock()
	defer client.lock.RUnlock()
	return client.continuous_write_failure_counter
}

func (client *xmemClient) reportOpFailure(readOp bool) {
	client.lock.Lock()
	defer client.lock.Unlock()

	if client.downtime_start.IsZero() {
		client.downtime_start = time.Now()
	}

	if !readOp {
		client.continuous_write_failure_counter++
	}
	if client.continuous_write_failure_counter > client.max_continuous_write_failure || time.Since(client.downtime_start) > client.max_downtime {
		client.healthy = false
	}
}

func (client *xmemClient) reportOpSuccess() {
	client.lock.Lock()
	defer client.lock.Unlock()

	client.downtime_start = time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC)
	client.continuous_write_failure_counter = 0
	client.success_counter++
	if client.success_counter > client.max_continuous_write_failure && client.backoff_factor > 0 {
		client.backoff_factor--
		client.success_counter = client.success_counter - client.max_continuous_write_failure
	}
	client.healthy = true

}

func (client *xmemClient) isConnHealthy() bool {
	client.lock.RLock()
	defer client.lock.RUnlock()
	return client.healthy
}

func (client *xmemClient) getMemClient() *mcc.Client {
	client.lock.RLock()
	defer client.lock.RUnlock()
	return client.memClient
}

func (client *xmemClient) getConn(readTimeout bool, writeTimeout bool) (io.ReadWriteCloser, int, error) {
	client.lock.RLock()
	defer client.lock.RUnlock()

	if client.memClient == nil {
		return nil, client.num_of_repairs, errors.New("memcached client is not set")
	}

	if !client.healthy {
		return nil, client.num_of_repairs, badConnectionError
	}

	conn := client.memClient.Hijack()
	if readTimeout {
		client.setReadTimeout(client.read_timeout)
	}

	if writeTimeout {
		client.setWriteTimeout(client.write_timeout)
	}
	return conn, client.num_of_repairs, nil
}

func (client *xmemClient) setReadTimeout(read_timeout_duration time.Duration) {
	conn := client.memClient.Hijack()
	conn.(net.Conn).SetReadDeadline(time.Now().Add(read_timeout_duration))
}

func (client *xmemClient) setWriteTimeout(write_timeout_duration time.Duration) {
	conn := client.memClient.Hijack()
	conn.(net.Conn).SetWriteDeadline(time.Now().Add(write_timeout_duration))
}

func (client *xmemClient) repairConn(memClient *mcc.Client, repair_count_at_error int, xmem_id string) bool {
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
		client.continuous_write_failure_counter = 0
		client.num_of_repairs++
		client.healthy = true
		return true
	} else {
		client.logger.Infof("client %v for %v has been repaired (num_of_repairs=%v, repair_count_at_error=%v), the repair request is ignored\n", client.name, xmem_id, client.num_of_repairs, repair_count_at_error)
		return false
	}
}

func (client *xmemClient) markConnUnhealthy() {
	client.lock.Lock()
	defer client.lock.Unlock()
	client.healthy = false
}

func (client *xmemClient) close() {
	client.lock.RLock()
	defer client.lock.RUnlock()
	client.memClient.Close()
}

func (client *xmemClient) repairCount() int {
	client.lock.RLock()
	defer client.lock.RUnlock()

	return client.num_of_repairs
}

func (client *xmemClient) getBackOffFactor() int {
	client.lock.RLock()
	defer client.lock.RUnlock()

	return client.backoff_factor
}

func (client *xmemClient) incrementBackOffFactor() {
	client.lock.Lock()
	defer client.lock.Unlock()

	client.backoff_factor++
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
	dataChan_control  chan bool

	//memcached client connected to the target bucket
	client_for_setMeta *xmemClient
	client_for_getMeta *xmemClient

	//configurable parameter
	config xmemConfig

	//queue for ready batches
	batches_ready_queue chan *dataBatch

	//batch to be accumulated
	batch *dataBatch
	// lock for adding requests to batch and for moving batches to batch ready queue
	batch_lock chan bool

	// count of mutations in the current batch
	// the same info is available through xmem.getBatch().count(), which could block, though
	// use this extra storage to avoid blocking
	cur_batch_count uint32

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
	counter_waittime uint32
	counter_batches  int32
	start_time       time.Time

	receive_token_ch chan int

	connType base.ConnType

	dataObj_recycler base.DataObjRecycler

	topic                 string
	last_ready_batch      int32
	last_ten_batches_size unsafe.Pointer // *[]uint32
	last_batch_id         int32

	// whether lww conflict resolution mode has been enabled
	source_cr_mode base.ConflictResolutionMode

	sourceBucketName string

	getMetaUserAgent string
	setMetaUserAgent string
}

func NewXmemNozzle(id string,
	topic string,
	connPoolNamePrefix string,
	connPoolConnSize int,
	connectString string,
	sourceBucketName string,
	targetBucketName string,
	password string,
	dataObj_recycler base.DataObjRecycler,
	source_cr_mode base.ConflictResolutionMode,
	logger_context *log.LoggerContext) *XmemNozzle {

	//callback functions from GenServer
	var msg_callback_func gen_server.Msg_Callback_Func
	var exit_callback_func gen_server.Exit_Callback_Func
	var error_handler_func gen_server.Error_Handler_Func

	server := gen_server.NewGenServer(&msg_callback_func,
		&exit_callback_func, &error_handler_func, logger_context, "XmemNozzle")
	part := NewAbstractPartWithLogger(id, server.Logger())

	xmem := &XmemNozzle{GenServer: server,
		AbstractPart:        part,
		bOpen:               true,
		lock_bOpen:          sync.RWMutex{},
		dataChan:            nil,
		receive_token_ch:    nil,
		client_for_setMeta:  nil,
		client_for_getMeta:  nil,
		config:              newConfig(server.Logger()),
		batches_ready_queue: nil,
		batch:               nil,
		batch_lock:          make(chan bool, 1),
		childrenWaitGrp:     sync.WaitGroup{},
		buf:                 nil,
		receiver_finch:      make(chan bool, 1),
		checker_finch:       make(chan bool, 1),
		sender_finch:        make(chan bool, 1),
		selfMonitor_finch:   make(chan bool, 1),
		counter_sent:        0,
		counter_received:    0,
		counter_waittime:    0,
		counter_batches:     0,
		dataObj_recycler:    dataObj_recycler,
		topic:               topic,
		source_cr_mode:      source_cr_mode,
		sourceBucketName:    sourceBucketName}

	initial_last_ten_batches_size := []uint32{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	atomic.StorePointer(&xmem.last_ten_batches_size, unsafe.Pointer(&initial_last_ten_batches_size))

	//set conflict resolver
	xmem.conflict_resolver = resolveConflict

	xmem.config.connectStr = connectString
	xmem.config.bucketName = targetBucketName
	xmem.config.password = password
	xmem.config.connPoolNamePrefix = connPoolNamePrefix
	xmem.config.connPoolSize = connPoolConnSize

	msg_callback_func = nil
	exit_callback_func = xmem.onExit
	error_handler_func = xmem.handleGeneralError
	return xmem

}

func (xmem *XmemNozzle) IsOpen() bool {
	xmem.lock_bOpen.RLock()
	defer xmem.lock_bOpen.RUnlock()

	return xmem.bOpen
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
	t := time.Now()
	xmem.Logger().Infof("%v starting ....settings=%v\n", xmem.Id(), settings)
	defer xmem.Logger().Infof("%v took %vs to start\n", xmem.Id(), time.Since(t).Seconds())

	err := xmem.SetState(common.Part_Starting)
	if err != nil {
		return err
	}

	err = xmem.initialize(settings)
	if err != nil {
		return err
	}
	xmem.Logger().Infof("%v finished initializing.", xmem.Id())
	xmem.childrenWaitGrp.Add(1)
	go xmem.selfMonitor(xmem.selfMonitor_finch, &xmem.childrenWaitGrp)

	xmem.childrenWaitGrp.Add(1)
	go xmem.receiveResponse(xmem.receiver_finch, &xmem.childrenWaitGrp)

	xmem.childrenWaitGrp.Add(1)
	go xmem.check(xmem.checker_finch, &xmem.childrenWaitGrp)

	xmem.childrenWaitGrp.Add(1)
	go xmem.processData_sendbatch(xmem.sender_finch, &xmem.childrenWaitGrp)

	xmem.start_time = time.Now()
	err = xmem.Start_server()
	xmem.SetState(common.Part_Running)
	xmem.Logger().Infof("%v has been started", xmem.Id())

	return err
}

func (xmem *XmemNozzle) Stop() error {
	xmem.Logger().Infof("Stopping %v\n", xmem.Id())
	err := xmem.SetState(common.Part_Stopping)
	if err != nil {
		return err
	}

	xmem.Logger().Debugf("%v processed %v items\n", xmem.Id(), atomic.LoadUint32(&xmem.counter_sent))

	//close data channel
	if xmem.dataChan != nil {
		close(xmem.dataChan)
	}

	if xmem.batches_ready_queue != nil {
		close(xmem.batches_ready_queue)
	}

	err = xmem.Stop_server()
	if err != nil {
		return err
	}

	err = xmem.SetState(common.Part_Stopped)
	if err == nil {
		xmem.Logger().Infof("%v has been stopped\n", xmem.Id())
	} else {
		xmem.Logger().Errorf("Error stopping %v. err=%v\n", xmem.Id(), err)
	}

	return err
}

func (xmem *XmemNozzle) batchReady() error {
	defer func() {
		if r := recover(); r != nil {
			if xmem.validateRunningState() == nil {
				// report error only when xmem is still in running state
				xmem.handleGeneralError(errors.New(fmt.Sprintf("%v", r)))
			}
		}
		xmem.Logger().Debugf("%v End moving batch, %v batches ready\n", xmem.Id(), len(xmem.batches_ready_queue))
	}()

	//move the batch to ready batches channel
	// no need to lock batch since batch is always locked before entering batchReady()
	count := xmem.batch.count()
	if count > 0 {
		xmem.Logger().Debugf("%v move the batch (count=%d) ready queue\n", xmem.Id(), count)
		select {
		case xmem.batches_ready_queue <- xmem.batch:
			xmem.Logger().Debugf("%v There are %d batches in ready queue\n", xmem.Id(), len(xmem.batches_ready_queue))
			xmem.initNewBatch()
		}
	}
	return nil
}

func (xmem *XmemNozzle) Receive(data interface{}) error {
	defer func() {
		if r := recover(); r != nil {
			xmem.Logger().Errorf("%v recovered from %v", xmem.Id(), r)
			xmem.handleGeneralError(errors.New(fmt.Sprintf("%v", r)))
		}
	}()

	err := xmem.validateRunningState()
	if err != nil {
		xmem.Logger().Infof("%v is in %v state, Recieve did no-op", xmem.Id(), xmem.State())
		return err
	}

	request, ok := data.(*base.WrappedMCRequest)

	if !ok {
		err = fmt.Errorf("Got data of unexpected type. data=%v", data)
		xmem.Logger().Errorf("%v %v", xmem.Id(), err)
		xmem.handleGeneralError(errors.New(fmt.Sprintf("%v", err)))
		return err

	}

	xmem.Logger().Debugf("%v data key=%v seq=%v is received", xmem.Id(), request.Req.Key, data.(*base.WrappedMCRequest).Seqno)
	xmem.Logger().Debugf("%v data channel len is %d\n", xmem.Id(), len(xmem.dataChan))

	xmem.accumuBatch(request)

	xmem.Logger().Debugf("%v received %v items, queue_size = %v\n", xmem.Id(), atomic.LoadUint32(&xmem.counter_received), len(xmem.dataChan))

	return nil
}

func (xmem *XmemNozzle) accumuBatch(request *base.WrappedMCRequest) {

	if string(request.Req.Key) == "" {
		panic(fmt.Sprintf("%v accumuBatch received request with Empty key, req.UniqueKey=%v\n", xmem.Id(), request.UniqueKey))
	}

	xmem.batch_lock <- true
	defer func() { <-xmem.batch_lock }()

	xmem.writeToDataChan(request)
	atomic.AddUint32(&xmem.counter_received, 1)

	curCount, _, isFull := xmem.batch.accumuBatch(request, xmem.optimisticRep)
	if curCount > 0 {
		atomic.StoreUint32(&xmem.cur_batch_count, curCount)
	}
	if isFull {
		xmem.batchReady()
	}
}

// caller of this method needs to be very careful to avoid deadlock
// for example, this method cannot be called from the code path
// that takes data off the data channel, since accumuBatch()
// may be holding the batch lock and waiting for data channel to become not full
func (xmem *XmemNozzle) getBatch() *dataBatch {
	xmem.batch_lock <- true
	defer func() { <-xmem.batch_lock }()
	return xmem.batch
}

// note that this method is specifically designed so that it never blocks
// if this method blocks when batch_lock is locked, dead lock could happen in the following scenario:
// 1. accumuBatch acquires batch_lock and tries to move xmem.batch into batches_ready_queue,
//    which gets blocked because batches_ready_queue is full
// 2. getBatchNonEmptyCh() is called within processData_sendbatch, which gets blocked since it
//    cannot acquire batch_lock
// 3. Once getBatchNonEmptyCh() is blocked, processData_sendbatch can never move to the next iteration and
//    take batches off batches_ready_queue so as to unblock accumuBatch.
//
// if it failes to acquire batch_lock, it still proceeds and returns a closed chan
// the caller, processData_sendbatch, will be able to proceed since reading from a closed channel does not block
func (xmem *XmemNozzle) getBatchNonEmptyCh() chan bool {
	select {
	case xmem.batch_lock <- true:
		defer func() { <-xmem.batch_lock }()
		return xmem.batch.batch_nonempty_ch
	default:
		// create and return a closed channel so that caller can proceed
		closed_ch := make(chan bool)
		close(closed_ch)
		return closed_ch
	}
}

func (xmem *XmemNozzle) processData_sendbatch(finch chan bool, waitGrp *sync.WaitGroup) (err error) {
	xmem.Logger().Infof("%v processData_sendbatch starts..........\n", xmem.Id())
	defer waitGrp.Done()
	for {
		select {
		case <-finch:
			goto done
		case batch, ok := <-xmem.batches_ready_queue:
			if !ok {
				xmem.Logger().Infof("%v batches_ready_queue closed. Exiting processData_sendBatch.", xmem.Id())
				goto done
			}

			if xmem.validateRunningState() != nil {
				xmem.Logger().Infof("%v has stopped.", xmem.Id())
				goto done
			}

			//batch get meta to find what need to be sent
			bigDoc_noRep_map, err := xmem.batchGetMeta(batch.bigDoc_map)
			if err != nil {
				xmem.Logger().Errorf("%v batchGetMeta failed. err=%v\n", xmem.Id(), err)
			} else {
				batch.bigDoc_noRep_map = bigDoc_noRep_map
			}

			err = xmem.processBatch(batch)
			if err != nil {
				if err == PartStoppedError {
					goto done
				}

				xmem.handleGeneralError(err)
			}
			xmem.recordBatchSize(batch.count())
		case <-xmem.getBatchNonEmptyCh():
			if xmem.validateRunningState() != nil {
				xmem.Logger().Infof("%v has stopped.", xmem.Id())
				goto done
			}

			if len(xmem.batches_ready_queue) == 0 {
				select {
				case xmem.batch_lock <- true:
					xmem.batchReady()
					<-xmem.batch_lock
				default:
				}
			}
		}
	}

done:
	xmem.Logger().Infof("%v processData_batch exits\n", xmem.Id())
	return
}

func (xmem *XmemNozzle) processBatch(batch *dataBatch) error {
	if xmem.IsOpen() {
		xmem.buf.flowControl()
		err := xmem.sendSetMeta_internal(batch)
		return err
	}
	return nil
}
func (xmem *XmemNozzle) onExit() {
	//in the process of stopping, no need to report any error to replication manager anymore
	xmem.buf.close()

	//notify the data processing routine
	close(xmem.sender_finch)
	close(xmem.receiver_finch)
	close(xmem.checker_finch)
	close(xmem.selfMonitor_finch)

	go xmem.finalCleanup()
}

func (xmem *XmemNozzle) finalCleanup() {
	xmem.childrenWaitGrp.Wait()

	//cleanup
	xmem.client_for_setMeta.close()
	xmem.client_for_getMeta.close()

	//recycle all the bufferred MCRequest to object pool
	if xmem.buf != nil {
		xmem.Logger().Infof("%v recycling %v objects in buffer\n", xmem.Id(), xmem.buf.itemCountInBuffer())
		for _, bufferredReq := range xmem.buf.slots {
			if bufferredReq != nil && bufferredReq.req != nil {
				xmem.recycleDataObj(bufferredReq.req)
			}
		}
	}

	if xmem.dataChan != nil {
		xmem.Logger().Infof("%v recycling %v objects in data channel\n", xmem.Id(), len(xmem.dataChan))
		for data := range xmem.dataChan {
			xmem.dataObj_recycler(xmem.topic, data)
		}
	}

}

func (xmem *XmemNozzle) batchSetMetaWithRetry(batch *dataBatch, numOfRetry int) error {
	var err error
	count := batch.count()
	batch_replicated_count := 0
	reqs_bytes := []byte{}
	index_reservation_list := make([][]uint16, 51)

	for i := 0; i < int(count); i++ {
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
			atomic.AddUint32(&xmem.counter_waittime, uint32(time.Since(item.Start_time).Seconds()*1000))
			needSend := needSend(item, batch, xmem.Logger())
			if needSend == Send {

				//blocking
				index, reserv_num, item_bytes := xmem.buf.enSlot(item)

				reqs_bytes = append(reqs_bytes, item_bytes...)

				reserv_num_pair := make([]uint16, 2)
				reserv_num_pair[0] = index
				reserv_num_pair[1] = uint16(reserv_num)
				index_reservation_list[batch_replicated_count] = reserv_num_pair
				batch_replicated_count++

				//ns_ssl_proxy choke if the batch size is too big
				if batch_replicated_count > 50 {
					//send it
					err = xmem.sendWithRetry(xmem.client_for_setMeta, numOfRetry, xmem.packageRequest(batch_replicated_count, reqs_bytes))

					if err != nil {
						xmem.Logger().Errorf("%v Failed to send. err=%v\n", xmem.Id(), err)
						for _, index_reserv_tuple := range index_reservation_list {
							xmem.buf.cancelReservation(index_reserv_tuple[0], int(index_reserv_tuple[1]))
						}
						return err
					}

					batch_replicated_count = 0
					reqs_bytes = []byte{}
					index_reservation_list = make([][]uint16, 51)
				}
			} else {
				if needSend == Not_Send_Failed_CR {
					//lost on conflict resolution on source side
					// this still counts as data sent
					additionalInfo := DataFailedCRSourceEventAdditional{Seqno: item.Seqno,
						Opcode:      encodeOpCode(item.Req.Opcode),
						IsExpirySet: (binary.BigEndian.Uint32(item.Req.Extras[4:8]) != 0),
						VBucket:     item.Req.VBucket,
					}
					xmem.RaiseEvent(common.NewEvent(common.DataFailedCRSource, nil, xmem, nil, additionalInfo))
				}

				xmem.recycleDataObj(item)
			}
		}

	}

	//send the batch in one shot
	if batch_replicated_count > 0 {
		err = xmem.sendWithRetry(xmem.client_for_setMeta, numOfRetry, xmem.packageRequest(batch_replicated_count, reqs_bytes))

		if err != nil {
			xmem.Logger().Errorf("%v Failed to send. err=%v\n", xmem.Id(), err)
			for _, index_reserv_tuple := range index_reservation_list {
				xmem.buf.cancelReservation(index_reserv_tuple[0], int(index_reserv_tuple[1]))
			}
			return err
		}
	}

	//log the data
	return err
}

//return true if doc_meta_source win; false otherwise
func resolveConflict(doc_meta_source documentMetadata,
	doc_meta_target documentMetadata, source_cr_mode base.ConflictResolutionMode, logger *log.CommonLogger) bool {
	if source_cr_mode == base.CRMode_LWW {
		return resolveConflictByCAS(doc_meta_source, doc_meta_target, logger)
	} else {
		return resolveConflictByRevSeq(doc_meta_source, doc_meta_target, logger)
	}
}

func resolveConflictByCAS(doc_meta_source documentMetadata,
	doc_meta_target documentMetadata, logger *log.CommonLogger) bool {
	ret := true
	if doc_meta_target.cas > doc_meta_source.cas {
		ret = false
	} else if doc_meta_target.cas == doc_meta_source.cas {
		if doc_meta_target.revSeq > doc_meta_source.revSeq {
			ret = false
		} else if doc_meta_target.revSeq == doc_meta_source.revSeq {
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
		err, rev := xmem.writeToClient(client, item_byte, true)
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
		xmem.Logger().Debugf("%v send batch count=%d\n", xmem.Id(), count)
		xmem.Logger().Debugf("So far, %v processed %d items", xmem.Id(), atomic.LoadUint32(&xmem.counter_sent))

		//batch send
		err = xmem.batchSetMetaWithRetry(batch, xmem.config.maxRetry)
		if err != nil && err != PartStoppedError {
			high_level_err := "Error writing documents to memcached in target cluster."
			xmem.Logger().Errorf("%v %v. err=%v", xmem.Id(), high_level_err, err)
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

	xmem.Logger().Debugf("%v GetMeta for %v documents\n", xmem.Id(), len(bigDoc_map))
	respMap := make(map[string]*mc.MCResponse, xmem.config.maxCount)
	opaque_keySeqno_map := make(map[uint32][]interface{})
	receiver_fin_ch := make(chan bool, 1)
	receiver_return_ch := make(chan bool, 1)

	reqs_bytes_list := [][]byte{}
	// stores the batch count for each reqs_bytes in reqs_bytes_list
	batch_count_list := []int{}

	var sequence uint16 = uint16(time.Now().UnixNano())
	reqs_bytes := []byte{}
	counter := 0
	opaque := getOpaque(0, sequence)
	sent_key_map := make(map[string]bool, len(bigDoc_map))

	//de-dupe and prepare the packages
	for key, originalReq := range bigDoc_map {
		docKey := string(originalReq.Req.Key)
		if docKey == "" {
			panic(fmt.Sprintf("%v unique-key= %v, Empty docKey, req=%v, bigDoc_map=%v", xmem.Id(), key, originalReq.Req, bigDoc_map))
		}

		if _, ok := sent_key_map[docKey]; !ok {
			// request extended meta from target only when
			// 1. extended meta is supported by replication
			// and 2. conflict resolution mode of source document is lww
			req := xmem.composeRequestForGetMeta(docKey, originalReq.Req.VBucket, opaque)
			reqs_bytes = append(reqs_bytes, req.Bytes()...)
			opaque_keySeqno_map[opaque] = []interface{}{docKey, originalReq.Seqno, originalReq.Req.VBucket, time.Now()}
			opaque++
			counter++
			sent_key_map[docKey] = true

			if counter > 50 {
				reqs_bytes_list = append(reqs_bytes_list, reqs_bytes)
				batch_count_list = append(batch_count_list, counter)
				counter = 0
				reqs_bytes = []byte{}
			}
		}
	}

	if counter > 0 {
		reqs_bytes_list = append(reqs_bytes_list, reqs_bytes)
		batch_count_list = append(batch_count_list, counter)
	}

	if len(reqs_bytes_list) != len(batch_count_list) {
		panic("Length of reqs_bytes_list and batch_count_list do not match")
	}

	//launch the receiver
	go func(count int, finch chan bool, return_ch chan bool, opaque_keySeqno_map map[uint32][]interface{}, respMap map[string]*mc.MCResponse, logger *log.CommonLogger) {
		defer func() {
			//handle the panic gracefully.
			if r := recover(); r != nil {
				if xmem.validateRunningState() == nil {
					errMsg := fmt.Sprintf("%v", r)
					//add to the error list
					xmem.Logger().Errorf("%v batchGetMeta receiver recovered from err = %v", xmem.Id(), errMsg)

					//repair the connection
					xmem.repairConn(xmem.client_for_getMeta, errMsg, xmem.client_for_getMeta.repairCount())
				}
			}
			close(return_ch)
		}()

		for {
			select {
			case <-finch:
				return
			default:
				if xmem.validateRunningState() != nil {
					xmem.Logger().Infof("%v has stopped, exit from getMeta receiver", xmem.Id())
					return
				}

				response, err, rev := xmem.readFromClient(xmem.client_for_getMeta, true)
				if err != nil {
					if err == fatalError {
						// if possible, log info about the corresponding request to facilitate debugging
						if response != nil {
							keySeqno, ok := opaque_keySeqno_map[response.Opaque]
							if ok {
								key, ok1 := keySeqno[0].(string)
								seqno, ok2 := keySeqno[1].(uint64)
								vbno, ok3 := keySeqno[2].(uint16)
								if ok1 && ok2 && ok3 {
									xmem.Logger().Infof("%v received fatal error from getMeta client. key=%v, seqno=%v, vb=%v, response=%v\n", xmem.Id(), key, seqno, vbno, response)
								}
							}
						}
					} else if err == badConnectionError || err == connectionClosedError {
						xmem.repairConn(xmem.client_for_getMeta, err.Error(), rev)
					}

					if !isNetTimeoutError(err) {
						logger.Errorf("%v batchGetMeta received fatal error and had to abort. Expected %v responses, got %v responses. err=%v", xmem.Id(), count, len(respMap), err)
						logger.Infof("%v Expected=%v, Received=%v\n", xmem.Id(), opaque_keySeqno_map, respMap)
						return
					} else {
						logger.Errorf("%v batchGetMeta timed out. Expected %v responses, got %v responses", xmem.Id(), count, len(respMap))
						logger.Infof("%v Expected=%v, Received=%v\n", xmem.Id(), opaque_keySeqno_map, respMap)
					}

					return

				} else {
					keySeqno, ok := opaque_keySeqno_map[response.Opaque]
					if ok {
						//success
						key, ok1 := keySeqno[0].(string)
						seqno, ok2 := keySeqno[1].(uint64)
						vbno, ok2 := keySeqno[2].(uint16)
						start_time, ok3 := keySeqno[3].(time.Time)
						if ok1 && ok2 && ok3 {
							respMap[key] = response

							additionalInfo := GetMetaReceivedEventAdditional{Key: key,
								Seqno:       seqno,
								Commit_time: time.Since(start_time),
							}
							xmem.RaiseEvent(common.NewEvent(common.GetMetaReceived, nil, xmem, nil, additionalInfo))

							if response.Status != mc.SUCCESS && !isIgnorableMCError(response.Status) && !isTemporaryMCError(response.Status) && response.Status != mc.KEY_ENOENT {
								if response.Status == mc.NOT_MY_VBUCKET {
									vb_err := fmt.Errorf("Received error %v on vb %v\n", base.ErrorNotMyVbucket, vbno)
									xmem.handleVBError(vbno, vb_err)
								} else {
									// this response requires connection reset

									// log the corresponding request to facilitate debugging
									xmem.Logger().Infof("%v received error from getMeta client. key=%v, seqno=%v, response=%v\n", xmem.Id(), key, seqno, response)
									err = fmt.Errorf("error response with status %v from memcached", response.Status)
									xmem.repairConn(xmem.client_for_getMeta, err.Error(), rev)
									// no need to wait further since connection has been reset
									return
								}
							}
						} else {
							panic("KeySeqno list is not formated as expected [string, uint64, time]")
						}
					}

				}

				//*count == 0 means write is still in session, can't return
				if len(respMap) >= count {
					logger.Debugf("%v Expected %v response, got all", xmem.Id(), count)
					return
				}
			}
		}

	}(len(opaque_keySeqno_map), receiver_fin_ch, receiver_return_ch, opaque_keySeqno_map, respMap, xmem.Logger())

	//send the requests
	for index, packet := range reqs_bytes_list {
		err, _ := xmem.writeToClient(xmem.client_for_getMeta, xmem.packageRequest(batch_count_list[index], packet), true)
		if err != nil {
			//kill the receiver and return
			close(receiver_fin_ch)
			return nil, err
		}
	}

	//wait for receiver to finish
	<-receiver_return_ch

	for _, wrappedReq := range bigDoc_map {
		key := string(wrappedReq.Req.Key)
		resp, ok := respMap[key]
		if ok && resp.Status == mc.SUCCESS {
			doc_meta_target := xmem.decodeGetMetaResp([]byte(key), resp)
			doc_meta_source := decodeSetMetaReq(wrappedReq)
			if !xmem.conflict_resolver(doc_meta_source, doc_meta_target, xmem.source_cr_mode, xmem.Logger()) {
				xmem.Logger().Debugf("%v doc %v failed source side conflict resolution. source meta=%v, target meta=%v. no need to send\n", xmem.Id(), key, doc_meta_source, doc_meta_target)
				bigDoc_noRep_map[wrappedReq.UniqueKey] = true
			} else {
				xmem.Logger().Debugf("%v doc %v succeeded source side conflict resolution. source meta=%v, target meta=%v. sending it to target\n", xmem.Id(), key, doc_meta_source, doc_meta_target)
			}
		} else if ok && resp.Status == mc.NOT_MY_VBUCKET {
			bigDoc_noRep_map[wrappedReq.UniqueKey] = false
		} else {
			if !ok || resp == nil {
				xmem.Logger().Debugf("%v batchGetMeta: doc %s is not found in target system, send it", xmem.Id(), key)
			} else if resp.Status == mc.KEY_ENOENT {
				xmem.Logger().Debugf("%v batchGetMeta: doc %s does not exist on target. Skip conflict resolution and send the doc", xmem.Id(), key)
			} else {
				xmem.Logger().Infof("%v batchGetMeta: memcached response for doc %s has error status %v. Skip conflict resolution and send the doc", xmem.Id(), key, resp.Status)
			}
		}
	}

	xmem.Logger().Debugf("%v Done with batchGetMeta, bigDoc_noRep_map=%v\n", xmem.Id(), bigDoc_noRep_map)
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
			xmem.buf.adjustRequest(item, index)
			xmem.Logger().Debugf("key=%v\n", item.Req.Key)
			xmem.Logger().Debugf("opcode=%v\n", item.Req.Opcode)
		}
		bytes := item.Req.Bytes()

		for j := 0; j < numOfRetry; j++ {
			err, rev := xmem.writeToClient(xmem.client_for_setMeta, xmem.packageRequest(1, bytes), true)
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
	poolName := xmem.getPoolName()
	return base.ConnPoolMgr().GetPool(poolName), nil
}

func (xmem *XmemNozzle) getOrCreateConnPool() (pool base.ConnPool, err error) {
	poolName := xmem.getPoolName()
	if !xmem.config.demandEncryption {
		pool, err = base.ConnPoolMgr().GetOrCreatePool(poolName, xmem.config.connectStr, xmem.config.bucketName, xmem.config.bucketName, xmem.config.password, xmem.config.connPoolSize)
		if err != nil {
			return nil, err
		}
	} else {
		//create a pool of SSL connection
		poolName := xmem.getPoolName()
		hostName := utils.GetHostName(xmem.config.connectStr)
		remote_mem_port, err := utils.GetPortNumber(xmem.config.connectStr)
		if err != nil {
			return nil, err
		}

		if xmem.config.memcached_ssl_port != 0 {
			xmem.Logger().Infof("%v Get or create ssl over memcached connection, memcached_ssl_port=%v\n", xmem.Id(), int(xmem.config.memcached_ssl_port))
			pool, err = base.ConnPoolMgr().GetOrCreateSSLOverMemPool(poolName, hostName, xmem.config.bucketName, xmem.config.bucketName, xmem.config.password,
				xmem.config.connPoolSize, int(xmem.config.memcached_ssl_port), xmem.config.certificate, xmem.config.san_in_certificate)

		} else {
			xmem.Logger().Infof("%v Get or create ssl over proxy connection", xmem.Id())
			pool, err = base.ConnPoolMgr().GetOrCreateSSLOverProxyPool(poolName, hostName, xmem.config.bucketName, xmem.config.bucketName, xmem.config.password,
				xmem.config.connPoolSize, int(remote_mem_port), int(xmem.config.local_proxy_port), int(xmem.config.remote_proxy_port), xmem.config.certificate)
		}
		if err != nil {
			return nil, err
		}
	}
	return pool, nil
}

func (xmem *XmemNozzle) initializeConnection() (err error) {
	poolName := xmem.getPoolName()
	xmem.Logger().Debugf("%v xmem.config= %v", xmem.Id(), xmem.config.connectStr)
	xmem.Logger().Debugf("%v poolName=%v", xmem.Id(), poolName)
	pool, err := xmem.getOrCreateConnPool()
	if err != nil && err == base.WrongConnTypeError {
		//there is a stale pool, the remote cluster settings are changed
		base.ConnPoolMgr().RemovePool(poolName)
		pool, err = xmem.getOrCreateConnPool()
	}
	if err != nil {
		return
	}
	xmem.connType = pool.ConnType()

	// this needs to be done after xmem.connType is set
	xmem.composeUserAgent()

	memClient_setMeta, err := pool.GetNew()

	if err != nil {
		return
	}

	memClient_getMeta, err := pool.GetNew()
	if err != nil {
		return
	}

	xmem.client_for_setMeta = newXmemClient(SetMetaClientName, xmem.config.readTimeout,
		xmem.config.writeTimeout, memClient_setMeta,
		xmem.config.maxRetry, xmem.config.max_read_downtime, xmem.Logger())
	xmem.client_for_getMeta = newXmemClient(GetMetaClientName, xmem.config.readTimeout,
		xmem.config.writeTimeout, memClient_getMeta,
		xmem.config.maxRetry, xmem.config.max_read_downtime, xmem.Logger())

	// send helo command to setMeta and getMeta clients
	xmem.sendHELO(true /*setMeta*/)
	xmem.sendHELO(false /*setMeta */)

	xmem.Logger().Infof("%v done with initializeConnection.", xmem.Id())
	return err
}

func (xmem *XmemNozzle) getPoolName() string {
	return xmem.config.connPoolNamePrefix + base.KeyPartsDelimiter + "Couch_Xmem_" + xmem.config.connectStr + base.KeyPartsDelimiter + xmem.config.bucketName
}

func (xmem *XmemNozzle) initNewBatch() {
	xmem.Logger().Debugf("%v initializing a new batch", xmem.Id())
	xmem.batch = newBatch(uint32(xmem.config.maxCount), uint32(xmem.config.maxSize), xmem.Logger())
	atomic.StoreUint32(&xmem.cur_batch_count, 0)
}

func (xmem *XmemNozzle) initialize(settings map[string]interface{}) error {
	err := xmem.config.initializeConfig(settings)
	if err != nil {
		return err
	}
	xmem.dataChan = make(chan *base.WrappedMCRequest, xmem.config.maxCount*10)
	xmem.bytes_in_dataChan = 0
	xmem.dataChan_control = make(chan bool, 1)
	xmem.dataChan_control <- true

	xmem.batches_ready_queue = make(chan *dataBatch, 100)

	xmem.counter_received = 0
	xmem.counter_sent = 0

	//init a new batch
	xmem.initNewBatch()

	xmem.receive_token_ch = make(chan int, xmem.config.maxCount*2)
	xmem.buf = newReqBuffer(uint16(xmem.config.maxCount*2), uint16(float64(xmem.config.maxCount)*0.2), xmem.receive_token_ch, xmem.Logger())

	xmem.receiver_finch = make(chan bool, 1)
	xmem.checker_finch = make(chan bool, 1)

	xmem.Logger().Infof("%v About to start initializing connection", xmem.Id())
	err = xmem.initializeConnection()
	if err == nil {
		xmem.Logger().Infof("%v Connection initialization completed successfully", xmem.Id())
	} else {
		xmem.Logger().Errorf("%v Error initializating connections. err=%v", xmem.Id(), err)
	}

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
				xmem.Logger().Infof("%v has stopped. Exiting", xmem.Id())
				goto done
			}

			response, err, rev := xmem.readFromClient(xmem.client_for_setMeta, true)
			if err != nil {
				if err == PartStoppedError {
					goto done
				} else if err == fatalError {
					// if possible, log the corresponding request to facilitate debugging
					if response != nil {
						pos := xmem.getPosFromOpaque(response.Opaque)
						wrappedReq, err := xmem.buf.slot(pos)
						if err == nil && wrappedReq != nil {
							req := wrappedReq.Req
							if req != nil && req.Opaque == response.Opaque {
								xmem.Logger().Infof("%v received fatal error from setMeta client. req=%v, seqno=%v, response=%v\n", xmem.Id(), req, wrappedReq.Seqno, response)
							}
						}
					}
					goto done
				} else if err == badConnectionError || err == connectionClosedError {
					xmem.Logger().Errorf("%v The connection is ruined. Repair the connection and retry.", xmem.Id())
					xmem.repairConn(xmem.client_for_setMeta, err.Error(), rev)
				}
			} else if response == nil {
				panic("readFromClient returned nil error and nil response")
			} else if response.Status != mc.SUCCESS && !isIgnorableMCError(response.Status) {
				if isTemporaryMCError(response.Status) {
					// target may be overloaded. increase backoff factor to alleviate stress on target
					xmem.client_for_setMeta.incrementBackOffFactor()

					// error is temporary. resend doc
					pos := xmem.getPosFromOpaque(response.Opaque)
					xmem.Logger().Errorf("%v Received temporary error in setMeta response. Response status=%v, err = %v, response=%v\n", xmem.Id(), response.Status.String(), err, response)
					//resend and reset the retry=0 as retry is an indicator of network status,
					//here we have received the response, so reset retry=0
					_, err = xmem.buf.modSlot(pos, xmem.resendWithReset)
				} else {
					var req *mc.MCRequest = nil
					var seqno uint64
					pos := xmem.getPosFromOpaque(response.Opaque)
					wrappedReq, err := xmem.buf.slot(pos)
					if err == nil && wrappedReq != nil {
						req = wrappedReq.Req
						seqno = wrappedReq.Seqno
						if req != nil && req.Opaque == response.Opaque {
							// found matching request
							if response.Status == mc.NOT_MY_VBUCKET {
								vb_err := fmt.Errorf("Received error %v on vb %v\n", base.ErrorNotMyVbucket, req.VBucket)
								xmem.handleVBError(req.VBucket, vb_err)
							} else if response.Status == mc.KEY_ENOENT {
								// KEY_ENOENT response is returned when a SetMeta request is on an existing document,
								// i.e., doc with non-0 CAS, and the target cannot find the document.
								// This is unlikely, but possible in the following scenario:
								// 1. a document is created on source
								// 2. the Document is replicated to target
								// 3. the document is deleted on target and tombstone is removed.
								// 4. the document is updated on source. this produces a SetWithMeta request with
								//    non-0 CAS and will get ENOENT response from target
								// this is an extremely rare scenario considering the fact that tombstones are kept for 7 days.
								// make GOXDCR exhibit the same behavior as that of 3.x XDCR -> log the error and resend the doc
								xmem.Logger().Errorf("%v received KEY_ENOENT error from setMeta client. response status=%v, opcode=%v, seqno=%v, req.Key=%v, req.Cas=%v, req.Extras=%v\n", xmem.Id(), response.Status, response.Opcode, seqno, string(req.Key), req.Cas, req.Extras)
								_, err = xmem.buf.modSlot(pos, xmem.resendWithReset)
							} else {
								// for other non-temporary errors, repair connections
								xmem.Logger().Errorf("%v received error response from setMeta client. Repairing connection. response status=%v, opcode=%v, seqno=%v, req.Key=%v, req.Cas=%v, req.Extras=%v\n", xmem.Id(), response.Status, response.Opcode, seqno, string(req.Key), req.Cas, req.Extras)
								xmem.repairConn(xmem.client_for_setMeta, "error response from memcached", rev)
							}
						} else if req != nil {
							xmem.Logger().Debugf("%v Got the response, response.Opaque=%v, req.Opaque=%v\n", xmem.Id(), response.Opaque, req.Opaque)
						} else {
							xmem.Logger().Debugf("%v Got the response, pos=%v, req in that pos is nil\n", xmem.Id(), pos)
						}
					}
				}
			} else {
				//raiseEvent
				pos := xmem.getPosFromOpaque(response.Opaque)
				wrappedReq, err := xmem.buf.slot(pos)
				if err != nil {
					xmem.Logger().Errorf("%v xmem buffer is in invalid state", xmem.Id())
					xmem.handleGeneralError(errors.New("xmem buffer is in invalid state"))
					goto done
				}
				var req *mc.MCRequest
				var seqno uint64
				var committing_time time.Duration
				var resp_wait_time time.Duration
				if wrappedReq != nil {
					req = wrappedReq.Req
					seqno = wrappedReq.Seqno
					committing_time = time.Since(wrappedReq.Start_time)
					resp_wait_time = time.Since(wrappedReq.Send_time)
				}

				if req != nil && req.Opaque == response.Opaque {
					xmem.Logger().Debugf("%v Got the response, key=%s, status=%v\n", xmem.Id(), req.Key, response.Status)

					additionalInfo := DataSentEventAdditional{Seqno: seqno,
						IsOptRepd:      xmem.optimisticRep(req),
						Opcode:         req.Opcode,
						IsExpirySet:    (binary.BigEndian.Uint32(req.Extras[4:8]) != 0),
						VBucket:        req.VBucket,
						Req_size:       req.Size(),
						Commit_time:    committing_time,
						Resp_wait_time: resp_wait_time,
					}
					xmem.RaiseEvent(common.NewEvent(common.DataSent, nil, xmem, nil, additionalInfo))

					//feedback the most current commit_time to xmem.config.respTimeout
					xmem.adjustRespTimeout(resp_wait_time)

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

func (xmem *XmemNozzle) handleVBError(vbno uint16, err error) {
	additionalInfo := &base.VBErrorEventAdditional{vbno, err, base.VBErrorType_Target}
	xmem.RaiseEvent(common.NewEvent(common.VBErrorEncountered, nil, xmem, nil, additionalInfo))
}

func (xmem *XmemNozzle) adjustRespTimeout(committing_time time.Duration) {
	oldRespTimeout := xmem.getRespTimeout()
	factor := committing_time.Seconds() / oldRespTimeout.Seconds()
	atomic.StorePointer(&xmem.config.respTimeout, unsafe.Pointer(&committing_time))
	xmem.adjustMaxIdleCount(factor)
}

func (xmem *XmemNozzle) adjustMaxIdleCount(factor float64) {
	old_maxIdleCount := xmem.getMaxIdleCount()
	new_maxIdleCount := uint32(float64(old_maxIdleCount) * factor)
	new_maxIdleCount = uint32(math.Max(float64(new_maxIdleCount), float64(old_maxIdleCount)))
	atomic.StoreUint32(&xmem.config.maxIdleCount, new_maxIdleCount)
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

// check if memcached response status indicates fatal error, which usually requires pipeline restart
func isFatalMCError(resp_status mc.Status) bool {
	switch resp_status {
	case mc.NO_BUCKET:
		fallthrough
	case mc.NOT_MY_VBUCKET:
		return true
	default:
		return false
	}
}

// check if memcached response status indicates error of temporary nature, which requires retrying corresponding requests
func isTemporaryMCError(resp_status mc.Status) bool {
	switch resp_status {
	case mc.TMPFAIL:
		fallthrough
	case mc.ENOMEM:
		fallthrough
	case mc.EBUSY:
		fallthrough
	case mc.NOT_INITIALIZED:
		return true
	default:
		return false
	}
}

// check if memcached response status indicates ignorable error, which requires no corrective action at all
func isIgnorableMCError(resp_status mc.Status) bool {
	switch resp_status {
	case mc.KEY_EEXISTS:
		return true
	default:
		return false
	}
}

// get max idle count adjusted by backoff_factor
func (xmem *XmemNozzle) getMaxIdleCount() uint32 {
	return atomic.LoadUint32(&(xmem.config.maxIdleCount))
}

// get max idle count adjusted by backoff_factor
func (xmem *XmemNozzle) getAdjustedMaxIdleCount() uint32 {
	max_idle_count := xmem.getMaxIdleCount()
	backoff_factor := math.Max(float64(xmem.client_for_getMeta.getBackOffFactor()), float64(xmem.client_for_setMeta.getBackOffFactor()))
	backoff_factor = math.Min(float64(10), backoff_factor)

	//if client_for_getMeta.backoff_factor > 0 or client_for_setMeta.backoff_factor > 0, it means the target system is possibly under load, need to be more patient before
	//declare the stuckness.
	if int(backoff_factor) > 1 {
		max_idle_count = max_idle_count * uint32(backoff_factor)
	}

	return max_idle_count
}

func (xmem *XmemNozzle) getRespTimeout() time.Duration {
	return *((*time.Duration)(atomic.LoadPointer(&(xmem.config.respTimeout))))
}

func (xmem *XmemNozzle) getOptiRepThreshold() uint32 {
	return atomic.LoadUint32(&(xmem.config.optiRepThreshold))
}

func (xmem *XmemNozzle) selfMonitor(finch chan bool, waitGrp *sync.WaitGroup) {
	defer waitGrp.Done()
	ticker := time.NewTicker(xmem.config.selfMonitorInterval)
	defer ticker.Stop()
	statsTicker := time.NewTicker(xmem.config.statsInterval)
	defer statsTicker.Stop()
	var sent_count uint32 = 0
	var received_count uint32 = 0
	var resp_waitingConfirm_count int = 0
	var repairCount_setMeta = 0
	var repairCount_getMeta = 0
	var count uint64
	var freeze_counter uint32 = 0
	for {
		select {
		case <-finch:
			goto done
		case <-ticker.C:
			if xmem.validateRunningState() != nil {
				xmem.Logger().Infof("%v has stopped. Exiting", xmem.Id())
				goto done
			}
			received_count = atomic.LoadUint32(&xmem.counter_received)
			buffer_count := xmem.buf.itemCountInBuffer()
			xmem_id := xmem.Id()
			isOpen := xmem.IsOpen()
			buffer_size := xmem.buf.bufferSize()
			empty_slots_pos := xmem.buf.empty_slots_pos
			batches_ready_queue := xmem.batches_ready_queue
			dataChan := xmem.dataChan
			xmem_count_sent := atomic.LoadUint32(&xmem.counter_sent)
			count++

			if xmem_count_sent == sent_count && int(buffer_count) == resp_waitingConfirm_count &&
				(len(xmem.dataChan) > 0 || buffer_count != 0) &&
				repairCount_setMeta == xmem.client_for_setMeta.repairCount() &&
				repairCount_getMeta == xmem.client_for_getMeta.repairCount() {
				freeze_counter++
			} else {
				freeze_counter = 0
			}
			sent_count = xmem_count_sent
			resp_waitingConfirm_count = int(buffer_count)
			repairCount_setMeta = xmem.client_for_setMeta.repairCount()
			repairCount_getMeta = xmem.client_for_getMeta.repairCount()
			if count == 10 {
				xmem.Logger().Debugf("%v- freeze_counter=%v, xmem.counter_sent=%v, len(xmem.dataChan)=%v, receive_count-%v, cur_batch_count=%v\n", xmem_id, freeze_counter, xmem_count_sent, len(dataChan), received_count, atomic.LoadUint32(&xmem.cur_batch_count))
				xmem.Logger().Debugf("%v open=%v checking..., %v item unsent, %v items waiting for response, %v batches ready\n", xmem_id, isOpen, len(xmem.dataChan), int(buffer_size)-len(empty_slots_pos), len(batches_ready_queue))
				count = 0
			}
			max_idle_count := xmem.getAdjustedMaxIdleCount()
			if freeze_counter > max_idle_count {
				xmem.Logger().Errorf("%v hasn't sent any item out for %v ticks, %v data in queue, flowcontrol=%v, con_retry_limit=%v, backoff_factor for client_setMeta is %v, backoff_factor for client_getMeta is %v", xmem_id, max_idle_count, len(dataChan), buffer_count <= xmem.buf.notify_threshold, xmem.client_for_setMeta.continuous_write_failure_counter, xmem.client_for_setMeta.getBackOffFactor(), xmem.client_for_getMeta.getBackOffFactor())
				xmem.Logger().Infof("%v open=%v checking..., %v item unsent, received %v items, sent %v items, %v items waiting for response, %v batches ready\n", xmem_id, isOpen, len(dataChan), received_count, xmem_count_sent, int(buffer_size)-len(empty_slots_pos), len(batches_ready_queue))
				//				utils.DumpStack(xmem.Logger())
				//the connection might not be healthy, it should not go back to connection pool
				xmem.client_for_setMeta.markConnUnhealthy()
				xmem.client_for_getMeta.markConnUnhealthy()
				xmem.handleGeneralError(errors.New("Xmem is stuck"))
				goto done
			}
		case <-statsTicker.C:
			xmem.RaiseEvent(common.NewEvent(common.StatsUpdate, nil, xmem, nil, []int{len(xmem.dataChan), xmem.bytesInDataChan()}))
		}
	}
done:
	xmem.Logger().Infof("%v selfMonitor routine exits", xmem.Id())

}

func (xmem *XmemNozzle) check(finch chan bool, waitGrp *sync.WaitGroup) {
	defer waitGrp.Done()
	ticker := time.NewTicker(xmem.getRespTimeout())
	defer ticker.Stop()
	for {
		select {
		case <-finch:
			goto done
		case <-ticker.C:
			if xmem.validateRunningState() != nil {
				goto done
			}
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
	xmem.Logger().Infof("%v checking routine exits", xmem.Id())
}

func (xmem *XmemNozzle) checkTimeout(req *bufferedMCRequest, pos uint16) (bool, error) {
	if req.timedout {
		return false, nil
	}

	if req.num_of_retry > xmem.config.maxRetry {
		req.timedout = true
		err := errors.New(fmt.Sprintf("%v Failed to resend document %s, has tried to resend it %v, maximum retry %v reached",
			xmem.Id(), req.req.Req.Key, req.num_of_retry, xmem.config.maxRetry))
		xmem.Logger().Error(err.Error())

		xmem.repairConn(xmem.client_for_setMeta, err.Error(), xmem.client_for_setMeta.repairCount())
		return false, err
	}

	respWaitTime := time.Since(*req.sent_time)
	if respWaitTime > xmem.timeoutDuration(req.num_of_retry) {
		modified, err := xmem.resend(req, pos)

		return modified, err
	}
	return false, nil
}

func (xmem *XmemNozzle) timeoutDuration(numofRetry int) time.Duration {
	duration := xmem.getRespTimeout()
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

	return true, err
}

func (xmem *XmemNozzle) resendWithReset(req *bufferedMCRequest, pos uint16) (bool, error) {
	xmem.Logger().Debugf("%v Retry sending %s, retry=%v, and reset retry=0 on successful send", xmem.Id(), req.req.Req.Key, req.num_of_retry)
	err := xmem.sendSingleSetMeta(false, req.req, pos, xmem.config.maxRetry)

	if err != nil {
		req.err = err

	} else {
		//reset to 0
		req.num_of_retry = 0
	}

	return true, err

}

func (xmem *XmemNozzle) resendForNewConn(req *bufferedMCRequest, pos uint16) (bool, error) {
	xmem.Logger().Debugf("%v Retry sending %s, retry=%v", xmem.Id(), req.req.Req.Key, req.num_of_retry)
	err := xmem.sendSingleSetMeta(false, req.req, pos, xmem.config.maxRetry)
	if err != nil {
		req.err = err
		//report error
		xmem.handleGeneralError(err)
		return false, err
	}
	req.num_of_retry = 0
	return true, err
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
		var avg_wait_time float64 = 0
		counter_sent := atomic.LoadUint32(&xmem.counter_sent)
		if counter_sent > 0 {
			avg_wait_time = float64(atomic.LoadUint32(&xmem.counter_waittime)) / float64(counter_sent)
		}
		return fmt.Sprintf("%v state =%v connType=%v received %v items, sent %v items, %v items waiting to confirm, %v in queue, %v in current batch, avg wait time is %vms, size of last ten batches processed %v, len(batches_ready_queue)=%v\n", xmem.Id(), xmem.State(), connType, atomic.LoadUint32(&xmem.counter_received), atomic.LoadUint32(&xmem.counter_sent), xmem.buf.itemCountInBuffer(), len(xmem.dataChan), atomic.LoadUint32(&xmem.cur_batch_count), avg_wait_time, xmem.getLastTenBatchSize(), len(xmem.batches_ready_queue))
	} else {
		return fmt.Sprintf("%v state =%v ", xmem.Id(), xmem.State())
	}
}

func (xmem *XmemNozzle) handleGeneralError(err error) {
	err1 := xmem.SetState(common.Part_Error)
	if err1 == nil {
		xmem.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, xmem, nil, err))
		xmem.Logger().Errorf("%v Raise error condition %v\n", xmem.Id(), err)
	} else {
		xmem.Logger().Infof("%v in shutdown process, err=%v is ignored\n", xmem.Id(), err)
	}
}

func (xmem *XmemNozzle) optimisticRep(req *mc.MCRequest) bool {
	if req != nil {
		return uint32(req.Size()) < xmem.getOptiRepThreshold()
	}
	return true
}

// ignore errors around HELO command since they are not critical to replication
func (xmem *XmemNozzle) sendHELO(setMeta bool) {
	if setMeta {
		utils.SendHELO(xmem.client_for_setMeta.getMemClient(), xmem.setMetaUserAgent, xmem.config.readTimeout, xmem.config.writeTimeout, xmem.Logger())
	} else {
		utils.SendHELO(xmem.client_for_getMeta.getMemClient(), xmem.getMetaUserAgent, xmem.config.readTimeout, xmem.config.writeTimeout, xmem.Logger())
	}
}

// compose user agent string for HELO command
func (xmem *XmemNozzle) composeUserAgent() {
	xmem.composeUserAgentForClient(true)
	xmem.composeUserAgentForClient(false)
}

func (xmem *XmemNozzle) composeUserAgentForClient(setMeta bool) {
	var buffer bytes.Buffer
	buffer.WriteString("Goxdcr Xmem ")
	if setMeta {
		buffer.WriteString(SetMetaClientName)
	} else {
		buffer.WriteString(GetMetaClientName)
	}
	buffer.WriteString(" SourceBucket:" + xmem.sourceBucketName)
	buffer.WriteString(" TargetBucket:" + xmem.config.bucketName)
	buffer.WriteString(" ConnType:" + xmem.connType.String())
	if setMeta {
		xmem.setMetaUserAgent = buffer.String()
	} else {
		xmem.getMetaUserAgent = buffer.String()
	}
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
	state := xmem.State()
	if state == common.Part_Stopping || state == common.Part_Stopped || state == common.Part_Error {
		return PartStoppedError
	}
	return nil
}

func (xmem *XmemNozzle) writeToClient(client *xmemClient, bytes []byte, renewTimeout bool) (error, int) {
	backoffFactor := client.getBackOffFactor()
	if backoffFactor > 0 {
		time.Sleep(time.Duration(backoffFactor) * default_backoff_wait_time)
	}

	conn, rev, err := xmem.getConn(client, false, renewTimeout)
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
			xmem.repairConn(client, err.Error(), rev)

		} else if isNetError(err) {
			client.reportOpFailure(false)
			wait_time := time.Duration(math.Pow(2, float64(client.curWriteFailureCounter()))*float64(rand.Intn(10)/10)) * xmem.config.writeTimeout
			xmem.Logger().Errorf("%v batchSend Failed, retry after %v\n", xmem.Id(), wait_time)
			time.Sleep(wait_time)

		}

		return err, rev
	}
}

func (xmem *XmemNozzle) readFromClient(client *xmemClient, resetReadTimeout bool) (*mc.MCResponse, error, int) {

	//set the read timeout for the underlying connection
	_, rev, err := xmem.getConn(client, resetReadTimeout, false)
	if err != nil {
		client.logger.Errorf("%v Can't read from client %v, failed to get connection, err=%v", xmem.Id(), client.name, err)
		return nil, err, rev
	}

	memClient := client.getMemClient()
	if memClient == nil {
		return nil, errors.New("memcached client is not set"), client.repairCount()
	}
	response, err := memClient.Receive()

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
				return nil, badConnectionError, rev
			} else if isNetError(err) {
				client.reportOpFailure(true)
				return response, err, rev
			} else if strings.HasPrefix(errMsg, "bad magic") {
				//the connection to couchbase server is ruined. it is not supposed to return response with bad magic number
				//now the only sensible thing to do is to repair the connection, then retry
				client.logger.Infof("%v err=%v\n", xmem.Id(), err)
				return nil, badConnectionError, rev
			}
		} else {
			if isFatalMCError(response.Status) && response.Status != mc.NOT_MY_VBUCKET {
				// restart pipeline for fatal mc errors
				high_level_err := "Received error response from memcached in target cluster."
				xmem.handleGeneralError(errors.New(high_level_err))
				client.logger.Errorf("%v %v. err=%v, response=%v", xmem.Id(), high_level_err, err, response)
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

	numOfRetry := 0
	backoffTime := default_newconn_backoff_time
	for {
		memClient, err := pool.GetNew()

		if err == nil {
			repaired := client.repairConn(memClient, rev, xmem.Id())
			if repaired {
				if client == xmem.client_for_setMeta {
					xmem.sendHELO(true /*setMeta*/)
					go xmem.onSetMetaConnRepaired()
				} else {
					xmem.sendHELO(false /*setMeta*/)
				}
			}

			xmem.Logger().Infof("%v - The connection for %v has been repaired\n", xmem.Id(), client.name)
			break

		} else {
			if numOfRetry < xmem.config.maxRetry {
				numOfRetry++
				// exponential backoff
				xmem.Logger().Infof("%v Error setting up new connections. err=%v. Retrying for %vth time after %v.", xmem.Id(), err, numOfRetry, backoffTime)
				time.Sleep(backoffTime)
				backoffTime *= 2
			} else {
				high_level_err := fmt.Sprintf("Failed to repair connections to target cluster after %v retries.", numOfRetry)
				xmem.handleGeneralError(errors.New(high_level_err))
				xmem.Logger().Errorf("%v - Failed to repair connections for %v. err=%v\n", xmem.Id(), client.name, err)
				return err
			}
		}
	}
	return nil
}

func (xmem *XmemNozzle) onSetMetaConnRepaired() error {
	size := xmem.buf.bufferSize()
	count := 0
	for i := 0; i < int(size); i++ {
		sent, err := xmem.buf.modSlot(uint16(i), xmem.resendForNewConn)
		if err != nil {
			return err
		}
		if sent {
			count++
		}
	}
	xmem.Logger().Infof("%v - %v unresponded items are resent\n", xmem.Id(), count)
	return nil

}

func (xmem *XmemNozzle) ConnStr() string {
	return xmem.config.connectStr
}

func (xmem *XmemNozzle) packageRequest(count int, reqs_bytes []byte) []byte {
	if xmem.ConnType() == base.SSLOverProxy {
		bytes := make([]byte, 8+len(reqs_bytes))
		binary.BigEndian.PutUint32(bytes[0:4], uint32(len(reqs_bytes)))
		binary.BigEndian.PutUint32(bytes[4:8], uint32(count))
		xmem.Logger().Debugf("batch_size =%v, batch_count=%v\n", len(reqs_bytes), count)
		copy(bytes[8:8+len(reqs_bytes)], reqs_bytes)
		return bytes
	} else {
		return reqs_bytes
	}
}

func (xmem *XmemNozzle) UpdateSettings(settings map[string]interface{}) error {
	optimisticReplicationThreshold, err := utils.GetIntSettingFromSettings(settings, metadata.OptimisticReplicationThreshold)
	if err != nil {
		return err
	}
	atomic.StoreUint32(&xmem.config.optiRepThreshold, uint32(optimisticReplicationThreshold))
	return nil
}

func (xmem *XmemNozzle) dataChanControl() {
	if xmem.bytesInDataChan() < max_datachannelSize {
		select {
		case xmem.dataChan_control <- true:
		default:
			//dataChan_control is already flagged.
		}
	}
}

func (xmem *XmemNozzle) writeToDataChan(item *base.WrappedMCRequest) {
	select {
	case <-xmem.dataChan_control:
		xmem.dataChan <- item
		atomic.AddInt32(&xmem.bytes_in_dataChan, int32(item.Req.Size()))
		xmem.dataChanControl()
	}
}

func (xmem *XmemNozzle) readFromDataChan() (*base.WrappedMCRequest, error) {
	item, ok := <-xmem.dataChan

	if !ok {
		//data channel has been closed, part must have been stopped
		return nil, PartStoppedError
	}

	atomic.AddInt32(&xmem.bytes_in_dataChan, int32(0-item.Req.Size()))
	xmem.dataChanControl()
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

func (xmem *XmemNozzle) getLastTenBatchSize() []uint32 {
	return *((*[]uint32)(atomic.LoadPointer(&xmem.last_ten_batches_size)))
}

func (xmem *XmemNozzle) recordBatchSize(batchSize uint32) {
	last_ten_batches_size := xmem.getLastTenBatchSize()
	for i := 9; i > 0; i-- {
		last_ten_batches_size[i] = last_ten_batches_size[i-1]
	}
	last_ten_batches_size[0] = batchSize
	atomic.StorePointer(&xmem.last_ten_batches_size, unsafe.Pointer(&last_ten_batches_size))
}
