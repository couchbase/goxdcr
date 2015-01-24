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
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

//configuration settings for XmemNozzle
const (
	SETTING_RESP_TIMEOUT           = "resp_timeout"
	XMEM_SETTING_DEMAND_ENCRYPTION = "demandEncryption"
	XMEM_SETTING_CERTIFICATE       = "certificate"
	XMEM_SETTING_REMOTE_PROXY_PORT = "remote_proxy_port"
	XMEM_SETTING_LOCAL_PROXY_PORT  = "local_proxy_port"

	//default configuration
	default_numofretry          int           = 10
	default_resptimeout         time.Duration = 500 * time.Millisecond
	default_dataChannelSize                   = 5000
	default_batchExpirationTime               = 300 * time.Millisecond
	default_maxRetryInterval                  = 30 * time.Second
	default_writeTimeOut        time.Duration = time.Duration(1) * time.Second
	default_readTimeout         time.Duration = time.Duration(1) * time.Second
	default_maxIdleCount        int           = 30
	default_selfMonitorInterval time.Duration = 1 * time.Second
	default_demandEncryption    bool          = false
	default_max_downtime        time.Duration = 3 * time.Second
)

const (
	GET_WITH_META    = mc.CommandCode(0xa0)
	SET_WITH_META    = mc.CommandCode(0xa2)
	DELETE_WITH_META = mc.CommandCode(0xa8)
)

var xmem_setting_defs base.SettingDefinitions = base.SettingDefinitions{SETTING_BATCHCOUNT: base.NewSettingDef(reflect.TypeOf((*int)(nil)), true),
	SETTING_BATCHSIZE:              base.NewSettingDef(reflect.TypeOf((*int)(nil)), true),
	SETTING_NUMOFRETRY:             base.NewSettingDef(reflect.TypeOf((*int)(nil)), false),
	SETTING_RESP_TIMEOUT:           base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	SETTING_WRITE_TIMEOUT:          base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	SETTING_READ_TIMEOUT:           base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	SETTING_MAX_RETRY_INTERVAL:     base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	SETTING_SELF_MONITOR_INTERVAL:  base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	SETTING_BATCH_EXPIRATION_TIME:  base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	SETTING_OPTI_REP_THRESHOLD:     base.NewSettingDef(reflect.TypeOf((*int)(nil)), true),
	XMEM_SETTING_DEMAND_ENCRYPTION: base.NewSettingDef(reflect.TypeOf((*bool)(nil)), false),
	XMEM_SETTING_CERTIFICATE:       base.NewSettingDef(reflect.TypeOf((*[]byte)(nil)), false),
	XMEM_SETTING_REMOTE_PROXY_PORT: base.NewSettingDef(reflect.TypeOf((*uint16)(nil)), false),
	XMEM_SETTING_LOCAL_PROXY_PORT:  base.NewSettingDef(reflect.TypeOf((*uint16)(nil)), false)}

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
	timedout     bool
	reservation  int
}

func newBufferedMCRequest(request *base.WrappedMCRequest, reservationNum int) *bufferedMCRequest {
	return &bufferedMCRequest{req: request,
		sent_time:    time.Now(),
		num_of_retry: 0,
		err:          nil,
		timedout:     false,
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
	notify_threshold  uint16
	fin_ch            chan bool
	logger            *log.CommonLogger
	notifych_lock     sync.RWMutex
	seqno_sorted_list map[uint16][]int
	seqno_list_lock   map[uint16]*sync.RWMutex
}

func newReqBuffer(size uint16, threshold uint16, logger *log.CommonLogger) *requestBuffer {
	logger.Debugf("Create a new request buffer of size %d\n", size)
	buf := &requestBuffer{
		slots:             make([]*bufferedMCRequest, size, size),
		sequences:         make([]uint16, size),
		empty_slots_pos:   make(chan uint16, size),
		size:              size,
		notifych:          nil,
		notify_threshold:  threshold,
		fin_ch:            make(chan bool, 1),
		logger:            logger,
		notifych_lock:     sync.RWMutex{},
		seqno_sorted_list: make(map[uint16][]int),
		seqno_list_lock:   make(map[uint16]*sync.RWMutex)}

	logger.Debug("Slots is initialized")

	//initialize the empty_slots_pos
	buf.initializeEmptySlotPos()

	buf.initializeSeqnoSortedList()

	logger.Debugf("new request buffer of size %d is created\n", size)
	return buf
}

func (buf *requestBuffer) initializeSeqnoSortedList() {
	for i := 0; i < 1024; i++ {
		buf.seqno_sorted_list[uint16(i)] = []int{}
		buf.seqno_list_lock[uint16(i)] = &sync.RWMutex{}
	}
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
	if pos < 0 || int(pos) >= len(buf.slots) {
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

	if req == nil || req.req == nil {
		return nil, nil
	} else {
		return req.req, nil
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

		buf.removeSeqnoFromSeqnoList(req.req)
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
		if req != nil {

			if req.reservation == reservation_num {
				req = nil
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
	buf.logger.Debugf("enSlot: pos=%d\n", pos)

	err := buf.validatePos(pos)
	if err != nil {
		return err
	}
	r := buf.slots[pos]

	if r == nil {
		return errors.New("Slot is not initialized, must be reserved first.")
	} else {
		if r.reservation != reservationNum {
			buf.logger.Errorf("Can't enSlot %d, doesn't have the reservation, %v", pos, r)
			return errors.New(fmt.Sprintf("Can't enSlot %d, doesn't have the reservation", pos))
		}
		r.req = req
		buf.updateSeqnoList(req)
	}
	buf.logger.Debugf("slot %d is occupied\n", pos)
	return nil
}

func (buf *requestBuffer) bufferSize() uint16 {
	return buf.size
}

func (buf *requestBuffer) updateSeqnoList(req *base.WrappedMCRequest) {
	vbno := req.Req.VBucket
	buf.seqno_list_lock[vbno].Lock()
	defer buf.seqno_list_lock[vbno].Unlock()
	sorted_seqno_list, ok := buf.seqno_sorted_list[vbno]
	if !ok {
		sorted_seqno_list = []int{}
	}

	seqno := req.Seqno
	oldlen := len(sorted_seqno_list)
	index := sort.Search(oldlen, func(i int) bool { return sorted_seqno_list[i] > int(seqno) })
	newlist := []int{}
	if index < len(sorted_seqno_list) && sorted_seqno_list[index] > int(seqno) {
		newlist = append(newlist, sorted_seqno_list[0:index]...)
		newlist = append(newlist, int(seqno))
		newlist = append(newlist, sorted_seqno_list[index:]...)
	} else {
		//not founded, this seqno is bigger than all seqno in sorted_seqno_list, added to the end
		newlist = append(newlist, sorted_seqno_list...)
		newlist = append(newlist, int(seqno))
	}
	newlen := len(newlist)

	buf.seqno_sorted_list[vbno] = newlist
	if !sort.IntsAreSorted(buf.seqno_sorted_list[vbno]) || newlen != oldlen+1 {
		panic(fmt.Sprintf("list %v is not valid. vbno=%v", buf.seqno_sorted_list[vbno], vbno))
	}
}

func (buf *requestBuffer) removeSeqnoFromSeqnoList(req *base.WrappedMCRequest) {
	vbno := req.Req.VBucket
	buf.seqno_list_lock[vbno].Lock()
	defer buf.seqno_list_lock[vbno].Unlock()
	sorted_seqno_list_vb := buf.seqno_sorted_list[vbno]

	seqno := req.Seqno
	index := sort.Search(len(sorted_seqno_list_vb), func(i int) bool {
		return sorted_seqno_list_vb[i] >= int(seqno)
	})
	if index < len(sorted_seqno_list_vb) && sorted_seqno_list_vb[index] == int(seqno) {
		var newlist []int = []int{}
		if index > 0 {
			newlist = append(newlist, sorted_seqno_list_vb[0:index]...)
		}
		if index < len(sorted_seqno_list_vb)-1 {
			newlist = append(newlist, sorted_seqno_list_vb[index+1:]...)
		}

		if len(newlist) != len(sorted_seqno_list_vb)-1 {
			panic(fmt.Sprintf("removeSeqnoFromSeqnoList is wrong. newlist=%v, oldlist=%v\n", newlist, sorted_seqno_list_vb))
		}

		buf.seqno_sorted_list[vbno] = newlist

	} else {
		panic(fmt.Sprintf("seqno %v is not in the sorted_seqno_list %v for vb=%v\n, index=%v", int(seqno), sorted_seqno_list_vb, vbno, index))
	}

}

func (buf *requestBuffer) getSmallestSeqnoInBuffer(vbno uint16) (uint64, error) {
	buf.seqno_list_lock[vbno].RLock()
	defer buf.seqno_list_lock[vbno].RUnlock()
	if len(buf.seqno_sorted_list[vbno]) > 0 {
		return uint64(buf.seqno_sorted_list[vbno][0]), nil
	} else {
		return 0, errors.New(fmt.Sprintf("No data items for vbucket %v in the buffer", vbno))
	}
}

func (buf *requestBuffer) hasSeqno(vbno uint16, seqno uint64) bool {
	buf.seqno_list_lock[vbno].RLock()
	defer buf.seqno_list_lock[vbno].RUnlock()
	seqno_sorted_list, ok := buf.seqno_sorted_list[vbno]
	if ok {
		index := sort.Search(len(seqno_sorted_list), func(i int) bool {
			return seqno_sorted_list[i] >= int(seqno)
		})
		return index < len(seqno_sorted_list) && seqno_sorted_list[index] == int(seqno)
	}
	return false
}

func (buf *requestBuffer) itemCountInBuffer() uint16 {
	var count uint16 = 0
	for _, seqnolistForvb := range buf.seqno_sorted_list {
		count = count + uint16(len(seqnolistForvb))
	}

	return count
}

/************************************
/* struct xmemConfig
*************************************/
type xmemConfig struct {
	baseConfig
	bucketName string
	//the duration to wait for the batch-sending to finish
	certificate       []byte
	demandEncryption  bool
	remote_proxy_port uint16
	local_proxy_port  uint16
	respTimeout       time.Duration
	max_downtime      time.Duration
	logger            *log.CommonLogger
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
		bucketName:        "",
		respTimeout:       default_resptimeout,
		demandEncryption:  default_demandEncryption,
		certificate:       []byte{},
		remote_proxy_port: 0,
		local_proxy_port:  0,
		max_downtime:      default_max_downtime,
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
			if val, ok := settings[XMEM_SETTING_REMOTE_PROXY_PORT]; ok {
				config.remote_proxy_port = val.(uint16)
			} else {
				return errors.New("demandEncryption=true, but remote_proxy_port is not set in settings")
			}
			if val, ok := settings[XMEM_SETTING_LOCAL_PROXY_PORT]; ok {
				config.local_proxy_port = val.(uint16)
			} else {
				return errors.New("demandEncryption=true, but local_proxy_port is not set in settings")
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
}

func newXmemClient(name string, read_timeout, write_timeout time.Duration,
	client *mcc.Client, poolName string, max_continuous_failure int, max_downtime time.Duration, logger *log.CommonLogger) *xmemClient {
	logger.Infof("xmem client %v is created with read_timeout=%v, write_timeout=%v, retry_limit=%v", name, read_timeout, write_timeout, max_continuous_failure)
	return &xmemClient{name: name,
		memClient:                  client,
		continuous_failure_counter: 0,
		logger:                 logger,
		poolName:               poolName,
		read_timeout:           read_timeout,
		write_timeout:          write_timeout,
		max_downtime:           max_downtime,
		max_continuous_failure: max_continuous_failure,
		healthy:                true,
		lock:                   sync.RWMutex{}}
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
	client.healthy = true

}

func (client *xmemClient) isConnHealthy() bool {
	return client.healthy
}

func (client *xmemClient) getConn(readTimeout bool, writeTimeout bool) (io.ReadWriteCloser, error) {
	if client.memClient == nil {
		return nil, errors.New("memcached client is not set")
	}

	if !client.isConnHealthy() {
		return nil, badConnectionError
	}

	conn := client.memClient.Hijack()
	if readTimeout {
		conn.(*net.TCPConn).SetReadDeadline(time.Now().Add(client.read_timeout))
	}
	if writeTimeout {
		conn.(*net.TCPConn).SetWriteDeadline(time.Now().Add(client.write_timeout))
	}
	return conn, nil
}

func (client *xmemClient) repairConn(memClient *mcc.Client) {
	client.lock.Lock()
	defer client.lock.Unlock()

	client.memClient.Close()
	client.memClient = memClient
	client.continuous_failure_counter = 0
	client.healthy = true
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
	} else {
		client.memClient.Close()
	}
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

	dataChan_lock sync.RWMutex
	//data channel to accept the incoming data
	dataChan chan *base.WrappedMCRequest
	//the total size of data (in byte) queued in dataChan
	bytes_in_dataChan int

	//memcached client connected to the target bucket
	client_for_setMeta *xmemClient
	client_for_getMeta *xmemClient

	//configurable parameter
	config xmemConfig

	//queue for ready batches
	batches_ready chan *dataBatch

	//batch to be accumulated
	batch           *dataBatch
	batch_move_lock sync.Mutex

	childrenWaitGrp sync.WaitGroup

	//buffer for the sent, but not yet confirmed data
	buf *requestBuffer

	//conflict resolover
	conflict_resolver ConflictResolver

	sender_finch      chan bool
	receiver_finch    chan bool
	checker_finch     chan bool
	selfMonitor_finch chan bool

	counter_sent     int
	counter_received int
	start_time       time.Time

	//the big seqno that is confirmed to be received on the vbucket
	//it is still possible that smaller seqno hasn't been received
	maxseqno_received_map map[uint16]uint64
}

func NewXmemNozzle(id string,
	connPoolNamePrefix string,
	connPoolConnSize int,
	connectString string,
	bucketName string,
	password string,
	logger_context *log.LoggerContext) *XmemNozzle {

	//callback functions from GenServer
	var msg_callback_func gen_server.Msg_Callback_Func
	var exit_callback_func gen_server.Exit_Callback_Func
	var error_handler_func gen_server.Error_Handler_Func

	server := gen_server.NewGenServer(&msg_callback_func,
		&exit_callback_func, &error_handler_func, logger_context, "XmemNozzle")
	part := NewAbstractPartWithLogger(id, server.Logger())

	xmem := &XmemNozzle{GenServer: server,
		AbstractPart:          part,
		bOpen:                 true,
		lock_bOpen:            sync.RWMutex{},
		dataChan_lock:         sync.RWMutex{},
		dataChan:              nil,
		client_for_setMeta:    nil,
		client_for_getMeta:    nil,
		config:                newConfig(server.Logger()),
		batches_ready:         nil,
		batch:                 nil,
		batch_move_lock:       sync.Mutex{},
		childrenWaitGrp:       sync.WaitGroup{},
		buf:                   nil,
		receiver_finch:        make(chan bool, 1),
		checker_finch:         make(chan bool, 1),
		sender_finch:          make(chan bool, 1),
		selfMonitor_finch:     make(chan bool, 1),
		counter_sent:          0,
		counter_received:      0,
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
	xmem.Logger().Info("Xmem starting ....")
	defer xmem.Logger().Infof("%v took %vs to start\n", xmem.Id(), time.Since(t).Seconds())

	xmem.SetState(common.Part_Starting)

	err := xmem.initialize(settings)
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
	close(xmem.dataChan)
	close(xmem.batches_ready)

	err = xmem.Stop_server()
	if err != nil {
		return err
	}

	err = xmem.SetState(common.Part_Stopped)
	if err == nil {
		xmem.Logger().Debugf("XmemNozzle %v is stopped\n", xmem.Id())
	}
	return err
}

func (xmem *XmemNozzle) IsOpen() bool {
	ret := xmem.bOpen
	return ret
}

func (xmem *XmemNozzle) batchReady() error {
	//move the batch to ready batches channel
	xmem.batch_move_lock.Lock()
	defer func() {
		xmem.batch_move_lock.Unlock()
		if r := recover(); r != nil {
			if xmem.validateRunningState() == nil {
				// report error only when xmem is still in running state
				xmem.handleGeneralError(errors.New(fmt.Sprintf("%v", r)))
			}
		}
		xmem.Logger().Debugf("%v End moving batch, %v batches ready\n", xmem.Id(), len(xmem.batches_ready))
	}()
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
	xmem.dataChan_lock.Lock()
	defer func() {
		xmem.dataChan_lock.Unlock()
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

	dataChan_len_old := len(xmem.dataChan)
	received_old := xmem.counter_received

	xmem.dataChan <- request
	xmem.counter_received++

	if xmem.counter_received <= received_old {
		panic(fmt.Sprintf("counter_received_old=%v, counter_received=%v, dataChan_len_old=%v, dataChan_len=%v",
			received_old, xmem.counter_received, dataChan_len_old, len(xmem.dataChan)))
	}

	if xmem.counter_received != len(xmem.dataChan)+xmem.counter_sent {
		xmem.Logger().Errorf("received=%v, sent=%v data buffered=%v", xmem.counter_received, xmem.counter_sent, len(xmem.dataChan))
	}

	size := request.Req.Size()
	xmem.bytes_in_dataChan = xmem.bytes_in_dataChan + size

	//accumulate the batchCount and batchSize
	if xmem.batch.accumuBatch(request, xmem.optimisticRep) {
		xmem.batchReady()
	}

	//raise DataReceived event
	additionalInfo := make(map[string]interface{})
	additionalInfo[STATS_QUEUE_SIZE] = len(xmem.dataChan)
	additionalInfo[STATS_QUEUE_SIZE_BYTES] = xmem.bytes_in_dataChan
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

	reqs_bytes := []byte{}

	index_reservation_map := make(map[uint16]int)
	batch_replicated_count := 0

	for i := 0; i < count; i++ {
		//check xmem's state, if it is already in stopping or stopped state, return
		err = xmem.validateRunningState()
		if err != nil {
			return err
		}

		item, ok := <-xmem.dataChan
		xmem.counter_sent++
		if xmem.counter_received != len(xmem.dataChan)+xmem.counter_sent {
			xmem.Logger().Errorf(fmt.Sprintf("received=%v, sent=%v data buffered=%v", xmem.counter_received, xmem.counter_sent, len(xmem.dataChan)))
		}

		if !ok {
			//data channel is close, part must be stopped
			return PartStoppedError
		}

		if item != nil {
			xmem.bytes_in_dataChan = xmem.bytes_in_dataChan - item.Req.Size()

			if needSend(item.Req, batch, xmem.logger) {
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
			}
		} else {
			//lost on conflict resolution on source side
			//TODO: raise event
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
		err = xmem.writeToClient(client, item_byte)
		if err == nil {
			return nil
		} else if err == badConnectionError {
			xmem.repairConn(client, err.Error())
		}
	}
	return err
}

func (xmem *XmemNozzle) sendSetMeta_internal(batch *dataBatch) error {
	var err error
	if batch != nil {
		count := batch.count()

		xmem.Logger().Infof("Send batch count=%d\n", count)

		xmem.Logger().Debugf("So far, xmem %v processed %d items", xmem.Id(), xmem.counter_sent)

		bigDoc_noRep_map, err := xmem.batchGetMeta(batch.bigDoc_map)
		if err != nil {
			xmem.Logger().Errorf("batchGetMeta failed. err=%v\n", err)
		} else {
			batch.bigDoc_noRep_map = bigDoc_noRep_map
		}

		//batch send
		err = xmem.batchSetMetaWithRetry(batch, xmem.config.maxRetry)
		if err != nil && err != PartStoppedError {
			xmem.handleGeneralError(err)

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
	opaque_key_map := make(map[uint32]string)
	opaque_seqno_map := make(map[uint32]uint64)
	waitGrp := &sync.WaitGroup{}
	receiver_fin_ch := make(chan bool, 1)

	err_list := []error{}

	//launch the receiver
	waitGrp.Add(1)
	go func(count int, finch chan bool, opaque_key_map map[uint32]string, opaque_seqno_map map[uint32]uint64, respMap map[string]*mc.MCResponse, timeout_duration time.Duration, err_list []error, waitGrp *sync.WaitGroup, logger *log.CommonLogger) {
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

				response, err := xmem.readFromClient(xmem.client_for_getMeta)
				if err != nil {
					if err == PartStoppedError {
						return
					} else if err == badConnectionError || err == connectionTimeoutReachLimitError || err == connectionClosedError || err == fatalError {
						xmem.repairConn(xmem.client_for_getMeta, err.Error())
						return
					}
					err_list = append(err_list, err)

				} else {
					key, ok := opaque_key_map[response.Opaque]
					if ok {
						//success
						respMap[key] = response

						seqno, ok := opaque_seqno_map[response.Opaque]
						if ok {
							additionalInfo := make(map[string]interface{})
							additionalInfo[EVENT_ADDI_DOC_KEY] = key
							additionalInfo[EVENT_ADDI_SEQNO] = seqno
							xmem.RaiseEvent(common.GetMetaReceived, nil, xmem, nil, additionalInfo)
						}
					}
				}

				if len(respMap) >= count {
					logger.Infof("Expected %v response, got all", count)
					return
				}

			}
		}

	}(len(bigDoc_map), receiver_fin_ch, opaque_key_map, opaque_seqno_map, respMap, 1*time.Second, err_list, waitGrp, xmem.Logger())


	var sequence uint16 = uint16(time.Now().UnixNano())
	reqs_bytes := []byte{}
	counter := 0
	opaque := xmem.getOpaque(0, sequence)
	for key, originalReq := range bigDoc_map {
		req := xmem.composeRequestForGetMeta(key, originalReq.Req.VBucket, opaque)
		reqs_bytes = append(reqs_bytes, req.Bytes()...)
		opaque_key_map[opaque] = key
		opaque_seqno_map[opaque] = originalReq.Seqno
		opaque++
		counter++

		additionalInfo := make(map[string]interface{})
		additionalInfo[EVENT_ADDI_DOC_KEY] = key
		additionalInfo[EVENT_ADDI_SEQNO] = originalReq.Seqno

		xmem.RaiseEvent(common.GetMetaSent, nil, xmem, nil, additionalInfo)

		if counter > 50 {
			err := xmem.writeToClient(xmem.client_for_getMeta, xmem.packageRequest(counter, reqs_bytes))
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

	var err error
	if counter > 0 {
		err = xmem.writeToClient(xmem.client_for_getMeta, xmem.packageRequest(counter, reqs_bytes))
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

	for key, resp := range respMap {
		if resp.Status == mc.SUCCESS {
			doc_meta_target := xmem.decodeGetMetaResp([]byte(key), resp)
			doc_meta_source := decodeSetMetaReq(bigDoc_map[key].Req)
			if !xmem.conflict_resolver(doc_meta_source, doc_meta_target, xmem.logger) {
				xmem.Logger().Debugf("doc %v (%v)failed on conflict resolution to %v, no need to send\n", key, doc_meta_source, doc_meta_target)
				bigDoc_noRep_map[key] = true
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
	//	Deleted    (24-27): 0x00000000 (0)
	//  Flags      (28-31): 0x00000001 (1)
	//  Exptime    (32-35): 0x00000007 (7)
	//  Seqno      (36-43): 0x0000000000000009 (9)
	//  ConfRes    (44)   : 0x01 (1)
	ret.deletion = (binary.BigEndian.Uint32(extras[0:4]) != 0)
	ret.flags = binary.BigEndian.Uint32(extras[4:8])
	ret.expiry = binary.BigEndian.Uint32(extras[8:12])
	ret.revSeq = binary.BigEndian.Uint64(extras[12:20])
	ret.cas = resp.Cas

	return ret

}

func decodeSetMetaReq(req *mc.MCRequest) documentMetadata {
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
	if xmem.client_for_setMeta != nil {
		if adjustRequest {
			xmem.adjustRequest(item, index)
			xmem.Logger().Debugf("key=%v\n", item.Req.Key)
			xmem.Logger().Debugf("opcode=%v\n", item.Req.Opcode)
		}
		bytes := item.Req.Bytes()
		return xmem.writeToClient(xmem.client_for_setMeta, xmem.packageRequest(1, bytes))
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
		hostName := utils.GetHostName(xmem.config.connectStr)
		remote_mem_port, err := utils.GetPortNumber(xmem.config.connectStr)
		if err != nil {
			return nil, err
		}
		pool, err = base.ConnPoolMgr().GetOrCreateSSLPool(poolName, hostName, xmem.config.bucketName, xmem.config.bucketName, xmem.config.password,
			base.DefaultConnectionSize, int(remote_mem_port), int(xmem.config.local_proxy_port), int(xmem.config.remote_proxy_port), xmem.config.certificate)
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
	if err != nil {
		return
	}

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
	xmem.dataChan = make(chan *base.WrappedMCRequest, xmem.config.maxCount*100)
	xmem.bytes_in_dataChan = 0
	xmem.batches_ready = make(chan *dataBatch, 100)

	xmem.counter_received = 0
	xmem.counter_sent = 0

	//enable send
	//	xmem.send_allow_ch <- true

	//init a new batch
	xmem.initNewBatch()

	xmem.buf = newReqBuffer(uint16(xmem.config.maxCount*100), uint16(float64(xmem.config.maxCount)*0.2), xmem.Logger())

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
		default:
			if xmem.validateRunningState() != nil {
				xmem.Logger().Infof("xmem %v has stopped", xmem.Id())
				goto done
			}

			if xmem.buf.itemCountInBuffer() > 0 {
				response, err := xmem.readFromClient(xmem.client_for_setMeta)
				if err != nil {
					if err == PartStoppedError || err == connectionClosedError || err == fatalError {
						goto done
					} else if err == badConnectionError {
						xmem.repairConn(xmem.client_for_setMeta, err.Error())
						xmem.Logger().Error("The connection is ruined. Repair the connection and retry.")
					} else if response != nil && isRecoverableMCError(response.Status) {

						pos := xmem.getPosFromOpaque(response.Opaque)
						xmem.Logger().Infof("%v pos=%d, Received error = %v in response, err = %v, response=%v\n", xmem.Id(), pos, response.Status.String(), err, response.Bytes())
						_, err = xmem.buf.modSlot(pos, xmem.resend)
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
					if wrappedReq != nil {
						req = wrappedReq.Req
						seqno = wrappedReq.Seqno
					}

					if req != nil && req.Opaque == response.Opaque {
						xmem.Logger().Debugf("%v Got the response, key=%s, status=%v\n", xmem.Id(), req.Key, response.Status)
						if response.Status != mc.SUCCESS {
							xmem.Logger().Debugf("*****%v-%v Got the response, response.Status=%v for doc %s*******\n", xmem.config.bucketName, xmem.Id(), response.Status, req.Key)
							xmem.Logger().Debugf("req.key=%s, req.Extras=%v, req.Cas=%v, req=%v\n", req.Key, req.Extras, req.Cas, req)
						}

						additionalInfo := make(map[string]interface{})
						additionalInfo[EVENT_ADDI_SEQNO] = seqno
						additionalInfo[EVENT_ADDI_OPT_REPD] = xmem.optimisticRep(req)

						//add additional information about hiseqno, which is going to be used
						//for checkpointing
						highseqno, err := xmem.getCurSeenHighSeqno(wrappedReq)
						if err == nil {
							additionalInfo[EVENT_ADDI_HISEQNO] = highseqno
						}

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
	}

done:
	xmem.Logger().Infof("%v receiveResponse exits\n", xmem.Id())
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

func isSeriousError(err error) bool {
	if err == nil {
		return false
	}
	netError, ok := err.(*net.OpError)
	return err == syscall.EPIPE || err.Error() == "use of closed network connection" || (ok && (!netError.Temporary() && !netError.Timeout()))
}

func isRecoverableMCError(resp_status mc.Status) bool {
	switch resp_status {
	case mc.TMPFAIL:
		return true
	default:
		return false
	}
}

func (xmem *XmemNozzle) selfMonitor(finch chan bool, waitGrp *sync.WaitGroup) {
	defer waitGrp.Done()
	ticker := time.Tick(xmem.config.selfMonitorInterval)
	var sent_count int = 0
	var count uint64
	freeze_counter := 0
	idle_counter := 0
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
			if xmem.counter_sent == sent_count {
				if len(xmem.dataChan) > 0 {
					freeze_counter++
					idle_counter = 0
				} else {
					freeze_counter = 0
					idle_counter++
				}
			} else {
				freeze_counter = 0
				idle_counter = 0
			}
			sent_count = xmem.counter_sent
			if count == 10 {
				xmem.Logger().Debugf("%v- freeze_counter=%v, xmem.counter_sent=%v, len(xmem.dataChan)=%v, receive_count-%v, cur_batch_count=%v\n", xmem.Id(), freeze_counter, xmem.counter_sent, len(xmem.dataChan), xmem.counter_received, xmem.batch.count())
				xmem.Logger().Debugf("%v open=%v checking..., %v item unsent, received %v items, sent %v items, %v items waiting for response, %v batches ready, current batch timeout at %v, current batch expire_ch size is %v\n", xmem.Id(), xmem.IsOpen(), len(xmem.dataChan), xmem.counter_received, xmem.counter_sent, int(xmem.buf.bufferSize())-len(xmem.buf.empty_slots_pos), len(xmem.batches_ready), xmem.batch.start_time.Add(xmem.batch.expiring_duration), len(xmem.batch.expire_ch))
				count = 0
			}
			if freeze_counter > xmem.config.maxIdleCount {
				xmem.Logger().Errorf("Xmem hasn't sent any item out for %v ticks, %v data in queue, flowcontrol=%v, con_retry_limit=%v", xmem.config.maxIdleCount, len(xmem.dataChan), xmem.buf.itemCountInBuffer() <= xmem.buf.notify_threshold, xmem.client_for_setMeta.continuous_failure_counter)
				xmem.Logger().Infof("%v open=%v checking..., %v item unsent, received %v items, sent %v items, %v items waiting for response, %v batches ready, current batch timeout at %v, current batch expire_ch size is %v\n", xmem.Id(), xmem.IsOpen(), len(xmem.dataChan), xmem.counter_received, xmem.counter_sent, int(xmem.buf.bufferSize())-len(xmem.buf.empty_slots_pos), len(xmem.batches_ready), xmem.batch.start_time.Add(xmem.batch.expiring_duration), len(xmem.batch.expire_ch))
				//				utils.DumpStack(xmem.Logger())
				//the connection might not be healthy, it should not go back to connection pool
				xmem.client_for_setMeta.markConnUnhealthy()
				xmem.client_for_getMeta.markConnUnhealthy()
				xmem.handleGeneralError(errors.New("Xmem is stuck"))
				goto done
			}
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
				xmem.batchReady()
			default:
			}
			xmem.Logger().Debugf("%v open=%v checking..., %v item unsent, received %v items, sent %v items, %v items waiting for response, %v batches ready, current batch timeout at %v, current batch expire_ch size is %v\n", xmem.Id(), xmem.IsOpen(), len(xmem.dataChan), xmem.counter_received, xmem.counter_sent, int(xmem.buf.bufferSize())-len(xmem.buf.empty_slots_pos), len(xmem.batches_ready), xmem.batch.start_time.Add(xmem.batch.expiring_duration), len(xmem.batch.expire_ch))
			if !xmem.config.demandEncryption {
				size := xmem.buf.bufferSize()
				timeoutCheckFunc := xmem.checkTimeout
				for i := 0; i < int(size); i++ {
					_, err := xmem.buf.modSlot(uint16(i), timeoutCheckFunc)
					if err != nil {
						xmem.Logger().Errorf("%v Failed to check timeout %v\n", xmem.Id(), err)
						if err == badConnectionError {
							xmem.repairConn(xmem.client_for_setMeta, err.Error())
						}
						break
					}
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
		xmem.handleGeneralError(err)
		return false, err
	}
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
	xmem.Logger().Debugf("%v Retry sending %s, retry=%v", xmem.Id(), req.req.Req.Key, req.num_of_retry)
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
	if xmem.State() == common.Part_Running {
		return fmt.Sprintf("Xmem %v state =%v received %v items, sent %v items, %v items waiting to confirm, %v in queue", xmem.Id(), xmem.State(), xmem.counter_received, xmem.counter_sent, int(xmem.buf.bufferSize())-len(xmem.buf.empty_slots_pos), len(xmem.dataChan))
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

func (xmem *XmemNozzle) getConn(client *xmemClient, readTimeout bool, writeTimeout bool) (io.ReadWriteCloser, error) {
	err := xmem.validateRunningState()
	if err != nil {
		return nil, err
	}
	ret, err := client.getConn(readTimeout, writeTimeout)
	return ret, err
}

func (xmem *XmemNozzle) validateRunningState() error {
	if xmem.State() == common.Part_Stopping || xmem.State() == common.Part_Stopped || xmem.State() == common.Part_Error {
		return PartStoppedError
	}
	return nil
}

func (xmem *XmemNozzle) writeToClient(client *xmemClient, bytes []byte) error {
	conn, err := xmem.getConn(client, false, true)
	if err != nil {
		return err
	}

	_, err = conn.Write(bytes)
	if err == nil {
		client.reportOpSuccess()
		return err
	} else {
		xmem.Logger().Errorf("%v writeToClient error: %s\n", xmem.Id(), fmt.Sprint(err))

		if isSeriousError(err) {
			xmem.repairConn(client, err.Error())

		} else if isNetError(err) {
			client.reportOpFailure()
			xmem.Logger().Errorf("%v batchSend Failed, retry later\n", xmem.Id())
			time.Sleep(time.Duration(2^(client.curFailureCounter())) * xmem.config.writeTimeout * time.Second)

		}

		return err
	}
}

func (xmem *XmemNozzle) readFromClient(client *xmemClient) (*mc.MCResponse, error) {
	if client.memClient == nil {
		return nil, errors.New("memcached client is not set")
	}

	//set the read timeout for the underlying connection
	_, err := xmem.getConn(client, true, false)
	if err != nil {
		client.logger.Errorf("Can't read from client %v, failed to get connection, err=%v", client.name, err)
		return nil, err
	}

	response, err := client.memClient.Receive()
	if err != nil {
		xmem.Logger().Debugf("%v readFromClient: %v\n", xmem.Id(), err)
		if err == io.EOF {
			return nil, connectionClosedError
		} else if isSeriousError(err) {
			xmem.repairConn(client, err.Error())
			return nil, badConnectionError
		} else if isNetError(err) {
			client.reportOpFailure()
			return response, err
		} else if strings.HasPrefix(err.Error(), "bad magic") {
			//the connection to couchbase server is ruined. it is not supposed to return response with bad magic number
			//now the only sensible thing to do is to repair the connection, then retry
			client.logger.Infof("err=%v\n", err)
			xmem.repairConn(client, err.Error())
			return nil, badConnectionError
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
			xmem.handleGeneralError(err)
			client.logger.Errorf("fatal err=%v", err)
			return response, fatalError
		} else if err == response {
			//response.Status != SUCCESSFUL, in this case, gomemcached return the response as err as well
			//return the err as nil so that caller can differentiate the application error from transport
			//error
			return response, nil
		}
	} else {
		//if no error, reset the client retry counter
		client.reportOpSuccess()
	}
	return response, err
}

func (xmem *XmemNozzle) repairConn(client *xmemClient, reason string) error {

	if xmem.validateRunningState() != nil {
		xmem.Logger().Infof("%v is not running, no need to repairConn", xmem.Id())
		return nil
	}

	xmem.Logger().Errorf("%v connection %v is broken due to %v, try to repair...\n", xmem.Id(), client.name, reason)
	pool, err := xmem.getConnPool()
	if err != nil {
		return err
	}
	memClient_setMeta, err := pool.Get()

	if err == nil {
		client.repairConn(memClient_setMeta)
		if client == xmem.client_for_setMeta {
			xmem.onSetMetaConnRepaired()
		}

		xmem.Logger().Infof("%v - The connection for %v is repaired\n", xmem.Id(), client.name)

	} else {
		xmem.Logger().Infof("%v - Connection for %v repair failed\n", xmem.Id(), client.name)
		xmem.handleGeneralError(err)
		return err
	}
	return nil
}

func (xmem *XmemNozzle) onSetMetaConnRepaired() {
	size := xmem.buf.bufferSize()
	for i := 0; i < int(size); i++ {
		xmem.buf.modSlot(uint16(i), xmem.resend)
	}
	xmem.Logger().Infof("%v - The unresponded items are resent\n", xmem.Id())

}

func (xmem *XmemNozzle) getCurSeenHighSeqno(wrappedReq *base.WrappedMCRequest) (uint64, error) {
	req := wrappedReq.Req
	seqno := wrappedReq.Seqno
	vbno := req.VBucket
	smallestSeqnoInBuf, err := xmem.buf.getSmallestSeqnoInBuffer(vbno)
	if err != nil {
		return 0, err
	}

	//update the maxseqno_received_map
	cur_maxseqno, ok := xmem.maxseqno_received_map[vbno]
	if !ok || cur_maxseqno < seqno {
		xmem.maxseqno_received_map[vbno] = seqno
	}
	if seqno < smallestSeqnoInBuf {
		//invalid state
		panic(fmt.Sprintf("Seqno %v is not tracked in buf, the smallestSeqnoInBuf = %v, vbno=%v, sorted_seqno_list=%v", seqno, smallestSeqnoInBuf, vbno, xmem.buf.seqno_sorted_list[vbno]))
	}
	if seqno == smallestSeqnoInBuf {

		highseqno := smallestSeqnoInBuf
		//see if all seqno between smallest seqno and maxseqno are all received
		for i := smallestSeqnoInBuf; i <= xmem.maxseqno_received_map[vbno]; i++ {
			if xmem.buf.hasSeqno(vbno, i) {
				break
			} else {
				highseqno = i
			}
		}

		return highseqno, nil
	}
	return 0, errors.New("No high sequence number yet")
}

func (xmem *XmemNozzle) ConnStr() string {
	return xmem.config.connectStr
}

func (xmem *XmemNozzle) packageRequest(count int, reqs_bytes []byte) []byte {
	if xmem.config.demandEncryption {
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
	pool := base.ConnPoolMgr().GetPool(getPoolName(xmem.config))
	if pool != nil {
		pool.ReleaseConnections()
	}

}
