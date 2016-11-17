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
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	mc "github.com/couchbase/gomemcached"
	base "github.com/couchbase/goxdcr/base"
	common "github.com/couchbase/goxdcr/common"
	gen_server "github.com/couchbase/goxdcr/gen_server"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/simple_utils"
	"github.com/couchbase/goxdcr/utils"
	"io/ioutil"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	SETTING_UPLOAD_WINDOW_SIZE = "upload_window_size"
	SETTING_CONNECTION_TIMEOUT = "connection_timeout"
	SETTING_RETRY_INTERVAL     = "retry_interval"

	//default configuration
	default_numofretry_capi          int           = 6
	default_retry_interval_capi      time.Duration = 30 * time.Millisecond
	default_maxRetryInterval_capi                  = 30 * time.Second
	default_writeTimeout_capi        time.Duration = time.Duration(1) * time.Second
	default_readTimeout_capi         time.Duration = time.Duration(10) * time.Second
	default_upload_window_size       int           = 3                 // erlang xdcr value
	default_connection_timeout                     = 180 * time.Second // erlang xdcr value
	default_selfMonitorInterval_capi time.Duration = 300 * time.Millisecond
	default_maxIdleCount_capi        int           = 30
	default_write_to_part_ch_timeout time.Duration = 10 * time.Second
)

var capi_setting_defs base.SettingDefinitions = base.SettingDefinitions{SETTING_BATCHCOUNT: base.NewSettingDef(reflect.TypeOf((*int)(nil)), true),
	SETTING_BATCHSIZE:             base.NewSettingDef(reflect.TypeOf((*int)(nil)), true),
	SETTING_OPTI_REP_THRESHOLD:    base.NewSettingDef(reflect.TypeOf((*int)(nil)), true),
	SETTING_BATCH_EXPIRATION_TIME: base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	SETTING_NUMOFRETRY:            base.NewSettingDef(reflect.TypeOf((*int)(nil)), false),
	SETTING_RETRY_INTERVAL:        base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	SETTING_WRITE_TIMEOUT:         base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	SETTING_READ_TIMEOUT:          base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	SETTING_MAX_RETRY_INTERVAL:    base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	SETTING_UPLOAD_WINDOW_SIZE:    base.NewSettingDef(reflect.TypeOf((*int)(nil)), false),
	SETTING_CONNECTION_TIMEOUT:    base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false)}

var NewEditsKey = "new_edits"
var DocsKey = "docs"
var MetaKey = "meta"
var BodyKey = "base64"
var IdKey = "id"
var RevKey = "rev"
var ExpirationKey = "expiration"
var FlagsKey = "flags"
var DeletedKey = "deleted"
var AttReasonKey = "att_reason"
var InvalidJson = "invalid_json"

var BodyPartsPrefix = "{\"new_edits\":false,\"docs\":["
var BodyPartsSuffix = "]}"
var BodyPartsDelimiter = ","
var SizePartDelimiter = "\r\n"

var CouchFullCommitKey = "X-Couch-Full-Commit"

var MalformedOK = "{\"ok\":true}\n"
var MalformedResponseError = "Received malformed response from tcp connection"

/************************************
/* struct capiBatch
*************************************/
type capiBatch struct {
	dataBatch
	vbno uint16
}

/************************************
/* struct capiConfig
*************************************/
type capiConfig struct {
	baseConfig
	uploadWindowSize int
	// timeout of capi rest calls
	connectionTimeout time.Duration
	retryInterval     time.Duration
	certificate       []byte
	// key = vbno; value = couchApiBase for capi calls, e.g., http://127.0.0.1:9500/target%2Baa3466851d268241d9465826d3d8dd11%2f13
	// this map serves two purposes: 1. provides a list of vbs that the capi is responsible for
	// 2. provides the couchApiBase for each of the vbs
	vbCouchApiBaseMap map[uint16]string
}

func newCapiConfig(logger *log.CommonLogger) capiConfig {
	return capiConfig{
		baseConfig: baseConfig{maxCount: -1,
			maxSize:             -1,
			maxRetry:            default_numofretry_capi,
			writeTimeout:        default_writeTimeout_capi,
			readTimeout:         default_readTimeout_capi,
			maxRetryInterval:    default_maxRetryInterval_capi,
			selfMonitorInterval: default_selfMonitorInterval_capi,
			connectStr:          "",
			username:            "",
			password:            "",
		},
		uploadWindowSize:  default_upload_window_size,
		connectionTimeout: default_connection_timeout,
		retryInterval:     default_retry_interval_capi,
	}

}

func (config *capiConfig) initializeConfig(settings map[string]interface{}) error {
	err := utils.ValidateSettings(capi_setting_defs, settings, config.logger)

	if err == nil {
		config.baseConfig.initializeConfig(settings)

		if val, ok := settings[SETTING_UPLOAD_WINDOW_SIZE]; ok {
			config.uploadWindowSize = val.(int)
		}
		if val, ok := settings[SETTING_CONNECTION_TIMEOUT]; ok {
			config.connectionTimeout = val.(time.Duration)
		}
		if val, ok := settings[SETTING_RETRY_INTERVAL]; ok {
			config.retryInterval = val.(time.Duration)
		}
	}
	return err
}

/************************************
/* struct CapiNozzle
*************************************/
type CapiNozzle struct {

	//parent inheritance
	gen_server.GenServer
	AbstractPart

	bOpen      bool
	lock_bOpen sync.RWMutex

	//data channels to accept the incoming data, one for each vb
	vb_dataChan_map map[uint16]chan *base.WrappedMCRequest
	//the total number of items queued in all data channels
	items_in_dataChan int32
	//the total size of data (in bytes) queued in all data channels
	bytes_in_dataChan int32

	client      *net.TCPConn
	lock_client sync.RWMutex

	//configurable parameter
	config capiConfig

	//queue for ready batches
	batches_ready chan *capiBatch

	batches_nonempty_ch chan bool

	//batches to be accumulated, one for each vb
	vb_batch_map      map[uint16]*capiBatch
	vb_batch_map_lock chan bool

	childrenWaitGrp sync.WaitGroup

	sender_finch      chan bool
	checker_finch     chan bool
	selfMonitor_finch chan bool

	counter_sent      uint32
	counter_received  uint32
	start_time        time.Time
	handle_error      bool
	lock_handle_error sync.RWMutex
	dataObj_recycler  base.DataObjRecycler
	topic             string
}

func NewCapiNozzle(id string,
	topic string,
	connectString string,
	username string,
	password string,
	certificate []byte,
	vbCouchApiBaseMap map[uint16]string,
	dataObj_recycler base.DataObjRecycler,
	logger_context *log.LoggerContext) *CapiNozzle {

	//callback functions from GenServer
	var msg_callback_func gen_server.Msg_Callback_Func
	var exit_callback_func gen_server.Exit_Callback_Func
	var error_handler_func gen_server.Error_Handler_Func

	server := gen_server.NewGenServer(&msg_callback_func,
		&exit_callback_func, &error_handler_func, logger_context, "CapiNozzle")
	part := NewAbstractPartWithLogger(id, server.Logger())

	capi := &CapiNozzle{GenServer: server, /*gen_server.GenServer*/
		AbstractPart:        part,                           /*part.AbstractPart*/
		bOpen:               true,                           /*bOpen	bool*/
		lock_bOpen:          sync.RWMutex{},                 /*lock_bOpen	sync.RWMutex*/
		config:              newCapiConfig(server.Logger()), /*config	capiConfig*/
		batches_ready:       nil,                            /*batches_ready chan *capiBatch*/
		childrenWaitGrp:     sync.WaitGroup{},               /*childrenWaitGrp sync.WaitGroup*/
		sender_finch:        make(chan bool, 1),
		checker_finch:       make(chan bool, 1),
		selfMonitor_finch:   make(chan bool, 1),
		batches_nonempty_ch: make(chan bool, 1),
		//		send_allow_ch:    make(chan bool, 1), /*send_allow_ch chan bool*/
		counter_sent:      0,
		handle_error:      true,
		lock_handle_error: sync.RWMutex{},
		counter_received:  0,
		dataObj_recycler:  dataObj_recycler,
		topic:             topic,
	}

	capi.config.connectStr = connectString
	capi.config.username = username
	capi.config.password = password
	capi.config.certificate = certificate
	capi.config.vbCouchApiBaseMap = vbCouchApiBaseMap

	msg_callback_func = nil
	exit_callback_func = capi.onExit
	error_handler_func = capi.handleGeneralError

	return capi

}

func (capi *CapiNozzle) IsOpen() bool {
	capi.lock_bOpen.RLock()
	defer capi.lock_bOpen.RUnlock()
	return capi.bOpen
}

func (capi *CapiNozzle) Open() error {
	capi.lock_bOpen.Lock()
	defer capi.lock_bOpen.Unlock()
	if !capi.bOpen {
		capi.bOpen = true

	}
	return nil
}

func (capi *CapiNozzle) Close() error {
	capi.lock_bOpen.Lock()
	defer capi.lock_bOpen.Unlock()
	if capi.bOpen {
		capi.bOpen = false
	}
	return nil
}

func (capi *CapiNozzle) handleError() bool {
	capi.lock_handle_error.RLock()
	defer capi.lock_handle_error.RUnlock()
	return capi.handle_error
}

func (capi *CapiNozzle) disableHandleError() {
	capi.lock_handle_error.Lock()
	defer capi.lock_handle_error.Unlock()
	capi.handle_error = false
}

func (capi *CapiNozzle) getClient() *net.TCPConn {
	capi.lock_client.RLock()
	defer capi.lock_client.RUnlock()
	return capi.client
}

func (capi *CapiNozzle) setClient(client *net.TCPConn) {
	capi.lock_client.Lock()
	defer capi.lock_client.Unlock()
	if capi.client != nil {
		capi.client.Close()
	}
	capi.client = client
}

func (capi *CapiNozzle) Start(settings map[string]interface{}) error {
	capi.Logger().Infof("%v starting ....\n", capi.Id())

	err := capi.SetState(common.Part_Starting)
	if err != nil {
		return err
	}

	err = capi.initialize(settings)
	capi.Logger().Infof("%v initialized\n", capi.Id())
	if err == nil {
		capi.childrenWaitGrp.Add(1)
		go capi.selfMonitor(capi.selfMonitor_finch, &capi.childrenWaitGrp)

		capi.childrenWaitGrp.Add(1)
		go capi.processData_batch(capi.sender_finch, &capi.childrenWaitGrp)

		capi.start_time = time.Now()
		err = capi.Start_server()
	}

	if err == nil {
		err = capi.SetState(common.Part_Running)
		if err == nil {
			capi.Logger().Infof("%v has been started successfully\n", capi.Id())
		}
	}
	if err != nil {
		capi.Logger().Errorf("%v failed to start. err=%v\n", capi.Id(), err)
	}
	return err
}

func (capi *CapiNozzle) Stop() error {
	capi.Logger().Infof("%v stopping \n", capi.Id())

	err := capi.SetState(common.Part_Stopping)
	if err != nil {
		return err
	}

	capi.Logger().Debugf("%v processed %v items\n", capi.Id(), atomic.LoadUint32(&capi.counter_sent))

	//close data channels
	for _, dataChan := range capi.vb_dataChan_map {
		close(dataChan)
	}

	if capi.batches_ready != nil {
		capi.Logger().Infof("%v closing batches ready\n", capi.Id())
		close(capi.batches_ready)
	}

	err = capi.Stop_server()

	err = capi.SetState(common.Part_Stopped)
	if err == nil {
		capi.Logger().Infof("%v has been stopped\n", capi.Id())
	} else {
		capi.Logger().Errorf("%v failed to stop. err=%v\n", capi.Id(), err)
	}

	return err
}

func (capi *CapiNozzle) batchReady(vbno uint16) error {
	//move the batch to ready batches channel
	defer func() {
		if r := recover(); r != nil {
			if capi.validateRunningState() == nil {
				// report error only when capi is still in running state
				capi.handleGeneralError(errors.New(fmt.Sprintf("%v", r)))
			}
		}

		capi.Logger().Debugf("%v End moving batch, %v batches ready\n", capi.Id(), len(capi.batches_ready))
	}()

	batch := capi.vb_batch_map[vbno]
	if batch.count() > 0 {
		capi.Logger().Debugf("%v move the batch (count=%d) for vb %v into ready queue\n", capi.Id(), batch.count(), vbno)
		select {
		case capi.batches_ready <- batch:
			capi.Logger().Debugf("%v There are %d batches in ready queue\n", capi.Id(), len(capi.batches_ready))

			capi.initNewBatch(vbno)
		}
	}
	return nil

}

func (capi *CapiNozzle) Receive(data interface{}) error {
	// the attempt to write to dataChan may panic if dataChan has been closed
	defer func() {
		if r := recover(); r != nil {
			capi.Logger().Errorf("%v recovered from %v", capi.Id(), r)
			if capi.validateRunningState() == nil {
				// report error only when capi is still in running state
				capi.handleGeneralError(errors.New(fmt.Sprintf("%v", r)))
			}
		}
	}()

	capi.Logger().Debugf("%v data key=%v seq=%v vb=%v is received", capi.Id(), data.(*base.WrappedMCRequest).Req.Key, data.(*base.WrappedMCRequest).Seqno, data.(*base.WrappedMCRequest).Req.VBucket)
	capi.Logger().Debugf("%v data channel len is %d\n", capi.Id(), atomic.LoadInt32(&capi.items_in_dataChan))

	req := data.(*base.WrappedMCRequest)

	vbno := req.Req.VBucket

	dataChan, ok := capi.vb_dataChan_map[vbno]
	if !ok {
		capi.Logger().Errorf("%v received a request with unexpected vb %v\n", capi.Id(), vbno)
		capi.Logger().Errorf("%v datachan map len=%v, map = %v \n", capi.Id(), len(capi.vb_dataChan_map), capi.vb_dataChan_map)
	}

	err := capi.validateRunningState()
	if err != nil {
		capi.Logger().Infof("%v is in %v state, Recieve did no-op", capi.Id(), capi.State())
		return err
	}

	dataChan <- req

	new_counter_received := atomic.AddUint32(&capi.counter_received, 1)
	size := req.Req.Size()
	new_items_in_dataChan := atomic.AddInt32(&capi.items_in_dataChan, 1)
	new_bytes_in_dataChan := atomic.AddInt32(&capi.bytes_in_dataChan, int32(size))

	//accumulate the batchCount and batchSize
	capi.accumuBatch(vbno, req)

	capi.Logger().Debugf("%v received %v items, queue_size = %v, bytes_in_dataChan=%v\n", capi.Id(), new_counter_received, new_items_in_dataChan, new_bytes_in_dataChan)

	return nil
}

func (capi *CapiNozzle) accumuBatch(vbno uint16, request *base.WrappedMCRequest) {
	capi.vb_batch_map_lock <- true
	defer func() { <-capi.vb_batch_map_lock }()

	batch := capi.vb_batch_map[vbno]
	_, isFirst, isFull := batch.accumuBatch(request, capi.optimisticRep)
	if isFirst {
		select {
		case capi.batches_nonempty_ch <- true:
		default:
			// batches_nonempty_ch is already flagged.
		}
	}

	if isFull {
		capi.batchReady(vbno)
	}

	capi.Logger().Debugf("%v batch for vb %v: batch=%v, batch.count=%v, batch.start_time=%v\n", capi.Id(), vbno, batch, batch.count(), batch.start_time)
}

func (capi *CapiNozzle) processData_batch(finch chan bool, waitGrp *sync.WaitGroup) (err error) {
	capi.Logger().Infof("%v processData starts..........\n", capi.Id())
	defer waitGrp.Done()
	for {
		select {
		case <-finch:
			goto done
		case batch, ok := <-capi.batches_ready:
			if !ok {
				capi.Logger().Infof("%v batches_ready closed. Exiting processData.", capi.Id())
				goto done
			}
			select {
			case <-finch:
				goto done
			default:
				if capi.validateRunningState() != nil {
					capi.Logger().Infof("%v has stopped. Exiting.", capi.Id())
					goto done
				}
				if capi.IsOpen() {
					capi.Logger().Debugf("%v Batch Send..., %v batches ready, %v items in queue, count_recieved=%v, count_sent=%v\n", capi.Id(), len(capi.batches_ready), atomic.LoadInt32(&capi.items_in_dataChan), atomic.LoadUint32(&capi.counter_received), atomic.LoadUint32(&capi.counter_sent))
					err = capi.send_internal(batch)
					if err != nil {
						capi.handleGeneralError(err)
						goto done
					}
				}
			}
		case <-capi.batches_nonempty_ch:
			if capi.validateRunningState() != nil {
				capi.Logger().Infof("%v has stopped. Exiting", capi.Id())
				goto done
			}

			if len(capi.batches_ready) == 0 {

				max_count, max_batch_vbno := capi.getBatchWithMaxCount()
				if max_count > 0 {
					select {
					case capi.vb_batch_map_lock <- true:
						capi.batchReady(max_batch_vbno)
						<-capi.vb_batch_map_lock
					default:
					}
				}
			}

			// check if a token needs to be put back into batches_nonempty_ch,
			// i.e., check if there is at least one non-empty batch remaining
			nonEmptyBatchExist := capi.checkIfNonEmptyBatchExist()

			if nonEmptyBatchExist {
				select {
				case capi.batches_nonempty_ch <- true:
				default:
					// batches_nonempty_ch is already flagged.
				}
			}
		}
	}

done:
	capi.Logger().Infof("%v processData_batch exits\n", capi.Id())
	return
}

func (capi *CapiNozzle) getBatchWithMaxCount() (max_count uint32, max_batch_vbno uint16) {
	max_count = 0
	max_batch_vbno = 0

	select {
	case capi.vb_batch_map_lock <- true:
		for vbno, batch := range capi.vb_batch_map {
			if batch.count() > max_count {
				max_count = batch.count()
				max_batch_vbno = vbno
			}
		}
		<-capi.vb_batch_map_lock
	default:
		// if cannot acquire lock on batch_map, return right away
	}

	return
}

func (capi *CapiNozzle) checkIfNonEmptyBatchExist() bool {
	nonEmptyBatchExist := false

	select {
	case capi.vb_batch_map_lock <- true:
	outer:
		for _, batch := range capi.vb_batch_map {
			select {
			case <-batch.batch_nonempty_ch:
				nonEmptyBatchExist = true
				break outer
			default:
				continue
			}
		}
		<-capi.vb_batch_map_lock
	default:
		// if cannot acquire lock on batch_map, return right away
		// return true to ensure that we will be checking for nonempty channels in the next iteration
		nonEmptyBatchExist = true
	}

	return nonEmptyBatchExist
}

func (capi *CapiNozzle) send_internal(batch *capiBatch) error {
	var err error
	if batch != nil {
		count := batch.count()

		capi.Logger().Infof("%v send batch count=%d for vb %v\n", capi.Id(), count, batch.vbno)

		new_counter_sent := atomic.AddUint32(&capi.counter_sent, count)
		capi.Logger().Debugf("So far, capi %v processed %d items", capi.Id(), new_counter_sent)

		var bigDoc_noRep_map map[string]bool
		bigDoc_noRep_map, err = capi.batchGetMeta(batch.vbno, batch.bigDoc_map)
		if err != nil {
			capi.Logger().Errorf("%v batchGetMeta failed. err=%v\n", capi.Id(), err)
		} else {
			batch.bigDoc_noRep_map = bigDoc_noRep_map
		}

		//batch send
		err = capi.batchSendWithRetry(batch)
	}
	return err
}

//batch call for document size larger than the optimistic threshold
func (capi *CapiNozzle) batchGetMeta(vbno uint16, bigDoc_map map[string]*base.WrappedMCRequest) (map[string]bool, error) {
	capi.Logger().Debugf("%v batchGetMeta called for vb %v and bigDoc_map with len %v, map=%v\n", capi.Id(), vbno, len(bigDoc_map), bigDoc_map)

	bigDoc_noRep_map := make(map[string]bool)

	if len(bigDoc_map) == 0 {
		return bigDoc_noRep_map, nil
	}

	couchApiBaseHost, couchApiBasePath, err := capi.getCouchApiBaseHostAndPathForVB(vbno)
	if err != nil {
		return nil, err
	}

	key_rev_map := make(map[string]string)
	key_seqnostarttime_map := make(map[string][]interface{})
	sent_id_map := make(map[string]bool)
	for id, req := range bigDoc_map {
		key := string(req.Req.Key)
		if _, ok := key_rev_map[key]; !ok {
			key_rev_map[key] = getSerializedRevision(req.Req)
			key_seqnostarttime_map[key] = []interface{}{req.Seqno, time.Now()}
			sent_id_map[id] = true
		}
	}

	body, err := json.Marshal(key_rev_map)
	if err != nil {
		return nil, err
	}

	var out interface{}
	err, statusCode := utils.QueryRestApiWithAuth(couchApiBaseHost, couchApiBasePath+base.RevsDiffPath, true, capi.config.username, capi.config.password, capi.config.certificate, false, base.MethodPost, base.JsonContentType,
		body, capi.config.connectionTimeout, &out, nil, false, capi.Logger())
	capi.Logger().Debugf("%v results of _revs_diff query for vb %v: err=%v, status=%v\n", capi.Id(), vbno, err, statusCode)
	if err != nil {
		capi.Logger().Errorf("_revs_diff query for vb %v failed with err=%v\n", vbno, err)
		return nil, err
	} else if statusCode != 200 {
		errMsg := fmt.Sprintf("Received unexpected status code %v from _revs_diff query for vbucket %v.\n", statusCode, vbno)
		capi.Logger().Errorf("%v %v", capi.Id(), errMsg)
		return nil, errors.New(errMsg)
	}

	for key, seqnostarttime := range key_seqnostarttime_map {
		additionalInfo := GetMetaReceivedEventAdditional{Key: key,
			Seqno:       seqnostarttime[0].(uint64),
			Commit_time: time.Since(seqnostarttime[1].(time.Time))}
		capi.RaiseEvent(common.NewEvent(common.GetMetaReceived, nil, capi, nil, additionalInfo))
	}

	bigDoc_rep_map, ok := out.(map[string]interface{})
	capi.Logger().Debugf("%v bigDoc_rep_map=%v\n", capi.Id(), bigDoc_rep_map)
	if !ok {
		return nil, errors.New(fmt.Sprintf("Error parsing return value from _revs_diff query for vbucket %v", vbno))
	}

	// bigDoc_noRep_map = doc_map - bigDoc_rep_map
	for id, req := range bigDoc_map {
		if _, found := sent_id_map[id]; found {
			docKey := string(req.Req.Key)
			if _, ok = bigDoc_rep_map[docKey]; !ok {
				bigDoc_noRep_map[id] = true
			}
		}
	}

	capi.Logger().Debugf("%v done with batchGetMeta,bigDoc_noRep_map=%v\n", capi.Id(), bigDoc_noRep_map)
	return bigDoc_noRep_map, nil
}

func (capi *CapiNozzle) batchSendWithRetry(batch *capiBatch) error {
	var err error
	vbno := batch.vbno
	count := batch.count()
	dataChan := capi.vb_dataChan_map[vbno]

	req_list := make([]*base.WrappedMCRequest, 0)

	for i := 0; i < int(count); i++ {
		item := <-dataChan

		atomic.AddInt32(&capi.items_in_dataChan, -1)
		atomic.AddInt32(&capi.bytes_in_dataChan, int32(0-item.Req.Size()))

		needSend := needSend(item, &batch.dataBatch, capi.Logger())
		if needSend == Send {
			capi.adjustRequest(item)
			req_list = append(req_list, item)
		} else {
			if needSend == Not_Send_Failed_CR {
				capi.Logger().Debugf("%v did not send doc with key %v since it failed conflict resolution\n", capi.Id(), string(item.Req.Key))
				additionalInfo := DataFailedCRSourceEventAdditional{Seqno: item.Seqno,
					Opcode:      encodeOpCode(item.Req.Opcode),
					IsExpirySet: (binary.BigEndian.Uint32(item.Req.Extras[4:8]) != 0),
					VBucket:     item.Req.VBucket,
				}
				capi.RaiseEvent(common.NewEvent(common.DataFailedCRSource, nil, capi, nil, additionalInfo))
			}

			capi.recycleDataObj(item)
		}

	}

	err = capi.batchUpdateDocsWithRetry(vbno, &req_list)
	if err == nil {
		for _, req := range req_list {
			// requests in req_list have strictly increasing seqnos
			// each seqno is the new high seqno
			additionalInfo := DataSentEventAdditional{Seqno: req.Seqno,
				IsOptRepd:   capi.optimisticRep(req.Req),
				Commit_time: time.Since(req.Start_time),
				Opcode:      req.Req.Opcode,
				IsExpirySet: (binary.BigEndian.Uint32(req.Req.Extras[4:8]) != 0),
				VBucket:     req.Req.VBucket,
				Req_size:    req.Req.Size(),
			}
			capi.RaiseEvent(common.NewEvent(common.DataSent, nil, capi, nil, additionalInfo))

			//recycle the request object
			capi.recycleDataObj(req)
		}
	} else {
		capi.Logger().Errorf("%v error updating docs on target. err=%v\n", capi.Id(), err)
		if err != PartStoppedError {
			capi.handleGeneralError(err)
		}
	}

	return err
}

func (capi *CapiNozzle) onExit() {
	//in the process of stopping, no need to report any error to replication manager anymore
	capi.disableHandleError()

	//notify the data processing routine
	close(capi.sender_finch)
	close(capi.checker_finch)
	close(capi.selfMonitor_finch)
	capi.childrenWaitGrp.Wait()

	//cleanup
	capi.Logger().Infof("%v releasing capi client", capi.Id())
	client := capi.getClient()
	if client != nil {
		client.Close()
	}

}

func (capi *CapiNozzle) selfMonitor(finch chan bool, waitGrp *sync.WaitGroup) {
	defer waitGrp.Done()
	statsTicker := time.NewTicker(capi.config.statsInterval)
	defer statsTicker.Stop()
	// commenting these out till they are tested
	/*ticker := time.Tick(capi.config.selfMonitorInterval)
	var sent_count int = 0
	var count uint64
	freeze_counter := 0
	idle_counter := 0*/
	for {
		select {
		case <-finch:
			goto done
		/*case <-ticker:
		if capi.validateRunningState() != nil {
			capi.Logger().Infof("capi %v has stopped.", capi.Id())
			goto done
		}

		count++
		if capi.counter_sent == sent_count {
			if capi.items_in_dataChan > 0 {
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
		sent_count = capi.counter_sent
		if count == 10 {
			capi.Logger().Debugf("%v- freeze_counter=%v, capi.counter_sent=%v, capi.items_in_dataChan=%v, receive_count-%v\n", capi.Id(), freeze_counter, capi.counter_sent, capi.items_in_dataChan, capi.counter_received)
			capi.Logger().Debugf("%v open=%v checking..., %v item unsent, received %v items, sent %v items, %v batches ready\n", capi.Id(), capi.IsOpen(), capi.items_in_dataChan, capi.counter_received, capi.counter_sent, len(capi.batches_ready))
			count = 0
		}
		if freeze_counter > capi.config.maxIdleCount {
			capi.Logger().Errorf("Capi hasn't sent any item out for %v ticks, %v data in queue", capi.config.maxIdleCount, capi.items_in_dataChan)
			capi.Logger().Infof("%v open=%v checking..., %v item unsent, received %v items, sent %v items, %v batches ready\n", capi.Id(), capi.IsOpen(), capi.items_in_dataChan, capi.counter_received, capi.counter_sent, len(capi.batches_ready))
			capi.handleGeneralError(errors.New("Capi is stuck"))
			goto done
		}*/

		case <-statsTicker.C:
			capi.RaiseEvent(common.NewEvent(common.StatsUpdate, nil, capi, nil, []int{int(atomic.LoadInt32(&capi.items_in_dataChan)), int(atomic.LoadInt32(&capi.bytes_in_dataChan))}))
		}
	}
done:
	capi.Logger().Infof("%v selfMonitor routine exits", capi.Id())

}

func (capi *CapiNozzle) validateRunningState() error {
	state := capi.State()
	if state == common.Part_Stopping || state == common.Part_Stopped || state == common.Part_Error {
		return PartStoppedError
	}
	return nil
}

func (capi *CapiNozzle) adjustRequest(req *base.WrappedMCRequest) {
	mc_req := req.Req
	mc_req.Opcode = encodeOpCode(mc_req.Opcode)
	mc_req.Cas = 0
}

// test func that uses http to update docs. may be helpful in debugging for isolating issues

/*func (capi *CapiNozzle) batchUpdateDocsWithRetry(req_list *[]*mc.MCRequest) error {
	var vbno uint16
	for _, req := range req_list {
		vbno = req.VBucket
		break
	}

	couchApiBaseHost, couchApiBasePath, err := capi.getCouchApiBaseHostAndPathForVB(vbno)
	if err != nil {
		return err
	}

	capi.Logger().Debugf(" req_list=%v len=%v\n", *req_list, len(*req_list))

	update_doc_map := make(map[string]interface{})
	update_doc_list := make([]map[string]interface{}, 0)
	update_doc_map[NewEditsKey] = false

	for _, req := range *req_list {
	capi.Logger().Debugf("appending to update_doc")
		update_doc_list = append(update_doc_list, getDocMap(req))
	}

	update_doc_map[DocsKey] = update_doc_list

	capi.Logger().Debugf(" update_doc_list=%v len=%v\n", update_doc_list, len(update_doc_list))

	capi.Logger().Debugf("update_doc_map=%v\n", update_doc_map)

	body, err := json.Marshal(update_doc_map)
	capi.Logger().Debugf("update_doc_map after marshalling: body=%v, err=%v\n", body, err)
	if err != nil {
		return err
	}

	var out interface{}
	err, statusCode := utils.QueryRestApiWithAuth(couchApiBaseHost, couchApiBasePath+base.BulkDocsPath, true, capi.config.username, capi.config.password, base.MethodPost, base.JsonContentType,
		body, &out, capi.Logger(), capi.config.certificate)
	capi.Logger().Debugf("result of _bulk_docs call for vb=%v: err=%v, status=%v\n", vbno, err, statusCode)
	if err != nil {
		return err
	} else if statusCode != 201 {
		return errors.New(fmt.Sprintf("Received unexpected status code %v from _bulk_docs call for vb=%v.\n", statusCode, vbno))
	}

	return nil
}*/

//batch call to update docs on target
func (capi *CapiNozzle) batchUpdateDocsWithRetry(vbno uint16, req_list *[]*base.WrappedMCRequest) error {
	if len(*req_list) == 0 {
		return nil
	}

	num_of_retry := 0
	backoffTime := capi.config.retryInterval
	for {
		err := capi.validateRunningState()
		if err != nil {
			return err
		}

		err = capi.batchUpdateDocs(vbno, req_list)
		if err == nil {
			// success. no need to retry further
			return nil
		}

		if num_of_retry < capi.config.maxRetry {
			// reset connection to ensure a clean start
			err = capi.resetConn()
			if err != nil {
				return err
			}
			num_of_retry++
			time.Sleep(backoffTime)
			backoffTime *= 2
			capi.Logger().Infof("%v retrying update docs for vb %v for the %vth time\n", capi.Id(), vbno, num_of_retry)
		} else {
			// max retry reached
			return errors.New(fmt.Sprintf("batch update docs failed for vb %v after %v retries", vbno, num_of_retry))
		}
	}
}

func (capi *CapiNozzle) batchUpdateDocs(vbno uint16, req_list *[]*base.WrappedMCRequest) (err error) {
	capi.Logger().Debugf("%v batchUpdateDocs, vbno=%v, len(req_list)=%v\n", capi.Id(), vbno, len(*req_list))

	couchApiBaseHost, couchApiBasePath, err := capi.getCouchApiBaseHostAndPathForVB(vbno)
	if err != nil {
		return
	}

	// construct docs to send
	doc_list := make([][]byte, 0)
	doc_length := 0
	for _, req := range *req_list {
		doc_map := getDocMap(req.Req)
		var doc_bytes []byte
		doc_bytes, err = json.Marshal(doc_map)
		if err != nil {
			return
		}
		doc_bytes = append(doc_bytes, BodyPartsDelimiter...)
		doc_length += len(doc_bytes)
		doc_list = append(doc_list, doc_bytes)
	}

	// remove the unnecessary delimiter at the end of doc list
	last_doc := doc_list[len(doc_list)-1]
	doc_list[len(doc_list)-1] = last_doc[:len(last_doc)-len(BodyPartsDelimiter)]
	doc_length -= len(BodyPartsDelimiter)

	total_length := len(BodyPartsPrefix) + doc_length + len(BodyPartsSuffix)

	http_req, _, err := utils.ConstructHttpRequest(couchApiBaseHost, couchApiBasePath+base.BulkDocsPath, true, capi.config.username, capi.config.password, capi.config.certificate, base.MethodPost, base.JsonContentType,
		nil, capi.Logger())
	if err != nil {
		return
	}

	// set content length.
	http_req.Header.Set(base.ContentLength, strconv.Itoa(total_length))

	// enable delayed commit
	http_req.Header.Set(CouchFullCommitKey, "false")

	capi.Logger().Debugf("%v updateDocs request=%v\n", capi.Id(), http_req)

	// unfortunately request.Write() does not preserve Content-Length. have to encode the request ourselves
	req_bytes, err := utils.EncodeHttpRequest(http_req)
	if err != nil {
		return
	}

	capi.Logger().Debugf("%v updateDocs encoded request=%v\n string form=%v\n", capi.Id(), req_bytes, string(req_bytes))

	resp_ch := make(chan bool, 1)
	err_ch := make(chan error, 2)
	fin_ch := make(chan bool)

	// data channel for body parts. The per-defined size controls the flow between
	// the two go routines below so as to reduce the chance of overwhelming the target server
	part_ch := make(chan []byte, capi.config.uploadWindowSize)
	// start go routine which actually writes to and reads from tcp connection
	go capi.tcpProxy(vbno, part_ch, resp_ch, err_ch, fin_ch)
	// start go rountine that write body parts to tcpProxy()
	go capi.writeDocs(vbno, req_bytes, doc_list, part_ch, err_ch, fin_ch)

	ticker := time.NewTicker(capi.config.connectionTimeout)
	defer ticker.Stop()
	select {
	case <-capi.sender_finch:
		// capi is stopping.
	case <-resp_ch:
		// response received. everything is good
		capi.Logger().Debugf("%v batchUpdateDocs for vb %v succesfully updated %v docs.\n", capi.Id(), vbno, len(*req_list))
	case err = <-err_ch:
		// error encountered
		capi.Logger().Errorf("%v batchUpdateDocs for vb %v failed with err %v.\n", capi.Id(), vbno, err)
	case <-ticker.C:
		// connection timed out
		errMsg := fmt.Sprintf("Connection timeout when updating docs for vb %v", vbno)
		capi.Logger().Errorf("%v %v", capi.Id(), errMsg)
		err = errors.New(errMsg)
	}

	// get all send routines to stop
	close(fin_ch)

	// Question: do we need to wait for send routines to stop? I guess not

	return err

}

func (capi *CapiNozzle) writeDocs(vbno uint16, req_bytes []byte, doc_list [][]byte, part_ch chan []byte,
	err_ch chan error, fin_ch chan bool) {

	partIndex := 0
	for {
		select {
		case <-fin_ch:
			capi.Logger().Debugf("%v terminating writeDocs because of closure of finch\n", capi.Id())
			return
		default:
			// if no error, keep sending body parts
			if partIndex == 0 {
				// send initial request to tcp
				capi.Logger().Debugf("%v req_bytes=%v\nreq_bytes_in_str=%v\n\n", capi.Id(), req_bytes, string(req_bytes))
				if !capi.writeToPartCh(part_ch, req_bytes) {
					return
				}
			} else if partIndex == 1 {
				// write body part prefix
				capi.Logger().Debugf("%v writing first body part %v", capi.Id(), BodyPartsPrefix)
				if !capi.writeToPartCh(part_ch, []byte(BodyPartsPrefix)) {
					return
				}
			} else if partIndex < len(doc_list)+2 {
				// write individual doc
				capi.Logger().Debugf("%v writing %vth doc = %v, doc_in_str=%v\n", capi.Id(), partIndex-2, doc_list[partIndex-2], string(doc_list[partIndex-2]))
				if !capi.writeToPartCh(part_ch, doc_list[partIndex-2]) {
					return
				}
			} else {
				// write body part suffix
				capi.Logger().Debugf("%v writing last body part %v\n", capi.Id(), BodyPartsSuffix)
				if !capi.writeToPartCh(part_ch, []byte(BodyPartsSuffix)) {
					return
				}
				// all parts have been sent. terminate sendBodyPart rountine
				capi.Logger().Debugf("%v closing part channel since all parts had been sent\n", capi.Id())
				close(part_ch)
				return
			}
			partIndex++
		}

	}
}

// use timeout to give it a chance to detect nozzle stop event and abort
func (capi *CapiNozzle) writeToPartCh(part_ch chan []byte, data []byte) bool {
	timeoutticker := time.NewTicker(default_write_to_part_ch_timeout)
	defer timeoutticker.Stop()
	for {
		select {
		case part_ch <- data:
			return true
		case <-timeoutticker.C:
			if capi.validateRunningState() != nil {
				capi.Logger().Infof("%v is no longer running, aborting writing to part ch", capi.Id())
				return false
			}
		}
	}
}

func (capi *CapiNozzle) tcpProxy(vbno uint16, part_ch chan []byte, resp_ch chan bool, err_ch chan error, fin_ch chan bool) {
	capi.Logger().Debugf("%v tcpProxy routine for vb %v is starting\n", capi.Id(), vbno)
	for {
		select {
		case <-fin_ch:
			capi.Logger().Debugf("%v tcpProxy routine is exiting because of closure of finch\n", capi.Id())
			return
		case part, ok := <-part_ch:

			if ok {
				client := capi.getClient()
				client.SetWriteDeadline(time.Now().Add(capi.config.writeTimeout))
				_, err := client.Write(part)
				capi.Logger().Debugf("%v wrote body part. part=%v, err=%v\n", capi.Id(), string(part), err)
				if err != nil {
					capi.Logger().Errorf("Received error when writing boby part. err=%v\n", err)
					err_ch <- err
					return
				}
			} else {
				// the closing of part_ch signals that all body parts have been sent. start receiving responses
				capi.Logger().Debugf("%v tcpProxy routine starting to receive response since all body parts have been sent\n", capi.Id())

				// read response
				client := capi.getClient()
				client.SetReadDeadline(time.Now().Add(capi.config.readTimeout))

				response, err := http.ReadResponse(bufio.NewReader(client), nil)
				if err != nil || response == nil {
					errMsg := fmt.Sprintf("Error reading response. vb=%v, err=%v\n", vbno, err)
					capi.Logger().Errorf("%v %v", capi.Id(), errMsg)
					err_ch <- errors.New(errMsg)
					return
				}

				defer response.Body.Close()

				if response.StatusCode != 201 {
					errMsg := fmt.Sprintf("Received unexpected status code, %v, from update docs request for vb %v\n", response.StatusCode, vbno)
					capi.Logger().Errorf("%v %v", capi.Id(), errMsg)
					err_ch <- errors.New(errMsg)

					// no need to read leftover bytes, if any, since connection will get reset soon
					return
				}

				_, err = ioutil.ReadAll(response.Body)
				if err != nil {
					// if we get an error reading the entirety of response body, e.g., because of timeout
					// we need to reset connection to give subsequent requests a clean start
					// there is no need to return error, though, since the current batch has already
					// succeeded (as signaled by the 201 response status)
					errMsg := MalformedResponseError + fmt.Sprintf(" vb=%v, err=%v\n", vbno, err)
					capi.Logger().Errorf("%v %v", capi.Id(), errMsg)
					capi.resetConn()
				}

				// notify caller that write succeeded
				resp_ch <- true

				return
			}
		}
	}

}

// produce a serialized document from mc request
func getDocMap(req *mc.MCRequest) map[string]interface{} {
	doc_map := make(map[string]interface{})
	meta_map := make(map[string]interface{})
	doc_map[MetaKey] = meta_map
	doc_map[BodyKey] = req.Body

	//TODO need to handle Key being non-UTF8?
	meta_map[IdKey] = string(req.Key)
	meta_map[RevKey] = getSerializedRevision(req)
	meta_map[ExpirationKey] = binary.BigEndian.Uint32(req.Extras[4:8])
	meta_map[FlagsKey] = binary.BigEndian.Uint32(req.Extras[0:4])
	if req.Opcode == base.DELETE_WITH_META {
		meta_map[DeletedKey] = true
	}

	if !simple_utils.IsJSON(req.Body) {
		meta_map[AttReasonKey] = InvalidJson
	}

	return doc_map
}

// produce serialized revision info in the form of revSeq-Cas+Expiration+Flags
func getSerializedRevision(req *mc.MCRequest) string {
	var revId [16]byte
	// CAS
	copy(revId[0:8], req.Extras[16:24])
	// expiration
	copy(revId[8:12], req.Extras[4:8])
	// flags
	copy(revId[12:16], req.Extras[0:4])

	revSeq := binary.BigEndian.Uint64(req.Extras[8:16])
	revSeqStr := strconv.FormatUint(revSeq, 10)
	revIdStr := hex.EncodeToString(revId[0:16])
	return revSeqStr + "-" + revIdStr
}

func (capi *CapiNozzle) initNewBatch(vbno uint16) {
	capi.Logger().Debugf("%v init a new batch for vb %v\n", capi.Id(), vbno)
	capi.vb_batch_map[vbno] = &capiBatch{*newBatch(uint32(capi.config.maxCount), uint32(capi.config.maxSize), capi.Logger()), vbno}
}

func (capi *CapiNozzle) initialize(settings map[string]interface{}) error {
	err := capi.config.initializeConfig(settings)
	if err != nil {
		return err
	}

	capi.vb_dataChan_map = make(map[uint16]chan *base.WrappedMCRequest)
	for vbno, _ := range capi.config.vbCouchApiBaseMap {
		capi.vb_dataChan_map[vbno] = make(chan *base.WrappedMCRequest, capi.config.maxCount*5)
	}
	capi.items_in_dataChan = 0
	capi.bytes_in_dataChan = 0
	capi.batches_ready = make(chan *capiBatch, len(capi.config.vbCouchApiBaseMap)*10)

	//enable send
	//	capi.send_allow_ch <- true

	//init new batches
	capi.vb_batch_map = make(map[uint16]*capiBatch)
	capi.vb_batch_map_lock = make(chan bool, 1)
	for vbno, _ := range capi.config.vbCouchApiBaseMap {
		capi.initNewBatch(vbno)
	}

	capi.checker_finch = make(chan bool, 1)

	capi.Logger().Debugf("%v about to start initializing connection", capi.Id())
	err = capi.initializeConn()
	if err == nil {
		capi.Logger().Infof("%v connection initialization completed.", capi.Id())
	} else {
		capi.Logger().Errorf("%v connection initialization failed with err=%v.", capi.Id(), err)
	}

	return err
}

/* capi does not increase retry interval for each retry since capi's wait is blocking wait
func (capi *CapiNozzle) timeoutDuration(numofRetry int) time.Duration {
	duration := capi.config.retryInterval
	for i := 1; i < numofRetry; i++ {
		duration *= 2
		if duration > capi.config.maxRetryInterval {
			duration = capi.config.maxRetryInterval
			break
		}
	}
	return duration
}*/

func (capi *CapiNozzle) StatusSummary() string {
	return fmt.Sprintf("%v received %v items, sent %v items", capi.Id(), atomic.LoadUint32(&capi.counter_received), atomic.LoadUint32(&capi.counter_sent))
}

func (capi *CapiNozzle) handleGeneralError(err error) {
	if capi.handleError() {
		capi.Logger().Errorf("%v raise error condition %v\n", capi.Id(), err)
		capi.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, capi, nil, err))
	} else {
		capi.Logger().Debugf("%v in shutdown process, err=%v is ignored\n", capi.Id(), err)
	}
}

func (capi *CapiNozzle) optimisticRep(req *mc.MCRequest) bool {
	if req != nil {
		return uint32(req.Size()) < capi.getOptiRepThreshold()
	}
	return true
}

func (capi *CapiNozzle) getOptiRepThreshold() uint32 {
	return atomic.LoadUint32(&(capi.config.optiRepThreshold))
}

func (capi *CapiNozzle) getPoolName(config capiConfig) string {
	return "Couch_Capi_" + config.connectStr
}

func (capi *CapiNozzle) getCouchApiBaseHostAndPathForVB(vbno uint16) (string, string, error) {
	couchApiBase, ok := capi.config.vbCouchApiBaseMap[vbno]
	if !ok {
		return "", "", errors.New(fmt.Sprintf("Cannot find couchApiBase for vbucket %v", vbno))
	}

	index := strings.LastIndex(couchApiBase, base.UrlDelimiter)
	if index < 0 {
		return "", "", errors.New(fmt.Sprintf("Error parsing couchApiBase for vbucket %v", vbno))
	}
	couchApiBaseHost := couchApiBase[:index]
	couchApiBasePath := couchApiBase[index:]

	return couchApiBaseHost, couchApiBasePath, nil
}

func (capi *CapiNozzle) initializeConn() error {
	return capi.initializeOrResetConn(true)
}

func (capi *CapiNozzle) resetConn() error {
	return capi.initializeOrResetConn(false)
}

func (capi *CapiNozzle) initializeOrResetConn(initializing bool) error {
	capi.Logger().Debugf("%v resetting capi connection\n", capi.Id())

	if capi.validateRunningState() != nil {
		capi.Logger().Infof("%v is not running, no need to resetConn", capi.Id())
		return nil
	}

	var pool *base.TCPConnPool
	var err error

	if initializing {
		pool, err = base.TCPConnPoolMgr().GetOrCreatePool(capi.getPoolName(capi.config), capi.config.connectStr, base.DefaultCAPIConnectionSize)
	} else {
		pool = base.TCPConnPoolMgr().GetPool(capi.getPoolName(capi.config))
		if pool == nil {
			// make sure that err is not nil when pool is nil
			err = errors.New("Error retrieving connection pool")
		}
	}

	if pool != nil {
		var newClient *net.TCPConn
		newClient, err = pool.GetNew()
		if err == nil && newClient != nil {
			// same settings as erlang xdcr
			newClient.SetKeepAlive(true)
			newClient.SetNoDelay(false)
			capi.setClient(newClient)
		}
	}

	if err != nil {
		capi.Logger().Errorf("%v - Connection reset failed. err=%v\n", capi.Id(), err)
		capi.handleGeneralError(err)
	}

	return err
}

func (capi *CapiNozzle) UpdateSettings(settings map[string]interface{}) error {
	optimisticReplicationThreshold, err := utils.GetIntSettingFromSettings(settings, metadata.OptimisticReplicationThreshold)
	if err != nil {
		return err
	}

	atomic.StoreUint32(&capi.config.optiRepThreshold, uint32(optimisticReplicationThreshold))
	return nil
}

func (capi *CapiNozzle) recycleDataObj(req *base.WrappedMCRequest) {
	if capi.dataObj_recycler != nil {
		capi.dataObj_recycler(capi.topic, req)
	}
}
