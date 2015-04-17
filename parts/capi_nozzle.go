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
	"bytes"
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
	"github.com/couchbase/goxdcr/utils"
	"net"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	SETTING_UPLOAD_WINDOW_SIZE = "upload_window_size"
	SETTING_CONNECTION_TIMEOUT = "connection_timeout"
	SETTING_RETRY_INTERVAL     = "retry_interval"

	//default configuration
	default_numofretry_capi          int           = 3
	default_batchExpirationTime_capi               = 10 * time.Second
	default_retry_interval_capi      time.Duration = 10 * time.Millisecond
	default_maxRetryInterval_capi                  = 30 * time.Second
	default_writeTimeout_capi        time.Duration = time.Duration(1) * time.Second
	default_readTimeout_capi         time.Duration = time.Duration(10) * time.Second
	default_upload_window_size       int           = 3                 // erlang xdcr value
	default_connection_timeout                     = 180 * time.Second // erlang xdcr value
	default_selfMonitorInterval_capi time.Duration = 300 * time.Millisecond
	default_maxIdleCount_capi        int           = 30
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
			batchExpirationTime: default_batchExpirationTime_capi,
			writeTimeout:        default_writeTimeout_capi,
			readTimeout:         default_readTimeout_capi,
			maxRetryInterval:    default_maxRetryInterval_capi,
			selfMonitorInterval: default_selfMonitorInterval_capi,
			maxIdleCount:        default_maxIdleCount_capi,
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
	items_in_dataChan int
	//the total size of data (in bytes) queued in all data channels
	bytes_in_dataChan int

	client *net.TCPConn

	//configurable parameter
	config capiConfig

	//queue for ready batches
	batches_ready chan *capiBatch

	//batches to be accumulated, one for each vb
	vb_batch_map       map[uint16]*capiBatch
	vb_batch_move_lock map[uint16]*sync.Mutex

	childrenWaitGrp sync.WaitGroup

	sender_finch      chan bool
	checker_finch     chan bool
	selfMonitor_finch chan bool

	counter_sent     int
	counter_received int
	start_time       time.Time
	handle_error     bool
}

func NewCapiNozzle(id string,
	connectString string,
	username string,
	password string,
	certificate []byte,
	vbCouchApiBaseMap map[uint16]string,
	logger_context *log.LoggerContext) *CapiNozzle {

	//callback functions from GenServer
	var msg_callback_func gen_server.Msg_Callback_Func
	var exit_callback_func gen_server.Exit_Callback_Func
	var error_handler_func gen_server.Error_Handler_Func

	server := gen_server.NewGenServer(&msg_callback_func,
		&exit_callback_func, &error_handler_func, logger_context, "CapiNozzle")
	part := NewAbstractPartWithLogger(id, server.Logger())

	capi := &CapiNozzle{GenServer: server, /*gen_server.GenServer*/
		AbstractPart:      part,                           /*part.AbstractPart*/
		bOpen:             true,                           /*bOpen	bool*/
		lock_bOpen:        sync.RWMutex{},                 /*lock_bOpen	sync.RWMutex*/
		config:            newCapiConfig(server.Logger()), /*config	capiConfig*/
		batches_ready:     nil,                            /*batches_ready chan *capiBatch*/
		childrenWaitGrp:   sync.WaitGroup{},               /*childrenWaitGrp sync.WaitGroup*/
		sender_finch:      make(chan bool, 1),
		checker_finch:     make(chan bool, 1),
		selfMonitor_finch: make(chan bool, 1),
		//		send_allow_ch:    make(chan bool, 1), /*send_allow_ch chan bool*/
		counter_sent:     0,
		handle_error:     true,
		counter_received: 0,
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

func (capi *CapiNozzle) Open() error {
	if !capi.bOpen {
		capi.bOpen = true

	}
	return nil
}

func (capi *CapiNozzle) Close() error {
	if capi.bOpen {
		capi.bOpen = false
	}
	return nil
}

func (capi *CapiNozzle) Start(settings map[string]interface{}) error {
	capi.Logger().Infof("Capi %v starting ....\n", capi.Id())
	err := capi.initialize(settings)
	capi.Logger().Infof("Capi %v initialized\n", capi.Id())
	if err == nil {
		capi.childrenWaitGrp.Add(1)
		go capi.selfMonitor(capi.selfMonitor_finch, &capi.childrenWaitGrp)

		capi.childrenWaitGrp.Add(1)
		go capi.check(capi.checker_finch, &capi.childrenWaitGrp)

		capi.childrenWaitGrp.Add(1)
		go capi.processData_batch(capi.sender_finch, &capi.childrenWaitGrp)

		capi.start_time = time.Now()
		err = capi.Start_server()
	}

	if err == nil {
		capi.Logger().Infof("Capi %v is started successfully\n", capi.Id())
	} else {
		capi.Logger().Errorf("Failed to start capi %v. err=%v\n", capi.Id(), err)
	}
	return err
}

func (capi *CapiNozzle) Stop() error {
	capi.Logger().Infof("Stop CapiNozzle %v\n", capi.Id())

	err := capi.SetState(common.Part_Stopping)
	if err != nil {
		return err
	}

	capi.Logger().Debugf("CapiNozzle %v processed %v items\n", capi.Id(), capi.counter_sent)

	//close data channels
	for _, dataChan := range capi.vb_dataChan_map {
		close(dataChan)
	}
	capi.Logger().Infof("closing batches ready for %v\n", capi.Id())
	close(capi.batches_ready)

	err = capi.Stop_server()

	err = capi.SetState(common.Part_Stopped)
	if err == nil {
		capi.Logger().Debugf("CapiNozzle %v is stopped\n", capi.Id())
	}

	return err
}

func (capi *CapiNozzle) IsOpen() bool {
	ret := capi.bOpen
	return ret
}

func (capi *CapiNozzle) batchReady(vbno uint16) error {
	//move the batch to ready batches channel
	capi.vb_batch_move_lock[vbno].Lock()
	defer func() {
		capi.vb_batch_move_lock[vbno].Unlock()

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
			capi.Logger().Debugf("There are %d batches in ready queue\n", len(capi.batches_ready))

			capi.initNewBatch(vbno)
		}
	}
	return nil

}

func (capi *CapiNozzle) Receive(data interface{}) error {
	capi.Logger().Debugf("data key=%v seq=%v vb=%v is received", data.(*base.WrappedMCRequest).Req.Key, data.(*base.WrappedMCRequest).Seqno, data.(*base.WrappedMCRequest).Req.VBucket)
	capi.Logger().Debugf("data channel len is %d\n", capi.items_in_dataChan)

	req := data.(*base.WrappedMCRequest)

	vbno := req.Req.VBucket

	dataChan, ok := capi.vb_dataChan_map[vbno]
	if !ok {
		capi.Logger().Errorf("%v received a request with unexpected vb %v\n", capi.Id(), vbno)
		capi.Logger().Errorf("datachan map len=%v, map = %v \n", len(capi.vb_dataChan_map), capi.vb_dataChan_map)
	}

	dataChan <- req

	capi.counter_received++
	size := req.Req.Size()
	capi.items_in_dataChan++
	capi.bytes_in_dataChan += size

	//accumulate the batchCount and batchSize
	batch := capi.vb_batch_map[vbno]
	if batch.accumuBatch(req, capi.optimisticRep) {
		capi.batchReady(vbno)
	}

	capi.Logger().Debugf("batch for vb %v: batch=%v, batch.count=%v, batch.start_time=%v\n", vbno, batch, batch.count(), batch.start_time)
	capi.Logger().Debugf("%v received %v items, queue_size = %v, bytes_in_dataChan=%v\n", capi.Id(), capi.counter_received, capi.items_in_dataChan, capi.bytes_in_dataChan)

	return nil
}

func (capi *CapiNozzle) processData_batch(finch chan bool, waitGrp *sync.WaitGroup) (err error) {
	capi.Logger().Infof("%v processData starts..........\n", capi.Id())
	defer waitGrp.Done()
	for {
		capi.Logger().Debugf("%v processData ....\n", capi.Id())
		select {
		case <-finch:
			goto done
		case batch := <-capi.batches_ready:
			select {
			case <-finch:
				goto done
			default:
				if capi.validateRunningState() != nil {
					capi.Logger().Infof("%v has stopped.", capi.Id())
					goto done
				}
				if capi.IsOpen() {
					capi.Logger().Debugf("%v Batch Send..., %v batches ready, %v items in queue, count_recieved=%v, count_sent=%v\n", capi.Id(), len(capi.batches_ready), capi.items_in_dataChan, capi.counter_received, capi.counter_sent)
					err = capi.send_internal(batch)
					if err != nil {
						capi.handleGeneralError(err)
					}
				}
			}

		}
	}

done:
	capi.Logger().Infof("%v processData_batch exits\n", capi.Id())
	return
}

func (capi *CapiNozzle) send_internal(batch *capiBatch) error {
	var err error
	if batch != nil {
		count := batch.count()

		capi.Logger().Infof("Send batch count=%d\n", count)

		capi.counter_sent = capi.counter_sent + count
		capi.Logger().Debugf("So far, capi %v processed %d items", capi.Id(), capi.counter_sent)

		bigDoc_noRep_map, err := capi.batchGetMeta(batch.vbno, batch.bigDoc_map)
		if err != nil {
			capi.Logger().Errorf("batchGetMeta failed. err=%v\n", err)
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
	capi.Logger().Debugf("batchGetMeta called for vb %v and bigDoc_map with len %v, map=%v\n", vbno, len(bigDoc_map), bigDoc_map)

	bigDoc_noRep_map := make(map[string]bool)

	if len(bigDoc_map) == 0 {
		return bigDoc_noRep_map, nil
	}

	couchApiBaseHost, couchApiBasePath, err := capi.getCouchApiBaseHostAndPathForVB(vbno)
	if err != nil {
		return nil, err
	}

	key_rev_map := make(map[string]string)
	key_seqno_map := make(map[string]uint64)
	sent_id_map := make(map[string]bool)
	for id, req := range bigDoc_map {
		key := string(req.Req.Key)
		if _, ok := key_rev_map[key]; !ok {
			key_rev_map[key] = getSerializedRevision(req.Req)
			key_seqno_map[key] = req.Seqno
			sent_id_map[id] = true
		}
	}

	body, err := json.Marshal(key_rev_map)
	if err != nil {
		return nil, err
	}

	for key, seqno := range key_seqno_map {
		additionalInfo := make(map[string]interface{})
		additionalInfo[EVENT_ADDI_DOC_KEY] = key
		additionalInfo[EVENT_ADDI_SEQNO] = seqno
		capi.RaiseEvent(common.GetMetaSent, nil, capi, nil, additionalInfo)
	}

	var out interface{}
	err, statusCode := utils.QueryRestApiWithAuth(couchApiBaseHost, couchApiBasePath+base.RevsDiffPath, true, capi.config.username, capi.config.password, capi.config.certificate, base.MethodPost, base.JsonContentType,
		body, capi.config.connectionTimeout, &out, capi.Logger())
	capi.Logger().Debugf("results of _revs_diff query for vb %v: err=%v, status=%v\n", vbno, err, statusCode)
	if err != nil {
		capi.Logger().Errorf("_revs_diff query for vb %v failed with err=%v\n", vbno, err)
		return nil, err
	} else if statusCode != 200 {
		errMsg := fmt.Sprintf("Received unexpected status code %v from _revs_diff query for vbucket %v.\n", statusCode, vbno)
		capi.Logger().Error(errMsg)
		return nil, errors.New(errMsg)
	}

	for key, seqno := range key_seqno_map {
		additionalInfo := make(map[string]interface{})
		additionalInfo[EVENT_ADDI_DOC_KEY] = key
		additionalInfo[EVENT_ADDI_SEQNO] = seqno
		capi.RaiseEvent(common.GetMetaReceived, nil, capi, nil, additionalInfo)
	}

	bigDoc_rep_map, ok := out.(map[string]interface{})
	capi.Logger().Debugf("bigDoc_rep_map=%v\n", bigDoc_rep_map)
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

	capi.Logger().Debugf("Done with batchGetMeta,bigDoc_noRep_map=%v\n", bigDoc_noRep_map)
	return bigDoc_noRep_map, nil
}

func (capi *CapiNozzle) batchSendWithRetry(batch *capiBatch) error {
	var err error
	vbno := batch.vbno
	count := batch.count()
	dataChan := capi.vb_dataChan_map[vbno]

	req_list := make([]*base.WrappedMCRequest, 0)

	for i := 0; i < count; i++ {
		item := <-dataChan

		capi.items_in_dataChan--
		capi.bytes_in_dataChan -= item.Req.Size()

		if needSend(item.Req, &batch.dataBatch, capi.logger) {
			req_list = append(req_list, item)
		} else {
			capi.Logger().Debugf("did not send doc with key %v since it failed conflict resolution\n", string(item.Req.Key))
			//lost on conflict resolution on source side
			additionalInfo := make(map[string]interface{})
			additionalInfo[EVENT_ADDI_SEQNO] = item.Seqno
			capi.RaiseEvent(common.DataFailedCRSource, item.Req, capi, nil, additionalInfo)
		}

	}

	err = capi.batchUpdateDocsWithRetry(vbno, &req_list)
	if err == nil {
		for _, req := range req_list {
			// requests in req_list have strictly increasing seqnos
			// each seqno is the new high seqno
			additionalInfo := make(map[string]interface{})
			additionalInfo[EVENT_ADDI_SEQNO] = req.Seqno
			additionalInfo[EVENT_ADDI_OPT_REPD] = capi.optimisticRep(req.Req)
			additionalInfo[EVENT_ADDI_HISEQNO] = req.Seqno
			capi.RaiseEvent(common.DataSent, req.Req, capi, nil, additionalInfo)
		}
	} else {
		capi.Logger().Errorf("Error updating docs on target. err=%v\n", err)
		if err != PartStoppedError {
			capi.handleGeneralError(err)
		}
	}

	return err
}

func (capi *CapiNozzle) onExit() {
	//in the process of stopping, no need to report any error to replication manager anymore
	capi.handle_error = false

	//notify the data processing routine
	close(capi.sender_finch)
	close(capi.checker_finch)
	close(capi.selfMonitor_finch)
	capi.childrenWaitGrp.Wait()

	//cleanup
	pool := base.TCPConnPoolMgr().GetPool(capi.getPoolName(capi.config))
	if pool != nil {
		capi.Logger().Infof("releasing capi client")
		pool.Release(capi.client)
	}

}

func (capi *CapiNozzle) selfMonitor(finch chan bool, waitGrp *sync.WaitGroup) {
	defer waitGrp.Done()
	statsTicker := time.Tick(capi.config.statsInterval)
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

		case <-statsTicker:
			additionalInfo := make(map[string]interface{})
			additionalInfo[STATS_QUEUE_SIZE] = capi.items_in_dataChan
			additionalInfo[STATS_QUEUE_SIZE_BYTES] = capi.bytes_in_dataChan
			capi.RaiseEvent(common.StatsUpdate, nil, capi, nil, additionalInfo)
		}
	}
done:
	capi.Logger().Infof("Capi %v selfMonitor routine exits", capi.Id())

}

func (capi *CapiNozzle) check(finch chan bool, waitGrp *sync.WaitGroup) {
	capi.Logger().Infof("check routine for %v starting with check interval %v ...\n", capi.Id(), capi.config.batchExpirationTime)
	defer waitGrp.Done()

	ticker := time.Tick(capi.config.batchExpirationTime)
	for {
		select {
		case <-finch:
			goto done
		case <-ticker:
			if capi.validateRunningState() != nil {
				goto done
			}

			// find all batches that have expired
			start_time_vb_map := make(map[float64]uint16)
			start_times := make([]float64, 0)
			for vbno, batch := range capi.vb_batch_map {
				select {
				case <-batch.expire_ch:
					start_time := float64(batch.start_time.UnixNano())
					start_time_vb_map[start_time] = vbno
					start_times = append(start_times, start_time)
				default:
				}
			}

			if len(start_times) > 0 {
				// sort start time in increasing order, so that we can move batches
				// that expired earlier to batches_ready queue earlier to ensure fairness
				sort.Float64s(start_times)
				for _, start_time := range start_times {

					vbno := start_time_vb_map[start_time]
					capi.Logger().Infof("%v batch for vb %v expired, moving it to ready queue\n", capi.Id(), vbno)
					capi.batchReady(vbno)
				}
			}

		}
	}
done:
	capi.Logger().Infof("check routine for %v exiting ...\n", capi.Id())
}

func (capi *CapiNozzle) validateRunningState() error {
	if capi.State() == common.Part_Stopping || capi.State() == common.Part_Stopped {
		return PartStoppedError
	}
	return nil
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
	retriedMalformedResponse := false
	for {
		err := capi.batchUpdateDocs(vbno, req_list)
		if err == nil {
			// success. no need to retry further
			return nil
		}
		isMalformedResponseError := strings.HasPrefix(err.Error(), MalformedResponseError)
		// The idea here is that if batchUpdateDocs failed with malformedResponse error, it will be retried at least once,
		// even when maxRetry has been reached. This is because a malformedResponse often signals that the update
		// has already been done successfully on the target side. We want to give it another shot, which very likely
		// will succeed, before we declare the update to have failed, which likely will lead to pipeline failure.
		if (isMalformedResponseError && (!retriedMalformedResponse || num_of_retry < capi.config.maxRetry)) ||
			(!isMalformedResponseError && num_of_retry < capi.config.maxRetry) {
			if err.Error() == MalformedResponseError {
				retriedMalformedResponse = true
			}
			// reset connection to ensure a clean start
			capi.resetConn()
			num_of_retry++
			time.Sleep(capi.config.retryInterval)
			capi.Logger().Infof("Retrying update docs for vb %v for the %vth time\n", vbno, num_of_retry)
		} else {
			// max retry reached
			return errors.New(fmt.Sprintf("batch update docs failed for vb %v after %v retries", vbno, num_of_retry))
		}
	}
}

func (capi *CapiNozzle) batchUpdateDocs(vbno uint16, req_list *[]*base.WrappedMCRequest) (err error) {
	capi.Logger().Debugf("batchUpdateDocs, vbno=%v, len(req_list)=%v\n", vbno, len(*req_list))

	//capi.resetConn()

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

	capi.Logger().Debugf("updateDocs request=%v\n", http_req)

	// unfortunately request.Write() does not preserve Content-Length. have to encode the request ourselves
	req_bytes, err := utils.EncodeHttpRequest(http_req)
	if err != nil {
		return
	}

	capi.Logger().Debugf("updateDocs encoded request=%v\n string form=%v\n", req_bytes, string(req_bytes))

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

	ticker := time.Tick(capi.config.connectionTimeout)
	select {
	case <-capi.sender_finch:
		// capi is stopping.
	case <-resp_ch:
		// response received. everything is good
		capi.Logger().Debugf("batchUpdateDocs for vb %v succesfully updated %v docs.\n", vbno, len(*req_list))
	case err = <-err_ch:
		// error encountered
		capi.Logger().Errorf("batchUpdateDocs for vb %v failed with err %v.\n", vbno, err)
	case <-ticker:
		// connection timed out
		errMsg := fmt.Sprintf("Connection timeout when updating docs for vb %v", vbno)
		capi.Logger().Error(errMsg)
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
			capi.Logger().Debugf("terminating writeDocs because of closure of finch\n")
			return
		default:
			// if no error, keep sending body parts
			if partIndex == 0 {
				// send initial request to tcp
				capi.Logger().Debugf("req_bytes=%v\nreq_bytes_in_str=%v\n\n", req_bytes, string(req_bytes))
				part_ch <- req_bytes
			} else if partIndex == 1 {
				// write body part prefix
				capi.Logger().Debugf("writing first body part %v", BodyPartsPrefix)
				part_ch <- []byte(BodyPartsPrefix)
			} else if partIndex < len(doc_list)+2 {
				// write individual doc
				capi.Logger().Debugf("writing %vth doc = %v\n", partIndex-2, doc_list[partIndex-2])
				part_ch <- doc_list[partIndex-2]
			} else {
				// write body part suffix
				capi.Logger().Debugf("writing last body part %v\n", BodyPartsSuffix)
				part_ch <- []byte(BodyPartsSuffix)
				// all parts have been sent. terminate sendBodyPart rountine
				capi.Logger().Debugf("closing part channel since all parts had been sent\n")
				close(part_ch)
				return
			}
			partIndex++
		}

	}
}

func (capi *CapiNozzle) tcpProxy(vbno uint16, part_ch chan []byte, resp_ch chan bool, err_ch chan error, fin_ch chan bool) {
	capi.Logger().Debugf("tcpProxy routine for vb %v is starting\n", vbno)
	for {
		select {
		case <-fin_ch:
			capi.Logger().Debugf("tcpProxy routine is exiting because of closure of finch\n", capi.Id())
			return
		case part, ok := <-part_ch:
			if ok {
				capi.client.SetWriteDeadline(time.Now().Add(capi.config.writeTimeout))
				_, err := capi.client.Write(part)
				capi.Logger().Debugf("Wrote body part. part=%v, err=%v\n", string(part), err)
				if err != nil {
					capi.Logger().Errorf("Received error when writing boby part. err=%v\n", err)
					err_ch <- err
					return
				}
			} else {
				// the closing of part_ch signals that all body parts have been sent. start receiving responses
				capi.Logger().Debugf("tcpProxy routine starting to receive response since all body parts have been sent\n", capi.Id())

				// read response
				res_buf := make([]byte, 300)
				capi.client.SetReadDeadline(time.Now().Add(capi.config.readTimeout))
				num_bytes, err := capi.client.Read(res_buf)
				capi.Logger().Debugf("read result err=%v, num_bytes=%v, res_buf=%v\n", err, num_bytes, string(res_buf))
				if err != nil {
					err_ch <- err
					return
				}

				if num_bytes == len(MalformedOK) && string(res_buf[:num_bytes]) == MalformedOK {
					// oocasionally batchUpdateDocs receive MalformedOK {{ok:true}) instead of
					// a well formed http response
					// when this happens, reset the connection to ensure that subsequent writes have a clean start
					// the previous update should have succeeded. there is no need to retry
					capi.resetConn()
					resp_ch <- true
					return
				}

				buffer := bytes.NewBuffer(res_buf)
				response, err := http.ReadResponse(bufio.NewReader(buffer), nil)

				if err != nil {
					errMsg := MalformedResponseError + fmt.Sprintf(" vb=%v. err=%v, num_bytes=%v, res_buf={%v}\n", vbno, err, num_bytes, string(res_buf[:num_bytes]))
					capi.Logger().Error(errMsg)
					err_ch <- errors.New(errMsg)
					return
				}

				capi.Logger().Debugf("http response =%v\n", response)

				if response.StatusCode != 201 {
					err_ch <- errors.New(fmt.Sprintf("receieved unexpected status code, %v, from update docs request for vb %v\n", response.StatusCode, vbno))
				} else {
					// It should not be necessary to reset connections when request succeeds. However,
					// "broken pipe" error was often received after a while when connections are shared
					// between requests. Resetting the connection for now. May need to look for a better way.
					//capi.resetConn()

					// notify caller that write succeeded
					resp_ch <- true
				}

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
	meta_map[DeletedKey] = (req.Opcode == mc.TAP_DELETE || req.Opcode == mc.UPR_DELETION || req.Opcode == mc.UPR_EXPIRATION)

	return doc_map
}

// produce serialized revision info in the form of revSeq-Cas+Expiration+Flags
func getSerializedRevision(req *mc.MCRequest) string {
	var revId [16]byte
	// CAS
	binary.BigEndian.PutUint64(revId[0:8], req.Cas)

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
	capi.Logger().Debugf("init a new batch for vb %v\n", vbno)
	capi.vb_batch_map[vbno] = &capiBatch{*newBatch(capi.config.maxCount, capi.config.maxSize, capi.config.batchExpirationTime, capi.Logger()), vbno}
}

func (capi *CapiNozzle) initialize(settings map[string]interface{}) error {
	err := capi.config.initializeConfig(settings)
	if err != nil {
		return err
	}

	capi.vb_dataChan_map = make(map[uint16]chan *base.WrappedMCRequest)
	for vbno, _ := range capi.config.vbCouchApiBaseMap {
		capi.vb_dataChan_map[vbno] = make(chan *base.WrappedMCRequest, capi.config.maxCount*100)
	}
	capi.items_in_dataChan = 0
	capi.bytes_in_dataChan = 0
	capi.batches_ready = make(chan *capiBatch, 1024)

	//enable send
	//	capi.send_allow_ch <- true

	//init new batches
	capi.vb_batch_map = make(map[uint16]*capiBatch)
	capi.vb_batch_move_lock = make(map[uint16]*sync.Mutex)
	for vbno, _ := range capi.config.vbCouchApiBaseMap {
		capi.vb_batch_move_lock[vbno] = &sync.Mutex{}
		capi.initNewBatch(vbno)
	}

	capi.checker_finch = make(chan bool, 1)

	capi.Logger().Debug("About to start initializing connection")
	if err == nil {
		err = capi.resetConn()
	}

	capi.Logger().Debug("Initialization is done")

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
	return fmt.Sprintf("Capi %v received %v items, sent %v items", capi.Id(), capi.counter_received, capi.counter_sent)
}

func (capi *CapiNozzle) handleGeneralError(err error) {
	if capi.handle_error {
		capi.Logger().Errorf("Raise error condition %v\n", err)
		otherInfo := utils.WrapError(err)
		capi.RaiseEvent(common.ErrorEncountered, nil, capi, nil, otherInfo)
	} else {
		capi.Logger().Debugf("%v in shutdown process, err=%v is ignored\n", capi.Id(), err)
	}
}

func (capi *CapiNozzle) optimisticRep(req *mc.MCRequest) bool {
	if req != nil {
		return req.Size() < capi.config.optiRepThreshold
	}
	return true
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

func (capi *CapiNozzle) resetConn() error {
	capi.Logger().Debugf("resetting capi connection for %v\n", capi.Id())

	if capi.validateRunningState() != nil {
		capi.Logger().Infof("%v is not running, no need to resetConn", capi.Id())
		return nil
	}

	if capi.client != nil {
		capi.client.Close()
	}

	pool, err := base.TCPConnPoolMgr().GetOrCreatePool(capi.getPoolName(capi.config), capi.config.connectStr, base.DefaultCAPIConnectionSize)
	if err == nil {
		capi.client, err = pool.Get()
	}

	if err == nil {
		// same settings as erlang xdcr
		capi.client.SetKeepAlive(true)
		capi.client.SetNoDelay(false)
		capi.Logger().Debugf("%v - The connection for capi client is reset successfully\n", capi.Id())
		return nil
	} else {
		capi.Logger().Errorf("%v - Connection reset failed\n", capi.Id())
		capi.handleGeneralError(err)
		return err
	}
}

func (capi *CapiNozzle) UpdateSettings(settings map[string]interface{}) error {
	optimisticReplicationThreshold, err := utils.GetIntSettingFromSettings(settings, metadata.OptimisticReplicationThreshold)
	if err != nil {
		return err
	}

	capi.config.optiRepThreshold = optimisticReplicationThreshold
	return nil
}
