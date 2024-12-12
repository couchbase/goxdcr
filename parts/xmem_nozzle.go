// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package parts

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/v8/base"
	baseclog "github.com/couchbase/goxdcr/v8/base/conflictlog"
	"github.com/couchbase/goxdcr/v8/common"
	"github.com/couchbase/goxdcr/v8/conflictlog"
	"github.com/couchbase/goxdcr/v8/crMeta"
	"github.com/couchbase/goxdcr/v8/hlv"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
	utilities "github.com/couchbase/goxdcr/v8/utils"
	"github.com/golang/snappy"
)

// configuration settings for XmemNozzle
const (
	SETTING_RESP_TIMEOUT             = "resp_timeout"
	XMEM_SETTING_DEMAND_ENCRYPTION   = "demandEncryption"
	XMEM_SETTING_ENCRYPTION_TYPE     = "encryptionType"
	XMEM_SETTING_CERTIFICATE         = metadata.XmemCertificate
	XMEM_SETTING_INSECURESKIPVERIFY  = "insecureSkipVerify"
	XMEM_SETTING_SAN_IN_CERITICATE   = "SANInCertificate"
	XMEM_SETTING_REMOTE_MEM_SSL_PORT = "remote_ssl_port"
	XMEM_SETTING_CLIENT_CERTIFICATE  = metadata.XmemClientCertificate
	XMEM_SETTING_CLIENT_KEY          = metadata.XmemClientKey

	default_demandEncryption bool = false
)

var xmem_setting_defs base.SettingDefinitions = base.SettingDefinitions{
	SETTING_BATCHCOUNT:              base.NewSettingDef(reflect.TypeOf((*int)(nil)), true),
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
	XMEM_SETTING_ENCRYPTION_TYPE:    base.NewSettingDef(reflect.TypeOf((*string)(nil)), false),
	XMEM_SETTING_CERTIFICATE:        base.NewSettingDef(reflect.TypeOf((*[]byte)(nil)), false),
	XMEM_SETTING_SAN_IN_CERITICATE:  base.NewSettingDef(reflect.TypeOf((*bool)(nil)), false),
	XMEM_SETTING_INSECURESKIPVERIFY: base.NewSettingDef(reflect.TypeOf((*bool)(nil)), false),
	XMEM_DEV_MAIN_SLEEP_DELAY:       base.NewSettingDef(reflect.TypeOf((*int)(nil)), false),
	XMEM_DEV_BACKFILL_SLEEP_DELAY:   base.NewSettingDef(reflect.TypeOf((*int)(nil)), false),
}

var UninitializedReseverationNumber = -1

func GetErrorXmemTargetUnknownCollection(mcr *base.WrappedMCRequest) error {
	return fmt.Errorf("%v scope %v collection %v given known manifest ID %v", base.StringTargetCollectionMappingErr,
		mcr.GetSourceCollectionNamespace().ScopeName, mcr.GetSourceCollectionNamespace().CollectionName, mcr.GetManifestId())
}

var ErrorXmemIsStuck = errors.New("Xmem is stuck")

var ErrorBufferInvalidState = errors.New("xmem buffer is in invalid state")

var ErrorFatalError = errors.New("received connection fatal error")

type ConflictResolver func(req *base.WrappedMCRequest, resp *mc.MCResponse, specs []base.SubdocLookupPathSpec, sourceId, targetId hlv.DocumentSourceId, logConflict bool, xattrEnabled bool, uncompressFunc base.UncompressFunc, logger *log.CommonLogger) (crMeta.ConflictDetectionResult, crMeta.ConflictResolutionResult, error)

var GetMetaClientName = "client_getMeta"
var SetMetaClientName = "client_setMeta"

/************************************
/* struct bufferedMCRequest
*************************************/

type bufferedMCRequest struct {
	req          *base.WrappedMCRequest
	sent_time    *time.Time
	num_of_retry int
	timedout     bool
	reservation  int
	// whether a mutation locked response has been received for the corresponding MCRequest
	// this affects the number of retries allowed on the MCRequest
	mutationLocked bool
	// If a mutation was not able to be sent because target KV was unable to find the specified collectionId
	collectionMapErr bool

	guardrailsHit [base.NumberOfGuardrailTypes]bool

	lock sync.RWMutex
}

func (req *bufferedMCRequest) setGuardrail(status mc.Status) {
	req.lock.Lock()
	defer req.lock.Unlock()
	idx := getGuardrailIdx(status)
	req.guardrailsHit[idx] = true
}

func newBufferedMCRequest() *bufferedMCRequest {
	return &bufferedMCRequest{req: nil,
		sent_time:    nil,
		num_of_retry: 0,
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
	request.mutationLocked = false
	request.collectionMapErr = false
	request.reservation = UninitializedReseverationNumber
}

/*
**********************************************************
/* struct requestBuffer
/* This is used to buffer the sent but yet confirmed data
***********************************************************
*/
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
	datapool         base.DataPool
}

func newReqBuffer(size uint16, threshold uint16, token_ch chan int, logger *log.CommonLogger, pool base.DataPool) *requestBuffer {
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
		occupied_count:   0,
		datapool:         pool,
	}

	logger.Debug("Slots have been initialized")

	buf.initialize()

	logger.Debugf("New request buffer of size %d has been created\n", size)
	return buf
}

func (buf *requestBuffer) close() {
	close(buf.fin_ch)
	buf.logger.Info("Request buffer has been closed. No blocking on flow control")
}

// not concurrent safe. Caller need to be aware
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

// blocking until the occupied slots are below threshold
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

// slot allow caller to get hold of the content in the slot without locking the slot
// @pos - the position of the slot
func (buf *requestBuffer) slot(pos uint16) (*base.WrappedMCRequest, error) {
	err := buf.validatePos(pos)
	if err != nil {
		return nil, err
	}

	req := buf.slots[pos]

	req.lock.RLock()
	defer req.lock.RUnlock()

	return req.req, nil
}

func (buf *requestBuffer) slotWithSentTime(pos uint16) (*base.WrappedMCRequest, *time.Time, error) {
	err := buf.validatePos(pos)
	if err != nil {
		return nil, nil, err
	}

	req := buf.slots[pos]

	req.lock.RLock()
	defer req.lock.RUnlock()

	return req.req, req.sent_time, nil
}

func (buf *requestBuffer) slotWithGuardrail(pos uint16) (*base.WrappedMCRequest, [base.NumberOfGuardrailTypes]bool, error) {
	var retSlice [base.NumberOfGuardrailTypes]bool
	err := buf.validatePos(pos)
	if err != nil {
		return nil, retSlice, err
	}

	req := buf.slots[pos]

	req.lock.RLock()
	defer req.lock.RUnlock()

	for i := 0; i < base.NumberOfGuardrailTypes; i++ {
		retSlice[i] = req.guardrailsHit[i]
	}
	return req.req, retSlice, nil
}

// modSlot allow caller to do book-keeping on the slot, like updating num_of_retry
// @pos - the position of the slot
// @modFunc - the callback function which is going to update the slot
func (buf *requestBuffer) modSlot(pos uint16, modFunc func(req *bufferedMCRequest, p uint16) (bool, error)) (bool, error) {
	err := buf.validatePos(pos)
	if err != nil {
		return false, err
	}

	req := buf.slots[pos]
	return modFunc(req, pos)
}

// evictSlot allow caller to empty the slot
// @pos - the position of the slot
func (buf *requestBuffer) evictSlot(pos uint16) error {
	// set reservation_num to -1 to skip reservation number check
	return buf.clearSlot(pos, -1 /*reservation_num*/)
}

// if reservation_num >=0, perform reservation number check
// otherwise, skip reservation number check
func (buf *requestBuffer) clearSlot(pos uint16, reservation_num int) error {
	err := buf.validatePos(pos)
	if err != nil {
		return err
	}

	req := buf.slots[pos]
	req.lock.Lock()
	defer req.lock.Unlock()

	for i := 0; i < base.NumberOfGuardrailTypes; i++ {
		req.guardrailsHit[i] = false
	}

	if req.req == nil {
		return nil
	}

	if reservation_num >= 0 && req.reservation != reservation_num {
		return errors.New("Clear slot failed, reservation number doesn't match")
	}

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
			default:
			}
		} else {
			buf.logger.Debugf("buffer's occupied slots is below threshold %v, no notify channel is specified though", buf.notify_threshold)
		}
	}

	return nil
}

func (buf *requestBuffer) cancelReservation(index uint16, reservation_num int) error {
	return buf.clearSlot(index, reservation_num)
}

func (buf *requestBuffer) enSlot(mcReq *base.WrappedMCRequest) (uint16, int, []byte, error) {
	var index uint16

	select {
	case index = <-buf.empty_slots_pos:
		break
	case <-buf.fin_ch:
		return 0, 0, nil, PartStoppedError
	}

	//non blocking
	//generate a random number
	reservation_num := rand.Int()

	req := buf.slots[index]
	req.lock.Lock()
	defer req.lock.Unlock()

	if req.req != nil || req.reservation != UninitializedReseverationNumber {
		panic(fmt.Sprintf("reserveSlot called on non-empty slot. req=%v, reservation=%v\n", req.req, req.reservation))
	}

	req.reservation = reservation_num
	req.req = mcReq
	buf.adjustRequest(mcReq, index)
	itemBytes, dpErr := buf.datapool.GetByteSlice(uint64(mcReq.Req.Size()))
	if dpErr != nil {
		itemBytes = mcReq.GetReqBytes()
	} else {
		mcReq.GetReqBytesPreallocated(itemBytes)
	}
	now := time.Now()
	req.sent_time = &now
	buf.token_ch <- 1

	//increase the occupied_count
	atomic.AddInt32(&buf.occupied_count, 1)
	return index, reservation_num, itemBytes, nil
}

// always called with lock on buf.slots[index]. no need for separate lock on buf.sequences[index]
func (buf *requestBuffer) adjustRequest(req *base.WrappedMCRequest, index uint16) {
	mc_req := req.Req
	mc_req.Opaque = base.GetOpaque(index, buf.sequences[int(index)])

	// Set opcode and CAS for normal (non-mobile/non-CCR) mode.
	if !req.HLVModeOptions.NoTargetCR {
		// No need for Cas locking in normal case (non-mobile/non-CCR mode) when target CR is expected
		// Wildcard CAS of 0 will not result in KEY_EEXISTS even if the request CAS != doc CAS
		mc_req.Cas = 0
	}
	mc_req.Opcode = req.GetMemcachedCommand()
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

func (buf *requestBuffer) getGuardRailHit() [base.NumberOfGuardrailTypes]bool {
	var guardrailHit [base.NumberOfGuardrailTypes]bool
	for i := uint16(0); i < buf.bufferSize(); i++ {
		_, guardrailPerBuf, err := buf.slotWithGuardrail(i)
		if err != nil {
			continue
		}
		for j := 0; j < base.NumberOfGuardrailTypes; j++ {
			if !guardrailHit[j] && guardrailPerBuf[j] {
				// Set if not set
				guardrailHit[j] = true
			}
		}
	}
	return guardrailHit
}

/*
***********************************
/* struct xmemConfig
************************************
*/
type xmemConfig struct {
	baseConfig
	bucketName string
	//the duration to wait for the batch-sending to finish
	certificate        []byte
	demandEncryption   bool
	encryptionType     string
	memcached_ssl_port uint16
	// in ssl over mem mode, whether target cluster supports SANs in certificates
	san_in_certificate             bool
	clientCertificate              []byte
	clientKey                      []byte
	respTimeout                    unsafe.Pointer // *time.Duration
	max_read_downtime              time.Duration
	logger                         *log.CommonLogger
	maxRetryMutationLocked         int
	maxRetryIntervalMutationLocked time.Duration
}

func newConfig(logger *log.CommonLogger) xmemConfig {
	config := xmemConfig{
		baseConfig: baseConfig{maxCount: -1,
			maxSize:             -1,
			writeTimeout:        base.XmemWriteTimeout,
			readTimeout:         base.XmemReadTimeout,
			maxRetryInterval:    base.XmemMaxRetryInterval,
			maxRetry:            base.XmemMaxRetry,
			selfMonitorInterval: base.XmemSelfMonitorInterval,
			connectStr:          "",
			username:            "",
			password:            "",
			mobileCompatible:    base.MobileCompatibilityOff,
		},
		bucketName:                     "",
		demandEncryption:               default_demandEncryption,
		certificate:                    []byte{},
		max_read_downtime:              base.XmemMaxReadDownTime,
		memcached_ssl_port:             0,
		logger:                         logger,
		maxRetryMutationLocked:         base.XmemMaxRetryMutationLocked,
		maxRetryIntervalMutationLocked: base.XmemMaxRetryIntervalMutationLocked,
	}

	atomic.StoreUint32(&config.maxIdleCount, uint32(base.XmemMaxIdleCount))
	resptimeout := base.XmemDefaultRespTimeout
	atomic.StorePointer(&config.respTimeout, unsafe.Pointer(&resptimeout))

	return config

}

func (config *xmemConfig) initializeConfig(settings metadata.ReplicationSettingsMap, utils utilities.UtilsIface) error {
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

			if val, ok := settings[XMEM_SETTING_ENCRYPTION_TYPE]; ok {
				config.encryptionType = val.(string)
			} else {
				return errors.New("demandEncryption=true, but encryptionType is not set in settings")
			}

			if val, ok := settings[XMEM_SETTING_CLIENT_CERTIFICATE]; ok {
				config.clientCertificate = val.([]byte)
			}

			if val, ok := settings[XMEM_SETTING_CLIENT_KEY]; ok {
				config.clientKey = val.([]byte)
			}

			if val, ok := settings[XMEM_SETTING_REMOTE_MEM_SSL_PORT]; ok {
				config.memcached_ssl_port = val.(uint16)

				if val, ok := settings[XMEM_SETTING_SAN_IN_CERITICATE]; ok {
					config.san_in_certificate = val.(bool)
				} else {
					return errors.New("SANInCertificate is not set in settings")
				}
			} else {
				if config.demandEncryption && config.encryptionType == metadata.EncryptionType_Full {
					return errors.New("demandEncryption=true and encryptionType=full, but remote_ssl_port is set in settings")
				}
			}
		}
		if val, ok := settings[base.VersionPruningWindowHrsKey]; ok {
			config.hlvPruningWindowSec = uint32(val.(int)) * 60 * 60
		}
		if val, ok := settings[base.EnableCrossClusterVersioningKey]; ok {
			config.crossClusterVers = val.(bool)
		}
		if config.crossClusterVers {
			config.vbHlvMaxCas = make(map[uint16]uint64)
			if val, ok := settings[base.HlvVbMaxCasKey]; ok {
				vbMaxCas := val.([]interface{})
				for i, casObj := range vbMaxCas {
					casStr := casObj.(string)
					cas, err := strconv.ParseUint(casStr, 10, 64)
					if err != nil {
						return fmt.Errorf("bad value %v in vbucketsMaxCas %v. err=%v", casStr, vbMaxCas, err)
					}
					config.vbHlvMaxCas[uint16(i)] = cas
				}
			}

			if val, ok := settings[base.TargetHlvVbMaxCasKey]; ok {
				config.targetVbHlvMaxCas = make(map[uint16]uint64)
				targetVbMaxCas := val.([]interface{})
				for i, casObj := range targetVbMaxCas {
					casStr := casObj.(string)
					cas, err := strconv.ParseUint(casStr, 10, 64)
					if err != nil {
						return fmt.Errorf("bad value %v in targetVbucketsMaxCas %v. err=%v", casStr, targetVbMaxCas, err)
					}
					config.targetVbHlvMaxCas[uint16(i)] = cas
				}
			}
		}
		if val, ok := settings[MOBILE_COMPATBILE]; ok {
			config.mobileCompatible = uint32(val.(int))
		}
	}
	return err
}

/*
***********************************
/* struct opaque_KeySeqnoMap
************************************
*/
type opaqueKeySeqnoMap map[uint32][]interface{}

/**
 * The interface slice is composed of:
 * 1. documentKey
 * 2. Sequence number
 * 3. vbucket number
 * 4. time.Now()
 * 5. ManifestId
 */

// This function will ensure that the returned interface slice will have distinct copies of argument references
// since everything coming in is passing by value
func compileOpaqueKeySeqnoValue(docKey string, seqno uint64, vbucket uint16, timeNow time.Time, manifestId uint64) []interface{} {
	return []interface{}{docKey, seqno, vbucket, timeNow, manifestId}
}

func (omap opaqueKeySeqnoMap) Clone() opaqueKeySeqnoMap {
	newMap := make(opaqueKeySeqnoMap)
	for k, v := range omap {
		newMap[k] = compileOpaqueKeySeqnoValue(v[0].(string), v[1].(uint64), v[2].(uint16), v[3].(time.Time), v[4].(uint64))
	}
	return newMap
}

func (omap opaqueKeySeqnoMap) Redact() opaqueKeySeqnoMap {
	for _, v := range omap {
		if !base.IsStringRedacted(v[0].(string)) {
			v[0] = base.TagUD(v[0])
		}
	}
	return omap
}

func (omap opaqueKeySeqnoMap) CloneAndRedact() opaqueKeySeqnoMap {
	clonedMap := make(opaqueKeySeqnoMap)
	for k, v := range omap {
		if !base.IsStringRedacted(v[0].(string)) {
			clonedMap[k] = compileOpaqueKeySeqnoValue(base.TagUD(v[0].(string)), v[1].(uint64), v[2].(uint16), v[3].(time.Time), v[4].(uint64))
		} else {
			clonedMap[k] = v
		}
	}
	return clonedMap
}

/*
***********************************
/* struct XmemNozzle
************************************
*/
type XmemNozzle struct {
	AbstractPart

	//remote cluster reference for retrieving up to date remote cluster reference
	remoteClusterSvc service_def.RemoteClusterSvc

	// used in HLV "source" actor ID
	sourceClusterUuid string
	targetClusterUuid string
	sourceBucketUuid  string
	targetBucketUuid  string

	// Source and target actor ID. Used for HLV in XATTR to identify source of changes.
	sourceActorId hlv.DocumentSourceId
	targetActorId hlv.DocumentSourceId

	bOpen      bool
	lock_bOpen sync.RWMutex

	//data channel to accept the incoming data
	dataChan chan *base.WrappedMCRequest
	//the total size of data (in byte) queued in dataChan
	bytes_in_dataChan int32
	dataChan_control  chan bool

	//memcached client connected to the target bucket
	client_for_setMeta *base.XmemClient
	client_for_getMeta *base.XmemClient

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

	finish_ch chan bool

	counter_sent                uint64 // sent item count going through dataChan. sent = received - from_target + retry_cr
	counter_received            uint64
	counter_ignored             uint64
	counter_compressed_received uint64
	counter_compressed_sent     uint64
	counter_retry_cr            uint64
	counter_to_resolve          uint64
	counter_to_setback          uint64
	counter_from_target         uint64
	counter_waittime            uint64
	counterNumGetMeta           uint64
	counterNumSubdocGet         uint64
	start_time                  time.Time
	counter_resend              uint64
	counter_tmperr              uint64 // count TMPERR for commands except getMeta. TMPERR for getMeta not counted since it doesn't block replication
	counter_eaccess             uint64 // count EACCESS for commands except getMeta. EACCESS for getMeta not counted since it doesn't block replication
	counter_locked              uint64 // counter of times LOCKED status is returned by KV
	counterGuardrailHit         uint64
	counterUnknownStatus        uint64
	counterNumGetEx             uint64
	counterNumGetCompressed     uint64

	receive_token_ch chan int

	connType base.ConnType

	topic                      string
	last_ten_batches_size      []uint32
	last_ten_batches_size_lock sync.RWMutex

	// whether lww conflict resolution mode has been enabled
	source_cr_mode base.ConflictResolutionMode

	// whether xattr has been enabled in the memcached connection to target
	xattrEnabled bool

	sourceBucketName string

	getMetaUserAgent string
	setMetaUserAgent string

	bandwidthThrottler service_def.BandwidthThrottlerSvc
	conflictMgr        service_def.ConflictManagerIface
	compressionSetting base.CompressionType
	utils              utilities.UtilsIface

	// Protect data memebers that may be accessed concurrently by Start() and Stop()
	// i.e., buf, dataChan, client_for_setMeta, client_for_setMeta
	// Access to these data members in other parts of xmem do not need to be protected,
	// since such access can only happen after Start() has completed successfully
	// and after the data members have been initialized and set
	stateLock sync.RWMutex

	vbList []uint16

	collectionEnabled uint32

	dataPool base.DataPool

	upstreamObjRecycler utilities.RecycleObjFunc

	upstreamErrReporter utilities.ErrReportFunc

	// If XError is enabled, then as part of connection initialization, the target
	// memcached will return an error map that contains more details about
	// what errors get returned and what they mean
	memcachedErrMap map[string]interface{}

	eventsProducer common.PipelineEventsProducer

	// position of the item in the buffer to error code mapping (for which UI alert is to be raised)
	errsForUIAlert    map[uint16]mc.Status
	errsForUIAlertMtx sync.RWMutex

	// for tracking temporary memcached errors
	tempMCErrMtx       sync.RWMutex
	tempMCErrLastSeen  time.Time
	tempMCErrSetShow   *time.Timer
	tempMCErrShow      bool
	tempMCErrUnsetShow *time.Timer

	importMutationEventRaised uint32
	importMutationEventId     int64

	mcRequestPool *base.MCRequestPool

	pipelineType common.PipelineType

	// pipeline level conflict logger
	conflictLogger baseclog.Logger

	stopOnce uint32
}

func getGuardrailIdx(status mc.Status) int {
	return int(status) - int(mc.BUCKET_RESIDENT_RATIO_TOO_LOW)
}

func getMcStatusFromGuardrailIdx(idx int) mc.Status {
	return mc.Status(int(mc.BUCKET_RESIDENT_RATIO_TOO_LOW) + idx)
}

func NewXmemNozzle(id string, remoteClusterSvc service_def.RemoteClusterSvc, sourceBucketUuid string, targetClusterUuid string, topic string, connPoolNamePrefix string, connPoolConnSize int,
	connectString string, sourceBucketName string, targetBucketName string, targetBucketUuid string, username string, password string, source_cr_mode base.ConflictResolutionMode,
	logger_context *log.LoggerContext, utilsIn utilities.UtilsIface, vbList []uint16, eventsProducer common.PipelineEventsProducer, sourceClusterUUID string, sourceHostname string, pipelineType common.PipelineType) *XmemNozzle {

	part := NewAbstractPartWithLogger(id, log.NewLogger("XmemNozzle", logger_context))

	xmem := &XmemNozzle{
		AbstractPart:        part,
		remoteClusterSvc:    remoteClusterSvc,
		sourceBucketUuid:    sourceBucketUuid,
		targetClusterUuid:   targetClusterUuid,
		bOpen:               true,
		lock_bOpen:          sync.RWMutex{},
		dataChan:            nil,
		receive_token_ch:    nil,
		client_for_setMeta:  nil,
		client_for_getMeta:  nil,
		config:              newConfig(part.Logger()),
		batches_ready_queue: nil,
		batch:               nil,
		batch_lock:          make(chan bool, 1),
		childrenWaitGrp:     sync.WaitGroup{},
		buf:                 nil,
		finish_ch:           make(chan bool, 1),
		counter_sent:        0,
		counter_received:    0,
		counter_ignored:     0,
		counter_waittime:    0,
		counterNumGetMeta:   0,
		counterNumSubdocGet: 0,
		counter_tmperr:      0,
		counter_eaccess:     0,
		topic:               topic,
		source_cr_mode:      source_cr_mode,
		sourceBucketName:    sourceBucketName,
		targetBucketUuid:    targetBucketUuid,
		utils:               utilsIn,
		vbList:              vbList,
		collectionEnabled:   1, /*Default to true unless otherwise disabled*/
		eventsProducer:      eventsProducer,
		errsForUIAlert:      make(map[uint16]mc.Status),
		mcRequestPool:       base.NewMCRequestPool(id, nil),
		sourceClusterUuid:   sourceClusterUUID,
		pipelineType:        pipelineType,
	}

	xmem.last_ten_batches_size = []uint32{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

	// set conflict resolver
	xmem.conflict_resolver = xmem.getConflictDetector(xmem.source_cr_mode)

	xmem.config.connectStr = connectString
	xmem.config.bucketName = targetBucketName
	xmem.config.username = username
	xmem.config.password = password
	xmem.config.connPoolNamePrefix = connPoolNamePrefix
	xmem.config.connPoolSize = connPoolConnSize
	xmem.config.sourceHostname = sourceHostname
	xmem.config.targetHostname = base.GetHostName(connectString)

	return xmem
}

func (xmem *XmemNozzle) SetBandwidthThrottler(bandwidthThrottler service_def.BandwidthThrottlerSvc) {
	xmem.bandwidthThrottler = bandwidthThrottler
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

func (xmem *XmemNozzle) Start(settings metadata.ReplicationSettingsMap) error {
	t := time.Now()
	xmem.Logger().Infof("%v starting ....settings=%v\n", xmem.Id(), settings.CloneAndRedact())
	defer func() {
		xmem.Logger().Infof("%v took %vs to start\n", xmem.Id(), time.Since(t).Seconds())
	}()

	err := xmem.SetState(common.Part_Starting)
	if err != nil {
		return err
	}

	xmem.dataPool = base.NewDataPool()

	err = xmem.initialize(settings)
	if err != nil {
		return err
	}

	xmem.Logger().Infof("%v finished initializing.", xmem.Id())
	xmem.childrenWaitGrp.Add(1)
	go xmem.selfMonitor(xmem.finish_ch, &xmem.childrenWaitGrp)

	xmem.childrenWaitGrp.Add(1)
	go xmem.receiveResponse(xmem.finish_ch, &xmem.childrenWaitGrp)

	xmem.childrenWaitGrp.Add(1)
	go xmem.checkAndRepairBufferMonitor(xmem.finish_ch, &xmem.childrenWaitGrp)

	xmem.childrenWaitGrp.Add(1)
	go xmem.processData_sendbatch(xmem.finish_ch, &xmem.childrenWaitGrp)

	xmem.start_time = time.Now()

	err = xmem.SetState(common.Part_Running)
	if err != nil {
		return err
	}

	xmem.Logger().Infof("%v has been started", xmem.Id())

	return nil
}

func (xmem *XmemNozzle) Stop() error {
	if !atomic.CompareAndSwapUint32(&xmem.stopOnce, 0, 1) {
		return PartAlreadyStoppedError
	}

	xmem.Logger().Infof("Stopping %v\n", xmem.Id())
	err := xmem.SetState(common.Part_Stopping)
	if err != nil {
		return err
	}

	xmem.Logger().Debugf("%v processed %v items\n", xmem.Id(), atomic.LoadUint64(&xmem.counter_sent))

	xmem.onExit()

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
			xmem.Logger().Warnf("%v recovered from panic in batchReady. arg=%v\n", xmem.Id(), r)
			if xmem.validateRunningState() == nil {
				// report error only when xmem is still in running state
				xmem.handleGeneralError(errors.New(fmt.Sprintf("%v", r)))
			}
		}
	}()

	//move the batch to ready batches channel
	// no need to lock batch since batch is always locked before entering batchReady()
	count := xmem.batch.count()
	if count > 0 {
		select {
		case xmem.batches_ready_queue <- xmem.batch:
			xmem.initNewBatch()
		// provides an alterntive exit path when xmem stops
		case <-xmem.finish_ch:
			return PartStoppedError
		}
	}
	return nil
}

func (xmem *XmemNozzle) Receive(data interface{}) error {
	defer func() {
		if r := recover(); r != nil {
			xmem.Logger().Warnf("%v recovered from panic in Receive. arg=%v", xmem.Id(), r)
			xmem.handleGeneralError(errors.New(fmt.Sprintf("%v", r)))
		}
	}()

	err := xmem.validateRunningState()
	if err != nil {
		xmem.Logger().Warnf("%v is in %v state, Receive did no-op", xmem.Id(), xmem.State())
		return err
	}

	request, ok := data.(*base.WrappedMCRequest)

	if !ok {
		xmem.Logger().Errorf("Got data of unexpected type. data=%v%v%v", base.UdTagBegin, data, base.UdTagEnd)
		err = fmt.Errorf("Got data of unexpected type")
		xmem.handleGeneralError(errors.New(fmt.Sprintf("%v", err)))
		return err
	}

	err = xmem.accumuBatch(request)
	if err != nil {
		xmem.handleGeneralError(err)
	}

	request.SiblingReqsMtx.RLock()
	for _, siblingReq := range request.SiblingReqs {
		err = xmem.accumuBatch(siblingReq)
		if err != nil {
			xmem.handleGeneralError(err)
		}
	}
	request.SiblingReqsMtx.RUnlock()

	return err
}

func (xmem *XmemNozzle) accumuBatch(request *base.WrappedMCRequest) error {
	if string(request.Req.Key) == "" {
		xmem.Logger().Errorf("%v accumuBatch received request with Empty key, req.UniqueKey=%v%s%v\n", xmem.Id(), base.UdTagBegin, request.UniqueKey, base.UdTagEnd)
		err := fmt.Errorf("%v accumuBatch received request with Empty key\n", xmem.Id())
		return err
	}

	xmem.checkAndUpdateReceivedStats(request)

	// For custom CR, we don't need to send document if target is the source of change.
	fromTarget, err := xmem.isChangesFromTarget(request)
	if err != nil {
		// We shouldn't hit error here. If we do, log an error. We will just treat the document as not from target
		xmem.Logger().Warnf("%v accumuBatch received error%v parsing the XATTR to determine if the change is from target for custom CR, req.UniqueKey=%v%s%v",
			xmem.Id(), err, base.UdTagBegin, request.UniqueKey, base.UdTagEnd)
	} else if fromTarget {
		// Change is from target. Don't replicate back
		additionalInfo := TargetDataSkippedEventAdditional{Seqno: request.Seqno,
			VbucketCommon: VbucketCommon{VBucket: request.Req.VBucket},
			Opcode:        request.GetMemcachedCommand(),
			IsExpirySet:   (len(request.Req.Extras) >= 8 && binary.BigEndian.Uint32(request.Req.Extras[4:8]) != 0),
			ManifestId:    request.GetManifestId(),
		}
		if request.OrigSrcVB != nil {
			additionalInfo.SetOrigSrcVB(*request.OrigSrcVB)
		}
		xmem.RaiseEvent(common.NewEvent(common.TargetDataSkipped, nil, xmem, nil, additionalInfo))
		xmem.recycleDataObj(request)
		atomic.AddUint64(&xmem.counter_from_target, 1)
		return nil
	}
	xmem.batch_lock <- true
	defer func() { <-xmem.batch_lock }()

	err = xmem.writeToDataChan(request)
	if err != nil {
		return err
	}

	curCount, _, isFull, err := xmem.batch.accumuBatch(request, xmem.optimisticRep)
	if err != nil {
		return err
	}

	if curCount > 0 {
		atomic.StoreUint32(&xmem.cur_batch_count, curCount)
	}
	if isFull {
		err = xmem.batchReady()
		if err != nil {
			return err
		}
	}
	return nil
}

func (xmem *XmemNozzle) checkAndUpdateReceivedStats(request *base.WrappedMCRequest) {
	if request.RetryCRCount > 0 {
		return
	}
	atomic.AddUint64(&xmem.counter_received, 1)
	if checkBool, err := base.IsRequestCompressedType(request, base.CompressionTypeSnappy); checkBool && err == nil {
		atomic.AddUint64(&xmem.counter_compressed_received, 1)
	} else if err != nil {
		xmem.Logger().Warnf("Error %v received while checking for data type %v", err, base.CompressionTypeSnappy)
	}
}

func (xmem *XmemNozzle) checkAndUpdateSentStats(request *base.WrappedMCRequest) {
	atomic.AddUint64(&xmem.counter_sent, 1)
	if checkBool, err := base.IsRequestCompressedType(request, base.CompressionTypeSnappy); checkBool && err == nil {
		atomic.AddUint64(&xmem.counter_compressed_sent, 1)
	} else if err != nil {
		xmem.Logger().Warnf("Error %v received while checking for data type %v", err, base.CompressionTypeSnappy)
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
//  1. accumuBatch acquires batch_lock and tries to move xmem.batch into batches_ready_queue,
//     which gets blocked because batches_ready_queue is full
//  2. getBatchNonEmptyCh() is called within processData_sendbatch, which gets blocked since it
//     cannot acquire batch_lock
//  3. Once getBatchNonEmptyCh() is blocked, processData_sendbatch can never move to the next iteration and
//     take batches off batches_ready_queue so as to unblock accumuBatch.
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
		case batch := <-xmem.batches_ready_queue:
			if xmem.validateRunningState() != nil {
				xmem.Logger().Infof("%v has stopped.", xmem.Id())
				goto done
			}

			// batchGet may use getMeta or subdoc_multi_lookup to get the results.
			// the getLookupMap will act like sendLookupMap (the docs which needs to be sent to target)
			// If conflict logging is on or we are doing CCR, we will have non-nil conflictMap and conflictLookupMap.
			noRepMap, conflictMap, sendLookupMap, conflictLookupMap, err := xmem.batchGet(batch.getMetaMap, nil)
			if err != nil {
				if err == PartStoppedError {
					goto done
				}
				xmem.handleGeneralError(err)
			}
			if len(conflictMap) > 0 && (xmem.conflictLoggingEnabled() || xmem.isCCR()) {
				// For all the keys with conflict, we will fetch the document body with all xattrs,
				// for conflict logging and CCR. GetEx command is used here. The responses of this batchGet
				// will internally be compared with the responses of conflictLookupMap to make sure that the
				// state of a given target document hasn't changed between the two batchGets. If it has changed,
				// it will be added in noRepMap2 and retried later.
				// The getLookupMap returned now will act like conflictLookupMap for the batch i.e.
				// info to be conflict logged and/or to resolve the conflict using CCR.
				noRepMap2, _, conflictLookupMap2, _, err := xmem.batchGet(conflictMap, conflictLookupMap)
				if err != nil {
					if err == PartStoppedError {
						goto done
					}
					xmem.handleGeneralError(err)
				}
				// Update the maps with the second round lookup.
				// These are the documents that changed on target between the first batchGet and the second batchGet.
				for k, v := range noRepMap2 {
					noRepMap[k] = v
				}
				batch.conflictLookupMap = conflictLookupMap2
			}
			// These are the documents that need not be replicated in this iteration.
			batch.noRepMap = noRepMap
			// These are the documents as a result of batchGet.
			batch.sendLookupMap = sendLookupMap

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
					err = xmem.batchReady()
					<-xmem.batch_lock
					if err != nil {
						if err == PartStoppedError {
							goto done
						} else {
							xmem.handleGeneralError(err)
						}
					}
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
	if !xmem.IsOpen() {
		return nil
	}

	xmem.buf.flowControl()
	return xmem.sendSetMeta_internal(batch)
}

func (xmem *XmemNozzle) onExit() {
	buf := xmem.getRequestBuffer()
	if buf != nil {
		buf.close()
	}

	//notify the data processing routine
	close(xmem.finish_ch)

	go xmem.finalCleanup()
}

func (xmem *XmemNozzle) finalCleanup() {
	xmem.childrenWaitGrp.Wait()

	//cleanup
	client_for_setMeta := xmem.getClient(true /*isSetMeta*/)
	if client_for_setMeta != nil {
		client_for_setMeta.Close()
	}
	client_for_getMeta := xmem.getClient(false /*isSetMeta*/)
	if client_for_getMeta != nil {
		client_for_getMeta.Close()
	}

	//recycle all the bufferred MCRequest to object pool
	buf := xmem.getRequestBuffer()
	if buf != nil {
		xmem.Logger().Infof("%v recycling %v objects in buffer\n", xmem.Id(), buf.itemCountInBuffer())
		for _, bufferredReq := range buf.slots {
			xmem.cleanupBufferedMCRequest(bufferredReq)
		}
	}
	xmem.conflictMgr = nil
}

func (xmem *XmemNozzle) cleanupBufferedMCRequest(req *bufferedMCRequest) {
	req.lock.Lock()
	defer req.lock.Unlock()
	if req.req != nil {
		xmem.recycleDataObj(req.req)
		resetBufferedMCRequest(req)
	}
}

func (xmem *XmemNozzle) batchSetMetaWithRetry(batch *dataBatch, numOfRetry int) error {
	var err error
	count := batch.count()
	batch_replicated_count := 0
	reqs_bytes := [][]byte{}

	// When processing a batch, the batch's responses are used to compose databytes to be sent
	// The responses can only be GC'ed after the actual bytes have been sent over the wire
	// to prevent zeroing out data prior to send
	// This is a list that stores such these responses
	respToGc := make(map[*mc.MCResponse]bool)
	defer func() {
		// make sure at the end, everything left is recycled
		for oneResp := range respToGc {
			oneResp.Recycle()
		}
	}()

	// A list of reservations for each request in the batch
	index_reservation_list := make([][]uint16, base.XmemMaxBatchSize+1)

	for i := 0; i < int(count); i++ {
		// Get a MCRequest from the data channel, which is a part of the batch
		item, err := xmem.readFromDataChan()
		if err != nil {
			return err
		}
		xmem.checkAndUpdateSentStats(item)
		if item != nil {
			xmem.checkSendDelayInjection()

			atomic.AddUint64(&xmem.counter_waittime, uint64(time.Since(item.Start_time).Seconds()*1000))

			// check if conflict logging needs to be done for this item
			cLogOptions := item.ConflictLoggerOptions
			cLogEnabled := cLogOptions != nil && cLogOptions.Enabled
			if cLogEnabled {
				resp, err := batch.conflictLookupMap.deregisterLookup(item.UniqueKey)
				if err == nil {
					// err is nil as in there was a conflict for this item. Log it in a conflict bucket.
					xmem.handleConflict(item, resp)

					// The log function performs a clone of necessary information to log,
					// safe to recycle.
					respToGc[resp.Resp] = true
				}
			}

			needSendStatus, err := needSend(item, batch, xmem.Logger())
			if err != nil {
				return err
			}
			switch needSendStatus {
			case Send:
				lookupResp, err := batch.sendLookupMap.deregisterLookup(item.UniqueKey)
				if xmem.nonOptimisticCROnly(item) && err != nil {
					// only relevent when only source CR is to be done i.e. mobile and CCR mode
					xmem.Logger().Warnf("For unique-key %v%s%v, error deregistering lookupResp for Send, err=%v", base.UdTagBegin, item.UniqueKey, base.UdTagEnd, err)
				}

				if lookupResp != nil && lookupResp.Resp.Opcode == mc.SUBDOC_MULTI_LOOKUP {
					item.HLVModeOptions.NoTargetCR = true
				} else {
					item.HLVModeOptions.NoTargetCR = false
				}

				if lookupResp != nil && lookupResp.Resp != nil {
					respToGc[lookupResp.Resp] = true
				}

				err = xmem.preprocessMCRequest(item, lookupResp)
				if err != nil {
					return err
				}

				//blocking
				index, reserv_num, item_bytes, err := xmem.buf.enSlot(item)
				if err != nil {
					return err
				}

				reqs_bytes = append(reqs_bytes, item_bytes)

				reserv_num_pair := make([]uint16, 2)
				reserv_num_pair[0] = index
				reserv_num_pair[1] = uint16(reserv_num)
				index_reservation_list[batch_replicated_count] = reserv_num_pair
				batch_replicated_count++

				//ns_ssl_proxy choke if the batch size is too big
				if batch_replicated_count > base.XmemMaxBatchSize {
					//send it
					err = xmem.sendWithRetry(xmem.client_for_setMeta, numOfRetry, reqs_bytes)

					if err != nil {
						xmem.Logger().Errorf("%v Failed to send. err=%v\n", xmem.Id(), err)
						for _, index_reserv_tuple := range index_reservation_list {
							xmem.buf.cancelReservation(index_reserv_tuple[0], int(index_reserv_tuple[1]))
						}
						return err
					}

					batch_replicated_count = 0
					for _, dpSlice := range reqs_bytes {
						xmem.dataPool.PutByteSlice(dpSlice)
					}
					reqs_bytes = [][]byte{}
					index_reservation_list = make([][]uint16, base.XmemMaxBatchSize+1)
				}
			case NotSendMerge:
				// Call conflictMgr to resolve conflict
				atomic.AddUint64(&xmem.counter_to_resolve, 1)
				lookupResp, err := batch.conflictLookupMap.deregisterLookup(item.UniqueKey)
				if err != nil {
					xmem.Logger().Warnf("For unique-key %v%s%v, error deregistering lookupResp for NotSendMerge, err=%v", base.UdTagBegin, item.UniqueKey, base.UdTagEnd, err)
				}
				if lookupResp == nil {
					xmem.Logger().Errorf("key=%q, batch.noRep=%v, sendLookup=%v, mergeLookup=%v\n",
						[]byte(item.UniqueKey), batch.noRepMap[item.UniqueKey], batch.sendLookupMap.responses[item.UniqueKey],
						lookupResp)
					panic(fmt.Sprintf("No response for key %v", item.UniqueKey))
				}
				err = xmem.conflictMgr.ResolveConflict(item, lookupResp, xmem.sourceActorId, xmem.targetActorId, xmem.uncompressBody, xmem.recycleDataObj)
				if err != nil {
					return err
				}
			case NotSendSetback:
				atomic.AddUint64(&xmem.counter_to_setback, 1)
				lookupResp, err := batch.conflictLookupMap.deregisterLookup(item.UniqueKey)
				if err != nil {
					xmem.Logger().Warnf("For unique-key %v%s%v, error deregistering lookupResp for NotSendMerge, err=%v", base.UdTagBegin, item.UniqueKey, base.UdTagEnd, err)
				}
				if lookupResp == nil {
					panic(fmt.Sprintf("No response for key %v", item.UniqueKey))
				}
				// This is the case where target has smaller CAS but it dominates source MV.
				err = xmem.conflictMgr.SetBackToSource(item, lookupResp, xmem.sourceActorId, xmem.targetActorId, xmem.uncompressBody, xmem.recycleDataObj)
				if err != nil {
					return err
				}
			case RetryTargetCasChanged:
				go func() {
					additionalInfo := SentCasChangedEventAdditional{
						IsGetDoc: true,
					}
					xmem.RaiseEvent(common.NewEvent(common.DataSentCasChanged, nil, xmem, []interface{}{item.GetTargetVB(), item.Seqno}, additionalInfo))
					xmem.retryAfterCasLockingFailure(item)
				}()
			case RetryTargetLocked:
				atomic.AddUint64(&xmem.counter_locked, 1)
				go xmem.retryAfterCasLockingFailure(item)
			case NotSendFailedCR:
				// lost on conflict resolution on source side
				// this still counts as data sent
				additionalInfo := DataFailedCRSourceEventAdditional{Seqno: item.Seqno,
					VbucketCommon:  VbucketCommon{VBucket: item.Req.VBucket},
					Opcode:         item.GetMemcachedCommand(),
					IsExpirySet:    (len(item.Req.Extras) >= 8 && binary.BigEndian.Uint32(item.Req.Extras[4:8]) != 0),
					ManifestId:     item.GetManifestId(),
					Cloned:         item.Cloned,
					CloneSyncCh:    item.ClonedSyncCh,
					ImportMutation: item.ImportMutation,
				}
				if item.OrigSrcVB != nil {
					additionalInfo.SetOrigSrcVB(*item.OrigSrcVB)
				}
				xmem.RaiseEvent(common.NewEvent(common.DataFailedCRSource, nil, xmem, nil, additionalInfo))
				if item.ImportMutation &&
					!item.HLVModeOptions.PreserveSync &&
					atomic.CompareAndSwapUint32(&xmem.importMutationEventRaised, 0, 1) {
					xmem.importMutationEventId = xmem.eventsProducer.AddEvent(
						base.LowPriorityMsg,
						base.ImportDetectedStr,
						base.EventsMap{},
						nil)
				}
				xmem.recycleDataObj(item)
			default:
				xmem.recycleDataObj(item)
			}
		}
	}

	//send the batch in one shot - in case the batch didn't hit the max limit
	if batch_replicated_count > 0 {
		err = xmem.sendWithRetry(xmem.client_for_setMeta, numOfRetry, reqs_bytes)

		if err != nil {
			xmem.Logger().Errorf("%v Failed to send. err=%v\n", xmem.Id(), err)
			for _, index_reserv_tuple := range index_reservation_list {
				xmem.buf.cancelReservation(index_reserv_tuple[0], int(index_reserv_tuple[1]))
			}
			return err
		}
	}
	return err
}

func (xmem *XmemNozzle) preprocessMCRequest(req *base.WrappedMCRequest, lookup *base.WrappedMCResponse) error {
	mc_req := req.Req

	if base.HasXattr(mc_req.DataType) && !xmem.xattrEnabled && mc_req.DataType&mcc.SnappyDataType == 0 {
		// if request contains xattr and xattr is not enabled in the memcached connection, strip xattr off the request

		// strip the xattr bit off data type
		mc_req.DataType = mc_req.DataType & ^(base.PROTOCOL_BINARY_DATATYPE_XATTR)
		// strip xattr off the mutation body
		if len(mc_req.Body) < 4 {
			xmem.Logger().Errorf("%v mutation body is too short to store xattr. key=%v%v%v, body=%v%v%v", xmem.Id(), base.UdTagBegin, string(mc_req.Key), base.UdTagEnd,
				base.UdTagBegin, mc_req.Body, base.UdTagEnd)
			return fmt.Errorf("%v mutation body is too short to store xattr", xmem.Id())
		}
		// the first four bytes in body stored the length of the xattr fields
		xattr_length := binary.BigEndian.Uint32(mc_req.Body[0:4])
		mc_req.Body = mc_req.Body[xattr_length+4:]
	}

	// Memcached does not like it when irrelevant flags are present; so unset them.
	if mc_req.DataType > 0 {
		flagsToTest := base.XattrDataType
		if xmem.compressionSetting == base.CompressionTypeSnappy {
			flagsToTest |= base.SnappyDataType
		}
		mc_req.DataType &= flagsToTest
	}

	// Send HLV and preserve _sync if necessary
	err := xmem.updateSystemXattrForTarget(req, lookup)
	if err != nil {
		var respBody []byte
		if lookup != nil && lookup.Resp != nil {
			respBody = lookup.Resp.Body
		}

		xmem.Logger().Errorf("%v error from updateSystemXattrForTarget, isSubdocOp=%v, key=%v%s%v, body=%v%v%v, respBody=%v%v%v, err=%v", xmem.Id(), req.IsSubdocOp(), base.UdTagBegin, string(req.Req.Key), base.UdTagEnd, base.UdTagBegin, req.Req.Body, base.UdTagEnd, base.UdTagBegin, respBody, base.UdTagEnd, err)
		return fmt.Errorf("%v error from updateSystemXattrForTarget, err=%v", xmem.Id(), err)
	}

	// Compress it if needed
	// if the request is a subdoc op, then the body would be composed of operational specs. KV doesn't support snappy compression on it.
	if !req.IsSubdocOp() && req.NeedToRecompress && mc_req.DataType&mcc.SnappyDataType == 0 {
		maxEncodedLen := snappy.MaxEncodedLen(len(mc_req.Body))
		bodyBytes, err := xmem.dataPool.GetByteSlice(uint64(maxEncodedLen))
		if err != nil {
			bodyBytes = make([]byte, maxEncodedLen)
			xmem.RaiseEvent(common.NewEvent(common.DataPoolGetFail, 1, xmem, nil, nil))
		}
		bodyBytes = snappy.Encode(bodyBytes, mc_req.Body)

		if len(mc_req.Body) < len(bodyBytes) {
			// not compressing MCRequest body as the snappy-compressed size is greater than the raw size
			req.SkippedRecompression = true
			if err == nil {
				xmem.dataPool.PutByteSlice(bodyBytes)
			}
			return nil
		}

		if err == nil {
			req.AddByteSliceForXmemToRecycle(bodyBytes)
		}
		mc_req.Body = bodyBytes
		mc_req.DataType = mc_req.DataType | mcc.SnappyDataType
		req.NeedToRecompress = false
	}
	return nil
}

func (xmem *XmemNozzle) getConflictDetector(source_cr_mode base.ConflictResolutionMode) (ret ConflictResolver) {
	switch source_cr_mode {
	case base.CRMode_Custom:
		xmem.Logger().Infof("Use DetectConflictWithHLV for conflict resolution.")
		ret = crMeta.ResolveConflictByHlv
	case base.CRMode_RevId:
		xmem.Logger().Infof("Use ResolveConflictByRevSeq for conflict resolution.")
		ret = crMeta.ResolveConflictByRevSeq
	case base.CRMode_LWW:
		xmem.Logger().Infof("Use ResolveConflictByCAS for conflict resolution.")
		ret = crMeta.ResolveConflictByCAS
	default:
		panic("Bad CR mode")
	}
	return ret
}

func (xmem *XmemNozzle) sendWithRetry(client *base.XmemClient, numOfRetry int, item_byte [][]byte) error {
	gcList := [][]byte{}

	var err error
	for j := 0; j < numOfRetry; j++ {
		err, rev := xmem.writeToClient(client, item_byte, true, gcList)
		if err == nil {
			return nil
		} else if err == base.BadConnectionError {
			xmem.repairConn(client, err.Error(), rev)
		}
	}

	for _, bytesToRecycle := range gcList {
		xmem.dataPool.PutByteSlice(bytesToRecycle)
	}
	return err
}

func (xmem *XmemNozzle) sendSetMeta_internal(batch *dataBatch) error {
	var err error
	if batch != nil {
		//batch send
		err = xmem.batchSetMetaWithRetry(batch, xmem.config.maxRetry)
		if err != nil && err != PartStoppedError {
			high_level_err := "Error writing documents to memcached in target cluster."
			xmem.Logger().Errorf("%v %v err=%v", xmem.Id(), high_level_err, err)
			xmem.handleGeneralError(errors.New(high_level_err))
		}
	}
	return err
}

// Launched as a part of the batchGetMeta and batchSubdocGet,
// which will fire off the requests and this is the handler to decrypt the info coming back
func (xmem *XmemNozzle) batchGetHandler(count int, finch chan bool, return_ch chan bool,
	opaque_keySeqno_map opaqueKeySeqnoMap, respMap map[string]*base.WrappedMCResponse, getSpecMap map[string][]base.SubdocLookupPathSpec, logger *log.CommonLogger) {
	defer func() {
		//handle the panic gracefully.
		if r := recover(); r != nil {
			if xmem.validateRunningState() == nil {
				errMsg := fmt.Sprintf("%v", r)
				//add to the error list
				xmem.Logger().Errorf("%v batchGet receiver recovered from err = %v", xmem.Id(), errMsg)

				//repair the connection
				xmem.repairConn(xmem.client_for_getMeta, errMsg, xmem.client_for_getMeta.RepairCount())
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
				return
			}

			response, err, rev := xmem.readFromClient(xmem.client_for_getMeta, true)
			if err != nil {
				if err == base.FatalError {
					// if possible, log info about the corresponding request to facilitate debugging
					if response != nil {
						keySeqno, ok := opaque_keySeqno_map[response.Opaque]
						if ok {
							key, ok1 := keySeqno[0].(string)
							seqno, ok2 := keySeqno[1].(uint64)
							vbno, ok3 := keySeqno[2].(uint16)
							if ok1 && ok2 && ok3 {
								xmem.Logger().Warnf("%v received fatal error from getMeta client. key=%v%s%v, seqno=%v, vb=%v, response=%v%v%v\n", xmem.Id(), base.UdTagBegin, key, base.UdTagEnd, seqno, vbno,
									base.UdTagBegin, response, base.UdTagEnd)
							}
						}
					}
				} else if err == base.BadConnectionError || err == base.ConnectionClosedError {
					xmem.repairConn(xmem.client_for_getMeta, err.Error(), rev)
				}

				if !isNetTimeoutError(err) && err != PartStoppedError {
					logger.Errorf("%v batchGet received fatal error and had to abort. Expected %v responses, got %v responses. err=%v", xmem.Id(), count, len(respMap), err)
					logger.Infof("%v Expected=%v, Received=%v%v%v\n", xmem.Id(), opaque_keySeqno_map.CloneAndRedact(), base.UdTagBegin, respMap, base.UdTagEnd)
				} else {
					logger.Errorf("%v batchGet timed out. Expected %v responses, got %v responses", xmem.Id(), count, len(respMap))
					logger.Infof("%v Expected=%v, Received=%v%v%v\n", xmem.Id(), opaque_keySeqno_map.CloneAndRedact(), base.UdTagBegin, respMap, base.UdTagEnd)
				}
				return
			} else {
				isGetMeta := response.Opcode == base.GET_WITH_META
				keySeqno, ok := opaque_keySeqno_map[response.Opaque]
				if ok {
					//success
					key, ok1 := keySeqno[0].(string)
					seqno, ok2 := keySeqno[1].(uint64)
					vbno, ok3 := keySeqno[2].(uint16)
					start_time, ok4 := keySeqno[3].(time.Time)
					manifestId, ok5 := keySeqno[4].(uint64)
					if ok1 && ok2 && ok3 && ok4 && ok5 {
						specs := getSpecMap[key]

						wrappedResp := &base.WrappedMCResponse{
							Specs: specs,
							Resp:  response,
						}

						// for the GetEx command SnappyEverywhere is enabled, the response body might be compressed
						// and the snappy datatype bit will be set.
						err := xmem.uncompressResponseBody(wrappedResp)
						if err != nil {
							xmem.Logger().Errorf("%v error uncompressing in getMeta client. key=%v%s%v, seqno=%v, response=%v%v%v, specs=%v%v%v, body=%v%v%v, datatype=%v", xmem.Id(),
								base.UdTagBegin, key, base.UdTagEnd, seqno,
								base.UdTagBegin, response, base.UdTagEnd,
								base.UdTagBegin, specs, base.UdTagEnd,
								base.UdTagBegin, wrappedResp.Resp.Body, base.UdTagEnd,
								wrappedResp.Resp.DataType,
							)
							return
						}

						respMap[key] = wrappedResp

						// GetMeta successful means that the target manifest ID is valid for the collection ID of this key
						additionalInfo := GetReceivedEventAdditional{
							Key:         key,
							Seqno:       seqno, // field is used only for CAPI nozzle
							Commit_time: time.Since(start_time),
							ManifestId:  manifestId,
						}
						var receivedEvent common.ComponentEventType
						if isGetMeta {
							receivedEvent = common.GetMetaReceived
						} else {
							// could be subdoc_lookup or getEx.
							receivedEvent = common.GetDocReceived
						}
						xmem.RaiseEvent(common.NewEvent(receivedEvent, nil, xmem, nil, additionalInfo))
						if response.Status != mc.SUCCESS && !base.IsIgnorableMCResponse(response, false) && !base.IsTemporaryMCError(response.Status) &&
							!base.IsCollectionMappingError(response.Status) {
							if base.IsTopologyChangeMCError(response.Status) {
								vb_err := fmt.Errorf("received error %v on vb %v", base.ErrorNotMyVbucket, vbno)
								xmem.handleVBError(vbno, vb_err)
							} else if base.IsEAccessError(response.Status) && !isGetMeta {
								// For getMeta, we will skip source side CR so this error is OK.
								// For subdoc_get, we will retry so increment backoff factor.
								xmem.client_for_getMeta.IncrementBackOffFactor()
								atomic.AddUint64(&xmem.counter_eaccess, 1)
								xmem.RaiseEvent(common.NewEvent(common.DataSentFailed, response.Status, xmem, nil, nil))
							} else if base.IsDocLocked(response) && !isGetMeta {
								// For getMeta, we will skip source side CR so this error is OK.
								// Plus the return code will be SUCCESS for getMeta if locked (but cas will be MaxCas)
								// For subdoc_get, we will retry so increment backoff factor.
								xmem.client_for_getMeta.IncrementBackOffFactor()
							} else {
								// log the corresponding request to facilitate debugging
								xmem.Logger().Warnf("%v received error from getMeta client. key=%v%s%v, seqno=%v, response=%v%v%v, specs=%v%v%v\n", xmem.Id(), base.UdTagBegin, key, base.UdTagEnd, seqno,
									base.UdTagBegin, response, base.UdTagEnd, base.UdTagBegin, specs, base.UdTagEnd)
								err = fmt.Errorf("error response with status %v from memcached", response.Status)
								xmem.repairConn(xmem.client_for_getMeta, err.Error(), rev)
								// no need to wait further since connection has been reset
								return
							}
						} else if base.IsTemporaryMCError(response.Status) && !isGetMeta {
							xmem.client_for_getMeta.IncrementBackOffFactor()
							atomic.AddUint64(&xmem.counter_tmperr, 1)
							xmem.RaiseEvent(common.NewEvent(common.DataSentFailed, response.Status, xmem, nil, nil))
						}
					} else {
						panic("KeySeqno list is not formated as expected [string, uint64, time]")
					}
				}
			}

			// *count < len(respMap) means write is still in session, can't return
			if len(respMap) >= count {
				logger.Debugf("%v Expected %v response, got all", xmem.Id(), count)
				return
			}
		}
	}
}

func (xmem *XmemNozzle) composeRequestForGetMeta(wrappedReq *base.WrappedMCRequest, opaque uint32) *base.WrappedMCRequest {
	newReq := xmem.mcRequestPool.Get()
	req := newReq.Req
	req.Body = req.Body[:0]
	req.VBucket = wrappedReq.Req.VBucket
	req.Key = wrappedReq.Req.Key
	req.Opaque = opaque
	req.Opcode = base.GET_WITH_META

	// if xattr is enabled, request that data type be included in getMeta response
	// Not needed for compression since GetMeta connection is to not use compression
	if xmem.xattrEnabled {
		if req.Extras == nil || len(req.Extras) == 0 {
			req.Extras = make([]byte, 1)
		} else {
			req.Extras = req.Extras[:1]
		}
		req.Extras[0] = byte(base.ReqExtMetaDataType)
	}
	return newReq
}

// GetEx is like Get, but returns the document body with the entire xattr section prefixed.
func (xmem *XmemNozzle) composeRequestForGetEx(wrappedReq *base.WrappedMCRequest, opaque uint32) *base.WrappedMCRequest {
	newReq := xmem.mcRequestPool.Get()
	req := newReq.Req
	req.Body = req.Body[:0]
	req.VBucket = wrappedReq.Req.VBucket
	req.Key = wrappedReq.Req.Key
	req.Opaque = opaque
	req.Opcode = mc.GETEX
	req.Extras = req.Extras[:0]
	req.Cas = 0

	return newReq
}

// For each document in the getMap, this routine will compose subdoc_get or getMeta or getEx, and send the requests.
// getEx respresents GetEx command needs to be run to get target document body along with the entire xattr section.
func (xmem *XmemNozzle) sendBatchGetRequest(getMap base.McRequestMap, retry int, getEx bool) (respMap map[string]*base.WrappedMCResponse, err error) {
	// if input size is 0, then we are done
	if len(getMap) == 0 {
		return nil, nil
	}

	respMap = make(map[string]*base.WrappedMCResponse, xmem.config.maxCount)

	opaque_keySeqno_map := make(opaqueKeySeqnoMap)
	receiver_fin_ch := make(chan bool, 1)
	receiver_return_ch := make(chan bool, 1)

	// A list (slice) of req_bytes
	reqs_bytes_list := [][][]byte{}

	// list to gc before this function returns
	gcList := [][]byte{}

	// Slice of requests that have been converted to serialized bytes
	reqs_bytes := [][]byte{}
	// Counts the number of requests that will fit into each req_bytes slice
	numOfReqsInReqBytesBatch := 0
	totalNumOfReqsForSubdocGet := 0
	totalNumOfReqsForGetMeta := 0
	totalNumOfReqsForGetEx := 0

	// Establish an opaque based on the current time - and since getting doc doesn't utilize buffer buckets, feed it 0
	opaque := base.GetOpaque(0, uint16(time.Now().UnixNano()))
	sent_key_map := make(map[string]bool, len(getMap))
	getSpecMap := make(map[string][]base.SubdocLookupPathSpec)

	var getWrappedReqToRecycle []*base.WrappedMCRequest

	//de-dupe and prepare the packages
	for _, originalReq := range getMap {
		docKey := string(originalReq.Req.Key)
		if docKey == "" {
			xmem.Logger().Errorf("%v received empty docKey. key= %v%q%v, req=%v%v%v, getMetaMap=%v%v%v", xmem.Id(),
				base.UdTagBegin, originalReq.Req.Key, base.UdTagEnd, base.UdTagBegin, originalReq.Req, base.UdTagEnd, base.UdTagBegin, getMap, base.UdTagEnd)
			return respMap, errors.New(xmem.Id() + " received empty docKey.")
		}

		if _, ok := sent_key_map[docKey]; !ok {
			getWrappedReq, getSpec := xmem.composeRequestForGet(originalReq, opaque, getEx)
			getWrappedReqToRecycle = append(getWrappedReqToRecycle, getWrappedReq)
			req := getWrappedReq.Req
			if getSpec != nil {
				getSpecMap[docKey] = getSpec
			}

			preAllocatedBytes, err := xmem.dataPool.GetByteSlice(uint64(req.Size()))
			if err != nil {
				// .Bytes() returns data ready to be fed over the wire
				reqs_bytes = append(reqs_bytes, req.Bytes())
				xmem.RaiseEvent(common.NewEvent(common.DataPoolGetFail, 1, xmem, nil, nil))
			} else {
				// BytesPreallocated will modify the preallocated byte slice
				req.BytesPreallocated(preAllocatedBytes)
				reqs_bytes = append(reqs_bytes, preAllocatedBytes)
				gcList = append(gcList, preAllocatedBytes)
			}

			// a Map of array of items and map key is the opaque currently based on time (passed to the target and back)
			opaque_keySeqno_map[opaque] = compileOpaqueKeySeqnoValue(docKey, originalReq.Seqno, originalReq.Req.VBucket, time.Now(), originalReq.GetManifestId())
			opaque++
			numOfReqsInReqBytesBatch++
			sent_key_map[docKey] = true

			if numOfReqsInReqBytesBatch > base.XmemMaxBatchSize {
				// Consider this a batch of requests and put it in as an element in reqs_bytes_list
				reqs_bytes_list = append(reqs_bytes_list, reqs_bytes)
				numOfReqsInReqBytesBatch = 0
				reqs_bytes = [][]byte{}
			}
			if req.Opcode == base.GET_WITH_META {
				totalNumOfReqsForGetMeta++
			} else if req.Opcode == mc.GETEX {
				totalNumOfReqsForGetEx++
			} else {
				totalNumOfReqsForSubdocGet++
			}
		}
	}

	// In case there are tail ends of the batch that did not fill base.XmemMaxBatchSize, append them to the end
	if numOfReqsInReqBytesBatch > 0 {
		reqs_bytes_list = append(reqs_bytes_list, reqs_bytes)
	}

	// launch the receiver - passing channel and maps in are fine since they are "reference types"
	go xmem.batchGetHandler(len(opaque_keySeqno_map), receiver_fin_ch, receiver_return_ch, opaque_keySeqno_map, respMap, getSpecMap, xmem.Logger())

	//send the requests
	for _, packet := range reqs_bytes_list {
		if totalNumOfReqsForSubdocGet == 0 && totalNumOfReqsForGetEx == 0 {
			// We are doing getMeta only. Failure is tolerated.
			err, _ = xmem.writeToClient(xmem.client_for_getMeta, packet, true, gcList)
		} else {
			// We have some subdoc lookup. The get must succeed. Do sendWithRetry
			err = xmem.sendWithRetry(xmem.client_for_getMeta, retry, packet)
		}
		if err != nil {
			// kill the receiver and return
			close(receiver_fin_ch)
			return
		}
	}
	if totalNumOfReqsForGetMeta > 0 {
		atomic.AddUint64(&xmem.counterNumGetMeta, uint64(totalNumOfReqsForGetMeta))
	}
	if totalNumOfReqsForSubdocGet > 0 {
		atomic.AddUint64(&xmem.counterNumSubdocGet, uint64(totalNumOfReqsForSubdocGet))
	}
	if totalNumOfReqsForGetEx > 0 {
		atomic.AddUint64(&xmem.counterNumGetEx, uint64(totalNumOfReqsForGetEx))
	}
	//wait for receiver to finish
	<-receiver_return_ch

	for _, bytesToRecycle := range gcList {
		xmem.dataPool.PutByteSlice(bytesToRecycle)
	}

	for _, getWrappedReq := range getWrappedReqToRecycle {
		if getWrappedReq == nil {
			// odd
			continue
		}
		xmem.mcRequestPool.Put(getWrappedReq)
	}

	return
}

func (xmem *XmemNozzle) SetConflictLogger(logger interface{}) error {
	if logger == nil {
		xmem.conflictLogger = nil
		return nil
	}

	cLogger, ok := logger.(baseclog.Logger)
	if !ok {
		return fmt.Errorf("invalid CLog type %T", cLogger)
	}
	xmem.conflictLogger = cLogger
	return nil
}

func (xmem *XmemNozzle) GetConflictLogger() interface{} {
	return xmem.conflictLogger
}

func (xmem *XmemNozzle) conflictLoggingEnabled() bool {
	return xmem.conflictLogger != nil
}

func parseHlvMouAndSyncXattrsFromBody(body []byte, datatype uint8, sendHlv, preserveSync bool) (hlv, mou, sync string, err error) {
	if base.HasXattr(datatype) {
		xattrIter, err1 := base.NewXattrIterator(body)
		if err1 != nil {
			err = fmt.Errorf("error getting new xattr iterator for doc body, err=%v", err1)
			return
		}

		for xattrIter.HasNext() {
			key, value, err1 := xattrIter.Next()
			if err1 != nil {
				err = fmt.Errorf("error getting next from iterator for doc body, err=%v", err1)
				return
			}
			if sendHlv && base.Equals(key, base.XATTR_HLV) {
				hlv = string(value)
			} else if preserveSync {
				if base.Equals(key, base.XATTR_MOBILE) {
					sync = string(value)
				} else if base.Equals(key, base.XATTR_MOU) {
					mou = string(value)
				}
			}
		}
	}

	return
}

// wrapper function to handle conflict (i.e. log conflict) between source (req) and target (resp) docs
func (xmem *XmemNozzle) handleConflict(req *base.WrappedMCRequest, resp *base.WrappedMCResponse) {
	err := xmem.logConflict(req, resp)
	if err == baseclog.ErrLoggerClosed {
		return
	}

	xmem.RaiseEvent(common.NewEvent(common.ConflictsDetected, nil, xmem, []interface{}{req.GetTargetVB(), req.Seqno}, err))
	if err == baseclog.ErrQueueFull || err == baseclog.ErrLoggerHibernated {
		xmem.Logger().Debugf("%v Conflict logger could not log for key=%v%s%v, err=%v",
			xmem.Id(),
			base.UdTagBegin, req.Req.Key, base.UdTagEnd,
			err,
		)
	} else if err != nil {
		// log and continue to replicate
		xmem.Logger().Errorf("%v Error when logging conflict for key=%v%s%v, req=%v%s%v, reqBody=%v%v%v, resp=%v, respBody=%v%v%v, err=%v",
			xmem.Id(),
			base.UdTagBegin, req.Req.Key, base.UdTagEnd,
			base.UdTagBegin, req.Req, base.UdTagEnd,
			base.UdTagBegin, req.Req.Body, base.UdTagEnd,
			resp.Resp.Opcode,
			base.UdTagBegin, resp.Resp.Body, base.UdTagEnd,
			err,
		)
	}
}

// logs the source (req) and target (resp) document (and their metadata) involved in the conflict to a conflict bucket.
func (xmem *XmemNozzle) logConflict(req *base.WrappedMCRequest, resp *base.WrappedMCResponse) error {
	var err error
	cLogOpts := req.ConflictLoggerOptions
	if cLogOpts == nil {
		// shouldn't happen ideally
		// because we have checked it before calling this function.
		return fmt.Errorf("cLogOpts is nil, but shouldn't be")
	}

	/* source document body and xattrs. */

	// make sure source doc is uncompressed
	if err := xmem.uncompressBody(req); err != nil {
		return fmt.Errorf("error decompressing source body, err=%v", err)
	}

	// parse the source metadata for dcp mutation
	sourceDoc := base.DecodeSetMetaReq(req)

	sourceHlvClone, sourceMouClone, sourceSyncClone, err := parseHlvMouAndSyncXattrsFromBody(req.Req.Body, req.Req.DataType, req.HLVModeOptions.SendHlv, req.HLVModeOptions.PreserveSync)
	if err != nil {
		return err
	}

	// source mutation from dcp already has all the xattrs.
	// just need to add conflict record xattr. The routine will create a clone of the body.
	sourceBodyClone, sourceDocDatatype, err := conflictlog.InsertConflictXattrToBody(req.Req.Body, req.Req.DataType)
	if err != nil {
		return fmt.Errorf("error inserting conflict xattr to source body, err=%v", err)
	}

	/* target document body and xattrs. */

	// get the cached metadata from first round of target lookups.
	targetCas := cLogOpts.TargetCas
	var targetExpiry, targetFlags uint32
	var targetIsDeleted bool
	var targetRevSeqno, targetVbUUID, targetSeqno uint64
	if cLogOpts.TargetInfo != nil {
		targetDocInfo := cLogOpts.TargetInfo
		targetExpiry = targetDocInfo.Expiry
		targetFlags = targetDocInfo.Flags
		targetIsDeleted = targetDocInfo.Deletion
		targetRevSeqno = targetDocInfo.RevSeq
		targetVbUUID = targetDocInfo.VbUUID
		targetSeqno = targetDocInfo.Seqno
	}

	var targetHlvClone, targetMouClone, targetSyncClone string
	if targetIsDeleted {
		// incase target is a tombstone, target GetEx response would not
		// have body or xattrs. Used the cached hlv, sync and mou for logging.
		targetHlvClone = cLogOpts.TargetHlv
		targetSyncClone = cLogOpts.TargetSync
		targetMouClone = cLogOpts.TargetMou
	} else {
		// the response body should be already uncompressed in getMeta handler.
		targetHlvClone, targetMouClone, targetSyncClone, err = parseHlvMouAndSyncXattrsFromBody(resp.Resp.Body, resp.Resp.DataType, req.HLVModeOptions.SendHlv, req.HLVModeOptions.PreserveSync)
		if err != nil {
			return err
		}
	}

	// the MCResponse body is from GetEx command,
	// so we just need to add conflict logging xattr to it. The routine
	// create a clone of the response body.
	targetBodyClone, targetDocDatatype, err := conflictlog.InsertConflictXattrToBody(resp.Resp.Body, resp.Resp.DataType)
	if err != nil {
		return fmt.Errorf("error inserting conflict xattr to target body, err=%v", err)
	}

	/* miscellaneous metadata. */

	req.SrcColNamespaceMtx.RLock()
	srcScopeName := req.SrcColNamespace.ScopeName
	srcCollectionName := req.SrcColNamespace.CollectionName
	req.SrcColNamespaceMtx.RUnlock()

	req.TgtColNamespaceMtx.RLock()
	tgtScopeName := req.TgtColNamespace.ScopeName
	tgtCollectionName := req.TgtColNamespace.CollectionName
	req.TgtColNamespaceMtx.RUnlock()

	_, startIdx, err := base.DecodeULEB128PrefixedKey(req.Req.Key)
	if err != nil {
		return fmt.Errorf("invalid prefixed key, key=%v%v%v, err=%v", base.UdTagBegin, req.Req.Key, base.UdTagEnd, err)
	}
	docKey := req.Req.Key[startIdx:]

	timestamp := time.Now()

	// Generate a conflict record from above information.
	conflictRecord := conflictlog.ConflictRecord{
		Timestamp: timestamp.Format(conflictlog.TimestampFormat),
		DocId:     string(docKey),
		Source: conflictlog.DocInfo{
			Scope:       srcScopeName,
			Collection:  srcCollectionName,
			BucketUUID:  xmem.sourceBucketUuid,
			ClusterUUID: xmem.sourceClusterUuid,
			NodeId:      xmem.config.sourceHostname,
			Expiry:      sourceDoc.Expiry,
			Flags:       sourceDoc.Flags,
			Cas:         sourceDoc.Cas,
			RevSeqno:    sourceDoc.RevSeq,
			IsDeleted:   sourceDoc.Deletion,
			Xattrs: conflictlog.Xattrs{
				Hlv:  sourceHlvClone,
				Sync: sourceSyncClone,
				Mou:  sourceMouClone,
			},
			Body:     sourceBodyClone,
			Datatype: sourceDocDatatype,
			VBNo:     req.Req.VBucket,
			VBUUID:   sourceDoc.VbUUID,
			Seqno:    sourceDoc.Seqno,
		},
		Target: conflictlog.DocInfo{
			Scope:       tgtScopeName,
			Collection:  tgtCollectionName,
			BucketUUID:  xmem.targetBucketUuid,
			ClusterUUID: xmem.targetClusterUuid,
			NodeId:      xmem.config.targetHostname,
			Expiry:      targetExpiry,
			Flags:       targetFlags,
			Cas:         targetCas,
			RevSeqno:    targetRevSeqno,
			IsDeleted:   targetIsDeleted,
			Xattrs: conflictlog.Xattrs{
				Hlv:  targetHlvClone,
				Sync: targetSyncClone,
				Mou:  targetMouClone,
			},
			Body:     targetBodyClone,
			Datatype: targetDocDatatype,
			VBNo:     req.Req.VBucket,
			VBUUID:   targetVbUUID,
			Seqno:    targetSeqno,
		},
		Datatype:            base.JSONDataType,
		StartTime:           timestamp,
		OriginatingPipeline: xmem.pipelineType,
	}

	conflictLogger := xmem.conflictLogger
	if conflictLogger != nil {
		loggerHandle, err := conflictLogger.Log(&conflictRecord)
		if err != nil {
			return err
		} else {
			cLogOpts.Handle = loggerHandle
		}
	}

	return nil
}

// For a given input of source and target, this function decides if these mutations are in conflict,
// if they can be be logged in a conflict bucket.
// 1. required settings are on i.e conflictLogging is on.
// 2. sourceDoc.Cas > max_cas of source vb (doc on source was updated after ECCV was turned on)
// 3. targetDoc.Cas > max_cas of target vb (doc on target was updated after ECCV was turned on)
func (xmem *XmemNozzle) canLogConflict(source *base.WrappedMCRequest, target *base.WrappedMCResponse) bool {
	cLogOpts := source.ConflictLoggerOptions
	if cLogOpts == nil || !cLogOpts.Enabled {
		// required settings are not enabled
		return false
	}

	sourceVbMaxCas, ok := xmem.config.vbHlvMaxCas[source.Req.VBucket]
	if !ok {
		// this path should not be hit
		return false
	}

	targetVbHlvMaxCas, ok := xmem.config.targetVbHlvMaxCas[source.Req.VBucket]
	if !ok {
		// this path should not be hit
		return false
	}

	// The document should be updated atleast once,
	// both on source and target after ECCV was turned on.
	return source.HLVModeOptions.ActualCas > sourceVbMaxCas && target.Resp.Cas > targetVbHlvMaxCas
}

// compareWithPrevLookup is used for processing of second round of lookups in batchGet.
// A given second lookup "currLookup" will be compared with the first lookup "prevLookup" to determine
// if the state of the target document has changed after the first lookup for a given xmem batch.
// If it has changed, currLookup will be added to noRepMap.
// If it has not changed, currLookup will be registered with the getLookupMap, which is the final result for the batch
// and also deletes the key from getMap to indicate that the lookup is done processing.
// The routine also adjusts respToGc incase the currLookup can be GC'ed.
func (xmem *XmemNozzle) compareWithPrevLookup(getMap base.McRequestMap, getLookupMap *responseLookup, noRepMap map[string]NeedSendStatus, prevLookup, currLookup *base.WrappedMCResponse,
	key, uniqueKey string, respToGc map[*mc.MCResponse]bool) bool {

	hasTmpErr := false
	casEarlier := prevLookup.Resp.Cas
	statusEarlier := prevLookup.Resp.Status
	casNow := currLookup.Resp.Cas
	statusNow := currLookup.Resp.Status
	if statusNow == mc.KEY_ENOENT {
		// could mean that the target doc is either a tombstone or does not exist.
		if statusEarlier == mc.KEY_ENOENT || base.IsDeletedSubdocLookupResponse(prevLookup.Resp) {
			err := getLookupMap.registerLookup(uniqueKey, currLookup)
			if err != nil {
				xmem.Logger().Warnf("For unique-key %v%s%v, error registering lookup for KEY_ENOENT response, err=%v",
					base.UdTagBegin, uniqueKey, base.UdTagEnd, err)
			}
			delete(getMap, uniqueKey)
		} else {
			// target doc created/recreated from previous lookup. Need to retry CR.
			if xmem.Logger().GetLogLevel() >= log.LogLevelDebug {
				xmem.Logger().Debugf("%v doc %v%q%v changed on target, opcode=%v, casNow=%v, casEarlier=%v, statusNow=%v, statusEarlier=%v. will need to retry",
					xmem.Id(), base.UdTagBegin, key, base.UdTagEnd, currLookup.Resp.Opcode, casNow, casEarlier, statusNow, statusEarlier)
			}
			noRepMap[uniqueKey] = RetryTargetCasChanged
			delete(getMap, uniqueKey)
			respToGc[currLookup.Resp] = true
		}
	} else if base.IsDocLocked(currLookup.Resp) {
		if xmem.Logger().GetLogLevel() >= log.LogLevelDebug {
			xmem.Logger().Debugf("%v doc %v%q%v is locked on the target for second lookup, opcode=%v, cas=%v, status=%v. will need to retry",
				xmem.Id(), base.UdTagBegin, key, base.UdTagEnd, currLookup.Resp.Opcode, currLookup.Resp.Cas, currLookup.Resp.Status)
		}
		noRepMap[uniqueKey] = RetryTargetLocked
		delete(getMap, uniqueKey)
		respToGc[currLookup.Resp] = true
	} else if base.IsSuccessGetResponse(currLookup.Resp) {
		if casNow == casEarlier {
			err := getLookupMap.registerLookup(uniqueKey, currLookup)
			if err != nil {
				xmem.Logger().Warnf("For unique-key %v%s%v, error registering lookup for compareWithPrevLookup, err=%v",
					base.UdTagBegin, uniqueKey, base.UdTagEnd, err)
			}
			delete(getMap, uniqueKey)
		} else {
			// cas changed on target from previous lookup. Need to retry CR.
			if xmem.Logger().GetLogLevel() >= log.LogLevelDebug {
				xmem.Logger().Debugf("%v doc %v%q%v changed on target, opcode=%v, casNow=%v, casEarlier=%v, status=%v. will need to retry",
					xmem.Id(), base.UdTagBegin, key, base.UdTagEnd, currLookup.Resp.Opcode, casNow, casEarlier, currLookup.Resp.Status)
			}
			noRepMap[uniqueKey] = RetryTargetCasChanged
			delete(getMap, uniqueKey)
			respToGc[currLookup.Resp] = true
		}
	} else if currLookup != nil {
		if base.IsTemporaryMCError(statusNow) {
			hasTmpErr = true
		}

		if currLookup.Resp != nil {
			xmem.Logger().Debugf("Received %v for key %v%q%v", xmem.PrintResponseStatusError(statusNow), base.UdTagBegin, key, base.UdTagEnd)
		} else {
			xmem.Logger().Debugf("Received nil MCResponse for key %v%q%v", base.UdTagBegin, key, base.UdTagEnd)
		}
		respToGc[currLookup.Resp] = true
	}

	return hasTmpErr
}

// If prevGetLookup doesn't exist, this routine will call either getMeta or subdoc_multi_lookup based on the specs set for the batch and the mutation,
// and return all the results in the getLookupMap.
// If prevGetLookup exists, this routine will call GetEx for the docs in getMap and compares the target lookups in prevGetLookup with the current set of lookups.
// If the target state has changed, the doc will be added to noRepMap. If it has not changed, it will be added to the getLookupMap.
// Input:
// - getMap: the documents we want to fetch from target. When this call returns, getMap will be empty.
// Output:
// - noRepMap: the documents we don't need to send to target or needs to be retried for several reasons.
// - getLookupMap: these are the target responses for docs in getMap. These are not in noRepMap. It could be:
// 1. first round of lookups: This is done to get only the target document metadata using get_meta or subdoc_lookup commands. No body is fetched. Conflict resolution (and detection) will be done in this round.
// 2. second round of lookups: This is done only if there were conflicts detected in (1) above. Target document body (along with all xattrs) is fetched using GetEx command. The responses in this round will be
// compared with the corresponding responses in (1) to ensure that nothing has changed between (1) and (2). If it has changed, it will be added to noRepMap and retried from step (1) again.
// - conflictMap: These documents have conflict. These are also in noRepMap with corresponding NeedSendStatus as appropriate. This is only valid for the first round of lookup.
// - conflictLookupMap: The response for the documents in conflictMap. This is also valid on the first round of lookup.
func (xmem *XmemNozzle) batchGet(getMap base.McRequestMap, prevGetLookup *responseLookup) (noRepMap map[string]NeedSendStatus,
	conflictMap base.McRequestMap, getLookupMap, conflictLookupMap *responseLookup, err error) {

	compareWithPrevLookup := prevGetLookup != nil
	noRepMap = make(map[string]NeedSendStatus)
	getLookupMap = NewResponseLookup()

	if !compareWithPrevLookup {
		// if compareWithPrevLookup is false, we are only getting target doc body.
		// No need to detect conflicts.
		conflictMap = make(base.McRequestMap)
		conflictLookupMap = NewResponseLookup()
	}

	var respMap map[string]*base.WrappedMCResponse
	var hasTmpErr bool

	// Some responses (eg. of those who are skipped to send) could have been refered by some other unique-keys (other mutations with the same doc key)
	// Store such responses here and take the call to recycle or not before the routine exits based on the refCnter
	respToGc := make(map[*mc.MCResponse]bool)
	defer func() {
		for resp := range respToGc {
			if getLookupMap.canRecycle(resp) &&
				conflictLookupMap.canRecycle(resp) {
				resp.Recycle()
			}
		}
	}()

	for i := 0; i < xmem.config.maxRetry || hasTmpErr; i++ {
		if hasTmpErr && (i+1)%(2*xmem.config.maxRetry) == 0 {
			xmem.Logger().Warnf("%s Received a lot of temporary errors for the batch for %v/%v times, secondLookup=%v", xmem.Id(), i, xmem.config.maxRetry, compareWithPrevLookup)
		}

		if len(getMap) == 0 {
			// Got all result
			return
		}
		err = xmem.validateRunningState()
		if err != nil {
			return nil, nil, nil, nil, err
		}
		hasTmpErr = false
		respMap, err = xmem.sendBatchGetRequest(getMap, xmem.config.maxRetry, compareWithPrevLookup)
		if err != nil {
			// Log the error. We will retry maxRetry times.
			xmem.Logger().Errorf("sentBatchGetRequest returned error '%v'. Retry number %v", err, i)
		}
		// Process the response the handler received
		for uniqueKey, wrappedReq := range getMap {
			key := string(wrappedReq.Req.Key)
			resp, ok := respMap[key]
			if !ok || resp == nil {
				if err == nil {
					xmem.Logger().Errorf("Received nil response for unique-key %v%s%v even with no errors", base.UdTagBegin, uniqueKey, base.UdTagEnd)
				}
				continue
			}

			// if response was compressed and it is now uncompressed,
			// make sure we mark the new slice allocated for xmem to be recycled at the end
			// of the req's processing.
			resp.RecyleExtraSlice(wrappedReq.AddByteSliceForXmemToRecycle)

			/*
				Possiblity #1: When compareWithPrevLookup is true, this is the second time we are performing lookup
				for this batch for getting target doc body and all xattrs for the detected conflicts. We will have to compare
				the results of this lookup with the previous round of lookup to ensure that the state of the target document
				hasn't changed between the two lookups.
			*/

			if compareWithPrevLookup {
				prevLookup, err := prevGetLookup.getLookup(uniqueKey)
				if err != nil {
					// this shouldn't happen.
					xmem.Logger().Warnf("For unique-key %v%s%v, error getting prev lookup response, err=%v", base.UdTagBegin, uniqueKey, base.UdTagEnd, err)
					continue
				}

				tempErr := xmem.compareWithPrevLookup(getMap, getLookupMap, noRepMap, prevLookup, resp, key, uniqueKey, respToGc)
				if tempErr {
					hasTmpErr = true
				}

				// done processing, so continue to the next response.
				continue
			}

			/*
				Possibilty #2: when compareWithPrevLookup is false, this is the first time we are performing lookup
				for this batch. Need to get target document metadata only to detect + resolve conflicts as necessary.
			*/

			if resp.Resp.Status == mc.KEY_ENOENT {
				err := getLookupMap.registerLookup(uniqueKey, resp)
				if err != nil {
					xmem.Logger().Warnf("For unique-key %v%s%v, error registering lookup for KEY_ENOENT response, err=%v", base.UdTagBegin, uniqueKey, base.UdTagEnd, err)
				}
				delete(getMap, uniqueKey)
			} else if base.IsDocLocked(resp.Resp) {
				if xmem.Logger().GetLogLevel() >= log.LogLevelDebug {
					xmem.Logger().Debugf("%v doc %v%q%v is locked on the target, opcode=%v, cas=%v, status=%v. will need to retry", xmem.Id(), base.UdTagBegin, key, base.UdTagEnd, resp.Resp.Opcode, resp.Resp.Cas, resp.Resp.Status)
				}
				noRepMap[uniqueKey] = RetryTargetLocked
				delete(getMap, uniqueKey)
				respToGc[resp.Resp] = true
			} else if base.IsSuccessGetResponse(resp.Resp) {
				conflictLoggingIsOn := xmem.canLogConflict(wrappedReq, resp)

				CDResult, CRResult, err := xmem.conflict_resolver(wrappedReq, resp.Resp, resp.Specs, xmem.sourceActorId, xmem.targetActorId, xmem.xattrEnabled, conflictLoggingIsOn, xmem.uncompressBody, xmem.Logger())
				if err != nil {
					// Log the error. We will retry
					xmem.Logger().Errorf("%v conflict_resolver: '%v'", xmem.Id(), err)
					continue
				}

				logConflict := conflictLoggingIsOn && CDResult.IsConflict()
				if logConflict {
					conflictMap[uniqueKey] = wrappedReq
					err := conflictLookupMap.registerLookup(uniqueKey, resp)
					if err != nil {
						// warn and continue to replicate
						xmem.Logger().Warnf("For unique-key %v%s%v, error registering lookup for conflictLookupMap response, err=%v", base.UdTagBegin, uniqueKey, base.UdTagEnd, err)
					}
				}

				switch CRResult {
				case crMeta.CRSendToTarget:
					// Import mutations sent will be counted when we send since we will get a more accurate count then.
					// If target document does not exist, we only parse for importCas (_mou.cas) at send time.
					err := getLookupMap.registerLookup(uniqueKey, resp)
					if err != nil {
						xmem.Logger().Warnf("For unique-key %v%s%v, error registering lookup for SendToTarget response, err=%v", base.UdTagBegin, uniqueKey, base.UdTagEnd, err)
					}
				case crMeta.CRSkip:
					noRepMap[uniqueKey] = NotSendFailedCR
					if !logConflict {
						// can only recycle if the response is not used for conflict logging
						respToGc[resp.Resp] = true
					}
				case crMeta.CRMerge:
					noRepMap[uniqueKey] = NotSendMerge
					conflictMap[uniqueKey] = wrappedReq
					err := conflictLookupMap.registerLookup(uniqueKey, resp)
					if err != nil {
						xmem.Logger().Warnf("For unique-key %v%s%v, error registering lookup for Merge response, err=%v", base.UdTagBegin, uniqueKey, base.UdTagEnd, err)
					}
					// TODO: recycle response after merge
				case crMeta.CRSetBackToSource:
					noRepMap[uniqueKey] = NotSendSetback
					conflictMap[uniqueKey] = wrappedReq
					err := conflictLookupMap.registerLookup(uniqueKey, resp)
					if err != nil {
						xmem.Logger().Warnf("For unique-key %v%s%v, error registering lookup for SetBackToSource response, err=%v", base.UdTagBegin, uniqueKey, base.UdTagEnd, err)
					}
					// TODO: recycle response after merge
				default:
					panic(fmt.Sprintf("Unexpcted conflict result %v", CRResult))
				}
				delete(getMap, uniqueKey)
			} else if opcode, _ := xmem.opcodeAndSpecsForGetOp(wrappedReq); opcode == base.GET_WITH_META {
				// For getMeta, we can just send optimistically
				err := getLookupMap.registerLookup(uniqueKey, resp)
				if err != nil {
					xmem.Logger().Warnf("For unique-key %v%s%v, error registering lookup for GET_WITH_META response, err=%v", base.UdTagBegin, uniqueKey, base.UdTagEnd, err)
				}
				delete(getMap, uniqueKey)
			} else if resp != nil {
				if base.IsTemporaryMCError(resp.Resp.Status) {
					hasTmpErr = true
				}

				if resp.Resp != nil {
					xmem.Logger().Debugf("Received %v for key %v%q%v", xmem.PrintResponseStatusError(resp.Resp.Status), base.UdTagBegin, key, base.UdTagEnd)
				} else {
					xmem.Logger().Debugf("Received nil MCResponse for key %v%q%v", base.UdTagBegin, key, base.UdTagEnd)
				}
				respToGc[resp.Resp] = true
			}
		}
	}
	if len(getMap) > 0 {
		err = fmt.Errorf("failed to get XATTR from target for %v documents after %v retries (secondLookup=%v)", len(getMap), xmem.config.maxRetry, compareWithPrevLookup)
	}
	return
}

func (xmem *XmemNozzle) opcodeAndSpecsForGetOp(wrappedReq *base.WrappedMCRequest) (mc.CommandCode, base.SubdocLookupPathSpecs) {
	incomingReq := wrappedReq.Req
	getSpecWithHlv := wrappedReq.GetMetaSpecWithHlv
	getSpecWithoutHlv := wrappedReq.GetMetaSpecWithoutHlv
	getBodySpec := wrappedReq.GetBodySpec
	var getSpecs base.SubdocLookupPathSpecs
	if xmem.isCCR() {
		// CCR mode requires fetching the document metadata and body for the purpose of conflict resolution
		// Since they are considered true conflicts
		getSpecs = getBodySpec
	} else if xmem.getCrossClusterVers() && wrappedReq.HLVModeOptions.ActualCas >= xmem.config.vbHlvMaxCas[incomingReq.VBucket] {
		// Note that there is no mixed mode support for import mutations. If enableCrossClusterVersioning is false,
		// and current source mutation already has HLV, we still don't get target importCas (_mou.cas) / HLV. The reason is to
		// figure out that current source mutation already has HLV will require us to parse the body. It has a
		// performance impact. Mobile does not expect to support import on target in mixed mode. This is because for
		// cas < max_cas or in mixed mode only active-passive XDCR-SGW setup is originally supported. Doing import
		// on target during mixed mode may cause data loss because target is the passive site and import on passive
		// site is not supported during mixed mode. See design spec for more details.
		getSpecs = getSpecWithHlv
		wrappedReq.HLVModeOptions.IncludeTgtHlv = true
	} else {
		getSpecs = getSpecWithoutHlv
	}
	if getSpecs == nil {
		return base.GET_WITH_META, nil
	}
	return mc.SUBDOC_MULTI_LOOKUP, getSpecs
}

// if getEx is true, it takes priority and the routine composes a getEx request.
func (xmem *XmemNozzle) composeRequestForGet(wrappedReq *base.WrappedMCRequest, opaque uint32, getEx bool) (*base.WrappedMCRequest, []base.SubdocLookupPathSpec) {
	if getEx {
		return xmem.composeRequestForGetEx(wrappedReq, opaque), nil
	}

	opcode, specs := xmem.opcodeAndSpecsForGetOp(wrappedReq)
	switch opcode {
	case base.GET_WITH_META:
		return xmem.composeRequestForGetMeta(wrappedReq, opaque), nil
	case mc.SUBDOC_MULTI_LOOKUP:
		return xmem.composeRequestForSubdocGet(specs, wrappedReq, opaque, 0), specs
	default:
		panic(fmt.Sprintf("Unknown opcode %v. Need to implement", opcode))
	}
}

// Request to get _xdcr or _vv XATTR. If document body is included, it must be the last path
func (xmem *XmemNozzle) composeRequestForSubdocGet(specs base.SubdocLookupPathSpecs, wrappedReq *base.WrappedMCRequest, opaque uint32, cas uint64) *base.WrappedMCRequest {
	newReq := xmem.mcRequestPool.Get()
	req := newReq.Req

	totalBodySize := specs.Size()
	dpSlice, dpErr := xmem.dataPool.GetByteSlice(uint64(totalBodySize))
	if dpErr != nil {
		req.Body = make([]byte, totalBodySize)
		xmem.RaiseEvent(common.NewEvent(common.DataPoolGetFail, 1, xmem, nil, nil))
	} else {
		// do not put dpSlice back in the bytes pool because newReq object will hold on to it, even when newReq is put back to MCRequest pool.
		req.Body = dpSlice
	}

	body := req.Body
	pos := 0
	for i := 0; i < len(specs); i++ {
		body[pos] = uint8(specs[i].Opcode)
		pos++
		body[pos] = specs[i].Flags
		pos++
		binary.BigEndian.PutUint16(body[pos:pos+2], uint16(len(specs[i].Path)))
		pos = pos + 2
		if len(specs[i].Path) > 0 {
			n := copy(body[pos:], specs[i].Path)
			pos = pos + n
		}
	}

	req.Cas = cas
	req.VBucket = wrappedReq.Req.VBucket
	req.Key = wrappedReq.Req.Key
	req.Opaque = opaque
	req.Extras = []byte{mc.SUBDOC_FLAG_ACCESS_DELETED}
	req.Body = body[:pos]
	req.Opcode = mc.SUBDOC_MULTI_LOOKUP

	return newReq
}

func (xmem *XmemNozzle) uncompressBody(req *base.WrappedMCRequest) error {
	if req.Req.DataType&mcc.SnappyDataType > 0 {
		decodedLen, err := snappy.DecodedLen(req.Req.Body)
		if err != nil {
			return err
		}
		body, err := xmem.dataPool.GetByteSlice(uint64(decodedLen))
		if err != nil {
			body = make([]byte, decodedLen)
			xmem.RaiseEvent(common.NewEvent(common.DataPoolGetFail, 1, xmem, nil, nil))
		} else {
			req.AddByteSliceForXmemToRecycle(body)
		}
		body, err = snappy.Decode(body, req.Req.Body)
		if err != nil {
			return err
		}
		req.Req.Body = body
		req.Req.DataType = req.Req.DataType &^ mcc.SnappyDataType
		req.NeedToRecompress = true
	}
	return nil
}

// The function will set resp.ExtraSlice and expects the caller to handle as appropriate.
func (xmem *XmemNozzle) uncompressResponseBody(resp *base.WrappedMCResponse) error {
	if resp.Resp.DataType&mcc.SnappyDataType == 0 || len(resp.Resp.Body) == 0 {
		return nil
	}

	decodedLen, err := snappy.DecodedLen(resp.Resp.Body)
	if err != nil {
		return err
	}
	// the response body is originally gotten by gomemcached using the same instance of xmem.dataPool.
	// so it should be safe to get a slice directly from the pool and then call Recycle() on the response.
	body, err := xmem.dataPool.GetByteSlice(uint64(decodedLen))
	if err != nil {
		body = make([]byte, decodedLen)
		// mark it as cannot recycle since we did not get it from datapool
		xmem.RaiseEvent(common.NewEvent(common.DataPoolGetFail, 1, xmem, nil, nil))
	} else {
		// set it so that the caller can handle the GC of this slice as needed.
		resp.ExtraSlice = body
	}

	body, err = snappy.Decode(body, resp.Resp.Body)
	if err != nil {
		return err
	}

	// recyle the original response body before reassigning.
	resp.Resp.Recycle()

	resp.Resp.Body = body
	resp.Resp.DataType = resp.Resp.DataType &^ mcc.SnappyDataType
	atomic.AddUint64(&xmem.counterNumGetCompressed, 1)

	return nil
}

// Preserve all Xattributes except source _sync and old HLV if it has been updated
// Returns the error if any and a boolean which indicates if we are replicating the source _mou.
// The caller may have to manually delete the target _mou if the return value is false.
// If a non-nil "lookupMap" is passed by the caller, it will be populated with the source xattrs
// written by "updater" (except _vv, _mou and _sync as they are well recognised by XDCR)
func (xmem *XmemNozzle) preserveSourceXattrs(wrappedReq *base.WrappedMCRequest, sourceDocMeta *crMeta.CRMetadata,
	updatedHLV bool, updater func(key []byte, val []byte) error, lookupMap map[string]bool) (bool, error) {

	mouIsReplicated := false
	if !base.HasXattr(wrappedReq.Req.DataType) {
		// no xattrs to process
		return mouIsReplicated, nil
	}

	req := wrappedReq.Req
	body := req.Body

	it, err := base.NewXattrIterator(body)
	if err != nil {
		return mouIsReplicated, err
	}
	for it.HasNext() {
		key, value, err := it.Next()
		if err != nil {
			return mouIsReplicated, err
		}

		xattrKey := string(key)
		switch xattrKey {
		case base.XATTR_HLV:
			if updatedHLV {
				// We have updated the hlv so skip the old HLV
				continue
			}
		case base.XATTR_MOBILE:
			if wrappedReq.HLVModeOptions.PreserveSync {
				// Skip the source _sync value.
				// For subdoc op, no need to create a spec to upsert target _sync, as it is already there on target
				xmem.RaiseEvent(common.NewEvent(common.SourceSyncXattrRemoved, nil, xmem, nil, nil))
				continue
			}
		case base.XATTR_MOU:
			mouIsReplicated = true
			if sourceDocMeta != nil && !sourceDocMeta.IsImportMutation() {
				// Remove cas (importCAS) from _mou if it is no longer an import mutation.
				// This happens when there are non-imported updates on top of an import mutation

				if wrappedReq.MouAfterProcessing == nil {
					// _mou had only cas (importCAS) and pRev, no need to write
					mouIsReplicated = false
					continue
				}
				value = wrappedReq.MouAfterProcessing
			}
		default:
			if lookupMap != nil {
				lookupMap[xattrKey] = true
			}
		}

		err = updater(key, value)
		if err != nil {
			return mouIsReplicated, err
		}
	}
	return mouIsReplicated, nil
}

// updates the request body (with xattrs), CAS, extras (flags), datatype and opcode to appropriate values for the request in the mobile/CCR mode.
// opaque is set in adjustRequest down the line.
func (xmem *XmemNozzle) updateSystemXattrForTarget(wrappedReq *base.WrappedMCRequest, lookup *base.WrappedMCResponse) (err error) {
	var needToUpdateSysXattrs bool
	if wrappedReq.Req.DataType&mcc.XattrDataType != 0 {
		// in this case, we may need to update HLV if source already has it, even if mobile/HLV are all off.
		needToUpdateSysXattrs = true
	} else if wrappedReq.HLVModeOptions.PreserveSync {
		needToUpdateSysXattrs = true
	} else if xmem.isCCR() {
		needToUpdateSysXattrs = true
	} else if wrappedReq.HLVModeOptions.SendHlv {
		maxCas := xmem.config.vbHlvMaxCas[wrappedReq.Req.VBucket]
		if wrappedReq.HLVModeOptions.ActualCas >= maxCas {
			needToUpdateSysXattrs = true
		}
	}

	if !needToUpdateSysXattrs {
		return
	}

	if err = xmem.uncompressBody(wrappedReq); err != nil {
		return
	}

	var vbHlvMaxCas uint64
	if wrappedReq.HLVModeOptions.SendHlv && xmem.config.vbHlvMaxCas != nil {
		vbHlvMaxCas = xmem.config.vbHlvMaxCas[wrappedReq.Req.VBucket]
	}

	sourceDoc := crMeta.NewSourceDocument(wrappedReq, xmem.sourceActorId)
	sourceDocMeta, err := sourceDoc.GetMetadata(xmem.uncompressBody)
	if err != nil {
		return err
	}

	updateHLV := crMeta.NeedToUpdateHlv(sourceDocMeta, vbHlvMaxCas, time.Duration(atomic.LoadUint32(&xmem.config.hlvPruningWindowSec))*time.Second)

	if wrappedReq.IsSubdocOp() {
		// for a subdoc op, we should always update HLV,
		// given that we need to perform a cas macro expansion.
		// Otherwise the target cas will rollback.
		return xmem.updateSystemXattrForSubdocOp(wrappedReq, lookup, sourceDocMeta, true)
	} else {
		return xmem.updateSystemXattrForMetaOp(wrappedReq, lookup, sourceDocMeta, updateHLV)
	}
}

// will update the system xattr to replicate when using the *_WITH_META commands.
func (xmem *XmemNozzle) updateSystemXattrForMetaOp(wrappedReq *base.WrappedMCRequest, lookup *base.WrappedMCResponse, sourceDocMeta *crMeta.CRMetadata, updateHLV bool) (err error) {
	// Now we need to update HLV xattr either because of new changes or because we have to prune
	// The max increase in body length is adding 2 uint32 and _vv\x00{"cvCas":"0x...","src":"<clusterId>","ver":"0x..."}\x00

	// Body with xattrs will look like the following:
	// | xattrTotalLen | xattr1Len | xattr1 | xattr2Len | xattr2Len | xattr2 | ... | xattrNLen | xattrN | docBody |
	// where,
	// 	xattrTotalLen									- 1 uint32, Total xattr section length from first byte of "xattr1Len" to last byte of "xattrNLen"
	// 	xattr1Len, xattr2Len, ... xattrNLen				- 1 unit32 each, Length of xattr1, xattr2, ... xattrN respectively
	// 	xattr1, xattr2, ... xattrN will each look like	- | xattrKey | 0x00 | xattrVal | 0x00 |
	//	docBody											- actual document body without any xattrs

	defer func() {
		// set opcode, CAS and extras (flags) for NoTargetCR SetWMeta mutations.
		wrappedReq.SetCasAndFlagsForMetaOp(lookup)
		wrappedReq.Req.Opcode = wrappedReq.GetMemcachedCommand()
	}()

	needToModifyBody := updateHLV || wrappedReq.HLVModeOptions.PreserveSync
	if !needToModifyBody {
		// there is no need to modify document body before replicating.
		return nil
	}

	maxBodyIncrease := 0
	if updateHLV {
		maxBodyIncrease = maxBodyIncrease + 8 /* 2 uint32 - xattrTotalLen and _vv's length */ +
			len(base.XATTR_HLV) + 2 /* _vv\x00{ */ +
			len(crMeta.HLV_CVCAS_FIELD) + 3 /* "cvCas": */ + 21 /* "0x<16bytes>", */ +
			len(crMeta.HLV_SRC_FIELD) + 3 /* "src": */ +
			len(xmem.sourceActorId) + 3 /* "<bucketId>", */ +
			len(crMeta.HLV_VER_FIELD) + 3 /* "ver": */ +
			20 /* "0x<16byte>" */ + 2 /* }\x00 */
	}

	var targetSyncVal []byte
	if wrappedReq.HLVModeOptions.PreserveSync && lookup != nil {
		targetSyncVal, _ = lookup.ResponseForAPath(base.XATTR_MOBILE)
		if len(targetSyncVal) > 0 {
			maxBodyIncrease = maxBodyIncrease + 8 + /* 2 uint32 - xattrTotalLen and _sync's length */
				len(base.XATTR_MOBILE) + 1 /* _sync\x00 */ +
				len(targetSyncVal) + 1 /* <targetSyncVal>\x00 */
		}
	}

	req := wrappedReq.Req
	body := req.Body
	newbodyLen := len(body) + maxBodyIncrease
	newbody, err := xmem.dataPool.GetByteSlice(uint64(newbodyLen))
	if err != nil {
		newbody = make([]byte, newbodyLen)
		xmem.RaiseEvent(common.NewEvent(common.DataPoolGetFail, 1, xmem, nil, nil))
	} else {
		wrappedReq.AddByteSliceForXmemToRecycle(newbody)
	}

	xattrComposer := base.NewXattrComposer(newbody)

	if updateHLV {
		_, pruned, err := crMeta.ConstructXattrFromHlvForSetMeta(sourceDocMeta, time.Duration(atomic.LoadUint32(&xmem.config.hlvPruningWindowSec))*time.Second, xattrComposer)
		if err != nil {
			err = fmt.Errorf("error decoding source mutation for key=%v%s%v, req=%v%v%v, reqBody=%v%v%v, sourceMeta=%s in updateSystemXattrForTarget, err=%v",
				base.UdTagBegin, req.Key, base.UdTagEnd,
				base.UdTagBegin, req, base.UdTagEnd,
				base.UdTagBegin, req.Body, base.UdTagEnd,
				sourceDocMeta, err)
			return err
		}

		xmem.RaiseEvent(common.NewEvent(common.HlvUpdated, nil, xmem, nil, nil))
		if pruned {
			xmem.RaiseEvent(common.NewEvent(common.HlvPruned, nil, xmem, nil, nil))
		}
	}

	if wrappedReq.HLVModeOptions.PreserveSync && len(targetSyncVal) > 0 {
		err = xattrComposer.WriteKV([]byte(base.XATTR_MOBILE), targetSyncVal)
		if err != nil {
			return err
		}
		xmem.RaiseEvent(common.NewEvent(common.TargetSyncXattrPreserved, nil, xmem, nil, nil))
	}

	// decide to preserve or not preserve source xattrs before replicating to target
	_, err = xmem.preserveSourceXattrs(wrappedReq, sourceDocMeta, updateHLV, xattrComposer.WriteKV, nil)
	if err != nil {
		return err
	}

	docWithoutXattr := base.FindSourceBodyWithoutXattr(req)
	out, atLeastOneXattr := xattrComposer.FinishAndAppendDocValue(docWithoutXattr, req, lookup)
	req.Body = out

	if atLeastOneXattr {
		req.DataType = req.DataType | base.PROTOCOL_BINARY_DATATYPE_XATTR
	} else {
		req.DataType = req.DataType & ^(base.PROTOCOL_BINARY_DATATYPE_XATTR)
	}

	return nil
}

// will update the system xattr to replicate when using the subdoc SET or subdoc DELETE commands.
func (xmem *XmemNozzle) updateSystemXattrForSubdocOp(wrappedReq *base.WrappedMCRequest, lookup *base.WrappedMCResponse, sourceDocMeta *crMeta.CRMetadata, updateHLV bool) (err error) {

	wrappedReq.SetSubdocOptionsForRetry(wrappedReq.Req.Body, wrappedReq.Req.Extras, wrappedReq.Req.DataType)

	req := wrappedReq.Req
	var spec base.SubdocMutationPathSpec
	specs := base.NewSubdocMutationPathSpecs()
	// lookup for all the source document xattrs,
	// expect for _vv, _mou and _sync, as they are well recognised by XDCR.
	sourceXattrsMap := make(map[string]bool)

	if updateHLV {
		maxHlvLen := 2 /* {...} */ +
			len(crMeta.HLV_CVCAS_FIELD) + 3 /* "cvCas": */ + 21 /* "0x<16bytes>", */ +
			len(crMeta.HLV_SRC_FIELD) + 3 /* "src": */ + len(xmem.sourceActorId) + 3 /* "<bucketId>", */ +
			len(crMeta.HLV_VER_FIELD) + 3 /* "ver": */ + 21 /* "0x<16byte>", */

		pv := sourceDocMeta.GetHLV().GetPV()
		if len(pv) > 0 {
			maxHlvLen += len(crMeta.HLV_PV_FIELD) + 3 /* "pv": */ + hlv.BytesRequired(pv) + 1 /* , */
		}

		mv := sourceDocMeta.GetHLV().GetMV()
		if len(mv) > 0 {
			maxHlvLen += len(crMeta.HLV_MV_FIELD) + 3 /* "mv": */ + hlv.BytesRequired(mv)
		}

		newHlv, err := xmem.dataPool.GetByteSlice(uint64(maxHlvLen))
		if err != nil {
			newHlv = make([]byte, maxHlvLen)
			xmem.RaiseEvent(common.NewEvent(common.DataPoolGetFail, 1, xmem, nil, nil))
		} else {
			wrappedReq.AddByteSliceForXmemToRecycle(newHlv)
		}

		pos, pruned, err := crMeta.ConstructHlv(newHlv, 0, sourceDocMeta, time.Duration(atomic.LoadUint32(&xmem.config.hlvPruningWindowSec))*time.Second)
		if err != nil {
			err = fmt.Errorf("error decoding source mutation for key=%v%s%v, req=%v%v%v, reqBody=%v%v%v in updateSystemXattrForSubdocOp, err=%v", base.UdTagBegin, req.Key, base.UdTagEnd, base.UdTagBegin, req, base.UdTagEnd, base.UdTagBegin, req.Body, base.UdTagEnd, err)
			return err
		}

		// spec to update HLV or _vv xattr
		spec = base.NewSubdocMutationPathSpec(uint8(base.SUBDOC_DICT_UPSERT), uint8(base.SUBDOC_FLAG_MKDIR_P|base.SUBDOC_FLAG_XATTR), []byte(base.XATTR_HLV), newHlv[:pos])
		specs = append(specs, spec)

		// set cvCas as regenerated Cas from mc.SET/DELETE using macro expansion.
		// MKDIR_P is not set, because we know for sure there exists cvCas on target since we used it for conflict resolution,
		// otherwise there is something wrong
		spec = base.NewSubdocMutationPathSpec(uint8(base.SUBDOC_DICT_UPSERT), uint8(base.SUBDOC_FLAG_XATTR|base.SUBDOC_FLAG_EXPAND_MACROS), []byte(crMeta.XATTR_CVCAS_PATH), []byte(base.CAS_MACRO_EXPANSION))
		specs = append(specs, spec)

		xmem.RaiseEvent(common.NewEvent(common.HlvUpdated, nil, xmem, nil, nil))
		if pruned {
			xmem.RaiseEvent(common.NewEvent(common.HlvPruned, nil, xmem, nil, nil))
		}
	}

	// decide to preserve or not preserve source xattrs before replicating to target
	mouReplicated, err := xmem.preserveSourceXattrs(wrappedReq, sourceDocMeta, updateHLV, specs.WriteKV, sourceXattrsMap)
	if err != nil {
		return err
	}

	// if we are not replicating source _mou or if source doesn't have _mou,
	// we have to individually add a spec to remove target _mou, if it has it.
	if !mouReplicated && wrappedReq.SubdocCmdOptions.TargetHasMou {
		spec = base.NewSubdocMutationPathSpec(uint8(base.SUBDOC_DELETE), uint8(base.SUBDOC_FLAG_XATTR), []byte(base.XATTR_MOU), nil)
		specs = append(specs, spec)
	}

	// delete all the other xattrs - user xattrs and system xattrs that XDCR doesn't recognise
	// (other than _vv, _sync and _mou), that are present on target document, but not on the source document.
	xtoc, err := lookup.ResponseForAPath(base.XattributeToc)
	if err != nil {
		return fmt.Errorf("%v: XTOC was not fetched, err=%v, eccv=%v, mobile=%v",
			xmem.Id(), err, xmem.getCrossClusterVers(), xmem.getMobileCompatible())
	}

	targetXattrs, err := base.NewXTOCIterator(xtoc)
	if err != nil {
		return err
	}

	for targetXattrs.HasNext() {
		targetXattr, err := targetXattrs.Next()
		if err != nil {
			return err
		}

		if len(targetXattr) == 0 ||
			base.Equals(targetXattr, base.XATTR_HLV) ||
			base.Equals(targetXattr, base.XATTR_MOBILE) ||
			base.Equals(targetXattr, base.XATTR_MOU) {
			// XDCR can recognise these system xattrs
			// and should be updated above. These should
			// not be deleted.
			continue
		}

		_, existsOnSourceDoc := sourceXattrsMap[string(targetXattr)]
		if !existsOnSourceDoc {
			spec = base.NewSubdocMutationPathSpec(uint8(base.SUBDOC_DELETE), uint8(base.SUBDOC_FLAG_XATTR), targetXattr, nil)
			specs = append(specs, spec)
		}
	}

	var accessDeleted bool
	// body path
	switch wrappedReq.SubdocCmdOptions.SubdocOp {
	case base.SubdocDelete:
		// CMD_DELETE - subdoc delete - no doc body
		spec = base.NewSubdocMutationPathSpec(uint8(mc.DELETE), uint8(0), []byte(""), nil)
		accessDeleted = true
	case base.SubdocSet:
		// CMD_SET - subdoc set
		body := base.FindSourceBodyWithoutXattr(wrappedReq.Req)
		spec = base.NewSubdocMutationPathSpec(uint8(mc.SET), uint8(0), []byte(""), body)
	default:
		panic(fmt.Sprintf("unknown subdoc op %v", wrappedReq.SubdocCmdOptions.SubdocOp))
	}
	specs = append(specs, spec)

	bodylen := specs.Size()
	newbody, err := xmem.dataPool.GetByteSlice(uint64(bodylen))
	if err != nil {
		newbody = make([]byte, bodylen)
		xmem.RaiseEvent(common.NewEvent(common.DataPoolGetFail, 1, xmem, nil, nil))
	} else {
		wrappedReq.AddByteSliceForXmemToRecycle(newbody)
	}

	// Compose the subdoc request and also set the appropriate CAS.
	base.ComposeRequestForSubdocMutation(specs, req, lookup.Resp.Cas, newbody, accessDeleted, wrappedReq.SubdocCmdOptions.TargetDocIsTombstone, true)

	return nil
}

// If the change is from targetCluster, we don't need to send it back.
// In fact, it will eventually lose CR. So we can short-circuit it's lifetime to
// avoid unnecessary processing down the line.
func (xmem *XmemNozzle) isChangesFromTarget(req *base.WrappedMCRequest) (bool, error) {
	if !xmem.usesHlv(req) {
		return false, nil
	}

	if req.RetryCRCount > 0 {
		return false, nil
	}

	doc := crMeta.NewSourceDocument(req, xmem.sourceActorId)
	meta, err := doc.GetMetadata(xmem.uncompressBody)
	if err != nil {
		return false, err
	}

	return meta.GetHLV().GetCvSrc() == xmem.targetActorId, nil
}

func (xmem *XmemNozzle) sendSingleSetMeta(bytesList [][]byte, numOfRetry int) error {
	var err error
	if xmem.client_for_setMeta != nil {
		gcList := [][]byte{}
		for j := 0; j < numOfRetry; j++ {
			err, rev := xmem.writeToClient(xmem.client_for_setMeta, bytesList, true, gcList)
			if err == nil {
				atomic.AddUint64(&xmem.counter_resend, 1)
				return nil
			} else if err == base.BadConnectionError {
				xmem.repairConn(xmem.client_for_setMeta, err.Error(), rev)
			}
		}
		for _, bytesToRecycle := range gcList {
			xmem.dataPool.PutByteSlice(bytesToRecycle)
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
		pool, err = base.ConnPoolMgr().GetOrCreatePool(poolName, xmem.config.connectStr, xmem.config.bucketName, xmem.config.username, xmem.config.password, xmem.config.connPoolSize, true /*plainAuth*/)
		if err != nil {
			return nil, err
		}
	} else if xmem.config.encryptionType == metadata.EncryptionType_Half {
		pool, err = base.ConnPoolMgr().GetOrCreatePool(poolName, xmem.config.connectStr, xmem.config.bucketName, xmem.config.username, xmem.config.password, xmem.config.connPoolSize, false /*plainAuth*/)
		if err != nil {
			return nil, err
		}
	} else {
		//create a pool of SSL connection
		poolName := xmem.getPoolName()
		hostName := base.GetHostName(xmem.config.connectStr)

		if xmem.config.memcached_ssl_port != 0 {
			xmem.Logger().Infof("%v Get or create ssl over memcached connection, memcached_ssl_port=%v\n", xmem.Id(), int(xmem.config.memcached_ssl_port))
			pool, err = base.ConnPoolMgr().GetOrCreateSSLOverMemPool(poolName, hostName, xmem.config.bucketName, xmem.config.username, xmem.config.password,
				xmem.config.connPoolSize, int(xmem.config.memcached_ssl_port), xmem.config.certificate, xmem.config.san_in_certificate,
				xmem.config.clientCertificate, xmem.config.clientKey)

		} else {
			return nil, fmt.Errorf("%v cannot find memcached ssl port", xmem.Id())
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
	if err == base.WrongConnTypeError {
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

	memClient_setMeta, err := xmem.getClientWithRetry(xmem.Id(), pool, xmem.finish_ch, true /*initializing*/, xmem.Logger())
	if err != nil {
		return
	}
	err = memClient_setMeta.EnableDataPool(xmem.dataPool.GetByteSlice, xmem.dataPool.PutByteSlice)
	if err != nil {
		xmem.Logger().Errorf("Error enabling datapool for setmeta client %v", err)
		return
	}
	// Set client immediately so that if the pipeline start times out during the next getClientWithRetry call, this client connection can be cleaned up
	xmem.setClient(base.NewXmemClient(SetMetaClientName, xmem.config.readTimeout,
		xmem.config.writeTimeout, memClient_setMeta,
		xmem.config.maxRetry, xmem.config.max_read_downtime, xmem.Logger()), true /*isSetMeta*/)

	memClient_getMeta, err := xmem.getClientWithRetry(xmem.Id(), pool, xmem.finish_ch, true /*initializing*/, xmem.Logger())
	if err != nil {
		return
	}
	err = memClient_getMeta.EnableDataPool(xmem.dataPool.GetByteSlice, xmem.dataPool.PutByteSlice)
	if err != nil {
		xmem.Logger().Errorf("Error enabling datapool for getmeta client %v", err)
		return
	}
	xmem.setClient(base.NewXmemClient(GetMetaClientName, xmem.config.readTimeout,
		xmem.config.writeTimeout, memClient_getMeta,
		xmem.config.maxRetry, xmem.config.max_read_downtime, xmem.Logger()), false /*isSetMeta*/)

	// send helo command to setMeta and getMeta clients
	features, err := xmem.sendHELO(true /*setMeta*/)
	if err != nil {
		return err
	}

	err = xmem.validateFeatures(features, true /*setMeta*/)
	if err != nil {
		return err
	}

	xerrorMapVersion := mc.ErrorMapCB50
	ref, err := xmem.remoteClusterSvc.RemoteClusterByUuid(xmem.targetClusterUuid, false)
	if err == nil {
		capability, err := xmem.remoteClusterSvc.GetCapability(ref)
		if err == nil && capability.HasAdvErrorMapSupport() {
			xerrorMapVersion = mc.ErrorMapCB75
		}
	}
	xmem.memcachedErrMap, err = xmem.client_for_setMeta.GetMemClient().GetErrorMap(mc.ErrorMapVersion(xerrorMapVersion))
	if err != nil {
		return err
	}

	// initialize this only once here
	xmem.xattrEnabled = features.Xattribute

	features, err = xmem.sendHELO(false /*setMeta */)
	if err != nil {
		return err
	}

	err = xmem.validateFeatures(features, false /*setMeta*/)
	if err != nil {
		return err
	}

	xmem.Logger().Infof("%v done with initializeConnection. %v", xmem.Id(), features.String())
	return err
}

func (xmem *XmemNozzle) validateFeatures(features utilities.HELOFeatures, setMeta bool) error {
	if !setMeta {
		// getMeta client
		if xmem.conflictLoggingEnabled() &&
			(!features.DataType || !features.Xattribute || !features.SnappyEverywhere) {
			err := fmt.Errorf("%v JSON, XATTR and SnappyEverywhere needs to be enabled for GetEx command. features=%+v", xmem.Id(), features)
			xmem.Logger().Error(err.Error())
			return err
		}
		return nil
	}

	// setMeta client
	if xmem.compressionSetting != features.CompressionType {
		errMsg := fmt.Sprintf("%v Attempted to send HELO with compression type: %v, but received response with %v",
			xmem.Id(), xmem.compressionSetting, features.CompressionType)
		xmem.Logger().Error(errMsg)
		if xmem.compressionSetting != base.CompressionTypeNone {
			return base.ErrorCompressionNotSupported
		} else {
			// This is potentially a serious issue
			return errors.New(errMsg)
		}
	}

	collectionRequested := atomic.LoadUint32(&xmem.collectionEnabled) != 0
	if collectionRequested && !features.Collections {
		xmem.Logger().Errorf("%v target should have collection support returned with no collection support", xmem.Id())
		return base.ErrorTargetCollectionsNotSupported
	}

	if !features.Xerror {
		xmem.Logger().Warnf("%v Attempted to send HELO with Xerror enabled, but received response without Xerror enabled",
			xmem.Id())
	}

	return nil
}

func (xmem *XmemNozzle) getPoolName() string {
	return xmem.config.connPoolNamePrefix + base.KeyPartsDelimiter + "Couch_Xmem_" + xmem.config.connectStr + base.KeyPartsDelimiter + xmem.config.bucketName
}

// HLVModeOptions.NoTargetCR is not set for the batch. It is set whenever subdoc get is used.
// For mixed mode where old mutations uses getMeta (mobile=off), target CR is used
func (xmem *XmemNozzle) initNewBatch() {
	xmem.batch = newBatch(uint32(xmem.config.maxCount), uint32(xmem.config.maxSize), xmem.Logger())
	atomic.StoreUint32(&xmem.cur_batch_count, 0)
	// For the current batch, getMeta, conflict detection algorithm, and setMeta depend on
	// CRR, HLV, and mobile
	isCCR := xmem.isCCR()
	crossClusterVers := xmem.getCrossClusterVers()
	isMobile := xmem.getMobileCompatible() != base.MobileCompatibilityOff
	conflictLoggingEnabled := xmem.conflictLoggingEnabled() &&
		xmem.config.vbHlvMaxCas != nil &&
		xmem.config.targetVbHlvMaxCas != nil

	// continue with the same behaviour during the entire lifecycle
	// for all the requests of this batch.
	xmem.batch.isCCR = isCCR
	xmem.batch.isMobile = isMobile
	xmem.batch.crossClusterVer = crossClusterVers
	xmem.batch.conflictLoggingEnabled = conflictLoggingEnabled

	subdocSpecOpt := base.SubdocSpecOption{}
	if isMobile {
		subdocSpecOpt.IncludeMobileSync = true
		subdocSpecOpt.IncludeVXattr = true
		subdocSpecOpt.IncludeXTOC = true
		// For mixed mode, we never need to get target HLV
		xmem.batch.getMetaSpecWithoutHlv = base.ComposeSpecForSubdocGet(subdocSpecOpt)
	}
	if crossClusterVers || isCCR {
		subdocSpecOpt.IncludeImportCas = crossClusterVers
		subdocSpecOpt.IncludeHlv = true // CCR needs target HLV for CR, crossClusterVers needs cvCas
		subdocSpecOpt.IncludeVXattr = true
		subdocSpecOpt.ConfictLoggingEnabled = conflictLoggingEnabled
		xmem.batch.getMetaSpecWithHlv = base.ComposeSpecForSubdocGet(subdocSpecOpt)
		// This is needed for CCR.
		subdocSpecOpt.IncludeBody = true
		xmem.batch.getBodySpec = base.ComposeSpecForSubdocGet(subdocSpecOpt)
	}
}

func (xmem *XmemNozzle) initializeCompressionSettings(settings metadata.ReplicationSettingsMap) error {
	compressionVal, ok := settings[SETTING_COMPRESSION_TYPE].(base.CompressionType)
	if !ok {
		// Unusual case
		xmem.Logger().Warnf("%v missing compression type setting. Defaulting to ForceUncompress", xmem.Id())
		compressionVal = base.CompressionTypeForceUncompress
	}

	switch compressionVal {
	case base.CompressionTypeNone:
		fallthrough
	case base.CompressionTypeSnappy:
		// Note for XMEM, compressionTypeSnappy simply means "negotiate with target to make sure it can receive snappy data"
		xmem.compressionSetting = base.CompressionTypeSnappy
	case base.CompressionTypeForceUncompress:
		// This is only used when target cannot receive snappy data
		xmem.compressionSetting = base.CompressionTypeNone
	default:
		return base.ErrorCompressionNotSupported
	}
	return nil
}

func (xmem *XmemNozzle) initialize(settings metadata.ReplicationSettingsMap) error {
	err := xmem.config.initializeConfig(settings, xmem.utils)
	if err != nil {
		return err
	}

	err = xmem.initializeCompressionSettings(settings)
	if err != nil {
		return err
	}

	if xmem.sourceActorId, err = hlv.UUIDstoDocumentSource(xmem.sourceBucketUuid, xmem.sourceClusterUuid); err != nil {
		xmem.Logger().Errorf("Cannot convert source bucket UUID %v to base64. Error: %v", xmem.sourceBucketUuid, err)
		return err
	}
	if xmem.targetActorId, err = hlv.UUIDstoDocumentSource(xmem.targetBucketUuid, xmem.targetClusterUuid); err != nil {
		xmem.Logger().Errorf("Cannot convert target bucket UUID %v to base64. Error: %v", xmem.targetClusterUuid, err)
		return err
	}
	xmem.Logger().Infof("%v: Using %s (sourceBucketUUID %s + sourceClusterUUID %s) and %s (targetBucketUUID %s + targetClusterUUID %s) as source and target actor IDs for HLV.",
		xmem.Id(), xmem.sourceActorId, xmem.sourceBucketUuid, xmem.sourceClusterUuid, xmem.targetActorId, xmem.targetBucketUuid, xmem.targetClusterUuid)

	xmem.setDataChan(make(chan *base.WrappedMCRequest, xmem.config.maxCount*10))
	xmem.bytes_in_dataChan = 0
	xmem.dataChan_control = make(chan bool, 1)
	// put a true here so the first writeToDataChan() can go through
	xmem.dataChan_control <- true

	xmem.batches_ready_queue = make(chan *dataBatch, 100)

	xmem.counter_received = 0
	xmem.counter_sent = 0
	xmem.counter_ignored = 0

	//init a new batch
	xmem.initNewBatch()

	xmem.receive_token_ch = make(chan int, xmem.config.maxCount*2)
	xmem.setRequestBuffer(newReqBuffer(uint16(xmem.config.maxCount*2), uint16(float64(xmem.config.maxCount)*0.2), xmem.receive_token_ch, xmem.Logger(), xmem.dataPool))

	_, exists := settings[ForceCollectionDisableKey]
	if exists {
		atomic.StoreUint32(&xmem.collectionEnabled, 0)
	}

	xmem.Logger().Infof("%v About to start initializing connection", xmem.Id())
	err = xmem.initializeConnection()
	if err == nil {
		xmem.Logger().Infof("%v Connection initialization completed successfully", xmem.Id())
	} else {
		xmem.Logger().Errorf("%v Error initializating connections. err=%v", xmem.Id(), err)
	}
	return err
}

func (xmem *XmemNozzle) getRequestBuffer() *requestBuffer {
	xmem.stateLock.RLock()
	defer xmem.stateLock.RUnlock()
	return xmem.buf
}

func (xmem *XmemNozzle) setRequestBuffer(buf *requestBuffer) {
	xmem.stateLock.Lock()
	defer xmem.stateLock.Unlock()
	xmem.buf = buf
}

func (xmem *XmemNozzle) getDataChan() chan *base.WrappedMCRequest {
	xmem.stateLock.RLock()
	defer xmem.stateLock.RUnlock()
	return xmem.dataChan
}

func (xmem *XmemNozzle) setDataChan(dataChan chan *base.WrappedMCRequest) {
	xmem.stateLock.Lock()
	defer xmem.stateLock.Unlock()
	xmem.dataChan = dataChan
}

func (xmem *XmemNozzle) getClient(isSetMeta bool) *base.XmemClient {
	xmem.stateLock.RLock()
	defer xmem.stateLock.RUnlock()
	if isSetMeta {
		return xmem.client_for_setMeta
	} else {
		return xmem.client_for_getMeta
	}
}

func (xmem *XmemNozzle) setClient(client *base.XmemClient, isSetMeta bool) {
	xmem.stateLock.Lock()
	defer xmem.stateLock.Unlock()
	if isSetMeta {
		xmem.client_for_setMeta = client
	} else {
		xmem.client_for_getMeta = client
	}
}

func shouldUpdateXmemErrorUIAlerts(posSeenBefore, seenErrForUIAlert bool, errSeenBefore, errSeenNow mc.Status) bool {
	return seenErrForUIAlert &&
		(!posSeenBefore || (posSeenBefore && errSeenBefore != errSeenNow))
}

// Used to raise/update UI alert for certain errors received by xmem.client_for_setMeta,
// and then subsequently dismiss them when some other type of response is received for the same buffer "pos".
func (xmem *XmemNozzle) updateErrsForUIAlert(response *mc.MCResponse, seenErrForUIAlert bool) {
	if response == nil {
		return
	}

	pos := xmem.getPosFromOpaque(response.Opaque)
	errSeenNow := response.Status

	xmem.errsForUIAlertMtx.RLock()
	errSeenBefore, posSeenBefore := xmem.errsForUIAlert[pos]
	xmem.errsForUIAlertMtx.RUnlock()

	if shouldUpdateXmemErrorUIAlerts(posSeenBefore, seenErrForUIAlert, errSeenBefore, errSeenNow) {
		xmem.errsForUIAlertMtx.Lock()
		errSeenBefore, seenBefore := xmem.errsForUIAlert[pos]
		if shouldUpdateXmemErrorUIAlerts(seenBefore, seenErrForUIAlert, errSeenBefore, errSeenNow) {
			xmem.errsForUIAlert[pos] = errSeenNow
		}
		xmem.errsForUIAlertMtx.Unlock()
	} else if !seenErrForUIAlert && posSeenBefore {
		xmem.errsForUIAlertMtx.Lock()
		if _, seenBefore := xmem.errsForUIAlert[pos]; !seenErrForUIAlert && seenBefore {
			delete(xmem.errsForUIAlert, pos)
		}
		xmem.errsForUIAlertMtx.Unlock()
	}
}

// Note that this function should not be used to
// identify if a given request performs optimistic cas locking or not.
// Use IsCasLockingRequest for that.
func (xmem *XmemNozzle) nonOptimisticCROnly(req *base.WrappedMCRequest) bool {
	if xmem.config.vbHlvMaxCas == nil || req == nil {
		return false
	}

	return xmem.getMobileCompatible() != base.MobileCompatibilityOff || xmem.usesHlv(req)

}

// returns true if HLV can be stamped/updated for this req.
func (xmem *XmemNozzle) usesHlv(req *base.WrappedMCRequest) bool {
	if xmem.config.vbHlvMaxCas == nil || req == nil {
		return false
	}

	return (xmem.getCrossClusterVers() &&
		req.HLVModeOptions.ActualCas >= xmem.config.vbHlvMaxCas[req.Req.VBucket]) ||
		xmem.isCCR()
}

func (xmem *XmemNozzle) isCCR() bool {
	return xmem.source_cr_mode == base.CRMode_Custom
}

func (xmem *XmemNozzle) receiveResponse(finch chan bool, waitGrp *sync.WaitGroup) {
	defer waitGrp.Done()

	var seenErrForUIAlert bool
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

			seenErrForUIAlert = false
			response, err, rev := xmem.readFromClient(xmem.client_for_setMeta, true)
			if err != nil {
				if err == PartStoppedError {
					goto done
				}

				if err == base.FatalError {
					// if possible, log the corresponding request to facilitate debugging
					if response != nil {
						pos := xmem.getPosFromOpaque(response.Opaque)
						wrappedReq, err := xmem.buf.slot(pos)
						if err == nil && wrappedReq != nil {
							req := wrappedReq.Req
							if req != nil && req.Opaque == response.Opaque {
								xmem.Logger().Warnf("%v received fatal error from setMeta client. req=%v, seqno=%v, response=%v\n", xmem.Id(), req, wrappedReq.Seqno, response)
							}
						}
					}
					xmem.Logger().Errorf("%v readFromClient received fatal error", xmem.Id())
					xmem.handleGeneralError(ErrorFatalError)
					goto done
				}

				if err == base.BadConnectionError || err == base.ConnectionClosedError {
					xmem.Logger().Errorf("%v The connection is ruined. Repair the connection and retry.", xmem.Id())
					xmem.repairConn(xmem.client_for_setMeta, err.Error(), rev)
					response.Recycle()
					continue
				}
			}

			if response == nil {
				errMsg := fmt.Sprintf("%v readFromClient returned nil error and nil response. Ignoring it", xmem.Id())
				xmem.Logger().Warn(errMsg)
				continue
			}

			pos := xmem.getPosFromOpaque(response.Opaque)
			wrappedReq, sent_time, err := xmem.buf.slotWithSentTime(pos)
			if err != nil {
				xmem.Logger().Errorf("%v xmem buffer is in invalid state", xmem.Id())
				xmem.handleGeneralError(ErrorBufferInvalidState)
				response.Recycle()
				goto done
			}

			if wrappedReq == nil {
				// wrappedReq == nil can only happen in the following scenario:
				// 1. The request was sent but response is slow
				// 2. checkAndRepairBufferMonitor resend it
				// 3. The response for the first send comes back and the request is evicted from its slot
				// Now we are seeing the response for the resend. So we can ignore it
				if xmem.Logger().GetLogLevel() >= log.LogLevelDebug {
					xmem.Logger().Debugf("%v Response %v received but request is not in buffer. pos=%v, opcode=%v, opaque=%v",
						xmem.Id(), response.Status, pos, response.Opcode, response.Opaque)
				}
				response.Recycle()
				continue
			}

			if wrappedReq.Req == nil {
				if xmem.Logger().GetLogLevel() >= log.LogLevelDebug {
					xmem.Logger().Debugf("%v Response %v received but request is nil. pos=%v, opcode=%v, opaque=%v",
						xmem.Id(), response.Status, pos, response.Opcode, response.Opaque)
				}
				response.Recycle()
				continue
			}

			if wrappedReq.Req.Opaque != response.Opaque {
				// This is similar to wrappedReq == nil, except the slot is already being reused so the opaque doesn't match
				if xmem.Logger().GetLogLevel() >= log.LogLevelDebug {
					xmem.Logger().Debugf("Request in the buffer for key %v%s%v has opaque=%v while the response opaque=%v, pos=%v, req.Opcode=%v, resp.Opcode=%v",
						base.UdTagBegin, bytes.Trim(wrappedReq.Req.Key, "\x00"), base.UdTagEnd,
						wrappedReq.Req.Opaque, response.Opaque, pos, wrappedReq.Req.Opcode, response.Opcode)
				}
				response.Recycle()
				continue
			}

			if response.Status != mc.SUCCESS && !base.IsIgnorableMCResponse(response, wrappedReq.IsCasLockingRequest()) {
				if base.IsMutationLockedError(response.Status) {
					// if target mutation is currently locked, resend doc
					atomic.AddUint64(&xmem.counter_locked, 1)
					// set the mutationLocked flag on the corresponding bufferedMCRequest
					// which will allow resendIfTimeout() method to perform resend with exponential backoff with appropriate settings
					_, err = xmem.buf.modSlot(pos, xmem.setMutationLockedFlag)
					if err != nil {
						xmem.Logger().Errorf("%v received error with setMutationLockedFlag, err=%v", xmem.Id(), err)
					}
				} else if base.IsTemporaryMCError(response.Status) {
					// target may be overloaded. increase backoff factor to alleviate stress on target
					xmem.client_for_setMeta.IncrementBackOffFactor()

					// error is temporary. resend doc
					// Don't spam the log. Keep a counter instead
					atomic.AddUint64(&xmem.counter_tmperr, 1)
					xmem.RaiseEvent(common.NewEvent(common.DataSentFailed, response.Status, xmem, nil, nil))
					seenErrForUIAlert = true
					//resend and reset the retry=0 as retry is an indicator of network status,
					//here we have received the response, so reset retry=0
					_, err = xmem.buf.modSlot(pos, xmem.resendWithReset)
					if err != nil {
						xmem.Logger().Errorf("%v received error for resendWithReset, err=%v", err)
					}
				} else if base.IsEAccessError(response.Status) {
					// Losing access can happen when it is revoked on purpose or can happen temporarily
					// when target is undergoing topology changes like node leaving cluster. We will retry.
					xmem.client_for_setMeta.IncrementBackOffFactor()
					// Don't spam the log. Keep a counter instead
					atomic.AddUint64(&xmem.counter_eaccess, 1)
					xmem.RaiseEvent(common.NewEvent(common.DataSentFailed, response.Status, xmem, nil, nil))
					seenErrForUIAlert = true
					_, err = xmem.buf.modSlot(pos, xmem.resendWithReset)
					if err != nil {
						xmem.Logger().Errorf("%v received error for resendWithReset during EAccess error, err=%v", xmem.Id(), err)
					}
				} else if base.IsEExistsError(response.Status) || base.IsENoEntError(response.Status) {
					// request failed because target Cas changed.
					if wrappedReq.IsCasLockingRequest() {
						// We only care about this if we are doing CAS locking for CCR or mobile, otherwise we can ignore this error.
						xmem.Logger().Debugf("%v Retry conflict resolution for %v%q%v because target Cas has changed (EEXISTS).", xmem.Id(), base.UdTagBegin, wrappedReq.Req.Key, base.UdTagEnd)
						additionalInfo := SentCasChangedEventAdditional{
							Opcode: wrappedReq.Req.Opcode,
						}
						xmem.RaiseEvent(common.NewEvent(common.DataSentCasChanged, nil, xmem, nil, additionalInfo))

						if xmem.buf.evictSlot(pos) != nil {
							panic(fmt.Sprintf("Failed to evict slot %d\n", pos))
						}

						// Put it back to the next batch so we can retry conflict resolution Do this in the background.
						// Otherwise xmem may go into deadlock if receiveResponse is blocked because dataChan is full
						go xmem.retryAfterCasLockingFailure(wrappedReq)
					}
				} else {
					var req *mc.MCRequest = nil
					var seqno uint64
					req = wrappedReq.Req
					seqno = wrappedReq.Seqno
					if base.IsTopologyChangeMCError(response.Status) {
						vb_err := fmt.Errorf("received error %v on vb %v", base.ErrorNotMyVbucket, req.VBucket)
						xmem.handleVBError(req.VBucket, vb_err)
						// Do not resend if it is topology error, since the error has already been handled the line above
						// as resending would be a waste of resources
						if xmem.buf.evictSlot(pos) != nil {
							panic(fmt.Sprintf("Failed to evict slot %d\n", pos))
						}
						// Once evicted, it won't be resent and needs to be recycled
						xmem.recycleDataObj(wrappedReq)
					} else if base.IsCollectionMappingError(response.Status) {
						// upstreamErrReporter will recycle wrappedReq once it's done
						xmem.upstreamErrReporter(wrappedReq)
						if xmem.buf.evictSlot(pos) != nil {
							panic(fmt.Sprintf("Failed to evict slot %d\n", pos))
						}
						atomic.AddUint64(&xmem.counter_ignored, 1)
					} else if base.IsGuardRailError(response.Status) {
						xmem.client_for_setMeta.IncrementBackOffFactor()
						// Don't spam the log. Keep a counter instead
						atomic.AddUint64(&xmem.counterGuardrailHit, 1)
						vbno := wrappedReq.GetTargetVB()
						seqno := wrappedReq.Seqno
						xmem.RaiseEvent(common.NewEvent(common.DataSentHitGuardrail, response.Status, xmem, []interface{}{vbno, seqno}, nil))
						markThenResend := func(req *bufferedMCRequest, pos uint16) (bool, error) {
							req.setGuardrail(response.Status)
							return xmem.resendWithReset(req, pos)
						}
						_, err = xmem.buf.modSlot(pos, markThenResend)
						if err != nil {
							xmem.Logger().Errorf("%v received error for markThenResend, err=%v", xmem.Id(), err)
						}
					} else {
						if response.Status == mc.XATTR_EINVAL {
							// There is something wrong with XATTR. This should never happen in released version.
							// Print the XATTR for debugging
							if req.DataType&mcc.SnappyDataType == mcc.SnappyDataType {
								body, err := snappy.Decode(nil, req.Body)
								if err == nil {
									xattrlen := binary.BigEndian.Uint32(body[0:4])
									bodyWithoutXattr, err := base.StripXattrAndGetBody(body)
									xmem.Logger().Errorf("%v received error response %v for key %v%s%v with compressed body. Total xattr length %v, body with xattr: %q, body without xattr: %q, err %v",
										xmem.Id(), response.Status, base.UdTagBegin, req.Key, base.UdTagEnd, xattrlen, body, bodyWithoutXattr, err)
								} else {
									xmem.Logger().Errorf("%v received error response %v for key %v%s%v with compressed body. Decode failed with error %v",
										xmem.Id(), response.Status, base.UdTagBegin, req.Key, base.UdTagEnd, err)
								}
							} else {
								xattrlen := binary.BigEndian.Uint32(req.Body[0:4])
								bodyWithoutXattr, err := base.StripXattrAndGetBody(req.Body)
								xmem.Logger().Errorf("%v received error response %v for key %v%s%v with uncompressed body. Total xattr length %v, body with xattr: %q, body without xattr %q, err: %v",
									xmem.Id(), response.Status, base.UdTagBegin, req.Key, base.UdTagEnd, xattrlen, req.Body, bodyWithoutXattr, err)
							}
						}
						// for other non-temporary errors, repair connections
						xmem.Logger().Errorf("%v received error response from setMeta client. Repairing connection. %v, opcode=%v, seqno=%v, req.Key=%v%s%v, req.Body=%v%v%v, req.Datatype=%v, req.Cas=%v, req.Extras=%v, response=%v%v%v",
							xmem.Id(), xmem.PrintResponseStatusError(response.Status), response.Opcode, seqno, base.UdTagBegin, string(req.Key), base.UdTagEnd, base.UdTagBegin, req.Body, base.UdTagEnd, req.DataType, req.Cas, req.Extras, base.UdTagBegin, response.Body, base.UdTagEnd)
						xmem.client_for_setMeta.ReportUnknownResponseReceived(response.Status)

						seenErrForUIAlert = true

						vbno := wrappedReq.Req.VBucket
						xmem.RaiseEvent(common.NewEvent(common.DataSentFailedUnknownStatus, response.Status, xmem, []interface{}{vbno, seqno}, nil))
						atomic.AddUint64(&xmem.counterUnknownStatus, 1)
						xmem.repairConn(xmem.client_for_setMeta, "error response from memcached", rev)
					}
				}
			} else {
				// Raise needed events
				var req *mc.MCRequest
				var seqno uint64
				var committing_time time.Duration
				var resp_wait_time time.Duration
				var manifestId uint64
				req = wrappedReq.Req
				seqno = wrappedReq.Seqno
				committing_time = time.Since(wrappedReq.Start_time)
				resp_wait_time = time.Since(*sent_time)
				manifestId = wrappedReq.GetManifestId()

				now := time.Now()
				// before moving the throughseqno forward,
				// wait for it's conflict logging to be done
				cLogErr := wrappedReq.WaitForConflictLogging(xmem.finish_ch)
				if cLogErr != nil && cLogErr != baseclog.ErrLogWaitAborted {
					// the errors are captured in the form of stats.
					// One has to turn on debug logging inorder to identify the exact errors
					// that are not captured by the stats.
					xmem.Logger().Debugf("Error waiting for conflict logging to finish, key=%v%s%v, err=%v",
						base.UdTagBegin, req.Key, base.UdTagEnd,
						cLogErr,
					)
				}
				cLogWaitTime := time.Since(now)

				isExpirySet := false
				subdocOpType := base.NotSubdoc
				if wrappedReq.IsSubdocOp() {
					isExpirySet = (len(req.Extras) > 1)
					subdocOpType = wrappedReq.SubdocCmdOptions.SubdocOp
				} else {
					isExpirySet = (len(req.Extras) >= 8 && binary.BigEndian.Uint32(req.Extras[4:8]) != 0)
				}

				additionalInfo := DataSentEventAdditional{
					Seqno:                seqno,
					VbucketCommon:        VbucketCommon{VBucket: wrappedReq.GetTargetVB()},
					IsOptRepd:            xmem.optimisticRep(wrappedReq),
					Opcode:               req.Opcode,
					IsExpirySet:          isExpirySet,
					Req_size:             req.Size(),
					Commit_time:          committing_time,
					Resp_wait_time:       resp_wait_time,
					ManifestId:           manifestId,
					FailedTargetCR:       base.IsEExistsError(response.Status),
					UncompressedReqSize:  req.Size() - wrappedReq.GetBodySize() + wrappedReq.GetUncompressedBodySize(),
					SkippedRecompression: wrappedReq.SkippedRecompression,
					ImportMutation:       wrappedReq.ImportMutation,
					Cloned:               wrappedReq.Cloned,
					CloneSyncCh:          wrappedReq.ClonedSyncCh,
					SubdocOpType:         subdocOpType,
					CasPoisonProtection:  xmem.handleCasPoisoning(wrappedReq, response),
					CLogWaitTime:         cLogWaitTime,
					CLogError:            cLogErr,
				}
				if wrappedReq.OrigSrcVB != nil {
					additionalInfo.SetOrigSrcVB(*wrappedReq.OrigSrcVB)
				}
				xmem.RaiseEvent(common.NewEvent(common.DataSent, nil, xmem, nil, additionalInfo))
				if wrappedReq.ImportMutation && xmem.getMobileCompatible() == base.MobileCompatibilityOff && atomic.CompareAndSwapUint32(&xmem.importMutationEventRaised, 0, 1) {
					xmem.importMutationEventId = xmem.eventsProducer.AddEvent(
						base.LowPriorityMsg,
						base.ImportDetectedStr,
						base.EventsMap{},
						nil)
				}

				//feedback the most current commit_time to xmem.config.respTimeout
				xmem.adjustRespTimeout(resp_wait_time)

				//empty the slot in the buffer
				if xmem.buf.evictSlot(pos) != nil {
					panic(fmt.Sprintf("Failed to evict slot %d\n", pos))
				}

				//put the request object back into the pool
				xmem.recycleDataObj(wrappedReq)
			}

			xmem.updateErrsForUIAlert(response, seenErrForUIAlert)
			response.Recycle()
		}
	}

done:
	xmem.Logger().Infof("%v receiveResponse exits\n", xmem.Id())
}

func (xmem *XmemNozzle) handleCasPoisoning(wrappedReq *base.WrappedMCRequest, response *mc.MCResponse) base.TargetKVCasPoisonProtectionMode {
	isSubDocOp := wrappedReq.IsSubdocOp()
	sentCas, err := wrappedReq.GetSentCas()
	if err != nil && !isSubDocOp {
		// if subDoc is not used and extras.cas is not set then it must be a coding error. Log it
		// Note: We shouldn't hit this case in practice
		xmem.Logger().Errorf("extras.cas is not set in req %v. len of extras:%v, isSubDocOp: %v", wrappedReq.Req, len(wrappedReq.Req.Extras), isSubDocOp)
	}
	if response.Status == mc.SUCCESS && (sentCas != 0 && sentCas != response.Cas) && !isSubDocOp { //replace mode
		// Currently CAS regeneration takes place in two scenario's
		// 1. If SubDoc is used
		// 2. If there is a CAS poisoned doc and KV's protection mode is set to "replace"
		// Note that this counter does not account for poisoned CAS's incase of subDocOp
		return base.ReplaceMode
	} else if response.Status == mc.CAS_VALUE_INVALID { //error mode
		return base.ErrorMode
	}

	return base.CasNotPoisoned
}

func (xmem *XmemNozzle) retryAfterCasLockingFailure(req *base.WrappedMCRequest) {
	if req.IsSubdocOp() {
		// If we have used subdoc command before for this request before, enter the retry assuming we wouldn't need subdoc command anymore
		if req.SubdocCmdOptions.SubdocOp == base.SubdocDelete {
			req.Req.Opcode = mc.DELETE_WITH_META
		} else {
			req.Req.Opcode = mc.SET_WITH_META
		}
		req.Req.Body = req.SubdocCmdOptions.BodyPreSubdocCmd
		req.Req.DataType = req.SubdocCmdOptions.DatatypePreSubdocCmd
		req.Req.Extras = req.SubdocCmdOptions.ExtrasPreSubdocCmd
		req.ResetSubdocOptionsForRetry()
	} else {
		// If it is ADD_WITH_META, it needs to be SET_WITH_META in the next try because target now has the doc
		if req.Req.Opcode == base.ADD_WITH_META {
			req.Req.Opcode = base.SET_WITH_META
		}
	}
	req.Req.Cas = binary.BigEndian.Uint64(req.Req.Extras[16:24])
	req.Req.Opaque = 0
	req.ResetCLogOptions()
	// Don't free the slices from datapool since that's the mutation body.
	req.RetryCRCount++
	err := xmem.accumuBatch(req)
	if err != nil {
		xmem.Logger().Errorf("%v Retry conflict resolution for %v%s%v failed accumuBatch call with error %v", xmem.Id(), base.UdTagBegin, bytes.Trim(req.Req.Key, "\x00"), base.UdTagEnd, err)
		xmem.handleGeneralError(err)
		return
	}
	atomic.AddUint64(&xmem.counter_retry_cr, 1)
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
	new_maxIdleCount = uint32(math.Max(float64(new_maxIdleCount), float64(base.XmemMaxIdleCountLowerBound)))
	new_maxIdleCount = uint32(math.Min(float64(new_maxIdleCount), float64(base.XmemMaxIdleCountUpperBound)))
	atomic.StoreUint32(&xmem.config.maxIdleCount, new_maxIdleCount)
}

func isNetTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	netError, ok := err.(*net.OpError)
	return ok && netError.Timeout()
}

// get max idle count adjusted by backoff_factor
func (xmem *XmemNozzle) getMaxIdleCount() uint32 {
	return atomic.LoadUint32(&(xmem.config.maxIdleCount))
}

// get max idle count adjusted by backoff_factor
func (xmem *XmemNozzle) getAdjustedMaxIdleCount() uint32 {
	max_idle_count := xmem.getMaxIdleCount()
	backoff_factor := math.Max(float64(xmem.client_for_getMeta.GetBackOffFactor()), float64(xmem.client_for_setMeta.GetBackOffFactor()))

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

/**
 * The self monitor's job is to observe the statistics going on the xmem nozzle.
 * Should there be any outstanding I/Os queued up after a certain amount of cycles,
 * the monitor will throw an error back to the pipeline to initiate a pipeline restart.
 */
func (xmem *XmemNozzle) selfMonitor(finch chan bool, waitGrp *sync.WaitGroup) {
	defer waitGrp.Done()
	ticker := time.NewTicker(xmem.config.selfMonitorInterval)
	defer ticker.Stop()
	statsTicker := time.NewTicker(xmem.config.statsInterval)
	defer statsTicker.Stop()
	var sent_count uint64 = 0
	var received_count uint64 = 0
	var ignored_count uint64 = 0
	var resp_waitingConfirm_count int = 0
	var repairCount_setMeta = 0
	var repairCount_getMeta = 0
	// if value is >= 0, it represents the eventId to dismiss. -1 represents no eventId.
	var guardrailMsgs [base.NumberOfGuardrailTypes]int64
	for i := 0; i < base.NumberOfGuardrailTypes; i++ {
		guardrailMsgs[i] = -1
	}
	var msgIdForUIAlert int64 = -1
	var previousTotalErrsForUIAlert map[mc.Status]uint16

	var count uint64
	// freeze_counter is used to count the number of check iterations that data has been stuck
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
			received_count = atomic.LoadUint64(&xmem.counter_received)
			buffer_count := xmem.buf.itemCountInBuffer()
			xmem_id := xmem.Id()
			isOpen := xmem.IsOpen()
			buffer_size := xmem.buf.bufferSize()
			empty_slots_pos := xmem.buf.empty_slots_pos
			batches_ready_queue := xmem.batches_ready_queue
			dataChan := xmem.dataChan
			xmem_count_sent := atomic.LoadUint64(&xmem.counter_sent)
			xmem_count_ignored := atomic.LoadUint64(&xmem.counter_ignored)
			count++

			if xmem_count_sent == sent_count && xmem_count_ignored == ignored_count &&
				int(buffer_count) == resp_waitingConfirm_count &&
				(len(xmem.dataChan) > 0 || buffer_count != 0) &&
				repairCount_setMeta == xmem.client_for_setMeta.RepairCount() &&
				repairCount_getMeta == xmem.client_for_getMeta.RepairCount() {
				// Increment freeze_counter if there is data waiting and the count hasn't increased since last time we checked
				freeze_counter++
			} else {
				// Any type of data movement resets the data freeze counter
				freeze_counter = 0
			}

			sent_count = xmem_count_sent
			ignored_count = xmem_count_ignored
			resp_waitingConfirm_count = int(buffer_count)
			repairCount_setMeta = xmem.client_for_setMeta.RepairCount()
			repairCount_getMeta = xmem.client_for_getMeta.RepairCount()
			if count == 10 {
				xmem.Logger().Debugf("%v- freeze_counter=%v, xmem.counter_sent=%v, xmem.counter_ignored=%v, len(xmem.dataChan)=%v, receive_count-%v, cur_batch_count=%v\n", xmem_id, freeze_counter, xmem_count_sent, xmem_count_ignored, len(dataChan), received_count, atomic.LoadUint32(&xmem.cur_batch_count))
				xmem.Logger().Debugf("%v open=%v checking..., %v item unsent, %v items waiting for response, %v batches ready\n", xmem_id, isOpen, len(xmem.dataChan), int(buffer_size)-len(empty_slots_pos), len(batches_ready_queue))
				count = 0
			}
			max_idle_count := xmem.getAdjustedMaxIdleCount()
			if freeze_counter > max_idle_count {
				xmem.Logger().Errorf("%v hasn't sent any item out for %v ticks, %v data in queue, flowcontrol=%v, con_retry_limit=%v, backoff_factor for client_setMeta is %v, backoff_factor for client_getMeta is %v", xmem_id, max_idle_count, len(dataChan), buffer_count <= xmem.buf.notify_threshold, xmem.client_for_setMeta.GetContinuousWriteFailureCounter(), xmem.client_for_setMeta.GetBackOffFactor(), xmem.client_for_getMeta.GetBackOffFactor())
				xmem.Logger().Infof("%v open=%v checking..., %v item unsent, received %v items, sent %v items, ignored %v item, %v items waiting for response, %v batches ready\n", xmem_id, isOpen, len(dataChan), received_count, xmem_count_sent, xmem_count_ignored, int(buffer_size)-len(empty_slots_pos), len(batches_ready_queue))
				//the connection might not be healthy, it should not go back to connection pool
				xmem.client_for_setMeta.MarkConnUnhealthy()
				xmem.client_for_getMeta.MarkConnUnhealthy()
				xmem.handleGeneralError(ErrorXmemIsStuck)
				goto done
			}

			currentGuardrailHit := xmem.buf.getGuardRailHit()
			for i := 0; i < base.NumberOfGuardrailTypes; i++ {
				if guardrailMsgs[i] == -1 && currentGuardrailHit[i] {
					// buffer indicates that this specific guardrail has been hit and we haven't displayed the message yet
					statusToUse := getMcStatusFromGuardrailIdx(i)
					eventId := xmem.eventsProducer.AddEvent(base.LowPriorityMsg,
						fmt.Sprintf("%s: %s", xmem.PrintResponseStatusError(statusToUse), xmem.topic), base.EventsMap{}, nil)
					guardrailMsgs[i] = eventId
				} else if guardrailMsgs[i] >= 0 && !currentGuardrailHit[i] {
					// buffer indicates that this guardrail is no longer an issue and we have a msg being displayed
					eventToDismiss := guardrailMsgs[i]
					err := xmem.eventsProducer.DismissEvent(int(eventToDismiss))
					if err != nil {
						xmem.Logger().Warnf("Unable to dismiss event %v: %v", eventToDismiss, err)
					} else {
						guardrailMsgs[i] = -1
					}
				}
			}

			// map of error responses (meant to get UI alerts) to their total count in the buffer
			var totalErrsForUIAlert map[mc.Status]uint16
			xmem.errsForUIAlertMtx.RLock()
			if len(xmem.errsForUIAlert) > 0 {
				xmem.processErrsForUIAlert(&totalErrsForUIAlert)
			} else if len(previousTotalErrsForUIAlert) > 0 {
				previousTotalErrsForUIAlert = map[mc.Status]uint16{}
			}
			xmem.errsForUIAlertMtx.RUnlock()

			if totalErrsForUIAlert == nil {
				if msgIdForUIAlert >= 0 {
					// delete the message if any, since there are no more error responses (for UI alerts) seen
					err := xmem.eventsProducer.DismissEvent(int(msgIdForUIAlert))
					if err != nil {
						xmem.Logger().Warnf("Unable to dismiss event %v: %v", msgIdForUIAlert, err)
					} else {
						msgIdForUIAlert = -1
					}
				}
				continue
			}

			var eventMsgStrBuilder strings.Builder
			eventMsgStrBuilder.WriteString("Following number of mutations are rejected by target due to error responses: ")
			changed := false
			for status, count := range totalErrsForUIAlert {
				eventMsgStrBuilder.WriteString(fmt.Sprintf("{%v : count=%v} | ", xmem.PrintResponseStatusError(status), count))
				if !changed && count != previousTotalErrsForUIAlert[status] {
					changed = true
				}
			}

			if !changed {
				// nothing changed from last the tick
				continue
			}

			eventMsgStrBuilder.WriteString("XMEM ID: " + xmem.Id())
			if msgIdForUIAlert >= 0 {
				// update the message
				err := xmem.eventsProducer.UpdateEvent(msgIdForUIAlert, eventMsgStrBuilder.String(), nil)
				if err != nil {
					xmem.Logger().Warnf("Unable to update event %v: %v", msgIdForUIAlert, err)
				}
			} else {
				// new message
				msgIdForUIAlert = xmem.eventsProducer.AddEvent(base.LowPriorityMsg, eventMsgStrBuilder.String(), base.EventsMap{}, nil)
			}
			previousTotalErrsForUIAlert = totalErrsForUIAlert

		case <-statsTicker.C:
			xmem.RaiseEvent(common.NewEvent(common.StatsUpdate, nil, xmem, nil, []int{len(xmem.dataChan), xmem.bytesInDataChan()}))
		}
	}
done:
	xmem.Logger().Infof("%v selfMonitor routine exits", xmem.Id())
}

// Helper function utilised by the (*XmemNozzle).selfMonitor() method
func (xmem *XmemNozzle) processErrsForUIAlert(totalErrsForUIAlert *map[mc.Status]uint16) {
	*totalErrsForUIAlert = make(map[mc.Status]uint16)
	tempMCErrSetShowDelay := xmem.config.selfMonitorInterval * time.Duration(base.TempMCErrorDisplayDelayFactor)

	for _, responseCode := range xmem.errsForUIAlert {
		// non-temporary errors are always displayed
		if !base.IsTemporaryMCError(responseCode) {
			(*totalErrsForUIAlert)[responseCode]++
			continue
		}

		xmem.tempMCErrMtx.Lock()
		xmem.tempMCErrLastSeen = time.Now()
		if xmem.tempMCErrSetShow == nil {
			xmem.tempMCErrSetShow = time.AfterFunc(
				tempMCErrSetShowDelay,
				func() {
					xmem.tempMCErrMtx.RLock()
					if xmem.tempMCErrLastSeen.Before(time.Now().Add(-xmem.config.selfMonitorInterval)) {
						xmem.tempMCErrMtx.RUnlock()
						return
					}
					xmem.tempMCErrMtx.RUnlock()

					xmem.tempMCErrMtx.Lock()
					xmem.tempMCErrShow = true
					xmem.tempMCErrUnsetShow = time.AfterFunc(tempMCErrSetShowDelay,
						func() {
							xmem.tempMCErrMtx.Lock()
							xmem.tempMCErrSetShow = nil
							xmem.tempMCErrShow = false
							xmem.tempMCErrUnsetShow = nil
							xmem.tempMCErrMtx.Unlock()
						},
					)
					xmem.tempMCErrMtx.Unlock()
				},
			)
		} else if xmem.tempMCErrShow {
			(*totalErrsForUIAlert)[responseCode]++
			if xmem.tempMCErrUnsetShow != nil && xmem.tempMCErrUnsetShow.Stop() { // to prevent an extraneous Reset() on the timer
				xmem.tempMCErrUnsetShow.Reset(tempMCErrSetShowDelay)
			}
		}
		xmem.tempMCErrMtx.Unlock()
	}
}

/**
 * Each check iteration, go through all the buffer slots to resend those that have not heard
 * back after timeout.
 * If after a certain number of resend, we still don't hear back from the target, then proceed
 * to repair the connection.
 */
func (xmem *XmemNozzle) checkAndRepairBufferMonitor(finch chan bool, waitGrp *sync.WaitGroup) {
	defer waitGrp.Done()
	ticker := time.NewTicker(base.XmemDefaultRespTimeout)
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
			timeoutCheckFunc := xmem.resendIfTimeout
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

func (xmem *XmemNozzle) resendIfTimeout(req *bufferedMCRequest, pos uint16) (bool, error) {
	req.lock.Lock()

	var modified bool
	var err error
	// perform op on req only when
	// 1. there is a valid WrappedMCRequest associated with req
	// and 2. req has not timed out, i.e., has not reached max retry limit
	if req.req != nil && !req.timedout {
		maxRetry := xmem.config.maxRetry
		if req.mutationLocked {
			maxRetry = xmem.config.maxRetryMutationLocked
		}
		if req.num_of_retry > maxRetry {
			req.timedout = true
			err = fmt.Errorf("%v Failed to resend document %v%q%v, has tried to resend it %v, maximum retry %v reached",
				xmem.Id(), base.UdTagBegin, req.req.Req.Key, base.UdTagEnd, req.num_of_retry, maxRetry)
			xmem.Logger().Error(err.Error())

			lastErrIsDueToCollectionMapping := req.collectionMapErr
			if lastErrIsDueToCollectionMapping {
				xmem.handleGeneralError(GetErrorXmemTargetUnknownCollection(req.req))
			}

			// release lock before the expensive repair calls
			req.lock.Unlock()

			if !lastErrIsDueToCollectionMapping {
				xmem.repairConn(xmem.client_for_setMeta, err.Error(), xmem.client_for_setMeta.RepairCount())
			}
			return true, err
		}

		respWaitTime := time.Since(*req.sent_time)
		if respWaitTime > xmem.timeoutDuration(req.num_of_retry, req.mutationLocked) {
			// resend the mutation

			bytesList := getBytesListFromReq(req, xmem.dataPool)
			old_sequence := xmem.buf.sequences[pos]

			// release the lock on req before calling sendSingleSetMeta, which may block
			req.lock.Unlock()

			err = xmem.sendSingleSetMeta(bytesList, xmem.config.maxRetry)

			if err == nil {
				// lock req again since it needs to be accessed and possibly modified
				req.lock.Lock()
				// update req only if it contains the same WrappedMCRequest as before the sendSingleSetMeta op
				// i.e., when the corresponding sequence number of the slot has not changed
				// i.e., when evictSlot and cancelReservation have not been called on the slot
				if req.req != nil && xmem.buf.sequences[pos] == old_sequence {
					now := time.Now()
					req.sent_time = &now
					req.num_of_retry += 1
					modified = true
				}
				req.lock.Unlock()
			}

			for _, gcSlice := range bytesList {
				xmem.dataPool.PutByteSlice(gcSlice)
			}
			return modified, err
		}
	}

	req.lock.Unlock()
	return false, nil
}

func getBytesListFromReq(req *bufferedMCRequest, dp base.DataPool) [][]byte {
	if req.req == nil || req.req.Req == nil {
		// should not happen. just in case
		return nil
	}
	bytesList := make([][]byte, 1)
	dpSlice, dpErr := dp.GetByteSlice(uint64(req.req.Req.Size()))
	if dpErr != nil {
		bytesList[0] = req.req.GetReqBytes()
	} else {
		req.req.GetReqBytesPreallocated(dpSlice)
		bytesList[0] = dpSlice
	}
	return bytesList
}

func (xmem *XmemNozzle) timeoutDuration(numofRetry int, isMutationLocked bool) time.Duration {
	duration := xmem.getRespTimeout()
	maxDuration := xmem.config.maxRetryInterval
	if isMutationLocked {
		maxDuration = xmem.config.maxRetryIntervalMutationLocked
	}
	for i := 1; i <= numofRetry; i++ {
		duration *= 2
		if duration > maxDuration {
			duration = maxDuration
			break
		}
	}
	return duration
}

func (xmem *XmemNozzle) resendWithReset(req *bufferedMCRequest, pos uint16) (bool, error) {
	req.lock.Lock()

	// check that there is a valid WrappedMCRequest associated with req
	if req.req == nil {
		req.lock.Unlock()
		return false, nil
	}

	var modified bool
	var err error

	bytesList := getBytesListFromReq(req, xmem.dataPool)
	old_sequence := xmem.buf.sequences[pos]

	if req.mutationLocked {
		req.mutationLocked = false
		modified = true
	}

	if req.collectionMapErr {
		req.collectionMapErr = false
		modified = true
	}

	// reset num_of_retry to 0 and reset timedout to false
	if req.num_of_retry != 0 || req.timedout {
		req.num_of_retry = 0
		req.timedout = false
		modified = true
	}

	// release the lock on req before calling sendSingleSetMeta, which may block
	req.lock.Unlock()

	err = xmem.sendSingleSetMeta(bytesList, xmem.config.maxRetry)
	for _, gcSlice := range bytesList {
		xmem.dataPool.PutByteSlice(gcSlice)
	}

	if err != nil {
		return modified, err
	}

	// lock req again since it needs to be accessed and possibly modified
	req.lock.Lock()
	// update req only if it contains the same WrappedMCRequest as before the sendSingleSetMeta op
	// i.e., when the corresponding sequence number of the slot has not changed
	// i.e., when evictSlot and cancelReservation have not been called on the slot
	if req.req != nil && xmem.buf.sequences[pos] == old_sequence {
		now := time.Now()
		req.sent_time = &now
		modified = true
		// keep req.num_of_retry as 0 since the sendSingleSetMeta op counts as the first send
		// and not as a retry
	}
	req.lock.Unlock()

	return modified, nil
}

// set mutationLocked flag on the bufferedMCRequest
// returns true if modification is made on the bufferedMCRequest
func (xmem *XmemNozzle) setMutationLockedFlag(req *bufferedMCRequest, pos uint16) (bool, error) {
	req.lock.Lock()
	defer req.lock.Unlock()

	// check that there is a valid WrappedMCRequest associated with req
	if req.req == nil {
		return false, nil
	}

	if !req.mutationLocked {
		req.mutationLocked = true
		req.num_of_retry = 0
		req.timedout = false
		return true, nil
	}

	// no op if req has already been marked as mutationLocked before

	return false, nil
}

func (xmem *XmemNozzle) getPosFromOpaque(opaque uint32) uint16 {
	result := uint16(0x0000FFFF & opaque)
	return result
}

func isCasLockingRequest(req *mc.MCRequest) bool {
	if req.Opcode == base.ADD_WITH_META && req.Cas == 0 {
		return true
	}
	if req.Opcode == base.SET_WITH_META && req.Cas != 0 {
		return true
	}
	return false
}

func (xmem *XmemNozzle) ConnType() base.ConnType {
	return xmem.connType
}

func (xmem *XmemNozzle) PrintStatusSummary() {

	if xmem.State() == common.Part_Running {
		connType := xmem.connType
		var avg_wait_time float64 = 0
		counter_sent := atomic.LoadUint64(&xmem.counter_sent)
		if counter_sent > 0 {
			avg_wait_time = float64(atomic.LoadUint64(&xmem.counter_waittime)) / float64(counter_sent)
		}
		xmem.Logger().Infof("%v state =%v connType=%v received %v items (%v compressed), sent %v items (%v compressed), target items skipped %v, ignored %v items, %v items waiting to confirm, %v in queue, %v in current batch, avg wait time is %vms, size of last ten batches processed %v, len(batches_ready_queue)=%v, resend=%v, locked=%v, repair_count_getMeta=%v, repair_count_setMeta=%v, retry_cr=%v, to resolve=%v, to setback=%v, numGetMeta=%v, numSubdocGet=%v, temp_err=%v, eaccess_err=%v, guardrailHit=%v, unknownStatusRec=%v, logConflicts=%v, numGetEx=%v, numGetComp=%v",
			xmem.Id(), xmem.State(), connType, atomic.LoadUint64(&xmem.counter_received),
			atomic.LoadUint64(&xmem.counter_compressed_received), atomic.LoadUint64(&xmem.counter_sent),
			atomic.LoadUint64(&xmem.counter_compressed_sent), atomic.LoadUint64(&xmem.counter_from_target), atomic.LoadUint64(&xmem.counter_ignored),
			xmem.buf.itemCountInBuffer(), len(xmem.dataChan),
			atomic.LoadUint32(&xmem.cur_batch_count), avg_wait_time, xmem.getLastTenBatchSize(),
			len(xmem.batches_ready_queue), atomic.LoadUint64(&xmem.counter_resend),
			atomic.LoadUint64(&xmem.counter_locked),
			xmem.client_for_getMeta.RepairCount(), xmem.client_for_setMeta.RepairCount(),
			atomic.LoadUint64(&xmem.counter_retry_cr), atomic.LoadUint64(&xmem.counter_to_resolve),
			atomic.LoadUint64(&xmem.counter_to_setback), atomic.LoadUint64(&xmem.counterNumGetMeta), atomic.LoadUint64(&xmem.counterNumSubdocGet),
			atomic.LoadUint64(&xmem.counter_tmperr), atomic.LoadUint64(&xmem.counter_eaccess),
			atomic.LoadUint64(&xmem.counterGuardrailHit), atomic.LoadUint64(&xmem.counterUnknownStatus),
			xmem.conflictLoggingEnabled(), atomic.LoadUint64(&xmem.counterNumGetEx), atomic.LoadUint64(&xmem.counterNumGetCompressed),
		)
	} else {
		xmem.Logger().Infof("%v state =%v ", xmem.Id(), xmem.State())
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

func (xmem *XmemNozzle) optimisticRep(wrappedReq *base.WrappedMCRequest) bool {
	req := wrappedReq.Req
	opcode, _ := xmem.opcodeAndSpecsForGetOp(wrappedReq)
	if opcode == mc.SUBDOC_MULTI_LOOKUP {
		// If we need to get anything from target, optimisticRep can't be used.
		return false
	}
	if req != nil {
		return uint32(req.Size()) < xmem.getOptiRepThreshold()
	}
	return true
}

func (xmem *XmemNozzle) sendHELO(setMeta bool) (utilities.HELOFeatures, error) {
	var features utilities.HELOFeatures
	features.Xattribute = true
	features.Xerror = true
	features.Collections = atomic.LoadUint32(&xmem.collectionEnabled) != 0

	if setMeta {
		// For setMeta, negotiate compression, if it is set
		features.CompressionType = xmem.compressionSetting
		return xmem.utils.SendHELOWithFeatures(xmem.client_for_setMeta.GetMemClient(), xmem.setMetaUserAgent, xmem.config.readTimeout, xmem.config.writeTimeout, features, xmem.Logger())
	}

	// For a getMeta client with conflict logging on,
	// To use the GetEx command, we will need the JSON, XATTR and SnappyEverywhere features enabled.
	if xmem.conflictLoggingEnabled() {
		features.DataType = true
		features.Xattribute = true
		features.SnappyEverywhere = true
	}
	// Since compression is value only, getMeta does not benefit from it. Use a non-compressed connection
	return xmem.utils.SendHELOWithFeatures(xmem.client_for_getMeta.GetMemClient(), xmem.getMetaUserAgent, xmem.config.readTimeout, xmem.config.writeTimeout, features, xmem.Logger())
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

/**
 * Gets the raw ioConnection (io.go ReadWriteCloser) and the number of repairs done on this connection (revisions)
 * Also sets the read/write timeout for the XmemClient (client)
 */
func (xmem *XmemNozzle) getConn(client *base.XmemClient, readTimeout bool, writeTimeout bool) (io.ReadWriteCloser, int, error) {
	err := xmem.validateRunningState()
	if err != nil {
		return nil, client.RepairCount(), err
	}
	return client.GetConn(readTimeout, writeTimeout)
}

func (xmem *XmemNozzle) validateRunningState() error {
	state := xmem.State()
	if state == common.Part_Stopping || state == common.Part_Stopped || state == common.Part_Error {
		return PartStoppedError
	}
	return nil
}

func (xmem *XmemNozzle) writeToClient(client *base.XmemClient, bytesList [][]byte, renewTimeout bool, gcList [][]byte) (err error, rev int) {
	if len(bytesList) == 0 {
		// should not happen. just to be safe
		return nil, 0
	}

	stack := &base.Stack{}
	stack.Push(bytesList)

	for {
		// provide an exit route when xmem stops
		if xmem.validateRunningState() != nil {
			return nil, 0
		}

		if stack.Empty() {
			break
		}

		// Bandwidth throttling
		curBytesList := stack.Pop().([][]byte)
		if len(curBytesList) == 0 {
			// should never get here
			continue
		}
		numberOfBytes, minNumberOfBytes, minIndex, numberOfBytesOfFirstItem := computeNumberOfBytes(curBytesList)
		bytesCanSend, bytesAllowed := xmem.bandwidthThrottler.Throttle(numberOfBytes, minNumberOfBytes, numberOfBytesOfFirstItem)

		if bytesCanSend == numberOfBytes {
			// if all the bytes can be sent, send them
			preAllocatedSlice, poolGetErr := xmem.dataPool.GetByteSlice(uint64(numberOfBytes))
			if poolGetErr != nil {
				xmem.RaiseEvent(common.NewEvent(common.DataPoolGetFail, 1, xmem, nil, nil))
				preAllocatedSlice = nil
			} else {
				gcList = append(gcList, preAllocatedSlice)
			}
			flattenedBytes := base.FlattenBytesList(curBytesList, numberOfBytes, preAllocatedSlice)
			err, rev = xmem.writeToClientWithoutThrottling(client, flattenedBytes, renewTimeout)
			if err != nil {
				return
			}
			continue
		}

		if bytesCanSend == minNumberOfBytes {
			// send minNumberOfBytes
			preAllocatedSlice, poolGetErr := xmem.dataPool.GetByteSlice(uint64(minNumberOfBytes))
			if poolGetErr != nil {
				xmem.RaiseEvent(common.NewEvent(common.DataPoolGetFail, 1, xmem, nil, nil))
				preAllocatedSlice = nil
			} else {
				gcList = append(gcList, preAllocatedSlice)
			}
			flattenedBytes := base.FlattenBytesList(curBytesList[:minIndex+1], minNumberOfBytes, preAllocatedSlice)
			err, rev = xmem.writeToClientWithoutThrottling(client, flattenedBytes, renewTimeout)
			if err != nil {
				return
			}

			curBytesList = curBytesList[minIndex+1:]
		} else if bytesCanSend == numberOfBytesOfFirstItem {
			// send numberOfBytesOfFirstItem
			preAllocatedSlice, poolGetErr := xmem.dataPool.GetByteSlice(uint64(numberOfBytesOfFirstItem))
			if poolGetErr != nil {
				xmem.RaiseEvent(common.NewEvent(common.DataPoolGetFail, 1, xmem, nil, nil))
				preAllocatedSlice = nil
			} else {
				gcList = append(gcList, preAllocatedSlice)
			}
			flattenedBytes := base.FlattenBytesList(curBytesList[:1], numberOfBytesOfFirstItem, preAllocatedSlice)
			err, rev = xmem.writeToClientWithoutThrottling(client, flattenedBytes, renewTimeout)
			if err != nil {
				return
			}

			curBytesList = curBytesList[1:]
		}

		if len(curBytesList) == 0 {
			continue
		}

		// construct a subset of the bytes that fit in bytesAllowed
		splitIndex := computeNumberOfBytesUsingBytesAllowed(curBytesList, bytesAllowed)
		if splitIndex < 0 {
			// cannot even send a single item
			// push curBytesList back to the stack to be retried at a later time
			stack.Push(curBytesList)
			start_time := time.Now()

			// We do not error out when wait() fails. We simply return without raising the event
			// We let the nozzle shutdown mechanism take care of gracefull cleanup
			err2 := xmem.bandwidthThrottler.Wait()
			if err2 != nil {
				xmem.Logger().Warnf("%s bandwidth throttler failed on wait: %v", xmem.Id(), err2)
				return
			}
			xmem.RaiseEvent(common.NewEvent(common.DataThrottled, nil, xmem, nil, time.Since(start_time)))
		} else {
			// split the input into two pieces
			if splitIndex < len(curBytesList)-1 {
				// push second part of input first so that it will be processed AFTER the first part
				stack.Push(curBytesList[splitIndex+1:])
			}
			stack.Push(curBytesList[:splitIndex+1])
		}
	}

	return
}

func computeNumberOfBytes(bytesList [][]byte) (numberOfBytes, minNumberOfBytes int64, minIndex int, numberOfBytesOfFirstItem int64) {
	numberOfBytesOfFirstItem = int64(len(bytesList[0]))

	for _, bytes := range bytesList {
		numberOfBytes += int64(len(bytes))
	}

	if len(bytesList) == 1 {
		// if there is only one item, set minNumberOfBytes = numberOfBytes
		minNumberOfBytes = numberOfBytes
		minIndex = 0
		return
	}

	targetMinNumberOfBytes := numberOfBytes * int64(base.PercentageOfBytesToSendAsMin) / 100
	var curNumberOfBytes int64 = 0
	minIndex = -1
	for index, bytes := range bytesList {
		curLen := int64(len(bytes))
		if curNumberOfBytes+curLen <= targetMinNumberOfBytes {
			curNumberOfBytes += curLen
			minIndex = index
		} else {
			// time to stop
			break
		}
	}

	if minIndex >= 0 {
		minNumberOfBytes = curNumberOfBytes
	} else {
		// this can happen if the first item has a size that is bigger than targetMinNumberOfBytes
		// include only the first item in minNumberOfBytes
		minNumberOfBytes = int64(len(bytesList[0]))
		minIndex = 0
	}

	return
}

func computeNumberOfBytesUsingBytesAllowed(bytesList [][]byte, bytesAllowed int64) (splitIndex int) {
	if bytesAllowed <= 0 {
		return -1
	}

	splitIndex = -1
	var splitBytes int64 = 0
	for index, bytes := range bytesList {
		splitBytes += int64(len(bytes))
		if splitBytes <= bytesAllowed {
			splitIndex = index
			continue
		} else {
			break
		}
	}
	return
}

func (xmem *XmemNozzle) writeToClientWithoutThrottling(client *base.XmemClient, bytes []byte, renewTimeout bool) (error, int) {
	time.Sleep(client.GetBackoffTime(false))

	conn, rev, err := xmem.getConn(client, false, renewTimeout)
	if err != nil {
		return err, rev
	}

	n, err := conn.Write(bytes)

	if err == nil {
		client.ReportOpSuccess()
		return err, rev
	} else {
		xmem.Logger().Errorf("%v writeToClient error: %s\n", xmem.Id(), fmt.Sprint(err))

		// repair connection if
		// 1. received serious net error like connection closed
		// or 2. sent incomplete data to target
		if xmem.utils.IsSeriousNetError(err) || (n != 0 && n != len(bytes)) {
			xmem.repairConn(client, err.Error(), rev)

		} else if base.IsNetError(err) {
			client.ReportOpFailure(false)
		}

		return err, rev
	}
}

func (xmem *XmemNozzle) readFromClient(client *base.XmemClient, resetReadTimeout bool) (*mc.MCResponse, error, int) {

	// Gets a connection and also set whether or not to reset the readTimeOut
	_, rev, err := xmem.getConn(client, resetReadTimeout, false)
	if err != nil {
		client.Logger().Errorf("%v Can't read from client %v, failed to get connection, err=%v", xmem.Id(), client.Name(), err)
		return nil, err, rev
	}

	memClient := client.GetMemClient()
	if memClient == nil {
		return nil, errors.New("memcached client is not set"), client.RepairCount()
	}
	response, err := memClient.Receive()

	if err != nil {
		isAppErr := false
		var errMsg string = ""
		if err == response {
			isAppErr = true
		} else {
			errMsg = err.Error()
		}

		if !isAppErr {
			if err == io.EOF {
				return nil, base.ConnectionClosedError, rev
			} else if xmem.utils.IsSeriousNetError(err) {
				return nil, base.BadConnectionError, rev
			} else if base.IsNetError(err) {
				client.ReportOpFailure(true)
				return response, err, rev
			} else if strings.HasPrefix(errMsg, "bad magic") {
				//the connection to couchbase server is ruined. it is not supposed to return response with bad magic number
				//now the only sensible thing to do is to repair the connection, then retry
				client.Logger().Warnf("%v err=%v\n", xmem.Id(), err)
				return nil, base.BadConnectionError, rev
			}
		} else {
			//response.Status != SUCCESSFUL, in this case, gomemcached return the response as err as well
			//return the err as nil so that caller can differentiate the application error from transport
			//error
			return response, nil, rev
		}
	} else {
		//if no error, reset the client retry counter
		client.ReportOpSuccess()
	}
	return response, err, rev
}

func (xmem *XmemNozzle) repairConn(client *base.XmemClient, reason string, rev int) error {

	if xmem.validateRunningState() != nil {
		xmem.Logger().Infof("%v is not running, no need to repairConn", xmem.Id())
		return nil
	}

	xmem.Logger().Errorf("%v connection %v is broken due to %v, try to repair...\n", xmem.Id(), client.Name(), reason)
	pool, err := xmem.getConnPool()
	if err != nil {
		xmem.handleGeneralError(err)
		return err
	}

	memClient, err := xmem.getClientWithRetry(xmem.Id(), pool, xmem.finish_ch, false /*initializing*/, xmem.Logger())
	if err != nil {
		xmem.handleGeneralError(err)
		xmem.Logger().Errorf("%v - Failed to repair connections for %v. err=%v\n", xmem.Id(), client.Name(), err)
		return err
	}

	repaired := client.RepairConn(memClient, rev, xmem.Id(), xmem.finish_ch)
	if repaired {
		var features utilities.HELOFeatures
		if client == xmem.client_for_setMeta {
			features, err = xmem.sendHELO(true /*setMeta*/)
			if err != nil {
				xmem.handleGeneralError(err)
				xmem.Logger().Errorf("%v - Failed to repair connections for %v. err=%v\n", xmem.Id(), client.Name(), err)
				return err
			}

			err = xmem.validateFeatures(features, true)
			if err != nil {
				xmem.handleGeneralError(err)
				return err
			}

			xmem.xattrEnabled = features.Xattribute
			if resetErr := xmem.onSetMetaConnRepaired(); resetErr != nil {
				xmem.Logger().Warnf("%v - onSetMetaConnReparied for %v err %v", xmem.Id(), client.Name(), resetErr)
			}
		} else {
			features, err = xmem.sendHELO(false /*setMeta*/)
			if err != nil {
				xmem.handleGeneralError(err)
				xmem.Logger().Errorf("%v - Failed to repair connections for %v. err=%v\n", xmem.Id(), client.Name(), err)
				return err
			}

			err = xmem.validateFeatures(features, false)
			if err != nil {
				xmem.handleGeneralError(err)
				return err
			}
		}
		xmem.Logger().Infof("%v - The connection for %v has been repaired\n", xmem.Id(), client.Name())
	}
	return nil
}

func (xmem *XmemNozzle) onSetMetaConnRepaired() error {
	size := xmem.buf.bufferSize()
	count := 0
	for i := 0; i < int(size); i++ {
		sent, err := xmem.buf.modSlot(uint16(i), xmem.resendWithReset)
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

func (xmem *XmemNozzle) UpdateSettings(settings metadata.ReplicationSettingsMap) error {
	devMainDelay, ok := settings[XMEM_DEV_MAIN_SLEEP_DELAY]
	if ok {
		devMainDelayInt := devMainDelay.(int)
		oldMainDelayInt := int(atomic.LoadUint32(&xmem.config.devMainSendDelay))
		atomic.StoreUint32(&xmem.config.devMainSendDelay, uint32(devMainDelayInt))
		if oldMainDelayInt != devMainDelayInt {
			xmem.Logger().Infof("%v updated main pipeline delay to %v millisecond(s)\n", xmem.Id(), devMainDelayInt)
		}
	}
	devBackfillDelay, ok := settings[XMEM_DEV_BACKFILL_SLEEP_DELAY]
	if ok {
		devBackfillDelayInt := devBackfillDelay.(int)
		oldBackfillDelayInt := int(atomic.LoadUint32(&xmem.config.devBackfillSendDelay))
		atomic.StoreUint32(&xmem.config.devBackfillSendDelay, uint32(devBackfillDelayInt))
		if oldBackfillDelayInt != devBackfillDelayInt {
			xmem.Logger().Infof("%v updated backfill pipeline delay to %v millisecond(s)\n", xmem.Id(), devBackfillDelay)
		}
	}
	optimisticReplicationThreshold, ok := settings[SETTING_OPTI_REP_THRESHOLD]
	if ok {
		optimisticReplicationThresholdInt := optimisticReplicationThreshold.(int)
		oldOptimisticInt := int(atomic.LoadUint32(&xmem.config.optiRepThreshold))
		atomic.StoreUint32(&xmem.config.optiRepThreshold, uint32(optimisticReplicationThresholdInt))
		if oldOptimisticInt != optimisticReplicationThresholdInt {
			xmem.Logger().Infof("%v updated optimistic replication threshold to %v\n", xmem.Id(), optimisticReplicationThresholdInt)
		}
	}
	hlvPruningWindow, ok := settings[HLV_PRUNING_WINDOW]
	if ok {
		hlvPruningWindowInt := hlvPruningWindow.(int)
		oldvPruningWindowInt := int(atomic.LoadUint32(&xmem.config.hlvPruningWindowSec))
		atomic.StoreUint32(&xmem.config.hlvPruningWindowSec, uint32(hlvPruningWindowInt))
		if oldvPruningWindowInt != hlvPruningWindowInt {
			xmem.Logger().Infof("%v updated %v to %v\n", xmem.Id(), HLV_PRUNING_WINDOW, hlvPruningWindowInt)
		}
	}

	return nil
}

func (xmem *XmemNozzle) dataChanControl() {
	if xmem.bytesInDataChan() < base.XmemMaxDataChanSize {
		select {
		case xmem.dataChan_control <- true:
		default:
			//dataChan_control is already flagged.
		}
	}
}

func (xmem *XmemNozzle) writeToDataChan(item *base.WrappedMCRequest) error {
	select {
	case <-xmem.dataChan_control:
		select {
		case xmem.dataChan <- item:
			atomic.AddInt32(&xmem.bytes_in_dataChan, int32(item.Size))
			xmem.dataChanControl()
			return nil
		// provides an alternative exit path when xmem stops
		case <-xmem.finish_ch:
			return PartStoppedError
		}
	case <-xmem.finish_ch:
		return PartStoppedError
	}
}

/**
 * Gets a MC request from the data channel, and tickles data channel controller
 */
func (xmem *XmemNozzle) readFromDataChan() (*base.WrappedMCRequest, error) {
	select {
	case item := <-xmem.dataChan:
		atomic.AddInt32(&xmem.bytes_in_dataChan, int32(0-item.Size))
		xmem.dataChanControl()
		return item, nil
	case <-xmem.finish_ch:
		return nil, PartStoppedError
	}
}

func (xmem *XmemNozzle) bytesInDataChan() int {
	return int(atomic.LoadInt32(&xmem.bytes_in_dataChan))
}

func (xmem *XmemNozzle) RecycleDataObj(incomingReq interface{}) {
	req, ok := incomingReq.(*base.WrappedMCRequest)
	if ok {
		xmem.recycleDataObj(req)
	}
}

func (xmem *XmemNozzle) recycleDataObj(req *base.WrappedMCRequest) {
	req.SlicesToBeReleasedMtx.Lock()
	if req.SlicesToBeReleasedByXmem != nil {
		for _, aSlice := range req.SlicesToBeReleasedByXmem {
			xmem.dataPool.PutByteSlice(aSlice)
		}
		req.SlicesToBeReleasedByXmem = nil
	}
	req.SlicesToBeReleasedMtx.Unlock()
	if xmem.upstreamObjRecycler != nil {
		xmem.upstreamObjRecycler(req)
	}
}

func (xmem *XmemNozzle) getLastTenBatchSize() string {
	xmem.last_ten_batches_size_lock.RLock()
	defer xmem.last_ten_batches_size_lock.RUnlock()
	var buffer bytes.Buffer
	buffer.WriteByte('[')
	first := true
	for _, batch_size := range xmem.last_ten_batches_size {
		if !first {
			buffer.WriteByte(',')
		} else {
			first = false
		}
		buffer.WriteString(fmt.Sprintf("%v", batch_size))
	}
	buffer.WriteByte(']')
	return buffer.String()
}

func (xmem *XmemNozzle) recordBatchSize(batchSize uint32) {
	xmem.last_ten_batches_size_lock.Lock()
	defer xmem.last_ten_batches_size_lock.Unlock()

	for i := 9; i > 0; i-- {
		xmem.last_ten_batches_size[i] = xmem.last_ten_batches_size[i-1]
	}
	xmem.last_ten_batches_size[0] = batchSize
}

func (xmem *XmemNozzle) getClientWithRetry(xmem_id string, pool base.ConnPool, finish_ch chan bool, initializing bool, logger *log.CommonLogger) (mcc.ClientIface, error) {
	getClientOpFunc := func(param interface{}) (interface{}, error) {
		// first verify whether target bucket is still valid
		targetClusterRef, err := xmem.remoteClusterSvc.RemoteClusterByUuid(xmem.targetClusterUuid, false /*refresh*/)
		if err != nil {
			return nil, err
		}
		err = xmem.utils.VerifyTargetBucket(xmem.config.bucketName, xmem.targetBucketUuid, targetClusterRef, logger)
		if err != nil {
			// err could be target bucket missing or recreated, or something else
			// skip creating client anyway
			// this will ensure that client will not be created against incorrect target bucket
			return nil, err
		}

		sanInCertificate := xmem.config.san_in_certificate
		if !initializing && xmem.config.encryptionType == metadata.EncryptionType_Full {
			connStr, err := targetClusterRef.MyConnectionStr()
			if err != nil {
				return nil, err
			}
			// hostAddr not used in full encryption mode
			_, _, _, err = xmem.utils.GetSecuritySettingsAndDefaultPoolInfo("" /*hostAddr*/, connStr,
				xmem.config.username, xmem.config.password, xmem.config.certificate, xmem.config.clientCertificate,
				xmem.config.clientKey, false /*scramShaEnabled*/, logger)
			if err != nil {
				return nil, err
			}
		}
		return pool.GetNew(sanInCertificate)
	}

	result, err := xmem.utils.ExponentialBackoffExecutorWithFinishSignal("xmem.getClientWithRetry", base.XmemBackoffTimeNewConn, base.XmemMaxRetryNewConn,
		base.MetaKvBackoffFactor, getClientOpFunc, nil /*param*/, finish_ch)
	if err != nil {
		high_level_err := fmt.Sprintf("Failed to set up connections to target cluster after %v retries.", base.XmemMaxRetryNewConn)
		logger.Errorf("%v %v", xmem_id, high_level_err)
		return nil, errors.New(high_level_err)
	}
	client, ok := result.(mcc.ClientIface)
	if !ok {
		// should never get here
		return nil, fmt.Errorf("%v getClientWithRetry returned wrong type of client", xmem_id)
	}
	return client, nil
}

func (xmem *XmemNozzle) ResponsibleVBs() []uint16 {
	return xmem.vbList
}

// Should only be done during pipeline construction
func (xmem *XmemNozzle) SetUpstreamObjRecycler(recycler func(interface{})) {
	xmem.upstreamObjRecycler = recycler
}

// Should only be done during pipeline construction
func (xmem *XmemNozzle) SetUpstreamErrReporter(reporter func(interface{})) {
	xmem.upstreamErrReporter = reporter
}

// Should only be done during pipeline construction
func (xmem *XmemNozzle) SetConflictManager(conflictMgr service_def.ConflictManagerIface) {
	xmem.conflictMgr = conflictMgr
}

func (xmem *XmemNozzle) checkSendDelayInjection() {
	if atomic.LoadUint32(&xmem.config.devBackfillSendDelay) > 0 {
		if _, pipelineType := common.DecomposeFullTopic(xmem.topic); pipelineType == common.BackfillPipeline {
			time.Sleep(time.Duration(atomic.LoadUint32(&xmem.config.devBackfillSendDelay)) * time.Millisecond)
		}
	}
	if atomic.LoadUint32(&xmem.config.devMainSendDelay) > 0 {
		if _, pipelineType := common.DecomposeFullTopic(xmem.topic); pipelineType == common.MainPipeline {
			time.Sleep(time.Duration(atomic.LoadUint32(&xmem.config.devMainSendDelay)) * time.Millisecond)
		}
	}
}

func (xmem *XmemNozzle) getMobileCompatible() int {
	return int(atomic.LoadUint32(&xmem.config.mobileCompatible))
}

func (xmem *XmemNozzle) getCrossClusterVers() bool {
	// No need for atomic load since we restart pipeline if this changes.
	return xmem.config.crossClusterVers
}

const (
	errorMapErrKey  = "errors"
	errorMapNameKey = "name"
	errorMapDescKey = "desc"
)

func (xmem *XmemNozzle) PrintResponseStatusError(status mc.Status) string {
	if xmem.memcachedErrMap == nil {
		return fmt.Sprintf("response status=%v", status)
	}

	errorsLookupTable, ok := xmem.memcachedErrMap[errorMapErrKey].(map[string]interface{})
	if !ok {
		return fmt.Sprintf("response status=%v", status)
	}

	statusCodeStr := strconv.FormatUint(uint64(status), 16)
	errDetail, ok := errorsLookupTable[statusCodeStr].(map[string]interface{})
	if !ok {
		return fmt.Sprintf("response status=%v", status)
	}

	return fmt.Sprintf("response errCode: %v errName: %v errDesc: %v ", statusCodeStr,
		errDetail[errorMapNameKey], errDetail[errorMapDescKey])
}
