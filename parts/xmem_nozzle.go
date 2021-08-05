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
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	base "github.com/couchbase/goxdcr/base"
	common "github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	utilities "github.com/couchbase/goxdcr/utils"
	"github.com/golang/snappy"
)

//configuration settings for XmemNozzle
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

// Return values from conflict detection
// When detecting/resolving conflicts between source and target document, all action are taken
// when the source cluster has larger CAS.
type ConflictResult uint32

const (
	TargetDominate     ConflictResult = iota // Source CAS is smaller. Skip
	SourceDominate     ConflictResult = iota // Source CAS is larger and it contains target. Send
	Conflict           ConflictResult = iota // Source CAS is larger and they don't contain each other. Merge
	TargetSetBack      ConflictResult = iota // Source CAS is larger but target dominates source MV. Set target back to source
	SourceEqualsTarget ConflictResult = iota
)

type RequestToResponse struct {
	req  *base.WrappedMCRequest
	resp *mc.MCResponse
}

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
	XMEM_SETTING_ENCRYPTION_TYPE:    base.NewSettingDef(reflect.TypeOf((*string)(nil)), false),
	XMEM_SETTING_CERTIFICATE:        base.NewSettingDef(reflect.TypeOf((*[]byte)(nil)), false),
	XMEM_SETTING_SAN_IN_CERITICATE:  base.NewSettingDef(reflect.TypeOf((*bool)(nil)), false),
	XMEM_SETTING_INSECURESKIPVERIFY: base.NewSettingDef(reflect.TypeOf((*bool)(nil)), false),
}

var UninitializedReseverationNumber = -1

func GetErrorXmemTargetUnknownCollection(mcr *base.WrappedMCRequest) error {
	return fmt.Errorf("%v scope %v collection %v given known manifest ID %v", base.StringTargetCollectionMappingErr,
		mcr.GetSourceCollectionNamespace().ScopeName, mcr.GetSourceCollectionNamespace().CollectionName, mcr.GetManifestId())
}

var ErrorXmemIsStuck = errors.New("Xmem is stuck")

var ErrorBufferInvalidState = errors.New("xmem buffer is in invalid state")

type ConflictResolver func(doc_metadata_source documentMetadata, doc_metadata_target documentMetadata, source_cr_mode base.ConflictResolutionMode, xattrEnabled bool, logger *log.CommonLogger) bool

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
	lock             sync.RWMutex
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

//modSlot allow caller to do book-keeping on the slot, like updating num_of_retry
//@pos - the position of the slot
//@modFunc - the callback function which is going to update the slot
func (buf *requestBuffer) modSlot(pos uint16, modFunc func(req *bufferedMCRequest, p uint16) (bool, error)) (bool, error) {
	err := buf.validatePos(pos)
	if err != nil {
		return false, err
	}

	req := buf.slots[pos]
	return modFunc(req, pos)
}

//evictSlot allow caller to empty the slot
//@pos - the position of the slot
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

func (buf *requestBuffer) enSlot(mcreq *base.WrappedMCRequest, isCustomCR bool) (uint16, int, []byte) {
	index := <-buf.empty_slots_pos

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
	req.req = mcreq
	buf.adjustRequest(mcreq, index, isCustomCR)
	item_bytes := mcreq.Req.Bytes()
	now := time.Now()
	req.sent_time = &now
	buf.token_ch <- 1

	//increase the occupied_count
	atomic.AddInt32(&buf.occupied_count, 1)

	return index, reservation_num, item_bytes
}

// always called with lock on buf.slots[index]. no need for separate lock on buf.sequences[index]
func (buf *requestBuffer) adjustRequest(req *base.WrappedMCRequest, index uint16, isCustomCR bool) {
	mc_req := req.Req
	mc_req.Opcode = encodeOpCode(mc_req, isCustomCR)
	// For Custom CR, Cas contains expected CR for Cas locking
	if isCustomCR == false {
		mc_req.Cas = 0
	}
	mc_req.Opaque = base.GetOpaque(index, buf.sequences[int(index)])
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
	}
	return err
}

/************************************
/* struct opaque_KeySeqnoMap
*************************************/
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

/************************************
/* struct XmemNozzle
*************************************/
type XmemNozzle struct {
	AbstractPart

	//remote cluster reference for retrieving up to date remote cluster reference
	remoteClusterSvc  service_def.RemoteClusterSvc
	sourceClusterUuid string // used to calculate sourceClusterId for custom CR
	targetClusterUuid string
	targetBucketUuid  string

	// Source and target cluster ID. Used for custom CR in XATTR _xdcr to identify source of changes.
	// For DP, they are the cluster UUID encoded in base64. For test we can set it to any unique string.
	sourceClusterId []byte
	targetClusterId []byte

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
	counter_batches             int64
	start_time                  time.Time
	counter_resend              uint64
	// counter of times LOCKED status is returned by KV
	counter_locked uint64

	receive_token_ch chan int

	connType base.ConnType

	topic                      string
	last_ready_batch           int32
	last_ten_batches_size      []uint32
	last_ten_batches_size_lock sync.RWMutex
	last_batch_id              int32

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
}

func NewXmemNozzle(id string,
	remoteClusterSvc service_def.RemoteClusterSvc,
	sourceClusterUuid string,
	targetClusterUuid string,
	topic string,
	connPoolNamePrefix string,
	connPoolConnSize int,
	connectString string,
	sourceBucketName string,
	targetBucketName string,
	targetBucketUuid string,
	username string,
	password string,
	source_cr_mode base.ConflictResolutionMode,
	logger_context *log.LoggerContext,
	utilsIn utilities.UtilsIface,
	vbList []uint16) *XmemNozzle {

	part := NewAbstractPartWithLogger(id, log.NewLogger("XmemNozzle", logger_context))

	xmem := &XmemNozzle{AbstractPart: part,
		remoteClusterSvc:    remoteClusterSvc,
		sourceClusterUuid:   sourceClusterUuid,
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
		counter_batches:     0,
		topic:               topic,
		source_cr_mode:      source_cr_mode,
		sourceBucketName:    sourceBucketName,
		targetBucketUuid:    targetBucketUuid,
		utils:               utilsIn,
		vbList:              vbList,
		collectionEnabled:   1, /*Default to true unless otherwise disabled*/
	}

	xmem.last_ten_batches_size = []uint32{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

	//set conflict resolver
	xmem.conflict_resolver = resolveConflict

	xmem.config.connectStr = connectString
	xmem.config.bucketName = targetBucketName
	xmem.config.username = username
	xmem.config.password = password
	xmem.config.connPoolNamePrefix = connPoolNamePrefix
	xmem.config.connPoolSize = connPoolConnSize

	if source_cr_mode == base.CRMode_Custom {
		xmem.dataPool = base.NewDataPool()
	}
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
		xmem.Logger().Warnf("%v accumuBatch received error %v parsing the XATTR to determine if the change is from target for custom CR, req.UniqueKey=%v%s%v",
			xmem.Id(), err, base.UdTagBegin, request.UniqueKey, base.UdTagEnd)
	} else if fromTarget {
		// Change is from target. Don't replicate back
		additionalInfo := TargetDataSkippedEventAdditional{Seqno: request.Seqno,
			Opcode:      encodeOpCode(request.Req, xmem.source_cr_mode == base.CRMode_Custom),
			IsExpirySet: (binary.BigEndian.Uint32(request.Req.Extras[4:8]) != 0),
			VBucket:     request.Req.VBucket,
			ManifestId:  request.GetManifestId(),
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
		case batch := <-xmem.batches_ready_queue:
			if xmem.validateRunningState() != nil {
				xmem.Logger().Infof("%v has stopped.", xmem.Id())
				goto done
			}

			var noRep_map map[string]NeedSendStatus
			if xmem.source_cr_mode != base.CRMode_Custom {
				//batch get meta to find what needs to be not sent via the noRep map
				noRep_map, err = xmem.batchGetMeta(batch.getMeta_map)
				if err != nil {
					xmem.Logger().Errorf("%v batchGetMeta failed. err=%v\n", xmem.Id(), err)
				} else {
					batch.bigDoc_noRep_map = noRep_map
				}
			} else {
				// From Xattr, we get a map of what should not be replicated, and a map to get target
				// document for further process
				noRep_map, getDoc_map, err := xmem.batchGetXattrForCustomCR(batch.getMeta_map)
				if err != nil {
					if err == PartStoppedError {
						goto done
					}
					xmem.handleGeneralError(err)
				}
				if len(getDoc_map) > 0 {
					batch.cr_resp_map, err = xmem.batchGetDocForCustomCR(getDoc_map, noRep_map)
					if err != nil {
						if err == PartStoppedError {
							goto done
						}
						xmem.handleGeneralError(err)
					}
				} else {
					batch.cr_resp_map = nil
				}
				batch.bigDoc_noRep_map = noRep_map
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
			atomic.AddUint64(&xmem.counter_waittime, uint64(time.Since(item.Start_time).Seconds()*1000))
			needSendStatus, err := needSend(item, batch, xmem.Logger())
			if err != nil {
				return err
			}
			if needSendStatus == Send {

				err = xmem.preprocessMCRequest(item)
				if err != nil {
					return err
				}

				//blocking
				index, reserv_num, item_bytes := xmem.buf.enSlot(item, xmem.source_cr_mode == base.CRMode_Custom)

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
					reqs_bytes = [][]byte{}
					index_reservation_list = make([][]uint16, base.XmemMaxBatchSize+1)
				}
			} else if needSendStatus == Not_Send_Detecting {
				xmem.Logger().Errorf("%v document %v%s%v has a unexpected Not_Send_Detecting status after conflict detection has finished", xmem.Id(),
					base.UdTagBegin, bytes.Trim(item.Req.Key, "\x00"), base.UdTagEnd)
				panic("Unexpected needSendStatus")
			} else if needSendStatus == To_Resolve {
				// Call conflictMgr to resolve conflict
				atomic.AddUint64(&xmem.counter_to_resolve, 1)
				lookupResp := batch.cr_resp_map[item.UniqueKey]
				if lookupResp == nil {
					panic(fmt.Sprintf("No response for key %v", item.UniqueKey))
				}
				err = xmem.conflictMgr.ResolveConflict(item, lookupResp, xmem.sourceClusterId, xmem.targetClusterId, xmem.recycleDataObj)
				if err != nil {
					return err
				}
			} else if needSendStatus == To_Setback {
				atomic.AddUint64(&xmem.counter_to_setback, 1)
				lookupResp := batch.cr_resp_map[item.UniqueKey]
				if lookupResp == nil {
					panic(fmt.Sprintf("No response for key %v", item.UniqueKey))
				}
				// This is the case where target has smaller CAS but it dominates source MV.
				err = xmem.conflictMgr.SetBackToSource(item, lookupResp, xmem.sourceClusterId, xmem.targetClusterId, xmem.recycleDataObj)
				if err != nil {
					return err
				}
			} else {
				if needSendStatus == Not_Send_Failed_CR {
					//lost on conflict resolution on source side
					// this still counts as data sent
					additionalInfo := DataFailedCRSourceEventAdditional{Seqno: item.Seqno,
						Opcode:      encodeOpCode(item.Req, xmem.source_cr_mode == base.CRMode_Custom),
						IsExpirySet: (binary.BigEndian.Uint32(item.Req.Extras[4:8]) != 0),
						VBucket:     item.Req.VBucket,
						ManifestId:  item.GetManifestId(),
					}
					xmem.RaiseEvent(common.NewEvent(common.DataFailedCRSource, nil, xmem, nil, additionalInfo))
				}
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

func (xmem *XmemNozzle) preprocessMCRequest(req *base.WrappedMCRequest) error {
	mc_req := req.Req

	contains_xattr := base.HasXattr(mc_req.DataType)
	if contains_xattr && !xmem.xattrEnabled {
		// if request contains xattr and xattr is not enabled in the memcached connection, strip xattr off the request

		// strip the xattr bit off data type
		mc_req.DataType = mc_req.DataType & ^(base.PROTOCOL_BINARY_DATATYPE_XATTR)
		// strip xattr off the mutation body
		if len(mc_req.Body) < 4 {
			xmem.Logger().Errorf("%v mutation body is too short to store xattr. key=%v%v%v, body=%v%v%v", xmem.Id(), base.UdTagBegin, string(mc_req.Key), base.UdTagEnd,
				base.UdTagBegin, mc_req.Body, base.UdTagEnd)
			return fmt.Errorf("%v mutation body is too short to store xattr.", xmem.Id())
		}
		// the first four bytes in body stored the length of the xattr fields
		xattr_length := binary.BigEndian.Uint32(mc_req.Body[0:4])
		mc_req.Body = mc_req.Body[xattr_length+4:]
	}

	// Memcached does not like it when any other flags are in there. Purge and re-do
	if mc_req.DataType > 0 {
		if xmem.compressionSetting == base.CompressionTypeSnappy {
			mc_req.DataType &= (base.PROTOCOL_BINARY_DATATYPE_XATTR | base.SnappyDataType)
		} else {
			mc_req.DataType &= base.PROTOCOL_BINARY_DATATYPE_XATTR
		}
	}
	if xmem.source_cr_mode == base.CRMode_Custom {
		err := xmem.updateCustomCRXattrForTarget(req)
		if err != nil {
			return err
		}
		// Compress it if needed
		if mc_req.DataType&mcc.SnappyDataType == 0 {
			maxEncodedLen := snappy.MaxEncodedLen(len(mc_req.Body))
			if maxEncodedLen > 0 && maxEncodedLen < len(mc_req.Body) {
				req.SlicesToBeReleasedMtx.RLock()
				if req.SlicesToBeReleased != nil {
					req.SlicesToBeReleasedMtx.RUnlock()
					req.SlicesToBeReleasedMtx.Lock()
					req.SlicesToBeReleased = make([][]byte, 0, 1)
					req.SlicesToBeReleasedMtx.Unlock()
					req.SlicesToBeReleasedMtx.RLock()
				}
				req.SlicesToBeReleasedMtx.RUnlock()
				body, err := xmem.dataPool.GetByteSlice(uint64(maxEncodedLen))
				if err != nil {
					body = make([]byte, maxEncodedLen)
					xmem.RaiseEvent(common.NewEvent(common.DataPoolGetFail, 1, xmem, nil, nil))
				} else {
					req.SlicesToBeReleasedMtx.Lock()
					req.SlicesToBeReleased = append(req.SlicesToBeReleased, body)
					req.SlicesToBeReleasedMtx.Unlock()
				}
				body = snappy.Encode(body, mc_req.Body)
				mc_req.Body = body
				mc_req.DataType = mc_req.DataType | mcc.SnappyDataType
			}
		}
	}
	return nil
}

//return true if doc_meta_source win; false otherwise
func resolveConflict(doc_meta_source documentMetadata, doc_meta_target documentMetadata,
	source_cr_mode base.ConflictResolutionMode, xattrEnabled bool, logger *log.CommonLogger) bool {
	if source_cr_mode == base.CRMode_LWW {
		return resolveConflictByCAS(doc_meta_source, doc_meta_target, xattrEnabled, logger)
	} else {
		return resolveConflictByRevSeq(doc_meta_source, doc_meta_target, xattrEnabled, logger)
	}
}

func resolveConflictByCAS(doc_meta_source documentMetadata,
	doc_meta_target documentMetadata, xattrEnabled bool, logger *log.CommonLogger) bool {
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
				if doc_meta_target.flags > doc_meta_source.flags {
					ret = false
				} else if doc_meta_target.flags == doc_meta_source.flags {
					ret = resolveConflictByXattr(doc_meta_source, doc_meta_target, xattrEnabled)
				}
			}
		}
	}
	return ret
}

func resolveConflictByRevSeq(doc_meta_source documentMetadata,
	doc_meta_target documentMetadata, xattrEnabled bool, logger *log.CommonLogger) bool {
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
				if doc_meta_target.flags > doc_meta_source.flags {
					ret = false
				} else if doc_meta_target.flags == doc_meta_source.flags {
					ret = resolveConflictByXattr(doc_meta_source, doc_meta_target, xattrEnabled)
				}
			}
		}
	}
	return ret
}

// if all other metadata fields are equal, use xattr field to decide whether source mutations should win
func resolveConflictByXattr(doc_meta_source documentMetadata,
	doc_meta_target documentMetadata, xattrEnabled bool) bool {
	if xattrEnabled {
		// if target is xattr enabled, source mutation has xattr, and target mutation does not have xattr
		// let source mutation win
		source_has_xattr := base.HasXattr(doc_meta_source.dataType)
		target_has_xattr := base.HasXattr(doc_meta_target.dataType)
		return source_has_xattr && !target_has_xattr
	} else {
		// if target is not xattr enabled, target mutation always does not have xattr
		// do not have let source mutation win even if source mutation has xattr,
		// otherwise source mutations need to be repeatly re-sent in backfill mode
		return false
	}
}

func (xmem *XmemNozzle) sendWithRetry(client *base.XmemClient, numOfRetry int, item_byte [][]byte) error {

	var err error
	for j := 0; j < numOfRetry; j++ {
		err, rev := xmem.writeToClient(client, item_byte, true)
		if err == nil {
			return nil
		} else if err == base.BadConnectionError {
			xmem.repairConn(client, err.Error(), rev)
		}
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
			xmem.Logger().Errorf("%v %v. err=%v", xmem.Id(), high_level_err, err)
			xmem.handleGeneralError(errors.New(high_level_err))
		}
	}
	return err
}

// Launched as a part of the batchGetMeta, batchGetXattrForCustomCR and batchGetDocForCustomCR,
// which will fire off the requests and this is the handler to decrypt the info coming back
func (xmem *XmemNozzle) batchGetMetaHandler(count int, finch chan bool, return_ch chan bool,
	opaque_keySeqno_map opaqueKeySeqnoMap, respMap base.MCResponseMap, logger *log.CommonLogger, isGetMeta bool, includeDoc bool) {
	var batchGetStr string
	if isGetMeta {
		batchGetStr = "batchGetMeta"
	} else {
		batchGetStr = "batchGet"
	}
	defer func() {
		//handle the panic gracefully.
		if r := recover(); r != nil {
			if xmem.validateRunningState() == nil {
				errMsg := fmt.Sprintf("%v", r)
				//add to the error list
				xmem.Logger().Errorf("%v %v receiver recovered from err = %v", xmem.Id(), batchGetStr, errMsg)

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
					logger.Errorf("%v %v received fatal error and had to abort. Expected %v responses, got %v responses. err=%v", xmem.Id(), batchGetStr, count, len(respMap), err)
					logger.Infof("%v Expected=%v, Received=%v\n", xmem.Id(), opaque_keySeqno_map.CloneAndRedact(), base.UdTagBegin, respMap, base.UdTagEnd)
				} else {
					logger.Errorf("%v %v timed out. Expected %v responses, got %v responses", xmem.Id(), batchGetStr, count, len(respMap))
					logger.Infof("%v Expected=%v, Received=%v\n", xmem.Id(), opaque_keySeqno_map.CloneAndRedact(), base.UdTagBegin, respMap, base.UdTagEnd)
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
					manifestId, ok4 := keySeqno[4].(uint64)
					if ok1 && ok2 && ok3 && ok4 {
						respMap[key] = response

						// GetMeta successful means that the target manifest ID is valid for the collection ID of this key
						additionalInfo := GetReceivedEventAdditional{Key: key,
							Seqno:       seqno, // field is used only for CAPI nozzle
							Commit_time: time.Since(start_time),
							ManifestId:  manifestId,
						}
						var receivedEvent common.ComponentEventType
						if !isGetMeta && includeDoc {
							receivedEvent = common.GetDocReceived
						} else {
							receivedEvent = common.GetMetaReceived
						}
						xmem.RaiseEvent(common.NewEvent(receivedEvent, nil, xmem, nil, additionalInfo))
						if response.Status != mc.SUCCESS && !base.IsIgnorableMCResponse(response, false) && !base.IsTemporaryMCError(response.Status) &&
							!base.IsCollectionMappingError(response.Status) {
							if base.IsTopologyChangeMCError(response.Status) {
								vb_err := fmt.Errorf("Received error %v on vb %v\n", base.ErrorNotMyVbucket, vbno)
								xmem.handleVBError(vbno, vb_err)
							} else if base.IsEAccessError(response.Status) {
								xmem.handleGeneralError(base.ErrorEAccess)
								return
							} else {
								// log the corresponding request to facilitate debugging
								xmem.Logger().Warnf("%v received error from getMeta client. key=%v%s%v, seqno=%v, response=%v%v%v\n", xmem.Id(), base.UdTagBegin, key, base.UdTagEnd, seqno,
									base.UdTagBegin, response, base.UdTagEnd)
								err = fmt.Errorf("error response with status %v from memcached", response.Status)
								xmem.repairConn(xmem.client_for_getMeta, err.Error(), rev)
								// no need to wait further since connection has been reset
								return
							}
						} else if base.IsTemporaryMCError(response.Status) && !isGetMeta {
							xmem.client_for_getMeta.IncrementBackOffFactor()
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
}

/**
 * batch call to memcached GetMeta command for document size larger than the optimistic threshold
 * Returns a map of all the keys that are fed in bigDoc_map, with a boolean value
 * The boolean value == true meaning that the document referred by key should *not* be replicated
 */
func (xmem *XmemNozzle) batchGetMeta(bigDoc_map base.McRequestMap) (map[string]NeedSendStatus, error) {
	bigDoc_noRep_map := make(BigDocNoRepMap)

	//if the bigDoc_map size is 0, return
	if len(bigDoc_map) == 0 {
		return bigDoc_noRep_map, nil
	}

	respMap := make(base.MCResponseMap, xmem.config.maxCount)
	opaque_keySeqno_map := make(opaqueKeySeqnoMap)
	receiver_fin_ch := make(chan bool, 1)
	receiver_return_ch := make(chan bool, 1)

	// A list (slice) of req_bytes
	reqs_bytes_list := [][][]byte{}

	// Slice of requests that have been converted to serialized bytes
	reqs_bytes := [][]byte{}
	// Counts the number of requests that will fit into each req_bytes slice
	numOfReqsInReqBytesBatch := 0

	// establish a opaque based on the current time - and since getting meta doesn't utilize buffer buckets, feed it 0
	opaque := base.GetOpaque(0, uint16(time.Now().UnixNano()))
	sent_key_map := make(map[string]bool, len(bigDoc_map))

	//de-dupe and prepare the packages
	for _, originalReq := range bigDoc_map {
		docKey := string(originalReq.Req.Key)
		if docKey == "" {
			xmem.Logger().Errorf("%v received empty docKey. unique-key= %v%s%v, req=%v%v%v, bigDoc_map=%v%v%v", xmem.Id(),
				base.UdTagBegin, docKey, base.UdTagEnd, base.UdTagBegin, originalReq.Req, base.UdTagEnd, base.UdTagBegin, bigDoc_map, base.UdTagEnd)
			return nil, fmt.Errorf("%v received empty docKey.", xmem.Id())
		}

		if _, ok := sent_key_map[docKey]; !ok {
			req := xmem.composeRequestForGetMeta(docKey, originalReq.Req.VBucket, opaque)
			// .Bytes() returns data ready to be fed over the wire
			reqs_bytes = append(reqs_bytes, req.Bytes())
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
		}
	}

	// In case there are tail ends of the batch that did not fill base.XmemMaxBatchSize, append them to the end
	if numOfReqsInReqBytesBatch > 0 {
		reqs_bytes_list = append(reqs_bytes_list, reqs_bytes)
	}

	// launch the receiver - passing channel and maps in are fine since they are "reference types"
	go xmem.batchGetMetaHandler(len(opaque_keySeqno_map), receiver_fin_ch, receiver_return_ch, opaque_keySeqno_map, respMap, xmem.Logger(), true, false)

	//send the requests
	for _, packet := range reqs_bytes_list {
		err, _ := xmem.writeToClient(xmem.client_for_getMeta, packet, true)
		if err != nil {
			//kill the receiver and return
			close(receiver_fin_ch)
			return nil, err
		}
	}

	//wait for receiver to finish
	<-receiver_return_ch

	// Parse the result once the handler has finished populating the respMap
	for _, wrappedReq := range bigDoc_map {
		key := string(wrappedReq.Req.Key)
		resp, ok := respMap[key]
		if ok && resp.Status == mc.SUCCESS {
			doc_meta_target, err := xmem.decodeGetMetaResp([]byte(key), resp)
			if err != nil {
				xmem.Logger().Warnf("%v batchGetMeta: Error decoding getMeta response for doc %v%s%v. err=%v. Skip conflict resolution and send the doc", xmem.Id(), base.UdTagBegin, key, base.UdTagEnd, err)
				continue
			}
			doc_meta_source := decodeSetMetaReq(wrappedReq.Req)
			if !xmem.conflict_resolver(doc_meta_source, doc_meta_target, xmem.source_cr_mode, xmem.xattrEnabled, xmem.Logger()) {
				if xmem.Logger().GetLogLevel() >= log.LogLevelDebug {
					docMetaSrcRedacted := doc_meta_source.CloneAndRedact()
					docMetaTgtRedacted := doc_meta_target.CloneAndRedact()
					xmem.Logger().Debugf("%v doc %v%s%v failed source side conflict resolution. source meta=%v, target meta=%v. no need to send\n", xmem.Id(), base.UdTagBegin, key, base.UdTagEnd, docMetaSrcRedacted, docMetaTgtRedacted)
				}
				bigDoc_noRep_map[wrappedReq.UniqueKey] = Not_Send_Failed_CR
			} else if xmem.Logger().GetLogLevel() >= log.LogLevelDebug {
				docMetaSrcRedacted := doc_meta_source.CloneAndRedact()
				docMetaTgtRedacted := doc_meta_target.CloneAndRedact()
				xmem.Logger().Debugf("%v doc %v%s%v succeeded source side conflict resolution. source meta=%v, target meta=%v. sending it to target\n", xmem.Id(), base.UdTagBegin, key, base.UdTagEnd, docMetaSrcRedacted, docMetaTgtRedacted)
			}
		} else if ok && base.IsTopologyChangeMCError(resp.Status) {
			bigDoc_noRep_map[wrappedReq.UniqueKey] = Not_Send_Other

		} else if ok && base.IsCollectionMappingError(resp.Status) {
			if xmem.Logger().GetLogLevel() >= log.LogLevelDebug {
				xmem.Logger().Debugf("%v batchGetMeta: doc %v%s%v could not be mapped to on target even though manifest says it exists. Skip conflict resolution and send the doc", xmem.Id(), base.UdTagBegin, key, base.UdTagEnd)
			}
		} else {
			if !ok || resp == nil {
				if xmem.Logger().GetLogLevel() >= log.LogLevelDebug {
					xmem.Logger().Debugf("%v batchGetMeta: doc %v%s%v is not found in target system, send it", xmem.Id(), base.UdTagBegin, key, base.UdTagEnd)
				}
			} else if resp.Status == mc.KEY_ENOENT {
				if xmem.Logger().GetLogLevel() >= log.LogLevelDebug {
					xmem.Logger().Debugf("%v batchGetMeta: doc %v%s%v does not exist on target. Skip conflict resolution and send the doc", xmem.Id(), base.UdTagBegin, key, base.UdTagEnd)
				}
			} else {
				xmem.Logger().Warnf("%v batchGetMeta: memcached response for doc %v%s%v has error status %v. Skip conflict resolution and send the doc", xmem.Id(), base.UdTagBegin, key, base.UdTagEnd, resp.Status)
			}
		}
	}

	if xmem.Logger().GetLogLevel() >= log.LogLevelDebug {
		xmem.Logger().Debugf("%v Done with batchGetMeta, bigDoc_noRep_map=%v\n", xmem.Id(), bigDoc_noRep_map.CloneAndRedact())
	}
	return bigDoc_noRep_map, nil
}

func (xmem *XmemNozzle) decodeGetMetaResp(key []byte, resp *mc.MCResponse) (documentMetadata, error) {
	ret := documentMetadata{}
	ret.key = key
	extras := resp.Extras
	ret.deletion = (binary.BigEndian.Uint32(extras[0:4]) != 0)
	ret.flags = binary.BigEndian.Uint32(extras[4:8])
	ret.expiry = binary.BigEndian.Uint32(extras[8:12])
	ret.revSeq = binary.BigEndian.Uint64(extras[12:20])
	ret.cas = resp.Cas
	if xmem.xattrEnabled {
		if len(extras) < 20 {
			return ret, fmt.Errorf("%v received unexpected getMeta response, which does not include data type in extras. extras=%v", xmem.Id(), extras)
		}
		ret.dataType = extras[20]
	} else {
		ret.dataType = resp.DataType
	}
	return ret, nil
}

func (xmem *XmemNozzle) decodeSubDocResp(key []byte, lookupResp *base.SubdocLookupResponse) documentMetadata {
	specs := lookupResp.Specs
	resp := lookupResp.Resp
	body := resp.Body
	if base.IsSuccessSubdocLookupResponse(resp) == false {
		return documentMetadata{}
	}
	pos := 0
	docMeta := documentMetadata{
		key:      key,
		revSeq:   0,
		cas:      resp.Cas,
		flags:    0,
		expiry:   0,
		deletion: base.IsDeletedSubdocLookupResponse(resp),
		dataType: 0,
	}
	for i := 0; i < len(specs); i++ {
		spec := specs[i]
		status := mc.Status(binary.BigEndian.Uint16(body[pos : pos+2]))
		pos = pos + 2
		xattrlen := int(binary.BigEndian.Uint32(body[pos : pos+4]))
		if pos+xattrlen > len(body) {
			// This should never happen
			xmem.Logger().Errorf("Returned value length %v for subdoc_get path %v exceeds body length", xattrlen, spec.Path)
			return documentMetadata{}
		}
		pos = pos + 4
		value := string(body[pos : pos+xattrlen])
		if status == mc.SUCCESS {
			switch string(spec.Path) {
			case base.VXATTR_REVID:
				if xattrlen < 3 {
					// This should never happen
					xmem.Logger().Errorf("Unexpected return value length %v for subdoc_get path %v", xattrlen, base.VXATTR_REVID)
					return documentMetadata{}
				}
				// KV returns $document.revid as a string. So skip the quotes in the return value
				if revid, err := strconv.ParseUint(value[1:xattrlen-1], 10, 64); err == nil {
					docMeta.revSeq = uint64(revid)
				}
			case base.VXATTR_DATATYPE:
				if isJson, err := regexp.Match(base.JsonDataTypeStr, body[pos:pos+xattrlen]); err == nil && isJson {
					docMeta.dataType = docMeta.dataType | base.JSONDataType
				}
				if isSnappy, err := regexp.Match(base.SnappyDataTypeStr, body[pos:pos+xattrlen]); err == nil && isSnappy {
					docMeta.dataType = docMeta.dataType | base.SnappyDataType
				}
				if isXattr, err := regexp.Match(base.XattrDataTypeStr, body[pos:pos+xattrlen]); err == nil && isXattr {
					docMeta.dataType = docMeta.dataType | base.XattrDataType
				}
			case base.VXATTR_EXPIRY:
				if expiry, err := strconv.ParseUint(value, 10, 32); err == nil {
					docMeta.expiry = uint32(expiry)
				}
			case base.VXATTR_FLAGS:
				if flag, err := strconv.ParseUint(value, 10, 32); err == nil {
					docMeta.flags = uint32(flag)
				}
			}
		}
		pos = pos + xattrlen
	}
	return docMeta
}

func (xmem *XmemNozzle) composeRequestForGetMeta(key string, vb uint16, opaque uint32) *mc.MCRequest {
	req := &mc.MCRequest{VBucket: vb,
		Key:    []byte(key),
		Opaque: opaque,
		Opcode: base.GET_WITH_META}

	// if xattr is enabled, request that data type be included in getMeta response
	// Not needed for compression since GetMeta connection is to not use compression
	if xmem.xattrEnabled {
		req.Extras = make([]byte, 1)
		req.Extras[0] = byte(base.ReqExtMetaDataType)
	}
	return req
}

func (xmem *XmemNozzle) sendBatchGetRequest(get_map base.McRequestMap, retry int, include_doc bool) (respMap base.MCResponseMap, specs []base.SubdocLookupPathSpec, err error) {
	// if input size is 0, then we are done
	if len(get_map) == 0 {
		return nil, nil, nil
	}

	respMap = make(base.MCResponseMap, xmem.config.maxCount)

	opaque_keySeqno_map := make(opaqueKeySeqnoMap)
	receiver_fin_ch := make(chan bool, 1)
	receiver_return_ch := make(chan bool, 1)

	// A list (slice) of req_bytes
	reqs_bytes_list := [][][]byte{}

	// Slice of requests that have been converted to serialized bytes
	reqs_bytes := [][]byte{}
	// Counts the number of requests that will fit into each req_bytes slice
	numOfReqsInReqBytesBatch := 0

	// Establish an opaque based on the current time - and since getting doc doesn't utilize buffer buckets, feed it 0
	opaque := base.GetOpaque(0, uint16(time.Now().UnixNano()))
	sent_key_map := make(map[string]bool, len(get_map))

	// This is the spec for the lookup paths
	specs = xmem.composeSpecForSubdocGet(include_doc)

	// Add each req in reqs_bytes_list and set sent_key_map for each doc key
	for _, originalReq := range get_map {
		docKey := string(originalReq.Req.Key)
		if docKey == "" {
			xmem.Logger().Errorf("%v received empty docKey. unique-key= %v%s%v, req=%v%v%v, getMeta_map=%v%v%v", xmem.Id(),
				base.UdTagBegin, originalReq.Req.Key, base.UdTagEnd, base.UdTagBegin, originalReq.Req, base.UdTagEnd, base.UdTagBegin, get_map, base.UdTagEnd)
			return respMap, specs, errors.New(xmem.Id() + " received empty docKey.")
		}

		if _, ok := sent_key_map[docKey]; !ok {
			req := xmem.composeRequestForSubdocGet(specs, docKey, originalReq.Req.VBucket, opaque)
			// .Bytes() returns data ready to be fed over the wire
			reqs_bytes = append(reqs_bytes, req.Bytes())
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
		}
	}

	// In case there are tail ends of the batch that did not fill base.XmemMaxBatchSize, append them to the end
	if numOfReqsInReqBytesBatch > 0 {
		reqs_bytes_list = append(reqs_bytes_list, reqs_bytes)
	}

	// launch the receiver - passing channel and maps in are fine since they are "reference types"
	go xmem.batchGetMetaHandler(len(opaque_keySeqno_map), receiver_fin_ch, receiver_return_ch, opaque_keySeqno_map, respMap, xmem.Logger(), false /* isGetMeta */, include_doc)

	//send the requests
	for _, packet := range reqs_bytes_list {
		err = xmem.sendWithRetry(xmem.client_for_getMeta, retry, packet)
		if err != nil {
			// kill the receiver and return
			close(receiver_fin_ch)
			return
		}
	}

	//wait for receiver to finish
	<-receiver_return_ch

	return
}

/**
 * Subdoc batch call to get XATTR for custom conflict resolution.
 * There is no target site custom CR so we will retry in case of failure.
 * This routine returns noRep_Map which contains the mutations that should not be replicated,
 * and getDoc_map which contains the mutations that need target documents for further process.
 */
func (xmem *XmemNozzle) batchGetXattrForCustomCR(getMeta_map base.McRequestMap) (noRep_map map[string]NeedSendStatus, getDoc_map base.McRequestMap, err error) {
	noRep_map = make(map[string]NeedSendStatus)
	getDoc_map = make(base.McRequestMap)
	var hasTmpErr bool
	for i := 0; i < xmem.config.maxRetry || hasTmpErr; i++ {
		err = xmem.validateRunningState()
		if err != nil {
			return nil, nil, err
		}
		hasTmpErr = false
		respMap, specs, err := xmem.sendBatchGetRequest(getMeta_map, xmem.config.maxRetry, false /* include_doc */)
		if err != nil {
			// Log the error. We will retry maxRetry times.
			xmem.Logger().Errorf("sentBatchGetRequest returned error '%v'. Retry number %v", err, i)
		}
		// Process the response the handler received
		keys_to_be_deleted := make(map[string]bool)
		for uniqueKey, wrappedReq := range getMeta_map {
			key := string(wrappedReq.Req.Key)
			resp, ok := respMap[key]
			if ok && (resp.Status == mc.KEY_ENOENT || base.IsSuccessSubdocLookupResponse(resp)) {
				var res ConflictResult
				res, err = xmem.detectConflictWithXattr(wrappedReq.Req, &base.SubdocLookupResponse{specs, resp})
				if err != nil {
					// Log the error. We will retry
					xmem.Logger().Errorf("%v Custom CR: '%v'", xmem.Id(), err)
					continue
				}
				switch res {
				case SourceDominate:
					// Set the expected CAS value at target
					if resp.Status == mc.KEY_ENOENT {
						wrappedReq.Req.Cas = 0
					} else {
						wrappedReq.Req.Cas = resp.Cas
					}
				case SourceEqualsTarget:
					noRep_map[uniqueKey] = Not_Send_Failed_CR
				case TargetDominate:
					noRep_map[uniqueKey] = Not_Send_Failed_CR
				case Conflict:
					noRep_map[uniqueKey] = Not_Send_Detecting
					getDoc_map[uniqueKey] = wrappedReq
				case TargetSetBack:
					noRep_map[uniqueKey] = Not_Send_Detecting
					getDoc_map[uniqueKey] = wrappedReq
				}
				keys_to_be_deleted[uniqueKey] = true
			} else if resp != nil {
				if base.IsTemporaryMCError(resp.Status) {
					hasTmpErr = true
				}
				if xmem.Logger().GetLogLevel() >= log.LogLevelDebug {
					xmem.Logger().Debugf("Received response status %v for key %v", resp.Status, key)
				}
			}
		}
		if len(keys_to_be_deleted) == len(getMeta_map) {
			// Got response for all
			return noRep_map, getDoc_map, nil
		} else {
			for uniqueKey, _ := range keys_to_be_deleted {
				delete(getMeta_map, uniqueKey)
			}
		}
	}
	if len(getMeta_map) > 0 {
		err = errors.New(fmt.Sprintf("Failed to get XATTR from target for %v documents after %v retries", len(getMeta_map), xmem.config.maxRetry))
	}
	return
}

func (xmem *XmemNozzle) composeSpecForSubdocGet(include_doc bool) (specs []base.SubdocLookupPathSpec) {
	specLen := 5
	if include_doc {
		specLen++
	}
	specs = make([]base.SubdocLookupPathSpec, 0, specLen)
	// _xdcr XATTR for CCR
	spec := base.SubdocLookupPathSpec{mc.SUBDOC_GET, mc.SUBDOC_FLAG_XATTR_PATH, []byte(base.XATTR_XDCR)}
	specs = append(specs, spec)
	// $document.revid
	spec = base.SubdocLookupPathSpec{mc.SUBDOC_GET, mc.SUBDOC_FLAG_XATTR_PATH, []byte(base.VXATTR_REVID)}
	specs = append(specs, spec)
	// $document.flags
	spec = base.SubdocLookupPathSpec{mc.SUBDOC_GET, mc.SUBDOC_FLAG_XATTR_PATH, []byte(base.VXATTR_FLAGS)}
	specs = append(specs, spec)
	spec = base.SubdocLookupPathSpec{mc.SUBDOC_GET, mc.SUBDOC_FLAG_XATTR_PATH, []byte(base.VXATTR_EXPIRY)}
	specs = append(specs, spec)
	// $document.datatype.
	spec = base.SubdocLookupPathSpec{mc.SUBDOC_GET, mc.SUBDOC_FLAG_XATTR_PATH, []byte(base.VXATTR_DATATYPE)}
	specs = append(specs, spec)

	if include_doc {
		spec = base.SubdocLookupPathSpec{base.GET, 0, nil}
		specs = append(specs, spec)
	}
	return specs
}

// Request to get _xdcr XATTR. If document body is included, it must be the last path
func (xmem *XmemNozzle) composeRequestForSubdocGet(specs []base.SubdocLookupPathSpec, key string, vb uint16, opaque uint32) *mc.MCRequest {
	bodylen := 0
	for i := 0; i < len(specs); i++ {
		bodylen = bodylen + specs[i].Size()
	}
	body := make([]byte, bodylen)

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

	return &mc.MCRequest{
		VBucket: vb,
		Key:     []byte(key),
		Opaque:  opaque,
		Extras:  []byte{mc.SUBDOC_FLAG_ACCESS_DELETED},
		Body:    body,
		Opcode:  mc.SUBDOC_MULTI_LOOKUP,
	}
}

/**
 * batch call to memcached subdoc_get command for documents that needs target document for custom CR
 */
func (xmem *XmemNozzle) batchGetDocForCustomCR(getDoc_map base.McRequestMap, noRep_map map[string]NeedSendStatus) (lookUpRespMap map[string]*base.SubdocLookupResponse, err error) {
	var respMap base.MCResponseMap
	var specs []base.SubdocLookupPathSpec
	var hasTmpErr bool
	lookUpRespMap = make(map[string]*base.SubdocLookupResponse)
	for i := 0; i < xmem.config.maxRetry || hasTmpErr; i++ {
		err = xmem.validateRunningState()
		if err != nil {
			return nil, err
		}
		hasTmpErr = false
		respMap, specs, err = xmem.sendBatchGetRequest(getDoc_map, xmem.config.maxRetry, true /* include_doc */)
		if err != nil {
			xmem.Logger().Errorf(err.Error())
		}
		// Process the response the handler received
		keys_to_be_deleted := make(map[string]bool)
		for uniqueKey, wrappedReq := range getDoc_map {
			key := string(wrappedReq.Req.Key)
			resp, ok := respMap[key]
			if ok && (base.IsSuccessSubdocLookupResponse(resp) || resp.Status == mc.KEY_ENOENT) {
				var res ConflictResult
				lookupResp := &base.SubdocLookupResponse{specs, resp}
				res, err = xmem.detectConflictWithXattr(wrappedReq.Req, lookupResp)
				if err != nil {
					// Log the error. We will retry
					xmem.Logger().Errorf("%v Custom CR: '%v'", xmem.Id(), err)
					continue
				}
				switch res {
				case SourceDominate:
					// We are here because we have a conflict based on XATTR. Now the same source dominates. Maybe target had a rollback?
					delete(noRep_map, uniqueKey)
					if resp.Status == mc.KEY_ENOENT {
						wrappedReq.Req.Cas = 0
					} else {
						wrappedReq.Req.Cas = resp.Cas
					}
				case SourceEqualsTarget:
					noRep_map[uniqueKey] = Not_Send_Failed_CR
				case TargetDominate:
					noRep_map[uniqueKey] = Not_Send_Failed_CR
				case Conflict:
					noRep_map[uniqueKey] = To_Resolve
					lookUpRespMap[uniqueKey] = lookupResp
				case TargetSetBack:
					noRep_map[uniqueKey] = To_Setback
					lookUpRespMap[uniqueKey] = lookupResp
				}
				keys_to_be_deleted[uniqueKey] = true
			} else if resp != nil {
				if base.IsTemporaryMCError(resp.Status) {
					hasTmpErr = true
				}
				if xmem.Logger().GetLogLevel() >= log.LogLevelDebug {
					xmem.Logger().Debugf("Received response status %v for key %v", resp.Status, key)
				}
			}
		}
		if len(keys_to_be_deleted) == len(getDoc_map) {
			// Got response for all
			return lookUpRespMap, nil
		} else {
			for uniqueKey, _ := range keys_to_be_deleted {
				delete(getDoc_map, uniqueKey)
			}
		}
	}
	if len(getDoc_map) > 0 {
		err = errors.New(fmt.Sprintf("Failed to get XATTR and document body from target for %v documents after %v retries", len(getDoc_map), xmem.config.maxRetry))
	}
	return
}

// This is called after getting the target document based on the lookup spec. We should have both source and target XATTRs
// If target doesn't exist (target.Status == KEY_ENOENT), return SourceDominate
func (xmem *XmemNozzle) detectConflictWithXattr(source *mc.MCRequest, lookupResp *base.SubdocLookupResponse) (ConflictResult, error) {
	target := lookupResp.Resp
	if target.Status == mc.KEY_ENOENT {
		// Target doesn't exist
		return SourceDominate, nil
	}
	targetMeta, err := lookupResp.FindTargetCustomCRXattr(xmem.targetClusterId)
	if err != nil {
		return TargetDominate, err
	}
	sourceMeta, err := base.FindSourceCustomCRXattr(source, xmem.sourceClusterId)
	if err != nil {
		xmem.Logger().Errorf("FindSourceCustomCRXattr returned %v for key %s, meta=%v", err, bytes.Trim(source.Key, "\x00"), sourceMeta)
		panic("FindSourceCustomCRXattr error")
		return TargetDominate, err
	}
	/*
	 * Source has smaller CAS
	 */
	if sourceMeta.GetCas() < targetMeta.GetCas() {
		// We can only replicate from larger CAS to smaller
		return TargetDominate, nil
	}

	isTargetJson, err := lookupResp.IsTargetJson()
	if err != nil {
		return TargetDominate, err
	}

	/*
	 * USE LWW for non-json or deleted docs.
	 */
	if source.DataType&mcc.JSONDataType == 0 || isTargetJson == false ||
		base.IsDeletedSubdocLookupResponse(target) || encodeOpCode(source, true) == base.DELETE_WITH_META {
		doc_meta_source := decodeSetMetaReq(source)
		doc_meta_target := xmem.decodeSubDocResp(source.Key, lookupResp)
		if xmem.conflict_resolver(doc_meta_source, doc_meta_target, base.CRMode_LWW, xmem.xattrEnabled, xmem.Logger()) {
			return SourceDominate, nil
		} else {
			return TargetDominate, nil
		}
	}

	/*
	 * Check if they are the same document.
	 */
	if sourceMeta.GetCas() == targetMeta.GetCas() && bytes.Equal(sourceMeta.DocumentSourceId(), targetMeta.DocumentSourceId()) {
		// Same CAS, generated by the same cluster
		return SourceEqualsTarget, nil
	}

	/*
	 * Source CAS same or larger than target
	 */
	var sourceContainTarget bool
	var targetContainSource bool
	if targetMeta.IsMergedDoc() {
		sourceContainTarget, err = sourceMeta.ContainsVV(targetMeta.GetMv())
	} else {
		sourceContainTarget, err = sourceMeta.ContainsVersion(targetMeta.DocumentSourceId(), targetMeta.GetCas())
	}
	if err != nil {
		return Conflict, err
	}
	if sourceMeta.IsMergedDoc() {
		targetContainSource, err = targetMeta.ContainsVV(sourceMeta.GetMv())
	} else {
		targetContainSource, err = targetMeta.ContainsVersion(sourceMeta.DocumentSourceId(), sourceMeta.GetCas())
	}
	if sourceContainTarget && targetContainSource {
		if sourceMeta.GetCas() > targetMeta.GetCas() {
			// The documents are the same but we want to converge to larger CAS
			return SourceDominate, nil
		} else {
			return SourceEqualsTarget, nil
		}
	} else if sourceContainTarget {
		return SourceDominate, nil
	} else if targetContainSource {
		if sourceMeta.GetCas() > targetMeta.GetCas() {
			// Target dominate but its CAS is smaller. Needs to set back to source
			if xmem.Logger().GetLogLevel() >= log.LogLevelDebug {
				xmem.Logger().Debugf("%v key: %s, TargetSetBack: source CAS=%v, cv=%v, cvid=%s, pcas=%s, mv=%s, target CAS=%v, cv=%v, cvid=%s, pcas=%s, mv=%s",
					xmem.Id(), bytes.Trim(source.Key, "\x00"),
					sourceMeta.GetCas(), sourceMeta.GetCv(), sourceMeta.GetCvId(), sourceMeta.GetPcas(), sourceMeta.GetMv(),
					targetMeta.GetCas(), targetMeta.GetCv(), targetMeta.GetCvId(), targetMeta.GetPcas(), targetMeta.GetMv())
			}
			return TargetSetBack, nil
		} else {
			// The reverse replication direction can take care of this.
			return TargetDominate, nil
		}
	} else {
		if sourceMeta.GetCas() > targetMeta.GetCas() {
			xmem.Logger().Debugf("%v Conflict: key: %s, source CAS=%v, cv=%v, cvid=%s, pcas=%s, mv=%s, target CAS=%v, cv=%v, cvid=%s, pcas=%s, mv=%s",
				xmem.Id(), bytes.Trim(source.Key, "\x00"),
				sourceMeta.GetCas(), sourceMeta.GetCv(), sourceMeta.GetCvId(), sourceMeta.GetPcas(), sourceMeta.GetMv(),
				targetMeta.GetCas(), targetMeta.GetCv(), targetMeta.GetCvId(), targetMeta.GetPcas(), targetMeta.GetMv())
		}
		return Conflict, nil
	}
}

func (xmem *XmemNozzle) updateCustomCRXattrForTarget(wrappedReq *base.WrappedMCRequest) (err error) {
	req := wrappedReq.Req
	meta, err := base.FindSourceCustomCRXattr(req, xmem.sourceClusterId)
	if err != nil {
		xmem.Logger().Errorf("%v: FindSourceCustomCRXattr returned %v, err: %v", xmem.Id(), meta, err)
		return err
	}

	if needTo, err := meta.NeedUpdate(xmem.config.hlvPruningWindow); needTo == false || err != nil {
		return err
	}

	// Now we need to update CCR metadata either because of new changes or because we have to prune
	// The max increase in body length is adding 2 uint32 and _xdcr\x00{"id":"<clusterId>","cv":"<18bytes>"}\x00
	body := req.Body
	var maxBodyIncrease = 8 + 24 + len(xmem.sourceClusterId) + 18
	newbodyLen := len(body) + maxBodyIncrease
	wrappedReq.SlicesToBeReleasedMtx.RLock()
	if wrappedReq.SlicesToBeReleased == nil {
		wrappedReq.SlicesToBeReleasedMtx.RUnlock()
		wrappedReq.SlicesToBeReleasedMtx.Lock()
		wrappedReq.SlicesToBeReleased = make([][]byte, 0, 2)
		wrappedReq.SlicesToBeReleasedMtx.Unlock()
		wrappedReq.SlicesToBeReleasedMtx.RLock()
	}
	wrappedReq.SlicesToBeReleasedMtx.RUnlock()
	newbody, err := xmem.dataPool.GetByteSlice(uint64(newbodyLen))
	if err != nil {
		newbody = make([]byte, newbodyLen)
		xmem.RaiseEvent(common.NewEvent(common.DataPoolGetFail, 1, xmem, nil, nil))
	} else {
		wrappedReq.SlicesToBeReleasedMtx.Lock()
		wrappedReq.SlicesToBeReleased = append(wrappedReq.SlicesToBeReleased, newbody)
		wrappedReq.SlicesToBeReleasedMtx.Unlock()
	}
	pos, err := meta.ConstructCustomCRXattrForSetMeta(newbody, 4, xmem.config.hlvPruningWindow)
	if err != nil {
		// TODO (MB-44587): Remove before shipping. This should never happen unless we badly formated _xdcr.pc/_xdcr_mv..
		panic(fmt.Sprintf("%v, updateCustomCRXattrForTarget encountered error %v. This may cause unnecessary merge.", xmem.Id(), err))
	}

	if req.DataType&mcc.XattrDataType > 0 {
		it, err := base.NewXattrIterator(body)
		if err != nil {
			xmem.Logger().Errorf("%v: updateCustomCRXattrForTarget received error '%v' from NewXattrIterator for body %v", xmem.Id(), err, body)
			return err
		}
		for it.HasNext() {
			key, value, err := it.Next()
			if err != nil {
				xmem.Logger().Errorf("%v: updateCustomCRXattrForTarget received error '%v' iterating through XATTR for body %v,", xmem.Id(), err, body)
				return err
			}
			if base.Equals(key, base.XATTR_XDCR) {
				continue
			}
			binary.BigEndian.PutUint32(newbody[pos:pos+4], uint32(len(key)+len(value)+2))
			pos = pos + 4
			copy(newbody[pos:pos+len(key)], key)
			pos = pos + len(key)
			newbody[pos] = '\x00'
			pos++
			copy(newbody[pos:pos+len(value)], value)
			pos = pos + len(value)
			newbody[pos] = '\x00'
			pos++
		}
	}
	binary.BigEndian.PutUint32(newbody[0:4], uint32(pos-4))
	docWithoutXattr := base.FindSourceBodyWithoutXattr(req)
	copy(newbody[pos:], docWithoutXattr)
	pos = pos + len(docWithoutXattr)
	req.Body = newbody[0:pos]
	req.DataType = req.DataType | mcc.XattrDataType
	return nil
}

// If the change is from targetCluster, we don't need to send it
func (xmem *XmemNozzle) isChangesFromTarget(req *base.WrappedMCRequest) (bool, error) {
	if xmem.source_cr_mode != base.CRMode_Custom {
		return false, nil
	}
	if req.Req.DataType&mcc.SnappyDataType > 0 {
		req.SlicesToBeReleasedMtx.RLock()
		if req.SlicesToBeReleased == nil {
			req.SlicesToBeReleasedMtx.RUnlock()
			req.SlicesToBeReleasedMtx.Lock()
			// For target we need up to 3 byte slices, 1 to decompress, 1 for body with new XATTR, 1 to compress
			// For merge back to source, we need up to 4 byte slices, 1 to decompress, 2 to format pcas and mv, 1 for new body and XATTR
			req.SlicesToBeReleased = make([][]byte, 0, 4)
			req.SlicesToBeReleasedMtx.Unlock()
			req.SlicesToBeReleasedMtx.RLock()
		}
		req.SlicesToBeReleasedMtx.RUnlock()
		decodedLen, err := snappy.DecodedLen(req.Req.Body)
		if err != nil {
			return false, err
		}
		body, err := xmem.dataPool.GetByteSlice(uint64(decodedLen))
		if err != nil {
			body = make([]byte, decodedLen)
			xmem.RaiseEvent(common.NewEvent(common.DataPoolGetFail, 1, xmem, nil, nil))
		} else {
			req.SlicesToBeReleasedMtx.Lock()
			req.SlicesToBeReleased = append(req.SlicesToBeReleased, body)
			req.SlicesToBeReleasedMtx.Unlock()
		}
		body, err = snappy.Decode(body, req.Req.Body)
		if err != nil {
			return false, err
		}
		req.Req.Body = body
		req.Req.DataType = req.Req.DataType &^ mcc.SnappyDataType
	}
	if req.RetryCRCount == 0 {
		ccrMeta, err := base.FindSourceCustomCRXattr(req.Req, xmem.sourceClusterId)
		if err != nil {
			return false, err
		}
		if bytes.Equal(ccrMeta.DocumentSourceId(), xmem.targetClusterId) {
			return true, nil
		}
	}
	return false, nil
}

func (xmem *XmemNozzle) sendSingleSetMeta(bytesList [][]byte, numOfRetry int) error {
	var err error
	if xmem.client_for_setMeta != nil {
		for j := 0; j < numOfRetry; j++ {
			err, rev := xmem.writeToClient(xmem.client_for_setMeta, bytesList, true)
			if err == nil {
				atomic.AddUint64(&xmem.counter_resend, 1)
				return nil
			} else if err == base.BadConnectionError {
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
	// Set client immediately so that if the pipeline start times out during the next getClientWithRetry call, this client connection can be cleaned up
	xmem.setClient(base.NewXmemClient(SetMetaClientName, xmem.config.readTimeout,
		xmem.config.writeTimeout, memClient_setMeta,
		xmem.config.maxRetry, xmem.config.max_read_downtime, xmem.Logger()), true /*isSetMeta*/)

	memClient_getMeta, err := xmem.getClientWithRetry(xmem.Id(), pool, xmem.finish_ch, true /*initializing*/, xmem.Logger())
	if err != nil {
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

	err = xmem.validateFeatures(features)
	if err != nil {
		return err
	}

	// initialize this only once here
	xmem.xattrEnabled = features.Xattribute

	// No need to check features for bare boned getMeta client
	_, err = xmem.sendHELO(false /*setMeta */)
	if err != nil {
		return err
	}

	xmem.Logger().Infof("%v done with initializeConnection. %v", xmem.Id(), features.String())
	return err
}

func (xmem *XmemNozzle) validateFeatures(features utilities.HELOFeatures) error {
	if xmem.compressionSetting != features.CompressionType {
		errMsg := fmt.Sprintf("%v Attempted to send HELO with compression type: %v, but received response with %v",
			xmem.Id(), xmem.compressionSetting, features.CompressionType)
		xmem.Logger().Error(errMsg)
		if xmem.compressionSetting != base.CompressionTypeForceUncompress {
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

func (xmem *XmemNozzle) initNewBatch() {
	xmem.batch = newBatch(uint32(xmem.config.maxCount), uint32(xmem.config.maxSize), xmem.Logger())
	atomic.StoreUint32(&xmem.cur_batch_count, 0)
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
	if xmem.source_cr_mode == base.CRMode_Custom {
		if xmem.sourceClusterId, err = base.HexToBase64(xmem.sourceClusterUuid); err != nil {
			xmem.Logger().Errorf("Cannot convert source cluster %v UUID to base64. Error: %v", xmem.sourceClusterUuid, err)
			return err
		}
		if xmem.targetClusterId, err = base.HexToBase64(xmem.targetClusterUuid); err != nil {
			xmem.Logger().Errorf("Cannot convert target cluster %v UUID to base64. Error: %v", xmem.targetClusterUuid, err)
			return err
		}
		xmem.Logger().Infof("%v: Using %s and %s as source and target cluster IDs for custom conflict resolution.", xmem.Id(), xmem.sourceClusterId, xmem.targetClusterId)
	}
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
	xmem.setRequestBuffer(newReqBuffer(uint16(xmem.config.maxCount*2), uint16(float64(xmem.config.maxCount)*0.2), xmem.receive_token_ch, xmem.Logger()))

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
				} else if err == base.FatalError {
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
					goto done
				} else if err == base.BadConnectionError || err == base.ConnectionClosedError {
					xmem.Logger().Errorf("%v The connection is ruined. Repair the connection and retry.", xmem.Id())
					xmem.repairConn(xmem.client_for_setMeta, err.Error(), rev)
				}
			} else if response == nil {
				errMsg := fmt.Sprintf("%v readFromClient returned nil error and nil response. Ignoring it", xmem.Id())
				xmem.Logger().Warn(errMsg)
			} else if response.Status != mc.SUCCESS && !base.IsIgnorableMCResponse(response, xmem.source_cr_mode == base.CRMode_Custom) {
				if base.IsMutationLockedError(response.Status) {
					// if target mutation is currently locked, resend doc
					pos := xmem.getPosFromOpaque(response.Opaque)
					atomic.AddUint64(&xmem.counter_locked, 1)
					// set the mutationLocked flag on the corresponding bufferedMCRequest
					// which will allow resendIfTimeout() method to perform resend with exponential backoff with appropriate settings
					_, err = xmem.buf.modSlot(pos, xmem.setMutationLockedFlag)
				} else if base.IsTemporaryMCError(response.Status) {
					// target may be overloaded. increase backoff factor to alleviate stress on target
					xmem.client_for_setMeta.IncrementBackOffFactor()

					// error is temporary. resend doc
					pos := xmem.getPosFromOpaque(response.Opaque)
					xmem.Logger().Warnf("%v Received temporary error in setMeta response. Response status=%v, err = %v, response=%v%v%v\n", xmem.Id(), response.Status.String(), err,
						base.UdTagBegin, response, base.UdTagEnd)
					//resend and reset the retry=0 as retry is an indicator of network status,
					//here we have received the response, so reset retry=0
					_, err = xmem.buf.modSlot(pos, xmem.resendWithReset)
				} else if (base.IsEExistsError(response.Status) || base.IsENoEntError(response.Status)) && xmem.source_cr_mode == base.CRMode_Custom {
					// request failed because target Cas changed. Raise event.
					pos := xmem.getPosFromOpaque(response.Opaque)
					wrappedReq, err := xmem.buf.slot(pos)
					if err != nil {
						xmem.Logger().Errorf("%v xmem buffer is in invalid state", xmem.Id())
						xmem.handleGeneralError(ErrorBufferInvalidState)
						goto done
					}
					if wrappedReq == nil {
						// This can only happen in the following scenario:
						// 1. The request was sent but response is slow
						// 2. checkAndRepairBufferMonitor resend it
						// 3. The response for the first send comes back and the request is evicted from its slot
						// Now we are seeing the response for the resend. So we can ignore it
						if xmem.Logger().GetLogLevel() >= log.LogLevelDebug {
							xmem.Logger().Debugf("%v Response %v received but request is not in buffer.", xmem.Id(), response.Status)
						}
					} else if wrappedReq.Req.Opaque != response.Opaque {
						// This is similar to above, except the slot is already being reused so the opaque doesn't match
						if xmem.Logger().GetLogLevel() >= log.LogLevelDebug {
							xmem.Logger().Debugf("Request in the buffer for key %v%s%v has opaque=%v while the response opaque=%v",
								base.UdTagBegin, bytes.Trim(wrappedReq.Req.Key, "\x00"), base.UdTagEnd, wrappedReq.Req.Opaque, response.Opaque)
						}
					} else {
						xmem.Logger().Infof("%v Retry conflict resolution for %v%s%v because target Cas has changed (EEXISTS).", xmem.Id(), base.UdTagBegin, bytes.Trim(wrappedReq.Req.Key, "\x00"), base.UdTagEnd)
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
					pos := xmem.getPosFromOpaque(response.Opaque)
					wrappedReq, err := xmem.buf.slot(pos)
					if err == nil && wrappedReq != nil {
						req = wrappedReq.Req
						seqno = wrappedReq.Seqno
						if req != nil && req.Opaque == response.Opaque {
							// found matching request
							if base.IsTopologyChangeMCError(response.Status) {
								vb_err := fmt.Errorf("Received error %v on vb %v\n", base.ErrorNotMyVbucket, req.VBucket)
								xmem.handleVBError(req.VBucket, vb_err)
							} else if base.IsCollectionMappingError(response.Status) {
								xmem.upstreamErrReporter(wrappedReq)
								if xmem.buf.evictSlot(pos) != nil {
									panic(fmt.Sprintf("Failed to evict slot %d\n", pos))
								}
								atomic.AddUint64(&xmem.counter_ignored, 1)
							} else {
								// for other non-temporary errors, repair connections
								if response.Status == mc.XATTR_EINVAL {
									// There is something wrong with XATTR. Let's print it
									xattrlen := binary.BigEndian.Uint32(req.Body[0:4])
									ccrxattrlen := binary.BigEndian.Uint32(req.Body[4:8])
									xmem.Logger().Errorf("%v received error response %v for request with total xattr length %v, custom CR xattr length %v, body: %v%v%v", xmem.Id(), response.Status, xattrlen, ccrxattrlen, base.UdTagBegin, req.Body, base.UdTagEnd)
								}
								xmem.Logger().Errorf("%v received error response from setMeta client. Repairing connection. response status=%v, opcode=%v, seqno=%v, req.Key=%v%v%v, req.Cas=%v, req.Extras=%v\n", xmem.Id(), response.Status, response.Opcode, seqno, base.UdTagBegin, string(req.Key), base.UdTagEnd, req.Cas, req.Extras)
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
				wrappedReq, sent_time, err := xmem.buf.slotWithSentTime(pos)
				if err != nil {
					xmem.Logger().Errorf("%v xmem buffer is in invalid state", xmem.Id())
					xmem.handleGeneralError(ErrorBufferInvalidState)
					goto done
				}
				var req *mc.MCRequest
				var seqno uint64
				var committing_time time.Duration
				var resp_wait_time time.Duration
				var manifestId uint64
				if wrappedReq != nil {
					req = wrappedReq.Req
					seqno = wrappedReq.Seqno
					committing_time = time.Since(wrappedReq.Start_time)
					resp_wait_time = time.Since(*sent_time)
					manifestId = wrappedReq.GetManifestId()
				}

				if req != nil && req.Opaque == response.Opaque {
					additionalInfo := DataSentEventAdditional{Seqno: seqno,
						IsOptRepd:      xmem.optimisticRep(req),
						Opcode:         req.Opcode,
						IsExpirySet:    (binary.BigEndian.Uint32(req.Extras[4:8]) != 0),
						VBucket:        req.VBucket,
						Req_size:       req.Size(),
						Commit_time:    committing_time,
						Resp_wait_time: resp_wait_time,
						ManifestId:     manifestId,
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
						xmem.Logger().Debugf("%v Got the response, response.Opcode=%v response.Opaque=%v, req.Opaque=%v req.Status=%v\n", xmem.Id(), response.Opcode, response.Opaque, req.Opaque, response.Status)
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

func (xmem *XmemNozzle) retryAfterCasLockingFailure(req *base.WrappedMCRequest) {
	// If it is ADD_WITH_META, it needs to be SET_WITH_META in the next try because target now has the doc
	if req.Req.Opcode == base.ADD_WITH_META {
		req.Req.Opcode = base.SET_WITH_META
	}
	req.Req.Opaque = 0
	req.Req.Cas = binary.BigEndian.Uint64(req.Req.Extras[16:24])
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
				//				utils.DumpStack(xmem.Logger())
				//the connection might not be healthy, it should not go back to connection pool
				xmem.client_for_setMeta.MarkConnUnhealthy()
				xmem.client_for_getMeta.MarkConnUnhealthy()
				xmem.handleGeneralError(ErrorXmemIsStuck)
				goto done
			}
		case <-statsTicker.C:
			xmem.RaiseEvent(common.NewEvent(common.StatsUpdate, nil, xmem, nil, []int{len(xmem.dataChan), xmem.bytesInDataChan()}))
		}
	}
done:
	xmem.Logger().Infof("%v selfMonitor routine exits", xmem.Id())

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
			err = errors.New(fmt.Sprintf("%v Failed to resend document %v%s%v, has tried to resend it %v, maximum retry %v reached",
				xmem.Id(), base.UdTagBegin, bytes.Trim(req.req.Req.Key, "\x00"), base.UdTagEnd, req.num_of_retry, maxRetry))
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

			bytesList := getBytesListFromReq(req)
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
			return modified, err
		}
	}

	req.lock.Unlock()
	return false, nil
}

func getBytesListFromReq(req *bufferedMCRequest) [][]byte {
	if req.req == nil || req.req.Req == nil {
		// should not happen. just in case
		return nil
	}
	bytesList := make([][]byte, 1)
	bytesList[0] = req.req.Req.Bytes()
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

	bytesList := getBytesListFromReq(req)
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

func encodeOpCode(req *mc.MCRequest, isCustomCR bool) mc.CommandCode {
	code := req.Opcode
	if code == mc.UPR_MUTATION || code == mc.TAP_MUTATION {
		if isCustomCR == true && req.Cas == 0 {
			return base.ADD_WITH_META
		} else {
			return base.SET_WITH_META
		}
	} else if code == mc.TAP_DELETE || code == mc.UPR_DELETION || code == mc.UPR_EXPIRATION {
		return base.DELETE_WITH_META
	}
	return code
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
		xmem.Logger().Infof("%v state =%v connType=%v received %v items (%v compressed), sent %v items (%v compressed), target items skipped %v, ignored %v items, %v items waiting to confirm, %v in queue, %v in current batch, avg wait time is %vms, size of last ten batches processed %v, len(batches_ready_queue)=%v, resend=%v, locked=%v, repair_count_getMeta=%v, repair_count_setMeta=%v, retry_cr=%v, to resolve=%v, to setback=%v\n",
			xmem.Id(), xmem.State(), connType, atomic.LoadUint64(&xmem.counter_received),
			atomic.LoadUint64(&xmem.counter_compressed_received), atomic.LoadUint64(&xmem.counter_sent),
			atomic.LoadUint64(&xmem.counter_compressed_sent), atomic.LoadUint64(&xmem.counter_from_target), atomic.LoadUint64(&xmem.counter_ignored),
			xmem.buf.itemCountInBuffer(), len(xmem.dataChan),
			atomic.LoadUint32(&xmem.cur_batch_count), avg_wait_time, xmem.getLastTenBatchSize(),
			len(xmem.batches_ready_queue), atomic.LoadUint64(&xmem.counter_resend),
			atomic.LoadUint64(&xmem.counter_locked),
			xmem.client_for_getMeta.RepairCount(), xmem.client_for_setMeta.RepairCount(),
			atomic.LoadUint64(&xmem.counter_retry_cr), atomic.LoadUint64(&xmem.counter_to_resolve), atomic.LoadUint64(&xmem.counter_to_setback))
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

func (xmem *XmemNozzle) optimisticRep(req *mc.MCRequest) bool {
	if xmem.source_cr_mode == base.CRMode_Custom {
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
	} else {
		// Since compression is value only, getMeta does not benefit from it. Use a non-compressed connection
		return xmem.utils.SendHELOWithFeatures(xmem.client_for_getMeta.GetMemClient(), xmem.getMetaUserAgent, xmem.config.readTimeout, xmem.config.writeTimeout, features, xmem.Logger())
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

func (xmem *XmemNozzle) writeToClient(client *base.XmemClient, bytesList [][]byte, renewTimeout bool) (err error, rev int) {
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
			flattenedBytes := base.FlattenBytesList(curBytesList, numberOfBytes)
			err, rev = xmem.writeToClientWithoutThrottling(client, flattenedBytes, renewTimeout)
			if err != nil {
				return
			}
			continue
		}

		if bytesCanSend == minNumberOfBytes {
			// send minNumberOfBytes
			flattenedBytes := base.FlattenBytesList(curBytesList[:minIndex+1], minNumberOfBytes)
			err, rev = xmem.writeToClientWithoutThrottling(client, flattenedBytes, renewTimeout)
			if err != nil {
				return
			}

			curBytesList = curBytesList[minIndex+1:]
		} else if bytesCanSend == numberOfBytesOfFirstItem {
			// send numberOfBytesOfFirstItem
			flattenedBytes := base.FlattenBytesList(curBytesList[:1], numberOfBytesOfFirstItem)
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
			xmem.bandwidthThrottler.Wait()
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
	backoffFactor := client.GetBackOffFactor()
	if backoffFactor > 0 {
		time.Sleep(time.Duration(backoffFactor) * base.XmemBackoffWaitTime)
	}

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

			err = xmem.validateFeatures(features)
			if err != nil {
				xmem.handleGeneralError(err)
				return err
			}

			xmem.xattrEnabled = features.Xattribute
			go xmem.onSetMetaConnRepaired()
		} else {
			// No need to check features for bare-bone getMeta client
			_, err = xmem.sendHELO(false /*setMeta*/)
			if err != nil {
				xmem.handleGeneralError(err)
				xmem.Logger().Errorf("%v - Failed to repair connections for %v. err=%v\n", xmem.Id(), client.Name(), err)
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
	optimisticReplicationThreshold, ok := settings[SETTING_OPTI_REP_THRESHOLD]
	if ok {
		optimisticReplicationThresholdInt := optimisticReplicationThreshold.(int)
		atomic.StoreUint32(&xmem.config.optiRepThreshold, uint32(optimisticReplicationThresholdInt))
		xmem.Logger().Infof("%v updated optimistic replication threshold to %v\n", xmem.Id(), optimisticReplicationThresholdInt)
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
			atomic.AddInt32(&xmem.bytes_in_dataChan, int32(item.Req.Size()))
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
		atomic.AddInt32(&xmem.bytes_in_dataChan, int32(0-item.Req.Size()))
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
	if req.SlicesToBeReleased != nil {
		for _, aSlice := range req.SlicesToBeReleased {
			xmem.dataPool.PutByteSlice(aSlice)
		}
		req.SlicesToBeReleased = nil
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

func (xmem *XmemNozzle) SetUpstreamErrReporter(reporter func(interface{})) {
	xmem.upstreamErrReporter = reporter
}

func (xmem *XmemNozzle) SetConflictManager(conflictMgr service_def.ConflictManagerIface) {
	xmem.conflictMgr = conflictMgr
}
