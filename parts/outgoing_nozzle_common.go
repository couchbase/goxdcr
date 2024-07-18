// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

// defines types and methods common to both xmem and capi nozzles
package parts

import (
	"errors"
	"sync/atomic"
	"time"

	mc "github.com/couchbase/gomemcached"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
)

const (
	// Developer injection section
	XMEM_DEV_MAIN_SLEEP_DELAY     = base.DevMainPipelineSendDelay
	XMEM_DEV_BACKFILL_SLEEP_DELAY = base.DevBackfillPipelineSendDelay
	// end developer injection section

	SETTING_BATCHCOUNT            = "batch_count"
	SETTING_BATCHSIZE             = "batch_size"
	SETTING_OPTI_REP_THRESHOLD    = "optimistic_replication_threshold"
	SETTING_BATCH_EXPIRATION_TIME = "batch_expiration_time"
	SETTING_NUMOFRETRY            = "max_retry"
	SETTING_WRITE_TIMEOUT         = "write_timeout"
	SETTING_READ_TIMEOUT          = "read_timeout"
	SETTING_MAX_RETRY_INTERVAL    = "max_retry_interval"
	SETTING_SELF_MONITOR_INTERVAL = "self_monitor_interval"
	SETTING_STATS_INTERVAL        = "stats_interval"
	SETTING_COMPRESSION_TYPE      = base.CompressionTypeKey

	STATS_QUEUE_SIZE               = "queue_size"
	STATS_QUEUE_SIZE_BYTES         = "queue_size_bytes"
	EVENT_ADDI_DOC_KEY             = "doc_key"
	EVENT_ADDI_SEQNO               = "source_seqno"
	EVENT_ADDI_OPT_REPD            = "optimistic_replicated"
	EVENT_ADDI_SETMETA_COMMIT_TIME = "setmeta_commit_time"
	EVENT_ADDI_GETMETA_COMMIT_TIME = "getmeta_commit_time"
	// the following are for MCRequest related info
	EVENT_ADDI_REQ_OPCODE     = "req_opcode"
	EVENT_ADDI_REQ_VBUCKET    = "req_vbucket"
	EVENT_ADDI_REQ_EXPIRY_SET = "req_expiry_set"
	EVENT_ADDI_REQ_SIZE       = "req_size"

	HLV_PRUNING_WINDOW = "versionPruningWindow"
	HLV_ENABLE         = "hlvEnable"
	HLV_MAX_CAS        = "hlv_vb_max_cas"
	MOBILE_COMPATBILE  = base.MobileCompatibleKey
)

type NeedSendStatus int

const (
	Send              NeedSendStatus = iota
	NotSendFailedCR   NeedSendStatus = iota
	NotSendMerge      NeedSendStatus = iota
	NotSendSetback    NeedSendStatus = iota
	RetryTargetLocked NeedSendStatus = iota
	NotSendOther      NeedSendStatus = iota
)

/*
***********************************
/* struct baseConfig
************************************
*/
type baseConfig struct {
	maxCount         int
	maxSize          int
	optiRepThreshold uint32
	maxRetryInterval time.Duration
	//the write timeout for tcp connection
	writeTimeout time.Duration
	//the read timeout for tcp connection
	readTimeout time.Duration
	//the max number of retry for read\write
	maxRetry int
	//the interval on which selfMonitor would be conducted
	selfMonitorInterval time.Duration
	//the interval on which stats are collected
	statsInterval time.Duration
	//the maximum number of idle round that xmem can have
	//exceeding this number indicate the possibility of stuck
	//due to network issues
	maxIdleCount        uint32
	connPoolNamePrefix  string
	connPoolSize        int
	connectStr          string
	username            string
	password            string
	hlvPruningWindowSec uint32 // Interval for pruning PV in seconds
	crossClusterVers    bool   // Whether to send HLV when bucket is not custom CR
	vbHlvMaxCas         map[uint16]uint64
	logger              *log.CommonLogger
	mobileCompatible    uint32

	devMainSendDelay     uint32
	devBackfillSendDelay uint32
}

// We determine the "commit" time as the time we hear back from the target, for statistics purposes
// This is shared between GetDocReceived and GetMetaReceived
type GetReceivedEventAdditional struct {
	Key         string
	Seqno       uint64
	Commit_time time.Duration
	ManifestId  uint64
}

type DataFailedCRSourceEventAdditional struct {
	Seqno          uint64
	Opcode         mc.CommandCode
	IsExpirySet    bool
	VBucket        uint16
	ManifestId     uint64
	Cloned         bool
	CloneSyncCh    chan bool
	ImportMutation bool
}

type TargetDataSkippedEventAdditional DataFailedCRSourceEventAdditional

type DataSentEventAdditional struct {
	Seqno                uint64
	IsOptRepd            bool
	Commit_time          time.Duration
	Resp_wait_time       time.Duration
	Opcode               mc.CommandCode
	IsExpirySet          bool
	VBucket              uint16
	Req_size             int
	ManifestId           uint64
	FailedTargetCR       bool
	UncompressedReqSize  int
	SkippedRecompression bool
	ImportMutation       bool
	Cloned               bool
	CloneSyncCh          chan bool
}

type DataFilteredAdditional struct {
	Key             string
	Seqno           uint64
	ManifestId      uint64
	FilteringStatus base.FilteringStatusType
}

type SentCasChangedEventAdditional struct {
	Opcode mc.CommandCode
}

// does not return error since the assumption is that settings have been validated prior
func (config *baseConfig) initializeConfig(settings metadata.ReplicationSettingsMap) {
	if val, ok := settings[XMEM_DEV_MAIN_SLEEP_DELAY]; ok {
		config.devMainSendDelay = uint32(val.(int))
	}
	if val, ok := settings[XMEM_DEV_BACKFILL_SLEEP_DELAY]; ok {
		config.devBackfillSendDelay = uint32(val.(int))
	}
	if val, ok := settings[SETTING_BATCHSIZE]; ok {
		config.maxSize = val.(int)
	}
	if val, ok := settings[SETTING_BATCHCOUNT]; ok {
		config.maxCount = val.(int)
	}
	if val, ok := settings[SETTING_SELF_MONITOR_INTERVAL]; ok {
		config.selfMonitorInterval = val.(time.Duration)
	}
	if val, ok := settings[SETTING_STATS_INTERVAL]; ok {
		config.statsInterval = time.Duration(val.(int)) * time.Millisecond
	}
	if val, ok := settings[SETTING_NUMOFRETRY]; ok {
		config.maxRetry = val.(int)
	}
	if val, ok := settings[SETTING_WRITE_TIMEOUT]; ok {
		config.writeTimeout = val.(time.Duration)
	}
	if val, ok := settings[SETTING_READ_TIMEOUT]; ok {
		config.readTimeout = val.(time.Duration)
	}
	if val, ok := settings[SETTING_MAX_RETRY_INTERVAL]; ok {
		config.maxRetryInterval = val.(time.Duration)
	}
	if val, ok := settings[SETTING_OPTI_REP_THRESHOLD]; ok {
		config.optiRepThreshold = uint32(val.(int))
	}
}

/**
 * struct dataBatch
 * NOTE the decoupling between the dataBatch "metadata" and the actual data within a batch to be sent.
 * The dataBatch is considered the "metadata" of a batch that is to be sent.
 * Each individual WrappedMCRequest data is actually put into a data channel in each respective out nozzle.
 * Each individual nozzle is supposed to read from its own data channel, and match the request (using the request's
 * unique ID) to a specific member's element within the dataBatch (i.e. bigDoc_noRep_map)
 */
type dataBatch struct {
	// The documents that need target metadata for source side conflict resolution.
	// They can be documents larger than optimistic replication threshold or
	// documents that needs source side custom conflict resolution.
	// Key of the map is the document unique key
	getMetaMap base.McRequestMap
	// tracks docs that do not need to be replicated based on source side conflict resolution
	// key of the map is the document key_revSeqno
	// value of the map can be anything except Send. Anything not in the map will be sent through setWithMeta
	noRepMap map[string]NeedSendStatus
	// For CCR, noRepMap value may be Not_Send_Merge or Not_Send_Setback. For these, the target document lookup
	// response is stored in here.
	mergeLookupMap *responseLookup
	// If mobile is on, for document winning conflict resolution, we need to preserve target _sync XATTR. The lookup for these are stored in here
	sendLookupMap *responseLookup

	// XMEM config may change but only affect the next batch
	// At the beginning of each batch we will check the config to decide the getMeta/getSubdoc and setMeta behavior
	// Note that these are only needed for CCR and mobile currently. The specs will be nil otherwise. If nil, getMeta will be used.
	// These are "templated" and references in each wrapped MCRequest will be created to refer to them
	getMetaSpecWithoutHlv []base.SubdocLookupPathSpec
	getMetaSpecWithHlv    []base.SubdocLookupPathSpec
	getBodySpec           []base.SubdocLookupPathSpec // This one will get document body in addition to document metadata. Used for CCR only

	curCount          uint32
	curSize           uint32
	capacity_count    uint32
	capacity_size     uint32
	start_time        time.Time
	logger            *log.CommonLogger
	batch_nonempty_ch chan bool
	nonempty_set      bool
}

type responseLookup struct {
	// uinque-key to response mapping
	responses map[string]*base.SubdocLookupResponse
	// There may be multiple unique-keys that are refering to the same response of a given document key
	// Therefore, GC or recycle the response only when refCnt is zero
	refCnter map[*mc.MCResponse]int
}

func NewResponseLookup() *responseLookup {
	return &responseLookup{
		responses: make(map[string]*base.SubdocLookupResponse),
		refCnter:  make(map[*mc.MCResponse]int),
	}
}

// stores response and increases the reference count by 1
func (lookup *responseLookup) registerLookup(uniqueKey string, resp *base.SubdocLookupResponse) error {
	if lookup == nil {
		return base.ErrorNilPtr
	}
	if resp == nil {
		return base.ErrorNilPtr
	}

	lookup.responses[uniqueKey] = resp
	lookup.refCnter[resp.Resp]++
	return nil
}

// returns the response and decreases the reference count by 1
func (lookup *responseLookup) deregisterLookup(uniqueKey string) (*base.SubdocLookupResponse, error) {
	if lookup == nil {
		return nil, base.ErrorNilPtr
	}

	response, ok := lookup.responses[uniqueKey]
	if !ok {
		return nil, base.ErrorNilPtr
	}
	if response == nil {
		return nil, base.ErrorNilPtr
	}

	lookup.refCnter[response.Resp]--
	return response, nil
}

// only recycle if the response referenced by noone responseLookup
func (lookup *responseLookup) canRecycle(resp *mc.MCResponse) bool {
	if lookup == nil {
		return true
	}
	refCnt, ok := lookup.refCnter[resp]
	if !ok {
		// not present at all in lookup map, safe to recycle
		return true
	}
	return refCnt <= 0
}

func newBatch(cap_count uint32, cap_size uint32, logger *log.CommonLogger) *dataBatch {
	return &dataBatch{
		curCount:          0,
		curSize:           0,
		capacity_count:    cap_count,
		capacity_size:     cap_size,
		getMetaMap:        make(base.McRequestMap),
		noRepMap:          nil,
		mergeLookupMap:    nil,
		sendLookupMap:     nil,
		batch_nonempty_ch: make(chan bool),
		nonempty_set:      false,
		logger:            logger}
}

func (b *dataBatch) accumuBatch(req *base.WrappedMCRequest, classifyFunc func(req *base.WrappedMCRequest) bool) (uint32, bool, bool, error) {
	var curCount uint32
	var isFirst bool = false
	var isFull bool = true

	if req != nil && req.Req != nil {
		// When this batch is established, it establishes these specs so store it to be used in case of mutation retries
		req.GetMetaSpecWithoutHlv = b.getMetaSpecWithoutHlv
		req.GetMetaSpecWithHlv = b.getMetaSpecWithHlv
		req.GetBodySpec = b.getBodySpec

		size := req.Req.Size()

		curCount = b.incrementCount(1)
		if !b.nonempty_set {
			isFirst = true
			b.start_time = time.Now()
			b.nonempty_set = true
			close(b.batch_nonempty_ch)
		}
		if !classifyFunc(req) {
			// If it fails the classifyFunc, then we're going to do bigDoc processing on it
			b.getMetaMap[req.UniqueKey] = req
		}
		curSize := b.incrementSize(uint32(size))
		if curCount < b.capacity_count && curSize < b.capacity_size*1000 {
			isFull = false
		}

		return curCount, isFirst, isFull, nil
	}

	return curCount, isFirst, isFull, errors.New("accumuBatch saw a nil req")
}

func (b *dataBatch) count() uint32 {
	return atomic.LoadUint32(&b.curCount)
}

func (b *dataBatch) size() uint32 {
	return atomic.LoadUint32(&b.curSize)
}

func (b *dataBatch) incrementCount(delta uint32) uint32 {
	return atomic.AddUint32(&b.curCount, delta)
}

func (b *dataBatch) incrementSize(delta uint32) uint32 {
	return atomic.AddUint32(&b.curSize, delta)
}

// Given a request to be sent and the batch of requests metadata that has been pre-processed
// returns three possible values
// Send - doc needs to be sent to target
// NotSendFailedCR - doc does not need to be sent to target since it failed source side conflict resolution (target wins)
// NotSendMerge - for CCR only, the documents need to be merged
// NotSendSetBack - for CCR only, the document needs to be sent back to source cluster
// NotSendOther - doc does not need to be sent to target for other reasons, e.g., since target no longer owns the vbucket involved
func needSend(req *base.WrappedMCRequest, batch *dataBatch, logger *log.CommonLogger) (NeedSendStatus, error) {
	if req == nil || req.Req == nil {
		return Send, errors.New("needSend saw a nil req")
	}

	failedCR, ok := batch.noRepMap[req.UniqueKey]
	if !ok {
		return Send, nil
	} else {
		return failedCR, nil
	}
}

type NoRepMap map[string]NeedSendStatus

func (norepMap NoRepMap) Clone() NoRepMap {
	var clonedMap NoRepMap = make(NoRepMap)
	for k, v := range norepMap {
		clonedMap[k] = v
	}
	return clonedMap
}

func (norepMap NoRepMap) Redact() NoRepMap {
	for k, v := range norepMap {
		// Right now, only User Data tag. In the future, need to check others
		if !base.IsStringRedacted(k) {
			norepMap[base.TagUD(k)] = v
			delete(norepMap, k)
		}
	}
	return norepMap
}

func (norepMap NoRepMap) CloneAndRedact() NoRepMap {
	var clonedMap NoRepMap = make(NoRepMap)
	for k, v := range norepMap {
		clonedMap[base.TagUD(k)] = v
	}
	return clonedMap
}
