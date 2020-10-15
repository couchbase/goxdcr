// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

// defines types and methods common to both xmem and capi nozzles
package parts

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	mc "github.com/couchbase/gomemcached"
	base "github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
)

const (
	//configuration param names
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
)

type NeedSendStatus int

const (
	Send               NeedSendStatus = iota
	Not_Send_Failed_CR NeedSendStatus = iota
	Not_Send_Detecting NeedSendStatus = iota // Still doing the conflict detection
	To_Resolve         NeedSendStatus = iota
	To_Setback         NeedSendStatus = iota
	Not_Send_Other     NeedSendStatus = iota
)

/************************************
/* struct baseConfig
*************************************/
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
	maxIdleCount       uint32
	connPoolNamePrefix string
	connPoolSize       int
	connectStr         string
	username           string
	password           string
	logger             *log.CommonLogger
}

type documentMetadata struct {
	key      []byte
	revSeq   uint64 //Item revision seqno
	cas      uint64 //Item cas
	flags    uint32 // Item flags
	expiry   uint32 // Item expiration time
	deletion bool
	dataType uint8 // item data type
}

func (doc_meta documentMetadata) String() string {
	return fmt.Sprintf("[key=%s; revSeq=%v;cas=%v;flags=%v;expiry=%v;deletion=%v:datatype=%v]", doc_meta.key, doc_meta.revSeq, doc_meta.cas, doc_meta.flags, doc_meta.expiry, doc_meta.deletion, doc_meta.dataType)
}

func (doc_meta *documentMetadata) Clone() *documentMetadata {
	var clone *documentMetadata
	if doc_meta != nil {
		clone = &documentMetadata{}
		*clone = *doc_meta
		clone.key = base.DeepCopyByteArray(doc_meta.key)
	}
	return clone
}

func (doc_meta *documentMetadata) Redact() *documentMetadata {
	if doc_meta != nil {
		if len(doc_meta.key) > 0 && !base.IsByteSliceRedacted(doc_meta.key) {
			doc_meta.key = base.TagUDBytes(doc_meta.key)
		}
	}
	return doc_meta
}

func (doc_meta *documentMetadata) CloneAndRedact() *documentMetadata {
	if doc_meta != nil {
		return doc_meta.Clone().Redact()
	}
	return doc_meta
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
	Seqno       uint64
	Opcode      mc.CommandCode
	IsExpirySet bool
	VBucket     uint16
	ManifestId  uint64
}

type TargetDataSkippedEventAdditional DataFailedCRSourceEventAdditional

type DataSentEventAdditional struct {
	Seqno          uint64
	IsOptRepd      bool
	Commit_time    time.Duration
	Resp_wait_time time.Duration
	Opcode         mc.CommandCode
	IsExpirySet    bool
	VBucket        uint16
	Req_size       int
	ManifestId     uint64
}

type DataFilteredAdditional struct {
	Key        string
	Seqno      uint64
	ManifestId uint64
}

type SentCasChangedEventAdditional struct {
	Opcode mc.CommandCode
}

// does not return error since the assumption is that settings have been validated prior
func (config *baseConfig) initializeConfig(settings metadata.ReplicationSettingsMap) {
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
	getMeta_map base.McRequestMap
	// tracks big docs that do not need to be replicated
	// key of the map is the document key_revSeqno
	// value of the map has two possible values:
	// 1. true - docs failed source side conflict resolution. in this case the docs will be counted in docs_failed_cr_source stats
	// 2. false - docs that will get rejected by target for other reasons, e.g., since target no longer owns the vbucket involved. in this case the docs will not be counted in docs_failed_cr_source stats
	bigDoc_noRep_map  map[string]NeedSendStatus
	cr_resp_map       map[string]*base.SubdocLookupResponse
	curCount          uint32
	curSize           uint32
	capacity_count    uint32
	capacity_size     uint32
	start_time        time.Time
	logger            *log.CommonLogger
	batch_nonempty_ch chan bool
	nonempty_set      bool
}

func newBatch(cap_count uint32, cap_size uint32, logger *log.CommonLogger) *dataBatch {
	return &dataBatch{
		curCount:          0,
		curSize:           0,
		capacity_count:    cap_count,
		capacity_size:     cap_size,
		getMeta_map:       make(base.McRequestMap),
		bigDoc_noRep_map:  make(map[string]NeedSendStatus),
		batch_nonempty_ch: make(chan bool),
		nonempty_set:      false,
		logger:            logger}
}

func (b *dataBatch) accumuBatch(req *base.WrappedMCRequest, classifyFunc func(req *mc.MCRequest) bool) (uint32, bool, bool, error) {
	var curCount uint32
	var isFirst bool = false
	var isFull bool = true

	if req != nil && req.Req != nil {
		size := req.Req.Size()

		curCount = b.incrementCount(1)
		if !b.nonempty_set {
			isFirst = true
			b.start_time = time.Now()
			b.nonempty_set = true
			close(b.batch_nonempty_ch)
		}
		if !classifyFunc(req.Req) {
			// If it fails the classifyFunc, then we're going to do bigDoc processing on it
			b.getMeta_map[req.UniqueKey] = req
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
// Not_Send_Failed_CR - doc does not need to be sent to target since it failed source side conflict resolution
// Not_Send_Other - doc does not need to be sent to target for other reasons, e.g., since target no longer owns the vbucket involved
func needSend(req *base.WrappedMCRequest, batch *dataBatch, logger *log.CommonLogger) (NeedSendStatus, error) {
	if req == nil || req.Req == nil {
		return Send, errors.New("needSend saw a nil req")
	}

	failedCR, ok := batch.bigDoc_noRep_map[req.UniqueKey]
	if !ok {
		return Send, nil
	} else {
		return failedCR, nil
	}
}

func decodeSetMetaReq(wrapped_req *base.WrappedMCRequest) documentMetadata {
	ret := documentMetadata{}
	req := wrapped_req.Req
	ret.key = req.Key
	ret.flags = binary.BigEndian.Uint32(req.Extras[0:4])
	ret.expiry = binary.BigEndian.Uint32(req.Extras[4:8])
	ret.revSeq = binary.BigEndian.Uint64(req.Extras[8:16])
	ret.cas = req.Cas
	ret.deletion = (req.Opcode == base.DELETE_WITH_META)
	ret.dataType = req.DataType

	return ret
}

type BigDocNoRepMap map[string]NeedSendStatus

func (norepMap BigDocNoRepMap) Clone() BigDocNoRepMap {
	var clonedMap BigDocNoRepMap = make(BigDocNoRepMap)
	for k, v := range norepMap {
		clonedMap[k] = v
	}
	return clonedMap
}

func (norepMap BigDocNoRepMap) Redact() BigDocNoRepMap {
	for k, v := range norepMap {
		// Right now, only User Data tag. In the future, need to check others
		if !base.IsStringRedacted(k) {
			norepMap[base.TagUD(k)] = v
			delete(norepMap, k)
		}
	}
	return norepMap
}

func (norepMap BigDocNoRepMap) CloneAndRedact() BigDocNoRepMap {
	var clonedMap BigDocNoRepMap = make(BigDocNoRepMap)
	for k, v := range norepMap {
		clonedMap[base.TagUD(k)] = v
	}
	return clonedMap
}
