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
	"fmt"
	mc "github.com/couchbase/gomemcached"
	base "github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"time"
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

	STATS_QUEUE_SIZE       = "queue_size"
	STATS_QUEUE_SIZE_BYTES = "queue_size_bytes"
	EVENT_ADDI_DOC_KEY     = "doc_key"
	EVENT_ADDI_SEQNO       = "source_seqno"
	EVENT_ADDI_OPT_REPD    = "optimistic_replicated"
	EVENT_ADDI_SETMETA_COMMIT_TIME = "setmeta_commit_time"
	EVENT_ADDI_GETMETA_COMMIT_TIME = "getmeta_commit_time"
)

type DataObjRecycler func (topic string, dataObj *base.WrappedMCRequest)

/************************************
/* struct baseConfig
*************************************/
type baseConfig struct {
	maxCount         int
	maxSize          int
	optiRepThreshold int
	maxRetryInterval time.Duration
	//the max time that a batch can wait in the queue
	batchExpirationTime time.Duration
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
	maxIdleCount       int
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
}

func (doc_meta documentMetadata) uniqueKey() string {
	return fmt.Sprintf("%s_%v", doc_meta.key, doc_meta.revSeq)
}

// does not return error since the assumption is that settings have been validated prior
func (config *baseConfig) initializeConfig(settings map[string]interface{}) {
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
	if val, ok := settings[SETTING_BATCH_EXPIRATION_TIME]; ok {
		config.batchExpirationTime = val.(time.Duration)
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
		config.optiRepThreshold = val.(int)
	}

}

/************************************
/* struct dataBatch
*************************************/
type dataBatch struct {
	// the document whose size is larger than optimistic replication threshold
	// key of the map is the document key
	bigDoc_map map[string]*base.WrappedMCRequest
	// the big docs that failed conflict resolution and do not need to be replicated
	// key of the map is the document key_revSeqno
	// the bool value in the map does not matter - the presence of a key does
	bigDoc_noRep_map  map[string]bool
	curCount          int
	curSize           int
	capacity_count    int
	capacity_size     int
	start_time        time.Time
	frozen            bool
	logger            *log.CommonLogger
	expiring_duration time.Duration
	expire_ch         <-chan time.Time
	expiration_set    bool
}

func newBatch(cap_count int, cap_size int, expiring_duration time.Duration, logger *log.CommonLogger) *dataBatch {
	return &dataBatch{
		curCount:          0,
		curSize:           0,
		capacity_count:    cap_count,
		capacity_size:     cap_size,
		expiring_duration: expiring_duration,
		bigDoc_map:        make(map[string]*base.WrappedMCRequest),
		bigDoc_noRep_map:  make(map[string]bool),
		expiration_set:    false,
		logger:            logger}
}

func (b *dataBatch) accumuBatch(req *base.WrappedMCRequest, classifyFunc func(req *mc.MCRequest) bool) bool {
	var ret bool = true

	if req != nil && req.Req != nil {
		size := req.Req.Size()

		b.curCount++
		if !b.expiration_set {
			b.start_time = time.Now()
			b.expire_ch = time.After(b.expiring_duration)
			b.expiration_set = true
		}
		if !classifyFunc(req.Req) {
			docMetadata := decodeSetMetaReq(req.Req)
			b.bigDoc_map[docMetadata.uniqueKey()] = req
		}
		b.curSize += size
		if b.curCount < b.capacity_count && b.curSize < b.capacity_size*1000 {
			ret = false
		}
	}
	return ret
}

func (b *dataBatch) count() int {
	return b.curCount
}

func (b *dataBatch) size() int {
	return b.curSize

}

func needSend(req *mc.MCRequest, batch *dataBatch, logger *log.CommonLogger) bool {
	if req == nil {
		logger.Info("req is null, not need to send")
		return false
	} else {
		docMetadata := decodeSetMetaReq(req)
		_, ok := batch.bigDoc_noRep_map[docMetadata.uniqueKey()]
		return !ok
	}
}

func decodeSetMetaReq(req *mc.MCRequest) documentMetadata {
	ret := documentMetadata{}
	ret.key = req.Key
	ret.flags = binary.BigEndian.Uint32(req.Extras[0:4])
	ret.expiry = binary.BigEndian.Uint32(req.Extras[4:8])
	ret.revSeq = binary.BigEndian.Uint64(req.Extras[8:16])
	ret.cas = req.Cas
	ret.deletion = (req.Opcode == base.DELETE_WITH_META)
	return ret
}

// TODO more common functions, e.g., data queuing and batch processing,
// may be refectored into a base class, BatchedNozzle

/************************************
/* struct BatchedNozzle
*************************************/
/*type BatchedNozzle struct {
}*/
