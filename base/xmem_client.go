/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/couchbase/gomemcached"

	mcc "github.com/couchbase/gomemcached/client"
	memcached "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/v8/log"
)

/*
***********************************
/* struct XmemClient
************************************
*/
var BadConnectionError = errors.New("Connection is bad")
var ConnectionClosedError = errors.New("Connection is closed")
var FatalError = errors.New("Fatal")
var repairTimeThreshold = time.Minute * 1

type XmemClient struct {
	name      string
	memClient memcached.ClientIface
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

	unknownStatusReceived map[gomemcached.Status]*time.Timer
}

func NewXmemClient(name string, read_timeout, write_timeout time.Duration,
	client mcc.ClientIface, max_continuous_failure int, max_downtime time.Duration, logger *log.CommonLogger) *XmemClient {
	logger.Infof("xmem client %v is created with read_timeout=%v, write_timeout=%v, retry_limit=%v", name, read_timeout, write_timeout, max_continuous_failure)
	return &XmemClient{name: name,
		memClient:                        client,
		continuous_write_failure_counter: 0,
		success_counter:                  0,
		logger:                           logger,
		read_timeout:                     read_timeout,
		write_timeout:                    write_timeout,
		max_downtime:                     max_downtime,
		max_continuous_write_failure:     max_continuous_failure,
		healthy:                          true,
		num_of_repairs:                   0,
		lock:                             sync.RWMutex{},
		backoff_factor:                   0,
		downtime_start:                   time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC),
		unknownStatusReceived:            map[gomemcached.Status]*time.Timer{},
	}
}

func (client *XmemClient) curWriteFailureCounter() int {
	client.lock.RLock()
	defer client.lock.RUnlock()
	return client.continuous_write_failure_counter
}

func (client *XmemClient) ReportOpFailure(readOp bool) {
	client.lock.Lock()
	defer client.lock.Unlock()

	client.success_counter = 0

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

func (client *XmemClient) ReportOpSuccess() {
	client.lock.Lock()
	defer client.lock.Unlock()

	client.downtime_start = time.Time{}
	client.continuous_write_failure_counter = 0
	if !client.hasReceivedUnknownResponse() {
		client.success_counter++
	}
	if client.success_counter > client.max_continuous_write_failure && client.backoff_factor > 0 && !client.hasReceivedUnknownResponse() {
		client.backoff_factor--
		client.success_counter = client.success_counter - client.max_continuous_write_failure
	}
	client.healthy = true
}

// The goal here is to ensure backoff occurs if XMEM continues to receive the same unknown status code
// Only if the unknown status code is no longer there after a period of time (determined by backoff period)
// will the client allow backoff to go away
func (client *XmemClient) ReportUnknownResponseReceived(unknownStatus gomemcached.Status) {
	client.lock.RLock()
	timer, ok := client.unknownStatusReceived[unknownStatus]
	client.lock.RUnlock()

	if !ok {
		// First time
		client.markUnknownStatus(unknownStatus)
	} else {
		timerHasNotExpired := timer.Reset(client.GetUnknownResponseTime(true))
		if !timerHasNotExpired {
			// Timer has already fired but hasn't had a chance to get the lock here yet
			// Corner case - just try to add another one after some sleep
			go func() {
				time.Sleep(1 * time.Second)
				client.markUnknownStatus(unknownStatus)
			}()
		}
	}
}

func (client *XmemClient) hasReceivedUnknownResponse() bool {
	return len(client.unknownStatusReceived) > 0
}

func (client *XmemClient) markUnknownStatus(unknownStatus gomemcached.Status) {
	client.lock.Lock()
	defer client.lock.Unlock()
	if _, ok := client.unknownStatusReceived[unknownStatus]; ok {
		// Someone jumped ahead already
		return
	}
	client.unknownStatusReceived[unknownStatus] = time.AfterFunc(client.GetUnknownResponseTime(false), func() {
		client.lock.Lock()
		defer client.lock.Unlock()
		delete(client.unknownStatusReceived, unknownStatus)
	})
	client.success_counter = 0 // to ensure backoff will happen and override ReportOpSuccess
}

func (client *XmemClient) isConnHealthy() bool {
	client.lock.RLock()
	defer client.lock.RUnlock()
	return client.healthy
}

func (client *XmemClient) GetMemClient() mcc.ClientIface {
	client.lock.RLock()
	defer client.lock.RUnlock()
	return client.memClient
}

/**
 * Gets the raw io ReadWriteCloser from the memClient, so that XmemClient has complete control,
 * and also sets whether or not the XmemClient will have read/write timeout as part of returning the connection
 */
func (client *XmemClient) GetConn(readTimeout bool, writeTimeout bool) (io.ReadWriteCloser, int, error) {
	client.lock.RLock()
	defer client.lock.RUnlock()

	if client.memClient == nil {
		return nil, client.num_of_repairs, errors.New("memcached client is not set")
	}

	if !client.healthy {
		return nil, client.num_of_repairs, BadConnectionError
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

func (client *XmemClient) setReadTimeout(read_timeout_duration time.Duration) {
	conn := client.memClient.Hijack()
	conn.(net.Conn).SetReadDeadline(time.Now().Add(read_timeout_duration))
}

func (client *XmemClient) setWriteTimeout(write_timeout_duration time.Duration) {
	conn := client.memClient.Hijack()
	conn.(net.Conn).SetWriteDeadline(time.Now().Add(write_timeout_duration))
}

func (client *XmemClient) RepairConn(memClient mcc.ClientIface, repair_count_at_error int, xmem_id string, finish_ch chan bool) bool {
	client.lock.Lock()
	defer client.lock.Unlock()

	if client.num_of_repairs == repair_count_at_error {
		if time.Since(client.last_failure) < repairTimeThreshold {
			client.backoff_factor++
		} else if !client.hasReceivedUnknownResponse() {
			client.backoff_factor = 0
		}

		client.memClient.Close()
		client.memClient = memClient
		client.continuous_write_failure_counter = 0
		client.num_of_repairs++
		client.last_failure = time.Now()
		client.healthy = true
		return true
	} else {
		client.logger.Infof("client %v for %v has been repaired (num_of_repairs=%v, repair_count_at_error=%v), the repair request is ignored\n", client.name, xmem_id, client.num_of_repairs, repair_count_at_error)
		return false
	}
}

func (client *XmemClient) MarkConnUnhealthy() {
	client.lock.Lock()
	defer client.lock.Unlock()
	client.healthy = false
}

func (client *XmemClient) Close() {
	client.lock.RLock()
	defer client.lock.RUnlock()
	client.memClient.Close()
}

func (client *XmemClient) RepairCount() int {
	client.lock.RLock()
	defer client.lock.RUnlock()

	return client.num_of_repairs
}

func (client *XmemClient) GetBackOffFactor() int {
	client.lock.RLock()
	defer client.lock.RUnlock()

	return client.backoff_factor
}

func (client *XmemClient) GetBackoffTime(getLock bool) time.Duration {
	if getLock {
		client.lock.RLock()
		defer client.lock.RUnlock()
	}

	if client.backoff_factor > 0 {
		return time.Duration(client.backoff_factor) * XmemBackoffWaitTime
	}
	return 0
}

// Adds 5 seconds for threshold
func (client *XmemClient) GetUnknownResponseTime(getLock bool) time.Duration {
	return client.GetBackoffTime(getLock) + 5*time.Second
}

func (client *XmemClient) IncrementBackOffFactor() {
	client.lock.Lock()
	defer client.lock.Unlock()

	if client.backoff_factor < XmemMaxBackoffFactor {
		client.backoff_factor++
	}
}

func (client *XmemClient) Name() string {
	return client.name
}
func (client *XmemClient) Logger() *log.CommonLogger {
	return client.logger
}

func (client *XmemClient) GetContinuousWriteFailureCounter() int {
	return client.continuous_write_failure_counter
}

func IsNetError(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*net.OpError)
	return ok
}

// check if memcached response status indicates ignorable error, which requires no corrective action at all
// If caslock is true, resp.Status will be KEY_EEXISTS/KEY_ENOENT if cas does not match
func IsIgnorableMCResponse(resp *gomemcached.MCResponse, caslock bool) bool {
	if resp == nil {
		return false
	}

	switch resp.Status {
	case gomemcached.KEY_ENOENT:
		if caslock {
			return false
		} else {
			return true
		}
	case gomemcached.KEY_EEXISTS:
		if caslock {
			return false
		} else {
			return true
		}
	case gomemcached.SUBDOC_BAD_MULTI: // This means at least one path failed for subdoc_multi_ commands. Only ignorable for multi_lookup.
		if resp.Opcode == gomemcached.SUBDOC_MULTI_LOOKUP {
			return true
		} else { // SUBDOC_MULTI_MUTATION
			return false
		}
	case gomemcached.SUBDOC_SUCCESS_DELETED:
		return true
	case gomemcached.CAS_VALUE_INVALID:
		return true
	case gomemcached.SUBDOC_MULTI_PATH_FAILURE_DELETED: // Same as SUBDOC_BAD_MULTI but on a deleted document
		if resp.Opcode == gomemcached.SUBDOC_MULTI_LOOKUP {
			return true
		} else { // SUBDOC_MULTI_MUTATION
			return false
		}
	}

	return false
}

// getMeta will return SUCCESS
// subdoc_multi_lookup may return the other status that are also success.
func IsSuccessGetResponse(resp *gomemcached.MCResponse) bool {
	if resp == nil {
		return false
	}
	switch resp.Status {
	case gomemcached.SUCCESS:
		return true
	case gomemcached.SUBDOC_SUCCESS_DELETED: // successfully looked up a deleted document
		return true
	case gomemcached.SUBDOC_BAD_MULTI: // the lookup is successful, some path doesn't exist
		return true
	case gomemcached.SUBDOC_MULTI_PATH_FAILURE_DELETED: // successfully looked up a deleted document, some path doesn't exist
		return true
	}
	return false
}

func IsDeletedSubdocLookupResponse(resp *gomemcached.MCResponse) bool {
	if resp == nil {
		return false
	}
	switch resp.Status {
	case gomemcached.SUBDOC_SUCCESS_DELETED: // successfully looked up a deleted document
		return true
	case gomemcached.SUBDOC_MULTI_PATH_FAILURE_DELETED: // successfully looked up a deleted document, some path doesn't exist
		return true
	}
	return false
}

// check if memcached response status indicates mutation locked error, which requires resending the corresponding request
func IsMutationLockedError(resp_status gomemcached.Status) bool {
	switch resp_status {
	case gomemcached.LOCKED:
		return true
	default:
		return false
	}
}

// check if memcached response status indicates error of temporary nature, which requires retrying corresponding requests
func IsTemporaryMCError(resp_status gomemcached.Status) bool {
	switch resp_status {
	case gomemcached.TMPFAIL:
		fallthrough
	case gomemcached.ENOMEM:
		fallthrough
	case gomemcached.EBUSY:
		fallthrough
	case gomemcached.NOT_INITIALIZED:
		fallthrough
		// this used to be remapped to and handled in the same way as TMPFAIL
		// keep the same behavior for now
	case gomemcached.SYNC_WRITE_IN_PROGRESS:
		return true
	default:
		return false
	}
}

// check if memcached response status indicates topology change,
// in which case we defer pipeline restart to topology change detector
func IsTopologyChangeMCError(resp_status gomemcached.Status) bool {
	switch resp_status {
	case gomemcached.NOT_MY_VBUCKET:
		return true
	default:
		return false
	}
}

func IsCollectionMappingError(resp_status gomemcached.Status) bool {
	switch resp_status {
	case gomemcached.UNKNOWN_COLLECTION:
		return true
	default:
		return false
	}
}

func IsGuardRailError(status gomemcached.Status) bool {
	switch status {
	case gomemcached.BUCKET_RESIDENT_RATIO_TOO_LOW:
		return true
	case gomemcached.BUCKET_DATA_SIZE_TOO_BIG:
		return true
	case gomemcached.BUCKET_DISK_SPACE_TOO_LOW:
		return true
	default:
		return false
	}
}

const NumberOfGuardrailTypes = int(gomemcached.BUCKET_DISK_SPACE_TOO_LOW) + 1 - int(gomemcached.BUCKET_RESIDENT_RATIO_TOO_LOW)

func IsEExistsError(resp_status gomemcached.Status) bool {
	switch resp_status {
	case gomemcached.KEY_EEXISTS:
		return true
	default:
		return false
	}
}

func IsENoEntError(resp_status gomemcached.Status) bool {
	switch resp_status {
	case gomemcached.KEY_ENOENT:
		return true
	default:
		return false
	}
}

func IsEAccessError(resp_status gomemcached.Status) bool {
	switch resp_status {
	case gomemcached.EACCESS:
		return true
	default:
		return false
	}
}
