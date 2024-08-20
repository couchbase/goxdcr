// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_impl

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/service_def"
	utilities "github.com/couchbase/goxdcr/v8/utils"
)

var ErrorInitializingAuditService = "Error initializing audit service."
var AuditServiceUserAgent = "Goxdcr Audit"

// opcode for memcached audit command
var AuditPutCommandCode = mc.AUDIT

type AuditSvc struct {
	top_svc     service_def.XDCRCompTopologySvc
	kvaddr      string
	logger      *log.CommonLogger
	utils       utilities.UtilsIface
	initialized bool
	stateLock   sync.RWMutex
}

func NewAuditSvc(top_svc service_def.XDCRCompTopologySvc, loggerCtx *log.LoggerContext, utilsIn utilities.UtilsIface) (*AuditSvc, error) {
	service := &AuditSvc{
		top_svc: top_svc,
		logger:  log.NewLogger("AuditSvc", loggerCtx),
		utils:   utilsIn,
	}

	service.logger.Infof("Created audit service.\n")
	return service, nil
}

func (service *AuditSvc) isInitialized() bool {
	service.stateLock.RLock()
	defer service.stateLock.RUnlock()
	return service.initialized
}

func (service *AuditSvc) initIfNeeded() error {
	if service.isInitialized() {
		return nil
	}

	service.stateLock.Lock()
	defer service.stateLock.Unlock()

	// check again in case someone else sneaked in before Lock()
	if service.initialized {
		return nil
	}

	err := service.utils.ExponentialBackoffExecutor("auditSvs.Init", base.RetryIntervalMetakv, base.MaxNumOfMetakvRetries,
		base.MetaKvBackoffFactor, service.init)
	if err == nil {
		service.initialized = true
	}
	return err
}

func (service *AuditSvc) Write(eventId uint32, event service_def.AuditEventIface) error {
	if service.logger.GetLogLevel() >= log.LogLevelDebug {
		service.logger.Debugf("Writing audit event. eventId=%v, event=%v\n", eventId, event.Clone().Redact())
	}

	err := service.initIfNeeded()
	if err != nil {
		service.logger.Errorf("Init failed with %v", err)
		return err
	}

	client, err := service.getClient()
	if err != nil {
		service.logger.Errorf("getClient failed with %v", err)
		return err
	}
	defer service.releaseClient(client)

	err = service.write_internal(client, eventId, event)
	// ignore errors when writing audit logs. simply log them
	if err != nil {
		err = service.utils.NewEnhancedError(service_def.ErrorWritingAudit, err)
		service.logger.Error(err.Error())
	}
	return nil
}

func (service *AuditSvc) write_internal(client mcc.ClientIface, eventId uint32, event service_def.AuditEventIface) error {
	req, err := composeAuditRequest(eventId, event)
	if err != nil {
		service.logger.Errorf("composeAuditRequest failed with error %v", err)
		return err
	}
	if service.logger.GetLogLevel() >= log.LogLevelDebug {
		service.logger.Debugf("audit request=%v%v%v\n", base.UdTagBegin, req, base.UdTagEnd)
	}

	conn := client.Hijack()
	conn.(*net.TCPConn).SetWriteDeadline(time.Now().Add(base.AuditWriteTimeout))

	if err := client.Transmit(req); err != nil {
		service.logger.Errorf("client.Transmit received error %v", err)
		return err
	}

	service.logger.Debugf("audit request transmitted\n")

	conn.(*net.TCPConn).SetReadDeadline(time.Now().Add(base.AuditReadTimeout))
	res, err := client.Receive()
	if service.logger.GetLogLevel() >= log.LogLevelDebug {
		if res != nil {
			service.logger.Debugf("audit response=%v%v%v, opcode=%v, opaque=%v, status=%v, err=%v\n", base.UdTagBegin, res, base.UdTagEnd, res.Opcode, res.Opaque, res.Status, err)
		} else {
			service.logger.Debugf("audit response is nil, err=%v\n", err)
		}
	}

	if err != nil {
		service.logger.Errorf("client.Receive received error=%v", err)
		return err
	} else if res.Opcode != AuditPutCommandCode {
		return errors.New(fmt.Sprintf("audit unexpected #opcode %v", res.Opcode))
	} else if req.Opaque != res.Opaque {
		return errors.New(fmt.Sprintf("audit opaque mismatch, %v over %v", req.Opaque, res.Opaque))
	} else if res.Status != mc.SUCCESS {
		return errors.New(fmt.Sprintf("audit unsuccessful status = %v", res.Status))
	}

	return nil
}

func composeAuditRequest(eventId uint32, event service_def.AuditEventIface) (*mc.MCRequest, error) {
	eventBytes, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}

	req := &mc.MCRequest{
		Opcode: AuditPutCommandCode}

	req.Extras = make([]byte, 4)
	binary.BigEndian.PutUint32(req.Extras[:4], eventId)

	req.Body = eventBytes

	req.Opaque = eventId
	return req, nil
}

func (service *AuditSvc) init() error {
	var err error
	service.kvaddr, err = service.top_svc.MyMemcachedAddr()
	if err != nil {
		return service.utils.NewEnhancedError(ErrorInitializingAuditService+" Error getting memcached address of current host.", err)
	}
	return nil
}

func (service *AuditSvc) getClient() (mcc.ClientIface, error) {
	// audit connection is sparse and is not kept alive
	client, err := service.utils.GetMemcachedConnection(service.kvaddr, "" /*bucketName*/, AuditServiceUserAgent,
		0 /*keepAlivePeriod*/, service.logger)

	if err != nil {
		return nil, err
	}

	service.logger.Debugf("audit service get client succeeded")

	return client, nil
}

func (service *AuditSvc) releaseClient(client mcc.ClientIface) error {
	err := client.Close()
	if err != nil {
		service.logger.Infof("Audit service failed to close connection. err=%v\n", err)
	}
	return nil
}
