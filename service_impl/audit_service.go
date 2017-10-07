// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package service_impl

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/service_def"
	utilities "github.com/couchbase/goxdcr/utils"
	"net"
	"time"
)

var ErrorInitializingAuditService = "Error initializing audit service."
var AuditServiceUserAgent = "Goxdcr Audit"

// opcode for memcached audit command
var AuditPutCommandCode = mc.CommandCode(0x27)

// write timeout
var WriteTimeout = 1000 * time.Millisecond

// read timeout
var ReadTimeout = 1000 * time.Millisecond

type AuditSvc struct {
	top_svc     service_def.XDCRCompTopologySvc
	kvaddr      string
	logger      *log.CommonLogger
	initialized bool
	utils       utilities.UtilsIface
}

func NewAuditSvc(top_svc service_def.XDCRCompTopologySvc, loggerCtx *log.LoggerContext, utilsIn utilities.UtilsIface) (*AuditSvc, error) {
	service := &AuditSvc{
		top_svc:     top_svc,
		logger:      log.NewLogger("AuditSvc", loggerCtx),
		initialized: false,
		utils:       utilsIn,
	}

	service.logger.Infof("Created audit service.\n")
	return service, nil
}

func (service *AuditSvc) Write(eventId uint32, event interface{}) error {
	service.logger.Debugf("Writing audit event. eventId=%v, event=%v\n", eventId, event)

	err := service.init()
	if err != nil {
		return err
	}
	client, err := service.getClient()
	if err != nil {
		return nil
	}
	defer service.releaseClient(client)

	err = service.write_internal(client, eventId, event)
	// ignore errors when writing audit logs. simply log them
	if err != nil {
		err = service.utils.NewEnhancedError(base.ErrorWritingAudit, err)
		service.logger.Error(err.Error())
	}
	return nil
}

func (service *AuditSvc) write_internal(client mcc.ClientIface, eventId uint32, event interface{}) error {
	req, err := composeAuditRequest(eventId, event)
	if err != nil {
		return err
	}
	service.logger.Debugf("audit request=%v\n", req)

	conn := client.Hijack()
	conn.(*net.TCPConn).SetWriteDeadline(time.Now().Add(WriteTimeout))

	if err := client.Transmit(req); err != nil {
		return err
	}

	service.logger.Debugf("audit request transmitted\n")

	conn.(*net.TCPConn).SetReadDeadline(time.Now().Add(ReadTimeout))
	res, err := client.Receive()
	service.logger.Debugf("audit response=%v, opcode=%v, opaque=%v, status=%v, err=%v\n", res, res.Opcode, res.Opaque, res.Status, err)

	if err != nil {
		return err
	} else if res.Opcode != AuditPutCommandCode {
		return errors.New(fmt.Sprintf("unexpected #opcode %v", res.Opcode))
	} else if req.Opaque != res.Opaque {
		return errors.New(fmt.Sprintf("opaque mismatch, %v over %v", req.Opaque, res.Opaque))
	} else if res.Status != mc.SUCCESS {
		return errors.New(fmt.Sprintf("unsuccessful status = %v", res.Status))
	}

	return nil
}

func composeAuditRequest(eventId uint32, event interface{}) (*mc.MCRequest, error) {
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
	if !service.initialized {
		service.kvaddr, err = service.top_svc.MyMemcachedAddr()
		if err != nil {
			return service.utils.NewEnhancedError(ErrorInitializingAuditService+" Error getting memcached address of current host.", err)
		} else {
			service.initialized = true
		}
	}
	return err
}

func (service *AuditSvc) getClient() (mcc.ClientIface, error) {
	// audit connection is not kept alive
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
