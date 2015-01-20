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
	"github.com/couchbase/cbauth"
	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/utils"
	"net"
	"time"
)

var ErrorInitializingAuditService = "Error initializing audit service."

// opcode for memcached audit command
var AuditPutCommandCode = mc.CommandCode(0x27)

// write timeout
var WriteTimeout = 1000 * time.Millisecond

// read timeout
var ReadTimeout = 1000 * time.Millisecond

type AuditSvc struct {
	top_svc service_def.XDCRCompTopologySvc
	kvaddr  string
	username  string
	password  string
	logger  *log.CommonLogger
}

func NewAuditSvc(top_svc service_def.XDCRCompTopologySvc, loggerCtx *log.LoggerContext) (*AuditSvc, error) {
	service := &AuditSvc{
		top_svc: top_svc,
		logger:  log.NewLogger("AuditService", loggerCtx),
	}

	service.logger.Infof("Created audit service.\n")
	return service, nil
}

func (service *AuditSvc) Write(eventId uint32, event interface{}) error {
	if service.kvaddr == "" {
		err := service.init()
		if err != nil {
			return err
		}	
	}

	go service.write(eventId, event)
	return nil
}

func (service *AuditSvc) write(eventId uint32, event interface{}) {
	client, err := service.getClient()
	if err != nil {
		return
	}
	defer service.releaseClient(client)
	
	err = service.write_internal(client, eventId, event)
	// ignore errors when writing audit logs. simply log them
	if err != nil {
		err = utils.NewEnhancedError(base.ErrorWritingAudit, err)
		service.logger.Error(err.Error())
	}
}

func (service *AuditSvc) write_internal(client *mcc.Client, eventId uint32, event interface{}) error {
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
	service.kvaddr, err = service.top_svc.MyMemcachedAddr()
	if err != nil {
		return utils.NewEnhancedError(ErrorInitializingAuditService+" Error getting memcached address of current host.", err)
	}

	clusterAddr, err := service.top_svc.MyConnectionStr()
	if err != nil {
		return utils.NewEnhancedError(ErrorInitializingAuditService+" Error getting address of current cluster.", err)
	}
	
	service.username, service.password, err = cbauth.GetMemcachedServiceAuth(clusterAddr)
	if err != nil {
		err = utils.NewEnhancedError(fmt.Sprintf(ErrorInitializingAuditService+" Error getting memcached credentials for cluster %v\n.", clusterAddr), err)
		return err
	}
	
	_, err = base.ConnPoolMgr().GetOrCreatePool(base.AuditServicePoolName, service.kvaddr, "", service.username, service.password, base.DefaultConnectionSize)
	return err
}
	
func (service *AuditSvc) getClient() (*mcc.Client, error) {
	pool := base.ConnPoolMgr().GetPool(base.AuditServicePoolName)
	
	client, err := pool.Get()
	if err != nil {
		return nil, err
	}

	service.logger.Debugf("audit service get client succeeded")

	return client, nil
}

func (service *AuditSvc) releaseClient(client *mcc.Client) error {
	pool := base.ConnPoolMgr().GetPool(base.AuditServicePoolName)
	pool.Release(client)
	return nil
}
