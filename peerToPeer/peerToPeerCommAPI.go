// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package peerToPeer

import (
	"fmt"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/service_def"
	"github.com/couchbase/goxdcr/v8/utils"
	"net/http"
	"strings"
	"time"
)

var ErrorInvalidOpcode = fmt.Errorf("invalid Opcode")
var ErrorInvalidMagic = fmt.Errorf("invalid Magic")
var ErrorReceiveChanFull = fmt.Errorf("opcode receiver channel is full")
var ErrorLifecycleMismatch = fmt.Errorf("lifecycle mismatch")
var ErrorMissingClientKey = fmt.Errorf("missing client key")
var ErrorMissingClientCert = fmt.Errorf("missing client cert")

type OpCode int

type ReqRespType int

type ReqRespCommon interface {
	Serialize() ([]byte, error)
	DeSerialize([]byte) error
	GetOpcode() OpCode
	GetType() ReqRespType
	GetSender() string
	GetOpaque() uint32
	RecordEnqueuedTime()
	GetEnqueuedTime() time.Time
}

type Request interface {
	ReqRespCommon
	CallBack(resp Response) (HandlerResult, error)
	GetTarget() string
	SameAs(other interface{}) (bool, error)
	GenerateResponse() interface{}
}

type Response interface {
	ReqRespCommon
	GetErrorString() string
}

type HandlerResult interface {
	GetError() error
	GetHttpStatusCode() int
}

type P2PSendType func(req Request, log *log.CommonLogger) (HandlerResult, error)

type PeerToPeerCommAPI interface {
	P2PReceive(reqOrResp ReqRespCommon) (HandlerResult, error)
	P2PSend(req Request, log *log.CommonLogger) (HandlerResult, error)
}

type P2pCommAPIimpl struct {
	receiveChs map[OpCode][]chan interface{}
	utils      utils.UtilsIface

	xdcrCompTopSvc service_def.XDCRCompTopologySvc
	securitySvc    service_def.SecuritySvc
}

func NewP2pCommAPIHelper(receiveChs map[OpCode][]chan interface{}, utils utils.UtilsIface, xdcrCompTopSvc service_def.XDCRCompTopologySvc, securitySvc service_def.SecuritySvc) *P2pCommAPIimpl {
	return &P2pCommAPIimpl{
		receiveChs:     receiveChs,
		utils:          utils,
		xdcrCompTopSvc: xdcrCompTopSvc,
		securitySvc:    securitySvc,
	}
}

func (p2p *P2pCommAPIimpl) P2PReceive(reqResp ReqRespCommon) (HandlerResult, error) {
	result := &HandlerResultImpl{}

	reqType := reqResp.GetType()
	if reqType == InvalidType {
		result.Err = ErrorInvalidMagic
		result.HttpStatusCode = http.StatusInternalServerError
		return result, ErrorInvalidMagic
	}

	reqOpcode := reqResp.GetOpcode()
	receiveChs, found := p2p.receiveChs[reqOpcode]
	if !found {
		result.Err = ErrorInvalidOpcode
		result.HttpStatusCode = http.StatusInternalServerError
		return result, ErrorInvalidOpcode
	}

	reqResp.RecordEnqueuedTime()
	select {
	case receiveChs[reqType] <- reqResp:
		result.HttpStatusCode = http.StatusOK
		return result, nil
	default:
		result.HttpStatusCode = http.StatusInternalServerError
		result.Err = ErrorReceiveChanFull
	}

	return result, ErrorReceiveChanFull
}

func (p2p *P2pCommAPIimpl) P2PSend(req Request, logger *log.CommonLogger) (HandlerResult, error) {
	payload, err := req.Serialize()
	if err != nil {
		return nil, err
	}
	authType := base.HttpAuthMechPlain
	var certificates, clientKey, clientCert []byte

	if p2p.securitySvc.IsClusterEncryptionLevelStrict() {
		authType = base.HttpAuthMechHttps
		certificates = p2p.securitySvc.GetCACertificates()
		if len(certificates) == 0 {
			return &HandlerResultImpl{
				Err:            base.ErrorNilCertificate,
				HttpStatusCode: http.StatusInternalServerError,
			}, base.ErrorNilCertificateStrictMode
		}

		isMandatory, err := p2p.xdcrCompTopSvc.ClientCertIsMandatory()
		if err == nil && isMandatory {
			// If n2n encryption is required and client cert is mandatory, then the traditional
			// cbauth username/pw "superuser" pairing will not work - and thus we must use clientCert and key
			// provided by the ns_server
			clientCert, clientKey = p2p.securitySvc.GetClientCertAndKey()
			result, err := checkClientCertAndKeyExists(clientCert, clientKey)
			if err != nil {
				return result, err
			}
		}
	}

	var out interface{}
	err, statusCode := p2p.utils.QueryRestApiWithAuth(req.GetTarget(), base.XDCRPeerToPeerPath, false, "", "", authType, certificates, true, clientCert, clientKey, base.MethodPost, base.JsonContentType,
		payload, base.P2PCommTimeout, &out, nil, false, logger)
	// utils returns this error because body is empty, which is fine
	if err == base.ErrorResourceDoesNotExist {
		err = nil
	} else if err != nil && strings.Contains(err.Error(), base.ErrorStringClientCertMandatory) {
		// This is a special case where client cert is set to mandatory but either the propagation
		// of the setting is not done cluster wide, or if the p2p is done before the cached value
		// is updated. Try again with client certs
		clientCert, clientKey = p2p.securitySvc.GetClientCertAndKey()
		result, err := checkClientCertAndKeyExists(clientCert, clientKey)
		if err != nil {
			return result, err
		}
		err, statusCode = p2p.utils.QueryRestApiWithAuth(req.GetTarget(), base.XDCRPeerToPeerPath, false, "", "", authType, certificates, true, clientCert, clientKey, base.MethodPost, base.JsonContentType,
			payload, base.P2PCommTimeout, &out, nil, false, logger)
		// Client Cert mandatory + n2n encryption turned on is only implemented with p2p already existing
		// No need to check for 404 return error
	}
	result := &HandlerResultImpl{HttpStatusCode: statusCode, Err: err}
	return result, err
}

// this is a wrapper function that returns appropriate error if client cert or key are missing
// generally speaking, this should not happen because ns_server should always be passing in a valid
// cert or key. If XDCR somehow gets in a terrible situation where the clientCert or key was not
// loaded successfully, leading to a CBSE or anything, there are 2 ways about it:
// 1. regenerateCerts or reload certs, which requires customer intervention
// 2. restart goxdcr ... which will allow security service to reload the key and cert from the files specified
func checkClientCertAndKeyExists(clientCert []byte, clientKey []byte) (HandlerResult, error) {
	if len(clientCert) == 0 {
		return &HandlerResultImpl{
			Err:            ErrorMissingClientCert,
			HttpStatusCode: http.StatusInternalServerError,
		}, ErrorMissingClientCert
	} else if len(clientKey) == 0 {
		return &HandlerResultImpl{
			Err:            ErrorMissingClientKey,
			HttpStatusCode: http.StatusInternalServerError,
		}, ErrorMissingClientKey
	}
	return nil, nil
}
