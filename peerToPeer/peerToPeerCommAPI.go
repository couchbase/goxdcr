// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package peerToPeer

import "fmt"

var ErrorInvalidOpcode = fmt.Errorf("Invalid Opcode")
var ErrorReceiveChanFull = fmt.Errorf("Opcode receiver channel is full")
var ErrorLifecycleMismatch = fmt.Errorf("Lifecycle mismatch")

type OpCode int

type ReqRespType int

type ReqRespCommon interface {
	Serialize() ([]byte, error)
	DeSerialize([]byte) error
	GetOpcode() OpCode
	GetType() ReqRespType
	GetSender() string
	GetOpaque() uint32
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
}

type HandlerResult interface {
	GetError() error
	GetHttpStatusCode() int
}

type P2PSendType func(req Request) (HandlerResult, error)

type PeerToPeerCommAPI interface {
	P2PReceive(reqOrResp ReqRespCommon) (HandlerResult, error)
	P2PSend(req Request) (HandlerResult, error)
}
