package parts

import (
	"encoding/binary"
	"errors"
	common "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	connector "github.com/Xiaomei-Zhang/couchbase_goxdcr/connector"
	log "github.com/Xiaomei-Zhang/couchbase_goxdcr/util"
	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
)

var ErrorInvalidDataForRouter = errors.New("Input data to Router is invalid.")
var ErrorNoDownStreamNodesForRouter = errors.New("No downstream nodes have been defined for the Router.")
var ErrorNoVbMapForRouter = errors.New("No vbMap has been defined for Router.")
var ErrorInvalidVbMapForRouter = errors.New("vbMap in Router is invalid.")

var logger_router *log.CommonLogger = log.NewLogger("Router", log.LogLevelInfo)

// XDCR Router does two things:
// 1. converts UprEvent to MCRequest
// 2. routes MCRequest to downstream parts
type Router struct {
	*connector.Router
	vbMap map[uint16]string // pvbno -> partId. This defines the loading balancing strategy of which vbnos would be routed to which part
}

func NewRouter(downStreamParts map[string]common.Part, vbMap map[uint16]string) (*Router, error) {
	router := &Router{
		vbMap: vbMap,
	}

	var routingFunc connector.Routing_Callback_Func = router.route
	router.Router = connector.NewRouter(downStreamParts, &routingFunc)

	logger_router.Infof("Router created with %d downstream parts \n", len(downStreamParts))
	return router, nil
}

func ComposeMCRequest(event *mcc.UprEvent) *mc.MCRequest {
	req := &mc.MCRequest{Cas: event.Cas,
		Opaque:  0,
		VBucket: event.VBucket,
		Key:     event.Key,
		Body:    event.Value,
		Extras:  make([]byte, 224)}
	//opCode
	switch event.Opcode {
	case mcc.UprStreamRequest:
		req.Opcode = mc.UPR_STREAMREQ
	case mcc.UprMutation:
		req.Opcode = mc.UPR_MUTATION
	case mcc.UprDeletion:
		req.Opcode = mc.UPR_DELETION
	case mcc.UprExpiration:
		req.Opcode = mc.UPR_EXPIRATION
	case mcc.UprCloseStream:
		req.Opcode = mc.UPR_CLOSESTREAM
	case mcc.UprSnapshot:
		req.Opcode = mc.UPR_SNAPSHOT
	case mcc.UprFlush:
		req.Opcode = mc.UPR_FLUSH
	}

	//extra
	if event.Opcode == mcc.UprMutation || event.Opcode == mcc.UprDeletion ||
		event.Opcode == mcc.UprExpiration {
		binary.BigEndian.PutUint64(req.Extras, event.Seqno)
		binary.BigEndian.PutUint32(req.Extras, event.Flags)
		binary.BigEndian.PutUint32(req.Extras, event.Expiry)
	} else if event.Opcode == mcc.UprSnapshot {
		binary.BigEndian.PutUint64(req.Extras, event.Seqno)
		binary.BigEndian.PutUint64(req.Extras, event.SnapstartSeq)
		binary.BigEndian.PutUint64(req.Extras, event.SnapendSeq)
		binary.BigEndian.PutUint32(req.Extras, event.SnapshotType)
	}

	return req
}

// Implementation of the routing algorithm
// Currently doing static dispatching based on vbucket number.
func (router *Router) route(data interface{}) (map[string]interface{}, error) {
	// only *mc.UprEvent type data is accepted
	uprEvent, ok := data.(*mcc.UprEvent)
	if !ok {
		return nil, ErrorInvalidDataForRouter
	}

	if router.vbMap == nil {
		return nil, ErrorNoVbMapForRouter
	}

	// use vbMap to determine which downstream part to route the request
	partId, ok := router.vbMap[uprEvent.VBucket]
	if !ok {
		return nil, ErrorInvalidVbMapForRouter
	}

	logger_router.Debugf("Data with vbno %d is routed to downstream part %s", uprEvent.VBucket, partId)

	result := make(map[string]interface{})
	result[partId] = ComposeMCRequest(uprEvent)

	return result, nil
}

func (router *Router) SetVbMap(vbMap map[uint16]string) {
	router.vbMap = vbMap
	logger_router.Infof("Set vbMap in Router")
	logger_router.Debugf("vbMap: %v", vbMap)
}
