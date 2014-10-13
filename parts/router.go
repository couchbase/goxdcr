package parts

import (
	"encoding/binary"
	"errors"
	"regexp"
	common "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	connector "github.com/Xiaomei-Zhang/couchbase_goxdcr/connector"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr/log"
	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
)

var ErrorInvalidDataForRouter = errors.New("Input data to Router is invalid.")
var ErrorNoDownStreamNodesForRouter = errors.New("No downstream nodes have been defined for the Router.")
var ErrorNoVbMapForRouter = errors.New("No vbMap has been defined for Router.")
var ErrorInvalidVbMapForRouter = errors.New("vbMap in Router is invalid.")

//var logger_router *log.CommonLogger = log.NewLogger("Router", log.LogLevelInfo)

// XDCR Router does two things:
// 1. converts UprEvent to MCRequest
// 2. routes MCRequest to downstream parts
type Router struct {
	*connector.Router
	filterRegexp  *regexp.Regexp // filter expression
	vbMap map[uint16]string // pvbno -> partId. This defines the loading balancing strategy of which vbnos would be routed to which part
	//Debug only, need to be rolled into statistics and monitoring
	counter map[string]int
}

func NewRouter(filterExpression  string,
	downStreamParts map[string]common.Part,
	vbMap map[uint16]string,
	logger_context *log.LoggerContext) (*Router, error) {
	// compile filter expression 
	var filterRegexp *regexp.Regexp
	var err error
	if len(filterExpression) > 0 {
		filterRegexp, err = regexp.Compile(filterExpression)
		if err != nil {
			return nil, err
		}
	}
	router := &Router{
		filterRegexp:  filterRegexp,
		vbMap:   vbMap,
		counter: make(map[string]int)}

	var routingFunc connector.Routing_Callback_Func = router.route
	router.Router = connector.NewRouter("XDCRRouter", downStreamParts, &routingFunc, logger_context, "XDCRRouter")

	//initialize counter
	for partId, _ := range downStreamParts {
		router.counter[partId] = 0
	}

	router.Logger().Infof("Router created with %d downstream parts \n", len(downStreamParts))
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
	req.Opcode = event.Opcode

	//extra
	if event.Opcode == mc.UPR_MUTATION || event.Opcode == mc.UPR_DELETION ||
		event.Opcode == mc.UPR_EXPIRATION {
		binary.BigEndian.PutUint64(req.Extras, event.Seqno)
		binary.BigEndian.PutUint32(req.Extras, event.Flags)
		binary.BigEndian.PutUint32(req.Extras, event.Expiry)
	} else if event.Opcode == mc.UPR_SNAPSHOT {
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
	result := make(map[string]interface{})
	
	// only *mc.UprEvent type data is accepted
	uprEvent, ok := data.(*mcc.UprEvent)
	if !ok {
		return nil, ErrorInvalidDataForRouter
	}
	
	// filter data if filter expession has been defined
	if router.filterRegexp != nil { 
		if !router.filterRegexp.Match(uprEvent.Key) {
			// if data does not match filter expression, drop it. return empty result
			router.Logger().Debugf("Data with key=%v has been filtered out", string(uprEvent.Key))
			return result, nil
		}
	}

	if router.vbMap == nil {
		return nil, ErrorNoVbMapForRouter
	}

	// use vbMap to determine which downstream part to route the request
	partId, ok := router.vbMap[uprEvent.VBucket]
	if !ok {
		return nil, ErrorInvalidVbMapForRouter
	}

	router.Logger().Debugf("Data with vbno=%d, opCode=%v is routed to downstream part %s", uprEvent.VBucket, uprEvent.Opcode, partId)

	switch uprEvent.Opcode {
	case mc.UPR_MUTATION, mc.UPR_DELETION, mc.UPR_EXPIRATION:
		result[partId] = ComposeMCRequest(uprEvent)
		router.counter[partId] = router.counter[partId] + 1
		router.Logger().Debugf("Rounting counter = %v\n", router.counter)
	default:
		router.Logger().Debugf("Uprevent OpCode=%v, is skipped\n", uprEvent.Opcode)
	}
	return result, nil
}

func (router *Router) SetVbMap(vbMap map[uint16]string) {
	router.vbMap = vbMap
	router.Logger().Infof("Set vbMap in Router")
	router.Logger().Debugf("vbMap: %v", vbMap)
}
