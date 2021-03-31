// Copyright 2020-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

// +build enterprise

package service_impl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing-ee/js-evaluator/defs"
	"github.com/couchbase/eventing-ee/js-evaluator/impl"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/service_def"
)

const EVENTING_FUNCTION_LIB = "xdcr"

var (
	// The number of goroutines that can call js-evaluator to get merge result
	numResolverWorkers = base.JSEngineThreadsPerWorker * base.JSEngineWorkersPerNode
	// The channel size for sending input to resolverWorkers. Keep this channel small so it won't have backup data from pipelines that went away
	inputChanelSize = numResolverWorkers
)

type ResolverSvc struct {
	logger      *log.CommonLogger
	InputCh     chan *base.ConflictParams // This accepts conflicting documents from XMEM
	handler     defs.Handler              // js-evaluator handler for REST request
	evaluator   defs.Evaluator
	functionUrl string
	top_svc     service_def.XDCRCompTopologySvc
	started     bool
}

func NewResolverSvc(top_svc service_def.XDCRCompTopologySvc) *ResolverSvc {
	return &ResolverSvc{top_svc: top_svc, logger: log.NewLogger("ResolverSvc", nil), started: false}
}

func (rs *ResolverSvc) ResolveAsync(aConflict *base.ConflictParams, finish_ch chan bool) {
	select {
	case rs.InputCh <- aConflict:
	case <-finish_ch:
	}
}

// This default function is only for internal testing. The create should not fail. If it fails, we log an error.
// When we create replication using this merge function, we will check if it exists.
func (rs *ResolverSvc) InitDefaultFunc() {
	reqBody := map[string]string{
		"name": base.DefaultMergeFunc,
		"code": base.DefaultMergeFuncBodyCC,
	}
	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(reqBody)
	if err != nil {
		rs.logger.Errorf("Encode failed for %v function. err: %v", base.DefaultMergeFunc, err)
		return
	}

	b := buffer.Bytes()
	functionUrl := fmt.Sprintf("%v/%v", rs.functionUrl, base.DefaultMergeFunc)
	req, err := http.NewRequest(base.MethodPost, functionUrl, bytes.NewBuffer(b))

	if err != nil {
		rs.logger.Errorf("Create new request failed for %v function. err: %v", base.DefaultMergeFunc, err)
		return
	}
	req.Header.Set(base.ContentType, base.JsonContentType)
	conn_str, err := rs.top_svc.MyMemcachedAddr()
	if err != nil {
		rs.logger.Errorf("Failed to get MyMemcachedAddr for %v function. err: %v", base.DefaultMergeFunc, err)
		return
	}
	username, password, err := cbauth.GetMemcachedServiceAuth(conn_str)
	if err != nil {
		rs.logger.Errorf("Failed to get authorization for %v function. err: %v", base.DefaultMergeFunc, err)
		return
	}
	req.SetBasicAuth(username, password)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		rs.logger.Errorf("Create %v received error %v", base.DefaultMergeFunc, err)
		return
	}
	if response.StatusCode == http.StatusOK {
		rs.logger.Infof("Created %v function", base.DefaultMergeFunc)
		return
	} else {
		rs.logger.Errorf("Create %v received http.Status %v for merge function %s, encoded: %s", base.DefaultMergeFunc, response.Status, reqBody, b)
		return
	}
}

func (rs *ResolverSvc) CheckMergeFunction(fname string) error {
	functionUrl := fmt.Sprintf("%v/%v", rs.functionUrl, fname)
	req, err := http.NewRequest(base.MethodGet, functionUrl, nil)
	req.Header.Set(base.ContentType, base.JsonContentType)
	conn_str, err := rs.top_svc.MyMemcachedAddr()
	if err != nil {
		return err
	}
	username, password, err := cbauth.GetMemcachedServiceAuth(conn_str)
	if err != nil {
		return err
	}
	req.SetBasicAuth(username, password)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if response.StatusCode == http.StatusOK {
		return nil
	} else {
		return fmt.Errorf("CheckMergeFunction received http.Status %v for merge function %v", response.Status, fname)
	}
}

func (rs *ResolverSvc) Start(sourceKVHost string, xdcrRestPort uint16) {
	err := rs.initEvaluator(sourceKVHost, xdcrRestPort)
	if err != nil {
		rs.logger.Errorf("Failed to Start resolverSvc. Error %v", err)
		return
	}
	rs.InputCh = make(chan *base.ConflictParams, inputChanelSize)
	for i := 0; i < numResolverWorkers; i++ {
		go rs.resolverWorker(i)
	}
	rs.functionUrl = fmt.Sprintf(base.FunctionUrlFmt, sourceKVHost, xdcrRestPort)
	rs.started = true
	rs.logger.Infof("ResolverSvc for custom CR is started.")
}

func (rs *ResolverSvc) Started() bool {
	return rs.started
}
func (rs *ResolverSvc) resolverWorker(threadId int) {
	for {
		rs.resolveOne(threadId)
	}
}

func (rs *ResolverSvc) resolveOne(threadId int) {
	input := <-rs.InputCh
	source := input.Source
	target := input.Target
	var params []interface{}
	sourceTime := base.CasToTime(source.Req.Cas).String()
	targetTime := base.CasToTime(target.Resp.Cas).String()
	sourceBody := base.FindSourceBodyWithoutXattr(source.Req)
	targetBody, err := target.FindTargetBodyWithoutXattr()
	if err != nil {
		input.ResultNotifier.NotifyMergeResult(input, nil, err)
	}

	params = append(params, string(source.Req.Key))
	params = append(params, string(sourceBody))
	params = append(params, sourceTime)
	params = append(params, string(input.SourceId))
	params = append(params, string(targetBody))
	params = append(params, targetTime)
	params = append(params, string(input.TargetId))
	res, err := rs.execute(EVENTING_FUNCTION_LIB, input.MergeFunction, params)
	input.ResultNotifier.NotifyMergeResult(input, res, err)
}

func (rs *ResolverSvc) initEvaluator(sourceKVHost string, xdcrRestPort uint16) error {
	engine := impl.NewEngine()

	config := make(map[defs.Config]interface{})
	config[defs.WorkersPerNode] = base.JSEngineWorkersPerNode
	config[defs.ThreadsPerWorker] = base.JSEngineThreadsPerWorker
	config[defs.NsServerURL] = base.GetHostAddr(sourceKVHost, xdcrRestPort)

	err := engine.Configure(config)
	if err.Err != nil {
		return fmt.Errorf("Unable to configure engine. err: %v", err.Err)
	}

	rs.handler = engine.UIHandler()
	http.HandleFunc(rs.handler.Path(), rs.functionsPathHandler)

	err = engine.Start()
	if err.Err != nil {
		return fmt.Errorf("Unable to start engine. err: %v", err.Err)
	}

	rs.evaluator = engine.Fetch()
	if rs.evaluator == nil {
		return fmt.Errorf("Unable to fetch javascript evaluator.")
	} else {
		rs.logger.Infof("Javascript evaluator started with %v worker and %v threads each.", config[defs.WorkersPerNode], config[defs.ThreadsPerWorker])
	}
	return nil
}

func (rs *ResolverSvc) functionsPathHandler(w http.ResponseWriter, r *http.Request) {
	// We can do any verification here before sending to js-evaluator's handler.
	rs.handler.Handler()(w, r)
}

func (rs *ResolverSvc) execute(libraryName string, functionName string, params []interface{}) (interface{}, error) {
	if rs.started == false {
		return nil, fmt.Errorf("ResolverSvc is not started.")
	}
	options := map[defs.Option]interface{}{defs.Timeout: 1000000 /* time in nanosecond for function to run */}
	res, err := rs.evaluator.Evaluate(libraryName, functionName, options, params)
	if err.Err != nil {
		return nil, fmt.Errorf("Javascript Evaluate() returned error: %v, error details: %v", err.Err, err.Details)
	} else {
		return res, nil
	}
}
