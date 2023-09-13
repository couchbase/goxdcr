// Copyright 2020-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

//go:build enterprise
// +build enterprise

package service_impl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/couchbase/cbauth"
	evaluatorApi "github.com/couchbase/eventing-ee/evaluator/api"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/crMeta"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/service_def"
)

var (
	// The number of goroutines that can call js-evaluator to get merge result
	numResolverWorkers = base.JSEngineWorkers
	// The channel size for sending input to resolverWorkers. Keep this channel small so it won't have backup data from pipelines that went away
	inputChanelSize = numResolverWorkers
)

type ResolverSvc struct {
	logger       *log.CommonLogger
	InputCh      chan *crMeta.ConflictParams // This accepts conflicting documents from XMEM
	engine       evaluatorApi.Engine
	adminService evaluatorApi.AdminService // js-evaluator handler for REST request
	workerPool   chan evaluatorApi.Worker
	functionUrl  string
	top_svc      service_def.XDCRCompTopologySvc
	started      bool
}

func NewResolverSvc(top_svc service_def.XDCRCompTopologySvc) *ResolverSvc {
	return &ResolverSvc{top_svc: top_svc, logger: log.NewLogger("ResolverSvc", nil), started: false}
}

func (rs *ResolverSvc) ResolveAsync(aConflict *crMeta.ConflictParams, finish_ch chan bool) {
	select {
	// CCR pipeline cannot start if resolver is not started. So the InputCh always exists at this call
	case rs.InputCh <- aConflict:
	case <-finish_ch:
	}
}

// This default function is only for internal testing. The create should not fail. If it fails, we log an error.
// When we create replication using this merge function, we will check if it exists.
func (rs *ResolverSvc) InitDefaultFunc() {
	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(base.DefaultMergeFuncBodyCC)
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
		rs.logger.Errorf("Create %v received http.Status %v for merge function %s, encoded: %s", base.DefaultMergeFunc, response.Status, base.DefaultMergeFuncBodyCC, b)
		return
	}
}

func (rs *ResolverSvc) CheckMergeFunction(fname string) error {
	functionUrl := fmt.Sprintf("%v/%v", rs.functionUrl, fname)
	req, err := http.NewRequest(base.MethodGet, functionUrl, nil)
	if err != nil {
		return err
	}
	req.Header.Set(base.ContentType, base.JsonContentType)
	// When a node first starts up and before it is a "cluster", REST call to KV such as MyMemchachedAddr()
	// will return 404. Retry until it is successful
	var conn_str string
	for {
		conn_str, err = rs.top_svc.MyMemcachedAddr()
		if err != nil {
			time.Sleep(base.CCRKVRestCallRetryInterval)
		} else {
			break
		}
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
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("CheckMergeFunction received http.Status %v for merge function %v", response.Status, fname)
	}
	return nil
}

func (rs *ResolverSvc) Start(sourceKVHost string, xdcrRestPort uint16) {
	err := rs.initEvaluator(sourceKVHost, xdcrRestPort)
	if err != nil {
		rs.logger.Errorf("Failed to Start resolverSvc. Error %v", err)
		return
	}
	rs.InputCh = make(chan *crMeta.ConflictParams, inputChanelSize)
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
	timeoutMs := input.TimeoutMs
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
	res, err := rs.execute(input.MergeFunction, input.MergeFunction, params, timeoutMs)
	input.ResultNotifier.NotifyMergeResult(input, res, err)
}

func (rs *ResolverSvc) initEvaluator(sourceKVHost string, xdcrRestPort uint16) error {
	rs.engine = evaluatorApi.Singleton

	globalCfg := evaluatorApi.GlobalConfig{}
	if fault := evaluatorApi.ConfigureGlobalConfig(globalCfg); fault != nil {
		return fmt.Errorf("Encountered error while configuring globalCfg. err: %v", fault.Error())
	}
	engConfig := evaluatorApi.StaticEngineConfig{}
	dynamicConfig := evaluatorApi.DynamicEngineConfig{}

	if fault := rs.engine.Initialize(engConfig, dynamicConfig); fault != nil {
		return fmt.Errorf("Unable to configure engine. err: %v", fault.Error())
	}
	rs.adminService = rs.engine.AdminService()
	http.HandleFunc(rs.adminService.Path(), rs.functionsPathHandler)
	rs.workerPool = make(chan evaluatorApi.Worker, base.JSEngineWorkers)
	for i := 0; i < base.JSEngineWorkers; i++ {
		workerInst, fault := rs.engine.Create(uint64(base.JSWorkerQuota))
		if fault != nil {
			return fmt.Errorf("Started %v of %v workers. Failed to start next worker", i, base.JSEngineWorkers)
		}
		rs.workerPool <- workerInst
	}

	rs.logger.Infof("Javascript evaluator started with %v workers at quota %v.", base.JSEngineWorkers, base.JSWorkerQuota)
	return nil
}

func (rs *ResolverSvc) functionsPathHandler(w http.ResponseWriter, r *http.Request) {
	if rs.top_svc.IsMyClusterDeveloperPreview() {
		rs.adminService.Handler()(w, r)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

func (rs *ResolverSvc) execute(libraryName string, functionName string, params []interface{}, timeoutMs uint32) (interface{}, error) {
	if rs.started == false {
		return nil, fmt.Errorf("ResolverSvc is not started.")
	}
	options := evaluatorApi.Options{Timeout: timeoutMs}
	worker := <-rs.workerPool
	defer func() {
		rs.workerPool <- worker
	}()
	var loctr evaluatorApi.Locator
	loctr.FromString(libraryName)

	onlyInWorker, onlyInStore, isVersionMismatch := worker.IsStale(loctr)
	if isVersionMismatch || onlyInWorker {
		if fault := worker.Unload(loctr); fault != nil {
			return nil, fault.Error()
		}
	}
	if isVersionMismatch || onlyInStore {
		if fault := worker.Load(loctr); fault != nil {
			return nil, fault.Error()
		}
	}

	res, fault := worker.Run(nil, libraryName, functionName, options, params...)
	if fault != nil && fault.Error() != nil {
		return nil, fmt.Errorf("Javascript Evaluate() returned error: %v", fault.Error())
	} else {
		return res, nil
	}
}
