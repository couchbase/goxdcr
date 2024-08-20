// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.
package streamApiWatcher

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/utils"
	"github.com/pkg/errors"
)

const defaultSelfRestartSleep = 1 * time.Second

type StreamApiWatcher interface {
	Start()
	Stop()
	GetResult() map[string]interface{}
}
type streamOutputCache struct {
	output        base.InterfaceMap
	mtx           sync.RWMutex
	initializer   sync.Once
	initializedCh chan bool
}

func (c *streamOutputCache) cacheStreamOutput(output base.InterfaceMap) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.output = output
	c.initializer.Do(func() {
		close(c.initializedCh)
	})
}

func (c *streamOutputCache) getOutput() base.InterfaceMap {
	<-c.initializedCh
	c.mtx.RLock()
	defer c.mtx.RUnlock()
	return c.output
}

type StreamApiWatcherImpl struct {
	path             string
	ch               chan base.InterfaceMap
	closeCh          chan bool
	finCh            chan bool
	waitGrp          sync.WaitGroup
	running          bool
	mutex            sync.Mutex
	connInfo         base.ClusterConnectionInfoProvider
	utils            utils.UtilsIface
	logger           *log.CommonLogger
	lastOutput       *streamOutputCache
	selfRestartSleep time.Duration
	resultCb         func()
}

func NewStreamApiWatcher(path string, connInfo base.ClusterConnectionInfoProvider, utils utils.UtilsIface, resultCb func(), logger *log.CommonLogger) *StreamApiWatcherImpl {
	watcher := StreamApiWatcherImpl{
		path:             path,
		ch:               make(chan base.InterfaceMap, 10),
		finCh:            nil,
		closeCh:          nil,
		running:          false,
		mutex:            sync.Mutex{},
		connInfo:         connInfo,
		utils:            utils,
		logger:           logger,
		lastOutput:       nil,
		resultCb:         resultCb,
		selfRestartSleep: defaultSelfRestartSleep,
	}
	return &watcher
}

func (w *StreamApiWatcherImpl) Start() {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.lastOutput = &streamOutputCache{
		output:        make(base.InterfaceMap),
		mtx:           sync.RWMutex{},
		initializer:   sync.Once{},
		initializedCh: make(chan bool),
	}
	w.finCh = make(chan bool)
	w.waitGrp = sync.WaitGroup{}
	w.waitGrp.Add(1)
	go w.watchClusterChanges()
	connStr, _ := w.connInfo.MyConnectionStr()
	w.logger.Infof("Start watching %v:%v.", connStr, w.path)
}

func (w *StreamApiWatcherImpl) Stop() {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	close(w.finCh)
	w.running = false
	w.lastOutput = nil
	w.waitGrp.Wait()
	w.logger.Infof("Stop watching %v", w.path)
}

func (w *StreamApiWatcherImpl) GetResult() map[string]interface{} {
	return w.lastOutput.getOutput()
}

func (w *StreamApiWatcherImpl) watchClusterChanges() {
	w.mutex.Lock()
	w.running = true
	w.closeCh = make(chan bool)
	w.mutex.Unlock()
	selfRestart := func() {
		sleepTimer := time.NewTicker(w.selfRestartSleep)
		select {
		case <-sleepTimer.C:
		case <-w.finCh:
			w.waitGrp.Done()
			return
		}
		go w.watchClusterChanges()
		w.logger.Infof("Restart watching %v after %v", w.path, w.selfRestartSleep)
		w.selfRestartSleep = 2 * w.selfRestartSleep
	}
	waitGrp := sync.WaitGroup{}
	waitGrp.Add(1)
	go w.runStreamingQuery(&waitGrp)
	defer waitGrp.Wait()
	for {
		select {
		case output := <-w.ch:
			w.lastOutput.cacheStreamOutput(output)
			// Having successfully received a stream result, we can reset restart wait time
			w.selfRestartSleep = defaultSelfRestartSleep
			if w.resultCb != nil {
				go w.resultCb()
			}
		case <-w.closeCh:
			selfRestart()
			return
		case <-w.finCh:
			w.waitGrp.Done()
			return
		}
	}
}

func (w *StreamApiWatcherImpl) streamResultCallback(result base.InterfaceMap) error {
	select {
	case w.ch <- result:
		return nil
	default:
		return errors.New("Failed to send stream API result for processing because the channel is full.")
	}
}

func (w *StreamApiWatcherImpl) runStreamingQuery(waitGrp *sync.WaitGroup) {
	defer waitGrp.Done()
	err := w.runStreamingEndpoint()
	if err != nil {
		w.handleError(err)
	}
}

func (w *StreamApiWatcherImpl) runStreamingEndpoint() error {
	var body = make([]byte, 0)
	connStr, err := w.connInfo.MyConnectionStr()
	if err != nil {
		return err
	}
	username, password, authMech, certificate, sanInCertificate, clientCertificate, clientKey, err := w.connInfo.MyCredentials()
	if err != nil {
		return err
	}
	authMode := w.utils.GetAuthMode(username, clientCertificate, w.path, authMech)
	req, host, err := w.utils.ConstructHttpRequest(connStr, w.path, true, username, password, authMech, authMode, base.MethodGet, base.JsonContentType, body, w.logger)
	if err != nil {
		return errors.New(fmt.Sprintf("ConstructHttpRequest failed for %v%v with error '%v'", connStr, w.path, err.Error()))
	}
	client, err := w.utils.GetHttpClient(username, authMech, certificate, sanInCertificate, clientCertificate, clientKey, host, w.logger)
	if err != nil {
		return errors.New(fmt.Sprintf("GetHttpClient failed for %v%v with error '%v'", connStr, w.path, err.Error()))
	}
	// We may not get a response for a long time after the initial one, so no timeout for this client.
	client.Timeout = 0
	res, err := client.Do(req)
	if err != nil {
		return errors.New(fmt.Sprintf("client.Do failed for %v%v with error '%v'", connStr, w.path, err.Error()))
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return errors.New(fmt.Sprintf("Get http://%v%v received status code %v", connStr, w.path, res.StatusCode))
	}
	errCh := make(chan error)
	reader := bufio.NewReader(res.Body)
	sendError := func(desc string, err error) {
		errorToSend := err
		if len(desc) > 0 {
			errorToSend = fmt.Errorf("Received error '%v' when '%v'", err.Error(), desc)
		}
		select {
		case <-w.finCh:
			// Watcher is stopping
		case <-w.closeCh:
			// Watcher is restarting
		case errCh <- errorToSend:
		}
	}
	// Process the streaming results
	go func() {
		for {
			select {
			case <-w.finCh:
				return
			case <-w.closeCh:
				return
			default:
				bod, err := reader.ReadBytes('\n')
				if err != nil {
					sendError("ReadBytes", err)
					return
				}
				if len(bod) == 1 && bod[0] == '\n' {
					continue
				}
				w.logger.Debugf("GET %v%v returned %q", connStr, w.path, bod)
				out := make(base.InterfaceMap)
				err = json.Unmarshal(bod, &out)
				if err != nil {
					sendError("Unmarshal", err)
					return
				}
				err = w.streamResultCallback(out)
				if err != nil {
					sendError("calling streamResultCallback", err)
				}
			}
		}
	}()

	select {
	case <-w.finCh:
	case <-w.closeCh:
	case err := <-errCh:
		w.handleError(err)
	}
	w.logger.Infof("runStreamingEndpoint for %v:%v is finished.", connStr, w.path)
	return nil
}

func (w *StreamApiWatcherImpl) handleError(err error) {
	// Log the error
	w.logger.Errorf("Path %v encountered error: %v", w.path, err.Error())
	w.mutex.Lock()
	defer w.mutex.Unlock()
	// all go routine should stop after this so we can restart
	if w.running == true {
		close(w.closeCh)
		w.running = false
	}
}

type StreamApiGetterFunc func(path string, connInfo base.ClusterConnectionInfoProvider, utils utils.UtilsIface, callback func(), logger *log.CommonLogger) StreamApiWatcher

func GetStreamApiWatcher(path string, connInfo base.ClusterConnectionInfoProvider, utils utils.UtilsIface, resultCb func(), logger *log.CommonLogger) StreamApiWatcher {
	return NewStreamApiWatcher(path, connInfo, utils, resultCb, logger)
}
