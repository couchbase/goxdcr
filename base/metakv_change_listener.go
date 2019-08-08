// Copyright (c) 2013-2019 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package base

import (
	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/goxdcr/log"
	"sync"
	"time"
)

// Check to see if parent caller (if singleton) is running
type ParentCallerCheckFunc func() bool

type ParentExitFunc func(bool)

// generic listener for metadata stored in metakv
type MetakvChangeListener struct {
	id                         string
	dirpath                    string
	cancel_chan                chan struct{}
	number_of_retry            int
	children_waitgrp           *sync.WaitGroup
	metadata_service_call_back MetadataServiceCallback
	logger                     *log.CommonLogger
	parentCaller               ParentCallerCheckFunc
	parentExit                 ParentExitFunc
}

func NewMetakvChangeListener(id, dirpath string, cancel_chan chan struct{},
	children_waitgrp *sync.WaitGroup,
	metadata_service_call_back MetadataServiceCallback,
	logger_ctx *log.LoggerContext,
	logger_name string,
	parentCaller ParentCallerCheckFunc,
	parentExit ParentExitFunc) *MetakvChangeListener {
	return &MetakvChangeListener{
		id:                         id,
		dirpath:                    dirpath,
		cancel_chan:                cancel_chan,
		children_waitgrp:           children_waitgrp,
		metadata_service_call_back: metadata_service_call_back,
		logger:                     log.NewLogger(logger_name, logger_ctx),
		parentCaller:               parentCaller,
		parentExit:                 parentExit,
	}
}

func (mcl *MetakvChangeListener) Id() string {
	return mcl.id
}

func (mcl *MetakvChangeListener) Start() error {

	mcl.children_waitgrp.Add(1)
	go mcl.observeChildren()

	mcl.logger.Infof("Started MetakvChangeListener %v\n", mcl.Id())
	return nil
}

func (mcl *MetakvChangeListener) Logger() *log.CommonLogger {
	return mcl.logger
}

func (mcl *MetakvChangeListener) observeChildren() {
	defer mcl.children_waitgrp.Done()
	err := metakv.RunObserveChildren(mcl.dirpath, mcl.metakvCallback, mcl.cancel_chan)
	// call failure call back only when there are real errors
	// err may be nil when observeChildren is canceled, in which case there is no need to call failure call back
	mcl.failureCallback(err)
}

// Implement callback function for metakv
// Never returns err since we do not want RunObserveChildren to abort
func (mcl *MetakvChangeListener) metakvCallback(path string, value []byte, rev interface{}) error {
	mcl.logger.Infof("metakvCallback called on listener %v with path = %v\n", mcl.Id(), path)

	go mcl.metakvCallback_async(path, value, rev)

	return nil
}

// Implement callback function for metakv
func (mcl *MetakvChangeListener) metakvCallback_async(path string, value []byte, rev interface{}) {
	err := mcl.metadata_service_call_back(path, value, rev)
	if err != nil {
		mcl.logger.Errorf("Error calling metadata service call back for listener %v. err=%v\n", mcl.Id(), err)
	}

	return

}

// callback function for listener failure event
func (mcl *MetakvChangeListener) failureCallback(err error) {
	mcl.logger.Infof("metakv.RunObserveChildren failed, err=%v\n", err)
	if err == nil && !mcl.parentCaller() {
		//callback is cancelled and replication_mgr is exiting.
		//no-op
		return
	}
	if mcl.number_of_retry < MaxNumOfMetakvRetries {
		// Incremental backoff to wait for metakv server to be ready - and restart listener
		var timeToSleep = time.Duration(mcl.number_of_retry+1) * RetryIntervalMetakv
		// Once we've calculated the timeToSleep correctly, then increment the number_of_retry
		mcl.number_of_retry++
		mcl.logger.Infof("metakv.RunObserveChildren (%v) will retry in %v ...\n", mcl.Id(), timeToSleep)
		time.Sleep(timeToSleep)
		mcl.Start()
	} else {
		// exit process if max retry reached
		mcl.logger.Infof("metakv.RunObserveChildren (%v) failed after max retry %v\n", mcl.Id(), MaxNumOfMetakvRetries)
		mcl.parentExit(false)
	}
}
