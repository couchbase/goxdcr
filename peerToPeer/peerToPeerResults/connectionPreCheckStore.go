// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package peerToPeerResults

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
)

type connectionPreCheckResults struct {
	connectionErrs base.ConnectionErrMapType
	numResponses   int
	numPeers       int
}

type connectionPreCheckStoreSingleton struct {
	results map[string]*connectionPreCheckResults
	logger  *log.CommonLogger
}

var connectionPreCheckStore *connectionPreCheckStoreSingleton = nil
var storeLock sync.Mutex

// get the singleton object
func GetConnectionPreCheckStore(logger *log.CommonLogger) *connectionPreCheckStoreSingleton {
	storeLock.Lock()
	defer storeLock.Unlock()

	if connectionPreCheckStore == nil {
		connectionPreCheckStore = &connectionPreCheckStoreSingleton{results: make(map[string]*connectionPreCheckResults), logger: logger}
	}
	return connectionPreCheckStore
}

// get the connection pre-check results from the store for a given pre-check task-id. base.ConnectionErrMapType is a sourceNode -> <targetNode -> list of errors> mapping
func (s *connectionPreCheckStoreSingleton) GetFromConnectionPreCheckStore(taskId string) (base.ConnectionErrMapType, bool, error) {
	storeLock.Lock()
	defer storeLock.Unlock()

	result := s.results
	preCheckResult, ok := result[taskId]
	if !ok {
		errMsg := fmt.Sprintf("No results found for taskId=%v in GetFromConnectionPreCheckStore", taskId)
		s.logger.Warnf(errMsg)
		return nil, false, errors.New(errMsg)
	}

	return preCheckResult.connectionErrs, preCheckResult.numResponses == preCheckResult.numPeers, nil
}

// set the results to the store for a given pre-check task-id and source node
func (s *connectionPreCheckStoreSingleton) SetToConnectionPreCheckStore(taskId string, myHostAddr string, connErrs base.HostToErrorsMapType) error {
	storeLock.Lock()
	defer storeLock.Unlock()

	_, ok := s.results[taskId]
	if !ok {
		errMsg := fmt.Sprintf("Invalid Task ID=%v in SetToConnectionPreCheckStore with HostAddr=%v", taskId, myHostAddr)
		s.logger.Errorf(errMsg)
		return errors.New(errMsg)
	}
	errs := s.results[taskId].connectionErrs
	if errs == nil {
		s.results[taskId].connectionErrs = make(base.ConnectionErrMapType)
	}

	s.results[taskId].connectionErrs[myHostAddr] = connErrs

	s.results[taskId].numResponses++

	return nil
}

// set the results to the store for a given pre-check task-id, source node and target node
func (s *connectionPreCheckStoreSingleton) SetToConnectionPreCheckStoreSpecificTarget(taskId string, myHostAddr string, targetHostAddr string, connectionErr []string) error {
	storeLock.Lock()
	defer storeLock.Unlock()

	_, ok := s.results[taskId]
	if !ok {
		errMsg := fmt.Sprintf("Invalid Task ID=%v in SetToConnectionPreCheckStoreSpecificTarget with HostAddr=%v and target node HostAddr=%v", taskId, myHostAddr, targetHostAddr)
		s.logger.Errorf(errMsg)
		return errors.New(errMsg)
	}
	errs := s.results[taskId].connectionErrs
	if errs == nil {
		s.results[taskId].connectionErrs = make(base.ConnectionErrMapType)
	}

	_, ok = s.results[taskId].connectionErrs[myHostAddr]
	if !ok {
		errMsg := fmt.Sprintf("Invalid HostAddr=%v in SetToConnectionPreCheckStoreSpecificTarget for taskID=%v and target node HostAddr=%v", myHostAddr, taskId, targetHostAddr)
		s.logger.Errorf(errMsg)
		return errors.New(errMsg)
	}

	_, ok = s.results[taskId].connectionErrs[myHostAddr][targetHostAddr]
	if !ok {
		errMsg := fmt.Sprintf("Invalid target node HostAddr=%v in SetToConnectionPreCheckStoreSpecificTarget for taskID=%v and HostAddr=%v", targetHostAddr, taskId, myHostAddr)
		s.logger.Errorf(errMsg)
		return errors.New(errMsg)
	}

	s.results[taskId].connectionErrs[myHostAddr][targetHostAddr] = connectionErr

	return nil
}

// initialize the data-structures for connection pre-check result for a given task-ID
func (s *connectionPreCheckStoreSingleton) InitConnectionPreCheckResults(taskId string, myHostAddr string, peers []string, connectionErrs base.HostToErrorsMapType) error {
	storeLock.Lock()
	defer storeLock.Unlock()

	s.results[taskId] = &connectionPreCheckResults{
		connectionErrs: make(base.ConnectionErrMapType),
		numResponses:   0,
		numPeers:       len(peers),
	}

	go s.runGC(taskId, base.ConnectionPreCheckGCTimeout)

	_, ok := s.results[taskId]
	if !ok {
		// should not get here
		errMsg := fmt.Sprintf("Invalid Task ID=%v in InitConnectionPreCheckResults with HostAddr=%v", taskId, myHostAddr)
		s.logger.Errorf(errMsg)
		return errors.New(errMsg)
	}
	errs := s.results[taskId].connectionErrs
	if errs == nil {
		s.results[taskId].connectionErrs = make(base.ConnectionErrMapType)
	}

	s.results[taskId].connectionErrs[myHostAddr] = connectionErrs

	if len(peers) != 0 {
		p2pKey := "P2P/" + myHostAddr

		s.results[taskId].connectionErrs[p2pKey] = make(base.HostToErrorsMapType)
		for _, peer := range peers {
			if peer != myHostAddr {
				s.results[taskId].connectionErrs[p2pKey][peer] = []string{base.ConnectionPreCheckMsgs[base.ConnPreChkSendingRequest]}
			}
		}
	}

	return nil
}

// delete the pre-check results after numSeconds amount of time from the store for a given task-id
func (s *connectionPreCheckStoreSingleton) runGC(taskId string, numSeconds time.Duration) {
	ticker := time.NewTicker(numSeconds)
	s.logger.Debugf("Started GC for connection pre-check results with taskId=%v with timeout=%v seconds", taskId, numSeconds)

	select {
	case <-ticker.C:
		storeLock.Lock()
		if _, ok := s.results[taskId]; ok {
			delete(s.results, taskId)
			s.logger.Debugf("Deleted connection pre-check results with taskId=%v", taskId)
		}
		storeLock.Unlock()
		break
	}

}
