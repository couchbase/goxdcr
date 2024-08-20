// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package peerToPeer

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/peerToPeer/peerToPeerResults"
	"github.com/couchbase/goxdcr/v8/service_def"
	"github.com/couchbase/goxdcr/v8/utils"
)

const maxWaitBeforeRPCInMs = 10000 // 10 seconds

type mccClientIfcWithErr struct {
	clientIfc mcc.ClientIface
	err       error
}

// helper function to establish connections to all the target nodes. Returns a hostname -> list of connection errors mapping
func executeConnectionPreCheck(ref *metadata.RemoteClusterReference, targetNodes, srvLookupResults []string, portsMap base.HostPortMapType, utils utils.UtilsIface, logger *log.CommonLogger) base.HostToErrorsMapType {
	result := make(base.HostToErrorsMapType)

	// before performing actual RCPs, perform a trivial DNS resolution check if the user input is DNS SRV.
	if len(srvLookupResults) > 0 {
		// check if there any errors in local DNS SRV lookup
		var errs []string
		err := ref.PopulateDnsSrvIfNeeded(logger)
		if err != nil {
			errMsg := fmt.Sprintf("Error while performing DNS SRV lookup in one of the peer nodes, err=%v", err)
			logger.Errorf("%v, srvLookupResults=%v", errMsg, srvLookupResults)
			errs = append(errs, errMsg)
		} else {
			// also check if the looked up entries in this node are same as that of the initiating node
			lookupResults := ref.GetSRVHostNames()
			if !base.EqualStringLists(srvLookupResults, lookupResults) {
				errMsg := "Mismatch in the DNS lookup results between nodes of the same cluster"
				logger.Errorf("%v, srvLookupResults=%v, lookupResults=%v", errMsg, srvLookupResults, lookupResults)
				errs = append(errs, errMsg)
			}
		}

		if len(errs) != 0 {
			result[base.PlaceHolderFieldKey] = errs
		}
	}

	// perform various connection checks now with the pre-computed targetNodes list from the initiating node,
	// irrespective of the DNS lookup errors above.
	var muForResult sync.Mutex
	var wgForResult sync.WaitGroup
	for _, hostAddr := range targetNodes {
		result[hostAddr] = []string{}

		wgForResult.Add(2)

		chNS := make(chan []error, 1)
		chKV := make(chan []error, 1)

		go connectToRemoteNS(ref, hostAddr, portsMap, chNS, utils, logger)
		go connectToRemoteKV(ref, hostAddr, portsMap, chKV, utils, logger)

		go collectResults(result, hostAddr, chNS, &wgForResult, &muForResult)
		go collectResults(result, hostAddr, chKV, &wgForResult, &muForResult)
	}

	wgForResult.Wait()
	logger.Infof("Errors while connecting to remote reference name=%v and uuid=%v: %v", ref.Name(), ref.Uuid(), result)
	return result
}

// helper function to connect to the ns_server of a given target node
func connectToRemoteNS(ref *metadata.RemoteClusterReference, hostAddr string, portsMap base.HostPortMapType, ch chan<- []error, utils utils.UtilsIface, logger *log.CommonLogger) {
	// sleep for a random amount of time to not overwhelm the target
	numMilliSec := rand.Intn(maxWaitBeforeRPCInMs)
	ticker := time.NewTicker(time.Duration(numMilliSec) * time.Millisecond)
	logger.Debugf("connectToRemoteNS(hostAddr=%v) sleeping for %v milliseconds", hostAddr, numMilliSec)
	<-ticker.C

	errs := make([]error, 0)
	defer func() { ch <- errs }()

	username, password, authMech, cert, SANInCert, clientCert, clientKey, err := ref.MyCredentials()
	if err != nil {
		errs = append(errs, err)
	}

	hostname := base.GetHostName(hostAddr)
	portsInfo, ok := portsMap[hostAddr]
	if !ok {
		err = errors.New("No port information found for hostname=" + hostname)
		errs = append(errs, err)
		return
	}

	var out interface{}
	var port uint16
	if !ref.DemandEncryption() || ref.EncryptionType() == metadata.EncryptionType_Half {
		port, ok = portsInfo[base.PortsKeysForConnectionPreCheck[base.MgmtIdxForConnPreChk]]
		if !ok {
			err = errors.New("No Mgt port information found for hostname=" + hostname)
			errs = append(errs, err)
			return
		}
	} else {
		port, ok = portsInfo[base.PortsKeysForConnectionPreCheck[base.MgmtSSLIdxForConnPreChk]]
		if !ok {
			err = errors.New("No MgtSSL port information found for hostname=" + hostname)
			errs = append(errs, err)
			return
		}
	}

	hostAddr = base.GetHostAddr(hostname, port)

	stopFunc := utils.StartDiagStopwatch(fmt.Sprintf("connectToRemoteNS(%v)", hostAddr), base.DiagInternalThreshold)
	err, _ = utils.QueryRestApiWithAuth(hostAddr, base.WhoAmIPath, false, username, password, authMech, cert, SANInCert, clientCert,
		clientKey, base.MethodGet, base.JsonContentType, nil, base.ConnectionPreCheckRPCTimeout, &out, nil, false, logger)
	timeTaken := stopFunc()

	if err != nil {
		errs = append(errs, err)
		return
	}

	// Warning related to possible network health issue
	if timeTaken > base.NWLatencyToleranceMilliSec {
		logger.Warnf("connectToRemoteNS(hostAddr=%v) slept for %vms and took %v for network call, which is above accepted threshold of %v", hostAddr, numMilliSec, timeTaken, base.NWLatencyToleranceMilliSec)
		err = fmt.Errorf("WARN: Connection check was successful, no errors, but the network call to remote server took longer than expected threshold. Actual=%v, Expected=%v", timeTaken, base.NWLatencyToleranceMilliSec)
		errs = append(errs, err)
	}
}

// helper function to connect to the KV of a given target node
func connectToRemoteKV(ref *metadata.RemoteClusterReference, hostAddr string, portsMap base.HostPortMapType, ch chan<- []error, utils utils.UtilsIface, logger *log.CommonLogger) {
	// sleep for a random amount of time to not overwhelm the target
	numMilliSec := rand.Intn(maxWaitBeforeRPCInMs)
	ticker := time.NewTicker(time.Duration(numMilliSec) * time.Millisecond)
	logger.Debugf("connectToRemoteKV(hostAddr=%v) sleeping for %v milliseconds", hostAddr, numMilliSec)
	<-ticker.C

	errs := make([]error, 0)
	defer func() { ch <- errs }()

	hostname := base.GetHostName(hostAddr)

	username, password, _, cert, SANInCert, clientCert, clientKey, err := ref.MyCredentials()
	if err != nil {
		errs = append(errs, err)
	}

	portsInfo, ok := portsMap[hostAddr]
	if !ok {
		err = errors.New("no port information found for hostname=" + hostname)
		errs = append(errs, err)
		return
	}

	var port uint16
	if !ref.DemandEncryption() || ref.EncryptionType() == metadata.EncryptionType_Half {
		port, ok = portsInfo[base.PortsKeysForConnectionPreCheck[base.KVIdxForConnPreChk]]
		if !ok {
			err = errors.New("no KV port information found for hostname=" + hostname)
			errs = append(errs, err)
			return
		}

	} else {
		port, ok = portsInfo[base.PortsKeysForConnectionPreCheck[base.KVSSLIdxForConnPreChk]]
		if !ok {
			err = errors.New("no KVSSL port information found for hostname=" + hostname)
			errs = append(errs, err)
			return
		}
	}
	resultCh := make(chan mccClientIfcWithErr, 1)

	stopFunc := utils.StartDiagStopwatch(fmt.Sprintf("connectToRemoteKV(%v)", hostAddr), base.DiagInternalThreshold)
	go func() {
		var conn mcc.ClientIface
		var err error
		if !ref.DemandEncryption() || ref.EncryptionType() == metadata.EncryptionType_Half {
			hostAddr = base.GetHostAddr(hostname, uint16(port))
			conn, err = base.NewConn(hostAddr, username, password, "", !ref.DemandEncryption(), base.KeepAlivePeriod, logger)
		} else {
			ssl_con_str := base.GetHostAddr(hostname, uint16(port))
			conn, err = base.NewTLSConn(ssl_con_str, username, password, cert, SANInCert, clientCert, clientKey, "", logger)
		}
		select {
		case resultCh <- mccClientIfcWithErr{clientIfc: conn, err: err}:
			return
		default:
			return
		}
	}()

	ticker = time.NewTicker(base.ConnectionPreCheckRPCTimeout)
	var client mcc.ClientIface = nil

	select {
	case <-ticker.C:
		errs = append(errs, fmt.Errorf("timeout encountered before successfully connecting to remote KV for hostAddr=%v", hostAddr))
		return
	case result := <-resultCh:
		ticker.Stop()
		client = result.clientIfc
		err = result.err
		if err != nil {
			errs = append(errs, err)
			return
		}
	}

	timeTaken := stopFunc()

	if client == nil {
		errs = append(errs, fmt.Errorf("nil client in connectToRemoteKV for hostAddr=%v", hostAddr))
		return
	}

	err = client.Close()
	if err != nil {
		errs = append(errs, err)
		return
	}

	// Warning related to possible network health issue
	if timeTaken > base.NWLatencyToleranceMilliSec {
		logger.Warnf("connectToRemoteKV(hostAddr=%v) slept for %vms and took %v for network call, which is above accepted threshold of %v", hostAddr, numMilliSec, timeTaken, base.NWLatencyToleranceMilliSec)
		err = fmt.Errorf("WARN: Connection check was successful, no errors, but the network call to remote memcached took longer than expected threshold. Actual=%v, Expected=%v", timeTaken, base.NWLatencyToleranceMilliSec)
		errs = append(errs, err)
	}
}

// collect the result from the parallely running go routines - connectToRemoteNS() and connectToRemoteKV()
func collectResults(result base.HostToErrorsMapType, hostAddr string, ch <-chan []error, wg *sync.WaitGroup, mu *sync.Mutex) {
	defer wg.Done()

	errNS := <-ch
	for _, err := range errNS {
		if err != nil {
			mu.Lock()
			result[hostAddr] = append(result[hostAddr], fmt.Sprintf("%v", err))
			mu.Unlock()
		}
	}
}

type ConnectionPreCheckHandler struct {
	*HandlerCommon
	utils utils.UtilsIface
}

func NewConnectionPreCheckHandler(reqCh []chan interface{}, logger *log.CommonLogger, lifecycleId string, cleanupInterval time.Duration, utils utils.UtilsIface, replicationSpecSvc service_def.ReplicationSpecSvc) *ConnectionPreCheckHandler {
	finCh := make(chan bool)
	handler := &ConnectionPreCheckHandler{
		HandlerCommon: NewHandlerCommon("ConnectionPreCheckHandler", logger, lifecycleId, finCh, cleanupInterval, reqCh, replicationSpecSvc),
		utils:         utils,
	}
	return handler
}

func (h *ConnectionPreCheckHandler) Start() error {
	h.HandlerCommon.Start()
	go h.handler()
	return nil
}

func (h *ConnectionPreCheckHandler) Stop() error {
	close(h.finCh)
	return nil
}

func (h *ConnectionPreCheckHandler) handler() {
	for {
		select {
		case <-h.finCh:
			return
		case req := <-h.receiveReqCh:
			// Can be either req or response
			preCheckReq, isReq := req.(*ConnectionPreCheckReq)
			if isReq {
				h.handleRequest(preCheckReq)
			}
		case resp := <-h.receiveRespCh:
			preCheckRes, isResp := resp.(*ConnectionPreCheckRes)
			if isResp {
				h.handleResponse(preCheckRes)
			}
		}
	}
}

func (h *ConnectionPreCheckHandler) handleRequest(req *ConnectionPreCheckReq) {
	results := executeConnectionPreCheck(req.TargetRef, req.TargetClusterNodes, req.SrvLookupResults, req.PortsMap, h.utils, h.logger)
	req.ConnectionErrs = results

	resp := req.GenerateResponse().(*ConnectionPreCheckRes)

	handlerResult, err := req.CallBack(resp)
	if err != nil || handlerResult.GetError() != nil {
		h.logger.Errorf("Unable to send resp %v to original req %v - %v %v", resp, req, err, handlerResult.GetError())
	}
}

func (h *ConnectionPreCheckHandler) handleResponse(resp *ConnectionPreCheckRes) {
	_, _, found := h.GetReqAndClearOpaque(resp.GetOpaque())
	if !found {
		h.logger.Errorf("ConnectionPreCheck Response Handler: Unable to find opaque %v", resp.GetOpaque())
		return
	}

	store := peerToPeerResults.GetConnectionPreCheckStore(h.logger)

	err := store.SetToConnectionPreCheckStoreSpecificTarget(resp.TaskId, "P2P/"+resp.Target, resp.Sender, []string{base.ConnectionPreCheckMsgs[base.ConnPreChkResponseObtained]})
	if err != nil {
		h.logger.Errorf("SetToConnectionPreCheckStore resulted in %v", err)
	}

	connErrs := resp.ConnectionErrs

	for _, node := range resp.TargetClusterNodes {
		if connErrs == nil {
			connErrs = make(base.HostToErrorsMapType)
		}
		_, ok := connErrs[node]
		if !ok || len(connErrs[node]) == 0 {
			connErrs[node] = []string{base.ConnectionPreCheckMsgs[base.ConnPreChkSuccessful]}
		}
	}

	err = store.SetToConnectionPreCheckStore(resp.TaskId, resp.Sender, connErrs)
	if err != nil {
		h.logger.Errorf("ConnectionPreCheck Response Handler: Error while SetToConnectionPreCheckStore=%v", err)
	}

	err = store.SetToConnectionPreCheckStoreSpecificTarget(resp.TaskId, "P2P/"+resp.Target, resp.Sender, []string{base.ConnectionPreCheckMsgs[base.ConnPreChkP2PSuccessful]})
	if err != nil {
		h.logger.Errorf("ConnectionPreCheck Response Handler: Error while SetToConnectionPreCheckStoreSpecificTarget=%v", err)
	}
}
