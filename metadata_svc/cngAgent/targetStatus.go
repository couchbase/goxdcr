// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package cngAgent

import (
	"fmt"
	"strings"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/metadata_svc"
	"google.golang.org/grpc/codes"
)

var _ metadata_svc.RemoteAgentClusterStatus = &RemoteCngAgent{}

func (ht *targetHealthTracker) incrementConnErrCount() {
	ht.mutex.Lock()
	defer ht.mutex.Unlock()
	ht.connErrCount++
}

func (ht *targetHealthTracker) resetConnErrCount() {
	ht.mutex.Lock()
	defer ht.mutex.Unlock()
	ht.connErrCount = 0
}

func (ht *targetHealthTracker) getConnErrCount() int {
	ht.mutex.RLock()
	defer ht.mutex.RUnlock()
	return ht.connErrCount
}

func (ht *targetHealthTracker) getAuthErrorReportStatus() metadata_svc.AuthErrReportStatus {
	ht.mutex.RLock()
	defer ht.mutex.RUnlock()
	return ht.authErrReportStatus
}

func (ht *targetHealthTracker) setAuthErrorReportStatus(val metadata_svc.AuthErrReportStatus) {
	ht.mutex.Lock()
	defer ht.mutex.Unlock()
	ht.authErrReportStatus = val
}

// updateHealth updates the health status of the target based on the gRPC status code and error.
func (ht *targetHealthTracker) updateHealth(target string, statusCode codes.Code, err error) {
	var resetConnErrors bool

	switch statusCode {
	case codes.Unauthenticated:
		changed, _ := ht.connectivityHelper.MarkNode(target, metadata.ConnAuthErr)
		if changed {
			ht.services.uiLog.Write(fmt.Sprintf("An authentication error occured while connecting to %s. Please check the remote reference credentials", target))
		}
		resetConnErrors = true
	case codes.OK:
		_, authErrFixed := ht.connectivityHelper.MarkNode(target, metadata.ConnValid)
		if authErrFixed {
			ht.services.uiLog.Write(fmt.Sprintf("The remote reference credentials to %s have now been fixed", target))
			ht.mutex.Lock()
			ht.configurationChanged = true
			ht.mutex.Unlock()
		}
		resetConnErrors = true
	default:
		// Any other error code is treated as a connection error
		ht.connectivityHelper.MarkNode(target, metadata.ConnError)
		// Increment the connection error count
		ht.incrementConnErrCount()

		ht.registerConnErr(metadata.ConnErr{
			FirstOccurence: time.Now(),
			TargetNode:     target,
			Cause:          fmt.Sprintf("statusCode=%v,err=%v", statusCode, err),
			Occurences:     1,
		})

		// Check if the error is due to IP family mismatch
		if err != nil && strings.Contains(err.Error(), base.IpFamilyOnlyErrorMessage) {
			ht.connectivityHelper.MarkIpFamilyError(true)
		} else {
			ht.connectivityHelper.MarkIpFamilyError(false)
		}
	}

	if resetConnErrors {
		// reset any previous connection error count
		ht.resetConnErrCount()
		// Clear any previous connection errors recorded on the reference
		ht.clearConnErrs()
		// clear any IP family error
		ht.connectivityHelper.MarkIpFamilyError(false)
	}
}

func (agent *RemoteCngAgent) ConfigurationHasChanged() bool {
	agent.healthTracker.mutex.RLock()
	defer agent.healthTracker.mutex.RUnlock()

	return agent.healthTracker.configurationChanged
}

// GetConnectivityStatus returns the current connectivity status of the remote cluster.
// If there is a connection error, it checks if the error has persisted beyond a threshold
// to determine whether to report ConnDegraded or ConnError.
func (agent *RemoteCngAgent) GetConnectivityStatus() metadata.ConnectivityStatus {
	connStatus := agent.healthTracker.connectivityHelper.GetNodeStatus(agent.HostName())
	if connStatus != metadata.ConnError {
		return connStatus
	}
	// If there is a connection error, check if it has persisted beyond the threshold
	// depending on which we report either ConnDegraded or ConnError
	if agent.healthTracker.getConnErrCount() < base.MaxAllowedRCDegradedCyclesForCng {
		return metadata.ConnDegraded
	}
	return metadata.ConnError
}

func (agent *RemoteCngAgent) GetUnreportedAuthError() bool {
	currentStatus := agent.GetConnectivityStatus()
	agentStatus := agent.healthTracker.getAuthErrorReportStatus()

	if currentStatus == metadata.ConnValid {
		// connection is valid, reset the auth error report status if it was reported before
		if agentStatus == metadata_svc.AuthErrReported {
			agent.healthTracker.setAuthErrorReportStatus(metadata_svc.AuthErrNotReported)
		}
		return false
	} else if currentStatus == metadata.ConnAuthErr && agentStatus == metadata_svc.AuthErrNotReported {
		// This agent hasn't reported auth error before, this will be the first time
		agent.healthTracker.setAuthErrorReportStatus(metadata_svc.AuthErrReported)
		return true
	}

	// Everything else do not report
	return false
}

// ResetConfigChangeState resets the configurationChanged flag in the health tracker
func (agent *RemoteCngAgent) ResetConfigChangeState() {
	agent.healthTracker.mutex.Lock()
	defer agent.healthTracker.mutex.Unlock()
	agent.healthTracker.configurationChanged = false
}
