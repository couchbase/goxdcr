package cngAgent

import (
	"context"
	"strings"
	"time"

	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/peerToPeer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// allowedToSendHeartbeats checks if the it is allowed to send heartbeats to the remote cluster.
func (hbt *heartBeatManager) allowedToSendHeartbeats(remoteHost, remoteUUID string) bool {
	switch {
	case !base.SrcHeartbeatEnabled:
		return false
	case metadata.IsCapellaHostname(remoteHost):
		return !base.SrcHeartbeatSkipCapellaTarget
	case !base.SrcHeartbeatSkipIntraCluster:
		return true
	default:
		myUUID, err := hbt.services.topologySvc.MyClusterUUID()
		if err != nil {
			// This should ideally never happen
			// Incase it does log a warning and return true to allow the heartbeat to be sent
			// At the worst case, we might end up sending heartbeats to same cluster
			// incase of intra-cluster replication(invalidating SrcHeartbeatSkipIntraCluster)
			hbt.logger.Warnf("agent for remote cluster %v failed to fetch local cluster's UUID: %v", remoteUUID, err.Error())
		}
		return (myUUID != remoteUUID)
	}
}

// specsReaderReady checks if the specs reader is ready.
func (hbt *heartBeatManager) specsReaderReady() bool {
	hbt.mutex.RLock()
	defer hbt.mutex.RUnlock()
	return hbt.specsReader != nil
}

// CanSendHeartbeats checks if the heartbeat manager is allowed to send heartbeats to the remote cluster.
func (hbt *heartBeatManager) CanSendHeartbeats() bool {
	isThisNodeOrchestrator, err := hbt.services.topologySvc.IsOrchestratorNode()
	if err != nil || !isThisNodeOrchestrator {
		return false
	}

	if !hbt.specsReaderReady() {
		return false
	}
	// Note that there is no need to check for source cluster compat, since a cngAgent
	// can only be created if the source cluster version >= 8.1.0 which supports heartbeat.

	// Darshan TODO: Add capability check once MB-68864 is addressed
	return true
}

// generateHeartbeatMetadata generates the heartbeat metadata for the remote cluster.
func (hbt *heartBeatManager) generateHeartbeatMetadata(clonedRef *metadata.RemoteClusterReference) (*metadata.HeartbeatMetadata, error) {
	var err error
	var sourceClusterUUID, sourceClusterName string

	if sourceClusterUUID, err = hbt.services.topologySvc.MyClusterUUID(); err != nil {
		return nil, err
	}
	if sourceClusterName, err = hbt.services.topologySvc.MyClusterName(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(sourceClusterName) == "" {
		sourceClusterName = base.UnknownSourceClusterName
	}

	specs, err := hbt.specsReader.AllReplicationSpecsWithRemote(clonedRef)
	if err != nil {
		return nil, err
	}

	nodesList, err := hbt.services.topologySvc.PeerNodesAdminAddrs()
	if err != nil {
		return nil, err
	}

	srcStr, err := hbt.services.topologySvc.MyHostAddr()
	if err != nil {
		return nil, err
	}
	// Add local node back amongst the peers
	nodesList = append(nodesList, srcStr)

	hbMetadata := &metadata.HeartbeatMetadata{
		SourceClusterUUID: sourceClusterUUID,
		SourceClusterName: sourceClusterName,
		SourceSpecsList:   specs,
		NodesList:         nodesList,
		TTL:               time.Duration(base.SrcHeartbeatExpiryFactor) * base.SrcHeartbeatMaxInterval(),
	}

	return hbMetadata, nil
}

// composeHeartBeatRequest composes the heartbeat request for CNG.
func (hbt *heartBeatManager) composeHeartBeatRequest(metadata *metadata.HeartbeatMetadata, target string) (*peerToPeer.SourceHeartbeatReq, error) {
	srcStr, err := hbt.services.topologySvc.MyHostAddr()
	if err != nil {
		return nil, err
	}

	common := peerToPeer.NewRequestCommon(srcStr, target, "", "", base.GetOpaque(0, uint16(time.Now().UnixNano())))
	req := peerToPeer.NewSourceHeartbeatReq(common).
		SetUUID(metadata.SourceClusterUUID).
		SetClusterName(metadata.SourceClusterName).
		SetNodesList(metadata.NodesList).
		SetTTL(metadata.TTL)
	for _, spec := range metadata.SourceSpecsList {
		req.AppendSpec(spec)
	}
	return req, nil
}

func (heartbeatStats *heartbeatStats) update(response *base.GrpcResponse[*internal_xdcr_v1.HeartbeatResponse]) {
	heartbeatStats.mutex.Lock()
	defer heartbeatStats.mutex.Unlock()
	heartbeatStats.totalHeartbeatsSent++
	if response.Code() == codes.OK {
		heartbeatStats.successfulHeartbeats++
	} else {
		switch response.Code() {
		case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.Unauthenticated, codes.PermissionDenied:
			heartbeatStats.connectionErrors++
		default:
			heartbeatStats.otherErrors++
		}
	}
}

// sendHeartbeat sends the heartbeat to the remote cluster.
func (hbt *heartBeatManager) sendHeartbeat(hbMetadata *metadata.HeartbeatMetadata, getGrpcOpts func() *base.GrpcOptions) *base.GrpcResponse[*internal_xdcr_v1.HeartbeatResponse] {
	grpcOpts := getGrpcOpts()
	req, err := hbt.composeHeartBeatRequest(hbMetadata, grpcOpts.ConnStr)
	if err != nil {
		return &base.GrpcResponse[*internal_xdcr_v1.HeartbeatResponse]{
			Status: status.New(codes.Unknown, err.Error()),
			Error:  err,
		}
	}
	serializedReq, err := req.Serialize()
	if err != nil {
		return &base.GrpcResponse[*internal_xdcr_v1.HeartbeatResponse]{
			Status: status.New(codes.Unknown, err.Error()),
			Error:  err,
		}
	}

	// Darshan TODO: use the global connection pool instead of creating a new connection here
	// This TODO is a placeholder until we have the conn pool checked in
	conn, err := base.NewCngConn(grpcOpts)
	if err != nil {
		return &base.GrpcResponse[*internal_xdcr_v1.HeartbeatResponse]{
			Status: status.New(codes.Unknown, err.Error()),
			Error:  err,
		}
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), base.ShortHttpTimeout)
	defer cancel()

	request := &base.GrpcRequest[*internal_xdcr_v1.HeartbeatRequest]{
		Context: ctx,
		Request: &internal_xdcr_v1.HeartbeatRequest{
			Payload: serializedReq,
		},
	}
	response := hbt.services.utils.CngHeartbeat(conn.Client(), request)
	if response.Code() == codes.OK {
		// In case of success, update the last sent heartbeat metadata
		hbt.mutex.Lock()
		hbt.lastSentHeartbeatMetadata = hbMetadata
		hbt.mutex.Unlock()
	}
	return response
}

func (hbt *heartBeatManager) setUUID(uuid string) {
	hbt.mutex.Lock()
	defer hbt.mutex.Unlock()
	hbt.remoteClusterUuid = uuid
}

func (hbt *heartBeatManager) UUID() string {
	hbt.mutex.RLock()
	defer hbt.mutex.RUnlock()
	return hbt.remoteClusterUuid
}

// printHeartbeatStats prints the heartbeat statistics.
func (hbt *heartBeatManager) printHeartbeatStats() {
	hbt.heartbeatStats.mutex.RLock()
	defer hbt.heartbeatStats.mutex.RUnlock()
	hbt.logger.Infof("Heartbeat statistics for remote cluster %v: total=%v, successful=%v, connectionErrors=%v, otherErrors=%v",
		hbt.UUID(), hbt.heartbeatStats.totalHeartbeatsSent, hbt.heartbeatStats.successfulHeartbeats, hbt.heartbeatStats.connectionErrors, hbt.heartbeatStats.otherErrors)
}

// runHeartbeatSender runs the heartbeat sender routine.
func (agent *RemoteCngAgent) runHeartbeatSender() {
	defer agent.waitGrp.Done()

	agent.refCache.mutex.RLock()
	refName := agent.refCache.reference.Name()
	agent.refCache.mutex.RUnlock()

	agent.logger.Infof("Heartbeat sender for remote cluster %s is started", refName)

	// SrcHeartbeatMinInterval defines how frequently to check for changes since the last heartbeat.
	minHBIntervalTicker := time.NewTicker(base.SrcHeartbeatMinInterval)
	defer minHBIntervalTicker.Stop()

	// SrcHeartbeatMaxInterval denotes the interval at which the heartbeat is sent.
	maxHBIntervalTicker := time.NewTicker(base.SrcHeartbeatMaxInterval())
	defer maxHBIntervalTicker.Stop()

	// printHeartbeatStatsTicker denotes the interval at which the heartbeat statistics are printed.
	printHeartbeatStatsTicker := time.NewTicker(base.SrcHeartbeatSummaryInterval)
	defer printHeartbeatStatsTicker.Stop()

	for {
		select {
		case <-agent.finCh:
			agent.logger.Infof("Heartbeat sender for remote cluster %v is stopped", refName)
			return
		case <-minHBIntervalTicker.C:
			if !agent.heartbeatManager.CanSendHeartbeats() {
				agent.logger.Debugf("%v: heartbeat can't be sent yet. Retry in the next interval.", refName)
				continue
			}

			clonedRef, _ := agent.GetReferenceClone(false)
			hbMetadata, err := agent.heartbeatManager.generateHeartbeatMetadata(clonedRef)
			if err != nil {
				agent.logger.Warnf("%v: failed to generate heartbeat metadata for cluster %v due to: %v", refName, clonedRef.Uuid(), err)
				continue
			}

			// check if the heartbeat metadata has changed since the last heartbeat
			agent.heartbeatManager.mutex.RLock()
			if agent.heartbeatManager.lastSentHeartbeatMetadata.SameAs(hbMetadata) {
				agent.logger.Debugf("%v: no metadata change for cluster %v since last heartbeat", refName, clonedRef.Uuid())
				agent.heartbeatManager.mutex.RUnlock()
				continue
			}
			agent.heartbeatManager.mutex.RUnlock()
			// send the heartbeat
			response := agent.heartbeatManager.sendHeartbeat(hbMetadata, agent.GetGrpcOpts)
			agent.heartbeatManager.heartbeatStats.update(response)
			// delay the next heartbeat by atmost SrcHeartbeatMaxInterval
			maxHBIntervalTicker.Reset(base.SrcHeartbeatMaxInterval())
		case <-maxHBIntervalTicker.C:
			if !agent.heartbeatManager.CanSendHeartbeats() {
				agent.logger.Debugf("%v: heartbeat can't be sent yet. Retry in the next interval.", refName)
				continue
			}

			clonedRef, _ := agent.GetReferenceClone(false)
			hbMetadata, err := agent.heartbeatManager.generateHeartbeatMetadata(clonedRef)
			if err != nil {
				agent.logger.Warnf("%v: failed to generate heartbeat metadata for cluster %v due to: %v", refName, clonedRef.Uuid(), err)
				continue
			}
			response := agent.heartbeatManager.sendHeartbeat(hbMetadata, agent.GetGrpcOpts)
			agent.heartbeatManager.heartbeatStats.update(response)
		case <-printHeartbeatStatsTicker.C:
			agent.heartbeatManager.printHeartbeatStats()
		}
	}
}
