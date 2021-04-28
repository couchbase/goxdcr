// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package pipeline_utils

import (
	"errors"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/parts"
	"github.com/couchbase/goxdcr/service_def"
	"strconv"
	"strings"
)

var ErrorNoSourceKV = errors.New("Invalid configuration. No source kv node is found.")

func GetSourceVBListPerPipeline(pipeline common.Pipeline) []uint16 {
	ret := []uint16{}
	sourceNozzles := pipeline.Sources()
	for _, sourceNozzle := range sourceNozzles {
		responsibleVBs := sourceNozzle.ResponsibleVBs()
		ret = append(ret, responsibleVBs...)
	}
	return ret
}

/**
 * Returns a map of: kvServerNode -> vBuckets that it is responsible for
 */
func GetSourceVBMap(cluster_info_svc service_def.ClusterInfoSvc, xdcr_topology_svc service_def.XDCRCompTopologySvc,
	sourceBucketName string, logger *log.CommonLogger) (kv_vb_map map[string][]uint16, number_of_source_nodes int, err error) {
	kv_vb_map = make(map[string][]uint16)

	server_vbmap, err := cluster_info_svc.GetLocalServerVBucketsMap(xdcr_topology_svc, sourceBucketName)
	if err != nil {
		return
	}

	number_of_source_nodes = len(server_vbmap)

	logger.Debugf("server_vbmap=%v\n", server_vbmap)
	nodes, err := xdcr_topology_svc.MyKVNodes()
	if err != nil {
		logger.Errorf("Failed to get my KV nodes, err=%v\n", err)
		return
	}

	if len(nodes) == 0 {
		err = ErrorNoSourceKV
		return
	}

	for _, node := range nodes {
		if vbnos, ok := server_vbmap[node]; ok {
			kv_vb_map[node] = vbnos
		}
	}
	return
}

func GetElementIdFromName(pipeline common.Pipeline, name string) string {
	return pipeline.FullTopic() + "_" + name
}

func GetElementIdFromNameAndIndex(pipeline common.Pipeline, name string, index int) string {
	return pipeline.FullTopic() + "_" + name + "_" + strconv.Itoa(index)
}

// as the name indicates, this method works only on element ids with index at the end
func GetElementNameFromIdWithIndex(id string) string {
	parts := strings.Split(id, "_")
	if len(parts) > 2 {
		return parts[len(parts)-2]
	}
	return ""
}

func RegisterAsyncComponentEventHandler(listenerMap map[string]common.AsyncComponentEventListener, listenerName string, handler common.AsyncComponentEventHandler) {
	for id, listener := range listenerMap {
		if listenerName == GetElementNameFromIdWithIndex(id) {
			listener.RegisterComponentEventHandler(handler)
		}
	}

}

func IsPipelineUsingCapi(pipeline common.Pipeline) bool {
	isCapi := false
	for _, target := range pipeline.Targets() {
		_, isCapi = target.(*parts.CapiNozzle)
		break
	}
	return isCapi
}

// check if the specified state indicates that the pipeline is still running
// many components, e.g., topology change detector, use this to decide if they need to abort operation
func IsPipelineRunning(state common.PipelineState) bool {
	return state == common.Pipeline_Initial || state == common.Pipeline_Starting || state == common.Pipeline_Running
}

func IsPipelineStopping(state common.PipelineState) bool {
	return state == common.Pipeline_Stopping || state == common.Pipeline_Stopped
}

// get first seen xattr seqno for each vbucket in pipeline
func GetXattrSeqnos(pipeline common.Pipeline) map[uint16]uint64 {
	ret := make(map[uint16]uint64)
	sourceNozzles := pipeline.Sources()
	for _, sourceNozzle := range sourceNozzles {
		xattr_seqnos := sourceNozzle.(*parts.DcpNozzle).GetXattrSeqnos()
		for vbno, xattr_seqno := range xattr_seqnos {
			ret[vbno] = xattr_seqno
		}
	}
	return ret
}
