package pipeline_utils

import (
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/parts"
	"github.com/couchbase/goxdcr/service_def"
	"time"
)

var ErrorNoSourceKV = errors.New("Invalid configuration. No source kv node is found.")

type Action func() error

func ExecWithTimeout(action Action, timeout_duration time.Duration, logger *log.CommonLogger) error {
	ret := make(chan error, 1)
	go func(finch chan error) {
		logger.Info("About to call function")
		err1 := action()
		ret <- err1
	}(ret)

	var retErr error
	timeoutticker := time.NewTicker(timeout_duration)
	for {
		select {
		case retErr = <-ret:
			logger.Infof("Finish executing %v\n", action)
			return retErr
		case <-timeoutticker.C:
			retErr = errors.New(fmt.Sprintf("Executing %v timed out", action))
			logger.Infof("Executing %v timed out", action)
			logger.Info("****************************")
			return retErr
		}
	}

}

func GetSourceVBListForReplication(cluster_info_svc service_def.ClusterInfoSvc, xdcr_topology_svc service_def.XDCRCompTopologySvc,
	spec *metadata.ReplicationSpecification, logger *log.CommonLogger) (map[string][]uint16, error) {
	kv_vb_map := make(map[string][]uint16)
	server_vbmap, err := cluster_info_svc.GetServerVBucketsMap(xdcr_topology_svc, spec.SourceBucketName)
	if err != nil {
		return nil, err
	}

	logger.Debugf("server_vbmap=%v\n", server_vbmap)
	nodes, err := xdcr_topology_svc.MyKVNodes()
	if err != nil {
		logger.Errorf("Failed to get my KV nodes, err=%v\n", err)
		return nil, err
	}

	if len(nodes) == 0 {
		return nil, ErrorNoSourceKV
	}

	for _, node := range nodes {
		var kvaddr string = ""
		var vbnos []uint16
		// iterate through serverVBMap and look for server addr that starts with "kvHost:"
		for kvaddr_iter, vbnos_iter := range server_vbmap {
			if kvaddr_iter == node {
				kvaddr = kvaddr_iter
				vbnos = vbnos_iter
				break
			}
		}
		if kvaddr != "" {
			kv_vb_map[kvaddr] = vbnos
		}
	}
	return kv_vb_map, nil
}

func GetSourceVBListPerPipeline(pipeline common.Pipeline) []uint16 {
	ret := []uint16{}
	sourceNozzles := pipeline.Sources()
	for _, sourceNozzle := range sourceNozzles {
		ret = append(ret, sourceNozzle.(*parts.DcpNozzle).GetVBList()...)
	}
	return ret
}
