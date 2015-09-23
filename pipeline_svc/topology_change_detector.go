package pipeline_svc

import (
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	comp "github.com/couchbase/goxdcr/component"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/parts"
	"github.com/couchbase/goxdcr/pipeline_utils"
	"github.com/couchbase/goxdcr/service_def"
	"sync"
	"time"
)

var source_topology_changedErr = errors.New("Topology has changed on source cluster")
var target_cluster_versionChangeErr = errors.New("Target cluster version has moved to 3.0 or above")

type TopologyChangeDetectorSvc struct {
	*comp.AbstractComponent

	//xdcr topology service
	xdcr_topology_svc  service_def.XDCRCompTopologySvc
	cluster_info_svc   service_def.ClusterInfoSvc
	remote_cluster_svc service_def.RemoteClusterSvc
	logger             *log.CommonLogger
	pipeline           common.Pipeline
	finish_ch          chan bool
	wait_grp           *sync.WaitGroup
}

func NewTopologyChangeDetectorSvc(cluster_info_svc service_def.ClusterInfoSvc,
	xdcr_topology_svc service_def.XDCRCompTopologySvc,
	remote_cluster_svc service_def.RemoteClusterSvc,
	logger_ctx *log.LoggerContext) *TopologyChangeDetectorSvc {
	logger := log.NewLogger("ToplogyChangeDetector", logger_ctx)
	return &TopologyChangeDetectorSvc{xdcr_topology_svc: xdcr_topology_svc,
		cluster_info_svc:   cluster_info_svc,
		remote_cluster_svc: remote_cluster_svc,
		AbstractComponent:  comp.NewAbstractComponentWithLogger("ToplogyChangeDetector", logger),
		pipeline:           nil,
		finish_ch:          make(chan bool, 1),
		wait_grp:           &sync.WaitGroup{},
		logger:             logger}
}

func (top_detect_svc *TopologyChangeDetectorSvc) Attach(pipeline common.Pipeline) error {
	top_detect_svc.pipeline = pipeline
	return nil
}

func (top_detect_svc *TopologyChangeDetectorSvc) Start(map[string]interface{}) error {
	//register itself with pipeline supervisor
	supervisor := top_detect_svc.pipeline.RuntimeContext().Service(base.PIPELINE_SUPERVISOR_SVC)
	if supervisor == nil {
		return fmt.Errorf("Error starting ToplogyChangeDetectorSvc for pipeline %v since pipeline supervisor does not exist", top_detect_svc.pipeline.Topic())
	}
	err := top_detect_svc.RegisterComponentEventListener(common.ErrorEncountered, supervisor.(*PipelineSupervisor))
	if err != nil {
		return err
	}

	top_detect_svc.wait_grp.Add(1)

	go top_detect_svc.watch(top_detect_svc.finish_ch, top_detect_svc.wait_grp)

	top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v has started", top_detect_svc.pipeline.Topic())
	return nil
}

func (top_detect_svc *TopologyChangeDetectorSvc) Stop() error {
	close(top_detect_svc.finish_ch)
	top_detect_svc.wait_grp.Wait()
	top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v has stopped", top_detect_svc.pipeline.Topic())
	return nil

}

func (top_detect_svc *TopologyChangeDetectorSvc) watch(fin_ch chan bool, waitGrp *sync.WaitGroup) {
	defer waitGrp.Done()

	checkingTargetVersion := top_detect_svc.needCheckTarget()
	top_detect_svc.logger.Infof("checkingTargetVersion=%v in ToplogyChangeDetectorSvc for pipeline %v", checkingTargetVersion, top_detect_svc.pipeline.Topic())
	//run it once right at the beginning
	top_detect_svc.validate(checkingTargetVersion)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-fin_ch:
			top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v received finish signal and is exitting", top_detect_svc.pipeline.Topic())
			return
		case <-ticker.C:
			if top_detect_svc.pipeline.State() != common.Pipeline_Running {
				//pipeline is no longer running, kill itself
				top_detect_svc.logger.Infof("Pipeline %v is no longer running. ToplogyChangeDetectorSvc is exitting.", top_detect_svc.pipeline.Topic())
				return
			}
			top_detect_svc.validate(checkingTargetVersion)
		}
	}
}

func (top_detect_svc *TopologyChangeDetectorSvc) validate(checkingTargetVersion bool) {
	err := top_detect_svc.validateSourceTopology()
	if err != nil && err == source_topology_changedErr {
		top_detect_svc.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, top_detect_svc, nil, err))
	}

	if checkingTargetVersion {
		err := top_detect_svc.validateTargetVersion()
		if err != nil && err == target_cluster_versionChangeErr {
			top_detect_svc.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, top_detect_svc, nil, err))
		} else {
			top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v received error=%v when validating target version", top_detect_svc.pipeline.Topic(), err)
		}
	}
}

func (top_detect_svc *TopologyChangeDetectorSvc) validateTargetVersion() (err error) {
	spec := top_detect_svc.pipeline.Specification()
	targetClusterRef, err := top_detect_svc.remote_cluster_svc.RemoteClusterByUuid(spec.TargetClusterUUID, true)
	if err == nil {
		hasSSLOverMemSupport, err := pipeline_utils.HasSSLOverMemSupport(top_detect_svc.cluster_info_svc, targetClusterRef)
		if err == nil && hasSSLOverMemSupport {
			top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v detected that remote cluster %v has upgraded to 3.0 or above", top_detect_svc.pipeline.Topic(), targetClusterRef.HostName)
			err = target_cluster_versionChangeErr
		}
	}
	return err
}

func (top_detect_svc *TopologyChangeDetectorSvc) needCheckTarget() bool {
	spec := top_detect_svc.pipeline.Specification()
	targetClusterRef, err := top_detect_svc.remote_cluster_svc.RemoteClusterByUuid(spec.TargetClusterUUID, true)
	if err == nil && targetClusterRef.DemandEncryption {
		pipeline := top_detect_svc.pipeline
		targets := pipeline.Targets()
		for _, target := range targets {
			//only check the first target nozzle, then return
			//the assumption is all target nozzle should have the same type
			//- capi, xmem with MemConn, xmem with SSLOverMem or xmem with SSLOverProxy
			if _, ok := target.(*parts.XmemNozzle); !ok {
				return false
			}
			connType := target.(*parts.XmemNozzle).ConnType()
			if connType == base.SSLOverMem || connType == base.MemConn {
				return false
			} else {
				return true
			}
		}

	}
	return false
}

func (top_detect_svc *TopologyChangeDetectorSvc) validateSourceTopology() error {
	top_detect_svc.logger.Infof("ToplogyChangeDetectorSvc for pipeline %v validateSourceTopology...", top_detect_svc.pipeline.Topic())
	vblist_now := pipeline_utils.GetSourceVBListPerPipeline(top_detect_svc.pipeline)

	vblist_supposed := []uint16{}
	kv_vb_map, err := pipeline_utils.GetSourceVBMapForReplication(top_detect_svc.cluster_info_svc, top_detect_svc.xdcr_topology_svc, top_detect_svc.pipeline.Specification(), top_detect_svc.logger)
	if err != nil {
		return err
	}

	for _, vblist := range kv_vb_map {
		vblist_supposed = append(vblist_supposed, vblist...)
	}

	if len(vblist_now) != len(vblist_supposed) {
		return source_topology_changedErr
	} else {
		for _, vbno := range vblist_supposed {
			found := func(vbno uint16, vblist_now []uint16) bool {
				for _, vb := range vblist_now {
					if vb == vbno {
						return true
					}
				}

				return false
			}(vbno, vblist_now)
			if !found {
				top_detect_svc.logger.Errorf("Source topology has changed for pipeline %v - vblist_supposed=%v, vblist_now=%v\n", top_detect_svc.pipeline.Topic(), vblist_supposed, vblist_now)
				return source_topology_changedErr

			}
		}
	}

	return nil
}

func (top_detect_svc *TopologyChangeDetectorSvc) UpdateSettings(settings map[string]interface{}) error {
	return nil
}
