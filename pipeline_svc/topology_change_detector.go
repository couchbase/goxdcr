package pipeline_svc

import (
	"errors"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/common"
	comp "github.com/couchbase/goxdcr/component"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/pipeline_utils"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/utils"
	"sync"
	"time"
)

var source_topology_changedErr = errors.New("Topology has changed on source cluster")

type TopologyChangeDetectorSvc struct {
	*comp.AbstractComponent

	//xdcr topology service
	xdcr_topology_svc service_def.XDCRCompTopologySvc
	cluster_info_svc  service_def.ClusterInfoSvc
	logger            *log.CommonLogger
	pipeline          common.Pipeline
	finish_ch         chan bool
	wait_grp          *sync.WaitGroup
}

func NewTopologyChangeDetectorSvc(cluster_info_svc service_def.ClusterInfoSvc, xdcr_topology_svc service_def.XDCRCompTopologySvc, logger_ctx *log.LoggerContext) *TopologyChangeDetectorSvc {
	logger := log.NewLogger("ToplogyChangeDetector", logger_ctx)
	return &TopologyChangeDetectorSvc{xdcr_topology_svc: xdcr_topology_svc,
		cluster_info_svc:  cluster_info_svc,
		AbstractComponent: comp.NewAbstractComponentWithLogger("ToplogyChangeDetector", logger),
		pipeline:          nil,
		finish_ch:         make(chan bool, 1),
		wait_grp:          &sync.WaitGroup{},
		logger:            logger}
}

func (top_detect_svc *TopologyChangeDetectorSvc) Attach(pipeline common.Pipeline) error {
	top_detect_svc.pipeline = pipeline
	return nil
}

func (top_detect_svc *TopologyChangeDetectorSvc) Start(map[string]interface{}) error {
	//register itself with pipeline supervisor
	supervisor := top_detect_svc.pipeline.RuntimeContext().Service(base.PIPELINE_SUPERVISOR_SVC)
	if supervisor == nil {
		return errors.New("Pipeline supervisor has to exist")
	}
	err := top_detect_svc.RegisterComponentEventListener(common.ErrorEncountered, supervisor.(*PipelineSupervisor))
	if err != nil {
		return err
	}

	top_detect_svc.wait_grp.Add(1)

	go top_detect_svc.watch(top_detect_svc.finish_ch, top_detect_svc.wait_grp)

	top_detect_svc.logger.Info("ToplogyChangeDetectorSvc has started")
	return nil
}

func (top_detect_svc *TopologyChangeDetectorSvc) Stop() error {
	close(top_detect_svc.finish_ch)
	top_detect_svc.wait_grp.Wait()
	top_detect_svc.logger.Info("ToplogyChangeDetectorSvc has stopped")
	return nil

}

func (top_detect_svc *TopologyChangeDetectorSvc) watch(fin_ch chan bool, waitGrp *sync.WaitGroup) {
	defer waitGrp.Done()

	//run it once right at the beginning
	err := top_detect_svc.validateSourceTopology()
	if err != nil && err == source_topology_changedErr {
		otherInfo := utils.WrapError(err)
		top_detect_svc.RaiseEvent(common.ErrorEncountered, nil, top_detect_svc, nil, otherInfo)
	}

	ticker := time.NewTicker(10 * time.Second)

	for {
		select {
		case <-fin_ch:
			top_detect_svc.logger.Info("Received finish signal, watch routine exit")
			return
		case <-ticker.C:
					if top_detect_svc.pipeline.State() != common.Pipeline_Running {
				//pipeline is no longer running, kill itself
				top_detect_svc.logger.Info("Pipeline is no longer running, exit.")
				return
			}
		
			err := top_detect_svc.validateSourceTopology()
			if err != nil && err == source_topology_changedErr {
				otherInfo := utils.WrapError(err)
				top_detect_svc.RaiseEvent(common.ErrorEncountered, nil, top_detect_svc, nil, otherInfo)
			}
		}
	}
}

func (top_detect_svc *TopologyChangeDetectorSvc) validateSourceTopology() error {
	top_detect_svc.logger.Info("validateSourceTopology...")
	vblist_now := pipeline_utils.GetSourceVBListPerPipeline(top_detect_svc.pipeline)

	vblist_supposed := []uint16{}
	kv_vb_map, err := pipeline_utils.GetSourceVBListForReplication(top_detect_svc.cluster_info_svc, top_detect_svc.xdcr_topology_svc, top_detect_svc.pipeline.Specification(), top_detect_svc.logger)
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
				top_detect_svc.logger.Errorf("Source topology has changed - vblist_supposed=%v, vblist_now=%v\n", vblist_supposed, vblist_now)
				return source_topology_changedErr

			}
		}
	}

	return nil
}
