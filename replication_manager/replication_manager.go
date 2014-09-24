// replication manager.

package replication_manager

import (
	common "github.com/Xiaomei-Zhang/couchbase_goxdcr/common"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr/pipeline_manager"
	log "github.com/Xiaomei-Zhang/couchbase_goxdcr/util"
	factory "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/factory"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/metadata"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/metadata_svc"
	"sync"
)

var logger_rm *log.CommonLogger = log.NewLogger("ReplicationManager", log.LogLevelInfo)

/************************************
/* struct ReplicationManager
*************************************/
type replicationManager struct {
	metadata_svc      metadata_svc.MetadataSvc
	cluster_info_svc  metadata_svc.ClusterInfoSvc
	xdcr_topology_svc metadata_svc.XDCRCompTopologySvc
	once              sync.Once
}

var replication_mgr replicationManager

func Initialize(metadata_svc metadata_svc.MetadataSvc, cluster_info_svc metadata_svc.ClusterInfoSvc, xdcr_topology_svc metadata_svc.XDCRCompTopologySvc) {
	replication_mgr.once.Do(func() {
		replication_mgr.init(metadata_svc, cluster_info_svc, xdcr_topology_svc)
	})
}

func (rm *replicationManager) init(metadataSvc metadata_svc.MetadataSvc,
	clusterSvc metadata_svc.ClusterInfoSvc,
	topologySvc metadata_svc.XDCRCompTopologySvc) {
	rm.metadata_svc = metadataSvc
	rm.cluster_info_svc = clusterSvc
	rm.xdcr_topology_svc = topologySvc
	fac := factory.NewXDCRFactory(metadataSvc, clusterSvc, topologySvc)
	pipeline_manager.PipelineManager(fac)

	logger_rm.Info("Replication manager is initialized")

}

func MetadataService() metadata_svc.MetadataSvc {
	return replication_mgr.metadata_svc
}

func ClusterInfoService() metadata_svc.ClusterInfoSvc {
	return replication_mgr.cluster_info_svc
}

func XDCRCompTopologyService() metadata_svc.XDCRCompTopologySvc {
	return replication_mgr.xdcr_topology_svc
}

func CreateReplication(sourceClusterUUID string, sourceBucket string, targetClusterUUID, targetBucket string, filterName string, settings map[string]interface{}) (common.Pipeline, error)  {
	logger_rm.Infof("Creating replication - sourceCluterUUID=%s, sourceBucket=%s, targetClusterUUID=%s, targetBucket=%s, filterName=%s, settings=%v\n", sourceClusterUUID,
		sourceBucket, targetClusterUUID, targetBucket, filterName, settings)
	spec, err := replication_mgr.createAndPersistReplicationSpec(sourceClusterUUID, sourceBucket, targetClusterUUID, targetBucket, filterName, settings)
	logger_rm.Debugf("replication specification %s is created and persisted\n", spec.Id())
	if err != nil {
		logger_rm.Errorf("%v\n", err)
		return nil, err
	}
	p, err := pipeline_manager.StartPipeline(spec.Id(), settings)
	if err == nil {
		logger_rm.Debugf("Pipeline %s is started\n", spec.Id())
	} else {
		logger_rm.Errorf("%v\n", err)
	}
	return p, err
}

func PauseReplication(topic string) error {
	logger_rm.Infof("Pausing replication %s\n", topic)
	err := pipeline_manager.StopPipeline(topic)
	if err != nil {
		logger_rm.Errorf("%v\n", err)
		return err
	}
	logger_rm.Debugf("Pipeline %s is stopped", topic)
	//update the replication specification's "active" setting to be false
	spec, err := replication_mgr.metadata_svc.ReplicationSpec(topic)
	if err != nil {
		logger_rm.Errorf("%v\n", err)
		return err
	}

	settings := spec.Settings()
	settings.SetActive(false)
	err = replication_mgr.metadata_svc.SetReplicationSpec(*spec)
	logger_rm.Debugf("Replication specification %s is set to active=false\n", topic)
	return err
}

func ResumeReplication(topic string) error {
	logger_rm.Infof("Resuming replication %s\n", topic)
	spec, err := replication_mgr.metadata_svc.ReplicationSpec(topic)
	if err != nil {
		logger_rm.Errorf("%v\n", err)
		return err
	}

	settings := spec.Settings()
	settingsMap := settings.ToMap()
	_, err = pipeline_manager.StartPipeline(topic, settingsMap)
	if err != nil {
		logger_rm.Errorf("%v\n", err)
		return err
	}
	logger_rm.Debugf("Pipeline %s is started", topic)

	settings.SetActive(true)
	err = replication_mgr.metadata_svc.SetReplicationSpec(*spec)
	if err == nil {
		logger_rm.Debugf("Replication specification %s is updated with active=true\n", topic)
	} else {
		logger_rm.Errorf("%v\n", err)
	}
	return err
}

func DeleteReplication(topic string) error {
	logger_rm.Infof("Deleting replication %s\n", topic)
	err := pipeline_manager.StopPipeline(topic)
	if err != nil {
		logger_rm.Errorf("%v\n", err)
		return err
	}
	logger_rm.Debugf("Pipeline %s is stopped", topic)

	err = replication_mgr.metadata_svc.DelReplicationSpec(topic)
	if err == nil {
		logger_rm.Debugf("Replication specification %s is deleted\n", topic)
	} else {
		logger_rm.Errorf("%v\n", err)
	}
	return err
}

func (rm *replicationManager) createAndPersistReplicationSpec(sourceClusterUUID, sourceBucket, targetClusterUUID, targetBucket, filterName string, settings map[string]interface{}) (*metadata.ReplicationSpecification, error) {
	spec := metadata.NewReplicationSpecification(sourceClusterUUID, sourceBucket, targetClusterUUID, targetBucket, filterName)
	s := metadata.SettingsFromMap(settings)
	spec.SetSettings(s)

	return spec, nil
}
