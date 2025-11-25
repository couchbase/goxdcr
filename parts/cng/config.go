package cng

import (
	"fmt"
	"time"

	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/service_def"
	"github.com/couchbase/goxdcr/v8/utils"
)

// Config is the configuration for a CNG nozzle
type Config struct {
	Replication ReplicationConfig
	Services    Services
	Tunables    Tunables
}

type ReplicationConfig struct {
	CRMode            base.ConflictResolutionMode
	SourceClusterUUID string
	SourceBucketName  string
	SourceBucketUUID  string

	TargetClusterUUID string
	TargetBucketName  string
	TargetBucketUUID  string

	vbUUIDMap map[uint16]string
}

func (rc *ReplicationConfig) SetVBUUIDMap(m map[uint16]*internal_xdcr_v1.GetVbucketInfoResponse) (err error) {
	rc.vbUUIDMap = make(map[uint16]string)
	for vbNo, info := range m {
		if info == nil || len(info.Vbuckets) == 0 {
			err = fmt.Errorf("no vbucket info found for vbucket %v", vbNo)
			return
		}
		rc.vbUUIDMap[vbNo] = fmt.Sprintf("%s", info.Vbuckets[0].GetUuid())
	}

	return nil
}

func (rc *ReplicationConfig) Validate() error {
	if rc.SourceClusterUUID == "" {
		return fmt.Errorf("SourceClusterUUID is empty")
	}
	if rc.SourceBucketName == "" {
		return fmt.Errorf("SourceBucketName is empty")
	}
	if rc.SourceBucketUUID == "" {
		return fmt.Errorf("SourceBucketUUID is empty")
	}
	if rc.TargetClusterUUID == "" {
		return fmt.Errorf("TargetClusterUUD is empty")
	}
	return nil
}

type Services struct {
	RemoteClusterSvc service_def.RemoteClusterSvc
	BWThrottler      service_def.BandwidthThrottlerSvc
	Utils            utils.UtilsIface
}

// Validate checks if the services are non-nil except BWThrottler which is optional
// BWThrottler is set explicitly via SetBandwidthThrottler
func (s *Services) Validate() error {
	if s.RemoteClusterSvc == nil {
		return fmt.Errorf("RemoteClusterSvc is nil")
	}
	if s.Utils == nil {
		return fmt.Errorf("Utils is nil")
	}
	return nil
}

type Tunables struct {
	InsecureSkipVerify bool
	DataChanSize       int
	WorkerCount        int
	ConnCount          int
	RetryInterval      int // in milliseconds
	Deadline           time.Duration
	// OptimisticThresholdSize is the document size (in bytes)
	// CNG TODO: make it dynamic i.e. settable at runtime
	OptimisticThresholdSize int
}

func (t *Tunables) String() string {
	return fmt.Sprintf("InsecureSkipVerify=%v, DataChanSize=%d, WorkerCount=%d, ConnCount=%d, RetryInterval=%d, Deadline=%dms, OptimisticThresholdSize=%d",
		t.InsecureSkipVerify, t.DataChanSize, t.WorkerCount, t.ConnCount, t.RetryInterval, t.Deadline.Milliseconds(), t.OptimisticThresholdSize)
}

func (t *Tunables) Validate() (err error) {
	if t.DataChanSize <= 0 {
		return fmt.Errorf("DataChanSize must be positive")
	}
	if t.WorkerCount <= 0 {
		return fmt.Errorf("WorkerCount must be positive")
	}
	if t.ConnCount <= 0 {
		return fmt.Errorf("ConnCount must be positive")
	}
	if t.RetryInterval <= 0 {
		return fmt.Errorf("RetryInterval must be positive")
	}
	if t.Deadline <= 0 {
		return fmt.Errorf("Deadline must be positive")
	}
	return nil
}
