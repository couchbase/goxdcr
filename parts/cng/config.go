package cng

import (
	"fmt"
	"strings"
	"sync/atomic"
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
	CRMode base.ConflictResolutionMode
	// Topic is the pipeline topic (full topic) used for pipeline-type inference (main vs backfill).
	// Optional.
	Topic             string
	SourceClusterUUID string
	SourceBucketName  string
	SourceBucketUUID  string

	TargetClusterUUID string
	TargetBucketName  string
	TargetBucketUUID  string

	vbUUIDMap map[uint16]uint64
}

func (rc *ReplicationConfig) SetVBUUIDMap(m map[uint16]*internal_xdcr_v1.GetVbucketInfoResponse) (err error) {
	rc.vbUUIDMap = make(map[uint16]uint64)
	for vbNo, info := range m {
		if info == nil || len(info.Vbuckets) == 0 {
			err = fmt.Errorf("no vbucket info found for vbucket %v", vbNo)
			return
		}
		rc.vbUUIDMap[vbNo] = info.Vbuckets[0].GetUuid()
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
	DataChanSize int
	WorkerCount  int
	ConnCount    int
	Deadline     time.Duration

	// (dev-only) Per-mutation send delay injection, applied on the write path.
	devMainSendDelayMs     atomic.Int64
	devBackfillSendDelayMs atomic.Int64
	// (dev-only) Inject gRPC "collection not found" errors on PushDocument.
	// Percent value is between 0 and 100.
	devColErrPercent atomic.Int64

	// (live updatable) optimisticThresholdSize is the document size (in bytes) below which we can do optimistic replication.
	optimisticThresholdSize atomic.Int64

	// (live updatable) Deadline for RPC calls to CNG
	rpcDeadlineMs atomic.Int64
}

func (t *Tunables) GetOptimisticThresholdSize() int {
	v := t.optimisticThresholdSize.Load()
	if v < 0 {
		return 0
	}
	return int(v)
}

func (t *Tunables) SetOptimisticThresholdSize(v int) {
	if v < 0 {
		v = 0
	}

	t.optimisticThresholdSize.Store(int64(v))
}

func (t *Tunables) String() string {
	s := strings.Builder{}
	fmt.Fprintf(&s, "DataChanSize=%d, ", t.DataChanSize)
	fmt.Fprintf(&s, "WorkerCount=%d, ", t.WorkerCount)
	fmt.Fprintf(&s, "ConnCount=%d, ", t.ConnCount)
	fmt.Fprintf(&s, "Deadline=%dms, ", t.Deadline.Milliseconds())
	fmt.Fprintf(&s, "OptimisticThresholdSize=%d", t.GetOptimisticThresholdSize())

	t.devParamsString(&s) // No-op for prod

	return s.String()
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
	if t.Deadline <= 0 {
		return fmt.Errorf("Deadline must be positive")
	}
	return nil
}
