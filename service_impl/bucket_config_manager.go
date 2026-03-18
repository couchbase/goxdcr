package service_impl

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/cbauth/revrpc"
	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
)

/**
 * BucketConfigManager is responsible for validating bucket configurations received from the cbauth/service
 * when a bucket settings is updated.
 *
 * The BucketConfigManager registers itself with cbauth/service and implements the service.Manager interface.
 * When a new bucket configuration is received, cbauth/service calls the ValidateBucketConfig method of the
 * BucketConfigManager, which performs the necessary validation and returns a result indicating whether the
 * configuration is valid or not.
 *
 * The manage is implemented as a singleon.
 */

var _ service.BucketConfigurationManager = (*BucketConfigManager)(nil)
var _ service.Manager = (*BucketConfigManager)(nil)

const (
	BucketConfigECCVKey    = "enable_cross_cluster_versioning"
	BucketConfigBucketName = "couch_bucket"
)

type BucketConfigManagerServices struct {
	ReplSvc          service_def.ReplicationSpecSvc
	RemoteClusterSvc service_def.RemoteClusterSvc
}

// BucketConfigManager is responsible for validating bucket configurations received from the cbauth/service
type BucketConfigManager struct {
	logger   *log.CommonLogger
	svc      BucketConfigManagerServices
	rev      atomic.Uint64
	initDone atomic.Bool
}

// Singleton instance of BucketConfigManager and sync.Once to ensure it is only initialized once
var bucketConfigManagerInst *BucketConfigManager
var bucketConfigManagerOnce sync.Once

// StartBucketConfigManager initializes the bucket config manager and registers it with cbauth/service.
// It is safe to call this function multiple times - only the first call will have an effect.
func StartBucketConfigManager(loggerCtx *log.LoggerContext, svc BucketConfigManagerServices) {
	bucketConfigManagerOnce.Do(func() {
		logger := log.NewLogger("bucketConfigManager", loggerCtx)
		bucketConfigManagerInst = &BucketConfigManager{
			logger: logger,
			svc:    svc,
		}
		logger.Infof("bucket config manager, isDisabled: %v", base.DisableBucketConfigManager)
		if base.DisableBucketConfigManager {
			return
		}

		// The call to cbauth/service to register the manager is blocking,
		// so we need to do it in a separate goroutine.
		go func() {
			if err := bucketConfigManagerInst.init(); err != nil {
				logger.Errorf("failed to initialize bucket config manager, err=%v", err)
			} else {
				logger.Infof("bucket config manager initialized successfully")
			}
		}()
	})
}

func (b *BucketConfigManager) init() error {
	b.logger.Infof("initializing bucket config manager")
	if err := service.RegisterManagerWithCompletionCallback(b, revrpc.DefaultBabysitErrorPolicy, func() error {
		b.initDone.Store(true)
		b.logger.Infof("bucket config manager registration complete")
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// ValidateBucketConfig is called by cbauth/service when a new bucket configuration is being updated.
// Multiple settings can be checked in a single call as the 'params' argument contains a map of all settings.
// The response `service.BucketValidationResult` contains a map where for each key (setting) value in the map
// is either a validation error or an indication that the setting is valid.
//
// The interface directly allows error message to be shown on UI when validation fails.
// For this an instance of `service.BucketConfigValidationError{Error: <string>, Message: <string>}` is added
// in the Result map.
func (b *BucketConfigManager) ValidateBucketConfig(params service.ValidateBucketConfigParams) (res *service.BucketValidationResult, err error) {
	now := time.Now()
	defer func() {
		b.logger.Infof("bucket config validation completed in %v",
			time.Since(now))
	}()
	res, err = b.validateBucketConfig(params)
	return
}

func (b *BucketConfigManager) validateBucketConfig(params service.ValidateBucketConfigParams) (res *service.BucketValidationResult, err error) {
	b.logger.Infof("bucket config manager request justReturn: %v, params: %v", params.JustReturnParams, params.ParsedParams)

	res = &service.BucketValidationResult{Result: make(map[string]any)}
	if params.JustReturnParams {
		return
	}

	if !b.initDone.Load() {
		err = fmt.Errorf("bucket config manager is not initialized yet")
		b.logger.Errorf("%v", err)
		return
	}

	bucketName := params.ParsedParams[BucketConfigBucketName]
	if bucketName == "" {
		// This ideally should never happen
		err = fmt.Errorf("bucket name not found in params: %v", params.ParsedParams)
		return
	}

	err = b.validateECCV(res.Result, bucketName, params)
	if err != nil {
		b.logger.Errorf("error validating ECCV setting for bucket %s, err: %v", bucketName, err)
		res.Result[BucketConfigECCVKey] = service.BucketConfigValidationError{Error: "validation_failed", Message: "unable to validate ECCV setting in XDCR"}
		return
	}

	return
}

// validate writes a config validation error with the given message into 'r' if 'problem' is true, as long as there is
// not already a config validation error.
func validate(r map[string]any, paramName string, problem bool, msg string) {
	existing, exists := r[paramName]
	if exists {
		if _, typeOk := existing.(service.BucketConfigValidationError); typeOk {
			return
		}
	}

	if problem {
		r[paramName] = service.BucketConfigValidationError{Error: "invalid_args", Message: msg}
	}
}

func (b *BucketConfigManager) validateECCV(r map[string]any, bucketName string, params service.ValidateBucketConfigParams) (err error) {
	val, exists := params.ParsedParams[BucketConfigECCVKey]
	if !exists {
		// Non existance is not an error per se.
		b.logger.Warnf("eccv setting not found for bucket %s, skipping eccv validation", bucketName)
		return
	}

	if val != "true" {
		return
	}

	b.logger.Infof("eccv check requested for bucket %s", bucketName)

	ref, err := b.getCNGRef(bucketName)
	if err != nil {
		return
	}

	if ref != nil {
		b.logger.Infof("eccv check result for bucket %s: CNG in use by ref name=%s, targetUUID=%s", bucketName, ref.Name(), ref.Uuid())
		validate(r,
			BucketConfigECCVKey,
			true,
			// We give two info in error message on UI for user to troubleshoot:
			// 1. It comes from XDCR (as it says 'remote cluster reference' and 'replication')
			// 2. It gives the exact remote ref name
			fmt.Sprintf("Bucket is being replicated using CNG with remote cluster reference '%s'", ref.Name()))
	} else {
		b.logger.Infof("eccv check result for bucket %s: CNG not in use", bucketName)
	}

	return
}

// getCNGRef returns the RemoteClusterReference configured with CNG and replicating the given bucket
// Returns nil if there is no such RemoteClusterReference
func (b *BucketConfigManager) getCNGRef(bucketName string) (ref *metadata.RemoteClusterReference, err error) {
	specIds, err := b.svc.ReplSvc.AllReplicationSpecIdsForBucket(bucketName)
	if err != nil || len(specIds) == 0 {
		return
	}

	for _, specId := range specIds {
		spec, err := b.svc.ReplSvc.ReplicationSpec(specId)
		if err != nil {
			return nil, err
		}

		ref, err := b.svc.RemoteClusterSvc.RemoteClusterByUuid(spec.TargetClusterUUID, false)
		if err != nil {
			return nil, err
		}

		if ref.IsCNG() {
			return ref, nil
		}
	}

	return
}

func (b *BucketConfigManager) GetNodeInfo() (*service.NodeInfo, error) {
	return &service.NodeInfo{}, nil
}

func (b *BucketConfigManager) Shutdown() error {
	return nil
}

func (b *BucketConfigManager) GetTaskList(rev service.Revision, cancel service.Cancel) (*service.TaskList, error) {
	// An empty, or zero, revision is sent when the connection is spun up. To complete the handshake we need to return
	// immediately. Thereafter we can just block, providing we respect cancel.
	if DecodeRev(rev) != 0 {
		select {
		case <-cancel:
			return nil, service.ErrCanceled
		case <-time.NewTimer(time.Minute).C:
			break
		}
	}

	b.rev.Add(1)
	return &service.TaskList{Rev: EncodeRev(b.rev.Load()), Tasks: make([]service.Task, 0)}, nil
}

func (b *BucketConfigManager) CancelTask(id string, rev service.Revision) error {
	return fmt.Errorf("unimplemented")
}

func (b *BucketConfigManager) GetCurrentTopology(rev service.Revision, cancel service.Cancel) (*service.Topology, error) {
	// Like 'GetTaskList' an empty or zero revision is part of the handshake which we must respond to immediately.
	if DecodeRev(rev) != 0 {
		select {
		case <-cancel:
			return nil, service.ErrCanceled
		case <-time.NewTimer(time.Minute).C:
			break
		}
	}

	return &service.Topology{
		Rev:   EncodeRev(b.rev.Load()),
		Nodes: make([]service.NodeID, 0),
	}, nil
}

func (b *BucketConfigManager) PrepareTopologyChange(change service.TopologyChange) error {
	return fmt.Errorf("unimplemented")
}

func (b *BucketConfigManager) StartTopologyChange(change service.TopologyChange) error {
	return fmt.Errorf("unimplemented")
}

// EncodeRev takes a revision as a uint64 and turns it into a 'service.Revision' - a byte array in big endian.
func EncodeRev(rev uint64) service.Revision {
	ext := make(service.Revision, 8)
	binary.BigEndian.PutUint64(ext, rev)

	return ext
}

// DecodeRev takes a 'service.Revision' - i.e. an eight element byte array - and turns it into a 'uint64'.
func DecodeRev(ext service.Revision) uint64 {
	if len(ext) != 8 {
		return 0
	}

	return binary.BigEndian.Uint64(ext)
}
