// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package metadata_svc

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/common"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
	utilities "github.com/couchbase/goxdcr/v8/utils"
)

const (
	// the key to the metadata that stores the keys of all remote clusters
	CheckpointsCatalogKeyPrefix = "ckpt"
	CheckpointsKeyPrefix        = CheckpointsCatalogKeyPrefix
	BrokenMappingKey            = "brokenMappings"
	GlobalInfoKey               = "globalInfo" //represents both GlobalTimestamps and GlobalTargetCounters
)

// Used to keep track of brokenmapping SHA and the count of checkpoint records referring to it
type BrokenMapShaRefCounter struct {
	lock           sync.RWMutex
	refCnt         map[string]uint64
	shaToMapping   metadata.ShaToCollectionNamespaceMap
	internalSpecId string
}

func NewBrokenMapShaRefCounter() *BrokenMapShaRefCounter {
	return &BrokenMapShaRefCounter{refCnt: make(map[string]uint64),
		shaToMapping: make(metadata.ShaToCollectionNamespaceMap),
	}
}

func (b *BrokenMapShaRefCounter) RegisterCkptDoc(doc *metadata.CheckpointsDoc) {
	if b == nil || doc == nil {
		return
	}

	b.lock.Lock()
	for _, record := range doc.Checkpoint_records {
		if record == nil || record.BrokenMappingsLen() == 0 {
			continue
		}
		brokenMappings := record.BrokenMappings()
		for sha, brokenMap := range brokenMappings {
			if brokenMap == nil || len(*brokenMap) == 0 {
				continue
			}
			b.refCnt[sha]++
			b.shaToMapping[sha] = brokenMap
		}
	}
	b.lock.Unlock()
}

type CheckpointsService struct {
	metadata_svc service_def.MetadataSvc
	logger       *log.CommonLogger
	utils        utilities.UtilsIface

	specsMtx    sync.RWMutex
	cachedSpecs map[string]*metadata.ReplicationSpecification
	ckptCaches  map[string]CheckpointsServiceCache
	// Certain situations such as merging checkpoints from other nodes require coarse locking
	stopTheWorldMtx map[string]*sync.RWMutex
	// Callers to brokenMap related method must be serialized due to ref counting
	getBrokenMapAccessTokenMap map[string]chan bool

	backfillSpecsMtx    sync.RWMutex
	cachedBackfillSpecs map[string]*metadata.BackfillReplicationSpec

	replicationSpecSvc    service_def.ReplicationSpecSvc
	brokenMapRefCountSvc  *ShaRefCounterService
	globalInfoRefCountSvc *ShaRefCounterService
}

func NewCheckpointsService(metadata_svc service_def.MetadataSvc, logger_ctx *log.LoggerContext, utils utilities.UtilsIface, replicationSpecService service_def.ReplicationSpecSvc) (*CheckpointsService, error) {
	logger := log.NewLogger("CheckpointSvc", logger_ctx)
	shaRefSvc, err := NewShaRefCounterService(getCollectionNsMappingsDocKey, metadata_svc, logger, utils)
	if err != nil {
		return nil, err
	}
	globalInfoRefCntSvc, err := NewShaRefCounterService(getGlobalInfoDocKey, metadata_svc, logger, utils)
	if err != nil {
		return nil, err
	}
	ckptSvc := &CheckpointsService{metadata_svc: metadata_svc,
		logger:                     logger,
		brokenMapRefCountSvc:       shaRefSvc,
		globalInfoRefCountSvc:      globalInfoRefCntSvc,
		cachedSpecs:                make(map[string]*metadata.ReplicationSpecification),
		ckptCaches:                 map[string]CheckpointsServiceCache{},
		cachedBackfillSpecs:        make(map[string]*metadata.BackfillReplicationSpec),
		utils:                      utils,
		replicationSpecSvc:         replicationSpecService,
		stopTheWorldMtx:            map[string]*sync.RWMutex{},
		getBrokenMapAccessTokenMap: make(map[string]chan bool),
	}
	return ckptSvc, ckptSvc.initWithSpecs()
}

func (ckpt_svc *CheckpointsService) CheckpointsDoc(replicationId string, vbno uint16) (*metadata.CheckpointsDoc, error) {
	return ckpt_svc.checkpointsDocInternal(replicationId, vbno, true)
}

func (ckpt_svc *CheckpointsService) checkpointsDocInternal(replicationId string, vbno uint16, rLockStopTheWorld bool) (*metadata.CheckpointsDoc, error) {
	cache, cacheErr := ckpt_svc.getCache(replicationId)
	if cacheErr == nil {
		ckptDoc, cacheGetErr := cache.GetOneVBDoc(vbno)
		if cacheGetErr == nil {
			return ckptDoc, nil
		}
	}

	if rLockStopTheWorld {
		mtx := ckpt_svc.getStopTheWorldMtx(replicationId)
		mtx.RLock()
		defer mtx.RUnlock()
	}

	key := ckpt_svc.getCheckpointDocKey(replicationId, vbno)
	result, rev, err := ckpt_svc.metadata_svc.Get(key)
	if err != nil {
		return nil, err
	}

	// Should exist because checkpoint manager must finish loading before allowing ckpt operations
	shaMap, _ := ckpt_svc.brokenMapRefCountSvc.GetShaNamespaceMap(replicationId)
	globalInfoShaMap, _ := ckpt_svc.globalInfoRefCountSvc.GetShaGlobalInfoMap(replicationId)

	ckpt_doc, err := ckpt_svc.constructCheckpointDoc(result, rev, shaMap, globalInfoShaMap)
	if err != nil {
		ckpt_svc.logger.Errorf("Unable to construct ckpt doc from metakv given replication: %v vbno: %v key: %v err %v",
			replicationId, vbno, key, err)
	}
	return ckpt_doc, err
}

func (ckpt_svc *CheckpointsService) getCheckpointCatalogKey(replicationId string) string {
	return CheckpointsCatalogKeyPrefix + base.KeyPartsDelimiter + replicationId
}

// Get a unique key to access metakv for brokenMappings
func getCollectionNsMappingsDocKey(replicationId string) string {
	return fmt.Sprintf("%v", CheckpointsKeyPrefix+base.KeyPartsDelimiter+replicationId+base.KeyPartsDelimiter+BrokenMappingKey)
}

func getGlobalInfoDocKey(replicationId string) string {
	return fmt.Sprintf("%v", CheckpointsKeyPrefix+base.KeyPartsDelimiter+replicationId+base.KeyPartsDelimiter+GlobalInfoKey)
}

// Get a unique key to access metakv for checkpoints
func (ckpt_svc *CheckpointsService) getCheckpointDocKey(replicationId string, vbno uint16) string {
	return fmt.Sprintf("%v%v", CheckpointsKeyPrefix+base.KeyPartsDelimiter+replicationId+base.KeyPartsDelimiter, vbno)
}

func (ckpt_svc *CheckpointsService) decodeVbnoFromCkptDocKey(ckptDocKey string) (uint16, error) {
	parts := strings.Split(ckptDocKey, base.KeyPartsDelimiter)
	vbnoStr := parts[len(parts)-1]
	vbno, err := strconv.Atoi(vbnoStr)
	if err != nil {
		return 0, err
	}
	return uint16(vbno), nil
}

func (ckpt_svc *CheckpointsService) isBrokenMappingDoc(ckptDocKey string) bool {
	return strings.Contains(ckptDocKey, BrokenMappingKey)
}

func (ckpt_svc *CheckpointsService) isGlobalInfoDoc(ckptDocKey string) bool {
	return strings.Contains(ckptDocKey, GlobalInfoKey)
}

func (ckpt_svc *CheckpointsService) DelCheckpointsDocs(replicationId string) error {
	// Deleting checkpoints docs will require removing broken mapping - thus need to prevent
	// concurrent broken map access
	accessTokenCh, err := ckpt_svc.getCkptDocsWithBrokenMappingAccess(replicationId)
	// This accessToken is really more necessary in terms of cleaning up pipeline
	// When this is done as part of the pipeline deletion, it's likely the access token is removed
	// However, pipeline manager should be synchronous in a way that it shouldn't start a new pipeline until the old
	// is deleted. So continue regardless
	if err == nil {
		// Get the actual go-ahead
		<-accessTokenCh
		// Once gotten it, will need to return it once done
		defer func() {
			accessTokenCh <- true
		}()
	}

	ckpt_svc.logger.Infof("DelCheckpointsDocs for replication %v...", replicationId)
	catalogKey := ckpt_svc.getCheckpointCatalogKey(replicationId)

	// DelCheckpointDocs is called with either main or backfill replication ID
	mainRepl, _ := common.DecomposeFullTopic(replicationId)
	curSpec, _ := ckpt_svc.replicationSpecSvc.ReplicationSpec(mainRepl)

	// No need for stop the world because replication would be deleted
	errMap := make(base.ErrorMap)
	var errMtx sync.Mutex

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		cache, cacheErr := ckpt_svc.getCache(replicationId)
		if cacheErr == nil {
			cache.InvalidateCache()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err_ret := ckpt_svc.metadata_svc.DelAllFromCatalog(catalogKey)
		if err_ret != nil {
			errMsg := fmt.Sprintf("Failed to delete checkpoints docs for %v - manual clean up may be required\n", replicationId)
			ckpt_svc.logger.Errorf(errMsg)
			errMtx.Lock()
			errMap["1"] = fmt.Errorf(errMsg)
			errMtx.Unlock()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		cleanupErr := ckpt_svc.brokenMapRefCountSvc.CleanupMapping(replicationId, ckpt_svc.utils)
		if cleanupErr != nil {
			errMsg := fmt.Sprintf("Failed to clean up internal counter for %v : %v - manual clean up may be required\n", replicationId, cleanupErr)
			ckpt_svc.logger.Errorf(errMsg)
			errMtx.Lock()
			errMap["2"] = fmt.Errorf(errMsg)
			errMtx.Unlock()
		}

		if curSpec != nil {
			// DelCheckpointsDocs is being called even though spec still exist
			// This means that this is called as a part of cleaning up pipeline
			// and not because a replication spec has been deleted
			ckpt_svc.brokenMapRefCountSvc.InitTopicShaCounterWithInternalId(replicationId, curSpec.InternalId)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		cleanupErr := ckpt_svc.globalInfoRefCountSvc.CleanupMapping(replicationId, ckpt_svc.utils)
		if cleanupErr != nil {
			errMsg := fmt.Sprintf("Failed to clean up globalInfo internal counter for %v : %v - manual clean up may be required\n", replicationId, cleanupErr)
			ckpt_svc.logger.Errorf(errMsg)
			errMtx.Lock()
			errMap["3"] = fmt.Errorf(errMsg)
			errMtx.Unlock()
		}

		if curSpec != nil {
			// DelCheckpointsDocs is being called even though spec still exist
			// This means that this is called as a part of cleaning up pipeline
			// and not because a replication spec has been deleted
			ckpt_svc.globalInfoRefCountSvc.InitTopicShaCounterWithInternalId(replicationId, curSpec.InternalId)
		}
	}()

	wg.Wait()
	ckpt_svc.logger.Infof("DelCheckpointsDocs is done for %v\n", replicationId)
	if len(errMap) == 0 {
		return nil
	}
	return fmt.Errorf(base.FlattenErrorMap(errMap))
}

// Need to have correct accounting after deleting checkpoingsDocs
func (ckpt_svc *CheckpointsService) postDelCheckpointsDoc(replicationId string, doc *metadata.CheckpointsDoc) (modified bool, err error) {
	if doc == nil {
		return
	}

	mtx := ckpt_svc.getStopTheWorldMtx(replicationId)
	mtx.Lock()
	defer mtx.Unlock()
	decrementerFunc, err := ckpt_svc.brokenMapRefCountSvc.GetDecrementerFunc(replicationId)
	if err != nil {
		return
	}

	for _, ckptRecord := range doc.Checkpoint_records {
		if ckptRecord == nil {
			continue
		}
		if ckptRecord.GetBrokenMappingShaCount() > 0 {
			for _, oneSha := range ckptRecord.GetBrokenMappingSha256s() {
				decrementerFunc(oneSha)
			}
		}
		modified = true
	}

	return
}

// If internalId is provided, will be used to check prior to upsert
func (ckpt_svc *CheckpointsService) DelCheckpointsDoc(replicationId string, vbno uint16, specInternalId string) error {
	ckpt_svc.logger.Debugf("DelCheckpointsDoc for replication %v and vbno %v...", replicationId, vbno)

	mtx := ckpt_svc.getStopTheWorldMtx(replicationId)
	mtx.Lock()

	ckptDocsPreDel, err := ckpt_svc.checkpointsDocInternal(replicationId, vbno, false)
	if err != nil && err != service_def.MetadataNotFoundErr {
		ckpt_svc.logger.Warnf("DelCheckpointsDocs had err retrieving existing cktps: %v", err)
	}

	cache, cacheErr := ckpt_svc.getCache(replicationId)
	if cacheErr == nil {
		cache.StoreOneVbDoc(vbno, nil, specInternalId)
	}

	key := ckpt_svc.getCheckpointDocKey(replicationId, vbno)
	_, rev, err := ckpt_svc.metadata_svc.Get(key)
	if err != nil {
		if err == service_def.MetadataNotFoundErr {
			// Nothing to delete
			err = nil
			ckpt_svc.logger.Warnf("DelCheckpointDoc called for %v and vbno %v but no prev ckpt was found", replicationId, vbno)
		}
		mtx.Unlock()
		return err
	}

	catalogKey := ckpt_svc.getCheckpointCatalogKey(replicationId)
	err = ckpt_svc.metadata_svc.DelWithCatalog(catalogKey, key, rev)
	if err != nil {
		ckpt_svc.logger.Errorf("Failed to delete checkpoints doc for replication %v and vbno %v\n", replicationId, vbno)
		mtx.Unlock()
		return err
	}
	mtx.Unlock()

	var needToDoRefCntDecrement bool
	if ckptDocsPreDel != nil {
		for _, record := range ckptDocsPreDel.GetCheckpointRecords() {
			if record == nil {
				continue
			}
			brokenMap := record.BrokenMappings()
			if brokenMap != nil && record.GetBrokenMappingShaCount() > 0 {
				needToDoRefCntDecrement = true
			}
		}
	}

	if !needToDoRefCntDecrement {
		return nil
	}

	// Doing ref cnt requires re-locking
	modified, err := ckpt_svc.postDelCheckpointsDoc(replicationId, ckptDocsPreDel)
	if err != nil {
		ckpt_svc.logger.Warnf("%v - postDelCheckpointsDoc returned %v", err)
		return err
	}

	if modified {
		err = ckpt_svc.brokenMapRefCountSvc.UpsertMapping(replicationId, specInternalId)
		if err != nil {
			ckpt_svc.logger.Warnf("%v - postDelCheckpointsDoc returned %v", err)
			return err
		}
	}
	return nil
}

// in addition to upserting checkpoint record, this method may also update xattr seqno
// and target cluster version in checkpoint doc
// these operations are done in the same metakv operation to ensure that they succeed and fail together
// Returns size of the checkpoint
func (ckpt_svc *CheckpointsService) UpsertCheckpoints(replicationId string, specInternalId string, vbno uint16, ckpt_record *metadata.CheckpointRecord) (int, error) {
	ckpt_svc.logger.Debugf("Persisting checkpoint record=%v for vbno=%v replication=%v\n", ckpt_record, vbno, replicationId)
	var size int
	key := ckpt_svc.getCheckpointDocKey(replicationId, vbno)

	if ckpt_record == nil {
		return size, errors.New("nil checkpoint record")
	}

	mtx := ckpt_svc.getStopTheWorldMtx(replicationId)
	mtx.RLock()
	defer mtx.RUnlock()

	var ckpt_doc *metadata.CheckpointsDoc
	cache, err := ckpt_svc.getCache(replicationId)
	if err == nil {
		ckpt_doc, err = cache.GetOneVBDoc(vbno)
	}

	if err != nil {
		// Either cache is invalidated (i.e. after a complete upsert)
		// Or doc is not found in cache... go to metakv to go get it
		ckpt_doc, err = ckpt_svc.checkpointsDocInternal(replicationId, vbno, false)
		if err == service_def.MetadataNotFoundErr {
			ckpt_doc = metadata.NewCheckpointsDoc(specInternalId)
			err = nil
		}
		if err != nil {
			return size, err
		}
	}

	added, removedRecords := ckpt_doc.AddRecord(ckpt_record)
	if !added {
		ckpt_svc.logger.Debug("the ckpt record to be added is the same as the current ckpt record in the ckpt doc. no-op.")
	} else {
		ckpt_json, err := json.Marshal(ckpt_doc)
		if err != nil {
			return size, err
		}

		//always update the checkpoint without revision
		err = ckpt_svc.metadata_svc.Set(key, ckpt_json, nil)
		if err != nil {
			ckpt_svc.logger.Errorf("Failed to set checkpoint doc key=%v, err=%v\n", key, err)
		} else {
			size = ckpt_doc.Size()
			ckpt_svc.logger.Debugf("Wrote checkpoint doc key=%v, Size=%v\n", key, size)
			err = ckpt_svc.RecordMappings(replicationId, ckpt_record, removedRecords)
			if err != nil && !strings.HasPrefix(replicationId, base.BackfillPipelineTopicPrefix) {
				// Backfill pipeline can be cleaned up
				ckpt_svc.logger.Errorf("Failed to record broken mapping err=%v\n", err)
			}
			if cache != nil {
				cacheErr := cache.StoreOneVbDoc(vbno, ckpt_doc, specInternalId)
				if cacheErr != nil {
					ckpt_svc.logger.Warnf("%v - Unable to store cache for vb %v: %v", replicationId, vbno, cacheErr)
				}
			}
		}
	}
	return size, err
}

// the stopTheWorld write lock must be held
func (ckpt_svc *CheckpointsService) upsertCheckpointsDoc(replicationId string, ckptDocs map[uint16]*metadata.CheckpointsDoc, internalId string) error {
	err := ckpt_svc.validateSpecIsValid(replicationId, internalId)
	if err != nil {
		return err
	}

	cache, cacheErr := ckpt_svc.getCache(replicationId)
	if cacheErr == nil {
		invalidateMeasureStop := ckpt_svc.utils.StartDiagStopwatch(fmt.Sprintf("%v_invalidateCache", replicationId), base.DiagInternalThreshold)
		cache.InvalidateCache()
		invalidateMeasureStop()
	}

	errMap := make(base.ErrorMap)
	for vbno, ckptDoc := range ckptDocs {
		if ckptDoc == nil || ckptDoc.Len() == 0 {
			continue
		}

		key := ckpt_svc.getCheckpointDocKey(replicationId, vbno)

		ckpt_json, err := json.Marshal(ckptDoc)
		if err != nil {
			return err
		}

		//always update the checkpoint without revision
		err = ckpt_svc.metadata_svc.Set(key, ckpt_json, nil)
		if err != nil {
			ckpt_svc.logger.Errorf("Failed to set checkpoint doc key=%v, err=%v\n", key, err)
			errMap[fmt.Sprintf("vb %v", vbno)] = err
			continue
		}

		for _, ckptRecord := range ckptDoc.Checkpoint_records {
			if ckptRecord == nil {
				continue
			}
			err = ckpt_svc.RecordMappings(replicationId, ckptRecord, nil)
			if err != nil {
				errMap[fmt.Sprintf("vb %v recordMapping", vbno)] = err
			}
		}
	}

	if len(errMap) > 0 {
		return fmt.Errorf(base.FlattenErrorMap(errMap))
	} else {
		return nil
	}
}

// Ensure that one single broken mapping that will be used for most, if not all, of the checkpoints, are persisted
func (ckpt_svc *CheckpointsService) PreUpsertBrokenMapping(replicationId string, specInternalId string, oneBrokenMapping *metadata.CollectionNamespaceMapping) error {
	if oneBrokenMapping == nil || len(*oneBrokenMapping) == 0 {
		return nil
	}
	return ckpt_svc.brokenMapRefCountSvc.RegisterMapping(replicationId, specInternalId, oneBrokenMapping, true)
}

func (ckpt_svc *CheckpointsService) UpsertBrokenMapping(replicationId string, specInternalId string) error {
	return ckpt_svc.brokenMapRefCountSvc.UpsertMapping(replicationId, specInternalId)
}

func (ckpt_svc *CheckpointsService) UpsertGlobalInfo(replicationId string, specInternalId string) error {
	return ckpt_svc.globalInfoRefCountSvc.UpsertGlobalInfo(replicationId, specInternalId, true)
}

func (ckpt_svc *CheckpointsService) PreUpsertGlobalInfo(replicationId string, specInternalId string, globalts metadata.GlobalInfo, globalCounters metadata.GlobalInfo) base.ErrorMap {
	errMap := make(base.ErrorMap)
	// Register global timestamp mapping (no metaKV write)
	if err := ckpt_svc.globalInfoRefCountSvc.RegisterMapping(replicationId, specInternalId, globalts, false); err != nil {
		errMap["register_global_timestamp"] = err
	}
	// Register global counters mapping (no metaKV write)
	if err := ckpt_svc.globalInfoRefCountSvc.RegisterMapping(replicationId, specInternalId, globalCounters, false); err != nil {
		errMap["register_global_counters"] = err
	}
	// Attempt to upsert mappings to metaKV
	// Note: If an upsert is already in progress, this will return errUpsertAlreadyOccurring and the persistence will be retried later.
	// Hence ok to ignore errUpsertAlreadyOccurring.
	if err := ckpt_svc.globalInfoRefCountSvc.UpsertGlobalInfo(replicationId, specInternalId, false); err != nil && err != errUpsertAlreadyOccurring {
		errMap["upsert_to_metakv"] = err
	}
	return errMap
}

func (ckpt_svc *CheckpointsService) LoadAllShaMappings(replicationId string) (*metadata.CollectionNsMappingsDoc, *metadata.GlobalInfoCompressedDoc, error) {
	accessTokenCh, err := ckpt_svc.getCkptDocsWithBrokenMappingAccess(replicationId)
	if err != nil {
		ckpt_svc.logger.Errorf("Unable to get access token for replId %v due to err %v", replicationId, err)
		return nil, nil, err
	}
	// Get the actual go-ahead
	<-accessTokenCh
	// Once gotten it, will need to return it once done
	defer func() {
		accessTokenCh <- true
	}()

	_, collectionNsMappingDoc, _, _, err := ckpt_svc.loadBrokenMappingsInternal(replicationId)
	if err != nil {
		return nil, nil, err
	}

	_, globalInfoDoc, _, _, err := ckpt_svc.loadGlobalInfoMappingsInternal(replicationId)
	if err != nil {
		return nil, nil, err
	}

	return collectionNsMappingDoc, globalInfoDoc, nil
}

func (ckpt_svc *CheckpointsService) LoadGlobalInfoMapping(replicationId string) (metadata.ShaToGlobalInfoMap, *metadata.GlobalInfoCompressedDoc, service_def.IncrementerFunc, bool, error) {
	accessTokenCh, err := ckpt_svc.getCkptDocsWithBrokenMappingAccess(replicationId)
	if err != nil {
		ckpt_svc.logger.Errorf("Unable to get access token for replId %v due to err %v", replicationId, err)
		return nil, nil, nil, false, err
	}
	// Get the actual go-ahead
	<-accessTokenCh
	// Once gotten it, will need to return it once done
	defer func() {
		accessTokenCh <- true
	}()
	return ckpt_svc.loadGlobalInfoMappingsInternal(replicationId)
}

func (ckpt_svc *CheckpointsService) LoadBrokenMappings(replicationId string) (metadata.ShaToCollectionNamespaceMap, *metadata.CollectionNsMappingsDoc, service_def.IncrementerFunc, bool, error) {
	accessTokenCh, err := ckpt_svc.getCkptDocsWithBrokenMappingAccess(replicationId)
	if err != nil {
		ckpt_svc.logger.Errorf("Unable to get access token for replId %v due to err %v", replicationId, err)
		return nil, nil, nil, false, err
	}
	// Get the actual go-ahead
	<-accessTokenCh
	// Once gotten it, will need to return it once done
	defer func() {
		accessTokenCh <- true
	}()
	return ckpt_svc.loadBrokenMappingsInternal(replicationId)
}

func (ckpt_svc *CheckpointsService) loadBrokenMappingsInternal(replicationId string) (metadata.ShaToCollectionNamespaceMap, *metadata.CollectionNsMappingsDoc, service_def.IncrementerFunc, bool, error) {
	shaMap, err := ckpt_svc.brokenMapRefCountSvc.GetShaNamespaceMap(replicationId)
	if err != nil {
		var emptyMap metadata.ShaToCollectionNamespaceMap
		return emptyMap, nil, nil, false, err
	}
	alreadyExists := len(shaMap) > 0

	mappingsDoc, err := ckpt_svc.brokenMapRefCountSvc.GetMappingsDoc(replicationId, !alreadyExists /*initIfNotFound*/)
	if err != nil {
		var emptyMap metadata.ShaToCollectionNamespaceMap
		return emptyMap, nil, nil, false, err
	}

	collectionNsMappingDoc := (*metadata.CollectionNsMappingsDoc)(mappingsDoc)
	shaToNamespaceMap, err := ckpt_svc.brokenMapRefCountSvc.GetShaToCollectionNsMap(replicationId, collectionNsMappingDoc)
	if err != nil {
		var emptyMap metadata.ShaToCollectionNamespaceMap
		return emptyMap, nil, nil, false, err
	}

	incrementerFunc, err := ckpt_svc.brokenMapRefCountSvc.GetIncrementerFunc(replicationId)
	if err != nil {
		var emptyMap metadata.ShaToCollectionNamespaceMap
		return emptyMap, nil, nil, false, err
	}

	return shaToNamespaceMap, collectionNsMappingDoc, incrementerFunc, alreadyExists, nil
}

func (ckpt_svc *CheckpointsService) loadGlobalInfoMappingsInternal(replicationId string) (metadata.ShaToGlobalInfoMap, *metadata.GlobalInfoCompressedDoc, service_def.IncrementerFunc, bool, error) {
	shaMap, err := ckpt_svc.globalInfoRefCountSvc.GetShaGlobalInfoMap(replicationId)
	if err != nil {
		var emptyMap metadata.ShaToGlobalInfoMap
		return emptyMap, nil, nil, false, err
	}
	alreadyExists := len(shaMap) > 0

	mappingsDoc, err := ckpt_svc.globalInfoRefCountSvc.GetMappingsDoc(replicationId, !alreadyExists /*initIfNotFound*/)
	if err != nil {
		var emptyMap metadata.ShaToGlobalInfoMap
		return emptyMap, nil, nil, false, err
	}

	gInfoCompressedDoc := (*metadata.GlobalInfoCompressedDoc)(mappingsDoc)
	shaToGInfoMap, err := ckpt_svc.globalInfoRefCountSvc.GetShaToGlobalInfoMap(replicationId, gInfoCompressedDoc)
	if err != nil {
		var emptyMap metadata.ShaToGlobalInfoMap
		return emptyMap, nil, nil, false, err
	}

	incrementerFunc, err := ckpt_svc.globalInfoRefCountSvc.GetIncrementerFunc(replicationId)
	if err != nil {
		var emptyMap metadata.ShaToGlobalInfoMap
		return emptyMap, nil, nil, false, err
	}

	return shaToGInfoMap, gInfoCompressedDoc, incrementerFunc, alreadyExists, nil
}

// When each vb does checkpointing and adds a new checkpoint record
// Ensure that the old, bumped out ones are refcounted correctly
// And do refcount for the newly added record's count as well
func (ckpt_svc *CheckpointsService) RecordMappings(replicationId string, ckptRecord *metadata.CheckpointRecord, removedRecords []*metadata.CheckpointRecord) error {
	incrementerFunc, err := ckpt_svc.brokenMapRefCountSvc.GetIncrementerFunc(replicationId)
	if err != nil {
		return err
	}
	decrementerFunc, err := ckpt_svc.brokenMapRefCountSvc.GetDecrementerFunc(replicationId)
	if err != nil {
		return err
	}

	globalTsInc, err := ckpt_svc.globalInfoRefCountSvc.GetIncrementerFunc(replicationId)
	if err != nil {
		return err
	}

	globalTsDec, err := ckpt_svc.globalInfoRefCountSvc.GetDecrementerFunc(replicationId)
	if err != nil {
		return err
	}

	clonedBrokenMaps := ckptRecord.BrokenMappings()
	for sha, oneBrokenMap := range clonedBrokenMaps {
		if oneBrokenMap == nil || len(*oneBrokenMap) == 0 {
			continue
		}
		incrementerFunc(sha, oneBrokenMap)
	}

	if ckptRecord.GlobalTimestampSha256 != "" && ckptRecord.GlobalTimestamp != nil {
		globalTsInc(ckptRecord.GlobalTimestampSha256, &ckptRecord.GlobalTimestamp)
	}
	if ckptRecord.GlobalCountersSha256 != "" && ckptRecord.GlobalCounters != nil {
		globalTsInc(ckptRecord.GlobalCountersSha256, &ckptRecord.GlobalCounters)
	}

	// The following was in a potential wrong place in earlier releases - we should attempt to increment first before dec
	for _, removedRecord := range removedRecords {
		if removedRecord == nil {
			continue
		}
		if removedRecord.GetBrokenMappingShaCount() > 0 {
			for _, oneSha256 := range removedRecord.GetBrokenMappingSha256s() {
				decrementerFunc(oneSha256)
			}
		}
		if removedRecord.GlobalTimestampSha256 != "" {
			globalTsDec(removedRecord.GlobalTimestampSha256)
		}
		if removedRecord.GlobalCountersSha256 != "" {
			globalTsDec(removedRecord.GlobalCountersSha256)
		}
	}
	return nil
}

func (ckpt_svc *CheckpointsService) getCache(replicationId string) (CheckpointsServiceCache, error) {
	ckpt_svc.specsMtx.RLock()
	defer ckpt_svc.specsMtx.RUnlock()

	if cache, found := ckpt_svc.ckptCaches[replicationId]; found {
		return cache, nil
	} else {
		return nil, base.ErrorNotFound
	}
}

// When brokenMappingsNeeded is true, the ckpts will have mappings populated, which takes more work
// GlobalTimestamp "mapping" will also piggy-back off of the same exclusive access control
func (ckpt_svc *CheckpointsService) CheckpointsDocs(replicationId string, brokenMappingsNeeded bool) (map[uint16]*metadata.CheckpointsDoc, error) {
	checkpointsDocs := make(map[uint16]*metadata.CheckpointsDoc)

	if brokenMappingsNeeded {
		accessTokenCh, err := ckpt_svc.getCkptDocsWithBrokenMappingAccess(replicationId)
		if err != nil {
			ckpt_svc.logger.Errorf("Unable to get access token for replId %v due to err %v", replicationId, err)
			return nil, err
		}
		// Get the actual go-ahead
		<-accessTokenCh
		// Once gotten it, will need to return it once done
		defer func() {
			accessTokenCh <- true
		}()
	}

	cache, cacheErr := ckpt_svc.getCache(replicationId)
	if cacheErr == nil {
		latestDocs, err := cache.GetLatestDocs()
		if err == nil {
			return latestDocs, nil
		}
	}

	catalogKey := ckpt_svc.getCheckpointCatalogKey(replicationId)

	ckpt_entries, err := ckpt_svc.metadata_svc.GetAllMetadataFromCatalog(catalogKey)
	if err != nil {
		ckpt_svc.logger.Errorf("%v had error when getting key %v - %v", replicationId, catalogKey, err)
		return nil, err
	}

	var ckptSvcCntIsPopulated bool

	var shaToBrokenMapping metadata.ShaToCollectionNamespaceMap
	var CollectionNsMappingsDoc *metadata.CollectionNsMappingsDoc
	var refCounterRecorder service_def.IncrementerFunc

	var shaToGlobalInfo metadata.ShaToGlobalInfoMap
	var globalInfoDoc *metadata.GlobalInfoCompressedDoc
	var globalInfoRecorder service_def.IncrementerFunc

	if brokenMappingsNeeded {
		shaToBrokenMapping, CollectionNsMappingsDoc, refCounterRecorder, ckptSvcCntIsPopulated, err = ckpt_svc.loadBrokenMappingsInternal(replicationId)
		if err != nil {
			ckpt_svc.logger.Errorf("Error when getting brokenMapping for %v - %v", replicationId, err)
			return nil, err
		}

		shaToGlobalInfo, globalInfoDoc, globalInfoRecorder, _, err = ckpt_svc.loadGlobalInfoMappingsInternal(replicationId)
		if err != nil {
			ckpt_svc.logger.Errorf("Error when getting GlobalTs for %v - %v", replicationId, err)
			return nil, err
		}
	}

	mtx := ckpt_svc.getStopTheWorldMtx(replicationId)
	if !ckptSvcCntIsPopulated {
		// This code path's initialization requires doing a level of ref counting
		// As a result, the init path needs to take place first before the rest can do read-only op
		mtx.Lock()
		defer mtx.Unlock()
	} else {
		mtx.RLock()
		defer mtx.RUnlock()
	}

	for _, ckpt_entry := range ckpt_entries {
		if ckpt_entry != nil {
			if ckpt_svc.isBrokenMappingDoc(ckpt_entry.Key) || ckpt_svc.isGlobalInfoDoc(ckpt_entry.Key) {
				continue
			}

			vbno, err := ckpt_svc.decodeVbnoFromCkptDocKey(ckpt_entry.Key)
			if err != nil {
				return nil, err
			}

			ckpt_doc, err := ckpt_svc.constructCheckpointDoc(ckpt_entry.Value, ckpt_entry.Rev, shaToBrokenMapping, shaToGlobalInfo)
			if err != nil {
				if err == service_def.MetadataNotFoundErr {
					ckpt_svc.logger.Errorf("Unable to construct ckpt doc from metakv given replicationId: %v vbno: %v and key: %v",
						replicationId, vbno, ckpt_entry.Key)
					continue
				} else {
					return nil, err
				}
			} else {
				checkpointsDocs[vbno] = ckpt_doc
				if brokenMappingsNeeded && !ckptSvcCntIsPopulated {
					ckpt_svc.registerCkptDocBrokenMappings(ckpt_doc, refCounterRecorder)
					ckpt_svc.registerGlobalTimestamps(ckpt_doc, globalInfoRecorder)
				}
			}
		}
	}

	if brokenMappingsNeeded {
		err = ckpt_svc.brokenMapRefCountSvc.GCDocUsingLatestCounterInfo(replicationId, CollectionNsMappingsDoc)
		if err != nil {
			ckpt_svc.logger.Errorf("Unable to GC brokenmapping cache - %v", err)
			return checkpointsDocs, err
		}

		err = ckpt_svc.globalInfoRefCountSvc.GCDocUsingLatestCounterInfo(replicationId, globalInfoDoc)
		if err != nil {
			ckpt_svc.logger.Errorf("Unable to GC globalTs cache - %v", err)
			return checkpointsDocs, err
		}

		shaToBrokenMapping, _, _, _, err = ckpt_svc.loadBrokenMappingsInternal(replicationId)
		if err != nil {
			ckpt_svc.logger.Errorf("Unable to refresh brokenmapping cache - %v", err)
			return checkpointsDocs, err
		}

		shaToGlobalInfo, _, _, _, err = ckpt_svc.loadGlobalInfoMappingsInternal(replicationId)
		if err != nil {
			ckpt_svc.logger.Errorf("Unable to refresh globalTimestamp cache - %v", err)
			return checkpointsDocs, err
		}

		err = ckpt_svc.brokenMapRefCountSvc.InitCounterShaToActualMappings(replicationId, CollectionNsMappingsDoc.SpecInternalId, shaToBrokenMapping)
		if err == nil {
			ckpt_svc.logger.Infof("Loaded brokenMap: %v", shaToBrokenMapping)
		} else {
			ckpt_svc.logger.Warnf("Error %v trying init sha counter to mapping: %v", err, shaToBrokenMapping)
		}

		err = ckpt_svc.globalInfoRefCountSvc.InitCounterShaToActualMappings(replicationId, CollectionNsMappingsDoc.SpecInternalId, shaToGlobalInfo)
		if err == nil {
			ckpt_svc.logger.Infof("Loaded globalTs: %v", shaToGlobalInfo)
		} else {
			ckpt_svc.logger.Warnf("Error %v trying init sha counter to globalInfo: %v", err, shaToGlobalInfo)
		}

		// BrokenMappingsNeeded means the checkpoints themselves have brokenMaps inside them and is valid for caching
		if cacheErr != base.ErrorNotFound {
			// Cache exists - and need to (re)-initialize this cache with the latest info
			// StoreLatestDocs cannot be called concurrently. And because brokenMappingsNeeded == true means
			// that only one caller at a time, this is safe
			cacheErr = cache.StoreLatestDocs(checkpointsDocs)
			if cacheErr != nil {
				ckpt_svc.logger.Warnf("Unable to store latest cache for %v - %v", replicationId, cacheErr)
			}
		}
	}
	return checkpointsDocs, nil

}

func (ckpt_svc *CheckpointsService) registerCkptDocBrokenMappings(ckpt_doc *metadata.CheckpointsDoc, recorder service_def.IncrementerFunc) {
	if ckpt_doc == nil || recorder == nil {
		return
	}
	for _, record := range ckpt_doc.Checkpoint_records {
		if record == nil || record.BrokenMappingsLen() == 0 {
			continue
		}

		for sha, oneMapping := range record.BrokenMappings() {
			if oneMapping == nil || len(*oneMapping) == 0 {
				continue
			}
			recorder(sha, oneMapping)
		}
	}
}

func (ckpt_svc *CheckpointsService) registerGlobalTimestamps(ckpt_doc *metadata.CheckpointsDoc, recorder service_def.IncrementerFunc) {
	if ckpt_doc == nil || recorder == nil || ckpt_doc.IsTraditional() {
		return
	}
	for _, record := range ckpt_doc.Checkpoint_records {
		if record != nil {
			if record.GlobalTimestampSha256 != "" {
				recorder(record.GlobalTimestampSha256, &record.GlobalTimestamp)
			}
			if record.GlobalCountersSha256 != "" {
				recorder(record.GlobalCountersSha256, &record.GlobalCounters)
			}
		}
	}
}

func (ckpt_svc *CheckpointsService) constructCheckpointDoc(content []byte, rev interface{}, shaToBrokenMapping metadata.ShaToCollectionNamespaceMap, shaToGInfoMapping metadata.ShaToGlobalInfoMap) (*metadata.CheckpointsDoc, error) {
	// The only time content is empty is when this is a fresh XDCR system and no checkpoints has been registered yet
	if len(content) > 0 {
		ckpt_doc := &metadata.CheckpointsDoc{}
		err := json.Unmarshal(content, ckpt_doc)
		if err != nil {
			return nil, err
		}

		// Must populate global timestamp mapping first as the mapping is needed to restore
		// the actual brokenmapping sha's needed for actual broken map population
		if len(shaToGInfoMapping) > 0 {
			err := ckpt_svc.populateGlobalInfoMapping(ckpt_doc, shaToGInfoMapping)
			if err != nil {
				return nil, err
			}
		}
		if len(shaToBrokenMapping) > 0 {
			err := ckpt_svc.populateActualMapping(ckpt_doc, shaToBrokenMapping)
			if err != nil {
				return nil, err
			}
		}

		return ckpt_doc, nil
	} else {
		return nil, service_def.MetadataNotFoundErr
	}
}

func (ckpt_svc *CheckpointsService) populateActualMapping(doc *metadata.CheckpointsDoc, shaToActualMapping metadata.ShaToCollectionNamespaceMap) error {
	if doc == nil {
		return nil
	}

	errMap := make(base.ErrorMap)
	for _, record := range doc.Checkpoint_records {
		if record == nil {
			continue
		}

		shaMapToFill := record.GetShaOnlyMap()
		if len(shaMapToFill) == 0 {
			continue
		}

		for oneShaToFill, _ := range shaMapToFill {
			mapping, exists := shaToActualMapping[oneShaToFill]
			if !exists {
				errMap[oneShaToFill] = base.ErrorNotFound
				continue
			}
			shaMapToFill[oneShaToFill] = mapping
		}

		err := record.LoadBrokenMapping(shaMapToFill)
		if err != nil {
			errMap[fmt.Sprintf("populateActualMapping traditional? %v for record created at %v loadMapping %v", record.IsTraditional(), record.CreationTime, shaMapToFill)] = err
		}
	}
	if len(errMap) > 0 {
		return fmt.Errorf("populateActualMapping for doc internal ID %v Unable to find shas %v",
			doc.SpecInternalId, base.FlattenErrorMap(errMap))
	}
	return nil
}

func (ckpt_svc *CheckpointsService) populateGlobalInfoMapping(doc *metadata.CheckpointsDoc, shaToGInfoMapping metadata.ShaToGlobalInfoMap) error {
	if doc == nil {
		return nil
	}

	errMap := make(base.ErrorMap)
	for _, record := range doc.Checkpoint_records {
		if record == nil {
			continue
		}

		shaMapToFill := record.GetGlobalInfoMap()
		if len(shaMapToFill) == 0 {
			continue
		}

		for oneShaToFill, _ := range shaMapToFill {
			mapping, exists := shaToGInfoMapping[oneShaToFill]
			if !exists {
				errMap[oneShaToFill] = base.ErrorNotFound
				continue
			}
			shaMapToFill[oneShaToFill] = mapping
		}

		err := record.LoadGlobalInfoMapping(shaMapToFill)
		if err != nil {
			errMap[fmt.Sprintf("populateGlobalInfoMapping traditional? %v for record created at %v loadMapping %v", record.IsTraditional(), record.CreationTime, shaMapToFill)] = err
		}
	}
	if len(errMap) > 0 {
		return fmt.Errorf("populateGlobalInfoMapping for doc internal ID %v Unable to find shas %v",
			doc.SpecInternalId, base.FlattenErrorMap(errMap))
	}
	return nil
}

// get vbnos of checkpoint docs for specified replicationId
func (ckpt_svc *CheckpointsService) GetVbnosFromCheckpointDocs(replicationId string) ([]uint16, error) {
	vbnos := make([]uint16, 0)
	catalogKey := ckpt_svc.getCheckpointCatalogKey(replicationId)
	ckpt_entries, err := ckpt_svc.metadata_svc.GetAllMetadataFromCatalog(catalogKey)
	if err != nil {
		return nil, err
	}

	for _, ckpt_entry := range ckpt_entries {
		if ckpt_entry != nil {
			vbno, err := ckpt_svc.decodeVbnoFromCkptDocKey(ckpt_entry.Key)
			if err != nil {
				return nil, err
			}
			vbnos = append(vbnos, vbno)
		}
	}
	return vbnos, nil
}

func (ckpt_svc *CheckpointsService) ReplicationSpecChangeCallback(metadataId string, oldVal interface{}, newVal interface{}, wg *sync.WaitGroup) error {
	if wg != nil {
		defer wg.Done()
	}

	oldSpec, ok := oldVal.(*metadata.ReplicationSpecification)
	if !ok {
		return base.ErrorInvalidInput
	}
	newSpec, ok := newVal.(*metadata.ReplicationSpecification)
	if !ok {
		return base.ErrorInvalidInput
	}

	if oldSpec != nil && newSpec == nil {
		ckpt_svc.specsMtx.Lock()
		delete(ckpt_svc.cachedSpecs, oldSpec.Id)
		delete(ckpt_svc.stopTheWorldMtx, oldSpec.Id)
		ckpt_svc.delCheckpointsDocsSerializeMapNoLock(oldSpec.Id)
		if ckpt_svc.ckptCaches[oldSpec.Id] != nil {
			ckpt_svc.ckptCaches[oldSpec.Id].SpecChangeCb(oldSpec, newSpec)
			delete(ckpt_svc.ckptCaches, oldSpec.Id)
		}
		backfillId := common.ComposeFullTopic(oldSpec.Id, common.BackfillPipeline)
		if ckpt_svc.ckptCaches[backfillId] != nil {
			ckpt_svc.ckptCaches[backfillId].SpecChangeCb(oldSpec, newSpec)
			delete(ckpt_svc.ckptCaches, backfillId)
		}
		ckpt_svc.specsMtx.Unlock()
		cleanupErr := ckpt_svc.brokenMapRefCountSvc.CleanupMapping(oldSpec.Id, ckpt_svc.utils)
		if cleanupErr != nil {
			ckpt_svc.logger.Errorf("DelCheckpointsDoc for %v brokenmapping had error: %v - manual clean up may be required", oldSpec.Id, cleanupErr)
		}
		cleanupErr = ckpt_svc.brokenMapRefCountSvc.CleanupMapping(backfillId, ckpt_svc.utils)
		if cleanupErr != nil {
			ckpt_svc.logger.Errorf("DelCheckpointsDoc for %v brokenmapping had error: %v - manual clean up may be required", backfillId, cleanupErr)
		}
		cleanupErr = ckpt_svc.globalInfoRefCountSvc.CleanupMapping(oldSpec.Id, ckpt_svc.utils)
		if cleanupErr != nil {
			ckpt_svc.logger.Errorf("DelGlobalTsMapping for %v had error: %v - manual clean up may be required", oldSpec.Id, cleanupErr)
		}
		cleanupErr = ckpt_svc.globalInfoRefCountSvc.CleanupMapping(backfillId, ckpt_svc.utils)
		if cleanupErr != nil {
			ckpt_svc.logger.Errorf("DelGlobalTsMapping for %v had error: %v - manual clean up may be required", backfillId, cleanupErr)
		}
	} else {
		if oldSpec == nil && newSpec != nil {
			backfillId := common.ComposeFullTopic(newSpec.Id, common.BackfillPipeline)

			waitGrp := sync.WaitGroup{}
			waitGrp.Add(2)
			go ckpt_svc.cleanOldLeftoverCkpts(newSpec.Id, newSpec.InternalId, &waitGrp)
			go ckpt_svc.cleanOldLeftoverCkpts(backfillId, newSpec.InternalId, &waitGrp)
			waitGrp.Wait()

			ckpt_svc.brokenMapRefCountSvc.InitTopicShaCounterWithInternalId(newSpec.Id, newSpec.InternalId)
			ckpt_svc.brokenMapRefCountSvc.InitTopicShaCounterWithInternalId(backfillId, newSpec.InternalId)
			ckpt_svc.globalInfoRefCountSvc.InitTopicShaCounterWithInternalId(newSpec.Id, newSpec.InternalId)
			ckpt_svc.globalInfoRefCountSvc.InitTopicShaCounterWithInternalId(backfillId, newSpec.InternalId)
		}
		ckpt_svc.specsMtx.Lock()
		ckpt_svc.cachedSpecs[newSpec.Id] = newSpec
		ckpt_svc.stopTheWorldMtx[newSpec.Id] = &sync.RWMutex{}
		if ckpt_svc.ckptCaches[newSpec.Id] == nil {
			ckpt_svc.ckptCaches[newSpec.Id] = NewCheckpointsServiceCache(ckpt_svc.logger, common.ComposeFullTopic(newSpec.Id, common.MainPipeline))
			ckpt_svc.ckptCaches[common.ComposeFullTopic(newSpec.Id, common.BackfillPipeline)] = NewCheckpointsServiceCache(ckpt_svc.logger, common.ComposeFullTopic(newSpec.Id, common.BackfillPipeline))
		}
		ckpt_svc.ckptCaches[newSpec.Id].SpecChangeCb(oldSpec, newSpec)
		ckpt_svc.ckptCaches[common.ComposeFullTopic(newSpec.Id, common.BackfillPipeline)].SpecChangeCb(oldSpec, newSpec)
		ckpt_svc.initCheckpointsDocsSerializeMapNoLock(newSpec.Id)
		ckpt_svc.specsMtx.Unlock()
	}
	return nil
}

func (ckpt_svc *CheckpointsService) cleanOldLeftoverCkpts(id, internalId string, w *sync.WaitGroup) {
	defer w.Done()
	oldCkptDocs, err := ckpt_svc.CheckpointsDocs(id, false)
	if err == nil && oldCkptDocs != nil {
		castedDocs := metadata.VBsCkptsDocMap(oldCkptDocs)
		if !castedDocs.InternalIdMatch(internalId) {
			ckpt_svc.logger.Warnf("Old ckpt docs with the same replId %v found - cleaning them up", id)
			ckpt_svc.DelCheckpointsDocs(id)
		}
	}
}

func (ckpt_svc *CheckpointsService) BackfillReplicationSpecChangeCallback(id string, oldVal interface{}, newVal interface{}) error {
	oldSpec, ok := oldVal.(*metadata.BackfillReplicationSpec)
	if !ok {
		return base.ErrorInvalidInput
	}
	newSpec, ok := newVal.(*metadata.BackfillReplicationSpec)
	if !ok {
		return base.ErrorInvalidInput
	}

	if oldSpec != nil && newSpec == nil {
		ckpt_svc.backfillSpecsMtx.Lock()
		delete(ckpt_svc.cachedBackfillSpecs, oldSpec.Id)
		ckpt_svc.backfillSpecsMtx.Unlock()
	} else {
		ckpt_svc.backfillSpecsMtx.Lock()
		ckpt_svc.cachedBackfillSpecs[newSpec.Id] = newSpec
		ckpt_svc.backfillSpecsMtx.Unlock()
	}
	return nil
}

func (ckpt_svc *CheckpointsService) initWithSpecs() error {
	specs, err := ckpt_svc.replicationSpecSvc.AllReplicationSpecs()
	if err != nil {
		return err
	}

	for specId, spec := range specs {
		ckpt_svc.cachedSpecs[specId] = spec
		ckpt_svc.stopTheWorldMtx[specId] = &sync.RWMutex{}
		ckpt_svc.initCheckpointsDocsSerializeMapNoLock(specId)
		ckpt_svc.initBrokenMapRefCounter(specId, spec)
		ckpt_svc.initGlobalInfoCounter(specId, spec)
	}

	return nil
}

func (ckpt_svc *CheckpointsService) initBrokenMapRefCounter(specId string, spec *metadata.ReplicationSpecification) {
	alreadyExists := ckpt_svc.brokenMapRefCountSvc.InitTopicShaCounterWithInternalId(specId, spec.InternalId)
	if alreadyExists {
		// Odd error - shouldn't happen
		ckpt_svc.logger.Warnf("BrokenMapRefCounter with spec %v internal %v already exists", specId, spec.InternalId)
	}
	backfillSpecId := common.ComposeFullTopic(spec.Id, common.BackfillPipeline)
	alreadyExists = ckpt_svc.brokenMapRefCountSvc.InitTopicShaCounterWithInternalId(backfillSpecId, spec.InternalId)
	if alreadyExists {
		// Odd error - shouldn't happen
		ckpt_svc.logger.Warnf("BrokenMapRefCounter with spec %v internal %v already exists", backfillSpecId, spec.InternalId)
	}
}

func (ckpt_svc *CheckpointsService) initGlobalInfoCounter(specId string, spec *metadata.ReplicationSpecification) {
	alreadyExists := ckpt_svc.globalInfoRefCountSvc.InitTopicShaCounterWithInternalId(specId, spec.InternalId)
	if alreadyExists {
		// Odd error - shouldn't happen
		ckpt_svc.logger.Warnf("GlobalInfoCounter with spec %v internal %v already exists", specId, spec.InternalId)
	}
	backfillSpecId := common.ComposeFullTopic(spec.Id, common.BackfillPipeline)
	alreadyExists = ckpt_svc.globalInfoRefCountSvc.InitTopicShaCounterWithInternalId(backfillSpecId, spec.InternalId)
	if alreadyExists {
		// Odd error - shouldn't happen
		ckpt_svc.logger.Warnf("GlobalInfoCounter with spec %v internal %v already exists", backfillSpecId, spec.InternalId)
	}
}

func (ckpt_svc *CheckpointsService) initCheckpointsDocsSerializeMapNoLock(specId string) {
	for i := common.PipelineTypeBegin; i < common.PipelineTypeInvalidEnd; i++ {
		ckpt_svc.getBrokenMapAccessTokenMap[common.ComposeFullTopic(specId, i)] = make(chan bool, 1)
		ckpt_svc.getBrokenMapAccessTokenMap[common.ComposeFullTopic(specId, i)] <- true
	}
}

func (ckpt_svc *CheckpointsService) delCheckpointsDocsSerializeMapNoLock(specId string) {
	for i := common.PipelineTypeBegin; i < common.PipelineTypeInvalidEnd; i++ {
		delete(ckpt_svc.getBrokenMapAccessTokenMap, common.ComposeFullTopic(specId, i))
		delete(ckpt_svc.getBrokenMapAccessTokenMap, common.ComposeFullTopic(specId, i))
	}
}

func (ckpt_svc *CheckpointsService) getStopTheWorldMtx(replId string) *sync.RWMutex {
	ckpt_svc.specsMtx.RLock()
	defer ckpt_svc.specsMtx.RUnlock()

	mtx, exists := ckpt_svc.stopTheWorldMtx[replId]
	if !exists {
		// potential race condition between spec deletion and access, return dummy
		return &sync.RWMutex{}
	} else {
		return mtx
	}
}

func (ckpt_svc *CheckpointsService) UpsertAndReloadCheckpointCompleteSet(replicationId string, mappingDoc *metadata.CollectionNsMappingsDoc, ckptDocs map[uint16]*metadata.CheckpointsDoc, internalId string, gInfoDoc *metadata.GlobalInfoCompressedDoc) error {
	err := ckpt_svc.validateSpecIsValid(replicationId, internalId)
	if err != nil {
		return err
	}

	mtx := ckpt_svc.getStopTheWorldMtx(replicationId)

	stopFunc := ckpt_svc.utils.StartDiagStopwatch("ckpt_svc.UpsertAndReloadCheckpointCompleteSet.Lock", base.DiagCkptMergeThreshold)
	mtx.Lock()
	stopFunc()

	// This step will take the set of {ckptDocs, brokenMaps} and use it as the gold standard for ref counting
	stopFunc2 := ckpt_svc.utils.StartDiagStopwatch("ckpt_svc.UpsertAndReloadCheckpointCompleteSet.brokenMap.reInitUsingMergedMappingDoc", base.DiagCkptMergeThreshold)
	reInitErr := ckpt_svc.brokenMapRefCountSvc.reInitUsingMergedMappingDoc(replicationId, mappingDoc, ckptDocs, internalId)
	stopFunc2()
	if reInitErr != nil {
		mtx.Unlock()
		return fmt.Errorf("%v - brokenMap.reInitUsingMergedMappingDoc error: %v", replicationId, reInitErr)
	}

	stopFuncGts := ckpt_svc.utils.StartDiagStopwatch("ckpt_svc.UpsertAndReloadCheckpointCompleteSet.globalInfo.reInitUsingMergedMappingDoc", base.DiagCkptMergeThreshold)
	reInitErr = ckpt_svc.globalInfoRefCountSvc.reInitUsingMergedMappingDoc(replicationId, gInfoDoc, ckptDocs, internalId)
	stopFuncGts()
	if reInitErr != nil {
		mtx.Unlock()
		return fmt.Errorf("%v - globalTs.reInitUsingMergedMappingDoc error: %v", replicationId, reInitErr)
	}

	// Once mapping is committed, invalidate the cache and write the ckpts
	stopFunc3 := ckpt_svc.utils.StartDiagStopwatch("ckpt_svc.UpsertAndReloadCheckpointCompleteSet.upsertCheckpointsDoc", base.DiagCkptMergeThreshold)
	upsertErr := ckpt_svc.upsertCheckpointsDoc(replicationId, ckptDocs, internalId)
	stopFunc3()
	if reInitErr != nil {
		mtx.Unlock()
		return fmt.Errorf("%v - upsertCheckpointsDoc error: %v", replicationId, upsertErr)
	}
	mtx.Unlock()

	// Last step is to reload from metakv which will populate the cache
	stopFunc4 := ckpt_svc.utils.StartDiagStopwatch("ckpt_svc.UpsertAndReloadCheckpointCompleteSet.ReloadCheckpointsDocs", base.DiagCkptMergeThreshold)
	_, reloadErr := ckpt_svc.CheckpointsDocs(replicationId, true)
	stopFunc4()
	if reloadErr != nil {
		ckpt_svc.logger.Errorf("upsertCheckpointsDoc reloading for %v had errors %v", replicationId, reloadErr)
		return reloadErr
	}
	return nil
}

func (ckpt_svc *CheckpointsService) DisableRefCntDecrement(topic string) {
	ckpt_svc.brokenMapRefCountSvc.DisableRefCntDecrement(topic)
	ckpt_svc.globalInfoRefCountSvc.DisableRefCntDecrement(topic)
}

func (ckpt_svc *CheckpointsService) EnableRefCntDecrement(topic string) {
	ckpt_svc.brokenMapRefCountSvc.EnableRefCntDecrement(topic)
	ckpt_svc.globalInfoRefCountSvc.EnableRefCntDecrement(topic)
}

func (ckpt_svc *CheckpointsService) validateSpecIsValid(fullReplId string, internalId string) error {
	replicationId, _ := common.DecomposeFullTopic(fullReplId)
	specCheck, err := ckpt_svc.replicationSpecSvc.ReplicationSpecReadOnly(replicationId)
	if err != nil {
		// Spec could have been deleted
		return fmt.Errorf("abort operation because spec %v is not found", replicationId)
	}
	if specCheck.InternalId != "" && internalId != "" && specCheck.InternalId != internalId {
		return fmt.Errorf("abort operation because spec %v internalId mismatch: expected %v actual %v",
			replicationId, internalId, specCheck.InternalId)
	}
	return nil
}

func (ckpt_svc *CheckpointsService) UpsertCheckpointsDone(replicationId string, internalId string) error {
	cache, cacheErr := ckpt_svc.getCache(replicationId)
	if cacheErr != nil {
		return cacheErr
	}
	cache.ValidateCache(internalId)
	return nil
}

// loading broken mappings into checkpointDocs is an operation that requires ref counting (ShaRefCounterService)
// So each request per replicationId must be serialized
func (ckpt_svc *CheckpointsService) getCkptDocsWithBrokenMappingAccess(replicationId string) (chan bool, error) {
	var errKeys []string
	ckpt_svc.specsMtx.RLock()
	accessCh, found := ckpt_svc.getBrokenMapAccessTokenMap[replicationId]
	if !found {
		for k, _ := range ckpt_svc.getBrokenMapAccessTokenMap {
			errKeys = append(errKeys, k)
		}
	}
	ckpt_svc.specsMtx.RUnlock()
	if !found {
		ckpt_svc.logger.Errorf("The map does not include %v but does have %v", replicationId, errKeys)
		return nil, base.ErrorNotFound
	}
	return accessCh, nil
}
