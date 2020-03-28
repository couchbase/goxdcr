package metadata_svc

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	"strconv"
	"strings"
	"sync"
)

const (
	// the key to the metadata that stores the keys of all remote clusters
	CheckpointsCatalogKeyPrefix = "ckpt"
	CheckpointsKeyPrefix        = CheckpointsCatalogKeyPrefix
	BrokenMappingKey            = "brokenMappings"
)

// Used to keep track of brokenmapping SHA and the count of checkpoint records referring to it
type BrokenMapShaRefCounter struct {
	lock           sync.RWMutex
	refCnt         map[string]uint64
	shaToMapping   metadata.ShaToCollectionNamespaceMap
	needToSync     bool // needs to sync refCnt to shaMap and then also persist to metakv
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
		if record == nil || len(record.BrokenMappingSha256) == 0 {
			continue
		}
		b.refCnt[record.BrokenMappingSha256]++
		b.shaToMapping[record.BrokenMappingSha256] = record.BrokenMappings()
	}
	b.lock.Unlock()
}

type CheckpointsService struct {
	metadata_svc service_def.MetadataSvc
	logger       *log.CommonLogger

	// Map of replicationId -> map of brokenmap sha's -> refCnts
	topicMapMtx     sync.RWMutex
	topicBrokenMaps map[string]*BrokenMapShaRefCounter
	// Prevent concurrent upsert for the same single backfill doc
	topicBrokenUpsertCh map[string]chan bool
}

func NewCheckpointsService(metadata_svc service_def.MetadataSvc, logger_ctx *log.LoggerContext) service_def.CheckpointsService {
	return &CheckpointsService{metadata_svc: metadata_svc,
		logger:              log.NewLogger("CheckpointSvc", logger_ctx),
		topicBrokenMaps:     make(map[string]*BrokenMapShaRefCounter),
		topicBrokenUpsertCh: make(map[string]chan bool),
	}
}

func (ckpt_svc *CheckpointsService) CheckpointsDoc(replicationId string, vbno uint16) (*metadata.CheckpointsDoc, error) {
	key := ckpt_svc.getCheckpointDocKey(replicationId, vbno)
	result, rev, err := ckpt_svc.metadata_svc.Get(key)
	if err != nil {
		return nil, err
	}

	ckpt_svc.topicMapMtx.RLock()
	// Should exist because checkpoint manager must finish loading before allowing ckpt operations
	counter, _ := ckpt_svc.topicBrokenMaps[replicationId]
	ckpt_svc.topicMapMtx.RUnlock()

	counter.lock.RLock()
	defer counter.lock.RUnlock()

	ckpt_doc, err := ckpt_svc.constructCheckpointDoc(result, rev, counter.shaToMapping)
	if err == service_def.MetadataNotFoundErr {
		ckpt_svc.logger.Errorf("Unable to construct ckpt doc from metakv given replication: %v vbno: %v key: %v",
			replicationId, vbno, key)
	}
	return ckpt_doc, err
}

func (ckpt_svc *CheckpointsService) SetBrokenMappingCache(replicationId, internalSpecId string, brokenMappings metadata.ShaToCollectionNamespaceMap) error {
	var counterInternalId string

	ckpt_svc.topicMapMtx.RLock()
	counter, ok := ckpt_svc.topicBrokenMaps[replicationId]
	if ok {
		counterInternalId = counter.internalSpecId
	}
	ckpt_svc.topicMapMtx.RUnlock()

	if !ok {
		return fmt.Errorf("SetBrokenMappingCache unable to find counter for %v", replicationId)
	}
	if counterInternalId != "" && counterInternalId != internalSpecId {
		return fmt.Errorf("Replication %v mismatch internalId %v vs %v", replicationId, counterInternalId, internalSpecId)
	}

	counter.lock.Lock()
	counter.shaToMapping = brokenMappings
	counter.internalSpecId = internalSpecId
	counter.lock.Unlock()
	return nil
}

func (ckpt_svc *CheckpointsService) getCheckpointCatalogKey(replicationId string) string {
	return CheckpointsCatalogKeyPrefix + base.KeyPartsDelimiter + replicationId
}

// Get a unique key to access metakv for brokenMappings
func (ckpt_svc *CheckpointsService) getBrokenMappingsDocKey(replicationId string) string {
	return fmt.Sprintf("%v", CheckpointsKeyPrefix+base.KeyPartsDelimiter+replicationId+base.KeyPartsDelimiter+BrokenMappingKey)
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

func (ckpt_svc *CheckpointsService) delBrokenMappingsDoc(replicationId string) {
	brokenMappingsDocKey := ckpt_svc.getBrokenMappingsDocKey(replicationId)

	ckpt_svc.topicMapMtx.RLock()
	upsertCh, ok := ckpt_svc.topicBrokenUpsertCh[replicationId]
	_, ok2 := ckpt_svc.topicBrokenMaps[replicationId]
	ckpt_svc.topicMapMtx.RUnlock()

	if !ok || !ok2 {
		// Things weren't setup correctly - just delete everything
		ckpt_svc.topicMapMtx.Lock()
		delete(ckpt_svc.topicBrokenMaps, replicationId)
		if !ok {
			close(ckpt_svc.topicBrokenUpsertCh[replicationId])
		}
		delete(ckpt_svc.topicBrokenUpsertCh, replicationId)
		ckpt_svc.topicMapMtx.Unlock()
		ckpt_svc.metadata_svc.Del(brokenMappingsDocKey, nil /*revision*/)
		return
	}

	// Don't allow any upserts to occur concurrently - force hold lock
	select {
	case <-upsertCh:
		defer func() {
			ckpt_svc.topicMapMtx.Lock()
			delete(ckpt_svc.topicBrokenMaps, replicationId)
			close(ckpt_svc.topicBrokenUpsertCh[replicationId])
			delete(ckpt_svc.topicBrokenUpsertCh, replicationId)
			ckpt_svc.topicMapMtx.Unlock()
		}()
		ckpt_svc.metadata_svc.Del(brokenMappingsDocKey, nil /*revision*/)
	}
}

func (ckpt_svc *CheckpointsService) DelCheckpointsDocs(replicationId string) error {
	ckpt_svc.logger.Infof("DelCheckpointsDocs for replication %v...", replicationId)
	catalogKey := ckpt_svc.getCheckpointCatalogKey(replicationId)
	err_ret := ckpt_svc.metadata_svc.DelAllFromCatalog(catalogKey)
	if err_ret != nil {
		ckpt_svc.logger.Errorf("Failed to delete checkpoints docs for %v\n", replicationId)
	} else {
		ckpt_svc.logger.Infof("DelCheckpointsDocs is done for %v\n", replicationId)
		ckpt_svc.delBrokenMappingsDoc(replicationId)
	}
	return err_ret
}

// Need to have correct accounting after deleting checkpoingsDocs
func (ckpt_svc *CheckpointsService) PostDelCheckpointsDoc(replicationId string, doc *metadata.CheckpointsDoc) error {
	ckpt_svc.topicMapMtx.RLock()
	counter := ckpt_svc.topicBrokenMaps[replicationId]
	ckpt_svc.topicMapMtx.RUnlock()

	if counter == nil || doc == nil {
		return base.ErrorInvalidInput
	}

	errMap := make(base.ErrorMap)
	counter.lock.Lock()
	for _, ckptRecord := range doc.Checkpoint_records {
		shaInRecord := ckptRecord.BrokenMappingSha256
		checkCount, cacheShaExists := counter.refCnt[shaInRecord]
		if !cacheShaExists || checkCount == 0 {
			// It should exist because it was just loaded
			err := fmt.Errorf("Odd error - %v missing sha hash %v from cache", replicationId, shaInRecord)
			errMap[shaInRecord] = err
			continue
		}
		counter.refCnt[shaInRecord]--
		if counter.refCnt[shaInRecord] == 0 {
			counter.needToSync = true
		}
	}
	counter.lock.Unlock()

	var err error
	if len(errMap) > 0 {
		err = fmt.Errorf(base.FlattenErrorMap(errMap))
	}
	return err
}

func (ckpt_svc *CheckpointsService) DelCheckpointsDoc(replicationId string, vbno uint16) error {
	ckpt_svc.logger.Debugf("DelCheckpointsDoc for replication %v and vbno %v...", replicationId, vbno)
	key := ckpt_svc.getCheckpointDocKey(replicationId, vbno)
	_, rev, err := ckpt_svc.metadata_svc.Get(key)
	if err != nil {
		return err
	}
	catalogKey := ckpt_svc.getCheckpointCatalogKey(replicationId)
	err = ckpt_svc.metadata_svc.DelWithCatalog(catalogKey, key, rev)
	if err != nil {
		ckpt_svc.logger.Errorf("Failed to delete checkpoints doc for replication %v and vbno %v\n", replicationId, vbno)
	} else {
		ckpt_svc.logger.Debugf("DelCheckpointsDoc is done for replication %v and vbno %v\n", replicationId, vbno)
	}
	return err
}

// in addition to upserting checkpoint record, this method may also update xattr seqno
// and target cluster version in checkpoint doc
// these operations are done in the same metakv operation to ensure that they succeed and fail together
// Returns size of the checkpoint
func (ckpt_svc *CheckpointsService) UpsertCheckpoints(replicationId string, specInternalId string, vbno uint16,
	ckpt_record *metadata.CheckpointRecord, xattr_seqno uint64, targetClusterVersion int) (int, error) {
	ckpt_svc.logger.Debugf("Persisting checkpoint record=%v xattr_seqno=%v for vbno=%v replication=%v\n", ckpt_record, xattr_seqno, vbno, replicationId)
	var size int

	if ckpt_record == nil {
		return size, errors.New("nil checkpoint record")
	}
	key := ckpt_svc.getCheckpointDocKey(replicationId, vbno)
	ckpt_doc, err := ckpt_svc.CheckpointsDoc(replicationId, vbno)
	if err == service_def.MetadataNotFoundErr {
		ckpt_doc = metadata.NewCheckpointsDoc(specInternalId)
		err = nil
	}
	if err != nil {
		return size, err
	}

	// check if xattr seqno in checkpoint doc needs to be updated
	if xattr_seqno > 0 {
		// the "ckpt_doc.XattrSeqno > xattr_seqno" check is needed since ckpt_doc.XattrSeqno
		// could go backward in rollback scenario
		if ckpt_doc.XattrSeqno == 0 || ckpt_doc.XattrSeqno > xattr_seqno {
			ckpt_doc.XattrSeqno = xattr_seqno
		}
	}

	// check if target cluster version in checkpoint doc needs to be updated
	// since cluster downgrade is not allowed, target version can only go up
	if ckpt_doc.TargetClusterVersion < targetClusterVersion {
		ckpt_doc.TargetClusterVersion = targetClusterVersion
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
			ckpt_svc.logger.Debugf("Wrote checkpoint doc key=%v, Size=%v\n", key, ckpt_doc.Size())
			size = ckpt_doc.Size()
			err = ckpt_svc.RecordBrokenMappings(replicationId, ckpt_record, removedRecords)
			if err != nil {
				ckpt_svc.logger.Errorf("Failed to record broken mapping err=%v\n", err)
			}
		}
	}
	return size, err
}

// Ensure that one single broken mapping that will be used for most, if not all, of the checkpoints, are persisted
func (ckpt_svc *CheckpointsService) PreUpsertBrokenMapping(replicationId string, specInternalId string, oneBrokenMapping *metadata.CollectionNamespaceMapping) error {
	if oneBrokenMapping == nil {
		return nil
	}

	ckpt_svc.topicMapMtx.RLock()
	refCounter, ok := ckpt_svc.topicBrokenMaps[replicationId]
	ckpt_svc.topicMapMtx.RUnlock()

	if !ok || refCounter == nil {
		return base.ErrorInvalidInput
	}

	sha, err := oneBrokenMapping.Sha256()
	if err != nil {
		return fmt.Errorf("Unable to get sha256 %v for mapping %v", replicationId, oneBrokenMapping.String())
	}
	shaString := fmt.Sprintf("%x", sha[:])

	refCounter.lock.RLock()
	_, exists := refCounter.shaToMapping[shaString]
	isCommitted := !refCounter.needToSync
	refCounter.lock.RUnlock()

	if exists && isCommitted {
		// Nothing need to be done
		return nil
	} else if !exists {
		refCounter.lock.Lock()
		_, exists = refCounter.shaToMapping[shaString]
		if !exists {
			refCounter.shaToMapping[shaString] = oneBrokenMapping
			refCounter.refCnt[shaString] = 0
			refCounter.needToSync = true
		}
		refCounter.lock.Unlock()
	}

	return ckpt_svc.upsertBrokenMappingInternal(replicationId, specInternalId, false /*cleanup*/)
}

func (ckpt_svc *CheckpointsService) UpsertBrokenMapping(replicationId string, specInternalId string) error {
	return ckpt_svc.upsertBrokenMappingInternal(replicationId, specInternalId, true /*cleanUp*/)
}

func (ckpt_svc *CheckpointsService) upsertBrokenMappingInternal(replicationId string, specInternalId string, cleanup bool) error {

	ckpt_svc.topicMapMtx.RLock()
	refCounter, ok := ckpt_svc.topicBrokenMaps[replicationId]
	upsertCh, ok2 := ckpt_svc.topicBrokenUpsertCh[replicationId]
	ckpt_svc.topicMapMtx.RUnlock()

	if !ok || !ok2 || refCounter == nil {
		return base.ErrorInvalidInput
	}

	refCounter.lock.RLock()
	needToSync := refCounter.needToSync
	refCounter.lock.RUnlock()
	if !needToSync {
		// no op
		return nil
	}

	select {
	case <-upsertCh:
		var err error
		// Got the go-ahead to do the upsert
		defer func() {
			upsertCh <- true
		}()
		defer func() {
			if err == nil {
				refCounter.lock.Lock()
				refCounter.needToSync = false
				refCounter.lock.Unlock()
			}
		}()

		refCounter.lock.Lock()
		if refCounter.internalSpecId == "" {
			refCounter.internalSpecId = specInternalId
		} else if refCounter.internalSpecId != specInternalId {
			err = fmt.Errorf("Upserting failed with mismatching internalId %v vs %v", refCounter.internalSpecId, specInternalId)
			refCounter.lock.Unlock()
			return err
		}
		// First sync clean up the mapping
		if cleanup {
			var keysToCleanUp []string
			for sha, _ := range refCounter.shaToMapping {
				count, exists := refCounter.refCnt[sha]
				if !exists || count == 0 {
					if count == 0 {
						delete(refCounter.refCnt, sha)
					}
					keysToCleanUp = append(keysToCleanUp, sha)
				}
			}
			for _, key := range keysToCleanUp {
				delete(refCounter.shaToMapping, key)
			}
		}

		// Make a clone for the upsert
		clonedMapping := refCounter.shaToMapping.Clone()
		refCounter.lock.Unlock()

		// Perform RMW
		brokenMappingsDoc, err := ckpt_svc.getBrokenmappingDocFromMetakv(replicationId)
		if err != nil {
			return err
		}
		if brokenMappingsDoc.SpecInternalId == "" {
			brokenMappingsDoc.SpecInternalId = specInternalId
		} else if brokenMappingsDoc.SpecInternalId != specInternalId {
			err = fmt.Errorf("Mismatching internalId in metakv %v vs %v", brokenMappingsDoc.SpecInternalId, specInternalId)
			return err
		}

		err = brokenMappingsDoc.LoadShaMap(clonedMapping)
		if err != nil {
			return err
		}
		err = ckpt_svc.upsertBrokenMappingsDoc(replicationId, brokenMappingsDoc, false /*new*/)
		if err != nil {
			ckpt_svc.logger.Errorf("Unable to persist brokenmap for %v - %v", replicationId, err)
		} else {
			ckpt_svc.logger.Infof("Persisted brokenMap for %v of %v size: %v", replicationId, clonedMapping, brokenMappingsDoc.Size())
		}
		return err
	default:
		return fmt.Errorf("Error upserting broken mapping for %v as an operation is happening already", replicationId)
	}

}

func (ckpt_svc *CheckpointsService) cleanupOutdatedBrokenmappings(replicationId string, brokenMappingsDoc *metadata.BrokenMappingsDoc) {
	if brokenMappingsDoc == nil {
		return
	}

	ckpt_svc.topicMapMtx.RLock()
	refCounter, ok := ckpt_svc.topicBrokenMaps[replicationId]
	upsertCh, ok2 := ckpt_svc.topicBrokenUpsertCh[replicationId]
	ckpt_svc.topicMapMtx.RUnlock()

	if !ok || !ok2 {
		return
	}

	select {
	case <-upsertCh:
		defer func() {
			upsertCh <- true
		}()
		var updatedCompressedList metadata.CompressedColNamespaceMappingList

		for _, oneCompressedMapping := range brokenMappingsDoc.BrokenMappingRecords {
			refCounter.lock.RLock()
			count, exists := refCounter.refCnt[oneCompressedMapping.Sha256Digest]
			refCounter.lock.RUnlock()
			if exists && count > 0 {
				updatedCompressedList = append(updatedCompressedList, oneCompressedMapping)
			}
		}

		if len(updatedCompressedList) < len(brokenMappingsDoc.BrokenMappingRecords) {
			brokenMappingsDoc.BrokenMappingRecords = updatedCompressedList
			err := ckpt_svc.upsertBrokenMappingsDoc(replicationId, brokenMappingsDoc, false /*new*/)
			if err != nil {
				ckpt_svc.logger.Errorf("Unable to upsert brokenmappings for %v", replicationId)
			} else {
				ckpt_svc.logger.Infof("Checkpoints service for %v cleaned up brokenmapping", replicationId)
			}
		}
	default:
		ckpt_svc.logger.Errorf("Unable to clean broken mappings for %v due to concurrent ongoing upsert operation", replicationId)
	}
}

func (ckpt_svc *CheckpointsService) upsertBrokenMappingsDoc(replicationId string, doc *metadata.BrokenMappingsDoc, addOp bool) error {
	brokenMappingsDocKey := ckpt_svc.getBrokenMappingsDocKey(replicationId)

	data, err := json.Marshal(doc)
	if err != nil {
		return err
	}

	if addOp {
		return ckpt_svc.metadata_svc.Add(brokenMappingsDocKey, data)
	} else {
		return ckpt_svc.metadata_svc.Set(brokenMappingsDocKey, data, nil /*revision*/)
	}
}

func (ckpt_svc *CheckpointsService) getBrokenmappingDocFromMetakv(replicationId string) (*metadata.BrokenMappingsDoc, error) {
	brokenMappingsDocKey := ckpt_svc.getBrokenMappingsDocKey(replicationId)
	data, _, err := ckpt_svc.metadata_svc.Get(brokenMappingsDocKey)

	if err != nil {
		return nil, err
	}

	brokenMappingsDoc := &metadata.BrokenMappingsDoc{}
	err = json.Unmarshal(data, brokenMappingsDoc)
	if err != nil {
		return nil, err
	}

	return brokenMappingsDoc, nil
}

func (ckpt_svc *CheckpointsService) LoadBrokenMappings(replicationId string) (metadata.ShaToCollectionNamespaceMap, *metadata.BrokenMappingsDoc, *BrokenMapShaRefCounter, string, error) {
	var firstRun bool

	brokenMappingsDoc, err := ckpt_svc.getBrokenmappingDocFromMetakv(replicationId)
	if err != nil && err == service_def.MetadataNotFoundErr {
		firstRun = true
		ckpt_svc.logger.Warnf("Unable to find any broken mappings for pipeline %v. Initializing an empty one...", replicationId)
		brokenMappingsDoc = &metadata.BrokenMappingsDoc{}
		err = ckpt_svc.upsertBrokenMappingsDoc(replicationId, brokenMappingsDoc, firstRun)
		if err != nil {
			return nil, nil, nil, "", err
		}
	} else if err != nil {
		ckpt_svc.logger.Errorf("Error retrieving broken mapping doc for replication %v - err %v", replicationId, err)
		return nil, nil, nil, "", err
	}

	ckpt_svc.topicMapMtx.RLock()
	refCounter, ok := ckpt_svc.topicBrokenMaps[replicationId]
	ckpt_svc.topicMapMtx.RUnlock()

	compiledShaNamespaceMap := make(metadata.ShaToCollectionNamespaceMap)
	if !firstRun {
		if ok && refCounter != nil && brokenMappingsDoc.SpecInternalId != "" && refCounter.internalSpecId != brokenMappingsDoc.SpecInternalId {
			err = fmt.Errorf("%v loaded an outdated brokenmapping doc", replicationId)
			return nil, nil, nil, "", err
		}

		compiledShaNamespaceMap, err = brokenMappingsDoc.ToShaMap()
		if err != nil {
			// Any error loading the sha to collectionNamespacemap is considered a fatal error
			// This includes any mismatching sha256 - we cannot allow the pipeline to start
			// TODO - MB-38506 - need to stop restarting if the error is not recoverable
			return nil, nil, nil, "", err
		}
	}

	if refCounter == nil || !ok {
		ckpt_svc.topicMapMtx.Lock()
		refCounter = NewBrokenMapShaRefCounter()
		if !firstRun {
			refCounter.internalSpecId = brokenMappingsDoc.SpecInternalId
		} else {
			// internalSpecId will be set into the brokenMappingsDoc during the first Upsert call
		}
		ckpt_svc.topicBrokenMaps[replicationId] = refCounter
		ckpt_svc.topicBrokenUpsertCh[replicationId] = make(chan bool, 1)
		ckpt_svc.topicBrokenUpsertCh[replicationId] <- true
		ckpt_svc.topicMapMtx.Unlock()
	}

	return compiledShaNamespaceMap, brokenMappingsDoc, refCounter, brokenMappingsDoc.SpecInternalId, nil
}

// When each vb does checkpointing and adds a new checkpoint record
// Ensure that the old, bumped out ones are refcounted correctly
// And do refcount for the newly added record's count as well
func (ckpt_svc *CheckpointsService) RecordBrokenMappings(replicationId string, ckptRecord *metadata.CheckpointRecord, removedRecords []*metadata.CheckpointRecord) error {
	if ckptRecord.BrokenMappings() == nil || len(*(ckptRecord.BrokenMappings())) == 0 {
		return nil
	}

	ckpt_svc.topicMapMtx.RLock()
	refCounter := ckpt_svc.topicBrokenMaps[replicationId]
	ckpt_svc.topicMapMtx.RUnlock()

	refCounter.lock.Lock()
	_, exists := refCounter.shaToMapping[ckptRecord.BrokenMappingSha256]
	if !exists {
		refCounter.shaToMapping[ckptRecord.BrokenMappingSha256] = ckptRecord.BrokenMappings()
		refCounter.needToSync = true
	}
	refCounter.refCnt[ckptRecord.BrokenMappingSha256]++

	for _, removedRecord := range removedRecords {
		if removedRecord != nil && len(removedRecord.BrokenMappingSha256) > 0 && refCounter.refCnt[removedRecord.BrokenMappingSha256] > 0 {
			refCounter.refCnt[removedRecord.BrokenMappingSha256]--
			if refCounter.refCnt[removedRecord.BrokenMappingSha256] == 0 {
				refCounter.needToSync = true
			}
		}
	}
	refCounter.lock.Unlock()

	return nil
}

// Should be called non-concurrently per pipeline
// When brokenMappingsNeeded is true, this should only be called once at the beginning of a pipeline start
func (ckpt_svc *CheckpointsService) CheckpointsDocs(replicationId string, brokenMappingsNeeded bool) (map[uint16]*metadata.CheckpointsDoc, error) {
	checkpointsDocs := make(map[uint16]*metadata.CheckpointsDoc)
	catalogKey := ckpt_svc.getCheckpointCatalogKey(replicationId)
	ckpt_entries, err := ckpt_svc.metadata_svc.GetAllMetadataFromCatalog(catalogKey)
	if err != nil {
		return nil, err
	}

	var shaToBrokenMapping metadata.ShaToCollectionNamespaceMap
	var brokenMappingsDoc *metadata.BrokenMappingsDoc
	var refCounter *BrokenMapShaRefCounter
	var internalId string
	if brokenMappingsNeeded {
		shaToBrokenMapping, brokenMappingsDoc, refCounter, internalId, err = ckpt_svc.LoadBrokenMappings(replicationId)
		if err != nil {
			return nil, err
		}
	}
	for _, ckpt_entry := range ckpt_entries {
		if ckpt_entry != nil {
			if ckpt_svc.isBrokenMappingDoc(ckpt_entry.Key) {
				continue
			}

			vbno, err := ckpt_svc.decodeVbnoFromCkptDocKey(ckpt_entry.Key)
			if err != nil {
				return nil, err
			}

			ckpt_doc, err := ckpt_svc.constructCheckpointDoc(ckpt_entry.Value, ckpt_entry.Rev, shaToBrokenMapping)
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
				refCounter.RegisterCkptDoc(ckpt_doc)
			}
		}
	}

	if brokenMappingsNeeded {
		ckpt_svc.cleanupOutdatedBrokenmappings(replicationId, brokenMappingsDoc)
		shaToBrokenMapping, _, _, internalId, err = ckpt_svc.LoadBrokenMappings(replicationId)
		if err != nil {
			ckpt_svc.logger.Errorf("Unable to refresh brokenmapping cache - %v", err)
			return checkpointsDocs, err
		}
		ckpt_svc.SetBrokenMappingCache(replicationId, internalId, shaToBrokenMapping)
		ckpt_svc.logger.Infof("Loaded brokenMap: %v", shaToBrokenMapping)
	}
	return checkpointsDocs, nil
}

func (ckpt_svc *CheckpointsService) constructCheckpointDoc(content []byte, rev interface{}, shaToBrokenMapping metadata.ShaToCollectionNamespaceMap) (*metadata.CheckpointsDoc, error) {
	// The only time content is empty is when this is a fresh XDCR system and no checkpoints has been registered yet
	if len(content) > 0 {
		ckpt_doc := &metadata.CheckpointsDoc{}
		err := json.Unmarshal(content, ckpt_doc)
		if err != nil {
			return nil, err
		}
		if len(shaToBrokenMapping) > 0 {
			ckpt_svc.populateActualMapping(ckpt_doc, shaToBrokenMapping)
		}
		return ckpt_doc, nil
	} else {
		ckpt_svc.logger.Errorf("Unable to construct valid checkpoint due to empty checkpoint data")
		return nil, service_def.MetadataNotFoundErr
	}
}

func (ckpt_svc *CheckpointsService) populateActualMapping(doc *metadata.CheckpointsDoc, shaToActualMapping metadata.ShaToCollectionNamespaceMap) {
	if doc == nil {
		return
	}

	for _, record := range doc.Checkpoint_records {
		if record == nil {
			continue
		}
		mapping, exists := shaToActualMapping[record.BrokenMappingSha256]
		if exists {
			record.LoadBrokenMapping(*mapping)
		}
	}
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
