// Copyright (c) 2013-2020 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

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
	*ShaRefCounterService
	metadata_svc service_def.MetadataSvc
	logger       *log.CommonLogger
}

func NewCheckpointsService(metadata_svc service_def.MetadataSvc, logger_ctx *log.LoggerContext) service_def.CheckpointsService {
	return &CheckpointsService{metadata_svc: metadata_svc,
		logger:               log.NewLogger("CheckpointSvc", logger_ctx),
		ShaRefCounterService: NewShaRefCounterService(getCollectionNsMappingsDocKey, metadata_svc),
	}
}

func (ckpt_svc *CheckpointsService) CheckpointsDoc(replicationId string, vbno uint16) (*metadata.CheckpointsDoc, error) {
	key := ckpt_svc.getCheckpointDocKey(replicationId, vbno)
	result, rev, err := ckpt_svc.metadata_svc.Get(key)
	if err != nil {
		return nil, err
	}

	// Should exist because checkpoint manager must finish loading before allowing ckpt operations
	shaMap, _ := ckpt_svc.GetShaNamespaceMap(replicationId)

	ckpt_doc, err := ckpt_svc.constructCheckpointDoc(result, rev, shaMap)
	if err == service_def.MetadataNotFoundErr {
		ckpt_svc.logger.Errorf("Unable to construct ckpt doc from metakv given replication: %v vbno: %v key: %v",
			replicationId, vbno, key)
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

func (ckpt_svc *CheckpointsService) DelCheckpointsDocs(replicationId string) error {
	ckpt_svc.logger.Infof("DelCheckpointsDocs for replication %v...", replicationId)
	catalogKey := ckpt_svc.getCheckpointCatalogKey(replicationId)
	err_ret := ckpt_svc.metadata_svc.DelAllFromCatalog(catalogKey)
	if err_ret != nil {
		ckpt_svc.logger.Errorf("Failed to delete checkpoints docs for %v\n", replicationId)
	} else {
		cleanMapErr := ckpt_svc.CleanupMapping(replicationId)
		if cleanMapErr != nil {
			ckpt_svc.logger.Warnf("DelCheckpointsDoc for brokenmapping had error: %v", cleanMapErr)
		}
		ckpt_svc.logger.Infof("DelCheckpointsDocs is done for %v\n", replicationId)
	}
	return err_ret
}

// Need to have correct accounting after deleting checkpoingsDocs
func (ckpt_svc *CheckpointsService) PostDelCheckpointsDoc(replicationId string, doc *metadata.CheckpointsDoc) error {
	decrementerFunc, err := ckpt_svc.GetDecrementerFunc(replicationId)
	if err != nil {
		return err
	}

	for _, ckptRecord := range doc.Checkpoint_records {
		shaInRecord := ckptRecord.BrokenMappingSha256
		decrementerFunc(shaInRecord)
	}

	return nil
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

	return ckpt_svc.RegisterMapping(replicationId, specInternalId, oneBrokenMapping)
}

func (ckpt_svc *CheckpointsService) UpsertBrokenMapping(replicationId string, specInternalId string) error {
	return ckpt_svc.UpsertMapping(replicationId, specInternalId)
}

func (ckpt_svc *CheckpointsService) LoadBrokenMappings(replicationId string) (metadata.ShaToCollectionNamespaceMap, *metadata.CollectionNsMappingsDoc, IncrementerFunc, error) {
	alreadyExists := ckpt_svc.InitTopicShaCounter(replicationId)
	mappingsDoc, err := ckpt_svc.GetMappingsDoc(replicationId, !alreadyExists /*initIfNotFound*/)
	if err != nil {
		var emptyMap metadata.ShaToCollectionNamespaceMap
		return emptyMap, nil, nil, err
	}

	shaToNamespaceMap, err := ckpt_svc.GetShaToCollectionNsMap(replicationId, mappingsDoc)
	if err != nil {
		var emptyMap metadata.ShaToCollectionNamespaceMap
		return emptyMap, nil, nil, err
	}

	incrementerFunc, err := ckpt_svc.GetIncrementerFunc(replicationId)
	if err != nil {
		var emptyMap metadata.ShaToCollectionNamespaceMap
		return emptyMap, nil, nil, err
	}

	return shaToNamespaceMap, mappingsDoc, incrementerFunc, nil
}

// When each vb does checkpointing and adds a new checkpoint record
// Ensure that the old, bumped out ones are refcounted correctly
// And do refcount for the newly added record's count as well
func (ckpt_svc *CheckpointsService) RecordBrokenMappings(replicationId string, ckptRecord *metadata.CheckpointRecord, removedRecords []*metadata.CheckpointRecord) error {
	if ckptRecord.BrokenMappings() == nil || len(*(ckptRecord.BrokenMappings())) == 0 {
		return nil
	}

	incrementerFunc, err := ckpt_svc.GetIncrementerFunc(replicationId)
	if err != nil {
		return err
	}
	decrementerFunc, err := ckpt_svc.GetDecrementerFunc(replicationId)
	if err != nil {
		return err
	}

	incrementerFunc(ckptRecord.BrokenMappingSha256, ckptRecord.BrokenMappings())

	for _, removedRecord := range removedRecords {
		if removedRecord != nil && len(removedRecord.BrokenMappingSha256) > 0 {
			decrementerFunc(removedRecord.BrokenMappingSha256)
		}
	}
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
	var CollectionNsMappingsDoc *metadata.CollectionNsMappingsDoc
	var refCounterRecorder IncrementerFunc
	if brokenMappingsNeeded {
		shaToBrokenMapping, CollectionNsMappingsDoc, refCounterRecorder, err = ckpt_svc.LoadBrokenMappings(replicationId)
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
				if brokenMappingsNeeded {
					ckpt_svc.registerCkptDocBrokenMappings(ckpt_doc, refCounterRecorder)
				}
			}
		}
	}

	if brokenMappingsNeeded {
		err = ckpt_svc.GCDocUsingLatestCounterInfo(replicationId, CollectionNsMappingsDoc)
		if err != nil {
			ckpt_svc.logger.Errorf("Unable to GC brokenmapping cache - %v", err)
			return checkpointsDocs, err
		}

		shaToBrokenMapping, _, _, err = ckpt_svc.LoadBrokenMappings(replicationId)
		if err != nil {
			ckpt_svc.logger.Errorf("Unable to refresh brokenmapping cache - %v", err)
			return checkpointsDocs, err
		}

		err = ckpt_svc.InitCounterShaToActualMappings(replicationId, CollectionNsMappingsDoc.SpecInternalId, shaToBrokenMapping)
		if err == nil {
			ckpt_svc.logger.Infof("Loaded brokenMap: %v", shaToBrokenMapping)
		} else {
			ckpt_svc.logger.Warnf("Error %v trying init sha counter to mapping: %v", err, shaToBrokenMapping)
		}
	}
	return checkpointsDocs, nil
}

func (ckpt_svc *CheckpointsService) registerCkptDocBrokenMappings(ckpt_doc *metadata.CheckpointsDoc, recorder IncrementerFunc) {
	if ckpt_doc == nil || recorder == nil {
		return
	}
	for _, record := range ckpt_doc.Checkpoint_records {
		if record == nil || len(record.BrokenMappingSha256) == 0 {
			continue
		}
		recorder(record.BrokenMappingSha256, record.BrokenMappings())
	}
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
