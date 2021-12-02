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
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	utilities "github.com/couchbase/goxdcr/utils"
	"sync"
)

type ShaRefCounterService struct {
	metadataSvc service_def.MetadataSvc
	// Map of replicationId -> map of sha's -> refCnts
	topicMapMtx sync.RWMutex
	topicMaps   map[string]*MapShaRefCounter

	// Given a replication ID, return the key to use for metadataSvc
	metakvDocKeyGetter func(string) string

	logger *log.CommonLogger
}

func NewShaRefCounterService(metakvDocKeyGetter func(string) string,
	metadataSvc service_def.MetadataSvc,
	logger *log.CommonLogger) *ShaRefCounterService {
	return &ShaRefCounterService{
		topicMaps:          make(map[string]*MapShaRefCounter),
		metakvDocKeyGetter: metakvDocKeyGetter,
		metadataSvc:        metadataSvc,
		logger:             logger,
	}
}

var emptyS2CNsMap = make(metadata.ShaToCollectionNamespaceMap)

func (s *ShaRefCounterService) GetShaNamespaceMap(topic string) (metadata.ShaToCollectionNamespaceMap, error) {
	s.topicMapMtx.RLock()
	defer s.topicMapMtx.RUnlock()
	counter, exists := s.topicMaps[topic]
	if !exists {
		return emptyS2CNsMap, base.ErrorInvalidInput
	}
	return counter.GetShaNamespaceMap(), nil
}

// Idempotent No-op if already exists
// Init doesn't set the count to 1 - it sets it to 0 - and then individual counts need to be counted using recorder
func (s *ShaRefCounterService) InitTopicShaCounterWithInternalId(topic, internalId string) (alreadyExists bool) {
	s.topicMapMtx.RLock()
	_, alreadyExists = s.topicMaps[topic]
	s.topicMapMtx.RUnlock()

	if alreadyExists {
		return
	}

	s.topicMapMtx.Lock()
	// check again
	_, alreadyExists = s.topicMaps[topic]
	if alreadyExists {
		s.topicMapMtx.Unlock()
		return
	}

	counter := NewMapShaRefCounterWithInternalId(topic, internalId, s.metadataSvc, s.metakvDocKeyGetter(topic), s.logger)
	s.topicMaps[topic] = counter
	s.topicMapMtx.Unlock()

	counter.Init()
	return
}

// Caller who calls initIfNotFound should ensure there is not a concurrent upsert operation somewhere else
func (s *ShaRefCounterService) GetMappingsDoc(topic string, initIfNotFound bool) (*metadata.CollectionNsMappingsDoc, error) {
	s.topicMapMtx.RLock()
	counter, ok := s.topicMaps[topic]
	s.topicMapMtx.RUnlock()

	if !ok {
		return nil, base.ErrorInvalidInput
	}

	return counter.GetMappingsDoc(initIfNotFound)
}

// Gets the sha to namespace map, and also sets refcounter's internal ID if needed
func (s *ShaRefCounterService) GetShaToCollectionNsMap(topic string, doc *metadata.CollectionNsMappingsDoc) (metadata.ShaToCollectionNamespaceMap, error) {
	var emptyS2CNsMap metadata.ShaToCollectionNamespaceMap
	s.topicMapMtx.RLock()
	refCounter, ok := s.topicMaps[topic]
	s.topicMapMtx.RUnlock()

	if !ok {
		return emptyS2CNsMap, base.ErrorInvalidInput
	}

	var err error
	compiledShaNamespaceMap, err := doc.ToShaMap()
	if err != nil {
		// Any error loading the sha to collectionNamespacemap is considered a fatal error
		// This includes any mismatching sha256 - we cannot allow the pipeline to start
		s.logger.Errorf("CompiledShaNamespaceMap %v with error %v", topic, err)
		return emptyS2CNsMap, err
	}

	if doc.SpecInternalId != "" {
		err = refCounter.CheckOrSetInternalSpecId(doc.SpecInternalId)
		if err != nil {
			return emptyS2CNsMap, err
		}
	}

	return compiledShaNamespaceMap, nil
}

func (s *ShaRefCounterService) GetIncrementerFunc(topic string) (service_def.IncrementerFunc, error) {
	s.topicMapMtx.RLock()
	counter, ok := s.topicMaps[topic]
	s.topicMapMtx.RUnlock()

	if !ok {
		return nil, base.ErrorInvalidInput
	}

	return counter.RecordOneCount, nil
}

func (s *ShaRefCounterService) GetDecrementerFunc(topic string) (service_def.DecrementerFunc, error) {
	s.topicMapMtx.RLock()
	counter, ok := s.topicMaps[topic]
	s.topicMapMtx.RUnlock()

	if !ok {
		return nil, base.ErrorInvalidInput
	}

	return counter.UnrecordOneCount, nil
}

func (s *ShaRefCounterService) GCDocUsingLatestCounterInfo(topic string, doc *metadata.CollectionNsMappingsDoc) error {
	if doc == nil {
		return base.ErrorInvalidInput
	}

	s.topicMapMtx.RLock()
	counter, ok := s.topicMaps[topic]
	s.topicMapMtx.RUnlock()
	if !ok {
		return base.ErrorInvalidInput
	}

	return counter.GCDocUsingLatestInfo(doc)
}

// Used to force the counters to have a certain sha map
// This should be called at the start of loading from metakv, before any record/unrecord is used
func (s *ShaRefCounterService) InitCounterShaToActualMappings(topic, internalSpecId string, mapping metadata.ShaToCollectionNamespaceMap) error {
	s.topicMapMtx.RLock()
	counter, ok := s.topicMaps[topic]
	s.topicMapMtx.RUnlock()

	if !ok {
		return base.ErrorInvalidInput
	}

	return counter.InitShaToActualMappings(topic, internalSpecId, mapping)
}

// Register a specific mapping - without actually adding a count
// If the mapping is already registered, then no-op
// If the mapping does not exist, then the mapping is saved and upserted to metakv
func (s *ShaRefCounterService) RegisterMapping(topic, internalSpecId string, mapping *metadata.CollectionNamespaceMapping) error {
	if mapping == nil {
		return base.ErrorInvalidInput
	}

	s.topicMapMtx.RLock()
	counter, ok := s.topicMaps[topic]
	s.topicMapMtx.RUnlock()

	if !ok {
		return base.ErrorInvalidInput
	}

	return counter.RegisterMapping(internalSpecId, mapping)
}

func (s *ShaRefCounterService) UpsertMapping(topic, specInternalId string) error {
	s.topicMapMtx.RLock()
	counter, ok := s.topicMaps[topic]
	s.topicMapMtx.RUnlock()

	if !ok {
		return base.ErrorInvalidInput
	}

	// TODO - once consistent metakv is in, the cleanup effort will need to be coordinated
	// Most likely called by the cluster master
	return counter.upsertMapping(specInternalId, true)
}

func (s *ShaRefCounterService) CleanupMapping(topic string, utils utilities.UtilsIface) error {
	s.topicMapMtx.Lock()
	counter, ok := s.topicMaps[topic]
	s.topicMapMtx.Unlock()

	doneFunc := utils.StartDiagStopwatch(fmt.Sprintf("CleanupMapping(%v) found? %v", topic, ok), base.DiagInternalThreshold)
	defer doneFunc()

	if !ok {
		// Still need to clean up the mapping
		temporaryCounter := NewMapShaRefCounterWithInternalId(topic, "", s.metadataSvc, s.metakvDocKeyGetter(topic), s.logger)
		temporaryCounter.Init()
		return temporaryCounter.DelAndCleanup()
	}

	s.topicMapMtx.Lock()
	delete(s.topicMaps, topic)
	s.topicMapMtx.Unlock()

	return counter.DelAndCleanup()
}

func (s *ShaRefCounterService) ReInitUsingMergedMappingDoc(topic string, brokenMappingDoc *metadata.CollectionNsMappingsDoc, ckptDocs map[uint16]*metadata.CheckpointsDoc, internalId string) error {
	s.topicMapMtx.RLock()
	counter, ok := s.topicMaps[topic]
	s.topicMapMtx.RUnlock()

	if !ok {
		return base.ErrorInvalidInput
	}

	return counter.ReInitUsingMergedMappingDoc(brokenMappingDoc, ckptDocs, internalId)
}

//func (c *MapShaRefCounter) ReInitUsingMergedMappingDoc(doc *metadata.CollectionNsMappingsDoc, ckpt) error {

func UnableToUpsertErr(id string) error {
	return fmt.Errorf("Unable to clean broken mappings for %v due to concurrent ongoing upsert operation", id)
}

// Used to keep track of brokenmapping SHA and the count of checkpoint records referring to it
type MapShaRefCounter struct {
	id                 string
	lock               sync.RWMutex
	singleUpsert       chan bool
	refCnt             map[string]uint64                    // map of sha to refCnt (1/2)
	shaToMapping       metadata.ShaToCollectionNamespaceMap // map of sha to actual mapping (2/2)
	needToSync         bool                                 // needs to sync refCnt to shaMap and then also persist to metakv
	needToSyncRevision uint64
	internalSpecId     string
	metadataSvc        service_def.MetadataSvc
	metakvOpKey        string
	logger             *log.CommonLogger
}

func NewMapShaRefCounterWithInternalId(topic, internalId string, metadataSvc service_def.MetadataSvc,
	metakvOpKey string, logger *log.CommonLogger) *MapShaRefCounter {
	return &MapShaRefCounter{refCnt: make(map[string]uint64),
		id:             topic,
		shaToMapping:   make(metadata.ShaToCollectionNamespaceMap),
		singleUpsert:   make(chan bool, 1),
		metadataSvc:    metadataSvc,
		metakvOpKey:    metakvOpKey,
		internalSpecId: internalId,
		logger:         logger,
	}
}

func NewMapShaRefCounter(topic string, metadataSvc service_def.MetadataSvc, metakvOpKey string, logger *log.CommonLogger) *MapShaRefCounter {
	return NewMapShaRefCounterWithInternalId(topic, "", metadataSvc, metakvOpKey, logger)
}

func (c *MapShaRefCounter) Init() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.singleUpsert <- true
}

func (c *MapShaRefCounter) CheckOrSetInternalSpecId(internalId string) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.internalSpecId == "" {
		c.internalSpecId = internalId
		return nil
	} else if c.internalSpecId != internalId {
		return fmt.Errorf("Invalid internalID %v vs %v", c.internalSpecId, internalId)
	}
	return nil
}

func (c *MapShaRefCounter) GetShaNamespaceMap() metadata.ShaToCollectionNamespaceMap {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.shaToMapping
}

func (c *MapShaRefCounter) upsertCollectionNsMappingsDoc(doc *metadata.CollectionNsMappingsDoc, addOp bool) error {
	data, err := json.Marshal(doc)
	if err != nil {
		return err
	}

	if addOp {
		return c.metadataSvc.Add(c.metakvOpKey, data)
	} else {
		return c.metadataSvc.Set(c.metakvOpKey, data, nil /*revision*/)
	}
}

func (c *MapShaRefCounter) getCollectionNsMappingsDocData() ([]byte, error) {
	docData, _, err := c.metadataSvc.Get(c.metakvOpKey)

	return docData, err
}

func (c *MapShaRefCounter) RecordOneCount(shaString string, mapping *metadata.CollectionNamespaceMapping) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if _, exists := c.shaToMapping[shaString]; !exists {
		c.shaToMapping[shaString] = mapping
		c.setNeedToSyncNoLock(true)
	}
	c.refCnt[shaString]++
}

func (c *MapShaRefCounter) UnrecordOneCount(shaString string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if count, exists := c.refCnt[shaString]; exists && count > 0 {
		c.refCnt[shaString]--
		if c.refCnt[shaString] == 0 {
			c.setNeedToSyncNoLock(true)
		}
	}
}

func (c *MapShaRefCounter) GCDocUsingLatestInfo(doc *metadata.CollectionNsMappingsDoc) error {
	c.lock.RLock()
	upsertCh := c.singleUpsert
	needToSyncRev := c.needToSyncRevision
	c.lock.RUnlock()

	select {
	case <-upsertCh:
		defer func() {
			upsertCh <- true
		}()
		var updatedCompressedList metadata.CompressedColNamespaceMappingList

		for _, oneCompressedMapping := range doc.NsMappingRecords {
			c.lock.RLock()
			count, exists := c.refCnt[oneCompressedMapping.Sha256Digest]
			c.lock.RUnlock()
			if exists && count > 0 {
				updatedCompressedList = append(updatedCompressedList, oneCompressedMapping)
			}
		}

		if len(updatedCompressedList) < len(doc.NsMappingRecords) {
			doc.NsMappingRecords = updatedCompressedList
			err := c.upsertCollectionNsMappingsDoc(doc, false /*new*/)
			if err == nil {
				c.lock.Lock()
				if c.needToSyncRevision == needToSyncRev {
					c.setNeedToSyncNoLock(false)
				}
				c.lock.Unlock()
			}
		}

		// Nothing uploaded
		return nil
	default:
		return UnableToUpsertErr(c.id)
	}
}

func (c *MapShaRefCounter) InitShaToActualMappings(topic, internalSpecId string, mapping metadata.ShaToCollectionNamespaceMap) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.internalSpecId != "" && c.internalSpecId != internalSpecId {
		return fmt.Errorf("Replication %v mismatch internalId %v vs %v", topic, c.internalSpecId, internalSpecId)
	}

	c.shaToMapping = mapping
	c.internalSpecId = internalSpecId
	return nil
}

// Currently, starts count at 0
func (c *MapShaRefCounter) RegisterMapping(internalSpecId string, mapping *metadata.CollectionNamespaceMapping) error {
	sha, err := mapping.Sha256()
	if err != nil {
		return fmt.Errorf("Unable to get sha256 %v for mapping %v", c.id, mapping.String())
	}
	shaString := fmt.Sprintf("%x", sha[:])

	c.lock.RLock()
	_, exists := c.shaToMapping[shaString]
	isCommitted := c.needToSync
	c.lock.RUnlock()

	if exists && isCommitted {
		// Nothing needs to be done
		return nil
	} else if !exists {
		c.lock.Lock()
		_, exists = c.shaToMapping[shaString]
		if !exists {
			c.shaToMapping[shaString] = mapping
			c.refCnt[shaString] = 0
			c.setNeedToSyncNoLock(true)
		}
		c.lock.Unlock()
	}

	return c.upsertMapping(internalSpecId, false)
}

func (c *MapShaRefCounter) GetMappingsDoc(initIfNotFound bool) (*metadata.CollectionNsMappingsDoc, error) {
	docData, err := c.getCollectionNsMappingsDocData()
	docReturn := &metadata.CollectionNsMappingsDoc{}

	if err != nil && err == service_def.MetadataNotFoundErr {
		if initIfNotFound {
			c.logger.Infof("GetMappingsDoc %v initialized a new mappingsDoc", c.id)
			err = c.upsertCollectionNsMappingsDoc(docReturn, true /*addOp*/)
			return docReturn, err
		} else {
			err = fmt.Errorf("GetMappingsDoc was requested for %v but was not found", c.id)
			c.logger.Errorf(err.Error())
			return nil, err
		}
	} else if err != nil {
		c.logger.Errorf("GetMappingsDoc %v retrieval got error %v", c.id, err)
		return nil, err
	}

	err = json.Unmarshal(docData, docReturn)
	if err != nil {
		return nil, err
	}

	c.logger.Infof("GetMappingsDoc %v retrieved an existing mappingsDoc", c.id)
	return docReturn, nil
}

// Write lock must be held
func (c *MapShaRefCounter) setNeedToSyncNoLock(val bool) {
	c.needToSync = val
	c.needToSyncRevision++
}

func (c *MapShaRefCounter) upsertMapping(specInternalId string, cleanup bool) error {
	c.lock.RLock()
	needToSync := c.needToSync
	needToSyncRev := c.needToSyncRevision
	upsertCh := c.singleUpsert
	c.lock.RUnlock()

	if !needToSync {
		// no-op
		return nil
	}

	select {
	case <-upsertCh:
		var err error
		// Got the go-ahead to do the upsert
		defer func() {
			if err == nil {
				var needToReUpsert bool
				c.lock.Lock()
				if c.needToSyncRevision == needToSyncRev {
					c.setNeedToSyncNoLock(false)
				} else {
					// Someone jumped in, will need to re-upsert
					needToReUpsert = true
				}
				c.lock.Unlock()
				if needToReUpsert {
					defer c.upsertMapping(specInternalId, cleanup)
				}
			}
			upsertCh <- true
		}()

		c.lock.Lock()
		if c.internalSpecId == "" {
			c.internalSpecId = specInternalId
		} else if specInternalId != "" && c.internalSpecId != specInternalId {
			err = fmt.Errorf("Upserting failed with mismatching internalId %v vs %v", c.internalSpecId, specInternalId)
			c.lock.Unlock()
			return err
		}
		// First sync clean up the mapping
		if cleanup {
			var keysToCleanUp []string
			for sha, _ := range c.shaToMapping {
				count, exists := c.refCnt[sha]
				if !exists || count == 0 {
					if count == 0 {
						delete(c.refCnt, sha)
					}
					keysToCleanUp = append(keysToCleanUp, sha)
				}
			}
			for _, key := range keysToCleanUp {
				delete(c.shaToMapping, key)
			}
		}

		// Make a clone for the upsert
		clonedMapping := c.shaToMapping.Clone()
		c.lock.Unlock()

		// Perform RMW
		collectionNsMappingsDoc, err := c.GetMappingsDoc(false /*initIfNotFound*/)
		if err != nil {
			return err
		}
		if collectionNsMappingsDoc.SpecInternalId == "" {
			collectionNsMappingsDoc.SpecInternalId = specInternalId
		} else if specInternalId != "" && collectionNsMappingsDoc.SpecInternalId != specInternalId {
			err = fmt.Errorf("Mismatching internalId in metakv %v vs %v", collectionNsMappingsDoc.SpecInternalId, specInternalId)
			return err
		}

		err = collectionNsMappingsDoc.LoadShaMap(clonedMapping)
		if err != nil {
			return err
		}
		err = c.upsertCollectionNsMappingsDoc(collectionNsMappingsDoc, false /*new*/)
		return err
	default:
		return fmt.Errorf("Error upserting broken mapping for %v as an operation is happening already", c.id)
	}
}

func (c *MapShaRefCounter) DelAndCleanup() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	// Don't allow any upserts to occur concurrently - force hold lock
	select {
	case <-c.singleUpsert:
		c.metadataSvc.Del(c.metakvOpKey, nil /*revision*/)
	}
	close(c.singleUpsert)
	return nil
}

// Used only when doing a complete overwrite after major merge operations
func (c *MapShaRefCounter) ReInitUsingMergedMappingDoc(brokenMappingDoc *metadata.CollectionNsMappingsDoc, ckptsDocs map[uint16]*metadata.CheckpointsDoc, internalId string) error {
	if brokenMappingDoc == nil {
		return base.ErrorNilPtr
	}

	newShaMap, err := brokenMappingDoc.ToShaMap()
	if err != nil {
		return err
	}

	c.lock.Lock()
	c.refCnt = make(map[string]uint64)
	c.shaToMapping = make(metadata.ShaToCollectionNamespaceMap)

	for sha, mapping := range newShaMap {
		c.shaToMapping[sha] = mapping
	}

	for _, ckptDoc := range ckptsDocs {
		for _, ckptRecord := range ckptDoc.Checkpoint_records {
			if ckptRecord == nil || ckptRecord.BrokenMappingSha256 == "" {
				continue
			}
			c.refCnt[ckptRecord.BrokenMappingSha256]++
		}
	}
	c.setNeedToSyncNoLock(true)
	c.lock.Unlock()

	return c.upsertMapping(internalId, false)
}
