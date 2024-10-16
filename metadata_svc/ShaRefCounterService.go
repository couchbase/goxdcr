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
	"sync"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
	utilities "github.com/couchbase/goxdcr/v8/utils"
)

type ShaRefCounterService struct {
	metadataSvc service_def.MetadataSvc
	utils       utilities.UtilsIface

	// Map of replicationId -> map of sha's -> refCnts
	topicMapMtx        sync.RWMutex
	topicMaps          map[string]*MapShaRefCounter
	topicGcDisabledSet map[string]bool

	// Given a replication ID, return the key to use for metadataSvc
	metakvDocKeyGetter func(string) string

	logger *log.CommonLogger
}

func NewShaRefCounterService(metakvDocKeyGetter func(string) string, metadataSvc service_def.MetadataSvc, logger *log.CommonLogger, utils utilities.UtilsIface) (*ShaRefCounterService, error) {
	refSvc := &ShaRefCounterService{
		topicMaps:          make(map[string]*MapShaRefCounter),
		topicGcDisabledSet: make(map[string]bool),
		metakvDocKeyGetter: metakvDocKeyGetter,
		metadataSvc:        metadataSvc,
		logger:             logger,
		utils:              utils,
	}

	return refSvc, nil
}

var emptyS2CNsMap = make(metadata.ShaToCollectionNamespaceMap)
var emptyGlobalTsMap = make(metadata.ShaToGlobalTimestampMap)

func (s *ShaRefCounterService) GetShaNamespaceMap(topic string) (metadata.ShaToCollectionNamespaceMap, error) {
	s.topicMapMtx.RLock()
	defer s.topicMapMtx.RUnlock()
	counter, exists := s.topicMaps[topic]
	if !exists {
		return emptyS2CNsMap, base.ErrorInvalidInput
	}
	return counter.GetShaNamespaceMap(), nil
}

func (s *ShaRefCounterService) GetShaGlobalTsMap(topic string) (metadata.ShaToGlobalTimestampMap, error) {
	s.topicMapMtx.RLock()
	defer s.topicMapMtx.RUnlock()
	counter, exists := s.topicMaps[topic]
	if !exists {
		return emptyGlobalTsMap, base.ErrorInvalidInput
	}
	return counter.GetShaGlobalTsMap(), nil
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
	defer s.topicMapMtx.Unlock()
	// check again
	_, alreadyExists = s.topicMaps[topic]
	if alreadyExists {
		return
	}

	counter := NewMapShaRefCounterWithInternalId(topic, internalId, s.metadataSvc, s.metakvDocKeyGetter(topic), s.logger)
	s.topicMaps[topic] = counter
	counter.Init()
	return
}

// Caller who calls initIfNotFound should ensure there is not a concurrent upsert operation somewhere else
func (s *ShaRefCounterService) GetMappingsDoc(topic string, initIfNotFound bool) (*metadata.CompressedMappings, error) {
	s.topicMapMtx.RLock()
	defer s.topicMapMtx.RUnlock()
	counter, ok := s.topicMaps[topic]

	if !ok {
		return nil, base.ErrorInvalidInput
	}

	return counter.GetMappingsDoc(initIfNotFound)
}

// Gets the sha to namespace map, and also sets refcounter's internal ID if needed
func (s *ShaRefCounterService) GetShaToCollectionNsMap(topic string, doc *metadata.CollectionNsMappingsDoc) (metadata.ShaToCollectionNamespaceMap, error) {
	var emptyS2CNsMap metadata.ShaToCollectionNamespaceMap
	s.topicMapMtx.RLock()
	defer s.topicMapMtx.RUnlock()
	refCounter, ok := s.topicMaps[topic]

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

func (s *ShaRefCounterService) GetShaToGlobalTsMap(topic string, doc *metadata.GlobalTimestampCompressedDoc) (metadata.ShaToGlobalTimestampMap, error) {
	var emptyShaToGlobalTsMap metadata.ShaToGlobalTimestampMap
	s.topicMapMtx.RLock()
	defer s.topicMapMtx.RUnlock()
	refCounter, ok := s.topicMaps[topic]

	if !ok {
		return emptyShaToGlobalTsMap, base.ErrorInvalidInput
	}

	var err error
	globalTsShaMap, err := doc.ToShaMap()
	if err != nil {
		// Any error loading the sha to collectionNamespacemap is considered a fatal error
		// This includes any mismatching sha256 - we cannot allow the pipeline to start
		s.logger.Errorf("globalTsShaMap %v with error %v", topic, err)
		return emptyShaToGlobalTsMap, err
	}

	if doc.SpecInternalId != "" {
		err = refCounter.CheckOrSetInternalSpecId(doc.SpecInternalId)
		if err != nil {
			return emptyShaToGlobalTsMap, err
		}
	}

	return globalTsShaMap, nil
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

	// If refCounting is disabled, then do not allow a decrement
	decrementWrapper := func(shaStr string) {
		s.topicMapMtx.RLock()
		disableIsSet, exists := s.topicGcDisabledSet[topic]
		s.topicMapMtx.RUnlock()

		if exists && disableIsSet {
			// Do not let count be decremented
		} else {
			counter.UnrecordOneCount(shaStr)
		}
	}
	return decrementWrapper, nil
}

// GCDocUsingLatestCounterInfo will take a document and remove any SHAs that do not have valid references anymore
func (s *ShaRefCounterService) GCDocUsingLatestCounterInfo(topic string, doc interface{}) error {
	if doc == nil {
		return base.ErrorInvalidInput
	}

	s.topicMapMtx.RLock()
	defer s.topicMapMtx.RUnlock()
	counter, ok := s.topicMaps[topic]
	if !ok {
		return base.ErrorInvalidInput
	}

	return counter.GCDocUsingLatestInfo(doc)
}

// Used to force the counters to have a certain sha map
// This should be called at the start of loading from metakv, before any record/unrecord is used
func (s *ShaRefCounterService) InitCounterShaToActualMappings(topic, internalSpecId string, mapping interface{}) error {
	s.topicMapMtx.RLock()
	defer s.topicMapMtx.RUnlock()
	counter, ok := s.topicMaps[topic]

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
	defer s.topicMapMtx.RUnlock()
	counter, ok := s.topicMaps[topic]

	if !ok {
		return base.ErrorInvalidInput
	}

	return counter.RegisterMapping(internalSpecId, mapping)
}

func (s *ShaRefCounterService) UpsertMapping(topic, specInternalId string) error {
	s.topicMapMtx.RLock()
	defer s.topicMapMtx.RUnlock()
	counter, ok := s.topicMaps[topic]

	if !ok {
		return base.ErrorInvalidInput
	}

	return counter.upsertMapping(specInternalId, true)
}

func (s *ShaRefCounterService) CleanupMapping(topic string, utils utilities.UtilsIface) error {
	s.topicMapMtx.Lock()
	defer s.topicMapMtx.Unlock()
	counter, ok := s.topicMaps[topic]

	doneFunc := utils.StartDiagStopwatch(fmt.Sprintf("CleanupMapping(%v) found? %v", topic, ok), base.DiagInternalThreshold)
	defer doneFunc()

	if !ok {
		// Still need to clean up the mapping
		temporaryCounter := NewMapShaRefCounterWithInternalId(topic, "", s.metadataSvc, s.metakvDocKeyGetter(topic), s.logger)
		temporaryCounter.Init()
		return temporaryCounter.DelAndCleanup()
	}

	delete(s.topicMaps, topic)
	delete(s.topicGcDisabledSet, topic)

	return counter.DelAndCleanup()
}

// After this call, the ref count should be synchronized
func (s *ShaRefCounterService) reInitUsingMergedMappingDoc(topic string, brokenMappingDoc *metadata.CollectionNsMappingsDoc, ckptDocs map[uint16]*metadata.CheckpointsDoc, internalId string) error {
	s.topicMapMtx.RLock()
	defer s.topicMapMtx.RUnlock()
	counter, ok := s.topicMaps[topic]

	if !ok {
		return base.ErrorInvalidInput
	}

	return counter.ReInitUsingMergedMappingDoc(brokenMappingDoc, ckptDocs, internalId)
}

func (s *ShaRefCounterService) DisableRefCntDecrement(topic string) {
	s.topicMapMtx.RLock()
	disabledSet, exists := s.topicGcDisabledSet[topic]
	s.topicMapMtx.RUnlock()

	if !exists || exists && !disabledSet {
		s.topicMapMtx.Lock()
		s.topicGcDisabledSet[topic] = true
		s.topicMapMtx.Unlock()
	}
}

func (s *ShaRefCounterService) EnableRefCntDecrement(topic string) {
	s.topicMapMtx.RLock()
	disabledSet, exists := s.topicGcDisabledSet[topic]
	s.topicMapMtx.RUnlock()

	if exists && disabledSet {
		// When re-enables ref cnt after being disabled, this means that the ref cnt may be inaccurate and requires a
		// global recount
		s.topicMapMtx.Lock()
		s.topicGcDisabledSet[topic] = false
		s.topicMapMtx.Unlock()
	}
}

func UnableToUpsertErr(id string) error {
	return fmt.Errorf("Unable to clean broken mappings for %v due to concurrent ongoing upsert operation", id)
}

// Used to keep track of brokenmapping SHA and the count of checkpoint records referring to it
type MapShaRefCounter struct {
	id                 string
	lock               sync.RWMutex
	singleUpsert       chan bool
	finch              chan bool
	refCnt             map[string]uint64                    // map of sha to refCnt
	shaToMapping       metadata.ShaToCollectionNamespaceMap // map of sha to actual mapping
	shaToGlobalTs      metadata.ShaToGlobalTimestampMap
	needToSync         bool // needs to sync refCnt to shaMap and then also persist to metakv
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
		shaToGlobalTs:  make(metadata.ShaToGlobalTimestampMap),
		singleUpsert:   make(chan bool, 1),
		finch:          make(chan bool, 1),
		metadataSvc:    metadataSvc,
		metakvOpKey:    metakvOpKey,
		internalSpecId: internalId,
		logger:         logger,
	}
}

var mapShaRefCounterStopped = errors.New("Replication has been deleted")

func (c *MapShaRefCounter) Init() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.singleUpsert <- true
}

func (c *MapShaRefCounter) CheckOrSetInternalSpecId(internalId string) error {
	if c.isClosed() {
		return mapShaRefCounterStopped
	}
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
	if c.isClosed() {
		return nil
	}
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.shaToMapping
}

func (c *MapShaRefCounter) GetShaGlobalTsMap() metadata.ShaToGlobalTimestampMap {
	if c.isClosed() {
		return nil
	}
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.shaToGlobalTs
}

func (c *MapShaRefCounter) upsertCompressedMappingDoc(doc *metadata.CompressedMappings, addOp bool) error {
	if c.isClosed() {
		return nil
	}
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
	if c.isClosed() {
		return nil, nil
	}
	docData, _, err := c.metadataSvc.Get(c.metakvOpKey)

	return docData, err
}

func (c *MapShaRefCounter) RecordOneCount(shaString string, value interface{}) {
	if c.isClosed() {
		return
	}

	mapping, brokenMapValOk := value.(*metadata.CollectionNamespaceMapping)
	globalTs, globalTsOk := value.(metadata.GlobalTimestamp)

	c.lock.Lock()
	defer c.lock.Unlock()
	if brokenMapValOk {
		if _, exists := c.shaToMapping[shaString]; !exists {
			c.shaToMapping[shaString] = mapping
			c.setNeedToSyncNoLock(true)
		}
	} else if globalTsOk {
		if _, exists := c.shaToGlobalTs[shaString]; !exists {
			c.shaToGlobalTs[shaString] = &globalTs
			c.setNeedToSyncNoLock(true)
		}
	} else {
		panic(fmt.Sprintf("coding bug type is %T", value))
	}
	c.refCnt[shaString]++
}

func (c *MapShaRefCounter) UnrecordOneCount(shaString string) {
	if c.isClosed() {
		return
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	if count, exists := c.refCnt[shaString]; exists && count > 0 {
		c.refCnt[shaString]--
		if c.refCnt[shaString] == 0 {
			c.setNeedToSyncNoLock(true)
		}
	}
}

func (c *MapShaRefCounter) GCDocUsingLatestInfo(doc interface{}) error {
	if c.isClosed() {
		return mapShaRefCounterStopped
	}

	nsMappingDoc, nsMappingDocType := doc.(*metadata.CollectionNsMappingsDoc)
	globalTsDoc, globalTsDocType := doc.(*metadata.GlobalTimestampCompressedDoc)

	c.lock.RLock()
	upsertCh := c.singleUpsert
	needToSyncRev := c.needToSyncRevision
	c.lock.RUnlock()

	select {
	case <-c.finch:
		return mapShaRefCounterStopped
	case <-upsertCh:
		defer func() {
			upsertCh <- true
		}()

		if nsMappingDocType {
			compressedMapping := (*metadata.CompressedMappings)(nsMappingDoc)
			c.gcUsingCompressedMappingDoc(compressedMapping, needToSyncRev)
		} else if globalTsDocType {
			compressedMapping := (*metadata.CompressedMappings)(globalTsDoc)
			c.gcUsingCompressedMappingDoc(compressedMapping, needToSyncRev)
		}

		return fmt.Errorf("unhandled Type: %T", doc)
	default:
		return UnableToUpsertErr(c.id)
	}
}

func (c *MapShaRefCounter) gcUsingCompressedMappingDoc(nsMappingDoc *metadata.CompressedMappings, needToSyncRev uint64) {
	var updatedCompressedList metadata.CompressedShaMappingList

	for _, oneCompressedMapping := range nsMappingDoc.NsMappingRecords {
		c.lock.RLock()
		count, exists := c.refCnt[oneCompressedMapping.Sha256Digest]
		c.lock.RUnlock()
		if exists && count > 0 {
			updatedCompressedList = append(updatedCompressedList, oneCompressedMapping)
		}
	}

	if len(updatedCompressedList) < len(nsMappingDoc.NsMappingRecords) {
		nsMappingDoc.NsMappingRecords = updatedCompressedList
		err := c.upsertCompressedMappingDoc(nsMappingDoc, false /*new*/)
		if err == nil {
			c.lock.Lock()
			if c.needToSyncRevision == needToSyncRev {
				c.setNeedToSyncNoLock(false)
			}
			c.lock.Unlock()
		}
	}
}

func (c *MapShaRefCounter) InitShaToActualMappings(topic, internalSpecId string, mapping interface{}) error {
	if c.isClosed() {
		return mapShaRefCounterStopped
	}
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.internalSpecId != "" && c.internalSpecId != internalSpecId {
		return fmt.Errorf("Replication %v mismatch internalId %v vs %v", topic, c.internalSpecId, internalSpecId)
	}

	shaToCollectionNsMap, shaToCollectionOK := mapping.(metadata.ShaToCollectionNamespaceMap)
	shaToGlobalTs, shaToGlobalTsOK := mapping.(metadata.ShaToGlobalTimestampMap)

	if shaToCollectionOK {
		c.shaToMapping = shaToCollectionNsMap
	} else if shaToGlobalTsOK {
		c.shaToGlobalTs = shaToGlobalTs
	} else {
		panic(fmt.Sprintf("incorrect type: %T", mapping))
	}

	c.internalSpecId = internalSpecId
	return nil
}

// Currently, starts count at 0
func (c *MapShaRefCounter) RegisterMapping(internalSpecId string, mapping *metadata.CollectionNamespaceMapping) error {
	if c.isClosed() {
		return mapShaRefCounterStopped
	}
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

func (c *MapShaRefCounter) GetMappingsDoc(initIfNotFound bool) (*metadata.CompressedMappings, error) {
	if c.isClosed() {
		return nil, mapShaRefCounterStopped
	}
	docData, err := c.getCollectionNsMappingsDocData()
	docReturn := &metadata.CompressedMappings{}

	if err != nil && err == service_def.MetadataNotFoundErr {
		if initIfNotFound {
			c.logger.Infof("GetMappingsDoc %v initialized a new mappingsDoc with internalID %v", c.id, c.internalSpecId)
			docReturn.SpecInternalId = c.internalSpecId
			err = c.upsertCompressedMappingDoc(docReturn, true /*addOp*/)
			return docReturn, err
		} else {
			err = fmt.Errorf("GetMappingsDoc was requested for %v but was not found", c.id)
			c.logger.Warnf(err.Error())
			return nil, service_def.MetadataNotFoundErr
		}
	} else if err != nil {
		c.logger.Errorf("GetMappingsDoc %v retrieval got error %v", c.id, err)
		return nil, err
	}

	err = json.Unmarshal(docData, docReturn)
	if err != nil {
		return nil, err
	}

	if docReturn.SpecInternalId != c.internalSpecId {
		// Log a warning, and expect the upper layer to know what to do about it
		c.logger.Warnf("Retrieved an out of date mapping doc with internal ID %v with current known ID %v",
			docReturn.SpecInternalId, c.internalSpecId)
		if initIfNotFound {
			emptyDoc := &metadata.CompressedMappings{}
			emptyDoc.SpecInternalId = c.internalSpecId
			c.logger.Infof("GetMappingsDoc %v re-initialized a new mappingsDoc with internalId %v", c.id, c.internalSpecId)
			err = c.upsertCompressedMappingDoc(emptyDoc, true /*addOp*/)
			return emptyDoc, err
		}
		return nil, err
	}

	c.logger.Infof("GetMappingsDoc %v retrieved an existing mappingsDoc", c.id)
	return docReturn, nil
}

// Write lock must be held
func (c *MapShaRefCounter) setNeedToSyncNoLock(val bool) {
	if c.isClosed() {
		return
	}
	c.needToSync = val
	c.needToSyncRevision++
}

func (c *MapShaRefCounter) upsertMapping(specInternalId string, cleanup bool) error {
	if c.isClosed() {
		return mapShaRefCounterStopped
	}
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
	case <-c.finch:
		return mapShaRefCounterStopped
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
		mappingDoc, err := c.GetMappingsDoc(false /*initIfNotFound*/)
		if err != nil {
			return err
		}
		if mappingDoc.SpecInternalId == "" {
			mappingDoc.SpecInternalId = specInternalId
		} else if specInternalId != "" && mappingDoc.SpecInternalId != specInternalId {
			err = fmt.Errorf("Mismatching internalId in metakv %v vs %v", mappingDoc.SpecInternalId, specInternalId)
			return err
		}

		collectionNsMappingsDoc := (*metadata.CollectionNsMappingsDoc)(mappingDoc)
		err = collectionNsMappingsDoc.LoadShaMap(clonedMapping)
		if err != nil {
			return err
		}
		err = c.upsertCompressedMappingDoc(mappingDoc, false /*new*/)
		return err
	default:
		return fmt.Errorf("Error upserting broken mapping for %v as an operation is happening already", c.id)
	}
}

func (c *MapShaRefCounter) DelAndCleanup() error {
	// Don't allow any upserts to occur concurrently - force hold lock
	select {
	case <-c.finch:
		return mapShaRefCounterStopped
	case <-c.singleUpsert:
		c.metadataSvc.Del(c.metakvOpKey, nil /*revision*/)
	}
	close(c.finch)
	return nil
}

func (c *MapShaRefCounter) isClosed() bool {
	select {
	case <-c.finch:
		return true
	default:
		return false
	}
}

// Used only when doing a complete overwrite after major merge operations
func (c *MapShaRefCounter) ReInitUsingMergedMappingDoc(brokenMappingDoc *metadata.CollectionNsMappingsDoc, ckptsDocs map[uint16]*metadata.CheckpointsDoc, internalId string) error {
	if c.isClosed() {
		return mapShaRefCounterStopped
	}
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
		if ckptDoc == nil {
			continue
		}
		for _, ckptRecord := range ckptDoc.Checkpoint_records {
			if ckptRecord == nil || ckptRecord.GetBrokenMappingShaCount() == 0 {
				continue
			}
			for _, oneSha := range ckptRecord.GetBrokenMappingSha256s() {
				c.refCnt[oneSha]++
			}
		}
	}
	c.setNeedToSyncNoLock(true)
	c.lock.Unlock()

	// Since this path is called when no other ckpt operation is occuring (or when decrementing is not possible),
	// take the opportunity to clean up
	return c.upsertMapping(internalId, true)
}
