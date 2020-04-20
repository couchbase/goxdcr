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
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	"sync"
)

type ShaRefCounterService struct {
	metadataSvc service_def.MetadataSvc
	// Map of replicationId -> map of sha's -> refCnts
	topicMapMtx sync.RWMutex
	topicMaps   map[string]*MapShaRefCounter
	// Prevent concurrent upsert for the same single backfill doc
	//	topicUpsertCh map[string]chan bool

	// Given a replication ID, return the key to use for metadataSvc
	metakvDocKeyGetter func(string) string
}

func NewShaRefCounterService(metakvDocKeyGetter func(string) string,
	metadataSvc service_def.MetadataSvc) *ShaRefCounterService {
	return &ShaRefCounterService{
		topicMaps: make(map[string]*MapShaRefCounter),
		//		topicUpsertCh:      make(map[string]chan bool),
		metakvDocKeyGetter: metakvDocKeyGetter,
		metadataSvc:        metadataSvc,
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
func (s *ShaRefCounterService) InitTopicShaCounter(topic string) (alreadyExists bool) {
	s.topicMapMtx.RLock()
	_, alreadyExists = s.topicMaps[topic]
	s.topicMapMtx.RUnlock()

	if alreadyExists {
		return
	}

	s.topicMapMtx.Lock()
	counter := NewMapShaRefCounter(topic, s.metadataSvc, s.metakvDocKeyGetter(topic))
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
		// TODO - MB-38506 - need to stop restarting if the error is not recoverable
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

type IncrementerFunc func(shaString string, mapping *metadata.CollectionNamespaceMapping)

type DecrementerFunc func(shaString string)

func (s *ShaRefCounterService) GetIncrementerFunc(topic string) (IncrementerFunc, error) {
	s.topicMapMtx.RLock()
	counter, ok := s.topicMaps[topic]
	s.topicMapMtx.RUnlock()

	if !ok {
		return nil, base.ErrorInvalidInput
	}

	return counter.RecordOneCount, nil
}

func (s *ShaRefCounterService) GetDecrementerFunc(topic string) (DecrementerFunc, error) {
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

	return counter.upsertMapping(topic, specInternalId, true /*cleanup*/)
}

func (s *ShaRefCounterService) CleanupMapping(topic string) error {
	s.topicMapMtx.Lock()
	counter, ok := s.topicMaps[topic]
	s.topicMapMtx.Unlock()

	if !ok {
		return nil
	}

	s.topicMapMtx.Lock()
	delete(s.topicMaps, topic)
	s.topicMapMtx.Unlock()

	return counter.DelAndCleanup()
}

func UnableToUpsertErr(id string) error {
	return fmt.Errorf("Unable to clean broken mappings for %v due to concurrent ongoing upsert operation", id)
}

// Used to keep track of brokenmapping SHA and the count of checkpoint records referring to it
type MapShaRefCounter struct {
	id             string
	lock           sync.RWMutex
	singleUpsert   chan bool
	refCnt         map[string]uint64                    // map of sha to refCnt (1/2)
	shaToMapping   metadata.ShaToCollectionNamespaceMap // map of sha to actual mapping (2/2)
	needToSync     bool                                 // needs to sync refCnt to shaMap and then also persist to metakv
	internalSpecId string
	metadataSvc    service_def.MetadataSvc
	metakvOpKey    string
}

func NewMapShaRefCounter(topic string, metadataSvc service_def.MetadataSvc, metakvOpKey string) *MapShaRefCounter {
	return &MapShaRefCounter{refCnt: make(map[string]uint64),
		id:           topic,
		shaToMapping: make(metadata.ShaToCollectionNamespaceMap),
		singleUpsert: make(chan bool, 1),
		metadataSvc:  metadataSvc,
		metakvOpKey:  metakvOpKey,
	}
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
		c.needToSync = true
	}
	c.refCnt[shaString]++
}

func (c *MapShaRefCounter) UnrecordOneCount(shaString string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if count, exists := c.refCnt[shaString]; exists && count > 0 {
		c.refCnt[shaString]--
		if c.refCnt[shaString] == 0 {
			c.needToSync = true
		}
	}
}

func (c *MapShaRefCounter) GCDocUsingLatestInfo(doc *metadata.CollectionNsMappingsDoc) error {
	c.lock.RLock()
	upsertCh := c.singleUpsert
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
				c.needToSync = false
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
			c.needToSync = true
		}
		c.lock.Unlock()
	}

	return c.upsertMapping(c.id, internalSpecId, false /*cleanup*/)
}

func (c *MapShaRefCounter) GetMappingsDoc(initIfNotFound bool) (*metadata.CollectionNsMappingsDoc, error) {
	docData, err := c.getCollectionNsMappingsDocData()
	docReturn := &metadata.CollectionNsMappingsDoc{}

	if err != nil && err == service_def.MetadataNotFoundErr {
		if initIfNotFound {
			err = c.upsertCollectionNsMappingsDoc(docReturn, true /*addOp*/)
			return docReturn, err
		} else {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	err = json.Unmarshal(docData, docReturn)
	if err != nil {
		return nil, err
	}

	return docReturn, nil
}

func (c *MapShaRefCounter) upsertMapping(topic, specInternalId string, cleanup bool) error {
	c.lock.RLock()
	needToSync := c.needToSync
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
			upsertCh <- true
		}()
		defer func() {
			if err == nil {
				c.lock.Lock()
				c.needToSync = false
				c.lock.Unlock()
			}
		}()

		c.lock.Lock()
		if c.internalSpecId == "" {
			c.internalSpecId = specInternalId
		} else if c.internalSpecId != specInternalId {
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
		} else if collectionNsMappingsDoc.SpecInternalId != specInternalId {
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
