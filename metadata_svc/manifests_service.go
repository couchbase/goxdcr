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
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
	"sync"
)

/**
 * NOTE: Currently checkpoints are still not replicated yet, so all the checkpoints
 * and the corresponding manifests live only on a single node.
 * The manifest storage key here is on a single node.
 * Once the checkpoints are shared, then each PersistNeededManifest() call will load the
 * shared checkpoints instead of just the single node,
 * and the existing mechanism should still work
 */
const ManifestsCatalogKeyPrefix = CheckpointsCatalogKeyPrefix + "/manifest"
const sourceString = "src"
const targetString = "tgt"

type ManifestsService struct {
	metadata_svc service_def.MetadataSvc
	logger       *log.CommonLogger
}

func getManifestDocKey(replicationId string, source bool) string {
	if source {
		return fmt.Sprintf("%v", ManifestsCatalogKeyPrefix+base.KeyPartsDelimiter+replicationId+base.KeyPartsDelimiter+sourceString)
	} else {
		return fmt.Sprintf("%v", ManifestsCatalogKeyPrefix+base.KeyPartsDelimiter+replicationId+base.KeyPartsDelimiter+targetString)
	}
}

func NewManifestsService(metadata_svc service_def.MetadataSvc, logger_ctx *log.LoggerContext) service_def.ManifestsService {
	return &ManifestsService{metadata_svc: metadata_svc,
		logger: log.NewLogger("ManifestService", logger_ctx)}
}

func (m *ManifestsService) UpsertSourceManifests(replSpec *metadata.ReplicationSpecification, src *metadata.ManifestsList) error {
	key := getManifestDocKey(replSpec.Id, true /*source*/)

	if src == nil {
		return base.ErrorInvalidInput
	}

	manifestsDoc := &metadata.ManifestsDoc{}
	manifestsDoc.SetCollectionsManifests(*src)

	return m.upsertInternal(key, manifestsDoc)
}

func (m *ManifestsService) UpsertTargetManifests(replSpec *metadata.ReplicationSpecification, tgt *metadata.ManifestsList) error {
	key := getManifestDocKey(replSpec.Id, false /*source*/)

	if tgt == nil {
		return base.ErrorInvalidInput
	}

	manifestsDoc := &metadata.ManifestsDoc{}
	manifestsDoc.SetCollectionsManifests(*tgt)

	return m.upsertInternal(key, manifestsDoc)
}

func (m *ManifestsService) upsertInternal(key string, data *metadata.ManifestsDoc) error {
	err := data.PreMarshal()
	if err != nil {
		return err
	}

	serializedJson, err := json.Marshal(data)
	if err != nil {
		return err
	}

	// TODO - better optimization? First check if it exists
	_, err = m.getInternal(key)
	if err == service_def.MetadataNotFoundErr {
		err = m.metadata_svc.Add(key, serializedJson)
	} else {
		err = m.metadata_svc.Set(key, serializedJson, nil)
	}

	getRevisionDoc, err := m.getInternal(key)
	if err != nil {
		m.logger.Warnf("Getting revision after set or add for %v resulted in err %v", key, err)
	} else {
		data.SetRevision(getRevisionDoc.Revision())
	}

	data.ClearCompressedData()
	return err
}

func (m *ManifestsService) GetSourceManifests(replSpec *metadata.ReplicationSpecification) (*metadata.ManifestsList, error) {
	key := getManifestDocKey(replSpec.Id, true /*source*/)
	manifestsDoc, err := m.getInternal(key)
	if err == service_def.MetadataNotFoundErr {
		return nil, service_def.MetadataNotFoundErr
	} else if err != nil {
		return nil, err
	} else {
		list := metadata.ManifestsList(manifestsDoc.CollectionsManifests())
		return &list, err
	}
}

func (m *ManifestsService) GetTargetManifests(replSpec *metadata.ReplicationSpecification) (*metadata.ManifestsList, error) {
	key := getManifestDocKey(replSpec.Id, false /*source*/)
	manifestsDoc, err := m.getInternal(key)
	if err == service_def.MetadataNotFoundErr {
		return nil, nil
	} else if err != nil {
		return nil, err
	} else {
		list := metadata.ManifestsList(manifestsDoc.CollectionsManifests())
		return &list, err
	}
}

func (m *ManifestsService) DelManifests(replSpec *metadata.ReplicationSpecification) error {
	errorMap := make(base.ErrorMap)
	var errMtx sync.Mutex

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		key := getManifestDocKey(replSpec.Id, true /*source*/)
		err := m.metadata_svc.Del(key, nil /*revision*/)
		if err != nil {
			errMtx.Lock()
			errorMap["sourceManifestDelOp"] = err
			errMtx.Unlock()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		key := getManifestDocKey(replSpec.Id, false /*source*/)
		err := m.metadata_svc.Del(key, nil /*revision*/)
		if err != nil {
			errMtx.Lock()
			errorMap["targetManifestDelOp"] = err
			errMtx.Unlock()
		}
	}()
	wg.Wait()

	if len(errorMap) > 0 {
		return fmt.Errorf(base.FlattenErrorMap(errorMap))
	} else {
		return nil
	}
}

func (m *ManifestsService) getInternal(key string) (*metadata.ManifestsDoc, error) {
	serializedJson, rev, err := m.metadata_svc.Get(key)
	if err != nil {
		if err != service_def.MetadataNotFoundErr {
			m.logger.Errorf("Unable to retrieve manifests using key %v err: %v", key, err)
		}
		return nil, err
	}

	manifestsDoc := &metadata.ManifestsDoc{}
	err = json.Unmarshal(serializedJson, manifestsDoc)
	if err != nil {
		m.logger.Errorf("Unable to unmarshal manifestsDoc err=%v", err)
		return nil, err
	}

	manifestsDoc.SetRevision(rev)

	err = manifestsDoc.PostUnmarshal()
	if err != nil {
		m.logger.Errorf("PostUnmarshal err: %v", err)
	}
	manifestsDoc.ClearCompressedData()
	return manifestsDoc, nil
}
