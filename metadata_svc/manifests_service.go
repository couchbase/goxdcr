package metadata_svc

import (
	"encoding/json"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/golang/snappy"
)

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

func (m *ManifestsService) upsertInternal(key string, data *metadata.ManifestsDoc) error {
	serializedJson, err := json.Marshal(data)
	if err != nil {
		return err
	}

	compressedContent := snappy.Encode(nil, serializedJson)

	err = m.metadata_svc.Set(key, compressedContent, nil)
	return err
}

func (m *ManifestsService) getInternal(key string) (*metadata.ManifestsDoc, error) {
	compressedContent, rev, err := m.metadata_svc.Get(key)
	if err != nil {
		m.logger.Errorf("Unable to retrieve manifests using key %v err: %v", key, err)
		return nil, err
	}

	var serializedJson []byte
	serializedJson, err = snappy.Decode(serializedJson, compressedContent)
	if err != nil {
		m.logger.Errorf("Unable to decompress manifests using key %v err: %v", key, err)
		return nil, err
	}

	manifestsDoc := &metadata.ManifestsDoc{}
	err = json.Unmarshal(serializedJson, manifestsDoc)
	if err != nil {
		m.logger.Errorf("Unable to unmarshal manifestsDoc err=%v", err)
		return nil, err
	}

	manifestsDoc.Revision = rev
	return manifestsDoc, nil
}

func (m *ManifestsService) UpsertSourceManifests(replSpec *metadata.ReplicationSpecification, src *metadata.ManifestsList) error {
	key := getManifestDocKey(replSpec.Id, true /*source*/)

	manifestsDoc := &metadata.ManifestsDoc{}
	manifestsDoc.CollectionsManifests = *src

	return m.upsertInternal(key, manifestsDoc)
}

func (m *ManifestsService) GetSourceManifests(replSpec *metadata.ReplicationSpecification) (*metadata.ManifestsList, error) {
	key := getManifestDocKey(replSpec.Id, true /*source*/)
	manifestsDoc, err := m.getInternal(key)
	if err == service_def.MetadataNotFoundErr {
		return nil, service_def.MetadataNotFoundErr
	} else if err != nil {
		return nil, err
	} else {
		list := metadata.ManifestsList(manifestsDoc.CollectionsManifests)
		return &list, err
	}
}

func (m *ManifestsService) UpsertTargetManifests(replSpec *metadata.ReplicationSpecification, tgt *metadata.ManifestsList) error {
	key := getManifestDocKey(replSpec.Id, false /*source*/)

	manifestsDoc := &metadata.ManifestsDoc{}
	manifestsDoc.CollectionsManifests = *tgt

	return m.upsertInternal(key, manifestsDoc)
}

func (m *ManifestsService) GetTargetManifests(replSpec *metadata.ReplicationSpecification) (*metadata.ManifestsList, error) {
	key := getManifestDocKey(replSpec.Id, false /*source*/)
	manifestsDoc, err := m.getInternal(key)
	if err == service_def.MetadataNotFoundErr {
		return nil, nil
	} else if err != nil {
		return nil, err
	} else {
		list := metadata.ManifestsList(manifestsDoc.CollectionsManifests)
		return &list, err
	}
}
