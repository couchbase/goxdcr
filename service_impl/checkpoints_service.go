package service_impl

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
)

const (
	// the key to the metadata that stores the keys of all remote clusters
	CheckpointsCatalogKeyPrefix = "ckpt"
	CheckpointsKeyPrefix        = CheckpointsCatalogKeyPrefix
)

type CheckpointsService struct {
	metadata_svc service_def.MetadataSvc
	logger       *log.CommonLogger
}

func NewCheckpointsService(metadata_svc service_def.MetadataSvc, logger_ctx *log.LoggerContext) service_def.CheckpointsService {
	return &CheckpointsService{metadata_svc: metadata_svc,
		logger: log.NewLogger("CheckpointService", logger_ctx)}
}

func (ckpt_svc *CheckpointsService) CheckpointsDoc(replicationId string, vbno uint16) (*metadata.CheckpointsDoc, error) {
	key := ckpt_svc.getCheckpointDocKey(replicationId, vbno)
	result, rev, err := ckpt_svc.metadata_svc.Get(key)
	if err != nil {
		return nil, err
	}
	ckpt_doc, err := ckpt_svc.constructCheckpointDoc(result, rev)
	return ckpt_doc, err
}

func (ckpt_svc *CheckpointsService) getCheckpointCatalogKey(replicationId string) string {
	return CheckpointsCatalogKeyPrefix + base.KeyPartsDelimiter + replicationId
}

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

func (ckpt_svc *CheckpointsService) DelCheckpointsDocs(replicationId string) error {
	ckpt_svc.logger.Info("DelCheckpointsDocs...")
	catalogKey := ckpt_svc.getCheckpointCatalogKey(replicationId)
	err_ret := ckpt_svc.metadata_svc.DelAllFromCatalog(catalogKey)
	if err_ret != nil {
		ckpt_svc.logger.Errorf("Failed to delete checkpoints docs for %v\n", replicationId)
	} else {
		ckpt_svc.logger.Infof("DelCheckpointsDocs is done for %v\n", replicationId)
	}
	return err_ret
}

func (ckpt_svc *CheckpointsService) UpsertCheckpoints(replicationId string, vbno uint16, ckpt_record *metadata.CheckpointRecord) error {
	ckpt_svc.logger.Debugf("Persisting checkpoint record=%v for vbno=%v replication=%v\n", ckpt_record, vbno, replicationId)
	if ckpt_record == nil {
		return errors.New("nil checkpoint record")
	}
	key := ckpt_svc.getCheckpointDocKey(replicationId, vbno)
	ckpt_doc, err := ckpt_svc.CheckpointsDoc(replicationId, vbno)
	if err != nil {
		return err
	}
	if ckpt_doc == nil {
		ckpt_doc = metadata.NewCheckpointsDoc()
	}
	ckpt_doc.AddRecord(ckpt_record)
	ckpt_json, err := json.Marshal(ckpt_doc)
	if err != nil {
		return err
	}

	err = ckpt_svc.metadata_svc.Set(key, ckpt_json, ckpt_doc.Revision)

	if err != nil {
		ckpt_svc.logger.Errorf("Failed to set checkpoint doc key=%v, err=%v\n", key, err)
	}
	return err
}

func (ckpt_svc *CheckpointsService) CheckpointsDocs(replicationId string) (map[uint16]*metadata.CheckpointsDoc, error) {
	checkpointsDocs := make(map[uint16]*metadata.CheckpointsDoc)
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

			ckpt_doc, err := ckpt_svc.constructCheckpointDoc(ckpt_entry.Value, ckpt_entry.Rev)
			if err != nil {
				return nil, err
			}
			checkpointsDocs[vbno] = ckpt_doc
		}
	}
	return checkpointsDocs, nil
}

func (ckpt_svc *CheckpointsService) constructCheckpointDoc(content []byte, rev interface{}) (*metadata.CheckpointsDoc, error) {
	var ckpt_doc *metadata.CheckpointsDoc = nil
	if len(content) > 0 {
		ckpt_doc = &metadata.CheckpointsDoc{}
		err := json.Unmarshal(content, ckpt_doc)
		if err != nil {
			return nil, err
		}
		ckpt_doc.Revision = rev
	}
	return ckpt_doc, nil
}
