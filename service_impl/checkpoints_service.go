package service_impl

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	"math"
	"strconv"
	"strings"
	"sync"
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
	ckpt_meta_entries, err := ckpt_svc.metadata_svc.GetAllMetadataFromCatalog(catalogKey)
	if err != nil {
		return err
	}
	
	if len(ckpt_meta_entries) == 0 {
		return nil
	}

	worker_load := 5
	num_of_workers := int(math.Ceil(float64(len(ckpt_meta_entries)) / float64(worker_load)))
	task_index_start := 0
	worker_wait_grp := &sync.WaitGroup{}
	errMap := make(map[int]error)
	for i := 0; i < num_of_workers; i++ {
		task_index_end := int(math.Min(float64(task_index_start+worker_load), float64(len(ckpt_meta_entries))))

		worker_wait_grp.Add(1)
		go func(key_entries []*service_def.MetadataEntry, ckpt_svc *CheckpointsService, waitGrp *sync.WaitGroup, errMap map[int]error, workerId int) {
			defer worker_wait_grp.Done()
			for _, entry := range key_entries {
				err := ckpt_svc.metadata_svc.DelWithCatalog(catalogKey, entry.Key, entry.Rev)
				if err != nil {
					errMap[workerId] = err
				}
				ckpt_svc.logger.Debugf("checkpoint file %v for replication %v is deleted\n", entry.Key, replicationId)
			}
		}(ckpt_meta_entries[task_index_start:task_index_end], ckpt_svc, worker_wait_grp, errMap, i)

		task_index_start = task_index_end
	}

	worker_wait_grp.Wait()

	var err_ret error = nil
	if len(errMap) > 0 {
		ckpt_svc.logger.Errorf("Failed to delete checkpoint files for %v, err=%v", replicationId, errMap)
		err_ret = errors.New(fmt.Sprintf("Failed to delete checkpoint files for %v, err=%v", replicationId, errMap))
	}
	ckpt_svc.logger.Infof("DelCheckpointsDocs is done for %v\n", replicationId)
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
	ckpt_keys, err := ckpt_svc.metadata_svc.GetAllKeysFromCatalog(catalogKey)
	if err != nil {
		return nil, err
	}

	for _, ckpt_key := range ckpt_keys {
		vbno, err := ckpt_svc.decodeVbnoFromCkptDocKey(ckpt_key)
		if err != nil {
			return nil, err
		}

		result, rev, err := ckpt_svc.metadata_svc.Get(ckpt_key)
		if err != nil {
			return nil, err
		}

		ckpt_doc, err := ckpt_svc.constructCheckpointDoc(result, rev)
		if err != nil {
			return nil, err
		}
		checkpointsDocs[vbno] = ckpt_doc
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
