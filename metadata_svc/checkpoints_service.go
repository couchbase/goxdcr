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
		logger: log.NewLogger("CheckpointSvc", logger_ctx)}
}

func (ckpt_svc *CheckpointsService) CheckpointsDoc(replicationId string, vbno uint16) (*metadata.CheckpointsDoc, error) {
	key := ckpt_svc.getCheckpointDocKey(replicationId, vbno)
	result, rev, err := ckpt_svc.metadata_svc.Get(key)
	if err != nil {
		return nil, err
	}
	ckpt_doc, err := ckpt_svc.constructCheckpointDoc(result, rev)
	if err == service_def.MetadataNotFoundErr {
		ckpt_svc.logger.Errorf("Unable to construct ckpt doc from metakv given replication: %v vbno: %v key: %v",
			replicationId, vbno, key)
	}
	return ckpt_doc, err
}

func (ckpt_svc *CheckpointsService) getCheckpointCatalogKey(replicationId string) string {
	return CheckpointsCatalogKeyPrefix + base.KeyPartsDelimiter + replicationId
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

func (ckpt_svc *CheckpointsService) DelCheckpointsDocs(replicationId string) error {
	ckpt_svc.logger.Infof("DelCheckpointsDocs for replication %v...", replicationId)
	catalogKey := ckpt_svc.getCheckpointCatalogKey(replicationId)
	err_ret := ckpt_svc.metadata_svc.DelAllFromCatalog(catalogKey)
	if err_ret != nil {
		ckpt_svc.logger.Errorf("Failed to delete checkpoints docs for %v\n", replicationId)
	} else {
		ckpt_svc.logger.Infof("DelCheckpointsDocs is done for %v\n", replicationId)
	}
	return err_ret
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
func (ckpt_svc *CheckpointsService) UpsertCheckpoints(replicationId string, specInternalId string, vbno uint16,
	ckpt_record *metadata.CheckpointRecord, xattr_seqno uint64, targetClusterVersion int) error {
	ckpt_svc.logger.Debugf("Persisting checkpoint record=%v xattr_seqno=%v for vbno=%v replication=%v\n", ckpt_record, xattr_seqno, vbno, replicationId)

	if ckpt_record == nil {
		return errors.New("nil checkpoint record")
	}
	key := ckpt_svc.getCheckpointDocKey(replicationId, vbno)
	ckpt_doc, err := ckpt_svc.CheckpointsDoc(replicationId, vbno)
	if err == service_def.MetadataNotFoundErr {
		ckpt_doc = metadata.NewCheckpointsDoc(specInternalId)
		err = nil
	}
	if err != nil {
		return err
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

	added := ckpt_doc.AddRecord(ckpt_record)
	if !added {
		ckpt_svc.logger.Debug("the ckpt record to be added is the same as the current ckpt record in the ckpt doc. no-op.")
	} else {
		ckpt_json, err := json.Marshal(ckpt_doc)
		if err != nil {
			return err
		}

		//always update the checkpoint without revision
		err = ckpt_svc.metadata_svc.Set(key, ckpt_json, nil)

		if err != nil {
			ckpt_svc.logger.Errorf("Failed to set checkpoint doc key=%v, err=%v\n", key, err)
		}
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
				if err == service_def.MetadataNotFoundErr {
					ckpt_svc.logger.Errorf("Unable to construct ckpt doc from metakv given replicationId: %v vbno: %v and key: %v",
						replicationId, vbno, ckpt_entry.Key)
					continue
				} else {
					return nil, err
				}
			} else {
				checkpointsDocs[vbno] = ckpt_doc
			}
		}
	}
	return checkpointsDocs, nil
}

func (ckpt_svc *CheckpointsService) constructCheckpointDoc(content []byte, rev interface{}) (*metadata.CheckpointsDoc, error) {
	// The only time content is empty is when this is a fresh XDCR system and no checkpoints has been registered yet
	if len(content) > 0 {
		ckpt_doc := &metadata.CheckpointsDoc{}
		err := json.Unmarshal(content, ckpt_doc)
		if err != nil {
			return nil, err
		}
		ckpt_doc.Revision = rev
		return ckpt_doc, nil
	} else {
		ckpt_svc.logger.Errorf("Unable to construct valid checkpoint due to empty checkpoint data")
		return nil, service_def.MetadataNotFoundErr
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
