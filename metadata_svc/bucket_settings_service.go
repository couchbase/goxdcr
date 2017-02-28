// Copyright (c) 2013 Couchbase, Inc.
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
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/utils"
)

const (
	// parent dir of all bucket settings
	BucketSettingsCatalogKey = "bucketSettings"
)

type BucketSettingsService struct {
	metadata_svc             service_def.MetadataSvc
	xdcr_comp_topology_svc   service_def.XDCRCompTopologySvc
	logger                   *log.CommonLogger
	metadata_change_callback base.MetadataChangeHandlerCallback
}

func NewBucketSettingsService(metadata_svc service_def.MetadataSvc,
	xdcr_comp_topology_svc service_def.XDCRCompTopologySvc,
	logger_ctx *log.LoggerContext) *BucketSettingsService {
	return &BucketSettingsService{
		metadata_svc:           metadata_svc,
		xdcr_comp_topology_svc: xdcr_comp_topology_svc,
		logger:                 log.NewLogger("BucketSettingsService", logger_ctx),
	}
}

func (service *BucketSettingsService) SetMetadataChangeHandlerCallback(call_back base.MetadataChangeHandlerCallback) {
	service.metadata_change_callback = call_back
}

// When bucketName is not found in metadata service, this method returns a default bucket settings with default values for all fields,
// e.g., false for lww_enabled. This is needed when bucket settings is never set on a bucket.
// TODO: The same will happen if an invalid/non-existent bucketName is passed in. Should we validate the bucket name with buckets in the cluster?
func (service *BucketSettingsService) BucketSettings(bucketName string) (*metadata.BucketSettings, error) {
	var bucketSettings *metadata.BucketSettings

	bucketUUID, err := service.getBucketUUID(bucketName)
	if err != nil {
		return nil, err
	}

	bytes, rev, err := service.metadata_svc.Get(getKeyFromBucketUUID(bucketUUID))

	if err == service_def.MetadataNotFoundErr {
		// if not found in metadata service, create a new one with default settings, e.g., false for lwwEnabled
		bucketSettings = metadata.NewBucketSettings(bucketName)
	} else {
		bucketSettings, err = constructBucketSettings(bytes, rev)
		if err != nil {
			return nil, err
		}
	}
	service.logger.Infof("BucketSettings for bucket %v with uuid %v is %v\n", bucketName, bucketUUID, bucketSettings)
	return bucketSettings, nil
}

// existing bucket settings may or may not be present when this method is called
func (service *BucketSettingsService) SetBucketSettings(bucketName string, bucketSettings *metadata.BucketSettings) error {
	bucketUUID, err := service.getBucketUUID(bucketName)
	if err != nil {
		return err
	}

	key := getKeyFromBucketUUID(bucketUUID)
	value, err := json.Marshal(bucketSettings)
	if err != nil {
		return err
	}

	_, rev, err := service.metadata_svc.Get(key)
	if err == service_def.MetadataNotFoundErr {
		err = service.metadata_svc.AddWithCatalog(BucketSettingsCatalogKey, key, value)
		if err != nil {
			return err
		}
	} else {
		// if there is an existing bucket settings, we need to use its revision number to ensure that set will succeed
		// other info in the existing bucket settings is not important
		err = service.metadata_svc.Set(key, value, rev)
		if err != nil {
			return err
		}
	}

	service.logger.Infof("BucketSettings for bucket %v with uuid %v is set as %v\n", bucketName, bucketUUID, bucketSettings)
	return nil
}

func getKeyFromBucketUUID(bucketUUID string) string {
	return BucketSettingsCatalogKey + base.KeyPartsDelimiter + bucketUUID
}

func getBucketUUIDFromKey(key string) string {
	return key[len(BucketSettingsCatalogKey)+len(base.KeyPartsDelimiter):]
}

func constructBucketSettings(value []byte, rev interface{}) (*metadata.BucketSettings, error) {
	if value == nil {
		return nil, nil
	}

	bucketSettings := &metadata.BucketSettings{}
	err := json.Unmarshal(value, bucketSettings)
	if err != nil {
		return nil, err
	}
	bucketSettings.Revision = rev
	return bucketSettings, nil
}

// Implement callback function for metakv
func (service *BucketSettingsService) BucketSettingsServiceCallback(path string, value []byte, rev interface{}) error {
	service.logger.Infof("BucketSettingsServiceCallback called on path = %v\n", path)

	bucketSettings, err := constructBucketSettings(value, rev)
	if err != nil {
		service.logger.Errorf("Error marshaling bucket settings. value=%v, err=%v\n", string(value), err)
		return err
	}

	if service.metadata_change_callback != nil {
		bucketUUID := getBucketUUIDFromKey(GetKeyFromPath(path))
		err = service.metadata_change_callback(bucketUUID, nil, bucketSettings)
		if err != nil {
			service.logger.Error(err.Error())
		}
	}
	return nil
}

func (service *BucketSettingsService) getBucketUUID(bucketName string) (string, error) {
	connStr, err := service.xdcr_comp_topology_svc.MyConnectionStr()
	if err != nil {
		return "", err
	}

	return utils.LocalBucketUUID(connStr, bucketName, service.logger)
}
