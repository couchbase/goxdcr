// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

// metadata service implementation leveraging metakv
package metadata_svc

import (
	"fmt"
	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/service_def"
	utilities "github.com/couchbase/goxdcr/v8/utils"
	"strings"
	"sync"
	"time"
)

type MetaKVMetadataSvc struct {
	logger   *log.CommonLogger
	utils    utilities.UtilsIface
	readOnly bool
}

func NewMetaKVMetadataSvc(logger_ctx *log.LoggerContext, utilsIn utilities.UtilsIface, readOnly bool) (*MetaKVMetadataSvc, error) {
	return &MetaKVMetadataSvc{
		logger:   log.NewLogger("MetadataSvc", logger_ctx),
		utils:    utilsIn,
		readOnly: readOnly,
	}, nil
}

//Wrap metakv.Get with retries
//if the key is not found in metakv, return nil, nil, service_def.MetadataNotFoundErr
//if metakv operation failed after max number of retries, return nil, nil, ErrorFailedAfterRetry
func (meta_svc *MetaKVMetadataSvc) Get(key string) ([]byte, interface{}, error) {
	var value []byte
	var rev interface{}
	var err error

	stopFunc := meta_svc.utils.StartDiagStopwatch(fmt.Sprintf("Get(%v)", key), base.DiagInternalThreshold)
	defer stopFunc()

	metakvOpGetFunc := func() error {
		value, rev, err = metakv.Get(getPathFromKey(key))
		if value == nil && rev == nil && err == nil {
			meta_svc.logger.Debugf("Can't find key=%v", key)
			err = service_def.MetadataNotFoundErr
			return nil
		} else if err == nil {
			return nil
		} else {
			meta_svc.logger.Warnf("metakv.Get failed. path=%v, err=%v", getPathFromKey(key), err)
			return err
		}
	}

	expOpErr := meta_svc.utils.ExponentialBackoffExecutor("metakv_svc_getOp", base.RetryIntervalMetakv, base.MaxNumOfMetakvRetries,
		base.MetaKvBackoffFactor, metakvOpGetFunc)

	if expOpErr != nil {
		// Executor will return error only if it timed out with ErrorFailedAfterRetry. Log it and override the ret err
		meta_svc.logger.Errorf("metakv.Get failed after max retry. path=%v, err=%v", getPathFromKey(key), err)
		err = expOpErr
	}

	return value, rev, err
}

func (meta_svc *MetaKVMetadataSvc) Add(key string, value []byte) error {
	if meta_svc.readOnly {
		return nil
	}
	return meta_svc.add(key, value, false)
}

func (meta_svc *MetaKVMetadataSvc) AddSensitive(key string, value []byte) error {
	if meta_svc.readOnly {
		return nil
	}
	return meta_svc.add(key, value, true)
}

//Wrap metakv.Add with retries
//if the key is already exist in metakv, return service_def.ErrorKeyAlreadyExist
//if metakv operation failed after max number of retries, return ErrorFailedAfterRetry
func (meta_svc *MetaKVMetadataSvc) add(key string, value []byte, sensitive bool) error {
	var err error
	var redactedValue []byte
	valueToPrint := &value
	start_time := time.Now()

	stopFunc := meta_svc.utils.StartDiagStopwatch(fmt.Sprintf("Get(%v)", key), base.DiagInternalThreshold)
	defer stopFunc()

	defer meta_svc.logger.Debugf("Took %vs to add %v to metakv\n", time.Since(start_time).Seconds(), key)

	var redactOnceSync sync.Once
	redactOnce := func() {
		redactOnceSync.Do(func() {
			if sensitive {
				redactedValue = base.DeepCopyByteArray(value)
				redactedValue = base.TagUDBytes(redactedValue)
				valueToPrint = &redactedValue
			}
		})
	}

	metakvOpAddFunc := func() error {
		if sensitive {
			err = metakv.AddSensitive(getPathFromKey(key), value)
		} else {
			err = metakv.Add(getPathFromKey(key), value)
		}
		if err == metakv.ErrRevMismatch {
			err = service_def.ErrorKeyAlreadyExist
			return nil
		} else if err == nil {
			return nil
		} else {
			redactOnce()
			meta_svc.logger.Warnf("metakv.Add failed. key=%v, value=%v, err=%v\n", key, valueToPrint, err)
			return err
		}
	}

	expOpErr := meta_svc.utils.ExponentialBackoffExecutor("metakv_svc_addOp", base.RetryIntervalMetakv, base.MaxNumOfMetakvRetries,
		base.MetaKvBackoffFactor, metakvOpAddFunc)

	if expOpErr != nil {
		// Executor will return error only if it timed out with ErrorFailedAfterRetry. Log it and override the ret err
		redactOnce()
		meta_svc.logger.Errorf("metakv.Add failed after max retry. key=%v, value=%v, err=%v\n", key, valueToPrint, err)
		err = expOpErr
	}

	return err
}

func (meta_svc *MetaKVMetadataSvc) AddWithCatalog(catalogKey, key string, value []byte) error {
	if meta_svc.readOnly {
		return nil
	}
	// ignore catalogKey
	return meta_svc.Add(key, value)
}

func (meta_svc *MetaKVMetadataSvc) AddSensitiveWithCatalog(catalogKey, key string, value []byte) error {
	if meta_svc.readOnly {
		return nil
	}
	// ignore catalogKey
	return meta_svc.AddSensitive(key, value)
}

func (meta_svc *MetaKVMetadataSvc) Set(key string, value []byte, rev interface{}) error {
	if meta_svc.readOnly {
		return nil
	}
	return meta_svc.set(key, value, rev, false)
}

func (meta_svc *MetaKVMetadataSvc) SetSensitive(key string, value []byte, rev interface{}) error {
	if meta_svc.readOnly {
		return nil
	}
	return meta_svc.set(key, value, rev, true)
}

//Wrap metakv.Set with retries
//if the rev provided doesn't match with the rev metakv has, return service_def.ErrorRevisionMismatch
//if metakv operation failed after max number of retries, return ErrorFailedAfterRetry
func (meta_svc *MetaKVMetadataSvc) set(key string, value []byte, rev interface{}, sensitive bool) error {
	var err error
	var redactedValue []byte
	valueToPrint := &value

	stopFunc := meta_svc.utils.StartDiagStopwatch(fmt.Sprintf("Set(%v)", key), base.DiagInternalThreshold)
	defer stopFunc()

	var redactOnceSync sync.Once
	redactOnce := func() {
		redactOnceSync.Do(func() {
			if sensitive {
				redactedValue = base.DeepCopyByteArray(value)
				redactedValue = base.TagUDBytes(redactedValue)
				valueToPrint = &redactedValue
			}
		})
	}

	metakvOpSetFunc := func() error {
		if sensitive {
			err = metakv.SetSensitive(getPathFromKey(key), value, rev)
		} else {
			err = metakv.Set(getPathFromKey(key), value, rev)
		}
		if err == metakv.ErrRevMismatch {
			err = service_def.ErrorRevisionMismatch
			return nil
		} else if err == nil {
			return nil
		} else {
			redactOnce()
			meta_svc.logger.Warnf("metakv.Set failed. key=%v, value=%v, err=%v\n", key, valueToPrint, err)
			return err
		}
	}

	expOpErr := meta_svc.utils.ExponentialBackoffExecutor("metakv_svc_setOp", base.RetryIntervalMetakv, base.MaxNumOfMetakvRetries,
		base.MetaKvBackoffFactor, metakvOpSetFunc)

	if expOpErr != nil {
		redactOnce()
		meta_svc.logger.Errorf("metakv.Set failed after max retry. key=%v, value=%v, err=%v\n", key, valueToPrint, err)
		err = expOpErr
	}
	return err
}

//Wrap metakv.Del with retries
//if the rev provided doesn't match with the rev metakv has, return service_def.ErrorRevisionMismatch
//if metakv operation failed after max number of retries, return ErrorFailedAfterRetry
func (meta_svc *MetaKVMetadataSvc) Del(key string, rev interface{}) error {
	if meta_svc.readOnly {
		return nil
	}
	var err error

	stopFunc := meta_svc.utils.StartDiagStopwatch(fmt.Sprintf("Del(%v)", key), base.DiagInternalThreshold)
	defer stopFunc()

	metakvOpDelFunc := func() error {
		err = metakv.Delete(getPathFromKey(key), rev)
		if err == metakv.ErrRevMismatch {
			err = service_def.ErrorRevisionMismatch
			return nil
		} else if err == nil {
			return nil
		} else {
			meta_svc.logger.Warnf("metakv.Delete failed. key=%v, rev=%v, err=%v\n", key, rev, err)
			return err
		}
	}

	expOpErr := meta_svc.utils.ExponentialBackoffExecutor("metakv_svc_delOp", base.RetryIntervalMetakv, base.MaxNumOfMetakvRetries,
		base.MetaKvBackoffFactor, metakvOpDelFunc)

	if expOpErr != nil {
		// Executor will return error only if it timed out with ErrorFailedAfterRetry. Log it and override the ret err
		meta_svc.logger.Errorf("metakv.Delete failed. key=%v, rev=%v, err=%v\n", key, rev, err)
		err = expOpErr
	}
	return err
}

func (meta_svc *MetaKVMetadataSvc) DelWithCatalog(catalogKey, key string, rev interface{}) error {
	if meta_svc.readOnly {
		return nil
	}
	// ignore catalogKey
	return meta_svc.Del(key, rev)
}

//Wrap metakv.RecursiveDelete with retries
//if metakv operation failed after max number of retries, return ErrorFailedAfterRetry
func (meta_svc *MetaKVMetadataSvc) DelAllFromCatalog(catalogKey string) error {
	if meta_svc.readOnly {
		return nil
	}
	stopFunc := meta_svc.utils.StartDiagStopwatch(fmt.Sprintf("DelAllFromCatalog(%v)", catalogKey), base.DiagInternalThreshold)
	defer stopFunc()

	metakvOpDelRFunc := func() error {
		err := metakv.RecursiveDelete(GetCatalogPathFromCatalogKey(catalogKey))
		if err == nil {
			return err
		} else {
			meta_svc.logger.Warnf("metakv.RecursiveDelete failed. catalogKey=%v, err=%v\n", catalogKey, err)
			return err
		}
	}

	expOpErr := meta_svc.utils.ExponentialBackoffExecutor("metakv_svc_delROp", base.RetryIntervalMetakv, base.MaxNumOfMetakvRetries,
		base.MetaKvBackoffFactor, metakvOpDelRFunc)

	if expOpErr != nil {
		// Executor will return error only if it timed out with ErrorFailedAfterRetry. Log it and override the ret err
		meta_svc.logger.Errorf("metakv.RecursiveDelete failed after max retry. catalogKey=%v\n", catalogKey)
	}
	return expOpErr
}

//Wrap metakv.ListAllChildren with retries
//if metakv operation failed after max number of retries, return ErrorFailedAfterRetry
func (meta_svc *MetaKVMetadataSvc) GetAllMetadataFromCatalog(catalogKey string) ([]*service_def.MetadataEntry, error) {
	var entries = make([]*service_def.MetadataEntry, 0)

	stopFunc := meta_svc.utils.StartDiagStopwatch(fmt.Sprintf("GetAllMetadataFromCatalog(%v)", catalogKey), base.DiagInternalThreshold)
	defer stopFunc()

	metakvOpGetAllMetadataFunc := func() error {
		kvEntries, err := metakv.ListAllChildren(GetCatalogPathFromCatalogKey(catalogKey))
		if err == nil {
			for _, kvEntry := range kvEntries {
				entries = append(entries, &service_def.MetadataEntry{GetKeyFromPath(kvEntry.Path), kvEntry.Value, kvEntry.Rev})
			}
			return err
		} else {
			meta_svc.logger.Warnf("metakv.ListAllChildren failed. path=%v, err=%v\n", GetCatalogPathFromCatalogKey(catalogKey), err)
			return err
		}
	}

	expOpErr := meta_svc.utils.ExponentialBackoffExecutor("metakv_svc_getAllMetaOp", base.RetryIntervalMetakv, base.MaxNumOfMetakvRetries,
		base.MetaKvBackoffFactor, metakvOpGetAllMetadataFunc)

	if expOpErr != nil {
		// Executor will return error only if it timed out with ErrorFailedAfterRetry. Log it and override the ret err
		meta_svc.logger.Errorf("metakv.ListAllChildren failed after max retry. path=%v\n", GetCatalogPathFromCatalogKey(catalogKey))
	}
	return entries, expOpErr
}

// get all keys from a catalog
func (meta_svc *MetaKVMetadataSvc) GetAllKeysFromCatalog(catalogKey string) ([]string, error) {
	keys := make([]string, 0)

	metaEntries, err := meta_svc.GetAllMetadataFromCatalog(catalogKey)
	if err != nil {
		return nil, err
	}
	for _, metaEntry := range metaEntries {
		keys = append(keys, metaEntry.Key)
	}
	return keys, nil
}

// metakv requires that all paths start with "/"
func getPathFromKey(key string) string {
	return base.KeyPartsDelimiter + key
}

// the following are exposed since they are needed by metakv call back function

// metakv requires that all parent paths start and end with "/"
func GetCatalogPathFromCatalogKey(catalogKey string) string {
	return base.KeyPartsDelimiter + catalogKey + base.KeyPartsDelimiter
}

func GetKeyFromPath(path string) string {
	if strings.HasPrefix(path, base.KeyPartsDelimiter) {
		return path[len(base.KeyPartsDelimiter):]
	} else {
		// should never get here
		panic(fmt.Sprintf("path=%v doesn't start with '/'", path))
	}
}
