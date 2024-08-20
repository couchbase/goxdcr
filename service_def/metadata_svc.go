// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

// wrapper service that retrieves data from gometa service
package service_def

import (
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/v8/base"
	"strings"
)

var MetadataNotFoundErr error = errors.New("key not found")
var ErrorKeyAlreadyExist = errors.New("key being added already exists")
var ErrorRevisionMismatch = errors.New("revision number does not match")
var MetaKVFailedAfterMaxTriesBaseString = "metakv failed for max number of retries"
var MetaKVFailedAfterMaxTries error = fmt.Errorf("%v = %v", MetaKVFailedAfterMaxTriesBaseString, base.MaxNumOfMetakvRetries)
var ErrorNotFound = errors.New("Not found") // corresponds to metakv
var ErrorSourceDefaultCollectionDNE = errors.New("Source bucket's default collection has been removed")

func DelOpConsideredPass(err error) bool {
	if err == ErrorRevisionMismatch || (err != nil && strings.Contains(err.Error(), MetaKVFailedAfterMaxTriesBaseString)) {
		return false
	}
	return true
}

// struct for general metadata entry maintained by metadata service
type MetadataEntry struct {
	Key   string
	Value []byte
	Rev   interface{}
}

type MetadataSvc interface {
	Get(key string) ([]byte, interface{}, error)
	Add(key string, value []byte) error
	AddSensitive(key string, value []byte) error
	Set(key string, value []byte, rev interface{}) error
	SetSensitive(key string, value []byte, rev interface{}) error
	Del(key string, rev interface{}) error

	// catalog related APIs
	AddWithCatalog(catalogKey, key string, value []byte) error
	AddSensitiveWithCatalog(catalogKey, key string, value []byte) error
	DelWithCatalog(catalogKey, key string, rev interface{}) error
	GetAllMetadataFromCatalog(catalogKey string) ([]*MetadataEntry, error)
	GetAllKeysFromCatalog(catalogKey string) ([]string, error)
	DelAllFromCatalog(catalogKey string) error
}
