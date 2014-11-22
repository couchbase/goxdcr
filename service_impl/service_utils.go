package service_impl

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/service_def"
)

// add a key to a catalog 
func AddKeyToCatalog(key, catalogKey string, metadata_svc service_def.MetadataSvc) error {
	var catalog []string

	result, err := metadata_svc.Get(catalogKey)
	if err != nil {
		// if catalog does not exist, create a new catalog
		catalog = make([]string, 0)
		
	} else {
		// unmarshal catalog 
		err = json.Unmarshal(result, &catalog) 
		if err != nil {
			return err
		}
	}
	
	// add key to catalog
	catalog = append(catalog, key)
	
	catalogBytes, err := json.Marshal(catalog)
	if err != nil {
		return err
	}
	// update/insert catalog
	return metadata_svc.Set(catalogKey, catalogBytes)
}

// remove a key from a catalog 
func RemoveKeyFromCatalog(key, catalogKey string, metadata_svc service_def.MetadataSvc) error {
	var catalog []string

	result, err := metadata_svc.Get(catalogKey)
	if err != nil {
		return errors.New(fmt.Sprintf("Error removing key %v from catalog %v since catalog does not exist\n", key, catalogKey))
	} 
	
	// unmarshal catalog 
	err = json.Unmarshal(result, &catalog) 
	if err != nil {
		return err
	}
	
	newCatalog := make([]string, 0)
	for _, oldKey := range catalog {
		if oldKey != key {
			newCatalog = append(newCatalog, oldKey)
		}
	}
	
	catalogBytes, err := json.Marshal(newCatalog)
	if err != nil {
		return err
	}
	// update catalog
	return metadata_svc.Set(catalogKey, catalogBytes)
}

// get all keys from a catalog 
func GetKeysFromCatalog(catalogKey string, metadata_svc service_def.MetadataSvc) ([]string, error) {
	var catalog []string

	result, err := metadata_svc.Get(catalogKey)
	if err != nil {
		// no catalog is ok
		return nil, nil
	} 
	
	// unmarshal catalog 
	err = json.Unmarshal(result, &catalog) 
	if err != nil {
		return nil, err
	}
	
	return catalog, nil
}

