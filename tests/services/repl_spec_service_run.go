// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

// Test for metadata service
package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/couchbase/goxdcr/v8/metadata_svc"
	"github.com/couchbase/goxdcr/v8/service_impl"
	utilities "github.com/couchbase/goxdcr/v8/utils"
	"os"
)

// test parameters
const (
	gometaExecutableName = "gometa"

	sourceClusterUUID = "localhost:9000"
	sourceBucketName  = "default"
	targetClusterUUID = "testuuid"
	targetBucketName  = "target"
	newBatchCount     = 345
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	fmt.Println("Start Testing repl spec service ...")

	// start and test ReplicationSpec service
	err := startReplicationSpecService()
	if err != nil {
		fmt.Println("Test failed. err: ", err)
	} else {
		fmt.Println("Test passed.")
	}
}

func startReplicationSpecService() error {
	utils := utilities.NewUtilities()
	metadataSvc, err := metadata_svc.NewMetaKVMetadataSvc(nil)
	if err != nil {
		return errors.New(fmt.Sprintf("Error starting metadata service. err=%v\n", err.Error()))
	}

	cluster_info_svc := service_impl.NewClusterInfoSvc(nil)
	top_svc, err := service_impl.NewXDCRTopologySvc(9000, 13000, 11977, false, cluster_info_svc, nil)
	if err != nil {
		return err
	}

	remote_cluster_svc, err := metadata_svc.NewRemoteClusterService(nil, metadataSvc, top_svc, cluster_info_svc, nil, utils)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	service, err := metadata_svc.NewReplicationSpecService(nil, remote_cluster_svc, metadataSvc, top_svc, cluster_info_svc, nil, nil, nil)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	// create a test replication spec
	spec, err := service.ConstructNewReplicationSpec(sourceBucketName, targetClusterUUID, targetBucketName)
	if err != nil {
		return err
	}

	specId := spec.Id

	// cleanup to ensure that there is no residue replication spec, e.g., from previously failed test runs
	_, err = service.DelReplicationSpec(specId)
	if err != nil {
		return err
	}

	// lookup non-existent key should fail
	spec2, err := service.ReplicationSpec(specId)
	if err == nil {
		return errors.New("Should have errored out looking up non-existent key")
	}

	// delete non-existent key is ok
	_, err = service.DelReplicationSpec(specId)
	if err != nil {
		return err
	}

	err = service.AddReplicationSpec(spec)
	if err != nil {
		return err
	}

	spec2, err = service.ReplicationSpec(specId)
	if err != nil {
		return err
	}
	if spec2.Id != specId || spec2.TargetClusterUUID != spec.TargetClusterUUID || spec2.Settings.BatchCount != spec.Settings.BatchCount {
		fmt.Println("params of spec retrieved: id=", spec2.Id, "; target cluster=", spec2.TargetClusterUUID, ";  batch count=", spec2.Settings.BatchCount)
		return errors.New("Read incorrect values of replication spec.")
	}

	// update spec
	spec.Settings.BatchCount = newBatchCount

	err = service.SetReplicationSpec(spec)
	if err != nil {
		return err
	}

	// verify update
	spec2, err = service.ReplicationSpec(specId)
	if err != nil {
		return err
	}
	if spec2.Settings.BatchCount != newBatchCount {
		return errors.New("Update to replication spec is lost")
	}

	_, err = service.DelReplicationSpec(specId)
	if err != nil {
		return err
	}

	// verify replication spec no longer exists after delete
	spec, err = service.ReplicationSpec(specId)
	if err == nil {
		return errors.New("Should have errored out looking up non-existent key")
	}

	return nil

}
