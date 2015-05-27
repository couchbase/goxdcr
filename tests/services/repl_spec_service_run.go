// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

// Test for metadata service
package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/couchbase/goxdcr/service_impl"
	"github.com/couchbase/goxdcr/metadata_svc"
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
	metadataSvc, err := metadata_svc.NewMetaKVMetadataSvc(nil)
	if err != nil {
		return errors.New(fmt.Sprintf("Error starting metadata service. err=%v\n", err.Error()))
	}

	cluster_info_svc := service_impl.NewClusterInfoSvc(nil)
	top_svc, err := service_impl.NewXDCRTopologySvc(9000, 13000, 11977, false, cluster_info_svc, nil)
	if err != nil {
		return err
	}

	remote_cluster_svc, err := metadata_svc.NewRemoteClusterService(nil, metadataSvc, top_svc, cluster_info_svc, nil)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	service, err := metadata_svc.NewReplicationSpecService(nil, remote_cluster_svc, metadataSvc, top_svc, cluster_info_svc, nil)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	// create a test replication spec
	spec, err := service.ConstructNewReplicationSpec (sourceBucketName, targetClusterUUID, targetBucketName)
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
