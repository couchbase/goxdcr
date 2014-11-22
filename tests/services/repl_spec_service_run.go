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
	"flag"
	"fmt"
	"os"
	"errors"
	metadata "github.com/couchbase/goxdcr/metadata"
	s "github.com/couchbase/goxdcr/service_impl"
)

// test parameters
const(
	 gometaExecutableName = "gometa"
	
	 sourceClusterUUID = "localhost:9000"
	 sourceBucketName = "default"
	 targetClusterUUID = "remote:9000"
	 targetBucketName = "target" 
	 filterName = "myActive"
	 newBatchCount = 345
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	fmt.Println("Start Testing metadata service ...")
	
	cmd, err := s.StartGometaService()
	if err != nil {
		fmt.Println("Test failed. err: ", err)
		return
	}
	defer s.KillGometaService(cmd)
	
	// start and test ReplicationSpec service
	err = startReplicationSpecService()
	if err != nil {
		fmt.Println("Test failed. err: ", err)
	} else {
		fmt.Println("Test passed.") 
	}
}

func startReplicationSpecService() error {
	metadataSvc, err := s.DefaultMetadataSvc()
	if err != nil {
		return errors.New(fmt.Sprintf("Error starting metadata service. err=%v\n", err.Error()))
	}
	
	service := s.NewReplicationSpecService(metadataSvc, nil)
		
	// create a test replication spec
	spec := metadata.NewReplicationSpecification(sourceClusterUUID, sourceBucketName, 
			targetClusterUUID, targetBucketName, filterName)
			
	specId := spec.Id
	
	// cleanup to ensure that there is no residue replication spec, e.g., from previously failed test runs
	err = service.DelReplicationSpec(specId)
	if err != nil {
		return err
	}
	
	// lookup non-existent key should fail
	spec2, err := service.ReplicationSpec(specId)
	if err == nil {
		return errors.New("Should have errored out looking up non-existent key")
	}
	
	// delete non-existent key is ok
	err = service.DelReplicationSpec(specId)
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
	if spec2.Id != specId || spec2.SourceClusterUUID != spec.SourceClusterUUID || spec2.Settings.BatchCount != spec.Settings.BatchCount {
		fmt.Println("params of spec retrieved: id=", spec2.Id, "; source cluster=", spec2.SourceClusterUUID, ";  batch count=", spec2.Settings.BatchCount)
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
	if  spec2.Settings.BatchCount != newBatchCount {
		return errors.New("Update to replication spec is lost")
	}
	
	err = service.DelReplicationSpec(specId)
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
