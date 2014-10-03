// Test for metadata service
package main

import (
	"flag"
	"fmt"
	"os"
	"errors"
	"os/exec"
	"time"
	metadata "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/metadata"
	s "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/services"
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

var options struct {
	hostAddr          string // host addr
}

func argParse() {
    // host address has to be consistent with that in the config file. There is no need or use to make it configurable.
	options.hostAddr = "localhost:5003"
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	fmt.Println("Start Testing metadata service ...")
	argParse()
	fmt.Printf("host addr=%s\n", options.hostAddr)
	fmt.Println("Done with parsing the arguments")
	
	cmd, err := startGometaService()
	if err != nil {
		fmt.Println("Test failed. err: ", err)
		return
	}
	err = cmd.Start()
	if err != nil {
		fmt.Println("Test failed. err: ", err)
		return
	}
	fmt.Println("started gometa service.")
	
	//wait for gometa service to finish starting
	time.Sleep(time.Second * 3)
	
	// start and test metadata service
	err = startMetadataService()
	if err != nil {
		fmt.Println("Test failed. err: ", err)
	} else {
		fmt.Println("Test passed.") 
	}
	
	// kill the gometa service
	if err = cmd.Process.Kill(); err != nil {
		fmt.Println("failed to kill gometa service. Please kill it manually")
	} else {
		fmt.Println("killed gometa service successfully")
	}
}

// start the gometa service, which the metadata service depends on
func startGometaService() (*exec.Cmd, error) {
	goPath := os.Getenv("GOPATH")
	
	
	objPath := goPath + "bin/gometa"
	srcPath := "/Users/yu/goprojects/src/github.com/couchbase/gometa/main/*.go"
	output, err := exec.Command("/bin/bash", "-c", "go build -o " + objPath + " " + srcPath).CombinedOutput()
	if err != nil {
		fmt.Println("Failed to build gometa. output:", string(output))
		return nil, err
	}

	// build command to run gometa executable to start server
	return exec.Command(objPath, "-config", goPath + "/src/github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/tests/services/config"), nil
}

func startMetadataService() error {
	service, err := s.NewMetadataSvc(options.hostAddr, nil)
	if err != nil {
		return errors.New("Error starting metadata service.")
	}
	
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
	
	err = service.AddReplicationSpec(*spec)
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
	
	err = service.SetReplicationSpec(*spec)
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
