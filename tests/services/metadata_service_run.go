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
	
	go startGometaService()
	//wait for gometa service to finish starting
	time.Sleep(time.Second * 3)
	
	err := startMetadataService()
	if err != nil {
		fmt.Println("Test failed. err: ", err)
	} else {
		fmt.Println("Test passed.") 
	}
}

// start the gometa service, which the metadata service depends on
func startGometaService() {
	gopath := os.Getenv("GOPATH")
	/* auto-building gometa executable is not yet working
	//opath := gopath + "/bin/" + gometaExecutableName
	opath := "/Users/yu/goprojects/bin/gometa"
	//spath := gopath + "/src/github.com/couchbase/gometa/main/*.go"
	spath := "/Users/yu/goprojects/src/github.com/couchbase/gometa/main/*.go"
	// build gometa executable
	err := exec.Command("/usr/local/go/bin/go", "build",  "-o", opath, spath).Run()
	if err != nil {
		fmt.Println("build failed. err: ", err)
		return
	}*/
	// run gometa executable to start server
	exec.Command(gopath + "/bin/gometa", "-config", gopath + "/src/github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/tests/services/config").Run()
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
