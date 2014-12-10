// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

// metadata service implementation leveraging gometa
package service_impl

import (
	"net/rpc"
	"errors"
	"fmt"
	"encoding/json"
	"github.com/couchbase/gometa/server"
	"github.com/couchbase/gometa/common"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/utils"
)

var goMetadataServiceMethod = "RequestReceiver.NewRequest"

type MetadataSvc struct {
	hostAddr    string // host addr
	client      *rpc.Client  // client for gometa service
	logger      *log.CommonLogger
}

// for testing only
func DefaultMetadataSvc() (*MetadataSvc, error) {
	return NewMetadataSvc(utils.GetHostAddr(base.LocalHostName, base.GometaRequestPortNumber), nil)
}

func NewMetadataSvc(hostAddr string, logger_ctx *log.LoggerContext) (*MetadataSvc, error) {
	meta_svc := &MetadataSvc{
					hostAddr:  hostAddr,  
					logger:    log.NewLogger("MetadataService", logger_ctx),
					}
		
	client, err := rpc.DialHTTP("tcp", meta_svc.hostAddr)
	if err == nil {
		meta_svc.client = client
		meta_svc.logger.Infof("Metdata service started with host=%v\n", meta_svc.hostAddr)
		return meta_svc, nil
	} else {
		return nil, err
	}
}

func (meta_svc *MetadataSvc) Get(key string) ([] byte, error) {
	opCode := common.GetOpCodeStr(common.OPCODE_GET)
	return meta_svc.sendRequest(opCode, key, nil)
}

func (meta_svc *MetadataSvc) Add(key string, value []byte) error {
	opCode := common.GetOpCodeStr(common.OPCODE_ADD)
	_, err := meta_svc.sendRequest(opCode, key, value)
	return err
}

func (meta_svc *MetadataSvc) AddWithCatalog(catalogKey, key string, value []byte) error {
	// first add the key
 	err := meta_svc.Add(key, value)
	if err != nil {
		return err
	}
	
	// then add key to catalog
	return meta_svc.AddKeyToCatalog(catalogKey, key)
}

func (meta_svc *MetadataSvc) Set(key string, value []byte) error {
	opCode := common.GetOpCodeStr(common.OPCODE_SET)
	_, err := meta_svc.sendRequest(opCode, key, value)
	return err
}

func (meta_svc *MetadataSvc) Del(key string) error {
	opCode := common.GetOpCodeStr(common.OPCODE_DELETE)
	_, err := meta_svc.sendRequest(opCode, key, nil)
	return err
}

func (meta_svc *MetadataSvc) DelWithCatalog(catalogKey, key string) error {
	// first remove key from catalog
	err := meta_svc.RemoveKeyFromCatalog(catalogKey, key)
	if err != nil {
		return err
	}
	
	// then delete the key
 	return meta_svc.Del(key)
}

func (meta_svc *MetadataSvc) sendRequest(opCode, key string, value []byte) ([]byte, error) {
	request := &server.Request{OpCode: opCode, Key: key, Value: value}
	var reply *server.Reply
	err := meta_svc.client.Call(goMetadataServiceMethod, request, &reply)
	if reply == nil {
		return nil, err
	} else {
		return reply.Result, err
	}
}

// add a key to a catalog 
func (meta_svc *MetadataSvc) AddKeyToCatalog(catalogKey, key string) error {
	var catalog []string

	result, err := meta_svc.Get(catalogKey)
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
	return meta_svc.Set(catalogKey, catalogBytes)
}

// remove a key from a catalog 
func (meta_svc *MetadataSvc) RemoveKeyFromCatalog(catalogKey, key string) error {
	var catalog []string

	result, err := meta_svc.Get(catalogKey)
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
	return meta_svc.Set(catalogKey, catalogBytes)
}

// get all keys from a catalog 
func (meta_svc *MetadataSvc) GetKeysFromCatalog(catalogKey string) ([]string, error) {
	var catalog []string

	result, err := meta_svc.Get(catalogKey)
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

// utility methods for starting and killing the gometa service which the metadata service depends on. 
// mostly for testing

/* no longer needed. Remove after things stablize

// start the gometa service
func StartGometaService() (*exec.Cmd, error) {
	fmt.Println("starting gometa service. this will take a couple seconds")
	goPaths := os.Getenv("GOPATH")

	goPathArr := strings.Split(goPaths, ":")
	
	// iterate through all defined gopath till we find the source path for gometa and goxdcr 
	var gometaDir string
	var goxdcrDir string
	for _, goPath := range goPathArr {
 		gometaDirCur := goPath + "/src/github.com/couchbase/gometa"
 		goxdcrDirCur := goPath + "/src/github.com/couchbase/goxdcr"
 		
 		command := exec.Command("/bin/bash", "-c", "test -d " + gometaDirCur)
 		err := command.Run()
		if err == nil {
			gometaDir = gometaDirCur
		} 
		
		command = exec.Command("/bin/bash", "-c", "test -d " + goxdcrDirCur)
 		err = command.Run()
		if err == nil {
			goxdcrDir = goxdcrDirCur
		}
		
		if gometaDir != "" && goxdcrDir != "" {
			break
		}
	}
	
	if gometaDir == "" || goxdcrDir == "" {
 		return nil, errors.New(fmt.Sprintf("Cannot find gometa or goxdcr in source path, %v\n", goPaths))	
	}
		
	// build gometa executable
	objPath := goxdcrDir + "/../../../../bin/gometa"
	gometaSrcPath := gometaDir + "/cmd/gometa/*.go"
	command := exec.Command("/bin/bash", "-c", "go build -o " + objPath + " " + gometaSrcPath)
	err := command.Run()
	if err != nil {
		fmt.Printf("Error executing command line - %v\n", command.Args)
		return nil, err
	}
		
	// run gometa executable to start server
	command = exec.Command("/bin/bash", "-c", objPath + " -config " + goxdcrDir + "/service_impl/metadata_svc_config")
    command.Stdin = os.Stdout   
	err = command.Start()
	if err != nil {
		fmt.Printf("Error executing gometa command err=%v\n", err)
		return nil, err
	}
	fmt.Printf("output=%v\n", outputs)
	
	//wait for gometa service to finish starting
	time.Sleep(time.Second * 3)
		
	fmt.Println("started gometa service.")
	return command, nil
}

// kill the gometa service
func KillGometaService(cmd *exec.Cmd) {
	if err := cmd.Process.Kill(); err != nil {
		fmt.Println("failed to kill gometa service. Please kill it manually")
	} else {
		fmt.Println("killed gometa service successfully")
	}
} */
