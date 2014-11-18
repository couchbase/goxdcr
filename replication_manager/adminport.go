// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

// replication manager's adminport.

package replication_manager

import (
	"encoding/json"
	"github.com/couchbase/goxdcr/base"
	"net/http"
	"strings"
	"bytes"
	"time"
	"errors"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/gen_server"
	ap "github.com/couchbase/goxdcr/adminport"
	utils "github.com/couchbase/goxdcr/utils"
)

var StaticPaths = [3]string{CreateReplicationPath, SettingsReplicationsPath, StatisticsPath}
var DynamicPathPrefixes = [4]string{DeleteReplicationPrefix, PauseReplicationPrefix, ResumeReplicationPrefix, SettingsReplicationsPath}

var MaxForwardingRetry = 5
var ForwardingRetryInterval = time.Second * 10

var logger_ap *log.CommonLogger = log.NewLogger("AdminPort", log.DefaultLoggerContext)

/************************************
/* struct Adminport
*************************************/
type Adminport struct {
	sourceKVHost  string
	gen_server.GenServer
	finch         chan bool
}

func NewAdminport(laddr string, finch chan bool) *Adminport {

	//callback functions from GenServer
	var msg_callback_func gen_server.Msg_Callback_Func
	var exit_callback_func gen_server.Exit_Callback_Func
	var error_handler_func gen_server.Error_Handler_Func

	server := gen_server.NewGenServer(&msg_callback_func,
		&exit_callback_func, &error_handler_func, log.DefaultLoggerContext, "Adminport")

	adminport := &Adminport{
		sourceKVHost:  laddr,
		GenServer:     server,           /*gen_server.GenServer*/
		finch:         finch, 
	}

	msg_callback_func = adminport.processRequest
	exit_callback_func = nil
	error_handler_func = nil
	
	logger_ap.Infof("Constructed adminport\n")

	return adminport

}

// admin-port entry point
func (adminport *Adminport) Start() {
	// start adminport gen_server
	adminport.Start_server()
	
	// start http server
	reqch := make(chan ap.Request)
	hostAddr :=  utils.GetHostAddr(adminport.sourceKVHost, base.AdminportNumber)
	server := ap.NewHTTPServer("xdcr", hostAddr, base.AdminportUrlPrefix, reqch, new(ap.Handler))

	server.Start()
	logger_ap.Infof("server started %v !\n", hostAddr)

	finch := adminport.finch
		count :=0
loop:
	for {
	count++
		select {
		case <-finch:
			break loop
		case req, ok := <-reqch: // admin requests are serialized here
			if ok == false {
				break loop
			}
			// forward message to adminport server for processing 
			adminport.SendMsg_async([]interface{}{req})
		default:
		}
	}
	
	logger_ap.Infof("adminport exited !\n")
	server.Stop()
	adminport.Stop_server()
	
}

// needed by Supervisor interface
func (adminport *Adminport) Id() string {
	return base.AdminportSupervisorId
}

func (adminport *Adminport) processRequest(msg []interface{}) error {
	// msg should consists of a single Request
	if len(msg) != 1 {
		return errors.New("Failed to decode message")
	} 
	
	req := msg[0].(ap.Request)
	httpReq := req.GetHttpRequest()
	if response, err := adminport.handleRequest(httpReq); err == nil {
		req.Send(response)
	} else {
		req.SendError(err)
	}
	return nil
}

func (adminport *Adminport) handleRequest(
	request *http.Request) (response []byte , err error) {
	
	logger_ap.Infof("handleRequest called\n")
	// TODO change to debug
	logger_ap.Infof("Request: %v \n", request)

	key, err := adminport.GetMessageKeyFromRequest(request)
	if err != nil {
		return nil, err
	}
	
	switch (key) {
	case CreateReplicationPath + base.UrlDelimiter + MethodPost:
		response, err = adminport.doCreateReplicationRequest(request)
	case DeleteReplicationPrefix + DynamicSuffix + base.UrlDelimiter + MethodDelete:
		fallthrough
	// historically, deleteReplication could use Post method	
	case DeleteReplicationPrefix + DynamicSuffix + base.UrlDelimiter + MethodPost:
		response, err = adminport.doDeleteReplicationRequest(request)
	case PauseReplicationPrefix + DynamicSuffix + base.UrlDelimiter + MethodPost:
		response, err = adminport.doPauseReplicationRequest(request)
	case ResumeReplicationPrefix + DynamicSuffix + base.UrlDelimiter + MethodPost:
		response, err = adminport.doResumeReplicationRequest(request)
	case SettingsReplicationsPath + DynamicSuffix + base.UrlDelimiter + MethodGet:
		response, err = adminport.doViewReplicationSettingsRequest(request)
	case SettingsReplicationsPath + DynamicSuffix + base.UrlDelimiter + MethodPost:
		response, err = adminport.doChangeReplicationSettingsRequest(request)
	case StatisticsPath + base.UrlDelimiter + MethodGet:
		response, err = adminport.doGetStatisticsRequest(request)
	default:
		err = ap.ErrorInvalidRequest
	}
	return response, err
}

func (adminport *Adminport) doCreateReplicationRequest(request *http.Request) ([]byte, error) {
	logger_ap.Infof("doCreateReplicationRequest called\n")
	
	fromBucket, toClusterUuid, toBucket, filterName, forward, settings, err := DecodeCreateReplicationRequest(request)
	if err != nil {
		return nil, err
	}
	
	fromClusterUuid, err := XDCRCompTopologyService().MyCluster()
	if err != nil {
		return nil, err
	}
	
	logger_ap.Debugf("fromClusterUuid=%v \n", fromClusterUuid)
	
	// apply default replication settings
	if err := ApplyDefaultSettings(&settings); err != nil {
		return nil, err
	}

	replicationId, err := CreateReplication(fromClusterUuid, fromBucket, toClusterUuid, toBucket, filterName, settings, forward)
	
	if err != nil {
		return nil, err
	} else {
		if forward {	
		// forward replication request to other KV nodes involved if necessary
		adminport.forwardReplicationRequest(request)	
		}
		
		return NewCreateReplicationResponse(replicationId), nil
	}
}

func (adminport *Adminport) doDeleteReplicationRequest(request *http.Request) ([]byte, error) {
	logger_ap.Infof("doDeleteReplicationRequest\n")

	replicationId, forward, err := DecodeReplicationIdAndForwardFlagFromHttpRequest(request, DeleteReplicationPrefix)
	if err != nil {
		return nil, err
	}
	
	logger_ap.Debugf("Request params: replicationId=%v\n", replicationId)
	
	err = DeleteReplication(replicationId, forward)
	
	if err != nil {
		return nil, err
	} else {
		if forward {		
			// forward replication request to other KV nodes involved 
			adminport.forwardReplicationRequest(request)
		}
		// no response body in success case
		return nil, nil
	}
}

func (adminport *Adminport) doPauseReplicationRequest(request *http.Request) ([]byte, error) {
	logger_ap.Infof("doPauseReplicationRequest\n")

	replicationId, forward, err := DecodeReplicationIdAndForwardFlagFromHttpRequest(request, PauseReplicationPrefix)
	if err != nil {
		return nil, err
	}
	
	logger_ap.Debugf("Request params: replicationId=%v\n", replicationId)
	
	err = PauseReplication(replicationId, forward, false/*sync*/)
	
	if err != nil {
		return nil, err
	} else {
		if forward {		
			// forward replication request to other KV nodes involved 
			adminport.forwardReplicationRequest(request)
		}
		// no response body in success case
		return nil, nil
	}
}

func (adminport *Adminport) doResumeReplicationRequest(request *http.Request) ([]byte, error) {
	logger_ap.Infof("doResumeReplicationRequest\n")

	replicationId, forward, err := DecodeReplicationIdAndForwardFlagFromHttpRequest(request, ResumeReplicationPrefix)
	if err != nil {
		return nil, err
	}
	
	logger_ap.Debugf("Request params: replicationId=%v\n", replicationId)
	
	err = ResumeReplication(replicationId, forward, false/*sync*/)
	
	if err != nil {
		return nil, err
	} else {
		if forward {		
			// forward replication request to other KV nodes involved 
			adminport.forwardReplicationRequest(request)
		}
		// no response body in success case
		return nil, nil
	}
}

func (adminport *Adminport) doViewReplicationSettingsRequest(request *http.Request) ([]byte, error) {
	logger_ap.Infof("doViewReplicationSettingsRequest\n")

	// get input parameters from request
	replicationId, err:= DecodeReplicationIdFromHttpRequest(request, SettingsReplicationsPath)
	if err != nil {
		return nil, err
	}
	
	logger_ap.Debugf("Request decoded: replicationId=%v", replicationId)
	
	// read replication spec with the specified replication id
	replSpec, err := MetadataService().ReplicationSpec(replicationId)
	if err != nil {
		return nil, err
	}
	
	// marshal replication settings in replication spec and return it
	return NewViewReplicationSettingsResponse(replSpec.Settings)
}

func (adminport *Adminport) doChangeReplicationSettingsRequest(request *http.Request) ([]byte, error) {
	logger_ap.Infof("doChangeReplicationSettingsRequest\n")
	
	// get input parameters from request
	replicationId, err:= DecodeReplicationIdFromHttpRequest(request, SettingsReplicationsPath)
	if err != nil {
		return nil, err
	}
	inputSettingsMap, err := DecodeSettingsFromRequest(request, true)
	if err != nil {
		return nil, err
	}
	
	logger_ap.Debugf("Request decoded: replicationId=%v; inputSettings=%v", replicationId, inputSettingsMap)
	
	err = HandleChangesToReplicationSettings(replicationId, inputSettingsMap)
	
	return nil, err
}

// get statistics for all running replications
func (adminport *Adminport) doGetStatisticsRequest(request *http.Request) ([]byte, error) {
	logger_ap.Infof("doGetStatisticsRequest\n")

	statsMap, err := GetStatistics()
	if err == nil {
		return json.Marshal(statsMap)
	} else {
		return nil, err
	}
}

// forward requests to other nodes.
func (adminport *Adminport) forwardReplicationRequest(request *http.Request) error {
	logger_ap.Infof("forwardReplicationRequest\n")
	
	myAddr, err := XDCRCompTopologyService().MyHost()
	if err != nil {
		return err
	}

	xdcrNodesMap, err := XDCRCompTopologyService().XDCRTopology()
	if err != nil {
		return err
	}
	
	if len(xdcrNodesMap) > 1 {
		if err = request.ParseForm(); err != nil {
			return err
		}
		
		// set "Forward" flag to false in the forwarded request
		var paramMap = make(map[string]interface{}, 0)
		for key, valArr := range request.Form {
			if len(valArr) > 0 {
				paramMap[key] = valArr[0]
	    	}
		}
		paramMap[Forward] = "false" 
		// this Encode op should never fail since paramMap is fully under control
		newBody, _ := EncodeMapIntoByteArray(paramMap)
	
		for xdcrNode, port := range xdcrNodesMap {
			// do not forward to current node 
			if xdcrNode != myAddr {
				go forwardReplicationRequestToXDCRNode(request.URL.String(), newBody, xdcrNode, int(port))
			}
		}
	}
	return nil
}

func forwardReplicationRequestToXDCRNode(oldRequestUrl string, newRequestBody []byte, xdcrAddr string, port int) (*http.Response, error) {
	logger_ap.Infof("forwardReplicationRequestToXDCRNode. oldRequestUrl=%v, newRequestBody=%v, xdcrAddr=%v, port=%v\n", 
	                oldRequestUrl, string(newRequestBody), xdcrAddr, port)

	newUrl := "http://" + utils.GetHostAddr(xdcrAddr, port) + oldRequestUrl
	newRequest, err := http.NewRequest(MethodPost, newUrl, bytes.NewBuffer(newRequestBody))
	if err != nil {
		return nil, err
	}
	newRequest.Header.Set(ContentType, DefaultContentType)
   
   	retryInterval := ForwardingRetryInterval
    for i := 0; i <= MaxForwardingRetry; i++ {
    	response, err := http.DefaultClient.Do(newRequest)
    	logger_ap.Infof("forwarding request=%v for the %vth time\n", newRequest, i + 1)
    	if err == nil && response.StatusCode == 200 {
    		logger_ap.Infof("forwarding request succeeded")
			return response, err
    	}
    	// if did not succeed, wait and try again
    	if i < MaxForwardingRetry {
    		time.Sleep(retryInterval)
    		retryInterval *= 2
    	}
    }

	// give up after max retry. the target node is likely dead. hopefully it will
	// get restarted and the required action, e.g., create/resumeReplication, will get performed then
   	logger_ap.Errorf("Error forwarding request after max retry")
   	return nil, errors.New("Error forwarding request after max retry")
}

// Get the message key from http request
func (adminport *Adminport) GetMessageKeyFromRequest(r *http.Request) (string, error) {
	var key string
	// remove adminport url prefix from path
	path := r.URL.Path[len(base.AdminportUrlPrefix):]
	// remove trailing "/" in path if it exists
	if strings.HasSuffix(path, base.UrlDelimiter) {
		path = path[:len(path)-1]
	}
	
	for _, staticPath := range StaticPaths {
		if path == staticPath {
			// if path in url is a static path, use it as name
			key = path
			break
		}
	}

	if len(key) == 0 {
		// if path does not match any static paths, check if it has a prefix that matches dynamic path prefixes
		for _, dynPathPrefix := range DynamicPathPrefixes {
			if strings.HasPrefix(path, dynPathPrefix) {
				key = dynPathPrefix + DynamicSuffix
				break
			}
		}
	}

	if len(key) == 0 {
		return "", utils.InvalidPathInHttpRequestError(r.URL.Path)
	} else {
		// add http method suffix to name to ensure uniqueness
		key += base.UrlDelimiter + strings.ToUpper(r.Method)

		//todo change to debug
		logger_ap.Infof("Request key decoded: %v\n", key)

		return key, nil
	}
}

// apply default replication settings for the ones that are not explicitly specified
func ApplyDefaultSettings(settings *map[string]interface{}) error {
	defaultSettings, err := ReplicationSettingsService().GetReplicationSettings()
	if err != nil {
		return err
	}
	
	for key, val := range defaultSettings.ToMap() {
		if _, ok := (*settings)[key]; !ok {
			(*settings)[key] = val
		}
	}
	return nil
}
