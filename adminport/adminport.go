// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

// replication manager's adminport.

package adminport

import (
	"encoding/json"
	"github.com/Xiaomei-Zhang/goxdcr/base"
	"net/http"
	"strings"
	"bytes"
	"github.com/Xiaomei-Zhang/goxdcr/log"
	rm "github.com/Xiaomei-Zhang/goxdcr/replication_manager"
	utils "github.com/Xiaomei-Zhang/goxdcr/utils"
)

var StaticPaths = [3]string{CreateReplicationPath, SettingsReplicationsPath, StatisticsPath}
var DynamicPathPrefixes = [4]string{DeleteReplicationPrefix, PauseReplicationPrefix, ResumeReplicationPrefix, SettingsReplicationsPath}

var logger_ap *log.CommonLogger = log.NewLogger("AdminPort", log.DefaultLoggerContext)


type xdcrRestHandler struct {
}

// admin-port entry point
func MainAdminPort(laddr string) {
	var err error

	h := new(xdcrRestHandler)
	reqch := make(chan Request)
	server := NewHTTPServer("xdcr", utils.GetHostAddr(laddr, base.AdminportNumber), base.AdminportUrlPrefix, reqch, new(Handler))

	server.Start()
	logger_ap.Infof("server started %v !\n", laddr)

loop:
	for {
		select {
		case req, ok := <-reqch: // admin requests are serialized here
			if ok == false {
				break loop
			}
			httpReq := req.GetHttpRequest()
			if response, err := h.handleRequest(httpReq); err == nil {
				req.Send(response)
			} else {
				req.SendError(err)
			}
		}
	}
	if err != nil {
		logger_ap.Errorf("%v\n", err)
	}
	logger_ap.Infof("adminport exited !\n")
	server.Stop()
}

func (h *xdcrRestHandler) handleRequest(
	request *http.Request) (response []byte , err error) {
	
	logger_ap.Infof("handleRequest called\n")
	// TODO change to debug
	logger_ap.Infof("Request: %v \n", request)

	key, err := h.GetMessageKeyFromRequest(request)
	if err != nil {
		return nil, err
	}
	
	switch (key) {
	case CreateReplicationPath + base.UrlDelimiter + MethodPost:
		response, err = h.doCreateReplicationRequest(request)
	case DeleteReplicationPrefix + DynamicSuffix + base.UrlDelimiter + MethodDelete:
		fallthrough
	// historically, deleteReplication could use Post method	
	case DeleteReplicationPrefix + DynamicSuffix + base.UrlDelimiter + MethodPost:
		response, err = h.doDeleteReplicationRequest(request)
	case PauseReplicationPrefix + DynamicSuffix + base.UrlDelimiter + MethodPost:
		response, err = h.doPauseReplicationRequest(request)
	case ResumeReplicationPrefix + DynamicSuffix + base.UrlDelimiter + MethodPost:
		response, err = h.doResumeReplicationRequest(request)
	case SettingsReplicationsPath + DynamicSuffix + base.UrlDelimiter + MethodGet:
		response, err = h.doViewReplicationSettingsRequest(request)
	case SettingsReplicationsPath + DynamicSuffix + base.UrlDelimiter + MethodPost:
		response, err = h.doChangeReplicationSettingsRequest(request)
	case StatisticsPath + base.UrlDelimiter + MethodGet:
		response, err = h.doGetStatisticsRequest(request)
	default:
		err = ErrorInvalidRequest
	}
	return response, err
}

func (h *xdcrRestHandler) doCreateReplicationRequest(request *http.Request) ([]byte, error) {
	logger_ap.Infof("doCreateReplicationRequest called\n")

	fromBucket, toClusterUuid, toBucket, filterName, forward, settings, err := DecodeCreateReplicationRequest(request)
	if err != nil {
		return nil, err
	}
	
	logger_ap.Debugf("Request params decoded: fromBucket=%v; toClusterUuid=%v; toBucket=%v; filterName=%v; forward=%v; settings=%v \n", 
					fromBucket, toClusterUuid, toBucket, filterName, forward, settings)
	
	fromClusterUuid, err := rm.XDCRCompTopologyService().MyCluster()
	if err != nil {
		return nil, err
	}
	
	logger_ap.Debugf("fromClusterUuid=%v \n", fromClusterUuid)
	
	// apply default replication settings
	if err := ApplyDefaultSettings(&settings); err != nil {
		return nil, err
	}
	
	replicationId, err := rm.CreateReplication(fromClusterUuid, fromBucket, toClusterUuid, toBucket, filterName, settings)
	if err != nil {
		return nil, err
	}

	// forward replication request to other KV nodes involved if necessary
	if forward {
		forwardedNodesMap, err := h.forwardReplicationRequest(request)
		if err != nil {
			// if some forward request failed, call deleteRelication on all 
			// nodes where the replication has been started
			for nodeAddr, infoArr := range forwardedNodesMap {
				// first element in infoArr is port number
				port := infoArr[1].(uint16)
				// second element in infoArr is a http response
				createReplResponse := infoArr[0].(*http.Response)
				// decode replication id from http response
				replId, err := DecodeCreateReplicationResponse(createReplResponse)
				if err == nil {
					// create a deleteReplication request for each node involved
					deleteRequest, err := NewDeleteReplicationRequest(replId, nodeAddr, int(port))
					if err == nil {
						http.DefaultClient.Do(deleteRequest)
					}
				} else {
					// not much we can do when we cannot get replId. 
					logger_ap.Errorf("Error decoding CreateReplicationResponse from node %v\n", nodeAddr)
				}
			}
			// call deleteReplication on current node
			rm.DeleteReplication(replicationId)

			return nil, err
		}
	}

	return NewCreateReplicationResponse(replicationId), nil
}

func (h *xdcrRestHandler) doDeleteReplicationRequest(request *http.Request) ([]byte, error) {
	logger_ap.Infof("doDeleteReplicationRequest\n")

	replicationId, forward, err := DecodeReplicationIdAndForwardFlagFromHttpRequest(request, DeleteReplicationPrefix)
	if err != nil {
		return nil, err
	}
	
	logger_ap.Debugf("Request params: replicationId=%v\n", replicationId)
	
	if err = rm.DeleteReplication(replicationId); err != nil {
		return nil, err
	}

	// forward replication request to other KV nodes involved 
	if forward {
		_, err = h.forwardReplicationRequest(request)
		if err != nil {
			// not much we can do when forwarding fails. it is not like we can 
			// resurract deleted replications 
			logger_ap.Errorf("Error forwarding DeleteReplicationRequest\n")
			return nil, err
		}
	}

	// no response body in success case
	return nil, nil
}

func (h *xdcrRestHandler) doPauseReplicationRequest(request *http.Request) ([]byte, error) {
	logger_ap.Infof("doPauseReplicationRequest\n")

	replicationId, forward, err := DecodeReplicationIdAndForwardFlagFromHttpRequest(request, PauseReplicationPrefix)
	if err != nil {
		return nil, err
	}
	
	logger_ap.Debugf("Request params: replicationId=%v\n", replicationId)
	
	if err = rm.PauseReplication(replicationId); err != nil {
		if strings.HasPrefix(err.Error(), "Warning") {
			// return warning messages in response body
			return []byte(err.Error()), nil
		} else {
			// return error
			return nil, err
		}
	}

	// forward replication request to other KV nodes involved 
	if forward {
		_, err = h.forwardReplicationRequest(request)
		if err != nil {
			// TODO
			logger_ap.Errorf("Error forwarding PauseReplicationRequest\n")
			return nil, err
		}
	}

	// no response body in success case
	return nil, nil
}

func (h *xdcrRestHandler) doResumeReplicationRequest(request *http.Request) ([]byte, error) {
	logger_ap.Infof("doResumeReplicationRequest\n")

	replicationId, forward, err := DecodeReplicationIdAndForwardFlagFromHttpRequest(request, ResumeReplicationPrefix)
	if err != nil {
		return nil, err
	}
	
	logger_ap.Debugf("Request params: replicationId=%v\n", replicationId)
	
	if err = rm.ResumeReplication(replicationId); err != nil {
		if strings.HasPrefix(err.Error(), "Warning") {
			// return warning messages in response body
			return []byte(err.Error()), nil
		} else {
			// return error
			return nil, err
		}
	}

	// forward replication request to other KV nodes involved 
	if forward {
		_, err = h.forwardReplicationRequest(request)
		if err != nil {
			// TODO
			logger_ap.Errorf("Error forwarding ResumeReplicationRequest\n")
			return nil, err
		}
	}

	// no response body in success case
	return nil, nil
}

func (h *xdcrRestHandler) doViewReplicationSettingsRequest(request *http.Request) ([]byte, error) {
	logger_ap.Infof("doViewReplicationSettingsRequest\n")

	// get input parameters from request
	replicationId, err:= DecodeReplicationIdFromHttpRequest(request, SettingsReplicationsPath)
	if err != nil {
		return nil, err
	}
	
	logger_ap.Debugf("Request decoded: replicationId=%v", replicationId)
	
	// read replication spec with the specified replication id
	replSpec, err := rm.MetadataService().ReplicationSpec(replicationId)
	if err != nil {
		return nil, err
	}
	
	// marshal replication settings in replication spec and return it
	return NewViewReplicationSettingsResponse(replSpec.Settings)
}

func (h *xdcrRestHandler) doChangeReplicationSettingsRequest(request *http.Request) ([]byte, error) {
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
	
	err = rm.HandleChangesToReplicationSettings(replicationId, inputSettingsMap)
	
	return nil, err
}

// get statistics for all running replications
func (h *xdcrRestHandler) doGetStatisticsRequest(request *http.Request) ([]byte, error) {
	logger_ap.Infof("doGetStatisticsRequest\n")

	statsMap, err := rm.GetStatistics()
	if err == nil {
		return json.Marshal(statsMap)
	} else {
		return nil, err
	}
}

// forward requests to other nodes.
// in case of error, return a list of nodes that the request has been forwarded to, so that caller can take undo action on these nodes
func (h *xdcrRestHandler) forwardReplicationRequest(request *http.Request) (map[string][]interface{}, error) {
	logger_ap.Infof("forwardReplicationRequest\n")
	
	myAddr, err := rm.XDCRCompTopologyService().MyHost()
	if err != nil {
		return nil, err
	}

	xdcrNodesMap, err := rm.XDCRCompTopologyService().XDCRTopology()
	
	forwardedNodesMap := make(map[string][]interface{})
	if len(xdcrNodesMap) > 1 {
		if err = request.ParseForm(); err != nil {
			return nil, err
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
				response, err := forwardReplicationRequestToXDCRNode(request.URL.String(), newBody, xdcrNode, int(port))
				if err != nil {
					return forwardedNodesMap, err
				} else {
					array := []interface{}{port, response}
					forwardedNodesMap[xdcrNode] = array
				}
			}
		}
	}
	return forwardedNodesMap, nil
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

    logger_ap.Infof("forwarded request=%v\n", newRequest)
        
    return http.DefaultClient.Do(newRequest)
}

// Get the message key from http request
func (h *xdcrRestHandler) GetMessageKeyFromRequest(r *http.Request) (string, error) {
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
	defaultSettings, err := rm.ReplicationSettingsService().GetReplicationSettings()
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
