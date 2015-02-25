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
	"errors"
	"fmt"
	"github.com/couchbase/cbauth"
	ap "github.com/couchbase/goxdcr/adminport"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/gen_server"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	utils "github.com/couchbase/goxdcr/utils"
	"net/http"
	"strings"
	"time"
)

var StaticPaths = [6]string{base.RemoteClustersPath, CreateReplicationPath, InternalSettingsPath, SettingsReplicationsPath, AllReplicationsPath, AllReplicationInfosPath}
var DynamicPathPrefixes = [5]string{base.RemoteClustersPath, DeleteReplicationPrefix, SettingsReplicationsPath, StatisticsPrefix, AllReplicationsPath}

var logger_ap *log.CommonLogger = log.NewLogger("AdminPort", log.DefaultLoggerContext)

/************************************
/* struct Adminport
*************************************/
type Adminport struct {
	sourceKVHost string
	xdcrRestPort uint16
	gen_server.GenServer
	finch chan bool
}

func NewAdminport(laddr string, xdcrRestPort uint16, finch chan bool) *Adminport {

	//callback functions from GenServer
	var msg_callback_func gen_server.Msg_Callback_Func
	var exit_callback_func gen_server.Exit_Callback_Func
	var error_handler_func gen_server.Error_Handler_Func

	server := gen_server.NewGenServer(&msg_callback_func,
		&exit_callback_func, &error_handler_func, log.DefaultLoggerContext, "Adminport")

	adminport := &Adminport{
		sourceKVHost: laddr,
		xdcrRestPort: xdcrRestPort,
		GenServer:    server, /*gen_server.GenServer*/
		finch:        finch,
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
	hostAddr := utils.GetHostAddr(adminport.sourceKVHost, adminport.xdcrRestPort)
	server := ap.NewHTTPServer("xdcr", hostAddr, base.AdminportUrlPrefix, reqch, new(ap.Handler))

	server.Start()
	logger_ap.Infof("server started %v !\n", hostAddr)

	finch := adminport.finch
	count := 0
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
	request *http.Request) (response []byte, err error) {

	logger_ap.Infof("handleRequest called\n")
	logger_ap.Debugf("Request: %v \n", request)

	key, err := adminport.GetMessageKeyFromRequest(request)
	if err != nil {
		return nil, err
	}
	logger_ap.Infof("MessageKey=%v\n", key)

	// authentication
	//TODO: authAdminCreds sometimes take a long time to return
	//for now, skip authentication for pools/default/replicationInfos because it is called a lot
	if key != AllReplicationInfosPath+base.UrlDelimiter+base.MethodGet {
		if key != base.RemoteClustersPath+base.UrlDelimiter+base.MethodGet {
			// most APIs require admin credentials
			err = authAdminCreds(request, false)
		} else {
			// getRemoteClusters() requires only read only admin credential
			err = authAdminCreds(request, true)
		}
	}

	if err != nil {
		return nil, err
	}
	logger_ap.Info("Authenticated....")

	switch key {
	case base.RemoteClustersPath + base.UrlDelimiter + base.MethodGet:
		response, err = adminport.doGetRemoteClustersRequest(request)
	case base.RemoteClustersPath + base.UrlDelimiter + base.MethodPost:
		response, err = adminport.doCreateRemoteClusterRequest(request)
	case base.RemoteClustersPath + DynamicSuffix + base.UrlDelimiter + base.MethodPost:
		response, err = adminport.doChangeRemoteClusterRequest(request)
	case base.RemoteClustersPath + DynamicSuffix + base.UrlDelimiter + base.MethodDelete:
		response, err = adminport.doDeleteRemoteClusterRequest(request)
	case AllReplicationsPath + base.UrlDelimiter + base.MethodGet:
		response, err = adminport.doGetAllReplicationsRequest(request)
	case AllReplicationsPath + DynamicSuffix + base.UrlDelimiter + base.MethodDelete:
		response, err = adminport.doDeleteAllReplicationsRequest(request)
	case AllReplicationInfosPath + base.UrlDelimiter + base.MethodGet:
		response, err = adminport.doGetAllReplicationInfosRequest(request)
	case CreateReplicationPath + base.UrlDelimiter + base.MethodPost:
		response, err = adminport.doCreateReplicationRequest(request)
	case DeleteReplicationPrefix + DynamicSuffix + base.UrlDelimiter + base.MethodDelete:
		fallthrough
	// historically, deleteReplication could use Post method
	case DeleteReplicationPrefix + DynamicSuffix + base.UrlDelimiter + base.MethodPost:
		response, err = adminport.doDeleteReplicationRequest(request)
	case InternalSettingsPath + base.UrlDelimiter + base.MethodGet:
		response, err = adminport.doViewInternalSettingsRequest(request)
	case InternalSettingsPath + base.UrlDelimiter + base.MethodPost:
		response, err = adminport.doChangeInternalSettingsRequest(request)
	case SettingsReplicationsPath + base.UrlDelimiter + base.MethodGet:
		response, err = adminport.doViewDefaultReplicationSettingsRequest(request)
	case SettingsReplicationsPath + base.UrlDelimiter + base.MethodPost:
		response, err = adminport.doChangeDefaultReplicationSettingsRequest(request)
	case SettingsReplicationsPath + DynamicSuffix + base.UrlDelimiter + base.MethodGet:
		response, err = adminport.doViewReplicationSettingsRequest(request)
	case SettingsReplicationsPath + DynamicSuffix + base.UrlDelimiter + base.MethodPost:
		response, err = adminport.doChangeReplicationSettingsRequest(request)
	case StatisticsPrefix + DynamicSuffix + base.UrlDelimiter + base.MethodGet:
		response, err = adminport.doGetStatisticsRequest(request)
	default:
		err = ap.ErrorInvalidRequest
	}
	return response, err
}

func (adminport *Adminport) doGetRemoteClustersRequest(request *http.Request) ([]byte, error) {
	logger_ap.Infof("doGetRemoteClustersRequest\n")

	remoteClusters, err := RemoteClusterService().RemoteClusters(false)
	if err != nil {
		return nil, err
	}

	return NewGetRemoteClustersResponse(remoteClusters)
}

func (adminport *Adminport) doCreateRemoteClusterRequest(request *http.Request) ([]byte, error) {
	logger_ap.Infof("doCreateRemoteClusterRequest\n")

	justValidate, remoteClusterRef, errorsMap, err := DecodeCreateRemoteClusterRequest(request)
	if err != nil {
		return nil, err
	} else if len(errorsMap) > 0 {
		logger_ap.Errorf("Validation error in inputs. errorsMap=%v\n", errorsMap)
		return EncodeRemoteClusterErrorsMapIntoByteArray(errorsMap)
	}

	logger_ap.Infof("Request params: justValidate=%v, remoterClusterRef=%v\n",
		justValidate, *remoteClusterRef)

	if justValidate {
		err = RemoteClusterService().ValidateRemoteCluster(remoteClusterRef)
		if err != nil {
			return nil, err
		} else {
			return nil, nil
		}
	} else {
		err = RemoteClusterService().AddRemoteCluster(remoteClusterRef)
		if err != nil {
			return nil, err
		} else {
			go writeRemoteClusterAuditEvent(base.CreateRemoteClusterRefEventId, remoteClusterRef, getRealUserIdFromRequest(request))

			return NewCreateRemoteClusterResponse(remoteClusterRef)
		}
	}
}

func (adminport *Adminport) doChangeRemoteClusterRequest(request *http.Request) ([]byte, error) {
	logger_ap.Infof("doChangeRemoteClusterRequest\n")
	remoteClusterName, err := DecodeDynamicParamInURL(request, base.RemoteClustersPath, "Remote Cluster Name")
	if err != nil {
		return nil, err
	}

	logger_ap.Infof("Request params: remoteClusterName=%v\n", remoteClusterName)

	justValidate, remoteClusterRef, errorsMap, err := DecodeCreateRemoteClusterRequest(request)
	if err != nil {
		return nil, err
	} else if len(errorsMap) > 0 {
		logger_ap.Errorf("Validation error in inputs. errorsMap=%v\n", errorsMap)
		return EncodeRemoteClusterErrorsMapIntoByteArray(errorsMap)
	}

	logger_ap.Infof("Request params: justValidate=%v, remoterClusterRef=%v\n",
		justValidate, *remoteClusterRef)

	remoteClusterService := RemoteClusterService()

	if justValidate {
		err = remoteClusterService.ValidateRemoteCluster(remoteClusterRef)
		if err != nil {
			return nil, err
		} else {
			return nil, nil
		}
	} else {
		err = remoteClusterService.SetRemoteCluster(remoteClusterName, remoteClusterRef)
		if err != nil {
			return nil, err
		} else {
			go writeRemoteClusterAuditEvent(base.UpdateRemoteClusterRefEventId, remoteClusterRef, getRealUserIdFromRequest(request))

			return NewCreateRemoteClusterResponse(remoteClusterRef)
		}
	}
}

func (adminport *Adminport) doDeleteRemoteClusterRequest(request *http.Request) ([]byte, error) {
	logger_ap.Infof("doDeleteRemoteClusterRequest\n")
	remoteClusterName, err := DecodeDynamicParamInURL(request, base.RemoteClustersPath, "Remote Cluster Name")
	if err != nil {
		return nil, err
	}

	logger_ap.Infof("Request params: remoteClusterName=%v\n", remoteClusterName)

	ref, err := RemoteClusterService().DelRemoteCluster(remoteClusterName)
	if err != nil {
		return nil, err
	}

	go writeRemoteClusterAuditEvent(base.DeleteRemoteClusterRefEventId, ref, getRealUserIdFromRequest(request))

	return NewDeleteRemoteClusterResponse()
}

func (adminport *Adminport) doGetAllReplicationsRequest(request *http.Request) ([]byte, error) {
	logger_ap.Infof("doGetAllReplicationsRequest\n")

	replSpecs, err := ReplicationSpecService().ActiveReplicationSpecs()
	if err != nil {
		return nil, err
	}

	return NewGetAllReplicationsResponse(replSpecs)
}

func (adminport *Adminport) doDeleteAllReplicationsRequest(request *http.Request) ([]byte, error) {
	logger_ap.Infof("doDeleteAllReplicationsRequest\n")

	// get input parameters from request
	bucket, err := DecodeDynamicParamInURL(request, AllReplicationsPath, "Bucket Name")
	if err != nil {
		return nil, err
	}

	logger_ap.Debugf("Request params: bucket=%v", bucket)

	return nil, DeleteAllReplications(bucket, getRealUserIdFromRequest(request))
}

func (adminport *Adminport) doGetAllReplicationInfosRequest(request *http.Request) ([]byte, error) {
	logger_ap.Infof("doGetAllReplicationInfosRequest\n")
	replInfos, err := GetReplicationInfos()
	if err != nil {
		return nil, err
	}
	return NewGetAllReplicationInfosResponse(replInfos)
}

func (adminport *Adminport) doCreateReplicationRequest(request *http.Request) ([]byte, error) {
	logger_ap.Info("doCreateReplicationRequest called")
	defer logger_ap.Info("Finish doCreateReplicationREquest call")

	fromBucket, toCluster, toBucket, settings, errorsMap, err := DecodeCreateReplicationRequest(request)
	if err != nil {
		return nil, err
	} else if len(errorsMap) > 0 {
		logger_ap.Errorf("Validation error in inputs. errorsMap=%v\n", errorsMap)
		return EncodeReplicationErrorsMapIntoByteArray(errorsMap)
	}

	logger_ap.Infof("Request parameters: fromBucket=%v, toCluster=%v, toBucket=%v, settings=%v\n",
		fromBucket, toCluster, toBucket, settings)

	replicationId, errorsMap, err := CreateReplication(fromBucket, toCluster, toBucket, settings, getRealUserIdFromRequest(request))

	if err != nil {
		return nil, err
	} else if len(errorsMap) > 0 {
		logger_ap.Errorf("Error creating replication. errorsMap=%v\n", errorsMap)
		return EncodeReplicationErrorsMapIntoByteArray(errorsMap)
	} else {
		return NewCreateReplicationResponse(replicationId), nil
	}
}

func (adminport *Adminport) doDeleteReplicationRequest(request *http.Request) ([]byte, error) {
	logger_ap.Infof("doDeleteReplicationRequest\n")
	replicationId, err := DecodeDynamicParamInURL(request, DeleteReplicationPrefix, "Replication Id")
	if err != nil {
		return nil, err
	}

	logger_ap.Debugf("Request params: replicationId=%v\n", replicationId)

	err = DeleteReplication(replicationId, getRealUserIdFromRequest(request))

	if err != nil {
		return nil, err
	} else {
		return NewDeleteReplicationResponse()
	}
}

func (adminport *Adminport) doViewInternalSettingsRequest(request *http.Request) ([]byte, error) {
	logger_ap.Infof("doViewInternalSettingsRequest\n")

	defaultSettings, err := ReplicationSettingsService().GetDefaultReplicationSettings()
	if err != nil {
		return nil, err
	}

	return NewInternalSettingsResponse(defaultSettings)
}

func (adminport *Adminport) doChangeInternalSettingsRequest(request *http.Request) ([]byte, error) {
	logger_ap.Infof("doChangeInternalSettingsRequest\n")

	settingsMap, errorsMap, err := DecodeSettingsFromInternalSettingsRequest(request)
	if err != nil {
		return nil, err
	} else if len(errorsMap) > 0 {
		logger_ap.Errorf("Validation error in inputs. errorsMap=%v\n", errorsMap)
		return EncodeReplicationErrorsMapIntoByteArray(errorsMap)
	}

	logger_ap.Infof("Request params: inputSettings=%v\n", settingsMap)

	errorsMap, err = UpdateDefaultReplicationSettings(settingsMap, getRealUserIdFromRequest(request))
	if err != nil {
		return nil, err
	} else if len(errorsMap) > 0 {
		logger_ap.Errorf("Validation error in inputs. errorsMap=%v\n", errorsMap)
		return EncodeReplicationErrorsMapIntoByteArray(errorsMap)
	} else {
		return nil, nil
	}
}

func (adminport *Adminport) doViewDefaultReplicationSettingsRequest(request *http.Request) ([]byte, error) {
	logger_ap.Infof("doViewDefaultReplicationSettingsRequest\n")

	defaultSettings, err := ReplicationSettingsService().GetDefaultReplicationSettings()
	if err != nil {
		return nil, err
	}

	return NewReplicationSettingsResponse(defaultSettings)
}

func (adminport *Adminport) doChangeDefaultReplicationSettingsRequest(request *http.Request) ([]byte, error) {
	logger_ap.Infof("doChangeDefaultReplicationSettingsRequest\n")

	justValidate, settingsMap, errorsMap, err := DecodeChangeReplicationSettings(request)
	if err != nil {
		return nil, err
	} else if len(errorsMap) > 0 {
		logger_ap.Errorf("Validation error in inputs. errorsMap=%v\n", errorsMap)
		return EncodeReplicationErrorsMapIntoByteArray(errorsMap)
	}

	logger_ap.Infof("Request params: justValidate=%v, inputSettings=%v\n", justValidate, settingsMap)

	if justValidate {
		return nil, nil
	}

	errorsMap, err = UpdateDefaultReplicationSettings(settingsMap, getRealUserIdFromRequest(request))
	if err != nil {
		return nil, err
	} else if len(errorsMap) > 0 {
		logger_ap.Errorf("Validation error in inputs. errorsMap=%v\n", errorsMap)
		return EncodeReplicationErrorsMapIntoByteArray(errorsMap)
	}

	// change default settings returns the default settings after changes
	defaultSettings, err := ReplicationSettingsService().GetDefaultReplicationSettings()
	if err != nil {
		return nil, err
	}

	return NewReplicationSettingsResponse(defaultSettings)
}

func (adminport *Adminport) doViewReplicationSettingsRequest(request *http.Request) ([]byte, error) {
	logger_ap.Infof("doViewReplicationSettingsRequest\n")

	// get input parameters from request
	replicationId, err := DecodeDynamicParamInURL(request, SettingsReplicationsPath, "Replication Id")
	if err != nil {
		return nil, err
	}

	logger_ap.Debugf("Request params: replicationId=%v", replicationId)

	// read replication spec with the specified replication id
	replSpec, err := ReplicationSpecService().ReplicationSpec(replicationId)
	if err != nil {
		return nil, err
	}

	// marshal replication settings in replication spec and return it
	return NewReplicationSettingsResponse(replSpec.Settings)
}

func (adminport *Adminport) doChangeReplicationSettingsRequest(request *http.Request) ([]byte, error) {
	logger_ap.Infof("doChangeReplicationSettingsRequest\n")

	// get input parameters from request
	replicationId, err := DecodeDynamicParamInURL(request, SettingsReplicationsPath, "Replication Id")
	if err != nil {
		return nil, err
	}
	logger_ap.Infof("Request params: replicationId=%v\n", replicationId)

	justValidate, settingsMap, errorsMap, err := DecodeChangeReplicationSettings(request)
	if err != nil {
		return nil, err
	} else if len(errorsMap) > 0 {
		logger_ap.Errorf("Validation error in inputs. errorsMap=%v\n", errorsMap)
		return EncodeReplicationErrorsMapIntoByteArray(errorsMap)
	}

	logger_ap.Infof("Request params: justValidate=%v, inputSettings=%v\n", justValidate, settingsMap)

	if justValidate {
		return nil, nil
	}

	errorsMap, err = UpdateReplicationSettings(replicationId, settingsMap, getRealUserIdFromRequest(request))
	if err != nil {
		return nil, err
	} else if len(errorsMap) > 0 {
		logger_ap.Errorf("Validation error in inputs. errorsMap=%v\n", errorsMap)
		return EncodeReplicationErrorsMapIntoByteArray(errorsMap)
	}

	// return replication settings after changes
	replSpec, err := ReplicationSpecService().ReplicationSpec(replicationId)
	if err != nil {
		return nil, err
	}
	logger_ap.Info("Done with doChangeReplicationSettingsRequest")
	return NewReplicationSettingsResponse(replSpec.Settings)
}

// get statistics for all running replications
func (adminport *Adminport) doGetStatisticsRequest(request *http.Request) ([]byte, error) {
	logger_ap.Infof("doGetStatisticsRequest\n")

	//pass the request to get the bucket name
	bucket, err := DecodeDynamicParamInURL(request, StatisticsPrefix, "Bucket Name")
	if err != nil {
		return nil, err
	}

	statsMap, err := GetStatistics(bucket)
	if err == nil {
		if statsMap == nil {
			return nil, errors.New(fmt.Sprintf("No replication for bucket %v", bucket))
		}
		return []byte(statsMap.String()), nil
	} else {
		return nil, err
	}
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

// returns error if credentials in request are not admin/read only admin
func authAdminCreds(request *http.Request, readOnly bool) error {
	// authentication
	logger_ap.Info("Start authAdminCreds")
	var err error
	var isAdmin bool
	creds, err := cbauth.AuthWebCreds(request)
	logger_ap.Infof("creds user = %v\n", creds.Name())
	if err != nil {
		return err
	}

	if readOnly {
		isAdmin, err = creds.IsROAdmin()

	} else {
		isAdmin, err = creds.IsAdmin()
	}

	logger_ap.Infof("done with authentication")
	if err != nil {
		return err
	}
	if !isAdmin {
		return errors.New("Unauthorized")
	}
	return nil
}

func writeRemoteClusterAuditEvent(eventId uint32, remoteClusterRef *metadata.RemoteClusterReference, realUserId *base.RealUserId) {
	event := &base.RemoteClusterRefEvent{
		GenericFields:         base.GenericFields{time.Now(), *realUserId},
		RemoteClusterName:     remoteClusterRef.Name,
		RemoteClusterHostname: remoteClusterRef.HostName,
		IsEncrypted:           remoteClusterRef.DemandEncryption}

	err := AuditService().Write(eventId, event)
	logAuditErrors(err)
}

func getRealUserIdFromRequest(request *http.Request) *base.RealUserId {
	creds, err := cbauth.AuthWebCreds(request)
	if err != nil {
		logger_rm.Errorf("Error getting real user id from http request. err=%v\n", err)
		// put unknown user in the audit log.
		return &base.RealUserId{"internal", "unknown"}
	}

	// TODO get source from creds
	return &base.RealUserId{"internal", creds.Name()}
}

func (adminport *Adminport) IsReadyForHeartBeat() bool {
	return adminport.IsStarted()
}
