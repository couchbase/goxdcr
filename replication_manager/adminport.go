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
	"errors"
	"fmt"
	"github.com/couchbase/cbauth"
	ap "github.com/couchbase/goxdcr/adminport"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/gen_server"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/pipeline_manager"
	"github.com/couchbase/goxdcr/simple_utils"
	"github.com/couchbase/goxdcr/utils"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"
)

import _ "net/http/pprof"

var StaticPaths = [10]string{base.RemoteClustersPath, CreateReplicationPath, InternalSettingsPath, SettingsReplicationsPath, AllReplicationsPath, AllReplicationInfosPath, RegexpValidationPrefix, MemStatsPath, BlockProfileStartPath, BlockProfileStopPath}
var DynamicPathPrefixes = [6]string{base.RemoteClustersPath, DeleteReplicationPrefix, SettingsReplicationsPath, StatisticsPrefix, AllReplicationsPath, BucketSettingsPrefix}

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
	err := adminport.Start_server()
	if err != nil {
		logger_ap.Errorf("Error calling Start_Server on adminport. err=%v\n", err)
		return
	}

	// start http server
	reqch := make(chan ap.Request)
	hostAddr := utils.GetHostAddr(adminport.sourceKVHost, adminport.xdcrRestPort)
	server := ap.NewHTTPServer("xdcr", hostAddr, base.AdminportUrlPrefix, reqch, new(ap.Handler))
	finch := adminport.finch

	err = server.Start()
	if err != nil {
		goto done
	}

	logger_ap.Infof("http server started %v !\n", hostAddr)

	for {
		select {
		case <-finch:
			goto done
		case req, ok := <-reqch: // admin requests are serialized here
			if ok == false {
				goto done
			}
			// forward message to adminport server for processing
			adminport.SendMsg_async([]interface{}{req})
		}
	}
done:
	server.Stop()
	adminport.Stop_server()
	if err != nil {
		logger_ap.Errorf("adminport exited with error. err=%v\n", err)
	} else {
		logger_ap.Info("adminport exited !\n")
	}
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

// handleRequest have two return values:
// 1. err. When err is not nil, response to the client has a status code of 500 InternalServerError and a body containing the error mssage in err.
// 2. a response of Response type. When err is nil, response to the client has a status code and a body in accordance with those in the Response object.
func (adminport *Adminport) handleRequest(
	request *http.Request) (response *ap.Response, err error) {

	logger_ap.Debugf("handleRequest called\n")

	key, err := adminport.GetMessageKeyFromRequest(request)
	if err != nil {
		return nil, err
	}
	logger_ap.Debugf("MessageKey=%v\n", key)

	// authentication
	//TODO: authAdminCreds sometimes take a long time to return
	//for now, skip authentication for pools/default/replicationInfos because it is called a lot
	if key != AllReplicationInfosPath+base.UrlDelimiter+base.MethodGet && key != MemStatsPath+base.UrlDelimiter+base.MethodGet {
		if key != base.RemoteClustersPath+base.UrlDelimiter+base.MethodGet {
			// most APIs require admin credentials
			err = authAdminCreds(request, false)
		} else {
			// getRemoteClusters() requires only read only admin credential
			err = authAdminCreds(request, true)
		}
	}

	if err != nil {
		return EncodeErrorMessageIntoResponse(err, http.StatusUnauthorized)
	}
	logger_ap.Debug("Authenticated....")

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
	case RegexpValidationPrefix + base.UrlDelimiter + base.MethodPost:
		response, err = adminport.doRegexpValidationRequest(request)
	case MemStatsPath + base.UrlDelimiter + base.MethodGet:
		response, err = adminport.doMemStatsRequest(request)
	case BlockProfileStartPath + base.UrlDelimiter + base.MethodPost:
		response, err = adminport.doStartBlockProfile(request)
	case BlockProfileStopPath + base.UrlDelimiter + base.MethodPost:
		response, err = adminport.doStopBlockProfile(request)
	case BucketSettingsPrefix + DynamicSuffix + base.UrlDelimiter + base.MethodGet:
		response, err = adminport.doGetBucketSettingsRequest(request)
	case BucketSettingsPrefix + DynamicSuffix + base.UrlDelimiter + base.MethodPost:
		response, err = adminport.doBucketSettingsChangeRequest(request)
	default:
		err = ap.ErrorInvalidRequest
	}
	return response, err
}

func (adminport *Adminport) doGetRemoteClustersRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Debugf("doGetRemoteClustersRequest\n")

	remoteClusters, err := RemoteClusterService().RemoteClusters(false)
	if err != nil {
		return nil, err
	}

	return NewGetRemoteClustersResponse(remoteClusters)
}

func (adminport *Adminport) doCreateRemoteClusterRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Infof("doCreateRemoteClusterRequest\n")
	defer logger_ap.Infof("Finished doCreateRemoteClusterRequest\n")

	remoteClusterService := RemoteClusterService()

	justValidate, remoteClusterRef, errorsMap, err := DecodeCreateRemoteClusterRequest(request)
	if err != nil {
		return nil, err
	} else if len(errorsMap) > 0 {
		logger_ap.Errorf("Validation error in inputs. errorsMap=%v\n", errorsMap)
		return EncodeRemoteClusterErrorsMapIntoResponse(errorsMap)
	}

	logger_ap.Infof("Request params: justValidate=%v, remoterClusterRef=%v\n",
		justValidate, remoteClusterRef)

	if justValidate {
		err = remoteClusterService.ValidateAddRemoteCluster(remoteClusterRef)
		return EncodeRemoteClusterErrorIntoResponse(err)
	} else {
		err = remoteClusterService.AddRemoteCluster(remoteClusterRef)
		if err != nil {
			return EncodeRemoteClusterErrorIntoResponse(err)
		} else {
			go writeRemoteClusterAuditEvent(base.CreateRemoteClusterRefEventId, remoteClusterRef, getRealUserIdFromRequest(request))

			return NewCreateRemoteClusterResponse(remoteClusterRef)
		}
	}
}

func (adminport *Adminport) doChangeRemoteClusterRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Infof("doChangeRemoteClusterRequest\n")
	defer logger_ap.Infof("Finished doChangeRemoteClusterRequest\n")
	remoteClusterName, err := DecodeDynamicParamInURL(request, base.RemoteClustersPath, "Remote Cluster Name")
	if err != nil {
		return EncodeRemoteClusterValidationErrorIntoResponse(err)
	}

	logger_ap.Infof("Request params: remoteClusterName=%v\n", remoteClusterName)

	justValidate, remoteClusterRef, errorsMap, err := DecodeCreateRemoteClusterRequest(request)
	if err != nil {
		return nil, err
	} else if len(errorsMap) > 0 {
		logger_ap.Errorf("Validation error in inputs. errorsMap=%v\n", errorsMap)
		return EncodeRemoteClusterErrorsMapIntoResponse(errorsMap)
	}

	logger_ap.Infof("Request params: justValidate=%v, remoterClusterRef=%v\n",
		justValidate, remoteClusterRef)

	remoteClusterService := RemoteClusterService()

	if justValidate {
		err = remoteClusterService.ValidateSetRemoteCluster(remoteClusterName, remoteClusterRef)
		return EncodeRemoteClusterErrorIntoResponse(err)
	} else {
		err = remoteClusterService.SetRemoteCluster(remoteClusterName, remoteClusterRef)
		if err != nil {
			return EncodeRemoteClusterErrorIntoResponse(err)
		} else {
			go writeRemoteClusterAuditEvent(base.UpdateRemoteClusterRefEventId, remoteClusterRef, getRealUserIdFromRequest(request))

			return NewCreateRemoteClusterResponse(remoteClusterRef)
		}
	}
}

func (adminport *Adminport) doDeleteRemoteClusterRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Infof("doDeleteRemoteClusterRequest\n")
	defer logger_ap.Infof("Finished doDeleteRemoteClusterRequest\n")
	remoteClusterName, err := DecodeDynamicParamInURL(request, base.RemoteClustersPath, "Remote Cluster Name")
	if err != nil {
		return EncodeRemoteClusterValidationErrorIntoResponse(err)
	}

	logger_ap.Infof("Request params: remoteClusterName=%v\n", remoteClusterName)

	remoteClusterService := RemoteClusterService()
	ref, err := remoteClusterService.RemoteClusterByRefName(remoteClusterName, false)
	if err != nil {
		return EncodeRemoteClusterValidationErrorIntoResponse(err)
	}

	// TODO get spec from replication status cache after the caching issue is fixed
	specs, err := ReplicationSpecService().AllReplicationSpecs()
	if err != nil {
		return nil, err
	}
	replIds := make([]string, 0)
	for _, spec := range specs {
		if spec.TargetClusterUUID == ref.Uuid {
			replIds = append(replIds, spec.Id)
		}
	}
	if len(replIds) > 0 {
		err = fmt.Errorf("Cannot delete remote cluster `%v` since it is referenced by replications %v", ref.Name, replIds)
		return EncodeRemoteClusterValidationErrorIntoResponse(err)
	}

	ref, err = remoteClusterService.DelRemoteCluster(remoteClusterName)
	if err != nil {
		return EncodeRemoteClusterErrorIntoResponse(err)
	}

	go writeRemoteClusterAuditEvent(base.DeleteRemoteClusterRefEventId, ref, getRealUserIdFromRequest(request))

	return NewOKResponse()
}

func (adminport *Adminport) doGetAllReplicationsRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Debugf("doGetAllReplicationsRequest\n")

	replIds := pipeline_manager.AllReplications()
	replSpecs := make(map[string]*metadata.ReplicationSpecification)
	for _, replId := range replIds {
		replSpecs[replId] = pipeline_manager.ReplicationStatus(replId).Spec()
	}

	return NewGetAllReplicationsResponse(replSpecs)
}

func (adminport *Adminport) doGetAllReplicationInfosRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Debugf("doGetAllReplicationInfosRequest\n")
	replInfos, err := GetReplicationInfos()
	if err != nil {
		return nil, err
	}
	return NewGetAllReplicationInfosResponse(replInfos)
}

func (adminport *Adminport) doCreateReplicationRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Info("doCreateReplicationRequest")
	defer logger_ap.Info("Finished doCreateReplicationRequest call")

	justValidate, fromBucket, toCluster, toBucket, settings, errorsMap, err := DecodeCreateReplicationRequest(request)
	if err != nil {
		return nil, err
	} else if len(errorsMap) > 0 {
		logger_ap.Errorf("Validation error in inputs. errorsMap=%v\n", errorsMap)
		return EncodeErrorsMapIntoResponse(errorsMap, true)
	}

	logger_ap.Infof("Request parameters: justValidate=%v, fromBucket=%v, toCluster=%v, toBucket=%v, settings=%v\n",
		justValidate, fromBucket, toCluster, toBucket, settings)

	replicationId, errorsMap, err := CreateReplication(justValidate, fromBucket, toCluster, toBucket, settings, getRealUserIdFromRequest(request))

	if err != nil {
		return EncodeReplicationSpecErrorIntoResponse(err)
	} else if len(errorsMap) > 0 {
		logger_ap.Errorf("Error creating replication. errorsMap=%v\n", errorsMap)
		return EncodeErrorsMapIntoResponse(errorsMap, true)
	} else {
		return NewCreateReplicationResponse(replicationId)
	}
}

func (adminport *Adminport) doDeleteReplicationRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Infof("doDeleteReplicationRequest\n")
	defer logger_ap.Infof("Finished doDeleteReplicationRequest\n")

	replicationId, err := DecodeDynamicParamInURL(request, DeleteReplicationPrefix, "Replication Id")
	if err != nil {
		return EncodeReplicationValidationErrorIntoResponse(err)
	}

	logger_ap.Debugf("Request params: replicationId=%v\n", replicationId)

	err = DeleteReplication(replicationId, getRealUserIdFromRequest(request))

	if err != nil {
		return EncodeReplicationSpecErrorIntoResponse(err)
	} else {
		return NewEmptyArrayResponse()
	}
}

func (adminport *Adminport) doViewInternalSettingsRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Infof("doViewInternalSettingsRequest\n")

	// default replication setting
	defaultSettings, err := ReplicationSettingsService().GetDefaultReplicationSettings()

	if err != nil {
		return nil, err
	}

	// default process settings
	defaultProcessSetting, err := GlobalSettingsService().GetDefaultGlobalSettings()

	if err != nil {
		return nil, err
	}

	return NewInternalSettingsResponse(defaultSettings, defaultProcessSetting)
}

func (adminport *Adminport) doChangeInternalSettingsRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Infof("doChangeInternalSettingsRequest\n")

	settingsMap, errorsMap := DecodeSettingsFromInternalSettingsRequest(request)
	if len(errorsMap) > 0 {
		logger_ap.Errorf("Validation error in inputs. errorsMap=%v\n", errorsMap)
		return EncodeInternalSettingsErrorsMapIntoResponse(errorsMap)
	}

	logger_ap.Infof("Request params: inputSettings=%v\n", settingsMap)

	errorsMap, err := UpdateDefaultSettings(settingsMap, getRealUserIdFromRequest(request))
	if err != nil {
		return nil, err
	} else if len(errorsMap) > 0 {
		logger_ap.Errorf("Validation error in inputs. errorsMap=%v\n", errorsMap)
		return EncodeInternalSettingsErrorsMapIntoResponse(errorsMap)
	} else {
		return NewEmptyArrayResponse()
	}
}

func (adminport *Adminport) doViewDefaultReplicationSettingsRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Infof("doViewDefaultReplicationSettingsRequest\n")

	defaultSettings, err := ReplicationSettingsService().GetDefaultReplicationSettings()
	if err != nil {
		return nil, err
	}
	// default process settings
	defaultProcessSetting, err := GlobalSettingsService().GetDefaultGlobalSettings()

	if err != nil {
		return nil, err
	}

	return NewDefaultReplicationSettingsResponse(defaultSettings, defaultProcessSetting)
}

func (adminport *Adminport) doChangeDefaultReplicationSettingsRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Infof("doChangeDefaultReplicationSettingsRequest\n")

	justValidate, settingsMap, errorsMap := DecodeChangeReplicationSettings(request, true)
	if len(errorsMap) > 0 {
		logger_ap.Errorf("Validation error in inputs. errorsMap=%v\n", errorsMap)
		return EncodeErrorsMapIntoResponse(errorsMap, false)
	}

	logger_ap.Infof("Request params: justValidate=%v, inputSettings=%v\n", justValidate, settingsMap)

	if !justValidate {
		errorsMap, err := UpdateDefaultSettings(settingsMap, getRealUserIdFromRequest(request))
		if err != nil {
			return nil, err
		} else if len(errorsMap) > 0 {
			logger_ap.Errorf("Validation error in inputs. errorsMap=%v\n", errorsMap)
			return EncodeErrorsMapIntoResponse(errorsMap, false)
		}
	}

	// change default settings returns the default settings after changes
	defaultSettings, err := ReplicationSettingsService().GetDefaultReplicationSettings()
	if err != nil {
		return nil, err
	}

	// default process settings
	defaultProcessSetting, err := GlobalSettingsService().GetDefaultGlobalSettings()

	if err != nil {
		return nil, err
	}

	return NewDefaultReplicationSettingsResponse(defaultSettings, defaultProcessSetting)
}

func (adminport *Adminport) doViewReplicationSettingsRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Infof("doViewReplicationSettingsRequest\n")

	// get input parameters from request
	replicationId, err := DecodeDynamicParamInURL(request, SettingsReplicationsPath, "Replication Id")
	if err != nil {
		return EncodeReplicationValidationErrorIntoResponse(err)
	}

	logger_ap.Debugf("Request params: replicationId=%v", replicationId)

	// read replication spec with the specified replication id
	replSpec, err := ReplicationSpecService().ReplicationSpec(replicationId)
	if err != nil {
		return EncodeReplicationSpecErrorIntoResponse(err)
	}

	// marshal replication settings in replication spec and return it
	return NewReplicationSettingsResponse(replSpec.Settings)
}

func (adminport *Adminport) doChangeReplicationSettingsRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Infof("doChangeReplicationSettingsRequest\n")

	// get input parameters from request
	replicationId, err := DecodeDynamicParamInURL(request, SettingsReplicationsPath, "Replication Id")
	if err != nil {
		return EncodeReplicationValidationErrorIntoResponse(err)
	}
	logger_ap.Infof("Request params: replicationId=%v\n", replicationId)

	justValidate, settingsMap, errorsMap := DecodeChangeReplicationSettings(request, false)
	if len(errorsMap) > 0 {
		logger_ap.Errorf("Validation error in inputs. errorsMap=%v\n", errorsMap)
		return EncodeErrorsMapIntoResponse(errorsMap, false)
	}

	logger_ap.Infof("Request params: justValidate=%v, inputSettings=%v\n", justValidate, settingsMap)

	if justValidate {
		return NewEmptyArrayResponse()
	}

	errorsMap, err = UpdateReplicationSettings(replicationId, settingsMap, getRealUserIdFromRequest(request))
	if err != nil {
		return nil, err
	} else if len(errorsMap) > 0 {
		logger_ap.Errorf("Validation error in inputs. errorsMap=%v\n", errorsMap)
		return EncodeErrorsMapIntoResponse(errorsMap, false)
	}

	// return replication settings after changes
	replSpec, err := ReplicationSpecService().ReplicationSpec(replicationId)
	if err != nil {
		return EncodeReplicationSpecErrorIntoResponse(err)
	}
	logger_ap.Info("Done with doChangeReplicationSettingsRequest")
	return NewReplicationSettingsResponse(replSpec.Settings)
}

// get statistics for all running replications
func (adminport *Adminport) doGetStatisticsRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Debugf("doGetStatisticsRequest\n")

	//pass the request to get the bucket name
	bucket, err := DecodeDynamicParamInURL(request, StatisticsPrefix, "Bucket Name")
	if err != nil {
		return EncodeReplicationValidationErrorIntoResponse(err)
	}

	statsMap, err := GetStatistics(bucket)
	if err == nil {
		if statsMap == nil {
			return NewEmptyArrayResponse()
		}
		return EncodeByteArrayIntoResponse([]byte(statsMap.String()))
	} else {
		return nil, err
	}
}

func (adminport *Adminport) doMemStatsRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Debugf("doMemStatsRequest\n")
	stats := new(runtime.MemStats)
	runtime.ReadMemStats(stats)
	bytes, _ := json.Marshal(stats)
	return EncodeByteArrayIntoResponse(bytes)
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
		return "", simple_utils.InvalidPathInHttpRequestError(r.URL.Path)
	} else {
		// add http method suffix to name to ensure uniqueness
		key += base.UrlDelimiter + strings.ToUpper(r.Method)

		logger_ap.Debugf("Request key decoded: %v\n", key)

		return key, nil
	}
}

// returns error if credentials in request are not admin/read only admin
func authAdminCreds(request *http.Request, readOnly bool) error {
	// authentication
	var err error
	var isAdmin bool
	creds, err := cbauth.AuthWebCreds(request)
	if err != nil {
		return err
	}
	logger_ap.Debugf("creds user = %v\n", creds.Name())

	if readOnly {
		isAdmin, err = creds.IsROAdmin()

	} else {
		isAdmin, err = creds.IsAdmin()
	}

	logger_ap.Debugf("done with authentication")
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
		GenericFields:         base.GenericFields{log.FormatTimeWithMilliSecondPrecision(time.Now()), *realUserId},
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

	return &base.RealUserId{creds.Source(), creds.Name()}
}

func (adminport *Adminport) IsReadyForHeartBeat() bool {
	return adminport.IsStarted()
}

func (adminport *Adminport) doRegexpValidationRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Infof("doRegexpValidationRequest\n")

	expression, keys, err := DecodeRegexpValidationRequest(request)
	if err != nil {
		return EncodeErrorMessageIntoResponse(err, http.StatusBadRequest)
	}

	logger_ap.Infof("Request params: expression=%v, keys=%v\n",
		expression, keys)

	matchesMap, err := utils.GetMatchedKeys(expression, keys)
	if err != nil {
		return EncodeErrorMessageIntoResponse(err, http.StatusBadRequest)
	}

	return NewRegexpValidationResponse(matchesMap)

}

func (adminport *Adminport) doStartBlockProfile(request *http.Request) (*ap.Response, error) {
	var err error = nil
	block_profile_rate := 1000
	err = request.ParseForm()
	if err != nil {
		return nil, err
	}
	for key, valArr := range request.Form {
		switch key {
		case base.BlockProfileRate:
			blockProfileRateStr := getStringFromValArr(valArr)
			if blockProfileRateStr != "" {
				block_profile_rate, err = strconv.Atoi(blockProfileRateStr)
			}
		}
	}
	if err == nil {
		runtime.SetBlockProfileRate(block_profile_rate)
		logger_ap.Infof("doStartBlockProfile - SetBlockPofileRate to %v naosecond", block_profile_rate)
		return NewEmptyArrayResponse()

	}
	return nil, err
}

func (adminport *Adminport) doStopBlockProfile(request *http.Request) (*ap.Response, error) {
	runtime.SetBlockProfileRate(-1)
	logger_ap.Info("doStopBlockProfile")
	return NewEmptyArrayResponse()
}

func (adminport *Adminport) doGetBucketSettingsRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Infof("doGetBucketSettingsRequest\n")
	defer logger_ap.Infof("doGetBucketSettingsRequest completed\n")

	bucketName, err := DecodeDynamicParamInURL(request, BucketSettingsPrefix, BucketName)
	if err != nil {
		return EncodeReplicationValidationErrorIntoResponse(err)
	}

	logger_ap.Infof("Request params: bucketName=%v\n", bucketName)

	bucketSettingsMap, err := getBucketSettings(bucketName)
	if err != nil {
		// if bucket does not exist, it is a validation error and not an internal error
		if err == utils.NonExistentBucketError {
			err = fmt.Errorf("Bucket %v does not exist", bucketName)
			return EncodeReplicationValidationErrorIntoResponse(err)
		}
		return nil, err
	}

	return EncodeObjectIntoResponse(bucketSettingsMap)
}

func (adminport *Adminport) doBucketSettingsChangeRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Infof("doBucketSettingsChangeRequest\n")
	defer logger_ap.Infof("doBucketSettingsChangeRequest completed\n")

	bucketName, err := DecodeDynamicParamInURL(request, BucketSettingsPrefix, BucketName)
	if err != nil {
		return EncodeReplicationValidationErrorIntoResponse(err)
	}

	lwwEnabled, err := DecodeBucketSettingsChangeRequest(request)
	if err != nil {
		return EncodeErrorMessageIntoResponse(err, http.StatusBadRequest)
	}

	logger_ap.Infof("Request params: bucketName=%v, lwwEnabled=%v\n",
		bucketName, lwwEnabled)

	bucketSettingsMap, err := setBucketSettings(bucketName, lwwEnabled, getRealUserIdFromRequest(request))
	if err != nil {
		if err == utils.NonExistentBucketError {
			// if bucket does not exist, it is a validation error and not an internal error
			err = fmt.Errorf("Bucket %v does not exist", bucketName)
			return EncodeReplicationValidationErrorIntoResponse(err)
		}
		return nil, err
	}

	return EncodeObjectIntoResponse(bucketSettingsMap)
}
