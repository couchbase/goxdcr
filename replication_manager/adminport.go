// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

// replication manager's adminport.

package replication_manager

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/cbauth"
	ap "github.com/couchbase/goxdcr/v8/adminport"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/gen_server"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/peerToPeer"
	"github.com/couchbase/goxdcr/v8/pipeline_utils"
	"github.com/couchbase/goxdcr/v8/service_def"
	utilities "github.com/couchbase/goxdcr/v8/utils"

	_ "net/http/pprof"
)

var StaticPaths = []string{base.RemoteClustersPath, CreateReplicationPath, SettingsReplicationsPath, AllReplicationsPath, AllReplicationInfosPath, RegexpValidationPrefix, MemStatsPath, BlockProfileStartPath, BlockProfileStopPath, XDCRInternalSettingsPath, XDCRPrometheusStatsPath, XDCRPrometheusStatsHighPath, base.XDCRPeerToPeerPath, base.XDCRConnectionPreCheckPath}
var DynamicPathPrefixes = []string{base.RemoteClustersPath, DeleteReplicationPrefix, SettingsReplicationsPath, StatisticsPrefix, AllReplicationsPath}

var logger_ap *log.CommonLogger = log.NewLogger(base.AdminPortKey, log.GetOrCreateContext(base.AdminPortKey))

/*
***********************************
/* struct Adminport
************************************
*/
type Adminport struct {
	sourceKVHost string
	xdcrRestPort uint16
	kvAdminPort  uint16
	gen_server.GenServer
	utils              utilities.UtilsIface
	prometheusExporter pipeline_utils.ExpVarExporter

	p2pMgr              peerToPeer.P2PManager
	p2pAPI              peerToPeer.PeerToPeerCommAPI
	securitySvc         service_def.SecuritySvc
	xdcrCompTopologySvc service_def.XDCRCompTopologySvc
}

func NewAdminport(laddr string, xdcrRestPort, kvAdminPort uint16, finch chan bool, utilsIn utilities.UtilsIface, p2pMgr peerToPeer.P2PManager, securitySvc service_def.SecuritySvc, xdcrCompTopologySvc service_def.XDCRCompTopologySvc) *Adminport {
	//callback functions from GenServer
	var msg_callback_func gen_server.Msg_Callback_Func
	var exit_callback_func gen_server.Exit_Callback_Func
	var error_handler_func gen_server.Error_Handler_Func

	server := gen_server.NewGenServer(&msg_callback_func,
		&exit_callback_func, &error_handler_func, log.GetOrCreateContext(base.AdminPortKey), base.AdminPortKey, utilsIn)

	adminport := &Adminport{
		sourceKVHost:        laddr,
		xdcrRestPort:        xdcrRestPort,
		kvAdminPort:         kvAdminPort,
		GenServer:           server, /*gen_server.GenServer*/
		utils:               utilsIn,
		prometheusExporter:  pipeline_utils.NewPrometheusExporter(service_def.GlobalStatsTable, pipeline_utils.NewPrometheusLabelsTable),
		p2pMgr:              p2pMgr,
		securitySvc:         securitySvc,
		xdcrCompTopologySvc: xdcrCompTopologySvc,
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
	hostAddr := base.GetHostAddr(adminport.sourceKVHost, adminport.xdcrRestPort)
	server := ap.NewHTTPServer("xdcr", hostAddr, base.AdminportUrlPrefix, reqch, new(ap.Handler))

	startErrCh := server.Start()

	logger_ap.Infof("http server started %v !\n", hostAddr)

	// Start P2pHelper
	var p2pErr error
	p2pErrCh := make(chan error, 1)
	adminport.p2pAPI, p2pErr = adminport.p2pMgr.Start()
	if p2pErr != nil {
		logger_ap.Errorf("Starting peerToPeerManager resulted in err %v", p2pErr)
		p2pErrCh <- p2pErr
	}

	for {
		select {
		case err = <-p2pErrCh:
			goto done
		case err = <-startErrCh:
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
	// adminport must never stop
	adminport.p2pMgr.Stop()
	server.Stop()
	adminport.Stop_server()
	if err != nil {
		logger_ap.Errorf("adminport exited with error. err=%v\n", err)
	} else {
		logger_ap.Info("adminport exited !\n")
	}
	panic("adminport must be running, forcing panic to restart goxdcr")
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
	response, err = adminport.checkAndRejectChunkedEncoding(request)
	if err != nil || response != nil {
		return
	}

	key, err := adminport.GetMessageKeyFromRequest(request)
	if err != nil {
		if strings.Contains(err.Error(), base.RESTInvalidPath) && strings.Contains(err.Error(), base.RESTHttpReq) {
			return EncodeObjectIntoResponseWithStatusCode(err.Error(), http.StatusNotFound)
		} else {
			return nil, err
		}
	}
	logger_ap.Debugf("MessageKey=%v\n", key)

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
	case XDCRInternalSettingsPath + base.UrlDelimiter + base.MethodGet:
		response, err = adminport.doViewXDCRInternalSettingsRequest(request)
	case XDCRInternalSettingsPath + base.UrlDelimiter + base.MethodPost:
		response, err = adminport.doChangeXDCRInternalSettingsRequest(request)
	case XDCRPrometheusStatsPath + base.UrlDelimiter + base.MethodGet:
		response, err = adminport.doGetPrometheusStatsRequest(request, false)
	case XDCRPrometheusStatsHighPath + base.UrlDelimiter + base.MethodGet:
		response, err = adminport.doGetPrometheusStatsRequest(request, true)
	case base.XDCRPeerToPeerPath + base.UrlDelimiter + base.MethodPost:
		response, err = adminport.doPostPeerToPeerRequest(request)
	case base.XDCRConnectionPreCheckPath + base.UrlDelimiter + base.MethodPost:
		response, err = adminport.doPostConnectionPreCheckRequest(request)
	case base.XDCRConnectionPreCheckPath + base.UrlDelimiter + base.MethodGet:
		response, err = adminport.doGetConnectionPreCheckResultRequest(request)
	default:
		errOutput := base.InvalidPathInHttpRequestError(key)
		response, err = EncodeObjectIntoResponseWithStatusCode(errOutput.Error(), http.StatusNotFound)
	}
	if response != nil && (response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden) {
		go writeLocalAccessDeniedEvent(service_def.LocalAccessDeniedEventId, request, getRealUserIdFromRequest(request), getLocalAndRemoteIps(request), fmt.Sprintf("Request received http statusCode %v", response.StatusCode))
	}
	return response, err
}

func (adminport *Adminport) doGetRemoteClustersRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Debugf("doGetRemoteClustersRequest\n")

	response, err := authWebCreds(request, base.PermissionRemoteClusterRead)
	if response != nil || err != nil {
		return response, err
	}

	remoteClusterGetOpts := parseGetRemoteClusterRequestQuery(request)

	var remoteClusters map[string]*metadata.RemoteClusterReference
	if remoteClusterGetOpts.ShouldPopulateRemoteBucketManifest() {
		rcClone, err := RemoteClusterService().RemoteClusterByUuid(remoteClusterGetOpts.RemoteClusterUuid, false)
		if err != nil {
			return EncodeRemoteClusterErrorIntoResponse(err)
		}
		manifest, err := RemoteClusterService().GetManifestByUuid(remoteClusterGetOpts.RemoteClusterUuid, remoteClusterGetOpts.BucketManifestBucketName, false, true)
		if err != nil {
			return EncodeRemoteClusterErrorIntoResponse(err)
		}

		rcClone.TargetBucketManifest = make(map[string]*metadata.CollectionsManifest)
		rcClone.TargetBucketManifest[remoteClusterGetOpts.BucketManifestBucketName] = manifest
		remoteClusters = make(map[string]*metadata.RemoteClusterReference)
		remoteClusters[rcClone.Id()] = rcClone
	} else {
		remoteClusters, err = RemoteClusterService().RemoteClusters()
		if remoteClusterGetOpts.RedactRequested {
			remoteClustersClone := make(map[string]*metadata.RemoteClusterReference)
			for k, v := range remoteClusters {
				remoteClustersClone[k] = v.CloneAndRedact()
			}
			remoteClusters = remoteClustersClone
		}
		if err != nil {
			return EncodeRemoteClusterErrorIntoResponse(err)
		}
	}
	return NewGetRemoteClustersResponse(remoteClusters)
}

type getOptsCommon struct {
	RedactRequested bool
}

type getReplicationsOpt struct {
	getOptsCommon
}

type getRemoteClusterOpts struct {
	getOptsCommon
	BucketManifestBucketName string
	RemoteClusterUuid        string
}

func (g getRemoteClusterOpts) ShouldPopulateRemoteBucketManifest() bool {
	return g.BucketManifestBucketName != "" && g.RemoteClusterUuid != ""
}

func parseGetRemoteClusterRequestQuery(request *http.Request) getRemoteClusterOpts {
	var opt getRemoteClusterOpts
	if request == nil {
		return opt
	}

	query := request.URL.Query()
	if query == nil || len(query) == 0 {
		return opt
	}

	parseGetOptsCommon(query, &opt.getOptsCommon)

	for key, valArr := range query {
		if key == base.RemoteBucketManifest && len(valArr) == 1 {
			// Should be in the format of <remoteClusteRUUID>/<Bucketname>
			uuidAndBucketName := strings.Split(valArr[0], base.UrlDelimiter)
			if len(uuidAndBucketName) == 2 {
				opt.RemoteClusterUuid = uuidAndBucketName[0]
				opt.BucketManifestBucketName = uuidAndBucketName[1]
			}
		}
	}
	return opt
}

func parseGetOptsCommon(query url.Values, opt *getOptsCommon) {
	for key, valArr := range query {
		if key == base.RedactRequested && len(valArr) == 1 && strings.ToLower(valArr[0]) == "true" {
			opt.RedactRequested = true
		}
	}
}

func parseGetReplicationsRequestQuery(request *http.Request) getReplicationsOpt {
	var opt getReplicationsOpt
	if request == nil {
		return opt
	}

	query := request.URL.Query()
	if query == nil || len(query) == 0 {
		return opt
	}

	parseGetOptsCommon(query, &opt.getOptsCommon)

	return opt
}

func isAccessDeniedErr(err error) bool {
	if strings.Contains(err.Error(), "status 401") || strings.Contains(err.Error(), fmt.Sprintf(base.AuditStatusFmt, http.StatusUnauthorized)) ||
		strings.Contains(err.Error(), "status 403") || strings.Contains(err.Error(), fmt.Sprintf(base.AuditStatusFmt, http.StatusForbidden)) ||
		strings.Contains(err.Error(), base.AuditWrongCertificateErr) {
		return true
	} else {
		return false
	}
}
func (adminport *Adminport) doCreateRemoteClusterRequest(request *http.Request) (*ap.Response, error) {
	redactedRequest := base.CloneAndTagHttpRequest(request) // This is not an operation that occurs regularly
	logger_ap.Infof("doCreateRemoteClusterRequest req=%v\n", redactedRequest)
	defer logger_ap.Infof("Finished doCreateRemoteClusterRequest\n")

	response, err := authWebCreds(request, base.PermissionRemoteClusterWrite)
	if response != nil || err != nil {
		return response, err
	}

	remoteClusterService := RemoteClusterService()

	justValidate, remoteClusterRef, errorsMap, err := DecodeRemoteClusterRequest(request)
	if err != nil {
		return nil, err
	} else if len(errorsMap) > 0 {
		logger_ap.Errorf("Validation error in inputs. errorsMap=%v\n", errorsMap)
		return EncodeRemoteClusterErrorsMapIntoResponse(errorsMap)
	}

	logger_ap.Infof("Request params: justValidate=%v, remoteClusterRef=%v\n",
		justValidate, remoteClusterRef.CloneAndRedact())

	if justValidate {
		err = remoteClusterService.ValidateAddRemoteCluster(remoteClusterRef)
		return EncodeRemoteClusterErrorIntoResponse(err)
	} else {
		err = remoteClusterService.AddRemoteCluster(remoteClusterRef, false /*skipConnectivityValidation*/)
		if err != nil {
			if isAccessDeniedErr(err) {
				go writeRemoteAccessDeniedEvent(service_def.CreateRemoteAccessDeniedEventId, remoteClusterRef, getRealUserIdFromRequest(request), getLocalAndRemoteIps(request), err.Error())
			}
			return EncodeRemoteClusterErrorIntoResponse(err)
		} else {
			go writeRemoteClusterAuditEvent(service_def.CreateRemoteClusterRefEventId, remoteClusterRef, getRealUserIdFromRequest(request), getLocalAndRemoteIps(request))
			go writeRemoteClusterSystemEvent(service_def.CreateRemoteClusterRefSystemEventId, remoteClusterRef)
			return NewCreateRemoteClusterResponse(remoteClusterRef)
		}
	}
}

func (adminport *Adminport) doChangeRemoteClusterRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Infof("doChangeRemoteClusterRequest\n")
	defer logger_ap.Infof("Finished doChangeRemoteClusterRequest\n")

	response, err := authWebCreds(request, base.PermissionRemoteClusterWrite)
	if response != nil || err != nil {
		return response, err
	}

	remoteClusterName, err := DecodeDynamicParamInURL(request, base.RemoteClustersPath, "Remote Cluster Name")
	if err != nil {
		return EncodeRemoteClusterValidationErrorIntoResponse(err)
	}

	logger_ap.Infof("Request params: remoteClusterName=%v\n", remoteClusterName)

	justValidate, remoteClusterRef, errorsMap, err := DecodeRemoteClusterRequest(request)
	if err != nil {
		return nil, err
	} else if len(errorsMap) > 0 {
		logger_ap.Errorf("Validation error in inputs. errorsMap=%v\n", errorsMap)
		return EncodeRemoteClusterErrorsMapIntoResponse(errorsMap)
	}

	logger_ap.Infof("Request params: justValidate=%v, remoteClusterRef=%v\n",
		justValidate, remoteClusterRef.CloneAndRedact())

	remoteClusterService := RemoteClusterService()

	if justValidate {
		err = remoteClusterService.ValidateSetRemoteCluster(remoteClusterName, remoteClusterRef)
		return EncodeRemoteClusterErrorIntoResponse(err)
	} else {
		err = remoteClusterService.SetRemoteCluster(remoteClusterName, remoteClusterRef)
		if err != nil {
			if isAccessDeniedErr(err) {
				go writeRemoteAccessDeniedEvent(service_def.UpdateRemoteAccessDeniedEventId, remoteClusterRef, getRealUserIdFromRequest(request), getLocalAndRemoteIps(request), err.Error())
			}
			return EncodeRemoteClusterErrorIntoResponse(err)
		} else {
			go writeRemoteClusterAuditEvent(service_def.UpdateRemoteClusterRefEventId, remoteClusterRef, getRealUserIdFromRequest(request), getLocalAndRemoteIps(request))
			go writeRemoteClusterSystemEvent(service_def.UpdateRemoteClusterRefSystemEventId, remoteClusterRef)
			return NewCreateRemoteClusterResponse(remoteClusterRef)
		}
	}
}

func (adminport *Adminport) doDeleteRemoteClusterRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Infof("doDeleteRemoteClusterRequest\n")
	defer logger_ap.Infof("Finished doDeleteRemoteClusterRequest\n")

	response, err := authWebCreds(request, base.PermissionRemoteClusterWrite)
	if response != nil || err != nil {
		return response, err
	}

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
		if spec.TargetClusterUUID == ref.Uuid() {
			replIds = append(replIds, spec.Id)
		}
	}
	if len(replIds) > 0 {
		err = fmt.Errorf("Cannot delete remote cluster `%v` since it is referenced by replications %v", ref.Name(), replIds)
		return EncodeRemoteClusterValidationErrorIntoResponse(err)
	}

	ref, err = remoteClusterService.DelRemoteCluster(remoteClusterName)
	if err != nil {
		return EncodeRemoteClusterErrorIntoResponse(err)
	}

	go writeRemoteClusterAuditEvent(service_def.DeleteRemoteClusterRefEventId, ref, getRealUserIdFromRequest(request), getLocalAndRemoteIps(request))
	go writeRemoteClusterSystemEvent(service_def.DeleteRemoteClusterRefSystemEventId, ref)

	return NewOKResponse()
}

func (adminport *Adminport) doGetAllReplicationsRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Debugf("doGetAllReplicationsRequest\n")

	response, err := authWebCreds(request, base.PermissionXDCRInternalRead)
	if response != nil || err != nil {
		return response, err
	}

	replicationsGetOpts := parseGetReplicationsRequestQuery(request)

	replIds := replication_mgr.pipelineMgr.AllReplications()
	replSpecs := make(map[string]*metadata.ReplicationSpecification)
	for _, replId := range replIds {
		rep_status, _ := replication_mgr.pipelineMgr.ReplicationStatus(replId)
		if rep_status != nil {
			if replicationsGetOpts.RedactRequested {
				replSpecs[replId] = rep_status.Spec().CloneAndRedact()
			} else {
				replSpecs[replId] = rep_status.Spec()
			}
		}
	}

	return NewGetAllReplicationsResponse(replSpecs)
}

func (adminport *Adminport) doGetAllReplicationInfosRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Debugf("doGetAllReplicationInfosRequest\n")

	response, err := authWebCreds(request, base.PermissionXDCRAdminInternalRead)
	if response != nil || err != nil {
		return response, err
	}

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

	response, err := authWebCreds(request, constructBucketPermission(fromBucket, base.PermissionBucketXDCRWriteSuffix))
	if response != nil || err != nil {
		return response, err
	}

	logger_ap.Infof("Request parameters: justValidate=%v, fromBucket=%v, toCluster=%v, toBucket=%v, settings=%v\n",
		justValidate, fromBucket, toCluster, toBucket, settings.CloneAndRedact())

	replicationId, errorsMap, err, warnings := CreateReplication(justValidate, fromBucket, toCluster, toBucket, settings,
		getRealUserIdFromRequest(request), getLocalAndRemoteIps(request))

	if err != nil {
		return EncodeReplicationSpecErrorIntoResponse(err)
	} else if len(errorsMap) > 0 {
		logger_ap.Errorf("Error creating replication. errorsMap=%v\n", errorsMap)
		return EncodeErrorsMapIntoResponse(errorsMap, true)
	} else {
		return NewCreateReplicationResponse(replicationId, warnings, justValidate)
	}
}

func (adminport *Adminport) doDeleteReplicationRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Infof("doDeleteReplicationRequest\n")
	defer logger_ap.Infof("Finished doDeleteReplicationRequest\n")

	replicationId, err := DecodeDynamicParamInURL(request, DeleteReplicationPrefix, "Replication Id")
	if err != nil {
		return EncodeReplicationValidationErrorIntoResponse(err)
	}

	logger_ap.Infof("Request params: replicationId=%v\n", replicationId)

	response, err := authWebCredsForReplication(request, replicationId, []string{base.PermissionBucketXDCRWriteSuffix})
	if response != nil || err != nil {
		return response, err
	}

	err = DeleteReplication(replicationId, getRealUserIdFromRequest(request), getLocalAndRemoteIps(request))

	if err != nil {
		return EncodeReplicationSpecErrorIntoResponse(err)
	} else {
		return NewEmptyArrayResponse()
	}
}

func (adminport *Adminport) doViewDefaultReplicationSettingsRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Infof("doViewDefaultReplicationSettingsRequest\n")

	response, err := authWebCreds(request, base.PermissionXDCRSettingsRead)
	if response != nil || err != nil {
		return response, err
	}

	defaultSettings, err := ReplicationSettingsService().GetDefaultReplicationSettings()
	if err != nil {
		return nil, err
	}
	// default process settings
	defaultProcessSetting, err := GlobalSettingsService().GetGlobalSettings()

	if err != nil {
		return nil, err
	}

	return NewDefaultReplicationSettingsResponse(defaultSettings, defaultProcessSetting)
}

func (adminport *Adminport) doChangeDefaultReplicationSettingsRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Infof("doChangeDefaultReplicationSettingsRequest\n")

	response, err := authWebCreds(request, base.PermissionXDCRSettingsWrite)
	if response != nil || err != nil {
		return response, err
	}

	// Changing default replication settings is not done on the UI normally but through REST or CLI so no need for warnings
	justValidate, settingsMap, errorsMap, _ := DecodeChangeReplicationSettings(request, "")
	if len(errorsMap) > 0 {
		logger_ap.Errorf("Validation error in inputs. errorsMap=%v\n", errorsMap)
		return EncodeErrorsMapIntoResponse(errorsMap, false)
	}

	logger_ap.Infof("Request params: justValidate=%v, inputSettings=%v\n", justValidate, settingsMap.CloneAndRedact())

	if !justValidate {
		errorsMap, err := UpdateDefaultSettings(settingsMap, getRealUserIdFromRequest(request), getLocalAndRemoteIps(request))
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
	defaultProcessSetting, err := GlobalSettingsService().GetGlobalSettings()

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

	logger_ap.Infof("Request params: replicationId=%v", replicationId)

	response, err := authWebCredsForReplication(request, replicationId, []string{base.PermissionBucketXDCRReadSuffix})
	if response != nil || err != nil {
		return response, err
	}

	// read replication spec with the specified replication id
	replSpec, err := ReplicationSpecService().ReplicationSpec(replicationId)
	if err != nil {
		return EncodeReplicationSpecErrorIntoResponse(err)
	}

	// marshal replication settings in replication spec and return it
	return NewReplicationSettingsResponse(replSpec.Settings, nil, false)
}

func (adminport *Adminport) doChangeReplicationSettingsRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Infof("doChangeReplicationSettingsRequest\n")

	// get input parameters from request
	replicationId, err := DecodeDynamicParamInURL(request, SettingsReplicationsPath, "Replication Id")
	if err != nil {
		return EncodeReplicationValidationErrorIntoResponse(err)
	}
	justValidate, settingsMap, errorsMap, includeWarnings := DecodeChangeReplicationSettings(request, replicationId)
	logger_ap.Infof("Request params: replicationId=%v justValidate=%v includeWarnings=%v", replicationId, justValidate, includeWarnings)
	if len(errorsMap) > 0 {
		logger_ap.Errorf("Validation error in inputs. errorsMap=%v", errorsMap)
		return EncodeErrorsMapIntoResponse(errorsMap, false)
	}

	logger_ap.Infof("Request params: justValidate=%v includeWarnings=%v inputSettings=%v", justValidate, includeWarnings, settingsMap.CloneAndRedact())

	// "pauseRequested" setting is special - it requires execute permission
	_, pauseRequestedSpecified := settingsMap[metadata.ActiveKey]
	// all other settings require write permission
	otherSettingsSpecified := (!pauseRequestedSpecified && len(settingsMap) > 0) || (pauseRequestedSpecified && len(settingsMap) > 1)
	permissionSuffices := make([]string, 0)
	if pauseRequestedSpecified {
		permissionSuffices = append(permissionSuffices, base.PermissionBucketXDCRExecuteSuffix)
	}
	// the "!pauseRequestedSpecified" clause is to ensure that write permission is checked when no settings have been specified,
	// just to be safe
	if otherSettingsSpecified || !pauseRequestedSpecified {
		permissionSuffices = append(permissionSuffices, base.PermissionBucketXDCRWriteSuffix)
	}

	response, err := authWebCredsForReplication(request, replicationId, permissionSuffices)
	if response != nil || err != nil {
		return response, err
	}

	if !justValidate {
		err = adminport.performOnetimeUserActions(settingsMap, replicationId)
		if err != nil {
			return EncodeReplicationSpecErrorIntoResponse(err)
		}
	}
	cleanupTempReplicationSettingKeys(settingsMap)

	errorsMap, err, warnings := UpdateReplicationSettings(replicationId, settingsMap, getRealUserIdFromRequest(request), getLocalAndRemoteIps(request), justValidate)
	if err != nil {
		logger_ap.Errorf("UpdateReplicationSettings error=%v", err)
		return nil, err
	} else if len(errorsMap) > 0 {
		logger_ap.Errorf("Validation error in inputs. errorsMap=%v\n", errorsMap)
		return EncodeErrorsMapIntoResponse(errorsMap, false)
	}

	if justValidate {
		logger_ap.Info("Done with doChangeReplicationSettingsRequest (justValidate)")
		// At this point, no error, just return warnings if any
		return NewReplicationSettingsResponse(nil, warnings, includeWarnings)
	}

	// return replication settings after changes
	replSpec, err := ReplicationSpecService().ReplicationSpec(replicationId)
	if err != nil {
		return EncodeReplicationSpecErrorIntoResponse(err)
	}

	logger_ap.Info("Done with doChangeReplicationSettingsRequest")
	return NewReplicationSettingsResponse(replSpec.Settings, warnings, includeWarnings)
}

func ForceManualBackfillRequest(replId string, incomingReq string) error {
	replSpec, err := ReplicationSpecService().ReplicationSpec(replId)
	if err != nil {
		return err
	}

	settingsMap, err := metadata.ParseBackfillIntoSettingMap(incomingReq, replSpec)
	if err != nil {
		return err
	}

	err = BackfillManager().GetPipelineSvc().UpdateSettings(settingsMap)
	if err != nil {
		logger_ap.Warnf("force backfill returned %v\n", err)
	}
	return nil
}

func (adminport *Adminport) performOnetimeUserActions(settingsMap metadata.ReplicationSettingsMap, replicationId string) error {
	var err error
	var skipP2P bool

	srcClusterCompat, err := adminport.xdcrCompTopologySvc.MyClusterCompatibility()
	skipP2P = err != nil || !base.IsClusterCompatible(srcClusterCompat, base.VersionForP2PManifestSharing)

	backfillRequest, manualBackfillRequest := settingsMap[metadata.CollectionsManualBackfillKey]
	if manualBackfillRequest {
		logger_ap.Infof("force manual backfill has been requested given %v", backfillRequest.(string))
		backfillReqStr := backfillRequest.(string)
		err = ForceManualBackfillRequest(replicationId, backfillReqStr)
		if err != nil {
			return err
		}
		if !skipP2P {
			err = adminport.p2pMgr.SendManualBackfill(replicationId, backfillReqStr)
			if err != nil {
				return err
			}
		}
	}

	vbno, delSpecificVbBackfillRequested := settingsMap[metadata.CollectionsDelVbBackfillKey]
	if delSpecificVbBackfillRequested {
		logger_ap.Infof("delete specific vb %v backfill has been requested", vbno.(int))
		err = DelSpecificBackfillRequest(replicationId, vbno.(int))
		if err != nil {
			return err
		}
	}

	_, delBackfillsRequested := settingsMap[metadata.CollectionsDelAllBackfillKey]
	if delBackfillsRequested {
		logger_ap.Infof("delete backfills has been requested")
		err = DelAllBackfillsRequest(replicationId)
		if err != nil {
			return err
		}
		if !skipP2P {
			err = adminport.p2pMgr.SendDelBackfill(replicationId)
			if err != nil {
				return err
			}
		}
	}

	eventId, dismissEventRequested := settingsMap[metadata.DismissEventKey]
	if dismissEventRequested {
		logger_ap.Infof("Dismiss event has been requested for %v, type: %v", eventId, reflect.TypeOf(eventId))
		replication_mgr.pipelineMgr.DismissEventForPipeline(replicationId, eventId.(int))
		return nil
	}
	return nil
}

func DelSpecificBackfillRequest(replId string, vbno int) error {
	settingsMap := make(map[string]interface{})
	settingsMap[base.NameKey] = replId
	settingsMap[metadata.CollectionsDelVbBackfillKey] = vbno
	err := BackfillManager().GetPipelineSvc().UpdateSettings(settingsMap)
	if err != nil {
		logger_ap.Warnf("del specific backfill returned %v\n", err)
	}
	return err
}

func DelAllBackfillsRequest(replId string) error {
	settingsMap := metadata.ParseDelBackfillIntoSettingMap(replId)
	err := BackfillManager().GetPipelineSvc().UpdateSettings(settingsMap)
	if err != nil {
		logger_ap.Warnf("del backfill returned %v\n", err)
	}
	return err
}

// get statistics for all running replications
func (adminport *Adminport) doGetStatisticsRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Debugf("doGetStatisticsRequest\n")

	response, err := authWebCreds(request, base.PermissionXDCRAdminInternalRead)
	if response != nil || err != nil {
		return response, err
	}

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

	response, err := authWebCreds(request, base.PermissionXDCRAdminInternalRead)
	if response != nil || err != nil {
		return response, err
	}

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
		return "", base.InvalidPathInHttpRequestError(r.URL.Path)
	} else {
		// add http method suffix to name to ensure uniqueness
		key += base.UrlDelimiter + strings.ToUpper(r.Method)

		logger_ap.Debugf("Request key decoded: %v\n", key)

		return key, nil
	}
}

func authenticateRequest(request *http.Request) (cbauth.Creds, error) {
	redactedRequest := base.CloneAndTagHttpRequest(request)
	var err error
	creds, err := cbauth.AuthWebCreds(request)
	if err != nil {
		logger_ap.Errorf("Error authenticating request. request=%v\n err= %v\n", redactedRequest, err)
		return nil, err
	}

	if logger_ap.GetLogLevel() >= log.LogLevelDebug {
		logger_ap.Debugf("request url=%v, creds user = %v%v%v\n", request.URL, base.UdTagBegin, creds.Name(), base.UdTagEnd)
	}
	return creds, nil
}

func authorizeRequest(creds cbauth.Creds, permission string) (bool, error) {
	allowed, err := creds.IsAllowed(permission)
	if err != nil {
		logger_ap.Errorf("Error occurred when checking for permission %v for creds %v%v%v. err=%v\n", permission, base.UdTagBegin, creds.Name(), base.UdTagEnd, err)
	}

	return allowed, err
}

// returns error if credentials in request do not have the specified permission
func authWebCreds(request *http.Request, permission string) (*ap.Response, error) {
	creds, err := authenticateRequest(request)

	if err != nil {
		return EncodeErrorMessageIntoResponse(err, http.StatusUnauthorized)
	}

	allowed, err := authorizeRequest(creds, permission)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return EncodeAuthorizationErrorMessageIntoResponse(permission)
	}

	return nil, nil
}

// returns error if credentials in request do not have all of the specified permissions
func authWebCredsForReplication(request *http.Request, replicationId string, permissionSuffices []string) (*ap.Response, error) {
	creds, err := authenticateRequest(request)

	if err != nil {
		return EncodeErrorMessageIntoResponse(err, http.StatusUnauthorized)
	}

	sourceBucket, err := metadata.GetSourceBucketNameFromReplicationId(replicationId)
	if err != nil {
		return EncodeReplicationValidationErrorIntoResponse(err)
	}

	permissions := make([]string, 0)
	for _, permissionSuffix := range permissionSuffices {
		permission := constructBucketPermission(sourceBucket, permissionSuffix)
		permissions = append(permissions, permission)
	}

	allowed := true
	for _, permission := range permissions {
		allowed, err = authorizeRequest(creds, permission)
		if err != nil {
			return nil, err
		}
		if !allowed {
			break
		}
	}

	if !allowed {
		if len(permissions) == 1 {
			return EncodeAuthorizationErrorMessageIntoResponse(permissions[0])
		} else {
			return EncodeAuthorizationErrorMessageIntoResponse2(permissions)
		}
	}

	// "nil, nil" is the only return value that would let caller proceed
	return nil, nil
}

func constructBucketPermission(bucketName, suffix string) string {
	return base.PermissionBucketPrefix + bucketName + suffix
}

func writeRemoteClusterSystemEvent(eventId service_def.EventIdType, ref *metadata.RemoteClusterReference) {
	var encryptionType string
	if ref.DemandEncryption() {
		encryptionType = ref.EncryptionType()
	} else {
		encryptionType = "plain"
	}
	args := make(map[string]string)
	args[base.RemoteClusterName] = ref.Name()
	args[base.RemoteClusterHostName] = ref.HostName()
	args[base.RemoteClusterUuid] = ref.Uuid()
	args[base.RemoteClusterEncryptionType] = encryptionType
	EventlogService().WriteEvent(eventId, args)
}

func writeRemoteClusterAuditEvent(eventId uint32, remoteClusterRef *metadata.RemoteClusterReference, realUserId *service_def.RealUserId, ips *service_def.LocalRemoteIPs) {
	event := &service_def.RemoteClusterRefEvent{
		GenericFields:         service_def.GenericFields{Timestamp: log.FormatTimeWithMilliSecondPrecision(time.Now()), RealUserid: *realUserId, LocalRemoteIPs: *ips},
		RemoteClusterName:     remoteClusterRef.Name(),
		RemoteClusterHostname: remoteClusterRef.HostName(),
		IsEncrypted:           remoteClusterRef.DemandEncryption(),
		EncryptionType:        remoteClusterRef.EncryptionType()}

	err := AuditService().Write(eventId, event)
	logAuditErrors(err)
}

func writeRemoteAccessDeniedEvent(eventId uint32, remoteClusterRef *metadata.RemoteClusterReference, realUserId *service_def.RealUserId, ips *service_def.LocalRemoteIPs, errorStr string) {
	event := &service_def.RemoteClusterAccessDeniedEvent{
		GenericFields:         service_def.GenericFields{Timestamp: log.FormatTimeWithMilliSecondPrecision(time.Now()), RealUserid: *realUserId, LocalRemoteIPs: *ips},
		RemoteClusterName:     remoteClusterRef.Name(),
		RemoteClusterHostname: remoteClusterRef.HostName(),
		RemoteUserName:        remoteClusterRef.UserName(),
		EncryptionType:        remoteClusterRef.EncryptionType(),
		ErrorMessage:          errorStr,
	}
	err := AuditService().Write(eventId, event)
	logAuditErrors(err)
}

func writeLocalAccessDeniedEvent(eventId uint32, request *http.Request, realUserId *service_def.RealUserId, ips *service_def.LocalRemoteIPs, errorStr string) {
	event := &service_def.LocalClusterAccessDeniedEvent{
		GenericFields: service_def.GenericFields{Timestamp: log.FormatTimeWithMilliSecondPrecision(time.Now()), RealUserid: *realUserId, LocalRemoteIPs: *ips},
		Request:       request.RequestURI,
		Method:        request.Method,
		ErrorMessage:  errorStr,
	}
	err := AuditService().Write(eventId, event)
	logAuditErrors(err)
}

func getRealUserIdFromRequest(request *http.Request) *service_def.RealUserId {
	creds, err := cbauth.AuthWebCreds(request)
	if err != nil {
		logger_rm.Errorf("Error getting real user id from http request. err=%v\n", err)
		// put unknown user in the audit log.
		return &service_def.RealUserId{"local", "unknown"}
	}

	return &service_def.RealUserId{creds.Domain(), creds.Name()}
}

func getLocalAndRemoteIps(request *http.Request) *service_def.LocalRemoteIPs {
	// remote - refers to the machine (ip and port) that caused the event to be triggered
	// The request.RemoteAddr should always be in IP:port format according to go documentation
	remoteIP := base.GetHostName(request.RemoteAddr)
	remotePortNo, _ := base.GetPortNumber(request.RemoteAddr)

	// local - refers to the machine (ip and port) that generated the audit event (the one XDCR is sitting on)
	var hostNameWithoutPort string
	var port uint16
	var portErr = base.ErrorNoPortNumber
	ctx := request.Context()
	addr, ok := ctx.Value(http.LocalAddrContextKey).(net.Addr)
	if ok && addr != nil && addr.String() != "" {
		hostNameWithoutPort = base.GetHostName(addr.String())
		port, portErr = base.GetPortNumber(addr.String())
	}

	if portErr != nil {
		// request.Host should contain portNo. If err, then use default admin port
		port = base.DefaultAdminPort
	}

	var localIP string
	var localPort uint16
	if len(hostNameWithoutPort) > 0 {
		ipCheck := net.ParseIP(hostNameWithoutPort)
		if ipCheck != nil {
			// The hostname being used is already an IP
			localIP = hostNameWithoutPort
			localPort = port
		} else {
			ipLookup, err := net.LookupIP(hostNameWithoutPort)
			if err != nil || len(ipLookup) == 0 {
				// Error case - just manually find the local IP and admin port
				ifaces, err := net.Interfaces()
				if err == nil {
				FINDIPLOOP:
					for _, i := range ifaces {
						addrs, err := i.Addrs()
						if err == nil {
							for _, addr := range addrs {
								var ip net.IP
								switch v := addr.(type) {
								case *net.IPNet:
									ip = v.IP
								case *net.IPAddr:
									ip = v.IP
								}
								localIP = ip.String()
								localPort = port
								break FINDIPLOOP
							}
						}
					}
				}
				if localIP == "" {
					// worst case scenario - empty fields
					localIP = ""
					localPort = 1
				}
			} else {
				localIP = ipLookup[0].String()
				localPort = port
			}
		}
	}

	remote := &service_def.IpAndPort{
		Ip:   remoteIP,
		Port: remotePortNo,
	}

	local := &service_def.IpAndPort{
		Ip:   localIP,
		Port: localPort,
	}
	iPsToReturn := &service_def.LocalRemoteIPs{
		Remote: remote,
		Local:  local,
	}
	return iPsToReturn
}

func (adminport *Adminport) IsReadyForHeartBeat() bool {
	return adminport.IsStarted()
}

func (adminport *Adminport) doRegexpValidationRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Infof("doRegexpValidationRequest\n")

	expression, docId, bucket, collectionNs, skipDoc, err := DecodeRegexpValidationRequest(request)
	if err != nil {
		return EncodeErrorMessageIntoResponse(err, http.StatusBadRequest)
	}

	response, err := authWebCreds(request, constructBucketPermission(bucket, base.PermissionBucketDataReadSuffix))
	if response != nil || err != nil {
		return response, err
	}

	logger_ap.Infof("Request params: expression=%v%v%v docId=%v%v%v bucket=%v scope=%v collection=%v skipDoc=%v",
		base.UdTagBegin, expression, base.UdTagEnd,
		base.UdTagBegin, docId, base.UdTagEnd,
		bucket, collectionNs.ScopeName, collectionNs.CollectionName, skipDoc)

	if skipDoc {
		// Only care about expression validation
		err := base.ValidateAdvFilter(expression)
		return NewRegexpValidationResponse(err == nil, err)
	} else {
		return NewRegexpValidationResponse(adminport.utils.FilterExpressionMatchesDoc(expression, docId, bucket, collectionNs, adminport.sourceKVHost, adminport.kvAdminPort))
	}
}

func (adminport *Adminport) doStartBlockProfile(request *http.Request) (*ap.Response, error) {
	response, err := authWebCreds(request, base.PermissionXDCRAdminInternalWrite)
	if response != nil || err != nil {
		return response, err
	}

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
	response, err := authWebCreds(request, base.PermissionXDCRAdminInternalWrite)
	if response != nil || err != nil {
		return response, err
	}

	runtime.SetBlockProfileRate(-1)
	logger_ap.Info("doStopBlockProfile")
	return NewEmptyArrayResponse()
}

func (adminport *Adminport) doViewXDCRInternalSettingsRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Infof("doViewXDCRInternalSettingsRequest\n")

	response, err := authWebCreds(request, base.PermissionXDCRInternalRead)
	if response != nil || err != nil {
		return response, err
	}

	internalSettings := InternalSettingsService().GetInternalSettings()

	return NewXDCRInternalSettingsResponse(internalSettings)
}

func (adminport *Adminport) doChangeXDCRInternalSettingsRequest(request *http.Request) (*ap.Response, error) {
	logger_ap.Infof("doChangeXDCRInternalSettingsRequest\n")

	response, err := authWebCreds(request, base.PermissionXDCRInternalWrite)
	if response != nil || err != nil {
		return response, err
	}

	settingsMap, errorsMap := DecodeSettingsFromXDCRInternalSettingsRequest(request)
	if len(errorsMap) > 0 {
		logger_ap.Errorf("Validation error in inputs. errorsMap=%v\n", errorsMap)
		return EncodeErrorsMapIntoResponse(errorsMap, false)
	}

	logger_ap.Infof("Request params: xdcrInternalSettings=%v\n", settingsMap.CloneAndRedact())

	internalSettings, errorsMap, err := InternalSettingsService().UpdateInternalSettings(settingsMap)
	if len(errorsMap) > 0 {
		logger_ap.Errorf("Validation error in inputs. errorsMap=%v\n", errorsMap)
		return EncodeErrorsMapIntoResponse(errorsMap, false)
	}

	if err != nil {
		logger_ap.Errorf("Error updating xdcr internal settings. err=%v\n", err)
		return nil, err
	}

	return NewXDCRInternalSettingsResponse(internalSettings)
}

func (adminport *Adminport) doGetPrometheusStatsRequest(request *http.Request, highCardinality bool) (*ap.Response, error) {
	if highCardinality {
		logger_ap.Debugf("doGetPrometheusStatsRequest high\n")
	} else {
		logger_ap.Debugf("doGetPrometheusStatsRequest\n")
	}

	response, err := authWebCreds(request, base.PermissionXDCRPrometheusRead)
	if response != nil || err != nil {
		return response, err
	}

	if highCardinality {
		// XDCR currently has no high-cardinality stats
		return EncodeByteArrayIntoPrometheusResponseWithStatusCode([]byte{}, http.StatusOK)
	}

	expVarMap, err := GetAllStatistics()
	if err != nil {
		return nil, err
	}
	adminport.prometheusExporter.LoadExpVarMap(expVarMap)
	outputBytes, err := adminport.prometheusExporter.Export()
	if err != nil {
		return EncodeErrorMessageIntoResponse(err, http.StatusInternalServerError)
	}
	return EncodeByteArrayIntoPrometheusResponseWithStatusCode(outputBytes, http.StatusOK)
}

func (adminport *Adminport) checkAndRejectChunkedEncoding(request *http.Request) (*ap.Response, error) {
	for _, encodingString := range request.TransferEncoding {
		if encodingString == base.RESTHttpChunkedEncoding {
			// With chunked encoding, reading or closing the body will hang. So force clean the request
			request.ContentLength = 0
			request.TransferEncoding = []string{}
			request.Body = nil
			return EncodeErrorMessageIntoResponse(base.ErrorChunkedEncodingNotSupported, http.StatusBadRequest)
		}
	}
	return nil, nil
}

func (adminport *Adminport) doPostPeerToPeerRequest(request *http.Request) (*ap.Response, error) {
	localRemoteIPs := getLocalAndRemoteIps(request) // The remoteIP is not really accurate since ns_server redirects
	response, err := authWebCreds(request, base.PermissionXDCRAdminInternalRead)
	if response != nil || err != nil {
		return response, err
	}
	req, err := peerToPeer.GenerateP2PReqOrResp(request, adminport.utils, adminport.securitySvc, adminport.Logger())
	if err != nil {
		adminport.Logger().Errorf("Unable to generate req or resp from %v err: %v\n", localRemoteIPs.Remote, err)
		return EncodeErrorMessageIntoResponse(err, http.StatusInternalServerError)
	}
	logger_ap.Infof("doPostPeerToPeerRequest of type %v from %v", req.GetOpcode(), req.GetSender())

	handlerResult, err := adminport.p2pAPI.P2PReceive(req)
	if err != nil {
		adminport.Logger().Errorf("P2PReceive from %v resulted in err %v", req.GetSender(), err)
		return EncodeErrorMessageIntoResponse(err, http.StatusInternalServerError)
	}
	return EncodeObjectIntoResponseWithStatusCode(handlerResult.GetError(), handlerResult.GetHttpStatusCode())
}

/* Connection Pre-check */

func (adminport *Adminport) doPostConnectionPreCheckRequest(request *http.Request) (*ap.Response, error) {
	redactedRequest := base.CloneAndTagHttpRequest(request)
	logger_ap.Infof("doPostConnectionPreCheckRequest req=%v", redactedRequest)
	defer logger_ap.Infof("Finished doPostConnectionPreCheckRequest")

	response, err := authWebCreds(request, base.PermissionRemoteClusterWrite)
	if response != nil || err != nil {
		return response, err
	}

	remoteClusterRef, errorsMap, err := DecodePostConnectionPreCheckRequest(request)
	if err != nil {
		return nil, err
	} else if len(errorsMap) > 0 {
		logger_ap.Errorf("Validation error in inputs. errorsMap=%v", errorsMap)
		return EncodeRemoteClusterErrorsMapIntoResponse(errorsMap)
	} else if remoteClusterRef == nil {
		logger_ap.Errorf("Invalid Remote Cluster Ref=%v", remoteClusterRef)
		return nil, fmt.Errorf("Remote cluster Ref is null")
	}

	taskId := generateTaskId(adminport.sourceKVHost)

	logger_ap.Infof("Request params: remoteClusterRef=%v; Task ID generated: %v", remoteClusterRef.CloneAndRedact(), taskId)

	response, err = NewConnectionPreCheckPostResponse(remoteClusterRef.HostName(), remoteClusterRef.UserName(), taskId)
	if err != nil {
		return nil, err
	}

	go adminport.p2pMgr.SendConnectionPreCheckRequest(remoteClusterRef, RemoteClusterService().InitRemoteClusterReference, taskId)

	return response, err
}

func (adminport *Adminport) doGetConnectionPreCheckResultRequest(request *http.Request) (*ap.Response, error) {
	redactedRequest := base.CloneAndTagHttpRequest(request)
	logger_ap.Infof("doGetConnectionPreCheckResultRequest req=%v", redactedRequest)
	defer logger_ap.Infof("Finished doGetConnectionPreCheckResultRequest")
	taskId, err := DecodeGetConnectionPreCheckResultRequest(request)
	if err != nil {
		return nil, err
	}
	res, done, err := adminport.p2pMgr.RetrieveConnectionPreCheckResult(taskId)

	return NewConnectionPreCheckGetResponse(taskId, res, done)
}
