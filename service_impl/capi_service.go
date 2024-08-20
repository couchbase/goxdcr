/*
Copyright 2015-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package service_impl

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
	utilities "github.com/couchbase/goxdcr/v8/utils"
)

const GoxdcrPrefix = "_goxdcr/"
const BucketUuidPrefix = "?bucket_uuid="

const (
	PRE_REPLICATE_CMD       string = "_pre_replicate"
	MASS_VBOPAQUE_CHECK_CMD string = "_mass_vbopaque_check"
	COMMIT_FOR_CKPT_CMD     string = "_commit_for_checkpoint"
)

// errors
var NO_VB_OPAQUE_IN_RESP_ERR error = errors.New("No vb opaque in the response")

// apiRequest is a structure for http request used for CAPI
type apiRequest struct {
	url               string
	path              string
	username          string
	password          string
	body              base.InterfaceMap
	httpAuthMech      base.HttpAuthMech
	certificate       []byte
	SANInCertificate  bool
	clientCertificate []byte
	clientKey         []byte
}

// Shallow copy bare bones information for redaction
func (req *apiRequest) CloneLite() *apiRequest {
	clonedReq := &apiRequest{}
	if req != nil {
		*clonedReq = *req
		clonedReq.certificate = nil
	}
	return clonedReq
}

func (req *apiRequest) redact() *apiRequest {
	if req != nil {
		if !base.IsStringRedacted(req.username) {
			req.username = base.TagUD(req.username)
		}
		if !base.IsStringRedacted(req.password) {
			req.password = base.TagUD(req.password)
		}
	}
	return req
}

func (req *apiRequest) CloneAndRedact() *apiRequest {
	if req != nil {
		return req.CloneLite().redact()
	}
	return req
}

// CAPIService is a wrapper around the rest interface provided by couchbase server
// It provide the following methods:
//  1. _pre_preplicate: check if the checkpoint on-file for a vb is valid on remote cluster
//  2. _mass_vbopaque_check: mass check if vb uuid on the remote cluster is the same as the one on-file for a list of vbs
//  3. _commit_for_checkpoint: ask the remote vbucket to commit and return back the seqno, or if the remote vbucket's UUID
//     has changed due to the topology change, in that case, new vb UUID would be returned
type CAPIService struct {
	logger *log.CommonLogger
	utils  utilities.UtilsIface
}

func NewCAPIService(logger_ctx *log.LoggerContext, utilsIn utilities.UtilsIface) *CAPIService {
	return &CAPIService{
		logger: log.NewLogger("CapiSvc", logger_ctx),
		utils:  utilsIn,
	}
}

// PrePrelicate (_pre_replicate)
// Parameters: remoteBucket - the information about the remote bucket
//
//	knownRemoteVBStatus - the current replication status of a vbucket
//	disableCkptBackwardsCompat
//
// returns:
//
//	bMatch - true if the remote vbucket matches the current replication status
//	current_remoteVBUUID - new remote vb uuid might be retured if bMatch = false and there was a topology change on remote vb
//	err
//
// Refer to ns_server/deps/ns_couchdb/src/capi_replication.erl for server side source code
func (capi_svc *CAPIService) PreReplicate(remoteBucket *service_def.RemoteBucketInfo,
	knownRemoteVBStatus *service_def.RemoteVBReplicationStatus, xdcrCheckpointingCapbility bool) (bMatch bool, current_remoteVBOpaque metadata.TargetVBOpaque, err error) {
	api_base, err := capi_svc.composeAPIRequestBaseForVb(remoteBucket, knownRemoteVBStatus.VBNo, PRE_REPLICATE_CMD)
	if err != nil {
		return
	}
	err = capi_svc.composePreReplicateBody(api_base, knownRemoteVBStatus)
	if err != nil {
		return
	}

	if capi_svc.logger.GetLogLevel() >= log.LogLevelDebug {
		capi_svc.logger.Debugf("request to _pre_replicate = %v\n", api_base.CloneAndRedact())
	}
	http_client := remoteBucket.RestAddrHttpClientMap[api_base.url]
	status_code, respMap, err := capi_svc.send_post(api_base, http_client, base.MaxRetryCapiService)
	capi_svc.logger.Debugf("response from _pre_replicate is status_code=%v respMap=%v for %v\n", status_code, respMap, knownRemoteVBStatus)
	if err != nil {
		capi_svc.logger.Errorf("Calling _pre_replicate on %v failed for vb=%v, err=%v\n", api_base.url, knownRemoteVBStatus.VBNo, err)
		return
	}

	bMatch, current_remoteVBOpaque, err = capi_svc.parsePreReplicateResp(api_base.url, status_code, respMap, knownRemoteVBStatus.VBNo, xdcrCheckpointingCapbility)

	if err == nil {
		capi_svc.logger.Debugf("_pre_replicate succeeded for vb=%v\n", knownRemoteVBStatus.VBNo)
	}
	return

}

// CommitForCheckpoint (_commit_for_checkpoint)
// Parameters: remoteBucket - the information about the remote bucket
//
//	remoteVBUUID - the remote vb uuid on file
//	vbno		   - the vb number
//
// returns:	  remote_seqno - the remote vbucket's high sequence number
//
//	vb_uuid	   - the new vb uuid if there was a topology change
//	err
func (capi_svc *CAPIService) CommitForCheckpoint(remoteBucket *service_def.RemoteBucketInfo, remoteVBOpaque metadata.TargetVBOpaque, vbno uint16) (remote_seqno uint64, vbOpaque metadata.TargetVBOpaque, err error) {
	api_base, err := capi_svc.composeAPIRequestBaseForVb(remoteBucket, vbno, COMMIT_FOR_CKPT_CMD)
	if err != nil {
		return
	}
	api_base.body["vb"] = vbno
	api_base.body["vbopaque"] = remoteVBOpaque.Value()
	http_client := remoteBucket.RestAddrHttpClientMap[api_base.url]
	status_code, respMap, err := capi_svc.send_post(api_base, http_client, base.MaxRetryCapiService)

	if err == nil && status_code == 400 {
		vbOpaque, err := getVBOpaqueFromRespMap(status_code, respMap, vbno)
		if err != nil {
			return 0, nil, err
		}

		return 0, vbOpaque, service_def.VB_OPAQUE_MISMATCH_ERR
	} else if err == nil && status_code == 200 {
		commitOpaque, ok := respMap["commitopaque"]
		if !ok {
			errMsg := fmt.Sprintf("No commitopaque found in resp. respMap=%v, err=%v", respMap, err)
			capi_svc.logger.Errorf(errMsg)
			return 0, nil, errors.New(errMsg)
		}

		commitOpaquePair, ok := commitOpaque.([]interface{})
		if ok {
			if len(commitOpaquePair) != 2 {
				errMsg := fmt.Sprintf("invalid commitopaque found in resp. respMap=%v, err=%v\n", respMap, err)
				capi_svc.logger.Errorf(errMsg)
				return 0, nil, errors.New(errMsg)
			}
			remote_seqno = uint64(commitOpaquePair[1].(float64))
		} else {
			// older cluster may return a commit opaque consisting of a string with remote vbuuid value. leave remote_seqno as 0 in this case
			_, ok := commitOpaque.(string)
			if !ok {
				errMsg := fmt.Sprintf("invalid commitopaque found in resp. respMap=%v, err=%v\n", respMap, err)
				capi_svc.logger.Errorf(errMsg)
				return 0, nil, errors.New(errMsg)
			}
		}

	} else {
		//error case
		msg := fmt.Sprintf("_commit_for_checkpoint failed for vb=%v, err=%v, status_code=%v, respMap=%v\n", vbno, err, status_code, respMap)
		capi_svc.logger.Error(msg)
		err = errors.New(msg)
		return 0, nil, err
	}
	return
}

func (capi_svc *CAPIService) composeAPIRequestBaseForVb(remoteBucket *service_def.RemoteBucketInfo, vbno uint16, restMethodName string) (*apiRequest, error) {
	rest_addr, err := capi_svc.lookUpConnectionStrForVb(remoteBucket, vbno)
	if err != nil {
		return nil, err
	}

	return capi_svc.composeAPIRequestBase(remoteBucket, rest_addr, restMethodName)
}

func (capi_svc *CAPIService) composeAPIRequestBaseForServer(remoteBucket *service_def.RemoteBucketInfo, server_addr string, restMethodName string) (*apiRequest, error) {
	rest_addr, err := capi_svc.lookUpConnectionStrForServer(remoteBucket, server_addr)
	if err != nil {
		return nil, err
	}

	return capi_svc.composeAPIRequestBase(remoteBucket, rest_addr, restMethodName)
}

func (capi_svc *CAPIService) composeAPIRequestBase(remoteBucket *service_def.RemoteBucketInfo, rest_addr string, restMethodName string) (*apiRequest, error) {
	if remoteBucket.RemoteClusterRef == nil || remoteBucket.UUID == "" {
		return nil, errors.New("Remote Bucket information is not fully populated")
	}

	username, password, httpAuthMech, certificate, sanInCertificate, clientCertificate, clientKey, err := remoteBucket.RemoteClusterRef.MyCredentials()
	if err != nil {
		return nil, err
	}

	//	    BodyBase = [{<<"bucket">>, Bucket},
	//                {<<"bucketUUID">>, bucketUUID}],
	api_base := &apiRequest{}
	api_base.url = rest_addr
	api_base.username = username
	api_base.password = password
	api_base.body = make(map[string]interface{})
	api_base.httpAuthMech = httpAuthMech
	api_base.certificate = certificate
	api_base.SANInCertificate = sanInCertificate
	api_base.clientCertificate = clientCertificate
	api_base.clientKey = clientKey

	if remoteBucket.UseCouchApiBase {
		// old way of using couchApiBase as the url
		api_base.path = restMethodName
		api_base.body["bucket"] = remoteBucket.BucketName
		api_base.body["bucketUUID"] = remoteBucket.UUID
	} else {
		// post-vulcan way of using new end point url in ns_server
		// e.g., _goxdcr/_pre_replicate/default?bucket_uuid=88f51cad913124c38d5a1894b51af683
		api_base.path = GoxdcrPrefix + restMethodName + base.UrlDelimiter + remoteBucket.BucketName + BucketUuidPrefix + remoteBucket.UUID
	}
	return api_base, nil
}

// compose a json request body
func (capi_svc *CAPIService) composePreReplicateBody(api_base *apiRequest, knownRemoteVBStatus *service_def.RemoteVBReplicationStatus) error {
	//    ReqBody = case RemoteCommitOpaque of
	//                  undefined -> [];
	//                  _ ->
	//                      [{<<"commitopaque">>, RemoteCommitOpaque}]
	//              end,

	if knownRemoteVBStatus == nil {
		return errors.New("Wrong parameter - knownRemoteVBStatus can't be nil")
	}
	api_base.body["vb"] = knownRemoteVBStatus.VBNo
	if !knownRemoteVBStatus.IsEmpty() {
		var remote_commit_opaque interface{}
		if targetVBUuid, ok := knownRemoteVBStatus.VBOpaque.(*metadata.TargetVBUuid); ok {
			//for newer clusters, commitopaque is a pair of vbuuid and seqno
			remote_commit_opaque = []interface{}{targetVBUuid.Value(), knownRemoteVBStatus.VBSeqno}
		} else if targetVBUuidAndTimestamp, ok := knownRemoteVBStatus.VBOpaque.(*metadata.TargetVBUuidAndTimestamp); ok {
			//for older clusters, commitopaque is just vbuuid
			remote_commit_opaque = targetVBUuidAndTimestamp.Target_vb_uuid
		} else if targetVBUuidStr, ok := knownRemoteVBStatus.VBOpaque.(*metadata.TargetVBUuidStr); ok {
			//for elastic search clusters, commitopaque is a string vbuuid
			remote_commit_opaque = targetVBUuidStr.Target_vb_uuid
		} else {
			return fmt.Errorf("Invalid target vb opaque, %v, in knownRemoteVBStatus.", knownRemoteVBStatus.VBOpaque)
		}

		api_base.body["commitopaque"] = remote_commit_opaque
	}
	return nil
}

func (capi_svc *CAPIService) send_post(api_base *apiRequest, client *http.Client, num_retry int) (int, map[string]interface{}, error) {
	var ret_map = make(map[string]interface{})
	body, err := json.Marshal(api_base.body)
	if err != nil {
		return 0, nil, err
	}

	err, statusCode := capi_svc.utils.InvokeRestWithRetryWithAuth(api_base.url, api_base.path, false, api_base.username, api_base.password, api_base.httpAuthMech, api_base.certificate, api_base.SANInCertificate, api_base.clientCertificate, api_base.clientKey, false, base.MethodPost, base.JsonContentType, body, 0, &ret_map, client, true, capi_svc.logger, num_retry)
	return statusCode, ret_map, err
}

func (capi_svc *CAPIService) parsePreReplicateResp(hostName string,
	resp_status_code int,
	respMap map[string]interface{},
	vbno uint16,
	xdcrCheckpointingCapbility bool) (bool, metadata.TargetVBOpaque, error) {
	if resp_status_code == 200 || resp_status_code == 400 {
		bMatch := (resp_status_code == 200)

		vbOpaque, err := getVBOpaqueFromRespMap(resp_status_code, respMap, vbno)
		if err != nil {
			return false, nil, err
		}

		capi_svc.logger.Debugf("_pre_replicate returned match=%v, remote VBOpaque=%v\n", bMatch, vbOpaque)
		return bMatch, vbOpaque, nil
	} else {
		var retError error = fmt.Errorf("unexpected status code, %v, in _pre_replicate response from host %v. respMap=%v\n", resp_status_code, hostName, respMap)

		//double check again disableCkptBackwardsCompat
		if resp_status_code == 404 && xdcrCheckpointingCapbility == false {
			//throw error
			capi_svc.logger.Infof("_pre_replicate failed. Response: 404 , reason: Target node %v is an old node", hostName)
			retError = service_def.NoSupportForXDCRCheckpointingError
		}

		return false, nil, retError

	}
}

func getVBOpaqueFromRespMap(resp_status_code int,
	respMap map[string]interface{},
	vbno uint16) (metadata.TargetVBOpaque, error) {
	vbOpaqueObj, ok := respMap["vbopaque"]
	if !ok {
		return nil, missingVBOpaqueError(resp_status_code, respMap, vbno)
	}

	vbOpaqueFloat, ok := vbOpaqueObj.(float64)
	if ok {
		// newer clusters return a single vbuuid as vb opaque
		return &metadata.TargetVBUuid{uint64(vbOpaqueFloat)}, nil
	}

	vbOpaqueString, ok := vbOpaqueObj.(string)
	if ok {
		// elastic search clusters return a string vbuuid as vb opaque
		return &metadata.TargetVBUuidStr{vbOpaqueString}, nil
	}

	// older clusters return a pair of vbuuid + startup time as vb opaque
	vbOpaquePair, ok := vbOpaqueObj.([]interface{})
	if !ok {
		// other types of vb opaques are invalid
		return nil, invalidVBOpaqueError(resp_status_code, respMap, vbno)
	}

	if len(vbOpaquePair) != 2 {
		return nil, invalidVBOpaqueError(resp_status_code, respMap, vbno)
	}

	vbUuid := vbOpaquePair[0]
	vbUuidStr, ok := vbUuid.(string)
	if !ok {
		return nil, invalidVBOpaqueError(resp_status_code, respMap, vbno)
	}

	startupTime := vbOpaquePair[1]
	startupTimeStr, ok := startupTime.(string)
	if !ok {
		return nil, invalidVBOpaqueError(resp_status_code, respMap, vbno)
	}

	return &metadata.TargetVBUuidAndTimestamp{vbUuidStr, startupTimeStr}, nil
}

func missingVBOpaqueError(resp_status_code int,
	respMap map[string]interface{},
	vbno uint16) error {
	return fmt.Errorf("missing vbopaque in _pre_replicate response. status_code=%v respMap=%v vbno=%v\n", resp_status_code, respMap, vbno)
}

func invalidVBOpaqueError(resp_status_code int,
	respMap map[string]interface{},
	vbno uint16) error {
	return fmt.Errorf("invalid vbopaque in _pre_replicate response. status_code=%v respMap=%v vbno=%v\n", resp_status_code, respMap, vbno)
}

func (capi_svc *CAPIService) lookUpConnectionStrForVb(remoteBucket *service_def.RemoteBucketInfo, vbno uint16) (string, error) {
	if remoteBucket == nil {
		return "", errors.New("Not a valid remote bucket, VBServerMap is not populated correctly")
	}
	var foundServerAddr string = ""

	for server_addr, vblist := range remoteBucket.VBServerMap {
		for _, vb := range vblist {
			if vb == vbno {
				foundServerAddr = server_addr
				goto Done
			}
		}
	}

Done:
	return capi_svc.lookUpConnectionStrForServer(remoteBucket, foundServerAddr)
}

func (capi_svc *CAPIService) lookUpConnectionStrForServer(remoteBucket *service_def.RemoteBucketInfo, server_addr string) (string, error) {
	if server_addr != "" {
		connectionStr, ok := remoteBucket.MemcachedAddrRestAddrMap[server_addr]
		if !ok {
			return "", errors.New(fmt.Sprintf("failed to find rest addr for _mass_vbopaque_check for server=%v", server_addr))
		}
		return connectionStr, nil
	} else {
		return "", errors.New(fmt.Sprintf("server addr is nil"))
	}

}
