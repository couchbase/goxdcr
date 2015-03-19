package service_impl

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/utils"
)

const (
	HTTP_RETRIES int = 5

	PRE_REPLICATE_CMD       string = "_pre_replicate"
	MASS_VBOPAQUE_CHECK_CMD string = "_mass_vbopaque_check"
	COMMIT_FOR_CKPT_CMD     string = "_commit_for_checkpoint"
)

//errors
var VB_OPAQUE_MISMATCH_ERR error = errors.New("The remote vb opaque doesn't match with the one provided")
var NO_VB_OPAQUE_IN_RESP_ERR error = errors.New("No vb opaque in the response")
var NO_REMOTE_OPAQUE_IN_RESP_ERR error = errors.New("No remote opaque in the response")

//apiRequest is a structure for http request used for CAPI
type apiRequest struct {
	url      string
	username string
	password string
	body     map[string]interface{}
	certificate []byte
}

//CAPIService is a wrapper around the rest interface provided by couchbase server
//It provide the following methods:
//		1. _pre_preplicate: check if the checkpoint on-file for a vb is valid on remote cluster
//		2. _mass_vbopaque_check: mass check if vb uuid on the remote cluster is the same as the one on-file for a list of vbs
//		3. _commit_for_checkpoint: ask the remote vbucket to commit and return back the seqno, or if the remote vbucket's UUID
//								   has changed due to the topology change, in that case, new vb UUID would be returned
type CAPIService struct {
	logger *log.CommonLogger
}

func NewCAPIService(logger_ctx *log.LoggerContext) *CAPIService {
	return &CAPIService{logger: log.NewLogger("CAPIService", logger_ctx)}
}

//PrePrelicate (_pre_replicate)
//Parameters: remoteBucket - the information about the remote bucket
//			  knownRemoteVBStatus - the current replication status of a vbucket
//			  disableCkptBackwardsCompat
//returns:
//		  bMatch - true if the remote vbucket matches the current replication status
//		  current_remoteVBUUID - new remote vb uuid might be retured if bMatch = false and there was a topology change on remote vb
//		  err
func (capi_svc *CAPIService) PreReplicate(remoteBucket *service_def.RemoteBucketInfo,
	knownRemoteVBStatus *service_def.RemoteVBReplicationStatus, disableCkptBackwardsCompat bool) (bMatch bool, current_remoteVBUUID uint64, err error) {
	capi_svc.logger.Debug("Calling _pre_replicate")
	api_base, err := capi_svc.composeAPIRequestBase(remoteBucket, knownRemoteVBStatus.VBNo)
	if err != nil {
		return
	}
	err = capi_svc.composePreReplicateBody(api_base, knownRemoteVBStatus)
	if err != nil {
		return
	}

	status_code, respMap, err := capi_svc.send_post(PRE_REPLICATE_CMD, api_base, HTTP_RETRIES)
	capi_svc.logger.Debugf("response from _pre_replicate is status_code=%v respMap=%v for %v\n", status_code, respMap, knownRemoteVBStatus)
	if err != nil {
		capi_svc.logger.Errorf("Calling _pre_replicate on %v failed, err=%v\n", api_base.url, err)
	}
	bMatch, current_remoteVBUUID, err = capi_svc.parsePreReplicateResp(api_base.url, status_code, respMap, knownRemoteVBStatus.VBNo, disableCkptBackwardsCompat)
	
	if err == nil {
		capi_svc.logger.Debugf("_pre_replicate succeeded for vb=%v\n", knownRemoteVBStatus.VBNo)
	}
	return

}

//CommitForCheckpoint (_commit_for_checkpoint)
//Parameters: remoteBucket - the information about the remote bucket
//			  remoteVBUUID - the remote vb uuid on file
//			  vbno		   - the vb number
//returns:	  remote_seqno - the remote vbucket's high sequence number
//			  vb_uuid	   - the new vb uuid if there was a topology change
//			  err
func (capi_svc *CAPIService) CommitForCheckpoint(remoteBucket *service_def.RemoteBucketInfo, remoteVBUUID uint64, vbno uint16) (remote_seqno uint64, vb_uuid uint64, err error) {
	capi_svc.logger.Debug("Calling _commit_for_checkpoint")
	api_base, err := capi_svc.composeAPIRequestBase(remoteBucket, vbno)
	if err != nil {
		return
	}
	api_base.body["vb"] = vbno
	api_base.body["vbopaque"] = remoteVBUUID
	status_code, respMap, err := capi_svc.send_post(COMMIT_FOR_CKPT_CMD, api_base, HTTP_RETRIES)
	if err == nil && status_code == 400 {
		vb_uuid_val, ok := respMap["vbopaque"]
		if ok {
			vb_uuid = uint64(vb_uuid_val.(float64))
			return 0, vb_uuid, VB_OPAQUE_MISMATCH_ERR
		} else {
			capi_svc.logger.Errorf("No vb uuid found in resp. respMap=%v, err=%v\n", respMap, err)
			return 0, 0, NO_VB_OPAQUE_IN_RESP_ERR
		}
	} else if err == nil && status_code == 200 {
		remote_seqno_pair, ok := respMap["commitopaque"].([]interface{})
		if !ok || len(remote_seqno_pair) != 2 {
			capi_svc.logger.Errorf("No commitopaque found in resp. respMap=%v, err=%v\n", respMap, err)
			return 0, 0, NO_REMOTE_OPAQUE_IN_RESP_ERR
		}

		remote_seqno = uint64(remote_seqno_pair[1].(float64))
	} else {
		//error case
		msg := fmt.Sprintf("_commit_for_checkpoint failed for vb=%v, err=%v, status_code=%v\n", vbno, err, status_code)
		capi_svc.logger.Error(msg)
		err = errors.New(msg)
		return 0, 0, err
	}
	return
}

//MassValidateVBUUIDs (_mass_vbopaque_check)
//Parameters: remoteBucket - the information about the remote bucket
//			  remoteVBUUIDs - the map of vbno and vbuuid
//returns: matching - the list of vb numbers whose vbuuid matches
//		   mismatching - the list of vb numbers whose vbuuid mismatches
//		   missing	- the list of vb numbers whose vbuuid is not kept on file
func (capi_svc *CAPIService) MassValidateVBUUIDs(remoteBucket *service_def.RemoteBucketInfo, remoteVBUUIDs [][]uint64) (matching []interface{}, mismatching []interface{}, missing []interface{}, err error) {
	capi_svc.logger.Debug("Calling _mass_vbopaque_check")

	//use the vbucket 0's capi api url
	api_base, err := capi_svc.composeAPIRequestBase(remoteBucket, 0)
	if err != nil {
		return
	}

	//    Body = [{<<"vbopaques">>, Pairs}],
	api_base.body["vbopaques"] = remoteVBUUIDs
	status_code, respMap, err := capi_svc.send_post(MASS_VBOPAQUE_CHECK_CMD, api_base, HTTP_RETRIES)
	capi_svc.logger.Debugf("response from _mass_vbopaque_check is %v\n", respMap)
	if err != nil {
		return nil, nil, nil, err
	}
	matching, mismatching, missing, err = capi_svc.parseMassValidateSeqNosResp(api_base.url, status_code, respMap, remoteVBUUIDs)
	return
}

func (capi_svc *CAPIService) parseMassValidateSeqNosResp(url string, resp_status_code int,
	respMap map[string]interface{}, remoteVBUUIDs [][]uint64) (matching []interface{}, mismatching []interface{}, missing []interface{}, err error) {
	mismatchingobj, ok := respMap["mismatched"]
	if !ok {
		err = errors.New("Can't find 'mismatched' in response")
		return
	}
	mismatching = mismatchingobj.([]interface{})
	missingobj, ok := respMap["missing"]
	if !ok {
		err = errors.New("Can't find 'missing' in response")
		return
	}
	missing = missingobj.([]interface{})
	bad_vb_list := append(missing, mismatching...)
	matching = []interface{}{}
	//loop through the provided remoteVBUUIDs
	for _, vbpair := range remoteVBUUIDs {
		vb := vbpair[0]
		if !utils.Contains(vb, bad_vb_list) {
			matching = append(matching, vb)
		}

	}
	return
}

func (capi_svc *CAPIService) composeAPIRequestBase(remoteBucket *service_def.RemoteBucketInfo, vbno uint16) (*apiRequest, error) {
	if remoteBucket.RemoteClusterRef == nil || remoteBucket.UUID == "" {
		return nil, errors.New("Remote Bucket information is not fully populated")
	}

	connectionStr, err := capi_svc.lookUpConnectionStr(remoteBucket, vbno)
	if err != nil {
		return nil, err
	}
	username, password, err := remoteBucket.RemoteClusterRef.MyCredentials()
	if err != nil {
		return nil, err
	}

	couchApiBaseUrl := connectionStr

	//	    BodyBase = [{<<"bucket">>, Bucket},
	//                {<<"bucketUUID">>, BucketUUID}],
	api_base := &apiRequest{}
	api_base.url = couchApiBaseUrl
	api_base.username = username
	api_base.password = password
	api_base.body = make(map[string]interface{})
	api_base.body["bucket"] = remoteBucket.BucketName
	api_base.body["bucketUUID"] = remoteBucket.UUID
	api_base.certificate = remoteBucket.RemoteClusterRef.Certificate
	return api_base, nil
}

//compose a json request body
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
		//commitopaque is a pair of vbuuid and seqno
		remote_commit_opaque := []interface{}{knownRemoteVBStatus.VBUUID, knownRemoteVBStatus.VBSeqno}
		api_base.body["commitopaque"] = remote_commit_opaque
	}
	return nil
}

func (capi_svc *CAPIService) send_post(restMethodName string, api_base *apiRequest, num_retry int) (int, map[string]interface{}, error) {
	var ret_map = make(map[string]interface{})
	body, err := json.Marshal(api_base.body)
	if err != nil {
		return 0, nil, err
	}
	//	body :=  []byte(`{"bucket":"default","bucketUUID":0,"vb":0}`)
	capi_svc.logger.Debugf("body=%s\n", body)
	err, statusCode := utils.InvokeRestWithRetryWithAuth(api_base.url, restMethodName, false, api_base.username, api_base.password, api_base.certificate, base.MethodPost, base.JsonContentType, body, 0, &ret_map, capi_svc.logger, num_retry)
	return statusCode, ret_map, err
}

func (capi_svc *CAPIService) parsePreReplicateResp(hostName string,
	resp_status_code int,
	respMap map[string]interface{},
	vbno uint16,
	disableCkptBackwardsCompat bool) (bool, uint64, error) {
	if resp_status_code == 200 || resp_status_code == 400 {
		bMatch := (resp_status_code == 200)
		vbuuidfloat, ok := respMap["vbopaque"].(float64)
		if !ok {
			return false, 0, errors.New("missing vbopaque in _pre_replicate response")
		}
		vbuuid := uint64(vbuuidfloat)
		capi_svc.logger.Debugf("_re_replicate returns remote VBOpaque=%v\n", vbuuid)
		capi_svc.logger.Debugf("_pre_replicate returned %v status", bMatch)
		return bMatch, vbuuid, nil
	} else {
		var retError error = nil

		//double check again disableCkptBackwardsCompat
		if resp_status_code == 404 && disableCkptBackwardsCompat == false {
			//throw error
			retError = errors.New(fmt.Sprintf("_pre_replicate failed on target node %v for vb=%v", hostName, vbno))
		}

		return false, 0, retError

	}
}

func (capi_svc *CAPIService) lookUpConnectionStr(remoteBucket *service_def.RemoteBucketInfo, vbno uint16) (string, error) {
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
	if foundServerAddr != "" {
		connectionStr, ok := remoteBucket.MemcachedAddrRestAddrMap[foundServerAddr]
		if !ok {
			return "", errors.New(fmt.Sprintf("failed to find server for vb=%v", vbno))
		}
		return connectionStr, nil
	} else {
		return "", errors.New(fmt.Sprintf("failed to find server for vb=%v", vbno))
	}

}
