package service_impl

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/utils"
	"net/http"
)

const (
	PRE_REPLICATE_CMD       string = "_pre_replicate"
	MASS_VBOPAQUE_CHECK_CMD string = "_mass_vbopaque_check"
	COMMIT_FOR_CKPT_CMD     string = "_commit_for_checkpoint"
)

//errors
var VB_OPAQUE_MISMATCH_ERR error = errors.New("The remote vb opaque doesn't match with the one provided")
var NO_VB_OPAQUE_IN_RESP_ERR error = errors.New("No vb opaque in the response")

//apiRequest is a structure for http request used for CAPI
type apiRequest struct {
	url                string
	username           string
	password           string
	body               map[string]interface{}
	certificate        []byte
	SANInCertificate   bool
	insecureSkipVerify bool
}

//CAPIService is a wrapper around the rest interface provided by couchbase server
//It provide the following methods:
//		1. _pre_preplicate: check if the checkpoint on-file for a vb is valid on remote cluster
//		2. _mass_vbopaque_check: mass check if vb uuid on the remote cluster is the same as the one on-file for a list of vbs
//		3. _commit_for_checkpoint: ask the remote vbucket to commit and return back the seqno, or if the remote vbucket's UUID
//								   has changed due to the topology change, in that case, new vb UUID would be returned
type CAPIService struct {
	cluster_info_service *ClusterInfoSvc
	logger               *log.CommonLogger
}

func NewCAPIService(cluster_info_service *ClusterInfoSvc, logger_ctx *log.LoggerContext) *CAPIService {
	return &CAPIService{
		cluster_info_service: cluster_info_service,
		logger:               log.NewLogger("CapiSvc", logger_ctx),
	}
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
	knownRemoteVBStatus *service_def.RemoteVBReplicationStatus, xdcrCheckpointingCapbility bool) (bMatch bool, current_remoteVBOpaque metadata.TargetVBOpaque, err error) {
	capi_svc.logger.Debug("Calling _pre_replicate")
	api_base, err := capi_svc.composeAPIRequestBaseForVb(remoteBucket, knownRemoteVBStatus.VBNo)
	if err != nil {
		return
	}
	err = capi_svc.composePreReplicateBody(api_base, knownRemoteVBStatus)
	if err != nil {
		return
	}

	capi_svc.logger.Debugf("request to _pre_replicate = %v\n", api_base)
	http_client := remoteBucket.RestAddrHttpClientMap[api_base.url]
	status_code, respMap, _, err := capi_svc.send_post(PRE_REPLICATE_CMD, api_base, http_client, base.HTTP_RETRIES)
	capi_svc.logger.Debugf("response from _pre_replicate is status_code=%v respMap=%v for %v\n", status_code, respMap, knownRemoteVBStatus)
	if err != nil {
		capi_svc.logger.Errorf("Calling _pre_replicate on %v failed for vb=%v, err=%v\n", api_base.url, knownRemoteVBStatus.VBNo, err)
		return false, nil, err
	}

	bMatch, current_remoteVBOpaque, err = capi_svc.parsePreReplicateResp(api_base.url, status_code, respMap, knownRemoteVBStatus.VBNo, xdcrCheckpointingCapbility)

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
func (capi_svc *CAPIService) CommitForCheckpoint(remoteBucket *service_def.RemoteBucketInfo, remoteVBOpaque metadata.TargetVBOpaque, vbno uint16) (remote_seqno uint64, vbOpaque metadata.TargetVBOpaque, err error) {
	capi_svc.logger.Debug("Calling _commit_for_checkpoint")
	api_base, err := capi_svc.composeAPIRequestBaseForVb(remoteBucket, vbno)
	if err != nil {
		return
	}
	api_base.body["vb"] = vbno
	api_base.body["vbopaque"] = remoteVBOpaque.Value()
	http_client := remoteBucket.RestAddrHttpClientMap[api_base.url]
	status_code, respMap, _, err := capi_svc.send_post(COMMIT_FOR_CKPT_CMD, api_base, http_client, base.HTTP_RETRIES)

	if err == nil && status_code == 400 {
		vbOpaque, err := getVBOpaqueFromRespMap(status_code, respMap, vbno)
		if err != nil {
			return 0, nil, err
		}

		return 0, vbOpaque, VB_OPAQUE_MISMATCH_ERR
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
		msg := fmt.Sprintf("_commit_for_checkpoint failed for vb=%v, err=%v, status_code=%v\n", vbno, err, status_code)
		capi_svc.logger.Error(msg)
		err = errors.New(msg)
		return 0, nil, err
	}
	return
}

//MassValidateVBUUIDs (_mass_vbopaque_check)
//Parameters: remoteBucket - the information about the remote bucket
//			  remoteVBUUIDs - the map of vbno and vbuuid
//returns: matching - the list of vb numbers whose vbuuid matches
//		   mismatching - the list of vb numbers whose vbuuid mismatches
//		   missing	- the list of vb numbers whose vbuuid is not kept on file
func (capi_svc *CAPIService) MassValidateVBUUIDs(remoteBucket *service_def.RemoteBucketInfo, remoteVBUUIDs map[uint16]metadata.TargetVBOpaque) (matching []uint16, mismatching []uint16, missing []uint16, err error) {
	capi_svc.logger.Info("Calling _mass_vbopaque_check")

	matching = make([]uint16, 0)
	mismatching = make([]uint16, 0)
	missing = make([]uint16, 0)
	for server_addr, vblist := range remoteBucket.VBServerMap {
		vbopaques := [][]interface{}{}
		for _, vbno := range vblist {
			if remotevbuuid, ok := remoteVBUUIDs[vbno]; ok && remotevbuuid != nil {
				vbopaques = append(vbopaques, []interface{}{vbno, remotevbuuid.Value()})
			}
		}

		if len(vbopaques) == 0 {
			continue
		}

		matching_per_server, mismatching_per_server, missing_per_server, err1 := capi_svc.massValidateVBUUIDsForServer(remoteBucket, vbopaques, server_addr, vblist)
		if err1 != nil {
			err = err1
			return
		}
		matching = append(matching, matching_per_server...)
		mismatching = append(mismatching, mismatching_per_server...)
		missing = append(missing, missing_per_server...)
	}

	return
}

func (capi_svc *CAPIService) massValidateVBUUIDsForServer(remoteBucket *service_def.RemoteBucketInfo, vbopaques [][]interface{},
	server_addr string, vblist []uint16) (matching []uint16, mismatching []uint16, missing []uint16, err error) {
	capi_svc.logger.Infof("Calling _mass_vbopaque_check for server %v\n", server_addr)

	api_base, err := capi_svc.composeAPIRequestBaseForServer(remoteBucket, server_addr)
	if err != nil {
		return
	}

	api_base.body["vbopaques"] = vbopaques
	http_client := remoteBucket.RestAddrHttpClientMap[api_base.url]
	status_code, respMap, _, err := capi_svc.send_post(MASS_VBOPAQUE_CHECK_CMD, api_base, http_client, base.HTTP_RETRIES)

	capi_svc.logger.Debugf("vbopaques=%v\n", vbopaques)
	if err != nil {
		return nil, nil, nil, err
	}
	matching, mismatching, missing, err = capi_svc.parseMassValidateSeqNosResp(api_base.url, status_code, respMap, vbopaques)
	return
}

func (capi_svc *CAPIService) parseMassValidateSeqNosResp(url string, resp_status_code int,
	respMap map[string]interface{}, vbOpaques [][]interface{}) (matching []uint16, mismatching []uint16, missing []uint16, err error) {
	mismatchingobj, ok := respMap["mismatched"]
	if !ok {
		err = errors.New("Can't find 'mismatched' in response")
		return
	}
	mismatching_pairs := mismatchingobj.([]interface{})
	mismatching, err = getVBLists(mismatching_pairs)
	if err != nil {
		return
	}
	missingobj, ok := respMap["missing"]
	if !ok {
		err = errors.New("Can't find 'missing' in response")
		return
	}
	missing_list := missingobj.([]interface{})
	missing = make([]uint16, len(missing_list))
	for index, vb := range missing_list {
		missing[index] = uint16(vb.(float64))
	}

	bad_vb_list := []uint16{}
	bad_vb_list = append(mismatching, missing...)
	matching = []uint16{}
	//loop through the provided vbOpaques
	for _, vbpair := range vbOpaques {
		vb, ok := vbpair[0].(uint16)
		if !ok {
			panic(fmt.Sprintf("wrong format of vbOpaques, the first element in %v is expected to be uint16", vbpair))
		}
		if !isInBadList(vb, bad_vb_list, capi_svc.logger) {
			matching = append(matching, vb)
		}

	}
	capi_svc.logger.Debugf("mismatching=%v, missing=%v, matching=%v\n", mismatching, missing, matching)
	return
}

func (capi_svc *CAPIService) composeAPIRequestBaseForVb(remoteBucket *service_def.RemoteBucketInfo, vbno uint16) (*apiRequest, error) {
	rest_addr, err := capi_svc.lookUpConnectionStrForVb(remoteBucket, vbno)
	if err != nil {
		return nil, err
	}

	return capi_svc.composeAPIRequestBase(remoteBucket, rest_addr)
}

func (capi_svc *CAPIService) composeAPIRequestBaseForServer(remoteBucket *service_def.RemoteBucketInfo, server_addr string) (*apiRequest, error) {
	rest_addr, err := capi_svc.lookUpConnectionStrForServer(remoteBucket, server_addr)
	if err != nil {
		return nil, err
	}

	return capi_svc.composeAPIRequestBase(remoteBucket, rest_addr)
}

func (capi_svc *CAPIService) composeAPIRequestBase(remoteBucket *service_def.RemoteBucketInfo, rest_addr string) (*apiRequest, error) {
	if remoteBucket.RemoteClusterRef == nil || remoteBucket.UUID == "" {
		return nil, errors.New("Remote Bucket information is not fully populated")
	}

	username, password, certificate, sanInCertificate, err := remoteBucket.RemoteClusterRef.MyCredentials()
	if err != nil {
		return nil, err
	}

	//	    BodyBase = [{<<"bucket">>, Bucket},
	//                {<<"bucketUUID">>, BucketUUID}],
	api_base := &apiRequest{}
	api_base.url = rest_addr
	api_base.username = username
	api_base.password = password
	api_base.body = make(map[string]interface{})
	api_base.body["bucket"] = remoteBucket.BucketName
	api_base.body["bucketUUID"] = remoteBucket.UUID
	api_base.certificate = certificate
	api_base.SANInCertificate = sanInCertificate
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

func (capi_svc *CAPIService) send_post(restMethodName string, api_base *apiRequest, client *http.Client, num_retry int) (int, map[string]interface{}, *http.Client, error) {
	var ret_map = make(map[string]interface{})
	body, err := json.Marshal(api_base.body)
	if err != nil {
		return 0, nil, nil, err
	}
	err, statusCode, ret_client := utils.InvokeRestWithRetryWithAuth(api_base.url, restMethodName, false, api_base.username, api_base.password, api_base.certificate, api_base.SANInCertificate, api_base.insecureSkipVerify, base.MethodPost, base.JsonContentType, body, 0, &ret_map, client, true, capi_svc.logger, num_retry)
	return statusCode, ret_map, ret_client, err
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

		capi_svc.logger.Debugf("_re_replicate returns remote VBOpaque=%v\n", vbOpaque)
		capi_svc.logger.Debugf("_pre_replicate returned %v status", bMatch)
		return bMatch, vbOpaque, nil
	} else {
		var retError error = fmt.Errorf("unexpected status code, %v, in _pre_replicate response", resp_status_code)

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

//helper function to MassValidateVBUUIDs
//it returns a list of vbno given a list of [vbno, vbuuid] pair
func getVBLists(pair_list []interface{}) ([]uint16, error) {
	ret := []uint16{}
	for _, pairObj := range pair_list {
		pair, ok := pairObj.([]interface{})
		if !ok || len(pair) != 2 {
			return ret, fmt.Errorf("_mass_vbopaque_check retruns an invalid response. element in mismatch field should have format of [vbno, vbuuid], it is %v, pair_list=%v", pairObj, pair_list)
		}
		vb := uint16(pair[0].(float64))
		ret = append(ret, vb)
	}
	return ret, nil
}

func isInBadList(vb uint16, bad_vb_list []uint16, logger *log.CommonLogger) bool {
	for _, vbno := range bad_vb_list {
		if vb == vbno {
			return true
		}
	}
	return false
}
