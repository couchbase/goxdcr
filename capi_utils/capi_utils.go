package capi_utils

import (
	"errors"
	"fmt"
	base "github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	utilities "github.com/couchbase/goxdcr/utils"
	"strconv"
	"strings"
)

var logger_capi_utils *log.CommonLogger = log.NewLogger("CapiUtils", log.DefaultLoggerContext)

func ConstructVBCouchApiBaseMap(targetBucketName string, targetBucketInfo map[string]interface{}, remoteClusterRef *metadata.RemoteClusterReference, utils utilities.UtilsIface, useExternal bool) (map[uint16]string, error) {
	serverCouchApiBaseMap, err := ConstructServerCouchApiBaseMap(targetBucketName, targetBucketInfo, remoteClusterRef, utils, useExternal)
	if err != nil {
		return nil, err
	}

	// construct vbCouchApiBaseMap map with key = vbno and value = couchApiBase
	vbCouchApiBaseMap := make(map[uint16]string)
	vbMap, err := utils.GetRemoteServerVBucketsMap(remoteClusterRef.HostName(), targetBucketName, targetBucketInfo, useExternal)
	if err != nil {
		return nil, err
	}
	for serverAddr, vbList := range vbMap {
		for _, vb := range vbList {
			// append vbno to couchApiBase for the server
			uri := serverCouchApiBaseMap[serverAddr] + base.CouchApiBaseUriDelimiter + strconv.Itoa(int(vb))
			vbCouchApiBaseMap[vb] = uri
		}
	}

	logger_capi_utils.Debugf("vbCouchApiBaseMap = %v\n", vbCouchApiBaseMap)
	return vbCouchApiBaseMap, nil
}

func ConstructServerCouchApiBaseMap(targetBucketName string, targetBucketInfo map[string]interface{}, remoteClusterRef *metadata.RemoteClusterReference, utils utilities.UtilsIface, useExternal bool) (map[string]string, error) {
	return ConstructCapiServiceEndPointMap(targetBucketName, targetBucketInfo, remoteClusterRef, utils, true /*useCouchApiBase*/, useExternal)
}

// construct map for capi service end point, e.g., _pre_replicate
func ConstructCapiServiceEndPointMap(targetBucketName string, targetBucketInfo map[string]interface{}, remoteClusterRef *metadata.RemoteClusterReference, utils utilities.UtilsIface, useCouchApiBase, useExternal bool) (map[string]string, error) {
	endPointMap := make(map[string]string)

	nodeList, err := utils.GetNodeListFromInfoMap(targetBucketInfo, logger_capi_utils)
	if err != nil {
		return nil, err
	}

	for _, node := range nodeList {
		nodeMap, ok := node.(map[string]interface{})
		if !ok {
			return nil, ErrorBuildingCapiServerEndPointMap(targetBucketName, remoteClusterRef.Name(), node)
		}

		var endPoint string
		var hostname string
		if useCouchApiBase {
			// endPoint is couchApiBase in nodeInfo
			hostname, err = utils.GetHostNameFromNodeInfo(remoteClusterRef.HostName(), nodeMap, logger_capi_utils)
			if err != nil {
				return nil, err
			}

			// get couchApiBase
			var couchApiBaseObj interface{}
			if remoteClusterRef.IsHttps() {
				couchApiBaseObj, ok = nodeMap[base.CouchApiBaseHttps]
			} else {
				couchApiBaseObj, ok = nodeMap[base.CouchApiBase]
			}
			if !ok {
				//skip this node, during rebalance it is possible that the node is on the server list, but it is not master for any vb, so no couchApiBase
				continue
			}
			endPoint, ok = couchApiBaseObj.(string)
			if !ok {
				return nil, ErrorBuildingCapiServerEndPointMap(targetBucketName, remoteClusterRef.Name(), node)
			}

			if useExternal {
				endPoint = utils.ReplaceCouchApiBaseObjWithExternals(endPoint, nodeMap)
			}
		} else {
			// endPoint is host address in nodeInfo
			endPoint, err = utils.GetHostAddrFromNodeInfo(remoteClusterRef.HostName(), nodeMap, remoteClusterRef.IsHttps(), logger_capi_utils, useExternal)
			if err != nil {
				return nil, err
			}

			hostname = base.GetHostName(endPoint)
		}

		// Get internal direct ports
		portsObj, ok := nodeMap[base.PortsKey]
		if !ok {
			return nil, ErrorBuildingCapiServerEndPointMap(targetBucketName, remoteClusterRef.Name(), node)
		}
		portsMap, ok := portsObj.(map[string]interface{})
		if !ok {
			return nil, ErrorBuildingCapiServerEndPointMap(targetBucketName, remoteClusterRef.Name(), node)
		}

		directPortObj, ok := portsMap[base.DirectPortKey]
		if !ok {
			return nil, ErrorBuildingCapiServerEndPointMap(targetBucketName, remoteClusterRef.Name(), node)
		}
		directPortFloat, ok := directPortObj.(float64)
		if !ok {
			return nil, ErrorBuildingCapiServerEndPointMap(targetBucketName, remoteClusterRef.Name(), node)
		}

		portToUse := uint16(directPortFloat)

		// Potentially, get external kv (direct) ports
		if useExternal {
			externalHostName, externalKvPort, externalKvPortErr, _, _ := utils.GetExternalAddressAndKvPortsFromNodeInfo(nodeMap)
			if len(externalHostName) > 0 {
				hostname = externalHostName
				if externalKvPortErr == nil {
					portToUse = uint16(externalKvPort)
				}
			}
		}

		// server addr = host:directPort or externalHost:kv
		serverAddr := base.GetHostAddr(hostname, portToUse)

		endPointMap[serverAddr] = endPoint
	}

	return endPointMap, nil
}

// Get the host:port portion of couchApiBase, which would be the connection string for capi tcp connection
func GetCapiConnectionStrFromCouchApiBase(couchApiBase string) (string, error) {
	parseErr := errors.New(fmt.Sprintf("Error parsing capi connection string from couchApiBase %v", couchApiBase))
	index := strings.Index(couchApiBase, "//")
	if index < 0 {
		return "", parseErr
	}
	couchApiBase2 := couchApiBase[index+2:]
	index = strings.Index(couchApiBase2, "/")
	if index < 0 {
		return "", parseErr
	}
	return couchApiBase2[:index], nil
}

func ErrorBuildingCapiServerEndPointMap(bucketName, refName string, info interface{}) error {
	errMsg := fmt.Sprintf("Error constructing capi service end point map for bucket %v on remote cluster %v because of failure to parse bucket info.", bucketName, refName)
	logger_capi_utils.Errorf("%v bucketInfo=%v.", errMsg, info)
	return errors.New(errMsg)
}
