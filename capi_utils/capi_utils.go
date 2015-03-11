package capi_utils

import (
	"errors"
	"fmt"
	base "github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/utils"
	"github.com/couchbase/go-couchbase"
	"strconv"
	"strings"
)

var logger_capi_utils *log.CommonLogger = log.NewLogger("CapiUtils", log.DefaultLoggerContext)

func ConstructVBCouchApiBaseMap(targetBucket *couchbase.Bucket, remoteClusterRef *metadata.RemoteClusterReference) (map[uint16]string, error) {
	var serverCouchApiBaseMap map[string]string
	var err error

	if !remoteClusterRef.DemandEncryption {
		serverCouchApiBaseMap = make(map[string]string)
		for _, node := range targetBucket.Nodes() {
			// get direct port
			directPort, ok := node.Ports[base.DirectPortKey]
			if !ok {
				return nil, ErrorBuildingVBCouchApiBaseMap(targetBucket.Name, remoteClusterRef.Name)
			}

			// server addr = host:directPort
			host := utils.GetHostName(node.Hostname)
			serverAddr := utils.GetHostAddr(host, uint16(directPort))

			serverCouchApiBaseMap[serverAddr] = node.CouchAPIBase
		}
	} else {
		// couchApiBaseHttps is needed for ssl.
		// somehow couchApiBaseHttps is not available in Node.
		// have to go the long way of using rest api call to retrieve it
		serverCouchApiBaseMap, err = ConstructServerCouchApiBaseMap(targetBucket, remoteClusterRef)
		if err != nil {
			return nil, err
		}
	}

	// construct vbCouchApiBaseMap map with key = vbno and value = couchApiBase
	vbCouchApiBaseMap := make(map[uint16]string)
	vbMap, err := targetBucket.GetVBmap(targetBucket.VBServerMap().ServerList)
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

func ConstructServerCouchApiBaseMap(targetBucket *couchbase.Bucket, remoteClusterRef *metadata.RemoteClusterReference) (map[string]string, error) {
	serverCouchApiBaseMap := make(map[string]string)

	parseError := ErrorBuildingVBCouchApiBaseMap(targetBucket.Name, remoteClusterRef.Name)

	var out interface{}
	err, _ := utils.QueryRestApiWithAuth(remoteClusterRef.HostName, targetBucket.URI, false, remoteClusterRef.UserName, remoteClusterRef.Password, []byte{}, base.MethodGet, "", nil, 0, &out, logger_capi_utils)
	if err != nil {
		return nil, utils.NewEnhancedError(fmt.Sprintf("Error constructing vb couchApiBase map for bucket %v on remote cluster %v because of failure to retrieve bucket info\n", targetBucket.Name, remoteClusterRef.Name), err)
	}

	infoMap, ok := out.(map[string]interface{})
	if !ok {
		return nil, parseError
	}

	nodes, ok := infoMap[base.NodesKey]
	if !ok {
		return nil, parseError
	}

	nodeList, ok := nodes.([]interface{})
	if !ok {
		return nil, parseError
	}

	for _, node := range nodeList {
		nodeInfoMap, ok := node.(map[string]interface{})
		if !ok {
			return nil, parseError
		}

		// get hostname
		hostname, ok := nodeInfoMap[base.HostNameKey]
		if !ok {
			return nil, parseError
		}
		hostnameStr, ok := hostname.(string)
		if !ok {
			return nil, parseError
		}

		// get direct port
		ports, ok := nodeInfoMap[base.PortsKey]
		if !ok {
			return nil, parseError
		}
		portsMap, ok := ports.(map[string]interface{})
		if !ok {
			return nil, parseError
		}
		directPort, ok := portsMap[base.DirectPortKey]
		if !ok {
			return nil, parseError
		}
		directPortFloat, ok := directPort.(float64)
		if !ok {
			return nil, parseError
		}
		directPortInt := uint16(directPortFloat)

		// server addr = host:directPort
		host := utils.GetHostName(hostnameStr)
		serverAddr := utils.GetHostAddr(host, uint16(directPortInt))

		// get couchApiBase
		var couchApiBase interface{}
		if remoteClusterRef.DemandEncryption {
			couchApiBase, ok = nodeInfoMap[base.CouchApiBaseHttps]
		} else {
			couchApiBase, ok = nodeInfoMap[base.CouchApiBase]
		}
		if !ok {
			return nil, parseError
		}

		couchApiBaseStr, ok := couchApiBase.(string)
		if !ok {
			return nil, parseError
		}

		serverCouchApiBaseMap[serverAddr] = couchApiBaseStr
	}

	logger_capi_utils.Infof("serverCouchApiBaseMap = %v\n", serverCouchApiBaseMap)

	return serverCouchApiBaseMap, nil
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

func ErrorBuildingVBCouchApiBaseMap(bucketName, refName string) error {
	return errors.New(fmt.Sprintf("Error constructing vb couchApiBase map for bucket %v on remote cluster %v because of failure to parse bucket info.", bucketName, refName))
}
