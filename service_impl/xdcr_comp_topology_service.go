// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package service_impl

import (
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/service_def"
	utilities "github.com/couchbase/goxdcr/utils"
	"reflect"
	"strings"
)

var ErrorParsingHostInfo = errors.New("Could not parse current host info from the result.server returned")
var ErrorParsingServicesInfo = errors.New("Could not parse services from the result server returned.")

type XDCRTopologySvc struct {
	adminport        uint16
	xdcrRestPort     uint16
	isEnterprise     bool
	isIpv6           bool
	cluster_info_svc service_def.ClusterInfoSvc
	logger           *log.CommonLogger
	utils            utilities.UtilsIface
}

func NewXDCRTopologySvc(adminport, xdcrRestPort uint16,
	isEnterprise bool, isIpv6 bool, cluster_info_svc service_def.ClusterInfoSvc,
	logger_ctx *log.LoggerContext, utilsIn utilities.UtilsIface) (*XDCRTopologySvc, error) {
	top_svc := &XDCRTopologySvc{
		adminport:        adminport,
		xdcrRestPort:     xdcrRestPort,
		isEnterprise:     isEnterprise,
		isIpv6:           isIpv6,
		cluster_info_svc: cluster_info_svc,
		logger:           log.NewLogger("TopoSvc", logger_ctx),
		utils:            utilsIn,
	}
	return top_svc, nil
}

func (top_svc *XDCRTopologySvc) MyHost() (string, error) {
	return top_svc.getHostName()
}

func (top_svc *XDCRTopologySvc) MyHostAddr() (string, error) {
	return top_svc.getHostAddr()
}

func (top_svc *XDCRTopologySvc) MyMemcachedAddr() (string, error) {
	nodeInfoMap, err := top_svc.getHostInfo()
	if err != nil {
		return "", err
	}

	port, err := top_svc.getHostMemcachedPortFromHostInfo(nodeInfoMap)
	if err != nil {
		return "", err
	}

	hostAddr, err := top_svc.getHostAddrFromHostInfo(nodeInfoMap)
	if err != nil {
		return "", err
	}

	hostName := base.GetHostName(hostAddr)

	return base.GetHostAddr(hostName, port), nil
}

func (top_svc *XDCRTopologySvc) MyAdminPort() (uint16, error) {
	return top_svc.adminport, nil
}

func (top_svc *XDCRTopologySvc) MyKVNodes() ([]string, error) {
	// as of now each xdcr instance is responsible for only one kv node
	nodes := make([]string, 1)
	// get the actual hostname used in server list and server vb map
	memcachedAddr, err := top_svc.MyMemcachedAddr()
	if err != nil {
		return nil, err
	}
	nodes[0] = memcachedAddr
	return nodes, nil
}

func (top_svc *XDCRTopologySvc) NumberOfKVNodes() (int, error) {
	nodeList, err := top_svc.getNodeList()
	if err != nil {
		return 0, err
	}
	return len(nodeList), nil
}

func (top_svc *XDCRTopologySvc) IsMyClusterEnterprise() (bool, error) {
	return top_svc.isEnterprise, nil
}

func (top_svc *XDCRTopologySvc) IsMyClusterIpv6() bool {
	return top_svc.isIpv6
}

func (top_svc *XDCRTopologySvc) GetLocalHostName() string {
	if top_svc.isIpv6 {
		return base.LocalHostNameIpv6
	} else {
		return base.LocalHostName
	}
}

// currently not used and not implemented
func (top_svc *XDCRTopologySvc) XDCRCompToKVNodeMap() (map[string][]string, error) {
	retmap := make(map[string][]string)
	return retmap, nil
}

// get information about current node from nodeService at /pools/nodes
func (top_svc *XDCRTopologySvc) getHostInfo() (map[string]interface{}, error) {
	nodeList, err := top_svc.getNodeList()
	if err != nil {
		return nil, err
	}

	for _, node := range nodeList {
		nodeInfoMap, ok := node.(map[string]interface{})
		if !ok {
			// should never get here
			return nil, ErrorParsingHostInfo
		}

		thisNode, ok := nodeInfoMap[base.ThisNodeKey]
		if ok {
			thisNodeBool, ok := thisNode.(bool)
			if !ok {
				// should never get here
				return nil, ErrorParsingHostInfo
			}
			if thisNodeBool {
				// found current node
				return nodeInfoMap, nil
			}
		}
	}

	return nil, ErrorParsingHostInfo
}

// get address of current node
func (top_svc *XDCRTopologySvc) getHostAddr() (string, error) {
	nodeInfoMap, err := top_svc.getHostInfo()
	if err != nil {
		return "", err
	}

	return top_svc.getHostAddrFromHostInfo(nodeInfoMap)
}

// get address of current node
func (top_svc *XDCRTopologySvc) getHostAddrFromHostInfo(nodeInfoMap map[string]interface{}) (string, error) {
	hostAddr, ok := nodeInfoMap[base.HostNameKey]
	if !ok {
		// should never get here
		return "", ErrorParsingHostInfo
	}
	hostAddrStr, ok := hostAddr.(string)
	if !ok {
		// should never get here
		return "", ErrorParsingHostInfo
	}
	return hostAddrStr, nil
}

// get name of current node
func (top_svc *XDCRTopologySvc) getHostName() (string, error) {
	hostAddrStr, err := top_svc.getHostAddr()
	if err != nil {
		return "", nil
	}
	hostname := base.GetHostName(hostAddrStr)
	return hostname, nil
}

// get memcached port of current node
func (top_svc *XDCRTopologySvc) getHostMemcachedPortFromHostInfo(nodeInfoMap map[string]interface{}) (uint16, error) {
	ports, ok := nodeInfoMap[base.PortsKey]
	if !ok {
		// should never get here
		return 0, ErrorParsingHostInfo
	}
	portsMap, ok := ports.(map[string]interface{})
	if !ok {
		// should never get here
		return 0, ErrorParsingHostInfo
	}

	directPort, ok := portsMap[base.DirectPortKey]
	if !ok {
		// should never get here
		return 0, ErrorParsingHostInfo
	}
	directPortFloat, ok := directPort.(float64)
	if !ok {
		// should never get here
		return 0, ErrorParsingHostInfo
	}

	return uint16(directPortFloat), nil
}

// implements base.ClusterConnectionInfoProvider
func (top_svc *XDCRTopologySvc) MyConnectionStr() (string, error) {
	host := top_svc.GetLocalHostName()
	return base.GetHostAddr(host, top_svc.adminport), nil
}

func (top_svc *XDCRTopologySvc) MyCredentials() (string, string, base.HttpAuthMech, []byte, bool, []byte, []byte, error) {
	return "", "", base.HttpAuthMechPlain, nil, false, nil, nil, nil
}

func (top_svc *XDCRTopologySvc) MyClusterUuid() (string, error) {
	var poolsInfo map[string]interface{}
	err, statusCode := top_svc.utils.QueryRestApi(top_svc.staticHostAddr(), base.PoolsPath, false, base.MethodGet, "", nil, 0, &poolsInfo, top_svc.logger)
	if err != nil || statusCode != 200 {
		return "", errors.New(fmt.Sprintf("Failed on calling %v, err=%v, statusCode=%v", base.PoolsPath, err, statusCode))
	}

	uuidObj, ok := poolsInfo[base.RemoteClusterUuid]
	if !ok {
		return "", errors.New("Could not get uuid of local cluster.")
	}
	uuid, ok := uuidObj.(string)
	if !ok {
		return "", errors.New(fmt.Sprintf("uuid of local cluster is of wrong type. Expected type: string; Actual type: %s", reflect.TypeOf(uuid)))
	}

	return uuid, nil
}

func (top_svc *XDCRTopologySvc) MyClusterVersion() (string, error) {
	var poolsInfo map[string]interface{}
	err, statusCode := top_svc.utils.QueryRestApi(top_svc.staticHostAddr(), base.PoolsPath, false, base.MethodGet, "", nil, 0, &poolsInfo, top_svc.logger)
	if err != nil || statusCode != 200 {
		return "", errors.New(fmt.Sprintf("Failed on calling %v, err=%v, statusCode=%v", base.PoolsPath, err, statusCode))
	}

	implVersionObj, ok := poolsInfo[base.ImplementationVersionKey]
	if !ok {
		return "", errors.New("Could not get implementation version of local cluster.")
	}
	implVersion, ok := implVersionObj.(string)
	if !ok {
		return "", errors.New(fmt.Sprintf("implementation version of local cluster is of wrong type. Expected type: string; Actual type: %s", reflect.TypeOf(implVersion)))
	}

	// implVersion is of format [version]-[buildNumber]-xxx, e.g., 2.5.1-1114-rel-enterprise
	// we need only the version portion
	implVersionParts := strings.Split(implVersion, "-")
	if len(implVersionParts) < 2 {
		return "", fmt.Errorf("implementation version of local cluster, %v, is of wrong format.", implVersion)
	}

	return implVersionParts[0], nil
}

func (top_svc *XDCRTopologySvc) staticHostAddr() string {
	return "http://" + base.GetHostAddr(top_svc.GetLocalHostName(), top_svc.adminport)
}

func (top_svc *XDCRTopologySvc) IsKVNode() (bool, error) {
	nodeInfoMap, err := top_svc.getHostInfo()
	if err != nil {
		return false, err
	}

	services, ok := nodeInfoMap[base.ServicesKey]
	if !ok {
		//if services is not there, it maybe a node prior to sherlock
		return true, nil
	}
	serviceStrs, ok := services.([]interface{})
	if !ok {
		return false, ErrorParsingServicesInfo
	}

	for _, serviceStr := range serviceStrs {
		svcStr, ok := serviceStr.(string)
		if !ok {
			return false, ErrorParsingServicesInfo

		}
		if svcStr == "kv" {
			return true, nil
		}
	}
	return false, nil
}

func (top_svc *XDCRTopologySvc) getNodeList() ([]interface{}, error) {
	var nodesInfo map[string]interface{}
	err, statusCode := top_svc.utils.QueryRestApi(top_svc.staticHostAddr(), base.NodesPath, false, base.MethodGet, "", nil, 0, &nodesInfo, top_svc.logger)
	if err != nil || statusCode != 200 {
		return nil, errors.New(fmt.Sprintf("Failed on calling %v, err=%v, statusCode=%v", base.NodesPath, err, statusCode))
	}
	// get node list from the map
	nodes, ok := nodesInfo[base.NodesKey]
	if !ok {
		// should never get here
		top_svc.logger.Errorf("no nodes")
		return nil, ErrorParsingHostInfo
	}

	nodeList, ok := nodes.([]interface{})
	if !ok {
		// should never get here
		return nil, ErrorParsingHostInfo
	}
	return nodeList, nil
}
