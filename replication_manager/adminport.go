// replication manager's adminport.

package replicationmanager

import (
	/*"net/http"
	"reflect"
	"fmt"
	"strings"
	"errors"*/
	ap "github.com/couchbase/indexing/secondary/adminport"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/protobuf"
	base "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/base"
	log "github.com/Xiaomei-Zhang/couchbase_goxdcr/util"
	//utils "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/utils"
)

const(
	REMOTE_CLUSTERS_PATH = "pools/default/remoteClusters"
	CREATE_REPLICATION_PATH = "controller/createReplication"
	INTERNAL_SETTINGS_PATH = "internalSettings"
	SETTINGS_REPLICATION_PATH = "settings/replications"
	DELETE_REPLICATION_PATH_PREFIX = "controller/cancelXDCR"
	STATISTICS_PATH_PREFIX = "pools/default/buckets"
	DYNAMIC_PATH_ID = "/dynamic"
)

var StaticPaths = [4]string{REMOTE_CLUSTERS_PATH, CREATE_REPLICATION_PATH, INTERNAL_SETTINGS_PATH, SETTINGS_REPLICATION_PATH}
var DynamicPathPrefixes = [4]string{REMOTE_CLUSTERS_PATH, DELETE_REPLICATION_PATH_PREFIX, SETTINGS_REPLICATION_PATH, STATISTICS_PATH_PREFIX}

var logger_ap *log.CommonLogger = log.NewLogger("AdminPort", log.LogLevelInfo)

// list of requests handled by this adminport
var reqGetRemoteClusters = &protobuf.GetRemoteClustersRequest{}
var reqCreateRemoteCluster = &protobuf.CreateRemoteClusterRequest{}
var reqDeleteRemoteCluster = &protobuf.DeleteRemoteClusterRequest{}
var reqCreateReplication = &protobuf.CreateReplicationRequest{}
var reqDeleteReplication = &protobuf.DeleteReplicationRequest{}
var reqViewSettings = &protobuf.ViewSettingsRequest{}
var reqChangeSettings = &protobuf.ChangeSettingsRequest{}
var reqGetStatistics = &protobuf.GetStatisticsRequest{}

// admin-port entry point
func mainAdminPort(laddr string, rm *ReplicationManager) {
	var err error

	reqch := make(chan ap.Request)
	server := ap.NewHTTPServer("xdcr", laddr, base.AdminportURLPrefix, reqch/*TODO, new(XDCRHandler)*/)
	server.Register(reqGetRemoteClusters)
	server.Register(reqCreateRemoteCluster)
	server.Register(reqDeleteRemoteCluster)
	server.Register(reqCreateReplication)
	server.Register(reqDeleteReplication)
	server.Register(reqViewSettings)
	server.Register(reqChangeSettings)
	server.Register(reqGetStatistics)
	
	server.Start()

loop:
	for {
		select {
		case req, ok := <-reqch: // admin requests are serialized here
			if ok == false {
				break loop
			}
			msg := req.GetMessage()
			if response, err := rm.handleRequest(msg, server); err == nil {
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

func (rm *ReplicationManager) handleRequest(
	msg ap.MessageMarshaller,
	server ap.Server) (response ap.MessageMarshaller, err error) {

	switch request := msg.(type) {
	case *protobuf.GetRemoteClustersRequest:
		response = rm.doGetRemoteClustersRequest(request)
	case *protobuf.CreateRemoteClusterRequest:
		response = rm.doCreateRemoteClusterRequest(request)
	case *protobuf.DeleteRemoteClusterRequest:
		response = rm.doDeleteRemoteClusterRequest(request)
	case *protobuf.CreateReplicationRequest:
		response = rm.doCreateReplicationRequest(request)
	case *protobuf.DeleteReplicationRequest:
		response = rm.doDeleteReplicationRequest(request)
	case *protobuf.ViewSettingsRequest:
		response = rm.doViewSettingsRequest(request)
	case *protobuf.ChangeSettingsRequest:
		response = rm.doChangeSettingsRequest(request)
	case *protobuf.GetStatisticsRequest:
		response = rm.doGetStatisticsRequest(request)
	default:
		err = c.ErrorInvalidRequest
	}
	return response, err
}

func (rm *ReplicationManager) doGetRemoteClustersRequest(request *protobuf.GetRemoteClustersRequest) ap.MessageMarshaller {
	logger_ap.Debugf("doGetRemoteClustersRequest\n")

	return nil
}

func (rm *ReplicationManager) doCreateRemoteClusterRequest(request *protobuf.CreateRemoteClusterRequest) ap.MessageMarshaller {
	logger_ap.Debugf("doCreateRemoteClusterRequest\n")

	return nil
}

func (rm *ReplicationManager) doDeleteRemoteClusterRequest(request *protobuf.DeleteRemoteClusterRequest) ap.MessageMarshaller {
	logger_ap.Debugf("doDeleteRemoteClusterRequest\n")

	return nil
}

func (rm *ReplicationManager) doDeleteReplicationRequest(request *protobuf.DeleteReplicationRequest) ap.MessageMarshaller {
	logger_ap.Debugf("doDeleteReplicationRequest\n")

	return nil
}

func (rm *ReplicationManager) doViewSettingsRequest(request *protobuf.ViewSettingsRequest) ap.MessageMarshaller {
	logger_ap.Debugf("doViewSettingsRequest\n")

	return nil
}

func (rm *ReplicationManager) doChangeSettingsRequest(request *protobuf.ChangeSettingsRequest) ap.MessageMarshaller {
	logger_ap.Debugf("doChangeSettingsRequest\n")

	return nil
}

func (rm *ReplicationManager) doGetStatisticsRequest(request *protobuf.GetStatisticsRequest) ap.MessageMarshaller {
	logger_ap.Debugf("doGetStatisticsRequest\n")

	return nil
}

func (rm *ReplicationManager) doCreateReplicationRequest(request *protobuf.CreateReplicationRequest) ap.MessageMarshaller {
	logger_ap.Debugf("doCreateReplicationRequest\n")
	
	 rm.StartReplication(request)
	
	// forward replication request to other KV nodes involved
	rm.forwardReplicationRequest(request)

	// TODO
	return nil
}

func (rm *ReplicationManager) forwardReplicationRequest(request *protobuf.CreateReplicationRequest) error {
	if !request.GetForward() {
	    // do nothing if "forward" flag in request is false
	    // this would happen if the request itself is a forwarded request
		return nil;
	}
	
	// turn off "forward" flag to prevent the forwarded request from being forwarded again
	off := false
	request.Forward = &off
	
	deployConfig, err := rm.GetXDCRDeployConfig()
	if err != nil {
		return err
	}
	
	// TODO get current xdcr node addr
	curXDCRAddr := "test"
	
	for xdcrAddr := range deployConfig.XDCRNodeMap {
		if xdcrAddr != curXDCRAddr {
			err := rm.forwardReplicationRequestToXDCRNode(request, xdcrAddr)
			if err != nil {
			// TODO what if forward fails. How do we cancel previously forwarded requests? 
			}
		}
	}
	
	return nil
}

func (rm *ReplicationManager) forwardReplicationRequestToXDCRNode(request *protobuf.CreateReplicationRequest, xdcrAddr string) error {
	var response ap.MessageMarshaller
	client := ap.NewHTTPClient(xdcrAddr, "")
	return client.Request(request, response)
}

/* TODO enable after secondary index changes are checked in
//XDCR implementation of RequestHandler 
type XDCRHandler struct{
	ap.Handler
}

// handles incoming requests
func (h *XDCRHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var err error
	
	s := h.GetServer()

	logger_ap.Infof("Request with path %v\n", r.URL.Path)

	// Fault-tolerance. No need to crash the server in case of panic.
	defer func() {
		if r := recover(); r != nil {
			logger_ap.Errorf("adminport.request.recovered `%v`\n",  r)
		} else if err != nil {
			logger_ap.Errorf("%v\n",err)
		} 
	}()

	var msg ap.MessageMarshaller
	// encode the entire http request into a byte array for use by MessageMarshaller
	data, err := utils.EncodeRequestIntoByteArray(r)
	if err != nil {
	
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	// look up name of message based on http request
	name, err := h.GetMessageNameFromRequest(r)
	if err != nil {
	
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	// get the message corresponsing to message name
	msg = s.GetMessages()[name]
	
	// Get an instance of request type and decode request into that.
	typeOfMsg := reflect.ValueOf(msg).Elem().Type()
	msg = reflect.New(typeOfMsg).Interface().(ap.MessageMarshaller)
	if err = msg.Decode(data); err != nil {
		err = fmt.Errorf("%v, %v", ap.ErrorDecodeRequest, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if msg == nil {
		err = ap.ErrorPathNotFound
		http.Error(w, "path not found", http.StatusNotFound)
		return
	}

	val := s.ProcessMessage(msg)

	switch v := (val).(type) {
	case ap.MessageMarshaller:
		if data, err := v.Encode(); err == nil {
			header := w.Header()
			header["Content-Type"] = []string{v.ContentType()}
			w.Write(data)
		} else {
			err = fmt.Errorf("%v, %v", ap.ErrorEncodeResponse, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			logger_ap.Errorf("%v", err)
		}

	case error:
		http.Error(w, v.Error(), http.StatusInternalServerError)
		err = fmt.Errorf("%v, %v", ap.ErrorInternal, v)
		logger_ap.Errorf("%v", err)
	}
}

// Get the message name from http request
func (h *XDCRHandler) GetMessageNameFromRequest(r *http.Request) (string, error) {
	var name string
	path := r.URL.Path
	for _, staticPath := range StaticPaths {
		if path == staticPath {
			// if path in url is a static path, use it as name
			name = path
			break
		} 
	}
	
	if len(name) == 0 {
		// if path does not match any static paths, check if it has a prefix that matches dynamic path prefixes
		for _, dynPathPrefix := range DynamicPathPrefixes {
			if strings.HasPrefix(path, dynPathPrefix) {
				name = path + DYNAMIC_PATH_ID
				break
			}
		}
	}
	
	if len(name) == 0 {
		return "", errors.New(fmt.Sprintf("Invalid path, %v, in http Request.", path))
	} else {
		return name, nil
	}
}*/

