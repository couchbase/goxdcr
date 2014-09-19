// replication manager's adminport.

package replication_manager

import (
	/*"net/http"
	"reflect"
	"fmt"
	"strings"
	"errors"*/
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/protobuf"
	ap "github.com/couchbase/indexing/secondary/adminport"
	sicommon "github.com/couchbase/indexing/secondary/common"
	//siprotobuf "github.com/couchbase/indexing/secondary/protobuf"
	log "github.com/Xiaomei-Zhang/couchbase_goxdcr/util"
	//utils "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/utils"
)

var StaticPaths = [3]string{protobuf.CREATE_REPLICATION_PATH, protobuf.INTERNAL_SETTINGS_PATH, protobuf.SETTINGS_REPLICATIONS_PATH}
var DynamicPathPrefixes = [3]string{protobuf.DELETE_REPLICATION_PREFIX, protobuf.SETTINGS_REPLICATIONS_PATH, protobuf.STATISTICS_PREFIX}

var logger_ap *log.CommonLogger = log.NewLogger("AdminPort", log.LogLevelInfo)

// list of requests handled by this adminport
var reqCreateReplication = &protobuf.CreateReplicationRequest{}
var reqDeleteReplication = &protobuf.DeleteReplicationRequest{}
var reqViewSettings = &protobuf.ViewSettingsRequest{}
var reqChangeGlobalSettings = &protobuf.ChangeGlobalSettingsRequest{}
var reqChangeReplicationSettings = &protobuf.ChangeReplicationSettingsRequest{}
var reqChangeInternalSettings = &protobuf.ChangeInternalSettingsRequest{}
var reqGetStatistics = &protobuf.GetStatisticsRequest{}

type xdcrRestHandler struct {
}

// admin-port entry point
func mainAdminPort(laddr string, h *xdcrRestHandler) {
	var err error

	reqch := make(chan ap.Request)
	server := ap.NewHTTPServer("xdcr", laddr, ""/*urlPrefix*/, reqch/*TODO, new(XDCRHandler)*/)
	server.Register(reqCreateReplication)
	server.Register(reqDeleteReplication)
	server.Register(reqViewSettings)
	server.Register(reqChangeGlobalSettings)
	server.Register(reqChangeReplicationSettings)
	server.Register(reqChangeInternalSettings)
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
			if response, err := h.handleRequest(msg, server); err == nil {
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

func (h *xdcrRestHandler) handleRequest(
	msg ap.MessageMarshaller,
	server ap.Server) (response ap.MessageMarshaller, err error) {

	switch request := msg.(type) {
	case *protobuf.CreateReplicationRequest:
		response = h.doCreateReplicationRequest(request)
	case *protobuf.DeleteReplicationRequest:
		response = h.doDeleteReplicationRequest(request)
	case *protobuf.ViewSettingsRequest:
		response = h.doViewSettingsRequest(request)
	case *protobuf.ChangeGlobalSettingsRequest:
		response = h.doChangeGlobalSettingsRequest(request)
	case *protobuf.ChangeReplicationSettingsRequest:
		response = h.doChangeReplicationSettingsRequest(request)
	case *protobuf.ChangeInternalSettingsRequest:
		response = h.doChangeInternalSettingsRequest(request)
	case *protobuf.GetStatisticsRequest:
		response = h.doGetStatisticsRequest(request)
	default:
		err = sicommon.ErrorInvalidRequest
	}
	return response, err
}

func (h *xdcrRestHandler) doDeleteReplicationRequest(request *protobuf.DeleteReplicationRequest) ap.MessageMarshaller {
	logger_ap.Debugf("doDeleteReplicationRequest\n")

	// err := rm.DeleteReplication(request)
	// return siprotobuf.NewError(err)
	return nil
}

func (h *xdcrRestHandler) doViewSettingsRequest(request *protobuf.ViewSettingsRequest) ap.MessageMarshaller {
	logger_ap.Debugf("doViewSettingsRequest\n")

	return nil
}

func (h *xdcrRestHandler) doChangeGlobalSettingsRequest(request *protobuf.ChangeGlobalSettingsRequest) ap.MessageMarshaller {
	logger_ap.Debugf("doChangeGlobalSettingsRequest\n")

	return nil
}

func (h *xdcrRestHandler) doChangeReplicationSettingsRequest(request *protobuf.ChangeReplicationSettingsRequest) ap.MessageMarshaller {
	logger_ap.Debugf("doChangeReplicationSettingsRequest\n")

	return nil
}

func (h *xdcrRestHandler) doChangeInternalSettingsRequest(request *protobuf.ChangeInternalSettingsRequest) ap.MessageMarshaller {
	logger_ap.Debugf("doChangeInternalSettingsRequest\n")

	return nil
}

func (h *xdcrRestHandler) doGetStatisticsRequest(request *protobuf.GetStatisticsRequest) ap.MessageMarshaller {
	logger_ap.Debugf("doGetStatisticsRequest\n")

	return nil
}

func (h *xdcrRestHandler) doCreateReplicationRequest(request *protobuf.CreateReplicationRequest) ap.MessageMarshaller {
	logger_ap.Debugf("doCreateReplicationRequest\n")

	h.startReplication(request)

	// forward replication request to other KV nodes involved
	h.forwardReplicationRequest(request)

	// TODO
	return nil
}

func (h *xdcrRestHandler) startReplication(request *protobuf.CreateReplicationRequest) error {
	//TODO: implement it
	tocluster := request.GetToCluster ()
	tobucket := request.GetToBucket()
	frombucket := request.GetFromBucket()
	fromcluster, err := XDCRCompTopologyService().MyCluster()
	if err != nil {
		return err
	}
	
	err = CreateReplication(fromcluster, frombucket, tocluster, tobucket, ""/*filter_name*/, nil)
	return err
}

func (h *xdcrRestHandler) forwardReplicationRequest(request *protobuf.CreateReplicationRequest) error {
	if !request.GetForward() {
		// do nothing if "forward" flag in request is false
		// this would happen if the request itself is a forwarded request
		return nil
	}

	// turn off "forward" flag to prevent the forwarded request from being forwarded again
	off := false
	request.Forward = &off

//	deployConfig, err := h.GetXDCRDeployConfig()
//	if err != nil {
//		return err
//	}
//
//	// TODO get current xdcr node addr
//	curXDCRAddr := "test"
//
//	for xdcrAddr := range deployConfig.XDCRNodeMap {
//		if xdcrAddr != curXDCRAddr {
//			err := rm.forwardReplicationRequestToXDCRNode(request, xdcrAddr)
//		}
//	}

	myAddr, err := XDCRCompTopologyService().MyHost () 
	if err != nil {
		return err
	}
	
	xdcrNodesMap, err := XDCRCompTopologyService().XDCRTopology()
	if err != nil {
		return err
	}
	for xdcrNode, port := range xdcrNodesMap {
		if xdcrNode != myAddr {
			err := h.forwardReplicationRequestToXDCRNode(request, xdcrNode, int(port))
//			if err != nil {
//				// TODO what if forward fails. How do we cancel previously forwarded requests?
//			}
		}
	}
	return err
}

func (h *xdcrRestHandler) forwardReplicationRequestToXDCRNode(request *protobuf.CreateReplicationRequest, xdcrAddr string, port int) error {
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
	    // add get/post suffix to name to distinguish between lookup/modify requests
		if r.Method == "" || r.Method == "GET" {
			name += GET_SUFFIX
		} else {
			name += POST_SUFFIX
		}
		return name, nil
	}
}*/
