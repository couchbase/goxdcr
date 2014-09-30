// replication manager's adminport.

package replication_manager

import (
	"errors"
	"fmt"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/base"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/protobuf"
	ap "github.com/ysui6888/indexing/secondary/adminport"
	sicommon "github.com/couchbase/indexing/secondary/common"
	"net/http"
	"reflect"
	"strings"
	//siprotobuf "github.com/couchbase/indexing/secondary/protobuf"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr/log"
	utils "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/utils"
)

var StaticPaths = [3]string{protobuf.CreateReplicationPath, protobuf.InternalSettingsPath, protobuf.SettingsReplicationsPath}
var DynamicPathPrefixes = [3]string{protobuf.DeleteReplicationPrefix, protobuf.SettingsReplicationsPath, protobuf.StatisticsPrefix}

var logger_ap *log.CommonLogger = log.NewLogger("AdminPort", log.DefaultLoggerContext)

// list of requests handled by this adminport
var reqCreateReplication = &protobuf.CreateReplicationRequest{}
var reqDeleteReplication = &protobuf.DeleteReplicationRequest{}
var reqViewInternalSettings = &protobuf.ViewInternalSettingsRequest{}
var reqChangeGlobalSettings = &protobuf.ChangeGlobalSettingsRequest{}
var reqChangeReplicationSettings = &protobuf.ChangeReplicationSettingsRequest{}
var reqChangeInternalSettings = &protobuf.ChangeInternalSettingsRequest{}
var reqGetStatistics = &protobuf.GetStatisticsRequest{}

type xdcrRestHandler struct {
}

// admin-port entry point
func MainAdminPort(laddr string) {
	var err error

	h := new(xdcrRestHandler)
	reqch := make(chan ap.Request)
	server := ap.NewHTTPServer("xdcr", laddr, base.AdminportUrlPrefix, reqch, new(XDCRHandler))
	server.Register(reqCreateReplication)
	server.Register(reqDeleteReplication)
	server.Register(reqViewInternalSettings)
	server.Register(reqChangeGlobalSettings)
	server.Register(reqChangeReplicationSettings)
	server.Register(reqChangeInternalSettings)
	server.Register(reqGetStatistics)

	server.Start()
	logger_ap.Infof("server started %v !\n", laddr)

loop:
	for {
		select {
		case req, ok := <-reqch: // admin requests are serialized here
			logger_ap.Infof("request received %v !\n", req)
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
		response, err = h.doCreateReplicationRequest(request)
	case *protobuf.DeleteReplicationRequest:
		response, err = h.doDeleteReplicationRequest(request)
	case *protobuf.ViewInternalSettingsRequest:
		response, err = h.doViewInternalSettingsRequest(request)
	case *protobuf.ChangeGlobalSettingsRequest:
		response, err = h.doChangeGlobalSettingsRequest(request)
	case *protobuf.ChangeReplicationSettingsRequest:
		response, err = h.doChangeReplicationSettingsRequest(request)
	case *protobuf.ChangeInternalSettingsRequest:
		response, err = h.doChangeInternalSettingsRequest(request)
	case *protobuf.GetStatisticsRequest:
		response, err = h.doGetStatisticsRequest(request)
	default:
		err = sicommon.ErrorInvalidRequest
	}
	return response, err
}

func (h *xdcrRestHandler) doViewInternalSettingsRequest(request *protobuf.ViewInternalSettingsRequest) (ap.MessageMarshaller, error) {
	logger_ap.Debugf("doViewInternalSettingsRequest\n")

	internalSettings, err := InternalSettingsService().GetInternalReplicationSettings()
	if err != nil {
		return nil, err
	}

	internalSettingsMsg := protobuf.NewInternalSettings(internalSettings)
	return &protobuf.ViewInternalSettingsResponse{Settings: internalSettingsMsg}, nil
}

func (h *xdcrRestHandler) doChangeGlobalSettingsRequest(request *protobuf.ChangeGlobalSettingsRequest) (ap.MessageMarshaller, error) {
	logger_ap.Debugf("doChangeGlobalSettingsRequest\n")

	return h.changeInternalSettings(request.GetSettings())
}

func (h *xdcrRestHandler) doChangeReplicationSettingsRequest(request *protobuf.ChangeReplicationSettingsRequest) (ap.MessageMarshaller, error) {
	logger_ap.Debugf("doChangeReplicationSettingsRequest\n")

	replId := request.GetId()
	replSettings := request.GetSettings()
	replSpec, err := MetadataService().ReplicationSpec(replId)
	if err != nil {
		return nil, err
	}
	replSpec.Settings().UpdateSettingsFromMap(protobuf.ReplicationSettingsToMap(replSettings))
	err = MetadataService().SetReplicationSpec(*replSpec)
	if err != nil {
		return nil, err
	}
	return &protobuf.EmptyMessage{}, nil
}

func (h *xdcrRestHandler) doChangeInternalSettingsRequest(request *protobuf.ChangeInternalSettingsRequest) (ap.MessageMarshaller, error) {
	logger_ap.Debugf("doChangeInternalSettingsRequest\n")

	return h.changeInternalSettings(request.GetSettings())
}

func (h *xdcrRestHandler) changeInternalSettings(inputSettings *protobuf.InternalSettings) (ap.MessageMarshaller, error) {
	internalSettings, err := InternalSettingsService().GetInternalReplicationSettings()
	if err != nil {
		return nil, err
	}

	internalSettings.UpdateSettingsFromMap(protobuf.InternalSettingsToMap(inputSettings))
	err = InternalSettingsService().SetInternalReplicationSettings(internalSettings)
	if err != nil {
		return nil, err
	}
	return &protobuf.EmptyMessage{}, nil
}

func (h *xdcrRestHandler) doGetStatisticsRequest(request *protobuf.GetStatisticsRequest) (ap.MessageMarshaller, error) {
	logger_ap.Debugf("doGetStatisticsRequest\n")

	return nil, nil
}

func (h *xdcrRestHandler) doCreateReplicationRequest(request *protobuf.CreateReplicationRequest) (ap.MessageMarshaller, error) {
	logger_ap.Debugf("doCreateReplicationRequest\n")

	replicationId, err := h.startReplication(request)
	if err != nil {
		return nil, err
	}

	// forward replication request to other KV nodes involved if necessary
	if request.GetForward() {
		// turn off "forward" flag to prevent the forwarded request from being forwarded again
		off := false
		request.Forward = &off

		forwardedNodesMap, err := h.forwardReplicationRequest(request)
		if err != nil {
			// if some forward request failed, call deleteRelication on all nodes where the replication has been started
			for nodeAddr, infoArr := range forwardedNodesMap {
				// first element in infoArr is port number
				port := infoArr[1].(uint16)
				// second element in infoArr is a CreateReplicationResponse
				createReplResponse := infoArr[0].(*protobuf.CreateReplicationResponse)
				// ask each node that called startReplication before to delete the replication
				h.forwardReplicationRequestToXDCRNode(protobuf.NewDeleteReplicationRequest(createReplResponse.GetId()), nodeAddr, int(port))
			}
			// call deleteReplication on current node
			h.doDeleteReplicationRequest(protobuf.NewDeleteReplicationRequest(replicationId))

			return nil, err
		}
	}

	return protobuf.NewCreateReplicationResponse(replicationId), nil
}

func (h *xdcrRestHandler) doDeleteReplicationRequest(request *protobuf.DeleteReplicationRequest) (ap.MessageMarshaller, error) {
	replId := request.GetId()
	logger_ap.Infof("doDeleteReplicationRequest on repliation id, %v\n", replId)

	err := DeleteReplication(replId)
	if err != nil {
		return nil, err
	}

	// forward replication request to other KV nodes involved if necessary
	if request.GetForward() {
		// turn off "forward" flag to prevent the forwarded request from being forwarded again
		off := false
		request.Forward = &off

		_, err = h.forwardReplicationRequest(request)
		if err != nil {
			// if some forward request failed, return error
			return nil, err
		}
	}

	// no return message in success case
	return &protobuf.EmptyMessage{}, nil
}

func (h *xdcrRestHandler) startReplication(request *protobuf.CreateReplicationRequest) (string, error) {
	tocluster := request.GetToCluster()
	tobucket := request.GetToBucket()
	frombucket := request.GetFromBucket()
	fromcluster, err := XDCRCompTopologyService().MyCluster()
	if err != nil {
		return "", err
	}
	filterName := request.GetFilterName()
	settings := request.GetSettings()

	return CreateReplication(fromcluster, frombucket, tocluster, tobucket, filterName, protobuf.ReplicationSettingsToMap(settings))
}

// forward requests to other nodes.
// in case of error, return a list of nodes that the request has been forwarded to, so that caller can take undo action on these nodes
func (h *xdcrRestHandler) forwardReplicationRequest(request ap.MessageMarshaller) (map[string][]interface{}, error) {
	myAddr, err := XDCRCompTopologyService().MyHost()
	if err != nil {
		return nil, err
	}

	xdcrNodesMap, err := XDCRCompTopologyService().XDCRTopology()
	if err != nil {
		return nil, err
	}

	forwardedNodesMap := make(map[string][]interface{})
	for xdcrNode, port := range xdcrNodesMap {
		if xdcrNode != myAddr {
			response, err := h.forwardReplicationRequestToXDCRNode(request, xdcrNode, int(port))
			if err != nil {
				return forwardedNodesMap, err
			} else {
				array := []interface{}{port, response}
				forwardedNodesMap[xdcrNode] = array
			}
		}
	}
	return nil, nil
}

func (h *xdcrRestHandler) forwardReplicationRequestToXDCRNode(request ap.MessageMarshaller, xdcrAddr string, port int) (ap.MessageMarshaller, error) {
	var response ap.MessageMarshaller
	client := ap.NewHTTPClient(xdcrAddr, "")
	err := client.Request(request, response)
	return response, err
}

//XDCR implementation of RequestHandler
type XDCRHandler struct {
	ap.Handler
}

// handles incoming requests
func (h *XDCRHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var err error

	s := h.GetServer()

	logger_ap.Infof("Request with path, %v, and method, %v\n", r.URL.Path, r.Method)

	// Fault-tolerance. No need to crash the server in case of panic.
	defer func() {
		if r := recover(); r != nil {
			logger_ap.Errorf("adminport.request.recovered `%v`\n", r)
		} else if err != nil {
			logger_ap.Errorf("%v\n", err)
		}
	}()

	var msg ap.MessageMarshaller
	// encode the entire http request into a byte array for use by MessageMarshaller
	data, err := utils.EncodeHttpRequestIntoByteArray(r)
	if err != nil {

		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// look up key of message based on http request
	key, err := h.GetMessageKeyFromRequest(r)
	logger_ap.Infof("message key %v\n", key)
	if err != nil {

		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// get the message corresponsing to message key
	msg, ok := s.GetMessages()[key]
	if !ok {
		logger_ap.Infof("messages: %v !\n", s.GetMessages())
		http.Error(w, fmt.Sprintf("Invalid message key, %v", key), http.StatusInternalServerError)
		return
	}

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

// Get the message key from http request
func (h *XDCRHandler) GetMessageKeyFromRequest(r *http.Request) (string, error) {
	var key string
	// remove adminport url prefix from path
	path := r.URL.Path[len(base.AdminportUrlPrefix):]
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
				key = dynPathPrefix + protobuf.DynamicSuffix
				break
			}
		}
	}

	if len(key) == 0 {
		return "", errors.New(fmt.Sprintf("Invalid path, %v, in http Request.", r.URL.Path))
	} else {
		key = base.AdminportUrlPrefix + key
		// add http method suffix to name to ensure uniqueness
		key += protobuf.UrlDelimiter + strings.ToUpper(r.Method)

		return key, nil
	}
}
