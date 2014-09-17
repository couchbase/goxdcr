// replication manager's adminport.

package replicationmanager

import (
	ap "github.com/couchbase/indexing/secondary/adminport"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/protobuf"
	base "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/base"
)

// list of requests handled by this adminport
var reqReplication = &protobuf.ReplicationRequest{}

// admin-port entry point
func mainAdminPort(laddr string, rm *ReplicationManager) {
	var err error

	reqch := make(chan ap.Request)
	server := ap.NewHTTPServer("xdcr", laddr, base.AdminportURLPrefix, reqch)
	server.Register(reqReplication)

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
		c.Errorf("%v %v\n", rm.logPrefix, err)
	}
	c.Infof("%v exited !\n", rm.logPrefix)
	server.Stop()
}

func (rm *ReplicationManager) handleRequest(
	msg ap.MessageMarshaller,
	server ap.Server) (response ap.MessageMarshaller, err error) {

	switch request := msg.(type) {
	case *protobuf.ReplicationRequest:
		response = rm.doReplicationRequest(request)
	default:
		err = c.ErrorInvalidRequest
	}
	return response, err
}

// handler for replication request
func (rm *ReplicationManager) doReplicationRequest(request *protobuf.ReplicationRequest) ap.MessageMarshaller {
	c.Debugf("%v doReplicationRequest\n", rm.logPrefix)
	
	// TODO
	err := rm.StartReplication(request)
	
	response := protobuf.NewReplicationResponse(request, err)
	
	// forward replication request to other KV nodes involved
	rm.forwardReplicationRequest(request)

	return response
}

func (rm *ReplicationManager) forwardReplicationRequest(request *protobuf.ReplicationRequest) error {
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

func (rm *ReplicationManager) forwardReplicationRequestToXDCRNode(request *protobuf.ReplicationRequest, xdcrAddr string) error {
	var response ap.MessageMarshaller
	client := ap.NewHTTPClient(xdcrAddr, "")
	return client.Request(request, response)
}
