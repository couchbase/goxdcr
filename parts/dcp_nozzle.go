// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package parts

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/gomemcached"
	base "github.com/couchbase/goxdcr/base"
	common "github.com/couchbase/goxdcr/common"
	gen_server "github.com/couchbase/goxdcr/gen_server"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/utils"
	"reflect"
	"sync"
	"time"
)

const (
	// start settings key name
	DCP_VBTimestamp        = "VBTimestamp"
	DCP_VBTimestampUpdator = "VBTimestampUpdater"
	DCP_Connection_Prefix  = "xdcr:"
)

var dcp_setting_defs base.SettingDefinitions = base.SettingDefinitions{DCP_VBTimestamp: base.NewSettingDef(reflect.TypeOf((*map[uint16]*base.VBTimestamp)(nil)), true)}

var ErrorEmptyVBList = errors.New("Invalid configuration for DCP nozzle. VB list cannot be empty.")

/************************************
/* struct DcpNozzle
*************************************/
type DcpNozzle struct {

	//parent inheritance
	gen_server.GenServer
	AbstractPart

	// the list of vbuckets that the dcp nozzle is responsible for
	// this allows multiple  dcp nozzles to be created for a kv node
	vbnos []uint16

	vb_stream_status map[uint16]bool

	// immutable fields
	bucket  *couchbase.Bucket
	uprFeed *couchbase.UprFeed
	// lock on uprFeed to avoid race condition
	lock_uprFeed sync.Mutex

	finch chan bool

	bOpen bool

	childrenWaitGrp sync.WaitGroup

	counter             int
	start_time          time.Time
	handle_error        bool
	cur_ts              map[uint16]*base.VBTimestamp
	vbtimestamp_updater func(uint16, uint64) (*base.VBTimestamp, error)
}

func NewDcpNozzle(id string,
	bucket *couchbase.Bucket,
	vbnos []uint16,
	logger_context *log.LoggerContext) *DcpNozzle {

	//callback functions from GenServer
	var msg_callback_func gen_server.Msg_Callback_Func
	var exit_callback_func gen_server.Exit_Callback_Func
	var error_handler_func gen_server.Error_Handler_Func

	server := gen_server.NewGenServer(&msg_callback_func,
		&exit_callback_func, &error_handler_func, logger_context, "DcpNozzle")
	part := NewAbstractPartWithLogger(id, server.Logger())

	dcp := &DcpNozzle{
		bucket:           bucket,
		vbnos:            vbnos,
		GenServer:        server,           /*gen_server.GenServer*/
		AbstractPart:     part,             /*AbstractPart*/
		bOpen:            true,             /*bOpen	bool*/
		childrenWaitGrp:  sync.WaitGroup{}, /*childrenWaitGrp sync.WaitGroup*/
		lock_uprFeed:     sync.Mutex{},
		vb_stream_status: make(map[uint16]bool),
	}

	msg_callback_func = nil
	exit_callback_func = dcp.onExit
	error_handler_func = dcp.handleGeneralError

	dcp.Logger().Debugf("Constructed Dcp nozzle %v with vblist %v\n", dcp.Id(), vbnos)

	return dcp

}

func (dcp *DcpNozzle) initialize(settings map[string]interface{}) (err error) {
	dcp.finch = make(chan bool)

	dcp.uprFeed, err = dcp.bucket.StartUprFeed(DCP_Connection_Prefix+dcp.Id(), uint32(0))

	// fetch start timestamp from settings
	dcp.cur_ts = settings[DCP_VBTimestamp].(map[uint16]*base.VBTimestamp)
	dcp.vbtimestamp_updater = settings[DCP_VBTimestampUpdator].(func(uint16, uint64) (*base.VBTimestamp, error))

	//initialize vb_stream_status
	for _, vb := range dcp.vbnos {
		dcp.vb_stream_status[vb] = false
	}
	return
}

func (dcp *DcpNozzle) Open() error {
	if !dcp.bOpen {
		dcp.bOpen = true

	}
	return nil
}

func (dcp *DcpNozzle) Close() error {
	if dcp.bOpen {
		dcp.bOpen = false
	}
	return nil
}

func (dcp *DcpNozzle) Start(settings map[string]interface{}) error {
	dcp.Logger().Infof("Dcp nozzle %v starting ....\n", dcp.Id())

	err := dcp.SetState(common.Part_Starting)
	if err != nil {
		return err
	}

	err = utils.ValidateSettings(dcp_setting_defs, settings, dcp.Logger())
	if err != nil {
		return err
	}

	dcp.Logger().Info("Dcp nozzle starting ....")
	err = dcp.initialize(settings)
	dcp.Logger().Info("....Finished dcp nozzle initialization....")
	if err != nil {
		return err
	}

	// start gen_server
	dcp.start_time = time.Now()
	err = dcp.Start_server()
	if err != nil {
		return err
	}

	// start data processing routine
	dcp.childrenWaitGrp.Add(1)
	go dcp.processData()

	// start vbstreams
	err = dcp.startUprStreams()
	if err != nil {
		dcp.handleGeneralError(err)
		return err
	}

	err = dcp.SetState(common.Part_Running)

	if err == nil {
		dcp.Logger().Info("Dcp nozzle is started")
	}

	return err
}

func (dcp *DcpNozzle) Stop() error {
	dcp.Logger().Infof("Stopping DcpNozzle %v\n", dcp.Id())
	err := dcp.SetState(common.Part_Stopping)
	if err != nil {
		return err
	}
	dcp.closeUprStreams()
	dcp.closeUprFeed()
	dcp.bucket.Close()
	dcp.Logger().Debugf("DcpNozzle %v processed %v items\n", dcp.Id(), dcp.counter)
	err = dcp.Stop_server()

	err = dcp.SetState(common.Part_Stopped)
	if err != nil {
		return err
	}
	dcp.Logger().Infof("DcpNozzle %v is stopped\n", dcp.Id())
	return err

}

func (dcp *DcpNozzle) closeUprStreams() error {
	dcp.Logger().Infof("Closing dcp streams for vb=%v\n", dcp.GetVBList())
	opaque := newOpaque()
	errMap := make(map[uint16]error)

	for _, vbno := range dcp.GetVBList() {
		err := dcp.uprFeed.UprCloseStream(vbno, opaque)
		if err != nil {
			errMap[vbno] = err
		}
	}

	if len(errMap) > 0 {
		msg := fmt.Sprintf("Failed to close upr streams, err=%v\n", errMap)
		dcp.Logger().Error(msg)
		return errors.New(msg)
	}
	return nil
}

func (dcp *DcpNozzle) closeUprFeed() bool {
	var actionTaken = false

	dcp.lock_uprFeed.Lock()
	defer dcp.lock_uprFeed.Unlock()
	if dcp.uprFeed != nil {
		dcp.Logger().Info("Ask uprfeed to close")
		//in the process of stopping, no need to report any error to replication manager anymore
		dcp.handle_error = false

		dcp.uprFeed.Close()
		dcp.uprFeed = nil
		actionTaken = true
	} else {
		dcp.Logger().Info("uprfeed is already closed. No-op")
	}

	return actionTaken
}

func (dcp *DcpNozzle) IsOpen() bool {
	ret := dcp.bOpen
	return ret
}

func (dcp *DcpNozzle) Receive(data interface{}) error {
	// DcpNozzle is a source nozzle and does not receive from upstream nodes
	return nil
}

func (dcp *DcpNozzle) processData() (err error) {
	dcp.Logger().Infof("%v processData starts..........\n", dcp.Id())
	defer dcp.childrenWaitGrp.Done()

	finch := dcp.finch
	mutch := dcp.uprFeed.C
	for {
		dcp.Logger().Debugf("%v processData ....\n", dcp.Id())
		select {
		case <-finch:
			goto done
		case m, ok := <-mutch: // mutation from upstream
			if !ok {
				dcp.Logger().Infof("DCP mutation channel is closed.Stop dcp nozzle now.")
				//set uprFeed to nil
				dcp.uprFeed = nil
				dcp.handleGeneralError(errors.New("DCP stream is closed."))
				goto done
			}
			if m.Opcode == gomemcached.UPR_STREAMREQ {
				if m.Status == gomemcached.NOT_MY_VBUCKET {
					dcp.Logger().Errorf("Raise error condition %v\n", base.ErrorNotMyVbucket)
					dcp.handleGeneralError(base.ErrorNotMyVbucket)
					return base.ErrorNotMyVbucket
				} else if m.Status == gomemcached.ROLLBACK {
					rollbackseq := binary.BigEndian.Uint64(m.Value[:8])
					vbno := m.VBucket

					//need to request the uprstream for the vbucket again
					dcp.cur_ts[vbno], err = dcp.vbtimestamp_updater(vbno, rollbackseq)
					if err != nil {
						dcp.Logger().Errorf("Failed to request dcp stream after receiving roll-back for vb=%v\n", vbno)
						dcp.handleGeneralError(err)
						return err
					}
					dcp.startUprStream(vbno)

				} else if m.Status == gomemcached.SUCCESS {
					vbno := m.VBucket
					_, ok := dcp.vb_stream_status[vbno]
					if ok {
						dcp.vb_stream_status[vbno] = true
						dcp.RaiseEvent(common.StreamingStart, m, dcp, nil, nil)
					} else {
						panic(fmt.Sprintf("Stream for vb=%v is not supposed to be opened\n", vbno))
					}
				}

			} else if m.Opcode == gomemcached.UPR_STREAMEND {
				err_streamend := fmt.Errorf("dcp stream for vb=%v is closed by producer", m.VBucket)
				dcp.Logger().Infof("%v: %v", dcp.Id(), err_streamend)
				dcp.handleGeneralError(err_streamend)
				goto done

			} else {
				if dcp.IsOpen() {
					switch m.Opcode {
					case gomemcached.UPR_MUTATION, gomemcached.UPR_DELETION, gomemcached.UPR_EXPIRATION:
						dcp.counter++
						dcp.RaiseEvent(common.DataReceived, m, dcp, nil /*derivedItems*/, nil /*otherInfos*/)
						dcp.Logger().Tracef("%v, Mutation %v:%v:%v <%v>, counter=%v, ops_per_sec=%v\n",
							dcp.Id(), m.VBucket, m.Seqno, m.Opcode, m.Key, dcp.counter, float64(dcp.counter)/time.Since(dcp.start_time).Seconds())

						// forward mutation downstream through connector
						if err := dcp.Connector().Forward(m); err != nil {
							dcp.handleGeneralError(err)
							goto done
						}
						// raise event for statistics collection
						dcp.RaiseEvent(common.DataProcessed, m, dcp, nil /*derivedItems*/, nil /*otherInfos*/)
					default:
						dcp.Logger().Debugf("Uprevent OpCode=%v, is skipped\n", m.Opcode)
					}
				}
			}
		}
	}
done:
	dcp.Logger().Infof("%v processData exits\n", dcp.Id())
	return
}

func (dcp *DcpNozzle) onExit() {
	//notify the data processing routine
	close(dcp.finch)
	dcp.childrenWaitGrp.Wait()

}

func (dcp *DcpNozzle) StatusSummary() string {
	return fmt.Sprintf("Dcp %v streamed %v items. %v streams inactive", dcp.Id(), dcp.counter, dcp.closedDcpStream())
}

func (dcp *DcpNozzle) handleGeneralError(err error) {

	err1 := dcp.SetState(common.Part_Error)
	if err1 == nil {
		otherInfo := utils.WrapError(err)
		dcp.RaiseEvent(common.ErrorEncountered, nil, dcp, nil, otherInfo)
		dcp.Logger().Errorf("Raise error condition %v\n", err)
	} else {
		dcp.Logger().Debugf("%v in shutdown process. err=%v is ignored\n", dcp.Id(), err)
	}
}

// start, restart or shutdown streams
func (dcp *DcpNozzle) startUprStreams() error {
	for _, vbts := range dcp.cur_ts {
		err := dcp.startUprStream(vbts.Vbno)
		if err != nil {
			return err
		}
	}

	return nil
}

func (dcp *DcpNozzle) startUprStream(vbno uint16) error {
	opaque := newOpaque()
	flags := uint32(0)
	seqEnd := uint64(0xFFFFFFFFFFFFFFFF)
	vbts := dcp.cur_ts[vbno]
	dcp.Logger().Debugf("%v starting vb stream for vb=%v, opaque=%v\n", dcp.Id(), vbno, opaque)
	return dcp.uprFeed.UprRequestStream(vbno, opaque, flags, vbts.Vbuuid, vbts.Seqno, seqEnd, vbts.SnapshotStart, vbts.SnapshotEnd)
}

// Set vb list in dcp nozzle
func (dcp *DcpNozzle) SetVBList(vbnos []uint16) error {
	if len(vbnos) == 0 {
		return ErrorEmptyVBList
	}
	dcp.vbnos = vbnos
	return nil
}

func (dcp *DcpNozzle) GetVBList() []uint16 {
	return dcp.vbnos
}

func (dcp *DcpNozzle) closedDcpStream() []uint16 {
	ret := []uint16{}
	for vb, active := range dcp.vb_stream_status {
		if !active {
			ret = append(ret, vb)
		}
	}
	return ret
}

// generate a new 16 bit opaque value set as MSB.
func newOpaque() uint16 {
	// bit 26 ... 42 from UnixNano().
	return uint16((uint64(time.Now().UnixNano()) >> 26) & 0xFFFF)
}
