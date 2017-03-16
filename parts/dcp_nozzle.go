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
	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	base "github.com/couchbase/goxdcr/base"
	common "github.com/couchbase/goxdcr/common"
	gen_server "github.com/couchbase/goxdcr/gen_server"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/simple_utils"
	"github.com/couchbase/goxdcr/utils"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// start settings key name
	DCP_VBTimestamp         = "VBTimestamps"
	DCP_VBTimestampUpdator  = "VBTimestampUpdater"
	DCP_Connection_Prefix   = "xdcr:"
	EVENT_DCP_DISPATCH_TIME = "dcp_dispatch_time"
	EVENT_DCP_DATACH_LEN    = "dcp_datach_length"
	DCP_Stats_Interval      = "stats_interval"
)

type DcpStreamState int

const (
	Dcp_Stream_NonInit = iota
	Dcp_Stream_Init    = iota
	Dcp_Stream_Active  = iota
)

var dcp_inactive_stream_check_interval = 10 * time.Second

var dcp_setting_defs base.SettingDefinitions = base.SettingDefinitions{DCP_VBTimestamp: base.NewSettingDef(reflect.TypeOf((*map[uint16]*base.VBTimestamp)(nil)), false)}

var ErrorEmptyVBList = errors.New("Invalid configuration for DCP nozzle. VB list cannot be empty.")

var MaxCountStreamsInactive uint8 = 40

var SizeOfUprFeedRandName = 16
var MaxRetryForIdGeneration = 5

type vbtsWithLock struct {
	ts   *base.VBTimestamp
	lock *sync.RWMutex
}

type streamStatusWithLock struct {
	state DcpStreamState
	lock  *sync.RWMutex
}

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

	// key - vb#
	// value - first seqno seen with xattr
	vb_xattr_seqno_map map[uint16]*uint64

	vb_stream_status map[uint16]*streamStatusWithLock

	// immutable fields
	sourceBucketName string
	targetBucketName string
	client           *mcc.Client
	uprFeed          *mcc.UprFeed
	// lock on uprFeed to avoid race condition
	lock_uprFeed sync.RWMutex

	finch chan bool

	bOpen      bool
	lock_bOpen sync.RWMutex

	childrenWaitGrp sync.WaitGroup

	counter_received uint32
	counter_sent     uint32
	// the counter_received stats from last dcp check
	counter_received_last uint32

	// the number of check intervals after which dcp still has inactive streams
	// inactive streams will be restarted after this count exceeds MaxCountStreamsInactive
	counter_streams_inactive uint8

	start_time          time.Time
	handle_error        bool
	cur_ts              map[uint16]*vbtsWithLock
	vbtimestamp_updater func(uint16, uint64) (*base.VBTimestamp, error)

	// the number of times that the dcp nozzle did not receive anything from dcp when there are
	// items remaining in dcp
	// dcp is considered to be stuck and pipeline broken when this number reaches a limit
	dcp_miss_count     int
	max_dcp_miss_count int

	xdcr_topology_svc service_def.XDCRCompTopologySvc

	stats_interval           time.Duration
	stats_interval_change_ch chan bool

	user_agent string

	is_capi bool
}

func NewDcpNozzle(id string,
	sourceBucketName, targetBucketName string,
	vbnos []uint16,
	xdcr_topology_svc service_def.XDCRCompTopologySvc,
	is_capi bool,
	logger_context *log.LoggerContext) *DcpNozzle {

	//callback functions from GenServer
	var msg_callback_func gen_server.Msg_Callback_Func
	var exit_callback_func gen_server.Exit_Callback_Func
	var error_handler_func gen_server.Error_Handler_Func

	server := gen_server.NewGenServer(&msg_callback_func,
		&exit_callback_func, &error_handler_func, logger_context, "DcpNozzle")
	part := NewAbstractPartWithLogger(id, server.Logger())

	dcp := &DcpNozzle{
		sourceBucketName:         sourceBucketName,
		targetBucketName:         targetBucketName,
		vbnos:                    vbnos,
		vb_xattr_seqno_map:       make(map[uint16]*uint64),
		GenServer:                server, /*gen_server.GenServer*/
		AbstractPart:             part,   /*AbstractPart*/
		bOpen:                    true,   /*bOpen	bool*/
		lock_bOpen:               sync.RWMutex{},
		childrenWaitGrp:          sync.WaitGroup{}, /*childrenWaitGrp sync.WaitGroup*/
		lock_uprFeed:             sync.RWMutex{},
		cur_ts:                   make(map[uint16]*vbtsWithLock),
		vb_stream_status:         make(map[uint16]*streamStatusWithLock),
		xdcr_topology_svc:        xdcr_topology_svc,
		stats_interval_change_ch: make(chan bool, 1),
		is_capi:                  is_capi,
	}

	msg_callback_func = nil
	exit_callback_func = dcp.onExit
	error_handler_func = dcp.handleGeneralError

	for _, vbno := range vbnos {
		dcp.cur_ts[vbno] = &vbtsWithLock{lock: &sync.RWMutex{}, ts: nil}
		dcp.vb_stream_status[vbno] = &streamStatusWithLock{lock: &sync.RWMutex{}, state: Dcp_Stream_NonInit}
		if !dcp.is_capi {
			var xattr_seqno uint64 = 0
			dcp.vb_xattr_seqno_map[vbno] = &xattr_seqno
		}
	}

	dcp.composeUserAgent()

	dcp.Logger().Debugf("Constructed Dcp nozzle %v with vblist %v\n", dcp.Id(), vbnos)

	return dcp

}

func (dcp *DcpNozzle) composeUserAgent() {
	dcp.user_agent = simple_utils.ComposeUserAgentWithBucketNames("Goxdcr Dcp ", dcp.sourceBucketName, dcp.targetBucketName)
}

func (dcp *DcpNozzle) initialize(settings map[string]interface{}) (err error) {
	dcp.finch = make(chan bool)

	addr, err := dcp.xdcr_topology_svc.MyMemcachedAddr()
	if err != nil {
		return err
	}

	dcp.client, err = utils.GetMemcachedConnection(addr, dcp.sourceBucketName, dcp.user_agent, dcp.Logger())
	if err != nil {
		return err
	}

	// xdcr will send ack to upr feed
	dcp.uprFeed, err = dcp.client.NewUprFeedWithConfig(true /*ackByClient*/)
	if err != nil {
		return err
	}

	randName, err := simple_utils.GenerateRandomId(SizeOfUprFeedRandName, MaxRetryForIdGeneration)
	if err != nil {
		return err
	}

	uprFeedName := DCP_Connection_Prefix + dcp.Id() + ":" + randName

	if dcp.is_capi {
		// no need to enable xattr for capi replication
		err = dcp.uprFeed.UprOpen(uprFeedName, uint32(0), base.UprFeedBufferSize)
	} else {
		// always enable xattr for xmem replication
		// even if target cluster does not support xattr, we still need to get xattr data type from dcp
		// for source side conflict resolution
		err = dcp.uprFeed.UprOpenWithXATTR(uprFeedName, uint32(0), base.UprFeedBufferSize)
	}

	if err != nil {
		dcp.Logger().Errorf("%v upr open failed. err=%v.\n", dcp.Id(), err)
		return err
	}

	// fetch start timestamp from settings
	dcp.vbtimestamp_updater = settings[DCP_VBTimestampUpdator].(func(uint16, uint64) (*base.VBTimestamp, error))

	if val, ok := settings[DCP_Stats_Interval]; ok {
		dcp.stats_interval = time.Duration(val.(int)) * time.Millisecond
	} else {
		return errors.New("setting 'stats_interval' is missing")
	}

	return
}

func (dcp *DcpNozzle) Open() error {
	dcp.lock_bOpen.Lock()
	defer dcp.lock_bOpen.Unlock()
	if !dcp.bOpen {
		dcp.bOpen = true

	}
	return nil
}

func (dcp *DcpNozzle) Close() error {
	dcp.lock_bOpen.Lock()
	defer dcp.lock_bOpen.Unlock()
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

	dcp.Logger().Infof("%v starting ....\n", dcp.Id())
	err = dcp.initialize(settings)
	if err != nil {
		return err
	}
	dcp.Logger().Infof("%v has been initialized\n", dcp.Id())

	// start gen_server
	dcp.start_time = time.Now()
	err = dcp.Start_server()
	if err != nil {
		return err
	}

	//start datachan length stats collection
	dcp.childrenWaitGrp.Add(1)
	go dcp.collectDcpDataChanLen(settings)

	uprFeed := dcp.getUprFeed()
	if uprFeed != nil {
		uprFeed.StartFeedWithConfig(base.UprFeedDataChanLength)
	}

	// start data processing routine
	dcp.childrenWaitGrp.Add(1)
	go dcp.processData()

	// start vbstreams
	dcp.childrenWaitGrp.Add(1)
	go dcp.startUprStreams()

	// check for inactive vbstreams
	dcp.childrenWaitGrp.Add(1)
	go dcp.checkInactiveUprStreams()

	err = dcp.SetState(common.Part_Running)

	if err == nil {
		dcp.Logger().Infof("%v has been started", dcp.Id())
	} else {
		dcp.Logger().Errorf("%v failed to start. err=%v", dcp.Id(), err)
	}

	return err
}

func (dcp *DcpNozzle) Stop() error {
	dcp.Logger().Infof("%v is stopping...\n", dcp.Id())
	err := dcp.SetState(common.Part_Stopping)
	if err != nil {
		return err
	}

	//notify children routines
	if dcp.finch != nil {
		close(dcp.finch)
	}

	dcp.closeUprStreams()
	dcp.closeUprFeed()
	dcp.Logger().Debugf("%v received %v items, sent %v items\n", dcp.Id(), dcp.counterReceived(), dcp.counterSent())
	err = dcp.Stop_server()

	err = dcp.SetState(common.Part_Stopped)
	if err != nil {
		return err
	}
	dcp.Logger().Infof("%v has been stopped\n", dcp.Id())
	return err

}

func (dcp *DcpNozzle) closeUprStreams() error {
	dcp.lock_uprFeed.Lock()
	defer dcp.lock_uprFeed.Unlock()

	if dcp.uprFeed != nil {
		dcp.Logger().Infof("%v Closing dcp streams for vb=%v\n", dcp.Id(), dcp.GetVBList())
		opaque := newOpaque()
		errMap := make(map[uint16]error)

		for _, vbno := range dcp.GetVBList() {
			stream_state, err := dcp.getStreamState(vbno)
			if err != nil {
				return err
			}
			if stream_state == Dcp_Stream_Active {
				err := dcp.uprFeed.CloseStream(vbno, opaque)
				if err != nil {
					errMap[vbno] = err
				}
			} else {
				dcp.Logger().Infof("%v There is no active stream for vb=%v\n", dcp.Id(), vbno)
			}
		}

		if len(errMap) > 0 {
			msg := fmt.Sprintf("Failed to close upr streams, err=%v\n", errMap)
			dcp.Logger().Errorf("%v %v", dcp.Id(), msg)
			return errors.New(msg)
		}
	} else {
		dcp.Logger().Infof("%v uprfeed is already closed. No-op", dcp.Id())
	}
	return nil
}

func (dcp *DcpNozzle) closeUprFeed() bool {
	var actionTaken = false

	dcp.lock_uprFeed.Lock()
	defer dcp.lock_uprFeed.Unlock()
	if dcp.uprFeed != nil {
		dcp.Logger().Infof("%v Ask uprfeed to close", dcp.Id())
		//in the process of stopping, no need to report any error to replication manager anymore
		dcp.handle_error = false

		dcp.uprFeed.Close()
		dcp.uprFeed = nil
		actionTaken = true
	} else {
		dcp.Logger().Infof("%v uprfeed is already closed. No-op", dcp.Id())
	}

	return actionTaken
}

func (dcp *DcpNozzle) IsOpen() bool {
	dcp.lock_bOpen.RLock()
	defer dcp.lock_bOpen.RUnlock()
	return dcp.bOpen
}

func (dcp *DcpNozzle) Receive(data interface{}) error {
	// DcpNozzle is a source nozzle and does not receive from upstream nodes
	return nil
}

func (dcp *DcpNozzle) processData() (err error) {
	dcp.Logger().Infof("%v processData starts..........\n", dcp.Id())
	defer dcp.childrenWaitGrp.Done()

	finch := dcp.finch
	uprFeed := dcp.getUprFeed()
	if uprFeed == nil {
		dcp.Logger().Infof("%v DCP feed has been closed. processData exits\n", dcp.Id())
		return
	}
	mutch := uprFeed.C
	for {
		select {
		case <-finch:
			goto done
		case m, ok := <-mutch: // mutation from upstream
			if !ok {
				dcp.Logger().Infof("%v DCP mutation channel has been closed.Stop dcp nozzle now.", dcp.Id())
				//close uprFeed
				dcp.closeUprFeed()
				dcp.handleGeneralError(errors.New("DCP upr feed has been closed."))
				goto done
			}

			// increment ack bytes in uprfeed, which is necessary for uprfeed flow control to work
			uprFeed.IncrementAckBytes(m.AckSize)

			if m.Opcode == mc.UPR_STREAMREQ {
				if m.Status == mc.NOT_MY_VBUCKET {
					vb_err := fmt.Errorf("Received error %v on vb %v\n", base.ErrorNotMyVbucket, m.VBucket)
					dcp.Logger().Errorf("%v %v", dcp.Id(), vb_err)
					dcp.handleVBError(m.VBucket, vb_err)
				} else if m.Status == mc.ROLLBACK {
					rollbackseq := binary.BigEndian.Uint64(m.Value[:8])
					vbno := m.VBucket

					//need to request the uprstream for the vbucket again
					updated_ts, err := dcp.vbtimestamp_updater(vbno, rollbackseq)
					if err != nil {
						err = fmt.Errorf("Failed to request dcp stream after receiving roll-back for vb=%v. err=%v\n", vbno, err)
						dcp.Logger().Errorf("%v %v", dcp.Id(), err)
						dcp.handleGeneralError(err)
						return err
					}
					err = dcp.setTS(vbno, updated_ts, true)
					if err != nil {
						err = fmt.Errorf("Failed to update start seqno for vb=%v. err=%v\n", vbno, err)
						dcp.Logger().Errorf("%v %v", dcp.Id(), err)
						dcp.handleGeneralError(err)
						return err

					}
					dcp.startUprStream(vbno, updated_ts)

				} else if m.Status == mc.SUCCESS {
					vbno := m.VBucket
					_, ok := dcp.vb_stream_status[vbno]
					if ok {
						dcp.setStreamState(vbno, Dcp_Stream_Active)
						dcp.RaiseEvent(common.NewEvent(common.StreamingStart, m, dcp, nil, nil))
					} else {
						panic(fmt.Sprintf("Stream for vb=%v is not supposed to be opened\n", vbno))
					}
				}

			} else if m.Opcode == mc.UPR_STREAMEND {
				vbno := m.VBucket
				stream_status, err := dcp.getStreamState(vbno)
				if err == nil && stream_status == Dcp_Stream_Active {
					err_streamend := fmt.Errorf("dcp stream for vb=%v is closed by producer", m.VBucket)
					dcp.Logger().Infof("%v: %v", dcp.Id(), err_streamend)
					dcp.handleVBError(vbno, err_streamend)
				}

			} else {
				if dcp.IsOpen() {
					switch m.Opcode {
					case mc.UPR_MUTATION, mc.UPR_DELETION, mc.UPR_EXPIRATION:
						start_time := time.Now()
						dcp.incCounterReceived()
						dcp.RaiseEvent(common.NewEvent(common.DataReceived, m, dcp, nil /*derivedItems*/, nil /*otherInfos*/))
						if !dcp.is_capi {
							dcp.handleXattr(m)
						}

						// forward mutation downstream through connector
						if err := dcp.Connector().Forward(m); err != nil {
							dcp.handleGeneralError(err)
							goto done
						}
						dcp.incCounterSent()
						// raise event for statistics collection
						dispatch_time := time.Since(start_time)
						dcp.RaiseEvent(common.NewEvent(common.DataProcessed, m, dcp, nil /*derivedItems*/, dispatch_time.Seconds()*1000000 /*otherInfos*/))
					case mc.UPR_SNAPSHOT:
						dcp.RaiseEvent(common.NewEvent(common.SnapshotMarkerReceived, m, dcp, nil /*derivedItems*/, nil /*otherInfos*/))
					default:
						dcp.Logger().Debugf("%v Uprevent OpCode=%v, is skipped\n", dcp.Id(), m.Opcode)
					}
				}
			}
		}
	}
done:
	dcp.Logger().Infof("%v processData exits\n", dcp.Id())
	return
}

func (dcp *DcpNozzle) handleXattr(upr_event *mcc.UprEvent) {
	event_has_xattr := simple_utils.HasXattr(upr_event.DataType)
	if event_has_xattr {
		xattr_seqno_obj, ok := dcp.vb_xattr_seqno_map[upr_event.VBucket]
		if ok {
			xattr_seqno := atomic.LoadUint64(xattr_seqno_obj)
			if xattr_seqno == 0 {
				// set xattr_seqno only if it has never been set before
				atomic.StoreUint64(xattr_seqno_obj, upr_event.Seqno)
			}
		}
	}
}

func (dcp *DcpNozzle) GetXattrSeqnos() map[uint16]uint64 {
	xattr_seqnos := make(map[uint16]uint64)
	for vbno, xattr_seqno_obj := range dcp.vb_xattr_seqno_map {
		xattr_seqnos[vbno] = atomic.LoadUint64(xattr_seqno_obj)
	}
	return xattr_seqnos
}

func (dcp *DcpNozzle) onExit() {
	dcp.childrenWaitGrp.Wait()

}

func (dcp *DcpNozzle) StatusSummary() string {
	msg := fmt.Sprintf("%v received %v items, sent %v items.", dcp.Id(), dcp.counterReceived(), dcp.counterSent())
	streams_inactive := dcp.inactiveDcpStreamsWithState()
	if len(streams_inactive) > 0 {
		msg += fmt.Sprintf(" streams inactive: %v", streams_inactive)
	}
	return msg
}

func (dcp *DcpNozzle) handleGeneralError(err error) {

	err1 := dcp.SetState(common.Part_Error)
	if err1 == nil {
		dcp.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, dcp, nil, err))
		dcp.Logger().Errorf("%v Raise error condition %v\n", dcp.Id(), err)
	} else {
		dcp.Logger().Debugf("%v in shutdown process. err=%v is ignored\n", dcp.Id(), err)
	}
}

func (dcp *DcpNozzle) handleVBError(vbno uint16, err error) {
	additionalInfo := &base.VBErrorEventAdditional{vbno, err, base.VBErrorType_Source}
	dcp.RaiseEvent(common.NewEvent(common.VBErrorEncountered, nil, dcp, nil, additionalInfo))
}

// start steam request will be sent when starting seqno is negotiated, it may take a few
func (dcp *DcpNozzle) startUprStreams() error {
	defer dcp.childrenWaitGrp.Done()

	var err error = nil
	dcp.Logger().Infof("%v: startUprStreams for %v...\n", dcp.Id(), dcp.GetVBList())

	init_ch := make(chan bool, 1)
	init_ch <- true

	finch := dcp.finch

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-finch:
			goto done
		case <-init_ch:
			err = dcp.startUprStreams_internal(dcp.GetVBList())
			if err != nil {
				return err
			}
		case <-ticker.C:
			streams_non_init := dcp.nonInitDcpStreams()
			if len(streams_non_init) == 0 {
				goto done
			}
			err = dcp.startUprStreams_internal(streams_non_init)
			if err != nil {
				return err
			}
		}
	}
done:
	dcp.Logger().Infof("%v: all dcp stream have been initialized.\n", dcp.Id())

	return nil
}

func (dcp *DcpNozzle) startUprStreams_internal(streams_to_start []uint16) error {
	for _, vbno := range streams_to_start {
		vbts, err := dcp.getTS(vbno, true)
		if err == nil && vbts != nil {
			err = dcp.startUprStream(vbno, vbts)
			if err != nil {
				dcp.handleGeneralError(err)
				dcp.Logger().Infof("%v: startUprStreams errored out, err=%v\n", dcp.Id(), err)
				return err
			}

		}
	}
	return nil
}

func (dcp *DcpNozzle) startUprStream(vbno uint16, vbts *base.VBTimestamp) error {
	opaque := newOpaque()
	flags := uint32(0)
	seqEnd := uint64(0xFFFFFFFFFFFFFFFF)
	dcp.Logger().Debugf("%v starting vb stream for vb=%v, opaque=%v\n", dcp.Id(), vbno, opaque)

	dcp.lock_uprFeed.RLock()
	defer dcp.lock_uprFeed.RUnlock()
	if dcp.uprFeed != nil {
		statusObj, ok := dcp.vb_stream_status[vbno]
		if ok && statusObj != nil {
			err := dcp.uprFeed.UprRequestStream(vbno, opaque, flags, vbts.Vbuuid, vbts.Seqno, seqEnd, vbts.SnapshotStart, vbts.SnapshotEnd)
			if err == nil {
				dcp.setStreamState(vbno, Dcp_Stream_Init)
			}
			return err
		} else {
			panic(fmt.Sprintf("%v Try to startUprStream for invalid vbno=%v", dcp.Id(), vbno))

		}
	}
	return nil
}

func (dcp *DcpNozzle) getUprFeed() *mcc.UprFeed {
	dcp.lock_uprFeed.RLock()
	defer dcp.lock_uprFeed.RUnlock()
	return dcp.uprFeed
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

type stateCheckFunc func(state DcpStreamState) bool

func (dcp *DcpNozzle) getDcpStreams(stateCheck stateCheckFunc) []uint16 {
	ret := []uint16{}
	for _, vb := range dcp.GetVBList() {
		state, _ := dcp.getStreamState(vb)
		if stateCheck(state) {
			ret = append(ret, vb)
		}
	}
	return ret
}

func (dcp *DcpNozzle) inactiveDcpStreams() []uint16 {
	return dcp.getDcpStreams(inactiveStateCheck)
}

func inactiveStateCheck(state DcpStreamState) bool {
	return state != Dcp_Stream_Active
}

func (dcp *DcpNozzle) initedButInactiveDcpStreams() []uint16 {
	return dcp.getDcpStreams(initedButInactiveStateCheck)
}

func initedButInactiveStateCheck(state DcpStreamState) bool {
	return state == Dcp_Stream_Init
}

func (dcp *DcpNozzle) nonInitDcpStreams() []uint16 {
	return dcp.getDcpStreams(nonInitStateCheck)
}

func nonInitStateCheck(state DcpStreamState) bool {
	return state == Dcp_Stream_NonInit
}

func (dcp *DcpNozzle) inactiveDcpStreamsWithState() map[uint16]DcpStreamState {
	ret := make(map[uint16]DcpStreamState)
	for _, vb := range dcp.GetVBList() {
		state, _ := dcp.getStreamState(vb)
		if state != Dcp_Stream_Active {
			ret[vb] = state
		}
	}
	return ret
}

// generate a new 16 bit opaque value set as MSB.
func newOpaque() uint16 {
	// bit 26 ... 42 from UnixNano().
	return uint16((uint64(time.Now().UnixNano()) >> 26) & 0xFFFF)
}

func (dcp *DcpNozzle) UpdateSettings(settings map[string]interface{}) error {
	ts_obj := utils.GetSettingFromSettings(settings, DCP_VBTimestamp)
	if ts_obj != nil {
		new_ts, ok := settings[DCP_VBTimestamp].(map[uint16]*base.VBTimestamp)
		if !ok || new_ts == nil {
			panic(fmt.Sprintf("setting %v should have type of map[uint16]*base.VBTimestamp", DCP_VBTimestamp))
		}
		err := dcp.onUpdateStartingSeqno(new_ts)
		if err != nil {
			return err
		}
	}

	if _, ok := settings[DCP_Stats_Interval]; ok {
		dcp.stats_interval = time.Duration(settings[DCP_Stats_Interval].(int)) * time.Millisecond
		dcp.stats_interval_change_ch <- true
	}

	return nil
}

func (dcp *DcpNozzle) onUpdateStartingSeqno(new_startingSeqnos map[uint16]*base.VBTimestamp) error {
	for vbno, vbts := range new_startingSeqnos {
		ts_withlock, ok := dcp.cur_ts[vbno]
		if ok && ts_withlock != nil {
			ts_withlock.lock.Lock()
			defer ts_withlock.lock.Unlock()
			if !dcp.isTSSet(vbno, false) {
				//only update the cur_ts if starting seqno has not been set yet
				dcp.Logger().Debugf("%v: Starting dcp stream for vb=%v, len(closed streams)=%v\n", dcp.Id(), vbno, len(dcp.inactiveDcpStreams()))
				dcp.setTS(vbno, vbts, false)
			}
		}
	}
	return nil
}

func (dcp *DcpNozzle) populateVBTS(vbts_map map[uint16]*base.VBTimestamp) error {
	if vbts_map != nil {
		for _, vbno := range dcp.vbnos {
			ts := vbts_map[vbno]
			if ts != nil {
				err := dcp.setTS(vbno, ts, true)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (dcp *DcpNozzle) setTS(vbno uint16, ts *base.VBTimestamp, need_lock bool) error {
	ts_entry := dcp.cur_ts[vbno]
	if ts_entry != nil {
		if need_lock {
			ts_entry.lock.Lock()
			defer ts_entry.lock.Unlock()
		}
		ts_entry.ts = ts
		return nil
	} else {
		return fmt.Errorf("setTS failed: vbno=%v is not tracked in cur_ts map", vbno)
	}
}

func (dcp *DcpNozzle) getTS(vbno uint16, need_lock bool) (*base.VBTimestamp, error) {
	ts_entry := dcp.cur_ts[vbno]
	if ts_entry != nil {
		if need_lock {
			ts_entry.lock.RLock()
			defer ts_entry.lock.RUnlock()
		}
		return ts_entry.ts, nil
	} else {
		return nil, fmt.Errorf("getTS failed: vbno=%v is not tracked in cur_ts map", vbno)
	}
}

//if the vbno is not belongs to this DcpNozzle, return true
func (dcp *DcpNozzle) isTSSet(vbno uint16, need_lock bool) bool {
	ts_entry := dcp.cur_ts[vbno]
	if ts_entry != nil {
		if need_lock {
			ts_entry.lock.RLock()
			defer ts_entry.lock.RUnlock()
		}
		return ts_entry.ts != nil
	}
	return true
}

func (dcp *DcpNozzle) setStreamState(vbno uint16, streamState DcpStreamState) {
	statusObj, ok := dcp.vb_stream_status[vbno]
	if ok && statusObj != nil {
		statusObj.lock.Lock()
		defer statusObj.lock.Unlock()
		statusObj.state = streamState
	} else {
		panic(fmt.Sprintf("Try to set stream state to invalid vbno=%v", vbno))
	}
}

func (dcp *DcpNozzle) getStreamState(vbno uint16) (DcpStreamState, error) {
	statusObj, ok := dcp.vb_stream_status[vbno]
	if ok && statusObj != nil {
		statusObj.lock.RLock()
		defer statusObj.lock.RUnlock()
		return statusObj.state, nil
	} else {
		return 0, fmt.Errorf("Try to get stream state to invalid vbno=%v", vbno)
	}
}

func (dcp *DcpNozzle) SetMaxMissCount(max_dcp_miss_count int) {
	dcp.max_dcp_miss_count = max_dcp_miss_count
}

func (dcp *DcpNozzle) checkInactiveUprStreams() {
	defer dcp.childrenWaitGrp.Done()

	fin_ch := dcp.finch

	dcp_inactive_stream_check_ticker := time.NewTicker(dcp_inactive_stream_check_interval)
	defer dcp_inactive_stream_check_ticker.Stop()

	for {
		select {
		case <-fin_ch:
			dcp.Logger().Infof("%v checkInactiveUprStreams routine is exiting because dcp nozzle has been stopped\n", dcp.Id())
			return
		case <-dcp_inactive_stream_check_ticker.C:
			if dcp.isFeedClosed() {
				dcp.Logger().Infof("%v checkInactiveUprStreams routine is exiting because upr feed has been closed\n", dcp.Id())
				dcp.closeUprFeed()
				dcp.handleGeneralError(errors.New("DCP upr feed has been closed."))
				return
			}
			err := simple_utils.ExecWithTimeout(dcp.checkInactiveUprStreams_once, 1000*time.Millisecond, dcp.Logger())
			if err != nil {
				// ignore error and continue
				dcp.Logger().Infof("Received error when checking inactive steams for %v. err=%v\n", dcp.Id(), err)
			}
		}
	}
}

// check if feed has been closed
func (dcp *DcpNozzle) isFeedClosed() bool {
	dcp.lock_uprFeed.RLock()
	defer dcp.lock_uprFeed.RUnlock()
	if dcp.uprFeed != nil {
		return dcp.uprFeed.Closed()
	}
	return true
}

// check if inactive streams need to be restarted
func (dcp *DcpNozzle) checkInactiveUprStreams_once() error {
	streams_inactive := dcp.initedButInactiveDcpStreams()
	if len(streams_inactive) > 0 {
		dcp.counter_streams_inactive++
		dcp.Logger().Infof("%v incrementing counter for inactive streams %v\n", dcp.Id(), dcp.counter_streams_inactive)
		if dcp.counter_streams_inactive > MaxCountStreamsInactive {
			dcp.Logger().Infof("%v restarting inactive streams %v\n", dcp.Id(), streams_inactive)
			dcp.forceCloseUprStreams(streams_inactive)
			err := dcp.startUprStreams_internal(streams_inactive)
			if err != nil {
				return err
			}
			dcp.counter_streams_inactive = 0
		}
	}
	return nil
}

// try to close upr streams for the vbnos specified, even if the upr streams currently have inactive state.
// startUprStream requests may have been sent out earlier. try to close to be safe. ignore any errors
func (dcp *DcpNozzle) forceCloseUprStreams(vbnos []uint16) {
	dcp.lock_uprFeed.RLock()
	defer dcp.lock_uprFeed.RUnlock()

	if dcp.uprFeed != nil {
		dcp.Logger().Infof("%v closing dcp streams for vbs=%v\n", dcp.Id(), vbnos)
		opaque := newOpaque()
		errMap := make(map[uint16]error)

		for _, vbno := range vbnos {
			err := dcp.uprFeed.CloseStream(vbno, opaque)
			if err != nil {
				errMap[vbno] = err
			}
		}

		if len(errMap) > 0 {
			dcp.Logger().Infof("%v Failed to close upr streams, err=%v\n", dcp.Id(), errMap)
		}
	}

}

// check if dcp is stuck
func (dcp *DcpNozzle) CheckStuckness(dcp_stats map[string]map[string]string) error {
	counter_received := dcp.counterReceived()
	if counter_received > dcp.counter_received_last {
		// dcp is ok if received more items from dcp
		dcp.counter_received_last = counter_received
		dcp.dcp_miss_count = 0
		return nil
	}

	if counter_received > dcp.counterSent() {
		// if dcp nozzle is holding an item that has not been processed by downstream parts,
		// cannot declare dcp broken regardless of what other stats say
		dcp.dcp_miss_count = 0
		return nil
	}

	// skip checking if dcp still has inactive streams
	if len(dcp.inactiveDcpStreams()) > 0 {
		dcp.dcp_miss_count = 0
		return nil
	}

	// check if there are items remaining in dcp
	dcp_has_items := dcp.dcpHasRemainingItemsForXdcr(dcp_stats)
	if !dcp_has_items {
		dcp.dcp_miss_count = 0
		return nil
	}

	// if we get here, there is probably something wrong with dcp
	dcp.dcp_miss_count++
	dcp.Logger().Infof("%v Incrementing dcp miss count. Dcp miss count = %v\n", dcp.Id(), dcp.dcp_miss_count)

	if dcp.dcp_miss_count > dcp.max_dcp_miss_count {
		//declare pipeline broken
		dcp.Logger().Errorf("%v is stuck", dcp.Id())
		return errors.New("Dcp is stuck")
	}

	return nil
}

func (dcp *DcpNozzle) dcpHasRemainingItemsForXdcr(dcp_stats map[string]map[string]string) bool {
	// Each dcp nozzle has an "items_remaining" stats in stats_map.
	// An example key for the stats is "eq_dcpq:xdcr:dcp_f58e0727200a19771e4459925908dd66/default/target_10.17.2.102:12000_0:items_remaining"
	xdcr_items_remaining_key := base.DCP_XDCR_STATS_PREFIX + dcp.Id() + base.DCP_XDCR_ITEMS_REMAINING_SUFFIX

	kv_nodes, err := dcp.xdcr_topology_svc.MyKVNodes()
	if err != nil {
		panic("Cannot get kv nodes")
	}

	for _, kv_node := range kv_nodes {
		per_node_stats_map, ok := dcp_stats[kv_node]
		if ok {
			if items_remaining_stats_str, ok := per_node_stats_map[xdcr_items_remaining_key]; ok {
				items_remaining_stats_int, err := strconv.ParseInt(items_remaining_stats_str, base.ParseIntBase, base.ParseIntBitSize)
				if err != nil {
					dcp.Logger().Errorf("%v Items remaining stats, %v, is not of integer type.", dcp.Id(), items_remaining_stats_str)
					continue
				}
				if items_remaining_stats_int > 0 {
					return true
				}
			}
		} else {
			dcp.Logger().Errorf("%v Failed to find dcp stats in statsMap returned for server=%v", dcp.Id(), kv_node)
		}
	}

	return false
}

func (dcp *DcpNozzle) counterReceived() uint32 {
	return atomic.LoadUint32(&dcp.counter_received)
}

func (dcp *DcpNozzle) incCounterReceived() {
	atomic.AddUint32(&dcp.counter_received, 1)
}

func (dcp *DcpNozzle) counterSent() uint32 {
	return atomic.LoadUint32(&dcp.counter_sent)
}

func (dcp *DcpNozzle) incCounterSent() {
	atomic.AddUint32(&dcp.counter_sent, 1)
}

func (dcp *DcpNozzle) collectDcpDataChanLen(settings map[string]interface{}) {
	defer dcp.childrenWaitGrp.Done()
	ticker := time.NewTicker(dcp.stats_interval)
	defer ticker.Stop()
	for {
		select {
		case <-dcp.finch:
			return
		case <-dcp.stats_interval_change_ch:
			ticker.Stop()
			ticker = time.NewTicker(dcp.stats_interval)
		case <-ticker.C:
			dcp.getDcpDataChanLen()
		}
	}

}

func (dcp *DcpNozzle) getDcpDataChanLen() {
	dcp_dispatch_len := 0
	dcp.lock_uprFeed.RLock()
	defer dcp.lock_uprFeed.RUnlock()
	if dcp.uprFeed == nil {
		//upr feed has been closed
		return
	} else {
		dcp_dispatch_len = len(dcp.uprFeed.C)
	}
	dcp.RaiseEvent(common.NewEvent(common.StatsUpdate, nil, dcp, nil, dcp_dispatch_len))

}
