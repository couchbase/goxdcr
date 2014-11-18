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
	"errors"
	"fmt"
	common "github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/log"
	base "github.com/couchbase/goxdcr/base"
	gen_server "github.com/couchbase/goxdcr/gen_server"
	"github.com/couchbase/gomemcached"
	"github.com/couchbase/goxdcr/utils"
	"github.com/couchbaselabs/go-couchbase"
	"reflect"
	"sync"
	"time"
)

const (
	// start settings key name
	DCP_SETTINGS_KEY = "dcp"
)

var dcp_setting_defs base.SettingDefinitions = base.SettingDefinitions{DCP_SETTINGS_KEY: base.NewSettingDef(reflect.TypeOf((*map[uint16]*base.VBTimestamp)(nil)), true)}

var ErrorEmptyVBList = errors.New("Invalid configuration for DCP nozzle. VB list cannot be empty.")

/************************************
/* struct DcpNozzle
*************************************/
type DcpNozzle struct {

	//parent inheritance
	gen_server.GenServer
	AbstractPart

	// the list of vbuckets that the kvfeed is responsible for
	// this allows multiple kvfeeds to be created for a kv node
	vbnos []uint16
	// immutable fields
	bucket  *couchbase.Bucket
	uprFeed *couchbase.UprFeed
	// lock on uprFeed to avoid race condition
	lock_uprFeed sync.Mutex

	finch chan bool

	bOpen bool

	childrenWaitGrp sync.WaitGroup

	counter    int
	start_time time.Time
}

func NewDcpNozzle(id string,
	bucket *couchbase.Bucket,
	vbnos []uint16,
	logger_context *log.LoggerContext) *DcpNozzle {

	//callback functions from GenServer
	var msg_callback_func gen_server.Msg_Callback_Func
	var exit_callback_func gen_server.Exit_Callback_Func
	var error_handler_func gen_server.Error_Handler_Func

	var isStarted_callback_func IsStarted_Callback_Func

	server := gen_server.NewGenServer(&msg_callback_func,
		&exit_callback_func, &error_handler_func, logger_context, "DcpNozzle")
	isStarted_callback_func = server.IsStarted
	part := NewAbstractPartWithLogger(id, &isStarted_callback_func, server.Logger())

	dcp := &DcpNozzle{
		bucket:          bucket,
		vbnos:           vbnos,
		GenServer:       server,           /*gen_server.GenServer*/
		AbstractPart:    part,             /*AbstractPart*/
		bOpen:           true,             /*bOpen	bool*/
		childrenWaitGrp: sync.WaitGroup{}, /*childrenWaitGrp sync.WaitGroup*/
		lock_uprFeed:    sync.Mutex{},
	}

	msg_callback_func = nil
	exit_callback_func = dcp.onExit
	error_handler_func = dcp.handleGeneralError
	
	dcp.Logger().Infof("Constructed Dcp nozzle %v with vblist %v\n", dcp.Id(), vbnos)

	return dcp

}

func (dcp *DcpNozzle) initialize(settings map[string]interface{}) (err error) {
	dcp.finch = make(chan bool)
	feedName := fmt.Sprintf("%v", time.Now().UnixNano())
	dcp.uprFeed, err = dcp.bucket.StartUprFeed(feedName, uint32(0))
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

	err := utils.ValidateSettings(dcp_setting_defs, settings, dcp.Logger())
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
	err = dcp.startUprStream(settings)
	if err != nil {
		dcp.Stop()
		return err
	}

	dcp.Logger().Info("Dcp nozzle is started")

	return err
}

func (dcp *DcpNozzle) Stop() error {
	// Stop() could be called from more than one go routines in dcp nozzle
	// Here we are using uprFeed to prevent the actual stop operations
	// from being executed more than once. In other words, the first call to Stop()
	// would get uprFeed to be closed and set to nil, and subsequent call(s) to Stop()
	// would be no ops. 
	if dcp.closeUprFeed() {
		dcp.Logger().Infof("Stopping DcpNozzle %v\n", dcp.Id())
		dcp.Logger().Debugf("DcpNozzle %v processed %v items\n", dcp.Id(), dcp.counter)
		err := dcp.Stop_server()
		dcp.Logger().Infof("DcpNozzle %v is stopped\n", dcp.Id())
		return err
	} else {
		dcp.Logger().Debugf("Stop() on DcpNozzle %v is skipped since the nozzle has already been stopped\n", dcp.Id())
	}
		
	return nil

}

func (dcp *DcpNozzle) closeUprFeed() bool {
	var actionTaken = false
	dcp.lock_uprFeed.Lock()
	defer dcp.lock_uprFeed.Unlock()
	if dcp.uprFeed != nil {	
		dcp.uprFeed.Close()
		dcp.uprFeed = nil
		actionTaken = true
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
		if dcp.IsOpen() {
			select {
			case <-finch:
				goto done
			case m, ok := <-mutch: // mutation from upstream
				if ok == false {
					dcp.Stop()
					goto done
				}
				if m.Status == gomemcached.NOT_MY_VBUCKET {
					dcp.Logger().Errorf("Raise error condition %v\n", base.ErrorNotMyVbucket)
					otherInfo := utils.WrapError(base.ErrorNotMyVbucket)
					dcp.RaiseEvent(common.ErrorEncountered, nil, dcp, nil, otherInfo)
					return base.ErrorNotMyVbucket
				}
				dcp.counter++
				dcp.Logger().Tracef("%v, Mutation %v:%v:%v <%v>, counter=%v, ops_per_sec=%v\n",
					dcp.Id(), m.VBucket, m.Seqno, m.Opcode, m.Key, dcp.counter, float64(dcp.counter)/time.Since(dcp.start_time).Seconds())

				// forward mutation downstream through connector
				if err := dcp.Connector().Forward(m); err != nil {
					dcp.handleGeneralError(err)
				}
				// raise event for statistics collection
				dcp.RaiseEvent(common.DataProcessed, nil /*item*/, dcp, nil /*derivedItems*/, nil /*otherInfos*/)
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
	return fmt.Sprintf("Dcp %v streamed %v items", dcp.Id(), dcp.counter)
}

func (dcp *DcpNozzle) handleGeneralError(err error) {
	dcp.Logger().Errorf("Raise error condition %v\n", err)
	otherInfo := utils.WrapError(err)
	dcp.RaiseEvent(common.ErrorEncountered, nil, dcp, nil, otherInfo)
}

// start, restart or shutdown streams
func (dcp *DcpNozzle) startUprStream(settings map[string]interface{}) error {

	// fetch restart-timestamp from settings
	ts := settings[DCP_SETTINGS_KEY].(map[uint16]*base.VBTimestamp)

	opaque := newOpaque()
	flags := uint32(0)
	seqEnd := uint64(0xFFFFFFFFFFFFFFFF)
	for _, vbts := range ts {
		dcp.Logger().Infof("%v starting vb stream for vb=%v\n", dcp.Id(), vbts.Vbno)
		err := dcp.uprFeed.UprRequestStream(vbts.Vbno, opaque, flags, vbts.Vbuuid, vbts.Seqno, seqEnd, vbts.SnapshotStart, vbts.SnapshotEnd)
		if err != nil {
			return err
		}
	}

	return nil
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

// generate a new 16 bit opaque value set as MSB.
func newOpaque() uint16 {
	// bit 26 ... 42 from UnixNano().
	return uint16((uint64(time.Now().UnixNano()) >> 26) & 0xFFFF)
}
