package parts

import (
	"errors"
	"fmt"
	base "github.com/couchbase/goxdcr/base"
	common "github.com/couchbase/goxdcr/common"
	"github.com/couchbase/goxdcr/metadata"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

/**
 * DCP Rollback Handshake Helper. See MB-25647 for handshake sequence design
 */
type dcpStreamReqHelper struct {
	// Internal copy of vbno
	vbno uint16
	// Internal interface of dcp nozzle to access outer wrapper methods
	dcp DcpNozzleIface

	// Locks all internals except for currentVersionWell
	lock sync.RWMutex
	// Keeps track of messages sent. Key is version of number, Value is the seqno for the vbucket
	sentMsgs map[uint16]uint64
	// Keeps track of the seqno that has been ACK'ed. Key is the seqno, and value is whether or not it's been ack'ed
	ackedMsgs map[uint64]bool
	// Whether or not this helper is no longer usable
	isDisabled bool

	// Current version counter well - monotonously increasing - use atomics and not lock
	currentVersionWell uint64
}

func (reqHelper *dcpStreamReqHelper) initialize() {
	reqHelper.sentMsgs = make(map[uint16]uint64)
	reqHelper.ackedMsgs = make(map[uint64]bool)
	atomic.StoreUint64(&reqHelper.currentVersionWell, 0)
}

func (reqHelper *dcpStreamReqHelper) isStreamActiveNoLock() bool {
	if reqHelper.isDisabled {
		return false
	}

	status, err := reqHelper.dcp.GetStreamState(reqHelper.vbno)
	if err != nil {
		reqHelper.dcp.Logger().Errorf("Invalid Stream state for vbno: %v even though helper for vb exists.", reqHelper.vbno)
		return false
	}
	return (status == Dcp_Stream_Active)
}

func (reqHelper *dcpStreamReqHelper) getNewVersion() uint16 {
	var newVersion uint64

	newVersion = atomic.AddUint64(&reqHelper.currentVersionWell, 1)

	if newVersion > math.MaxUint16 {
		errStr := fmt.Sprintf("Error: dcpStreamHelper for vbno: %v internal version overflow", reqHelper.vbno)
		reqHelper.lock.RLock()
		defer reqHelper.lock.RUnlock()
		if !reqHelper.isDisabled {
			reqHelper.dcp.Logger().Errorf(errStr)
			reqHelper.dcp.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, nil, nil, errStr))
		}
		atomic.StoreUint64(&reqHelper.currentVersionWell, 0)
		newVersion = 0
	}

	return uint16(newVersion)
}

// Gets the number of versions that has not been ack'ed
func (reqHelper *dcpStreamReqHelper) getNumberOfOutstandingReqs() int {
	reqHelper.lock.RLock()
	defer reqHelper.lock.RUnlock()
	// Find number of un-ack'ed msgs
	var count int
	for _, seqno := range reqHelper.sentMsgs {
		if !reqHelper.ackedMsgs[seqno] {
			count++
		}
	}
	return count
}

// Write lock must be held
func (reqHelper *dcpStreamReqHelper) deregisterRequestNoLock(version uint16) {
	seqno, ok := reqHelper.sentMsgs[version]
	if ok {
		// Mark that this seqno has been ack'ed
		reqHelper.ackedMsgs[seqno] = true
	}
}

/**
 * Register a sent request into the map for book-keeping
 */
func (reqHelper *dcpStreamReqHelper) registerRequest(version uint16, seqno uint64) (alreadyAcked bool, helperErr error) {
	reqHelper.lock.Lock()
	defer reqHelper.lock.Unlock()
	if reqHelper.isDisabled {
		helperErr = reqHelper.getDisabledError()
		return
	}
	if reqHelper.isStreamActiveNoLock() {
		alreadyAcked = true
	} else {
		alreadyAcked = reqHelper.ackedMsgs[seqno]
	}

	if !alreadyAcked {
		reqHelper.sentMsgs[version] = seqno
	}
	return
}

/**
 * Processes a rollback response.
 * Returns error if there's any issues with look-ups
 * Returns a bool to represent whether or not to ignore this response
 */
func (reqHelper *dcpStreamReqHelper) processRollbackResponse(version uint16) (ignoreResponse bool, helperErr error) {
	reqHelper.lock.Lock()
	defer reqHelper.lock.Unlock()

	if reqHelper.isDisabled {
		helperErr = reqHelper.getDisabledError()
		return
	}
	// default to not ignore response
	ignoreResponse = false
	var acked bool

	if reqHelper.isStreamActiveNoLock() {
		// If the vb stream is active already, ignore all rollback requests
		ignoreResponse = true
	} else {
		// Check to see if this seqno has been rejected "sent back a ROLLBACK" before. If so, then this is already handled.
		seqno, seqok := reqHelper.sentMsgs[version]
		if !seqok {
			helperErr = errors.New(fmt.Sprintf("Received a ROLLBACK message for vbno=%v with version=%v, but was never sent.",
				reqHelper.vbno, version))
			reqHelper.dcp.Logger().Warnf(helperErr.Error())
			ignoreResponse = true
			return
		}
		acked = reqHelper.ackedMsgs[seqno]

		if acked {
			ignoreResponse = true
		}

		if reqHelper.sentMsgs[version] == 0 {
			// It is weird that we sent out a rollbackseqno of 0 to DCP and it tells us to rollback again.
			// This should not happen. Restart pipeline. It has the same effect of panic where XDCR DCP nozzles restart.
			helperErr = errors.New(fmt.Sprintf("Received a ROLLBACK message for vbno=%v with seqno=%v, last sent was 0",
				reqHelper.vbno, reqHelper.sentMsgs[version]))
		}
	}

	if !acked {
		reqHelper.deregisterRequestNoLock(version)
	}
	return
}

/**
 * When a success is returned, this helper will remove the recorded response and make future
 * rollback operations no-op.
 * All history is reset until a registerRequest is called again
 */
func (reqHelper *dcpStreamReqHelper) processSuccessResponse(version uint16) {
	reqHelper.lock.Lock()
	defer reqHelper.lock.Unlock()

	if reqHelper.isDisabled {
		return
	}

	// reset stats
	reqHelper.initialize()
}

func (reqHelper *dcpStreamReqHelper) disable() {
	reqHelper.lock.Lock()
	defer reqHelper.lock.Unlock()
	reqHelper.isDisabled = true
	reqHelper.dcp = nil
}

func (reqHelper *dcpStreamReqHelper) getDisabledError() error {
	return fmt.Errorf("vbReqHelper %v is disabled", reqHelper.vbno)
}

//////////////////////// VBTimestampHelper /////////////////////////////////
type vbtsNegotiatorState uint32

const (
	vbtsInit        vbtsNegotiatorState = iota
	vbtsWaiting     vbtsNegotiatorState = iota
	vbtsNegotiating vbtsNegotiatorState = iota
	vbtsDone        vbtsNegotiatorState = iota
	vbtsStopping    vbtsNegotiatorState = iota
	vbtsExited      vbtsNegotiatorState = iota
)

func (s *vbtsNegotiatorState) get() vbtsNegotiatorState {
	return (vbtsNegotiatorState)(atomic.LoadUint32((*uint32)(s)))
}

func (s *vbtsNegotiatorState) set(oldState, newState vbtsNegotiatorState) bool {
	return atomic.CompareAndSwapUint32((*uint32)(s), (uint32)(oldState), (uint32)(newState))
}

func (s *vbtsNegotiatorState) setForce(newState vbtsNegotiatorState) {
	atomic.StoreUint32((*uint32)(s), (uint32)(newState))
}

type callbackMap map[uint16]common.TimestampCb

type vbtsMapWithLock struct {
	mutex sync.RWMutex
	vbMap map[uint16]*base.VBTimestamp
}

func NewVbtsMapWithLock() *vbtsMapWithLock {
	return &vbtsMapWithLock{vbMap: make(map[uint16]*base.VBTimestamp)}
}

type vbtsNegotiator struct {
	dcp   *DcpNozzle
	vbnos []uint16

	// Warm up period is considered when it's collecting as many pipelines starting as possible
	// before deciding the point to start streams
	waitingPeriod        time.Duration
	waitingWGroup        sync.WaitGroup
	waitingFinCh         chan bool
	waitingCloseOnceFunc sync.Once

	// Always first level locking
	state vbtsNegotiatorState

	internalLock    sync.RWMutex
	numPipelines    int
	topicToCallback map[string]callbackMap

	topicToVbTsMap map[string]*vbtsMapWithLock
}

func NewVbtsNegotiator(dcp *DcpNozzle, vbnos []uint16, waitingPeriod time.Duration) *vbtsNegotiator {
	helper := &vbtsNegotiator{
		dcp:             dcp,
		vbnos:           vbnos,
		topicToCallback: make(map[string]callbackMap),
		waitingPeriod:   waitingPeriod,
		waitingFinCh:    make(chan bool),
		topicToVbTsMap:  make(map[string]*vbtsMapWithLock),
	}

	return helper
}

func (v *vbtsNegotiator) Cleanup() {
	v.state.setForce(vbtsStopping)
	v.waitingCloseOnceFunc.Do(func() { close(v.waitingFinCh) })
	v.waitingWGroup.Wait()

	// TODO - mem leak?
	//	v.dcp = nil
	v.state.setForce(vbtsExited)
}

func (v *vbtsNegotiator) Start(settings metadata.ReplicationSettingsMap) error {
	topic, ok := settings[PipelineTopic].(string)
	if !ok {
		panic("vbtshelper cannot find pipelinetopic")
	}

	// If pipeline has exited, is safe to restart
	v.state.set(vbtsExited, vbtsInit)

	switch v.state.get() {
	case vbtsInit:
		v.waitingWGroup.Add(1)
		go v.startWaitingPeriod()
		v.state.set(vbtsInit, vbtsWaiting)
		fallthrough
	case vbtsWaiting:
		v.registerPipeline(topic)
	case vbtsStopping:
		return fmt.Errorf("%v is stopping, cannot restart yet", v.dcp.Id())
	default:
		v.dcp.Logger().Infof("Pipeline %v started too late", topic)
	}

	return nil
}

func (v *vbtsNegotiator) Stop(settings metadata.ReplicationSettingsMap) {
	topicName, ok := settings[PipelineTopic].(string)
	if !ok {
		panic("Unable to find topic")
	} else {
		v.dcp.Logger().Infof("Dcp nozzle %v received Stop call from pipeline %v", v.dcp.Id(), topicName)
	}

	switch v.state.get() {
	case vbtsInit:
		fallthrough
	case vbtsWaiting:
		v.unregisterPipeline(topicName)
		v.dcp.Logger().Infof("Successfully unregistered pipeline %v from vbtsnegotiator", topicName)
	}
}

func (v *vbtsNegotiator) startWaitingPeriod() {
	defer v.waitingWGroup.Done()
	waitingTimer := time.NewTimer(v.waitingPeriod)
	defer waitingTimer.Stop()
	v.dcp.Logger().Infof("%v is starting a waiting period for %v to allow other pipelines to join before starting a universal stream",
		v.dcp.Id(), v.waitingPeriod)

	for {
		select {
		case <-v.waitingFinCh:
			v.dcp.Logger().Infof("%v vbtimestamp negotiator forced to exit", v.dcp.Id())
			return
		case <-waitingTimer.C:
			err := v.negotiate()
			if err != nil {
				v.dcp.Logger().Errorf("%v error when negotiating DCP stream: %v", v.dcp.Id(), err)
				v.dcp.handleGeneralError(err)
			}
		}
	}
}

func (v *vbtsNegotiator) negotiate() error {
	success := v.state.set(vbtsWaiting, vbtsNegotiating)
	if !success {
		err := fmt.Errorf("Unable to move to negotiate phase. Current phase: %v", v.state.get())
		v.dcp.handleGeneralError(err)
		return err
	}
	v.dcp.Logger().Infof("%v waiting period has expired. Starting negotiation between %v pipelines", v.dcp.Id(), v.getNumPipelines())

	finalTimestamp := make(map[uint16]*base.VBTimestamp)
	invalidVbs := make(map[uint16]bool)
	var validVbs []uint16

	v.internalLock.RLock()
	defer v.internalLock.RUnlock()
	for _, vbtsWLock := range v.topicToVbTsMap {
		vbtsWLock.mutex.RLock()
		for vbno, ts := range vbtsWLock.vbMap {
			if finalTimestamp[vbno] == nil {
				finalTimestamp[vbno] = &base.VBTimestamp{Vbno: vbno}
			} else if _, isInvalid := invalidVbs[vbno]; isInvalid {
				continue
			} else if finalTimestamp[vbno].Vbuuid != ts.Vbuuid {
				invalidVbs[vbno] = true
				continue
			}

			if finalTimestamp[vbno].Seqno == 0 || ts.Seqno < finalTimestamp[vbno].Seqno {
				finalTimestamp[vbno].Seqno = ts.Seqno
			}
			if finalTimestamp[vbno].SnapshotStart == 0 || ts.SnapshotStart < finalTimestamp[vbno].SnapshotStart {
				finalTimestamp[vbno].SnapshotStart = ts.SnapshotStart
			}
			if finalTimestamp[vbno].SnapshotEnd == 0 || ts.SnapshotEnd < finalTimestamp[vbno].SnapshotEnd {
				finalTimestamp[vbno].SnapshotEnd = ts.SnapshotEnd
			}
			if finalTimestamp[vbno].ManifestIDs.SourceManifestId == 0 || ts.ManifestIDs.SourceManifestId < finalTimestamp[vbno].ManifestIDs.SourceManifestId {
				finalTimestamp[vbno].ManifestIDs.SourceManifestId = ts.ManifestIDs.SourceManifestId
			}
			if finalTimestamp[vbno].ManifestIDs.TargetManifestId == 0 || ts.ManifestIDs.TargetManifestId < finalTimestamp[vbno].ManifestIDs.TargetManifestId {
				finalTimestamp[vbno].ManifestIDs.TargetManifestId = ts.ManifestIDs.TargetManifestId
			}
		}
		vbtsWLock.mutex.RUnlock()
	}
	if len(invalidVbs) > 0 {
		v.dcp.Logger().Warnf("InvalidVBs: %v", invalidVbs)
	} else {
		for vbno, _ := range finalTimestamp {
			validVbs = append(validVbs, vbno)
		}
	}

	//	UprGetFailoverLog(vb []uint16) (map[uint16]*FailoverLog, error)
	failoverLogs, err := v.dcp.client.UprGetFailoverLog(validVbs)
	v.dcp.Logger().Infof("NEIL DEBUG failoverlogs err: %v logs: %v", err, failoverLogs)

	err = v.dcp.onUpdateStartingSeqno(finalTimestamp)
	v.state.set(vbtsNegotiating, vbtsDone)
	return err
}

func (v *vbtsNegotiator) getNumPipelines() int {
	v.internalLock.RLock()
	defer v.internalLock.RUnlock()
	return v.numPipelines
}

func (v *vbtsNegotiator) registerPipeline(topic string) {
	v.internalLock.Lock()
	defer v.internalLock.Unlock()

	if _, exists := v.topicToCallback[topic]; exists {
		panic(fmt.Sprintf("Topic: %v should not have existed", topic))
	}

	v.numPipelines++

	newMap := make(callbackMap)
	for _, vbno := range v.vbnos {
		newMap[vbno] = nil
	}
	v.topicToCallback[topic] = newMap

	v.topicToVbTsMap[topic] = NewVbtsMapWithLock()

	v.dcp.Logger().Infof("DCP %v received pipeline %v registration. %v total registered.", v.dcp.Id(), topic, v.numPipelines)
}

func (v *vbtsNegotiator) unregisterPipeline(topic string) {
	v.internalLock.Lock()
	defer v.internalLock.Unlock()

	delete(v.topicToCallback, topic)
	v.numPipelines++
	v.dcp.Logger().Infof("DCP %v received pipeline %v de-registration. %v total left", v.dcp.Id(), topic, v.numPipelines)
}

func (v *vbtsNegotiator) HandleVbtsFromCheckpointMgr(pipelineTopic string, vbTimestamps map[uint16]*base.VBTimestamp) {
	v.internalLock.RLock()
	defer v.internalLock.RUnlock()

	storedMap, ok := v.topicToVbTsMap[pipelineTopic]
	if !ok {
		panic("Not found")
	}

	storedMap.mutex.Lock()
	defer storedMap.mutex.Unlock()

	for vb, ts := range vbTimestamps {
		if existingTs, exists := storedMap.vbMap[vb]; exists {
			panic(fmt.Sprintf("Checkpoint manager resent vb % with %v, currently: %v", vb, ts, existingTs))
		} else {
			storedMap.vbMap[vb] = ts
		}
	}
}
