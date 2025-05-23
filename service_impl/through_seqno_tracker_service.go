// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_impl

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/common"
	component "github.com/couchbase/goxdcr/v8/component"
	"github.com/couchbase/goxdcr/v8/conflictlog"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/parts"
	pipeline_pkg "github.com/couchbase/goxdcr/v8/pipeline"
	"github.com/couchbase/goxdcr/v8/pipeline_svc"
	"github.com/couchbase/goxdcr/v8/pipeline_utils"
	"github.com/couchbase/goxdcr/v8/service_def"
)

type ThroughSeqnoTrackerSvc struct {
	*component.AbstractComponent

	// Attached pipeline
	attachedPipeline common.Pipeline

	// map of vbs that the tracker tracks.
	vb_map           map[uint16]bool
	variableVbTgtMap map[uint16]bool

	// through_seqno seen by outnozzles based on the docs that are actually sent to target
	// This is a map that is considered the "baseline" of sequence numbers. Whenever a batch operation
	// that figures out timestamps, it updates this map. And whenever a caller asks this service for
	// the "highest" seqno that is considered synchronized between source and target, the tracker service
	// starts with the number listed in this map and builds on top. (See GetThroughSeqnos)
	through_seqno_map map[uint16]*base.SeqnoWithLock

	// stores for each vb a sorted list of the seqnos that have been sent to and confirmed by target
	vb_sent_seqno_list_map map[uint16]*base.SortedSeqnoListWithLock

	// Note: lists in the following two maps are treated in the same way in through_seqno computation
	// they are maintained as two seperate lists because insertions into the lists are simpler
	// and quicker this way - each insertion is simply an append to the end of the list

	// stores for each vb a sorted list of seqnos that have been filtered out
	vb_filtered_seqno_list_map map[uint16]*base.SortedSeqnoListWithLock
	// stores for each vb a sorted list of seqnos that have failed conflict resolution on source
	vb_failed_cr_seqno_list_map map[uint16]*base.SortedSeqnoListWithLock

	// gap_seqno_list_1[i] stores the start seqno of the ith gap range
	// gap_seqno_list_2[i] stores the end seqno of  the ith gap range
	// for example, if we receive seqno 5 and then 10 from dcp, the gap range is [6,9]
	// 6 will be appended to the end of gap_seqno_list_1, and 10 appended to gap_seqno_list_2
	// for a given vb, gap_seqno_list_1 and gap_seqno_list_2 always have the same length
	vb_gap_seqno_list_map map[uint16]*DualSortedSeqnoListWithLock

	// tracks the last seen seqno streamed out by dcp, so that we can tell the gap between the last seen seqno
	// and the current seen seqno
	vb_last_seen_seqno_map map[uint16]*base.SeqnoWithLock

	// System events sent down are recorded in this list
	vbSystemEventsSeqnoListMap SeqnoManifestsMapType

	// stores sequence numbers that are ignored and not sent out because backfills will cover
	vbIgnoredSeqnoListMap map[uint16]*base.SortedSeqnoListWithLock

	// Keeps track of the manifestIDs associated with a range of sequence numbers
	// Given multiple ranges of sequence numbers, such as {[a, b), [b, c)}
	// elements of list 1 are the top of each range, in this case: b and c.
	// When the high sequence number is in flux, maxuint64 is used and can be changed.
	//
	// List2 represent the manifestID associated with the range.
	//
	// For example, seqno of [0, 100) is manifestID 1, and [100, 200) is manifest ID of 2
	// [200, inf) is manifestID of 3
	//
	// n:        0   1         2
	// list1:  100 200 highSeqno (seqno)
	// list2:    1   2         3 (manifestId)
	vbTgtSeqnoManifestMap SeqnoManifestsMapType

	id     string
	rep_id string

	logger      *log.CommonLogger
	unitTesting bool

	// For backfill pipeline, it'll require coordination effort
	// 1. DCP will first send streamEnd
	// 2. This service will need to find the last DCP seen seqno and watch for it to be processed
	vbBackfillHelperActive    uint32
	vbBackfillLastDCPSeqnoMap map[uint16]*base.SeqnoWithLock

	// The done map is used to make sure only the VB's that received streamStart should wait for streamEnd
	// And then only those received streamEnd should wait for ThroughSeqno to catch up to the lastSeenDCPSeqno
	// Uses SeqnoWithLock but ultimately only need 3 values
	//
	// 0: This VB should receive streamEnd
	// 1: StreamEnd received
	// 2: ThroughSeqno Reached
	vbBackfillHelperDoneMap map[uint16]*base.SeqnoWithLock

	// If a source mutation is cloned multiple times to be sent to multiple different target namespaces
	// this tracker will keep track of all the cloned instances. Once all the cloned instances are processed
	// then the seqno is removed
	// The first list stores the seqno
	// Second list's corresponding element stores the number of outstanding cloned requests that have not been processed
	vbClonedTracker map[uint16]*DualSortedSeqnoListWithLock

	// When OSO mode is on, everything else is on pause, except internally we need to keep track of the MAX seqno
	// that has been received while OSO is in progress. As soon as OSO ends, the max that has been received AND processed
	// will now be used for both highSeqno, snapshotStart, and snapshotEnd for resume purposes
	// Since XDCR data processing always lags behind DCP, it is possible for concurrent OSO sessions ongoing
	// The tracker map will provide ability to track sessions
	// Once a session is over, the tracker will synchronize (essentially, introduce a gap) of seqnos into the
	// throughseqno tracker service
	osoModeActive              uint32
	vbOsoModeSessionDCPTracker OSOSeqnoTrackerMapType
	osoSnapshotRaiser          func(vbno uint16, seqno uint64)
	osoCntReceivedFromDCP      uint64

	variableVBMode bool
	// If variable VB mode is on, this will be the list of all target VBs
	variableTgtVbs []uint16

	// seqno_list_1 tracks all the conflict logging requests.
	// seqno_list_2 tracks all the conflict logging responses (conflict logging requests that are done processing).
	// It is assumed that seqno_list_2 is a subset of seqno_list_1.
	// i.e. len(seqno_list_2) <= len(seqno_list_1).
	// It's the pipeline's responsibility that this assumption are upheld.
	cLogTrackerVbMap            map[uint16]*DualSortedSeqnoListWithLock
	nonMonotonicThroughSeqnoCnt atomic.Uint64
	faultyCLogTrackerCnt        atomic.Uint64

	cLogExists bool

	// keeps track of slow GetThroughSeqno calls
	slowThroughSeqnoCalls throughSeqnoCallStats
	// keeps track of count of instances therein we
	// saw a session end when we shouldn't have.
	unusualOsoSessionEnd atomic.Uint64
	// keeps a track of the number of vbs that received bypass stream
	backfillStreamBypassCnt atomic.Uint32
}

// stats will be in milliseconds.
type throughSeqnoCallStats struct {
	min   atomic.Uint64
	max   atomic.Uint64
	count atomic.Uint64
	sum   atomic.Uint64
}

// struct containing two seqno lists that need to be accessed and locked together
type DualSortedSeqnoListWithLock struct {
	seqno_list_1 []uint64
	seqno_list_2 []uint64
	lock         *sync.RWMutex

	// By default, the two lists are always assumed to be of equal length.
	// Use setNoLenRestriction to set it to true, which means there is no such
	// equality restriction.
	overrideLenRestriction bool
}

const (
	VBSeqnoNotDone           uint64 = iota
	VBSeqnoStreamEndReceived uint64 = iota
	VBSeqnoBypassed          uint64 = iota
	VBSeqnoAllProcessed      uint64 = iota
)

func newDualSortedSeqnoListWithLock() *DualSortedSeqnoListWithLock {
	return &DualSortedSeqnoListWithLock{
		seqno_list_1: make([]uint64, 0),
		seqno_list_2: make([]uint64, 0),
		lock:         &sync.RWMutex{},
	}
}

func (list_obj *DualSortedSeqnoListWithLock) setNoLenRestriction() *DualSortedSeqnoListWithLock {
	list_obj.lock.Lock()
	defer list_obj.lock.Unlock()

	list_obj.overrideLenRestriction = true
	return list_obj
}

// Note that the function doesn't sort the lists explicitly before returning.
// Use sortAndGetSeqnoLists function to explicitly sort the lists.
func (list_obj *DualSortedSeqnoListWithLock) getSortedSeqnoLists(vbno uint16, identifier string) ([]uint64, []uint64) {
	list_obj.lock.RLock()
	defer list_obj.lock.RUnlock()

	if !list_obj.overrideLenRestriction && len(list_obj.seqno_list_1) != len(list_obj.seqno_list_2) {
		panic(fmt.Sprintf("type %v vbno %v lengths of equal dual lists do not match. list_1=%v, list_2=%v", identifier, vbno, list_obj.seqno_list_1, list_obj.seqno_list_2))
	}

	return base.DeepCopyUint64Array(list_obj.seqno_list_1), base.DeepCopyUint64Array(list_obj.seqno_list_2)
}

// same as getSortedSeqnoLists, but explicitly sorts the lists before returning.
func (list_obj *DualSortedSeqnoListWithLock) sortAndGetSeqnoLists() ([]uint64, []uint64) {
	list_obj.lock.Lock()
	defer list_obj.lock.Unlock()

	base.SortUint64List(list_obj.seqno_list_1)
	base.SortUint64List(list_obj.seqno_list_2)
	return base.DeepCopyUint64Array(list_obj.seqno_list_1), base.DeepCopyUint64Array(list_obj.seqno_list_2)
}

func (list_obj *DualSortedSeqnoListWithLock) getLengthOfSeqnoLists() int {
	list_obj.lock.RLock()
	defer list_obj.lock.RUnlock()

	return len(list_obj.seqno_list_1)
}

func (list_obj *DualSortedSeqnoListWithLock) getLengthOfSeparateLists() (int, int) {
	list_obj.lock.RLock()
	defer list_obj.lock.RUnlock()

	return len(list_obj.seqno_list_1), len(list_obj.seqno_list_2)
}

// append seqnos to the end of seqno_lists
func (list_obj *DualSortedSeqnoListWithLock) appendSeqnos(seqno_1 uint64, seqno_2 uint64) {
	list_obj.lock.Lock()
	defer list_obj.lock.Unlock()
	list_obj.seqno_list_1 = append(list_obj.seqno_list_1, seqno_1)
	list_obj.seqno_list_2 = append(list_obj.seqno_list_2, seqno_2)
}

// append seqno to the end of seqno_list_1 only
func (list_obj *DualSortedSeqnoListWithLock) appendSeqnoToList1(seqno uint64) {
	list_obj.lock.Lock()
	defer list_obj.lock.Unlock()
	list_obj.seqno_list_1 = append(list_obj.seqno_list_1, seqno)
}

// append seqno to the end of seqno_list_2 only
func (list_obj *DualSortedSeqnoListWithLock) appendSeqnoToList2(seqno2 uint64) {
	list_obj.lock.Lock()
	defer list_obj.lock.Unlock()
	list_obj.seqno_list_2 = append(list_obj.seqno_list_2, seqno2)
}

// truncate all seqnos that are no larger than passed in through_seqno
// through_seqno is assumed to be not present in the lists, since we are dealing with
// gap seqnos.
func (list_obj *DualSortedSeqnoListWithLock) truncateSeqnos(through_seqno uint64) {
	list_obj.lock.Lock()
	defer list_obj.lock.Unlock()

	if list_obj.overrideLenRestriction {
		list_obj.seqno_list_1 = truncateOneSeqnoList(through_seqno, list_obj.seqno_list_1)
		list_obj.seqno_list_2 = truncateOneSeqnoList(through_seqno, list_obj.seqno_list_2)
	} else {
		list_obj.seqno_list_1 = truncateGapSeqnoList(through_seqno, list_obj.seqno_list_1)
		list_obj.seqno_list_2 = truncateGapSeqnoList(through_seqno, list_obj.seqno_list_2)
	}
}

// When we truncate based on a through_seqno, this is the "reverse" truncate of the Seqnos
// In other words, the lists (like systemEvents) may not contain the actual throughSeqno
// When we truncate, will truncate regularly if the seqno is in the list.
// If it is not in the list, we will round down and then truncate.
// i.e.
// list1:
// 0 2 4 5 6...
// Truncate at 4:
// 4 5 6 ...
// Truncate at 3:
// 2 4 5 6 ...
func (list_obj *DualSortedSeqnoListWithLock) truncateSeqno1Floor(throughSeqno uint64) {
	list_obj.lock.Lock()
	defer list_obj.lock.Unlock()

	index, found := base.SearchUint64List(list_obj.seqno_list_1, throughSeqno)
	if found {
		list_obj.seqno_list_1 = list_obj.seqno_list_1[index:]
		list_obj.seqno_list_2 = list_obj.seqno_list_2[index:]
	} else if index > 0 {
		list_obj.seqno_list_1 = list_obj.seqno_list_1[index-1:]
		list_obj.seqno_list_2 = list_obj.seqno_list_2[index-1:]
	}
}

// Given a through seqno, find it in the list1. If it is not in List1, then use the next lower seqno
// in list1 as the found "pair"
// Then, return the list2 pair value
func (list_obj *DualSortedSeqnoListWithLock) getList2BasedonList1Floor(throughSeqno uint64) (uint64, error) {
	list_obj.lock.RLock()
	defer list_obj.lock.RUnlock()

	index, found := base.SearchUint64List(list_obj.seqno_list_1, throughSeqno)
	if found {
		list2Element := list_obj.seqno_list_2[index]
		return list2Element, nil
	} else if index > 0 {
		list2Element := list_obj.seqno_list_2[index-1]
		return list2Element, nil
	} else {
		return 0, base.ErrorNotFound
	}
}

type OSOSeqnoTrackerMapType map[uint16]*OsoSessionsTracker

func (t *OSOSeqnoTrackerMapType) Debug(vbno uint16) string {
	sessions := (*t)[vbno]
	if sessions == nil {
		return "No sessions"
	}
	sessions.lock.RLock()
	defer sessions.lock.RUnlock()
	var outputStrings []string
	for i, session := range sessions.sessions {
		session.lock.RLock()
		outputStrings = append(outputStrings, fmt.Sprintf("session %v: %+v : done? %v", i, session, session.isDoneNoLock()))
		session.lock.RUnlock()
	}

	return strings.Join(outputStrings, "\n")
}

func (t *OSOSeqnoTrackerMapType) StartNewOSOSession(tsTracker *ThroughSeqnoTrackerSvc, vbno uint16) {
	lastSeenSeqno := tsTracker.vb_last_seen_seqno_map[vbno].GetSeqno()
	lastSeenManifestId, manifestErr := tsTracker.vbSystemEventsSeqnoListMap.GetManifestId(vbno, lastSeenSeqno)
	if manifestErr != nil {
		lastSeenManifestId = 0
	}

	sessionTracker := (*t)[vbno]
	sessionTracker.RegisterNewSession(lastSeenSeqno, lastSeenManifestId, vbno).SetVariableVBMode(tsTracker.variableVBMode, tsTracker.variableTgtVbs)
}

func (t *OSOSeqnoTrackerMapType) GetStartingSeqno(vbno uint16) (seqno uint64, osoAcive bool) {
	sessionTracker := (*t)[vbno]
	return sessionTracker.GetStartingSeqno()
}

func (t *OSOSeqnoTrackerMapType) EndOSOSessionReceived(tsTracker *ThroughSeqnoTrackerSvc, vbno uint16, minSeqnoFromDCP, maxSeqnoFromDCP, numSeqnoFromDCP uint64) (bool, *OsoSession) {
	sessionTracker := (*t)[vbno]
	return sessionTracker.RegisterSessionEnd(vbno, tsTracker, minSeqnoFromDCP, maxSeqnoFromDCP, numSeqnoFromDCP)
}

func (t *OSOSeqnoTrackerMapType) ShouldProcessAsOSO(vbno uint16, seqno uint64) (bool, *OsoSession) {
	sessionTracker := (*t)[vbno]
	return sessionTracker.ShouldProcessAsOSO(seqno)
}

func (t *OSOSeqnoTrackerMapType) ShouldStreamEnd(vbno uint16) bool {
	sessionTracker := (*t)[vbno]
	return sessionTracker.ShouldStreamEnd()
}

func (t *OSOSeqnoTrackerMapType) TruncateSession(vbno uint16, session *OsoSession) {
	sessionTracker := (*t)[vbno]
	sessionTracker.TruncateSessionViaPtr(session)
}

type OsoSessionsTracker struct {
	lock           sync.RWMutex
	sessions       []*OsoSession
	variableVBMode bool
	tsTracker      *ThroughSeqnoTrackerSvc
}

func (o *OsoSessionsTracker) RegisterNewSession(lastSeenSeqno uint64, lastSeenManifestId uint64, vbno uint16) *OsoSession {
	o.lock.Lock()
	defer o.lock.Unlock()

	newSession := &OsoSession{
		vbno:                           vbno,
		lock:                           sync.RWMutex{},
		dcpSeqnoWhenSessionStarts:      lastSeenSeqno,
		dcpManifestIdWhenSessionStarts: lastSeenManifestId,
		dcpSentOsoEnd:                  false,
		maxSeqnoFromDCP:                0,
		minSeqnoFromDCP:                math.MaxUint64,
		unsureBufferedProcessedSeqnos:  newDualSortedSeqnoListWithLock(),
		unsureBufferedCLogSeqnos:       newDualSortedSeqnoListWithLock().setNoLenRestriction(),
		tsTracker:                      o.tsTracker,
	}

	if o.variableVBMode {
		newSession.variableMaxManifestIdProcessed = make(map[uint16]uint64)
		newSession.variableUnsureBufferedProcessSeqnos = make(map[uint16]*DualSortedSeqnoListWithLock)
	}

	o.sessions = append(o.sessions, newSession)
	return newSession
}

// If a latest session is in progress and DCP hasn't sent an end to this session
// then the throughSeqno should not be higher than the DCP's last seen seqno
// This is because the gap that's supposed to be created by this oso session hasn't been injected into tsTracker yet
// If there's no open session, return false for osoActive so the caller can know to ignore OSO concepts
func (o *OsoSessionsTracker) GetStartingSeqno() (seqno uint64, osoActive bool) {
	o.lock.RLock()
	defer o.lock.RUnlock()

	// Find a session that is active
	for idx := 0; idx < len(o.sessions); idx++ {
		session := o.sessions[idx]
		session.lock.RLock()
		if session.isDoneNoLock() {
			session.lock.RUnlock()
			continue
		}

		// Found a session that is active
		osoActive = true
		seqno = session.GetStartingSeqnoNoLock()
		session.lock.RUnlock()
		return
	}

	// Did not find any session
	return
}

// Used when DCP sends a OSO snapshot end marker
// Only the last session will be "open"
// When a session is ended, the session is now considered "closed" but potentially not yet done
func (o *OsoSessionsTracker) RegisterSessionEnd(vbno uint16, tsTracker *ThroughSeqnoTrackerSvc, minSeqnoFromDCP, maxSeqnoFromDCP, numSeqnoFromDCP uint64) (bool, *OsoSession) {
	o.lock.RLock()
	defer o.lock.RUnlock()
	// Only the last session should be active
	idx := len(o.sessions) - 1
	if idx < 0 {
		panic("idxErr")
	}
	latestSession := o.sessions[idx]
	isDone := latestSession.RegisterSessionEnd(vbno, tsTracker, minSeqnoFromDCP, maxSeqnoFromDCP, numSeqnoFromDCP)
	return isDone, latestSession
}

func (o *OsoSessionsTracker) TruncateSessionsNoLock(lastIdx int) {
	if len(o.sessions)-1 < lastIdx {
		panic(fmt.Sprintf("truncating idx %v given length %v", lastIdx, len(o.sessions)))
	}

	o.sessions = append(o.sessions[0:lastIdx], o.sessions[lastIdx+1:]...)
}

func (o *OsoSessionsTracker) ShouldProcessAsOSO(seqno uint64) (shouldProcessAsOSO bool, sessionPtr *OsoSession) {
	o.lock.RLock()
	defer o.lock.RUnlock()

	// Go from the beginning to the end
	// If the end session is not closed yet, then process it as an "unsure" seqno
	for idx := 0; idx < len(o.sessions); idx++ {
		sessionPtr = o.sessions[idx]
		shouldProcessAsOSO = sessionPtr.ShouldProcessAsOSO(seqno)
		if shouldProcessAsOSO {
			return
		}
	}
	return
}

// Stream can end given that all the sessions have received OSO snapshot
// end marker. This can just be done by checking if the last open session
// has received OSO snapshot end marker because OSO snapshot marker events
// are synchronous.
func (o *OsoSessionsTracker) ShouldStreamEnd() (shouldStreamEnd bool) {
	o.lock.RLock()
	defer o.lock.RUnlock()

	lastSessionIdx := len(o.sessions) - 1
	if lastSessionIdx < 0 {
		// no active sessions, the stream should end.
		shouldStreamEnd = true
		return
	}

	sessionPtr := o.sessions[lastSessionIdx]
	shouldStreamEnd = sessionPtr.ReceivedOSOEnd()
	return
}

func (o *OsoSessionsTracker) MarkManifestReceived(manifestId uint64, sessionIdx int) {
	o.lock.RLock()
	defer o.lock.RUnlock()
	session := o.sessions[sessionIdx]

	if session.IsDone() {
		panic("latestSession is done but we are calling MarkManifestReceived")
	}

	session.MarkManifestReceived(manifestId)
}

func (o *OsoSessionsTracker) TruncateSessionViaPtr(session *OsoSession) {
	o.lock.Lock()
	defer o.lock.Unlock()

	for i := 0; i < len(o.sessions); i++ {
		if o.sessions[i] == session {
			o.sessions = append(o.sessions[:i], o.sessions[i+1:]...)
			return
		}
	}
}

func (o *OsoSessionsTracker) numSessions() int {
	o.lock.RLock()
	defer o.lock.RUnlock()

	return len(o.sessions)
}

func newOsoSessionTracker(variableVBMode bool, tsTracker *ThroughSeqnoTrackerSvc) *OsoSessionsTracker {
	return &OsoSessionsTracker{
		lock:           sync.RWMutex{},
		sessions:       make([]*OsoSession, 0),
		variableVBMode: variableVBMode,
		tsTracker:      tsTracker,
	}
}

type OsoSession struct {
	lock                           sync.RWMutex
	vbno                           uint16
	dcpSeqnoWhenSessionStarts      uint64
	dcpManifestIdWhenSessionStarts uint64
	dcpSentOsoEnd                  bool
	seqnosFromDCPCnt               uint64
	seqnosHandledCnt               uint64

	// The following three integers completely represents the metrics of a
	// full OSO session only if dcpSentOsoEnd is true (i.e. on receipt of
	// OSO snapshot end marker). minSeqnoFromDCP and maxSeqnoFromDCP are
	// updated as and when the throughSeqnoSvc receives DataReceived events
	// from dcpNozzle (but doesn't provide a full picture until we receive
	// OSO snapshot end marker). However, numSeqnoFromDCP is only set when
	// OSO end snapshot marker is received.
	minSeqnoFromDCP uint64
	maxSeqnoFromDCP uint64
	numSeqnoFromDCP uint64

	maxManifestIdFromDCP   uint64
	maxManifestIdProcessed uint64 // Target Manifest for non-variable mode
	// first list keeps track of seqnos that are unsure
	// second list keeps the manifestID that corresponds to the seqno
	// Each session will potentially have max base.EventChanSize unsure seqnos
	unsureBufferedProcessedSeqnos *DualSortedSeqnoListWithLock

	// In variableVB mode, each target VB could have a max manifestID
	variableVbMode                      bool
	variableMaxManifestIdProcessed      map[uint16]uint64
	variableUnsureBufferedProcessSeqnos map[uint16]*DualSortedSeqnoListWithLock

	// cLogRequested indicates the number of conflict logging requests
	// for this session.
	cLogRequestedCnt uint64
	// cLogResponded indicates the number of conflict logging requests
	// that were done processing for this session.
	cLogRespondedCnt uint64

	// unsure cLog seqnos for the current session.
	// seqno_list_1 is for cLog requests. seqno_list_2 is for cLog responses.
	unsureBufferedCLogSeqnos *DualSortedSeqnoListWithLock

	tsTracker *ThroughSeqnoTrackerSvc
}

func (s *OsoSession) SetVariableVBMode(variableVBMode bool, tgtVbs []uint16) *OsoSession {
	if !variableVBMode {
		return s
	}

	s.variableVbMode = true
	for _, tgtVb := range tgtVbs {
		s.variableUnsureBufferedProcessSeqnos[tgtVb] = newDualSortedSeqnoListWithLock()
	}
	return s
}

func (s *OsoSession) IsDone() bool {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.isDoneNoLock()
}

func (s *OsoSession) ReceivedOSOEnd() bool {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.dcpSentOsoEnd
}

// Returns true i.e. this current session done, only when all the CLog requests of the session were done.
func (s *OsoSession) isDoneWithCLog() bool {
	return s.cLogRequestedCnt == s.cLogRespondedCnt
}

// A session is done if all these conditions are satisfied:
// 1. DCP has sent an OSO end, and
// 2. ThroughSeqnoSvc has received all the DataReceived events from DcpNozzle (given the event handling is asynchronous).
// 3. All the seqnos the DCP sent during this session has been processed by downstream parts, and
// 4. Conflict logger doesn't exist or it exists and has finished logging all the requests of this session.
func (s *OsoSession) isDoneNoLock() bool {
	tsTracker := s.tsTracker

	if s.dcpSentOsoEnd &&
		s.seqnosFromDCPCnt == s.numSeqnoFromDCP &&
		s.seqnosFromDCPCnt == s.seqnosHandledCnt {
		if tsTracker.cLogExists {
			// now we know that the current OSO session is done.
			// Try to fit in that unsure buffers of conflict logger in
			// this current session. Then we will deem this current session done,
			// only when all the requests of the session were done.
			return s.isDoneWithCLog()
		}

		// no conflict logger
		return true
	}
	return false
}

// any event that leads to calling this function, should make sure that it will be
// considered as part of markThroughSeqnoRelatedOsoEvent implemented in dcp_nozzle.
// Currently it is just DataReceived and SeqnoAdvReceived events (which inturn is a
// result of UPR_MUTATION, UPR_DELETION, UPR_EXPIRATION and DCP_SEQNO_ADV dcp events).
func (s *OsoSession) MarkSeqnoReceived(seqno uint64) (isDone bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.seqnosFromDCPCnt++

	if seqno < s.minSeqnoFromDCP {
		s.minSeqnoFromDCP = seqno
	}
	if seqno > s.maxSeqnoFromDCP {
		s.maxSeqnoFromDCP = seqno
	}

	isDone = s.isDoneNoLock()
	return
}

// If the targetVbno is non-nil, it will be used for target manifest ID tracking as it means
// the caller is confident about the target manifest ID
func (s *OsoSession) MarkSeqnoProcessed(vbno uint16, seqno uint64, manifestId uint64, tracker *ThroughSeqnoTrackerSvc, tgtVbToUse *uint16) (isDone bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.isDoneNoLock() {
		// if session is already done then it's a very very tight race condition
		// Originally, this seqno would have gone into unsureList, but because now we're sure it's not
		// part of OSO, just mark it done
		// Because OSO session Start will be synchronized, it guarantees that if this session (*s) is called
		// it means that isDone was false when this session was open
		tracker.addSentSeqnoAndManifestId(vbno, seqno, manifestId, tgtVbToUse)
		tracker.unusualOsoSessionEnd.Add(1)
		return
	}

	if !s.seqnoIsWithinRange(seqno) {
		// This sent seqno may or may not be within this oso session
		// Because the OnEvent() is async, we're not sure until a OSO end is received
		if tgtVbToUse != nil {
			s.variableUnsureBufferedProcessSeqnos[*tgtVbToUse].appendSeqnos(seqno, manifestId)
		} else {
			s.unsureBufferedProcessedSeqnos.appendSeqnos(seqno, manifestId)
		}
		return
	}

	s.markSeqnoProcessedInternal(manifestId, tgtVbToUse)

	isDone = s.isDoneNoLock()
	return
}

// If tgtVbToUse is provided and non-nil, then it's variableVB mode
func (s *OsoSession) markSeqnoProcessedInternal(manifestId uint64, tgtVbToUse *uint16) {
	s.seqnosHandledCnt++

	if tgtVbToUse == nil {
		if manifestId > s.maxManifestIdProcessed {
			s.maxManifestIdProcessed = manifestId
		}
	} else {
		if manifestId > s.variableMaxManifestIdProcessed[*tgtVbToUse] {
			s.variableMaxManifestIdProcessed[*tgtVbToUse] = manifestId
		}
	}
}

func (s *OsoSession) GetStartingSeqnoNoLock() uint64 {
	return s.dcpSeqnoWhenSessionStarts
}

// This is called when DCP has sent OSO end
// This session will know that DCP has sent an OSO End, but technically the session is not done yet
// until all the seqnos that DCP has sent has been processed by downstream parts
// When a session has ended, the seqno's in this session will now be introduced into tsTracker as a singular seqno,
// essentially creating a gap, pretending that deduping occurred
func (s *OsoSession) RegisterSessionEnd(vbno uint16, tsTracker *ThroughSeqnoTrackerSvc, minSeqnoFromDCP, maxSeqnoFromDCP, numSeqnoFromDCP uint64) (isDone bool) {
	s.lock.Lock()

	s.dcpSentOsoEnd = true

	// since we now has OSO snapshot end marker, we now have the full picture of the
	// latest snapshot with respect to dcp. Set min, max and number of seqnos of
	// the snapshot, as seen by dcpNozzle to process unsure buffers, given that some
	// of the asynchronous DataReceived events might be yet to come to throughSeqnoSvc.
	s.minSeqnoFromDCP = minSeqnoFromDCP
	s.maxSeqnoFromDCP = maxSeqnoFromDCP
	s.numSeqnoFromDCP = numSeqnoFromDCP

	if tsTracker.osoSnapshotRaiser != nil {
		// Since we have a contiguous list of sessions, we only need to raise the snapshot for the maxSeqno of all the sessions
		tsTracker.osoSnapshotRaiser(vbno, s.maxSeqnoFromDCP)
	} else {
		tsTracker.logger.Warnf("oso snapshot raiser is nil")
	}

	s.processUnsureBuffer(vbno, tsTracker)

	s.lock.Unlock()

	// When a session is done, we will treat the whole OSO session as a single gap
	// from lastSeqnoSeen+1 to maxProcessed, it will be as if there's a big gap
	tsTracker.processGapSeqnos(vbno, maxSeqnoFromDCP)

	return s.IsDone()
}

// Essentially, the session kept track of the seqno at the session's registration/inception
// The session then kept track of the max seqno (since oso is out of order) from DCP, and then
// only returns "true" for done() only if that max seqno has been handled
// Once done, the playback method here is invoked by upper level caller
// When OSO end is sent from DCP, the osoSession handles that and introduces a gap into tsTracker
// Playback is a counterpart mechanism that essentially fills in the gap via the maxSeqno in the session
// and acknowledges that the gap is now complete along with all other necessary max's that took place during the OSO
// session (i.e. maxManifesId).
func (s *OsoSession) PlaybackOntoTsTracker(tsTracker *ThroughSeqnoTrackerSvc, vbno uint16) uint64 {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if !s.isDoneNoLock() {
		panic("should be done")
	}

	maxProcessedSeqno := s.maxSeqnoFromDCP

	// This takes care of filling in the gap created by RegisterSessionEnd()
	tsTracker.addSentSeqno(vbno, maxProcessedSeqno)

	if s.maxManifestIdFromDCP > s.dcpManifestIdWhenSessionStarts {
		tsTracker.vbSystemEventsSeqnoListMap.UpdateOrAppendSeqnoManifest(vbno, maxProcessedSeqno, s.maxManifestIdProcessed)
	}

	// Playback will depend on the recording logic to properly choose either variable VB or non-variable VB mode
	if s.maxManifestIdProcessed > 0 {
		tsTracker.vbTgtSeqnoManifestMap.UpdateOrAppendSeqnoManifest(vbno, maxProcessedSeqno, s.maxManifestIdProcessed)
	}
	// The following does the same above logic but in the case of variable VBucket
	for tgtVbno, maxManifestIdProcessed := range s.variableMaxManifestIdProcessed {
		tsTracker.vbTgtSeqnoManifestMap.UpdateOrAppendSeqnoManifest(tgtVbno, maxProcessedSeqno, maxManifestIdProcessed)
	}
	return maxProcessedSeqno
}

func (s *OsoSession) GetMaxDCPSeenSeqno() uint64 {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.maxSeqnoFromDCP
}

// ShouldProcessAsOSO chain is called as part when the dcp nozzle or the downstream parts raise event saying a seqnoToBeProcessed has been
// received or handled. This routine returns true if seqnoToBeProcessed is (or could be in case end snapshot is not received yet) part of s.
//
// Given a seqnoToBeProcessed, if this session is not done, and this session "contains" the seqnoToBeProcessed that is waiting to be processed,
// then this seqnoToBeProcessed should be handled the OSO way against this session. If that's not the case, it "could be" part of this session
// only if snapshot end marker is not received yet (will be part of unsure buffer though). If even that is not the case, seqnoToBeProcessed
// cannot be processed against this session.
func (s *OsoSession) ShouldProcessAsOSO(seqnoToBeProcessed uint64) (shouldProcessAsOSO bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	// If this session is done, there's no way that seqnoToBeProcessed is part of this session
	if s.isDoneNoLock() {
		return
	}

	if s.seqnoIsWithinRange(seqnoToBeProcessed) {
		// seqnoToBeProcessed essentially falls within the gap range that's currently covered in this session
		shouldProcessAsOSO = true
		return
	}

	if !s.dcpSentOsoEnd {
		// We may or may not be sure if this processSeqno is within this range yet
		shouldProcessAsOSO = true
		return
	}
	return
}

// Lock needs to be held
func (s *OsoSession) seqnoIsWithinRange(seqnoToBeProcessed uint64) bool {
	return seqnoToBeProcessed >= s.minSeqnoFromDCP && seqnoToBeProcessed <= s.maxSeqnoFromDCP
}

func (s *OsoSession) MarkManifestReceived(manifestId uint64) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if manifestId > s.maxManifestIdFromDCP {
		s.maxManifestIdFromDCP = manifestId
	}
}

// Lock is held
// When a session is closed, the "unsure" list of seqnos can now be checked against the range
// Anything not against the range can be marked as non-OSO processed seqnos and fill in the gaps
func (s *OsoSession) processUnsureBuffer(vbno uint16, tsTracker *ThroughSeqnoTrackerSvc) {
	if !s.variableVbMode {
		s.processOneUnsureBufferedSeqnosList(s.unsureBufferedProcessedSeqnos, vbno, tsTracker, nil)
	} else {
		for tgtVb, unsureBufferedProcessedSeqno := range s.variableUnsureBufferedProcessSeqnos {
			s.processOneUnsureBufferedSeqnosList(unsureBufferedProcessedSeqno, vbno, tsTracker, &tgtVb)
		}
	}

	// do it for conflict logging requests and responses too.
	s.processCLogUnsureBufferedSeqnosList(vbno, tsTracker)
}

func (s *OsoSession) processCLogUnsureBufferedSeqnosList(vbno uint16, tsTracker *ThroughSeqnoTrackerSvc) {
	unsureCLogReqList, unsureCLogResList := s.unsureBufferedCLogSeqnos.getSortedSeqnoLists(vbno, "CLogReqResList")
	for _, seqno := range unsureCLogReqList {
		if s.seqnoIsWithinRange(seqno) {
			s.cLogRequestedCnt++
		} else {
			// seqno is not from this session, most likely from a previous snapshot.
			// The previous snapshot could have been non-OSO.
			tsTracker.appendCLogReq(vbno, seqno)
		}
	}

	for _, seqno := range unsureCLogResList {
		if s.seqnoIsWithinRange(seqno) {
			s.cLogRespondedCnt++
		} else {
			// seqno is not from this session, most likely from a previous snapshot.
			// The previous snapshot could have been non-OSO.
			tsTracker.appendCLogRes(vbno, seqno)
		}
	}
}

func (s *OsoSession) processOneUnsureBufferedSeqnosList(unsureBufferedProcessedSeqnos *DualSortedSeqnoListWithLock, vbno uint16, tsTracker *ThroughSeqnoTrackerSvc, tgtVb *uint16) {
	listsLen := unsureBufferedProcessedSeqnos.getLengthOfSeqnoLists()
	for i := 0; i < listsLen; i++ {
		seqno := unsureBufferedProcessedSeqnos.seqno_list_1[i]
		manifestId := unsureBufferedProcessedSeqnos.seqno_list_2[i]
		if s.seqnoIsWithinRange(seqno) {
			s.markSeqnoProcessedInternal(manifestId, tgtVb)
		} else {
			tsTracker.addSentSeqnoAndManifestId(vbno, seqno, manifestId, tgtVb)
		}
	}
}

func (s *OsoSession) markCLogReqSeqno(vbno uint16, seqno uint64) (isDone bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.isDoneNoLock() {
		// if session is already done then it's a very very tight race condition
		// Originally, this seqno would have gone into unsureList, but because now we're sure it's not
		// part of OSO, just mark it done
		// Because OSO session Start will be synchronized, it guarantees that if this session (*s) is called
		// it means that isDone was false when this session was open
		s.tsTracker.appendCLogReq(vbno, seqno)
		s.tsTracker.unusualOsoSessionEnd.Add(1)
		return
	}

	if !s.seqnoIsWithinRange(seqno) {
		// This sent seqno may or may not be within this oso session
		// Because the OnEvent() is async, we're not sure until a OSO end is received
		s.unsureBufferedCLogSeqnos.appendSeqnoToList1(seqno)
		return
	}

	// seqno is part of this session
	s.cLogRequestedCnt++

	isDone = s.isDoneNoLock()
	return
}

func (s *OsoSession) markCLogResSeqno(vbno uint16, seqno uint64) (isDone bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.isDoneNoLock() {
		// if session is already done then it's a very very tight race condition
		// Originally, this seqno would have gone into unsureList, but because now we're sure it's not
		// part of OSO, just mark it done
		// Because OSO session Start will be synchronized, it guarantees that if this session (*s) is called
		// it means that isDone was false when this session was open
		s.tsTracker.appendCLogRes(vbno, seqno)
		s.tsTracker.unusualOsoSessionEnd.Add(1)
		return
	}

	if !s.seqnoIsWithinRange(seqno) {
		// This sent seqno may or may not be within this oso session
		// Because the OnEvent() is async, we're not sure until a OSO end is received
		s.unsureBufferedCLogSeqnos.appendSeqnoToList2(seqno)
		return
	}

	// seqno is part of this session
	s.cLogRespondedCnt++

	isDone = s.isDoneNoLock()
	return
}

type SeqnoManifestsMapType map[uint16]*DualSortedSeqnoListWithLock

// If the manifestID has not changed, then this function will update the existing seqno associated with the manifestID
// to be the incoming (and higher) seqno.
// If the manifestID has changed, then this function will perform append
func (t *SeqnoManifestsMapType) UpdateOrAppendSeqnoManifest(vbno uint16, seqno, manifestId uint64) error {
	if t == nil {
		return base.ErrorInvalidInput
	}

	lists, found := (*t)[vbno]
	if !found {
		return base.ErrorInvalidInput
	}

	lists.lock.Lock()
	defer lists.lock.Unlock()

	if len(lists.seqno_list_1) == 0 {
		// First time
		lists.seqno_list_1 = append(lists.seqno_list_1, seqno)
		lists.seqno_list_2 = append(lists.seqno_list_2, manifestId)
	} else {
		N := len(lists.seqno_list_1) - 1
		highestSeqno := lists.seqno_list_1[N]
		currentManifestId := lists.seqno_list_2[N]
		if manifestId > currentManifestId {
			if seqno < highestSeqno {
				// It is possible that data sent out can be out of order
				// For example, seqno 3 is sent out first with manifest 1
				// Then seqno 2 is sent out but tagged with manifest 2 as the
				// new target manifest is updated
				// In this case, tag the higher seqno (3) with the highest manifest (2)
				lists.seqno_list_2[N] = manifestId
			} else {
				// Need to add the new manifest as the top of the list
				lists.seqno_list_1 = append(lists.seqno_list_1, seqno)
				lists.seqno_list_2 = append(lists.seqno_list_2, manifestId)
			}
		} else if manifestId == currentManifestId && seqno > highestSeqno {
			// Note this seqno as the last known seqno for the current highest manifestId
			lists.seqno_list_1[N] = seqno
		}
	}
	return nil
}

func (t *SeqnoManifestsMapType) GetManifestId(vbno uint16, seqno uint64) (uint64, error) {
	if t == nil {
		return 0, base.ErrorInvalidInput
	}

	listsObj, ok := (*t)[vbno]
	if !ok {
		return 0, base.ErrorInvalidInput
	}

	listsObj.lock.RLock()
	emptyList := len(listsObj.seqno_list_1) == 0 && len(listsObj.seqno_list_2) == 0
	listsObj.lock.RUnlock()
	if emptyList {
		return 0, nil
	}

	manifestId, err := listsObj.getList2BasedonList1Floor(seqno)
	if err != nil {
		err = fmt.Errorf("Unable to retrieve manifest for vb %v, requesting seqno %v\n", vbno, seqno)
	}
	return manifestId, err
}

// specifically used for gap seqno list since the input through_seqno
// should never be found in the list.
func truncateGapSeqnoList(through_seqno uint64, seqno_list []uint64) []uint64 {
	index, found := base.SearchUint64List(seqno_list, through_seqno)
	if found {
		panic(fmt.Sprintf("through_seqno %v cannot be in gap_seqno_list", through_seqno))
	} else if index > 0 {
		return seqno_list[index:]
	}

	return seqno_list
}

// used in a generic manner which truncates the input seqno_list to
// have no elements less than or equal to through_seqno.
func truncateOneSeqnoList(through_seqno uint64, seqno_list []uint64) []uint64 {
	index, found := base.SearchUint64List(seqno_list, through_seqno)
	if found {
		return seqno_list[index+1:]
	} else if index > 0 {
		return seqno_list[index:]
	}

	return seqno_list
}

func NewThroughSeqnoTrackerSvc(logger_ctx *log.LoggerContext, osoSnapshotRaiser func(vbno uint16, seqno uint64), hasConflictLogger bool) *ThroughSeqnoTrackerSvc {
	logger := log.NewLogger("ThrSeqTrackSvc", logger_ctx)
	tsTracker := &ThroughSeqnoTrackerSvc{
		AbstractComponent:           component.NewAbstractComponentWithLogger("ThrSeqTrackSvc", logger),
		logger:                      logger,
		vb_map:                      make(map[uint16]bool),
		through_seqno_map:           make(map[uint16]*base.SeqnoWithLock),
		vb_last_seen_seqno_map:      make(map[uint16]*base.SeqnoWithLock),
		vb_sent_seqno_list_map:      make(map[uint16]*base.SortedSeqnoListWithLock),
		vb_filtered_seqno_list_map:  make(map[uint16]*base.SortedSeqnoListWithLock),
		vb_failed_cr_seqno_list_map: make(map[uint16]*base.SortedSeqnoListWithLock),
		vb_gap_seqno_list_map:       make(map[uint16]*DualSortedSeqnoListWithLock),
		vbSystemEventsSeqnoListMap:  make(SeqnoManifestsMapType),
		vbIgnoredSeqnoListMap:       make(map[uint16]*base.SortedSeqnoListWithLock),
		vbTgtSeqnoManifestMap:       make(SeqnoManifestsMapType),
		vbBackfillLastDCPSeqnoMap:   make(map[uint16]*base.SeqnoWithLock),
		vbBackfillHelperDoneMap:     make(map[uint16]*base.SeqnoWithLock),
		vbClonedTracker:             make(map[uint16]*DualSortedSeqnoListWithLock),
		vbOsoModeSessionDCPTracker:  make(OSOSeqnoTrackerMapType),
		osoSnapshotRaiser:           osoSnapshotRaiser,
		cLogTrackerVbMap:            make(map[uint16]*DualSortedSeqnoListWithLock),
		cLogExists:                  hasConflictLogger,
	}

	return tsTracker
}

func (tsTracker *ThroughSeqnoTrackerSvc) initialize(pipeline common.Pipeline) {
	tsTracker.rep_id = pipeline.Topic()
	tsTracker.id = pipeline.FullTopic() + "_" + base.ThroughSeqnoTracker
	tsTracker.attachedPipeline = pipeline
	sourceVBList := base.SortUint16List(pipeline_utils.GetSourceVBListPerPipeline(pipeline))
	targetVBList := base.SortUint16List(pipeline_utils.GetTargetVBListPerPipeline(pipeline))

	if !base.AreSortedUint16ListsTheSame(sourceVBList, targetVBList) {
		tsTracker.variableVBMode = true
		tsTracker.variableTgtVbs = targetVBList
		tsTracker.variableVbTgtMap = make(map[uint16]bool)
		for _, tgtVb := range targetVBList {
			tsTracker.variableVbTgtMap[tgtVb] = true
		}
	}

	for _, vbno := range sourceVBList {
		tsTracker.vb_map[vbno] = true

		tsTracker.through_seqno_map[vbno] = base.NewSeqnoWithLock()
		tsTracker.vb_last_seen_seqno_map[vbno] = base.NewSeqnoWithLock()
		tsTracker.vb_filtered_seqno_list_map[vbno] = base.NewSortedSeqnoListWithLock()
		tsTracker.vb_gap_seqno_list_map[vbno] = newDualSortedSeqnoListWithLock()
		tsTracker.vbSystemEventsSeqnoListMap[vbno] = newDualSortedSeqnoListWithLock()
		tsTracker.vbIgnoredSeqnoListMap[vbno] = base.NewSortedSeqnoListWithLock()
		tsTracker.vbBackfillLastDCPSeqnoMap[vbno] = base.NewSeqnoWithLock()
		tsTracker.vbBackfillHelperDoneMap[vbno] = base.NewSeqnoWithLock()

		tsTracker.vb_sent_seqno_list_map[vbno] = base.NewSortedSeqnoListWithLock()
		tsTracker.vbTgtSeqnoManifestMap[vbno] = newDualSortedSeqnoListWithLock()
		tsTracker.vbOsoModeSessionDCPTracker[vbno] = newOsoSessionTracker(tsTracker.variableVBMode, tsTracker)
		tsTracker.vbClonedTracker[vbno] = newDualSortedSeqnoListWithLock()
		tsTracker.vb_failed_cr_seqno_list_map[vbno] = base.NewSortedSeqnoListWithLock()

		tsTracker.cLogTrackerVbMap[vbno] = newDualSortedSeqnoListWithLock().setNoLenRestriction()
	}

	// Initialize for all the targetVBs as well
	for _, vbno := range targetVBList {
		if _, alreadyDone := tsTracker.vb_map[vbno]; alreadyDone {
			continue
		}
		tsTracker.vb_sent_seqno_list_map[vbno] = base.NewSortedSeqnoListWithLock()
		tsTracker.vbTgtSeqnoManifestMap[vbno] = newDualSortedSeqnoListWithLock()
		tsTracker.vbOsoModeSessionDCPTracker[vbno] = newOsoSessionTracker(tsTracker.variableVBMode, tsTracker)
		tsTracker.vbClonedTracker[vbno] = newDualSortedSeqnoListWithLock()
		tsTracker.vb_failed_cr_seqno_list_map[vbno] = base.NewSortedSeqnoListWithLock()

		tsTracker.cLogTrackerVbMap[vbno] = newDualSortedSeqnoListWithLock().setNoLenRestriction()
	}

}

func (tsTracker *ThroughSeqnoTrackerSvc) Attach(pipeline common.Pipeline) error {
	tsTracker.logger.Infof("Attach through seqno tracker with %v %v\n", pipeline.Type(), pipeline.InstanceId())

	tsTracker.initialize(pipeline)

	asyncListenerMap := pipeline_pkg.GetAllAsyncComponentEventListeners(pipeline)

	pipeline_utils.RegisterAsyncComponentEventHandler(asyncListenerMap, base.DataSentEventListener, tsTracker)
	pipeline_utils.RegisterAsyncComponentEventHandler(asyncListenerMap, base.DataFailedCREventListener, tsTracker)
	pipeline_utils.RegisterAsyncComponentEventHandler(asyncListenerMap, base.OutNozzleDataSkippedEventListener, tsTracker)
	pipeline_utils.RegisterAsyncComponentEventHandler(asyncListenerMap, base.DataFilteredEventListener, tsTracker)
	pipeline_utils.RegisterAsyncComponentEventHandler(asyncListenerMap, base.DataReceivedEventListener, tsTracker)
	pipeline_utils.RegisterAsyncComponentEventHandler(asyncListenerMap, base.DataClonedEventListener, tsTracker)
	pipeline_utils.RegisterAsyncComponentEventHandler(asyncListenerMap, base.TrueConflictsEventListener, tsTracker)
	pipeline_utils.RegisterAsyncComponentEventHandler(asyncListenerMap, base.CLogWriteStatusEventListener, tsTracker)

	conflictMgr := pipeline.RuntimeContext().Service(base.CONFLICT_MANAGER_SVC)
	if conflictMgr != nil {
		conflictMgr.(*pipeline_svc.ConflictManager).RegisterComponentEventListener(common.DataMerged, tsTracker)
		conflictMgr.(*pipeline_svc.ConflictManager).RegisterComponentEventListener(common.MergeCasChanged, tsTracker)
		conflictMgr.(*pipeline_svc.ConflictManager).RegisterComponentEventListener(common.MergeFailed, tsTracker)
	}

	//register pipeline supervisor as through seqno service's error handler
	supervisor := pipeline.RuntimeContext().Service(base.PIPELINE_SUPERVISOR_SVC)
	if supervisor == nil {
		err := errors.New("Pipeline supervisor has to exist")
		tsTracker.logger.Errorf("%v", err)
		return err
	}
	err := tsTracker.RegisterComponentEventListener(common.ErrorEncountered, supervisor.(common.PipelineSupervisorSvc))
	if err != nil {
		tsTracker.logger.Errorf("%v", err)
		return err
	}

	if pipeline.Type() == common.BackfillPipeline {
		// For backfill pipeline, the throughSeqnoTrackerSvc is needed to do the following (per needed VB):
		// 1. Listen to DCP for stream end
		// 2. Once (1) occurs, retrieve the "lastSeen" seqno. This is the last seqno that DCP received
		// 3. Watch for this seqno to be handled, i.e. become the throughSeqnNo
		// 4. Raise an Event to backfill request handler
		backfillMgrPipelineSvc := pipeline.RuntimeContext().Service(base.BACKFILL_MGR_SVC)
		if backfillMgrPipelineSvc == nil {
			err := errors.New("Backfill Manager has to exist")
			tsTracker.logger.Errorf("%v", err)
			return err
		}

		componentListenerGetter, ok := backfillMgrPipelineSvc.(service_def.BackfillMgrComponentListenerGetter)
		if !ok {
			err := fmt.Errorf("For backfill pipeline service, cannot cast as componentListenerGetter type: %v", reflect.TypeOf(componentListenerGetter))
			tsTracker.logger.Errorf("%v", err)
			return err
		}

		backfillPipelineMgrComponentListener, err := componentListenerGetter.GetComponentEventListener(pipeline)
		if err != nil {
			return err
		}

		tsTracker.RegisterComponentEventListener(common.LastSeenSeqnoDoneProcessed, backfillPipelineMgrComponentListener)
		if err != nil {
			tsTracker.logger.Errorf("%v", err)
			return err
		}

		dcp_parts := pipeline.Sources()
		for _, dcp := range dcp_parts {
			dcp.RegisterComponentEventListener(common.StreamingEnd, tsTracker)
			dcp.RegisterComponentEventListener(common.StreamingBypassed, tsTracker)
		}
	}

	return nil
}

func (tsTracker *ThroughSeqnoTrackerSvc) markUprEventAsFiltered(uprEvent *mcc.UprEvent) {
	if uprEvent == nil {
		return
	}
	seqno := uprEvent.Seqno
	vbno := uprEvent.VBucket
	tsTracker.addFilteredSeqno(vbno, seqno)
}

func (tsTracker *ThroughSeqnoTrackerSvc) markMCRequestAsIgnored(req *base.WrappedMCRequest) {
	if req == nil {
		return
	}
	seqno := req.Seqno
	vbno := req.GetSourceVB()
	tsTracker.addIgnoredSeqno(vbno, seqno)
}

// Implement ComponentEventListener
func (tsTracker *ThroughSeqnoTrackerSvc) OnEvent(event *common.Event) {
	tsTracker.ProcessEvent(event)
}

func (*ThroughSeqnoTrackerSvc) ListenerPipelineType() common.ListenerPipelineType {
	return common.ListenerNotShared
}

func (tsTracker *ThroughSeqnoTrackerSvc) ProcessEvent(event *common.Event) error {
	if !tsTracker.isPipelineRunning() {
		tsTracker.logger.Tracef("Pipeline %s is no longer running, skip ProcessEvent for %v\n", tsTracker.rep_id, event)
	}

	shouldProceed := tsTracker.preProcessOutgoingClonedEvent(event)
	if !shouldProceed {
		return nil
	}

	switch event.EventType {
	case common.DataSent:
		sentAdditional := event.OtherInfos.(parts.DataSentEventAdditional)
		vbno := sentAdditional.GetVB()
		seqno := sentAdditional.Seqno
		manifestId := sentAdditional.ManifestId
		shouldProcessAsOSO, session := tsTracker.shouldProcessAsOso(vbno, seqno)
		var tgtVbToUse *uint16
		if tsTracker.variableVBMode {
			tgtVbToUse = &sentAdditional.VBucket
		}
		if !shouldProcessAsOSO {
			tsTracker.addSentSeqnoAndManifestId(vbno, seqno, manifestId, tgtVbToUse)
		} else {
			done := session.MarkSeqnoProcessed(vbno, seqno, manifestId, tsTracker, tgtVbToUse)
			if done {
				tsTracker.HandleDoneSession(vbno, session)
			}
		}
	case common.MergeCasChanged:
		// Cas changed count as sent. We will converge to new mutation in the source
		casChangedEvent := event.OtherInfos.(pipeline_svc.DataMergeCasChangedEventAdditional)
		vbno := casChangedEvent.GetVB()
		seqno := casChangedEvent.Seqno
		manifestId := casChangedEvent.ManifestId
		shouldProcessAsOSO, session := tsTracker.shouldProcessAsOso(vbno, seqno)
		var tgtVbToUse *uint16
		if tsTracker.variableVBMode {
			tgtVbToUse = &casChangedEvent.VBucket
		}
		if !shouldProcessAsOSO {
			tsTracker.addSentSeqnoAndManifestId(vbno, seqno, manifestId, tgtVbToUse)
		} else {
			done := session.MarkSeqnoProcessed(vbno, seqno, manifestId, tsTracker, tgtVbToUse)
			if done {
				tsTracker.HandleDoneSession(vbno, session)
			}
		}
	case common.DataMerged:
		dataMergedEvt := event.OtherInfos.(pipeline_svc.DataMergedEventAdditional)
		vbno := dataMergedEvt.GetVB()
		seqno := dataMergedEvt.Seqno
		manifestId := dataMergedEvt.ManifestId
		processedAsOSO, session := tsTracker.shouldProcessAsOso(vbno, seqno)
		var tgtVbToUse *uint16
		if tsTracker.variableVBMode {
			tgtVbToUse = &dataMergedEvt.VBucket
		}
		if !processedAsOSO {
			tsTracker.addSentSeqnoAndManifestId(vbno, seqno, manifestId, tgtVbToUse)
		} else {
			done := session.MarkSeqnoProcessed(vbno, seqno, manifestId, tsTracker, tgtVbToUse)
			if done {
				tsTracker.HandleDoneSession(vbno, session)
			}
		}
	case common.MergeFailed:
		failedEvt := event.OtherInfos.(pipeline_svc.DataMergeFailedEventAdditional)
		vbno := failedEvt.GetVB()
		seqno := failedEvt.Seqno
		manifestId := failedEvt.ManifestId
		processedAsOSO, session := tsTracker.shouldProcessAsOso(vbno, seqno)
		var tgtVbToUse *uint16
		if tsTracker.variableVBMode {
			tgtVbToUse = &failedEvt.VBucket
		}
		if !processedAsOSO {
			tsTracker.addSentSeqnoAndManifestId(vbno, seqno, manifestId, tgtVbToUse)
		} else {
			done := session.MarkSeqnoProcessed(vbno, seqno, manifestId, tsTracker, tgtVbToUse)
			if done {
				tsTracker.HandleDoneSession(vbno, session)
			}
		}
	case common.DataFiltered:
		uprEvent := event.Data.(*mcc.UprEvent)
		vbno := uprEvent.VBucket
		seqno := uprEvent.Seqno
		manifestId := event.OtherInfos.(parts.DataFilteredAdditional).ManifestId
		processedAsOSO, session := tsTracker.shouldProcessAsOso(vbno, seqno)
		tsTracker.markUprEventAsFiltered(uprEvent)
		var tgtVbToUse *uint16
		if tsTracker.variableVBMode {
			tgtVb := base.GetVBucketNo(uprEvent.Key, len(tsTracker.variableTgtVbs))
			tgtVbToUse = &tgtVb
		}
		if !processedAsOSO {
			if tgtVbToUse != nil {
				tsTracker.addManifestId(*tgtVbToUse, uprEvent.Seqno, manifestId)
			} else {
				tsTracker.addManifestId(vbno, uprEvent.Seqno, manifestId)
			}
		} else {
			done := session.MarkSeqnoProcessed(vbno, seqno, manifestId, tsTracker, tgtVbToUse)
			if done {
				tsTracker.HandleDoneSession(vbno, session)
			}
		}
	case common.DataUnableToFilter:
		err := event.DerivedData[0].(error)
		// If error is recoverable, do not mark it filtered in order to avoid data loss
		if !base.FilterErrorIsRecoverable(err) {
			uprEvent := event.Data.(*mcc.UprEvent)
			vbno := uprEvent.VBucket
			seqno := uprEvent.Seqno
			manifestId := event.OtherInfos.(parts.DataFilteredAdditional).ManifestId
			processedAsOSO, session := tsTracker.shouldProcessAsOso(vbno, seqno)
			var tgtVbToUse *uint16
			if tsTracker.variableVBMode {
				tgtVb := base.GetVBucketNo(uprEvent.Key, len(tsTracker.variableTgtVbs))
				tgtVbToUse = &tgtVb
			}
			if !processedAsOSO {
				tsTracker.markUprEventAsFiltered(uprEvent)
			} else {
				done := session.MarkSeqnoProcessed(vbno, seqno, manifestId, tsTracker, tgtVbToUse)
				if done {
					tsTracker.HandleDoneSession(vbno, session)
				}
			}
		}
	case common.DataFailedCRSource:
		dataFailedCRSrc := event.OtherInfos.(parts.DataFailedCRSourceEventAdditional)
		seqno := dataFailedCRSrc.Seqno
		vbno := dataFailedCRSrc.GetVB()
		manifestId := dataFailedCRSrc.ManifestId
		processedAsOSO, session := tsTracker.shouldProcessAsOso(vbno, seqno)
		var tgtVbToUse *uint16
		if tsTracker.variableVBMode {
			tgtVbToUse = &dataFailedCRSrc.VBucket
		}
		if !processedAsOSO {
			tsTracker.addFailedCRSeqno(vbno, seqno)
			if tgtVbToUse != nil {
				tsTracker.addManifestId(*tgtVbToUse, seqno, manifestId)
			} else {
				tsTracker.addManifestId(vbno, seqno, manifestId)
			}
		} else {
			done := session.MarkSeqnoProcessed(vbno, seqno, manifestId, tsTracker, tgtVbToUse)
			if done {
				tsTracker.HandleDoneSession(vbno, session)
			}
		}
	case common.SubdocCmdSkippedDueToLimits:
		fallthrough
	case common.TargetDataSkipped:
		skippedEvt := event.OtherInfos.(parts.OutNozzleDataSkippedEventAdditional)
		seqno := skippedEvt.Seqno
		vbno := skippedEvt.GetVB()
		manifestId := skippedEvt.ManifestId
		shouldProcessAsOSO, session := tsTracker.shouldProcessAsOso(vbno, seqno)
		var tgtVbToUse *uint16
		if tsTracker.variableVBMode {
			tgtVbToUse = &skippedEvt.VBucket
		}
		if !shouldProcessAsOSO {
			tsTracker.addFailedCRSeqno(vbno, seqno)
			if tgtVbToUse != nil {
				tsTracker.addManifestId(*tgtVbToUse, seqno, manifestId)
			} else {
				tsTracker.addManifestId(vbno, seqno, manifestId)
			}
		} else {
			done := session.MarkSeqnoProcessed(vbno, seqno, manifestId, tsTracker, tgtVbToUse)
			if done {
				tsTracker.HandleDoneSession(vbno, session)
			}
		}
	case common.DataReceived:
		upr_event := event.Data.(*mcc.UprEvent)
		seqno := upr_event.Seqno
		vbno := upr_event.VBucket
		// Sets last sequence number, and should the sequence number skip due to gap, this will take care of it
		processedAsOso, session := tsTracker.shouldProcessAsOso(vbno, seqno)
		if !processedAsOso {
			tsTracker.processGapSeqnos(vbno, seqno)
		} else {
			done := session.MarkSeqnoReceived(seqno)
			if done {
				tsTracker.HandleDoneSession(vbno, session)
			}
		}
	case common.SystemEventReceived:
		uprEvent := event.Data.(*mcc.UprEvent)
		seqno := uprEvent.Seqno
		vbno := uprEvent.VBucket
		// SystemEventReceived should not be OSO related, because system events tell of a subscriber of a collection creation/drop
		// When OSO is used, it is only used on a single collection - thus no reason why it would receive an system event
		// But just in case, have the handler here implemented
		processedAsOSO, session := tsTracker.shouldProcessAsOso(vbno, seqno)
		if !processedAsOSO {
			tsTracker.processGapSeqnos(vbno, seqno)
			tsTracker.markSystemEvent(uprEvent)
		} else {
			manifestId, _ := uprEvent.GetManifestId()
			session.MarkManifestReceived(manifestId)
		}
	case common.SeqnoAdvReceived:
		// For SeqnoAdv - handle it as any other mutations that's being passed down
		uprEvent := event.Data.(*mcc.UprEvent)
		seqno := uprEvent.Seqno
		vbno := uprEvent.VBucket
		processedAsOSO, session := tsTracker.shouldProcessAsOso(vbno, seqno)
		if !processedAsOSO {
			tsTracker.processGapSeqnos(vbno, seqno)
		} else {
			done := session.MarkSeqnoReceived(seqno)
			if done {
				tsTracker.HandleDoneSession(vbno, session)
			}
		}
		// But instead of doing additional work, just follow the recipe for OutNozzleDataSkipped
		if !processedAsOSO {
			tsTracker.addIgnoredSeqno(vbno, seqno)
		} else {
			done := session.MarkSeqnoProcessed(vbno, seqno, 0, tsTracker, nil)
			if done {
				tsTracker.HandleDoneSession(vbno, session)
			}
		}
	case common.DataNotReplicated:
		wrappedMcr := event.Data.(*base.WrappedMCRequest)
		manifestInfo := event.OtherInfos.(parts.ManifestAdditional)
		vbno := wrappedMcr.GetSourceVB()
		seqno := wrappedMcr.Seqno
		manifestAddn := event.OtherInfos.(parts.ManifestAdditional)
		recycler := manifestAddn.RecycleFunc
		processedAsOSO, session := tsTracker.shouldProcessAsOso(vbno, seqno)
		var tgtVbToUse *uint16
		if tsTracker.variableVBMode {
			tgtVbToUse = &wrappedMcr.Req.VBucket
		}
		if !processedAsOSO {
			tsTracker.markMCRequestAsIgnored(wrappedMcr)
		} else {
			done := session.MarkSeqnoProcessed(vbno, seqno, manifestInfo.ManifestId, tsTracker, tgtVbToUse)
			if done {
				tsTracker.HandleDoneSession(vbno, session)
			}
		}
		recycler(wrappedMcr)
	case common.StreamingEnd:
		// this event is only raised for backfill pipelines.
		vbno, ok := event.Data.(uint16)
		if !ok {
			err := fmt.Errorf("invalid vbno data type raised for StreamingEnd. Type: %v", reflect.TypeOf(event.Data))
			tsTracker.logger.Errorf(err.Error())
			tsTracker.handleGeneralError(err)
			return err
		}
		endSeqno, ok := event.OtherInfos.(uint64)
		if !ok {
			err := fmt.Errorf("invalid endSeqno data type raised for StreamingEnd. Type: %v", reflect.TypeOf(event.OtherInfos))
			tsTracker.logger.Errorf(err.Error())
			tsTracker.handleGeneralError(err)
			return err
		}
		shouldStreamEnd := tsTracker.shouldStreamEnd(vbno)
		if shouldStreamEnd {
			tsTracker.handleBackfillStreamEnd(vbno, endSeqno)
		} else {
			err := fmt.Errorf("received streamEnd before receiving oso end")
			tsTracker.logger.Errorf(err.Error())
			tsTracker.handleGeneralError(err)
			return err
		}
	case common.StreamingBypassed:
		vbno, ok := event.Data.(uint16)
		if !ok {
			err := fmt.Errorf("Invalid vbno data type raised for StreamingBypassed. Type: %v", reflect.TypeOf(event.Data))
			tsTracker.logger.Errorf(err.Error())
			tsTracker.handleGeneralError(err)
			return err
		}
		tsTracker.handleBackfillStreamBypass(vbno)
	case common.DataCloned:
		data := event.Data.([]interface{})
		vbno := data[0].(uint16)
		seqno := data[1].(uint64)
		totalCount := data[2].(int)
		syncCh := data[4].(chan bool)
		tsTracker.handleDataClonedEvent(vbno, seqno, totalCount, syncCh)
	case common.OsoSnapshotReceived:
		mode := event.Data.(bool)
		helper := event.DerivedData
		vbno := helper[0].(uint16)
		syncCh := helper[1].(chan bool)
		minSeqnoFromDCP := helper[2].(uint64)
		maxSeqnoFromDCP := helper[3].(uint64)
		numSeqnoFromDCP := helper[4].(uint64)
		isDone, session := tsTracker.ProcessOsoMode(mode, vbno, minSeqnoFromDCP, maxSeqnoFromDCP, numSeqnoFromDCP)
		close(syncCh)
		if isDone {
			tsTracker.HandleDoneSession(vbno, session)
		}
	case common.ConflictsDetected:
		info := event.Data.(conflictlog.CLogReqT)
		vbno := info.Vbno
		seqno := info.Seqno
		shouldProcessAsOSO, session := tsTracker.shouldProcessAsOso(vbno, seqno)
		if shouldProcessAsOSO {
			done := session.markCLogReqSeqno(vbno, seqno)
			if done {
				tsTracker.HandleDoneSession(vbno, session)
			}
		} else {
			tsTracker.appendCLogReq(vbno, seqno)
		}
		syncCh := event.OtherInfos.(chan bool)
		syncCh <- true
	case common.CLogWriteStatus:
		info := event.Data.(conflictlog.CLogRespT)
		if !info.ThroughSeqnoRelated {
			// not a through seqno event.
			return nil
		}
		vbno := info.Vbno
		seqno := info.Seqno
		shouldProcessAsOSO, session := tsTracker.shouldProcessAsOso(vbno, seqno)
		if shouldProcessAsOSO {
			done := session.markCLogResSeqno(vbno, seqno)
			if done {
				tsTracker.HandleDoneSession(vbno, session)
			}
		} else {
			tsTracker.appendCLogRes(vbno, seqno)
		}
	default:
		tsTracker.logger.Warnf("Incorrect event type, %v, received by %v", event.EventType, tsTracker.id)
	}
	return nil
}

// If the targetVbno is non-nil, it will be used for target manifest ID tracking as it means
// the caller is confident about the target manifest ID
func (tsTracker *ThroughSeqnoTrackerSvc) addSentSeqnoAndManifestId(vbno uint16, seqno uint64, manifestId uint64, tgtVbno *uint16) {
	tsTracker.addSentSeqno(vbno, seqno)
	var vbnoToUse *uint16 = &vbno
	if tgtVbno != nil {
		vbnoToUse = tgtVbno
	}
	tsTracker.addManifestId(*vbnoToUse, seqno, manifestId)
}

func (tsTracker *ThroughSeqnoTrackerSvc) handleBackfillStreamBypass(vbno uint16) {
	tsTracker.vbBackfillHelperDoneMap[vbno].SetSeqno(VBSeqnoBypassed)
	streamBypassCnt := tsTracker.backfillStreamBypassCnt.Add(1)
	// if common.StreamingBypassed is raised for all the responsible VBs, spin the bgScanForThroughSeqno
	if streamBypassCnt == uint32(len(tsTracker.vbBackfillHelperDoneMap)) {
		if atomic.CompareAndSwapUint32(&tsTracker.vbBackfillHelperActive, 0, 1) {
			go tsTracker.bgScanForThroughSeqno()
		}
	}
}

func (tsTracker *ThroughSeqnoTrackerSvc) handleBackfillStreamEnd(vbno uint16, endSeqno uint64) {
	if atomic.CompareAndSwapUint32(&tsTracker.vbBackfillHelperActive, 0, 1) {
		go tsTracker.bgScanForThroughSeqno()
	}

	tsTracker.vbBackfillLastDCPSeqnoMap[vbno].SetSeqno(endSeqno)
	tsTracker.vbBackfillHelperDoneMap[vbno].SetSeqno(VBSeqnoStreamEndReceived)
}

func (tsTracker *ThroughSeqnoTrackerSvc) bgScanForThroughSeqno() {
	// Wait for max time it takes for xmem to finish sending a mutation
	totalScanTime := time.Duration(base.XmemMaxRetry) * base.XmemMaxRetryInterval
	progressCheckTime := base.XmemMaxRetryInterval // a multiple of totalScanTime
	killTimer := time.NewTimer(totalScanTime)
	periodicScanner := time.NewTicker(base.ThroughSeqnoBgScannerFreq)
	logPrinter := time.NewTicker(base.ThroughSeqnoBgScannerLogFreq)
	progressChecker := time.NewTicker(progressCheckTime)

	defer killTimer.Stop()
	defer periodicScanner.Stop()
	defer logPrinter.Stop()
	defer progressChecker.Stop()

	var numProgressChecks, numProgressions, totalDonePrev int

	for {
		select {
		case <-killTimer.C:
			tsTracker.Logger().Infof("%v bg scanner: In the last %v, progress was observed %v times out of %v checks",
				tsTracker.id, totalScanTime, numProgressions, numProgressChecks)
			if numProgressions > 0 {
				// with the default settings, this may cause a worst case wait time upto 30mins more if the progress
				// is stuck after a subset of progress is observed.
				numProgressions = 0
				numProgressChecks = 0
			} else {
				total, totalDone, waitingOnVbs := tsTracker.bgScanForDoneVBs()
				if total == totalDone {
					// Last minute catch
					tsTracker.logger.Infof("%v Background check task finished", tsTracker.Id())
					return
				}

				// With the default settings, error out when there is no progress whatsoever in the last 25mins.
				err := fmt.Errorf("ThroughSeqno %v background waiting backfill streams to all be replicated timed out. Expected count: %v actual count %v. VBs waiting on: %v",
					tsTracker.Id(), total, totalDone, waitingOnVbs)
				tsTracker.handleGeneralError(err)
				return
			}
		case <-progressChecker.C:
			_, totalDone, _ := tsTracker.bgScanForDoneVBs()
			numProgressChecks++
			if totalDone > totalDonePrev {
				numProgressions++
				totalDonePrev = totalDone
			}
		case <-logPrinter.C:
			total, totalDone, waitingOn := tsTracker.bgScanForDoneVBs()
			tsTracker.Logger().Infof("%v bg scanner: total %v totalDone %v waitingOnVBs %v", tsTracker.id, total, totalDone, waitingOn)
		case <-periodicScanner.C:
			var doneLists []uint16
			if !tsTracker.isPipelineRunning() {
				tsTracker.Logger().Infof("%v bg scanner: pipeline no longer running, stopping", tsTracker.id)
				return
			}

			throughSeqnos := tsTracker.GetThroughSeqnos()

			for vbno, lastSeenSeqnoLocked := range tsTracker.vbBackfillLastDCPSeqnoMap {
				currentState := tsTracker.vbBackfillHelperDoneMap[vbno].GetSeqno()
				// This part only handles either streamEndReceived or Bypassed
				if currentState != VBSeqnoStreamEndReceived && currentState != VBSeqnoBypassed {
					// StreamEnd hasn't been received yet OR
					// Already marked done
					// so don't check the Seqnos
					continue
				}

				// Otherwise, at this stage it means seqnoEnd has been sent by the producer
				// Need to ensure throughSeqno is caught up with what DCP nozzle last saw before streamEnd
				// Or a special case of bypassed
				lastSeenSeqno := lastSeenSeqnoLocked.GetSeqno()
				vbThroughSeqno := throughSeqnos[vbno]
				if currentState == VBSeqnoBypassed || vbThroughSeqno >= lastSeenSeqno {
					tsTracker.vbBackfillHelperDoneMap[vbno].SetSeqno(VBSeqnoAllProcessed)
					doneLists = append(doneLists, vbno)
				}
			}

			if len(doneLists) > 0 {
				for _, vbno := range doneLists {
					go tsTracker.RaiseEvent(common.NewEvent(common.LastSeenSeqnoDoneProcessed, vbno, tsTracker, nil, nil))
				}
			}

			total, totalDone, _ := tsTracker.bgScanForDoneVBs()
			// Ensure that all the VBs have been marked done
			if totalDone == total {
				tsTracker.logger.Infof("%v Background check task finished", tsTracker.Id())
				return
			}
		}
	}
}

func (tsTracker *ThroughSeqnoTrackerSvc) bgScanForDoneVBs() (total, totalDone int, waitingOnVbs []uint16) {
	total = len(tsTracker.vbBackfillHelperDoneMap)
	for vbno, seqnoWithLock := range tsTracker.vbBackfillHelperDoneMap {
		if seqnoWithLock.GetSeqno() == VBSeqnoAllProcessed {
			totalDone++
		} else {
			waitingOnVbs = append(waitingOnVbs, vbno)
		}
	}
	return
}

func (tsTracker *ThroughSeqnoTrackerSvc) markSystemEvent(uprEvent *mcc.UprEvent) {
	if uprEvent == nil {
		return
	}
	seqno := uprEvent.Seqno
	vbno := uprEvent.VBucket

	manifestId, _ := uprEvent.GetManifestId()
	tsTracker.addSystemSeqno(vbno, seqno, manifestId)
}

func (tsTracker *ThroughSeqnoTrackerSvc) addSentSeqno(vbno uint16, sent_seqno uint64) {
	ok := tsTracker.validateVbno(vbno, "addSentSeqno")
	if !ok {
		return
	}

	tsTracker.vb_sent_seqno_list_map[vbno].AppendSeqno(sent_seqno)
}

func (tsTracker *ThroughSeqnoTrackerSvc) addFilteredSeqno(vbno uint16, filtered_seqno uint64) {
	ok := tsTracker.validateVbno(vbno, "addFilteredSeqno")
	if !ok {
		return
	}

	tsTracker.vb_filtered_seqno_list_map[vbno].AppendSeqno(filtered_seqno)
}

func (tsTracker *ThroughSeqnoTrackerSvc) addIgnoredSeqno(vbno uint16, ignoredSeqno uint64) {
	ok := tsTracker.validateVbno(vbno, "addIgnoredSeqno")
	if !ok {
		return
	}

	tsTracker.vbIgnoredSeqnoListMap[vbno].AppendSeqno(ignoredSeqno)
}

func (tsTracker *ThroughSeqnoTrackerSvc) addFailedCRSeqno(vbno uint16, failed_cr_seqno uint64) {
	ok := tsTracker.validateVbno(vbno, "addFailedCRSeqno")
	if !ok {
		return
	}

	tsTracker.vb_failed_cr_seqno_list_map[vbno].AppendSeqno(failed_cr_seqno)
}

func (tsTracker *ThroughSeqnoTrackerSvc) processGapSeqnos(vbno uint16, current_seqno uint64) {
	ok := tsTracker.validateVbno(vbno, "processGapSeqnos")
	if !ok {
		return
	}

	last_seen_seqno_obj := tsTracker.vb_last_seen_seqno_map[vbno]

	last_seen_seqno_obj.Lock()
	defer last_seen_seqno_obj.Unlock()
	last_seen_seqno := last_seen_seqno_obj.GetSeqnoWithoutLock()
	if last_seen_seqno == 0 {
		// this covers the case where the replication resumes from checkpoint docs
		last_seen_seqno = tsTracker.through_seqno_map[vbno].GetSeqno()
	}
	last_seen_seqno_obj.SetSeqnoWithoutLock(current_seqno)

	// Only append gap seqno if current_seqno is greater than 0
	// Otherwise, this will introduce a humongous gap (0 to MaxUint64) that can lead to
	// eating up CPU cycles, and also lead to never-ending backfill pipelines
	if current_seqno > 0 && last_seen_seqno < current_seqno-1 {
		// If the current sequence number is not consecutive, then this means we have hit a gap. Store it in gap list.
		tsTracker.vb_gap_seqno_list_map[vbno].appendSeqnos(last_seen_seqno+1, current_seqno-1)
	}
}

func (tsTracker *ThroughSeqnoTrackerSvc) addSystemSeqno(vbno uint16, systemEventSeqno, manifestId uint64) {
	ok := tsTracker.validateVbno(vbno, "addSystemSeqno")
	if !ok {
		return
	}

	tsTracker.vbSystemEventsSeqnoListMap[vbno].appendSeqnos(systemEventSeqno, manifestId)
}

func (tsTracker *ThroughSeqnoTrackerSvc) setSourceSeqnoManifestIdPair(vbno uint16, seqno, manifestId uint64) {
	ok := tsTracker.validateVbno(vbno, "setSourceSeqnoManifestIdPair")
	if !ok {
		return
	}

	tsTracker.vbSystemEventsSeqnoListMap[vbno].lock.Lock()
	defer tsTracker.vbSystemEventsSeqnoListMap[vbno].lock.Unlock()
	tsTracker.vbSystemEventsSeqnoListMap[vbno].seqno_list_1 = []uint64{seqno}
	tsTracker.vbSystemEventsSeqnoListMap[vbno].seqno_list_2 = []uint64{manifestId}
}

func (tsTracker *ThroughSeqnoTrackerSvc) setTargetSeqnoManifestIdPair(vbno uint16, seqno, manifestId uint64) {
	ok := tsTracker.validateVbno(vbno, "setTargetSeqnoManifestIdPair")
	if !ok {
		return
	}

	tsTracker.vbTgtSeqnoManifestMap[vbno].lock.Lock()
	defer tsTracker.vbTgtSeqnoManifestMap[vbno].lock.Unlock()
	tsTracker.vbTgtSeqnoManifestMap[vbno].seqno_list_1 = []uint64{seqno}
	tsTracker.vbTgtSeqnoManifestMap[vbno].seqno_list_2 = []uint64{manifestId}
}

func (tsTracker *ThroughSeqnoTrackerSvc) addManifestId(vbno uint16, seqno, manifestId uint64) {
	ok := tsTracker.validateVbno(vbno, "addManifestId")
	if !ok {
		return
	}

	tsTracker.vbTgtSeqnoManifestMap.UpdateOrAppendSeqnoManifest(vbno, seqno, manifestId)
}

func (tsTracker *ThroughSeqnoTrackerSvc) truncateSeqnoLists(vbno uint16, through_seqno uint64) {
	tsTracker.vb_sent_seqno_list_map[vbno].TruncateSeqnos(through_seqno)
	tsTracker.vb_filtered_seqno_list_map[vbno].TruncateSeqnos(through_seqno)
	tsTracker.vb_failed_cr_seqno_list_map[vbno].TruncateSeqnos(through_seqno)
	tsTracker.vb_gap_seqno_list_map[vbno].truncateSeqnos(through_seqno)
	tsTracker.vbSystemEventsSeqnoListMap[vbno].truncateSeqno1Floor(through_seqno)
	tsTracker.vbIgnoredSeqnoListMap[vbno].TruncateSeqnos(through_seqno)
	tsTracker.vbTgtSeqnoManifestMap[vbno].truncateSeqno1Floor(through_seqno)
	tsTracker.vbClonedTracker[vbno].truncateSeqno1Floor(through_seqno)
	tsTracker.cLogTrackerVbMap[vbno].truncateSeqnos(through_seqno)
}

/**
 * It's possible that even though the last sequence number actually sent is N, the next few numbers
 * are actually handled and decided not to be sent. For example, N+1 may have been decided to be filtered
 * out, and N+2 has failed CR, and N+3 to N+5 has been deduplicated as part of the DCP snapshot and has
 * been accounted for in the gap seq number dual-list. In this example, the through seqno would be N+5.
 *
 * As a service, its job is to determine the "highest" number, and it could be either
 * actually sent, or filtered, or some other "evented", such that any transaction below this number is
 * considered "synchronized" between the source and destination.
 *
 * This method, given a vbucket number, returns that number.
 *
 * Note that the method is called "GetThroughSeqno" but also note that a through seqno may not necessarily
 * mean that a mutation is physically sent over to the target. Do not confuse this with through_seqno_map,
 * which only keeps track of physically sent numbers.
 */
func (tsTracker *ThroughSeqnoTrackerSvc) GetThroughSeqno(vbno uint16) uint64 {
	ok := tsTracker.validateVbno(vbno, "GetThroughSeqno")
	if !ok {
		return 0
	}

	// lock through_seqno_map[vbno] throughout the computation to ensure that
	// two GetThroughSeqno() routines won't interleave, which would cause issues
	// since we truncate seqno lists in accordance with through_seqno
	through_seqno_obj := tsTracker.through_seqno_map[vbno]
	through_seqno_obj.Lock()
	defer through_seqno_obj.Unlock()

	var osoModeStartingSeqno uint64
	var osoModeActive bool
	if atomic.LoadUint32(&tsTracker.osoModeActive) == 1 {
		// if osoModeActive is false, it means that a session is not currently in progress
		// The rest of the code should ignore osoModeStartingSeqno if that's the case
		osoModeStartingSeqno, osoModeActive = tsTracker.vbOsoModeSessionDCPTracker.GetStartingSeqno(vbno)
	}
	last_through_seqno := through_seqno_obj.GetSeqnoWithoutLock()
	sent_seqno_list := tsTracker.vb_sent_seqno_list_map[vbno].GetSortedSeqnoList(true)
	max_sent_seqno := maxSeqno(sent_seqno_list, osoModeActive, osoModeStartingSeqno)
	filtered_seqno_list := tsTracker.vb_filtered_seqno_list_map[vbno].GetSortedSeqnoList(false)
	max_filtered_seqno := maxSeqno(filtered_seqno_list, osoModeActive, osoModeStartingSeqno)
	failed_cr_seqno_list := tsTracker.vb_failed_cr_seqno_list_map[vbno].GetSortedSeqnoList(true)
	max_failed_cr_seqno := maxSeqno(failed_cr_seqno_list, osoModeActive, osoModeStartingSeqno)
	gap_seqno_list_1, gap_seqno_list_2 := tsTracker.vb_gap_seqno_list_map[vbno].getSortedSeqnoLists(vbno, "gap")
	max_end_gap_seqno := maxSeqno(gap_seqno_list_2, osoModeActive, osoModeStartingSeqno)
	systemEventSeqnoList, _ := tsTracker.vbSystemEventsSeqnoListMap[vbno].getSortedSeqnoLists(vbno, "sys")
	maxSystemEventSeqno := maxSeqno(systemEventSeqnoList, osoModeActive, osoModeStartingSeqno)
	ignoredSeqnoList := tsTracker.vbIgnoredSeqnoListMap[vbno].GetSortedSeqnoList(true)
	maxIgnoredSeqno := maxSeqno(ignoredSeqnoList, osoModeActive, osoModeStartingSeqno)

	// Goal of algorithm:
	// Find the right through_seqno for stats and checkpointing, with the constraint that through_seqno cannot be
	// a gap seqno, since we do not want to use gap seqnos for checkpointing

	// Starting from last_through_seqno, find the largest N such that last_through_seqno+1, last_through_seqno+2,
	// .., last_through_seqno+N all exist in filtered_seqno_list, failed_cr_seqno_list, sent_seqno_list,
	// systemEventSeqnoList, ignoreSeqnoList, or a gap range, and that last_through_seqno+N itself is not in a gap range.
	// Return last_through_seqno+N as the current through_seqno. Note that N could be 0.

	// If conflict logger exists, processCLoggerRequests is called on the above computed through_seqno to make sure conflicts are
	// all processed until through_seqno. This is to ensure that all conflicts detected are not dropped/lost on pipeline restarts.

	through_seqno := last_through_seqno

	iter_seqno := last_through_seqno
	var last_sent_index int = -1
	var last_filtered_index int = -1
	var last_failed_cr_index int = -1
	var found_seqno_type int = -1
	var lastSysEventIndex int = -1
	var lastIgnoredIndex int = -1

	const (
		SeqnoTypeSent     int = iota
		SeqnoTypeFiltered int = iota
		SeqnoTypeFailedCR int = iota
		SeqnoTypeSysEvent int = iota
		SeqnoTypeIgnored  int = iota
	)

	for {
		iter_seqno++
		if iter_seqno <= max_sent_seqno {
			var sent_index int
			var sent_found bool
			if osoModeActive {
				sent_index, sent_found = base.SearchUint64ListUnsorted(sent_seqno_list, iter_seqno)
			} else {
				sent_index, sent_found = base.SearchUint64List(sent_seqno_list, iter_seqno)
			}
			if sent_found {
				last_sent_index = sent_index
				found_seqno_type = SeqnoTypeSent
				// A sequence number, if exists in one of the list, will not be duplicated in the other lists
				continue
			}
		}

		if iter_seqno <= max_filtered_seqno {
			var filtered_index int
			var filtered_found bool
			if osoModeActive {
				filtered_index, filtered_found = base.SearchUint64ListUnsorted(filtered_seqno_list, iter_seqno)
			} else {
				filtered_index, filtered_found = base.SearchUint64List(filtered_seqno_list, iter_seqno)
			}
			if filtered_found {
				last_filtered_index = filtered_index
				found_seqno_type = SeqnoTypeFiltered
				// A sequence number, if exists in one of the list, will not be duplicated in the other lists
				continue
			}
		}

		if iter_seqno <= max_failed_cr_seqno {
			var failed_cr_index int
			var failed_cr_found bool
			if osoModeActive {
				failed_cr_index, failed_cr_found = base.SearchUint64ListUnsorted(failed_cr_seqno_list, iter_seqno)
			} else {
				failed_cr_index, failed_cr_found = base.SearchUint64List(failed_cr_seqno_list, iter_seqno)
			}
			if failed_cr_found {
				last_failed_cr_index = failed_cr_index
				found_seqno_type = SeqnoTypeFailedCR
				// A sequence number, if exists in one of the list, will not be duplicated in the other lists
				continue
			}
		}

		if iter_seqno <= max_end_gap_seqno {
			gap_found, end_gap_seqno := isSeqnoGapSeqno(gap_seqno_list_1, gap_seqno_list_2, iter_seqno)
			if gap_found {
				// if iter_seqno is in a gap range, skip to the end of the gap range
				iter_seqno = end_gap_seqno
				continue
			}
		}

		if iter_seqno <= maxSystemEventSeqno {
			var systemEventIdx int
			var systemEventFound bool
			if osoModeActive {
				systemEventIdx, systemEventFound = base.SearchUint64ListUnsorted(systemEventSeqnoList, iter_seqno)
			} else {
				systemEventIdx, systemEventFound = base.SearchUint64List(systemEventSeqnoList, iter_seqno)
			}
			if systemEventFound {
				lastSysEventIndex = systemEventIdx
				found_seqno_type = SeqnoTypeSysEvent
				continue
			}
		}

		if iter_seqno <= maxIgnoredSeqno {
			var ignoredIdx int
			var ignoredFound bool
			if osoModeActive {
				ignoredIdx, ignoredFound = base.SearchUint64ListUnsorted(ignoredSeqnoList, iter_seqno)
			} else {
				ignoredIdx, ignoredFound = base.SearchUint64List(ignoredSeqnoList, iter_seqno)
			}
			if ignoredFound {
				lastIgnoredIndex = ignoredIdx
				found_seqno_type = SeqnoTypeIgnored
				// A sequence number, if exists in one of the list, will not be duplicated in the other lists
				continue
			}
		}

		// stop if cannot find seqno in any of the lists
		break
	}

	if last_sent_index >= 0 || last_filtered_index >= 0 || last_failed_cr_index >= 0 || lastSysEventIndex >= 0 || lastIgnoredIndex >= 0 {
		switch found_seqno_type {
		case SeqnoTypeSent:
			through_seqno = sent_seqno_list[last_sent_index]
		case SeqnoTypeFiltered:
			through_seqno = filtered_seqno_list[last_filtered_index]
		case SeqnoTypeFailedCR:
			through_seqno = failed_cr_seqno_list[last_failed_cr_index]
		case SeqnoTypeSysEvent:
			through_seqno = systemEventSeqnoList[lastSysEventIndex]
		case SeqnoTypeIgnored:
			through_seqno = ignoredSeqnoList[lastIgnoredIndex]
		default:
			panic(fmt.Sprintf("unexpected found_seqno_type, %v", found_seqno_type))
		}
	}

	cleanupFunc := func(throughSeqno uint64) {
		through_seqno_obj.SetSeqnoWithoutLock(throughSeqno)

		// truncate no longer needed entries from seqno lists to reduce memory/cpu overhead for future computations
		go tsTracker.truncateSeqnoLists(vbno, throughSeqno)
	}

	return tsTracker.processCLoggerRequests(vbno, through_seqno, last_through_seqno,
		sent_seqno_list, failed_cr_seqno_list, filtered_seqno_list, systemEventSeqnoList, ignoredSeqnoList, cleanupFunc)
}

// Given the newly computed throughSeqno (i.e. newThroughSeqno), if conflict logging is in the mix, this routine will find and return the
// maximum seqno less than or equal to newThroughSeqno until which all conflict logging requests are completely processed in that moment.
//
// The returned seqno will be present in sentSeqnoList U failedCRSeqnoList U filteredSeqnoList U systemEventSeqnoList[1:] U ignoredSeqnoList.
// Note: systemEventSeqnoList[1:] is used instead of systemEventSeqnoList because systemEventSeqnoList uses truncateSeqno1Floor.
//
// Pre-requisites:
// cLogRes is a subset of cLogReq.
//
// Algorithm:
// (1) Find the smallest seqno for which conflict logging was requested but not done.
// This is efficiently done by sorting cLogReq and cLogRes and finding the minimum index i such that cLogReq[i] != cLogRes[i].
// (2) Find the greatest seqno in sentSeqnoList U failedCRSeqnoList U filteredSeqnoList U systemEventSeqnoList[1:] U ignoredSeqnoList,
// such that it is less than the seqno found in (1). This is efficiently done by computing one seqno each (candidates) in each of the above mentioned
// lists such that each one of them are less than the seqno found in (1). The maximum of all these candidate seqno will be the final throughSeq number.
//
// If we are not able to find a new higher seqno than the last iteration, lastThroughSeqno will be returned.
//
//	sentSeqnoList, failedCRSeqnoList, filteredSeqnoList, systemEventSeqnoList, ignoredSeqnoList are assumed to be sorted.
func (tsTracker *ThroughSeqnoTrackerSvc) processCLoggerRequests(vbno uint16, newThroughSeqno, lastThroughSeqno uint64, sentSeqnoList, failedCRSeqnoList, filteredSeqnoList, systemEventSeqnoList, ignoredSeqnoList []uint64, cleanup func(uint64)) uint64 {
	if !tsTracker.cLogExists {
		cleanup(newThroughSeqno)
		return newThroughSeqno
	}

	// cLogReq donates the conflict logging requests generated by xmem nozzle.
	// cLogRes donates the conflict logging requests that are completely done by conflict logger.
	cLogReq, cLogRes := tsTracker.cLogTrackerVbMap[vbno].sortAndGetSeqnoLists()

	if tsTracker.logger.GetLogLevel() >= log.LogLevelTrace {
		tsTracker.logger.Tracef("%v vb %v, req=%v, res=%v, sent=%v, failed=%v, system=%v, ignored=%v, filtered=%v, new=%v, old=%v",
			tsTracker.rep_id, vbno, cLogReq, cLogRes, sentSeqnoList, failedCRSeqnoList, systemEventSeqnoList, ignoredSeqnoList, filteredSeqnoList, newThroughSeqno, lastThroughSeqno)
	}

	if len(cLogRes) > len(cLogReq) {
		// This breaks our pre-requisite as described above and should not happen.
		// Have debug info and donot run the algorithm i.e. behave as if there was no conflict logger.
		// When this code path is hit, there is a risk of dropped conflicts when replication restarts.
		tsTracker.logger.Debugf("%v faulty cLogTracker vb %v, req=%v, res=%v, sent=%v, failed=%v, system=%v, ignored=%v, filtered=%v, new=%v, old=%v",
			tsTracker.rep_id, vbno, cLogReq, cLogRes, sentSeqnoList, failedCRSeqnoList, systemEventSeqnoList, ignoredSeqnoList, filteredSeqnoList, newThroughSeqno, lastThroughSeqno)
		tsTracker.faultyCLogTrackerCnt.Add(1)
		cleanup(newThroughSeqno)
		return newThroughSeqno
	}

	if len(cLogRes) == len(cLogReq) {
		// could be either because of:
		// 1. conflict logging is not enabled. OR
		// 2. all conflict logging requests are done.
		cleanup(newThroughSeqno)
		return newThroughSeqno
	}

	// cLogReqNotDoneIdx represents the minimum index of the seqno for which conflict logging
	// was requested, but it has not been processed yet by the conflict logger.
	cLogReqNotDoneIdx := len(cLogRes)
	for cLogReqIdx, cLogResIdx := 0, 0; cLogResIdx < len(cLogRes); cLogReqIdx, cLogResIdx = cLogReqIdx+1, cLogResIdx+1 {
		if cLogRes[cLogResIdx] != cLogReq[cLogReqIdx] {
			cLogReqNotDoneIdx = cLogReqIdx
			break
		}
	}

	// all conflict logging requests were completed until newThroughSeqno.
	if newThroughSeqno < cLogReq[cLogReqNotDoneIdx] {
		cleanup(newThroughSeqno)
		return newThroughSeqno
	}

	// Find the maximum element less than or equal to cLog req that is not done yet in each seqno list.
	// Each one of these are candidates for the final through seqno.
	var ok1, ok2, ok3, ok4, ok5 bool
	var max1, max2, max3, max4, max5 uint64

	max1, ok1 = base.FindLessThan(sentSeqnoList, cLogReq[cLogReqNotDoneIdx])
	max2, ok2 = base.FindLessThan(failedCRSeqnoList, cLogReq[cLogReqNotDoneIdx])
	max3, ok3 = base.FindLessThan(filteredSeqnoList, cLogReq[cLogReqNotDoneIdx])
	max4, ok4 = base.FindLessThan(ignoredSeqnoList, cLogReq[cLogReqNotDoneIdx])
	if len(systemEventSeqnoList) > 0 {
		max5, ok5 = base.FindLessThan(systemEventSeqnoList[1:], cLogReq[cLogReqNotDoneIdx])
	}

	if !ok1 && !ok2 && !ok3 && !ok4 && !ok5 {
		// We did not find any new candidates to be returned.
		// Return the last computed through seqno.
		cleanup(lastThroughSeqno)
		return lastThroughSeqno
	}

	// Find the overall maximum of all the candidates.
	finalThroughSeqno := max(max1, max2, max3, max4, max5)
	if finalThroughSeqno < lastThroughSeqno {
		// This is a defensive check to ensure we have a monotonically increasing throughSeqno.
		// We treat this case similar to if there was no conflict logger with the risk of losing conflicts
		// when there is a replication restart.
		tsTracker.logger.Debugf("%v throughSeqno rolled back for vb %v, req=%v, res=%v, sent=%v, failed=%v, system=%v, ignored=%v, filtered=%v, new=%v, old=%v, final=%v",
			tsTracker.rep_id, vbno, cLogReq, cLogRes, sentSeqnoList, failedCRSeqnoList, systemEventSeqnoList, ignoredSeqnoList, filteredSeqnoList, newThroughSeqno, lastThroughSeqno, finalThroughSeqno)
		tsTracker.nonMonotonicThroughSeqnoCnt.Add(1)
		cleanup(newThroughSeqno)
		return newThroughSeqno
	}

	cleanup(finalThroughSeqno)
	return finalThroughSeqno
}

// if seqno is a gap seqno, return (true, seqno of end of gap range)
// otherwise, return (false, 0)
func isSeqnoGapSeqno(gap_seqno_list_1, gap_seqno_list_2 []uint64, seqno uint64) (bool, uint64) {
	if len(gap_seqno_list_1) == 0 {
		return false, 0
	}
	index, is_start_gap_seqno := base.SearchUint64List(gap_seqno_list_1, seqno)
	if is_start_gap_seqno {
		return true, gap_seqno_list_2[index]
	}

	// gap_range_index is the index of the largest start_gap_seqno that is smaller than seqno
	gap_range_index := index - 1
	if gap_range_index < 0 {
		return false, 0
	}

	if gap_seqno_list_2[gap_range_index] >= seqno {
		// seqno is between gap_seqno_list_1[gap_range_index] and gap_seqno_list_2[gap_range_index]
		// and hence is a gap seqno
		return true, gap_seqno_list_2[gap_range_index]
	}

	return false, 0

}

func (tsTracker *ThroughSeqnoTrackerSvc) GetThroughSeqnos() map[uint16]uint64 {
	start_time := time.Now()
	defer func() {
		expectedTime := base.ThresholdForThroughSeqnoComputation
		if tsTracker.cLogExists {
			expectedTime = 2 * expectedTime
		}
		time_taken := time.Since(start_time)
		if time_taken > expectedTime {
			timeInMs := uint64(time_taken.Milliseconds())
			tsTracker.logger.Debugf("%v GetThroughSeqnos completed after %v", tsTracker.id, time_taken)
			tsTracker.slowThroughSeqnoCalls.count.Add(1)
			tsTracker.slowThroughSeqnoCalls.sum.Add(timeInMs)
			minNow := tsTracker.slowThroughSeqnoCalls.min.Load()
			maxNow := tsTracker.slowThroughSeqnoCalls.max.Load()
			tsTracker.slowThroughSeqnoCalls.min.Store(min(timeInMs, minNow))
			tsTracker.slowThroughSeqnoCalls.max.Store(max(timeInMs, maxNow))
		}
	}()

	result_map := make(map[uint16]uint64)

	listOfVbs := tsTracker.getVbList()
	vb_per_worker := 20
	start_index := 0

	wait_grp := &sync.WaitGroup{}
	executor_id := 0
	result_map_map := make(map[int]map[uint16]uint64)
	for {
		end_index := start_index + vb_per_worker
		if end_index > len(listOfVbs) {
			end_index = len(listOfVbs)
		}
		vbs_for_executor := listOfVbs[start_index:end_index]
		result_map_map[executor_id] = make(map[uint16]uint64)
		wait_grp.Add(1)
		go tsTracker.getThroughSeqnos(executor_id, vbs_for_executor, result_map_map[executor_id], wait_grp)
		start_index = end_index
		executor_id++
		if start_index >= len(listOfVbs) {
			break
		}
	}

	wait_grp.Wait()

	for _, exec_result_map := range result_map_map {
		for vbno, seqno := range exec_result_map {
			result_map[vbno] = seqno
		}
	}

	return result_map
}

func (tsTracker *ThroughSeqnoTrackerSvc) GetThroughSeqnosAndManifestIds() (throughSeqnos, srcManifestIds, tgtManifestIds map[uint16]uint64) {
	throughSeqnos = tsTracker.GetThroughSeqnos()
	srcManifestIds = make(map[uint16]uint64)
	tgtManifestIds = make(map[uint16]uint64)

	for vbno, seqno := range throughSeqnos {
		manifestId, err := tsTracker.vbTgtSeqnoManifestMap.GetManifestId(vbno, seqno)
		if err != nil {
			tsTracker.logger.Warnf(err.Error())
		} else {
			tgtManifestIds[vbno] = manifestId
		}

		manifestId, err = tsTracker.vbSystemEventsSeqnoListMap.GetManifestId(vbno, seqno)
		if err != nil {
			tsTracker.logger.Warnf(err.Error())
		} else {
			srcManifestIds[vbno] = manifestId
		}
	}

	// Variable VB mode will has have to go through additional individual target VB and set the highest manifest ID
	for _, tgtVb := range tsTracker.variableTgtVbs {
		if _, exists := tsTracker.vb_map[tgtVb]; exists {
			// This is already captured as part of the source part above
			continue
		}

		// For target manifest ID in variable VB mode, we don't care about the throughSeqno - just
		// return the max manifestID that the target VB has seen
		manifestId, err := tsTracker.vbTgtSeqnoManifestMap.GetManifestId(tgtVb, math.MaxUint64)
		if err != nil {
			tsTracker.logger.Warnf(err.Error())
		} else {
			tgtManifestIds[tgtVb] = manifestId
		}
	}
	return
}

func (tsTracker *ThroughSeqnoTrackerSvc) getThroughSeqnos(executor_id int, listOfVbs []uint16, result_map map[uint16]uint64, wait_grp *sync.WaitGroup) {
	tsTracker.logger.Tracef("%v getThroughSeqnos executor %v is working on vbuckets %v", tsTracker.id, executor_id, listOfVbs)
	defer wait_grp.Done()

	for _, vbno := range listOfVbs {
		result_map[vbno] = tsTracker.GetThroughSeqno(vbno)
	}
}

func (tsTracker *ThroughSeqnoTrackerSvc) SetStartSeqno(vbno uint16, seqno uint64, manifestIds base.CollectionsManifestIdsTimestamp) {
	ok := tsTracker.validateVbno(vbno, "setStartSeqno")
	if !ok {
		return
	}

	obj := tsTracker.through_seqno_map[vbno]
	obj.SetSeqno(seqno)
	// When Pipeline starts from a checkpoint (SetVBTimeStamp) or rollback occurs (UpdateVBTimeStamp)
	// the throughSeqno is set to "seqno"
	// The manifestIDs also need to be set accordingly
	// Since it is resuming from a ckpt OR a rollback, any previously known information is no longer needed
	// because the source + target manifest pairs are only used as part of GetThroughSeqnos
	// and after the above Set call, the "seqno" will be the new starting point
	tsTracker.setSourceSeqnoManifestIdPair(vbno, seqno, manifestIds.SourceManifestId)

	if len(manifestIds.GlobalTargetManifestIds) > 0 {
		// Note that for rollback scenario, there's no way right now to really "rollback" the target manifest ID for
		// a given target VB given that all the target VBs are all intertwined for every source VB
		// For global timestamp, we cannot use "setTargetSeqnoAndManifestPair" because it is possible that while
		// one source VB (i.e. vb 1) experiences a rollback, and need to reset the starting timestamp, other source VBs
		// could continue to run and it'll be incorrect to "set" a single entry in the tgtSeqnoManifestMap given
		// all the VBs are mixed together
		for tgtVbno, gtsManifest := range manifestIds.GlobalTargetManifestIds {
			err := tsTracker.vbTgtSeqnoManifestMap.UpdateOrAppendSeqnoManifest(tgtVbno, seqno, gtsManifest)
			if err != nil {
				tsTracker.logger.Warnf(err.Error())
			}
		}
	} else {
		tsTracker.setTargetSeqnoManifestIdPair(vbno, seqno, manifestIds.TargetManifestId)
	}
}

func (tsTracker *ThroughSeqnoTrackerSvc) validateVbno(vbno uint16, caller string) (ok bool) {
	var raiseErr bool
	if tsTracker.variableVBMode {
		_, tgtOk := tsTracker.variableVbTgtMap[vbno]
		_, srcOk := tsTracker.vb_map[vbno]
		if !tgtOk && !srcOk {
			raiseErr = true
		}
	} else {
		if _, ok := tsTracker.vb_map[vbno]; !ok {
			raiseErr = true
		}
	}

	if raiseErr {
		err := fmt.Errorf("method %v in tracker service for pipeline %v received invalid vbno. vbno=%v; valid vbnos=%v",
			caller, tsTracker.id, vbno, tsTracker.vb_map)
		tsTracker.handleGeneralError(err)
	}

	ok = !raiseErr
	return
}

// handle fatal error, raise error event to pipeline supervisor
func (tsTracker *ThroughSeqnoTrackerSvc) handleGeneralError(err error) {
	tsTracker.logger.Error(err.Error())
	tsTracker.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, tsTracker, nil, err))
}

func (tsTracker *ThroughSeqnoTrackerSvc) getVbList() []uint16 {
	vb_list := make([]uint16, 0)
	for vbno, _ := range tsTracker.vb_map {
		vb_list = append(vb_list, vbno)
	}
	return vb_list
}

func (tsTracker *ThroughSeqnoTrackerSvc) Id() string {
	return tsTracker.id
}

func (tsTracker *ThroughSeqnoTrackerSvc) isPipelineRunning() bool {
	if tsTracker.unitTesting {
		return true
	}

	return pipeline_utils.IsPipelineRunning(tsTracker.attachedPipeline.State())
}

func maxSeqno(seqno_list []uint64, osoModeActive bool, osoModeSeqno uint64) uint64 {
	length := len(seqno_list)
	if length > 0 {
		if osoModeActive {
			// Oso mode active means we pretend we haven't received anything higher than the "last seen seqno" that was stored
			// at the moment OSO mode was turned on
			for i := length - 1; i >= 0; i-- {
				if seqno_list[i] <= osoModeSeqno {
					return seqno_list[i]
				}
			}
			return 0
		} else {
			return seqno_list[length-1]
		}
	} else {
		return 0
	}
}

func (tsTracker *ThroughSeqnoTrackerSvc) PrintStatusSummary() {
	start_time := time.Now()
	var count, sum_sent, max_sent, sum_filtered, max_filtered, sum_failed_cr, max_failed_cr, sum_gap, max_gap,
		max_ignored, sum_ignored, max_system, sum_system, max_clog_req, sum_clog_req, max_clog_res, sum_clog_res,
		sum_oso_sessions, max_oso_sessions, sum_backfill_done int
	for _, vbno := range tsTracker.getVbList() {
		length_sent_seqno_list := tsTracker.vb_sent_seqno_list_map[vbno].GetLengthOfSeqnoList()
		length_filtered_seqno_list := tsTracker.vb_filtered_seqno_list_map[vbno].GetLengthOfSeqnoList()
		length_failed_cr_seqno_list := tsTracker.vb_failed_cr_seqno_list_map[vbno].GetLengthOfSeqnoList()
		length_gap_seqno_list := tsTracker.vb_gap_seqno_list_map[vbno].getLengthOfSeqnoLists()
		length_ignore_seqno_list := tsTracker.vbIgnoredSeqnoListMap[vbno].GetLengthOfSeqnoList()
		length_system_events_seqno_list := tsTracker.vbSystemEventsSeqnoListMap[vbno].getLengthOfSeqnoLists()
		length_CLog_req_seqno_list, length_CLog_res_seqno_list := tsTracker.cLogTrackerVbMap[vbno].getLengthOfSeparateLists()
		length_oso_sessions_list := tsTracker.vbOsoModeSessionDCPTracker[vbno].numSessions()
		backfill_helper := tsTracker.vbBackfillHelperDoneMap[vbno].GetSeqno()

		count++

		sum_sent += length_sent_seqno_list
		max_sent = max(max_sent, length_sent_seqno_list)

		sum_filtered += length_filtered_seqno_list
		max_filtered = max(max_filtered, length_filtered_seqno_list)

		sum_failed_cr += length_failed_cr_seqno_list
		max_failed_cr = max(max_failed_cr, length_failed_cr_seqno_list)

		sum_gap += length_gap_seqno_list
		max_gap = max(max_gap, length_gap_seqno_list)

		sum_ignored += length_ignore_seqno_list
		max_ignored = max(max_ignored, length_ignore_seqno_list)

		sum_system += length_system_events_seqno_list
		max_system = max(max_system, length_system_events_seqno_list)

		sum_oso_sessions += length_oso_sessions_list
		max_oso_sessions = max(max_oso_sessions, length_oso_sessions_list)

		if backfill_helper == VBSeqnoStreamEndReceived || backfill_helper == VBSeqnoBypassed {
			sum_backfill_done++
		}

		sum_clog_req += length_CLog_req_seqno_list
		max_clog_req = max(max_clog_req, length_CLog_req_seqno_list)

		sum_clog_res += length_CLog_res_seqno_list
		max_clog_res = max(max_clog_res, length_CLog_res_seqno_list)

		if tsTracker.logger.GetLogLevel() >= log.LogLevelDebug {
			// have some detailed debugging info if needed in crisis.
			through_seqno_obj := tsTracker.through_seqno_map[vbno]
			through_seqno_obj.Lock()
			last_through_seqno := through_seqno_obj.GetSeqnoWithoutLock()
			sent_seqno_list := tsTracker.vb_sent_seqno_list_map[vbno].GetSortedSeqnoList(false)
			filtered_seqno_list := tsTracker.vb_filtered_seqno_list_map[vbno].GetSortedSeqnoList(false)
			failed_cr_seqno_list := tsTracker.vb_failed_cr_seqno_list_map[vbno].GetSortedSeqnoList(false)
			gap_seqno_list_1, gap_seqno_list_2 := tsTracker.vb_gap_seqno_list_map[vbno].getSortedSeqnoLists(vbno, "gap")
			systemEventSeqnoList, _ := tsTracker.vbSystemEventsSeqnoListMap[vbno].getSortedSeqnoLists(vbno, "sys")
			ignoredSeqnoList := tsTracker.vbIgnoredSeqnoListMap[vbno].GetSortedSeqnoList(false)
			cLogReq, cLogRes := tsTracker.cLogTrackerVbMap[vbno].sortAndGetSeqnoLists()
			through_seqno_obj.Unlock()
			tsTracker.logger.Debugf("%v: lastThroughSeqno=%v, lists=[%v %v %v %v %v %v %v %v %v %v], osoSessions=%s",
				vbno, tsTracker.vbOsoModeSessionDCPTracker.Debug(vbno), last_through_seqno, sent_seqno_list, filtered_seqno_list,
				failed_cr_seqno_list, gap_seqno_list_1, gap_seqno_list_2, systemEventSeqnoList, ignoredSeqnoList, cLogReq, cLogRes)
		}
	}

	tsTracker.logger.Infof("time=%v vbs=%v lists[max avg]:{sent=[%v %v] filtered=[%v %v] failed_cr=[%v %v] gap=[%v %v] ignored=[%v %v] system=[%v %v]} oso:{on=%v max=%v avg=%v received=%v unusualEnd=%v} bacfillDone=%v slowGetTSMs:{max=%v min=%v sum=%v count=%v} cLogOn=%v cLogLists[max avg]:{req=[%v %v] res=[%v %v]} nonMonoTS=%v faultyCLog=%v",
		time.Since(start_time), count, max_sent, sum_sent/count, max_filtered, sum_filtered/count, max_failed_cr, sum_failed_cr/count,
		max_gap, sum_gap/count, max_ignored, sum_ignored/count, max_system, sum_system/count, atomic.LoadUint32(&tsTracker.osoModeActive) == 1,
		max_oso_sessions, sum_oso_sessions/count, atomic.LoadUint64(&tsTracker.osoCntReceivedFromDCP), tsTracker.unusualOsoSessionEnd.Load(),
		sum_backfill_done, tsTracker.slowThroughSeqnoCalls.max.Load(), tsTracker.slowThroughSeqnoCalls.min.Load(), tsTracker.slowThroughSeqnoCalls.sum.Load(),
		tsTracker.slowThroughSeqnoCalls.count.Load(), tsTracker.cLogExists, max_clog_req, sum_clog_req/count, max_clog_res, sum_clog_res/count,
		tsTracker.nonMonotonicThroughSeqnoCnt.Load(), tsTracker.faultyCLogTrackerCnt.Load(),
	)
}

func (tsTracker *ThroughSeqnoTrackerSvc) handleDataClonedEvent(vbno uint16, reqSeqno uint64, totalInstances int, syncCh chan bool) {
	defer close(syncCh)
	if totalInstances < 2 {
		return
	}
	tracker, exists := tsTracker.vbClonedTracker[vbno]
	if !exists {
		// not my vb
		return
	}
	tracker.lock.Lock()
	var found bool
	if atomic.LoadUint32(&tsTracker.osoModeActive) == 1 {
		_, found = base.SearchUint64ListUnsorted(tracker.seqno_list_1, reqSeqno)
	} else {
		_, found = base.SearchUint64List(tracker.seqno_list_1, reqSeqno)
	}
	if found {
		panic(fmt.Sprintf("tsTracker received same seqno %v of %v total cloned events", reqSeqno, totalInstances))
	}
	tracker.lock.Unlock()
	tracker.appendSeqnos(reqSeqno, uint64(totalInstances))
}

// Returns false if there are cloned events still outstanding to prevent throughSeqno from being moved
func (tsTracker *ThroughSeqnoTrackerSvc) preProcessOutgoingClonedEvent(event *common.Event) bool {
	if !event.EventType.IsOutNozzleThroughSeqnoRelated() {
		return true
	}
	var syncCh chan bool
	cloned := false
	var seqno uint64
	var vbno uint16

	switch event.EventType {
	case common.SubdocCmdSkippedDueToLimits:
		fallthrough
	case common.TargetDataSkipped:
		if event.OtherInfos.(parts.OutNozzleDataSkippedEventAdditional).Cloned == true {
			cloned = true
			syncCh = event.OtherInfos.(parts.OutNozzleDataSkippedEventAdditional).CloneSyncCh
		}
		seqno = event.OtherInfos.(parts.OutNozzleDataSkippedEventAdditional).Seqno
		vbno = event.OtherInfos.(parts.OutNozzleDataSkippedEventAdditional).VBucket
	case common.DataFailedCRSource:
		if event.OtherInfos.(parts.DataFailedCRSourceEventAdditional).Cloned == true {
			cloned = true
			syncCh = event.OtherInfos.(parts.DataFailedCRSourceEventAdditional).CloneSyncCh
		}
		seqno = event.OtherInfos.(parts.DataFailedCRSourceEventAdditional).Seqno
		vbno = event.OtherInfos.(parts.DataFailedCRSourceEventAdditional).VBucket
	case common.DataSent:
		if event.OtherInfos.(parts.DataSentEventAdditional).Cloned == true {
			cloned = true
			syncCh = event.OtherInfos.(parts.DataSentEventAdditional).CloneSyncCh
		}
		vbno = event.OtherInfos.(parts.DataSentEventAdditional).VBucket
		seqno = event.OtherInfos.(parts.DataSentEventAdditional).Seqno
	case common.DataNotReplicated:
		wrappedMcr := event.Data.(*base.WrappedMCRequest)
		if wrappedMcr.Cloned == true {
			cloned = true
			syncCh = wrappedMcr.ClonedSyncCh
		}
		vbno = wrappedMcr.GetSourceVB()
		seqno = wrappedMcr.Seqno
	default:
		panic("Implement me")
	}

	if !cloned {
		return true
	}

	if syncCh != nil {
		select {
		case <-syncCh:
			break
		}
	}
	tracker, exists := tsTracker.vbClonedTracker[vbno]
	if !exists {
		// not my vb
		return true
	}

	tracker.lock.Lock()
	defer tracker.lock.Unlock()
	var index int
	var found bool
	if atomic.LoadUint32(&tsTracker.osoModeActive) == 1 {
		index, found = base.SearchUint64ListUnsorted(tracker.seqno_list_1, seqno)
	} else {
		index, found = base.SearchUint64List(tracker.seqno_list_1, seqno)
	}
	if !found {
		return true
	}

	if tracker.seqno_list_2[index] == 0 {
		panic("Received more events than originally marked duplicated")
	}

	tracker.seqno_list_2[index]--

	if tracker.seqno_list_2[index] == 0 {
		return true
	}
	return false
}

func (tsTracker *ThroughSeqnoTrackerSvc) ProcessOsoMode(active bool, vbno uint16, minSeqnoFromDCP, maxSeqnoFromDCP, numSeqnoFromDCP uint64) (bool, *OsoSession) {
	if active {
		// Turn OSO tracking on
		atomic.StoreUint32(&tsTracker.osoModeActive, 1)
		atomic.AddUint64(&tsTracker.osoCntReceivedFromDCP, 1)
		tsTracker.vbOsoModeSessionDCPTracker.StartNewOSOSession(tsTracker, vbno)
		return false, nil
	} else {
		return tsTracker.vbOsoModeSessionDCPTracker.EndOSOSessionReceived(tsTracker, vbno, minSeqnoFromDCP, maxSeqnoFromDCP, numSeqnoFromDCP)
	}
}

func (tsTracker *ThroughSeqnoTrackerSvc) shouldStreamEnd(vbno uint16) (shouldStreamEnd bool) {
	if atomic.LoadUint32(&tsTracker.osoModeActive) == 0 {
		return true
	}

	return tsTracker.vbOsoModeSessionDCPTracker.ShouldStreamEnd(vbno)
}

// If tsTracker is in OSO mode, this routine can be used to fit in the input seqno to the right session
// for both data received from dcp and data processed by various pipeline upstream parts.
func (tsTracker *ThroughSeqnoTrackerSvc) shouldProcessAsOso(vbno uint16, seqno uint64) (processedAsOso bool, sessionPtr *OsoSession) {
	if atomic.LoadUint32(&tsTracker.osoModeActive) == 0 {
		return
	}

	return tsTracker.vbOsoModeSessionDCPTracker.ShouldProcessAsOSO(vbno, seqno)
}

func (tsTracker *ThroughSeqnoTrackerSvc) HandleDoneSession(vbno uint16, session *OsoSession) {
	session.PlaybackOntoTsTracker(tsTracker, vbno)
	tsTracker.vbOsoModeSessionDCPTracker.TruncateSession(vbno, session)
}

func (tsTracker *ThroughSeqnoTrackerSvc) appendCLogReq(vbno uint16, seqno uint64) {
	ok := tsTracker.validateVbno(vbno, "appendCLogReq")
	if !ok {
		return
	}

	cLogReqResList := tsTracker.cLogTrackerVbMap[vbno]
	cLogReqResList.appendSeqnoToList1(seqno)
}

func (tsTracker *ThroughSeqnoTrackerSvc) appendCLogRes(vbno uint16, seqno uint64) {
	ok := tsTracker.validateVbno(vbno, "appendCLogRes")
	if !ok {
		return
	}

	cLogReqResList := tsTracker.cLogTrackerVbMap[vbno]
	cLogReqResList.appendSeqnoToList2(seqno)
}
