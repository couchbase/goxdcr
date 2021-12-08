/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package pipeline

import (
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	utilities "github.com/couchbase/goxdcr/utils"
	"strings"
	"sync"
	"time"
)

var ErrorCannotDismiss = errors.New("Specified event cannot be dismissed")

type PipelineEventList struct {
	EventInfos []*base.EventInfo
	TimeInfos  []int64
	Mutex      *sync.RWMutex
}

func (p *PipelineEventList) Len() int {
	p.Mutex.RLock()
	defer p.Mutex.RUnlock()
	return len(p.EventInfos)
}

// Each pipeline can only have a single grand-daddy of broken map event
// This will create a broken map event if none is found
// Also will update the timestamp
// DoneFunc must be called as soon as the event is done editing
// DelFunc is used if brokenmap is not meant to exist in the ErrorList to be returned and must be called before doneFunc
func (p *PipelineEventList) LockAndGetBrokenMapEventForEditing(idWell *int64) (event *base.EventInfo, doneFunc func(), delFunc func()) {
	var idx int

	p.Mutex.RLock()
	doneFunc = func() {
		p.Mutex.RUnlock()
	}

	delFunc = func() {
		// Upgrade lock
		p.Mutex.RUnlock()
		p.Mutex.Lock()

		// Check again to make sure that idx didn't move
		var delIdx int
		var delFound bool
		for delIdx = 0; delIdx < len(p.EventInfos); delIdx++ {
			if p.EventInfos[delIdx].EventType == base.BrokenMappingInfoType {
				delFound = true
				break
			}
		}

		if delFound {
			p.EventInfos = append(p.EventInfos[:delIdx], p.EventInfos[delIdx+1:]...)
		}
		p.Mutex.Unlock()
		p.Mutex.RLock()
	}

	for idx = 0; idx < len(p.EventInfos); idx++ {
		if p.EventInfos[idx].EventType == base.BrokenMappingInfoType {
			// Update this slot's time
			p.TimeInfos[idx] = time.Now().UnixNano()
			event = p.EventInfos[idx]
			return
		}
	}

	// Not found
	event = p.tempUpgradeLockAndCreateNewBrokenMapEvent(idWell)

	return
}

func (p *PipelineEventList) tempUpgradeLockAndCreateNewBrokenMapEvent(idWell *int64) *base.EventInfo {
	p.Mutex.RUnlock()
	p.Mutex.Lock()

	newEvent := base.NewEventInfo()
	newEvent.EventId = base.GetEventIdFromWell(idWell)
	newEvent.EventType = base.BrokenMappingInfoType
	p.EventInfos = append(p.EventInfos, newEvent)
	idx := len(p.EventInfos) - 1
	event := p.EventInfos[idx]
	p.TimeInfos = append(p.TimeInfos, time.Now().UnixNano())

	p.Mutex.Unlock()
	p.Mutex.RLock()
	return event
}

type PipelineEventsManager interface {
	GetCurrentEvents() *PipelineEventList
	AddEvent(eventType base.EventInfoType, eventDesc string, eventExtras base.EventsMap) (eventId int64)
	ClearNonBrokenMapEvents()
	ClearNonBrokenMapEventsWithString(substr string)
	LoadLatestBrokenMap(mapping metadata.CollectionNamespaceMapping)
	ContainsEvent(eventId int) bool
	DismissEvent(eventId int) error
	ResetDismissedHistory()
	BackfillUpdateCb(diffPair *metadata.CollectionNamespaceMappingsDiffPair, srcManifestsDelta []*metadata.CollectionsManifest) error
}

// The pipeline events mgr's job is to handle the exporting of events and also remember the user's preference
// on dismissing events
type PipelineEventsMgr struct {
	events      PipelineEventList
	specId      string
	specGetter  ReplicationSpecGetter
	eventIdWell *int64
	logger      *log.CommonLogger
	utils       utilities.UtilsIface

	cachedBrokenMap        metadata.PipelineEventBrokenMap
	updateBrokenMapEventCh chan bool

	isMigrationMode bool
}

func NewPipelineEventsMgr(eventIdWell *int64, specId string, specGetter ReplicationSpecGetter, logger *log.CommonLogger, utils utilities.UtilsIface) *PipelineEventsMgr {
	if eventIdWell == nil {
		idWell := int64(-1)
		eventIdWell = &idWell
	}
	mgr := &PipelineEventsMgr{
		events: PipelineEventList{
			Mutex: &sync.RWMutex{},
		},
		specId:                 specId,
		specGetter:             specGetter,
		eventIdWell:            eventIdWell,
		cachedBrokenMap:        metadata.NewPipelineEventBrokenMap(),
		logger:                 logger,
		updateBrokenMapEventCh: make(chan bool, 1),
		utils:                  utils,
	}
	mgr.updateBrokenMapEventCh <- true
	return mgr
}

func (p *PipelineEventsMgr) GetCurrentEvents() *PipelineEventList {
	p.updateBrokenMapEventIfNeeded()

	return &p.events
}

// Used internally - does not update brokenmap but rather after an eventID already confirmed to exist
func (p *PipelineEventsMgr) getEvent(eventId int) (*base.EventInfo, error) {
	p.events.Mutex.RLock()
	defer p.events.Mutex.RUnlock()

	for _, event := range p.events.EventInfos {
		if int(event.EventId) == eventId {
			return event, nil
		}
		subEvent, err := event.GetSubEvent(eventId)
		if subEvent != nil && err == nil {
			return subEvent, err
		}
	}
	return nil, base.ErrorNotFound
}

func (p *PipelineEventsMgr) AddEvent(eventType base.EventInfoType, eventDesc string, eventExtras base.EventsMap) int64 {
	if eventExtras.IsNil() {
		eventExtras.Init()
	}

	newEvent := base.NewEventInfo()
	newEvent.EventId = base.GetEventIdFromWell(p.eventIdWell)
	newEvent.EventType = eventType
	newEvent.EventDesc = eventDesc
	newEvent.EventExtras = eventExtras

	p.events.Mutex.Lock()
	defer p.events.Mutex.Unlock()
	p.events.TimeInfos = append(p.events.TimeInfos, time.Now().UnixNano())
	p.events.EventInfos = append(p.events.EventInfos, newEvent)
	return newEvent.EventId
}

// When pipeline is paused, brokenMap events need to stay once pipeline resumes because no further mutations will
// go through the router and re-trigger the brokenmap events... but other events like warnings, errors, persistent
// messages, should be reset and then they can be re-triggered as needed
func (p *PipelineEventsMgr) ClearNonBrokenMapEvents() {
	p.events.Mutex.Lock()
	defer p.events.Mutex.Unlock()

	var replacementList []*base.EventInfo
	var replacementTime []int64

	for i, event := range p.events.EventInfos {
		if event.EventType != base.BrokenMappingInfoType {
			continue
		}
		replacementList = append(replacementList, event)
		replacementTime = append(replacementTime, p.events.TimeInfos[i])
	}

	p.events.EventInfos = replacementList
	p.events.TimeInfos = replacementTime
}

func (p *PipelineEventsMgr) ClearNonBrokenMapEventsWithString(substr string) {
	p.events.Mutex.Lock()
	defer p.events.Mutex.Unlock()

	var replacementList []*base.EventInfo
	var replacementTime []int64

	for i, event := range p.events.EventInfos {
		if event.EventType != base.BrokenMappingInfoType {
			continue
		}
		if !strings.Contains(event.EventDesc, substr) {
			replacementList = append(replacementList, event)
			replacementTime = append(replacementTime, p.events.TimeInfos[i])
		}
	}

	p.events.EventInfos = replacementList
	p.events.TimeInfos = replacementTime
}

func (p *PipelineEventsMgr) LoadLatestBrokenMap(readOnlyBrokenMap metadata.CollectionNamespaceMapping) {
	p.cachedBrokenMap.LoadLatestBrokenMap(readOnlyBrokenMap)
}

func (p *PipelineEventsMgr) updateBrokenMapEventIfNeeded() {
	if !p.cachedBrokenMap.NeedsToUpdate() {
		return
	}

	select {
	case <-p.updateBrokenMapEventCh:
		updateTimeout := base.ExecWithTimeout(p.updateBrokenMapEvent, base.ReplStatusExportBrokenMapTimeout, p.logger)
		if updateTimeout != nil {
			p.logger.Warnf("Updating brokenMap event is taking longer. It will finish up eventually")
		}
	default:
		return
	}
}

func (p *PipelineEventsMgr) updateBrokenMapEvent() error {
	stopFunc := p.utils.StartDiagStopwatch(fmt.Sprintf("updateBrokenMapEvent - %v", p.specId), base.ReplStatusExportBrokenMapTimeout)
	defer stopFunc()

	p.updateMigrationMode()

	// As part of the update below, clear the booleans
	p.cachedBrokenMap.MarkUpdated()

	brokenMapEvent, doneFunc, delFunc := p.events.LockAndGetBrokenMapEventForEditing(p.eventIdWell)
	defer doneFunc()

	eventIsEmpty := p.cachedBrokenMap.ExportToEvent(brokenMapEvent, p.eventIdWell, p.isMigrationMode)
	if eventIsEmpty {
		delFunc()
	}
	p.updateBrokenMapEventCh <- true
	return nil
}

func (p *PipelineEventsMgr) ContainsEvent(eventId int) bool {
	p.events.Mutex.RLock()
	defer p.events.Mutex.RUnlock()
	for _, eventInfo := range p.events.EventInfos {
		if eventInfo.ContainsEvent(eventId) {
			return true
		}
	}
	return false
}

func (p *PipelineEventsMgr) DismissEvent(eventId int) error {
	event, err := p.getEvent(eventId)
	if err != nil {
		return err
	}

	if !event.EventType.CanDismiss() {
		return ErrorCannotDismiss
	}

	return p.registerDismissEventAction(event)
}

func (p *PipelineEventsMgr) registerDismissEventAction(event *base.EventInfo) error {
	switch event.EventType {
	case base.LowPriorityMsg:
		return p.handleDismissLowPriorityMsg(event)
	case base.BrokenMappingInfoType:
		return p.handleDismissBrokenMapEvent(event)
	default:
		// Do nothing
		return fmt.Errorf("Not implemented")
	}
}

func (p *PipelineEventsMgr) handleDismissBrokenMapEvent(incomingEvent *base.EventInfo) error {
	// Dismissing an event from the current brokenmap situation requires knowing which case it is:
	// (Different levels)
	// 0. Dismissing an entire brokenMap event
	// 1. Dismissing an entire source scope name namespace
	// 2. Dismissing an entire sourceScope.sourceCollection namespace (1 -> N possible with migration mode)
	// 3. Dismissing a single sourceScope.sourceCollection -> targetScope.targetCollection event

	level, srcEventDesc, tgtEventDesc, err := p.getIncomingEventBrokenmapLevelAndDesc(incomingEvent)
	if err != nil {
		p.logger.Errorf("Unable to handle dismiss brokenmap event %v - %v", incomingEvent.EventId, err)
		return err
	}

	return p.cachedBrokenMap.RegisterDismissAction(level, srcEventDesc, tgtEventDesc)
}

// Returns:
// 1. level count
// 2. Source namespace descriptor
// 3. target namespace descriptor
func (p *PipelineEventsMgr) getIncomingEventBrokenmapLevelAndDesc(incomingEvent *base.EventInfo) (int, string, string, error) {
	var level int = -1
	eventsList := p.GetCurrentEvents()
	eventsList.Mutex.RLock()
	defer eventsList.Mutex.RUnlock()

	// First get the overall brokenmap event
	for _, brokenMappingEvent := range eventsList.EventInfos {
		if brokenMappingEvent.EventType != base.BrokenMappingInfoType {
			continue
		}
		// Found
		level = 0
		if brokenMappingEvent.EventId == incomingEvent.EventId {
			return level, "", "", nil
		}
		// Dive deeper - entire scope level
		brokenMappingEvent.EventExtras.GetRWLock().RLock()
		defer brokenMappingEvent.EventExtras.GetRWLock().RUnlock()
		for _, scopeEventRaw := range brokenMappingEvent.EventExtras.EventsMap {
			level = 1
			scopeEvent := scopeEventRaw.(*base.EventInfo)
			if scopeEvent.EventId == incomingEvent.EventId {
				return level, scopeEvent.EventDesc, "", nil
			}
			// Look at individual s.c under this scope
			scopeEvent.EventExtras.GetRWLock().RLock()
			for _, srcNsEventRaw := range scopeEvent.EventExtras.EventsMap {
				level = 2
				srcNsEvent := srcNsEventRaw.(*base.EventInfo)
				if srcNsEvent.EventId == incomingEvent.EventId {
					scopeEvent.EventExtras.GetRWLock().RUnlock()
					return level, srcNsEvent.EventDesc, "", nil
				}
				// Look at one single target entry of s.c
				srcNsEvent.EventExtras.GetRWLock().RLock()
				for _, tgtNsEventRaw := range srcNsEvent.EventExtras.EventsMap {
					level = 3
					tgtNsEvent := tgtNsEventRaw.(*base.EventInfo)
					if tgtNsEvent.EventId == incomingEvent.EventId {
						srcNsEvent.EventExtras.GetRWLock().RUnlock()
						scopeEvent.EventExtras.GetRWLock().RUnlock()
						// For level 3, require some special descriptor
						// Since a single targetScope.targetCol is not enough
						// we need to pass back an entirety of s.c:st.ct
						return level, srcNsEvent.EventDesc, tgtNsEvent.EventDesc, nil
					}
				}
				srcNsEvent.EventExtras.GetRWLock().RUnlock()
			}
			scopeEvent.EventExtras.GetRWLock().RUnlock()
		}
	}
	return level, "", "", base.ErrorNotFound
}

// Dismissing a low priority msg simply removes it from the queue
func (p *PipelineEventsMgr) handleDismissLowPriorityMsg(event *base.EventInfo) error {
	eventsList := p.GetCurrentEvents()
	eventsList.Mutex.Lock()
	defer eventsList.Mutex.Unlock()

	for i, checkEvent := range eventsList.EventInfos {
		if checkEvent.SameAs(event) {
			eventsList.EventInfos = append(eventsList.EventInfos[:i], eventsList.EventInfos[i+1:]...)
			return nil
		}
	}
	return base.ErrorNotFound
}

func (p *PipelineEventsMgr) ResetDismissedHistory() {
	p.cachedBrokenMap.ResetAllDismissedHistory()
}

func (p *PipelineEventsMgr) BackfillUpdateCb(diffPair *metadata.CollectionNamespaceMappingsDiffPair, srcManifestsDelta []*metadata.CollectionsManifest) error {
	return p.cachedBrokenMap.UpdateWithNewDiffPair(diffPair, srcManifestsDelta)
}

func (p *PipelineEventsMgr) updateMigrationMode() {
	stopFunc := p.utils.StartDiagStopwatch(fmt.Sprintf("updateMigrationMode - %v", p.specId), base.DiagInternalThreshold)
	defer stopFunc()

	spec, err := p.specGetter(p.specId)
	if err != nil {
		p.logger.Warnf("Received err %v when getting spec: %v", err, spec)
		return
	}
	collectionMode := spec.Settings.GetCollectionModes()
	p.isMigrationMode = collectionMode.IsMigrationOn()
}
