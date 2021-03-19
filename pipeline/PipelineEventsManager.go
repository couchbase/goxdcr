package pipeline

import (
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"sync"
	"time"
)

var ErrorCannotDismiss = errors.New("Specified event cannot be dismissed")

type PipelineEventList struct {
	EventInfos []base.EventInfo
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
	idxPtr := &idx

	p.Mutex.Lock()
	doneFunc = func() {
		p.Mutex.Unlock()
	}

	delFunc = func() {
		p.EventInfos = append(p.EventInfos[:*idxPtr], p.EventInfos[*idxPtr+1:]...)
	}

	for idx = 0; idx < len(p.EventInfos); idx++ {
		if p.EventInfos[idx].EventType == base.BrokenMappingInfoType {
			// Update this slot's time
			p.TimeInfos[idx] = time.Now().UnixNano()
			event = &p.EventInfos[idx]
			return
		}
	}

	// Not found
	newEvent := base.EventInfo{
		EventId:     base.GetEventIdFromWell(idWell),
		EventType:   base.BrokenMappingInfoType,
		EventDesc:   "",
		EventExtras: base.NewEventsMap(),
	}
	p.EventInfos = append(p.EventInfos, newEvent)
	idx = len(p.EventInfos) - 1
	event = &p.EventInfos[idx]
	p.TimeInfos = append(p.TimeInfos, time.Now().UnixNano())

	return
}

type PipelineEventsManager interface {
	GetCurrentEvents() *PipelineEventList
	AddEvent(eventType base.ErrorInfoType, eventDesc string, eventExtras base.EventsMap)
	LoadLatestBrokenMap(mapping metadata.CollectionNamespaceMapping)
	ContainsEvent(eventId int) bool
	DismissEvent(eventId int) error
}

// The pipeline events mgr's job is to handle the exporting of events and also remember the user's preference
// on dismissing events
type PipelineEventsMgr struct {
	events      PipelineEventList
	specId      string
	specGetter  ReplicationSpecGetter
	eventIdWell *int64
	logger      *log.CommonLogger

	cachedBrokenMap metadata.PipelineEventBrokenMap
}

func NewPipelineEventsMgr(eventIdWell *int64, specId string, specGetter ReplicationSpecGetter, logger *log.CommonLogger) *PipelineEventsMgr {
	if eventIdWell == nil {
		idWell := int64(-1)
		eventIdWell = &idWell
	}
	mgr := &PipelineEventsMgr{
		events: PipelineEventList{
			Mutex: &sync.RWMutex{},
		},
		specId:          specId,
		specGetter:      specGetter,
		eventIdWell:     eventIdWell,
		cachedBrokenMap: metadata.NewPipelineEventBrokenMap(),
		logger:          logger,
	}
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
			return &event, nil
		}
		subEvent, err := event.GetSubEvent(eventId)
		if subEvent != nil && err == nil {
			return subEvent, err
		}
	}
	return nil, base.ErrorNotFound
}

func (p *PipelineEventsMgr) AddEvent(eventType base.ErrorInfoType, eventDesc string, eventExtras base.EventsMap) {
	if eventExtras.IsNil() {
		eventExtras.Init()
	}

	newEvent := base.EventInfo{
		EventId:     base.GetEventIdFromWell(p.eventIdWell),
		EventType:   eventType,
		EventDesc:   eventDesc,
		EventExtras: eventExtras,
	}

	p.events.Mutex.Lock()
	defer p.events.Mutex.Unlock()
	p.events.TimeInfos = append(p.events.TimeInfos, time.Now().UnixNano())
	p.events.EventInfos = append(p.events.EventInfos, newEvent)
}

func (p *PipelineEventsMgr) LoadLatestBrokenMap(readOnlyBrokenMap metadata.CollectionNamespaceMapping) {
	p.cachedBrokenMap.LoadLatestBrokenMap(readOnlyBrokenMap)
}

func (p *PipelineEventsMgr) updateBrokenMapEventIfNeeded() {
	if !p.cachedBrokenMap.NeedsToUpdate() {
		return
	}
	// As part of the update below, clear the booleans
	p.cachedBrokenMap.MarkUpdated()

	brokenMapEvent, doneFunc, delFunc := p.events.LockAndGetBrokenMapEventForEditing(p.eventIdWell)
	defer doneFunc()

	eventIsEmpty := p.cachedBrokenMap.ExportToEvent(brokenMapEvent, p.eventIdWell)
	if eventIsEmpty {
		delFunc()
	}

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
			scopeEvent := scopeEventRaw.(base.EventInfo)
			if scopeEvent.EventId == incomingEvent.EventId {
				return level, scopeEvent.EventDesc, "", nil
			}
			// Look at individual s.c under this scope
			scopeEvent.EventExtras.GetRWLock().RLock()
			for _, srcNsEventRaw := range scopeEvent.EventExtras.EventsMap {
				level = 2
				srcNsEvent := srcNsEventRaw.(base.EventInfo)
				if srcNsEvent.EventId == incomingEvent.EventId {
					scopeEvent.EventExtras.GetRWLock().RUnlock()
					return level, srcNsEvent.EventDesc, "", nil
				}
				// Look at one single target entry of s.c
				srcNsEvent.EventExtras.GetRWLock().RLock()
				for _, tgtNsEventRaw := range srcNsEvent.EventExtras.EventsMap {
					level = 3
					tgtNsEvent := tgtNsEventRaw.(base.EventInfo)
					if tgtNsEvent.EventId == incomingEvent.EventId {
						srcNsEvent.EventExtras.GetRWLock().RUnlock()
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