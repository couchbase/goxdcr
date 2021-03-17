package pipeline

import (
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/metadata"
	"sync"
	"sync/atomic"
	"time"
)

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
		EventId:     atomic.AddInt64(idWell, 1),
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
	GetCurrentEvents() PipelineEventList
	AddEvent(eventType base.ErrorInfoType, eventDesc string, eventExtras base.EventsMap)
	LoadLatestBrokenMap(mapping metadata.CollectionNamespaceMapping)
}

type PipelineEventsMgr struct {
	events      PipelineEventList
	specId      string
	specGetter  ReplicationSpecGetter
	eventIdWell *int64

	cachedBrokenMap metadata.PipelineEventBrokenMap
}

func NewPipelineEventsMgr(eventIdWell *int64, specId string, specGetter ReplicationSpecGetter) *PipelineEventsMgr {
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
	}
	return mgr
}

func (p *PipelineEventsMgr) GetCurrentEvents() PipelineEventList {
	p.updateBrokenMapEventIfNeeded()

	return p.events
}

func (p *PipelineEventsMgr) AddEvent(eventType base.ErrorInfoType, eventDesc string, eventExtras base.EventsMap) {
	if eventExtras.IsNil() {
		eventExtras.Init()
	}

	newEvent := base.EventInfo{
		EventId:     atomic.AddInt64(p.eventIdWell, 1),
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

	brokenMapEvent, doneFunc, delFunc := p.events.LockAndGetBrokenMapEventForEditing(p.eventIdWell)
	defer doneFunc()

	// TODO Part 2 - filter out events based on user's dismissal feedback history
	eventIsEmpty := p.cachedBrokenMap.ExportToEvent(brokenMapEvent, p.eventIdWell)
	if eventIsEmpty {
		delFunc()
	}

}
