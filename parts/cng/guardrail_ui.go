package cng

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/common"
	"github.com/couchbase/goxdcr/v8/log"
)

const (
	guardrailMsgResidentRatioTooLow = "BUCKET_RESIDENT_RATIO_TOO_LOW"
	guardrailMsgDataSizeTooBig      = "BUCKET_DATA_SIZE_TOO_BIG"
	guardrailMsgDiskSpaceTooLow     = "BUCKET_DISK_SPACE_TOO_LOW"
)

type guardrailEntry struct {
	name     string
	lastSeen atomic.Int64
	// eventId is only mutated by the monitor goroutine.
	eventId int64
}

type guardrailUI struct {
	producer common.PipelineEventsProducer
	topic    string
	stopCh   <-chan struct{}
	logger   *log.CommonLogger

	tick         time.Duration
	activeWindow time.Duration

	states map[CNGErrorCode]*guardrailEntry
	order  []CNGErrorCode
	startOnce sync.Once
}

func newGuardrailUI(topic string, producer common.PipelineEventsProducer, stopCh <-chan struct{}, logger *log.CommonLogger) *guardrailUI {
	g := &guardrailUI{
		producer:     producer,
		topic:        topic,
		stopCh:       stopCh,
		logger:       logger,
		tick:         base.XmemSelfMonitorInterval,
		activeWindow: 2 * base.XmemSelfMonitorInterval,
		states:       make(map[CNGErrorCode]*guardrailEntry),
	}

	// Stable order for UI events
	g.order = []CNGErrorCode{
		ERR_GUARDRAIL_BUCKET_RESIDENT_RATIO_TOO_LOW,
		ERR_GUARDRAIL_BUCKET_DATA_SIZE_TOO_BIG,
		ERR_GUARDRAIL_BUCKET_DISK_SPACE_TOO_LOW,
	}

	g.states[ERR_GUARDRAIL_BUCKET_RESIDENT_RATIO_TOO_LOW] = &guardrailEntry{name: guardrailMsgResidentRatioTooLow, eventId: -1}
	g.states[ERR_GUARDRAIL_BUCKET_DATA_SIZE_TOO_BIG] = &guardrailEntry{name: guardrailMsgDataSizeTooBig, eventId: -1}
	g.states[ERR_GUARDRAIL_BUCKET_DISK_SPACE_TOO_LOW] = &guardrailEntry{name: guardrailMsgDiskSpaceTooLow, eventId: -1}

	return g
}

func (g *guardrailUI) start() {
	if g == nil || g.producer == nil {
		return
	}
	g.startOnce.Do(func() {
		go g.run()
	})
}

func (g *guardrailUI) noteGuardrail(code CNGErrorCode) {
	if g == nil {
		return
	}
	entry, ok := g.states[code]
	if !ok {
		return
	}
	entry.lastSeen.Store(time.Now().UnixNano())
}

func (g *guardrailUI) run() {
	ticker := time.NewTicker(g.tick)
	defer ticker.Stop()

	for {
		select {
		case <-g.stopCh:
			g.dismissAll()
			return
		case <-ticker.C:
			g.tickOnce()
		}
	}
}

func (g *guardrailUI) tickOnce() {
	now := time.Now()

	for _, code := range g.order {
		entry := g.states[code]
		if entry == nil {
			continue
		}
		last := entry.lastSeen.Load()
		active := last != 0 && now.Sub(time.Unix(0, last)) <= g.activeWindow

		if entry.eventId == -1 && active {
			msg := fmt.Sprintf("%s: %s", entry.name, g.topic)
			eventId := g.producer.AddEvent(base.LowPriorityMsg, msg, base.EventsMap{}, nil)
			entry.eventId = eventId
			continue
		}

		if entry.eventId >= 0 && !active {
			eventToDismiss := entry.eventId
			err := g.producer.DismissEvent(int(eventToDismiss))
			if err != nil {
				if g.logger != nil {
					g.logger.Warnf("Unable to dismiss event %v: %v", eventToDismiss, err)
				}
			} else {
				entry.eventId = -1
			}
		}
	}
}

func (g *guardrailUI) dismissAll() {
	if g == nil || g.producer == nil {
		return
	}

	for _, code := range g.order {
		entry := g.states[code]
		if entry == nil {
			continue
		}
		eventToDismiss := entry.eventId
		if eventToDismiss < 0 {
			continue
		}
		err := g.producer.DismissEvent(int(eventToDismiss))
		if err != nil {
			if g.logger != nil {
				g.logger.Warnf("Unable to dismiss event %v: %v", eventToDismiss, err)
			}
			continue
		}
		entry.eventId = -1
	}
}
