// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package connector

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/common"
	component "github.com/couchbase/goxdcr/v8/component"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
)

// A VBTranslator's job is to handle VB translation in the case where the number of VBs betwee source and target
// buckets do not match
type VBTranslator struct {
	*component.AbstractComponent
}

func NewVBTranslator(id string, loggerContext *log.LoggerContext) *VBTranslator {
	t := &VBTranslator{
		AbstractComponent: component.NewAbstractComponentWithLogger(id, log.NewLogger(id, loggerContext)),
	}
	return t
}

func (V VBTranslator) GetLayoutString(part common.Part) string {
	//TODO implement me
	panic("implement me")
}

func (V VBTranslator) RegisterUpstreamPart(part common.Part) error {
	//TODO implement me
	panic("implement me")
}

func (V VBTranslator) Forward(data interface{}) error {
	//TODO implement me
	panic("implement me")
}

func (V VBTranslator) DownStreams() map[string]common.Part {
	//TODO implement me
	panic("implement me")
}

func (V VBTranslator) AddDownStream(partId string, part common.Part) error {
	//TODO implement me
	panic("implement me")
}

func (V VBTranslator) UpdateSettings(settings metadata.ReplicationSettingsMap) error {
	//TODO implement me
	panic("implement me")
}

func (V VBTranslator) IsStartable() bool {
	//TODO implement me
	panic("implement me")
}

func (V VBTranslator) Start() error {
	//TODO implement me
	panic("implement me")
}

func (V VBTranslator) Stop() error {
	//TODO implement me
	panic("implement me")
}

func (v *VBTranslator) GetUpstreamObjRecycler() func(obj interface{}) {
	panic("implement me")
}

// NoOpVBTranslator is used when no VB translation is needed
type NoOpVBTranslator struct {
	*component.AbstractComponent

	downstreamPartMap   map[string]common.Part
	downstreamPartVBMap map[uint16]common.Part
	downstreamPartMtx   sync.RWMutex

	upstreamPartMap map[string]common.Part
	upstreamPartMtx sync.RWMutex
}

func NewNoOpTranslator() *NoOpVBTranslator {
	return &NoOpVBTranslator{
		AbstractComponent:   component.NewAbstractComponent(""),
		downstreamPartMap:   map[string]common.Part{},
		downstreamPartVBMap: map[uint16]common.Part{},
		upstreamPartMap:     map[string]common.Part{},
	}
}

func (n *NoOpVBTranslator) UpdateSettings(settings metadata.ReplicationSettingsMap) error {
	return nil
}

func (n *NoOpVBTranslator) GetLayoutString(upstreamPart common.Part) string {
	n.upstreamPartMtx.RLock()
	part := n.upstreamPartMap[upstreamPart.Id()]
	n.upstreamPartMtx.RUnlock()

	var retString string
	if part != nil {
		// We just need one VB to match to find the right actual downstream connector to get its layout
		upstreamVBs := part.ResponsibleVBs()
		if len(upstreamVBs) > 0 {
			n.downstreamPartMtx.RLock()
			retString = n.downstreamPartVBMap[upstreamVBs[0]].Connector().GetLayoutString(part)
			n.downstreamPartMtx.RUnlock()
		}
	}
	return retString
}

func (n *NoOpVBTranslator) RegisterUpstreamPart(part common.Part) error {
	n.upstreamPartMtx.Lock()
	defer n.upstreamPartMtx.Unlock()

	if _, exists := n.upstreamPartMap[part.Id()]; exists {
		return fmt.Errorf("Upstream part %s has already been registered", part.Id())
	}
	n.upstreamPartMap[part.Id()] = part
	return nil
}

func (n *NoOpVBTranslator) Forward(data interface{}) error {
	uprEvent, ok := data.(*base.WrappedUprEvent)
	if !ok {
		return fmt.Errorf("Incorrect data format: %v", reflect.TypeOf(data))
	}

	n.downstreamPartMtx.RLock()
	defer n.downstreamPartMtx.RUnlock()
	return n.downstreamPartVBMap[uprEvent.UprEvent.VBucket].Receive(data)
}

func (n *NoOpVBTranslator) DownStreams() map[string]common.Part {
	n.downstreamPartMtx.RLock()
	defer n.downstreamPartMtx.RUnlock()

	return n.downstreamPartMap
}

func (n *NoOpVBTranslator) AddDownStream(partId string, part common.Part) error {
	n.downstreamPartMtx.Lock()
	defer n.downstreamPartMtx.Unlock()

	n.downstreamPartMap[partId] = part
	for _, vb := range part.ResponsibleVBs() {
		n.downstreamPartVBMap[vb] = part
	}
	return nil
}

func (n *NoOpVBTranslator) IsStartable() bool {
	return false
}

func (n *NoOpVBTranslator) Start() error {
	return nil
}

func (n *NoOpVBTranslator) Stop() error {
	return nil
}

// VB Translator needs to take a specific obj and figure out which VB
// the original request comes from and give it to the specific part
func (n *NoOpVBTranslator) GetUpstreamObjRecycler() func(obj interface{}) {
	recycleObj := func(vbno uint16, obj interface{}) {
		n.downstreamPartMtx.RLock()
		recycler := n.downstreamPartVBMap[vbno].Connector().GetUpstreamObjRecycler()
		n.downstreamPartMtx.RUnlock()
		recycler(obj)
	}

	dynamicRecycler := func(obj interface{}) {
		switch obj.(type) {
		case *base.WrappedUprEvent:
			vbno := obj.(*base.WrappedUprEvent).UprEvent.VBucket
			recycleObj(vbno, obj)
		case *base.CollectionNamespace:
			ns := obj.(*base.CollectionNamespace)
			if ns.GetUseRecycleVbnoHint() {
				recycleObj(ns.GetRecycleVbnoHint(), obj)
			}
		case *base.WrappedMCRequest:
			vbno := obj.(*base.WrappedMCRequest).Req.VBucket
			recycleObj(vbno, obj)
		default:
			panic(fmt.Sprintf("Coding bug type is %v", reflect.TypeOf(obj)))
		}
	}
	return dynamicRecycler
}
