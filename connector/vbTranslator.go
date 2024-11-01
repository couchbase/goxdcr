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

type VBTranslatorCommon struct {
	*component.AbstractComponent

	downstreamPartMap   map[string]common.Part
	downstreamPartVBMap map[uint16]common.Part
	downstreamPartMtx   sync.RWMutex

	upstreamPartMap map[string]common.Part
	upstreamPartMtx sync.RWMutex
}

func NewVBTranslatorCommon() VBTranslatorCommon {
	return VBTranslatorCommon{
		AbstractComponent:   component.NewAbstractComponent(""),
		downstreamPartMap:   map[string]common.Part{},
		downstreamPartVBMap: map[uint16]common.Part{},
		upstreamPartMap:     map[string]common.Part{},
	}
}

func (n *VBTranslatorCommon) UpdateSettings(settings metadata.ReplicationSettingsMap) error {
	return nil
}

func (n *VBTranslatorCommon) RegisterUpstreamPart(part common.Part) error {
	n.upstreamPartMtx.Lock()
	defer n.upstreamPartMtx.Unlock()

	if _, exists := n.upstreamPartMap[part.Id()]; exists {
		return fmt.Errorf("Upstream part %s has already been registered", part.Id())
	}
	n.upstreamPartMap[part.Id()] = part
	return nil
}

func (n *VBTranslatorCommon) DownStreams() map[string]common.Part {
	n.downstreamPartMtx.RLock()
	defer n.downstreamPartMtx.RUnlock()

	return n.downstreamPartMap
}

func (n *VBTranslatorCommon) AddDownStream(partId string, part common.Part) error {
	n.downstreamPartMtx.Lock()
	defer n.downstreamPartMtx.Unlock()

	n.downstreamPartMap[partId] = part
	for _, vb := range part.ResponsibleVBs() {
		n.downstreamPartVBMap[vb] = part
	}
	return nil
}

func (n *VBTranslatorCommon) IsStartable() bool {
	return false
}

func (n *VBTranslatorCommon) Start() error {
	return nil
}

func (n *VBTranslatorCommon) Stop() error {
	return nil
}

// VB Translator needs to take a specific obj and figure out which VB
// the original request comes from and give it to the specific part
func (n *VBTranslatorCommon) GetUpstreamObjRecycler() func(obj interface{}) {
	recycleObj := func(vbno uint16, obj interface{}) {
		n.downstreamPartMtx.RLock()
		recycler := n.downstreamPartVBMap[vbno].Connector().GetUpstreamObjRecycler()
		n.downstreamPartMtx.RUnlock()
		recycler(obj)
	}

	dynamicRecycler := func(obj interface{}) {
		switch obj.(type) {
		case *base.WrappedUprEvent:
			vbno := obj.(*base.WrappedUprEvent).GetSourceVB()
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

func (v *VBTranslatorCommon) forwardDataToDownstream(data interface{}, vbno uint16) error {
	v.downstreamPartMtx.RLock()
	defer v.downstreamPartMtx.RUnlock()

	return v.downstreamPartVBMap[vbno].Receive(data)
}

// NoOpVBTranslator is used when no VB translation is needed
type NoOpVBTranslator struct {
	VBTranslatorCommon
}

func NewNoOpTranslator() *NoOpVBTranslator {
	return &NoOpVBTranslator{
		VBTranslatorCommon: NewVBTranslatorCommon(),
	}
}

func (n *NoOpVBTranslator) Forward(data interface{}) error {
	uprEvent, ok := data.(*base.WrappedUprEvent)
	if !ok {
		return fmt.Errorf("Incorrect data format: %v", reflect.TypeOf(data))
	}

	return n.forwardDataToDownstream(data, uprEvent.GetTargetVB())
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

// A VBTranslator's job is to handle VB translation in the case where the number of VBs betwee source and target
// buckets do not match
type VBTranslator struct {
	VBTranslatorCommon

	logger          *log.CommonLogger
	srcNumVBs       int
	tgtNumVBs       int
	numOfSrcNozzles int
	routersAdded    int
}

func NewVBTranslator(id string, loggerContext *log.LoggerContext, srcNumVBs, tgtNumVBs, numOfSrcNozzles int) *VBTranslator {
	t := &VBTranslator{
		VBTranslatorCommon: NewVBTranslatorCommon(),
		logger:             log.NewLogger(id, loggerContext),
		srcNumVBs:          srcNumVBs,
		tgtNumVBs:          tgtNumVBs,
		numOfSrcNozzles:    numOfSrcNozzles,
	}
	return t
}

func (v *VBTranslator) Forward(data interface{}) error {
	var wrappedUpr *base.WrappedUprEvent
	var ok bool
	if wrappedUpr, ok = data.(*base.WrappedUprEvent); !ok {
		return fmt.Errorf("Incorrect data type: %v", reflect.TypeOf(data))
	}

	newVB := uint16(CbCrc(wrappedUpr.UprEvent.Key) % uint32(v.tgtNumVBs))
	wrappedUpr.TranslatedVB = &newVB

	return v.forwardDataToDownstream(data, newVB)
}

func (v *VBTranslator) GetLayoutString(upstreamPart common.Part) string {
	return ""
}

func (v *VBTranslator) AddDownStream(partId string, part common.Part) error {
	commonErr := v.VBTranslatorCommon.AddDownStream(partId, part)
	if commonErr != nil {
		return commonErr
	}

	if v.srcNumVBs >= v.tgtNumVBs {
		// No additional work needs to be done
		return nil
	}

	v.downstreamPartMtx.Lock()
	defer v.downstreamPartMtx.Unlock()

	v.routersAdded++
	if v.routersAdded != v.numOfSrcNozzles {
		// More routers will need to be added
		return nil
	}

	additionalVBs := make(map[string][]uint16)

	// At this point, all routers have been added. Do round-robin
	var vbsListToRoundRobin []uint16
	for vb, _ := range v.downstreamPartVBMap {
		vbsListToRoundRobin = append(vbsListToRoundRobin, vb)
	}
	var idxToUse int

	for i := 0; i < v.tgtNumVBs; i++ {
		// It is possible for a source VB distribution of this node to have a range of [non-0, X]
		if _, vbIsMapped := v.downstreamPartVBMap[uint16(i)]; vbIsMapped {
			continue
		}

		routerToReuse := v.downstreamPartVBMap[vbsListToRoundRobin[idxToUse]]
		idxToUse = (idxToUse + 1) % len(vbsListToRoundRobin)

		v.downstreamPartVBMap[uint16(i)] = routerToReuse

		// After reusing the router, this reused router is now responsible for this specific target VB that was not mapped
		// before
		additionalVBs[routerToReuse.Id()] = append(additionalVBs[routerToReuse.Id()], uint16(i))
	}

	for routerId, vbsToAdd := range additionalVBs {
		settingsMap := make(metadata.ReplicationSettingsMap)
		settingsMap[metadata.VariableVBAdditionalVBs] = vbsToAdd
		err := v.downstreamPartMap[routerId].UpdateSettings(settingsMap)
		if err != nil {
			return err
		}
	}
	return nil
}
