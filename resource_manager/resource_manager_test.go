package resource_manager

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	common_mock "github.com/couchbase/goxdcr/v8/common/mocks"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/pipeline"
	pipeline_mock "github.com/couchbase/goxdcr/v8/pipeline/mocks"
	pipeline_manager "github.com/couchbase/goxdcr/v8/pipeline_manager/mocks"
	service_def "github.com/couchbase/goxdcr/v8/service_def/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type pipelineInfo struct {
	Stats *ReplStats
}

func getReplSettings(pdata *pipelineData) *metadata.ReplicationSettings {
	s := metadata.DefaultReplicationSettings()
	s.Values[metadata.PriorityKey] = pdata.Priority
	if pdata.CLogEnabled {
		s.Values[metadata.CLogKey] = base.ConflictLoggingMappingInput{}
	}

	return s
}

type pipelineData struct {
	Priority    base.PriorityType
	CLogEnabled bool
	Stats       *ReplStats
}

func createAutoData(inp *testInput) *autoData {
	tt := &autoData{
		Specs:         map[string]*metadata.ReplicationSpecification{},
		BackfillSpecs: map[string]*metadata.BackfillReplicationSpec{},
		Pipelines:     map[string]pipelineInfo{},
	}

	for id, pdata := range inp.pipes {
		if strings.HasPrefix(id, "backfill_") {
			tt.BackfillSpecs[id] = &metadata.BackfillReplicationSpec{}
		} else {
			tt.Specs[id] = &metadata.ReplicationSpecification{
				Id:       id,
				Settings: getReplSettings(pdata),
			}
		}

		tt.Pipelines[id] = pipelineInfo{
			Stats: pdata.Stats,
		}
	}

	return tt
}

type autoData struct {
	Specs         map[string]*metadata.ReplicationSpecification
	BackfillSpecs map[string]*metadata.BackfillReplicationSpec
	Pipelines     map[string]pipelineInfo
}

type testInput struct {
	name      string
	pipes     map[string]*pipelineData
	cpu       int64
	autoData  *autoData
	prevState *State
}

func (t *testInput) CollectReplStats() map[*metadata.GenericSpecification]*ReplStats {
	ret := map[*metadata.GenericSpecification]*ReplStats{}

	for id, spec := range t.autoData.Specs {
		genSpec := metadata.GenericSpecification(spec)
		ret[&genSpec] = t.autoData.Pipelines[id].Stats
	}

	for id, spec := range t.autoData.BackfillSpecs {
		genSpec := metadata.GenericSpecification(spec)
		ret[&genSpec] = t.autoData.Pipelines[id].Stats
	}

	return ret
}

func Test_computeTokens(t *testing.T) {
	now := time.Now()
	prevTS := now.Add(-1 * time.Second).UnixNano()
	testData := []*testInput{
		{
			name: "low-low",
			cpu:  99,
			prevState: &State{
				replStatsMap: map[string]*ReplStats{
					"R-1": {
						changesLeft:         200,
						docsReceivedFromDcp: 800,
						docsRepQueue:        80,
						timestamp:           prevTS,
					},
					"R-2": {
						changesLeft:         200,
						docsReceivedFromDcp: 800,
						docsRepQueue:        80,
						timestamp:           prevTS,
					},
				},
			},
			pipes: map[string]*pipelineData{
				"R-1": {
					Priority:    base.PriorityTypeLow,
					CLogEnabled: true,
					Stats: &ReplStats{
						changesLeft:         100,
						docsReceivedFromDcp: 1000,
						docsRepQueue:        100,
						timestamp:           time.Now().UnixNano(),
					},
				},
				"R-2": {
					Priority: base.PriorityTypeLow,
					Stats: &ReplStats{
						changesLeft:         100,
						docsReceivedFromDcp: 1000,
						docsRepQueue:        100,
						timestamp:           time.Now().UnixNano(),
					},
				},
			},
		},
	}

	for _, tt := range testData {
		tt.autoData = createAutoData(tt)
		t.Run(tt.name, func(t *testing.T) {
			doesMedPriorityExists := false
			for _, pdata := range tt.pipes {
				if pdata.Priority == base.PriorityTypeMedium {
					doesMedPriorityExists = true
					break
				}
			}

			//CollectReplStats() map[*metadata.GenericSpecification]*ReplStats
			xdcrTopoSvc := service_def.NewXDCRCompTopologySvc(t)
			//xdcrTopoSvc.EXPECT().IsKVNode().Return(true, nil)

			replSvc := service_def.NewReplicationSpecSvc(t)
			replSvc.EXPECT().AllActiveReplicationSpecsReadOnly().RunAndReturn(func() (map[string]*metadata.ReplicationSpecification, error) {
				return tt.autoData.Specs, nil
			})

			backfilleReplSvc := service_def.NewBackfillReplSvc(t)
			backfilleReplSvc.EXPECT().AllActiveBackfillSpecsReadOnly().RunAndReturn(func() (map[string]*metadata.BackfillReplicationSpec, error) {
				return tt.autoData.BackfillSpecs, nil
			})

			pipeMgr := pipeline_manager.NewPipelineMgrIface(t)
			if doesMedPriorityExists {
				pipeMgr.EXPECT().ReplicationStatus(mock.Anything).RunAndReturn(func(topic string) (pipeline.ReplicationStatusIface, error) {
					_, ok := tt.autoData.Pipelines[topic]
					if !ok {
						return nil, fmt.Errorf("repl %s not found", topic)
					}

					replStatus := pipeline_mock.NewReplicationStatusIface(t)

					pipeObj := common_mock.NewPipeline(t)
					pipeObj.EXPECT().UpdateSettings(mock.Anything).RunAndReturn(func(rsm metadata.ReplicationSettingsMap) error {
						return nil
					})

					if strings.HasPrefix(topic, "backfill_") {
						replStatus.EXPECT().BackfillPipeline().Return(pipeObj)
					} else {
						replStatus.EXPECT().Pipeline().Return(pipeObj)
					}

					replStatus.EXPECT().SetCustomSettings(mock.Anything).RunAndReturn(func(m map[string]interface{}) {
					})

					return replStatus, nil
				})

			}
			thSvc := service_def.NewThroughputThrottlerSvc(t)
			thSvc.EXPECT().UpdateSettings(mock.Anything).RunAndReturn(func(m map[string]interface{}) map[string]error {
				return nil
			})

			log.DefaultLoggerContext.SetLogLevel(log.LogLevelDebug)
			rm := NewResourceManager(pipeMgr, replSvc, xdcrTopoSvc, thSvc, log.DefaultLoggerContext, backfilleReplSvc)
			rm.previousState = tt.prevState
			rm.cpu = tt.cpu
			rm.SetReplStatsGetter(tt)
			assert.NoError(t, rm.manageResourcesOnce())
		})
	}
}

/*
func Test_computeTokens(t *testing.T) {
	testData := map[string]map[string]*pipelineData{
		"high-low-low": {
			"R-1": {
				Priority: base.PriorityTypeLow,
				Stats: &ReplStats{
					changesLeft:         100,
					docsReceivedFromDcp: 0,
					docsRepQueue:        0,
					timestamp:           time.Now().UnixNano(),
				},
			},
			"R-2": {
				Priority: base.PriorityTypeLow,
				Stats: &ReplStats{
					changesLeft:         100,
					docsReceivedFromDcp: 100,
					docsRepQueue:        100,
					timestamp:           time.Now().UnixNano(),
				},
			},
		},
		/*
			"high-high": {
				"R-1": {
					Priority: base.PriorityTypeHigh,
					Stats: &ReplStats{
						changesLeft:         100000,
						docsReceivedFromDcp: 100000,
						docsRepQueue:        1000,
						timestamp:           time.Now().UnixNano(),
					},
				},
				"R-2": {
					Priority: base.PriorityTypeHigh,
					Stats: &ReplStats{
						changesLeft:         10000,
						docsReceivedFromDcp: 10000,
						docsRepQueue:        100,
						timestamp:           time.Now().UnixNano(),
					},
				},
			},
				"low-low": {
					"R-1": {
						Priority: base.PriorityTypeLow,
						Stats: &ReplStats{
							changesLeft:         100000,
							docsReceivedFromDcp: 100000,
							docsRepQueue:        1000,
							timestamp:           time.Now().UnixNano(),
						},
					},
					"R-2": {
						Priority: base.PriorityTypeLow,
						Stats: &ReplStats{
							changesLeft:         10000,
							docsReceivedFromDcp: 10000,
							docsRepQueue:        100,
							timestamp:           time.Now().UnixNano(),
						},
					},
				},
				"med-med": {
					"R-1": {
						Priority: base.PriorityTypeMedium,
						Stats: &ReplStats{
							changesLeft:         100000,
							docsReceivedFromDcp: 100000,
							docsRepQueue:        1000,
							timestamp:           time.Now().UnixNano(),
						},
					},
					"R-2": {
						Priority: base.PriorityTypeMedium,
						Stats: &ReplStats{
							changesLeft:         10000,
							docsReceivedFromDcp: 10000,
							docsRepQueue:        100,
							timestamp:           time.Now().UnixNano(),
						},
					},
				},
				"med-low": {
					"R-1": {
						Priority: base.PriorityTypeMedium,
						Stats: &ReplStats{
							changesLeft:         100000,
							docsReceivedFromDcp: 100000,
							docsRepQueue:        1000,
							timestamp:           time.Now().UnixNano(),
						},
					},
					"R-2": {
						Priority: base.PriorityTypeLow,
						Stats: &ReplStats{
							changesLeft:         10000,
							docsReceivedFromDcp: 10000,
							docsRepQueue:        100,
							timestamp:           time.Now().UnixNano(),
						},
					},
				},
				"high-high-low": {
					"R-1": {
						Priority: base.PriorityTypeHigh,
						Stats: &ReplStats{
							changesLeft:         100000,
							docsReceivedFromDcp: 100000,
							docsRepQueue:        1000,
							timestamp:           time.Now().UnixNano(),
						},
					},
					"R-2": {
						Priority: base.PriorityTypeHigh,
						Stats: &ReplStats{
							changesLeft:         10,
							docsReceivedFromDcp: 100000,
							docsRepQueue:        10,
							timestamp:           time.Now().UnixNano(),
						},
					},
					"R-3": {
						Priority: base.PriorityTypeLow,
						Stats: &ReplStats{
							changesLeft:         10000,
							docsReceivedFromDcp: 10000,
							docsRepQueue:        100,
							timestamp:           time.Now().UnixNano(),
						},
					},
				},
	}

	for name, data := range testData {
		t.Run(name, func(t *testing.T) {
			doesMedPriorityExists := false
			for _, pdata := range data {
				if pdata.Priority == base.PriorityTypeMedium {
					doesMedPriorityExists = true
					break
				}
			}

			tt := createTestParams(data)

			//CollectReplStats() map[*metadata.GenericSpecification]*ReplStats
			xdcrTopoSvc := service_def.NewXDCRCompTopologySvc(t)
			//xdcrTopoSvc.EXPECT().IsKVNode().Return(true, nil)

			replSvc := service_def.NewReplicationSpecSvc(t)
			replSvc.EXPECT().AllActiveReplicationSpecsReadOnly().RunAndReturn(func() (map[string]*metadata.ReplicationSpecification, error) {
				return tt.Specs, nil
			})

			backfilleReplSvc := service_def.NewBackfillReplSvc(t)
			backfilleReplSvc.EXPECT().AllActiveBackfillSpecsReadOnly().RunAndReturn(func() (map[string]*metadata.BackfillReplicationSpec, error) {
				return tt.BackfillSpecs, nil
			})

			pipeMgr := pipeline_manager.NewPipelineMgrIface(t)
			if doesMedPriorityExists {
				pipeMgr.EXPECT().ReplicationStatus(mock.Anything).RunAndReturn(func(topic string) (pipeline.ReplicationStatusIface, error) {
					_, ok := tt.Pipelines[topic]
					if !ok {
						return nil, fmt.Errorf("repl %s not found", topic)
					}

					replStatus := pipeline_mock.NewReplicationStatusIface(t)

					pipeObj := common_mock.NewPipeline(t)
					pipeObj.EXPECT().UpdateSettings(mock.Anything).RunAndReturn(func(rsm metadata.ReplicationSettingsMap) error {
						return nil
					})

					if strings.HasPrefix(topic, "backfill_") {
						replStatus.EXPECT().BackfillPipeline().Return(pipeObj)
					} else {
						replStatus.EXPECT().Pipeline().Return(pipeObj)
					}

					replStatus.EXPECT().SetCustomSettings(mock.Anything).RunAndReturn(func(m map[string]interface{}) {
					})

					return replStatus, nil
				})

			}
			thSvc := service_def.NewThroughputThrottlerSvc(t)
			thSvc.EXPECT().UpdateSettings(mock.Anything).RunAndReturn(func(m map[string]interface{}) map[string]error {
				return nil
			})

			log.DefaultLoggerContext.SetLogLevel(log.LogLevelDebug)
			rm := NewResourceManager(pipeMgr, replSvc, xdcrTopoSvc, thSvc, log.DefaultLoggerContext, backfilleReplSvc)
			rm.SetReplStatsGetter(tt)
			assert.NoError(t, rm.manageResourcesOnce())
		})
	}
}
*/
