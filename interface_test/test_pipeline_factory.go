// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package interface_test

import (
	common "github.com/couchbase/goxdcr/common"
	connector "github.com/couchbase/goxdcr/connector"
	pipeline "github.com/couchbase/goxdcr/pipeline"
	pipeline_ctx "github.com/couchbase/goxdcr/pipeline_ctx"
)

type testPipelineFactory struct {
}

func (f *testPipelineFactory) NewPipeline(topic string) (common.Pipeline, error) {

	inNozzle1 := newInComingNozzle("incoming1")
	outNozzle1 := newOutgoingNozzle("out1")
	con1 := connector.NewSimpleConnector("con1", nil, nil)
	part1 := newTestPart("part1")
	con1.AddDownStream("part1", part1)
	con11 := connector.NewSimpleConnector("con11", nil, nil)
	con11.AddDownStream("out1", outNozzle1)
	inNozzle1.SetConnector(con1)
	part1.SetConnector(con11)

	inNozzle2 := newInComingNozzle("incoming2")
	outNozzle2 := newOutgoingNozzle("out2")
	con2 := connector.NewSimpleConnector("con2", nil, nil)
	part2 := newTestPart("part2")
	con2.AddDownStream("part2", part2)
	con22 := connector.NewSimpleConnector("con22", nil, nil)
	con22.AddDownStream("out2", outNozzle2)
	inNozzle2.SetConnector(con2)
	part2.SetConnector(con22)

	sources := make(map[string]common.Nozzle)
	targets := make(map[string]common.Nozzle)
	sources["incoming1"] = inNozzle1
	sources["incoming2"] = inNozzle2
	targets["out1"] = outNozzle1
	targets["out2"] = outNozzle2

	pipeline := pipeline.NewGenericPipeline(topic, sources, targets)

	ctx, err := pipeline_ctx.New(pipeline)
	metricsCollector := NewMetricsCollector()
	
	outNozzle1.RegisterComponentEventListener (common.DataSent, metricsCollector)
	outNozzle2.RegisterComponentEventListener (common.DataSent, metricsCollector)
	
	ctx.RegisterService("error_handler", NewErrorHandler())
	ctx.RegisterService("counter_statistic_collector", metricsCollector)

	pipeline.SetRuntimeContext(ctx)
	return pipeline, err
}
