// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package interface_test

import (
	common "github.com/couchbase/goxdcr/common"
	xdcrlog "github.com/couchbase/goxdcr/log"
	pipeline_manager "github.com/couchbase/goxdcr/pipeline_manager"
	"log"
	"testing"
	"time"
)

func TestPipeline(t *testing.T) {
	settings := make(map[string]interface{})
	settings["start_int"] = 0
	settings["increase_amount"] = 2
	pipelineMgr := pipeline_manager.PipelineManager(&testPipelineFactory{}, xdcrlog.DefaultLoggerContext)

	ticker := time.NewTicker(600 * time.Millisecond)
	tickCount := 0

	go func() {
		var svc common.PipelineService = nil
		for {
			if svc == nil {
				pipeline := pipelineMgr.Pipeline("ABC")
				if pipeline != nil {
					log.Println("TIKER -- Pipeline is running")
					ctx := pipeline.RuntimeContext()
					if ctx != nil {
						svc = ctx.Service("counter_statistic_collector")
					}
					if svc == nil {
						t.Error("counter_statistic_collector is not a registed service on pipeline runtime")
						t.FailNow()
					}
				}
			}
			if svc != nil {
				log.Println("TICKER---Pipeline is up, start monitoring")
				for now := range ticker.C {
					count := svc.(*testMetricsCollector).MetricsValue()
					log.Printf("TICKER---%s -- %d data is processed\n", now.String(), count)
					tickCount++
					//					log.Printf("TICKER---tickCount is %d\n", tickCount)

				}
				break
			}

		}
	}()

	_, err := pipelineMgr.startPipeline("ABC", settings)

	if err != nil {
		t.Error("Failed to start pipeline ABC")
		t.FailNow()
	}
	log.Println("Done with starting pipeline")

	finchan := make(chan bool)
	go func(finchan chan bool) {
		log.Println("Start timer.....")
		time.Sleep(time.Second * 3)

		log.Println("About to stop ABC")
		pipelineMgr.stopPipeline("ABC")
		ticker.Stop()
		finchan <- true
	}(finchan)

	<-finchan
	log.Println("Succeed")
}
