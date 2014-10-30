package interface_test

import (
	common "github.com/Xiaomei-Zhang/goxdcr/common"
	pipeline_manager "github.com/Xiaomei-Zhang/goxdcr/pipeline_manager"
	xdcrlog "github.com/Xiaomei-Zhang/goxdcr/log"
	"testing"
	"time"	
	"log"
)


func TestPipeline(t *testing.T) {
	settings := make(map[string]interface{})
	settings["start_int"] = 0
	settings["increase_amount"] = 2
	pipeline_manager.PipelineManager(&testPipelineFactory{}, xdcrlog.DefaultLoggerContext)

	ticker := time.NewTicker(600 * time.Millisecond)
	tickCount := 0

	go func() {
		var svc common.PipelineService = nil
		for {
			if svc == nil {
				pipeline := pipeline_manager.Pipeline("ABC")
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

	_, err := pipeline_manager.StartPipeline("ABC", settings)

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
		pipeline_manager.StopPipeline("ABC")
		ticker.Stop()
		finchan <- true
	}(finchan)

	<-finchan
	log.Println("Succeed")
}
