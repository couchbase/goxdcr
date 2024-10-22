package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/service_impl/throttlerSvcImpl"
)

type ThrottlerTest struct {
	Limit          int64 `json:"limit"`
	ItemsToProcess int   `json:"itemsToProcess"`
	WorkerCount    int   `json:"workerCount"`
}

func tworker(wg *sync.WaitGroup, svc *throttlerSvcImpl.ThroughputThrottler, workch chan bool) {
	defer wg.Done()
	for ok := range workch {
		if !ok {
			break
		}

		done := false
		for !done {
			done = svc.CanSend(false)
			if !done {
				svc.Wait()
			}
		}
	}
}

func throttlerTest(cfg Config) (err error) {
	logger := log.NewLogger("throttlerTest", log.DefaultLoggerContext)
	svc := throttlerSvcImpl.NewThroughputThrottlerSvc(logger.LoggerContext())
	svc.Start()

	svc.UpdateSettings(map[string]interface{}{
		"LowTokens": cfg.ThrottlerTest.Limit,
	})

	start := time.Now()
	wg := &sync.WaitGroup{}
	workch := make(chan bool, cfg.ThrottlerTest.ItemsToProcess)
	for i := 0; i < cfg.ThrottlerTest.WorkerCount; i++ {
		wg.Add(1)
		go tworker(wg, svc, workch)
	}

	for i := 0; i < cap(workch); i++ {
		workch <- true
	}

	close(workch)
	fmt.Println("waiting")
	wg.Wait()
	end := time.Now()

	fmt.Printf("elapsed time: %s\n", end.Sub(start))

	return
}
