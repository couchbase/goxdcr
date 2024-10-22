package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/conflictlog"
	"github.com/couchbase/goxdcr/v8/log"
)

type TestStruct struct {
	Id  string
	Str string `json:"str"`
}

// ConflictLogLoadTest is the config for load testing of the
// conflict logging
type ConflictLogLoadTest struct {
	// ConnLimit limits the number of connections
	ConnLimit int `json:"connLimit"`

	// IOPSLimit is max allowed IOPS to the source cluster
	IOPSLimit int64 `json:"iopsLimit"`

	DefaultLoggerOptions *ConflictLoggerOptions `json:"defaultLoggerOptions"`

	// Loggers define multiple logger loads to run simultaneuosly
	Loggers map[string]*ConflictLoggerOptions `json:"loggers"`
}

type ConflictLoggerOptions struct {
	// Target is the target conflict bucket details
	Target base.ConflictLogTarget `json:"target"`

	// DocSizeRange is the min and max size in bytes of the source & target documents
	// in a conflict
	DocSizeRange [2]int `json:"docSizeRange"`

	// DocLoadCount is the count of conflicts to log
	// Note: the actual count of docs written will be much larger
	DocLoadCount int `json:"docLoadCount"`

	// Batch count is the count of conflicts are logged before
	// waiting for all of them
	BatchCount int `json:"batchCount"`

	// XMemCount simulates the XMemNozzle count which calls the Log()
	XMemCount int `json:"xmemCount"`

	// ----- Logger configuration -----
	// LogQueue is the logger's internal channel capacity
	LogQueue int `json:"logQueue"`

	// WorkerCount is the logger's spawned worker count
	WorkerCount int `json:"workerCount"`

	// SetMeta timeout in milliseconds
	SetMetaTimeout int `json:"setMetaTimeout"`

	// GetPool timeout in milliseconds
	GetPoolTimeout int `json:"getPoolTimeout"`
}

func genRandomJson(id string, min, max int) ([]byte, error) {
	n := min + rand.Intn(max-min)
	t := TestStruct{
		Id:  id,
		Str: RandomString[:n],
	}

	buf, err := json.Marshal(&t)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func createCRDDoc(prefix string, i int, min, max int) (crd *conflictlog.ConflictRecord, err error) {
	sourceDocId := fmt.Sprintf("%s-%d", prefix, i)
	sourceBuf, err := genRandomJson(sourceDocId, min, max)
	if err != nil {
		return
	}

	targetBuf, err := genRandomJson(sourceDocId, min, max)
	if err != nil {
		return
	}

	crd = &conflictlog.ConflictRecord{
		Source: conflictlog.DocInfo{
			Id:       sourceDocId,
			Body:     sourceBuf,
			Datatype: base.JSONDataType,
		},
		Target: conflictlog.DocInfo{
			Id:       sourceDocId,
			Body:     targetBuf,
			Datatype: base.JSONDataType,
		},
	}

	return
}

func runLoggerLoad(wg *sync.WaitGroup, logger *log.CommonLogger, opts *ConflictLoggerOptions) (err error) {
	m, err := conflictlog.GetManager()
	if err != nil {
		return
	}

	logger.Infof("target: %v", opts.Target)

	options := []conflictlog.LoggerOpt{
		conflictlog.WithMapper(conflictlog.NewFixedMapper(logger, opts.Target)),
		conflictlog.WithCapacity(opts.LogQueue),
		conflictlog.WithWorkerCount(opts.WorkerCount),
	}

	if opts.SetMetaTimeout > 0 {
		options = append(options, conflictlog.WithSetMetaTimeout(time.Duration(opts.SetMetaTimeout)*time.Millisecond))
	}

	if opts.GetPoolTimeout > 0 {
		options = append(options, conflictlog.WithPoolGetTimeout(time.Duration(opts.GetPoolTimeout)*time.Millisecond))
	}

	clog, err := m.NewLogger(logger, "1234", options...)
	if err != nil {
		return
	}

	docCountPerXmem := opts.DocLoadCount / opts.XMemCount
	finch := make(chan bool, 1)

	logger.Infof("xmemCount=%d, docCountPerXmem=%d", opts.XMemCount, docCountPerXmem)

	for j := 0; j < opts.XMemCount; j++ {
		wg.Add(1)
		n := j
		go func() {
			defer wg.Done()

			docIdPrefix := fmt.Sprintf("xmem-doc-%d", n)
			logger.Infof("starting xmem with keyPrefix=%s", docIdPrefix)

			handles := []base.ConflictLoggerHandle{}
			for i := 0; i < docCountPerXmem; i++ {
				if i%opts.BatchCount == 0 {
					for _, h := range handles {
						err = h.Wait(finch)
						if err != nil {
							logger.Errorf("error in conflict log err=%v", err)
							return
						}
					}

					handles = []base.ConflictLoggerHandle{}
				}

				min, max := opts.DocSizeRange[0], opts.DocSizeRange[1]
				crd, err := createCRDDoc(docIdPrefix, i, min, max)
				if err != nil {
					logger.Errorf("error in creating crd doc err=%v", err)
					return
				}

				logger.Debugf("writing to conflict log")
				h, err := clog.Log(crd)
				if err != nil {
					logger.Errorf("error in sending conflict log err=%v", err)
					if err == conflictlog.ErrQueueFull {
						continue
					}
					return
				}

				handles = append(handles, h)
			}

			for _, h := range handles {
				err = h.Wait(finch)
				if err != nil {
					logger.Errorf("error in conflict log err=%v", err)
					return
				}
			}
		}()
	}

	return
}

func conflictLogLoadTest(cfg Config) (err error) {
	opts := cfg.ConflictLogPertest

	m, err := conflictlog.GetManager()
	if err != nil {
		return
	}

	m.SetConnLimit(opts.ConnLimit)
	m.SetIOPSLimit(opts.IOPSLimit)

	wg := &sync.WaitGroup{}
	logger := log.NewLogger("conflictLoadTest", log.DefaultLoggerContext)

	start := time.Now()

	for name, loggerOpts := range opts.Loggers {
		wg.Add(1)
		name := name
		loggerOpts := loggerOpts
		go func() {
			defer wg.Done()
			err := runLoggerLoad(wg, logger, loggerOpts)
			if err != nil {
				logger.Errorf("run logger load failed, name=%s, err=%v", name, err)
			}
		}()
	}

	wg.Wait()

	end := time.Now()

	connCount := m.ConnPool().Count()
	fmt.Printf("connCount in pool: %d\n", connCount)
	fmt.Printf("Finished in %s\n", end.Sub(start))

	return nil
}
