/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package evaluator

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/eventing-ee/evaluator/api"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/stretchr/testify/assert"
)

var cas1 = 123456789
var cas2 = 234567890
var id1 = "Cluster1"
var id2 = "Cluster2"
var engineWorkerCount int = base.JSEngineWorkers
var evaluatorQuota int = base.JSWorkerQuota

var functionUrl = fmt.Sprintf(base.FunctionUrlFmt, "127.0.0.1", "8080")
var username = "Administrator"
var password = "wewewe"

var once sync.Once
var sleepTime = 30 * time.Second
var engine = api.Singleton
var workerpool chan api.Worker

// All function files are under ../tools/testScripts/customConflict/
func createFunction(funcName string) (int, []byte, error) {
	fileName := "../../tools/testScripts/customConflict/" + funcName + ".js"
	body, err := ioutil.ReadFile(fileName)
	if err != nil {
		return 0, nil, err
	}
	return createFunctionInner(funcName, body)
}

func createFunctionInner(funcName string, body []byte) (int, []byte, error) {
	funcPath := functionUrl + "/" + funcName
	req, err := http.NewRequest(base.MethodPost, funcPath, bytes.NewBuffer(body))
	req.Close = true
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set(base.ContentType, base.JsonContentType)
	req.SetBasicAuth(username, password)
	resp, err := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	if err != nil {
		fmt.Printf("Create function %v returned error %v\n", funcName, err)
		return 0, nil, err
	}
	if resp.StatusCode == http.StatusBadRequest {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		return resp.StatusCode, bodyBytes, err
	}
	return resp.StatusCode, nil, err
}
func deleteFunction(funcName string) (int, error) {
	funcPath := functionUrl + "/" + funcName
	req, err := http.NewRequest(base.MethodDelete, funcPath, nil)
	req.Close = true
	if err != nil {
		return 0, err
	}
	req.Header.Set(base.ContentType, base.JsonContentType)
	req.SetBasicAuth(username, password)
	resp, err := http.DefaultClient.Do(req)
	resp.Body.Close()
	return resp.StatusCode, err
}

func checkFunction(funcName string) (int, error) {
	funcPath := functionUrl + "/" + funcName
	req, err := http.NewRequest(base.MethodGet, funcPath, nil)
	req.Close = true
	if err != nil {
		return 0, err
	}
	req.Header.Set(base.ContentType, base.JsonContentType)
	req.SetBasicAuth(username, password)
	resp, err := http.DefaultClient.Do(req)
	resp.Body.Close()
	return resp.StatusCode, err
}

func createMultiFunctions(funcPrefix string, count int) (statusCode int, err error) {
	t1 := time.Now()
	bodyFmt := mergeFunctionBodyFmt()
	for i := 0; i < count; i++ {
		funcName := fmt.Sprintf("%v%v", funcPrefix, i)
		body := fmt.Sprintf(bodyFmt, funcName)
		statusCode, _, err = createFunctionInner(funcName, []byte(body))
		if err != nil || statusCode != http.StatusOK {
			return statusCode, err
		}
	}
	collectStats(fmt.Sprintf("After creating %v %v*", count, funcPrefix), time.Since(t1))
	return statusCode, err
}

func checkMultiFunctions(funcPrefix string, count int, expectedReturn int) (res int, err error) {
	for i := 0; i < count; i++ {
		funcName := fmt.Sprintf("%v%v", funcPrefix, i)
		res, err = checkFunction(funcName)
		if err != nil || res != expectedReturn {
			return res, err
		}
	}
	return res, err
}

func deleteMultiFunctions(funcPrefix string, count int) (res int, err error) {
	t1 := time.Now()
	for i := 0; i < count; i++ {
		funcName := fmt.Sprintf("%v%v", funcPrefix, i)
		res, err = deleteFunction(funcName)
		if err != nil || res != http.StatusOK {
			return res, err
		}
	}
	callTime := time.Since(t1)
	runtime.GC()
	debug.FreeOSMemory()
	collectStats(fmt.Sprintf("After Deleting %v %v*", count, funcPrefix), callTime)
	return res, err
}

func mergeFunctionBodyFmt() string {
	var body string
	body = "function " + "%s " + "(key, sourceDoc, sourceCas, sourceId, targetDoc, targetCas, targetId) \n"
	body = body + "{ doc1Js = JSON.parse(sourceDoc);\n"
	body = body + "doc2Js = JSON.parse(targetDoc);\n"
	body = body + "docJs = {};\n"
	// Add some lines to make this function body size to be around 2K
	for len(body) < 2000 {
		body = body + "targetCas = targetCas + 1;\n"
	}
	body = body + "if (sourceCas > targetCas) docJs = {...doc2Js, ...doc1Js};\n"
	body = body + "else docJs = {...doc1Js, ...doc2Js};\n"
	body = body + "let doc = JSON.stringify(docJs);\n"
	return body + "return doc;\n}"
}

// The doc generated will be:
// {"K000000000":"V123456789V12...","K000000001":"V123456789V12...",...}
// nameInc allows caller to specify different names for the docs:
// nameInc=3 will generate the names K000000000, K000000003, K000000006, ...
func generateDoc(docSize int, fieldValueSize int, nameInc int) string {
	maxLen := 20 * 1024 * 1024
	if docSize > maxLen {
		docSize = maxLen
	}
	doc := "{"
	fieldNameFmt := "\"K%09d\":"
	fieldValue := "\"" + strings.Repeat("V123456789", fieldValueSize/10) + "\","
	// fieldName is "F000000001". Length is 10 + 3 for ':' and quotes
	numField := (docSize - 1) / (13 + len(fieldValue))
	for i := 0; i < numField; i++ {
		fieldName := fmt.Sprintf(fieldNameFmt, i*nameInc)
		doc = doc + fieldName + fieldValue
	}
	// Pad last field to make the exact length
	padLen := docSize - len(doc)
	if padLen > 0 {
		doc = strings.TrimSuffix(doc, "\",")
		doc = doc + strings.Repeat("X", padLen) + "\"}"
	} else {
		doc = strings.TrimSuffix(doc, ",")
		doc = doc + "}"
	}
	return doc
}

func handleHTTP(mux *http.ServeMux) {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Printf("Unable to start HTTP server, err : %v\n", err)
	}

	server := &http.Server{
		Addr:    listener.Addr().String(),
		Handler: mux,
	}

	err = server.Serve(listener)
	if err != nil && err != http.ErrServerClosed {
		fmt.Printf("Unable to start HTTP server, err : %v\n", err)
	}
}

func initEvaluator() {
	once.Do(func() {
		runtime.GOMAXPROCS(4)
		if err := InitSigar(); err != nil {
			fmt.Printf("InitSigar returned error %v\n", err)
			os.Exit(1)
		}
		collectStats("Before Evaluator starts", 0)
		workerStr := os.Getenv("EVALUATOR_WORKERS")
		if workerStr != "" {
			workers, err := strconv.Atoi(workerStr)
			if err != nil {
				fmt.Printf("Invalid value $EVALUATOR_WORKERS=%v, use default\n", workerStr)
			} else {
				engineWorkerCount = workers
			}
		}
		workerpool = make(chan api.Worker, engineWorkerCount)
		quotaStr := os.Getenv("EVALUATOR_QUOTA")
		if quotaStr != "" {
			quota, err := strconv.Atoi(quotaStr)
			if err != nil {
				fmt.Printf("Invalid value EVALUATOR_QUOTA=%v, use default\n", quotaStr)
			} else {
				evaluatorQuota = quota
			}
		}
		globalCfg := api.GlobalConfig{}
		api.ConfigureGlobalConfig(globalCfg)

		engConfig := api.StaticEngineConfig{}
		dynamicConfig := api.DynamicEngineConfig{}

		if fault := engine.Initialize(engConfig, dynamicConfig); fault != nil {
			fmt.Printf("Unable to configure engine. err: %v\n", fault.Error())
			os.Exit(1)
		}
		collectStats("Evaluator initialized", 0)
		mux := http.NewServeMux()
		adminService := engine.AdminService()
		mux.HandleFunc(adminService.Path(), adminService.Handler())
		go handleHTTP(mux)

		for i := 0; i < engineWorkerCount; i++ {
			workerInst, fault := engine.Create(uint64(evaluatorQuota))
			if fault != nil {
				fmt.Printf("Create worker %v failed with %v\n", i, fault.Error())
				os.Exit(1)
			}
			workerpool <- workerInst
		}

		collectStats("After Evaluator starts", 0)
	})
}

func execute(funcName string, opt api.Options, params []interface{}) (interface{}, api.Fault) {
	worker := <-workerpool
	defer func() {
		workerpool <- worker
	}()
	var loctr api.Locator
	loctr.FromString(funcName)

	onlyInWorker, onlyInStore, isVersionMismatch := worker.IsStale(loctr)
	if isVersionMismatch || onlyInWorker {
		if fault := worker.Unload(loctr); fault != nil {
			return nil, fault
		}
	}
	if isVersionMismatch || onlyInStore {
		if fault := worker.Load(loctr); fault != nil {
			return nil, fault
		}
	}

	result, fault := worker.Run(nil, funcName, funcName, opt, params...)

	return result, fault
}

type testStats_t struct {
	description  string
	maxProc      int
	alloc        uint64
	totalAlloc   uint64
	sys          uint64
	heapInuse    uint64
	heapIdle     uint64
	heapReleased uint64
	procMemSize  uint64
	procMemRSS   uint64
	cpuPercent   float64
	numThreads   int
	elapseTime   time.Duration
	workerHeaps  []uint64
}

var testStats []testStats_t

func collectStats(msg string, elapseTime time.Duration) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	stats := testStats_t{
		description:  msg,
		alloc:        m.Alloc,
		totalAlloc:   m.TotalAlloc,
		sys:          m.Sys,
		heapInuse:    m.HeapInuse,
		heapIdle:     m.HeapIdle,
		heapReleased: m.HeapReleased,
		elapseTime:   elapseTime,
	}
	if procStats, err := GetProcessRSS(); err == nil {
		stats.procMemRSS = procStats.ProcMemRSS
		stats.procMemSize = procStats.ProcMemSize
	}
	if thrd, err := GetProcessThread(); err == nil {
		stats.numThreads = thrd
	}
	if cpuPercent, err := GetProcessCpu(); err == nil {
		stats.cpuPercent = cpuPercent
	}
	stats.maxProc = runtime.GOMAXPROCS(0)
	if len(workerpool) > 0 {
		for i := 0; i < engineWorkerCount; i++ {
			w := <-workerpool
			heapSize := w.GetHeapSize()
			stats.workerHeaps = append(stats.workerHeaps, heapSize)
			workerpool <- w
		}
	}
	testStats = append(testStats, stats)
}
func printStats() {
	quota := float64(evaluatorQuota)
	unit := "bytes"
	if quota/(1024*1024*1024) >= 1 {
		quota = quota / (1024 * 1024 * 1024)
		unit = "GB"
	} else if quota/(1024*1024) >= 1 {
		quota = quota / (1024 * 1024)
		unit = "MB"
	} else if quota/1024 >= 1 {
		quota = quota / 1024
		unit = "KB"
	}
	fmt.Printf("Test stats (workers = %v, quota = %.1f %v):\n", engineWorkerCount, quota, unit)
	fmt.Println("golangMem = heapInuse + heapIdle - heapReleased")
	fmt.Println("-------------------------------------------------")
	fmt.Printf("%-*s", 35, "Description")
	//fmt.Printf("%*s", 12, "Alloc")
	//fmt.Printf("%*s", 12, "TotalAlloc")
	//fmt.Printf("%*s", 12, "sys")
	//fmt.Printf("%*s", 12, "HeapInUse")
	//fmt.Printf("%*s", 12, "HeapIdle")
	//fmt.Printf("%*s", 13, "HeapReleased")
	fmt.Printf("%*s", 13, "golangMem")
	fmt.Printf("%*s", 13, "ProcMemRSS")
	fmt.Printf("%*s", 13, "ProcMemSize")
	fmt.Printf("%*s", 12, "CpuPercent")
	fmt.Printf("%*s", 12, "NumThreads")
	fmt.Printf("%*s", 16, "ElapseTime")
	fmt.Printf("%*s", 10, "MaxProc")
	fmt.Printf("%*s\n", 18, "WorkerHeapSizes")

	for _, s := range testStats {
		fmt.Printf("%-*v", 35, s.description)
		//fmt.Printf("%*v", 12, s.alloc)
		//fmt.Printf("%*v", 12, s.totalAlloc)
		//fmt.Printf("%*v", 12, s.sys)
		//fmt.Printf("%*v", 12, s.heapInuse)
		//fmt.Printf("%*v", 12, s.heapIdle)
		//fmt.Printf("%*v", 13, s.heapReleased)
		fmt.Printf("%*.1f MB", 10, float64(s.heapInuse+s.heapIdle-s.heapReleased)/(1024*1024))
		fmt.Printf("%*.1f MB", 10, float64(s.procMemRSS)/(1024*1024))
		fmt.Printf("%*.1f MB", 10, float64(s.procMemSize)/(1024*1024))
		fmt.Printf("%*.2f", 12, s.cpuPercent)
		fmt.Printf("%*v", 12, s.numThreads)
		fmt.Printf("%*v", 16, s.elapseTime)
		fmt.Printf("%*v", 10, s.maxProc)
		for i, heap := range s.workerHeaps {
			if i == 0 {
				fmt.Printf("   ")
			} else {
				fmt.Printf(",")
			}
			fmt.Printf("%v", heap)
		}
		fmt.Printf("\n")
	}
}

func doEvaluatorWorkload(t *testing.T, repeat int, funcName string, opt api.Options, params []interface{}) {
	assert := assert.New(t)
	waitGrp := sync.WaitGroup{}
	t1 := time.Now()
	for thrd := 0; thrd < engineWorkerCount; thrd++ {
		waitGrp.Add(1)
		go func(tid int) {
			defer waitGrp.Done()
			for i := 0; i < repeat; i++ {
				_, fault := execute(funcName, opt, params)
				if fault != nil {
					fmt.Printf("%v\n", params)
					fmt.Printf("Thread %v returned fault: %v\n", tid, fault.Error())
				}
				assert.Nil(fault, fmt.Sprintf("Evaluate returned fault: %v for calling thread %v", fault, tid))
			}
		}(thrd)
	}
	waitGrp.Wait()
	evalTime := time.Since(t1)
	collectStats(fmt.Sprintf("%v %v evals per thread", repeat, funcName), evalTime)
}

func doEvaluatorMultiFunctionsWorkload(t *testing.T, prefix string, funcCount int, opt api.Options, params []interface{}) {
	assert := assert.New(t)
	waitGrp := sync.WaitGroup{}
	t1 := time.Now()
	for thrd := 0; thrd < engineWorkerCount; thrd++ {
		waitGrp.Add(1)
		go func(tid int) {
			defer waitGrp.Done()
			for suffix := 0; suffix < funcCount; suffix++ {
				funcName := fmt.Sprintf("%v%v", prefix, suffix)
				_, fault := execute(funcName, opt, params)
				assert.Nil(fault, fmt.Sprintf("Evaluate returned fault: %v for calling thread %v", fault, tid))
			}
		}(thrd)
	}
	waitGrp.Wait()
	evalTime := time.Since(t1)

	collectStats(fmt.Sprintf("After %v*%v evals per thread", prefix, funcCount), evalTime)
}

func TestEvaluatorError(t *testing.T) {
	fmt.Println("\n============= Test case start: TestEvaluatorError =============")
	defer fmt.Println("============= Test case end: TestEvaluatorError =============")
	assert := assert.New(t)
	initEvaluator()
	doc1 := generateDoc(2*1024, 50, 2)
	doc2 := generateDoc(2*1024, 50, 3)
	assert.Equal(2*1024, len(doc1))
	assert.Equal(2*1024, len(doc2))
	opt := api.Options{}
	var params []interface{}
	params = append(params, "key")
	params = append(params, doc1)
	params = append(params, cas1)
	params = append(params, id1)
	params = append(params, doc2)
	params = append(params, cas2)
	params = append(params, id2)
	//1. Function with syntax errors. The function should not be created
	funcName := "TruncatedBody"
	body := "function " + funcName + "(key, sourceDoc, sourceCas, sourceId, targetDoc, targetCas, targetId) \n"
	body = body + "{ doc1Js = JSON.parse(sourceDoc);\n"
	body = body + "doc2Js = JSON.parse(targetDoc);\n"
	body = body + "docJs = {};\n"
	body = body + "if (sourceCas > targetCas) docJs = {...doc2Js, ...doc1Js};\n"
	body = body + "else docJs = {...doc1Js, ...doc2Js};\n"
	body = body + "let doc = JSON.stringify(docJs);\n"
	res, bodyBytes, err := createFunctionInner(funcName, []byte(body))
	assert.Nil(err)
	assert.Equal(http.StatusBadRequest, res)
	fmt.Printf("----- Creating functions with syntax errors returned: -----\n%s\n", bodyBytes)
	res, err = checkFunction(funcName)
	assert.Nil(err)
	assert.Equal(http.StatusNotFound, res)

	//2. Function that loops forever.
	fmt.Printf("----- Function eval timeout test: -----\n")
	funcName = "loopForever"
	res, _, err = createFunction(funcName)
	assert.Nil(err)
	assert.Equal(http.StatusOK, res)
	// Function is created
	res, err = checkFunction(funcName)
	assert.Nil(err)
	assert.Equal(http.StatusOK, res)
	opt.Timeout = 3000 // 3s
	result, fault := execute(funcName, opt, params)
	fmt.Printf("funcName: %v \nresult: %v\nFault: %v\n", funcName, result, fault.Error())
	assert.NotNil(fault)

	// 3. Functions that causes exceptions
	fmt.Printf("----- Function with unexpected input causing exception: -----\n")
	funcName = "simpleMerge"
	res, _, err = createFunction(funcName)
	assert.Nil(err)
	assert.Equal(http.StatusOK, res)
	res, err = checkFunction(funcName)
	assert.Nil(err)
	assert.Equal(http.StatusOK, res)
	params[1] = "A string"
	result, fault = execute(funcName, opt, params)
	fmt.Printf("\nfuncName: %v \nresult: %v, \nerror: %v\n", funcName, result, fault.Error())
	assert.NotNil(fault)
}

func TestEvaluatorWorkloadSingleFunctionLargeDocs(t *testing.T) {
	fmt.Println("\n============= Test case start: TestEvaluatorWorkloadSingleFunctionLargeDocs =============")
	defer fmt.Println("============= Test case end: TestEvaluatorWorkloadSingleFunctionLargeDocs =============")
	assert := assert.New(t)
	initEvaluator()

	doc1 := generateDoc(1*1024*1024, 10000, 2)
	doc2 := generateDoc(1*1024*1024, 10000, 3)
	assert.Equal(1*1024*1024, len(doc1))
	assert.Equal(1*1024*1024, len(doc2))
	evaluatorWorkloadSingleFunc(doc1, doc2, t)
}

func TestEvaluatorWorkloadSingleFunctionSmallDocs(t *testing.T) {
	fmt.Println("\n============= Test case start: TestEvaluatorWorkloadSingleFunctionSmallDocs =============")
	defer fmt.Println("============= Test case end: TestEvaluatorWorkloadSingleFunctionSmallDocs =============")
	assert := assert.New(t)
	initEvaluator()

	doc1 := generateDoc(2*1024, 50, 2)
	doc2 := generateDoc(2*1024, 50, 3)
	assert.Equal(2*1024, len(doc1))
	assert.Equal(2*1024, len(doc2))
	evaluatorWorkloadSingleFunc(doc1, doc2, t)
}

func evaluatorWorkloadSingleFunc(doc1, doc2 string, t *testing.T) {
	assert := assert.New(t)
	opt := api.Options{}
	var params []interface{}
	params = append(params, "key")
	params = append(params, doc1)
	params = append(params, cas1)
	params = append(params, id1)
	params = append(params, doc2)
	params = append(params, cas2)
	params = append(params, id2)

	// start with clean slate
	runtime.GC()
	debug.FreeOSMemory()
	collectStats("Start of SingleFunc test", 0)
	funcName := "func0"
	prefix := "func"
	count := 1
	res, err := createMultiFunctions(prefix, count)
	assert.Nil(err)
	assert.Equal(http.StatusOK, res)
	res, err = checkFunction(funcName)
	assert.Nil(err)
	assert.Equal(http.StatusOK, res)

	doEvaluatorWorkload(t, 1, funcName, opt, params)
	doEvaluatorWorkload(t, 10, funcName, opt, params)
	doEvaluatorWorkload(t, 100, funcName, opt, params)
	doEvaluatorWorkload(t, 1000, funcName, opt, params)

	res, err = deleteFunction(funcName)
	assert.Nil(err)
	assert.Equal(http.StatusOK, res)
	res, err = checkFunction(funcName)
	assert.Nil(err)
	assert.Equal(http.StatusNotFound, res)
	runtime.GC()
	debug.FreeOSMemory()
	collectStats("After dropping func", 0)
	if sleepTime > 0 {
		time.Sleep(sleepTime)
		collectStats(fmt.Sprintf("After sleep %v", sleepTime), sleepTime)
	}
	printStats()
}
func TestEvaluatorWorkloadMultiFunctionsSmallDoc(t *testing.T) {
	fmt.Println("\n============= Test case start: TestEvaluatorWorkloadMultiFunctionsSmallDoc =============")
	defer fmt.Println("============= Test case end: TestEvaluatorWorkloadMultiFunctionsSmallDoc =============")
	assert := assert.New(t)
	initEvaluator()
	doc1 := generateDoc(2*1024, 50, 2)
	doc2 := generateDoc(2*1024, 50, 3)
	assert.Equal(2*1024, len(doc1))
	assert.Equal(2*1024, len(doc2))

	opt := api.Options{}
	evaluatorWorkloadMultiFuncs(doc1, doc2, opt, t)
}

func TestEvaluatorWorkloadMultiFunctionsLargeDocs(t *testing.T) {
	fmt.Println("\n============= Test case start: TestEvaluatorWorkloadMultiFunctionsLargeDocs =============")
	defer fmt.Println("============= Test case end: TestEvaluatorWorkloadMultiFunctionsLargeDocs =============")
	assert := assert.New(t)
	initEvaluator()
	// If we have too many json fields, the evaluation may take a long time and timeout after 5s
	largeDoc1 := generateDoc(1*1024*1024, 10000, 2)
	largeDoc2 := generateDoc(1*1024*1024, 10000, 3)
	assert.Equal(1*1024*1024, len(largeDoc1))
	assert.Equal(1*1024*1024, len(largeDoc2))
	opt := api.Options{}
	// In a busy system, OS context switch may cause longer execution time.
	// Timeout > 10s has been observed with 10 workers and 1000 functions
	opt.Timeout = 60000 // 60s
	evaluatorWorkloadMultiFuncs(largeDoc1, largeDoc2, opt, t)
}

func evaluatorWorkloadMultiFuncs(doc1, doc2 string, opt api.Options, t *testing.T) {
	assert := assert.New(t)
	var params []interface{}
	params = append(params, "key")
	params = append(params, doc1)
	params = append(params, cas1)
	params = append(params, id1)
	params = append(params, doc2)
	params = append(params, cas2)
	params = append(params, id2)

	// start with clean slate
	runtime.GC()
	debug.FreeOSMemory()
	collectStats("Test", 0)

	counts := [7]int{1, 10, 100, 1000, 100, 10, 1}

	prefix := "func"
	for _, count := range counts {
		res, err := createMultiFunctions(prefix, count)
		assert.Nil(err)
		assert.Equal(http.StatusOK, res)
		res, err = checkMultiFunctions(prefix, count, http.StatusOK)
		assert.Nil(err)
		assert.Equal(http.StatusOK, res)
		doEvaluatorMultiFunctionsWorkload(t, prefix, count, opt, params)
		res, err = deleteMultiFunctions(prefix, count)
		assert.Nil(err)
		assert.Equal(http.StatusOK, res)
	}

	if sleepTime > 0 {
		time.Sleep(sleepTime)
		collectStats(fmt.Sprintf("After sleep %v", sleepTime), sleepTime)
	}
	printStats()
}
func TestEvaluatorWorkloadSlowFunction(t *testing.T) {
	fmt.Println("\n============= Test case start: TestEvaluatorWorkloadSlowFunction =============")
	defer fmt.Println("============= Test case end: TestEvaluatorWorkloadSlowFunction =============")
	assert := assert.New(t)
	initEvaluator()
	doc1 := generateDoc(2*1024, 50, 2)
	doc2 := generateDoc(2*1024, 50, 3)
	assert.Equal(2*1024, len(doc1))
	assert.Equal(2*1024, len(doc2))
	opt := api.Options{}
	var params []interface{}
	params = append(params, "key")
	params = append(params, doc1)
	params = append(params, cas1)
	params = append(params, id1)
	params = append(params, doc2)
	params = append(params, cas2)
	params = append(params, id2)

	// start with clean slate
	runtime.GC()
	debug.FreeOSMemory()
	collectStats("Start of slow function test", 0)
	funcName := "slowFunc"
	res, _, err := createFunction(funcName)
	assert.Nil(err)
	assert.Equal(http.StatusOK, res)
	res, err = checkFunction(funcName)
	assert.Nil(err)
	assert.Equal(http.StatusOK, res)

	doEvaluatorWorkload(t, 1, funcName, opt, params)
	doEvaluatorWorkload(t, 10, funcName, opt, params)
	doEvaluatorWorkload(t, 30, funcName, opt, params)

	res, err = deleteFunction(funcName)
	assert.Nil(err)
	assert.Equal(http.StatusOK, res)
	res, err = checkFunction(funcName)
	assert.Nil(err)
	assert.Equal(http.StatusNotFound, res)
	runtime.GC()
	debug.FreeOSMemory()
	collectStats("After dropping func", 0)
	//fileName := "SlowFuncPprof.out"
	//f, err := os.Create(fileName)
	//if err != nil {
	//	fmt.Printf("Failed to open %v\n", fileName)
	//} else {
	//	pprof.WriteHeapProfile(f)
	//	f.Close()
	//}
	if sleepTime > 0 {
		time.Sleep(sleepTime)
		collectStats(fmt.Sprintf("After sleep %v", sleepTime), sleepTime)
	}
	printStats()
}
