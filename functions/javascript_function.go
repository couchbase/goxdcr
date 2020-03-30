package functions

import (
	"errors"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/eventing-ee/js-evaluator/defs"
	"github.com/couchbase/eventing-ee/js-evaluator/impl"
	"net/http"
)

var logger *log.CommonLogger = log.NewLogger("Functions", log.DefaultLoggerContext)
var evaluator defs.Evaluator

func Init(sourceKVHost string, xdcrRestPort uint16) {
	engine := impl.NewEngine()

	config := make(map[defs.Config]interface{})
	config[defs.WorkersPerNode] = base.JSEngineWorkersPerNode
	config[defs.ThreadsPerWorker] = base.JSEngineThreadsPerWorker
	config[defs.NsServerURL] = base.GetHostAddr(sourceKVHost, xdcrRestPort)

	err := engine.Configure(config)
	if err.Err != nil {
		logger.Errorf("Unable to configure engine. err: %v", err.Err)
		return
	}

	handle := engine.UIHandler()
	http.HandleFunc("/functions/", handle.Handler())

	err = engine.Start()
	if err.Err != nil {
		logger.Errorf("Unable to start engine. err: %v", err.Err)
		return
	}

	evaluator = engine.Fetch()
	if evaluator == nil {
		logger.Errorf("Unable to fetch javascript evaluator.")
	} else {
		logger.Infof("Javascript evaluator started with %v worker and %v threads each.", config[defs.WorkersPerNode], config[defs.ThreadsPerWorker])
	}
}

func Execute(libraryName string, functionName string, params[]interface{}) (interface{}, error) {
	if evaluator == nil {
		return nil, errors.New("Function evaluator does not exist.")
	}
	// TODO: credentials
	options := map[defs.Option]interface{}{defs.Timeout: 1000000 /* time in nanosecond for function to run */}
	res, err := evaluator.Evaluate(libraryName, functionName, options, params)
	if err.Err != nil {
		logger.Errorf("%v %v", err.Err, err.Details)
		return nil, err.Err
	} else {
		return res, nil
	}
}
