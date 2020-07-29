// Copyright (c) 2020 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

// +build enterprise

package functions

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/couchbase/eventing-ee/js-evaluator/defs"
	"github.com/couchbase/eventing-ee/js-evaluator/impl"
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
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

func Execute(libraryName string, functionName string, params []interface{}) (interface{}, error) {
	if evaluator == nil {
		return nil, errors.New("Function evaluator does not exist.")
	}
	// TODO: credentials
	options := map[defs.Option]interface{}{defs.Timeout: 1000000 /* time in nanosecond for function to run */}
	res, err := evaluator.Evaluate(libraryName, functionName, options, params)
	if err.Err != nil {
		return nil, fmt.Errorf("Javascript Evaluate() returned error: %v, error details: %v", err.Err, err.Details)
	} else {
		return res, nil
	}
}
