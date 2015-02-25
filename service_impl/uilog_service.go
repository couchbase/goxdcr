// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package service_impl

import (
	"github.com/couchbase/goxdcr/base"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/service_def"
	"github.com/couchbase/goxdcr/utils"
	"time"
)

type UILogSvc struct {
	top_svc service_def.XDCRCompTopologySvc
	logger  *log.CommonLogger
}

func NewUILogSvc(top_svc service_def.XDCRCompTopologySvc, loggerCtx *log.LoggerContext) *UILogSvc {
	service := &UILogSvc{
		top_svc: top_svc,
		logger:  log.NewLogger("UILogService", loggerCtx),
	}

	service.logger.Infof("Created ui log service.\n")
	return service
}

func (service *UILogSvc) Write(message string) {
	if message == "" {
		return
	}

	go service.writeUILog_async(message)
}

func (service *UILogSvc) writeUILog_async(message string) {
	start_time := time.Now()
	defer service.logger.Infof("It took %vs to call writeUILog_async\n", time.Since(start_time).Seconds())
	hostname, err := service.top_svc.MyConnectionStr()
	if err != nil {
		// should never get here
		panic(err.Error())
	}

	paramMap := make(map[string]interface{})
	paramMap[base.UILogComponentKey] = base.UILogXDCRComponent
	paramMap[base.UILogLogLevelKey] = base.UILogXDCRLogLevel
	paramMap[base.UILogMessageKey] = message
	body, _ := utils.EncodeMapIntoByteArray(paramMap)

	err, statusCode := utils.InvokeRestWithRetry(hostname, base.UILogPath, false, base.MethodPost, "", body, 0, service.logger, nil, base.UILogRetry)
	if err != nil {
		service.logger.Errorf("Error writing UI log. err = %v\n", err.Error())
	} else {
		if statusCode != 200 {
			service.logger.Errorf("Error writing UI log. Received status code %v from http response.\n", statusCode)
		}
	}
}
