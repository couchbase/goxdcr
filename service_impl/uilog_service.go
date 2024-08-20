// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_impl

import (
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/service_def"
	utilities "github.com/couchbase/goxdcr/v8/utils"
	"time"
)

type UILogSvc struct {
	top_svc service_def.XDCRCompTopologySvc
	logger  *log.CommonLogger
	utils   utilities.UtilsIface
}

func NewUILogSvc(top_svc service_def.XDCRCompTopologySvc, loggerCtx *log.LoggerContext, utilsIn utilities.UtilsIface) *UILogSvc {
	service := &UILogSvc{
		top_svc: top_svc,
		logger:  log.NewLogger("UILogSvc", loggerCtx),
		utils:   utilsIn,
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
		// should never get here. in case we do, log error and abort
		service.logger.Warnf("Failed to write ui log. err=%v, message=%v", err, message)
		return
	}

	paramMap := make(map[string]interface{})
	paramMap[base.UILogComponentKey] = base.UILogXDCRComponent
	paramMap[base.UILogLogLevelKey] = base.UILogXDCRLogLevel
	paramMap[base.UILogMessageKey] = message
	body, _ := service.utils.EncodeMapIntoByteArray(paramMap)

	err, statusCode := service.utils.InvokeRestWithRetry(hostname, base.UILogPath, false, base.MethodPost, "", body, 0, nil, nil, false, service.logger, base.UILogRetry)
	if err != nil {
		service.logger.Errorf("Error writing UI log. err = %v\n", err.Error())
	} else {
		if statusCode != 200 {
			service.logger.Errorf("Error writing UI log. Received status code %v from http response.\n", statusCode)
		}
	}
}
