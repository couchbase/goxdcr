// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package service_impl

import (
	"bytes"
	"fmt"
	"github.com/couchbase/cbauth"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/service_def"
	"github.com/couchbase/goxdcr/v8/utils"
	"net/http"
	"strconv"
	"time"
)

const (
	CreateRemoteClusterRefEventDesc     = "Create remote cluster ref"
	UpdateRemoteClusterRefEventDesc     = "Update remote cluster ref"
	DeleteRemoteClusterRefEventDesc     = "Delete remote cluster ref"
	CreateReplicationEventDesc          = "Create replication"
	PauseReplicationEventDesc           = "Pause replication"
	ResumeReplicationEventDesc          = "Resume replication"
	DeleteReplicationEventDesc          = "Delete replication"
	UpdateReplicationSettingDesc        = "Update replication setting"
	UpdateDefaultReplicationSettingDesc = "Update default replication setting"
)

type EventSeverityType string

const (
	EventSeverityInfo  EventSeverityType = "info"
	EventSeverityWarn  EventSeverityType = "warn"
	EventSeverityError EventSeverityType = "error"
	EventSeverityFatal EventSeverityType = "fatal"
)

const (
	// Event field names. The first 5 fields are mandatory. The description field must be 1-80 bytes
	EventTimeStampKey   = "timestamp"
	EventComponentKey   = "component"
	EventSeverityKey    = "severity"
	EventIdKey          = "event_id"
	EventDescriptionKey = "description"
	EventExtraKey       = "extra_attributes"
)
const EventLogPath = "/_event"
const EventXdcrComponent = "xdcr"
const MaxEventLength = 3 * 1024
const EventReportRetry = 5

type EventLogSvc struct {
	topologySvc      service_def.XDCRCompTopologySvc
	utils            utils.UtilsIface
	logger           *log.CommonLogger
	idDescriptionMap map[service_def.EventIdType]string
	idSeverittyMap   map[service_def.EventIdType]EventSeverityType
}

func NewEventLog(topSvc service_def.XDCRCompTopologySvc, utils utils.UtilsIface, logCtx *log.LoggerContext) *EventLogSvc {
	eventLog := EventLogSvc{
		topologySvc: topSvc,
		utils:       utils,
		logger:      log.NewLogger("EventLogSvc", logCtx),
	}
	eventLog.idDescriptionMap = map[service_def.EventIdType]string{
		service_def.CreateRemoteClusterRefSystemEventId:          CreateRemoteClusterRefEventDesc,
		service_def.UpdateRemoteClusterRefSystemEventId:          UpdateRemoteClusterRefEventDesc,
		service_def.DeleteRemoteClusterRefSystemEventId:          DeleteRemoteClusterRefEventDesc,
		service_def.CreateReplicationSystemEventId:               CreateReplicationEventDesc,
		service_def.PauseReplicationSystemEventId:                PauseReplicationEventDesc,
		service_def.ResumeReplicationSystemEventId:               ResumeReplicationEventDesc,
		service_def.DeleteReplicationSystemEventId:               DeleteReplicationEventDesc,
		service_def.UpdateReplicationSettingSystemEventId:        UpdateReplicationSettingDesc,
		service_def.UpdateDefaultReplicationSettingSystemEventId: UpdateDefaultReplicationSettingDesc,
	}
	// Default severity: info
	eventLog.idSeverittyMap = map[service_def.EventIdType]EventSeverityType{}
	return &eventLog
}

func (service *EventLogSvc) WriteEvent(eventId service_def.EventIdType, args map[string]string) {
	if eventId < service_def.MinSystemEventId || eventId > service_def.MaxSystemEventId {
		service.logger.Errorf("Event id %v is out of range.", eventId)
		return
	}
	service.writeEvent_internal(eventId, args)
}

func (service *EventLogSvc) writeEvent_internal(eventId service_def.EventIdType, args map[string]string) {
	stopFunc := service.utils.StartDiagStopwatch("writeEvent_internal", base.DiagInternalThreshold)
	defer stopFunc()
	hostname, err := service.topologySvc.MyConnectionStr()
	if err != nil {
		// should never get here. in case we do, log error and abort
		service.logger.Errorf("Failed to write system event log. err=%v, event_id=%v", err, eventId)
		return
	}
	uuid, err := base.UUIDV4()
	if err != nil {
		service.logger.Warnf("Failed to generate UUIDV4, err=%v, eventId=%v", err, eventId)
		return
	}
	severity, ok := service.idSeverittyMap[eventId]
	if !ok {
		severity = EventSeverityInfo
	}
	body := make([]byte, MaxEventLength)
	pos := 0
	body, pos = base.WriteJsonRawMsg(body, []byte(EventTimeStampKey), pos, base.WriteJsonKey, len(EventTimeStampKey), true)
	// Events timestamp must have the format YYYY-MM-DDThh:mm:ss:SSSZ.
	// To not drop precision in SSSZ when last digits are 0, we have to use 000Z below instead of 999Z
	value := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	body, pos = base.WriteJsonRawMsg(body, []byte(value), pos, base.WriteJsonValue, len(value), false)
	body, pos = base.WriteJsonRawMsg(body, []byte(EventComponentKey), pos, base.WriteJsonKey, len(EventComponentKey), false)
	body, pos = base.WriteJsonRawMsg(body, []byte(EventXdcrComponent), pos, base.WriteJsonValue, len(EventXdcrComponent), false)
	body, pos = base.WriteJsonRawMsg(body, []byte(EventSeverityKey), pos, base.WriteJsonKey, len(EventSeverityKey), false)
	body, pos = base.WriteJsonRawMsg(body, []byte(severity), pos, base.WriteJsonValue, len(severity), false)
	body, pos = base.WriteJsonRawMsg(body, []byte(EventIdKey), pos, base.WriteJsonKey, len(EventIdKey), false)
	value = fmt.Sprintf("%v", eventId)
	body, pos = base.WriteJsonRawMsg(body, []byte(value), pos, base.WriteJsonValueNoQuotes, len(value), false)
	body, pos = base.WriteJsonRawMsg(body, []byte(EventDescriptionKey), pos, base.WriteJsonKey, len(EventDescriptionKey), false)
	description := service.idDescriptionMap[eventId]
	body, pos = base.WriteJsonRawMsg(body, []byte(description), pos, base.WriteJsonValue, len(description), false)
	body, pos = base.WriteJsonRawMsg(body, []byte("uuid"), pos, base.WriteJsonKey, len("uuid"), false)
	body, pos = base.WriteJsonRawMsg(body, []byte(uuid), pos, base.WriteJsonValue, len(uuid), false)
	if len(args) > 0 {
		body, pos = base.WriteJsonRawMsg(body, []byte(EventExtraKey), pos, base.WriteJsonKey, len(EventExtraKey), false)
		isFirstKey := true
		for k, v := range args {
			// To add this field, there are 6 bytes of extras: 4 quotes and 2 separators "...":"...",
			if pos+len(k)+len(v)+6 > MaxEventLength {
				service.logger.Warnf("Skip field %v:%v for event %v because it would exceed maximum length of %v", k, v, eventId, MaxEventLength)
				continue
			}
			body, pos = base.WriteJsonRawMsg(body, []byte(k), pos, base.WriteJsonKey, len(k), isFirstKey)
			body, pos = base.WriteJsonRawMsg(body, []byte(v), pos, base.WriteJsonValue, len(v), false)
			isFirstKey = false
		}
		body[pos] = '}'
		pos++
	}
	body[pos] = '}'
	pos++

	var out interface{}
	err, statusCode := service.invokeRestWithRetryAfter(hostname, EventLogPath, body[:pos])
	if err != nil {
		service.logger.Errorf("Error writing event log for event %v. err = %v\n", eventId, err.Error())
	} else {
		if statusCode != 200 {
			service.logger.Errorf("Error writing Event log for event %v. Received status code %v from http response. Output: %v, Request body: %s", eventId, statusCode, out, body[:pos])
		}
	}
}

// This is for system event log endpoint of ns_server. In case of failure, the response may contain "Retry-After"
// with 1-10 seconds. This routine will parse the response and retry accordingly.
func (service *EventLogSvc) invokeRestWithRetryAfter(baseURL, path string, body []byte) (err error, statusCode int) {
	client := http.Client{}
	reader := bytes.NewReader(body)
	url := service.utils.EnforcePrefix("http://", baseURL)
	req, err := http.NewRequest(base.MethodPost, url+path, reader)
	if err != nil {
		return
	}
	err = cbauth.SetRequestAuth(req)
	if err != nil {
		return
	}
	req.Header.Set(base.ContentType, base.JsonContentType)
	var backoff_time time.Duration = 500 * time.Millisecond
	for i := 0; i < EventReportRetry; i++ {
		res, err := client.Do(req)
		if res != nil {
			statusCode = res.StatusCode
		}
		if err == nil && res != nil {
			if res.StatusCode == http.StatusOK {
				if res.Body != nil {
					res.Body.Close()
				}
				return nil, http.StatusOK
			}
			retryAfter := res.Header.Get("Retry-After")
			if retryAfter != "" {
				secs, err := strconv.Atoi(retryAfter)
				if err == nil {
					backoff_time = time.Duration(secs) * time.Second
				} else {
					backoff_time = backoff_time + backoff_time
				}
			} else {
				backoff_time = backoff_time + backoff_time
			}
		}
		if res != nil && res.Body != nil {
			res.Body.Close()
		}
		if i < EventReportRetry-1 {
			time.Sleep(backoff_time)
		}
	}
	return
}
