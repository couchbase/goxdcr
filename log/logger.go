// Copyright (c) 2013 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package log

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

type LogLevel int

var GOXDCR_COMPONENT_CODE = "GOXDCR."

const (
	LogLevelFatal LogLevel = iota
	LogLevelError
	LogLevelWarn
	LogLevelInfo
	LogLevelDebug
	LogLevelTrace
)

// log level on UI and rest api
const (
	LOG_LEVEL_FATAL_STR string = "Fatal"
	LOG_LEVEL_ERROR_STR string = "Error"
	LOG_LEVEL_WARN_STR  string = "Warn"
	LOG_LEVEL_INFO_STR  string = "Info"
	LOG_LEVEL_DEBUG_STR string = "Debug"
	LOG_LEVEL_TRACE_STR string = "Trace"
)

// log level in log files
const (
	LOG_LEVEL_FATAL_LOG_STR string = "FATA"
	LOG_LEVEL_ERROR_LOG_STR string = "ERRO"
	LOG_LEVEL_WARN_LOG_STR  string = "WARN"
	LOG_LEVEL_INFO_LOG_STR  string = "INFO"
	LOG_LEVEL_DEBUG_LOG_STR string = "DEBU"
	LOG_LEVEL_TRACE_LOG_STR string = "TRAC"
)

const (
	XdcrLogFileName      = "xdcr.log"
	XdcrTraceLogFileName = "xdcr_trace.log"
	XdcrErrorLogFileName = "xdcr_errors.log"
)

// keep module separate from log.Logger so that we can control its formating
type XdcrLogger struct {
	logger *log.Logger
	module string
}

type CommonLogger struct {
	loggers map[LogLevel]*XdcrLogger
	context *LoggerContext
}

type LoggerContext struct {
	Log_writers map[LogLevel]*LogWriter
	Log_level   LogLevel
}

func (lc *LoggerContext) SetLogLevel(logLevel LogLevel) {
	if lc.Log_level != logLevel {
		// update log level only when necessary, e.g., when explicitly requested by user
		lc.Log_level = logLevel
	}
}

type LogWriter struct {
	writer io.Writer
}

// LogWriter implements io.Writer interface
func (lw *LogWriter) Write(p []byte) (n int, err error) {
	return lw.writer.Write(p)
}

func CopyCtx(ctx_to_copy *LoggerContext) *LoggerContext {
	return &LoggerContext{Log_writers: ctx_to_copy.Log_writers,
		Log_level: ctx_to_copy.Log_level}
}

var DefaultLoggerContext *LoggerContext

// before logging paramters become available, direct all logging to stdout
func init() {
	logWriters := make(map[LogLevel]*LogWriter)
	logWriter := &LogWriter{os.Stdout}
	logWriters[LogLevelFatal] = logWriter
	logWriters[LogLevelError] = logWriter
	logWriters[LogLevelWarn] = logWriter
	logWriters[LogLevelInfo] = logWriter
	logWriters[LogLevelDebug] = logWriter
	logWriters[LogLevelTrace] = logWriter

	DefaultLoggerContext = &LoggerContext{
		Log_writers: logWriters,
		Log_level:   LogLevelInfo,
	}
}

// re-initializes default logger context with runtime logging parameters
// log entries will be written to log files after this point
func Init(logFileDir string, maxLogFileSize, maxNumberOfLogFiles uint64) error {
	// xdcr log file
	xdcrLogFilePath := filepath.Join(logFileDir, XdcrLogFileName)
	xdcrLogWriter, err := NewRotatingLogFileWriter(xdcrLogFilePath, maxLogFileSize, maxNumberOfLogFiles)
	if err != nil {
		return err
	}
	xdcrLogWriterWrapper := DefaultLoggerContext.Log_writers[LogLevelInfo]
	xdcrLogWriterWrapper.writer = xdcrLogWriter

	// xdcr trace log -- both debug level and trace level entries are written to this file
	xdcrTraceFilePath := filepath.Join(logFileDir, XdcrTraceLogFileName)
	xdcrTraceWriter, err := NewRotatingLogFileWriter(xdcrTraceFilePath, maxLogFileSize, maxNumberOfLogFiles)
	if err != nil {
		return err
	}
	xdcrDebugWriterWrapper := DefaultLoggerContext.Log_writers[LogLevelDebug]
	xdcrDebugWriterWrapper.writer = xdcrTraceWriter
	xdcrTraceWriterWrapper := DefaultLoggerContext.Log_writers[LogLevelTrace]
	xdcrTraceWriterWrapper.writer = xdcrTraceWriter

	// xdcr error log file
	xdcrErrorFilePath := filepath.Join(logFileDir, XdcrErrorLogFileName)
	xdcrErrorWriter, err := NewRotatingLogFileWriter(xdcrErrorFilePath, maxLogFileSize, maxNumberOfLogFiles)
	if err != nil {
		return err
	}
	xdcrErrorWriterWrapper := DefaultLoggerContext.Log_writers[LogLevelError]
	xdcrErrorWriterWrapper.writer = xdcrErrorWriter

	return nil
}

func NewLogger(module string, logger_context *LoggerContext) *CommonLogger {
	context := DefaultLoggerContext
	if logger_context != nil {
		context = logger_context
	}
	loggers := make(map[LogLevel]*XdcrLogger)
	for logLevel, logWriter := range context.Log_writers {
		loggers[logLevel] = &XdcrLogger{log.New(logWriter, "", 0), module}
	}
	return &CommonLogger{loggers, context}
}

func (l *CommonLogger) logMsgf(level LogLevel, format string, v ...interface{}) {
	if l.context.Log_level >= level {
		l.loggers[level].logger.Printf(l.processCommonFields(level)+format, v...)
	}
}

func (l *CommonLogger) logMsg(level LogLevel, msg string) {
	if l.context.Log_level >= level {
		l.loggers[level].logger.Println(l.processCommonFields(level) + msg)
	}
}

func (l *CommonLogger) Fatalf(format string, v ...interface{}) {
	l.logMsgf(LogLevelFatal, format, v...)
}

func (l *CommonLogger) Errorf(format string, v ...interface{}) {
	l.logMsgf(LogLevelError, format, v...)
}

func (l *CommonLogger) Warnf(format string, v ...interface{}) {
	l.logMsgf(LogLevelWarn, format, v...)
}

func (l *CommonLogger) Infof(format string, v ...interface{}) {
	l.logMsgf(LogLevelInfo, format, v...)
}

func (l *CommonLogger) Debugf(format string, v ...interface{}) {
	l.logMsgf(LogLevelDebug, format, v...)
}

func (l *CommonLogger) Tracef(format string, v ...interface{}) {
	l.logMsgf(LogLevelTrace, format, v...)
}

func (l *CommonLogger) Fatal(msg string) {
	l.logMsg(LogLevelFatal, msg)
}

func (l *CommonLogger) Error(msg string) {
	l.logMsg(LogLevelError, msg)
}

func (l *CommonLogger) Warn(msg string) {
	l.logMsg(LogLevelWarn, msg)
}

func (l *CommonLogger) Info(msg string) {
	l.logMsg(LogLevelInfo, msg)
}

func (l *CommonLogger) Debug(msg string) {
	l.logMsg(LogLevelDebug, msg)
}

func (l *CommonLogger) Trace(msg string) {
	l.logMsgf(LogLevelTrace, msg)
}

func (l *CommonLogger) LoggerContext() *LoggerContext {
	return l.context
}

func (l *CommonLogger) GetLogLevel() LogLevel {
	return l.context.Log_level
}

func LogLevelFromStr(levelStr string) (LogLevel, error) {
	var level LogLevel
	switch levelStr {
	case LOG_LEVEL_ERROR_STR:
		level = LogLevelError
	case LOG_LEVEL_WARN_STR:
		level = LogLevelWarn
	case LOG_LEVEL_INFO_STR:
		level = LogLevelInfo
	case LOG_LEVEL_DEBUG_STR:
		level = LogLevelDebug
	case LOG_LEVEL_TRACE_STR:
		level = LogLevelTrace
	default:
		return -1, errors.New(fmt.Sprintf("%v is not a valid log level", levelStr))
	}
	return level, nil
}

func (level LogLevel) String() string {
	switch level {
	case LogLevelFatal:
		return LOG_LEVEL_FATAL_STR
	case LogLevelError:
		return LOG_LEVEL_ERROR_STR
	case LogLevelWarn:
		return LOG_LEVEL_WARN_STR
	case LogLevelInfo:
		return LOG_LEVEL_INFO_STR
	case LogLevelDebug:
		return LOG_LEVEL_DEBUG_STR
	case LogLevelTrace:
		return LOG_LEVEL_TRACE_STR
	}
	return ""
}

func (level LogLevel) LogString() string {
	switch level {
	case LogLevelFatal:
		return LOG_LEVEL_FATAL_LOG_STR
	case LogLevelError:
		return LOG_LEVEL_ERROR_LOG_STR
	case LogLevelWarn:
		return LOG_LEVEL_WARN_LOG_STR
	case LogLevelInfo:
		return LOG_LEVEL_INFO_LOG_STR
	case LogLevelDebug:
		return LOG_LEVEL_DEBUG_LOG_STR
	case LogLevelTrace:
		return LOG_LEVEL_TRACE_LOG_STR
	}
	return ""
}

// compose fields that are common to all log messages
// example log entry:
// 2017-01-26T14:21:22.523-08:00 INFO GOXDCR.HttpServer: [xdcr:127.0.0.1:13000] starting ...
func (l *CommonLogger) processCommonFields(level LogLevel) string {
	var buffer bytes.Buffer
	buffer.WriteString(FormatTimeWithMilliSecondPrecision(time.Now()))
	buffer.WriteString(" ")
	buffer.WriteString(level.LogString())
	buffer.WriteString(" ")
	buffer.WriteString(GOXDCR_COMPONENT_CODE)
	buffer.WriteString(l.loggers[level].module)
	buffer.WriteString(": ")
	return buffer.String()
}

// example format: 2015-03-17T10:15:06.717-07:00
func FormatTimeWithMilliSecondPrecision(origTime time.Time) string {
	return origTime.Format("2006-01-02T15:04:05.000Z07:00")
}
