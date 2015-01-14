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
	"errors"
	"fmt"
	"log"
	"os"
	"io"
	"path/filepath"
)

type LogLevel int

const (
	LogLevelError LogLevel = iota
	// LogLevelInfo log messages for info
	LogLevelInfo
	// LogLevelDebug log messages for info and debug
	LogLevelDebug
	// LogLevelTrace log messages info, debug and trace
	LogLevelTrace
)

const (
	LOG_LEVEL_ERROR_STR string = "Error"
	LOG_LEVEL_INFO_STR  string = "Info"
	LOG_LEVEL_DEBUG_STR string = "Debug"
	LOG_LEVEL_TRACE_STR string = "Trace"
)

const (
	XdcrLogFileName = "xdcr.log"
	XdcrTraceLogFileName = "xdcr_trace.log"
	XdcrErrorLogFileName = "xdcr_errors.log"
)

type CommonLogger struct {
	loggers  map[LogLevel]*log.Logger
	context *LoggerContext
}

type LoggerContext struct {
	Log_writers  map[LogLevel] *LogWriter
	Log_level LogLevel
}

type LogWriter struct {
	writer  io.Writer
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
	logWriters := make(map[LogLevel] *LogWriter)
	logWriter := &LogWriter{os.Stdout}
	logWriters[LogLevelInfo] = logWriter
	logWriters[LogLevelDebug] = logWriter
	logWriters[LogLevelTrace] = logWriter
	logWriters[LogLevelError] = logWriter
	
	DefaultLoggerContext = &LoggerContext {
		Log_writers: logWriters,
		Log_level: LogLevelInfo,
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
	loggers := make(map[LogLevel]*log.Logger)
	for logLevel, logWriter := range context.Log_writers {
		loggers[logLevel] = log.New(logWriter, module, log.Lmicroseconds)
	}
	return &CommonLogger{loggers, context}
}

func (l *CommonLogger) logMsgf(level LogLevel, prefix string, format string, v ...interface{}) {
	if l.context.Log_level >= level {
		l.loggers[level].Printf(prefix + format, v...)
	}
}

func (l *CommonLogger) logMsg(level LogLevel, prefix string, msg string) {
	if l.context.Log_level >= level {
		l.loggers[level].Println(prefix + msg)
	}
}

func (l *CommonLogger) Infof(format string, v ...interface{}) {
	l.logMsgf(LogLevelInfo, "[INFO] ", format, v...)
}

func (l *CommonLogger) Debugf(format string, v ...interface{}) {
	l.logMsgf(LogLevelDebug, "[DEBUG] ", format, v...)
}

func (l *CommonLogger) Tracef(format string, v ...interface{}) {
	l.logMsgf(LogLevelTrace, "[TRACE] ", format, v...)
}

func (l *CommonLogger) Errorf(format string, v ...interface{}) {
	l.logMsgf(LogLevelError, "[ERROR] ", format, v...)
}

func (l *CommonLogger) Info(msg string) {
	l.logMsg(LogLevelInfo, "[INFO] ", msg)
}

func (l *CommonLogger) Debug(msg string) {
	l.logMsg(LogLevelDebug, "[DEBUG] ", msg)
}

func (l *CommonLogger) Trace(msg string) {
	l.logMsgf(LogLevelTrace, "[TRACE] ", msg)
}

func (l *CommonLogger) Error(msg string) {
	l.logMsg(LogLevelError, "[ERROR] ", msg)
}

func (l *CommonLogger) LoggerContext() *LoggerContext {
	return l.context
}

func (l *CommonLogger) GetLogLevel () LogLevel {
	return l.context.Log_level
}

func LogLevelFromStr(levelStr string) (LogLevel, error) {
	var level LogLevel
	switch levelStr {
	case LOG_LEVEL_ERROR_STR:
		level = LogLevelError
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
	case LogLevelError:
		return LOG_LEVEL_ERROR_STR
	case LogLevelInfo:
		return LOG_LEVEL_INFO_STR
	case LogLevelDebug:
		return LOG_LEVEL_DEBUG_STR
	case LogLevelTrace:
		return LOG_LEVEL_TRACE_STR
	}
	return ""
}


