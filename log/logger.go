package log

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
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

type CommonLogger struct {
	logger  *log.Logger
	context *LoggerContext
}

type LoggerContext struct {
	Log_file  io.Writer
	Log_level LogLevel
}

func CopyCtx(ctx_to_copy *LoggerContext) *LoggerContext {
	return &LoggerContext{Log_file: ctx_to_copy.Log_file,
		Log_level: ctx_to_copy.Log_level}
}

var DefaultLoggerContext = &LoggerContext{os.Stdout, LogLevelInfo}

func NewLogger(module string, logger_context *LoggerContext) *CommonLogger {
	context := DefaultLoggerContext
	if logger_context != nil {
		context = logger_context
	}
	l := log.New(context.Log_file, module, log.Lmicroseconds)
	return &CommonLogger{l, context}
}

func (l *CommonLogger) logMsgf(level LogLevel, prefix string, format string, v ...interface{}) {
	if l.context.Log_level >= level {
		l.logger.Printf(prefix + format, v...)
	}
}

func (l *CommonLogger) logMsg(level LogLevel, prefix string, msg string) {
	if l.context.Log_level >= level {
		l.logger.Println(prefix + msg)
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
