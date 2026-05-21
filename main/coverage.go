//go:build go_coverage

package main

import (
	"github.com/couchbase/gocoverage"
	"github.com/couchbase/goxdcr/v8/log"
)

func init() {
	logger := log.NewLogger("CodeCov", log.DefaultLoggerContext)
	gocoverage.SetLogger(func(format string, args ...any) {
		logger.Infof(format, args...)
	})
}
