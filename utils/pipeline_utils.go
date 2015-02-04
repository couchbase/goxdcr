package utils

import (
	"errors"
	"fmt"
	"github.com/couchbase/goxdcr/log"
	"runtime"
	"time"
)

type Action func() error

func ExecWithTimeout(action Action, timeout_duration time.Duration, logger *log.CommonLogger) error {
	ret := make(chan error, 1)
	go func(finch chan error) {
		logger.Debug("About to call function")
		err1 := action()
		ret <- err1
	}(ret)

	var retErr error
	timeoutticker := time.NewTicker(timeout_duration)
	for {
		select {
		case retErr = <-ret:
			logger.Infof("Finish executating %v\n", action)
			return retErr
		case <-timeoutticker.C:
			retErr = errors.New(fmt.Sprintf("Executating %v timed out", action))
			logger.Infof("Executating %v timed out", action)
			logger.Info("****************************")
			var buf []byte = make([]byte, 1000000)
			runtime.Stack(buf, true)
			logger.Debugf("%v", string(buf))
			return retErr
		}
	}

}
