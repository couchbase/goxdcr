package main

import (
	"encoding/json"
	"fmt"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/conflictlog"
	"github.com/couchbase/goxdcr/v8/log"
)

type GocbcoreTest struct {
	Addr   string                 `json:"addr"`
	Target base.ConflictLogTarget `json:"target"`
	Count  int                    `json:"count"`
	Key    string                 `json:"key"`
	Doc    map[string]interface{} `json:"doc"`
}

func gocbcoreTest(cfg Config) (err error) {
	opts := cfg.GocbcoreTest

	logger := log.NewLogger("gocbcoreTest", log.DefaultLoggerContext)

	memdAddrGetter := &MemAddrGetter{
		addr: opts.Addr,
	}

	conn, err := conflictlog.NewGocbConn(logger, memdAddrGetter, opts.Target.Bucket, nil)
	if err != nil {
		return
	}

	buf, err := json.Marshal(opts.Doc)
	if err != nil {
		return
	}

	for i := 0; i < opts.Count; i++ {
		key := fmt.Sprintf("%s-%d", opts.Key, i)
		err = conn.SetMeta(key, buf, base.JSONDataType, opts.Target)
		if err != nil {
			return
		}
	}

	return
}
