package main

import (
	"fmt"
	"time"

	"github.com/couchbase/goxdcr/v8/base"
	baseClog "github.com/couchbase/goxdcr/v8/base/conflictlog"
	"github.com/couchbase/goxdcr/v8/conflictlog"
	"github.com/couchbase/goxdcr/v8/log"
)

type GomemcachedTest struct {
	Bucket     string `json:"bucket"`
	BucketUUID string `json:"bucketUUID"`
	VBCount    int    `json:"vbCount"`
	Address    string `json:"address"`
}

func gomemcachedTest(cfg Config) (err error) {
	logger := log.NewLogger("gomemcTest", log.DefaultLoggerContext)

	conn, err := conflictlog.NewMemcachedConn(logger,
		xsvc.Utils,
		conflictlog.NewManifestCache(),
		cfg.GomemcachedTest.Bucket,
		cfg.GomemcachedTest.BucketUUID,
		cfg.GomemcachedTest.VBCount,
		cfg.GomemcachedTest.Address,
		xsvc.SecuritySvc,
		true)

	if err != nil {
		return
	}

	key := "test1"
	val := []byte(`{"a": 1}`)

	target := baseClog.Target{
		Bucket: "conflicts",
		NS:     base.DefaultCollectionNamespace,
	}

	for i := 0; i < 100; i++ {
		realKey := fmt.Sprintf("%s%d", key, i)
		fmt.Println(">> creating doc:", realKey)
		err = conn.SetMeta(realKey, val, base.JSONDataType, target)
		if err != nil {
			fmt.Println("error in setMeta:", err)
			return
		}

		time.Sleep(30 * time.Second)
	}

	return
}
