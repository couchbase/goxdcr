package main

import (
	"fmt"

	"github.com/couchbase/cbauth"
)

type CBAuthTest struct {
	AddrList []string `json:"addrList"`
}

func cbauthTest(cfg Config) (err error) {
	encCfg, err := cbauth.GetClusterEncryptionConfig()
	if err != nil {
		return
	}

	fmt.Printf("encCfg: %#v\n", encCfg)
	for _, addr := range cfg.CBAuthTest.AddrList {
		user, passwd, err := cbauth.GetMemcachedServiceAuth(addr)
		if err != nil {
			return err
		}

		fmt.Printf("addr=%s user=[%s] password=[%s]\n", addr, user, passwd)
	}

	return
}
