package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/couchbase/goxdcr/v8/conflictlog"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/utils"

	"net/http"
	_ "net/http/pprof"
)

type MemAddrGetter struct {
	addr string
}

type EncLevelGetter struct {
	strict bool
}

func (e *EncLevelGetter) IsMyClusterEncryptionLevelStrict() bool {
	return e.strict
}

func (m *MemAddrGetter) MyMemcachedAddr() (string, error) {
	return m.addr, nil
}

func loadConfigFile(filepath string) (cfg Config, err error) {
	f, err := os.Open(filepath)
	if err != nil {
		return
	}

	d := json.NewDecoder(f)

	err = d.Decode(&cfg)
	if err != nil {
		return
	}

	return
}

func main() {
	if len(os.Args[1:]) < 1 {
		fmt.Println("missing config json file")
		os.Exit(1)
	}

	var name string
	if len(os.Args[1:]) > 1 {
		name = os.Args[2]
	}

	configFile := os.Args[1]
	cfg, err := loadConfigFile(configFile)
	if err != nil {
		fmt.Printf("failed to load config file err=%v\n", err)
		os.Exit(1)
	}

	if name == "" {
		name = cfg.Name
	}

	fmt.Printf("cfg=%v\n", cfg)

	go func() {
		err := http.ListenAndServe(fmt.Sprintf(":%d", cfg.DebugPort), nil)
		fmt.Printf("failed to launch debug prof server, err=%v\n", err)
	}()

	logLevel, err := log.LogLevelFromStr(cfg.LogLevel)
	if err != nil {
		fmt.Printf("error=%v\n", err)
		return
	}

	log.DefaultLoggerContext.SetLogLevel(logLevel)

	addrGetter := &MemAddrGetter{
		addr: cfg.MemcachedAddr,
	}

	xsvc, err := InitXdcrServices(&cfg)
	if err != nil {
		fmt.Printf("error in initing services: %v\n", err)
		os.Exit(1)
	}

	utils := utils.NewUtilities()

	conflictlog.InitManager(log.DefaultLoggerContext, utils, addrGetter, xsvc.SecuritySvc)

	switch name {
	case "conflictLogLoadTest":
		err = conflictLogLoadTest(cfg)
	case "gocbcoreTest":
		err = gocbcoreTest(cfg)
	case "throttlerTest":
		err = throttlerTest(cfg)
	case "cbauthTest":
		err = cbauthTest(cfg)
	default:
		fmt.Println("error: unknown config name =", cfg.Name)
	}

	if err != nil {
		fmt.Printf("error=%v\n", err)
	}
}
