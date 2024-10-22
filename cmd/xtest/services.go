package main

import (
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/service_def"
	"github.com/couchbase/goxdcr/v8/service_impl"
)

type XdcrServices struct {
	SecuritySvc service_def.SecuritySvc
}

func InitXdcrServices(cfg *Config) (svc *XdcrServices, err error) {
	securitySvc := service_impl.NewSecurityService(cfg.ClusterCAFile, log.GetOrCreateContext(base.SecuritySvcKey))
	securitySvc = securitySvc.SetClientKeyFile(cfg.ClientKeyFile).SetClientCertFile(cfg.ClientCertFile)
	err = securitySvc.Start()
	if err != nil {
		return
	}

	svc = &XdcrServices{
		SecuritySvc: securitySvc,
	}

	return
}

func initServices(cfg *Config) (err error) {

	return err
}
