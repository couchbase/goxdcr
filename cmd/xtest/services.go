package main

import (
	"fmt"
	"os"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/couchbase/goxdcr/v8/service_def"
	"github.com/couchbase/goxdcr/v8/service_impl"
	"github.com/couchbase/goxdcr/v8/utils"
)

type XdcrServices struct {
	SecuritySvc service_def.SecuritySvc
	TopSvc      service_def.XDCRCompTopologySvc
	Utils       *utils.Utilities
}

var xsvc *XdcrServices

func InitXdcrServices(cfg *Config) (svc *XdcrServices, err error) {
	securitySvc := service_impl.NewSecurityService(cfg.ClusterCAFile, log.GetOrCreateContext(base.SecuritySvcKey))
	securitySvc = securitySvc.SetClientKeyFile(cfg.ClientKeyFile).SetClientCertFile(cfg.ClientCertFile)
	err = securitySvc.Start()
	if err != nil {
		return
	}

	utils := utils.NewUtilities()
	topSvc, err := service_impl.NewXDCRTopologySvc(
		uint16(cfg.SourceKVAdminPort),
		uint16(cfg.XdcrRestPort),
		true, "required", "optional", securitySvc, log.GetOrCreateContext(base.TopoSvcKey), utils)
	if err != nil {
		fmt.Printf("Error starting xdcr topology service. err=%v\n", err)
		os.Exit(1)
	}

	svc = &XdcrServices{
		SecuritySvc: securitySvc,
		TopSvc:      topSvc,
		Utils:       utils,
	}

	return
}

func initServices(cfg *Config) (err error) {

	return err
}
