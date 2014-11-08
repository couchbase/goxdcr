CMAKE_MINIMUM_REQUIRED (VERSION 2.8)
PROJECT (goxdcr)

# This project is intended to be built as part of a Couchbase Server build,
# but for nominal support building stand-alone, the following variables
# need to be specified:
#
# CMAKE_MODULE_PATH - should contain a directory containing
#      FindCouchbaseGo.cmake, eg. tlm/cmake/Modules
#
# CMAKE_INSTALL_PREFIX - should point to a directory where the bin/ output
#      directory will be placed
#
# GODEPSPATH - should point to a Go workspace directory containing all
#      transitive Go dependencies

INCLUDE (FindCouchbaseGo)

GoInstall (TARGET goxdcr PACKAGE github.com/couchbase/goxdcr/main
  GOPATH "${PROJECT_SOURCE_DIR}/../../../.." "${GODEPSDIR}"
  INSTALL_PATH bin OUTPUT goxdcr)
