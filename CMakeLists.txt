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
# GODEPSDIR - should point to a Go workspace directory containing all
#      transitive Go dependencies
#
# FORESTDB_INCLUDE_DIR - should point to where libforestdb/forestdb.h exists
#
# FORESTDB_LIBRARY_DIR - should point to where libforestdb.so resides
#
# Optionally:
#
# FORESTDB_TARGET - if set, assumed to the the CMake target name which
# builds libforestdb.so; will set dependencies to ensure it is build
# before attempting to compile indexer

INCLUDE (FindCouchbaseGo)

SET (_forestdb_dep)
IF (DEFINED FORESTDB_TARGET)
  SET (_forestdb_dep DEPENDS ${FORESTDB_TARGET})
ENDIF (DEFINED FORESTDB_TARGET)

# IF wanted to run unit test during build, uncomment below
#add_custom_command(
#  OUTPUT goxdcr_unit_tests
#  COMMAND ${PROJECT_SOURCE_DIR}/run_unit_tests.sh ${PROJECT_SOURCE_DIR} > goxdcr_unit_tests.log
#)

#add_custom_target(run ALL
#  DEPENDS goxdcr_unit_tests
#)

SET (LDFLAGS)
IF (APPLE)
  # On macOS, we must use -rpath to locate libraries
  # TODO: Remove debug info stripping (-s) added to allow 1.7.6 to work on macOS
  SET (LDFLAGS "-s -extldflags '-Wl,-rpath,@executable_path/../lib'")
ENDIF ()

SET (xdcrGoTags "pcre")

SET(CGO_INCLUDE_DIRS "${FORESTDB_INCLUDE_DIR};${PCRE_INCLUDE_DIR};${sigar_SOURCE_DIR}/include")
SET(CGO_LIBRARY_DIRS "${FORESTDB_LIBRARY_DIR};${PCRE_LIBRARY_DIR};${sigar_BINARY_DIR}/src")

GoInstall (TARGET goxdcr PACKAGE github.com/couchbase/goxdcr/main
  GOPATH "${PROJECT_SOURCE_DIR}/../../../.." "${GODEPSDIR}"
  ${_forestdb_dep}
  CGO_INCLUDE_DIRS "${CGO_INCLUDE_DIRS}"
  CGO_LIBRARY_DIRS "${CGO_LIBRARY_DIRS}"
  GOTAGS "${xdcrGoTags}"
  LDFLAGS "${LDFLAGS}"
  INSTALL_PATH bin OUTPUT goxdcr
  GOVERSION 1.11.5)

ADD_DEPENDENCIES(goxdcr sigar)
