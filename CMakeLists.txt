# Copyright 2014-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

CMAKE_MINIMUM_REQUIRED (VERSION 3.13)
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

# To compile with injection, run at the top level:
# make clean; make -j12 EXTRA_CMAKE_OPTIONS=-DXDCR_INJECTION_FRAMEWORK=ON
OPTION (XDCR_INJECTION_FRAMEWORK "Include developer injection framework" OFF)

SET (LDFLAGS)

IF (BUILD_ENTERPRISE)
   MESSAGE(STATUS "Building EE edition of XDCR")
   IF (XDCR_INJECTION_FRAMEWORK)
      MESSAGE(STATUS "XDCR with injection framework turned on")
      SET (xdcrGoTags "pcre enterprise dev")
   ELSE (XDCR_INJECTION_FRAMEWORK)
      MESSAGE(STATUS "XDCR with injection framework turned off")
      SET (xdcrGoTags "pcre enterprise")
   ENDIF (XDCR_INJECTION_FRAMEWORK)
   SET (NEWEVAL_DIR ../eventing-ee/evaluator)
   SET(_eval_no_default_path NO_DEFAULT_PATH)
ELSE (BUILD_ENTERPRISE)
   MESSAGE(STATUS "Building CE edition of XDCR")
ENDIF (BUILD_ENTERPRISE)

GET_FILENAME_COMPONENT (CURL_LIBRARY_DIR "${CURL_LIBRARIES}" DIRECTORY)

SET(CGO_INCLUDE_DIRS "${FORESTDB_INCLUDE_DIR};${sigar_SOURCE_DIR}/include;${NEWEVAL_INCLUDE_DIR};${PCRE_INCLUDE_DIR};${CURL_INCLUDE_DIR}")
SET(CGO_LIBRARY_DIRS "${FORESTDB_LIBRARY_DIR};${sigar_BINARY_DIR}/src;${evaluator_BINARY_DIR};${PCRE_LIBRARY_DIR};${CURL_LIBRARY_DIR}")

GoModBuild(TARGET goxdcr PACKAGE github.com/couchbase/goxdcr/v8/main
  INSTALL_PATH bin OUTPUT goxdcr
  CGO_INCLUDE_DIRS "${CGO_INCLUDE_DIRS}"
  CGO_LIBRARY_DIRS "${CGO_LIBRARY_DIRS}"
  GOTAGS "${xdcrGoTags}"
  LDFLAGS "${LDFLAGS}"
  GOVERSION SUPPORTED_NEWER)

ADD_DEPENDENCIES(goxdcr sigar n1ql-yacc)
IF (BUILD_ENTERPRISE)
  ADD_DEPENDENCIES (goxdcr jseval)
ENDIF ()

# Unit tests for each individual goxdcr module
foreach (unitTestModule backfill_manager base base/conflictlog base/filter conflictlog crMeta factory hlv metadata metadata_svc parts peerToPeer pipeline pipeline_svc pipeline_manager pipeline_utils service_impl streamApiWatcher utils base/iopool service_impl/throttlerSvcImpl)
  GoTest (TARGET goxdcr-go-test-${unitTestModule}
  PACKAGE github.com/couchbase/goxdcr/v8/${unitTestModule}
  GOARGS -short
  GOTAGS dev
  GOVERSION SUPPORTED_NEWER)

  # Set working directory to go.mod file location
  set_tests_properties(goxdcr-go-test-${unitTestModule} PROPERTIES
      WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}")
endforeach()

AddMetricsMetadata (JSON etc/metrics_metadata.json COMPONENT goxdcr)

GoModTidySetup()
