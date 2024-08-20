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

IF (BUILD_ENTERPRISE)
   MESSAGE(STATUS "Building EE edition of XDCR")
   SET (xdcrGoTags "pcre enterprise")
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

AddMetricsMetadata (JSON etc/metrics_metadata.json COMPONENT goxdcr)
