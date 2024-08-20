// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

//go:build ignore

package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/couchbase/goxdcr/v8/service_def"
)

const (
	GlobalStatsTableFile = "../etc/metrics_metadata.json"
)

func GenerateStatsTable() {
	origTable := service_def.GlobalStatsTable

	// ns_server would like us to prepend "xdcr_" in front of each key
	tableToGenerate := make(service_def.StatisticsPropertyMap)
	// Shallow copy the values
	for key, value := range origTable {
		keyStr := fmt.Sprintf("xdcr_%s", key)
		unitStr := service_def.GlobalBaseUnitTable[value.MetricType.Unit]
		if unitStr != "" {
			keyStr = fmt.Sprintf("%s_%s", keyStr, unitStr)
		}
		tableToGenerate[keyStr] = value
	}

	out, err := json.MarshalIndent(tableToGenerate, "", "    ")
	if err != nil {
		fmt.Printf("Error generating stats table: %v", err)
		os.Exit(1)
	}
	err = os.WriteFile(GlobalStatsTableFile, out, 0644)
	if err != nil {
		fmt.Printf("Error writing stats table: %v", err)
		os.Exit(1)
	}
}

func main() {
	for i := 0; i < len(os.Args); i++ {
		if os.Args[i] == "GlobalStatsTable" {
			GenerateStatsTable()
		}
	}
	return
}
