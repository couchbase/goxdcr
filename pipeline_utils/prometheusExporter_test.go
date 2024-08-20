/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package pipeline_utils

import (
	"encoding/json"
	"expvar"
	"fmt"
	"github.com/couchbase/goxdcr/v8/service_def"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

func TestPrometheusExpVarParseMap(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPrometheusExpVarParseMap	=================")
	defer fmt.Println("============== Test case start: TestPrometheusExpVarParseMap =================")

	expVarMap := &expvar.Map{}
	expVarMap.Add("testInt", 12)
	expVarMap.AddFloat("testFloat", 13.3)

	parseMap := make(ExpVarParseMapType)
	assert.False(parseMap.CheckNoKeyChanges(expVarMap))

	parseMap["testInt"] = 12
	assert.False(parseMap.CheckNoKeyChanges(expVarMap))

	parseMap["testFloat"] = 13.3
	assert.True(parseMap.CheckNoKeyChanges(expVarMap))

	subExpVarMap := &expvar.Map{}
	varString := expvar.String{}
	varString.Set("String")
	subExpVarMap.Set("subString", &varString)
	expVarMap.Set("subMap", subExpVarMap)
	assert.False(parseMap.CheckNoKeyChanges(expVarMap))

	subMap := make(ExpVarParseMapType)
	parseMap["subMap"] = subMap
	assert.False(parseMap.CheckNoKeyChanges(expVarMap))

	subMap["subString"] = "String"
	parseMap["subMap"] = subMap
	assert.True(parseMap.CheckNoKeyChanges(expVarMap))

	exporter := NewPrometheusExporter(nil, nil)
	exporter.LoadExpVarMap(expVarMap)
	assert.Equal(int64(12), exporter.expVarParseMap["testInt"].(int64))
	assert.Equal(float64(13.3), exporter.expVarParseMap["testFloat"].(float64))
	assert.Equal("String", exporter.expVarParseMap["subMap"].(ExpVarParseMapType)["subString"].(string))
}

func TestPrometheusParseMapToMetricMap(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPrometheusParseMapToMetricMap =================")
	defer fmt.Println("============== Test case start: TestPrometheusParseMapToMetricMap =================")

	// This file is a shortcut produced by converting expVar.Map into a ExpVarParseMapType, and then saved to a file
	// If the "LoadExpVarMap" function logic is changed, this file will need to be regenerated
	expVarMarshalledBytes, err := ioutil.ReadFile("testdata/expVarStatsDump.json")
	if err != nil {
		panic(err.Error())
	}
	expVarParseMap := make(map[string]interface{})
	err = json.Unmarshal(expVarMarshalledBytes, &expVarParseMap)
	if err != nil {
		panic(err.Error())
	}

	assert.NotNil(expVarParseMap)

	// Unit test unmarshal will unmarshal them into map[string]interface{}
	// Need to re-convert
	convertedMap := make(ExpVarParseMapType)
	for k, v := range expVarParseMap {
		convertedMap[k] = ExpVarParseMapType(v.(map[string]interface{}))
	}

	exporter := NewPrometheusExporter(service_def.GlobalStatsTable, NewPrometheusLabelsTable)
	exporter.expVarParseMap = convertedMap

	exporter.LoadMetricsMap(true)

	assert.Equal(len(exporter.metricsMap), len(service_def.GlobalStatsTable))

	replStatus := exporter.metricsMap["changes_left"]
	assert.NotNil(replStatus)
	stats := replStatus["0746d42b7e44e5840dc02a9249efaef0/B1/B2"]
	assert.NotNil(stats)
	assert.Equal(float64(23234), stats.Value.(float64))
	stats = replStatus["0746d42b7e44e5840dc02a9249efaef0/B0/B2"]
	assert.NotNil(stats)
	assert.Equal(float64(0), stats.Value.(float64))

	replStatus = exporter.metricsMap["size_rep_queue"]
	assert.NotNil(replStatus)
	stats = replStatus["0746d42b7e44e5840dc02a9249efaef0/B1/B2"]
	assert.NotNil(stats)
	assert.Equal(float64(1339711), stats.Value.(float64))
	stats = replStatus["0746d42b7e44e5840dc02a9249efaef0/B0/B2"]
	assert.NotNil(stats)
	assert.Equal(float64(0), stats.Value.(float64))

	_, err = exporter.Export()
	assert.Nil(err)
}
