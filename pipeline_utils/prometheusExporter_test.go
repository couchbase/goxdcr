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
	"io/ioutil"
	"testing"

	"github.com/couchbase/goxdcr/v8/metadata"
	"github.com/couchbase/goxdcr/v8/service_def"
	"github.com/stretchr/testify/assert"
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
	assert.Equal(float64(23234), stats.GetValue().(float64))
	stats = replStatus["0746d42b7e44e5840dc02a9249efaef0/B0/B2"]
	assert.NotNil(stats)
	assert.Equal(float64(0), stats.GetValue().(float64))

	replStatus = exporter.metricsMap["size_rep_queue"]
	assert.NotNil(replStatus)
	stats = replStatus["0746d42b7e44e5840dc02a9249efaef0/B1/B2"]
	assert.NotNil(stats)
	assert.Equal(float64(1339711), stats.GetValue().(float64))
	stats = replStatus["0746d42b7e44e5840dc02a9249efaef0/B0/B2"]
	assert.NotNil(stats)
	assert.Equal(float64(0), stats.GetValue().(float64))

	_, err = exporter.Export()
	assert.Nil(err)
}

func TestPrometheusRemoteHeartbeat(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPrometheusRemoteHeartbeat =================")
	defer fmt.Println("============== Test case start: TestPrometheusRemoteHeartbeat =================")

	var srcBucketName = "srcBucket"
	var tgtBucketName = "tgtBucket"
	var tgtBucketName2 = "tgtBucket2"

	var srcClusterUuid1 = "abcde"
	var srcClusterName1 = "name1"
	var srcClusterUuid2 = "fghij"
	var srcClusterName2 = "name2"

	spec1, err := metadata.NewReplicationSpecification(srcBucketName, "", tgtBucketName, "", "")
	assert.Nil(err)

	spec2, err := metadata.NewReplicationSpecification(srcBucketName, "", tgtBucketName2, "", "")
	assert.Nil(err)

	// GetHeartbeatsReceivedV1 outputs
	sourceClusterNames := make(map[string]string)
	sourceClusterNames[srcClusterUuid1] = srcClusterName1
	sourceClusterNames[srcClusterUuid2] = srcClusterName2

	sourceSpecs := make(map[string][]*metadata.ReplicationSpecification)
	sourceSpecs[srcClusterUuid1] = []*metadata.ReplicationSpecification{spec1, spec2}
	sourceSpecs[srcClusterUuid2] = []*metadata.ReplicationSpecification{spec1}

	sourceNodes := make(map[string][]string)
	sourceNodes[srcClusterUuid1] = []string{"node1", "node2", "node3", "node4"}
	sourceNodes[srcClusterUuid2] = []string{"node5", "node6", "node7"}

	sourceHbSizes := make(map[string]int64)
	sourceHbSizes[srcClusterUuid1] = 1234
	sourceHbSizes[srcClusterUuid2] = 5678

	// Use already templated data
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
	exporter.LoadSourceClustersInfoV1(sourceClusterNames, sourceSpecs, sourceNodes, sourceHbSizes)

	numNodes := exporter.metricsMap[service_def.SOURCE_CLUSTER_NUM_NODES]
	assert.NotNil(numNodes)
	assert.Len(numNodes, 2)
	// There are 2 clusters, one node for one cluster, two nodes for another cluster
	var fourNodeClusterFound bool
	var threeNodeClusterFound bool
	for _, statPerIdentifier := range numNodes {
		if statPerIdentifier.GetValue() == 4 {
			fourNodeClusterFound = true
		}
		if statPerIdentifier.GetValue() == 3 {
			threeNodeClusterFound = true
		}
	}
	assert.True(threeNodeClusterFound && fourNodeClusterFound)

	numRepls := exporter.metricsMap[service_def.SOURCE_CLUSTER_NUM_REPL]
	assert.NotNil(numRepls)
	assert.Len(numRepls, 2)
	var oneReplFound bool
	var twoReplFound bool
	for _, statPerIdentifier := range numRepls {
		if statPerIdentifier.GetValue() == 1 {
			oneReplFound = true
		}
		if statPerIdentifier.GetValue() == 2 {
			twoReplFound = true
		}
	}
	assert.True(oneReplFound && twoReplFound)

	numHbSize := exporter.metricsMap[service_def.SOURCE_CLUSTER_HB_RECV_SIZE]
	assert.NotNil(numHbSize)
	assert.Len(numHbSize, 2)
	var size1234Found bool
	var size5678Found bool
	for _, statPerIdentifier := range numHbSize {
		if statPerIdentifier.GetValue() == 1234 {
			size1234Found = true
		}
		if statPerIdentifier.GetValue() == 5678 {
			size5678Found = true
		}
	}
	assert.True(size1234Found && size5678Found)
}

func TestPrometheusNumOfReplicationsPerTarget(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPrometheusNumOfReplicationsPerTarget =================")
	defer fmt.Println("============== Test case end: TestPrometheusNumOfReplicationsPerTarget =================")

	var srcBucketName = "srcBucket"
	var tgtBucketName = "tgtBucket"
	var tgtBucketName2 = "tgtBucket2"
	var tgtClusterUUID = "tgtUUID"

	spec1, err := metadata.NewReplicationSpecification(srcBucketName, "", tgtClusterUUID, tgtBucketName, "")
	assert.Nil(err)

	spec2, err := metadata.NewReplicationSpecification(srcBucketName, "", tgtClusterUUID, tgtBucketName2, "")
	assert.Nil(err)

	var replIds []string
	replIds = append(replIds, spec1.Id)
	replIds = append(replIds, spec2.Id)
	var tgtUUIDs []string = []string{tgtClusterUUID}

	// Use already templated data
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
	assert.Nil(exporter.LoadReplicationIds(tgtUUIDs, replIds))

	numEntries := exporter.metricsMap[service_def.TOTAL_REPLICATIONS_COUNT]
	assert.Len(numEntries, 1)

	for _, statPerIdentifier := range numEntries {
		assert.Equal(statPerIdentifier.GetValue(), 2)
	}
}

func TestPrometheusNumOfReplicationsPerTargetTwoTargets(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPrometheusNumOfReplicationsPerTargetTwoTargets =================")
	defer fmt.Println("============== Test case end: TestPrometheusNumOfReplicationsPerTargetTwoTargets =================")

	var srcBucketName = "srcBucket"
	var tgtBucketName = "tgtBucket"
	var tgtBucketName2 = "tgtBucket2"
	var tgtClusterUUID = "tgtUUID"
	var tgtClusterUUID2 = "tgtUUID2"

	spec1, err := metadata.NewReplicationSpecification(srcBucketName, "", tgtClusterUUID, tgtBucketName, "")
	assert.Nil(err)

	spec2, err := metadata.NewReplicationSpecification(srcBucketName, "", tgtClusterUUID, tgtBucketName2, "")
	assert.Nil(err)

	spec3, err := metadata.NewReplicationSpecification(srcBucketName, "", tgtClusterUUID2, tgtBucketName, "")
	assert.Nil(err)

	var replIds []string
	replIds = append(replIds, spec1.Id)
	replIds = append(replIds, spec2.Id)
	replIds = append(replIds, spec3.Id)
	var tgtUUIDs []string = []string{tgtClusterUUID, tgtClusterUUID2}

	// Use already templated data
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
	assert.Nil(exporter.LoadReplicationIds(tgtUUIDs, replIds))

	numEntries := exporter.metricsMap[service_def.TOTAL_REPLICATIONS_COUNT]
	assert.Len(numEntries, 2)

	var oneFound bool
	var twoFound bool
	for _, statPerIdentifier := range numEntries {
		if statPerIdentifier.GetValue() == 2 {
			twoFound = true
		}

		if statPerIdentifier.GetValue() == 1 {
			oneFound = true
		}
	}

	assert.True(twoFound)
	assert.True(oneFound)
}

func TestPrometheusLoadTargetClusterDataUsages(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPrometheusLoadTargetClusterDataUsages =================")
	defer fmt.Println("============== Test case end: TestPrometheusLoadTargetClusterDataUsages =================")

	var tgtClusterUUID = "tgtUUID"

	// Create fake data usages map
	// [0] = dataSent, [1] = dataReceived
	dataUsages := map[string][]int64{
		tgtClusterUUID: {1024, 2048}, // 1KB sent, 2KB received
	}

	exporter := NewPrometheusExporter(service_def.GlobalStatsTable, NewPrometheusLabelsTable)

	// Test initial load
	assert.Nil(exporter.LoadTargetClusterDataUsages(dataUsages))

	// Verify the metric was recorded
	numEntries := exporter.metricsMap[service_def.REMOTE_CLUSTER_MONITORING_METADATA_TX]
	assert.Len(numEntries, 1)

	// Verify the value is the sum of dataSent and dataReceived
	for _, statPerIdentifier := range numEntries {
		// 1024 + 2048 = 3072
		assert.Equal(int64(3072), statPerIdentifier.GetValue())
	}

	// Test that cache works - loading same data again shouldn't change anything
	assert.Nil(exporter.LoadTargetClusterDataUsages(dataUsages))
	assert.Len(exporter.metricsMap[service_def.REMOTE_CLUSTER_MONITORING_METADATA_TX], 1)

	// Test cache invalidation - update data
	dataUsages[tgtClusterUUID] = []int64{2048, 4096}
	assert.Nil(exporter.LoadTargetClusterDataUsages(dataUsages))

	// Verify the value was updated
	numEntries = exporter.metricsMap[service_def.REMOTE_CLUSTER_MONITORING_METADATA_TX]
	for _, statPerIdentifier := range numEntries {
		// 2048 + 4096 = 6144
		assert.Equal(int64(6144), statPerIdentifier.GetValue())
	}
}

func TestPrometheusLoadTargetClusterDataUsagesTwoClusters(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPrometheusLoadTargetClusterDataUsagesTwoClusters =================")
	defer fmt.Println("============== Test case end: TestPrometheusLoadTargetClusterDataUsagesTwoClusters =================")

	var tgtClusterUUID1 = "tgtUUID1"
	var tgtClusterUUID2 = "tgtUUID2"

	// Create fake data usages map with two clusters
	dataUsages := map[string][]int64{
		tgtClusterUUID1: {1024, 2048}, // 1KB sent, 2KB received = 3KB total
		tgtClusterUUID2: {4096, 8192}, // 4KB sent, 8KB received = 12KB total
	}

	exporter := NewPrometheusExporter(service_def.GlobalStatsTable, NewPrometheusLabelsTable)

	assert.Nil(exporter.LoadTargetClusterDataUsages(dataUsages))

	// Verify two entries were created
	numEntries := exporter.metricsMap[service_def.REMOTE_CLUSTER_MONITORING_METADATA_TX]
	assert.Len(numEntries, 2)

	// Verify both clusters have the correct values
	var cluster1Found bool
	var cluster2Found bool
	for _, statPerIdentifier := range numEntries {
		value := statPerIdentifier.GetValue()
		if value == int64(3072) { // 1024 + 2048
			cluster1Found = true
		}
		if value == int64(12288) { // 4096 + 8192
			cluster2Found = true
		}
	}

	assert.True(cluster1Found)
	assert.True(cluster2Found)
}

func TestPrometheusLoadTargetClusterDataUsagesZeroValues(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPrometheusLoadTargetClusterDataUsagesZeroValues =================")
	defer fmt.Println("============== Test case end: TestPrometheusLoadTargetClusterDataUsagesZeroValues =================")

	var tgtClusterUUID = "tgtUUID"

	// Test with zero values
	dataUsages := map[string][]int64{
		tgtClusterUUID: {0, 0},
	}

	exporter := NewPrometheusExporter(service_def.GlobalStatsTable, NewPrometheusLabelsTable)

	assert.Nil(exporter.LoadTargetClusterDataUsages(dataUsages))

	numEntries := exporter.metricsMap[service_def.REMOTE_CLUSTER_MONITORING_METADATA_TX]
	assert.Len(numEntries, 1)

	for _, statPerIdentifier := range numEntries {
		assert.Equal(int64(0), statPerIdentifier.GetValue())
	}
}

func TestPrometheusLoadTargetClusterDataUsagesEmptyMap(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPrometheusLoadTargetClusterDataUsagesEmptyMap =================")
	defer fmt.Println("============== Test case end: TestPrometheusLoadTargetClusterDataUsagesEmptyMap =================")

	// Test with empty map
	dataUsages := map[string][]int64{}

	exporter := NewPrometheusExporter(service_def.GlobalStatsTable, NewPrometheusLabelsTable)

	assert.Nil(exporter.LoadTargetClusterDataUsages(dataUsages))

	// Should create the metric entry but with no clusters
	numEntries := exporter.metricsMap[service_def.REMOTE_CLUSTER_MONITORING_METADATA_TX]
	assert.Len(numEntries, 0)
}

func TestPrometheusLoadTargetClusterDataUsagesCacheInvalidation(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPrometheusLoadTargetClusterDataUsagesCacheInvalidation =================")
	defer fmt.Println("============== Test case end: TestPrometheusLoadTargetClusterDataUsagesCacheInvalidation =================")

	var tgtClusterUUID1 = "tgtUUID1"
	var tgtClusterUUID2 = "tgtUUID2"

	exporter := NewPrometheusExporter(service_def.GlobalStatsTable, NewPrometheusLabelsTable)

	// Initial load with one cluster
	dataUsages := map[string][]int64{
		tgtClusterUUID1: {1024, 2048},
	}
	assert.Nil(exporter.LoadTargetClusterDataUsages(dataUsages))
	assert.Len(exporter.metricsMap[service_def.REMOTE_CLUSTER_MONITORING_METADATA_TX], 1)

	// Add a second cluster - should invalidate cache
	dataUsages[tgtClusterUUID2] = []int64{4096, 8192}
	assert.Nil(exporter.LoadTargetClusterDataUsages(dataUsages))
	assert.Len(exporter.metricsMap[service_def.REMOTE_CLUSTER_MONITORING_METADATA_TX], 2)

	// Remove first cluster - should invalidate cache
	delete(dataUsages, tgtClusterUUID1)
	assert.Nil(exporter.LoadTargetClusterDataUsages(dataUsages))
	assert.Len(exporter.metricsMap[service_def.REMOTE_CLUSTER_MONITORING_METADATA_TX], 1)

	// Verify only second cluster remains
	for _, statPerIdentifier := range exporter.metricsMap[service_def.REMOTE_CLUSTER_MONITORING_METADATA_TX] {
		assert.Equal(int64(12288), statPerIdentifier.GetValue()) // 4096 + 8192
	}
}

func TestPrometheusLoadTargetClusterDataUsagesLargeValues(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestPrometheusLoadTargetClusterDataUsagesLargeValues =================")
	defer fmt.Println("============== Test case end: TestPrometheusLoadTargetClusterDataUsagesLargeValues =================")

	var tgtClusterUUID = "tgtUUID"

	// Test with large values (simulate GB of data)
	dataUsages := map[string][]int64{
		tgtClusterUUID: {1073741824, 2147483648}, // 1GB sent, 2GB received
	}

	exporter := NewPrometheusExporter(service_def.GlobalStatsTable, NewPrometheusLabelsTable)

	assert.Nil(exporter.LoadTargetClusterDataUsages(dataUsages))

	numEntries := exporter.metricsMap[service_def.REMOTE_CLUSTER_MONITORING_METADATA_TX]
	assert.Len(numEntries, 1)

	for _, statPerIdentifier := range numEntries {
		// 1GB + 2GB = 3GB = 3221225472 bytes
		assert.Equal(int64(3221225472), statPerIdentifier.GetValue())
	}
}
