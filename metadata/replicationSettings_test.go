// +build !pcre

/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package metadata

import (
	"fmt"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/stretchr/testify/assert"
	"testing"
)

func setupBoilerPlate() *ReplicationSettings {
	return DefaultReplicationSettings()
}

func TestUpdateCompressionSettings(t *testing.T) {
	assert := assert.New(t)
	settings := setupBoilerPlate()
	fmt.Println("============== Test case start: TestUpdateCompressionSettings =================")

	assert.NotNil(settings)

	compressionSettingMap := make(map[string]interface{})
	// Acceptable values are 0 and 1
	compressionSettingMap[CompressionTypeKey] = 1
	returnedMap, errorMap := settings.UpdateSettingsFromMap(compressionSettingMap)
	assert.Equal(0, len(errorMap))
	assert.Equal(1, len(returnedMap))

	fmt.Println("============== Test case end: TestUpdateCompressionSettings =================")
}

func TestValidateCompressionSetting(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestValidateCompressionSetting =================")

	// enterprise enabled non CAPI - acceptable values are None and Auto.
	converted, err := ValidateAndConvertReplicationSettingsValue(CompressionTypeKey, "None", "", true, false)
	assert.NotNil(converted)
	assert.Nil(err)
	converted, err = ValidateAndConvertReplicationSettingsValue(CompressionTypeKey, "Auto", "", true, false)
	assert.NotNil(converted)
	assert.Nil(err)
	converted, err = ValidateAndConvertReplicationSettingsValue(CompressionTypeKey, "Snappy", "", true, false)
	assert.NotNil(err)

	// Not enterprise - disallow everything except reset
	converted, err = ValidateAndConvertReplicationSettingsValue(CompressionTypeKey, "None", "", false, false)
	assert.NotNil(converted)
	assert.Nil(err)
	converted, err = ValidateAndConvertReplicationSettingsValue(CompressionTypeKey, "Snappy", "", false, false)
	assert.NotNil(err)

	// Invalid is not to be taken
	converted, err = ValidateAndConvertReplicationSettingsValue(CompressionTypeKey, "Invalid", "", true, false)
	assert.NotNil(err)

	// Garbage
	converted, err = ValidateAndConvertReplicationSettingsValue(CompressionTypeKey, "asdf", "", true, false)
	assert.NotNil(err)
	fmt.Println("============== Test case end: TestValidateCompressionSetting =================")
}

func TestDefaultFilterLevel(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestDefaultFilterLevel =================")
	defaultSettings := DefaultReplicationSettings()
	assert.Equal(base.FilterVersionKeyOnly, defaultSettings.Values[FilterVersionKey])
	fmt.Println("============== Test case end: TestDefaultFilterLevel =================")
}

func TestViewOldFilterAsNew(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestViewOldFilterAsNew =================")
	settings := setupBoilerPlate()
	settings.Values[FilterVersionKey] = 0
	var expression string = "^abc"
	// replication_spec_service will call this once loaded from metakv
	settings.FilterExpression = base.UpgradeFilter(expression)
	settings.Values[FilterExpressionKey] = base.UpgradeFilter(expression)

	outMap := settings.ToMap(false /*default*/)

	assert.Equal(base.UpgradeFilter(expression), outMap[FilterExpressionKey])

	fmt.Println("============== Test case end: TestViewOldFilterAsNew =================")
}

// Test <5.5 to >=5.5 upgrade
func TestUpgradeCompressionType(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestUpgradeCompressionType =================")
	settings := ReplicationSettings{}
	settings.Settings = nil // to simulate upgrade

	assert.Equal(0, settings.CompressionType)

	settings.PostProcessAfterUnmarshalling()
	assert.Equal(base.CompressionTypeAuto, settings.CompressionType)
	fmt.Println("============== Test case end: TestUpgradeCompressionType =================")
}

func TestDefaultFilterExpDel(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestDefaultFilterExpDel =================")
	defaultSettings := DefaultReplicationSettings()
	assert.Equal(base.FilterExpDelNone, defaultSettings.Values[FilterExpDelKey])
	getDefaultMode := defaultSettings.GetExpDelMode()
	assert.Equal(base.FilterExpDelNone, getDefaultMode)
	defaultSettings.Settings.Values[base.FilterExpDelKey] = base.FilterExpDelSkipDeletes
	getMode := defaultSettings.GetExpDelMode()
	assert.Equal(base.FilterExpDelSkipDeletes, getMode)
	fmt.Println("============== Test case end: TestDefaultFilterExpDel =================")
}

func TestFlagBasedUpdates(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestFlagBasedUpdates =================")
	existSettings := DefaultReplicationSettings()
	assert.Equal(base.FilterExpDelNone, existSettings.Values[FilterExpDelKey])
	// Incoming map of one flag set
	incomingMap := make(map[string]interface{})
	incomingMap[FilterExpDelKey] = base.FilterExpDelSkipExpiration
	existSettings.UpdateSettingsFromMap(incomingMap)
	assert.Equal(base.FilterExpDelSkipExpiration, existSettings.Values[FilterExpDelKey].(base.FilterExpDelType))

	// Turn off one flag
	existSettings = DefaultReplicationSettings()
	existSettings.Values[FilterExpDelKey] = base.FilterExpDelAllFiltered
	checkMode := existSettings.GetExpDelMode()
	assert.True(checkMode&base.FilterExpDelStripExpiration > 0)
	assert.True(checkMode&base.FilterExpDelSkipDeletes > 0)
	assert.True(checkMode&base.FilterExpDelSkipExpiration > 0)
	incomingMap = make(map[string]interface{})
	var newMode base.FilterExpDelType
	newMode.SetSkipDeletes(true)
	newMode.SetStripExpiration(true)
	newMode.SetSkipExpiration(false)
	incomingMap[FilterExpDelKey] = newMode
	existSettings.UpdateSettingsFromMap(incomingMap)
	checkMode = existSettings.GetExpDelMode()
	assert.True(checkMode&base.FilterExpDelStripExpiration > 0)
	assert.True(checkMode&base.FilterExpDelSkipDeletes > 0)
	assert.False(checkMode&base.FilterExpDelSkipExpiration > 0)

	outMap := existSettings.ToRESTMap(false /*default*/)
	stripExp, ok := outMap[BypassExpiryKey]
	assert.True(ok)
	assert.True(stripExp.(bool))
	filterDel, ok := outMap[FilterDelKey]
	assert.True(ok)
	assert.True(filterDel.(bool))
	filterExp, ok := outMap[FilterExpKey]
	assert.True(ok)
	assert.False(filterExp.(bool))

	restMap := existSettings.ToRESTMap(false /*defaultSettings*/)
	_, ok = restMap[FilterExpDelKey]
	assert.False(ok)

	fmt.Println("============== Test case end: TestFlagBasedUpdates =================")
}

func TestValidateSettingExpDelFlagPersist(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestValidateSettingExpDelFlagPersist =================")
	helper := NewMultiValueHelper()
	var valArr []string = []string{"true"}
	var key string = FilterExpKey
	checkValArr := valArr[0]
	retKey, retValArr, err := helper.CheckAndConvertMultiValue(key, valArr, false)
	assert.Equal(base.FilterExpDelKey, retKey)
	assert.NotEqual(retValArr[0], checkValArr)
	assert.Nil(err)
	assert.Equal(retValArr[0], base.FilterExpDelSkipExpiration.String())
	key = FilterDelKey
	valArr[0] = "true"
	retKey, retValArr, err = helper.CheckAndConvertMultiValue(key, valArr, false)
	var checkVal base.FilterExpDelType
	checkVal.SetSkipDeletes(true)
	checkVal.SetSkipExpiration(true)
	assert.Equal(checkVal.String(), retValArr[0])
	fmt.Println("============== Test case end: TestValidateSettingExpDelFlagPersist =================")
}

func TestMultiValueHelperCheckAndConvert(t *testing.T) {
	assert := assert.New(t)
	fmt.Println("============== Test case start: TestMultiValueHelperCheckAndConvert =================")
	mvHelper := NewMultiValueHelper()
	var valArr []string

	// Assuming user passes in 2 flags, skipDel and skipExp
	key := FilterExpKey
	valArr = append(valArr, "true")
	key, valArr, err := mvHelper.CheckAndConvertMultiValue(key, valArr, false)
	assert.Nil(err)
	assert.NotEqual(FilterExpKey, key)
	assert.NotEqual("true", valArr[0])
	key = FilterDelKey
	valArr[0] = "true"
	key, valArr, err = mvHelper.CheckAndConvertMultiValue(key, valArr, false)
	assert.Nil(err)

	// test export to settingsmap
	settingsMap := make(ReplicationSettingsMap)
	mvHelper.ExportToSettingsMap(settingsMap)
	assert.Equal(1, len(settingsMap))

	// Test import - assuming existing setting already has stripTTL set
	// Combining both should result in ALL
	replSettings := setupBoilerPlate()
	var setVal base.FilterExpDelType
	setVal.SetStripExpiration(true)
	replSettings.Values[FilterExpDelKey] = setVal
	assert.NotEqual(base.FilterExpDelNone, replSettings.Values[FilterExpDelKey].(base.FilterExpDelType))

	changedSettingsMap, errMap := replSettings.UpdateSettingsFromMap(settingsMap)
	assert.Equal(1, len(changedSettingsMap))
	assert.Equal(0, len(errMap))
	assert.Equal(base.FilterExpDelAllFiltered, replSettings.Values[FilterExpDelKey].(base.FilterExpDelType))

	// Assuming all 3 were specified now
	mvHelper = NewMultiValueHelper()
	key = FilterExpKey
	valArr[0] = "false"
	mvHelper.CheckAndConvertMultiValue(key, valArr, false)
	key = FilterDelKey
	valArr[0] = "true"
	mvHelper.CheckAndConvertMultiValue(key, valArr, false)
	key = BypassExpiryKey
	valArr[0] = "false"
	mvHelper.CheckAndConvertMultiValue(key, valArr, false)
	mvHelper.ExportToSettingsMap(settingsMap)
	assert.Equal(1, len(settingsMap))
	replSettings.UpdateSettingsFromMap(settingsMap)
	assert.Equal(base.FilterExpDelSkipDeletes, replSettings.Values[FilterExpDelKey].(base.FilterExpDelType))

	fmt.Println("============== Test case end: TestMultiValueHelperCheckAndConvert =================")
}
