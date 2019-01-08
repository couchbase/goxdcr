package metadata

import (
	"fmt"
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

	// enterprise enabled non CAPI - acceptable values are 0 and 1
	converted, err := ValidateAndConvertReplicationSettingsValue(CompressionTypeKey, "None", "", true, false)
	assert.NotNil(converted)
	assert.Nil(err)
	converted, err = ValidateAndConvertReplicationSettingsValue(CompressionTypeKey, "Snappy", "", true, false)
	assert.NotNil(converted)
	assert.Nil(err)

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
