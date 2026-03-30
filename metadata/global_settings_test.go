//go:build !pcre
// +build !pcre

/*
Copyright 2025-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package metadata

import (
	"testing"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/stretchr/testify/assert"
)

var connPoolErrorKey = base.RemoteMemcachedConnPoolValidationKey

func TestGlobalSettings_MinConnsCannotExceedMaxConns(t *testing.T) {
	assert := assert.New(t)

	t.Run("min exceeds max should fail", func(t *testing.T) {
		settings := DefaultGlobalSettings()
		_, errorMap := settings.UpdateSettingsFromMap(map[string]interface{}{
			base.RemoteMemcachedConnPoolMaxConnsKey: 5,
			base.RemoteMemcachedConnPoolMinConnsKey: 10,
		})
		assert.NotEmpty(errorMap)
		assert.Contains(errorMap, connPoolErrorKey)
	})

	t.Run("min exceeds default max should fail", func(t *testing.T) {
		settings := DefaultGlobalSettings()
		_, errorMap := settings.UpdateSettingsFromMap(map[string]interface{}{
			base.RemoteMemcachedConnPoolMinConnsKey: base.DefaultRemoteMemcachedConnPoolMaxConns + 1,
		})
		assert.NotEmpty(errorMap)
		assert.Contains(errorMap, connPoolErrorKey)
	})

	t.Run("max below existing min should fail", func(t *testing.T) {
		settings := DefaultGlobalSettings()
		_, errorMap := settings.UpdateSettingsFromMap(map[string]interface{}{
			base.RemoteMemcachedConnPoolMinConnsKey: 5,
		})
		assert.Empty(errorMap)

		_, errorMap = settings.UpdateSettingsFromMap(map[string]interface{}{
			base.RemoteMemcachedConnPoolMaxConnsKey: 3,
		})
		assert.NotEmpty(errorMap)
		assert.Contains(errorMap, connPoolErrorKey)
	})

	t.Run("min equals max should succeed", func(t *testing.T) {
		settings := DefaultGlobalSettings()
		_, errorMap := settings.UpdateSettingsFromMap(map[string]interface{}{
			base.RemoteMemcachedConnPoolMaxConnsKey: 5,
			base.RemoteMemcachedConnPoolMinConnsKey: 5,
		})
		assert.Empty(errorMap)
	})

	t.Run("min below max should succeed", func(t *testing.T) {
		settings := DefaultGlobalSettings()
		_, errorMap := settings.UpdateSettingsFromMap(map[string]interface{}{
			base.RemoteMemcachedConnPoolMaxConnsKey: 10,
			base.RemoteMemcachedConnPoolMinConnsKey: 3,
		})
		assert.Empty(errorMap)
	})
}
