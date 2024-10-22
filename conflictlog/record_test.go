package conflictlog

import (
	"fmt"
	"testing"

	"github.com/couchbase/goxdcr/v8/base"
	"github.com/stretchr/testify/assert"
)

func Test_insertConflictXattrToBody(t *testing.T) {
	var body []byte = []byte(`{"foo":"bar"}`)
	tests := []struct {
		name     string
		body     []byte
		datatype uint8
	}{
		{
			name:     "non-empty json body",
			body:     body,
			datatype: base.JSONDataType,
		},
		{
			name:     "empty json body",
			body:     []byte{},
			datatype: base.JSONDataType,
		},
		{
			name:     "non-empty binary body",
			body:     []byte(`0000`),
			datatype: 0,
		},
		{
			name:     "empty binary body",
			body:     []byte{},
			datatype: 0,
		},
		{
			name: "json body with existing xattrs",
			// The body already has an user xattr "xxdcr_conflict": true
			body: []byte{0, 0, 0, 24, 0, 0, 0, 20, 120, 120, 100,
				99, 114, 95, 99, 111, 110, 102, 108, 105, 99, 116,
				0, 116, 114, 117, 101, 0, 123, 34, 102, 111, 111, 34,
				58, 34, 98, 97, 114, 34, 125},
			datatype: base.JSONDataType | base.XattrDataType,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newBody, newDatatype, err := InsertConflictXattrToBody(tt.body, tt.datatype)
			assert.Nil(t, err)
			assert.Contains(t, string(newBody), fmt.Sprintf(`_xdcr_conflict%ctrue%c`, 0, 0))
			assert.Greater(t, newDatatype&base.XattrDataType, uint8(0))
			assert.Equal(t, newDatatype&tt.datatype, tt.datatype)
		})
	}
}
