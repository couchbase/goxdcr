// +build !pcre

package parts

import (
	"fmt"
	"github.com/couchbase/goxdcr/base"
	utilities "github.com/couchbase/goxdcr/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

var filterId string = "testFilter"
var realUtil *utilities.Utilities = utilities.NewUtilities()

func TestFilter(t *testing.T) {
	fmt.Println("============== Test case start: TestFilterCreation =================")
	assert := assert.New(t)

	filter, err := NewFilter(filterId, "REGEXP_CONTAINS(META().id, \"^abc\")", realUtil)

	assert.Nil(err)
	assert.NotNil(filter)
	assert.Equal(0, len(base.ReservedWordsReplaceMap))

	fmt.Println("============== Test case end: TestFilterCreation =================")
}
