// Copyright (c) 2018-2019 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package parts

import (
	"fmt"
	"github.com/couchbase/gojsonsm"
	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/base"
	utilities "github.com/couchbase/goxdcr/utils"
)

type FilterIface interface {
	FilterUprEvent(uprEvent *mcc.UprEvent) bool
}

type Filter struct {
	id                       string
	filterExpressionInternal string
	matcher                  gojsonsm.Matcher
	utils                    utilities.UtilsIface
	dp                       utilities.DataPoolIface
	flags                    base.FilterFlagType
}

func NewFilter(id string, filterExpression string, utils utilities.UtilsIface) (*Filter, error) {
	dpPtr := utilities.NewDataPool()
	if dpPtr == nil {
		return nil, base.ErrorNoDataPool
	}

	if len(filterExpression) == 0 {
		return nil, base.ErrorInvalidInput
	}

	filter := &Filter{
		id:                       id,
		filterExpressionInternal: base.ReplaceKeyWordsForExpression(filterExpression),
		utils:                    utils,
		dp:                       dpPtr,
	}

	matcher, err := gojsonsm.GetFilterExpressionMatcher(filter.filterExpressionInternal)
	if err != nil {
		return nil, fmt.Errorf("Unable to parse expression: %v Err: %v", filter.filterExpressionInternal, err.Error())
	}
	filter.matcher = matcher

	if filter.matcher == nil {
		return nil, base.ErrorNoMatcher
	}

	if !base.FilterContainsXattrExpression(filter.filterExpressionInternal) {
		filter.flags |= base.FilterFlagSkipXattr
	}

	if !base.FilterContainsKeyExpression(filter.filterExpressionInternal) {
		filter.flags |= base.FilterFlagSkipKey
	}

	return filter, nil
}

func (filter *Filter) FilterUprEvent(uprEvent *mcc.UprEvent) bool {
	if uprEvent == nil {
		return false
	}

	if uprEvent.Opcode == mc.UPR_DELETION || uprEvent.Opcode == mc.UPR_EXPIRATION {
		// For now, pass through
		return true
	}

	sliceToBeFiltered, err, releaseFunc := filter.utils.ProcessUprEventForFiltering(uprEvent, filter.dp, filter.flags)
	if releaseFunc != nil {
		defer releaseFunc()
	}
	if err != nil {
		return false
	}
	return filter.FilterByteSlice(sliceToBeFiltered)
}

func (filter *Filter) FilterByteSlice(slice []byte) bool {
	defer filter.matcher.Reset()
	match, _ := filter.matcher.Match(slice)
	return match
}
