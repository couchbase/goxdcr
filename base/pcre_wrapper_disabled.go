// Copyright 2019 Couchbase, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

// +build !pcre

package base

import (
	"regexp"
	"strings"
)

type PcreWrapper struct {
}

func MakePcreRegex(expression string) (PcreWrapperInterface, error) {
	if strings.Contains(expression, ExternalKeyKey) {
		return MakeMetaKeyWorkaround(), nil
	} else if strings.Contains(expression, ExternalKeyXattr) {
		return MakeMetaXattrWorkaround(), nil
	} else {
		return &PcreWrapper{}, ErrorNotSupported
	}
}

func (p *PcreWrapper) ReplaceAll(orig, strToSub []byte, flags int) []byte {
	// Do nothing just return the original bytes
	return orig
}

type PcreMetaKeyWorkaround struct {
}

// Note, this is not meant to be a direct replacement of pcre. This is only supported for if PCRE is not supported to do META() conversion
func MakeMetaKeyWorkaround() PcreWrapperInterface {
	return &PcreMetaKeyWorkaround{}
}

func (p *PcreMetaKeyWorkaround) ReplaceAll(orig, strToSub []byte, flags int) []byte {
	keyRegex := regexp.MustCompile(ExternalKeyKey)
	retBytes := keyRegex.ReplaceAllLiteral(orig, strToSub)
	return retBytes
}

type PcreMetaXattrWorkaround struct {
}

func MakeMetaXattrWorkaround() PcreWrapperInterface {
	return &PcreMetaXattrWorkaround{}
}

func (p *PcreMetaXattrWorkaround) ReplaceAll(orig, strToSub []byte, flags int) []byte {
	xattrRegex := regexp.MustCompile(ExternalKeyXattr)
	retBytes := xattrRegex.ReplaceAllLiteral(orig, strToSub)

	return retBytes
}
