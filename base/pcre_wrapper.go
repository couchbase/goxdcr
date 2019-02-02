// Copyright (c) 2013-2019 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

// +build pcre

package base

import (
	"github.com/glenn-brown/golang-pkg-pcre/src/pkg/pcre"
)

type PcreWrapper struct {
	pcreRegex *pcre.Regexp
}

func MakePcreRegex(expression string) (PcreWrapperInterface, error) {
	pcreRegex := pcre.MustCompile(expression, 0 /*flags*/)
	wrapper := &PcreWrapper{pcreRegex: &pcreRegex}
	return wrapper, nil
}

func (p *PcreWrapper) ReplaceAll(orig, strToSub []byte, flags int) []byte {
	return p.pcreRegex.ReplaceAll(orig, strToSub, flags)
}
