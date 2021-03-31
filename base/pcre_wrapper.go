// Copyright 2013-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

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
