// Copyright 2019-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

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
