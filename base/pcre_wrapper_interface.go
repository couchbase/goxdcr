// Copyright 2019 Couchbase, Inc. All rights reserved.

package base

type PcreWrapperInterface interface {
	ReplaceAll(orig, strToSubstitute []byte, flags int) []byte
}
