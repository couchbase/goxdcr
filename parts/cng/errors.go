package cng

import "errors"

var (
	ErrCollectionNotFound = errors.New("collection not found")
	ErrScopeNotFound      = errors.New("scope not found")
	ErrNoReadAccess       = errors.New("no read access to the target collection")
)
