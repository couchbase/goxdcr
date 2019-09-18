package memcached

import (
	"encoding/json"
	"fmt"
)

// Collection based filter
type CollectionsFilter struct {
	ManifestUid    uint64
	UseManifestUid bool
	StreamId       uint16
	UseStreamId    bool

	// Use either ScopeId OR CollectionsList, not both
	CollectionsList []uint32
	ScopeId         uint32
}

type nonStreamIdNonResumeScopeMeta struct {
	ScopeId string `json:"scope"`
}

type nonStreamIdResumeScopeMeta struct {
	ManifestId string `json:"uid"`
}

type nonStreamIdNonResumeCollectionsMeta struct {
	CollectionsList []string `json:"collections"`
}

type nonStreamIdResumeCollectionsMeta struct {
	ManifestId      string   `json:"uid"`
	CollectionsList []string `json:"collections"`
}

func (c *CollectionsFilter) IsValid() error {
	if c.UseStreamId {
		return fmt.Errorf("Not implemented yet")
	}
	if c.UseManifestUid {
		return fmt.Errorf("Not implemented yet")
	}

	if len(c.CollectionsList) > 0 && c.ScopeId > 0 {
		return fmt.Errorf("Collection list is specified but scope ID is also specified")
	}

	return nil
}

func (c *CollectionsFilter) ToStreamReqBody() ([]byte, error) {
	if err := c.IsValid(); err != nil {
		return nil, err
	}

	var output interface{}

	switch c.UseStreamId {
	case true:
		// TODO
		return nil, fmt.Errorf("NotImplemented0")
	case false:
		switch c.UseManifestUid {
		case true:
			// TODO
			return nil, fmt.Errorf("NotImplemented1")
		case false:
			switch len(c.CollectionsList) > 0 {
			case true:
				collections := &nonStreamIdNonResumeCollectionsMeta{}
				for _, collectionUint := range c.CollectionsList {
					collections.CollectionsList = append(collections.CollectionsList, fmt.Sprintf("%x", collectionUint))
				}
				output = *collections
			case false:
				output = nonStreamIdNonResumeScopeMeta{ScopeId: fmt.Sprintf("%x", c.ScopeId)}
			}
		}
	}

	data, err := json.Marshal(output)
	if err != nil {
		return nil, err
	} else {
		return data, nil
	}
}
