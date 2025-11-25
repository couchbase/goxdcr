package main

import "github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"

type PushDocumentReq struct {
	Key         string            `json:"key"`
	Bucket      string            `json:"bucket"`
	Scope       string            `json:"scope"`
	Collection  string            `json:"collection"`
	Body        string            `json:"body"`
	StoreCas    int64             `json:"storeCas"`
	Revno       uint64            `json:"revno"`
	ContentType string            `json:"contentType"`
	Flags       int32             `json:"flags"`
	Expiry      *int64            `json:"expiry"`
	IsDeleted   bool              `json:"isDeleted"`
	Xattrs      map[string]string `json:"xattrs"`
}

type GetVbucketInfoReqOnce struct {
	BucketName     string   `json:"bucket_name"`
	IncludeHistory bool     `json:"include_history"`
	IncludeMaxCas  bool     `json:"include_max_cas"`
	VbucketIds     []uint32 `json:"vbucket_ids"`
	VBCount        int      `json:"vb_count"`
}

type Config struct {
	Addr     string `json:"addr"`
	Username string `json:"username"`
	Password string `json:"password"`
	CertFile string `json:"certfile"`

	GetBucketInfoReq         *internal_xdcr_v1.GetBucketInfoRequest
	WatchCollectionsReq      *internal_xdcr_v1.WatchCollectionsRequest
	GetVBucketInfoReq        *internal_xdcr_v1.GetVbucketInfoRequest
	GetVBucketInfoReqOnceReq *GetVbucketInfoReqOnce
	PushDocumentReq          *PushDocumentReq
}
