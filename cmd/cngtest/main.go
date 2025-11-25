package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/couchbase/goprotostellar/genproto/internal_xdcr_v1"
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/utils"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("Usage: <rpc> <config_file>\n")
		fmt.Printf("<rpc> = getBucketInfo|watchCollections|getVBucketInfo|getVBucketInfoOnce|pushDocument\n")
		fmt.Printf("        rpc name is case insensitive\n")
		os.Exit(1)
	}

	rpcName := os.Args[1]
	cfgfile := os.Args[2]

	cfg := Config{}

	buf, err := os.ReadFile(cfgfile)
	if err != nil {
		log.Fatal(err)
	}

	err = json.Unmarshal(buf, &cfg)
	if err != nil {
		log.Fatal(err)
	}

	credsFn := func() *base.Credentials {
		return &base.Credentials{
			UserName_: cfg.Username,
			Password_: cfg.Password,
		}
	}

	cert, err := os.ReadFile(cfg.CertFile)
	if err != nil {
		log.Fatal("unable to read certfile", err)
	}

	opts, err := base.NewGrpcOptions(cfg.Addr, credsFn, cert, false)
	if err != nil {
		log.Fatal(err)
	}

	conn, err := base.NewCngConn(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	utilsSvc := utils.NewUtilities()

	rpcName = strings.ToLower(rpcName)

	switch rpcName {
	case "getbucketinfo":
		if cfg.GetBucketInfoReq == nil {
			log.Fatalf("GetBucketInfoReq should be nil for GetBucketInfo rpc\n")
		}
		err = getBucketInfo(conn, &cfg)
	case "watchcollections":
		if cfg.WatchCollectionsReq == nil {
			log.Fatalf("WatchCollectionsReq cannot be nil for WatchCollections rpc\n")
		}
		err = watchCollections(conn, &cfg)
	case "getvbucketinfo":
		if cfg.GetVBucketInfoReq == nil {
			log.Fatalf("GetVBucketInfoReq cannot be nil for GetVBucketInfo rpc\n")
		}
		err = getVbucketInfo(conn, &cfg)
	case "getvbucketinfoonce":
		if cfg.GetVBucketInfoReqOnceReq == nil {
			log.Fatalf("GetVBucketInfoOnceReq cannot be nil for GetVBucketInfoOnce rpc\n")
		}
		err = getVbucketInfoOnce(utilsSvc, conn, &cfg)
	case "pushdocument":
		if cfg.PushDocumentReq == nil {
			log.Fatalf("PushDocumentReq cannot be nil for PushDocument rpc\n")
		}
		err = pushDocument(conn, &cfg)
	default:
		log.Fatalf("Unknown rpc name %s\n", rpcName)
	}

	if err != nil {
		log.Fatal(err)
	}
}

func getVbucketInfoOnce(utils utils.UtilsIface, conn *base.CngConn, cfg *Config) (err error) {
	vBucketIds := cfg.GetVBucketInfoReqOnceReq.VbucketIds

	if cfg.GetVBucketInfoReqOnceReq.VBCount > 0 {
		vBucketIds = make([]uint32, cfg.GetVBucketInfoReqOnceReq.VBCount)
		for i := 0; i < cfg.GetVBucketInfoReqOnceReq.VBCount; i++ {
			vBucketIds[i] = uint32(i)
		}
	}

	req := &internal_xdcr_v1.GetVbucketInfoRequest{
		BucketName:     cfg.GetVBucketInfoReqOnceReq.BucketName,
		IncludeHistory: &cfg.GetVBucketInfoReqOnceReq.IncludeHistory,
		IncludeMaxCas:  &cfg.GetVBucketInfoReqOnceReq.IncludeMaxCas,
		VbucketIds:     vBucketIds,
	}

	rsp, err := utils.CngGetVbucketInfoOnce(context.Background(), conn.Client(), req)
	if err != nil {
		return err
	}

	for vbno, info := range rsp {
		fmt.Printf("%d => %+v\n", vbno, info)
	}
	return
}

func pushDocument(conn *base.CngConn, cfg *Config) (err error) {
	xattrs := make(map[string][]byte, len(cfg.PushDocumentReq.Xattrs))

	for k, v := range cfg.PushDocumentReq.Xattrs {
		xattrs[k] = []byte(v)
	}

	cas := genCas()
	fmt.Printf("Generated cas: %d\n", cas)

	req := &internal_xdcr_v1.PushDocumentRequest{
		Key:            cfg.PushDocumentReq.Key,
		BucketName:     cfg.PushDocumentReq.Bucket,
		ScopeName:      cfg.PushDocumentReq.Scope,
		CollectionName: cfg.PushDocumentReq.Collection,
		StoreCas:       uint64(cas),
		Revno:          cfg.PushDocumentReq.Revno,
		ContentFlags:   uint32(cfg.PushDocumentReq.Flags),
		IsDeleted:      cfg.PushDocumentReq.IsDeleted,
		Xattrs:         xattrs,
	}

	if cfg.PushDocumentReq.Expiry != nil {
		req.ExpiryTime = &timestamppb.Timestamp{Seconds: *cfg.PushDocumentReq.Expiry, Nanos: 0}
	}

	req.Content = &internal_xdcr_v1.PushDocumentRequest_ContentUncompressed{
		ContentUncompressed: []byte(cfg.PushDocumentReq.Body),
	}

	if cfg.PushDocumentReq.ContentType == "json" {
		req.ContentType = internal_xdcr_v1.ContentType_CONTENT_TYPE_JSON
	} else {
		req.ContentType = internal_xdcr_v1.ContentType_CONTENT_TYPE_NONJSON
	}

	rsp, err := conn.Client().PushDocument(context.Background(), req)
	if err != nil {
		return err
	}

	fmt.Printf("Response: %v\n", rsp)

	return
}

func getBucketInfo(conn *base.CngConn, cfg *Config) (err error) {
	rsp, err := conn.Client().GetBucketInfo(context.Background(), &internal_xdcr_v1.GetBucketInfoRequest{
		BucketName: cfg.WatchCollectionsReq.BucketName,
	})
	if err != nil {
		return err
	}

	fmt.Printf("Response:\n")
	fmt.Printf("   uuid: %s\n", rsp.BucketUuid)
	fmt.Printf("   name: %s\n", rsp.ConflictResolutionType.String())
	fmt.Printf("   eccv: %v\n", rsp.CrossClusterVersioningEnabled)
	fmt.Printf("   numVBuckets: %d\n", rsp.NumVbuckets)

	return
}

func watchCollections(conn *base.CngConn, cfg *Config) (err error) {
	ctx := context.Background()
	stream, err := conn.Client().WatchCollections(ctx, &internal_xdcr_v1.WatchCollectionsRequest{
		BucketName: cfg.WatchCollectionsReq.BucketName,
	})
	if err != nil {
		return err
	}

	rsp := &internal_xdcr_v1.WatchCollectionsResponse{}
	if err = stream.RecvMsg(rsp); err != nil {
		return err
	}

	fmt.Printf("Manifest UID: %v\n", rsp.ManifestUid)
	for _, s := range rsp.Scopes {
		fmt.Printf("Scope: name=[%s] id=%d\n", s.ScopeName, s.ScopeId)
		for _, c := range s.Collections {
			fmt.Printf("  Collection: name=[%s] id=%d\n", c.CollectionName, c.CollectionId)
		}
	}

	return
}

func getVbucketInfo(conn *base.CngConn, cfg *Config) (err error) {
	stream, err := conn.Client().GetVbucketInfo(context.Background(), cfg.GetVBucketInfoReq)
	if err != nil {
		log.Fatal(err)
	}

	var rsp internal_xdcr_v1.GetVbucketInfoResponse
	err = stream.RecvMsg(&rsp)
	if err != nil {
		log.Fatal(err)
	}

	for _, v := range rsp.Vbuckets {
		fmt.Printf("Response: %+v\n", v)
	}

	return nil
}

func genCas() int64 {
	n := time.Now().UnixNano()

	return n
}
