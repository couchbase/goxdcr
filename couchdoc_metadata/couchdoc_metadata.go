package couchdoc_metadata

import (
	"github.com/couchbase/gomemcached"
	"encoding/binary"
)

const GET_WITH_META gomemcached.CommandCode = 1

type CouchDocMetadata struct {
	Deleted  uint32
	Flags    uint32 // Item flags
	Expiry   uint32 // Item expiration time
	Cas      uint64 // CAS value of the item
	RevSeqno uint64 // revision sequence number of the mutation
}

func GetDocMetadataFromResp(resp *gomemcached.MCResponse) *CouchDocMetadata {
	//        {ok, #mc_header{status=?SUCCESS},
	//             #mc_entry{ext = Ext, cas = CAS}, _NCB} ->
	//            <<MetaFlags:32/big, ItemFlags:32/big,
	//              Expiration:32/big, SeqNo:64/big>> = Ext,
	//            RevId = <<CAS:64/big, Expiration:32/big, ItemFlags:32/big>>,
	//            Rev = {SeqNo, RevId},

	doc_metadata := &CouchDocMetadata{}
	if resp.Opcode == GET_WITH_META {

		doc_metadata.Deleted = binary.BigEndian.Uint32(resp.Extras[:4])
		doc_metadata.Flags = binary.BigEndian.Uint32(resp.Extras[4:8])
		doc_metadata.Expiry = binary.BigEndian.Uint32(resp.Extras[8:12])
		doc_metadata.RevSeqno = binary.BigEndian.Uint64(resp.Extras[12:20])
		doc_metadata.Cas = resp.Cas

	}
	return doc_metadata
}
