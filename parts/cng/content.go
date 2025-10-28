package cng

import (
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/golang/snappy"
)

type content struct {
	// The returned Body is always be compressed
	Body []byte
	// Xattrs will be nil if there is no xattr
	Xattrs map[string][]byte

	// Indicates whether the Body is compressed or not
	NotCompressed bool

	// Indicates whether the Body is a JSON document
	IsJson bool
}

// getContent extracts the body and xattr key-values into a map
// If xattr is not present, XattrMap will be nil
// If body is not compressed, it will be compressed before returning
// If body is compressed and xattr is present, xattr will be extracted and body will be recompressed
// If body is compressed and xattr is not present, body will be returned as is
func getContent(logger *log.CommonLogger, req *base.WrappedMCRequest) (c content, err error) {
	c.IsJson = req.Req.DataType&base.JSONDataType > 0

	if base.HasXattr(req.Req.DataType) {
		var body []byte
		if req.NeedToRecompress {
			// If true, then body is not compressed
			body = req.Req.Body
		} else {
			buflen := req.GetUncompressedBodySize()
			body = make([]byte, buflen)
			_, err = snappy.Decode(body, req.Req.Body)
			if err != nil {
				logger.Errorf("Failed to snappy decode body for key=%s, err=%v",
					string(req.OriginalKey), err)
				return
			}
		}
		c.Xattrs, err = getXattrMap(body)
		if err != nil {
			logger.Errorf("Failed to get xattr map for key=%s, err=%v",
				string(req.OriginalKey), err)
			return
		}

		bodyWithoutXattr, err := base.StripXattrAndGetBody(body)
		if err != nil {
			logger.Errorf("Failed to strip xattr for key=%s, err=%v",
				string(req.OriginalKey), err)
			return c, err
		}

		cbuf := make([]byte, snappy.MaxEncodedLen(len(bodyWithoutXattr)))
		c.Body = snappy.Encode(cbuf, bodyWithoutXattr)
		req.NeedToRecompress = false
		return c, nil
	}

	if req.NeedToRecompress || req.Req.DataType&base.SnappyDataType == 0 {
		cbuf := make([]byte, snappy.MaxEncodedLen(len(req.Req.Body)))
		c.Body = snappy.Encode(cbuf, req.Req.Body)
		req.NeedToRecompress = false
	} else {
		c.Body = req.Req.Body
	}
	return
}

// getXattrMap extracts the xattr key-values into a map
// The body is expected to have xattrs and uncompressed values
// At the time of this writing, values are slices of bytes on the
// original buf.
func getXattrMap(body []byte) (m map[string][]byte, err error) {
	itr, err := base.NewXattrIterator(body)
	if err != nil {
		return nil, err
	}

	m = make(map[string][]byte)
	for itr.HasNext() {
		key, value, err := itr.Next()
		if err != nil {
			return nil, err
		}

		m[string(key)] = value
	}

	return
}
