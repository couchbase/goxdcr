package cng

import (
	"github.com/couchbase/goxdcr/v8/base"
	"github.com/couchbase/goxdcr/v8/log"
	"github.com/golang/snappy"
)

type content struct {
	// The returned Body is always be compressed and without xattrs (if any).
	Body []byte
	// Xattrs will be nil if there is no xattr
	Xattrs map[string][]byte

	// Indicates whether the Body is compressed or not
	NotCompressed bool

	// Indicates whether the Body is a JSON document
	IsJson bool

	// pooledBody, if non-nil, is the original full-cap slice obtained from the
	// data pool that backs Body. It must be returned to the same pool via recycle.
	pooledBody []byte
}

// recycle returns the pooled buffer (if any) backing Body to pool.
// Safe to call multiple times and on a zero-value content.
func (c *content) recycle(pool base.DataPool) {
	if c == nil || c.pooledBody == nil {
		return
	}
	pool.PutByteSlice(c.pooledBody)
	c.pooledBody = nil
}

// getEncodeBuf gets a buffer suitable as the destination for snappy.Encode.
// Falls back to make() on pool errors so replication is never blocked by pool failures.
// The returned slice is the original full-cap buffer; pooled is true iff it came from pool.
func getEncodeBuf(pool base.DataPool, srcLen int) (buf []byte, pooled bool) {
	size := snappy.MaxEncodedLen(srcLen)
	buf, err := pool.GetByteSlice(uint64(size))
	if err != nil {
		return make([]byte, size), false
	}
	return buf, true
}

// getContent extracts the body and xattr key-values into a map
// If xattr is not present, XattrMap will be nil
// If body is not compressed, it will be compressed before returning
// If body is compressed and xattr is present, xattr will be extracted and body will be recompressed
// If body is compressed and xattr is not present, body will be returned as is
func getContent(logger *log.CommonLogger, pool base.DataPool, req *base.WrappedMCRequest) (c content, err error) {
	c.IsJson = req.Req.DataType&base.JSONDataType > 0

	if base.HasXattr(req.Req.DataType) {
		var body []byte
		if req.Req.DataType&base.SnappyDataType == 0 {
			// If true, then body is not compressed
			body = req.Req.Body
		} else {
			body, err = snappy.Decode(nil, req.Req.Body)
			if err != nil {
				logger.Errorf("Failed to snappy decode body for key=%s[1]s%[3]s%[2]s, dataType=%[4]d, buf=%s[1]s%[5]s%[2]s, err=%[6]v",
					base.UdTagBegin,
					base.UdTagEnd,
					req.OriginalKey, req.Req.DataType, req.Req.Body, err)
				return
			}
		}
		c.Xattrs, err = getXattrMap(body)
		if err != nil {
			logger.Errorf("Failed to get xattr map for key=%s[1]s%[3]s%[2]s, err=%[4]v",
				base.UdTagBegin,
				base.UdTagEnd,
				req.OriginalKey, err)
			return
		}

		bodyWithoutXattr, err := base.StripXattrAndGetBody(body)
		if err != nil {
			logger.Errorf("Failed to strip xattr for key=%s[1]s%[3]s%[2]s, err=%[4]v",
				base.UdTagBegin,
				base.UdTagEnd,
				req.OriginalKey, err)
			return c, err
		}

		cbuf, pooled := getEncodeBuf(pool, len(bodyWithoutXattr))
		if pooled {
			c.pooledBody = cbuf
		}
		c.Body = snappy.Encode(cbuf, bodyWithoutXattr)
		return c, nil
	}

	if req.Req.DataType&base.SnappyDataType == 0 {
		cbuf, pooled := getEncodeBuf(pool, len(req.Req.Body))
		if pooled {
			c.pooledBody = cbuf
		}
		c.Body = snappy.Encode(cbuf, req.Req.Body)
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
