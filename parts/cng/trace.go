package cng

import (
	"context"
	"fmt"
	"strings"
	"time"

	mc "github.com/couchbase/gomemcached"
)

// Trace keeps track of the operations performed for a mutation
// The fields are set during various stages of processing
type Trace struct {
	opcode mc.CommandCode

	// source vbucket number
	vbno uint16

	// optimistic indicates whether replication is done in optimistic mode or not
	optimistic bool

	// isExpiry indicates whether the mutation has an expiration
	isExpiry bool

	// isDelete indicates whether the mutation is a deletion
	isDelete bool

	// retryCount indicates how many times the mutation transfer was retried
	retryCount uint64

	// commitTime is the time taken for from DCP to processing completion
	commitTime time.Duration

	// checked indicates whether conflict check was performed
	checked bool
	// conflictCheckRsp holds the response from the conflict check
	// This is relevant only if checked is true
	conflictCheckRsp conflictCheckRsp

	// pushed indicates whether pushDocument was performed
	pushed bool
	// pushRsp holds the response from the pushDocument
	// This is relevant only if pushed is true
	pushRsp PushDocRsp
}

func (d Trace) Equal(other Trace) bool {
	res := d.opcode == other.opcode &&
		d.vbno == other.vbno &&
		d.optimistic == other.optimistic &&
		d.isExpiry == other.isExpiry &&
		d.isDelete == other.isDelete &&
		d.checked == other.checked
	if !res {
		return false
	}

	if d.checked && !d.conflictCheckRsp.Equal(other.conflictCheckRsp) {
		return false
	}
	if d.pushed && !d.pushRsp.Equal(other.pushRsp) {
		return false
	}

	return true
}

func (d Trace) String() string {
	sbuf := strings.Builder{}
	sbuf.WriteString(fmt.Sprintf("opcode=%s, vbno=%d, optimistic=%v, isDel=%v, isExp=%v, checked: %v",
		d.opcode.String(), d.vbno, d.optimistic, d.isDelete, d.isExpiry, d.checked))

	if d.checked {
		sbuf.WriteString(fmt.Sprintf(", conflictRsp: (%v)", d.conflictCheckRsp))
	}

	sbuf.WriteString(fmt.Sprintf(", pushed: %v", d.pushed))

	if d.pushed {
		sbuf.WriteString(fmt.Sprintf(", pushRsp: (%s)", d.pushRsp.String()))
	}

	return sbuf.String()
}

func startTrace(ctx context.Context, trace *Trace) (newCtx context.Context) {
	return context.WithValue(ctx, MutationTraceContextKey, trace)
}

func getTrace(ctx context.Context) (*Trace, error) {
	val := ctx.Value(MutationTraceContextKey)
	if val == nil {
		err := fmt.Errorf("mutationTrace not found in context")
		return nil, err
	}
	trace, ok := val.(*Trace)
	if !ok {
		err := fmt.Errorf("mutationTrace not found in context")
		return nil, err
	}
	return trace, nil
}
