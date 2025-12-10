package base

import (
	"encoding/json"
	"fmt"
)

const (
	// DevReplicationOpts is an setting to be used on a case-by-case basis to enable/disable extra options.
	// A restart of the replication is needed for the setting to take effect.
	// Instead of proliferating multiple settings, we group them into a single setting.
	//
	// The command to enable the options is like:
	// curl -X POST -u username:password http://<host>:<port>/settings/replications/<repl_id> \
	// 	-d devReplOpts='{"disableDataPool":true, "strictDataPoolUse":true,"enableXmemProtocolCheck":true}'
	//
	// The command to disable the setting is like:
	// curl -X POST -u username:password http://<host>:<port>/settings/replications/<repl_id> \
	// 	-d devReplOpts=''
	//
	// where <repl_id> is the replication id (e.g 'eb38dd65f59f9c314c9e68ccad8123dc%2FB1%2FB1')
	DevReplOptsKey string = "devReplOpts"
)

type DevReplOpts struct {
	// DisableDataPool disables the use of data pool in the nozzle
	DisableDataPool bool `json:"disableDataPool"`

	// StrictDataPoolUse errors out if data pool is misused
	StrictDataPoolUse bool `json:"strictDataPoolUse"`

	// EnableXmemProtocolCheck enables the XMEM protocol
	EnableXmemProtocolCheck bool `json:"enableXmemProtocolCheck"`
}

// Returns false if any of the options is set
func (opts DevReplOpts) IsDefault() bool {
	return !opts.DisableDataPool && !opts.StrictDataPoolUse && !opts.EnableXmemProtocolCheck
}

func (opts DevReplOpts) String() string {
	return fmt.Sprintf("disableDataPool:%v, strictDataPoolUse:%v, enableXmemProtocolCheck:%v",
		opts.DisableDataPool, opts.StrictDataPoolUse, opts.EnableXmemProtocolCheck)
}

// ParseDevReplOpts parses the dev replication options from a string
// Format is json: '{"disableDataPool":true,"strictDataPoolUse":false,"enableXmemProtocolCheck":true}'
// If the string is empty, it will return default options
func ParseDevReplOpts(val string) (opts DevReplOpts, err error) {
	if val == "" {
		return
	}

	if err = json.Unmarshal([]byte(val), &opts); err != nil {
		return
	}

	return
}
