package base

import (
	"fmt"
	"strings"
)

const (
	// DevReplicationOpts is an setting to be used on a case-by-case basis to enable/disable extra options.
	// A restart of the replication is needed for the setting to take effect.
	// Instead of proliferating multiple settings, we group them into a single setting.
	// Format of the value: "option1=value1,option2=value2"
	DevReplOptsKey string = "devReplOpts"

	// Disables the use of data pool optimizations in XDCR replication
	// Values: "true" or "false"
	DevReplDisableDataPool string = "disableDataPool"

	// Errors out if data pool is misused
	// Values: "true" or "false"
	DevReplStrictDataPoolUse string = "strictDataPoolUse"

	// Enables the XMEM protocol check on disconnects
	// Values: "true" or "false"
	DevReplEnableXmemProtocolCheck string = "enableXmemProtocolCheck"
)

type DevReplOpts struct {
	// DisableDataPool disables the use of data pool in the nozzle
	DisableDataPool bool

	// StrictDataPoolUse errors out if data pool is misused
	StrictDataPoolUse bool

	// EnableXmemProtocolCheck enables the XMEM protocol check on disconnects
	// Note: Not in use at the moment
	EnableXmemProtocolCheck bool
}

func (opts *DevReplOpts) String() string {
	return fmt.Sprintf("%s=%v,%s=%v,%s=%v",
		DevReplDisableDataPool, opts.DisableDataPool,
		DevReplStrictDataPoolUse, opts.StrictDataPoolUse,
		DevReplEnableXmemProtocolCheck, opts.EnableXmemProtocolCheck)
}

// ParseDevReplOpts parses the dev replication options from a string
// Format: "option1=value1,option2=value2"
// If the string is empty, returns default options
// i.e. opts will always be non-nil if err == nil
func ParseDevReplOpts(val string) (opts *DevReplOpts, err error) {
	opts = &DevReplOpts{}

	if val == "" {
		return
	}

	parts := strings.Split(val, ",")
	for _, opt := range parts {
		kv := strings.Split(opt, "=")
		if len(kv) != 2 {
			err = fmt.Errorf("invalid dev replication opt=[%s]", opt)
			return
		}

		key := kv[0]
		value := kv[1]

		switch key {
		case DevReplDisableDataPool:
			opts.DisableDataPool = (value == "true")
		case DevReplStrictDataPoolUse:
			opts.StrictDataPoolUse = (value == "true")
		case DevReplEnableXmemProtocolCheck:
			opts.EnableXmemProtocolCheck = (value == "true")
		default:
			err = fmt.Errorf("unknown dev replication opt: key=%v", key)
			return
		}
	}

	return
}
