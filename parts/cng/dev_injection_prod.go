//go:build !dev

package cng

import (
	"io"

	"github.com/couchbase/goxdcr/v8/metadata"
)

func (t *Tunables) devParamsString(w io.Writer) {
}

func (n *Nozzle) initDevInjections(settings metadata.ReplicationSettingsMap) {
	// no-op
}

func (n *Nozzle) updateDevInjections(settings metadata.ReplicationSettingsMap) {
	// no-op
}
