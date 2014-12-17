package metakvsanity

import (
	"os"

	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metakv"
)

// MaybeRun is a simple at-runtime sanity test of metakv. It is
// something temporary which is safe to run every time goxdcr starts.
func MaybeRun() {
	logger := log.NewLogger("metakv", log.DefaultLoggerContext)

	if os.Getenv("COUCHBASE_METAKV_SANITY") != "" {
		metakv.ExecuteBasicSanityTest(logger.Info)
	}

	if l := os.Getenv("COUCHBASE_METAKV_DEBUG"); l != "" {
		logger.Infof("Starting _metakv debugging endpoint on `%s'", l)
		GoRunDebugEndpoint(l)
	}
}
