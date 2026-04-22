module github.com/couchbase/goxdcr/v8

go 1.26.0

replace github.com/couchbase/eventing => ../eventing

replace github.com/couchbase/eventing-ee => ../eventing-ee

replace github.com/couchbase/gomemcached => ../gomemcached

replace github.com/couchbase/regulator => ../regulator

replace github.com/couchbase/go_json => ../go_json

replace github.com/couchbase/gocbcrypto => ../gocbcrypto

replace github.com/couchbase/query => ../query

replace github.com/couchbase/query-ee => ../query-ee

replace github.com/couchbase/n1fty => ../n1fty

replace github.com/couchbase/cbft => ../../../../../cbft

replace github.com/couchbase/cbgt => ../../../../../cbgt

replace github.com/couchbase/hebrew => ../../../../../hebrew

replace github.com/couchbase/cbauth => ../cbauth

require (
	github.com/couchbase/cbauth v0.1.18
	github.com/couchbase/eventing-ee v0.0.0-00010101000000-000000000000
	github.com/couchbase/go-couchbase v0.1.1
	github.com/couchbase/gocb/v2 v2.12.2-0.20260407160817-f3cc07463f4d // TODO: MB-70129
	github.com/couchbase/gocbcore/v9 v9.1.11
	github.com/couchbase/gomemcached v0.3.4
	github.com/couchbase/goprotostellar v1.0.6-0.20260407143512-d7af25156dcc
	github.com/couchbase/goutils v0.1.2
	github.com/couchbase/tools-common/http v1.0.7
	github.com/couchbaselabs/gojsonsm v1.0.1
	github.com/glenn-brown/golang-pkg-pcre v0.0.0-20120522223659-48bb82a8b8ce
	github.com/golang/snappy v1.0.0
	github.com/google/uuid v1.6.0
	github.com/icrowley/fake v0.0.0-20240710202011-f797eb4a99c0
	github.com/pkg/errors v0.9.1
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/stretchr/testify v1.11.1
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260203192932-546029d2fa20
	google.golang.org/grpc v1.80.0
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/alecthomas/participle v0.7.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/corpix/uarand v0.0.0-20170723150923-031be390f409 // indirect
	github.com/couchbase/clog v0.1.0 // indirect
	github.com/couchbase/go_json v0.0.0-20250803152502-4641f4ef8578 // indirect
	github.com/couchbase/gocbcore/v10 v10.9.1 // indirect
	github.com/couchbase/gocbcoreps v0.1.5-0.20260107140814-1c3a03f888f8 // indirect
	github.com/couchbase/query v0.0.0-20231201224521-b47444ea33a9 // indirect
	github.com/couchbase/regulator v0.0.0-00010101000000-000000000000 // indirect
	github.com/couchbase/tools-common/errors v1.0.0 // indirect
	github.com/couchbaselabs/gocbconnstr/v2 v2.0.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/gorilla/mux v1.8.1 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_golang v1.23.0 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.67.5 // indirect
	github.com/prometheus/procfs v0.16.1 // indirect
	github.com/santhosh-tekuri/jsonschema v1.2.4 // indirect
	github.com/stretchr/objx v0.5.3 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.65.0 // indirect
	go.opentelemetry.io/otel v1.40.0 // indirect
	go.opentelemetry.io/otel/metric v1.40.0 // indirect
	go.opentelemetry.io/otel/trace v1.40.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.1 // indirect
	go.yaml.in/yaml/v2 v2.4.3 // indirect
	golang.org/x/crypto v0.48.0 // indirect
	golang.org/x/net v0.51.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.35.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
