module github.com/couchbase/goxdcr/v8

go 1.22.5

replace github.com/couchbase/eventing-ee => ../eventing-ee

replace github.com/couchbase/gomemcached => ../gomemcached

replace github.com/couchbase/regulator => ../regulator

replace github.com/couchbase/go_json => ../go_json

replace github.com/couchbase/query => ../query

replace github.com/couchbase/query-ee => ../query-ee

replace github.com/couchbase/n1fty => ../n1fty

replace github.com/couchbase/gocb/v2 => github.com/couchbase/gocb/v2 v2.2.5

replace github.com/couchbase/cbft => ../../../../../cbft

replace github.com/couchbase/cbgt => ../../../../../cbgt

replace github.com/couchbase/hebrew => ../../../../../hebrew

require (
	github.com/couchbase/cbauth v0.1.12
	github.com/couchbase/eventing-ee v0.0.0-00010101000000-000000000000
	github.com/couchbase/go-couchbase v0.1.1
	github.com/couchbase/gocb/v2 v2.5.4
	github.com/couchbase/gocbcore/v9 v9.1.11
	github.com/couchbase/gomemcached v0.2.2-0.20230407174933-7d7ce13da8cc
	github.com/couchbase/goutils v0.1.2
	github.com/couchbaselabs/gojsonsm v1.0.1
	github.com/glenn-brown/golang-pkg-pcre v0.0.0-20120522223659-48bb82a8b8ce
	github.com/golang/snappy v0.0.4
	github.com/google/uuid v1.6.0
	github.com/icrowley/fake v0.0.0-20240710202011-f797eb4a99c0
	github.com/pkg/errors v0.9.1
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/stretchr/testify v1.9.0
)

require (
	github.com/alecthomas/participle v0.7.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/corpix/uarand v0.0.0-20170723150923-031be390f409 // indirect
	github.com/couchbase/clog v0.1.0 // indirect
	github.com/couchbase/go_json v0.0.0-20220330123059-4473a21887c8 // indirect
	github.com/couchbase/query v0.0.0-00010101000000-000000000000 // indirect
	github.com/couchbase/regulator v0.0.0-00010101000000-000000000000 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/flatbuffers v24.3.25+incompatible // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/gorilla/mux v1.8.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v1.13.0 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.37.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/santhosh-tekuri/jsonschema v1.2.4 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	golang.org/x/crypto v0.22.0 // indirect
	golang.org/x/sys v0.23.0 // indirect
	google.golang.org/protobuf v1.33.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
