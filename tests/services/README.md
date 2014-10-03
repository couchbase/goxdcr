Before running the metadata service unit test, metadata_service_run.go, one needs
to make sure that gometa executable is compiled and put in $GOPATH/bin. This can
be done using following command:

go build -o $GOPATH/bin/gometa $GOPATH/src/github.com/couchbase/gometa/main/*.go

The unit test will start the gometa service automatically, but will not stop it
after the test completes. The gometa service needs to be stopped manually.



