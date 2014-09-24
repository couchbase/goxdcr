package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/replication_manager"
	c "github.com/couchbase/indexing/secondary/common"
)

var done = make(chan bool)
var cluster = "localhost:9000"

var options struct {
	adminport string
	info      bool
	debug     bool
	trace     bool
}

func argParse() {
	flag.BoolVar(&options.info, "info", false,
		"enable info level logging")
	flag.BoolVar(&options.debug, "debug", false,
		"enable debug level logging")
	flag.BoolVar(&options.trace, "trace", false,
		"enable trace level logging")
	flag.StringVar(&options.adminport, "adminport", "localhost:9999",
		"adminport address")
	flag.Parse()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] <cluster-addr> \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	argParse()
	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(1)
	}

	cluster = args[0]

	if options.trace {
		c.SetLogLevel(c.LogLevelTrace)
	} else if options.debug {
		c.SetLogLevel(c.LogLevelDebug)
	} else if options.info {
		c.SetLogLevel(c.LogLevelInfo)
	}
	go replication_manager.MainAdminPort(options.adminport)
	<-done
}
