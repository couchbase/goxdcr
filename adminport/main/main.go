package main

import (
	"flag"
	"fmt"
	"os"

	ap "github.com/Xiaomei-Zhang/couchbase_goxdcr_impl/adminport"
)

var done = make(chan bool)
var nodeAddr = "localhost"


func argParse() {
	flag.Parse()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] <node-addr> \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	argParse()
	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(1)
	}

	nodeAddr = args[0]
	go ap.MainAdminPort(nodeAddr)
	<-done
}
