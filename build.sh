#!/bin/bash

build_xdcr(){

    echo "Building xdcr..."
    cd main
    go build -o xdcr
    cd ..
    echo "Done"
    echo "xdcr binary under bin/"
}

build_tests(){
echo "Building tests..."
cd tests
cd common
go clean
go install
cd ../adminport
go clean
go install
cd ../xmem
go clean
go install
cd ../factory
go clean
go install
cd ../latency_test
go clean
go install
cd ../router
go clean 
go install
cd ../dcpnozzle
go clean
go install
cd ../pipeline
go clean
go install
cd ../remote_cluster
go clean
go install
cd ..
echo "Done"
}

clean_xdcr(){
echo "Clean xdcr..."
cd main
go clean
cd ..
rm -f bin/xdcr
}

clean_tests(){
echo "Clean tests..."
cd tests
cd common
go clean
cd ../adminport
go clean
cd ../xmem
go clean
cd ../factory
go clean
cd ../latency_test
go clean
cd ../router
go clean 
cd ../dcpnozzle
go clean
cd ../pipeline
go clean
cd ../remote_cluster
go clean
cd ..
echo "Done"
}

if [ -z "$1" ]
then
build_xdcr
build_tests
elif [ $1 == "xdcr" ]
then
build_xdcr
elif [ $1 == "tests" ]
then
build_tests
elif [ $1 == "clean" ]
then
echo "Cleaning..."
clean_xdcr
clean_tests
echo "Done"
else
echo "Unknown build option"
fi
