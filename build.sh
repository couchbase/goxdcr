#!/bin/bash


build_base(){
echo "Building base..."
cd base
go clean
go install
cd ..
echo "Done"
}

build_factory(){
echo "Building factory..."
cd factory
go clean
go install
cd ..
echo "Done"
}

build_gen_server(){
echo "Building gen_server..."
cd gen_server
go clean
go install
cd ..
echo "Done"
}

build_parts(){
echo "Building parts..."
cd parts
go clean
go install
cd ..
echo "Done"
}

build_utils(){
echo "Building utils..."
cd utils
go clean
go install
cd ..
echo "Done"
}

build_tests(){
echo "Building tests..."
cd tests
cd xmem
go clean
go install
cd ../factory
go clean
go install
cd ../router
go clean 
go install
cd ../kvfeed
go clean
go install
cd ..
echo "Done"
}

clean_base(){
echo "Clean base..."
cd base
go clean
cd ..
echo "Done"
}

clean_factory(){
echo "Clean factory..."
cd factory
go clean
cd ..
echo "Done"
}

clean_gen_server(){
echo "Clean gen_server..."
cd gen_server
go clean
cd ..
echo "Done"
}

clean_parts(){
echo "Clean parts..."
cd parts
go clean
cd ..
echo "Done"
}

clean_utils(){
echo "Clean utils..."
cd utils
go clean
cd ..
echo "Done"
}

clean_tests(){
echo "Clean tests..."
cd tests
cd xmem
go clean
cd ..
echo "Done"
}

if [ -z "$1" ]
then
build_base
build_factory
build_gen_server
build_parts
build_utils
build_tests
elif [ $1 == "base" ]
then
build_base
elif [ $1 == "factory" ]
then
build_factory
elif [ $1 == "gen_server" ]
then
build_gen_server
elif [ $1 == "parts" ]
then
build_parts
elif [ $1 == "utils" ]
then
build_utils
elif [ $1 == "tests" ]
then
build_tests
elif [ $1 == "clean" ]
then
echo "Cleaning..."
clean_base
clean_factory:
clean_gen_server
clean_parts
clean_utils
clean_tests
echo "Done"
else
echo "Unknown build option"
fi
