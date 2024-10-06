#!/bin/bash

if [ ! -d "build_debug" ]; then
    mkdir build_debug
fi

cd build_debug

export PRINT_LOG=ON

cmake -DCMAKE_BUILD_TYPE=Debug --verbose=1 .. 
cmake --build . -j16
# make -j16 -verbose