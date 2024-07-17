#!/bin/bash

if [ ! -d "build_debug" ]; then
    mkdir build_debug
fi

cd build_debug

export PRINT_LOG=ON

cmake -DCMAKE_BUILD_TYPE=Debug ..
cmake --build . -j16