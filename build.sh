#!/bin/bash

if [ ! -d "build" ]; then
    mkdir build
fi

cd build

export PRINT_LOG=ON
export TIME_OPEN=ON

cmake -DCMAKE_BUILD_TYPE=Release ..
cmake --build . -j16