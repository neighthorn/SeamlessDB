cmake_minimum_required(VERSION 3.16)
project(seamless_client)

set(CMAKE_CXX_STANDARD 20)
find_package(Threads REQUIRED)



add_executable(${PROJECT_NAME} main.cpp)


target_link_libraries(seamless_client
        pthread readline 
)