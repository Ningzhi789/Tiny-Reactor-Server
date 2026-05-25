cmake_minimum_required(VERSION 3.16)
project(c__)

set(CMAKE_CXX_STANDARD 20)

add_executable(c__ main.cpp
        server.cpp
        client.cpp)
