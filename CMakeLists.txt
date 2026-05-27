cmake_minimum_required(VERSION 3.16)
project(Tiny-Reactor-Server)

set(CMAKE_CXX_STANDARD 20)

add_executable(Tiny-Reactor-Server main.cpp
        server.cpp
        client.cpp
        ThreadPool.hpp
        Connection.hpp
        Buffer.hpp)
