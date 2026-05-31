cmake_minimum_required(VERSION 3.16)
project(Tiny-Reactor-Server)

set(CMAKE_CXX_STANDARD 20)

find_package(OpenSSL REQUIRED)

# 服务端
add_executable(Tiny-Reactor-Server
        server.cpp
        ThreadPool.hpp
        Connection.hpp
        Buffer.hpp
        Timer.hpp
        HttpParser.hpp
        Logger.hpp
        MysqlConnPool.hpp
        SubReactor.hpp)

target_link_libraries(Tiny-Reactor-Server
        mysqlclient
        pthread
        ${OPENSSL_LIBRARIES})

# 客户端
add_executable(client
        client.cpp)

target_link_libraries(client
        pthread
        ${OPENSSL_LIBRARIES})
