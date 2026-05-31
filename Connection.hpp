#ifndef CONNECTION_HPP
#define CONNECTION_HPP

#include <iostream>
#include <unistd.h>
#include "Buffer.hpp"
#include <mutex>
#include <chrono>
#include <atomic>
#include "HttpParser.hpp"
#include "Logger.hpp"
#include <openssl/ssl.h>


class Connection {
public:
    int fd;
    Buffer read_buffer;
    std::mutex buffer_mutex;
    std::chrono::steady_clock::time_point expire_time;
    SSL* ssl=nullptr;
    bool ssl_accepted=false;
    std::mutex ssl_mutex;
    std::atomic<bool> needs_rearm{false};
    std::atomic<bool> is_closing{false};

    HttpParser http_parser; // 🔥 v9新增：每个连接独享的状态机解析实例
    //构造函数
    explicit Connection(int client_fd):fd{client_fd} {
        LOG_INFO("[Connection] born, fd: "+std::to_string(fd));
        //std::cout << "【Connection 诞生】封装新 fd: " << fd << std::endl;
    }

    //析构函数
    ~Connection() {
        if (fd!=-1) {
            LOG_INFO("[Connection] destroyed, fd " + std::to_string(fd)+ " releasing resources and close()");
            //std::cout << "【Connection 析构】引用计数归零！fd " << fd << " 真正释放资源并执行 close()" << std::endl;
            close(fd);
        }
        if (ssl)
            SSL_free(ssl);
    }

    // 禁用拷贝构造和赋值，防止智能指针外的不安全拷贝
    Connection(const Connection&)=delete;
    Connection& operator=(const Connection&)=delete;
};

#endif