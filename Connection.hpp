#ifndef CONNECTION_HPP
#define CONNECTION_HPP

#include <iostream>
#include <unistd.h>
#include "Buffer.hpp"       //v7新增，引入自己的缓冲区
#include <mutex>            //v7新增

class Connection {
public:
    int fd;
    Buffer read_buffer;     //每个连接独享的应用层接收缓冲区
    std::mutex buffer_mutex;        // v7必须加上这把互斥锁，用来保护上面的 read_buffer

    //构造函数
    explicit Connection(int client_fd):fd{client_fd} {
        std::cout << "【Connection 诞生】封装新 fd: " << fd << std::endl;
    }

    //析构函数
    ~Connection() {
        if (fd!=-1) {
            std::cout << "【Connection 析构】引用计数归零！fd " << fd << " 真正释放资源并执行 close()" << std::endl;
            close(fd);
        }
    }

    // 禁用拷贝构造和赋值，防止智能指针外的不安全拷贝
    Connection(const Connection&)=delete;
    Connection& operator=(const Connection&)=delete;
};

#endif