#ifndef CONNECTION_HPP
#define CONNECTION_HPP

#include <iostream>
#include <unistd.h>

class Connection {
public:
    int fd;

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