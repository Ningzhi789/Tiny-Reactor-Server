#include <iostream>
#include <cstring>
#include <sys/socket.h>     //socket核心头文件
#include <netinet/in.h>     //包含socketaddr_in结构体
#include <unistd.h>         //包含close函数
#include <thread>           //多线程核心头文件
#include <chrono>           //用于时间延迟
#include <sys/epoll.h>      //epoll核心头文件
#include "ThreadPool.hpp"   //引入手写线程池

const int MAX_EVENTS=1024;
const int BUFFER_SIZE=1024;

// 模拟耗时的业务处理函数（运行在线程池中）
void process_business(int client_fd,std::string request_msg) {
    // 1. 模拟复杂的耗时业务（比如查数据库、复杂的逻辑运算等延迟）
    // 即使这里睡眠 2 秒，也完全不会影响主线程 epoll 接收其他人的请求！
    std::this_thread::sleep_for(std::chrono::seconds(2));

    std::cout << "【工作线程 " << std::this_thread::get_id()
              << "】业务处理完毕，正在回传 fd " << client_fd << std::endl;
    // 2. 组装响应数据并发送给客户端
    std::string response ="【V4高级架构回执】: " + request_msg;
    send(client_fd,response.c_str(),response.length(),0);
}

int main() {
    // 1.socket
    int server_fd=socket(AF_INET,SOCK_STREAM,0);
    if (server_fd==-1) {
        std::cout<<"failed created socket_fd"<<"\n";
        return -1;
    }

    //设置端口复用
    int opt=1;
    setsockopt(server_fd,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));

    // 2.addr and port
    sockaddr_in server_addr{};
    server_addr.sin_family=AF_INET;     //ipv4
    server_addr.sin_addr.s_addr=INADDR_ANY;     //监听所有可用网络接口
    server_addr.sin_port=htons(8088);

    // 3.bind
    if (bind(server_fd,(struct sockaddr*)&server_addr,sizeof(server_addr))<0) {
        std::cerr<<"bind failed"<<"\n";
        close(server_fd);
        return -1;
    }

    // 4.listen
    if (listen(server_fd,5)<0) {
        std::cerr<<"listen failed"<<"\n";
        close(server_fd);
        return -1;
    }
    std::cout<<"listening 8088"<<"\n";

    // 创建epoll实例
    // epoll_create1(0) 是现代 Linux 推荐写法
    int epoll_fd=epoll_create1(0);
    if (epoll_fd==-1) {
        std::cerr << "创建 epoll 实例失败！" << std::endl;
        close(server_fd);
        return -1;
    }

    //把服务器的监听套接字加入到 epoll 实例中
    epoll_event ev{};
    ev.events=EPOLLIN;      // 监听读事件（当有新客户端来连接时，server_fd 会触发读事件）
    ev.data.fd=server_fd;   // 把关联的 fd 存进去

    // EPOLL_CTL_ADD 代表将该 fd 添加到 epoll 监听树中
    if (epoll_ctl(epoll_fd,EPOLL_CTL_ADD,server_fd,&ev)==-1) {
        std::cerr << "将监听 fd 添加到 epoll 失败！" << std::endl;
        close(server_fd);
        close(epoll_fd);
        return -1;
    }

    // 🔥 初始化一个拥有 4 个核心工作线程的线程池
    ThreadPool pool(4);
    // 用于存放被唤醒的就绪事件数组
    epoll_event events[MAX_EVENTS];

    // 进入单线程事件大循环
    while (true) {
        // 主线程牢牢守护在这里，只负责监听 I/O 事件
        int nfds=epoll_wait(epoll_fd,events,MAX_EVENTS,-1);
        if (nfds == -1) {
            std::cerr << "epoll_wait 错误！" << std::endl;
            break;
        }

        // 依次处理 epoll_wait 返回的就绪事件
        for (int i=0;i<nfds;i++) {
            int current_fd=events[i].data.fd;

            // 情况 A：如果是 server_fd 有动静，说明是【新客户端要求连接】
            if (current_fd==server_fd) {
                sockaddr_in client_addr{};
                socklen_t client_len=sizeof(client_addr);
                int client_fd=accept(server_fd,(struct sockaddr*)&client_addr,&client_len);
                if (client_fd < 0) {
                    std::cerr << "接受新连接失败！" << std::endl;
                    continue;
                }
                std::cout << "成功接受客户端连接，分配 fd: " << client_fd << std::endl;

                // 把这个新客户端的 client_fd 也注册到 epoll 监听名单里
                epoll_event client_ev{};
                client_ev.events=EPOLLIN;       // 依然监听它发消息
                client_ev.data.fd=client_fd;
                epoll_ctl(epoll_fd,EPOLL_CTL_ADD,client_fd,&client_ev);
                std::cout << "【主线程】捕获新连接，已托管至 epoll，fd: " << client_fd << std::endl;
            }
            // 情况 B：如果是普通的 client_fd 有动静，说明【有客户端发消息过来了】
            else if (events[i].events & EPOLLIN){
                char buffer[BUFFER_SIZE]={0};
                ssize_t bytes_read=read(current_fd,buffer,sizeof(buffer)-1);

                if (bytes_read > 0) {
                    std::cout << "【主线程】快速读取 fd " << current_fd << " 数据完毕，打包任务抛给线程池！" << std::endl;

                    // 🔥 核心架构升级：将业务逻辑处理封装为 lambda 表达式，打包投递给线程池
                    std::string req_str(buffer);
                    pool.enqueue([current_fd, req_str]() {
                        process_business(current_fd, req_str);
                    });
                }
                // bytes_read == 0 代表客户端主动断开连接
                else if (bytes_read == 0) {
                    std::cout << "【客户端离线】fd " << current_fd << " 主动断开连接。" << std::endl;
                    // 从 epoll 监听树中移除（高版本 Linux 传 NULL 即可）
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, current_fd, nullptr);
                    close(current_fd); // 必须关闭 fd 释放系统资源
                }
                // 发生错误
                else {
                    std::cerr << "读取 fd " << current_fd << " 发生异常错误。" << std::endl;
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, current_fd, nullptr);
                    close(current_fd);
                }
            }
        }
    }

    // 8.close
    close(server_fd);
    close(epoll_fd);
    std::cout<<"close"<<"\n";
    return 0;
}