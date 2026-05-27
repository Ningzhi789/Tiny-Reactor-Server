#include <iostream>
#include <cstring>
#include <sys/socket.h>     //socket核心头文件
#include <netinet/in.h>     //包含socketaddr_in结构体
#include <unistd.h>         //包含close函数
#include <thread>           //多线程核心头文件
#include <chrono>           //用于时间延迟
#include <sys/epoll.h>      //epoll核心头文件
#include "ThreadPool.hpp"   //引入手写线程池
#include <fcntl.h>          //非阻塞
#include <cerrno>           //捕获errno错误码
#include <unordered_map>    //新增：用于全局管理连接生命周期
#include <memory>           //新增：智能指针核心头文件
#include "Connection.hpp"   //新增：引入连接封装类

const int MAX_EVENTS=1024;
const int BUFFER_SIZE=1024;

// 🔥 v5新增工具函数：将指定的文件描述符(fd)设置为非阻塞模式
void set_nonblocking(int fd) {
    //获取老标志
    int flags=fcntl(fd,F_GETFL,0);
    if (flags==-1) {
        std::cerr << "获取 fcntl 标志失败！" << std::endl;
        return;
    }
    // 在老标志基础上，追加 O_NONBLOCK (非阻塞) 标志
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// 🔥 v6修改：业务处理函数不再传裸 fd，而是传入持有的智能指针
void process_business(std::shared_ptr<Connection> conn,std::string request_msg) {
    // 模拟复杂的耗时业务
    std::this_thread::sleep_for(std::chrono::seconds(2));

    std::cout << "【工作线程 " << std::this_thread::get_id()
              << "】业务处理完毕，正在回传 fd " << conn.use_count() << std::endl;

    // 直接通过智能指针内部安全的 fd 发送数据
    std::string response ="【v6(v4+ET+智能指针)回执：】" + request_msg;
    send(conn->fd,response.c_str(),response.length(),0);
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
    // 🔥 v5 注意：监听套接字 server_fd 建议保持默认的水平触发(LT)，确保新连接不漏掉
    ev.events=EPOLLIN;      // 监听读事件（当有新客户端来连接时，server_fd 会触发读事件）
    ev.data.fd=server_fd;   // 把关联的 fd 存进去

    // EPOLL_CTL_ADD 代表将该 fd 添加到 epoll 监听树中
    if (epoll_ctl(epoll_fd,EPOLL_CTL_ADD,server_fd,&ev)==-1) {
        std::cerr << "将监听 fd 添加到 epoll 失败！" << std::endl;
        close(server_fd);
        close(epoll_fd);
        return -1;
    }

    // 初始化一个拥有 4 个核心工作线程的线程池
    ThreadPool pool(4);
    // 用于存放被唤醒的就绪事件数组
    epoll_event events[MAX_EVENTS];

    // 🔥 v6核心修改：使用哈希表集中管理所有在线连接的智能指针
    std::unordered_map<int,std::shared_ptr<Connection>> conn_map;

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

                // 接受连接后，必须立刻将该客户端 fd 设为非阻塞
                set_nonblocking(client_fd);

                // 🔥 v6关键动作 1：为新连接创建 shared_ptr，并强行托管到全局 Map 中
                // 此时全局 Map 持有它，引用计数为 1
                auto conn = std::make_shared<Connection>(client_fd);
                conn_map[client_fd]=conn;

                // 把这个新客户端的 client_fd 也注册到 epoll 监听名单里
                epoll_event client_ev{};
                // 注册事件时，显式加上 EPOLLET (边缘触发)
                client_ev.events=EPOLLIN | EPOLLET;       // 依然监听它发消息
                client_ev.data.fd=client_fd;
                epoll_ctl(epoll_fd,EPOLL_CTL_ADD,client_fd,&client_ev);
                std::cout << "【主线程】捕获新连接，已托管至 epoll，fd: " << client_fd << std::endl;
            }
            // 情况 B：如果是普通的 client_fd 有动静，说明【有客户端发消息过来了】
            else if (events[i].events & EPOLLIN) {
                // 先从 Map 里安全地取出这个连接的智能指针
                if (conn_map.find(current_fd)==conn_map.end())
                    continue;
                auto conn=conn_map[current_fd];

                char buffer[BUFFER_SIZE]={0};
                std::string total_req_str="";   //拼接数据
                bool is_closed=false;           //标记客户端是否断开
                // 🔥 v5关键修改 3：因为是 ET 模式，必须用 while(true) 循环读取，直到读空
                while (true) {
                    memset(buffer,0,sizeof(buffer));
                    ssize_t bytes_read=read(current_fd,buffer,sizeof(buffer)-1);

                    if (bytes_read>0)
                        total_req_str+=buffer;
                    else if (bytes_read==0) {
                        // read 返回 0，代表客户端关闭了连接
                        std::cout << "【主线程】监测到客户端下线，fd: " << current_fd << std::endl;
                        is_closed = true;
                        break; // 跳出读取循环
                    }
                    else {
                        // read 返回 -1。在非阻塞模式下，需要根据 errno 错误码进一步判断
                        // EAGAIN 或 EWOULDBLOCK 代表内核缓冲区已经空了，本次数据彻底读完了！
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            break; // 属于正常退出，数据读完了
                        }
                        // 如果是 EINTR，代表被系统信号中断，可以继续读，这里简单起见也按错误处理
                        std::cerr << "读取 fd " << current_fd << " 发生异常错误，错误码: " << errno << std::endl;
                        is_closed = true;
                        break;
                    }
                }

                //退出循环后继续处理后续
                if (is_closed) {
                    // 🔥 v6关键动作 2：客户端断开时，只从 epoll 树中摘除，并从全局 Map 中无情抹去！
                    // 注意：此时主线程绝对不手工调用 close(current_fd)！
                    epoll_ctl(epoll_fd,EPOLL_CTL_DEL,current_fd,nullptr);
                    conn_map.erase(current_fd);
                    std::cout << "【主线程】已将 fd " << current_fd << " 从全局 Map 中解绑。" << std::endl;
                }
                else if (!total_req_str.empty()) {
                    // 🔥 v6关键动作 3：通过 Lambda 表达式“按值捕获” conn 指针抛给线程池
                    // 按值复制会导致引用计数增加（此时至少为 2：Map 里有一个，Lambda 闭包里持有一个）

                    pool.enqueue([conn,total_req_str]() {
                       process_business(conn,total_req_str);
                    });
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