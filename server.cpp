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
#include <unordered_map>    //用于全局管理连接生命周期
#include <memory>           //智能指针核心头文件
#include "Connection.hpp"   //引入连接封装类
#include <arpa/inet.h>      //v7新增 用于ntohl和htonl

const int MAX_PACKET_SIZE=65535;
const int MAX_EVENTS=1024;
const int BUFFER_SIZE=1024;


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

// 🔥 线程池中的核心业务：流式协议解码状态机
void process_business(std::shared_ptr<Connection> conn) {
    std::vector<std::string> ready_messages;
    bool maflipped_packet=false;        //标记是否遭遇恶意攻击

    {
        std::lock_guard<std::mutex> lock(conn->buffer_mutex);
        // 循环解析，直到缓冲区里的数据不够凑成一个完整的包
        while (true) {
            // 1. 检查有没有满 4 字节的 Header
            if (conn->read_buffer.readable_bytes()<4)
                break;      //数据不够，下一次收包
            // 2. 窥探前 4 字节，读出 Body 的目标长度
            int raw_len=0;
            std::memcpy(&raw_len,conn->read_buffer.peek(),4);

            // v7🔥 优化一：将网络字节序（大端）安全转换为当前主机字节序
            int target_body_len = ntohl(raw_len);

            // v7🔥 优化二：边界防御。包长度小于0或大于64KB，直接判定为恶意/畸形包
            if (target_body_len > MAX_PACKET_SIZE || target_body_len < 0) {
                std::cerr << "【安全警报】检测到非法畸形大包，长度: " << target_body_len
                          << "，来自 fd: " << conn->fd << "。断开连接！" << std::endl;
                maflipped_packet = true;
                break;
            }

            // 3. 检查缓冲区里的剩余总数据，是否满足（Header 4字节 + Body 长度）
            if (conn->read_buffer.readable_bytes()<(4+target_body_len))     // 发生了拆包：虽然拿到了长度，但身体还没完全传输过来
                break;

            // 4. 说明有一个绝对完整的包躺在里面了。先剥离 4 字节头部
            conn->read_buffer.retrieve(4);
            // 5. 精准提取出指定长度的 Body 数据
            std::string request_msg(conn->read_buffer.peek(),target_body_len);
            conn->read_buffer.retrieve(target_body_len);

            ready_messages.push_back(request_msg);

        }
    }
    // 如果是恶意包，直接关闭套接字，主线程对应的 Map 会在下次读事件或心跳中彻底清理
    if (maflipped_packet) {
        shutdown(conn->fd, SHUT_RDWR); // 优雅关闭读写通道，逼迫其下线
        return;
    }
    // 锁外执行业务逻辑
    for (const auto& request_msg : ready_messages) {
        std::this_thread::sleep_for(std::chrono::seconds(2));
        std::cout << "【工作线程】安全解码成功！内容: " << request_msg << std::endl;

        std::string response = "v7收到了消息：" + request_msg;
        send(conn->fd, response.c_str(), response.length(), 0);
    }

}

int main() {
    // 1.socket
    int server_fd=socket(AF_INET,SOCK_STREAM,0);
    //设置端口复用
    int opt=1;
    setsockopt(server_fd,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));

    // 2.addr and port
    sockaddr_in server_addr{};
    server_addr.sin_family=AF_INET;     //ipv4
    server_addr.sin_addr.s_addr=INADDR_ANY;     //监听所有可用网络接口
    server_addr.sin_port=htons(8088);
    // 3.bind
    bind(server_fd,(struct sockaddr*)&server_addr,sizeof(server_addr));

    // 4.listen
    listen(server_fd,5);
    std::cout<<"listening 8088"<<"\n";

    // 创建epoll实例
    // epoll_create1(0) 是现代 Linux 推荐写法
    int epoll_fd=epoll_create1(0);
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

    // 初始化一个拥有 4 个核心工作线程的线程池
    ThreadPool pool(4);
    epoll_event events[MAX_EVENTS];

    std::unordered_map<int,std::shared_ptr<Connection>> conn_map;

    while (true) {
        int nfds=epoll_wait(epoll_fd,events,MAX_EVENTS,-1);
        if (nfds == -1) {
            std::cerr << "epoll_wait 错误！" << std::endl;
            break;
        }

        // 依次处理 epoll_wait 返回的就绪事件
        for (int i=0;i<nfds;i++) {
            int current_fd=events[i].data.fd;

            // 情况 A：如果是 server_fd 有动静
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
            // 情况 B：如果是普通的 client_fd 有动静
            else if (events[i].events & EPOLLIN) {
                // 先从 Map 里安全地取出这个连接的智能指针
                if (conn_map.find(current_fd)==conn_map.end())
                    continue;
                auto conn=conn_map[current_fd];

                char buffer[BUFFER_SIZE]={0};
                std::string total_req_str="";   //拼接数据
                bool is_closed=false;           //标记客户端是否断开

                // 🔥 v7主线程职责非常纯粹：利用 ET 模式疯狂卸货，全部追加进连接的内部 Buffer
                while (true) {
                    memset(buffer,0,sizeof(buffer));
                    ssize_t bytes_read=read(current_fd,buffer,sizeof(buffer)-1);

                    if (bytes_read>0)
                        conn->read_buffer.append(buffer,bytes_read);
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
                    // 注意：此时主线程绝对不手工调用 close(current_fd)！
                    epoll_ctl(epoll_fd,EPOLL_CTL_DEL,current_fd,nullptr);
                    conn_map.erase(current_fd);
                    std::cout << "【主线程】已将 fd " << current_fd << " 从全局 Map 中解绑。" << std::endl;
                }
                else {

                    pool.enqueue([conn]() {
                       process_business(conn);
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