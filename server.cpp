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
#include <arpa/inet.h>      //用于ntohl和htonl
#include "Timer.hpp"
#include "Logger.hpp"
#include "MysqlConnPool.hpp"

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

// 🔥 【V9 动态路由版】：支持长连接连环解包 + 锁外长耗时加工 + 多维纵深安全防御
void process_business(std::shared_ptr<Connection> conn) {
    std::vector<std::string> ready_messages;
    bool maflipped_packet=false;        //标记是否遭遇恶意攻击
    // 步骤 A：短暂加锁，驱动状态机将缓冲区里的字节流数据进行齿轮跳转解析
    {
        std::lock_guard<std::mutex> lock(conn->buffer_mutex);

        // 循环解析，直到缓冲区里的数据不够凑成一个完整的 HTTP 包（支持 Pipeline 长连接连环解包）
        while (true) {
            // 检查一：如果缓冲区被榨干读空了，直接退出循环
            if (conn->read_buffer.readable_bytes()==0)
                break;

            // 检查二【恶意攻击防御】：驱动有限状态机进行解析
            // 如果 parse 返回 false，代表触发了 HttpParser 内部布下的 8KB 头部淹没洪水攻击防御
            if (!conn->http_parser.parse(conn->read_buffer)) {
                maflipped_packet=true;
                break;
            }

            // 检查三：如果状态机本轮成功推到了 FINISH 状态，说明一个绝对完整的 HTTP 包躺在里面了
            if (conn->http_parser.status()==HttpParser::PARSE_FINISH) {
                // 纵深防御：核对 HTTP 协议内的 Content-Length 身体长度
                std::string content_len_str=conn->http_parser.get_header("Content-Length");
                if (!content_len_str.empty()) {
                    try {
                        int content_len=std::stoi(content_len_str);
                        // 继承 V8 的硬核防御：如果恶意虚报 Body 长度超过 MAX_PACKET_SIZE (64KB) 或小于 0
                        if (content_len > MAX_PACKET_SIZE || content_len < 0) {
                            std::cerr << "【安全警报】fd " << conn->fd << " 虚报 Content-Length: "
                                      << content_len << " 字节，触发纵深拉闸！" << std::endl;
                            maflipped_packet = true;
                            break;
                        }
                    }catch (...) {
                        maflipped_packet=true;
                        break;
                    }
                }
                // 精准提取出本次请求的有效信息（例如客户端请求的 URL 路径），塞进 ready 队列
                ready_messages.push_back(conn->http_parser.url());

                // 🔥【核心复位】：由于 TCP 是字节流，为了让 while(true) 继续解析下一个长连接请求，
                // 必须在把包拿走后，立刻将状态机重置回初始状态（PARSE_REQUESTLINE），满血迎接下一发数据
                conn->http_parser.reset();
                // 🔥【铁律修复】：成功解析完一包后，必须将已被消费的缓冲区彻底清空！
                // 这样下一轮循环时 readable_bytes() 就会归零，从而清脆地 break 弹出循环

            }else {
                // 如果状态机状态不是 FINISH，说明遭遇了【流式拆包】：缓冲区有残余数据但不够凑成完整一包
                // 退出循环，等主线程在 epoll 驱动下把下一次的数据追加进来
                break;
            }
        }
    }
    // 如果是恶意包，直接关闭套接字，主线程对应的 Map 会在下次读事件或心跳中彻底清理
    if (maflipped_packet) {
        shutdown(conn->fd, SHUT_RDWR); // 优雅关闭读写通道，逼迫其下线
        return;
    }
    // 锁外执行业务逻辑
    for (const auto& url_path : ready_messages) {
        // std::this_thread::sleep_for(std::chrono::seconds(1));
        LOG_INFO("【工作线程】安全解码成功！内容: "+url_path);
        //std::cout << "【工作线程】安全解码成功！内容: " << url_path << std::endl;

        std::string chat_prefix="/chat?msg=";
        std::string http_response="";

        // 🔀 路由分支 A：如果 URL 匹配到了我们的聊天特区暗号
        if (url_path.find(chat_prefix) == 0) {
            // 1. 提取出 `/chat?msg=` 后面的纯文本密文
            std::string raw_msg=url_path.substr(chat_prefix.length());

            // 2. 【新增防御】：URL 反向解码，将网络传输中的 %20 还原回空格 ' '
            size_t pos;
            while ((pos=raw_msg.find("%20"))!=std::string::npos)
                raw_msg.replace(pos,3," ");

            // 🛡️【v11新增工业级数据落地】：利用 RAII 机制安全借出连接
            MYSQL* mysql_conn=nullptr;
            {
                ConnectionRAII mysql_guard(&mysql_conn);        // 🌟 构造函数内自动向池子借出一条连接

                if (mysql_conn) {
                    // 组装一条安全的 SQL 插入语句，把聊天消息持久化到 chat_log 表中
                    std::string sql_query = "INSERT INTO chat_log(message, chat_time) VALUES('" + raw_msg + "', NOW());";

                    if (mysql_query(mysql_conn,sql_query.c_str())==0) {
                        LOG_INFO("成功将聊天记录持久化写入 MySQL 数据库。");
                    } else {
                        LOG_ERROR("SQL 语句执行失败！原因: " + std::string(mysql_error(mysql_conn)));
                    }
                }

            }

            // 3. 组装干净的聊天文本响应体
            std::string reply_body = "【V9 核心聊天回执】: " + raw_msg;
            // 4. 打包成合规的 HTTP 协议头，Content-Type 声明为纯文本 text/plain
            http_response =
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/plain; charset=utf-8\r\n"
                "Content-Length: " + std::to_string(reply_body.length()) + "\r\n"
                "Connection: keep-alive\r\n"
                "\r\n" +
                reply_body;

            //std::cout << "【工作线程】成功投递聊天文本响应。" << std::endl;
            LOG_INFO("【工作线程】成功投递聊天文本响应。");

        }
        else {
            // 组装标准网页真实源码内容 (HTML Body)
            std::string html_content =
                "<html>"
                "<head><title>Tiny-Reactor V9</title><meta charset='utf-8'></head>"
                "<body style='background-color:#f0f2f5; font-family:sans-serif; text-align:center; padding-top:50px;'>"
                "<h1 style='color:#1890ff;'>🚀  Tiny-Reactor V9 </h1>"
                "<p style='color:#555;'>这是一个标准的单 Reactor 多线程异步 HTTP 服务器</p>"
                "<div style='background:#fff; border-radius:8px; display:inline-block; padding:20px; box-shadow:0 4px 12px rgba(0,0,0,0.1);'>"
                "<strong>当前运行模式：</strong> 边缘触发ET + 非阻塞I/O + 智能指针安全闭环 + 堆时钟定时器 + 手撕有限状态机"
                "</div>"
                "</body>"
                "</html>";
            // 严格遵循工业级 HTTP 规范，拼装合规的 HTTP 响应报文（包含状态行、响应头、空行、响应体）
            http_response =
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/html; charset=utf-8\r\n"
                "Content-Length: " + std::to_string(html_content.length()) + "\r\n"
                "Connection: keep-alive\r\n"  // 明确支持长连接
                "\r\n" +
                html_content;
        }
        send(conn->fd,http_response.c_str(),http_response.length(),0);

    }

}

int main() {

    // 初始化双缓冲日志引擎，所有的日志将被打入当前目录下的 server.log 文件中
    Logger::getInstance().init("server.log");
    LOG_INFO("========== Tiny-Reactor 异步日志系统成功启动 ==========");

    // 🔥【v11新增】：启动并创建 8 个 MySQL 物理连接的动态复用池
    // 请根据 Linux 本地的实际数据库配置修改：IP, 用户名, 密码, 数据库名, 端口, 连接数
    if (!MysqlConnPool::getInstance().init("127.0.0.1", "root", "root", "chat_db", 3306, 8)) {
        LOG_ERROR("数据库连接池启动失败，服务器拉闸！");
        return -1;
    }

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

    // v8新增 实例化一个定时器管理器
    TimerManager timer_manager;

    while (true) {
        // 🔥 v8 关键重构点：把最后一个参数从 -1 (永久阻塞) 改为 1000 (1秒超时醒来一次)
        // 这样即使没有任何网络网络事件发生，主线程每隔 1 秒也会自动醒来，执行下面的僵尸清理

        int nfds=epoll_wait(epoll_fd,events,MAX_EVENTS,1000);
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
                LOG_INFO("成功接受客户端连接，分配 fd: "+std::to_string(client_fd));
                //std::cout <<  "成功接受客户端连接，分配 fd: "<< client_fd << std::endl;

                // 接受连接后，必须立刻将该客户端 fd 设为非阻塞
                set_nonblocking(client_fd);

                auto conn = std::make_shared<Connection>(client_fd);
                conn_map[client_fd]=conn;

                // 🔥 v8关键重构点：新连接注册成功后，立刻为其绑定一个 15 秒过期的定时器
                timer_manager.add_timer(conn,15);
                // 把这个新客户端的 client_fd 也注册到 epoll 监听名单里
                epoll_event client_ev{};
                // 注册事件时，显式加上 EPOLLET (边缘触发)
                client_ev.events=EPOLLIN | EPOLLET;       // 依然监听它发消息
                client_ev.data.fd=client_fd;
                epoll_ctl(epoll_fd,EPOLL_CTL_ADD,client_fd,&client_ev);
                LOG_INFO("【主线程】捕获新连接，已托管至 epoll，fd: "+std::to_string(client_fd));
                //std::cout << "【主线程】捕获新连接，已托管至 epoll，fd: " << client_fd << std::endl;
            }
            // 情况 B：如果是普通的 client_fd 有动静
            else if (events[i].events & EPOLLIN) {
                // 先从 Map 里安全地取出这个连接的智能指针
                if (conn_map.find(current_fd)==conn_map.end())
                    continue;
                auto conn=conn_map[current_fd];

                // 🔥 v8关键重构点：只要客户端有数据交互，说明它还活着，立刻为其“续命” 15 秒！
                timer_manager.add_timer(conn, 15);

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
                        LOG_INFO("【主线程】监测到客户端下线，fd: "+std::to_string(current_fd));
                        // std::cout << "【主线程】监测到客户端下线，fd: " << current_fd << std::endl;
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
                    LOG_INFO("【主线程】已删除 fd: "+std::to_string(current_fd)+"从全局 Map 中解绑。 ");
                    //std::cout << "【主线程】已将 fd " << current_fd << " 从全局 Map 中解绑。" << std::endl;
                }
                else {

                    pool.enqueue([conn]() {
                       process_business(conn);
                    });
                }
            }
        }
        // 🔥 【V8 核心大招】：每轮事件处理完或 1 秒超时醒来，主线程雷打不动地执行一次堆顶盘点
        timer_manager.handle_expired_timers(conn_map, epoll_fd);
    }

    // 8.close
    close(server_fd);
    close(epoll_fd);
    std::cout<<"close"<<"\n";
    return 0;
}