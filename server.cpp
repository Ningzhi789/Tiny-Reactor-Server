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
#include "SubReactor.hpp"
#include <sys/sendfile.h>   // V12 零拷贝必备系统调用
#include <sys/stat.h>       // 获取静态文件状态（大小）必备
#include <sys/stat.h>       // 用于读取网页文件属性
#include <openssl/ssl.h>    // 引入 SSL 核心
#include <openssl/err.h>    // 增加这行
#include <atomic>           // 引入原子级无锁安全计数器

const int MAX_PACKET_SIZE=65535;
const int MAX_EVENTS=1024;
const int BUFFER_SIZE=1024;

ThreadPool* g_pool =nullptr;
SSL_CTX* g_ssl_ctx=nullptr;

// 增加以下三发全局原子雷达，无锁高能统计系统状态
std::atomic<uint64_t> g_metrics_total_requests{0};  // 总请求吞吐数
std::atomic<uint64_t> g_metrics_chat_count{0};      // 聊天业务命中数
std::atomic<uint64_t> g_metrics_static_count{0};    // 静态网页请求数

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

// 🔥 【V12 动态路由版】：工作线程被唤醒后，直接持锁执行解析、SQL落地和回执发送
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
        LOG_INFO("Worker decoded msg: " + url_path);

        // 只要从网络包里成功剥离出一条有效路由，总吞吐指标立刻无锁自增 1
        ++g_metrics_total_requests;

        std::string chat_prefix="/chat?msg=";
        std::string http_response="";

        // 👑 1. 首位拦截：如果监控中心前来拉取时序性能指标
        if (url_path=="/metrics") {
            LOG_INFO("Prometheus监控中心发起拉去请求，正在清算时序指标");

            // 组装完全对齐 Prometheus 工业规范的明文时序度量单据
            std::string metrics_body =
                "# HELP tiny_reactor_requests_total Total processing throughput count\n"
                "# TYPE tiny_reactor_requests_total counter\n"
                "tiny_reactor_requests_total " + std::to_string(g_metrics_total_requests.load()) + "\n\n"

                "# HELP tiny_reactor_chat_business_total Total processed chat logs\n"
                "# TYPE tiny_reactor_chat_business_total counter\n"
                "tiny_reactor_chat_business_total " + std::to_string(g_metrics_chat_count.load()) + "\n\n"

                "# HELP tiny_reactor_static_pages_total Total zero-copy or static web requests\n"
                "# TYPE tiny_reactor_static_pages_total counter\n"
                "tiny_reactor_static_pages_total " + std::to_string(g_metrics_static_count.load()) + "\n";

            // 打包成标准的 Prometheus 监控回执响应头
            http_response =
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n"
                "Content-Length: " + std::to_string(metrics_body.length()) + "\r\n"
                "Connection: keep-alive\r\n\r\n" +
                metrics_body;

            SSL_write(conn->ssl,http_response.c_str(),http_response.length());
        }
        // 🔀 路由分支 A：如果 URL 匹配到了我们的聊天特区暗号
        else if (url_path.find(chat_prefix) == 0) {
            // 1. 提取出 `/chat?msg=` 后面的纯文本密文
            std::string raw_msg=url_path.substr(chat_prefix.length());
            ++g_metrics_chat_count;
            // 2. 【新增防御】：URL 反向解码，将网络传输中的 %20 还原回空格 ' '
            size_t pos;
            while ((pos=raw_msg.find("%20"))!=std::string::npos)
                raw_msg.replace(pos,3," ");

            // 🛡️【v11新增工业级数据落地】：利用 RAII 机制安全借出连接
            MYSQL* mysql_conn=nullptr;
            {
                ConnectionRAII mysql_guard(&mysql_conn);        // 🌟 构造函数内自动向池子借出一条连接

                if (mysql_conn) {
                    // V12 工业级安全落地：利用预编译参数化查询（Prepare Statement）隔离数据与指令
                    MYSQL_STMT* stmt=mysql_stmt_init(mysql_conn);

                    // 1. 将带有 ? 占位符的 SQL 骨架送入内核锁定语法树
                    std::string sql="INSERT INTO chat_log(message, chat_time) VALUES(?, NOW());";
                    mysql_stmt_prepare(stmt,sql.c_str(),sql.length());

                    // 2. 绑定具体的变量参数
                    MYSQL_BIND bind[1];
                    memset(bind,0,sizeof(bind));
                    bind[0].buffer_type=MYSQL_TYPE_STRING;
                    bind[0].buffer=(char*)raw_msg.c_str();
                    bind[0].buffer_length=raw_msg.length();

                    // 3. 安全注入并物理执行
                    mysql_stmt_bind_param(stmt,bind);
                    if (mysql_stmt_execute(stmt) == 0) {
                        LOG_INFO("STMT prepared statement persisted chat log.");
                    } else {
                        LOG_ERROR("STMT execute failed: " + std::string(mysql_stmt_error(stmt)));
                    }
                    mysql_stmt_close(stmt);
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
            LOG_INFO("Chat text response sent.");

            {
                std::lock_guard<std::mutex> ssl_lock(conn->ssl_mutex);
                SSL_write(conn->ssl,http_response.c_str(),http_response.length());
            }
        }else {
            LOG_INFO("Static page request, sendfile zero-copy.");
            ++g_metrics_static_count;
            // 💡 提示：需要在服务器同级目录下提前新建一个真实的文本文件 index.html
            std::string filepath="index.html";
            int file_fd=open(filepath.c_str(),O_RDONLY);
            if (file_fd==-1) {
                std::string err_404 = "HTTP/1.1 404 NOT FOUND\r\nContent-Length: 0\r\n\r\n";
                {
                    std::lock_guard<std::mutex> ssl_lock(conn->ssl_mutex);
                    SSL_write(conn->ssl, err_404.c_str(), err_404.length());
                }
                return;
            }

            struct stat stat_buf;
            fstat(file_fd, &stat_buf);

            // ① 发送标准的 HTTP 协议报头
            std::string header = "HTTP/1.1 200 OK\r\n"
                             "Content-Type: text/html; charset=utf-8\r\n"
                             "Content-Length: " + std::to_string(stat_buf.st_size) + "\r\n"
                             "Connection: keep-alive\r\n\r\n";

            {
                std::lock_guard<std::mutex> ssl_lock(conn->ssl_mutex);
                SSL_write(conn->ssl,header.c_str(),header.length());
            }
            // ② 核心权衡：在用户态通过 4KB 缓冲区边读盘边利用 SSL_write 加密发射
            char file_buf[4096];
            while (true) {
                ssize_t r_bytes =read(file_fd,file_buf,sizeof(file_buf));
                if (r_bytes<=0) break;
                {
                    std::lock_guard<std::mutex> ssl_lock(conn->ssl_mutex);
                    SSL_write(conn->ssl,file_buf,r_bytes);
                }
            }
            close(file_fd);
        }
    }
}

int main() {

    // 初始化双缓冲日志引擎，所有的日志将被打入当前目录下的 server.log 文件中
    Logger::getInstance().init("server.log");
    LOG_INFO("========== Tiny-Reactor Async Logger Started ==========");

    // ➕ 核心注入①：空降 OpenSSL 引擎整体初始化与安全证书挂载大闸
    SSL_library_init();
    OpenSSL_add_all_algorithms();
    SSL_load_error_strings();
    g_ssl_ctx=SSL_CTX_new(TLS_server_method());
    if (!g_ssl_ctx) {
        LOG_ERROR("SSL_CTX_new failed!");
        ERR_print_errors_fp(stderr);
        return -1;
    }

    // 强制加载证书与私钥
    if (SSL_CTX_use_certificate_file(g_ssl_ctx, "server.crt", SSL_FILETYPE_PEM)<=0||SSL_CTX_use_PrivateKey_file(g_ssl_ctx, "server.key", SSL_FILETYPE_PEM)<=0) {
        LOG_ERROR("SSL cert or key load failed, server aborting!");
        return -1;
    }

    if (!MysqlConnPool::getInstance().init("127.0.0.1", "root", "root", "chat_db", 3306, 8)) {
        LOG_ERROR("MySQL connection pool init failed, server aborting!");
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
    g_pool=&pool;
    // 筑巢子 Reactor 池：根据 CPU 核心数启动 3 个独立的 I/O 子线程
    const int SUB_REACTOR_NUM=3;
    std::vector<std::unique_ptr<SubReactor>> sub_reactors;
    for (int i=0;i<SUB_REACTOR_NUM;i++) {
        sub_reactors.push_back(std::make_unique<SubReactor>());
        sub_reactors[i]->start();   // 驱动各个子 Reactor 事件死循环运转
    }
    int rr_index=0;     // 用于 Round-Robin 轮询分发的递增游标

    // std::unordered_map<int,std::shared_ptr<Connection>> conn_map;
    // TimerManager timer_manager;

    // 主 Reactor 纯净的专属事件死循环
    while (true) {
        // 主线程永久阻塞死守监听套接字即可，再也无需兼顾僵尸清理，彻底解放
        int nfds=epoll_wait(epoll_fd,events,MAX_EVENTS,1000);
        if (nfds == -1) {
            std::cerr << "epoll_wait 错误！" << std::endl;
            break;
        }

        // 依次处理 epoll_wait 返回的就绪事件
        for (int i=0;i<nfds;i++) {
            int current_fd=events[i].data.fd;

            // 主线程唯一关心的动静：8088 端口新客户敲门
            if (current_fd==server_fd) {
                sockaddr_in client_addr{};
                socklen_t client_len=sizeof(client_addr);
                int client_fd=accept(server_fd,(struct sockaddr*)&client_addr,&client_len);
                if (client_fd < 0) {
                    std::cerr << "接受新连接失败！" << std::endl;
                    continue;
                }
                LOG_INFO("Accepted client fd: "+std::to_string(client_fd));
                //std::cout <<  "成功接受客户端连接，分配 fd: "<< client_fd << std::endl;

                // 接受连接后，必须立刻将该客户端 fd 设为非阻塞
                set_nonblocking(client_fd);

                auto conn = std::make_shared<Connection>(client_fd);

                // v14 核心：为每一个新降生的连接描述符分发 SSL 盾牌，并初始化为 Accept 接收状态
                conn->ssl=SSL_new(g_ssl_ctx);
                if (!conn->ssl) {
                    LOG_ERROR("SSL_new failed! fd: " + std::to_string(client_fd));
                    ERR_print_errors_fp(stderr);
                    close(client_fd);
                    continue;
                }
                SSL_set_fd(conn->ssl,client_fd);
                SSL_set_accept_state(conn->ssl);

                // 👑 核心派发：扔给选中的子 Reactor 托管，并自增 rr_index 游标
                sub_reactors[rr_index]->dispatch_connection(conn);
                LOG_INFO("Dispatched fd "+std::to_string(client_fd)+" to SubReactor ["+std::to_string(rr_index)+"]");
                rr_index=(rr_index+1)%SUB_REACTOR_NUM;

                // 🔥 关键修复：委派后，必须把 client_fd 从主 Reactor 的 epoll 中移除！
                // 主 Reactor 不需要关心 client fd 的任何事件，交给子 Reactor 全权管理
                // 如果不移除，主 epoll（LT 模式）也会收到 client fd 的 EPOLLIN，造成混乱
                epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client_fd, nullptr);
            }
        }
    }

    // 8.close
    close(server_fd);
    close(epoll_fd);
    std::cout<<"close"<<"\n";
    return 0;
}