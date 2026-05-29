#ifndef SUB_REACTOR_HPP
#define SUB_REACTOR_HPP

#include <iostream>
#include <unordered_map>
#include <memory>
#include <thread>
#include <mutex>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include "Connection.hpp"
#include "Timer.hpp"        // 引入堆时钟定时器
#include "ThreadPool.hpp"   // 引入线程池声明

// 声明外部全局线程池指针，方便子 Reactor 跨线程投递业务
extern ThreadPool* g_pool;
// 声明外部的业务加工函数，保持逻辑解耦
void process_business(std::shared_ptr< Connection> conn);

class SubReactor {
public:
    int sub_epoll_fd;
    int wakeup_fd;      // 跨线程唤醒的利器（eventfd）
    std::thread sub_thread;

    // 每个子 Reactor 拥有自己独立的连接管理生命周期 Map，告别全局大锁
    std::unordered_map<int,std::shared_ptr< Connection>> sub_conn_map;
    std::mutex sub_mutex;       // 保护 sub_conn_map 的动态增删安全

    TimerManager timer_manager;

    SubReactor() {
        // 1. 创建属于子线程独立的 epoll 监听实例
        sub_epoll_fd=epoll_create1(0);
        if (sub_epoll_fd==-1) {
            LOG_ERROR("子 Reactor 创建 epoll 失败！");
            exit(-1);
        }

        // 2. 创生内核级 eventfd。极其轻量，专门用于主线程跨线程通知子线程
        wakeup_fd=eventfd(0,EFD_NONBLOCK|EFD_CLOEXEC);
        if (wakeup_fd==-1) {
            LOG_ERROR("子 Reactor 创建 eventfd 失败！");
            exit(-1);
        }

        // 3. 把这个唤醒管道 wakeup_fd 注册进子 epoll 树中
        epoll_event ev{};
        ev.events=EPOLLIN|EPOLLET;
        ev.data.fd=wakeup_fd;
        epoll_ctl(sub_epoll_fd,EPOLL_CTL_ADD,wakeup_fd,&ev);
    }

    ~SubReactor() {
        if (sub_thread.joinable())
            sub_thread.join();
        close(sub_epoll_fd);
        close(wakeup_fd);
    }

    // 由主线程拉闸，让子 Reactor 的专属事件循环并发
    void start() {
        sub_thread=std::thread([this]() {
            this->run();
        });
    }

    // 主线程调用的核心接口：跨越线程边界，向该子 Reactor 派发连接
    void dispatch_connection(std::shared_ptr<Connection> conn) {
        {
            std::lock_guard<std::mutex> lock(sub_mutex);
            sub_conn_map[conn->fd] =conn;       // 锁内安全注入各自的专属 Map
        }
        // 将该客户端注册进子 Reactor 专属的 epoll 树中
        epoll_event ev{};
        ev.events=EPOLLIN|EPOLLET;
        ev.data.fd=conn->fd;
        epoll_ctl(sub_epoll_fd,EPOLL_CTL_ADD,conn->fd,&ev);

        timer_manager.add_timer(conn,60);

        // 向 eventfd 物理写入一个 8 字节的整数（触发计数加1）
        // 从而瞬间强行唤醒可能正在阻塞在 epoll_wait 的子线程事件循环！
        uint64_t signal_val=1;
        write(wakeup_fd,&signal_val,sizeof(signal_val));
    }
private:
    // 子线程专属的死循环事件响应中枢
    void run() {
        const int MAX_SUB_EVENTS=1024;
        epoll_event events[MAX_SUB_EVENTS];
        char buffer[1024]={0};

        while (true) {
            // 每一个子 Reactor 沉睡在自己的 epoll 树上，有事件则醒，无事件则挂起，绝不浪费算力
            int nfds=epoll_wait(sub_epoll_fd,events,MAX_SUB_EVENTS,1000);
            if (nfds==-1) {
                if (errno==EINTR) continue;
                break;
            }

            for (int i=0;i<nfds;++i) {
                int current_fd=events[i].data.fd;

                // 🔥 情况一：如果是主线程通过 eventfd 飘过来的唤醒信号，必须消费计数器！
                if (current_fd==wakeup_fd) {
                    uint64_t val;
                    read(wakeup_fd,&val,sizeof(val));   // 消费 eventfd 计数器，为下次唤醒重置边缘
                    continue;
                }

                // 情况二 & 三：优先处理 EPOLLIN（即使同时携带 EPOLLHUP，也要先把数据读完）
                if (events[i].events&EPOLLIN) {
                    std::shared_ptr<Connection> conn=nullptr;
                    {
                        std::lock_guard<std::mutex> lock(sub_mutex);
                        auto it=sub_conn_map.find(current_fd);
                        if (it!=sub_conn_map.end()) {
                            conn=it->second;
                        }
                    }
                    if (conn) {
                        // 刷新时间
                        timer_manager.add_timer(conn,60);

                        bool is_closed =false;
                        // 子 Reactor 线程在 ET 边缘触发下拉取字节流
                        while (true) {
                            memset(buffer,0,sizeof(buffer));
                            ssize_t bytes_read=read(current_fd,buffer,sizeof(buffer)-1);

                            if (bytes_read>0) {
                                std::lock_guard<std::mutex> lock(conn->buffer_mutex);
                                conn->read_buffer.append(buffer,bytes_read);

                            }else if (bytes_read==0) {
                                is_closed=true;
                                break;
                            }else {
                                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                    break; // 数据读干净了，正常退出
                                }
                                is_closed = true;
                                break;
                            }
                        }
                        if (is_closed) {
                            // 客户端主动断开或异常，子 Reactor 现场就地枪决清理
                            epoll_ctl(sub_epoll_fd,EPOLL_CTL_DEL,current_fd,nullptr);
                            std::lock_guard<std::mutex> lock(sub_mutex);
                            sub_conn_map.erase(current_fd);
                        }else {
                            // 数据读完后，将长耗时状态机解析与数据库持久化打包扔进线程池！
                            if (g_pool) {
                                g_pool->enqueue([conn]() {
                                    process_business(conn);
                                });
                            }
                        }

                    }
                }

                // 情况四：纯 EPOLLERR/EPOLLHUP（无 EPOLLIN），连接已断开，现场清理
                if ((events[i].events & (EPOLLERR | EPOLLHUP))
                    && sub_conn_map.count(current_fd)) {
                    epoll_ctl(sub_epoll_fd,EPOLL_CTL_DEL,current_fd,nullptr);
                    std::lock_guard<std::mutex> lock(sub_mutex);
                    sub_conn_map.erase(current_fd);
                }
            }
            // 去中心化盘点】：每轮事件结束，子 Reactor 独立清算自己树上的僵尸连接
            timer_manager.handle_expired_timers(sub_conn_map, sub_epoll_fd);
        }
    }
};


#endif
