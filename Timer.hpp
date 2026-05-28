#ifndef TIMER_HPP
#define TIMER_HPP

#include <chrono>
#include <memory>
#include <queue>
#include <vector>
#include <unordered_map>
#include <sys/epoll.h>
#include <iostream>

#include "Connection.hpp"

// 前向声明 Connection 类，避免头文件循环包含
class Connection;

// 定时器节点结构体
struct TimerNode {
    std::chrono::steady_clock::time_point expire_time;      // 绝对过期时间点
    std::weak_ptr<Connection> conn_ptr;     // 🔥 必须用弱引用，防止循环强引用导致内存泄漏

    // 重载 > 运算符。因为 std::priority_queue 默认是大顶堆，
    // 我们用 > 就能把它强行逆转为“时间越小越在堆顶”的小顶堆
    bool operator>(const TimerNode& other) const {
        return expire_time > other.expire_time;
    }
};

class TimerManager {
public:
    // 添加或刷新一个定时器
    void add_timer(std::shared_ptr< Connection> conn,int timeout_seconds);

    // 核心清理函数：轮询堆顶，提出僵尸连接
    void handle_expired_timers(std::unordered_map<int,std::shared_ptr<Connection>>& conn_map,int epoll_fd);
private:

    // 使用STL容器组装小顶堆
    std::priority_queue<TimerNode,std::vector<TimerNode>,std::greater<TimerNode>> min_heap;
};

// 实现添加定时器
inline void TimerManager::add_timer(std::shared_ptr<Connection> conn,int timeout_seconds) {
    auto now=std::chrono::steady_clock::now();
    auto expire=now+std::chrono::seconds(timeout_seconds);

    // 1. 同步更新连接对象内部的绝对过期时间
    conn->expire_time=expire;

    // 2. 将新时间节点压入小顶堆
    min_heap.push({expire,conn});
}

// 实现清理僵尸连接
inline void TimerManager::handle_expired_timers(std::unordered_map<int,std::shared_ptr<Connection>>& conn_map, int epoll_fd) {
    auto now=std::chrono::steady_clock::now();

    while (!min_heap
        .empty()) {
        auto top=min_heap.top();

        // 如果堆顶（最早过期的节点）都没超时，说明后面的也绝对没超时，直接收工
        if (top.expire_time>now)
            break;

        // 堆顶超时了！尝试通过提升 weak_ptr 来检查连接是否还存活
        if (auto conn=top.conn_ptr.lock()) {
            // 🔥 【惰性删除核心判断】：
            // 如果连接对象的当前真实过期时间 和 堆顶这个节点的时间完全一致，说明它期间没说过话，是真的超时了！
            if (top.expire_time<=now) {
                std::cout << "【安全防御】检测到僵尸连接超时，正在强制踢人！fd: " << conn->fd << std::endl;
                // 从系统多路复用树中摘除，并从全局资产 Map 中抹去
                epoll_ctl(epoll_fd,EPOLL_CTL_DEL,conn->fd,nullptr);
                conn_map.erase(conn->fd);
                // 此时 Map 释放了它，只要没有未完成的后台任务，该连接会在这一刻自发析构并执行 close()
            }
            // 如果 conn->expire_time > now，说明期间小明发过消息，这个堆顶节点是“陈旧失效”的，不做处理
        }
        // 弹出堆顶已处理或失效的节点
        min_heap.pop();
    }
}

#endif