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
#include "Logger.hpp"

class Connection;

struct TimerNode {
    std::chrono::steady_clock::time_point expire_time;
    std::weak_ptr<Connection> conn_ptr;

    bool operator>(const TimerNode& other) const {
        return expire_time > other.expire_time;
    }
};

class TimerManager {
public:
    void add_timer(std::shared_ptr< Connection> conn,int timeout_seconds);
    void handle_expired_timers(std::unordered_map<int,std::shared_ptr<Connection>>& conn_map,int epoll_fd);
private:
    std::priority_queue<TimerNode,std::vector<TimerNode>,std::greater<TimerNode>> min_heap;
};

inline void TimerManager::add_timer(std::shared_ptr<Connection> conn,int timeout_seconds) {
    auto now=std::chrono::steady_clock::now();
    auto expire=now+std::chrono::seconds(timeout_seconds);
    conn->expire_time=expire;
    min_heap.push({expire,conn});
}

inline void TimerManager::handle_expired_timers(std::unordered_map<int,std::shared_ptr<Connection>>& conn_map, int epoll_fd) {
    auto now=std::chrono::steady_clock::now();

    while (!min_heap.empty()) {
        auto top=min_heap.top();

        if (top.expire_time>now)
            break;

        if (auto conn=top.conn_ptr.lock()) {
            if (conn->expire_time == top.expire_time) {
                std::string msg="[Timer] Zombie conn kicking fd: "+std::to_string(conn->fd);
                LOG_INFO(msg);
                epoll_ctl(epoll_fd,EPOLL_CTL_DEL,conn->fd,nullptr);
                conn_map.erase(conn->fd);
            }
        }
        min_heap.pop();
    }
}

#endif
