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
#include "Timer.hpp"
#include "ThreadPool.hpp"
#include <openssl/ssl.h>
#include <openssl/err.h>

extern ThreadPool* g_pool;
void process_business(std::shared_ptr< Connection> conn);

class SubReactor {
public:
    int sub_epoll_fd;
    int wakeup_fd;
    std::thread sub_thread;
    std::unordered_map<int,std::shared_ptr< Connection>> sub_conn_map;
    std::mutex sub_mutex;
    TimerManager timer_manager;

    SubReactor() {
        sub_epoll_fd=epoll_create1(0);
        if (sub_epoll_fd==-1) { LOG_ERROR("SubReactor: failed to create epoll!"); exit(-1); }
        wakeup_fd=eventfd(0,EFD_NONBLOCK|EFD_CLOEXEC);
        if (wakeup_fd==-1) { LOG_ERROR("SubReactor: failed to create eventfd!"); exit(-1); }
        epoll_event ev{}; ev.events=EPOLLIN|EPOLLET; ev.data.fd=wakeup_fd;
        epoll_ctl(sub_epoll_fd,EPOLL_CTL_ADD,wakeup_fd,&ev);
    }

    ~SubReactor() {
        if (sub_thread.joinable()) sub_thread.join();
        close(sub_epoll_fd); close(wakeup_fd);
    }

    void start() { sub_thread=std::thread([this]() { this->run(); }); }

    void dispatch_connection(std::shared_ptr<Connection> conn) {
        { std::lock_guard<std::mutex> lock(sub_mutex); sub_conn_map[conn->fd]=conn; }
        epoll_event ev{}; ev.events=EPOLLIN|EPOLLONESHOT; ev.data.fd=conn->fd;
        epoll_ctl(sub_epoll_fd,EPOLL_CTL_ADD,conn->fd,&ev);
        timer_manager.add_timer(conn,60);
        uint64_t signal_val=1; write(wakeup_fd,&signal_val,sizeof(signal_val));
    }
private:
    void run() {
        const int MAX_SUB_EVENTS=1024;
        epoll_event events[MAX_SUB_EVENTS];
        char buffer[1024]={0};

        while (true) {
            int nfds=epoll_wait(sub_epoll_fd,events,MAX_SUB_EVENTS,1000);
            if (nfds==-1) { if (errno==EINTR) continue; break; }

            for (int i=0;i<nfds;++i) {
                int current_fd=events[i].data.fd;

                if (current_fd==wakeup_fd) {
                    uint64_t val; read(wakeup_fd,&val,sizeof(val));
                    {
                        std::lock_guard<std::mutex> lock(sub_mutex);
                        for(auto& kv : sub_conn_map) {
                            if(kv.second->needs_rearm.load()) {
                                kv.second->needs_rearm.store(false);
                                epoll_event rev{}; rev.events=EPOLLIN|EPOLLET|EPOLLONESHOT; rev.data.fd=kv.first;
                                epoll_ctl(sub_epoll_fd,EPOLL_CTL_MOD,kv.first,&rev);
                            }
                        }
                    }
                    continue;
                }

                if (events[i].events&EPOLLIN) {
                    std::shared_ptr<Connection> conn=nullptr;
                    {
                        std::lock_guard<std::mutex> lock(sub_mutex);
                        auto it=sub_conn_map.find(current_fd);
                        if (it!=sub_conn_map.end()) conn=it->second;
                    }
                    if (!conn) continue;

                    if (!conn->ssl_accepted) {
                        int ret; int ssl_err=0;
                        {
                            std::lock_guard<std::mutex> ssl_lock(conn->ssl_mutex);
                            ret=SSL_accept(conn->ssl);
                            if(ret<=0) ssl_err=SSL_get_error(conn->ssl,ret);
                        }
                        if (ret==1) {
                            conn->ssl_accepted=true;
                            { std::string _m="[SubReactor] fd "+std::to_string(current_fd)+" SSL handshake OK"; LOG_INFO(_m); }
                        } else {
                            { std::string _m="[SubReactor] fd "+std::to_string(current_fd)+" SSL_accept ret="+std::to_string(ret)+" ssl_err="+std::to_string(ssl_err); LOG_INFO(_m); }
                            if (ssl_err==SSL_ERROR_WANT_READ||ssl_err==SSL_ERROR_WANT_WRITE) {
                                conn->needs_rearm.store(true); uint64_t v=1; write(wakeup_fd,&v,sizeof(v));
                                continue;
                            }
                            epoll_ctl(sub_epoll_fd,EPOLL_CTL_DEL,current_fd,nullptr);
                            { std::lock_guard<std::mutex> lock(sub_mutex); sub_conn_map.erase(current_fd); }
                            { std::string _m="[SubReactor] fd "+std::to_string(current_fd)+" SSL handshake failed"; LOG_INFO(_m); }
                            continue;
                        }
                    }

                    { std::lock_guard<std::mutex> lock(sub_mutex); timer_manager.add_timer(conn,60); }

                    { std::string _m="[SubReactor] fd "+std::to_string(current_fd)+" entering SSL_read loop"; LOG_INFO(_m); }
                    bool is_closed=false; int ssl_read_errno=0;
                    while (true) {
                        memset(buffer,0,sizeof(buffer));
                        int bytes_read; int ssl_err=0;
                        {
                            std::lock_guard<std::mutex> ssl_lock(conn->ssl_mutex);
                            bytes_read=SSL_read(conn->ssl,buffer,sizeof(buffer)-1);
                            if(bytes_read<=0) ssl_err=SSL_get_error(conn->ssl,bytes_read);
                        }
                        if (bytes_read>0) {
                            std::lock_guard<std::mutex> lock(conn->buffer_mutex);
                            conn->read_buffer.append(buffer,bytes_read);
                            { std::string _m="[SubReactor] fd "+std::to_string(current_fd)+" SSL_read got "+std::to_string(bytes_read)+" bytes"; LOG_INFO(_m); }
                        } else {
                            ssl_read_errno=errno;
                            { std::string _m="[SubReactor] fd "+std::to_string(current_fd)+" SSL_read ret="+std::to_string(bytes_read)+" ssl_err="+std::to_string(ssl_err)+" errno="+std::to_string(ssl_read_errno); LOG_INFO(_m); }
                            if (ssl_err==SSL_ERROR_WANT_READ||ssl_err==SSL_ERROR_WANT_WRITE) break;
                            if (ssl_err==SSL_ERROR_ZERO_RETURN) { is_closed=true; }
                            else { is_closed=true; }
                            break;
                        }
                    }

                    if (is_closed) {
                        { std::string _m="[SubReactor] fd "+std::to_string(current_fd)+" is_closed=true"; LOG_INFO(_m); }
                        epoll_ctl(sub_epoll_fd,EPOLL_CTL_DEL,current_fd,nullptr);
                        { std::lock_guard<std::mutex> lock(sub_mutex); sub_conn_map.erase(current_fd); }
                    } else {
                        { std::string _m="[SubReactor] fd "+std::to_string(current_fd)+" is_closed=false enqueuing"; LOG_INFO(_m); }
                        if (g_pool) {
                            g_pool->enqueue([this, conn]() {
                                process_business(conn);
                                conn->is_closing.store(false);
                                conn->needs_rearm.store(true);
                                uint64_t v=1; write(wakeup_fd,&v,sizeof(v));
                            });
                        }
                    }
                    continue;
                }

                if (events[i].events & (EPOLLERR | EPOLLHUP)) {
                    epoll_ctl(sub_epoll_fd,EPOLL_CTL_DEL,current_fd,nullptr);
                    std::lock_guard<std::mutex> lock(sub_mutex); sub_conn_map.erase(current_fd);
                }
            }
            timer_manager.handle_expired_timers(sub_conn_map, sub_epoll_fd);
        }
    }
};

#endif
