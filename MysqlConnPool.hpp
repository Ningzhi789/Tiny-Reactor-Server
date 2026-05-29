#ifndef MYSQL_CONN_POOL_HPP
#define MYSQL_CONN_POOL_HPP

#include <mysql/mysql.h>
#include <string>
#include <queue>
#include <mutex>
#include <semaphore.h>
#include <memory>
#include "Logger.hpp"

class MysqlConnPool {
public:
    // 单例模式：全局独一份的连接池中枢
    static MysqlConnPool& getInstance() {
        static MysqlConnPool instance;
        return instance;
    }

    // 初始化池子
    bool init(std::string host,std::string user,std::string pass,std::string db_name,int port,int max_conn) {
        host_=host;
        user_=user;
        pass_=pass;
        db_name_=db_name;
        port_=port;

        for (int i=0;i<max_conn;i++) {
            MYSQL* conn=mysql_init(nullptr);
            if (!conn) {
                LOG_ERROR("MySQL 实例初始化失败！");
                return false;
            }
            // 自动重连机制：防止数据库连接因超时被服务器单方面断开（MySQL 默认8小时无交互断开）
            long reconnect=1;
            mysql_options(conn,MYSQL_OPT_RECONNECT,&reconnect);

            conn = mysql_real_connect(conn,host_.c_str(),user_.c_str(),pass_.c_str(),db_name_.c_str(),port_,nullptr,0);
            if (!conn) {
                LOG_ERROR("MySQL 连接物理握手失败！驱动错误原因: " + std::string(mysql_error(conn)));
                return false;
            }
            conn_queue_.push(conn);
            free_conn_++;
        }
        // 初始化内核信号量：可用资源数等于成功创建的空闲连接数
        sem_init(&sem_,0,free_conn_);
        max_conn_=free_conn_;

        LOG_INFO("========== MySQL 连接池初始化成功，当前空闲连接数: " + std::to_string(free_conn_) + " ==========");
        return true;
    }

    // 借出连接（若无空闲，线程会被信号量安全挂起阻塞）
    MYSQL* getConnection() {
        // 信号量 P 操作：如果资源计数为0，当前工作线程会在此无损挂起，绝对不空转占用 CPU
        sem_wait(&sem_);

        std::lock_guard<std::mutex> lock(mutex_);
        if (conn_queue_.empty())
            return nullptr;

        MYSQL* conn=conn_queue_.front();
        conn_queue_.pop();

        free_conn_--;
        cur_conn_++;
        return conn;
    }

    // 归还连接回池子
    void releaseConnection(MYSQL*conn) {
        if (!conn)
            return;

        std::lock_guard<std::mutex> lock(mutex_);
        conn_queue_.push(conn);

        free_conn_++;
        cur_conn_--;

        // 信号量 V 操作：可用连接数加1，自动唤醒正在死等连接的其他后台工作线程
        sem_post(&sem_);
    }

    ~MysqlConnPool() {
        std::lock_guard<std::mutex> lock(mutex_);
        while (!conn_queue_.empty()) {
            MYSQL* conn=conn_queue_.front();
            conn_queue_.pop();
            mysql_close(conn);
        }
        mysql_library_end();
        sem_destroy(&sem_);

    }

private:
    MysqlConnPool():
    free_conn_{0},
    cur_conn_{0},
    max_conn_{0}{}

    std::string host_;
    std::string user_;
    std::string pass_;
    std::string db_name_;
    int port_;

    std::queue<MYSQL*> conn_queue_;     // 存放物理连接的先进先出队列
    std::mutex mutex_;                  // 保护队列并发弹出的互斥锁
    sem_t sem_;                         // 资源同步多线程信号量

    int free_conn_;     // 空闲连接数
    int cur_conn_;      // 当前已经被借出的连接数
    int max_conn_;      // 最大连接数
};

// 👑 RAII 守卫类：利用局部变量生存期管理连接分配回收
class ConnectionRAII {
public:
    // 构造时自动借出
    ConnectionRAII(MYSQL**con) {
        *con=MysqlConnPool::getInstance().getConnection();
        con_raii_=*con;
    }

    ~ConnectionRAII() {
        MysqlConnPool::getInstance().releaseConnection(con_raii_);
    }
private:
    MYSQL* con_raii_;
};

#endif
