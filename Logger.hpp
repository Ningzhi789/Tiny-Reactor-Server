#ifndef LOGGER_HPP
#define LOGGER_HPP

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <chrono>
#include <cstring>

class Logger {
public:
    static const int KLargeBuffer=4000 *1024;       //4MB标准工业缓冲区大小

    //内部固定大小缓冲区类
    class FixedBuffer {
    public:
        FixedBuffer():
        data_{new char[KLargeBuffer]},
        cur_{data_}{}
        ~FixedBuffer(){delete[] data_;}

        void append(const char* buf,size_t len) {
            if (static_cast<size_t>(avail())>len) {
                std::memcpy(cur_,buf,len);
                cur_+=len;
            }
        }
        const char* data() const{return data_;}
        int length() const{return static_cast<int>(cur_-data_);}
        int avail() const{return KLargeBuffer-static_cast<int>(cur_-data_);}
        void reset(){cur_=data_;}

    private:
        char* data_;
        char* cur_;;
    };

    // 单例模式
    static Logger& getInstance() {
        static Logger instance;
        return instance;
    }

    // 初始化日志文件并拉起后台线程
    void init(const std::string& log_file_name) {
        log_file_name_ = log_file_name;
        file_.open(log_file_name_,std::ios::out|std::ios::app);
        running_=true;
        currentBuffer_=std::make_unique<FixedBuffer>();
        nextBuffer_=std::make_unique<FixedBuffer>();
        buffers_.reserve(16);
        backendThread_=std::thread(&Logger::threadFunc,this);
    }
    ~Logger() {
        if (running_) {
            running_=false;
            cond_.notify_one();
            if (backendThread_.joinable())
                backendThread_.join();
        }
        if (file_.is_open()) {
            file_.close();
        }
    }

    // 前台极速投递接口：只写内存
    void append(const char*logline,int len) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (currentBuffer_->avail()>len) {
            currentBuffer_->append(logline,len);
        }else {
            buffers_.push_back(std::move(currentBuffer_));
            if (nextBuffer_) {
                currentBuffer_=std::move(nextBuffer_);      // 启用备用缓冲区
            }else {
                currentBuffer_=std::make_unique<FixedBuffer>();     // 极端情况下动态分发
            }
            currentBuffer_->append(logline,len);
            cond_.notify_one();     // 快速唤醒后台写盘线程
        }
    }

    // 格式化日志
    void log(const std::string& level,const std::string& msg) {
        auto now=std::chrono::system_clock::now();
        auto time_t_now=std::chrono::system_clock::to_time_t(now);
        char time_str[32];
        std::strftime(time_str,sizeof(time_str),"%Y-%m-%d %H:%M:%S",std::localtime(&time_t_now));

        // 统一日志线上格式：[时间][级别][线程ID] 内容
        std::string line = "[" + std::string(time_str) + "][" + level + "]["
                           + std::to_string(reinterpret_cast<uint64_t>(pthread_self())) + "] " + msg + "\n";
        append(line.c_str(), line.length());
    }



private:
    Logger():running_(false){}

    // 后台专门负责磁盘 I/O 的写盘线程函数
    void threadFunc() {
        // 后台准备两块干净的专属缓冲区，用于和前台对调
        std::unique_ptr<FixedBuffer> backendBuffer1=std::make_unique<FixedBuffer>();
        std::unique_ptr<FixedBuffer> backendBuffer2=std::make_unique<FixedBuffer>();
        std::vector<std::unique_ptr<FixedBuffer>> buffersToWrite;
        buffersToWrite.reserve(16);
        while (running_) {
            {
                std::unique_lock<std::mutex> lock(mutex_);
                if (buffers_.empty()) {
                    // 每隔 1 秒或者被唤醒时进行盘点
                    cond_.wait_for(lock, std::chrono::seconds(1));
                }
                // 将前台未满的当前缓冲区强行收网，放入队列
                buffers_.push_back(std::move(currentBuffer_));
                // ⚡【核心魔术：双缓冲指针置换】
                currentBuffer_=std::move(backendBuffer1);       // 把干净的缓冲区递向前台
                if (!nextBuffer_) {
                    nextBuffer_=std::move(backendBuffer2);
                }
                buffersToWrite.swap(buffers_);      // 把攒满的队列瞬间偷到后台缓冲区
            }// 🔓 锁在这里极速释放！前台线程可以无干扰地继续疯狂写日志

            if (buffersToWrite.empty()) continue;

            // 在锁外慢悠悠地执行真正的长耗时磁盘写盘
            for (const auto& buffer: buffersToWrite) {
                if (file_.is_open()) {
                    file_.write(buffer->data(),buffer->length());
                }
            }
            if (file_.is_open())
                file_.flush();

            // 重新回收物理内存，留作下一次置换，避免频繁 new/delete 造成堆碎片
            if (!backendBuffer1) {
                backendBuffer1=std::move(buffersToWrite.back());
                buffersToWrite.pop_back();
                backendBuffer1->reset();

            }
            if (!backendBuffer2) {
                backendBuffer2=std::move(buffersToWrite.back());
                buffersToWrite.pop_back();
                backendBuffer2->reset();
            }
            buffersToWrite.clear();
        }
        // 临死清仓：服务器关闭前强制把残余日志全部刷入磁盘
        if (currentBuffer_&&currentBuffer_->length()>0) {
            file_.write(currentBuffer_->data(),currentBuffer_->length());
        }
        for (const auto& buffer: buffers_) {
            file_.write(buffer->data(),buffer->length());
        }
        file_.flush();
    }

    std::string log_file_name_;
    std::ofstream file_;
    bool running_;
    std::mutex mutex_;
    std::condition_variable cond_;
    std::thread backendThread_;
    std::unique_ptr<FixedBuffer> currentBuffer_;
    std::unique_ptr<FixedBuffer> nextBuffer_;
    std::vector<std::unique_ptr<FixedBuffer>>  buffers_;
};

// 宏定义：方便全站一行调用
#define LOG_INFO(msg) Logger::getInstance().log("INFO", msg)
#define LOG_ERROR(msg) Logger::getInstance().log("ERROR", msg)

#endif