#ifndef THREAD_POOL_HPP
#define THREAD_POOL_HPP

#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <iostream>

class ThreadPool {
public:
    explicit ThreadPool(size_t threads) :stop(false) {
        for (size_t i =0;i < threads; i++) {
            workers.emplace_back([this]() {
                while (true) {
                std::function<void()> task;
                {
                    // 独占锁，用来安全地从任务队列取任务
                    std::unique_lock<std::mutex> lock(this->queue_mutex);

                    // 等待条件变量：直到线程池停止，或者队列里有新任务
                    this->condition.wait(lock,[this]() {
                        return this->stop || !this->tasks.empty();
                    });

                    // 如果线程池停止且队列空了，线程退出
                    if (this->stop && this->tasks.empty()) return;

                    // 抢到任务，出队
                    task = std::move(this->tasks.front());
                    this->tasks.pop();
                }
                // 执行任务
                task();
                }
            });
        }
    }

    //投递任务的函数
    void enqueue(std::function<void()> task) {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            if (stop) throw std::runtime_error("enqueue on stopped ThreadPool");
            tasks.push(task);
        }
        condition.notify_one();     // 通知一个线程有任务了
    }

    // 析构函数
    ~ThreadPool() {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            stop=true;
        }
        condition.notify_all();     //唤醒所有线程
        for (std::thread &worker:workers) {
            if (worker.joinable()) worker.join();       //等待所有线程执行完当前任务
        }
    }

private:
    std::vector<std::thread> workers;       // 线程池
    std::queue<std::function<void()>> tasks; // 任务队列

    std::mutex queue_mutex;                  // 队列互斥锁
    std::condition_variable condition;       // 条件变量
    bool stop;                               // 停止标志
};

#endif
