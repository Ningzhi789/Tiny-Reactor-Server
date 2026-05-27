#ifndef BUFFER_HPP
#define BUFFER_HPP

#include <vector>
#include <string>
#include <algorithm>
#include <cstring>

class Buffer {
public:
    Buffer():
        read_index{0},
        write_index{0}{}

    // 返回有多少字节的数据可以读
    size_t readable_bytes() const {
        return write_index-read_index;
    }

    // 返回缓冲区还有多少剩余空间可以写
    size_t writable_bytes() const {
        return buffer.size()-write_index;
    }

    // 向缓冲区追加数据
    void append(const char* data,size_t len) {
        if (writable_bytes()<len)
            buffer.resize(write_index+len);     //扩容长度
        std::memcpy(&buffer[write_index],data,len);
        write_index+=len;
    }

    // 查看当前的读指针位置
    const char* peek() const {
        return &buffer[read_index];
    }

    // 推进读指针（代表已经消费了 len 字节的数据）
    void retrieve(size_t len) {
        read_index+=len;
        if (read_index==write_index) {
            // 如果读写指针重合，说明数据清空了，直接复位，省去挪动内存的开销
            read_index = 0;
            write_index = 0;
        }
    }

    // 将可读数据打包成字符串拿走
    std::string retrieve_all_as_string() {
        std::string str(peek(),readable_bytes());
        retrieve(readable_bytes());
        return str;
    }

private:
    std::vector<char> buffer;
    size_t read_index;
    size_t write_index;
};

#endif
