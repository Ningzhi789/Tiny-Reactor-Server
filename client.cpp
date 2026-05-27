#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>      //包含inet_pton()函数
#include <netinet/in.h>
#include <unistd.h>
#include <vector>


int main() {
    //  1.socket
    int client_fd=socket(AF_INET,SOCK_STREAM,0);
    if (client_fd==-1) {
        std::cout<<"socket failed"<<"\n";
        return -1;
    }

    // 2.addr and port
    sockaddr_in server_addr{};
    server_addr.sin_family=AF_INET;
    server_addr.sin_port=htons(8088);

    if (inet_pton(AF_INET,"127.0.0.1",&server_addr.sin_addr)<=0) {
        std::cerr<<"address error"<<"\n";
        close(client_fd);
        return -1;
    }

    // 3. connect
    if (connect(client_fd,(struct sockaddr*)&server_addr,sizeof(server_addr))<0) {
        std::cout<<"connect failed"<<"\n";
        close(client_fd);
        return -1;
    }
    std::cout<<"连接成功"<<"\n";

    // 4. send
    std::string input;
    char buffer[1024];
    while (true) {
        std::cout<<"输入："<<"\n";
        std::getline(std::cin,input);
        if (input=="exit")
            break;
        if (input.empty())
            continue;

        // 🔥 【V7 核心修改点】：打包流式协议报文（4字节 Header + Body）
        uint32_t body_len=input.length();      // 1. 获取业务身体数据的绝对长度
        uint32_t net_len=htonl(body_len);      // 2. 将本地字节序的整数转换为标准网络字节序（大端）

        // 3. 申请一个连续的动态缓冲区，大小刚好等于：4 字节头部 + 身体长度
        std::vector<char> send_buf(4+body_len);

        // 4. 精准拷贝：前 4 字节塞进长度标签，后面紧跟真正的文本内容
        std::memcpy(send_buf.data(),&net_len,4);
        std::memcpy(send_buf.data()+4,input.c_str(),body_len);
        // 5. 将这块打包好的完整内存一次性安全发送出去
        send(client_fd,send_buf.data(),send_buf.size(),0);

        memset(buffer,0,sizeof(buffer));
        ssize_t bytes_read=read(client_fd,buffer,sizeof(buffer)-1);
        if (bytes_read>0)
            std::cout<<buffer<<"\n";
        else {
            std::cout<<"断开连接"<<"\n";
            break;
        }

    }


    // 6.close
    close(client_fd);
    std::cout<<"close"<<"\n";

    return 0;
}