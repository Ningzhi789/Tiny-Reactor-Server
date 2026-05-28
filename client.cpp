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

        // 1. 【新增数据预处理】：空格转义。把句子里的普通空格 ' ' 换成安全传输符号 "%20"
        std::string encoded_input="";
        for (char c: input) {
            if (c==' ')
                encoded_input+="%20";
            else
                encoded_input+=c;
        }
        // 2. 【核心修改点】：不再打包二进制长度，而是将消息包装成纯文本的标准 HTTP GET 请求报文
        // 注意：每一行都要以 \r\n 结尾，最后必须多加一个完整的 \r\n（空行）代表 HTTP 头部结束
        std::string http_request =
            "GET /chat?msg=" + encoded_input + " HTTP/1.1\r\n" +
            "Host: 127.0.0.1:8088\r\n" +
            "User-Agent: TerminalChatClient\r\n" +
            "Connection: keep-alive\r\n" +
            "\r\n";

        send(client_fd,http_request.c_str(),http_request.length(),0);

        memset(buffer,0,sizeof(buffer));
        ssize_t bytes_read=read(client_fd,buffer,sizeof(buffer)-1);
        if (bytes_read>0) {
            std::string response_str(buffer,bytes_read);
            // 4. 【核心协议过滤】：手撕解包，在客户端抹去死板的 HTTP 协议报头
            // 寻找连续的两个换行符 "\r\n\r\n"（代表 HTTP 头部结束，真正干净的 Body 聊天数据开始）
            size_t body_pos=response_str.find("\r\n");
            if (body_pos!=std::string::npos) {
                std::cout<<response_str.substr(body_pos+4)<<"\n\n";
            }else {
                std::cout << "收到未知格式响应：" << response_str << "\n\n";
            }
        }else {
            std::cout << "服务器已断开连接" << "\n";
            break;
        }

    }


    // 6.close
    close(client_fd);
    std::cout<<"close"<<"\n";

    return 0;
}