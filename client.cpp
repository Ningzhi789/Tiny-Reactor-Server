#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>      //包含inet_pton()函数
#include <netinet/in.h>
#include <unistd.h>
#include <vector>
#include <openssl/ssl.h>
#include <openssl/err.h>

// 注意：客户端不使用服务端的 Logger（未调用 init() 会导致 segfault）


int main() {
    // OpenSSL 1.1.0+ 自动初始化，无需手动调用 SSL_library_init()
    SSL_CTX* client_ctx = SSL_CTX_new(TLS_client_method());
    if (!client_ctx) {
        std::cerr << "SSL_CTX_new 失败！OpenSSL 未正确初始化。" << std::endl;
        ERR_print_errors_fp(stderr);
        return -1;
    }
    //  1.socket
    int client_fd=socket(AF_INET,SOCK_STREAM,0);
    if (client_fd==-1) {
        std::cerr<<"socket failed"<<"\n";
        SSL_CTX_free(client_ctx);
        return -1;
    }

    // 2.addr and port
    sockaddr_in server_addr{};
    server_addr.sin_family=AF_INET;
    server_addr.sin_port=htons(8088);

    if (inet_pton(AF_INET,"127.0.0.1",&server_addr.sin_addr)<=0) {
        std::cerr<<"address error"<<"\n";
        close(client_fd);
        SSL_CTX_free(client_ctx);
        return -1;
    }

    // 3. connect
    if (connect(client_fd,(struct sockaddr*)&server_addr,sizeof(server_addr))<0) {
        std::cerr<<"connect failed"<<"\n";
        close(client_fd);
        SSL_CTX_free(client_ctx);
        return -1;
    }
    std::cout<<"连接成功"<<"\n";
    // TLS安全握手
    SSL* ssl=SSL_new(client_ctx);
    if (!ssl) {
        std::cerr << "SSL_new 失败！" << std::endl;
        ERR_print_errors_fp(stderr);
        close(client_fd);
        SSL_CTX_free(client_ctx);
        return -1;
    }
    SSL_set_fd(ssl,client_fd);
    if (SSL_connect(ssl) <= 0) {
        std::cerr << "SSL 握手失败！" << std::endl;
        ERR_print_errors_fp(stderr);
        SSL_free(ssl);
        close(client_fd);
        SSL_CTX_free(client_ctx);
        return -1;
    }
    std::cout << "TLS 安全加密隧道建立成功！" << std::endl;
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

        SSL_write(ssl,http_request.c_str(),http_request.length());

        memset(buffer,0,sizeof(buffer));
        ssize_t bytes_read = SSL_read(ssl, buffer, sizeof(buffer) - 1);
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
    SSL_shutdown(ssl);
    SSL_free(ssl);
    close(client_fd);
    SSL_CTX_free(client_ctx);
    std::cout<<"close"<<"\n";

    return 0;
}
