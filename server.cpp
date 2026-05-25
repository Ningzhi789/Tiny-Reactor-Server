#include <iostream>
#include <cstring>
#include <sys/socket.h>     //socket核心头文件
#include <netinet/in.h>     //包含socketaddr_in结构体
#include <unistd.h>         //包含close函数

int main() {
    // 1.socket
    int server_fd=socket(AF_INET,SOCK_STREAM,0);
    if (server_fd==-1) {
        std::cout<<"failed created socket_fd"<<"\n";
        return -1;
    }

    // 2.addr and port
    sockaddr_in server_addr{};
    server_addr.sin_family=AF_INET;     //ipv4
    server_addr.sin_addr.s_addr=INADDR_ANY;     //监听所有可用网络接口
    server_addr.sin_port=htons(8080);

    // 3.bind
    if (bind(server_fd,(struct sockaddr*)&server_addr,sizeof(server_addr))<0) {
        std::cerr<<"bind failed"<<"\n";
        close(server_fd);
        return -1;
    }

    // 4.listen
    if (listen(server_fd,5)<0) {
        std::cerr<<"listen failed"<<"\n";
        close(server_fd);
        return -1;
    }
    std::cout<<"listening"<<"\n";

    // 5.accept
    sockaddr_in client_addr{};
    socklen_t client_addr_len=sizeof(client_addr);
    int client_fd=accept(server_fd,(struct sockaddr*)&client_addr,&client_addr_len);
    if (client_fd<0) {
        std::cerr<<"accept failed"<<"\n";
        close(server_fd);
        return -1;
    }
    std::cout<<"connect"<<"\n";

    // 6.receive
    char buffer[1024]={0};
    ssize_t bytes_read=read(client_fd,buffer,sizeof(buffer)-1);
    if (bytes_read>0)
        std::cout<<buffer<<"\n";

    // 7.send
    const char* response="received";
    send(client_fd,response,strlen(response),0);
    std::cout<<"send received ack";

    // 8.close
    close(server_fd);
    close(client_fd);
    std::cout<<"close"<<"\n";

    return 0;
}