#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>      //包含inet_pton()函数
#include <netinet/in.h>
#include <unistd.h>

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
        send(client_fd,input.c_str(),input.length(),0);

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