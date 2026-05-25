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
    server_addr.sin_port=htons(8080);

    if (inet_pton(AF_INET,"127.0.0.1",&server_addr.sin_addr)<=0) {
        std::cerr<<"address error"<<"\n";
        return -1;
    }

    // 3. connect
    if (connect(client_fd,(struct sockaddr*)&server_addr,sizeof(server_addr))<0) {
        std::cout<<"connect failed"<<"\n";
        close(client_fd);
        return -1;
    }

    // 4. send
    const char* msg="hello world";
    send(client_fd,msg,strlen(msg),0);
    std::cout<<"sent"<<"\n";

    // 5.receive
    char buffer[1024]={0};
    ssize_t bytes_read=read(client_fd,buffer,sizeof(buffer)-1);
    if (bytes_read>0)
        std::cout<<buffer<<"\n";

    // 6.close
    close(client_fd);
    std::cout<<"close"<<"\n";

    return 0;
}