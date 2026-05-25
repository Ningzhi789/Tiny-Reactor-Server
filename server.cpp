#include <iostream>
#include <cstring>
#include <sys/socket.h>     //socket核心头文件
#include <netinet/in.h>     //包含socketaddr_in结构体
#include <unistd.h>         //包含close函数
#include <thread>           //多线程核心头文件

void handle_client(int client_fd) {
    std::cout << "【子线程 " << std::this_thread::get_id() << "】接管连接 fd: " << client_fd << std::endl;

    char buffer[1024];

    //循环让客户端持续聊天，直到主动断开
    while (true) {
        memset(buffer,0,sizeof(buffer));        //清空缓冲区

        //阻塞读取客户端数据
        ssize_t bytes_read=read(client_fd,buffer,sizeof(buffer)-1);
        if (bytes_read>0) {
            std::cout<<"receive message from "<<client_fd<<" "<<buffer<<"\n";
            std::string response="received: " + std::string(buffer);
            send(client_fd,response.c_str(),response.length(),0);
        }
        else if (bytes_read==0) {
            std::cout<<client_fd<<"断开连接"<<"\n";
            break;
        }
        else {
            std::cerr<<client_fd<<"发生错误"<<"\n";
            break;
        }
    }
    close(client_fd);
    std::cout<<"finish"<<"\n";
}

int main() {
    // 1.socket
    int server_fd=socket(AF_INET,SOCK_STREAM,0);
    if (server_fd==-1) {
        std::cout<<"failed created socket_fd"<<"\n";
        return -1;
    }

    //设置端口复用
    int opt=1;
    setsockopt(server_fd,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));


    // 2.addr and port
    sockaddr_in server_addr{};
    server_addr.sin_family=AF_INET;     //ipv4
    server_addr.sin_addr.s_addr=INADDR_ANY;     //监听所有可用网络接口
    server_addr.sin_port=htons(8088);

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

    while (true) {
        // 5.accept
        sockaddr_in client_addr{};
        socklen_t client_addr_len=sizeof(client_addr);
        int client_fd=accept(server_fd,(struct sockaddr*)&client_addr,&client_addr_len);
        if (client_fd<0) {
            std::cerr<<"accept failed"<<"\n";
            continue;
        }
        std::cout<<"connect "<<client_fd<<"\n";

        // 核心：为当前客户端创建子线程
        // 将 handle_client 函数和参数 client_fd 传给 std::thread
        std::thread t(handle_client, client_fd);

        // 为什么要用 detach() 而不是 join()？
        // join() 会让主线程卡住，等待子线程运行结束才继续，这就退化回单线程了。
        // detach() 是将子线程与主线程“分离”，子线程在后台独立运行，生命周期由系统接管，
        // 这样主线程就能立刻回到循环开头，去 accept 下一个连接。
        t.detach();
    }

    // 8.close
    close(server_fd);
    std::cout<<"close"<<"\n";
    return 0;
}