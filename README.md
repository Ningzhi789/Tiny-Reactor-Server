# v1-v9
## v1：同步阻塞迭代服务器 (Sequential Server)
【架构设计】：

整个服务器只有一个主线程。在一个死循环里，先调用 accept() 阻塞等待新客户端；等来一个客户端后，立刻在原地调用 read() 阻塞等待它发数据，处理完业务并 send() 之后，才能回到顶部去接待下一个客户端。

【致命痛点】：

同步双重阻塞，毫无并发能力。 如果当前连接的客户端不发数据，或者由于网络延迟卡住了，整个服务器就会死死停在 read() 那一行。此时，后续所有的其他客户端都无法被 accept() 接待，只能在操作系统的 TCP 全连接队列（Backlog）里死等直到超时断开。

【如何迈向下一代】：

必须把“监听”和“干活”剥离开来，不能让一个客户端拖垮全世界 $\rightarrow$ 引入多线程。

## v2：多线程服务器 (Thread-per-Connection)
【架构设计】：

主线程只管在 while(true) 循环里负责 accept() 接待。只要新来一个客户端分配了 fd，主线程立刻调用 pthread_create() 或 std::thread 当场创建一个全新的工作线程，把这个 fd 丢给子线程去原地进行阻塞的 read/write 业务处理。

【致命痛点】：

遭遇 C10K 资源耗尽瓶颈。 线程在 Linux 下是一个轻量级进程（LWP），创建和销毁都属于高昂的内核系统调用。高并发场景下，频繁创建线程会榨干系统资源；其次，Linux 默认每个线程栈要占用 8MB 内存，几千个连接就会直接触发 OOM（内存溢出） 崩溃；最致命的是，上千个线程在单核或少核 CPU 上疯狂切换，线程上下文切换（Context Switch）的开销会直接让 CPU 满载占满，服务器陷入死锁般的卡顿。

【如何迈向下一代】：

必须让极少数的线程管极多数的套接字 $\rightarrow$ 步入事件驱动（Event-Driven）和 I/O 多路复用。

## v3：单线程 Reactor 模式 (select/poll)
【架构设计】：

第一次引入“事件侦听”机制。整个服务器重新回归单线程，但套接字被托管给了 select 或 poll 监听器。主线程死循环调用 select() 阻塞，只要内核中有一堆 fd 中的任意一个有了读写动静，select 就会醒来，主线程再去挨个处理就绪的 fd。

【致命痛点】：

轮询效率低下与物理坑位限制。 select 受限于内核宏定义，单进程最大只能监听 1024 个 fd（FD_SETSIZE限制）；虽然 poll 换用了链表数组解除了 1024 限制，但它们俩底层都采用了极其低效的 $O(N)$ 轮询机制。每次事件触发，服务器都不知道具体是哪个 fd 响应了，必须用一个 for 循环把成千上万个托管的 fd 全部扫描一遍。此外，每次调用都需要把整个 fd 集合在用户态和内核态之间来回复制，并发量一上来，CPU 都在做无意义的拷贝和空转。

【如何迈向下一代】：

必须让操作系统内核在套接字就绪时，主动把具体的 fd 递过来，而不是让用户态去猜 $\rightarrow$ 换用 Linux 专属的 Epoll。
<img width="2816" height="1536" alt="v3_epoll_socket_server" src="https://github.com/user-attachments/assets/31a42efa-eb89-4e9a-88c9-9160d103512f" />

## v4：高性能非阻塞底座 (Epoll ET + Non-blocking)
【架构设计】：

换用 Linux 性能大杀器 epoll。底层基于红黑树（高效增删改 fd）与双向就绪链表（只存放真正有动静的 fd），实现 $O(1)$ 的惊人就绪通知。为了将吞吐量榨干，开启了边缘触发（Edge Triggered, ET）模式。

【致命痛点】：

单线程 Reactor 被耗时业务瞬间卡死。 此时整个 Reactor 依然只有主线程。ET 模式要求主线程必须用 while(true) 循环调用 read() 一直读到 EAGAIN 报错。如果读出来的业务请求需要进行 2 秒钟的数据库查询、密码加解密或者 sleep 模拟，那么整个主线程的 epoll_wait 就会被死死卡住 2 秒。这 2 秒内，即便网卡收到了再多的新客户端包，服务器也毫无反应，单核吞吐量在长耗时业务面前瞬间崩塌。

【如何迈向下一代】：

主线程是尊贵的“接待员”，绝不能去干拧螺丝的体力活 $\rightarrow$ 引入多线程线程池，实现 I/O 与计算分离。
<img width="2816" height="1536" alt="v4_线程池" src="https://github.com/user-attachments/assets/57729b7f-6da8-4ad6-ba64-50643027c78e" />
<img width="2816" height="1536" alt="v4_整体架构" src="https://github.com/user-attachments/assets/87f0aca9-774b-431c-bf50-2a766baa0fdd" />


## v5：单 Reactor + ThreadPool 线程池 (I/O 与计算解耦)

【架构设计】：
初始化一个固定线程数（如 4 线程）的中央线程池。主线程极其纯粹，只在 epoll_wait 醒来后，利用 ET 模式疯狂 read 卸货。一旦把数据读成了一个原始字符串，立刻用 pool.enqueue 打包成一个 Lambda 任务扔进并发队列。主线程拍拍屁股继续回去盯着 Epoll，而多工作线程在后台并发从队列里抢任务去执行耗时的业务逻辑。

【致命痛点】：

生命周期撕裂，遭遇非安全内存 Core Dump。 这是多线程异步网络库里最恐怖的竞态 Bug。当后台工作线程正在优哉游哉地处理那 2 秒的业务时，对端客户端由于网络波动或者等不及，突然断开了。前台主线程被 Epoll 唤醒，顺手把全局 Map 里的物理连接删了，并执行了 close(fd) 销毁了内存。等后台线程算完回过神来，准备拿那个 fd 指针发回执时，指针指向的内存已经变成废墟，服务器瞬间发生 Segmentation Fault（段错误）崩溃挂掉。

【如何迈向下一代】：

后台线程不死，连接对象的生命就必须强行吊着，谁说了都不算 $\rightarrow$ 引入现代 C++ 智能指针接管资产。
<img width="2816" height="1536" alt="v5_ET模式" src="https://github.com/user-attachments/assets/6b754ab9-9483-463c-8dd2-625884fa1259" />
<img width="2816" height="1536" alt="v5_水平触发和边缘触发" src="https://github.com/user-attachments/assets/88b18ab3-2a14-47bb-b39e-eddfc8a13947" />



## v6：智能指针生命周期闭环 (现代 C++ RAII)

【架构设计】：

彻底废除裸指针，全局使用 std::unordered_map<int, std::shared_ptr<Connection>>。最精妙的改动发生在向线程池投递任务时：pool.enqueue([conn]() { ... })。利用 Lambda 表达式的按值捕获（By Value），使得 Connection 对象的强引用计数瞬间加 1。

【致命痛点】：

遭遇 TCP 粘包与拆包，数据内容发生恶性重叠或截断。 跨过了内存安全的坎，却撞上了网络传输的硬墙。TCP 是面向字节流的，就像自来水，没有任何消息边界。客户端高频发两个 "hello"，服务器可能一次性吐出 "hellohello"（粘包）；或者因为网卡分片，一次只读到 "he"，剩下的 "llo" 下次才来（拆包）。服务器此时如果盲目地把 Buffer 里的内容当作独立请求去解析，业务逻辑彻底错乱。

【如何迈向下一代】：

必须为每个连接配一个“原材料仓库”，并制定彼此对齐的“切割暗号” $\rightarrow$ 自研应用层 Buffer 与自定义长度协议。
<img width="2752" height="1536" alt="v6_智能指针生命周期安全闭环" src="https://github.com/user-attachments/assets/941fe38a-b755-4631-bf20-5a0fc8bc7f37" />


## v7：动态应用层 Buffer + 4字节定长私有协议

【架构设计】：

为 Connection 补充了基于 std::vector<char> 的流式缓冲区，并自研了 peek/retrieve 读写指针。制定了私有工业协议：4字节 Header（存储网络字节序 htonl 的 Body 长度）+ 后续的 Body 文本。主线程只管 append 进 Buffer，工作线程加锁保护 buffer_mutex，利用 memcpy 窥探前 4 字节，数据够一个整包才切下来拿走，数据不够（拆包）就 break 退出等下次，切完包执行 retrieve 复位内存。

【致命痛点】：

僵尸连接恶意耗尽描述符攻击 (DoS)。 服务器虽然能完美切包了，但是如果有些恶意客户端连上服务器后，死活不发任何一个字节，或者发了 4 个字节后就永远装死。此时它们既不触发读写事件，也不下线。因为 Linux 单进程默认的文件描述符上限非常低，坑位一旦被这些空闲的“僵尸连接”占满，服务器就会报出 EMFILE 错误（Too many open files），再也无法接待任何新客人。

【如何迈向下一代】：

必须在内部布下一柄不说话就无情抹杀的终极斩杀剑 $\rightarrow$ 引入基于最小堆的惰性删除定时器。
<img width="2752" height="1536" alt="v7_buffer解决tcp粘包" src="https://github.com/user-attachments/assets/341c9f69-88c4-4d7f-a5dc-c71239ac4d2c" />


## v8：最小堆 (Min-Heap) 定时器机制 (主动资源防御)

【架构设计】：

利用 std::priority_queue 组装小顶堆时钟中心。堆顶永远保留全站最早即将过期的连接节点。主线程的 epoll_wait 不再死等，而是设置 1000 毫秒超时。每隔 1 秒主线程自动醒来，雷打不动地执行一次堆顶盘点。

【修复的致命 Bug】：

在开发惰性删除时，如果写成恒真判据 if (top.expire_time <= now)，会导致一直在积极聊天的活跃客户端因为老堆节点的到期而遭到服务器“误杀”强踢。你通过将其重构为真正的惰性判据：if (conn->expire_time == top.expire_time) 成功化解了这一危机。 只有最新真实时间与堆顶老时间完全吻合，才坐实其一直在装死，当场通过 conn_map.erase 联动智能指针 RAII 触发析构，安全 close 并踢人。

【致命痛点】：

协议封闭，无法融入全球 Web 互联网生态。 我们的服务器目前只听得懂“前 4 字节是整型”的私有二进制暗号，这意味着我们必须搭配专属的 client.cpp 才能运行。如果用全球通用的 Chrome 浏览器 去输入 IP 端口访问，浏览器发送的是纯文本的 HTTP 报文，我们的服务器会误将前面的英文字母解析成天文数字长度，直接判定遭遇恶意包攻击而断开，无法化身为真正的 Web 服务器。

【如何迈向下一代】：

将切割蛋糕的工序从“量尺寸数格子（二进制4字节）”，升级为“理解文本语法（HTTP协议）” $\rightarrow$ 引入有限状态机解析 HTTP。
<img width="2816" height="1536" alt="v8_min_heap" src="https://github.com/user-attachments/assets/b1b798d6-4eb3-4eb9-8beb-5f9be119f2e4" />


## v9：有限状态机 (FSM) 纯手撕 HTTP/1.1 + 动态路由分流

【架构设计】：

完全拥抱互联网标杆。自研了主从嵌套的高效有限状态机（FSM），利用 \r\n 作为流式边界，零拷贝推进读指针，实现 PARSE_REQUESTLINE、PARSE_HEADERS 的高效状态跳转。

【修复的致命 Bug】：

链接期静态符号未定义错误：通过将类内 static constexpr char CRLF[2] 优化为函数内原生字面量指针 "\r\n"，优雅通过了跨 C++ 标准的版本链接。

长连接断流 Bug：在 process_business 业务层重新套回外层 while(true) 连环解包流，配合包尾的 conn->http_parser.reset() 满血复位，成功支持了 HTTP/1.1 Pipeline 长连接长效通信，遭遇流式拆包时能安全 break 挂起。

多路动态路由分流：锁内快速切包分流，锁外长耗时执行。根据 URL 路径，实现 /chat?msg=xxx（聊天流：自动执行 URL Decode 空格转义并剥离 HTTP 头纯文本回传）与常规路径（网页流：渲染精美 HTML 网页）的完美动态分流，双端互不干扰。

纵深防御水位线：同步布下了“主线程 64KB Buffer 积压上限拉闸”与“状态机类外全局 8KB Max Header Size 洪水攻击防御限制”，以及“Content-Length 越界越界安全审查”，杜绝了黑客意图通过无穷文本流把服务器内存撑爆（OOM DoS）的路径。
<img width="1376" height="768" alt="v9_FSM" src="https://github.com/user-attachments/assets/efb7f300-247e-4405-87ca-410b5f3ca337" />

