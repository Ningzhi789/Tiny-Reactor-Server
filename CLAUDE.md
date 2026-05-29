# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run

**Prerequisites:** CMake 3.16+, C++20 compiler, libmysqlclient-dev (`mysql/mysql.h`).

```bash
# Build server and client
cd /home/qt/cpp_projects/Tiny-Reactor-Server
mkdir -p build && cd build
cmake .. && make -j$(nproc)

# Run server (requires MySQL running with chat_db configured)
./Tiny-Reactor-Server   # starts on port 8088, logs to server.log

# Run client (separate terminal)
./client                 # connects to 127.0.0.1:8088
```

**Note:** The CMake target `Tiny-Reactor-Server` builds both `server.cpp` (the reactor) and `client.cpp` (the chat client) into a single binary named `Tiny-Reactor-Server`. The actual server executable is `Tiny-Reactor-Server` (NOT `./server`). The client binary is `./client`.

**MySQL setup required for chat persistence:**
```sql
CREATE DATABASE IF NOT EXISTS chat_db;
USE chat_db;
CREATE TABLE IF NOT EXISTS chat_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    message TEXT,
    chat_time DATETIME
);
```

## Project Architecture

A **single-threaded Reactor-based HTTP server** using epoll edge-triggered I/O multiplexing, with a thread pool for business logic and a MySQL connection pool for data persistence. All I/O is non-blocking.

### Educational Evolution (v1 → v9)

This project is a **progressive architectural tutorial** — each version in the README documents a specific architectural stage and its fatal flaw, demonstrating why the next layer was needed:

| Version | Pattern | Fatal Flaw |
|---------|---------|------------|
| v1 | Sequential blocking | Zero concurrency |
| v2 | Thread-per-connection | C10K resource exhaustion |
| v3 | Single-threaded Reactor (select/poll) | O(N) polling, 1024-fd limit |
| v4 | Epoll ET + non-blocking | CPU-bound ops block the reactor |
| v5 | Reactor + ThreadPool | Use-after-free on connection close |
| v6 | shared_ptr lifecycle | TCP粘包/拆包 (no message boundaries) |
| v7 | Dynamic Buffer + length protocol | Zombie connections (DoS via fd exhaustion) |
| v8 | Min-Heap timer | Proprietary protocol (incompatible with browsers) |
| v9 | FSM HTTP/1.1 parser + routing | (current — MySQL persistence added in v11) |

### Core Components (all header-only)

- **`server.cpp`** — Entry point. Sets up listening socket, epoll instance, event loop. Handles `EPOLLIN` (read HTTP request → parse → route → enqueue to thread pool) and client disconnect. Initializes Logger, ThreadPool, TimerManager, and MysqlConnPool.

- **`Connection.hpp`** — Per-connection state: fd, read/write `Buffer`, `HttpParser` state machine instance, `expire_time` for idle timeout, and a `buffer_mutex`. Managed via `shared_ptr` in `conn_map` (fd → `shared_ptr<Connection>`). Lifetime extended by weak_ptr in timer heap and by value-capture in thread pool lambdas.

- **`Buffer.hpp`** — `std::vector<char>`-backed read/write buffer with `read_index` / `write_index` cursors. No actual `readFd()`/`writeFd()` methods despite Architecture doc — read/write is done inline in `server.cpp`'s event loop. Provides `append()`, `peek()`, `retrieve()`, `retrieveAllAsString()`.

- **`HttpParser.hpp`** — FSM-based HTTP/1.1 request parser. States: `PARSE_REQUESTLINE` → `PARSE_HEADERS` → `PARSE_BODY` → `PARSE_FINISH`. Parses method, URL, headers into member fields. Resets for Keep-Alive pipeline. **8KB MAX_HEADER_LIMIT** as flood-attack defense.

- **`ThreadPool.hpp`** — Fixed-size pool (default 4 threads). Producer-consumer queue with `std::function<void()>` tasks, `std::mutex` + `std::condition_variable`. Server posts business logic here via `pool.enqueue([conn]() { process_business(conn); })`, keeping the reactor loop responsive.

- **`Timer.hpp`** — Idle timeout via `std::priority_queue` min-heap of `TimerNode` (each: `expire_time` + `weak_ptr<Connection>`). Lazy expiration: the event loop checks with 1s timeout on `epoll_wait`; `handle_expired_timers()` pops expired entries and compares the stored expiry against the connection's current `expire_time` to avoid killing refreshed connections.

- **`Logger.hpp`** — Singleton async logger with double-buffering. Front-end `append()` writes to `currentBuffer_` (4MB). Thread safe with minimal lock contention (lock only for buffer swap). Backend thread wakes on 1s timer or buffer-full notification, swaps buffers, writes to file.

- **`MysqlConnPool.hpp`** — Singleton MySQL connection pool (`libmysqlclient`). Maintains `std::queue<MYSQL*>`, uses POSIX semaphore (`sem_t`) for blocking-get semantics. Includes `ConnectionRAII` RAII holder for automatic borrow/return.

- **`client.cpp`** — Minimal terminal chat client. Wraps user input into `GET /chat?msg=... HTTP/1.1`, URL-encodes spaces as `%20`, strips HTTP headers from responses. Connects to `127.0.0.1:8088`.

### Data Flow (Request Lifecycle)

```
[Client] --HTTP GET/POST--> [Listening Socket]
                                  |
                              epoll_wait()
                                  |
  [server.cpp event loop] --EPOLLIN--> loop readv until EAGAIN
                                    append to conn->read_buffer
                                          |
                                    pool.enqueue([conn]() { process_business(conn); })
                                          |
                              ====== IN WORKER THREAD ======
                                    lock(buffer_mutex)
                                    while(there's a complete HTTP request)
                                        HttpParser::parse() -> FSM drives
                                        extract url path
                                        reset parser for pipeline
                                    unlock
                                    for each ready message:
                                        route: /chat?msg=... -> chat handler (with MySQL persist)
                                               else            -> HTML page response
                                    send() response directly on worker thread
```

### Key Design Decisions & Bug-Fix Sagas

- **shared_ptr lifecycle safety**: Thread pool tasks capture `shared_ptr<Connection>` by value, incrementing the refcount. This prevents the reactor from destroying the connection while a worker thread is still processing it. The `conn_map` is the sole owner; erasing from it triggers destruction only when all worker lambdas complete.

- **Edge-triggered epoll read loop**: `EPOLLET` means the reactor must read ALL available data in one shot. The event loop uses a `while(true)` loop with 1024-byte reads until `EAGAIN` or `EWOULDBLOCK`, appending everything into `conn->read_buffer`.

- **Lazy timer expiration**: The timer heap may contain stale entries for connections that have been refreshed (received new data). The critical guard `conn->expire_time == top.expire_time` prevents killing active connections — only connections whose current expiry matches the heap top's (meaning they never got refreshed) are reaped.

- **HTTP FSM with pipeline support**: After extracting a complete request, the parser is reset via `http_parser.reset()` so the same connection can handle the next pipelined request. If data is incomplete (拆包), the loop breaks and waits for epoll to deliver more data.

- **Flood attack defense chain**: (1) 64KB read buffer backlog limit in the reactor, (2) 8KB max header size in HttpParser, (3) Content-Length validation (< 64KB, non-negative), (4) malformed-packet detection with immediate `shutdown()`.

### Dependencies

- **libmysqlclient-dev** (`mysql/mysql.h`) — required for MysqlConnPool
- Linux kernel with epoll support
- pthreads (used via std::thread)

### Known Issues / Debugging Notes

- The CMake target `Tiny-Reactor-Server` produces a binary named `Tiny-Reactor-Server` — not `./server` as older docs suggest.
- `client.cpp` uses a naive HTTP header stripping (`find("\r\n")`), not proper double-CRLF parsing.
- `Logger::init()` must be called before any `LOG_INFO`/`LOG_ERROR` macro use.
- MySQL connection must be running with credentials matching the hardcoded values in `server.cpp` L182.