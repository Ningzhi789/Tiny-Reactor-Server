#ifndef HTTP_PARSER_HPP
#define HTTP_PARSER_HPP

#include <string>
#include <unordered_map>
#include <algorithm>
#include <filesystem>
#include <iostream>
#include "Buffer.hpp"
#include "Logger.hpp"

constexpr size_t MAX_HEADER_LIMIT = 8192;

class HttpParser {
public:
    // 状态机主状态
    enum ProcessState {
        PARSE_REQUESTLINE, // 正在解析请求行
        PARSE_HEADERS,     // 正在解析请求头
        PARSE_BODY,        // 正在解析请求体
        PARSE_FINISH       // 解析完全部完成
    };
    enum LineState {
        LINE_OK,   // 成功读到完整的一行（以\r\n结尾）
        LINE_BAD,  // 行语法错误
        LINE_OPEN  // 数据不完整，需要继续等待收包（遭遇拆包）
    };

    HttpParser():
        status_(PARSE_REQUESTLINE){}

    // 重置状态机，方便长连接（Keep-Alive）复用对象
    void reset() {
        status_ = PARSE_REQUESTLINE;
        method_="";
        url_="";
        version_="";
        headers_.clear();
    }

    ProcessState status() const{return status_;}
    std::string method() const{return method_;}
    std::string url() const{return url_;}
    std::string get_header(const std::string&key){return headers_[key];}

    // 🔥 驱动状态机运转的核心主控制中枢
    bool parse(Buffer& buf) {
        bool has_more_data=true;
        // 🔥 【恶意攻击防御一】：工业级标准限制，请求头（含请求行）最大允许 8192 字节（8KB）
        // 如果缓冲区积压超过 8KB 依然没有把状态机推到 FINISH，判定为畸形请求包或缓冲区淹没攻击
        if (buf.readable_bytes() > MAX_HEADER_LIMIT && status_ != PARSE_FINISH) {
            const char* peek_start = buf.peek();
            // 在前 8KB 范围内强行搜索 \r\n
            const char* crlf = std::search(peek_start, peek_start + MAX_HEADER_LIMIT,"\r\n" , "\r\n"+2);
            if (crlf == peek_start + MAX_HEADER_LIMIT) {
                std::cerr << "【安全警报】单行文本或头部累计超过 8KB 仍无结束符，判定为恶意洪水攻击！" << std::endl;
                return false; // 触发解析失败，逼迫服务器切断连接
            }
        }
        while (has_more_data&&status_!=PARSE_FINISH) {
            // 1. 找到当前缓冲区的行尾结束符 \r\n
            const char* peek_start=buf.peek();
            const char* crlf=std::search(peek_start,peek_start+buf.readable_bytes(),"\r\n" , "\r\n"+2);

            // 如果没找到 \r\n，说明当前行数据不全（遭遇拆包），退出等下一次数据
            if (crlf == peek_start + buf.readable_bytes()) {
                if (status_ == PARSE_BODY) {
                    // 处理 Body 的特殊逻辑，此处简单起见主要支持 GET 请求
                }
                break;
            }

            // 截取当前行的纯文本内容
            std::string line(peek_start,crlf);
            size_t line_len=line.length()+2;        //加上\r\n的长度

            // 2. 根据主状态机的当前状态，进行逻辑分流跳转
            switch (status_) {
                case PARSE_REQUESTLINE: {
                    if (!parse_request_line(line))   return false;
                    buf.retrieve(line_len);
                    status_=PARSE_HEADERS;
                    break;
                }
                case PARSE_HEADERS: {
                    if(line.empty()) {
                        // 如果读到了空行，代表 HTTP 头部结束了！
                        buf.retrieve(line_len);
                        status_=PARSE_FINISH;
                        break;
                    }
                    if (!parse_header_line(line)) return false;
                    buf.retrieve(line_len);
                    break;
                }
                default:
                    has_more_data = false;
                    break;
            }
        }
        return true;
    }

private:
    // 解析请求行: "GET /index.html HTTP/1.1"
    bool parse_request_line(const std::string& line) {
        auto s1=std::find(line.begin(),line.end(),' ');
        if (s1==line.end()) return false;
        method_=std::string(line.begin(),s1);

        auto s2=std::find(s1+1,line.end(),' ');
        if (s2==line.end()) return false;
        url_=std::string(s1+1,s2);
        version_=std::string(s2+1,line.end());

        LOG_INFO("【FSM状态机】解析请求行成功! Method: " + method_ +", URL: " + url_);
        //std::cout << "【FSM状态机】解析请求行成功! Method: " << method_ << ", URL: " << url_ << std::endl;
        return true;
    }

    // 解析请求头行: "Host: 127.0.0.1"
    bool parse_header_line(const std::string& line) {
        auto pos=line.find(':');
        if (pos==std::string::npos)
            return false;
        std::string key=line.substr(0,pos);     //截取冒号前面的键
        std::string val=line.substr(pos+2);     //截取冒号后面的值，排除空格
        headers_[key]=val;
        return true;
    }

    ProcessState status_;
    std::string method_;
    std::string url_;
    std::string version_;
    std::unordered_map<std::string, std::string> headers_;

};


#endif
