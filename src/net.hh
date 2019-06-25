// Copyright 2017 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//

#ifndef LYNKSTOR_NET_HH
#define LYNKSTOR_NET_HH

#include <string>
#include <netinet/in.h>
#include <sys/epoll.h>

namespace lynkstor {

const int NetSocketConnMaxLimit = 1000;
const int NetSocketConnRecvBufLimit = 4094;

class NetSocketConn;
class NetSocketKeeper {
   private:
    int sock_fp_;
    int conn_max_limit_;
    bool sock_valid() const { return sock_fp_ >= 0; }

   public:
    NetSocketKeeper();
    ~NetSocketKeeper();
    int Fd();
    int Listen(std::string host, int port);
    NetSocketConn *Accept();
}; // class NetSocketKeeper

class NetSocketConn {
   private:
    int m_fdcn;
    int m_recv_buf_limit;

   public:
    NetSocketConn(int fdcn);
    ~NetSocketConn();
    bool Send(const std::string &) const;
    int Recv(std::string &) const;
}; // class NetSocketConn

extern bool NetSocketSend(int fd, const std::string &msg);
extern int NetSocketRecv(int fd, std::string &msg);
extern void NetNonBlock(int fd, bool yes);

} // namespace lynkstor

#endif
