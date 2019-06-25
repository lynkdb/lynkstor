// Copyright 2017 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//

#include <iostream>
#include <string>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <fstream>
#include <unistd.h>
#include <errno.h>
#include <cstring>
#include "net.hh"
#include "cmd.hh"
#include "cjet/log.hh"
#include "utils.hh"

namespace lynkstor {

//
NetSocketConn::NetSocketConn(int fdcn) {
    m_fdcn = fdcn;
    m_recv_buf_limit = NetSocketConnRecvBufLimit;
}

NetSocketConn::~NetSocketConn() {}

bool NetSocketConn::Send(const std::string &s) const {
    return NetSocketSend(m_fdcn, s);
}

int NetSocketConn::Recv(std::string &s) const {
    return NetSocketRecv(m_fdcn, s);
}

//
bool NetSocketSend(int fd, const std::string &s) {

    std::string msg = s;

    while (true) {

        int n = ::send(fd, msg.c_str(), msg.size(), MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EAGAIN) {
                // CJET_LOG("warn", "NetSocketSend send EAGAIN");
                // ::usleep(50);
                continue;
            }
            // CJET_LOG("error", "send errno %d", errno);
            return false;
        }

        if (n < msg.size()) {
            if (n > 0) {
                msg.erase(0, n);
            }
            continue;
        }

        break;
    }

    return true;
}

const int netSocketRecvTimeout = 10000000;

int NetSocketRecv(int fd, std::string &s) {

    NetNonBlock(fd, true);

    int num = 0;
    int n = 0;
    int tryus = 0;

    while (true) {
        char buf[NetSocketConnRecvBufLimit + 1];
        memset(buf, 0, NetSocketConnRecvBufLimit + 1);

        n = ::read(fd, buf, NetSocketConnRecvBufLimit);

        if (n == -1) {

            if (errno == EAGAIN) {

                // CJET_LOG("warn", "NetSocketRecv read EAGAIN");
                if (num > 0) {
                    break;
                }

                if (tryus > netSocketRecvTimeout) {
                    return -1;
                }

                ::usleep(50);
                tryus += 50;
                continue;
            }

            return -1;
        }

        if (n > 0) {
            s.append(buf, n);
            num += n;
        }

        if (n == NetSocketConnRecvBufLimit) {
            continue;
        }

        break;
    }

    return num;
}

//
NetSocketKeeper::NetSocketKeeper()
    : sock_fp_(-1), conn_max_limit_(NetSocketConnMaxLimit) {}

NetSocketKeeper::~NetSocketKeeper() {
    if (sock_valid()) {
        ::close(sock_fp_);
        CJET_LOG("info", "net-keeper sock closed");
    }
}

int NetSocketKeeper::Fd() { return sock_fp_; }

int NetSocketKeeper::Listen(std::string host, int port) {

    // create
    sock_fp_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (!sock_valid()) {
        return -1;
    }

    // setting
    int opt_ru = 1;
    if (::setsockopt(sock_fp_, SOL_SOCKET, SO_REUSEADDR, (const char *)&opt_ru,
                     sizeof(opt_ru)) != 0) {
        return -1;
    }

    int opt_reu = 1;
    if (::setsockopt(sock_fp_, SOL_SOCKET, SO_REUSEADDR, &opt_reu,
                     sizeof(opt_reu)) != 0) {
        return -1;
    }

    // int opt_sb = 2 * 1024 * 1024;
    // if (::setsockopt(sock_fp_, SOL_SOCKET, SO_SNDBUF, (char *)&opt_sb,
    //                  sizeof(opt_sb)) != 0) {
    //     return -1;
    // }

    // bind
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr(host.c_str()); // or INADDR_ANY
    addr.sin_port = htons(port);
    if (::bind(sock_fp_, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        return -1;
    }

    // listen
    if (::listen(sock_fp_, conn_max_limit_) != 0) {
        return -1;
    }

    // NetNonBlock(sock_fp_, true);

    return sock_fp_;
}

void NetNonBlock(int fd, bool yes) {
    if (fd > 0) {
        if (yes) {
            ::fcntl(fd, F_SETFL, O_NONBLOCK | O_RDWR);
        } else {
            ::fcntl(fd, F_SETFL, O_RDWR);
        }
    }
}

NetSocketConn *NetSocketKeeper::Accept() {

    struct sockaddr_in addr;
    socklen_t addr_len = sizeof(addr);

    int sock = ::accept(sock_fp_, (sockaddr *)&addr, &addr_len);
    if (sock == -1) {
        return NULL;
    }

    struct linger opt_lg = {1, 0};
    if (::setsockopt(sock, SOL_SOCKET, SO_LINGER, (const char *)&opt_lg,
                     sizeof(opt_lg)) != 0) {
        return NULL;
    }

    NetSocketConn *conn = new NetSocketConn(sock);

    return conn;
}

} // namespace lynkstor
