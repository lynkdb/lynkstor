// Copyright 2017 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//

#include <iostream>
#include <string>
#include <sys/socket.h>
#include <netdb.h>
// #include <fcntl.h>
#include <arpa/inet.h>
// #include <netinet/in.h>
// #include <netinet/tcp.h>
#include <unistd.h>
#include <string.h>

#include "client.hh"
#include "../net.hh"
#include "../utils.hh"
// #include "../logger.hh"

namespace lynkstor {
namespace client {

Client *Client::Connect(const std::string &addr, int port) {

    int sock = -1;
    struct sockaddr_in saddr;
    ::bzero(&saddr, sizeof(saddr));
    saddr.sin_family = AF_INET;
    saddr.sin_port = htons((short)port);
    saddr.sin_addr.s_addr = ::inet_addr(addr.c_str());

    if ((sock = ::socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        return NULL;
    }

    if (::connect(sock, (struct sockaddr *)&saddr, sizeof(saddr)) == -1) {
        return NULL;
    }

    int opt = 1;
    ::setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, (void *)&opt, sizeof(opt));

    Client *cn = new Client();
    cn->sock = sock;

    return cn;
}

Result *Client::Cmd(std::string cmd, std::string arg1, std::string arg2,
                    std::string arg3, std::string arg4) {

    std::string buf = "*5" + delim;

    buf.append("$" + std::to_string(cmd.size()) + delim);
    buf.append(cmd + delim);

    buf.append("$" + std::to_string(arg1.size()) + delim);
    buf.append(arg1 + delim);

    buf.append("$" + std::to_string(arg2.size()) + delim);
    buf.append(arg2 + delim);

    buf.append("$" + std::to_string(arg3.size()) + delim);
    buf.append(arg3 + delim);

    buf.append("$" + std::to_string(arg4.size()) + delim);
    buf.append(arg4 + delim);

    return cmd_exec(buf);
}

Result *Client::Cmd(std::string cmd, std::string arg1, std::string arg2,
                    std::string arg3) {

    std::string buf = "*4" + delim;

    buf.append("$" + std::to_string(cmd.size()) + delim);
    buf.append(cmd + delim);

    buf.append("$" + std::to_string(arg1.size()) + delim);
    buf.append(arg1 + delim);

    buf.append("$" + std::to_string(arg2.size()) + delim);
    buf.append(arg2 + delim);

    buf.append("$" + std::to_string(arg3.size()) + delim);
    buf.append(arg3 + delim);

    return cmd_exec(buf);
}

Result *Client::Cmd(std::string cmd, std::string arg1, std::string arg2) {

    std::string buf = "*3" + delim;

    buf.append("$" + std::to_string(cmd.size()) + delim);
    buf.append(cmd + delim);

    buf.append("$" + std::to_string(arg1.size()) + delim);
    buf.append(arg1 + delim);

    buf.append("$" + std::to_string(arg2.size()) + delim);
    buf.append(arg2 + delim);

    return cmd_exec(buf);
}

Result *Client::Cmd(std::string cmd, std::string arg1) {

    std::string buf = "*2" + delim;

    buf.append("$" + std::to_string(cmd.size()) + delim);
    buf.append(cmd + delim);

    buf.append("$" + std::to_string(arg1.size()) + delim);
    buf.append(arg1 + delim);

    return cmd_exec(buf);
}

Result *Client::Cmd(std::string cmd) {

    std::string buf;
    buf.append("*1" + delim);
    buf.append("$" + std::to_string(cmd.size()) + delim);
    buf.append(cmd + delim);

    return cmd_exec(buf);
}

Result *Client::cmd_exec(const std::string data) {

    // std::cout << "CMD: SEND: " << data << "\n";

    if (!lynkstor::NetSocketSend(sock, data)) {
        return NewResult(0, "net error");
    }

    Result *rs = new Result();

    if (cmd_parse(rs) == 1) {
        rs->status = ResultOK;
        return rs;
    }

    return rs;
}

int Client::read() {

    int n = -1;

    while (true) {
        char buf[bufio_read_size + 1];
        ::bzero(buf, bufio_read_size + 1);

        n = ::read(sock, buf, bufio_read_size);
        if (n == -1) {

            if (errno == EAGAIN) {
                // lynkstor_logger_print("warn", "client read EAGAIN");
                ::usleep(50);
                continue;
            }

            return -1;
        } else if (n > 0) {
            m_buffer.append(buf, n);
        }

        if (n != bufio_read_size) {
            break;
        }
    }

    return n;
}

std::string Client::cmd_line() {

    std::string s;

    while (true) {

        int n = m_buffer.find_first_of("\r\n", 0);
        if (n < 0) {
            if (read() < 0) {
                break;
            }
            continue;
        }

        s = m_buffer.substr(0, n);
        m_buffer.erase(0, n + 2);
        break;
    }

    return s;
}

int Client::cmd_parse(Result *rs) {

    std::string line = cmd_line();
    if (line.size() < 1) {
        return -1;
    }

    int size = 0;

    switch (line[0]) {

        // Errors
        case '-':
            rs->data = line.substr(1);
            rs->cap = 0;
            rs->status = ResultError;
            // std::cout << "RE - {{{" << rs->data << "}}}\n";
            break;

        // Simple Strings, Integers
        case '+':
        case ':':
            rs->data = line.substr(1);
            rs->cap = 1;
            // std::cout << "RE + {{{" << rs->data << "}}}\n";
            break;

        // Bulk Strings
        case '$':
            size = str_to_int(line.substr(1));
            if (size > 0) {
                int n = cmd_parse_string(rs, size);
                if (n == 1) {
                    rs->cap = 1;
                } else if (n == -1) {
                    rs->status = ResultError;
                }
            }
            break;

        // Array
        case '*':
            size = str_to_int(line.substr(1));
            // std::cout << "array " << size << "\n";
            if (size > 0) {
                cmd_parse_array(rs, size);
            }
            break;
    }

    if (rs->status == 0) {
        rs->status = ResultOK;
    }

    return 0;
}

int Client::cmd_parse_string(Result *rs, int size) {

    while (true) {

        if (size + 2 > m_buffer.size()) {
            if (read() < 0) {
                return -1;
            }
            continue;
        }

        rs->data = m_buffer.substr(0, size);
        m_buffer.erase(0, size + 2);

        break;
    }
    return 1;
}

int Client::cmd_parse_array(Result *rs, int cap) {

    int size = 0;

    for (int i = 0; i < cap; i++) {

        std::string line = cmd_line();

        if (line.size() < 2) {
            rs->status = ResultError;
            rs->cap = 0;
            return -1;
        }

        switch (line[0]) {

            case '$':
                size = str_to_int(line.substr(1));
                if (size > 0) {
                    Result *item = new Result();
                    int n = cmd_parse_string(item, size);
                    if (n == 1) {
                        item->cap += 1;
                        rs->items.push_back(item);
                    } else if (n == -1) {
                        rs->status = ResultError;
                    }
                }
                break;

            case '*':
                size = str_to_int(line.substr(1));
                Result *item = new Result();
                if (size > 0) {
                    cmd_parse_array(item, size);
                }
                rs->items.push_back(item);
                break;
        }
    }

    return 0;
}

void Client::Close() {
    if (sock > 0) {
        ::close(sock);
        sock = -1;
    }
}

} // namespace client
} // namespace lynkstor
