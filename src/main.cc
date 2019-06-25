// Copyright 2017 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//

#include <iostream>
#include <string>
#include <exception>
#include <stdexcept>
#include <unistd.h>
#include <fcntl.h>

#include "jemalloc/jemalloc.h"

#include "version.hh"
#include "net.hh"
#include "data.hh"
#include "server.hh"
#include "server_rpc.hh"
#include "cjet/flag.hh"
#include "cjet/log.hh"
#include "cjet/conf.hh"
#include "hrpc.hh"
#include "utils.hh"
#include "status.hh"

void help() {
    std::cout << "Usage:\n";
    std::cout << "  lynkstor -server [-config /path/to/lynkstor.conf]\n ";
    std::cout << "  lynkstor -version\n";
    std::cout << "  lynkstor -help\n\n";
}

void daemonize() {

    if (fork() != 0) {
        exit(0);
    }
    setsid();

    int fd = open("/dev/null", O_RDWR, 0);
    if (fd != -1) {
        dup2(fd, STDIN_FILENO);
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        if (fd > STDERR_FILENO) {
            close(fd);
        }
    }
}

int pidfile(const char *file) {
    FILE *fp = fopen(file, "w");
    if (fp) {
        fprintf(fp, "%d\n", (int)getpid());
        fclose(fp);
    }
    return 0;
}

void version_info() {
    std::cout << lynkstor::LYNKSTOR_NAME << " " << lynkstor::LYNKSTOR_VERSION
              << "\n";
}

void server_start() {
    //
    if (cjet::conf::Config::Init(cjet::flag::Value("config")) != 0) {
        throw std::invalid_argument("failed on init config");
    }
}

int main(int argc, char **argv) {

    cjet::flag::Init(argc, argv);
    CJET_LOG("info", "%s/%s start", lynkstor::LYNKSTOR_NAME.c_str(),
             lynkstor::LYNKSTOR_VERSION.c_str());

    if (cjet::flag::Has("version")) {
        version_info();
    } else if (cjet::flag::Has("help")) {
        help();
    } else if (cjet::flag::Has("server")) {

        try {
            //
            if (cjet::conf::Config::Init(cjet::flag::Value("config")) != 0) {
                throw std::invalid_argument("failed on init config");
            }

            lynkstor::status::ConfigAuth =
                cjet::conf::Config::Value("server/requirepass");

            if (cjet::conf::Config::Value("replication/enable") == "yes") {
                lynkstor::status::ConfigReplicationEnable = true;
            } else {
                lynkstor::status::ConfigReplicationEnable = false;
            }

            std::string engine_name;
            if (cjet::conf::Config::Value("leveldb") == "enable") {
                engine_name = "leveldb";
#ifdef LYNKSTOR_ENGINE_ROCKSDB
            } else if (cjet::conf::Config::Value("rocksdb") == "enable") {
                engine_name = "rocksdb";
#endif
            } else {
                throw std::invalid_argument("no engine config found");
            }

            //
            if (cjet::flag::Has("daemon") ||
                cjet::conf::Config::Value("server/daemon") == "yes") {
                daemonize();
            }

            //
            auto *meta = lynkstor::data::NewEngine(engine_name, "meta");
            if (meta == NULL) {
                throw std::invalid_argument("failed on init meta engine");
            }

            auto *data = lynkstor::data::NewEngine(engine_name, "data");
            if (data == NULL) {
                throw std::invalid_argument("failed on init data engine");
            }

            //
            std::string ip = cjet::flag::Value("bind");
            if (ip.size() < 7) {
                ip = cjet::conf::Config::Value("server/bind");
            }
            if (ip.size() < 7) {
                ip = "127.0.0.1";
            }
            int port = cjet::flag::ValueSize("port", 0, 65535);
            if (port == 0) {
                port = cjet::conf::Config::ValueSize("server/port", 0, 65535);
            }

            int rpc_port = cjet::flag::ValueSize("rpc_port", 0, 65535);
            if (rpc_port < 1) {
                rpc_port =
                    cjet::conf::Config::ValueSize("server/rpc_port", 0, 65535);
            }

            if (rpc_port > 0) {

                hrpc::Server *rpcServer = hrpc::NewServer(ip, rpc_port);
                rpcServer->RegisterService(
                    new lynkstor::LynkStorServiceImpl(data, meta));

                if (lynkstor::status::ConfigAuth != "") {
                    rpcServer->RegisterAuthMac(
                        hrpc::NewAuthMac("root", lynkstor::status::ConfigAuth));
                }

                pthread_t prpc;
                if (pthread_create(&prpc, NULL, rpcServer->Start, rpcServer) !=
                    0) {
                    throw std::invalid_argument(
                        "failed to create server/rpc pthread");
                }

                CJET_LOG("info", "hrpc/bind tcp://0.0.0.0:%d", rpc_port);
            }

            if (port > 0) {
                lynkstor::NetSocketKeeper kpr;
                if (!kpr.Listen(ip, port)) {
                    throw std::invalid_argument("failed to init network");
                }

                //
                lynkstor::Server server(data, meta, &kpr);
                server.Start();
            }
        }
        catch (const std::invalid_argument &e) {
            std::cout << "FATAL: " << e.what() << "\n";
            return 7;
        }
        catch (...) {
            std::cout << "FATAL: Exception during startup, aborting\n";
            return 7;
        }
    } else {
        help();
        return 1;
    }

    return 0;
}
