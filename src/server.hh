// Copyright 2017 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//

#ifndef LYNKSTOR_SERVER_HH
#define LYNKSTOR_SERVER_HH

#include "data.hh"
#include "cmd.hh"
#include "net.hh"
#include "types.hh"

namespace lynkstor {

class ServerContext {
   public:
    data::Engine *db_data_;
    data::Engine *db_meta_;
    ServerContext(data::Engine *data, data::Engine *meta)
        : db_data_(data), db_meta_(meta) {};
}; // class ServerContext

class Server {
   private:
    data::Engine *db_data_;
    data::Engine *db_meta_;
    NetSocketKeeper *net_kpr_;

   public:
    Server(data::Engine *data, data::Engine *meta, NetSocketKeeper *nsk)
        : db_data_(data), db_meta_(meta), net_kpr_(nsk) {};
    ~Server() {};
    int Start();
    static int NetConnHandler(ServerContext &ev_ctx, CmdReqBuffer *cb);
}; // class Server

class ServerBackend {
   private:
    data::Engine *db_data_;
    data::Engine *db_meta_;
    static void repPullWorkerEntry(data::Engine *db, TypeKvPairs *pairs);

   public:
    ServerBackend(data::Engine *data, data::Engine *meta)
        : db_data_(data), db_meta_(meta) {};
    int Start();
    static void *TimeToOutWorker(void *ptr);
    static void *PutlogWorker(void *ptr);
    static void *RepPullWorker(void *ptr);
}; // class ServerBackend

static const int64_t putlog_time_usec_min = 864000 * 1e6;
static const int64_t putlog_num_min = 1000;
static const int64_t putlog_num_max = 100000;

} // namespace lynkstor

#endif
