// Copyright 2018 Eryx <evorui аt gmail dοt com>, All rights reserved.
//

#ifndef LYNKSTOR_SERVER_RPC_HH
#define LYNKSTOR_SERVER_RPC_HH

#include "data.hh"
#include "data_base.hh"
#include "lynkstor.pb.h"
#include "hrpc.hh"

namespace lynkstor {

class LynkStorServiceImpl : public LynkStorService {
   private:
    data::Engine *db_data_;
    data::Engine *db_meta_;

   public:
    LynkStorServiceImpl(data::Engine *data, data::Engine *meta)
        : db_data_(data), db_meta_(meta) {};
    ~LynkStorServiceImpl() {};

    virtual void SysPing(hrpc::RpcController *ctr, const LynkStorResult *req,
                         LynkStorResult *rep, hrpc::Closure *done);

    virtual void RepFull(hrpc::RpcController *ctr, const LynkStorRepQuery *req,
                         LynkStorResult *rep, hrpc::Closure *done);

    virtual void RepPull(hrpc::RpcController *ctr, const LynkStorRepQuery *req,
                         LynkStorResult *rep, hrpc::Closure *done);
};

} // namespace lynkstor

#endif
