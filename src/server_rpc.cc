// Copyright 2018 Eryx <evorui аt gmail dοt com>, All rights reserved.
//

#include "server_rpc.hh"
#include "data.hh"
#include "data_base.hh"
#include "types.hh"
#include "status.hh"
#include "hrpc.hh"

namespace lynkstor {

const std::string cmd_system_rep_async_cut({data::NS_REP_SYNC_CUT});

static data::iobase dbase(data::NS_KV_DATA);

void LynkStorServiceImpl::SysPing(hrpc::RpcController *ctr,
                                  const lynkstor::LynkStorResult *req,
                                  lynkstor::LynkStorResult *rep,
                                  hrpc::Closure *done) {

    rep->set_data(std::string("PONG ") + req->data());
    if (done) {
        done->Run();
    }
};

void LynkStorServiceImpl::RepFull(hrpc::RpcController *ctr,
                                  const lynkstor::LynkStorRepQuery *req,
                                  lynkstor::LynkStorResult *rep,
                                  hrpc::Closure *done) {

    if (status::sys_putlog_ver_min == 0) {
        rep->set_status(types::ResultError);
        rep->set_data("service is not available");
    } else {

        std::string off = req->key_offset();
        if (off.size() == 0) {
            off.append(1, data::NS_REP_SYNC_OFF);
        }
        if (off[0] < data::NS_REP_SYNC_OFF || off[0] > data::NS_REP_SYNC_CUT) {
            rep->set_status(types::ResultError);
        } else {

            CmdRpcContext ctx(db_data_, db_meta_, rep);
            dbase.db_scan_handler(ctx, off, cmd_system_rep_async_cut,
                                  req->limit(), data::ByteSize8MB, 0);
        }
    }

    if (done) {
        done->Run();
    }
};

void LynkStorServiceImpl::RepPull(hrpc::RpcController *ctr,
                                  const lynkstor::LynkStorRepQuery *req,
                                  lynkstor::LynkStorResult *rep,
                                  hrpc::Closure *done) {

    if (status::sys_putlog_ver_min == 0) {
        rep->set_status(types::ResultError);
        rep->set_data("service is not available");
    } else if (req->data_version() < status::sys_putlog_ver_min) {
        rep->set_status(types::ResultError);
        rep->set_attrs(types::AttrTypeLimitOutRange);
        rep->set_data(std::to_string(status::sys_putlog_ver_max));
    } else {

        auto off = dbase.ns_exp_tlog_enc(req->data_version(), "");
        off.append(1, 0xff);
        auto cut = dbase.ns_exp_tlog_enc(utils::time_us_max, "");

        CmdRpcContext ctx(db_data_, db_meta_, rep);
        dbase.db_scan_handler(ctx, off, cut, req->limit(), data::ByteSize8MB,
                              9);
    }

    if (done) {
        done->Run();
    }
};

} // namespace lynkstor
