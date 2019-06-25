// Copyright 2017 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//

#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string>

#include "data.hh"
#include "data_base.hh"
#include "net.hh"
#include "utils.hh"
#include "server.hh"
#include "event.hh"
#include "cjet/log.hh"
#include "types.hh"
#include "status.hh"
#include "hrpc.hh"

namespace lynkstor {

static data::iobase dbase(data::NS_KV_DATA);

int Server::Start() {

    ServerBackend backend(db_data_, db_meta_);
    if (backend.Start() != 0) {
        return -1;
    }

    CJET_LOG("info", "server/base started");

    ServerContext sctx(db_data_, db_meta_);

    EventLoop *el = new EventLoop(net_kpr_->Fd());
    for (;;) {

        int n = el->Wait();

        for (int i = 0; i < n; i++) {
            if (NetConnHandler(sctx, el->fired_[i].req_cmd_) == -1) {
                el->FiredDel(i);
            }
        }
    }

    CJET_LOG("warn", "server/base exit");

    return 0;
}

int Server::NetConnHandler(ServerContext &sctx, CmdReqBuffer *cb) {

    for (;;) {

        std::vector<std::string> argv = cb->Argv();
        if (cb->err_) {
            return -1;
        }

        int argc = argv.size();
        if (argc < 1) {
            break;
        }
        // CJET_LOG("info", "NetConnHandler IN argc num %d", argc);

        if (argc > 0) {

            argv[0] = str_to_upper(argv[0]);
            // std::cerr << "CMD " << argv[0] << " " << argc << "\n";

            {
                if (!cb->authed_ && lynkstor::status::ConfigAuth == "") {
                    cb->authed_ = true;
                }

                if (argv[0] == "AUTH") {
                    if (argc > 1) {
                        if (argv[1] == lynkstor::status::ConfigAuth) {
                            cb->authed_ = true;
                            NetSocketSend(cb->fd_, CmdReplyOK);
                        } else {
                            NetSocketSend(cb->fd_, CmdReplyAuthError);
                        }
                    } else {
                        NetSocketSend(cb->fd_, CmdReplyAuthError);
                    }
                    return 0;
                }

                if (!cb->authed_ && argv[0] != "COMMAND") {
                    NetSocketSend(cb->fd_, CmdReplyNoAuth);
                    return 0;
                }
            }

            CmdEntry *ce = CmdEntryGet(argv[0]);

            if (!ce) {
                if (!NetSocketSend(cb->fd_, CmdReplyNotImplemented)) {
                    return -1;
                }
                break;
            }
            if (argc < ce->ArgcMin || argc > ce->ArgcMax) {
                if (!NetSocketSend(cb->fd_, CmdReplyErr)) {
                    return -1;
                }
                break;
            }

            CmdContext ctx(sctx.db_data_, sctx.db_meta_);
            ce->Handler(ctx, argv);

            // CJET_LOG("info", "Handler reply bulk: %d, inline: %d, quit: %d",
            //      ctx.Reply.Bulk.size(), ctx.Reply.Inline.size(),
            //      ctx.Reply.Quit);

            //
            std::string send = ctx.Reply.Send();
            if (send.size() == 0) {
                send = CmdReplyOK;
            }

            if (!NetSocketSend(cb->fd_, send)) {
                return -1;
            }

            if (ctx.Reply.Quit) {
                return -1;
            }

        } else {
            break; // debug
        }

        if (!cb->Next()) {
            break;
        }
    }

    return 0;
}

int ServerBackend::Start() {

    pthread_t ptto;
    if (pthread_create(&ptto, NULL, &ServerBackend::TimeToOutWorker, this) !=
        0) {
        CJET_LOG("error", "failed to create server/tto-handler pthread");
        return -1;
    }

    pthread_t rlkp;
    if (pthread_create(&rlkp, NULL, &ServerBackend::PutlogWorker, this) != 0) {
        CJET_LOG("error", "failed to create server/rep-log-keeper pthread");
        return -1;
    }

    pthread_t prp;
    if (pthread_create(&prp, NULL, &ServerBackend::RepPullWorker, this) != 0) {
        CJET_LOG("error", "failed to create server/rep-pull pthread");
        return -1;
    }

    return 0;
}

void *ServerBackend::TimeToOutWorker(void *ptr) {

    ServerBackend *cb = (ServerBackend *)ptr;

    std::string off = dbase.ns_ttl_enc(0, "");
    data::Status s;
    uint64_t tn = 0;
    data::WriteBatch batch;
    int batch_cap = 0;

    for (;;) {
        usleep(300 * 1000);

        tn = utils::timenow_ms();
        std::string cut = dbase.ns_ttl_enc(tn, "");
        cut.push_back(0xff);

        data::EngineIterator *it = cb->db_data_->NewIterator();
        if (it == NULL) {
            continue;
        }

        for (it->Seek(off); it->Valid(); it->Next()) {

            std::string key = it->key().ToString();

            if (key.size() < 1 || key > cut) {
                break;
            }

            batch.Delete(key);
            batch_cap += 1;

            if (key.size() > 9) {

                std::string value;
                s = cb->db_data_->Get(key.substr(9), &value);
                if (s.ok()) {
                    data::KvObject obj(value);
                    if (obj.meta.expired() > 0 && obj.meta.expired() <= tn) {
                        batch.Delete(key.substr(9));
                    }
                }
            }

            if (batch_cap >= 10) {
                cb->db_data_->Write(data::WriteOptions(), &batch);
                batch.Clear();
                batch_cap = 0;
            }
        }

        if (batch_cap > 0) {
            cb->db_data_->Write(data::WriteOptions(), &batch);
            batch.Clear();
            batch_cap = 0;
        }

        delete it;
    }

    return 0;
}

void *ServerBackend::PutlogWorker(void *ptr) {

    if (!lynkstor::status::ConfigReplicationEnable) {
        return 0;
    }

    ServerBackend *cb = (ServerBackend *)ptr;
    bool putlog_init = false;
    data::Status s;

    for (;;) {
        if (putlog_init) {
            sleep(600);
        }
        putlog_init = true;

        //
        std::string tlog_value;
        uint64_t tlog_ver =
            dbase.ng_incr_u64(cb->db_data_, data::ns_exp_base_stats_tlog, 0);
        if (tlog_ver < 1) {
            continue;
        }
        sleep(1);

        // CJET_LOG("warn", "putlog %d, min/max %d/%d", tlog_ver,
        //      status::sys_putlog_ver_min, status::sys_putlog_ver_max);

        if (status::sys_putlog_ver_max < tlog_ver) {
            status::sys_putlog_ver_max = tlog_ver;
        }

        if (status::sys_putlog_ver_min + putlog_num_min >
            status::sys_putlog_ver_max) {
            status::sys_putlog_ver_min =
                status::sys_putlog_ver_max - putlog_num_min;
        }

        if (status::sys_putlog_ver_min + putlog_num_max <
            status::sys_putlog_ver_max) {
            status::sys_putlog_ver_min =
                status::sys_putlog_ver_max - putlog_num_max;
        }

        if (status::sys_putlog_ver_min < 1) {
            status::sys_putlog_ver_min = 1;
        }

        data::EngineIterator *it = cb->db_data_->NewIterator();
        if (it == NULL) {
            continue;
        }

        std::string off = dbase.ns_exp_tlog_enc(0, "");
        std::string cut = dbase.ns_exp_tlog_enc(utils::time_us_max, "");
        uint64_t alive_time = utils::timenow_us() - putlog_time_usec_min;

        // CJET_LOG("info", "putlog_keeper clean less than %lld us",
        // alive_time);

        int stats_num = 0;
        int64_t sync_size = 0;
        int64_t ver_off = 0;

        for (it->Seek(off); it->Valid(); it->Next()) {

            std::string key = it->key().ToString();

            if (key.size() < 1 || key > cut || key < off) {
                break;
            }

            data::KvObject obj(it->value().ToString());
            if (obj.Valid() && obj.meta.version() > 0) {

                if ((obj.meta.updated() > alive_time) ||
                    (status::sys_putlog_ver_min + putlog_num_min >
                     status::sys_putlog_ver_max)) {
                    break;
                }

                if (status::sys_putlog_ver_min < obj.meta.version()) {
                    status::sys_putlog_ver_min = obj.meta.version();
                    // CJET_LOG("warn", "putlog_keeper status putlog_min/max:
                    // %d/%d",
                    //      status::sys_putlog_ver_min,
                    //      status::sys_putlog_ver_max);
                }

                //
                if (ver_off == 0) {
                    ver_off = obj.meta.version();
                }

            } else {

                if (!obj.Valid()) {
                    CJET_LOG("info", "putlog_keeper err obj.Valid()");
                }

                if (obj.meta.version() < 1) {
                    CJET_LOG("info", "putlog_keeper err obj.meta.version()");
                }
            }

            // CJET_LOG("warn", "putlog_keeper status putlog del: %lld",
            //      obj.meta.updated() / 1e6);

            s = cb->db_data_->Del(key);
            if (!s.ok()) {
                break;
            }

            stats_num += 1;
            sync_size += it->value().size();
        }

        delete it;

        if (stats_num > 0) {
            CJET_LOG("info",
                     "putlog_keeper clean num %d, size %lld, time-set %lld, "
                     "version from "
                     "%lld to %lld, current %lld",
                     stats_num, sync_size, alive_time, ver_off,
                     status::sys_putlog_ver_min, status::sys_putlog_ver_max);
        }
    }

    return 0;
}

void *ServerBackend::RepPullWorker(void *ptr) {

    ServerBackend *cb = (ServerBackend *)ptr;

    const std::string min = data::ns_exp_base_stats_rep_uplist_cli("");
    const std::string max = min + std::string(16, 0xff);

    for (;;) {
        sleep(1);

        data::EngineIterator *it = cb->db_data_->NewIterator();
        if (it == NULL) {
            continue;
        }

        int n = 0;
        for (it->Seek(min); it->Valid(); it->Next()) {

            if (it->key().size() < 3 || it->key().ToString() > max) {
                break;
            }

            TypeKvPairs *pairs = new TypeKvPairs();
            types::kv_pairs_decode(pairs, it->value().ToString());
            if (types::kv_pairs_get(pairs, "id").size() == 16) {
                cb->repPullWorkerEntry(cb->db_data_, pairs);
                n += 1;
            }
            delete pairs;
        }

        delete it;

        if (n == 0) {
            sleep(3);
        }
    }

    return 0;
}

typedef std::unordered_map<std::string, uint64_t> client_connect_errors;

static client_connect_errors pull_errors;

void ServerBackend::repPullWorkerEntry(data::Engine *db_data_,
                                       TypeKvPairs *pairs) {

    auto upid = types::kv_pairs_get(pairs, "id");
    auto host = types::kv_pairs_get(pairs, "host");
    auto auth = types::kv_pairs_get(pairs, "auth");
    auto port = std::stoi(types::kv_pairs_get(pairs, "port"));

    uint64_t pull_created = utils::timenow_us();

    //
    client_connect_errors::iterator eit = pull_errors.find(upid);
    if (eit != pull_errors.end()) {
        uint64_t last_failed = eit->second;
        if (last_failed > 0) {
            if (pull_created - last_failed < 10000000) {
                return;
            }
        }
    }

    data::Status s;
    int limit = 100;

    std::string addr = fmt::sprintf("tcp://%s:%d", host, port);
    hrpc::Channel *chan = hrpc::ChannelPoll(addr);
    if (chan == NULL) {
        CJET_LOG("error", "failed to create rpc/channel %s", addr.c_str());
        pull_errors[upid] = utils::timenow_us();
        return;
    }

    hrpc::AuthMac *authmac = NULL;
    if (auth.size() > 0) {
        authmac = hrpc::NewAuthMac("root", auth);
    }

    LynkStorService::Stub stub(chan);

    // std::cout << "RepPull Response " << response.data() << "\n";

    std::string full_offset;
    std::string full_offkey =
        data::ns_exp_base_stats_rep_full_offset_cli + upid;
    int sync_num = 0;
    int sync_num_ttl = 0;
    int sync_num_del = 0;
    int64_t sync_size = 0;

    //
    std::string version_key =
        data::ns_exp_base_stats_rep_pull_offset_cli + upid;
    std::string version;
    uint64_t version_u64 = 0;
    uint64_t version_u64_lastset = 0;

    LynkStorResult *rs = NULL;
    LynkStorRepQuery qry;

    //
    db_data_->Get(full_offkey, &full_offset);
    if (full_offset.size() > 1) {
        goto full;
    } else {
        goto pull;
    }

pull:
    db_data_->Get(version_key, &version);
    if (version == "") {
        version = "0";
    }
    version_u64 = str_to_uint64(version);
    version_u64_lastset = version_u64;

    // CJET_LOG("debug", "rep_pull id: %s, ver: %d", upid.c_str(), version_u64);

    qry.set_node_id(upid);
    qry.set_limit(limit);

    for (;;) {

        if (rs != NULL) {
            delete rs;
        }
        rs = new LynkStorResult();

        hrpc::ClientContext ctx(authmac);

        qry.set_data_version(version_u64);

        stub.RepPull(&ctx, &qry, rs, NULL);

        if (!ctx.OK()) {
            CJET_LOG("error", "rep/pull error code %d, msg %s", ctx.ErrorCode(),
                     ctx.ErrorText().c_str());
            pull_errors[upid] = utils::timenow_us();
            goto close;
        }

        if (rs->status() != types::ResultOK) {

            if (types::AttrAllow(rs->attrs(), types::AttrTypeLimitOutRange)) {

                version_u64 = str_to_uint64(rs->data());

                db_data_->Put(version_key, std::to_string(version_u64));

                CJET_LOG("warn", "rep/pull out range %s, min %lld ",
                         addr.c_str(), version_u64);

                goto full;
            }
            CJET_LOG("warn", "rep/pull version %lld, attrs: %lld, error %s",
                     version_u64, rs->attrs(), rs->data().c_str());
            pull_errors[upid] = utils::timenow_us();
            goto close;
        }

        for (int i = 0; i < rs->items_size(); i++) {

            std::string key = rs->items(i).key();
            if (key.size() < 2) {
                continue;
            }

            if (uint8_t(key[0]) < data::NS_REP_SYNC_OFF ||
                uint8_t(key[0]) > data::NS_REP_SYNC_CUT) {
                CJET_LOG("error", "rep/pull invalid key-prefix type %d",
                         uint8_t(key[0]));
                continue;
            }

            data::KvObject upobj(rs->items(i).data());
            if (!upobj.MetaValid()) {
                continue;
            }

            if (upobj.meta.version() > version_u64) {
                version_u64 = upobj.meta.version();
            }

            std::string lval;
            s = db_data_->Get(key, &lval);
            if (s.ok() && lval.size() > 2) {
                data::KvObject lobj(lval);
                if (lobj.Valid() &&
                    lobj.meta.updated() > upobj.meta.updated()) { // TODO
                    continue;
                }
            }

            data::WriteBatch batch;

            if (upobj.meta.type() == data::KvMetaTypeDelete) {
                batch.Delete(key);
                sync_num_del += 1;
            } else {

                //
                if (upobj.meta.expired() > 0) {
                    batch.Put(
                        data::iobase_ns_ttl_enc(upobj.meta.expired(), key),
                        "1");
                    sync_num_ttl += 1;
                }

                //
                if (lynkstor::status::ConfigReplicationEnable) {
                    auto ver = dbase.ng_incr_u64(
                        db_data_, data::ns_exp_base_stats_tlog, 1);
                    upobj.meta.set_version(ver);
                } else {
                    upobj.meta.set_version(0);
                }

                batch.Put(key, upobj.Encode());
            }

            s = db_data_->Write(data::WriteOptions(), &batch);
            if (!s.ok()) {
                goto close;
            }
            sync_num += 1;
        }

        if (version_u64 > version_u64_lastset) {
            s = db_data_->Put(version_key, std::to_string(version_u64));
            if (!s.ok()) {
                break;
            }
            version_u64_lastset = version_u64;
        }

        if (types::AttrAllow(rs->attrs(), types::AttrTypeDone)) {
            break;
        }
    }

    if (sync_num > 0) {
        CJET_LOG("info",
                 "rep/pull node %s, ver %lld, num %d, ttl %d, "
                 "del %d, in %lld us",
                 upid.c_str(), version_u64, sync_num, sync_num_ttl,
                 sync_num_del, (utils::timenow_us() - pull_created));
    }
    goto close;

full:
    // CJET_LOG("warn", "rep/full async version %lld", version_u64);

    if (full_offset.size() == 0) {
        full_offset.append(1, data::NS_REP_SYNC_OFF);
    }
    CJET_LOG("warn", "rep/full async version %lld", version_u64);

    qry.set_node_id(upid);
    qry.set_limit(limit);

    for (;;) {

        if (rs != NULL) {
            delete rs;
        }
        rs = new LynkStorResult();

        hrpc::ClientContext ctx(authmac);

        qry.set_key_offset(full_offset);

        stub.RepFull(&ctx, &qry, rs, NULL);

        if (!ctx.OK()) {
            CJET_LOG("error", "rep/full error code %d, msg %s", ctx.ErrorCode(),
                     ctx.ErrorMessage().c_str());
            break;
        }

        if (rs->status() != types::ResultOK || rs->items_size() < 1) {
            if (types::AttrAllow(rs->attrs(), types::AttrTypeDone)) {
                db_data_->Del(full_offkey);
                CJET_LOG("warn", "rep/full done node %s, num %d, in %lld ms",
                         upid.c_str(), sync_num,
                         (utils::timenow_us() - pull_created) / 1000);
            } else {
                CJET_LOG("error", "rep/full error status %d", rs->status());
            }
            break;
        }

        for (int i = 0; i < rs->items_size(); i++) {

            std::string key = rs->items(i).key();
            full_offset = rs->items(i).key();

            if (uint8_t(key[0]) < data::NS_REP_SYNC_OFF ||
                uint8_t(key[0]) > data::NS_REP_SYNC_CUT) {
                CJET_LOG("error", "rep/full invalid key-prefix type %d",
                         uint8_t(key[0]));
                continue;
            }

            data::KvObject upobj(rs->items(i).data());
            if (!upobj.MetaValid()) {
                continue;
            }

            std::string lval;
            s = db_data_->Get(key, &lval);
            if (s.ok() && lval.size() > 2) {
                data::KvObject lobj(lval);
                if (lobj.Valid() &&
                    lobj.meta.updated() > upobj.meta.updated()) {
                    continue;
                }
            }

            data::WriteBatch batch;

            if (upobj.meta.type() == data::KvMetaTypeDelete) {
                batch.Delete(key);
            } else {

                //
                if (upobj.meta.expired() > 0) {
                    batch.Put(
                        data::iobase_ns_ttl_enc(upobj.meta.expired(), key),
                        "1");
                }

                //
                if (lynkstor::status::ConfigReplicationEnable) {
                    auto ver = dbase.ng_incr_u64(
                        db_data_, data::ns_exp_base_stats_tlog, 1);
                    upobj.meta.set_version(ver);
                } else {
                    upobj.meta.set_version(0);
                }

                batch.Put(key, upobj.Encode());
            }

            s = db_data_->Write(data::WriteOptions(), &batch);
            if (!s.ok()) {
                goto close;
            }
            sync_num += 1;
            sync_size += rs->items(i).data().size();
        }

        int64_t in = (utils::timenow_us() - pull_created) / 1000;
        if (in < 1) {
            in = 1;
        }
        CJET_LOG("info",
                 "rep/full node %s, num %d, size %lld, time %llds, %lld/s",
                 upid.c_str(), sync_num, sync_size, in / 1000,
                 (sync_size * 1000 / in));

        s = db_data_->Put(full_offkey, full_offset);
        if (!s.ok()) {
            break;
        }

        if (types::AttrAllow(rs->attrs(), types::AttrTypeDone)) {
            db_data_->Del(full_offkey);
            break;
        }
    }
    goto close;

close:
    if (rs != NULL) {
        delete rs;
    }
    if (authmac != NULL) {
        delete authmac;
    }
};

} // namespace lynkstor
