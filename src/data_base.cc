// Copyright 2017 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//

#include "data_base.hh"
#include "data.hh"
#include <iostream>
#include <string>
#include <vector>
#include "cmd.hh"
#include "utils.hh"
#include "cjet/log.hh"
#include "types.hh"
#include "status.hh"

namespace lynkstor {
namespace data {

void iobase::raw_incr(CmdContext &ctx, const std::string &key, int64_t val) {

    std::string val_prev;
    data::Status s = ctx.Stor->Get(key, &val_prev);
    KvObject *obj;

    if (s.ok() && val_prev.size() > 2) {
        obj = new KvObject(val_prev);
        if (obj->value.size() < 2 || obj->value[0] != value_ns_bytes) {
            return ctx.Reply.Error();
        }
        val += str_to_int64(obj->value.substr(1));
        obj->enc = "";
        obj->value = value_ns_bytes + std::to_string(val);
    } else {
        obj = new KvObject(value_ns_bytes + std::to_string(val), NULL);
    }

    //
    if (lynkstor::status::ConfigReplicationEnable) {
        obj->meta.set_updated(utils::timenow_us());
        auto ver = db_incr_u64(ctx, data::ns_exp_base_stats_tlog, 1);
        obj->meta.set_version(ver);

        WriteBatch batch;
        batch.Put(ns_exp_tlog_enc(ver, key), obj->Encode());
        batch.Put(key, obj->Encode());

        s = ctx.Stor->Write(WriteOptions(), &batch);

    } else {
        s = ctx.Stor->Put(key, obj->Encode());
    }

    if (s.ok()) {
        ctx.Reply.Int(val);
    } else {
        ctx.Reply.Error();
    }

    delete obj;
}

void iobase::raw_incrf(CmdContext &ctx, const std::string &keyenc, float val) {

    std::string val_prev;
    data::Status s = ctx.Stor->Get(keyenc, &val_prev);
    KvObject *obj;
    if (s.ok() && val_prev.size() > 0) {
        obj = new KvObject(val_prev);
        if (obj->value.size() < 2 || obj->value[0] != value_ns_bytes) {
            delete obj;
            return ctx.Reply.Error();
        }
        val += str_to_float(obj->value.substr(1));
        obj->enc = "";
        obj->value = value_ns_bytes + std::to_string(val);
    } else {
        obj = new KvObject(value_ns_bytes + std::to_string(val), NULL);
    }

    //
    if (lynkstor::status::ConfigReplicationEnable) {
        obj->meta.set_updated(utils::timenow_us());
        auto ver = db_incr_u64(ctx, data::ns_exp_base_stats_tlog, 1);
        obj->meta.set_version(ver);

        WriteBatch batch;
        batch.Put(ns_exp_tlog_enc(ver, keyenc), obj->Encode());
        batch.Put(keyenc, obj->Encode());

        s = ctx.Stor->Write(WriteOptions(), &batch);

    } else {
        s = ctx.Stor->Put(keyenc, obj->Encode());
    }

    if (s.ok()) {
        ctx.Reply.Push(value_ns_bytes + std::to_string(val));
    } else {
        ctx.Reply.Error();
    }

    delete obj;
}

void iobase::raw_scan(CmdContext &ctx, const std::string &k1,
                      const std::string &k2, int limit, bool rk, bool rv,
                      bool ex_skip, bool meta_skip, int prefix_len) {

    auto min = k1;
    auto max = k2;

    if (int i = 100 - max.size() > 0) {
        max.append(i, 0xff);
    }

    if (limit > kv_scan_limit_max) {
        limit = kv_scan_limit_max;
    } else if (limit < 1) {
        limit = 1;
    }

    EngineIterator *it = ctx.Stor->NewIterator();
    if (it == NULL) {
        return ctx.Reply.Error();
    }

    it->Seek(min);
    if (it->Valid() && it->key().ToString() <= min) {
        it->Next();
    }

    uint64_t tn = utils::timenow_ms();
    for (; it->Valid(); it->Next()) {

        if (limit < 1) {
            break;
        }

        std::string key = it->key().ToString();
        if (key.size() <= prefix_len || key.compare(max) >= 0) {
            break;
        }

        KvObject obj(it->value().ToString());

        if (ex_skip) {
            if (obj.meta.expired() > 0 && obj.meta.expired() < tn) {
                continue;
            }
        }

        if (rk) {
            ctx.Reply.Push(key.substr(prefix_len));
        }
        if (rv) {
            if (meta_skip) {
                ctx.Reply.Push(obj.value);
            } else {
                ctx.Reply.Push(it->value().ToString());
            }
        }

        limit--;
    }

    delete it;
}

int iobase::raw_scan_handler(CmdContext &ctx, const std::string &k1,
                             const std::string &k2, int limit, int prefix_len,
                             iobase_scan_handler fn) {

    auto min = k1;
    auto max = k2;

    if (int i = 100 - max.size() > 0) {
        max.append(i, 0xff);
    }

    if (limit > kv_scan_limit_max) {
        limit = kv_scan_limit_max;
    } else if (limit < 1) {
        limit = 1;
    }

    EngineIterator *it = ctx.Stor->NewIterator();
    if (it == NULL) {
        return -1;
    }

    it->Seek(min);
    if (it->Valid() && it->key().ToString() <= min) {
        it->Next();
    }

    for (; it->Valid(); it->Next()) {

        if (limit < 1) {
            break;
        }

        std::string key = it->key().ToString();

        if (key.size() <= prefix_len || key.compare(max) >= 0) {
            break;
        }

        if (fn(ctx, key.substr(prefix_len), it->value().ToString()) == -1) {
            break;
        }

        limit--;
    }

    delete it;
    return 0;
}

uint64_t iobase::db_incr_u64(CmdContext &ctx, const std::string &key,
                             int64_t val) {
    return ng_incr_u64(ctx.Stor, key, val);
}

uint64_t iobase::ng_incr_u64(Engine *ctx, const std::string &key, int64_t val) {

    std::string val_prev;
    data::Status s = ctx->Get(key, &val_prev);
    if (s.ok() && val_prev.size() > 0) {
        int64_t prev = str_to_int64(val_prev);
        if (val == 0) {
            return prev;
        }
        val += prev;
    }

    if (val < 0) {
        val = 0;
    }
    s = ctx->Put(key, std::to_string(val));

    if (s.ok() && val > 0) {
        return val;
    }

    return 0;
}

void iobase::db_scan_handler(CmdRpcContext &ctx, const std::string &k1,
                             const std::string &k2, int limit, int limit_size,
                             int prefix_len) {
    auto min = k1;
    auto max = k2;

    if (int i = 100 - max.size() > 0) {
        max.append(i, 0xff);
    }

    if (limit > kv_scan_limit_max) {
        limit = kv_scan_limit_max;
    } else if (limit < 1) {
        limit = 1;
    }

    if (limit_size < 1 || limit_size > kv_scan_limit_size_max) {
        limit_size = kv_scan_limit_size_max;
    } else if (limit_size < kv_scan_limit_size_min) {
        limit_size = kv_scan_limit_size_min;
    }

    EngineIterator *it = ctx.Stor->NewIterator();
    if (it == NULL) {
        ctx.Reply->set_status(types::ResultError);
        return;
    }

    it->Seek(min);
    if (it->Valid() && it->key().ToString() <= min) {
        it->Next();
    }

    for (; it->Valid(); it->Next()) {

        if (limit < 1) {
            break;
        }

        std::string key = it->key().ToString();
        if (key.size() <= prefix_len || key.compare(max) >= 0) {
            break;
        }

        limit_size -= (key.size() + it->value().size());
        if (limit_size < 1) {
            break;
        }

        auto item = ctx.Reply->add_items();
        item->set_key(key.substr(prefix_len));
        item->set_data(it->value().ToString());

        limit--;
    }

    delete it;

    if (limit > 0 && limit_size > 0) {
        ctx.Reply->set_attrs(types::AttrTypeDone);
    }

    ctx.Reply->set_status(types::ResultOK);
}

int iobase::kv_new(CmdContext &ctx, std::string key, std::string value,
                   KvWriteOptions *opts) {
    std::string keyenc = ns_keyenc(key);

    //
    std::string v;
    data::Status s = ctx.Stor->Get(keyenc, &v);
    if (s.ok()) {
        return 0;
    }

    KvObject obj(value, opts);
    if (!obj.Valid()) {
        return -1;
    }

    // value
    WriteBatch batch;

    //
    if (lynkstor::status::ConfigReplicationEnable) {
        obj.meta.set_updated(utils::timenow_us());
        auto ver = db_incr_u64(ctx, data::ns_exp_base_stats_tlog, 1);
        obj.meta.set_version(ver);
        batch.Put(ns_exp_tlog_enc(ver, keyenc), obj.Encode());
    }

    //
    batch.Put(keyenc, obj.Encode());

    // meta
    // std::string m_key = ns_meta_enc(keyenc);
    // batch.Put(m_key, obj.MetaEncode());

    // ttl
    if (obj.meta.expired() > 0) {
        batch.Put(ns_ttl_enc(obj.meta.expired(), keyenc), "1");
    }

    // commit
    s = ctx.Stor->Write(WriteOptions(), &batch);
    if (!s.ok()) {
        return -1;
    }

    return 1;
}

int iobase::kv_mnew(CmdContext &ctx, const CmdArgv &argv) {

    if (argv.size() >= 3 && argv.size() % 2 == 1) {

        int num = (argv.size() - 1) / 2;
        int num_set = 0;

        data::Status s;
        WriteBatch batch;
        for (int i = 1; i < argv.size(); i += 2) {

            std::string keyenc = ns_keyenc(argv[i]);
            std::string value;

            s = ctx.Stor->Get(keyenc, &value);
            if (s.ok() && value.size() < 2) {
                KvObject obj(argv[i + 1], NULL);
                if (obj.Valid()) {

                    //
                    // std::string m_key = ns_meta_enc(keyenc);
                    // batch.Put(m_key, obj.MetaEncode());

                    //
                    if (lynkstor::status::ConfigReplicationEnable) {
                        obj.meta.set_updated(utils::timenow_us());
                        auto ver =
                            db_incr_u64(ctx, data::ns_exp_base_stats_tlog, 1);
                        obj.meta.set_version(ver);
                        batch.Put(ns_exp_tlog_enc(ver, keyenc), obj.Encode());
                    }

                    // value
                    batch.Put(keyenc, obj.Encode());

                    num_set++;
                }
            }
        }

        if (num_set > 0) {
            s = ctx.Stor->Write(WriteOptions(), &batch);
            if (!s.ok()) {
                return -1;
            }
            if (num_set == num) {
                return 1;
            }
        }

        return 0;
    }

    return -1;
}

int iobase::kv_put(CmdContext &ctx, std::string key, std::string value,
                   KvWriteOptions *opts) {

    KvObject obj(value, opts);
    if (!obj.Valid()) {
        return -1;
    }

    std::string keyenc = ns_keyenc(key);
    data::Status s;

    //
    if (opts != NULL && opts->OpAllow(data::KvOpCreate)) {
        std::string v;
        s = ctx.Stor->Get(keyenc, &v);
        if (s.ok()) {
            return 0;
        }
    }

    // std::cerr << "TTO " << obj.meta.expired() << "\n";

    // value
    WriteBatch batch;

    if (lynkstor::status::ConfigReplicationEnable) {
        obj.meta.set_updated(utils::timenow_us());
        auto ver = db_incr_u64(ctx, data::ns_exp_base_stats_tlog, 1);
        obj.meta.set_version(ver);
        batch.Put(ns_exp_tlog_enc(ver, keyenc), obj.Encode());
    }

    batch.Put(keyenc, obj.Encode());

    // meta
    // std::string m_key = ns_meta_enc(keyenc);
    // batch.Put(m_key, obj.MetaEncode());

    // ttl
    if (obj.meta.expired() > data::ttlat_ms_min) {
        batch.Put(ns_ttl_enc(obj.meta.expired(), keyenc), "1");
        // print_bs(ns_ttl_enc(obj.meta.expired(), keyenc));
    }

    // commit
    s = ctx.Stor->Write(WriteOptions(), &batch);
    if (!s.ok()) {
        return -1;
    }

    return 1;
}

int iobase::kv_mput(CmdContext &ctx, const CmdArgv &argv) {

    if (argv.size() >= 3 && argv.size() % 2 == 1) {

        WriteBatch batch;
        int bnum = 0;
        for (int i = 1; i < argv.size(); i += 2) {
            std::string keyenc = ns_keyenc(argv[i]);
            KvObject obj(argv[i + 1], NULL);
            if (obj.Valid()) {

                if (lynkstor::status::ConfigReplicationEnable) {
                    obj.meta.set_updated(utils::timenow_us());
                    auto ver =
                        db_incr_u64(ctx, data::ns_exp_base_stats_tlog, 1);
                    obj.meta.set_version(ver);
                    batch.Put(ns_exp_tlog_enc(ver, keyenc), obj.Encode());
                }

                // value
                batch.Put(keyenc, obj.Encode());

                // meta
                // std::string m_key = ns_meta_enc(keyenc);
                // batch.Put(m_key, obj.MetaEncode());
                bnum++;
            }
        }
        if (bnum > 0) {
            data::Status s = ctx.Stor->Write(WriteOptions(), &batch);
            if (s.ok()) {
                return 1;
            }
        }
    }

    return -1;
}

int64_t iobase::kv_ttl_get(CmdContext &ctx, std::string key) {

    std::string keyenc = ns_keyenc(key);
    std::string value;
    data::Status s = ctx.Stor->Get(keyenc, &value);
    if (s.ok() && value.size() > 2) {
        KvMeta meta;
        base_meta_decode(&meta, value);
        if (meta.expired() == 0) {
            return -1;
        }
        uint64_t tn = utils::timenow_ms();
        if (meta.expired() > tn) {
            return (meta.expired() - tn);
        }
    }

    return -2;
}

std::string iobase::kv_get(CmdContext &ctx, std::string key) {
    std::string keyenc = ns_keyenc(key);
    std::string value;
    data::Status s = ctx.Stor->Get(keyenc, &value);
    if (!s.ok()) {
        return "";
    }

    return KvObjectDecodeValueBody(value);

    KvObject obj(value);
    if (obj.meta.expired() > 0) {
        uint64_t tn = utils::timenow_ms();
        if (obj.meta.expired() < tn) {
            return "";
        }
    }

    return obj.value;
}

KvObject iobase::kv_get_object(CmdContext &ctx, std::string key) {
    std::string keyenc = ns_keyenc(key);
    std::string value;
    data::Status s = ctx.Stor->Get(keyenc, &value);
    KvObject obj(value);
    if (!s.ok() || value.size() < 2) {
        obj.not_found = true;
    } else if (obj.meta.expired() > 0) {
        uint64_t tn = utils::timenow_ms();
        if (obj.meta.expired() < tn) {
            obj.not_found = true;
        }
    }

    return obj;
}

int iobase::kv_del(CmdContext &ctx, const CmdArgv &argv) {
    int n = 0;
    data::Status s;
    for (int i = 1; i < argv.size(); i++) {
        std::string keyenc = ns_keyenc(argv[i]);
        std::string value;
        s = ctx.Stor->Get(keyenc, &value);
        if (s.ok()) {

            if (lynkstor::status::ConfigReplicationEnable) {
                KvObject obj(value);
                obj.meta.set_updated(utils::timenow_us());
                auto ver = db_incr_u64(ctx, data::ns_exp_base_stats_tlog, 1);
                obj.meta.set_version(ver);
                obj.meta.set_type(data::KvMetaTypeDelete);
                obj.value = "";

                WriteBatch batch;
                batch.Put(ns_exp_tlog_enc(ver, keyenc), obj.Encode());
                batch.Delete(keyenc);
                s = ctx.Stor->Write(WriteOptions(), &batch);

            } else {
                s = ctx.Stor->Del(keyenc);
            }

            if (s.ok()) {
                n++;
            }
        }
    }
    return n;
}

void iobase::kv_scan(CmdContext &ctx, const std::string &k1,
                     const std::string &k2, int limit, bool rk, bool rv,
                     bool rvf, int prefix_len) {
    std::string min = ns_keyenc(k1);
    std::string max = ns_keyenc(k2);

    if (int i = 100 - max.size() > 0) {
        max.append(i, 0xff);
    }

    if (limit > kv_scan_limit_max) {
        limit = kv_scan_limit_max;
    } else if (limit < 1) {
        limit = 1;
    }

    EngineIterator *it = ctx.Stor->NewIterator();
    if (it == NULL) {
        return ctx.Reply.Error();
    }

    it->Seek(min);
    if (it->Valid() && it->key().ToString() <= min) {
        it->Next();
    }

    uint64_t tn = utils::timenow_ms();
    for (; it->Valid(); it->Next()) {

        if (limit < 1) {
            break;
        }

        std::string key = it->key().ToString();
        if (key.size() <= prefix_len || key.compare(max) >= 0) {
            break;
        }

        KvObject obj(it->value().ToString());
        if (obj.meta.expired() > 0 && obj.meta.expired() < tn) {
            continue;
        }

        if (rk) {
            ctx.Reply.Push(key.substr(prefix_len));
        }
        if (rvf) {
            ctx.Reply.Push(it->value().ToString());
        } else if (rv) {
            ctx.Reply.Push(obj.value);
        }

        limit--;
    }

    delete it;
}

void iobase::kv_revscan(CmdContext &ctx, const std::string &k1,
                        const std::string &k2, int limit, bool rk, bool rv,
                        bool rvf, int prefix_len) {
    std::string max = ns_keyenc(k1);
    std::string min = ns_keyenc(k2);

    if (int i = 100 - max.size() > 0) {
        max.append(i, 0xff);
    }

    if (limit > kv_scan_limit_max) {
        limit = kv_scan_limit_max;
    } else if (limit < 1) {
        limit = 1;
    }

    EngineIterator *it = ctx.Stor->NewIterator();
    if (it == NULL) {
        return ctx.Reply.Error();
    }

    it->Seek(max);
    if (it->Valid() && it->key().ToString() >= max) {
        it->Prev();
    }

    uint64_t tn = utils::timenow_ms();
    for (; it->Valid(); it->Prev()) {

        if (limit < 1) {
            break;
        }

        std::string key = it->key().ToString();
        if (key.size() <= prefix_len || key.compare(min) <= 0) {
            break;
        }

        KvObject obj(it->value().ToString());
        if (obj.meta.expired() > 0 && obj.meta.expired() < tn) {
            continue;
        }

        if (rk) {
            ctx.Reply.Push(key.substr(prefix_len));
        }
        if (rvf) {
            ctx.Reply.Push(it->value().ToString());
        } else if (rv) {
            ctx.Reply.Push(obj.value);
        }

        limit--;
    }

    delete it;
}

} // namespace data
} // namespace lynkstor
