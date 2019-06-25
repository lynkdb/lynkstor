// Copyright 2017 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//

#include <iostream>
#include <string>
#include <vector>
#include "cmd.hh"
#include "data.hh"
#include "data_base.hh"
#include "utils.hh"
#include "cjet/log.hh"
#include "lynkstor.pb.h"
#include "status.hh"

namespace lynkstor {

class KvProg {

   public:
    // Put key value [EX seconds] [PX milliseconds] [NX|XX]
    static void Put(CmdContext &ctx, const CmdArgv &argv);
    // Get key
    static void Get(CmdContext &ctx, const CmdArgv &argv);
    // Scan key_start key_end limit
    static void Scan(CmdContext &ctx, const CmdArgv &argv);
    // RevScan key_start key_end limit
    static void RevScan(CmdContext &ctx, const CmdArgv &argv);
    // Del key [key ...]
    static void Del(CmdContext &ctx, const CmdArgv &argv);
    static void Incr(CmdContext &ctx, const CmdArgv &argv);
    static void Meta(CmdContext &ctx, const CmdArgv &argv);

    KvProg() {
        CmdEntryReg("KvProgPut", KvProg::Put, 2, 2);
        CmdEntryReg("KvProgGet", KvProg::Get, 2, 2);
        CmdEntryReg("KvProgScan", KvProg::Scan, 4, 4);
        CmdEntryReg("KvProgRevScan", KvProg::RevScan, 4, 4);
        CmdEntryReg("KvProgDel", KvProg::Del, 2, -1);
        CmdEntryReg("KvProgIncr", KvProg::Incr, 3, 3);
        CmdEntryReg("KvProgMeta", KvProg::Meta, 2, 2);
    }
}; // class KvProg

const uint64_t KvProgTtlNsecFix = 15e17; // 15e(8+9) nanoseconds

const uint32_t KvProgKeyEntryUnknown = 0;
const uint32_t KvProgKeyEntryBytes = 1;
const uint32_t KvProgKeyEntryUint = 4;
const uint32_t KvProgKeyEntryIncr = 16;

const uint64_t KvProgOpMetaSum = 1 << 1;
const uint64_t KvProgOpMetaSize = 1 << 2;
const uint64_t KvProgOpCreate = 1 << 13;
const uint64_t KvProgOpForce = 1 << 14;
const uint64_t KvProgOpFoldMeta = 1 << 15;
const uint64_t KvProgOpLogEnable = 1 << 16;

static KvProg _prog_init;
static data::iobase dbase(data::NS_PROG_DATA);

static bool prog_key_valid(lynkstor::KvProgKey key) {
    if (key.items_size() > 0 && key.items_size() <= 20) {
        return true;
    }
    return false;
}

static std::string prog_key_encode(lynkstor::KvProgKey key) {
    std::string k = "0";
    k[0] = uint8_t(key.items_size());
    for (int i = 0; i < key.items_size(); i++) {
        if (i + 1 < key.items_size()) {
            if (key.items(i).data().size() > 0) {
                k.append(1, uint8_t(key.items(i).data().size()));
                k.append(key.items(i).data());
            } else {
                k.append(1, 0);
            }
        } else if (key.items(i).data().size() > 0) {
            k.append(key.items(i).data());
        }
    }
    return k;
}

static std::string prog_key_fold_encode(lynkstor::KvProgKey key) {
    std::string k = "0";
    k[0] = uint8_t(key.items_size());
    for (int i = 0; i < (key.items_size() - 1); i++) {
        if (key.items(i).data().size() > 0) {
            k.append(1, uint8_t(key.items(i).data().size()));
            k.append(key.items(i).data());
        } else {
            k.append(1, 0);
        }
    }
    return k;
}

static int prog_key_fold_len(lynkstor::KvProgKey key) {
    int n = key.items_size();
    if (n > 0) {
        for (int i = 0; i < (key.items_size() - 1); i++) {
            n += key.items(i).data().size();
        }
        n++;
    }
    return n;
}

static bool prog_opaction_allow(uint64_t actions, uint64_t v) {
    return ((v & actions) == v);
}

void KvProg::Put(CmdContext &ctx, const CmdArgv &argv) {

    lynkstor::KvProgKeyValueCommit pc;

    if (!pc.ParseFromString(argv[1])) {
        return ctx.Reply.Error();
    }

    if (!prog_key_valid(pc.key())) {
        return ctx.Reply.Error();
    }

    if (pc.value().size() < 2) {
        return ctx.Reply.Error();
    }

    auto key = prog_key_encode(pc.key());
    // std::cerr << "key encode " << key << "\n";

    data::KvObject prev;
    prev.not_found = true;
    data::KvObject obj(pc.value(), NULL);
    bool fold_stats = false;

    if (pc.has_options()) {

        auto opts = pc.options();

        if (prog_opaction_allow(opts.actions(), KvProgOpCreate)) {
            prev = dbase.kv_get_object(ctx, key);
            if (!prev.not_found) {
                return ctx.Reply.Int(0);
            }
        }

        if (prev.not_found &&
            (opts.prev_sum() > 0 ||
             prog_opaction_allow(opts.actions(), KvProgOpFoldMeta))) {
            prev = dbase.kv_get_object(ctx, key);
        }

        if (!prev.not_found && opts.prev_sum() > 0) {
            if (opts.prev_sum() != prev.ValueCrc32()) {
                return ctx.Reply.Error();
            }
        }

        if (prog_opaction_allow(opts.actions(), KvProgOpMetaSum)) {
            obj.meta.set_sum(1);
        }

        if (prog_opaction_allow(opts.actions(), KvProgOpMetaSize)) {
            obj.meta.set_size(1);
        }

        if (opts.expired() > KvProgTtlNsecFix) {
            obj.meta.set_expired(opts.expired() /
                                 1e6); // nanosecond to millisecond
        }

        if (prog_opaction_allow(opts.actions(), KvProgOpFoldMeta)) {
            fold_stats = true;
        }
    }

    data::WriteBatch batch;
    std::string nskey = dbase.ns_keyenc(key);

    if (obj.meta.expired() > data::ttlat_ms_min) {
        batch.Put(dbase.ns_ttl_enc(obj.meta.expired(), nskey), "1");
    }

    if (fold_stats) {
        std::string fold_key = prog_key_fold_encode(pc.key());
        auto fold = dbase.kv_get_object(ctx, fold_key);

        uint64_t value_size = pc.value().size() - 1;
        if (fold.meta.num() > 0) {

            if (prev.not_found) {
                fold.meta.set_num(fold.meta.num() + 1);
                fold.meta.set_size(fold.meta.size() + value_size);
            } else {
                int64_t si = int64_t(fold.meta.size()) + int64_t(value_size) -
                             int64_t(prev.ValueSize());
                if (si >= 0) {
                    fold.meta.set_size(si);
                } else {
                    fold.meta.set_size(0);
                }
            }

        } else {
            fold.meta.set_num(1);
            fold.meta.set_size(value_size);
        }
        obj.meta.set_num(1);

        std::string fold_enc = fold.Encode();
        if (fold_enc.size() > 1) {
            batch.Put(dbase.ns_keyenc(fold_key), fold_enc);
        }
    }

    if (lynkstor::status::ConfigReplicationEnable) {
        obj.meta.set_updated(utils::timenow_us());
        auto ver = dbase.db_incr_u64(ctx, data::ns_exp_base_stats_tlog, 1);
        obj.meta.set_version(ver);
        batch.Put(dbase.ns_exp_tlog_enc(ver, nskey), obj.Encode());
    }

    batch.Put(nskey, obj.Encode());

    // commit
    data::Status s = ctx.Stor->Write(data::WriteOptions(), &batch);
    if (!s.ok()) {
        return ctx.Reply.Error();
    }

    // std::cerr << "prog len " << argv[1].size() << "\n";
    // std::cerr << "prog key len " << pc.key().items_size() << "\n";
    // for (int i = 0; i < pc.key().items_size(); i++) {
    //     auto key_entry = pc.key().items(i);
    //     std::cerr << "prog key entry " << key_entry.type() << ", "
    //               << key_entry.data() << "\n";
    // }

    return ctx.Reply.Int(1);
}

void KvProg::Get(CmdContext &ctx, const CmdArgv &argv) {
    lynkstor::KvProgKey pk;

    if (!pk.ParseFromString(argv[1])) {
        return ctx.Reply.Error();
    }

    if (!prog_key_valid(pk)) {
        return ctx.Reply.Error();
    }

    auto key = prog_key_encode(pk);

    std::string value = dbase.kv_get(ctx, key);
    if (value.size() > 0) {
        ctx.Reply.Push(value);
    } else {
        ctx.Reply.NotFound();
    }
}

void KvProg::Scan(CmdContext &ctx, const CmdArgv &argv) {

    lynkstor::KvProgKey off;
    lynkstor::KvProgKey cut;

    if (!off.ParseFromString(argv[1]) || !cut.ParseFromString(argv[2])) {
        return ctx.Reply.Error();
    }

    if (!prog_key_valid(off) || !prog_key_valid(cut)) {
        return ctx.Reply.Error();
    }

    dbase.kv_scan(ctx, prog_key_encode(off), prog_key_encode(cut),
                  str_to_int(argv[3]), true, true, false,
                  prog_key_fold_len(off));
}

void KvProg::RevScan(CmdContext &ctx, const CmdArgv &argv) {

    lynkstor::KvProgKey off;
    lynkstor::KvProgKey cut;

    if (!off.ParseFromString(argv[1]) || !cut.ParseFromString(argv[2])) {
        return ctx.Reply.Error();
    }

    if (!prog_key_valid(off) || !prog_key_valid(cut)) {
        return ctx.Reply.Error();
    }

    dbase.kv_revscan(ctx, prog_key_encode(off), prog_key_encode(cut),
                     str_to_int(argv[3]), true, true, false,
                     prog_key_fold_len(off));
}

void KvProg::Del(CmdContext &ctx, const CmdArgv &argv) {
    lynkstor::KvProgKeyValueCommit pc;

    if (!pc.ParseFromString(argv[1])) {
        return ctx.Reply.Error();
    }

    if (!prog_key_valid(pc.key())) {
        return ctx.Reply.Error();
    }

    std::string key = prog_key_encode(pc.key());

    data::KvObject prev = dbase.kv_get_object(ctx, key);
    if (prev.not_found) {
        return ctx.Reply.Int(0);
    }

    if (prev.meta.num() == 1) {
        std::string fold_key = prog_key_fold_encode(pc.key());
        data::KvObject fold = dbase.kv_get_object(ctx, fold_key);
        if (!fold.not_found) {
            if (fold.meta.num() > 1) {
                if (fold.meta.size() > prev.meta.size()) {
                    fold.meta.set_size(fold.meta.size() - prev.meta.size());
                } else {
                    fold.meta.set_size(0);
                }
                fold.meta.set_num(fold.meta.num() - 1);

                std::string fold_enc = fold.Encode();
                ctx.Stor->Put(dbase.ns_keyenc(fold_key), fold_enc);
            } else {
                ctx.Stor->Del(dbase.ns_keyenc(fold_key));
            }
        }
    }

    data::Status s;
    auto keyenc = dbase.ns_keyenc(key);

    if (lynkstor::status::ConfigReplicationEnable) {
        prev.meta.set_updated(utils::timenow_us());
        auto ver = dbase.db_incr_u64(ctx, data::ns_exp_base_stats_tlog, 1);
        prev.meta.set_version(ver);
        prev.meta.set_type(data::KvMetaTypeDelete);
        prev.value = "";

        // CJET_LOG("info", "delete v %lld", ver);

        data::WriteBatch batch;
        batch.Put(dbase.ns_exp_tlog_enc(ver, keyenc), prev.Encode());
        batch.Delete(keyenc);
        s = ctx.Stor->Write(data::WriteOptions(), &batch);

    } else {
        s = ctx.Stor->Del(keyenc);
    }

    if (s.ok()) {
        ctx.Reply.Int(1);
    } else {
        ctx.Reply.Int(0);
    }
}

void KvProg::Incr(CmdContext &ctx, const CmdArgv &argv) {
    lynkstor::KvProgKey pk;

    if (!pk.ParseFromString(argv[1])) {
        return ctx.Reply.Error();
    }

    if (!prog_key_valid(pk)) {
        return ctx.Reply.Error();
    }

    auto key = prog_key_encode(pk);
    // std::cerr << "incr " << str_to_int64(argv[2]) << "\n";

    dbase.raw_incr(ctx, dbase.ns_keyenc(key), str_to_int64(argv[2]));
}

void KvProg::Meta(CmdContext &ctx, const CmdArgv &argv) {

    lynkstor::KvProgKey pk;

    if (!pk.ParseFromString(argv[1])) {
        return ctx.Reply.Error();
    }

    if (!prog_key_valid(pk)) {
        return ctx.Reply.Error();
    }

    auto key = prog_key_encode(pk);

    auto obj = dbase.kv_get_object(ctx, key);
    if (obj.not_found) {
        return ctx.Reply.NotFound();
    }
    std::string meta_enc = obj.MetaEncode();
    if (meta_enc.size() > 0) {
        ctx.Reply.Push(meta_enc);
    } else {
        ctx.Reply.NotFound();
    }
}

} // namespace lynkstor
