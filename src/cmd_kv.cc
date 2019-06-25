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

namespace lynkstor {

class Kv {
   private:
    static data::KvWriteOptions write_options_parse(const CmdArgv &argv);

   public:
    // Put key value [EX seconds] [PX milliseconds] [NX|XX]
    static void Put(CmdContext &ctx, const CmdArgv &argv);
    static void MultiPut(CmdContext &ctx, const CmdArgv &argv);
    // Get key
    static void Get(CmdContext &ctx, const CmdArgv &argv);
    static void MultiGet(CmdContext &ctx, const CmdArgv &argv);
    static void Meta(CmdContext &ctx, const CmdArgv &argv);
    // Scan key_start key_end limit
    static void Scan(CmdContext &ctx, const CmdArgv &argv);
    // RevScan key_start key_end limit
    static void RevScan(CmdContext &ctx, const CmdArgv &argv);
    // Del key [key ...]
    static void Del(CmdContext &ctx, const CmdArgv &argv);
    static void Exists(CmdContext &ctx, const CmdArgv &argv);
    static void Incr(CmdContext &ctx, const CmdArgv &argv);
    static void Ttl(CmdContext &ctx, const CmdArgv &argv);

    Kv() {
        CmdEntryReg("KvPut", Kv::Put, 3, 7);
        CmdEntryReg("KvMPut", Kv::MultiPut, 3, -1);
        CmdEntryReg("KvGet", Kv::Get, 2, 2);
        CmdEntryReg("KvMGet", Kv::MultiGet, 2, -1);
        CmdEntryReg("KvMeta", Kv::Meta, 2, 2);
        CmdEntryReg("KvScan", Kv::Scan, 4, 4);
        CmdEntryReg("KvRevScan", Kv::RevScan, 4, 4);
        CmdEntryReg("KvDel", Kv::Del, 2, -1);
        CmdEntryReg("KvExists", Kv::Exists, 2, 2);
        CmdEntryReg("KvIncr", Kv::Incr, 3, 3);
        CmdEntryReg("KvTtl", Kv::Ttl, 2, 2);
    }
}; // class Kv

static Kv _kv_init;
static data::iobase dbase(data::NS_KV_DATA);

data::KvWriteOptions Kv::write_options_parse(const CmdArgv &argv) {

    data::KvWriteOptions opts;

    for (int i = 3; i < argv.size(); i++) {
        if (argv[i] == "NX") {
            opts.OpSet(data::KvOpCreate);
        } else if (argv[i] == "PX") {
            i += 1;
            if (i + 1 <= argv.size()) {
                opts.Ttl(str_to_int64(argv[i]));
            }
        }
    }

    return opts;
};

void Kv::Put(CmdContext &ctx, const CmdArgv &argv) {

    data::KvWriteOptions opts = write_options_parse(argv);

    int rs = dbase.kv_put(ctx, argv[1], argv[2], &opts);

    if (rs == -1) {
        ctx.Reply.Error();
    } else if (rs == 0) {
        ctx.Reply.Int(0);
    } else {
        ctx.Reply.OK();
    }
}

void Kv::MultiPut(CmdContext &ctx, const CmdArgv &argv) {
    if (dbase.kv_mput(ctx, argv) == 1) {
        ctx.Reply.OK();
    } else {
        ctx.Reply.Error();
    }
}

void Kv::Get(CmdContext &ctx, const CmdArgv &argv) {
    std::string value = dbase.kv_get(ctx, argv[1]);
    if (value.size() > 0) {
        ctx.Reply.Push(value);
    } else {
        ctx.Reply.NotFound();
    }
}

void Kv::Meta(CmdContext &ctx, const CmdArgv &argv) {
    auto obj = dbase.kv_get_object(ctx, argv[1]);
    if (obj.not_found) {
        return ctx.Reply.NotFound();
    }
    std::string meta_enc = obj.MetaEncode();
    if (meta_enc.size() > 1) {
        ctx.Reply.Push(meta_enc);
    } else {
        ctx.Reply.NotFound();
    }
}

void Kv::MultiGet(CmdContext &ctx, const CmdArgv &argv) {
    for (int i = 1; i < argv.size(); i++) {
        auto value = dbase.kv_get(ctx, argv[i]);
        if (value.size() > 0) {
            ctx.Reply.Push(value);
        } else {
            ctx.Reply.PushNil();
        }
    }
}

void Kv::Scan(CmdContext &ctx, const CmdArgv &argv) {
    dbase.kv_scan(ctx, argv[1], argv[2], str_to_int(argv[3]), true, true, false,
                  1);
}

void Kv::RevScan(CmdContext &ctx, const CmdArgv &argv) {
    dbase.kv_revscan(ctx, argv[1], argv[2], str_to_int(argv[3]), true, true,
                     false, 1);
}

void Kv::Del(CmdContext &ctx, const CmdArgv &argv) {
    ctx.Reply.Int(dbase.kv_del(ctx, argv));
}

void Kv::Exists(CmdContext &ctx, const CmdArgv &argv) {
    std::string value = dbase.kv_get(ctx, argv[1]);
    if (value.size() > 0) {
        ctx.Reply.Int(1);
    } else {
        ctx.Reply.Int(0);
    }
}

void Kv::Incr(CmdContext &ctx, const CmdArgv &argv) {
    dbase.raw_incr(ctx, dbase.ns_keyenc(argv[1]), str_to_int64(argv[2]));
}

void Kv::Ttl(CmdContext &ctx, const CmdArgv &argv) {
    int64_t ttl = dbase.kv_ttl_get(ctx, argv[1]);
    if (ttl > 0) {
        ctx.Reply.Int(ttl);
    } else if (ttl == -1) {
        ctx.Reply.Int(-1);
    } else {
        ctx.Reply.Int(-2);
    }
}

} // namespace lynkstor
