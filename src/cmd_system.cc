// Copyright 2017 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//

#include <string>
#include <vector>
#include <unistd.h>
#include <pthread.h>
#include "cmd.hh"
#include "data_base.hh"
#include "cjet/log.hh"
#include "utils.hh"
#include "types.hh"
#include "status.hh"
#include "version.hh"

namespace lynkstor {

class CmdSystem {
   public:
    static void Command(CmdContext &ctx, const CmdArgv &argv);
    static void Echo(CmdContext &ctx, const CmdArgv &argv);
    static void Ping(CmdContext &ctx, const CmdArgv &argv);
    static void Quit(CmdContext &ctx, const CmdArgv &argv);
    static void Info(CmdContext &ctx, const CmdArgv &argv);
    static void RepList(CmdContext &ctx, const CmdArgv &argv);
    static void RepSet(CmdContext &ctx, const CmdArgv &argv);
    static void RepDel(CmdContext &ctx, const CmdArgv &argv);
    CmdSystem() {
        CmdEntryReg("COMMAND", CmdSystem::Command, 1, 1);
        CmdEntryReg("ECHO", CmdSystem::Echo, 1, 2);
        CmdEntryReg("PING", CmdSystem::Ping, 1, 2);
        CmdEntryReg("QUIT", CmdSystem::Quit, 1, 1);
        CmdEntryReg("INFO", CmdSystem::Info, 1, 1);
        CmdEntryReg("REP_LIST", CmdSystem::RepList, 1, 1);
        CmdEntryReg("REP_SET", CmdSystem::RepSet, 5, 9);
        CmdEntryReg("REP_DEL", CmdSystem::RepDel, 2, 2);
    }
}; // class CmdSystem

static CmdSystem _cmd_system_init;
static data::iobase dbase(data::NS_KV_DATA);

void CmdSystem::Command(CmdContext &ctx, const CmdArgv &argv) {}

void CmdSystem::Echo(CmdContext &ctx, const CmdArgv &argv) {
    ctx.Reply.Inline = "+" + str_trim(argv[1], " \r\n") + "\r\n";
}

void CmdSystem::Ping(CmdContext &ctx, const CmdArgv &argv) {
    if (argv.size() > 1) {
        ctx.Reply.Inline = "+PONG " + str_trim(argv[1], " \r\n") + "\r\n";
    } else {
        ctx.Reply.Inline = "+PONG\r\n";
    }
}

void CmdSystem::Quit(CmdContext &ctx, const CmdArgv &argv) {
    ctx.Reply.Quit = true;
}

void CmdSystem::Info(CmdContext &ctx, const CmdArgv &argv) {

    ctx.Reply.Push(fmt::sprintf("version: %s", LYNKSTOR_VERSION));
    ctx.Reply.Push(fmt::sprintf("uptime: %lld", status::sys_uptime));

    if (lynkstor::status::ConfigReplicationEnable) {
        ctx.Reply.Push("replication: enable");
    } else {
        ctx.Reply.Push("replication: disable");
    }

    ctx.Reply.Push(fmt::sprintf("putlog version range: %lld ~ %lld",
                                status::sys_putlog_ver_min,
                                status::sys_putlog_ver_max));

    std::vector<std::string> ks = {"leveldb.approximate-memory-usage",
                                   "leveldb.stats", /*"leveldb.sstables"*/};

    for (std::string k : ks) {
        std::string v;
        ctx.Stor->GetProperty(k, &v);
        ctx.Reply.Push(k + " {{{\n" + v + "\n}}}\n");
    }
}

int rep_list_handler(CmdContext &ctx, const std::string &key,
                     const std::string &value) {

    TypeKvPairs obj;
    types::kv_pairs_decode(&obj, value);
    if (types::kv_pairs_get(&obj, "id") == "") {
        return -1;
    }

    CmdReply cp;
    cp.Push("id");
    cp.Push(types::kv_pairs_get(&obj, "id"));

    std::vector<std::string> ks = {"host", "port", "auth", "schema"};
    for (std::string k : ks) {
        std::string s = types::kv_pairs_get(&obj, k);
        if (s.size() > 0) {
            cp.Push(k);
            cp.Push(s);
        }
    }

    ctx.Reply.ArrayPush(cp);

    return 0;
}

void CmdSystem::RepList(CmdContext &ctx, const CmdArgv &argv) {

    data::iobase_scan_handler fn = rep_list_handler;

    dbase.raw_scan_handler(ctx, data::ns_exp_base_stats_rep_uplist_cli(""),
                           data::ns_exp_base_stats_rep_uplist_cli(""), 1000, 2,
                           fn);
}

void CmdSystem::RepSet(CmdContext &ctx, const CmdArgv &argv) {

    std::string id = "";
    std::string host = "";
    std::string port = "";
    std::string auth = "";
    std::string scheme = "hrpc";

    for (int i = 1; i < argv.size(); i++) {
        if ((i + 1) < argv.size()) {
            auto value = argv[i + 1];
            if (argv[i] == "id") {
                id = value;
            } else if (argv[i] == "host") {
                host = value;
            } else if (argv[i] == "port") {
                port = value;
            } else if (argv[i] == "auth") {
                auth = value;
            } else if (argv[i] == "scheme") {
                scheme = value;
            }
        }
    }

    if (host.size() > 0) {
        if (host.size() < 7) {
            ctx.Reply.Error("invalid host");
            return;
        }
    }

    if (port.size() > 0) {
        int port_n = std::stoi(port);
        if (port_n < 1 || port_n > 65535) {
            ctx.Reply.Error("invalid port");
            return;
        }
    }

    TypeKvPairs obj;

    if (id.size() > 0) {
        std::string value;
        std::string key = data::ns_exp_base_stats_rep_uplist_cli(id);
        data::Status s = ctx.Stor->Get(key, &value);
        if (!s.ok()) {
            ctx.Reply.Error("id not found");
            return;
        }
        types::kv_pairs_decode(&obj, value);
    } else {
        id = utils::idhash_rand_hex_string(16);
    }

    types::kv_pairs_set(&obj, "id", id);

    if (host.size() > 0) {
        types::kv_pairs_set(&obj, "host", host);
    }

    if (port.size() > 0) {
        types::kv_pairs_set(&obj, "port", port);
    }

    if (auth.size() > 0) {
        types::kv_pairs_set(&obj, "auth", auth);
    }

    if (scheme.size() > 0) {
        types::kv_pairs_set(&obj, "scheme", scheme);
    }

    CJET_LOG(
        "warn",
        "system rep_set : id %s, scheme %s, host %s, port %s, auth ********",
        id.c_str(), scheme.c_str(), host.c_str(), port.c_str());

    data::Status s = ctx.Stor->Put(data::ns_exp_base_stats_rep_uplist_cli(id),
                                   types::kv_pairs_encode(&obj));
    if (!s.ok()) {
        ctx.Reply.Error();
    } else {
        ctx.Reply.OK();
    }
}

void CmdSystem::RepDel(CmdContext &ctx, const CmdArgv &argv) {

    std::string id = argv[1];
    std::string value;
    data::Status s =
        ctx.Stor->Get(data::ns_exp_base_stats_rep_uplist_cli(id), &value);
    if (!s.ok()) {
        ctx.Reply.Error("no id found");
        return;
    }

    CJET_LOG("warn", "system rep_del id:%s", id.c_str());

    s = ctx.Stor->Del(data::ns_exp_base_stats_rep_uplist_cli(id));
    if (!s.ok()) {
        ctx.Reply.Error();
    } else {
        ctx.Reply.OK();
    }
}

} // namespace lynkstor
