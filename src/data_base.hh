// Copyright 2017 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//

#ifndef LYNKSTOR_DATA_BASE_HH
#define LYNKSTOR_DATA_BASE_HH

#include <string>
#include "cmd.hh"
#include "utils.hh"
#include "util_crc32.hh"
#include "lynkstor.pb.h"

namespace lynkstor {
namespace data {

const char NS_BASE_TTL = 18;  // <ttl:8> <ns:1> <element...:n>
const char NS_KV_DATA = 32;   // <ns:1> <key:n>
const char NS_PROG_DATA = 36; // <ns:1> <n> <size:key-1> <size:key-n>
const char NS_OS_META = 42;   // <ns:1> <n> <size:key-1> <size:key-n>
const char NS_OS_DATA = 43;   // <ns:1> <b4> <b4> <key>

const char NS_REP_SYNC_OFF = NS_KV_DATA;
const char NS_REP_SYNC_CUT = NS_OS_DATA;

const char NS_EXP_BASE_STATS = 48;                     // <key>
const char NS_EXP_BASE_STATS_TLOG = 36;                // <key>
const char NS_EXP_BASE_STATS_REP_UPLIST_CLI = 37;      // <key>
const char NS_EXP_BASE_STATS_REP_PULL_OFFSET_CLI = 38; // <key>
const char NS_EXP_BASE_STATS_REP_FULL_OFFSET_CLI = 39; // <key>
const char NS_EXP_BASE_TLOG_DATA = 52;                 // <1> <uint64> <key>

const char kvobj_t_v1 = 0x01;
const char value_ns_bytes = 0x00;
// const char value_ns_uint = 1;
// const char value_ns_nint = 2;
const char value_ns_json = 20;
const char value_ns_protobuf = 21;

const uint64_t KvOpMetaSum = 1 << 1;
const uint64_t KvOpMetaSize = 1 << 2;
const uint64_t KvOpCreate = 1 << 13;
const uint64_t KvOpForce = 1 << 14;
const uint64_t KvOpFoldMeta = 1 << 15;
const uint64_t KvOpLogEnable = 1 << 16;

const uint64_t ttl_ms_min = 1e3;
const uint64_t ttl_ms_max = 365 * 86400 * 1e3;
const uint64_t ttlat_ms_min = 15e11;
const uint64_t ttlat_ms_max = 40e11;

const uint8_t KvMetaTypeDelete = 5;

const int ScanStatusLimitOutRange = 2;
const int ByteSize1KB = 1024;
const int ByteSize1MB = 1024 * 1024;
const int ByteSize8MB = 8 * 1024 * 1024;

static uint64_t ttl_to_ttlat(int64_t ttl) {
    if (ttl < ttl_ms_min) {
        return 0;
    }
    if (ttl > ttl_ms_max) {
        ttl = ttl_ms_max;
    }
    return ttl + utils::timenow_ms();
}

const std::string ns_exp_base_stats_tlog({NS_EXP_BASE_STATS,
                                          NS_EXP_BASE_STATS_TLOG});
const std::string ns_exp_base_stats_rep_pull_offset_cli(
    {NS_EXP_BASE_STATS, NS_EXP_BASE_STATS_REP_PULL_OFFSET_CLI});
const std::string ns_exp_base_stats_rep_full_offset_cli(
    {NS_EXP_BASE_STATS, NS_EXP_BASE_STATS_REP_FULL_OFFSET_CLI});

static inline std::string ns_exp_base_stats_rep_uplist_cli(std::string up_id) {
    std::string s({NS_EXP_BASE_STATS, NS_EXP_BASE_STATS_REP_UPLIST_CLI});
    if (up_id.size() > 0) {
        s.append(up_id);
    }
    return s;
}

typedef struct KvWriteOptions {
    int64_t ttl;
    uint64_t ttlat;
    uint64_t actions;
    KvWriteOptions() : ttl(0), ttlat(0), actions(0) {};
    void OpSet(uint64_t v) {
        actions = actions | v;
    };
    bool OpAllow(uint64_t v) {
        return ((v & actions) == v);
    };
    void Ttl(int64_t ttl) {
        ttlat = ttl_to_ttlat(ttl);
    };
} KvWriteOptions;

static inline std::string base_meta_v1_encode(KvMeta *meta) {
    std::string enc;
    enc.append(1, uint8_t(0));

    //
    if (meta->type() > 0 || meta->version() > 0 || meta->expired() > 0 ||
        meta->created() > 0 || meta->updated() > 0 || meta->size() > 0 ||
        meta->sum() > 0 || meta->num() > 0) {
        std::string enc_data;
        meta->SerializeToString(&enc_data);
        if (enc_data.size() > 0 && enc_data.size() < 64) {
            enc[0] = uint8_t(enc_data.size());
            enc.append(enc_data);
        }
    }

    return enc;
}

static inline void base_meta_v1_encode_inner(KvMeta *meta, std::string *enc) {

    //
    if (meta->type() > 0 || meta->version() > 0 || meta->expired() > 0 ||
        meta->created() > 0 || meta->updated() > 0 || meta->size() > 0 ||
        meta->sum() > 0 || meta->num() > 0) {
        std::string enc_data;
        meta->SerializeToString(&enc_data);
        if (enc_data.size() > 0 && enc_data.size() < 64) {
            enc->at(1) = uint8_t(enc_data.size());
            enc->append(enc_data);
        }
    }
}

static inline void base_meta_v1_decode(KvMeta *meta, const std::string &val) {
    meta->ParseFromString(val);
}

static inline void base_meta_decode(KvMeta *meta, const std::string &val) {
    if (val.size() < 2) {
        return;
    }
    switch (val[0]) {
        case kvobj_t_v1:
            int msize = val[1];
            if (msize > 0 && msize + 2 <= val.size()) {
                base_meta_v1_decode(meta, val.substr(2, msize));
            }
            break;
    }
}

static inline std::string KvObjectDecodeValueBody(const std::string &val) {
    if (val.size() < 2) {
        return "";
    }
    switch (val[0]) {
        case kvobj_t_v1:
            int msize = val[1];
            if (msize + 2 < val.size()) {
                return val.substr(msize + 2);
            }
            break;
    }
    return "";
}

typedef struct KvObject {
    KvMeta meta;
    std::string meta_enc;
    std::string enc;
    std::string value;
    bool not_found = false;
    KvObject() {};
    KvObject(const std::string &val, const KvWriteOptions *opts) {
        value = val;
        if (opts != NULL && opts->ttlat > 0) {
            meta.set_expired(opts->ttlat);
        }
    };
    KvObject(const std::string &val) {
        if (val.size() < 2) {
            return;
        }
        enc = val;

        switch (val[0]) {
            case kvobj_t_v1:
                int msize = val[1];
                if (msize > 0 && msize + 2 <= val.size()) {
                    base_meta_v1_decode(&meta, val.substr(2, msize));
                }
                if (msize + 2 < val.size()) {
                    value = val.substr(msize + 2);
                }
                break;
        }
    };
    bool MetaValid() {
        if (value[0] != value_ns_bytes && value[0] != value_ns_json &&
            value[0] != value_ns_protobuf) {
            return false;
        }
        return true;
    };
    bool Valid() {
        if (!MetaValid()) {
            return false;
        }
        if (value.size() < 1) {
            return false;
        }
        return true;
    };
    std::string Encode() {
        // if (enc.size() >= 2) {
        //     return enc;
        // }

        // p1
        std::string enc({kvobj_t_v1, 0x00}); //.append(1, kvobj_t_v1);

        // p1.5
        if (value.size() > 1) {
            if (meta.size() > 0) {
                meta.set_size(value.size() - 1);
            } else {
                meta.set_size(0);
            }
            if (meta.sum() > 0) {
                meta.set_sum(util_crc32::ChecksumIEEE(value.substr(1).c_str(),
                                                      value.size() - 1));
            }
        }

        // p2
        base_meta_v1_encode_inner(&meta, &enc);

        // p3
        enc.append(value);

        return enc;
    };
    std::string MetaEncode() {
        std::string menc;
        menc.append(1, kvobj_t_v1);
        if (meta_enc.size() == 0) {
            meta_enc = base_meta_v1_encode(&meta);
        }
        menc.append(meta_enc);
        return menc;
    };
    void ValueSet(const char ns_prefix, const std::string &bs) {
        enc.clear();
        value.clear();
        value.append(1, ns_prefix);
        value.append(bs);
    };
    uint64_t ValueSize() {
        if (value.size() > 0) {
            return value.size() - 1;
        }
        return 0;
    };
    uint32_t ValueCrc32() {
        if (value.size() > 1) {
            return util_crc32::ChecksumIEEE(value.substr(1).c_str(),
                                            value.size() - 1);
        }
        return 0;
    };
} KvObject;

//

/*
typedef struct ProgKeyEntry {
    uint32_t Type;
    std::string Data;
    ProgKeyEntry(uint32_t t, std::string v) {
        Type = t;
        Data = v;
    };
} ProgKeyEntry;

typedef struct ProgKey {
    uint32_t Type;
    std::vector<ProgKeyEntry *> Items;
    void Append(std::string v) {
        Items.push_back(new ProgKeyEntry(ProgKeyEntryBytes, v));
    };
    void Append(uint8_t v) {
        Items.push_back(new ProgKeyEntry(ProgKeyEntryUint, uint8_to_bs(v)));
    };
    void Append(uint16_t v) {
        Items.push_back(new ProgKeyEntry(ProgKeyEntryUint, uint16_to_bs(v)));
    };
    void Append(uint32_t v) {
        Items.push_back(new ProgKeyEntry(ProgKeyEntryUint, uint32_to_bs(v)));
    };
    void Append(uint64_t v) {
        Items.push_back(new ProgKeyEntry(ProgKeyEntryUint, uint64_to_bs(v)));
    };
    std::string Encode(const char ns) {
        if (Items.size() == 0) {
            return "";
        }
        std::string enc;
        enc.append(1, ns);
        enc.append(1, uint8_t(Items.size()));
        for (int i = 0; i < Items.size(); i++) {

            if ((i + 1) < Items.size()) {

                if (Items[i]->Data.size() > 0) {
                    enc.append(1, uint8_t(Items[i]->Data.size()));
                    enc.append(Items[i]->Data);
                } else {
                    enc.append(1, uint8_t(0));
                }

            } else if (Items[i]->Data.size() > 0) {
                enc.append(Items[i]->Data);
            }
        }
        return enc;
    };
} ProgKey;

typedef struct ProgWriteOptions {
    int64_t Expired; // nano
    uint32_t PrevSum;
    uint64_t PrevVersion;
    uint64_t Version;
    uint64_t Actions;
    ProgWriteOptions() {
        Expired = 0;
    };
    void OpSet(uint64_t v) {
        Actions = Actions | v;
    };
    bool OpAllow(uint64_t v) {
        return ((v & Actions) == v);
    };
    uint64_t ExpiredUnixNano() {
        if (Expired > prog_ttl_min) {
            return Expired;
        }
        return 0;
    };
} ProgWriteOptions;

typedef struct Result {
    int err;
    int64_t rs_int;
    std::string rs_str;
} Result;
*/

typedef struct ValueBytes {
    std::string data;
    ValueBytes() : data("") {};
    ValueBytes(std::string value) {
        data = value;
    };
    bool Valid() {
        if (data.size() < 1) {
            return false;
        }
        if (data[0] != value_ns_bytes && data[0] != value_ns_json &&
            data[0] != value_ns_protobuf) {
            return false;
        }
        return true;
    };
    std::string String() {
        if (data.size() > 1) {
            return data.substr(1);
        }
        return "";
    };
    uint64_t Uint64() {
        if (data.size() > 1) {
            return str_to_uint64(data.substr(1));
        }
        return 0;
    };
    uint32_t Uint32() {
        return uint32_t(Uint64());
    };
    uint16_t Uint16() {
        return uint16_t(Uint64());
    };
    uint8_t Uint8() {
        return uint8_t(Uint64());
    };
    int64_t Int64() {
        if (data.size() > 1) {
            return str_to_int64(data.substr(1));
        }
        return 0;
    };
    int32_t Int32() {
        return int32_t(Int64());
    };
    int16_t Int16() {
        return int16_t(Int64());
    };
    int8_t Int8() {
        return int8_t(Int64());
    };
    double Float64() {
        if (data.size() > 1) {
            return str_to_float64(data.substr(1));
        }
        return 0;
    };
    float Float32() {
        if (data.size() > 1) {
            return str_to_float(data.substr(1));
        }
        return 0;
    };
    bool Bool() {
        auto v = String();
        if (v.size() > 0 && (v == "1" || v == "true" || v == "yes")) {
            return true;
        }
        return false;
    };
} ValueBytes;

static inline std::string ValueBytesEncode(std::string value) {
    return value_ns_bytes + value;
}

typedef int (*iobase_scan_handler)(CmdContext &ctx, const std::string &key,
                                   const std::string &value);

static inline std::string iobase_ns_ttl_enc(uint64_t ttl,
                                            const std::string &key) {
    std::string s;
    s.append(1, NS_BASE_TTL);
    s.append(uint64_to_bs(ttl));
    s.append(key);
    return s;
}

//
class iobase {
   private:
    char ns_data_this = NS_KV_DATA;
    const int kv_scan_limit_max = 100000;
    const int kv_scan_limit_size_min = 2 * 1024 * 1024;
    const int kv_scan_limit_size_max = 8 * 1024 * 1024;
    static const KvWriteOptions *kv_wodef;

   public:
    inline std::string ns_ttl_enc(uint64_t ttl, const std::string &key) {
        return iobase_ns_ttl_enc(ttl, key);
    }
    inline std::string ns_exp_tlog_enc(uint64_t ver, const std::string &key) {
        std::string s;
        s.append(1, NS_EXP_BASE_TLOG_DATA);
        s.append(uint64_to_bs(ver));
        if (key.size() > 0) {
            s.append(key);
        }
        return s;
    }
    inline std::string ns_keyenc(const std::string &key) {
        std::string s;
        s.append(1, ns_data_this);
        if (key.size() > 0) {
            s.append(key);
        }
        return s;
    }
    iobase(char ns_kv) {
        ns_data_this = ns_kv;
    };

    //
    uint64_t db_incr_u64(CmdContext &ctx, const std::string &key,
                         int64_t value);
    uint64_t ng_incr_u64(Engine *ctx, const std::string &key, int64_t value);
    void db_scan_handler(CmdRpcContext &ctx, const std::string &off,
                         const std::string &cut, int limit, int limit_size,
                         int prefix_len);

    // meta
    int64_t meta_ttl_get(CmdContext &ctx, std::string key);

    // the value at key after the increment operation.
    void raw_incr(CmdContext &ctx, const std::string &key, int64_t value);
    void raw_incrf(CmdContext &ctx, const std::string &key, float value);
    void raw_scan(CmdContext &ctx, const std::string &off,
                  const std::string &cut, int limit, bool rk, bool rv,
                  bool ex_skip, bool meta_skip, int prefix_len);
    int raw_scan_handler(CmdContext &ctx, const std::string &off,
                         const std::string &cut, int limit, int prefix_len,
                         iobase_scan_handler fn);

    //
    // kv_* Key/Value data APIs

    // 1 if the key was set
    // 0 if the key was not set
    int kv_new(CmdContext &ctx, std::string key, std::string value,
               KvWriteOptions *opts);

    // 1 if the all the keys were set.
    // 0 if no key was set (at least one key already existed).
    // -1 error
    int kv_mnew(CmdContext &ctx, const CmdArgv &argv);

    // 1 if the put was executed correctly
    // -1 error
    int kv_put(CmdContext &ctx, std::string key, std::string value,
               KvWriteOptions *opts);

    //  1 if all the keys were set.
    // -1 error
    int kv_mput(CmdContext &ctx, const CmdArgv &argv);

    // n  timeout in seconds.
    // -2 if the key does not exist.
    // -1 if the key exists but has no associated expire.
    int64_t kv_ttl_get(CmdContext &ctx, std::string key);

    // 0~n the number of keys that were removed
    int kv_del(CmdContext &ctx, const CmdArgv &argv);

    // list of values at the specified keys.
    std::string kv_get(CmdContext &ctx, std::string key);
    // std::string kv_meta(CmdContext &ctx, std::string key);
    KvObject kv_get_object(CmdContext &ctx, std::string key);

    // array of elements may contain two elements, a key (rk = true)
    //  and a value (rv = true).
    void kv_scan(CmdContext &ctx, const std::string &off,
                 const std::string &cut, int limit, bool rk, bool rv, bool rvf,
                 int prefix_len);
    void kv_revscan(CmdContext &ctx, const std::string &off,
                    const std::string &cut, int limit, bool rk, bool rv,
                    bool rvf, int prefix_len);
};

} // namespace data
} // namespace lynkstor

#endif
