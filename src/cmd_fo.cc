// Copyright 2017 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//

#include <iostream>
#include <string>
#include <vector>
#include "cmd.hh"
#include "data.hh"
#include "data_base.hh"
#include "utils.hh"
#include "util_crc32.hh"
#include "util_utf8.hh"
#include "cjet/log.hh"
#include "lynkstor.pb.h"
#include "status.hh"

namespace lynkstor {

class FileObject {

   public:
    static void MpInit(CmdContext &ctx, const CmdArgv &argv);
    static void MpPut(CmdContext &ctx, const CmdArgv &argv);
    static void MpGet(CmdContext &ctx, const CmdArgv &argv);
    static void Get(CmdContext &ctx, const CmdArgv &argv);
    static void Scan(CmdContext &ctx, const CmdArgv &argv);
    static void RevScan(CmdContext &ctx, const CmdArgv &argv);

    FileObject() {
        CmdEntryReg("FoMpInit", FileObject::MpInit, 2, 2);
        CmdEntryReg("FoMpPut", FileObject::MpPut, 2, 2);
        CmdEntryReg("FoMpGet", FileObject::MpGet, 2, 2);
        CmdEntryReg("FoGet", FileObject::Get, 2, 2);
        CmdEntryReg("FoScan", FileObject::Scan, 4, 4);
        CmdEntryReg("FoRevScan", FileObject::RevScan, 4, 4);
    }
}; // class FileObject

const int FileObjectPathDirNumMax = 16; // TODO
const int FileObjectPathLenMax = 200;   // TODO

const uint64_t FileObjectEntryAttrVersion1 = 1 << 1;   //
const uint64_t FileObjectEntryAttrIsDir = 1 << 2;      //
const uint64_t FileObjectEntryAttrBlockSize4 = 1 << 4; //
const uint64_t FileObjectEntryAttrCommiting = 1 << 22; //

const uint64_t fileobject_attr_def =
    FileObjectEntryAttrVersion1 | FileObjectEntryAttrBlockSize4; // TODO

const uint64_t FileObjectBlockSize4MB = 4 * 1024 * 1024;
const uint32_t FileObjectBlockNumMax = 1 << 31;

static FileObject _fileobject_init;
static data::iobase db_osm(data::NS_OS_META);
static data::iobase db_osd(data::NS_OS_DATA);

static bool fileobject_path_valid(const std::string &path, bool is_file,
                                  bool is_dir) {
    if (path.size() < 2 || path.size() > FileObjectPathLenMax ||
        path[0] != '/') {
        return false;
    }

    if (int(path.find("//")) >= 0 || int(path.find("/./")) >= 0 ||
        int(path.find("/../")) >= 0) {
        return false; // TODO
    }

    if ((is_dir && is_file) || (is_dir && path[path.size() - 1] == '/') ||
        (is_file && path[path.size() - 1] != '/')) {
        return true;
    }

    if (path.find_first_of('/', 1) + 1 >= path.size()) {
        return false;
    }

    if (!util_utf8::valid(path)) {
        return false;
    }

    return false;
}

static int fileobject_path_dir_num(const std::string &path) {
    int n = 0;
    for (int i = 1; i < path.size(); i++) {
        if (path[i] == '/') {
            n += 1;
        }
    }
    return n;
}

static bool fileobject_attr_allow(uint64_t base, uint64_t v) {
    return ((v & base) == v);
}

static inline void fileobject_value_decode(FileObjectEntryMeta *meta,
                                           const std::string &val) {
    meta->ParseFromString(val);
}

static inline std::string fileobject_meta_encode(FileObjectEntryMeta *meta) {
    std::string enc_data;
    meta->SerializeToString(&enc_data);
    return enc_data;
}

static inline std::string fileobject_block_encode(FileObjectEntryBlock *block) {
    std::string enc_data;
    block->SerializeToString(&enc_data);
    return enc_data;
}

static inline uint16_t fileobject_block_sn(uint64_t time_in_us) {
    uint32_t sec = time_in_us / 4096e6;
    return uint16_t(sec % 65536);
}

static inline int fileobject_meta_path_fold_pos(const std::string &key) {
    int bn = key.find_last_of('/');
    if (bn < 0) {
        bn = 0;
    }
    return bn;
}

static inline std::string fileobject_meta_path_encode(const std::string &path) {

    int bn = path.find_first_of('/', 1);
    int dn = fileobject_path_dir_num(path);
    if (bn < 2 || dn < 1 || dn > FileObjectPathDirNumMax) {
        return "";
    }

    std::string bns = path.substr(1, bn - 1);

    uint32_t bucket_id = util_crc32::ChecksumIEEE(bns.c_str(), bns.size());

    std::string s = uint32_to_bs(bucket_id) + path.substr(bn);

    s[4] = uint8_t(dn);

    return s;
}

static inline std::string fileobject_block_path_encode(uint16_t sn,
                                                       const std::string &path,
                                                       uint32_t block_num) {
    int bn = path.find_first_of('/', 1);
    int dn = fileobject_path_dir_num(path);
    if (bn < 2 || dn < 1 || dn > FileObjectPathDirNumMax) {
        return "";
    }

    std::string bns = path.substr(1, bn - 1);

    uint32_t bucket_id = util_crc32::ChecksumIEEE(bns.c_str(), bns.size());

    std::string s = uint32_to_bs(bucket_id) + uint16_to_bs(sn) +
                    path.substr(bn + 1) + "." + std::to_string(block_num);

    return s;
}

void FileObject::MpInit(CmdContext &ctx, const CmdArgv &argv) {

    lynkstor::FileObjectEntryInit mpInit;

    if (!mpInit.ParseFromString(argv[1])) {
        return ctx.Reply.Error();
    }

    if (mpInit.size() < 1) {
        return ctx.Reply.Error("invalid meta/size");
    }

    if (!fileobject_attr_allow(fileobject_attr_def, mpInit.attrs())) {
        return ctx.Reply.Error("invalid attrs");
    }

    if (!fileobject_path_valid(mpInit.path(), true, false)) {
        return ctx.Reply.Error("invalid path");
    }
    auto meta_key = fileobject_meta_path_encode(mpInit.path());
    auto meta_pre = db_osm.kv_get_object(ctx, meta_key);
    FileObjectEntryMeta os_entry_meta;
    if (!meta_pre.not_found) {
        if (!meta_pre.Valid()) {
            return ctx.Reply.Error("invalid meta");
        }
        fileobject_value_decode(&os_entry_meta, meta_pre.value.substr(1));
        if (os_entry_meta.size() != mpInit.size()) {
            return ctx.Reply.Error("invalid meta/size");
        }
        return ctx.Reply.Push(meta_pre.Encode());
    }

    auto tn = utils::timenow_us();

    //
    data::KvObject meta_obj;
    meta_obj.meta.set_created(tn);
    meta_obj.meta.set_updated(tn);

    //
    os_entry_meta.set_path(mpInit.path());
    os_entry_meta.set_size(mpInit.size());
    os_entry_meta.set_attrs(FileObjectEntryAttrVersion1 |
                            FileObjectEntryAttrBlockSize4 |
                            FileObjectEntryAttrCommiting);
    os_entry_meta.set_sn(fileobject_block_sn(meta_obj.meta.created()));

    auto os_meta_enc = fileobject_meta_encode(&os_entry_meta);

    meta_obj.ValueSet(data::value_ns_protobuf, os_meta_enc);

    auto meta_nskey = db_osm.ns_keyenc(meta_key);
    data::Status s;
    if (lynkstor::status::ConfigReplicationEnable) {

        auto ver = db_osm.db_incr_u64(ctx, data::ns_exp_base_stats_tlog, 1);
        if (ver < 1) {
            return ctx.Reply.Error();
        }
        meta_obj.meta.set_version(ver);

        data::WriteBatch batch;
        batch.Put(db_osm.ns_exp_tlog_enc(ver, meta_nskey), meta_obj.Encode());
        batch.Put(meta_nskey, meta_obj.Encode());

        s = ctx.Stor->Write(data::WriteOptions(), &batch);

    } else {
        s = ctx.Stor->Put(meta_nskey, meta_obj.Encode());
    }

    if (s.ok()) {
        ctx.Reply.Push(meta_obj.Encode());
    } else {
        ctx.Reply.Error();
    }
}

void FileObject::MpPut(CmdContext &ctx, const CmdArgv &argv) {

    FileObjectEntryBlock mpBlock;

    if (!mpBlock.ParseFromString(argv[1])) {
        return ctx.Reply.Error();
    }

    if (mpBlock.size() < 1) {
        return ctx.Reply.Error("invalid size");
    }

    if (!fileobject_attr_allow(fileobject_attr_def, mpBlock.attrs())) {
        return ctx.Reply.Error("invalid attrs");
    }

    if (!fileobject_path_valid(mpBlock.path(), true, false)) {
        return ctx.Reply.Error("invalid path");
    }

    if (mpBlock.num() > FileObjectBlockNumMax) {
        return ctx.Reply.Error("invalid block/num");
    }
    uint64_t num_max = mpBlock.size() / FileObjectBlockSize4MB;
    if (mpBlock.size() % FileObjectBlockSize4MB == 0) {
        num_max -= 1;
    }
    if (mpBlock.num() > num_max) {
        return ctx.Reply.Error("invalid block/num");
    }
    if (mpBlock.num() < num_max) {
        if (mpBlock.data().size() != FileObjectBlockSize4MB) {
            return ctx.Reply.Error("invalid block/size");
        }
    } else {
        if (mpBlock.data().size() !=
            (mpBlock.size() % FileObjectBlockSize4MB)) {
            return ctx.Reply.Error("invalid block/size");
        }
    }

    if (mpBlock.sum() > 0) {
        if (mpBlock.sum() != util_crc32::ChecksumIEEE(mpBlock.data().c_str(),
                                                      mpBlock.data().size())) {
            return ctx.Reply.Error("invalid block/sum-check");
        }
    }

    auto meta_key = fileobject_meta_path_encode(mpBlock.path());

    //
    auto meta_obj = db_osm.kv_get_object(ctx, meta_key);
    if (meta_obj.not_found) {
        return ctx.Reply.Error("un init");
    }
    if (!meta_obj.Valid()) {
        return ctx.Reply.Error("error meta");
    }
    if (meta_obj.meta.created() < 1e6) {
        return ctx.Reply.Error("error meta");
    }
    uint16_t sn = fileobject_block_sn(meta_obj.meta.created());

    //
    FileObjectEntryMeta os_entry_meta;
    fileobject_value_decode(&os_entry_meta, meta_obj.value.substr(1));
    if (os_entry_meta.size() != mpBlock.size()) {
        return ctx.Reply.Error("invalid meta/size");
    }
    if (os_entry_meta.sn() != sn) {
        return ctx.Reply.Error("invalid meta/sn");
    }
    if (os_entry_meta.commit_key() != mpBlock.commit_key()) {
        return ctx.Reply.Error("invalid commit_key");
    }
    if (!fileobject_attr_allow(os_entry_meta.attrs(),
                               FileObjectEntryAttrCommiting)) {
        return ctx.Reply.OK();
    }
    bool blk_ok = false;
    for (int i = 0; i < os_entry_meta.blocks_size(); i++) {
        if (os_entry_meta.blocks(i) == mpBlock.num()) {
            blk_ok = true;
            break;
        }
    }
    if (blk_ok) {
        return ctx.Reply.OK();
    }

    //
    auto tn = utils::timenow_us();
    data::KvObject blk_obj;
    blk_obj.meta.set_created(tn);
    blk_obj.meta.set_updated(tn);

    //
    mpBlock.clear_commit_key();
    auto os_block_enc = fileobject_block_encode(&mpBlock);
    blk_obj.ValueSet(data::value_ns_protobuf, os_block_enc);

    //
    auto blk_key = fileobject_block_path_encode(os_entry_meta.sn(),
                                                mpBlock.path(), mpBlock.num());
    auto blk_nskey = db_osd.ns_keyenc(blk_key);

    //
    data::WriteBatch batch;
    uint64_t ver = 0;

    if (lynkstor::status::ConfigReplicationEnable) {
        ver = db_osd.db_incr_u64(ctx, data::ns_exp_base_stats_tlog, 2);
        if (ver < 2) {
            return ctx.Reply.Error("server error");
        }
        blk_obj.meta.set_version(ver - 1);
        batch.Put(db_osd.ns_exp_tlog_enc(ver - 1, blk_nskey), blk_obj.Encode());
    }
    batch.Put(blk_nskey, blk_obj.Encode());

    //
    os_entry_meta.add_blocks(mpBlock.num());
    if (os_entry_meta.blocks_size() >= num_max + 1) {
        os_entry_meta.clear_blocks();
        os_entry_meta.clear_commit_key();
        os_entry_meta.set_attrs(fileobject_attr_def);
    }
    meta_obj.ValueSet(data::value_ns_protobuf,
                      fileobject_meta_encode(&os_entry_meta));

    //
    auto meta_nskey = db_osm.ns_keyenc(meta_key);
    meta_obj.meta.set_updated(tn);
    if (ver > 1) {
        meta_obj.meta.set_version(ver);
        batch.Put(db_osd.ns_exp_tlog_enc(ver, meta_nskey), meta_obj.Encode());
    }
    batch.Put(meta_nskey, meta_obj.Encode());

    // commit
    auto s = ctx.Stor->Write(data::WriteOptions(), &batch);
    if (!s.ok()) {
        return ctx.Reply.Error();
    }

    ctx.Reply.OK();
}

void FileObject::MpGet(CmdContext &ctx, const CmdArgv &argv) {

    FileObjectEntryBlock mpBlock;

    if (!mpBlock.ParseFromString(argv[1])) {
        return ctx.Reply.Error("invalid data");
    }

    if (!fileobject_path_valid(mpBlock.path(), true, false)) {
        return ctx.Reply.Error("invalid path");
    }

    if (mpBlock.num() > FileObjectBlockNumMax) {
        return ctx.Reply.Error("invalid block/num");
    }

    std::string blk_key = fileobject_block_path_encode(
        mpBlock.sn(), mpBlock.path(), mpBlock.num());

    std::string value;
    data::Status s = ctx.Stor->Get(db_osd.ns_keyenc(blk_key), &value);
    if (!s.ok() || value.size() < 2) {
        return ctx.Reply.Error("object not found");
    }

    ctx.Reply.Push(value);
}

void FileObject::Get(CmdContext &ctx, const CmdArgv &argv) {

    if (!fileobject_path_valid(argv[1], true, false)) {
        return ctx.Reply.Error("invalid path");
    }

    auto meta_key = fileobject_meta_path_encode(argv[1]);
    std::string value;
    data::Status s = ctx.Stor->Get(db_osm.ns_keyenc(meta_key), &value);
    if (!s.ok() || value.size() < 2) {
        return ctx.Reply.Error("object not found");
    }

    ctx.Reply.Push(value);
}

void FileObject::Scan(CmdContext &ctx, const CmdArgv &argv) {

    if (!fileobject_path_valid(argv[1], true, true) ||
        !fileobject_path_valid(argv[2], true, true)) {
        return ctx.Reply.Error();
    }

    auto k1 = fileobject_meta_path_encode(argv[1]);
    auto k2 = fileobject_meta_path_encode(argv[2]);

    if (k1[0] != k2[0]) {
        return ctx.Reply.Error();
    }

    int pn = k1.find_last_of('/') + 1;
    if (pn < 6) {
        pn = 6;
    }

    db_osm.kv_scan(ctx, k1, k2, str_to_int(argv[3]), true, false, true, pn);
}

void FileObject::RevScan(CmdContext &ctx, const CmdArgv &argv) {

    if (!fileobject_path_valid(argv[1], true, true) ||
        !fileobject_path_valid(argv[2], true, true)) {
        return ctx.Reply.Error();
    }

    auto k1 = fileobject_meta_path_encode(argv[1]);
    auto k2 = fileobject_meta_path_encode(argv[2]);

    if (k1[0] != k2[0]) {
        return ctx.Reply.Error();
    }
    int pn = k1.find_last_of('/') + 1;
    if (pn < 6) {
        pn = 6;
    }

    db_osm.kv_revscan(ctx, k1, k2, str_to_int(argv[3]), true, false, true, pn);
}

} // namespace lynkstor
