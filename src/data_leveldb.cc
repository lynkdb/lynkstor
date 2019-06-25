// Copyright 2017 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//

#include <iostream>
#include <string>
#include "fmt/format.h"

#include "leveldb/options.h"
#include "leveldb/iterator.h"
#include "leveldb/cache.h"
#include "leveldb/filter_policy.h"

#include "cjet/flag.hh"
#include "cjet/log.hh"
#include "cjet/conf.hh"
#include "data.hh"
#include "data_base.hh"
#include "data_leveldb.hh"

namespace lynkstor {
namespace data {

//
int LevelDBEngine::Init(const std::string &name) {

    leveldb::Options options;
    options.create_if_missing = true;

    std::string datadir = cjet::flag::Value("data_dir");
    if (datadir == "") {
        datadir = cjet::conf::Config::Value("server/data_dir");
        if (datadir == "") {
            CJET_LOG("error", "no data_dir setting found");
            return err;
        }
    }
    if (datadir[datadir.size() - 1] != '/') {
        datadir += "/";
    }
    datadir += fmt::sprintf("%s_leveldb_%d.%d", name.c_str(), 0, 0);

    //
    size_t v = cjet::conf::Config::ValueSize("leveldb/block_cache", 8, 1024);
    if (name == "meta") {
        v = v / 8;
        if (v < 8) {
            v = 8;
        }
    }
    options.block_cache = leveldb::NewLRUCache(v * ByteSize1MB);

    //
    options.filter_policy = leveldb::NewBloomFilterPolicy(10);

    //
    if (name != "meta" &&
        cjet::conf::Config::Value("leveldb/compression") == "snappy") {
        options.compression = leveldb::kSnappyCompression;
    } else {
        options.compression = leveldb::kNoCompression;
    }

    //
    if (name == "meta") {
        options.max_file_size = 2 * ByteSize1MB; // default 2MB
    } else {
        v = cjet::conf::Config::ValueSize("leveldb/max_file_size", 2, 32);
        options.max_file_size = v * ByteSize1MB; // default 2MB
    }

    //
    v = cjet::conf::Config::ValueSize("leveldb/write_buffer_size", 4, 64);
    if (name == "meta") {
        v = 4;
    }
    options.write_buffer_size = v * ByteSize1MB; // default 4MB

    //
    v = cjet::conf::Config::ValueSize("leveldb/max_open_files", 200, 2000);
    options.max_open_files = v; // default 1000

    //
    options.block_size = 8 * ByteSize1KB;

    //
    data::Status s = leveldb::DB::Open(options, datadir, &ldb);
    if (!s.ok()) {
        CJET_LOG("error", "db init error : %s", s.ToString().c_str());
        return err;
    }
    CJET_LOG("info", "db init %s ok", datadir.c_str());

    return ok;
}

Status LevelDBEngine::Put(const std::string &key, const std::string &value) {
    if (ldb == NULL) {
        return Status::Corruption("un-init");
    }

    return ldb->Put(leveldb::WriteOptions(), key, value);
}

Status LevelDBEngine::Get(const std::string &key, std::string *value) {
    if (ldb == NULL) {
        return Status::Corruption("un-init");
    }

    return ldb->Get(leveldb::ReadOptions(), key, value);
}

Status LevelDBEngine::Del(const std::string &key) {
    if (ldb == NULL) {
        return Status::Corruption("un-init");
    }

    return ldb->Delete(leveldb::WriteOptions(), key);
}

Status LevelDBEngine::Write(const WriteOptions &opts, WriteBatch *updates) {
    if (ldb == NULL) {
        return Status::Corruption("un-init");
    }

    if (updates->args_.size() < 1) {
        return Status::OK();
    }

    leveldb::WriteBatch batch;

    for (int i = 0; i < updates->args_.size(); i++) {
        switch (updates->args_[i].action_) {
            case 1:
                batch.Put(updates->args_[i].key_, updates->args_[i].value_);
                break;
            case 2:
                batch.Delete(updates->args_[i].key_);
                break;
        }
    }

    return ldb->Write(opts, &batch);
}

EngineIterator *LevelDBEngine::NewIterator() {
    if (ldb != NULL) {
        leveldb::Iterator *it = ldb->NewIterator(leveldb::ReadOptions());
        if (it != NULL) {
            return new LevelDBIterator(it);
        }
    }
    return NULL;
}

bool LevelDBEngine::GetProperty(const std::string key, std::string *value) {
    if (ldb == NULL) {
        return false;
    }
    return ldb->GetProperty(key, value);
};

} // namespace data
} // namespace lynkstor
