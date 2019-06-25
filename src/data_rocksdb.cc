// Copyright 2017 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//

#include <iostream>
#include <string>
#include "fmt/format.h"

#include "rocksdb/options.h"
#include "rocksdb/iterator.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"

#include "cjet/flag.hh"
#include "cjet/log.hh"
#include "data.hh"
#include "data_base.hh"
#include "data_rocksdb.hh"
#include "config.hh"

namespace lynkstor {
namespace data {

//
int EngineRocksDB::Init(const std::string &name) {

    rocksdb::Options options;

    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();

    options.create_if_missing = true;

    std::string datadir = cjet::flag::Value("data_dir");
    if (datadir == "") {
        datadir = Config::Value("server/data_dir");
        if (datadir == "") {
            CJET_LOG("error", "no data_dir set");
            return err;
        }
    }
    if (datadir[datadir.size() - 1] != '/') {
        datadir += "/";
    }
    datadir += fmt::sprintf("%s_rocksdb_%d.%d", name.c_str(), 0, 0);

    //
    if (name != "meta" && Config::Value("rocksdb/compression") == "snappy") {
        // options.compression = rocksdb::kSnappyCompression;
    } else {
        // options.compression = rocksdb::kNoCompression;
    }

    //
    int v = Config::ValueSize("rocksdb/write_buffer_size", 4, 64);
    if (name == "meta") {
        v = 4;
    }
    options.db_write_buffer_size = v * ByteSize1MB; // default 4MB

    //
    rocksdb::Status s = rocksdb::DB::Open(options, datadir, &ldb);
    if (!s.ok()) {
        CJET_LOG("error", "db init error : %s", s.ToString().c_str());
        return err;
    }

    Status s2 = Put("__db_init_check__", "__value__");
    if (!s2.ok()) {
        CJET_LOG("error", "db init error : %s", s2.ToString().c_str());
        return err;
    }
    Del("__db_init_check__");

    CJET_LOG("info", "db init %s ok", datadir.c_str());

    return ok;
}

Status EngineRocksDB::Put(const std::string &key, const std::string &value) {
    if (ldb == NULL) {
        return Status::Corruption("un-init");
    }

    rocksdb::Status s = ldb->Put(rocksdb::WriteOptions(), key, value);
    if (!s.ok()) {
        return Status::Corruption(s.ToString());
    }
    return Status::OK();
}

Status EngineRocksDB::Get(const std::string &key, std::string *value) {
    if (ldb == NULL) {
        return Status::Corruption("un-init");
    }

    rocksdb::Status s = ldb->Get(rocksdb::ReadOptions(), key, value);
    if (!s.ok()) {
        return Status::Corruption(s.ToString());
    }
    return Status::OK();
}

Status EngineRocksDB::Del(const std::string &key) {
    if (ldb == NULL) {
        return Status::Corruption("un-init");
    }

    rocksdb::Status s = ldb->Delete(rocksdb::WriteOptions(), key);
    if (!s.ok()) {
        return Status::Corruption(s.ToString());
    }
    return Status::OK();
}

Status EngineRocksDB::Write(const WriteOptions &opts, WriteBatch *updates) {
    if (ldb == NULL) {
        return Status::Corruption("un-init");
    }

    if (updates->args_.size() < 1) {
        return Status::OK();
    }

    rocksdb::WriteBatch batch;

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

    rocksdb::Status s = ldb->Write(rocksdb::WriteOptions(), &batch);
    // ldb->Write(rocksdb::WriteOptions(), (rocksdb::WriteBatch *)updates);
    if (!s.ok()) {
        return Status::Corruption(s.ToString());
    }
    return Status::OK();
}

EngineIterator *EngineRocksDB::NewIterator() {
    if (ldb != NULL) {
        rocksdb::Iterator *it = ldb->NewIterator(rocksdb::ReadOptions());
        if (it != NULL) {
            return new RocksDBIterator(it);
        }
    }
    return NULL;
}

bool EngineRocksDB::GetProperty(const std::string key, std::string *value) {
    if (ldb == NULL) {
        return false;
    }
    return ldb->GetProperty(key, value);
};

} // namespace data
} // namespace lynkstor
