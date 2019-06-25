// Copyright 2017 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.

#ifndef LYNKSTOR_DATA_ROCKSDB_HH
#define LYNKSTOR_DATA_ROCKSDB_HH

#include <string>
#include "data.hh"
#include "rocksdb/db.h"
#include "rocksdb/iterator.h"

namespace lynkstor {
namespace data {

class RocksDBIterator : public EngineIterator {
   private:
    rocksdb::Iterator *m_it;

   public:
    RocksDBIterator(rocksdb::Iterator *it) {
        m_it = it;
    };
    ~RocksDBIterator() {
        if (m_it != NULL) {
            delete m_it;
        }
    };
    void Seek(const Slice &key) {
        m_it->Seek(key.ToString());
    };
    void SeekToFirst() {
        m_it->SeekToFirst();
    };
    void SeekToLast() {
        m_it->SeekToLast();
    };
    bool Valid() const {
        return m_it->Valid();
    };
    void Next() {
        m_it->Next();
    };
    void Prev() {
        m_it->Prev();
    };
    Slice key() const {
        return m_it->key().ToString();
    };
    Slice value() const {
        return m_it->value().ToString();
    };
}; // class RocksDBIterator

class EngineRocksDB : public Engine {
   private:
    rocksdb::DB *ldb;

   public:
    EngineRocksDB() : ldb(NULL) {};
    ~EngineRocksDB() {
        if (ldb != NULL) {
            delete ldb;
        }
    };
    int Init(const std::string &name);
    Status Put(const std::string &key, const std::string &value);
    Status Get(const std::string &key, std::string *value);
    Status Del(const std::string &key);
    Status Write(const WriteOptions &opts, WriteBatch *updates);
    EngineIterator *NewIterator();
    bool GetProperty(const std::string key, std::string *value);
};

} // namespace data
} // namespace lynkstor

#endif
