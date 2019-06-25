// Copyright 2017 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.

#ifndef LYNKSTOR_DATA_LEVELDB_HH
#define LYNKSTOR_DATA_LEVELDB_HH

#include <string>
#include "data.hh"
#include "leveldb/options.h"
#include "leveldb/iterator.h"

namespace lynkstor {
namespace data {

class LevelDBIterator : public EngineIterator {
   private:
    leveldb::Iterator *m_it;

   public:
    LevelDBIterator(leveldb::Iterator *it) {
        m_it = it;
    };
    ~LevelDBIterator() {
        if (m_it != NULL) {
            delete m_it;
        }
    };
    void Seek(const Slice &key) {
        m_it->Seek(key);
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
        return m_it->key();
    };
    Slice value() const {
        return m_it->value();
    };
}; // class LevelDBIterator

class LevelDBEngine : public Engine {
   private:
    leveldb::DB *ldb;

   public:
    LevelDBEngine() : ldb(NULL) {};
    ~LevelDBEngine() {
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
