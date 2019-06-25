// Copyright 2017 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//

#ifndef LYNKSTOR_DATA_HH
#define LYNKSTOR_DATA_HH

#include <string>
#include <iostream>
#include <vector>

#include "leveldb/db.h"
#include "leveldb/iterator.h"
#include "leveldb/write_batch.h"

namespace lynkstor {
namespace data {

typedef leveldb::Slice Slice;
// typedef leveldb::WriteBatch WriteBatch;
typedef leveldb::WriteOptions WriteOptions;
typedef leveldb::Status Status;

struct WriteBatchEntry {
   public:
    int action_;
    std::string key_;
    std::string value_;
    WriteBatchEntry(int action, const std::string &key,
                    const std::string &value) {
        action_ = action;
        key_ = key;
        value_ = value;
    };
    WriteBatchEntry(int action, const std::string &key) {
        action_ = action;
        key_ = key;
    };
};

class WriteBatch {
   public:
    std::vector<WriteBatchEntry> args_;
    WriteBatch() {
        args_.clear();
    };
    ~WriteBatch() {
        args_.clear();
    };
    void Put(const std::string &key, const std::string &value) {
        // std::cout << __FILE__ << " " << __LINE__ << " \n";
        args_.push_back(WriteBatchEntry(1, key, value));
        // std::cout << __FILE__ << " " << __LINE__ << " \n";
    };
    void Delete(const std::string &key) {
        args_.push_back(WriteBatchEntry(2, key));
    };
    void Clear() {
        args_.clear();
    };
};

class EngineIterator {
   public:
    virtual ~EngineIterator() {};
    virtual void Seek(const Slice &key) = 0;
    virtual void SeekToFirst() = 0;
    virtual void SeekToLast() = 0;
    virtual bool Valid() const = 0;
    virtual void Next() = 0;
    virtual void Prev() = 0;
    virtual Slice key() const = 0;
    virtual Slice value() const = 0;
}; // class EngineIterator

class Engine {
   public:
    static const int ok = 0;
    static const int err = -1;
    static const int err_uninit = -2;
    virtual int Init(const std::string &name) = 0;
    virtual Status Put(const std::string &key, const std::string &value) = 0;
    virtual Status Get(const std::string &key, std::string *value) = 0;
    virtual Status Del(const std::string &key) = 0;
    virtual Status Write(const WriteOptions &opts, WriteBatch *updates) = 0;
    virtual EngineIterator *NewIterator() = 0;
    virtual bool GetProperty(std::string key, std::string *value) = 0;
};

extern Engine *NewEngine(const std::string &engine_name,
                         const std::string &data_name);

// typedef Status (*TtoHandler)(WriteBatch &wb, const std::string key);

} // namespace data
} // namespace lynkstor

#endif
