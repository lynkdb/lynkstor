// Copyright 2017 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//

#include "data.hh"
#include "data_leveldb.hh"

#ifdef LYNKSTOR_ENGINE_ROCKSDB
#include "data_rocksdb.hh"
#endif

namespace lynkstor {
namespace data {

Engine *NewEngine(const std::string &engine_name,
                  const std::string &data_name) {
    Engine *engine = NULL;

    if (engine_name == "leveldb") {
        engine = new LevelDBEngine();
        if (engine->Init(data_name) == 0) {
            return engine;
        }
    }
#ifdef LYNKSTOR_ENGINE_ROCKSDB
    else if (engine_name == "rocksdb") {
        engine = new EngineRocksDB();
        if (engine->Init(data_name) == 0) {
            return engine;
        }
    }
#endif

    return NULL;
}

} // namespace data
} // namespace lynkstor
