// Copyright 2017 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//

#ifndef LYNKSTOR_TYPES_HH
#define LYNKSTOR_TYPES_HH

#include "lynkstor.pb.h"

namespace lynkstor {
namespace types {

static const uint64_t AttrTypeDone = 1 << 16;
static const uint64_t AttrTypeLimitOutRange = 1 << 24;

static inline bool AttrAllow(uint64_t base, uint64_t op) {
    return ((op & base) == op);
}

// refer: https://github.com/lynkdb/iomix/blob/master/skv/result.go

static const uint8_t ResultOK = 1;
static const uint8_t ResultError = 2;
static const uint8_t ResultNotFound = 3;
static const uint8_t ResultBadArgument = 4;
static const uint8_t ResultNoAuth = 5;
static const uint8_t ResultServerError = 6;
static const uint8_t ResultNetError = 7;
static const uint8_t ResultTimeout = 8;
static const uint8_t ResultUnknown = 9;

static inline void kv_pairs_decode(TypeKvPairs* obj, const std::string& v) {
    obj->ParseFromString(v);
};

static inline std::string kv_pairs_encode(const TypeKvPairs* ps) {
    std::string enc;
    ps->SerializeToString(&enc);
    return enc;
}

static inline std::string kv_pairs_get(const TypeKvPairs* ps,
                                       const std::string& key) {
    for (int i = 0; i < ps->items_size(); i++) {
        if (ps->items(i).key() == key) {
            return ps->items(i).value();
        }
    }
    return "";
}

void static inline kv_pairs_set(TypeKvPairs* ps, const std::string& key,
                                const std::string& value) {
    for (int i = 0; i < ps->items_size(); i++) {
        if (ps->mutable_items(i)->key() == key) {
            ps->mutable_items(i)->set_value(value);
            return;
        }
    }

    auto item = ps->add_items();

    item->set_key(key);
    item->set_value(value);
}

} // namespace types
} // namespace lynkstor
#endif
