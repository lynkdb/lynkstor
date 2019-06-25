// Copyright 2017 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//

#ifndef LYNKSTOR_CLIENT_HH
#define LYNKSTOR_CLIENT_HH

#include <string>
#include <vector>
#include "../utils.hh"
#include "../data_base.hh"

namespace lynkstor {
namespace client {

// refer: https://github.com/lynkdb/iomix/blob/master/skv/result.go
const uint8_t ResultOK = 1;
const uint8_t ResultError = 2;
const uint8_t ResultNotFound = 3;
const uint8_t ResultBadArgument = 4;
const uint8_t ResultNoAuth = 5;
const uint8_t ResultServerError = 6;
const uint8_t ResultNetError = 7;
const uint8_t ResultTimeout = 8;
const uint8_t ResultUnknown = 9;

const uint8_t kvobj_t_v1 = 0x01;
const uint8_t value_ns_bytes = 0x00;
const uint8_t value_ns_json = 20;

const int bufio_size = 4096;
const int bufio_read_size = 4095;

const std::string delim = "\r\n";

typedef struct ResultKvEntry {
    std::string key;
    std::string value;
    ResultKvEntry() {};
    ResultKvEntry(std::string key, std::string value) {
        key = key;
        value = value;
    };
} ResultKvEntry;

class Result {
   private:
    data::ValueBytes* value = NULL;
    int kv_offset = 0;

   public:
    uint8_t status;
    std::string data;
    int cap;
    std::vector<Result*> items;

    Result() : status(0), cap(0) {};
    ~Result() {
        if (value != NULL) {
            delete value;
        }
        for (int i = 0; i < items.size(); i++) {
            delete items[i];
        }
        items.clear();
    };
    uint8_t Status() { return status; }
    bool OK() { return status == ResultOK; }
    bool NotFound() { return status == ResultNotFound; }
    std::string String() {
        return data;
    };
    uint64_t Uint64() {
        if (data.size() > 0) {
            return str_to_uint64(data);
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
        if (data.size() > 0) {
            return str_to_int64(data);
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
        if (data.size() > 0) {
            return str_to_float64(data);
        }
        return 0;
    };
    float Float32() {
        if (data.size() > 0) {
            return str_to_float(data);
        }
        return 0;
    };
    bool Bool() {
        if (data.size() > 0 &&
            (data == "1" || data == "true" || data == "yes")) {
            return true;
        }
        return false;
    };
    data::ValueBytes* ValueBytes() {
        if (value == NULL) {
            value = new data::ValueBytes(data);
        }
        return value;
    };
    int KvLen() {
        return (items.size() / 2);
    };
    bool KvValid() {
        if (kv_offset + 2 <= items.size()) {
            return true;
        }
        return false;
    };
    std::string KvKey() {
        if (KvValid()) {
            return items[kv_offset]->data;
        }
        return "";
    };
    std::string KvValue() {
        if (KvValid()) {
            return items[kv_offset + 1]->data;
        }
        return "";
    };
    void KvNext() {
        kv_offset += 2;
    };
    int ItemSize() {
        return items.size();
    };
    Result* Item(int i) {
        if (i + 1 <= items.size()) {
            return items[i];
        }
        return NULL;
    };
};

class Client {
   private:
    int sock;
    std::string m_buffer;
    int m_buffer_offset;
    int read();
    std::string cmd_line();
    int cmd_parse(Result* rs);
    int cmd_parse_string(Result* rs, int n);
    int cmd_parse_array(Result* rs, int cap);
    Result* cmd_exec(const std::string data);

   public:
    static Client* Connect(const std::string& ip, int port);
    Client() : sock(-1), m_buffer_offset(0) {};
    ~Client() {
        Close();
    };
    Result* Cmd(std::string cmd, std::string arg1, std::string arg2,
                std::string arg3, std::string arg4);
    Result* Cmd(std::string cmd, std::string arg1, std::string arg2,
                std::string arg3);
    Result* Cmd(std::string cmd, std::string arg1, std::string arg2);
    Result* Cmd(std::string cmd, std::string arg1);
    Result* Cmd(std::string cmd);
    void Close();
};

static inline Result* NewResult(int8_t status, std::string error) {
    Result* rs = new Result();

    if (status > 0) {
        rs->status = status;
    }

    if (error != "") {
        if (rs->status == 0) {
            rs->status = ResultError;
        }
        rs->data = error;
    }

    return rs;
};

} // namespace client
} // namespace lynkstor

#endif
