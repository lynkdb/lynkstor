// Copyright 2017 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//

#ifndef LYNKSTOR_CMD_HH
#define LYNKSTOR_CMD_HH

#include <string>
#include <vector>
#include <unordered_map>
#include "net.hh"
#include "data.hh"
#include "lynkstor.pb.h"

namespace lynkstor {

const std::string CmdReplyOK = "+OK\r\n";
const std::string CmdReplyErr = "-ERR\r\n";
const std::string CmdReplyNotFound = "$-1\r\n";
const std::string CmdReplyNotImplemented = "-ERR NotImplemented\r\n";
const std::string CmdReplyNoAuth = "-ERR NOAUTH Authentication required.\r\n";
const std::string CmdReplyAuthError = "-ERR invalid password\r\n";

const int CmdScanLimitMax = 100000;
const size_t CmdReqValueMaxSize = 8388608; // 8MB

class CmdReqBuffer;
class CmdReply;
class CmdContext;
class CmdEntry;

typedef std::vector<std::string> CmdArgv;
typedef std::unordered_map<std::string, CmdEntry *> CmdMap;
typedef void (*CmdEntryHandler)(CmdContext &ctx, const CmdArgv &argv);

class CmdReply {
   private:
    int array_size_;

   public:
    bool Quit;
    std::string Inline;
    std::string Bulk;

    CmdReply() : array_size_(0), Quit(false) {}
    ~CmdReply() {};
    int Size() {
        return array_size_;
    };
    void Push(const std::string &str);
    void PushNil();
    void Int(long long n);
    void OK();
    void NotFound();
    void Error();
    void Error(std::string msg);
    void ArrayPush(CmdReply p);
    const std::string Send();
}; // class CmdReply

class CmdContext {
   public:
    data::Engine *Stor;
    data::Engine *Meta;
    CmdReply Reply;
    CmdContext(data::Engine *data, data::Engine *meta) {
        Stor = data;
        Meta = meta;
    };
}; // class CmdContext

class CmdRpcContext {
   public:
    data::Engine *Stor;
    data::Engine *Meta;
    LynkStorResult *Reply;
    CmdRpcContext(data::Engine *data, data::Engine *meta, LynkStorResult *rep) {
        Stor = data;
        Meta = meta;
        Reply = rep;
    };
    ~CmdRpcContext() {};
}; // class CmdRpcContext

struct CmdEntry {
    std::string Name;
    CmdEntryHandler Handler;
    int ArgcMin;
    int ArgcMax;
}; // class CmdEntry

class CmdList {
   private:
    CmdMap items_;

   public:
    void Reg(std::string name, CmdEntryHandler c, int arg_min, int arg_max);
    CmdEntry *Get(std::string name);
}; // class Cmd

extern void CmdEntryReg(std::string name, CmdEntryHandler c, int arg_min,
                        int arg_max);
extern CmdEntry *CmdEntryGet(std::string name);

class CmdReqBuffer {
   private:
    int array_len_;
    size_t bulk_len_;
    std::string buffer_;
    int buffer_offset_;
    std::vector<std::string> cmd_;
    int parse();

   public:
    int fd_;
    bool authed_;
    bool err_;
    CmdReqBuffer(int fd)
        : array_len_(0),
          bulk_len_(0),
          buffer_(""),
          buffer_offset_(0),
          authed_(false),
          err_(false) {
        fd_ = fd;
    };
    ~CmdReqBuffer() {
        // std::cout << "cmd.buffer del\n";
    };
    const std::vector<std::string> Argv();
    const bool Next();
    void Append(const std::string &s) {
        buffer_.append(s);
    };
}; // class CmdReqBuffer

} // namespace lynkstor

#endif
