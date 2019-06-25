// Copyright 2017 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//

#include <iostream>

#include "cmd.hh"
#include "data.hh"
#include "utils.hh"
#include "cjet/log.hh"

namespace lynkstor {

static CmdList _cmd_list;

void CmdReply::Push(const std::string& str) {
    if (str.size() > 0) {
        Bulk.append("$" + std::to_string(str.size()) + "\r\n" + str + "\r\n");
        // Bulk.append(str + "\r\n");
    } else {
        Bulk.append("$0\r\n");
    }
    array_size_++;
}

void CmdReply::PushNil() {
    Bulk.append("$-1\r\n");
    array_size_++;
}

void CmdReply::ArrayPush(CmdReply p) {
    Bulk.append(p.Send());
    array_size_++;
}

void CmdReply::Int(long long n) {
    Inline = ":";
    Inline.append(std::to_string(n));
    Inline.append("\r\n");
}

void CmdReply::OK() { Inline = CmdReplyOK; }

void CmdReply::NotFound() { Inline = CmdReplyNotFound; }

void CmdReply::Error() { Inline = CmdReplyErr; }

void CmdReply::Error(std::string str) {
    if (str.size() == 0) {
        Inline = CmdReplyErr;
    } else {
        Inline = "-" + str + "\r\n";
    }
}

const std::string CmdReply::Send() {

    if (Inline.size() > 0) {
        return Inline;
    } else if (array_size_ > 1) {
        return "*" + std::to_string(array_size_) + "\r\n" + Bulk;
    } else if (array_size_ > 0) {
        return Bulk;
    }

    return "";
}

void CmdList::Reg(std::string name, CmdEntryHandler c, int argc_min,
                  int argc_max) {

    name = str_to_upper(name);
    CmdEntry* ce = Get(name);
    if (!ce) {
        ce = new CmdEntry();
        ce->Name = name;
        items_[ce->Name] = ce;
    }
    ce->Handler = c;
    if (argc_min < 1) {
        argc_min = 1;
    }
    if (argc_max == -1) {
        argc_max = 10000;
    } else if (argc_max < argc_min) {
        argc_max = argc_min;
    }
    ce->ArgcMin = argc_min;
    ce->ArgcMax = argc_max;
}

CmdEntry* CmdList::Get(std::string name) {
    if (items_.size() > 0) {
        CmdMap::iterator it = items_.find(name);
        if (it != items_.end()) {
            return it->second;
        }
    }
    return NULL;
}

void CmdEntryReg(std::string name, CmdEntryHandler c, int arg_min,
                 int arg_max) {
    return _cmd_list.Reg(name, c, arg_min, arg_max);
}

CmdEntry* CmdEntryGet(std::string name) {
    //
    return _cmd_list.Get(name);
}

const std::vector<std::string> CmdReqBuffer::Argv() {

    cmd_.clear();

    int err_num = 0;
    int rs = 0;

    for (;;) {

        rs = parse();
        if (rs > 0) {
            return cmd_;
        } else if (rs < 0) {
            err_ = true;
            break;
        }

        std::string buf;
        rs = NetSocketRecv(fd_, buf);

        if (rs == -1) {
            err_ = true;
            break;
        }

        if (buf.size() > 0) {
            buffer_.append(buf);
            err_num = 0;
        } else {
            err_num += 1;
        }

        if (err_num > 100) {
            err_ = true;
            CJET_LOG("warn", "net-io: max error retry");
            break;
        }
    }

    return cmd_;
}

const bool CmdReqBuffer::Next() {
    //
    return buffer_.size() > 0;
}

int CmdReqBuffer::parse() {

    int idx = 0;

    for (;;) {

        if (array_len_ <= 0) {

            if (buffer_.size() < 4) {
                break;
            }

            //
            if (buffer_[0] == '$') {
                array_len_ = 1;
                continue;
            }

            //
            if (buffer_[0] != '*') {
                return -1;
            }

            //
            idx = buffer_.find_first_of("\r\n");
            if (idx == -1) {
                if (buffer_.size() > 10) {
                    return -1;
                }
                break;
            }
            if (idx < 2) {
                return -1;
            }

            array_len_ = str_to_int(buffer_.substr(1, idx - 1));
            buffer_.erase(0, idx + 2);

            if (array_len_ < 1 || array_len_ > 100000) {
                return -1;
            }
        }

        if (array_len_ <= cmd_.size()) {
            array_len_ = 0;
            return 1;
        }

        if (buffer_.size() < 4) {
            break;
        }

        if (buffer_[0] != '$') {
            return -1;
        }

        //
        idx = buffer_.find_first_of("\r\n");
        if (idx == -1) {
            if (buffer_.size() > 13) {
                return -1;
            }
            break;
        }
        if (idx < 2) {
            return -1;
        }

        if (buffer_[1] == '-' || buffer_[1] == '0') {
            cmd_.push_back("");
            buffer_.erase(0, idx + 2);
        } else {

            bulk_len_ = str_to_int(buffer_.substr(1, idx - 1));
            if (bulk_len_ > 0 && bulk_len_ < CmdReqValueMaxSize) {
                if (bulk_len_ + idx + 4 > buffer_.size()) {
                    break;
                }
                cmd_.push_back(buffer_.substr(idx + 2, bulk_len_));
                buffer_.erase(0, idx + bulk_len_ + 4);
            } else {
                return -1;
            }
        }
    }

    if (array_len_ > 0 && array_len_ <= cmd_.size()) {
        array_len_ = 0;
        return 1;
    }

    return 0;
}

} // namespace lynkstor
