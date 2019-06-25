// Copyright 2017 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//

#ifndef LYNKSTOR_EVENT_HH
#define LYNKSTOR_EVENT_HH

#include <sys/epoll.h>
#include <fcntl.h>
#include <unistd.h>
#include "cmd.hh"

namespace lynkstor {

const int EventLoopSize = 1024;

typedef struct EventContext {
    int fdev_;
    int fdc_;
    CmdReqBuffer *req_cmd_;
    void operator=(const EventContext *ec) {
        this->fdev_ = ec->fdev_;
        this->fdc_ = ec->fdc_;
        this->req_cmd_ = ec->req_cmd_;
    };
    EventContext(int fdev, int fdc) {
        fdev_ = fdev;
        fdc_ = fdc;
        req_cmd_ = new CmdReqBuffer(fdc);
    };
    ~EventContext() {
        if (fdc_ >= 0) {
            ::close(fdc_);
        }
        if (req_cmd_ != NULL) {
            delete req_cmd_;
        }
    };
} EventContext; // struct EventContext

class EventLoop {
   private:
    int size_;
    int fdep_;
    int fdsock_;
    static void non_block(int fd, bool yes);

   public:
    epoll_event *events_;
    EventContext *fired_;

    EventLoop(int fdsock);
    ~EventLoop();

    void Del(int fd);
    void Set(int fd, int flags);
    void FiredDel(int i);
    int Wait();
}; // class EventLoop

} // namespace lynkstor

#endif
