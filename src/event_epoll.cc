// Copyright 2017 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//

#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include "event.hh"
#include "cmd.hh"
#include "cjet/log.hh"

namespace lynkstor {

EventLoop::EventLoop(int fdsock) {

    //
    size_ = EventLoopSize;

    //
    fdsock_ = fdsock;
    non_block(fdsock, true);

    //
    fdep_ = epoll_create(size_);
    if (fdep_ == -1) {
        return;
    }

    //
    events_ = (epoll_event *)calloc(size_ + 1, sizeof(epoll_event));
    fired_ = (EventContext *)calloc(size_ + 1, sizeof(EventContext));

    //
    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = fdsock;
    if (epoll_ctl(fdep_, EPOLL_CTL_ADD, fdsock, &ev) == -1) {
        ::close(fdep_);
        fdep_ = -1;
        ::free(events_);
        ::free(fired_);
    }
}

EventLoop::~EventLoop() {
    if (fdep_) {
        ::close(fdep_);
    }
    if (events_) {
        ::free(events_);
    }
    if (fired_) {
        ::free(fired_);
    }
}

void EventLoop::FiredDel(int i) {

    // CJET_LOG("debug", "EV FDEL %d", i);
    EventContext ec = fired_[i];
    Del(ec.fdev_);
}

void EventLoop::Set(int fd, int flags) {

    if (fd < 1) {
        return;
    }

    struct epoll_event evn;
    evn.events = flags;
    ::epoll_ctl(fdep_, EPOLL_CTL_MOD, fd, &evn);
}

void EventLoop::Del(int fd) {

    if (fd < 1) {
        return;
    }

    struct epoll_event evn;
    evn.events = 0;
    evn.data.u64 = 0;
    evn.data.fd = fd;
    ::epoll_ctl(fdep_, EPOLL_CTL_DEL, fd, &evn);
}

int EventLoop::Wait() {

    if (fdep_ == -1) {
        return -1;
    }

    int en = epoll_wait(fdep_, events_, size_, -1);
    int efn = 0;

    for (int i = 0; i < en; i++) {

        struct epoll_event *e = events_ + i;

        if (e->events & EPOLLRDHUP) {

            // CJET_LOG("debug", "EV Closed %d", e->data.fd);
            EventContext *ec = ((EventContext *)e->data.ptr);
            delete ec;

        } else if (e->data.fd == fdsock_) {

            struct sockaddr_storage ss;
            socklen_t slen = sizeof(ss);
            int fdin = ::accept(fdsock_, (struct sockaddr *)&ss, &slen);
            if (fdin != -1) {
                non_block(fdin, true);

                // CJET_LOG("info", "EV New %d %d", e->data.fd, fdin);

                EventContext *ev_ctx = new EventContext(0, fdin);

                struct epoll_event evn;
                evn.data.ptr = ev_ctx;
                evn.events = EPOLLIN | EPOLLERR | EPOLLHUP | EPOLLRDHUP;

                if (::epoll_ctl(fdep_, EPOLL_CTL_ADD, fdin, &evn) == -1) {
                    delete ev_ctx;
                    // CJET_LOG("info", "EV ER %d", fdin);
                }
            }

        } else if ((e->events & EPOLLERR) || (e->events & EPOLLHUP)) {

            // CJET_LOG("info", "EV ER %d", e->data.fd);

            EventContext *ec = ((EventContext *)e->data.ptr);
            delete ec;

        } else if ((e->events & EPOLLIN)) {

            fired_[efn] = ((EventContext *)e->data.ptr);
            fired_[efn].fdev_ = e->data.fd;
            efn++;

        } else {
            // CJET_LOG("info", "EV ER %d", e->data.fd);

            EventContext *ec = ((EventContext *)e->data.ptr);
            delete ec;
            Del(e->data.fd);
        }
    }

    return efn;
}

void EventLoop::non_block(int fd, bool yes) {
    if (fd) {
        if (yes) {
            ::fcntl(fd, F_SETFL, O_RDWR | O_NONBLOCK);
        } else {
            ::fcntl(fd, F_SETFL, O_RDWR);
        }
    }
}

} // namespace lynkstor
