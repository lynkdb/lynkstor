// Copyright 2017 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//

#ifndef LYNKSTOR_STATUS_HH
#define LYNKSTOR_STATUS_HH

#include <stdint.h>
#include <iostream>

namespace lynkstor {
namespace status {

extern int64_t sys_uptime;
extern int64_t sys_putlog_ver_min;
extern int64_t sys_putlog_ver_max;
extern std::string ConfigAuth;
extern bool ConfigReplicationEnable;

} // namespace status
} // namespace lynkstor

#endif
