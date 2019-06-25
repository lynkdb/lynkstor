// Copyright 2017 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//

#include "status.hh"
#include "utils.hh"

namespace lynkstor {
namespace status {

int64_t sys_putlog_ver_min = 0;
int64_t sys_putlog_ver_max = 0;
int64_t sys_uptime = utils::timenow_us() / 1e6;
std::string ConfigAuth = "";
bool ConfigReplicationEnable = false;

} // namespace status
} // namespace lynkstor
