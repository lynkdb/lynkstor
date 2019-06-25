// Copyright 2017 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//

#ifndef LYNKSTOR_UTILS_UTF8_HH
#define LYNKSTOR_UTILS_UTF8_HH

#include <string>

namespace lynkstor {
namespace util_utf8 {

static bool valid(const std::string &str) {

    int i = 0;
    int len = str.size();
    const unsigned char *s = (const unsigned char *)str.c_str();

    while (i < len) {

        if (s[i] < 0x80) {
            i += 1;
        } else if ((s[i] & 0xe0) == 0xc0) {
            if (i + 1 >= len || (s[i + 1] & 0xc0) != 0x80 ||
                (s[i] & 0xfe) == 0xc0) {
                return false;
            }
            i += 2;
        } else if ((s[i] & 0xf0) == 0xe0) {
            if (i + 2 >= len || (s[i + 1] & 0xc0) != 0x80 ||
                (s[i + 2] & 0xc0) != 0x80 ||
                (s[i] == 0xe0 && (s[i + 1] & 0xe0) == 0x80) ||
                (s[i] == 0xed && (s[i + 1] & 0xe0) == 0xa0) ||
                (s[i] == 0xef && s[i + 1] == 0xbf &&
                 (s[i + 2] & 0xfe) == 0xbe)) {
                return false;
            }
            i += 3;
        } else if ((s[i] & 0xf8) == 0xf0) {
            if (i + 3 >= len || (s[i + 1] & 0xc0) != 0x80 ||
                (s[i + 2] & 0xc0) != 0x80 || (s[i + 3] & 0xc0) != 0x80 ||
                (s[i] == 0xf0 && (s[i + 1] & 0xf0) == 0x80) ||
                (s[i] == 0xf4 && s[i + 1] > 0x8f) || s[i] > 0xf4) {
                return false;
            }
            i += 4;
        } else {
            return false;
        }
    }

    return true;
}

} // namespace util_utf8
} // namespace lynkstor

#endif
