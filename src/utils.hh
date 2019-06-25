// Copyright 2017 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//

#ifndef LYNKSTOR_UTILS_HH
#define LYNKSTOR_UTILS_HH

#include <string>
#include <cstdlib>
#include <ctime>
#include <algorithm>
#include <cctype>
#include <sys/time.h>
#include "fmt/format.h"
#include "fmt/time.h"
#include <endian.h>
#include <iostream>

static inline char char_to_upper(const char c) {
    if (c <= 'z' && c >= 'a') {
        return (c - ('a' - 'A'));
    }
    return c;
}

static inline std::string str_to_upper(const std::string &str) {
    std::string rs = str;
    for (int i = 0; i < str.size(); i++) {
        rs[i] =
            (str[i] <= 'z' && str[i] >= 'a') ? str[i] - ('a' - 'A') : str[i];
    }
    return rs;
}

static inline int str_to_int(const std::string &str) {
    return std::atoi(str.c_str());
}

static inline int64_t str_to_int64(const std::string &str) {
    return std::atoll(str.c_str());
}

static inline uint64_t str_to_uint64(const std::string &str) {
    return std::atoll(str.c_str());
}

static inline float str_to_float(const std::string &str) {
    return std::atof(str.c_str());
}

static inline double str_to_float64(const std::string &str) {
    return std::atof(str.c_str());
}

static inline size_t str_to_size_t(const std::string &str) {
    char *end;
    return (size_t)std::strtoll(str.c_str(), &end, 10);
}

static inline uint64_t bs_to_uint64(std::string bs) {

    if (bs.size() < 8) {
        return 0;
    }

    char *bsc = const_cast<char *>(bs.c_str());
    unsigned char *b = reinterpret_cast<unsigned char *>(bsc);

    return uint64_t(b[7]) | uint64_t(b[6]) << 8 | uint64_t(b[5]) << 16 |
           uint64_t(b[4]) << 24 | uint64_t(b[3]) << 32 | uint64_t(b[2]) << 40 |
           uint64_t(b[1]) << 48 | uint64_t(b[0]) << 56;
}

static inline uint32_t bs_to_uint32(std::string bs) {

    if (bs.size() < 4) {
        return 0;
    }

    char *bsc = const_cast<char *>(bs.c_str());
    unsigned char *b = reinterpret_cast<unsigned char *>(bsc);

    return uint32_t(b[3]) | uint32_t(b[2]) << 8 | uint32_t(b[1]) << 16 |
           uint32_t(b[0]) << 24;
}

static inline void print_bs(std::string bs) {
    if (bs.size() == 0) {
        return;
    }

    char *bsc = const_cast<char *>(bs.c_str());
    unsigned char *b = reinterpret_cast<unsigned char *>(bsc);

    std::cout << bs.size() << " [";
    for (int i = 0; i < bs.size(); i++) {
        printf("%d ", uint8_t(b[i]));
        // printf("%02x ", b[i]);
    }
    std::cout << "]\n";
}

static inline std::string uint64_to_bs(uint64_t v) {

    char b[8];

    b[0] = (v >> 56);
    b[1] = (v >> 48);
    b[2] = (v >> 40);
    b[3] = (v >> 32);
    b[4] = (v >> 24);
    b[5] = (v >> 16);
    b[6] = (v >> 8);
    b[7] = (v);

    return std::string(b, 8);
}

static inline std::string uint32_to_bs(uint32_t v) {

    char b[4];

    b[0] = (v >> 24);
    b[1] = (v >> 16);
    b[2] = (v >> 8);
    b[3] = (v);

    return std::string(b, 4);
}

static inline std::string uint16_to_bs(uint16_t v) {

    char b[2];

    b[0] = (v >> 8);
    b[1] = (v);

    return std::string(b, 2);
}

static inline std::string uint8_to_bs(uint8_t v) {

    char b[1];

    b[1] = (v);

    return std::string(b, 1);
}

static inline std::string str_trim_left(const std::string &s,
                                        std::string cutset) {
    if (cutset.size() > 0 && s.size() > 0) {
        int i = s.find_first_not_of(cutset);
        return s.substr(i);
    }
    return s;
}

static inline std::string str_trim_right(const std::string &s,
                                         std::string cutset) {
    if (cutset.size() > 0 && s.size() > 0) {
        int i = s.find_last_not_of(cutset);
        return s.substr(0, i + 1);
    }
    return s;
}

static inline std::string str_trim(const std::string &s, std::string cutset) {
    if (cutset.size() > 0 && s.size() > 0) {
        int i = s.find_first_not_of(cutset);
        int j = s.find_last_not_of(cutset);
        if (i == -1) {
            return "";
        } else {
            return s.substr(i, j - i + 1);
        }
    }
    return s;
}

namespace lynkstor {
namespace utils {
typedef struct TypeMetaTime {
    ::timeval tv;
    TypeMetaTime() {
        ::gettimeofday(&tv, NULL);
    };
    std::string Format(std::string fmtstr) {
        return fmt::format("{:" + fmtstr + "}", *::gmtime(&tv.tv_sec));
    };
    uint64_t Uint64() {
        return (tv.tv_sec * 1000) + (tv.tv_usec / 1000);
    };
    int64_t Int64() {
        return (tv.tv_sec * 1000) + (tv.tv_usec / 1000);
    };
} TypeMetaTime;

static const uint64_t time_ms_max = 40e11;
static inline uint64_t timenow_ms() {
    ::timeval tn;
    ::gettimeofday(&tn, NULL);
    return (tn.tv_sec * 1e3) + (tn.tv_usec / 1e3);
}

static const uint64_t time_us_max = 40e14;
static inline uint64_t timenow_us() {
    ::timeval tn;
    ::gettimeofday(&tn, NULL);
    return (tn.tv_sec * 1e6) + tn.tv_usec;
}

static inline std::string idhash_rand_string(size_t len = 8,
                                             std::string const &chars =
                                                 "0123456789abcdef") {

    if (len < 2) {
        len = 2;
    } else if (len > 100) {
        len = 100;
    } else if (len % 2 > 0) {
        len -= 1;
    }

    std::mt19937_64 gen{std::random_device()()};

    std::uniform_int_distribution<size_t> dist{0, chars.length() - 1};

    std::string rs;

    std::generate_n(std::back_inserter(rs), len,
                    [&] { return chars[dist(gen)]; });

    return rs;
}

static inline std::string idhash_rand_hex_string(size_t n) {
    return idhash_rand_string(n, "0123456789abcdef");
}

} // namespace utils
} // namespace lynkstor

#endif
