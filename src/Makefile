# Copyright 2017 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
#

CC=gcc
CXX=g++
MAKE=make

#CFLAGS = -std=c++11 -D__STDC_FORMAT_MACROS -Wall -O2 -Wno-sign-compare
CFLAGS = -std=c++11 -g -O0 -Wno-sign-compare
CFLAGS += -I "../deps/jemalloc/include"
CFLAGS += -I "../deps/leveldb/include"
CFLAGS += -I "../deps/leveldb"
# CFLAGS += -I "../deps/rocksdb/include"
# CFLAGS += -I "../deps/flatbuffers/include"
CFLAGS += -I "../deps/protobuf/src"
CFLAGS += -I "../deps/fmt"
CFLAGS += -I "../deps"
CFLAGS += -I "../deps/cjet/include"
CFLAGS += -I "../deps/hrpc/src"

CLIBS =
CLIBS += "../deps/jemalloc/lib/libjemalloc.a"
CLIBS += "../deps/leveldb/libleveldb.a"
# CLIBS += "../deps/flatbuffers/libflatbuffers.a"
CLIBS += "../deps/snappy/build/libsnappy.a"
CLIBS += "../deps/protobuf/build/libprotobuf.a"
CLIBS += "../deps/fmt/fmt/libfmt.a"
CLIBS += "../deps/cjet/src/cjet.a"
CLIBS += "../deps/hrpc/src/hrpc.a"
CLIBS += "../deps/nanomsg/build/libnanomsg.a"
# CLIBS += "../deps/rocksdb/librocksdb.a"
CLIBS += -pthread
#CLIBS += -ltcmalloc
#CLIBS += -lunwind
#CLIBS += -lprofiler
CLIBS += -lanl

# g++ -Wl,-Bstatic -lz -lfoo -Wl,-Bdynamic -lbar -Wl,--as-needed
# Will link zlib and libfoo as static, and libbar as dynamic . --as-needed will drop any unused dynamic library.

# CentOS 7
# yum install -y yum install -y zlib-devel bzip2-devel lz4-devel

#
OBJS = main.o net.o cmd.o event_epoll.o data.o data_base.o cmd_system.o cmd_kv.o cmd_prog.o cmd_fo.o server.o server_rpc.o status.o lynkstor.pb.o
OBJS_EXAMPLE = example.o net.o cmd.o event_epoll.o data.o data_leveldb.o data_base.o cmd_system.o cmd_kv.o cmd_prog.o cmd_fo.o server.o status.o lynkstor.pb.o client/client.o
EXE_SERVER = ../bin/lynkstor
EXE_EXAMPLE = ../bin/example

OBJS +=  data_leveldb.o

ifdef LYNKSTOR_ENGINE_ROCKSDB
	OBJS +=  data_rocksdb.o
	CFLAGS +=  -D LYNKSTOR_ENGINE_ROCKSDB
	CLIBS += -llz4 -lz -lzstd -lbz2
endif


all: ${OBJS}
	${CXX} ${CFLAGS} -o ${EXE_SERVER} ${OBJS} ${CLIBS}
#	strip -s ${EXE_SERVER}

example: ${OBJS_EXAMPLE}
	${CXX} ${CFLAGS} -o ${EXE_EXAMPLE} ${OBJS_EXAMPLE} ${CLIBS}
#	strip -s ${EXE_EXAMPLE}

main.o:
	${CXX} ${CFLAGS} -o main.o -c main.cc

example.o:
	${CXX} ${CFLAGS} -o example.o -c example.cc

net.o:
	${CXX} ${CFLAGS} -o net.o -c net.cc

cmd.o:
	${CXX} ${CFLAGS} -o cmd.o -c cmd.cc

event_epoll.o:
	${CXX} ${CFLAGS} -o event_epoll.o -c event_epoll.cc

data.o:
	${CXX} ${CFLAGS} -o data.o -c data.cc

data_leveldb.o:
	${CXX} ${CFLAGS} -o data_leveldb.o -c data_leveldb.cc

data_rocksdb.o:
	${CXX} ${CFLAGS} -o data_rocksdb.o -c data_rocksdb.cc

data_base.o:
	${CXX} ${CFLAGS} -o data_base.o -c data_base.cc

cmd_system.o:
	${CXX} ${CFLAGS} -o cmd_system.o -c cmd_system.cc

cmd_kv.o:
	${CXX} ${CFLAGS} -o cmd_kv.o -c cmd_kv.cc

cmd_prog.o:
	${CXX} ${CFLAGS} -o cmd_prog.o -c cmd_prog.cc

cmd_fo.o:
	${CXX} ${CFLAGS} -o cmd_fo.o -c cmd_fo.cc

server.o:
	${CXX} ${CFLAGS} -o server.o -c server.cc

server_rpc.o:
	${CXX} ${CFLAGS} -o server_rpc.o -c server_rpc.cc

status.o:
	${CXX} ${CFLAGS} -o status.o -c status.cc

lynkstor.pb.o:
	${CXX} ${CFLAGS} -o lynkstor.pb.o -c lynkstor.pb.cc

client/client.o:
	${CXX} ${CFLAGS} -o client/client.o -c client/client.cc

clean:
	rm -f ${EXE_SERVER} *.o *.a
	rm -f client/*.o

