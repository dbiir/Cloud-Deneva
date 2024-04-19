CC=/usr/bin/g++
CFLAGS=-Wall -Werror -std=c++11 -g3 -ggdb -O0 -fno-strict-aliasing -fno-omit-frame-pointer -D_GLIBCXX_USE_CXX11_ABI=0
#CFLAGS += -fsanitize=address -fno-stack-protector -fno-omit-frame-pointer

.SUFFIXES: .o .cpp .h .cc

SRC_DIRS = ./ ./benchmarks/ ./client/ ./concurrency_control/ ./storage/ ./transport/ ./system/ ./statistics/#./unit_tests/
DEPS = -I. -I./benchmarks -I./client/ -I./concurrency_control -I./storage -I./transport -I./system -I./statistics #-I./unit_tests

CFLAGS += $(DEPS) -D NOGRAPHITE=1 -Wno-sizeof-pointer-memaccess -Wno-error=class-memaccess 
LDFLAGS = -L. -Wl,-rpath -pthread -lrt -lnanomsg -lanl -lcurl
#LDFLAGS = -Wall -L. -L$(NNMSG) -L$(JEMALLOC)/lib -Wl,-rpath,$(JEMALLOC)/lib -pthread -gdwarf-3 -lrt -std=c++11
LDFLAGS += $(CFLAGS)
LIBS =

DB_MAINS = ./client/client_main.cpp ./storage/storage_main.cpp
CL_MAINS = ./system/main.cpp ./storage/storage_main.cpp
ST_MAINS = ./system/main.cpp ./client/client_main.cpp

CPPS_DB = $(foreach dir,$(SRC_DIRS),$(filter-out $(DB_MAINS), $(wildcard $(dir)*.cpp)))
CPPS_CL = $(foreach dir,$(SRC_DIRS),$(filter-out $(CL_MAINS), $(wildcard $(dir)*.cpp)))
CPPS_ST = $(foreach dir,$(SRC_DIRS),$(filter-out $(ST_MAINS), $(wildcard $(dir)*.cpp)))

#CPPS = $(wildcard *.cpp)
OBJS_DB = $(addprefix obj/, $(notdir $(CPPS_DB:.cpp=.o)))
OBJS_CL = $(addprefix obj/, $(notdir $(CPPS_CL:.cpp=.o)))
OBJS_ST = $(addprefix obj/, $(notdir $(CPPS_ST:.cpp=.o)))

#NOGRAPHITE=1

all: check_directory rundb runcl runst
#unit_test

check_directory:
    ifeq (,$(wildcard obj))
	    mkdir obj
    endif

.PHONY: deps_db
deps:$(CPPS_DB)
	$(CC) $(CFLAGS) -MM $^ > obj/deps
	sed '/^[^ ]/s/^/obj\//g' obj/deps > obj/deps.tmp
	mv obj/deps.tmp obj/deps
-include obj/deps

rundb : $(OBJS_DB)
	$(CC) -o $@ $^ $(LDFLAGS) $(LIBS)
#	$(CC) -o $@ $^ $(LDFLAGS) $(LIBS)
./obj/%.o: transport/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) $(LIBS) -o $@ $<
#	$(CC) -c $(CFLAGS) $(INCLUDE) $(LIBS) -o $@ $<
#./deps/%.d: %.cpp
#	$(CC) -MM -MT $*.o -MF $@ $(CFLAGS) $<
./obj/%.o: benchmarks/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: storage/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: system/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: statistics/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: concurrency_control/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: client/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: %.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<

runst : $(OBJS_ST)
	$(CC) -o $@ $^ $(LDFLAGS) $(LIBS)
#	$(CC) -o $@ $^ $(LDFLAGS) $(LIBS)
./obj/%.o: transport/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) $(LIBS) -o $@ $<
#	$(CC) -c $(CFLAGS) $(INCLUDE) $(LIBS) -o $@ $<
#./deps/%.d: %.cpp
#	$(CC) -MM -MT $*.o -MF $@ $(CFLAGS) $<
./obj/%.o: benchmarks/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: storage/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: system/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: statistics/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: concurrency_control/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: client/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: %.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<


runcl : $(OBJS_CL)
	$(CC) -o $@ $^ $(LDFLAGS) $(LIBS)
#	$(CC) -o $@ $^ $(LDFLAGS) $(LIBS)
./obj/%.o: transport/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) $(LIBS) -o $@ $<
#	$(CC) -c $(CFLAGS) $(INCLUDE) $(LIBS) -o $@ $<
#./deps/%.d: %.cpp
#	$(CC) -MM -MT $*.o -MF $@ $(CFLAGS) $<
./obj/%.o: benchmarks/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: storage/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: system/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: statistics/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: concurrency_control/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: client/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: %.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<

.PHONY: clean
clean:
	rm -f obj/*.o obj/.depend rundb runcl runst unit_test
