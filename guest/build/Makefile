# Portable host build of the RestartProof guest harness.
#   make            -> POSIX binaries in guest/bin (HOST_FIXTURE path)
#   make -f qnx.mk  -> QNX 8.0 build with the SDP toolchain (see docs/)

CC      ?= cc
CFLAGS  ?= -O2 -Wall -Wextra -std=c11
CPPFLAGS += -I../include -D_POSIX_C_SOURCE=200809L -D_DEFAULT_SOURCE
BIN     := ../bin

TRANSPORT := ../src/transport_posix.c
SHARED    := ../src/transport_ctl.c ../src/rp_http.c

all: $(BIN)/rp_worker $(BIN)/rp_supervisor $(BIN)/rp_client

$(BIN):
	mkdir -p $(BIN)

$(BIN)/rp_worker: ../src/worker.c $(SHARED) $(TRANSPORT) | $(BIN)
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ $^ $(LDLIBS)

$(BIN)/rp_supervisor: ../src/supervisor.c $(SHARED) $(TRANSPORT) | $(BIN)
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ $^ $(LDLIBS)

$(BIN)/rp_client: ../src/client.c $(SHARED) $(TRANSPORT) | $(BIN)
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ $^ $(LDLIBS)

clean:
	rm -rf $(BIN)

.PHONY: all clean
