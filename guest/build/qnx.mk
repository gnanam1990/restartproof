# QNX 8.0 build of the RestartProof guest harness.
#
# Requires a licensed QNX SDP sourced in the environment (qnxsdp-env.sh),
# which provides the QNX_HOST/QNX_TARGET variables and the qcc compiler
# front-end. No SDK files are distributed with this repository.
#
#   source $QNX_SDP/qnxsdp-env.sh
#   make -f qnx.mk               # x86_64 QEMU guest (default)
#   make -f qnx.mk VARIANT=aarch64le
#
# Output: guest/bin-qnx/{rp_worker,rp_supervisor,rp_client}

VARIANT ?= x86_64

ifeq ($(VARIANT),x86_64)
  CC := qcc -Vgcc_ntox86_64
else ifeq ($(VARIANT),aarch64le)
  CC := qcc -Vgcc_ntoaarch64le
else
  $(error unknown VARIANT '$(VARIANT)'; use x86_64 or aarch64le)
endif

CFLAGS  ?= -O2 -Wall -Wextra -std=c11
CPPFLAGS += -I../include
LDLIBS  += -lsocket
BIN     := ../bin-qnx

TRANSPORT := ../src/transport_qnx.c
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
