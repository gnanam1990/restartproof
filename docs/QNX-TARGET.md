# Running RestartProof on a QNX 8.0 target

This is the measured-target path. **A Linux/macOS host run is not QNX
proof** — the portable backend exists so the same harness logic is
exercised in CI, labeled `HOST_FIXTURE`.

## Prerequisites (not redistributed)

- A licensed **QNX SDP 8.0** host install (`qnxsdp-env.sh`). We do not
  ship the SDK, license material or guest images.
- A **QNX 8.0 QEMU guest** (QNX Everywhere image or licensed target),
  booted with user-mode networking so the guest can reach the host at
  `10.0.2.2` (QEMU default) — or equivalent host-reachable networking.
  The host must also reach the guest's control port (`10.0.2.15` under
  QEMU user networking, or use `hostfwd`).
- This repository checked out on the host.

## Build the guest binaries

```sh
source /path/to/qnxsdp-env.sh
cd guest/build
make -f qnx.mk                    # x86_64 QEMU guest (default)
make -f qnx.mk VARIANT=aarch64le  # ARM64 guest
```

Output: `guest/bin-qnx/{rp_worker,rp_supervisor,rp_client}`.

## The executable operator path

The bridge is real and testable: `rp operate` performs the whole
observe/collect phase over the supervisor's TCP control channel —
nonce handshake, bounded barrier wait, restart, evidence gather
(`RP_CTL_EVENTS`), stop, seal, snapshot. Nothing is a placeholder
promise; every step below works today against the host fixture and
works identically on a licensed target.

### 1. Start the receipt sink reachable from the guest

```sh
python3 -m rp sink serve --host 0.0.0.0 --port 8077
```

### 2. Plan and register the run

```sh
python3 -m rp plan --scenario label-retry --profile qnx8-qemu \
    --sink keyed --client-key lab-qnx-1
python3 -m rp operate <run-id> --register
```

`--register` allocates the run's sink registration and prints the
launch parameters: `runNonce`, `writerToken`, `opId`,
`payloadDigest`, `maxAttempts`. The writer token is handed to the
target binaries below — it stays in the private journal and is never
exported into the evidence bundle.

### 3. Start the expressly cooperating harness on the guest

Copy the three `bin-qnx` binaries to the guest (scp / qconn / target
filesystem), then **on the guest**:

```sh
./rp_supervisor --rundir /tmp/rp_<run-id> \
    --worker-image /tmp/rp_worker \
    --run <run-id> --nonce <runNonce> \
    --sink http://10.0.2.2:8077 --writer-token <writerToken> \
    --ctl-port 9010 --endpoint-base rp_spool_<run-id>

./rp_client --rundir /tmp/rp_<run-id> --run <run-id> \
    --nonce <runNonce> --op <opId> \
    --payload-json '<pinned payload>' \
    --payload-digest <payloadDigest> --attempt-prefix att \
    --max-attempts 2 --timeout-ms 8000 --hold-first 1 \
    --report /tmp/rp_<run-id>/client_report.json
```

The supervisor registers its own child workers only — no arbitrary
process control. The client discovers the worker endpoint through the
supervisor registry (`name_open` on the re-attached name after a
restart).

### 4. Drive the scenario from the host operator

```sh
python3 -m rp operate <run-id> --ctl 10.0.2.15:9010
```

This:

1. **Attaches** — `RP_CTL_STATUS` with the run nonce; a supervisor
   bound to another run is refused (child-outside-registry, T-013).
2. **Waits (bounded) for the worker barrier** — polls `RP_CTL_EVENTS`,
   which streams the guest's `events.jsonl`, `client_report.json` and
   `uname` over the control channel.
3. **Sends `RESTART`** with a fresh monotonic control generation.
4. **Gathers the client report and event journal** via `RP_CTL_EVENTS`.
   The reply's `uname` is recorded as the guest-reported target
   identity (`targetIdentity.uname` in the bundle).
5. **Stops the supervisor, closes the epoch, seals the sink, snapshots
   the durable effect rows** — the identical tail `rp run` performs.

If the guest rundir is mounted/synced instead, pass
`--guest-dir <path>` and the same steps read `events.jsonl` /
`client_report.json` locally; ctl is still used for attach,
restart and stop.

### 5. Classify and inspect

```sh
python3 -m rp report <run-id>          # verdict + evidence bundle
python3 -m rp effects <run-id>         # attributed sink rows
python3 -m rp console                  # accessible side-by-side view
```

The bundle's `provenance` comes from the pinned profile
(`QNX 8.0 QEMU guest`, `hostFixture: false`), and `targetIdentity`
carries the guest's own `uname` string with `execution:
"operator-bridge"`.

## Exercising the same path without a target

The identical orchestration runs against the host fixture: spawn the
POSIX binaries by hand (the same argv `rp run` uses) and point
`--ctl` at their printed `ctl.json` port. The test suite does exactly
this (`tests/test_m06_console.py`), so the operator path's mechanics —
handshake, barrier, restart, gather, seal — are real coverage, while
the *transport* difference (native `name_attach` IPC vs POSIX sockets)
remains honestly unverified until a licensed run.

## Current unverified boundary

- `rp run --profile qnx8-qemu` reports setup/unsupported (exit 3):
  the host runner cannot launch processes on a guest.
- `rp operate` is the supported target path and has been exercised
  end-to-end against the host fixture only. No licensed QNX target
  has executed it — target verdicts remain **UNVERIFIED** until then.
- `transport_qnx.c` compiles only under `__QNXNTO__` and has not run
  on a licensed guest.
