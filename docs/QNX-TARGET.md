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
- This repository checked out on the host.

## Build the guest binaries

```sh
source /path/to/qnxsdp-env.sh
cd guest/build
make -f qnx.mk                    # x86_64 QEMU guest (default)
make -f qnx.mk VARIANT=aarch64le  # ARM64 guest
```

Output: `guest/bin-qnx/{rp_worker,rp_supervisor,rp_client}`.

## Wire up a run

1. Start the host receipt sink reachable from the guest:

   ```sh
   python3 -m rp sink serve --host 0.0.0.0 --port 8077
   ```

2. Plan the run on the host to pin the manifest and get the run nonce:

   ```sh
   python3 -m rp plan --scenario label-retry --profile qnx8-qemu \
       --sink keyed --client-key lab-qnx-1
   python3 -m rp manifest <run-id>   # note runNonce + payloadDigest
   ```

3. Copy the three `bin-qnx` binaries into the guest (scp/qnx target
   filesystem) plus the run directory.

4. **On the guest**, start the supervisor with a registered endpoint
   name and the host sink URL:

   ```sh
   ./rp_supervisor --rundir /tmp/rp_run1 \
       --worker-image /tmp/rp_worker \
       --run <run-id> --nonce <runNonce> \
       --sink http://10.0.2.2:8077 --writer-token <writerToken> \
       --ctl-port 0 --endpoint-base rp_spool_<run-id>
   ```

   The supervisor registers its own child workers; it will only ever
   signal those children (no arbitrary process control). It writes
   `ctl.json` and `registry.json` into the rundir.

5. On the guest, drive the client:

   ```sh
   ./rp_client --rundir /tmp/rp_run1 --run <run-id> --nonce <runNonce> \
       --op label-job-0001 --payload-json '<payload json>' \
       --payload-digest <digest> --attempt-prefix att \
       --max-attempts 2 --timeout-ms 8000 --hold-first 1 \
       --report /tmp/rp_run1/client_report.json
   ```

6. The host runner sends `RESTART` on the supervisor ctl channel when the
   barrier lands (see `rp collect` / `rp run` once wired to a guest
   delegate — `RP_QNX_GUEST_ADDR`). The client rediscovers the fresh
   endpoint (`name_open` on the re-attached name) and retries.

7. Collect `events.jsonl`, `registry.json`, `client_report.json` and the
   sink ledger; the host-side reducer/verdict treats the run like any
   other — with `provenance: qnx8-qemu` and the guest's reported OS
   version instead of `HOST_FIXTURE`.

## Current unverified boundary

`rp run --profile qnx8-qemu` reports setup/unsupported (exit 3) until a
guest delegate is configured; the QNX transport compiles only under
`__QNXNTO__` and has not yet been executed on a licensed guest.
