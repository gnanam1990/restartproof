# RestartProof

Tell an embedded release engineer **how many externally observable effects
survived a worker restart and retry**.

A packing-station label service accepts a job; its worker commits the effect
to an independent receipt sink, then stops before replying to the client.
From the sender's side the result is ambiguous — a retry might produce one
spool receipt, two, none, or an unresolvable outcome. RestartProof measures
which of those actually happened, at the receipt boundary, and exports the
evidence.

- **Plain sink** — every accepted delivery appends a spool record → a retry
  after a lost reply produces *two* distinct effects (measured defect).
- **Keyed sink** — deduplicates on a stable operation id → the same retry
  returns the *original* receipt; exactly one durable effect.
- **Withheld coverage** — when the sink's coverage seal is unavailable the
  run reports *incomplete* rather than guessing.

## Status

| Surface | State |
|---|---|
| Scenario contracts / run store | Implemented (M01) |
| Receipt sink | Implemented (M02) |
| IPC harness | Implemented + host-tested; real QNX guest run **unverified** (M03) |
| Runner / retry | Implemented (M04) |
| Verdict / evidence | Implemented (M05) |
| Lab console | Pending (M06) |

**A Linux/macOS host run is not QNX proof.** Host execution is labeled
`HOST_FIXTURE` and cannot satisfy the QNX release gate.

## Requirements

- Python 3.11+ (standard library only — no dependencies)
- A C compiler (`cc`, `clang` or `gcc`) for the host fixture harness
- For the real target: QNX 8.0 SDP (`qcc`) plus a running QEMU guest
  (see `docs/QNX-TARGET.md`); neither is redistributed here

## Quick start

```sh
python3 -m rp doctor                      # environment + toolchain check
python3 -m rp plan --scenario label-retry \
    --profile host-fixture --sink keyed --client-key demo-1
python3 -m rp runs
python3 -m rp manifest run-…
python3 -m rp run --scenario label-retry \
    --profile host-fixture --sink keyed --client-key demo-1   # full run
python3 -m rp collect run-…               # reattach / finish collection
python3 -m rp report run-…                # sealed verdict + evidence bundle
python3 -m rp sink start                  # receipt sink service (per home)
python3 -m rp sink status                 # health + adapter version
python3 -m rp sink stop
make -C guest/build                       # portable harness (HOST_FIXTURE)
python3 -m unittest discover -s tests -v  # test suite
```

State lives under `./.rp` (override with `--home` or `$RP_HOME`).

## Architecture

See `docs/ARCHITECTURE.md`. In brief: `rp plan` pins an immutable manifest
(target profile, run nonce, operation ids, payload digests, declared effect
boundary) into a durable SQLite run store before anything launches. Facts
carry the run nonce; foreign-nonce evidence is quarantined, never attributed.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | measured pass |
| 1 | measured failure (e.g. duplicate or missing effect) |
| 2 | review / incomplete evidence / usage error |
| 3 | setup / unsupported target or schema |

## License

MIT — see `LICENSE`.
