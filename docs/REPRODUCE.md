# Clean-checkout reproduction

Everything below runs from a fresh clone with only Python 3.11+ and a
host C compiler — no third-party packages, no network services beyond
loopback, no private state. Expected outputs are stated so a reviewer
can compare without trusting this document.

## 1. Build and self-check

```sh
git clone https://github.com/gnanam1990/restartproof.git
cd restartproof
python3 -m rp doctor
make -C guest/build
```

`rp doctor` lists the host compiler and reports the QNX toolchain as
ABSENT unless a licensed SDP is installed — that absence is expected
and honest.

## 2. The two measured host-fixture runs

```sh
python3 -m rp run --scenario label-retry --profile host-fixture \
    --sink keyed --client-key demo-keyed
python3 -m rp run --scenario label-retry --profile host-fixture \
    --sink plain --client-key demo-plain
python3 -m rp runs          # note the two run ids
python3 -m rp report <keyed-run-id>   # exit 0 — PASS
python3 -m rp report <plain-run-id>   # exit 1 — FAIL_DUPLICATE
```

Expected: the keyed run passes (one deduplicated durable effect); the
plain run fails with two distinct effects — the measured duplicate a
retry-after-lost-reply produces without an idempotency key.

## 3. Independent inspection

```sh
python3 -m rp effects <plain-run-id>  # two attributable rows, seq 1,2
python3 -m rp effects <keyed-run-id>  # one attributable row, seq 1
python3 -m rp sink status             # adapter + ledger health
python3 -m rp console --run <keyed-run-id> --run <plain-run-id>
```

`console.html` opens in any browser — no scripts; verdict cards stay
readable at 200% zoom and each links to its exported `evidence.json`.

## 4. Full test suite

```sh
python3 -m unittest discover -s tests -v
```

58 tests at last local run, covering the contracts (M01), sink (M02),
harness (M03), runner/retry (M04), verdict reducer (M05), console and
operator bridge (M06) — including the real end-to-end operator flow.

## 5. The QNX gate (honestly unverified)

A licensed QNX 8.0 SDP + QEMU guest is required for the measured-target
path; neither is redistributed. With them, follow
`docs/QNX-TARGET.md`: `rp operate <run> --register` prints the launch
parameters, the guest binaries run on the target, and
`rp operate <run> --ctl <guest>:<port>` drives the identical
orchestration exercised in `tests/test_m06_console.py`. Until a
licensed target executes it, target verdicts are **UNVERIFIED** — a
host run is labeled `HOST_FIXTURE` and never satisfies the QNX gate.
