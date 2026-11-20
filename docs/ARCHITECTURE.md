# Architecture

RestartProof measures externally observable effects at an independent
durable boundary — the receipt sink's spool ledger — never at a socket,
ACK, or client return code.

```
            plan (pin manifest)                observe (bounded)
   ┌─────────────────────┐               ┌────────────────────────┐
   │ rp plan → state.db  │               │ rp run / rp operate    │
   │  manifest, nonce,   │   journal     │  attach → barrier →    │
   │  opIds, digests     ├──────────────▶│  restart → gather →    │
   └─────────────────────┘   (steps)     │  stop → close → seal   │
                                         └──────────┬─────────────┘
                                                    │
   C guest harness (cooperating, disposable)        │
   rp_client ──deliver──▶ rp_worker ──commit──▶ receipt sink (HTTP)
        ▲                    │                     SQLite ledger
        │               rp_supervisor            keyed/plain adapter
        │               (owns one worker         epoch close + seal
        │                image; barrier +        coverage)
        │                generation control)
        └────── rediscover endpoint after restart
```

## Transports

- **Measured channel** (client↔worker): POSIX Unix sockets on the host
  fixture; QNX `name_attach`/`MsgSend` on a licensed target.
- **Control channel** (runner/operator↔supervisor): TCP everywhere,
  framed `[u32 len][struct]`, carrying the run nonce and a monotonic
  control generation. `RP_CTL_EVENTS` streams the guest's gathered
  evidence (events, client report, `uname`) to the operator bridge.

## Evidence model

- `state.db` — runs, append-only nonce-bound facts, quarantine, and a
  step journal that makes `run`/`collect`/`operate` resumable.
- `rp report` reduces only attributable effect rows + pinned
  cardinality + the sink's coverage seal into
  PASS / FAIL_DUPLICATE / FAIL_MISSING / REVIEW / SETUP (exit 0/1/1/2/3)
  and writes `evidence.json` — a portable bundle with credentials and
  private paths scrubbed.
- `rp console` renders the same bundles as an accessible static HTML
  page; every card names its boundary and target provenance.

## Honesty rules

- Host execution is labeled `HOST_FIXTURE`; a qnx-profile run is QNX
  evidence only when driven through `rp operate` against a licensed
  target, and is marked unrun otherwise.
- A withheld seal yields REVIEW, never an inferred count.
- The supervisor only ever signals its own registered worker child.
