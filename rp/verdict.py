"""M05 — Verdict and evidence.

Derives the classification ONLY from attributable effect records, the
pinned expected cardinality, and the sink's independent coverage seal
(FR-09). A lost IPC reply, an ACK, or a return code is never evidence of
an effect by itself (INV-03). A complete report may still be FAIL or
REVIEW (INV-05) — the verdict reasons are exported with the bundle
(FR-10).

Dedup/conflict rules (T-010):
  * the same (sinkId, runId, seq) read twice counts once;
  * same key, different content => conflicting copies are quarantined
    and that seq is excluded from trustworthy attribution;
  * conflicts that could change the classification => REVIEW; conflicts
    under an already-determined FAIL => FAIL, observation marked
    incomplete.

CI exit mapping: PASS 0, FAIL 1, REVIEW 2, SETUP/unsupported 3.
"""

from __future__ import annotations

import json
import os

from . import __version__, SCHEMA_VERSION
from .errors import UsageError
from .runner import run_dir
from .util import digest_obj, utc_now

BUNDLE_VERSION = 1

VERDICT_PASS = "PASS"
VERDICT_FAIL_DUPLICATE = "FAIL_DUPLICATE"
VERDICT_FAIL_MISSING = "FAIL_MISSING"
VERDICT_REVIEW = "REVIEW"
VERDICT_SETUP = "SETUP"

_EXIT = {VERDICT_PASS: 0, VERDICT_FAIL_DUPLICATE: 1,
         VERDICT_FAIL_MISSING: 1, VERDICT_REVIEW: 2, VERDICT_SETUP: 3}


def _row_key(row: dict) -> tuple:
    return (row.get("sinkId"), row.get("run_id"), row.get("seq"))


def _row_content(row: dict) -> str:
    return digest_obj({k: v for k, v in row.items()
                       if k not in ("received_utc", "_source")})


def dedup_observations(observations: list[dict]) -> dict:
    """Group observations by (sinkId, runId, seq).

    Returns {trustworthy: [rows], copies: int, conflicted: {seq: rows}}.
    Rows differing only in receivedUtc are the same observation.
    """
    groups: dict[tuple, list[dict]] = {}
    for row in observations:
        groups.setdefault(_row_key(row), []).append(row)
    trustworthy, conflicted, copies = [], {}, 0
    for key, rows in groups.items():
        contents = {_row_content(r) for r in rows}
        if len(contents) == 1:
            trustworthy.append(rows[0])
            copies += len(rows) - 1
        else:
            conflicted[key] = rows
    trustworthy.sort(key=lambda r: (r.get("seq") or 0))
    return {"trustworthy": trustworthy, "copies": copies,
            "conflicted": conflicted}


def reduce_verdict(run: dict, journal: dict, facts: list[dict],
                   observations: list[dict]) -> dict:
    """Classify one run. Returns the verdict sub-object + analysis."""
    m = run.get("manifest") or {}
    rid = run["run_id"]
    ops = m.get("operations") or []

    if run.get("status") in ("unsupported", "diagnostic") \
            or not m.get("runNonce"):
        return {
            "verdict": {"code": VERDICT_SETUP,
                        "reason": run.get("diagnostic")
                                  or "run has no pinned manifest",
                        "incomplete": False,
                        "exitCode": _EXIT[VERDICT_SETUP]},
            "analysis": {}}

    # ---- attributable effect set ----------------------------------------
    ded = dedup_observations(observations)
    attributable = [r for r in ded["trustworthy"]
                    if r.get("run_id") == rid]
    conflicted = {k: v for k, v in ded["conflicted"].items()
                  if k[1] == rid}

    # ---- coverage -------------------------------------------------------
    seal = ((journal.get("seal.stored") or {}).get("state") or {}) \
        .get("seal")
    coverage_complete = bool(
        seal and seal.get("closedEpoch") and seal.get("writerDrained")
        and seal.get("coverage") == "complete")

    # ---- required scenario steps ---------------------------------------
    client = (journal.get("client.started") or {}).get("state") or {}
    hold_required = bool(client.get("holdFirst", True))
    barrier = (journal.get("barrier.observed") or {}).get("state") or {}
    restart = (journal.get("restart.sent") or {}).get("state") or {}
    steps_ok = (not hold_required) or (
        barrier.get("observed") and restart.get("sent"))
    report_fact = next((f["payload"] for f in facts
                        if f["kind"] == "client.report"), None)
    attempts = (report_fact or {}).get("attempts", [])

    incomplete_reasons = []
    if not coverage_complete:
        incomplete_reasons.append(
            "no complete closed coverage seal" if not seal else
            f"coverage={seal.get('coverage')} "
            f"drained={seal.get('writerDrained')}")
    if hold_required and not steps_ok:
        incomplete_reasons.append("required barrier/restart not observed")
    if conflicted:
        incomplete_reasons.append(
            f"{len(conflicted)} conflicting receipt cop"
            f"{'ies' if len(conflicted) != 1 else 'y'} quarantined")

    out = {"verdict": None,
           "analysis": {
               "attributable": attributable,
               "conflicted": {f"{k[0]}:{k[2]}": v
                              for k, v in conflicted.items()},
               "duplicateReads": ded["copies"],
               "seal": seal, "coverageComplete": coverage_complete,
               "stepsOk": steps_ok, "attempts": attempts}}

    def verdict(code, reason, incomplete=False):
        out["verdict"] = {"code": code, "reason": reason,
                          "incomplete": incomplete,
                          "exitCode": _EXIT[code]}
        return out

    for op in ops:
        expected = op["expectedCount"]
        n = sum(1 for r in attributable
                if r.get("op_id") == op["opId"])
        if n > expected:
            return verdict(
                VERDICT_FAIL_DUPLICATE,
                f"{n} distinct attributable effects for op {op['opId']} "
                f"(expected {expected})", bool(incomplete_reasons))
        if n == expected:
            if conflicted:
                return verdict(
                    VERDICT_REVIEW,
                    "conflicting receipt copies could hide additional "
                    f"effects for {op['opId']}; verdict compromised", True)
            if coverage_complete and steps_ok:
                return verdict(
                    VERDICT_PASS,
                    f"exactly one attributable effect for {op['opId']} "
                    "inside a complete sealed interval with the required "
                    "barrier/restart evidence")
            return verdict(
                VERDICT_REVIEW,
                "effect count matches but evidence is incomplete: "
                + "; ".join(incomplete_reasons), True)
        if n == 0:
            if conflicted:
                return verdict(
                    VERDICT_REVIEW,
                    "no attributable effects but conflicting copies were "
                    "quarantined; missingness cannot be claimed", True)
            if (seal and seal.get("empty") and coverage_complete
                    and attempts):
                return verdict(
                    VERDICT_FAIL_MISSING,
                    "execution produced zero attributable effects inside "
                    "an authenticated closed empty interval")
            return verdict(
                VERDICT_REVIEW,
                "no attributable effects and no trustworthy sealed "
                "evidence of an empty interval"
                + (": " + "; ".join(incomplete_reasons)
                   if incomplete_reasons else ""), True)
        # 0 < n < expected: fewer than declared, more than zero
        return verdict(
            VERDICT_REVIEW,
            f"{n} attributable effects for {op['opId']} is below the "
            f"expected {expected}; cannot distinguish missing from "
            "incomplete coverage", True)

    return verdict(VERDICT_REVIEW, "manifest declares no operations",
                   True)


def _observations(store, run, journal) -> tuple[list[dict], bool]:
    """Effect rows from durable snapshots + a live reader-token read.

    A second read of the same (sinkId, runId, seq) is a copy, not a new
    effect. Returns (rows, live_read_succeeded).
    """
    from .sink import http_json, sink_alive, read_service_info
    rid = run["run_id"]
    rows: list[dict] = []
    for f in store.facts(rid, "sink.effects"):
        for row in (f["payload"].get("effects") or []):
            row = dict(row)
            row.setdefault("sinkId", f["payload"].get("sinkId"))
            row.setdefault("run_id", rid)
            row["_source"] = "snapshot"
            rows.append(row)
    live = False
    sink = (journal.get("sink.registered") or {}).get("state") or {}
    info = read_service_info(store.home)
    port = sink.get("sinkPort") or (info or {}).get("port")
    if port and sink_alive({"port": port}):
        from_seq, more = 0, True
        while more:
            try:
                st, body = http_json(
                    f"http://127.0.0.1:{port}/v1/runs/{rid}/effects"
                    f"?from_seq={from_seq}&limit=500",
                    token=sink.get("readerToken"))
            except OSError:
                break
            if st != 200:
                break
            live = True
            for row in body.get("effects", []):
                row = dict(row)
                row.setdefault("sinkId", sink.get("sinkId"))
                row.setdefault("run_id", rid)
                row["_source"] = "live"
                rows.append(row)
            more = body.get("hasMore", False)
            from_seq = body.get("nextFromSeq", from_seq + 500)
    return rows, live


def build_bundle(store, run) -> dict:
    """Assemble the portable evidence bundle (FR-10)."""
    rid = run["run_id"]
    m = run.get("manifest") or {}
    journal = store.journal_read(rid)
    facts = store.facts(rid)
    observations, live = _observations(store, run, journal)
    reduced = reduce_verdict(run, journal, facts, observations)
    a = reduced["analysis"]
    target = m.get("target") or {}
    qnx = bool(target.get("qnx"))
    report_fact = next((f["payload"] for f in facts
                        if f["kind"] == "client.report"), None)

    bundle = {
        "bundleVersion": BUNDLE_VERSION,
        "schemaVersion": SCHEMA_VERSION,
        "generatedUtc": utc_now(),
        "runId": rid,
        "manifestDigest": m.get("manifestDigest"),
        "runNonce": m.get("runNonce"),
        "target": target,
        "provenance": target.get("label") or (
            "qnx" if qnx else "HOST_FIXTURE"),
        "hostFixture": not qnx,
        "effectBoundary": m.get("effectBoundary"),
        "operations": [
            {"opId": o["opId"], "expectedCount": o["expectedCount"],
             "payloadDigest": o["payloadDigest"],
             "attempts": (report_fact or {}).get("attempts", [])}
            for o in (m.get("operations") or [])],
        "effects": {
            "attributableCount": len(a.get("attributable", [])),
            "attributable": a.get("attributable", []),
            "duplicateReadsIgnored": a.get("duplicateReads", 0),
            "conflicted": a.get("conflicted", {})},
        "coverage": a.get("seal"),
        "controlEvidence": {
            "barrier": (journal.get("barrier.observed") or {})
                .get("state"),
            "restart": (journal.get("restart.sent") or {}).get("state"),
            "stepsOk": a.get("stepsOk")},
        "thresholds": {
            "maxAttempts": (m.get("retry") or {}).get("maxAttempts"),
            "expectedCardinality": [
                {"opId": o["opId"], "count": o["expectedCount"]}
                for o in (m.get("operations") or [])]},
        "builds": {"manifest": m.get("buildDigests"),
                   "rpVersion": __version__},
        "evidenceSources": {
            "sinkSnapshotFacts": len(store.facts(rid, "sink.effects")),
            "liveSinkRead": live},
        "journal": {k: v["state"] for k, v in journal.items()},
        "quarantine": store.quarantined(rid),
        "verdict": reduced["verdict"],
    }
    bundle["bundleDigest"] = digest_obj(
        {k: v for k, v in bundle.items() if k != "bundleDigest"})
    return bundle


def _write_json(path: str, bundle: dict) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        os.chmod(tmp, 0o600)
        json.dump(bundle, fh, indent=2, sort_keys=True)
    os.replace(tmp, path)


def report(store, run, out_path: str | None = None) -> dict:
    """Build + persist the bundle, return the verdict sub-object."""
    bundle = build_bundle(store, run)
    path = out_path or os.path.join(
        run_dir(store.home, run["run_id"]), "evidence.json")
    _write_json(path, bundle)
    v = dict(bundle["verdict"])
    v["bundlePath"] = path
    v["bundleDigest"] = bundle["bundleDigest"]
    status = "reported" if v["code"] != VERDICT_SETUP else run["status"]
    store.set_status(run["run_id"], status)
    return v
