"""M02 — Receipt sink.

An independent HTTP service owning the durable spool ledger (SQLite, WAL,
synchronous=FULL). The ledger lives outside worker write permissions: guest
workers only ever see a run-scoped writer token; the runner/viewer use a
separate read token; registration needs the lab admin token.

Adapters (FR-03):
  plain  — every accepted delivery appends one effect.
  keyed  — unique (run, opId) key checked in the SAME transaction as the
           effect commit (INV-02, FR-04): identical payload returns the
           original receipt; a different payload under the same key is
           rejected.

Writer boundary: POST /v1/runs/<id>/close closes the admission epoch and
drains in-flight writers (bounded). POST /v1/runs/<id>/seal then returns an
independently generated coverage seal over the closed sequence interval —
or empty=true with null bounds when the interval is truly empty.
"""

from __future__ import annotations

import json
import os
import secrets
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .errors import UsageError
from .util import digest_obj, utc_now

ADAPTER_VERSION = "rp.sink/1"
MAX_BODY = 64 * 1024

_SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS runs (
  run_id        TEXT PRIMARY KEY,
  run_nonce     TEXT NOT NULL,
  mode          TEXT NOT NULL CHECK (mode IN ('plain','keyed')),
  writer_token  TEXT NOT NULL,
  reader_token  TEXT NOT NULL,
  epoch_state   TEXT NOT NULL DEFAULT 'open',
  next_seq      INTEGER NOT NULL DEFAULT 1,
  created_utc   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS effects (
  run_id         TEXT NOT NULL,
  seq            INTEGER NOT NULL,
  op_id          TEXT NOT NULL,
  attempt_id     TEXT NOT NULL,
  payload_digest TEXT NOT NULL,
  payload_json   TEXT,
  received_utc   TEXT NOT NULL,
  PRIMARY KEY (run_id, seq)
);
CREATE TABLE IF NOT EXISTS keys (
  run_id         TEXT NOT NULL,
  op_id          TEXT NOT NULL,
  seq            INTEGER NOT NULL,
  payload_digest TEXT NOT NULL,
  PRIMARY KEY (run_id, op_id)
);
CREATE TABLE IF NOT EXISTS seals (
  run_id         TEXT PRIMARY KEY,
  payload_json   TEXT NOT NULL
);
"""


class Ledger:
    """Direct SQLite access, serialized internally for handler threads."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)) or ".",
                    exist_ok=True)
        self._conn = sqlite3.connect(db_path, timeout=30,
                                     check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()
        self._lock = threading.RLock()

    def close(self):
        self._conn.close()

    # -- registration -----------------------------------------------------

    def register_run(self, run_id: str, run_nonce: str, mode: str) -> dict:
        if mode not in ("plain", "keyed"):
            raise UsageError(f"sink mode must be plain or keyed, not {mode}")
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if row is not None:
                if row["run_nonce"] != run_nonce:
                    raise SinkReject(
                        403, "nonce_mismatch",
                        "run id registered under a different nonce")
                if row["mode"] != mode:
                    raise SinkReject(
                        409, "mode_mismatch",
                        "run id registered under a different mode")
                return {"writerToken": row["writer_token"],
                        "readerToken": row["reader_token"]}
            writer, reader = secrets.token_hex(16), secrets.token_hex(16)
            self._conn.execute(
                "INSERT INTO runs(run_id, run_nonce, mode, writer_token,"
                " reader_token, created_utc) VALUES (?,?,?,?,?,?)",
                (run_id, run_nonce, mode, writer, reader, utc_now()))
            self._conn.commit()
            return {"writerToken": writer, "readerToken": reader}

    def get_run(self, run_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            return dict(row) if row else None

    # -- delivery ----------------------------------------------------------

    def _receipt(self, run_id: str, seq: int, duplicate: bool) -> dict:
        return {"seq": seq, "duplicate": duplicate,
                "effectDigest": digest_obj({"runId": run_id, "seq": seq})}

    def deliver(self, run_id: str, op_id: str, attempt_id: str,
                payload_digest: str, payload_json: str | None):
        """Commit one delivery. Returns (receipt, duplicate_flag)."""
        with self._lock:
            # Re-read run state inside the lock: next_seq and epoch_state
            # may have moved since the handler fetched the row.
            run = self.get_run(run_id)
            if run["epoch_state"] != "open":
                raise SinkReject(409, "epoch_closed",
                                 "admission epoch is closed for this run")
            if run["mode"] == "plain":
                with self._conn:  # atomic
                    seq = run["next_seq"]
                    self._insert_effect(run_id, seq, op_id, attempt_id,
                                        payload_digest, payload_json)
                    self._conn.execute(
                        "UPDATE runs SET next_seq=? WHERE run_id=?",
                        (seq + 1, run_id))
                return self._receipt(run_id, seq, False), False

            # keyed: key ownership + effect commit in one transaction
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT seq, payload_digest FROM keys"
                    " WHERE run_id=? AND op_id=?",
                    (run_id, op_id)).fetchone()
                if row is not None:
                    if row["payload_digest"] != payload_digest:
                        raise SinkReject(
                            409, "payload_conflict",
                            f"opId {op_id!r} already owns a receipt for a "
                            "different payload digest")
                    self._conn.commit()
                    return self._receipt(run_id, row["seq"], True), True
                seq = run["next_seq"]
                self._insert_effect(run_id, seq, op_id, attempt_id,
                                    payload_digest, payload_json)
                self._conn.execute(
                    "INSERT INTO keys(run_id, op_id, seq, payload_digest)"
                    " VALUES (?,?,?,?)",
                    (run_id, op_id, seq, payload_digest))
                self._conn.execute(
                    "UPDATE runs SET next_seq=? WHERE run_id=?",
                    (seq + 1, run_id))
                self._conn.commit()
                return self._receipt(run_id, seq, False), False
            except Exception:
                self._conn.rollback()
                raise

    def _insert_effect(self, run_id, seq, op_id, attempt_id,
                       payload_digest, payload_json):
        self._conn.execute(
            "INSERT INTO effects(run_id, seq, op_id, attempt_id,"
            " payload_digest, payload_json, received_utc)"
            " VALUES (?,?,?,?,?,?,?)",
            (run_id, seq, op_id, attempt_id, payload_digest,
             payload_json, utc_now()))

    # -- epoch / seal -------------------------------------------------------

    def close_epoch(self, run_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE runs SET epoch_state='closed' WHERE run_id=?",
                (run_id,))
            self._conn.commit()

    def effect_rows(self, run_id: str, from_seq: int = 0,
                    limit: int = 500) -> tuple[list[dict], bool]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM effects WHERE run_id=? AND seq>=?"
                " ORDER BY seq LIMIT ?", (run_id, from_seq, limit + 1)
            ).fetchall()
        has_more = len(rows) > limit
        out = []
        for r in rows[:limit]:
            d = dict(r)
            d["effectDigest"] = digest_obj({"runId": run_id, "seq": d["seq"]})
            out.append(d)
        return out, has_more

    def get_seal(self, run_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT payload_json FROM seals WHERE run_id=?",
                (run_id,)).fetchone()
            return json.loads(row["payload_json"]) if row else None

    def seal(self, run_id: str, drained: bool) -> dict:
        """Generate (once) or return the run's coverage seal."""
        with self._lock:
            run = self.get_run(run_id)
            if run["epoch_state"] != "closed":
                raise SinkReject(409, "epoch_open",
                                 "close the admission epoch before sealing")
            if not drained:
                raise SinkReject(409, "writers_in_flight",
                                 "writer boundary has not drained")
            existing = self.get_seal(run_id)
            if existing is not None:
                return existing
            stats = self._conn.execute(
                "SELECT COUNT(*) c, MIN(seq) lo, MAX(seq) hi"
                " FROM effects WHERE run_id=?", (run_id,)).fetchone()
            count, lo, hi = stats["c"], stats["lo"], stats["hi"]
            complete = count == 0 or (hi - lo + 1 == count)
            seal = {
                "adapterVersion": ADAPTER_VERSION,
                "runId": run_id,
                "runNonce": run["run_nonce"],
                "firstSeq": lo if count else None,
                "lastSeq": hi if count else None,
                "count": count,
                "empty": count == 0,
                "coverage": "complete" if complete else "partial",
                "closedEpoch": True,
                "writerDrained": True,
                "sealedUtc": utc_now(),
            }
            seal["sealDigest"] = digest_obj(seal)
            self._conn.execute(
                "INSERT INTO seals(run_id, payload_json) VALUES (?,?)",
                (run_id, json.dumps(seal, sort_keys=True)))
            self._conn.commit()
            return seal


class SinkReject(Exception):
    def __init__(self, status: int, code: str, reason: str):
        super().__init__(reason)
        self.status, self.code, self.reason = status, code, reason


class SinkService:
    """HTTP boundary around the ledger with per-run credential checks."""

    def __init__(self, db_path: str, sink_id: str | None = None,
                 admin_token: str | None = None):
        self.ledger = Ledger(db_path)
        self.sink_id = sink_id or f"sink-{secrets.token_hex(4)}"
        self.admin_token = admin_token or secrets.token_hex(16)
        self._inflight = 0
        self._closing: set[str] = set()
        self._cond = threading.Condition()
        self.started = time.time()

    # -- writer-boundary accounting ----------------------------------------

    def _acquire_writer(self, run_id: str) -> bool:
        with self._cond:
            if run_id in self._closing:
                return False
            self._inflight += 1
            return True

    def _release_writer(self):
        with self._cond:
            self._inflight -= 1
            self._cond.notify_all()

    def close_and_drain(self, run_id: str, timeout_s: float = 10.0) -> dict:
        """Close the epoch, then wait (bounded) for in-flight writers."""
        with self._cond:
            self._closing.add(run_id)
        self.ledger.close_epoch(run_id)
        deadline = time.monotonic() + timeout_s
        with self._cond:
            while self._inflight > 0:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return {"closed": True, "drained": False}
                self._cond.wait(timeout=remaining)
        return {"closed": True, "drained": True}

    # -- HTTP ---------------------------------------------------------------

    def make_server(self, host: str, port: int) -> ThreadingHTTPServer:
        service = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt, *a):
                pass

            def _body(self) -> dict:
                length = int(self.headers.get("Content-Length") or 0)
                if length > MAX_BODY:
                    raise SinkReject(413, "too_large", "request body >64KB")
                raw = self.rfile.read(length) if length else b"{}"
                try:
                    return json.loads(raw.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    raise SinkReject(400, "bad_json", "body must be JSON")

            def _token(self) -> str:
                auth = self.headers.get("Authorization", "")
                return auth[7:] if auth.startswith("Bearer ") else ""

            def _run_checked(self, run_id: str, token_kind: str) -> dict:
                run = service.ledger.get_run(run_id)
                if run is None:
                    raise SinkReject(404, "unknown_run",
                                     f"no such run {run_id!r}")
                tok = self._token()
                if token_kind == "writer" and tok != run["writer_token"]:
                    raise SinkReject(403, "bad_token", "writer token required")
                if token_kind == "reader" and tok != run["reader_token"]:
                    raise SinkReject(403, "bad_token", "reader token required")
                if token_kind == "admin" and tok != service.admin_token:
                    raise SinkReject(403, "bad_token", "admin token required")
                return run

            def _send(self, status: int, obj: dict):
                data = json.dumps(obj).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def _route(self):
                try:
                    if self.command == "GET" and self.path == "/v1/health":
                        return self._send(200, {
                            "sinkId": service.sink_id,
                            "adapterVersion": ADAPTER_VERSION,
                            "uptimeS": round(time.time()
                                             - service.started, 3)})
                    if self.command == "POST" and self.path == "/v1/runs":
                        if self._token() != service.admin_token:
                            raise SinkReject(403, "bad_token",
                                             "admin token required")
                        b = self._body()
                        for f in ("runId", "runNonce", "mode"):
                            if not b.get(f):
                                raise SinkReject(400, "missing_field",
                                                 f"{f} required")
                        tokens = service.ledger.register_run(
                            b["runId"], b["runNonce"], b["mode"])
                        return self._send(200, {
                            "sinkId": service.sink_id,
                            "runId": b["runId"], **tokens})
                    parts = [p for p in self.path.split("?")[0].split("/")
                             if p]
                    # /v1/runs/<id>/(deliver|close|seal|effects)
                    if len(parts) >= 3 and parts[:2] == ["v1", "runs"]:
                        run_id, action = parts[2], parts[3] if len(
                            parts) > 3 else ""
                        if self.command == "POST" and action == "deliver":
                            run = self._run_checked(run_id, "writer")
                            b = self._body()
                            if b.get("runNonce") != run["run_nonce"]:
                                raise SinkReject(
                                    403, "nonce_mismatch",
                                    "delivery nonce does not match run")
                            for f in ("opId", "attemptId", "payloadDigest"):
                                if not b.get(f):
                                    raise SinkReject(400, "missing_field",
                                                     f"{f} required")
                            if b.get("payload") is not None and \
                                    digest_obj(b["payload"]) != \
                                    b["payloadDigest"]:
                                raise SinkReject(
                                    400, "digest_mismatch",
                                    "declared payloadDigest does not match "
                                    "the delivered payload")
                            if not service._acquire_writer(run_id):
                                raise SinkReject(409, "epoch_closed",
                                                 "epoch is closing")
                            try:
                                receipt, dup = service.ledger.deliver(
                                    run_id, b["opId"], b["attemptId"],
                                    b["payloadDigest"],
                                    json.dumps(b.get("payload"),
                                               sort_keys=True)
                                    if b.get("payload") is not None else None)
                            finally:
                                service._release_writer()
                            return self._send(200, {
                                "sinkId": service.sink_id,
                                "runId": run_id, "receipt": receipt})
                        if self.command == "POST" and action == "close":
                            self._run_checked(run_id, "reader")
                            result = service.close_and_drain(run_id)
                            return self._send(
                                200 if result["drained"] else 409,
                                {"sinkId": service.sink_id,
                                 "runId": run_id, **result})
                        if self.command == "POST" and action == "seal":
                            self._run_checked(run_id, "reader")
                            with service._cond:
                                drained = service._inflight == 0
                            seal = service.ledger.seal(run_id, drained)
                            seal["sinkId"] = service.sink_id
                            return self._send(200, {
                                "sinkId": service.sink_id, "seal": seal})
                        if self.command == "GET" and action == "seal":
                            self._run_checked(run_id, "reader")
                            seal = service.ledger.get_seal(run_id)
                            if seal is None:
                                raise SinkReject(404, "no_seal",
                                                 "run is not sealed")
                            seal["sinkId"] = service.sink_id
                            return self._send(200, {
                                "sinkId": service.sink_id, "seal": seal})
                        if self.command == "GET" and action == "effects":
                            self._run_checked(run_id, "reader")
                            qs = urllib.parse.parse_qs(
                                urllib.parse.urlsplit(self.path).query)
                            from_seq = int(qs.get("from_seq", [0])[0])
                            limit = min(int(qs.get("limit", [500])[0]), 500)
                            rows, more = service.ledger.effect_rows(
                                run_id, from_seq, limit)
                            return self._send(200, {
                                "sinkId": service.sink_id, "runId": run_id,
                                "effects": rows, "hasMore": more,
                                "nextFromSeq":
                                    rows[-1]["seq"] + 1 if rows else from_seq})
                        if self.command == "GET" and action == "":
                            run = self._run_checked(run_id, "reader")
                            return self._send(200, {
                                "sinkId": service.sink_id,
                                "runId": run_id, "mode": run["mode"],
                                "epochState": run["epoch_state"],
                                "effectCount": run["next_seq"] - 1})
                    raise SinkReject(404, "not_found", self.path)
                except SinkReject as exc:
                    self._send(exc.status, {"error": exc.code,
                                            "reason": exc.reason})
                except Exception as exc:  # boundary: never leak traceback
                    self._send(500, {"error": "internal",
                                     "reason": str(exc)})

            do_GET = _route
            do_POST = _route

        server = ThreadingHTTPServer((host, port), Handler)
        server.daemon_threads = True
        return server


# ---------------------------------------------------------------------------
# Host-side client helpers (used by the runner and by tests)


def http_json(url: str, payload: dict | None = None, token: str | None = None,
              timeout: float = 10.0) -> tuple[int, dict]:
    req = urllib.request.Request(url, method="POST" if payload is not None
                                 else "GET")
    req.add_header("Accept", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, data=data, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        try:
            return exc.code, json.loads(exc.read().decode("utf-8"))
        except Exception:
            return exc.code, {"error": "unparseable"}


def service_info_path(home: str) -> str:
    return os.path.join(home, "sink.json")


def write_service_info(home: str, info: dict) -> None:
    path = service_info_path(home)
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        os.chmod(tmp, 0o600)
        json.dump(info, fh, indent=2)
    os.replace(tmp, path)


def read_service_info(home: str) -> dict | None:
    try:
        with open(service_info_path(home)) as fh:
            return json.load(fh)
    except (OSError, json.JSONDecodeError):
        return None


def sink_alive(info: dict | None) -> bool:
    if not info:
        return False
    try:
        status, _ = http_json(f"http://127.0.0.1:{info['port']}/v1/health",
                              timeout=2.0)
        return status == 200
    except OSError:
        return False


def _sink_log_tail(home: str, lines: int = 15) -> str:
    try:
        with open(os.path.join(home, "sink.log"), "rb") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            fh.seek(max(0, size - 4096))
            tail = fh.read().decode("utf-8", "replace")
        return "\n".join(tail.splitlines()[-lines:])
    except OSError:
        return "(no sink.log)"


def ensure_sink(home: str, timeout_s: float = 30.0) -> dict:
    """Return info for the home's sink service, starting it if needed."""
    info = read_service_info(home)
    if sink_alive(info):
        return info
    import subprocess
    import sys
    os.makedirs(home, exist_ok=True)
    log_path = os.path.join(home, "sink.log")
    log = open(log_path, "ab")
    try:
        proc = subprocess.Popen(
            [sys.executable, "-m", "rp", "--home", home,
             "sink", "serve", "--port", "0"],
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    finally:
        log.close()
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        info = read_service_info(home)
        if info and info.get("pid") == proc.pid and sink_alive(info):
            return info
        if proc.poll() is not None:
            raise UsageError(
                f"sink service exited early (rc={proc.returncode}); "
                f"log tail:\n{_sink_log_tail(home)}")
        time.sleep(0.05)
    raise UsageError(f"sink service did not come up within {timeout_s}s; "
                     f"log tail:\n{_sink_log_tail(home)}")


def stop_sink(home: str) -> bool:
    """Stop the home's sink service. Returns True if one was stopped."""
    info = read_service_info(home)
    if not info or not info.get("pid"):
        return False
    import signal
    try:
        os.kill(info["pid"], signal.SIGTERM)
    except OSError:
        pass
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        try:
            os.kill(info["pid"], 0)
        except OSError:
            break
        time.sleep(0.05)
    try:
        os.unlink(service_info_path(home))
    except OSError:
        pass
    return True
