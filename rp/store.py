"""M01 — durable run store.

SQLite-backed store under the run home. Runs are keyed by an idempotent
client key (FR-01/T-001): starting a scenario twice with one client key
retrieves the same run and its pinned manifest across process restarts.

Facts are append-only. Any fact whose runNonce does not match the run's
pinned nonce is quarantined instead of attributed — distinct runs cannot
consume each other's receipts (INV-01).
"""

from __future__ import annotations

import json
import os
import sqlite3

from .errors import UsageError
from .util import canonical_json, utc_now

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
  run_id        TEXT PRIMARY KEY,
  client_key    TEXT UNIQUE,
  created_utc   TEXT NOT NULL,
  status        TEXT NOT NULL,
  diagnostic    TEXT,
  profile_id    TEXT,
  sink_mode     TEXT,
  scenario_id   TEXT,
  manifest_json TEXT
);
CREATE TABLE IF NOT EXISTS facts (
  seq           INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id        TEXT NOT NULL,
  kind          TEXT NOT NULL,
  payload_json  TEXT NOT NULL,
  recorded_utc  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS quarantine (
  seq           INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id        TEXT NOT NULL,
  kind          TEXT NOT NULL,
  reason        TEXT NOT NULL,
  payload_json  TEXT NOT NULL,
  recorded_utc  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS journal (
  run_id        TEXT NOT NULL,
  step          TEXT NOT NULL,
  control_generation INTEGER NOT NULL,
  state_json    TEXT,
  updated_utc   TEXT NOT NULL,
  PRIMARY KEY (run_id, step)
);
CREATE TABLE IF NOT EXISTS meta (
  k TEXT PRIMARY KEY,
  v TEXT
);
"""

RUN_STATUSES = ("planned", "running", "collected", "reported",
                "unsupported", "diagnostic")


def home_path(home: str) -> str:
    os.makedirs(home, exist_ok=True)
    return os.path.join(home, "state.db")


class Store:
    def __init__(self, home: str):
        self.home = home
        os.makedirs(home, exist_ok=True)
        self.db_path = os.path.join(home, "state.db")
        self._conn = sqlite3.connect(self.db_path, timeout=30)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ---- runs -----------------------------------------------------------

    def create_run(self, *, run_id: str, client_key: str, manifest: dict,
                   status: str = "planned", diagnostic: str | None = None):
        """Insert a run row; returns (run_row, created).

        A second call with the same client_key returns the existing run
        (T-001). If the same key is reused with a materially different
        request, that is an operator error, not a new run.
        """
        m = manifest or {}
        try:
            self._conn.execute(
                "INSERT INTO runs(run_id, client_key, created_utc, status,"
                " diagnostic, profile_id, sink_mode, scenario_id,"
                " manifest_json) VALUES (?,?,?,?,?,?,?,?,?)",
                (run_id, client_key, utc_now(), status, diagnostic,
                 m.get("profileId"), m.get("sinkMode"),
                 m.get("scenarioId"), json.dumps(manifest, sort_keys=True)))
            self._conn.commit()
            return self.get_run(run_id), True
        except sqlite3.IntegrityError:
            existing = self.get_by_client_key(client_key)
            if existing is None:  # run_id collision, vanishingly unlikely
                raise UsageError(f"run id collision: {run_id}")
            old = existing["manifest"]
            if manifest and old and (
                    old.get("scenarioDigest") != m.get("scenarioDigest")
                    or old.get("profileId") != m.get("profileId")
                    or old.get("sinkMode") != m.get("sinkMode")):
                raise UsageError(
                    f"client key {client_key!r} is already bound to run "
                    f"{existing['run_id']} with a different scenario/profile/"
                    "sink; use a new client key for a different request")
            return existing, False

    def _row(self, row) -> dict:
        out = dict(row)
        out["manifest"] = json.loads(out.pop("manifest_json") or "null")
        return out

    def get_run(self, run_id: str) -> dict | None:
        cur = self._conn.execute("SELECT * FROM runs WHERE run_id=?",
                                 (run_id,))
        row = cur.fetchone()
        return self._row(row) if row else None

    def get_by_client_key(self, client_key: str) -> dict | None:
        cur = self._conn.execute("SELECT * FROM runs WHERE client_key=?",
                                 (client_key,))
        row = cur.fetchone()
        return self._row(row) if row else None

    def list_runs(self) -> list[dict]:
        cur = self._conn.execute(
            "SELECT * FROM runs ORDER BY created_utc, run_id")
        return [self._row(r) for r in cur.fetchall()]

    def set_status(self, run_id: str, status: str,
                   diagnostic: str | None = None) -> None:
        if status not in RUN_STATUSES:
            raise UsageError(f"unknown run status {status!r}")
        self._conn.execute(
            "UPDATE runs SET status=?, diagnostic=COALESCE(?, diagnostic)"
            " WHERE run_id=?", (status, diagnostic, run_id))
        self._conn.commit()

    # ---- facts (append-only) -------------------------------------------

    def record_fact(self, run_id: str, kind: str, payload: dict) -> dict:
        """Append a fact. Facts with a foreign runNonce are quarantined."""
        run = self.get_run(run_id)
        if run is None:
            raise UsageError(f"unknown run {run_id}")
        manifest_nonce = (run.get("manifest") or {}).get("runNonce")
        fact_nonce = payload.get("runNonce")
        if manifest_nonce is not None and fact_nonce is not None \
                and fact_nonce != manifest_nonce:
            reason = (f"fact runNonce {fact_nonce} does not match run "
                      f"nonce {manifest_nonce}; quarantined (INV-01)")
            self._conn.execute(
                "INSERT INTO quarantine(run_id, kind, reason, payload_json,"
                " recorded_utc) VALUES (?,?,?,?,?)",
                (run_id, kind, reason,
                 canonical_json(payload).decode("utf-8"), utc_now()))
            self._conn.commit()
            return {"quarantined": True, "reason": reason}
        cur = self._conn.execute(
            "INSERT INTO facts(run_id, kind, payload_json, recorded_utc)"
            " VALUES (?,?,?,?)",
            (run_id, kind, canonical_json(payload).decode("utf-8"),
             utc_now()))
        self._conn.commit()
        return {"quarantined": False, "seq": cur.lastrowid}

    def facts(self, run_id: str, kind: str | None = None) -> list[dict]:
        if kind:
            cur = self._conn.execute(
                "SELECT * FROM facts WHERE run_id=? AND kind=? ORDER BY seq",
                (run_id, kind))
        else:
            cur = self._conn.execute(
                "SELECT * FROM facts WHERE run_id=? ORDER BY seq", (run_id,))
        out = []
        for row in cur.fetchall():
            d = dict(row)
            d["payload"] = json.loads(d.pop("payload_json"))
            out.append(d)
        return out

    def quarantined(self, run_id: str) -> list[dict]:
        cur = self._conn.execute(
            "SELECT * FROM quarantine WHERE run_id=? ORDER BY seq",
            (run_id,))
        out = []
        for row in cur.fetchall():
            d = dict(row)
            d["payload"] = json.loads(d.pop("payload_json"))
            out.append(d)
        return out

    # ---- journal ---------------------------------------------------------

    def journal_write(self, run_id: str, step: str,
                      control_generation: int, state: dict) -> None:
        self._conn.execute(
            "INSERT INTO journal(run_id, step, control_generation,"
            " state_json, updated_utc) VALUES (?,?,?,?,?)"
            " ON CONFLICT(run_id, step) DO UPDATE SET"
            " control_generation=excluded.control_generation,"
            " state_json=excluded.state_json, updated_utc=excluded.updated_utc",
            (run_id, step, control_generation,
             json.dumps(state, sort_keys=True), utc_now()))
        self._conn.commit()

    def journal_read(self, run_id: str) -> dict:
        cur = self._conn.execute(
            "SELECT * FROM journal WHERE run_id=? ORDER BY rowid", (run_id,))
        return {r["step"]: {"controlGeneration": r["control_generation"],
                            "state": json.loads(r["state_json"] or "null"),
                            "updatedUtc": r["updated_utc"]}
                for r in cur.fetchall()}

    def next_control_generation(self, run_id: str) -> int:
        cur = self._conn.execute(
            "SELECT COALESCE(MAX(control_generation),0)+1 AS g"
            " FROM journal WHERE run_id=?", (run_id,))
        return cur.fetchone()["g"]
