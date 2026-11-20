"""M04 — Runner and retry.

Drives a bounded run end to end: register at the sink, launch the guest
supervisor + client (host fixture), wait for the post-commit barrier,
send RESTART over the control channel, collect the durable evidence,
then stop the writer boundary and request the coverage seal (FR-07,
FR-08).

The journal in the run store makes every step idempotent: a restarted
collector re-enters the same machine and skips completed steps (T-007).
Collection NEVER sends an effect-producing command — no new client, no
new delivery (INV-04). A deferred RESTART is a control-plane completion
of the pinned scenario, not an operation.

Stale control generations are impossible to replay because every control
command carries a fresh generation drawn from the durable journal; the
supervisor independently rejects anything <= the last generation it saw
and journals ctl_reject (T-008).
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time

from . import ctlmsg
from .errors import UnsupportedError, UsageError
from .sink import ensure_sink, http_json
from .util import canonical_json, utc_now

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GUEST_BUILD = os.path.join(REPO_ROOT, "guest", "build")
GUEST_BIN = os.path.join(REPO_ROOT, "guest", "bin")

STEPS = ("sink.registered", "supervisor.started", "client.started",
         "barrier.observed", "restart.sent", "client.done",
         "supervisor.stopped", "evidence.imported", "epoch.closed",
         "seal.stored", "sink.snapshot")

# Bounded waits (NFR-01). None of these may block forever; the env
# overrides exist so the test suite can shrink the same bounds, not so
# callers can remove them.
def _env_s(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default


BARRIER_WAIT_S = _env_s("RP_BARRIER_WAIT_S", 30.0)
SUPERVISOR_UP_S = _env_s("RP_SUPERVISOR_UP_S", 15.0)
CLIENT_WAIT_S = _env_s("RP_CLIENT_WAIT_S", 120.0)
STOP_WAIT_S = _env_s("RP_STOP_WAIT_S", 5.0)
CTL_TIMEOUT_S = 10.0
CLIENT_TIMEOUT_MS = 8000


def run_dir(home: str, run_id: str) -> str:
    return os.path.join(home, "runs", run_id)


def endpoint_dir(run_id: str) -> str:
    """POSIX endpoint sockets need a SHORT path: struct sockaddr_un's
    sun_path is ~104 bytes and a home dir under $TMPDIR already exceeds
    it on macOS. /tmp is the documented POSIX endpoint root; on QNX the
    endpoint is a registered name, not a path (transport_qnx)."""
    return os.path.join("/tmp", f"rp-{run_id}")


def _read_events(rundir: str) -> list[dict]:
    """Tail events.jsonl; a partially flushed final line is skipped."""
    path = os.path.join(rundir, "events.jsonl")
    try:
        with open(path) as fh:
            lines = fh.read().splitlines()
    except OSError:
        return []
    out = []
    for line in lines:
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out


def _wait_file(path: str, timeout_s: float) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if os.path.exists(path):
            return True
        time.sleep(0.05)
    return os.path.exists(path)


def _wait_event(rundir: str, kind: str,
                timeout_s: float) -> dict | None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        for e in _read_events(rundir):
            if e.get("kind") == kind:
                return e
        time.sleep(0.05)
    return None


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


class Runner:
    def __init__(self, home: str, store):
        self.home = home
        self.store = store
        self._procs: dict[str, subprocess.Popen] = {}

    def _proc_alive(self, step: str, pid: int | None) -> bool:
        """Liveness that reaps zombies for OUR children (poll() reaps),
        and falls back to kill(pid,0) for pids adopted after a collector
        restart (orphans are reaped by init, so kill(0) is accurate)."""
        proc = self._procs.get(step)
        if proc is not None:
            return proc.poll() is None
        return pid is not None and _pid_alive(pid)

    # -- launch steps -------------------------------------------------------

    def _ensure_guest_bins(self):
        bins = [os.path.join(GUEST_BIN, b)
                for b in ("rp_worker", "rp_supervisor", "rp_client")]
        if all(os.path.isfile(b) for b in bins):
            return
        import shutil
        if not any(shutil.which(c)
                   for c in ("cc", "gcc", "clang", "make")):
            raise UnsupportedError(
                "no host C toolchain: cannot build the HOST_FIXTURE guest "
                "harness (make -C guest/build)")
        proc = subprocess.run(["make", "-C", GUEST_BUILD],
                              capture_output=True, text=True, timeout=180)
        if proc.returncode != 0:
            raise UsageError(
                f"guest build failed (rc={proc.returncode}):\n"
                f"{proc.stdout}{proc.stderr}")
        if not all(os.path.isfile(b) for b in bins):
            raise UsageError("guest build produced no binaries in guest/bin")

    def _register_sink(self, run) -> dict:
        info = ensure_sink(self.home)
        m = run["manifest"]
        st, body = http_json(
            f"http://127.0.0.1:{info['port']}/v1/runs",
            {"runId": run["run_id"], "runNonce": m["runNonce"],
             "mode": m["sinkMode"]},
            token=info["adminToken"])
        if st != 200:
            raise UsageError(f"sink registration failed ({st}): {body}")
        return {"sinkPort": info["port"], "sinkId": body["sinkId"],
                "writerToken": body["writerToken"],
                "readerToken": body["readerToken"]}

    def _spawn_supervisor(self, run, sink: dict) -> dict:
        m = run["manifest"]
        rd = run_dir(self.home, run["run_id"])
        ep = endpoint_dir(run["run_id"])
        os.makedirs(rd, exist_ok=True)
        os.makedirs(ep, mode=0o700, exist_ok=True)
        log = open(os.path.join(rd, "supervisor.log"), "wb")
        proc = subprocess.Popen([
            os.path.join(GUEST_BIN, "rp_supervisor"),
            "--rundir", rd,
            "--worker-image", os.path.join(GUEST_BIN, "rp_worker"),
            "--run", run["run_id"], "--nonce", m["runNonce"],
            "--sink", f"http://127.0.0.1:{sink['sinkPort']}",
            "--writer-token", sink["writerToken"],
            "--ctl-port", "0", "--endpoint-base", ep],
            stdin=subprocess.DEVNULL, stdout=log,
            stderr=subprocess.STDOUT, start_new_session=True)
        self._procs["supervisor.started"] = proc
        ctl_json = os.path.join(rd, "ctl.json")
        deadline = time.monotonic() + SUPERVISOR_UP_S
        while time.monotonic() < deadline:
            if os.path.exists(ctl_json):
                break
            if proc.poll() is not None:
                log.close()
                raise UsageError(
                    f"supervisor exited early (rc={proc.returncode}); "
                    f"see {rd}/supervisor.log")
            time.sleep(0.05)
        if not os.path.exists(ctl_json):
            log.close()
            raise UsageError("supervisor did not publish ctl.json within "
                             f"{SUPERVISOR_UP_S}s")
        log.close()
        with open(ctl_json) as fh:
            ctl = json.load(fh)
        return {"pid": proc.pid, "ctlPort": ctl["ctlPort"], "rundir": rd}

    def _spawn_client(self, run, hold_first: bool) -> dict:
        m = run["manifest"]
        rd = run_dir(self.home, run["run_id"])
        op = m["operations"][0]
        payload = op.get("payload")
        if payload is None:
            raise UsageError(
                "manifest does not pin the operation payload; re-plan the "
                "run so the launch payload is pinned verbatim")
        if len(canonical_json(payload)) > 512:
            raise UsageError("operation payload exceeds the 512-byte "
                             "wire contract")
        report = os.path.join(rd, "client_report.json")
        proc = subprocess.Popen([
            os.path.join(GUEST_BIN, "rp_client"),
            "--rundir", rd, "--run", run["run_id"],
            "--nonce", m["runNonce"], "--op", op["opId"],
            "--payload-json", json.dumps(payload, sort_keys=True),
            "--payload-digest", op["payloadDigest"],
            "--attempt-prefix", "att",
            "--max-attempts", str(m["retry"].get("maxAttempts", 2)),
            "--timeout-ms", str(CLIENT_TIMEOUT_MS),
            "--hold-first", "1" if hold_first else "0",
            "--report", report],
            stdin=subprocess.DEVNULL,
            stdout=open(os.path.join(rd, "client.log"), "wb"),
            stderr=subprocess.STDOUT, start_new_session=True)
        self._procs["client.started"] = proc
        return {"pid": proc.pid, "reportPath": report, "opId": op["opId"],
                "holdFirst": hold_first}

    # -- control plane ------------------------------------------------------

    def _ctl_port(self, rundir: str) -> int:
        with open(os.path.join(rundir, "ctl.json")) as fh:
            return json.load(fh)["ctlPort"]

    def _send_ctl(self, run, kind: int, detail: str) -> dict:
        """One bounded control request with a fresh journal generation."""
        rid = run["run_id"]
        rd = run_dir(self.home, rid)
        try:
            port = self._ctl_port(rd)
        except (OSError, json.JSONDecodeError):
            return {"sent": False, "reason": "ctl_unavailable",
                    "kind": detail}
        cg = self.store.next_control_generation(rid)
        msg = ctlmsg.pack_ctl(kind=kind, nonce=run["manifest"]["runNonce"],
                              control_generation=cg)
        try:
            reply = ctlmsg.ctl_request("127.0.0.1", port, msg,
                                       timeout=CTL_TIMEOUT_S)
        except (OSError, ValueError) as exc:
            return {"sent": False, "reason": f"ctl_error:{exc}",
                    "controlGeneration": cg, "kind": detail}
        return {"sent": True, "controlGeneration": cg, "reply": reply,
                "kind": detail}

    # -- the step machine -----------------------------------------------------

    def _mark(self, run_id: str, step: str, state: dict):
        cg = self.store.next_control_generation(run_id)
        self.store.journal_write(run_id, step, cg, state)

    def execute(self, run, hold_first: bool = True,
                stop_after: str | None = None) -> dict:
        """Drive launch steps, then observe. Idempotent via the journal."""
        rid = run["run_id"]
        m = run["manifest"] or {}
        if not m.get("runNonce"):
            raise UsageError(f"run {rid} has no pinned manifest")
        if m.get("target", {}).get("qnx"):
            raise UnsupportedError(
                "profile declares a QNX target; the host runner cannot "
                "drive a guest. Supply binaries to the guest and drive it "
                "per docs/QNX-TARGET.md (guest delegate: unverified)")

        j = self.store.journal_read(rid)
        if "seal.stored" in j:
            # idempotent relaunch on a collected run: no state regresses
            return {"runId": rid, "collected": True,
                    "rundir": run_dir(self.home, rid), "reused": True}
        self.store.set_status(rid, "running")
        if "sink.registered" not in j:
            self._mark(rid, "sink.registered", self._register_sink(run))
            j = self.store.journal_read(rid)
        if "supervisor.started" not in j:
            self._ensure_guest_bins()
            sup = self._spawn_supervisor(run, j["sink.registered"]["state"])
            self._mark(rid, "supervisor.started", sup)
            j = self.store.journal_read(rid)
        if stop_after == "supervisor.started":
            return {"runId": rid, "stoppedAfter": stop_after}
        if "client.started" not in j:
            self._mark(rid, "client.started",
                       self._spawn_client(run, hold_first))
        if stop_after == "client.started":
            return {"runId": rid, "stoppedAfter": stop_after}
        return self._observe(run, stop_after=stop_after)

    def collect(self, run, stop_after: str | None = None) -> dict:
        """Reattach to an existing run; launch steps are never repeated.

        No effect-producing command is ever sent here (INV-04): collection
        only observes files, may complete a pending control-plane RESTART
        required by the pinned scenario, and imports + seals evidence.
        """
        rid = run["run_id"]
        j = self.store.journal_read(rid)
        if "supervisor.started" not in j:
            raise UsageError(
                f"run {rid} was never launched; nothing to collect")
        return self._observe(run, stop_after=stop_after)

    def _observe(self, run, stop_after: str | None = None) -> dict:
        rid = run["run_id"]
        rd = run_dir(self.home, rid)
        nonce = run["manifest"]["runNonce"]
        j = self.store.journal_read(rid)
        done = lambda s: s in j  # noqa: E731

        if not done("barrier.observed"):
            ev = _wait_event(rd, "barrier", BARRIER_WAIT_S)
            if ev is None:
                self._mark(rid, "barrier.observed",
                           {"observed": False,
                            "reason": f"no barrier within {BARRIER_WAIT_S}s"})
            else:
                self._mark(rid, "barrier.observed",
                           {"observed": True, **ev})
            j = self.store.journal_read(rid)

        if stop_after == "barrier.observed":
            return {"runId": rid, "stoppedAfter": stop_after}

        if not done("restart.sent"):
            barrier = j.get("barrier.observed", {}).get("state") or {}
            if barrier.get("observed"):
                self._mark(rid, "restart.sent",
                           self._send_ctl(run, ctlmsg.CTL_RESTART,
                                          "restart"))
            else:
                self._mark(rid, "restart.sent",
                           {"sent": False,
                            "reason": "no barrier observed; interruption "
                                      "cannot be claimed (T-006)"})
            j = self.store.journal_read(rid)

        if stop_after == "restart.sent":
            return {"runId": rid, "stoppedAfter": stop_after}

        if not done("client.done"):
            state = j.get("client.started", {}).get("state") or {}
            report = state.get("reportPath",
                               os.path.join(rd, "client_report.json"))
            pid = state.get("pid")
            deadline = time.monotonic() + CLIENT_WAIT_S
            while (time.monotonic() < deadline
                   and not os.path.exists(report)
                   and self._proc_alive("client.started", pid)):
                time.sleep(0.1)
            present = os.path.exists(report)
            self._mark(rid, "client.done",
                       {"reportPath": report, "reportPresent": present,
                        "clientAlive":
                            self._proc_alive("client.started", pid)})
            j = self.store.journal_read(rid)

        if not done("supervisor.stopped"):
            self._mark(rid, "supervisor.stopped",
                       self._send_ctl(run, ctlmsg.CTL_STOP, "stop"))
            j = self.store.journal_read(rid)

        if stop_after == "supervisor.stopped":
            return {"runId": rid, "stoppedAfter": stop_after}

        # -- import: append-only facts from the durable journal files ----
        # drain: the supervisor journals its final events while exiting;
        # wait (bounded) for it to die so the event stream is final and
        # a re-collect sees no late facts (INV-04)
        sup_state = (j.get("supervisor.started", {}).get("state") or {})
        sup_pid = sup_state.get("pid")
        deadline = time.monotonic() + STOP_WAIT_S
        while (time.monotonic() < deadline
               and self._proc_alive("supervisor.started", sup_pid)):
            time.sleep(0.05)

        state = j.get("evidence.imported", {}).get("state") or {}
        through = int(state.get("through", 0))
        events = _read_events(rd)
        for e in events[through:]:
            self.store.record_fact(rid, f"guest.{e.get('kind', '?')}",
                                   {"runNonce": nonce, **e})
        through = len(events)
        report_path = os.path.join(rd, "client_report.json")
        report = None
        if not state.get("reportImported") and os.path.exists(report_path):
            try:
                with open(report_path) as fh:
                    report = json.load(fh)
                self.store.record_fact(rid, "client.report",
                                       {"runNonce": nonce, **report})
            except (OSError, json.JSONDecodeError):
                pass
        self._mark(rid, "evidence.imported",
                   {"through": through, "reportImported": report is not None})
        j = self.store.journal_read(rid)

        finish_collection(self.store, run, self._mark)
        return {"runId": rid, "collected": True,
                "rundir": rd, "collectedUtc": utc_now()}


def finish_collection(store, run, mark) -> None:
    """Writer boundary closed, sealed, snapshotted (FR-08).

    Shared tail of local (runner) and operator-bridge collection; all
    steps are journal-guarded so re-entry never repeats them.
    """
    rid = run["run_id"]
    nonce = run["manifest"]["runNonce"]
    j = store.journal_read(rid)

    # a step marked unreachable is recorded (for evidence) but retried on
    # the next collection pass — transient sink loss must not freeze a
    # withheld seal into the run forever
    def done(step: str) -> bool:
        state = (j.get(step) or {}).get("state") or {}
        return step in j and state.get("reachable", True) is not False

    sink = j["sink.registered"]["state"]

    if not done("epoch.closed"):
        try:
            st, body = http_json(
                f"http://127.0.0.1:{sink['sinkPort']}"
                f"/v1/runs/{rid}/close", {},
                token=sink["readerToken"])
            mark(rid, "epoch.closed", {"http": st, **body})
        except OSError as exc:
            # a withheld boundary is evidence too — record the failure,
            # never a guessed seal (T-013)
            mark(rid, "epoch.closed",
                 {"http": 0, "reachable": False, "error": str(exc)})
        j = store.journal_read(rid)
    if not done("seal.stored"):
        try:
            st, body = http_json(
                f"http://127.0.0.1:{sink['sinkPort']}"
                f"/v1/runs/{rid}/seal", {},
                token=sink["readerToken"])
            mark(rid, "seal.stored", {"http": st, **body})
        except OSError as exc:
            mark(rid, "seal.stored",
                 {"http": 0, "reachable": False, "error": str(exc)})
        j = store.journal_read(rid)

    # durable effect snapshot: the reducer can work offline and a
    # second read of the same row is deduped, not double-counted
    if not done("sink.snapshot"):
        try:
            rows, more, from_seq = [], True, 0
            st = 0
            while more:
                st, body = http_json(
                    f"http://127.0.0.1:{sink['sinkPort']}"
                    f"/v1/runs/{rid}/effects"
                    f"?from_seq={from_seq}&limit=500",
                    token=sink["readerToken"])
                if st != 200:
                    break
                rows.extend(body.get("effects", []))
                more = body.get("hasMore", False)
                from_seq = body.get("nextFromSeq", from_seq + 500)
            if st == 200:
                store.record_fact(
                    rid, "sink.effects",
                    {"runNonce": nonce, "sinkId": sink["sinkId"],
                     "effects": rows})
            mark(rid, "sink.snapshot",
                 {"reachable": True, "http": st, "rowCount": len(rows)})
        except OSError as exc:
            mark(rid, "sink.snapshot",
                 {"reachable": False, "error": str(exc)})

    store.set_status(rid, "collected")
