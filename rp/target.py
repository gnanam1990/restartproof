"""M06 — Operator bridge: drive an expressly started, registered
cooperating harness from outside the runner.

This is the QNX operator path: the operator builds the guest harness
with the QNX SDP, copies rp_supervisor/rp_worker/rp_client to the
licensed target, starts them per docs/QNX-TARGET.md, and this module
performs the observe phase over the TCP control channel — nonce
handshake, bounded barrier wait, restart, evidence gather
(RP_CTL_EVENTS or a synced guest dir), stop, seal, snapshot.

The identical orchestration is exercisable against the host fixture:
spawn the same binaries locally and point --ctl at their control port.
Every displayed verdict still names its provenance — a qnx-profile run
reported through this path is a real target run (with the guest's own
uname captured); a host-fixture run stays HOST_FIXTURE. Absent a
licensed target the profile reports setup/unsupported (T-012).

Control stays limited to the owned child and owned spool: the ctl
handshake carries the run nonce and monotonic control generations, and
the bridge only seals the run the caller expressly registered.
"""

from __future__ import annotations

import json
import os
import time

from . import ctlmsg
from .errors import UsageError
from .runner import (BARRIER_WAIT_S, CLIENT_WAIT_S, CTL_TIMEOUT_S,
                     STOP_WAIT_S, Runner, finish_collection, run_dir)
from .sink import http_json
from .util import utc_now

EVENTS_POLL_S = float(os.environ.get("RP_EVENTS_POLL_S", "0.5"))

# journal steps owned by the bridge (the launch steps — sink.registered,
# operate.attached — plus the shared observe/collect step names)
BRIDGE_STEPS = ("operate.attached",)


class Operator:
    """Attach to an expressly started registered harness and collect."""

    def __init__(self, home: str, store):
        self.home = home
        self.store = store

    def _mark(self, run_id: str, step: str, state: dict):
        cg = self.store.next_control_generation(run_id)
        self.store.journal_write(run_id, step, cg, state)

    # -- registration -------------------------------------------------------

    def register(self, run) -> dict:
        """Allocate the run's sink registration; return launch params.

        The writer token is handed to the target's supervisor/worker by
        the operator — it stays in the private journal, never in the
        exported evidence bundle.
        """
        rid = run["run_id"]
        m = run["manifest"] or {}
        if not m.get("runNonce"):
            raise UsageError(f"run {rid} has no pinned manifest")
        j = self.store.journal_read(rid)
        existed = "sink.registered" in j
        if not existed:
            sink = Runner(self.home, self.store)._register_sink(run)
            self._mark(rid, "sink.registered", sink)
            j = self.store.journal_read(rid)
        sink = j["sink.registered"]["state"]
        return {
            "runId": rid,
            "runNonce": m["runNonce"],
            "sinkUrl": f"http://<host-reachable>:{sink['sinkPort']}",
            "writerToken": sink["writerToken"],
            "sinkMode": m["sinkMode"],
            "opId": m["operations"][0]["opId"],
            "payloadDigest": m["operations"][0]["payloadDigest"],
            "payload": m["operations"][0].get("payload"),
            "maxAttempts": m["retry"].get("maxAttempts", 2),
            "alreadyRegistered": existed,
        }

    # -- ctl helpers ----------------------------------------------------------

    def _ctl(self, run, kind: int, ctl_host: str, ctl_port: int,
             detail: str) -> dict:
        rid = run["run_id"]
        cg = self.store.next_control_generation(rid)
        msg = ctlmsg.pack_ctl(
            kind=kind, nonce=run["manifest"]["runNonce"],
            control_generation=cg)
        try:
            reply = ctlmsg.ctl_request(ctl_host, ctl_port, msg,
                                       timeout=CTL_TIMEOUT_S)
        except (OSError, ValueError) as exc:
            return {"sent": False, "reason": f"ctl_error:{exc}",
                    "controlGeneration": cg, "kind": detail}
        return {"sent": True, "controlGeneration": cg, "reply": reply,
                "kind": detail}

    def _gather(self, run, ctl_host: str, ctl_port: int,
                guest_dir: str | None) -> dict | None:
        """One evidence read: ctl RP_CTL_EVENTS, or a synced guest dir."""
        if guest_dir:
            events = []
            try:
                with open(os.path.join(guest_dir, "events.jsonl")) as fh:
                    for line in fh.read().splitlines():
                        try:
                            events.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue
            except OSError:
                pass
            report = None
            try:
                with open(os.path.join(guest_dir,
                                       "client_report.json")) as fh:
                    report = json.load(fh)
            except (OSError, json.JSONDecodeError):
                pass
            return {"uname": None, "events": events,
                    "clientReport": report}
        try:
            got = ctlmsg.ctl_request_events(
                ctl_host, ctl_port, run["manifest"]["runNonce"],
                timeout=CTL_TIMEOUT_S)
        except (OSError, ValueError, json.JSONDecodeError):
            return None
        if got["reply"]["status"] != ctlmsg.CTL_OK:
            return None
        return got["evidence"]

    def _wait_evidence(self, run, ctl_host, ctl_port, guest_dir,
                       predicate, timeout_s: float):
        """Poll the gather channel until predicate(evidence) or deadline."""
        deadline = time.monotonic() + timeout_s
        last = None
        while time.monotonic() < deadline:
            ev = self._gather(run, ctl_host, ctl_port, guest_dir)
            if ev is not None:
                last = ev
                hit = predicate(ev)
                if hit:
                    return hit
            time.sleep(EVENTS_POLL_S)
        return None

    # -- the operate step machine ------------------------------------------

    def operate(self, run, ctl_host: str, ctl_port: int,
                guest_dir: str | None = None,
                stop_after: str | None = None) -> dict:
        """Drive barrier/restart/collect against an attached harness."""
        rid = run["run_id"]
        m = run["manifest"] or {}
        nonce = m.get("runNonce")
        if not nonce:
            raise UsageError(f"run {rid} has no pinned manifest")
        j = self.store.journal_read(rid)
        if "sink.registered" not in j:
            raise UsageError(
                f"run {rid} is not registered with a sink; run "
                f"`rp operate {rid} --register` first")
        rd = run_dir(self.home, rid)
        os.makedirs(rd, exist_ok=True)

        # -- attach: prove this ctl endpoint belongs to THIS run (T-013) --
        if "operate.attached" not in j:
            try:
                status = ctlmsg.ctl_request(
                    ctl_host, ctl_port,
                    ctlmsg.pack_ctl(kind=ctlmsg.CTL_STATUS, nonce=nonce),
                    timeout=CTL_TIMEOUT_S)
            except (OSError, ValueError) as exc:
                self._mark(rid, "operate.attached",
                           {"attached": False, "reason": str(exc)})
                raise UsageError(
                    f"ctl endpoint {ctl_host}:{ctl_port} unreachable: "
                    f"{exc}")
            if status["status"] != ctlmsg.CTL_OK:
                self._mark(rid, "operate.attached",
                           {"attached": False,
                            "status": status["status"],
                            "detail": status.get("detail")})
                raise UsageError(
                    "control endpoint rejected this run's nonce "
                    f"(status {status['status']}: {status.get('detail')})"
                    " — refusing to drive a child outside the run "
                    "registry")
            ev = self._gather(run, ctl_host, ctl_port, guest_dir)
            self._mark(rid, "operate.attached",
                       {"attached": True, "ctlPort": ctl_port,
                        "generation": status["generation"],
                        "targetUname": (ev or {}).get("uname"),
                        "via": "guest-dir" if guest_dir else "ctl"})
            j = self.store.journal_read(rid)
        if stop_after == "operate.attached":
            return {"runId": rid, "stoppedAfter": stop_after}

        done = lambda s: s in j  # noqa: E731

        # -- barrier -------------------------------------------------------
        if not done("barrier.observed"):
            hit = self._wait_evidence(
                run, ctl_host, ctl_port, guest_dir,
                lambda ev: next((e for e in ev.get("events", [])
                                 if e.get("kind") == "barrier"), None),
                BARRIER_WAIT_S)
            if hit is None:
                self._mark(rid, "barrier.observed",
                           {"observed": False,
                            "reason": f"no barrier within "
                                      f"{BARRIER_WAIT_S}s"})
            else:
                self._mark(rid, "barrier.observed",
                           {"observed": True, **hit})
            j = self.store.journal_read(rid)

        # -- restart -------------------------------------------------------
        if not done("restart.sent"):
            barrier = j.get("barrier.observed", {}).get("state") or {}
            if barrier.get("observed"):
                self._mark(rid, "restart.sent",
                           self._ctl(run, ctlmsg.CTL_RESTART,
                                     ctl_host, ctl_port, "restart"))
            else:
                self._mark(rid, "restart.sent",
                           {"sent": False,
                            "reason": "no barrier observed; interruption "
                                      "cannot be claimed (T-006)"})
            j = self.store.journal_read(rid)

        # -- client report ---------------------------------------------------
        last_evidence = None
        if not done("client.done"):
            hit = self._wait_evidence(
                run, ctl_host, ctl_port, guest_dir,
                lambda ev: ev.get("clientReport"), CLIENT_WAIT_S)
            self._mark(rid, "client.done",
                       {"reportPresent": hit is not None})
            j = self.store.journal_read(rid)

        # -- supervisor stop ---------------------------------------------------
        if not done("supervisor.stopped"):
            # snapshot evidence BEFORE stop: the ctl endpoint dies with
            # the supervisor, so ctl-mode gathers must happen first
            last_evidence = self._gather(run, ctl_host, ctl_port,
                                         guest_dir)
            self._mark(rid, "supervisor.stopped",
                       self._ctl(run, ctlmsg.CTL_STOP,
                                 ctl_host, ctl_port, "stop"))
            j = self.store.journal_read(rid)

        # drain: let the supervisor write its final events, bounded
        time.sleep(min(0.2, STOP_WAIT_S))

        # -- import ------------------------------------------------------------
        if not done("evidence.imported"):
            state = j.get("evidence.imported", {}).get("state") or {}
            through = int(state.get("through", 0))
            # prefer a post-stop gather (guest-dir sees final events),
            # else the pre-stop snapshot (ctl is dead by now)
            ev = (self._gather(run, ctl_host, ctl_port, guest_dir)
                  or last_evidence or {})
            events = ev.get("events", [])
            for e in events[through:]:
                self.store.record_fact(rid, f"guest.{e.get('kind', '?')}",
                                       {"runNonce": nonce, **e})
            report = ev.get("clientReport")
            if report is not None and not state.get("reportImported"):
                self.store.record_fact(rid, "client.report",
                                       {"runNonce": nonce, **report})
                report_path = os.path.join(rd, "client_report.json")
                with open(report_path, "w") as fh:
                    os.chmod(report_path, 0o600)
                    json.dump(report, fh, indent=2, sort_keys=True)
            self._mark(rid, "evidence.imported",
                       {"through": len(events),
                        "reportImported": report is not None})
            j = self.store.journal_read(rid)

        finish_collection(self.store, run, self._mark)
        return {"runId": rid, "collected": True, "rundir": rd,
                "collectedUtc": utc_now(),
                "operated": True}
