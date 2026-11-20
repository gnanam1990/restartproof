"""M03 — IPC harness. Covers T-005 and T-006 on the portable backend.

The SAME harness logic (protocol, barrier, restart, rediscovery, bounded
waits) is exercised here on the POSIX transport — labeled HOST_FIXTURE
evidence. The QNX transport (name_attach/MsgSend) is the target path;
a real QNX 8.0 guest run remains the unverified gate (T-005/T-006 QNX
portion). A Linux/macOS host run is not QNX proof.

T-005 (host): receipt committed, barrier entry recorded, original reply
             lost, generation restarted, fresh endpoint discovered.
T-006 (host): no barrier -> RESTART refused; nothing may claim the
             interruption happened.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import threading
import time

def _load_json(path):
    with open(path) as fh:
        return json.load(fh)
import unittest

from helpers import RpTestCase, REPO_ROOT

from rp import ctlmsg
from rp.sink import SinkService, http_json
from rp.util import digest_obj

BIN = os.path.join(REPO_ROOT, "guest", "bin")
RUN, NONCE, OP = "run-harness", "beefcafe01234567", "op-h1"
PAYLOAD = {"media": "label-2x1", "text": "RP-M03", "copies": 1}
PDIGEST = digest_obj(PAYLOAD)


def _have_cc():
    return any(shutil.which(x) for x in ("cc", "gcc", "clang"))


def build_guest():
    proc = subprocess.run(["make", "-C", os.path.join(REPO_ROOT,
                                                    "guest", "build")],
                          capture_output=True, text=True)
    return proc.returncode == 0, proc.stdout + proc.stderr


@unittest.skipUnless(_have_cc(), "no host C compiler")
class HarnessCase(RpTestCase):
    @classmethod
    def setUpClass(cls):
        ok, log = build_guest()
        if not ok:
            raise unittest.SkipTest(f"guest build failed:\n{log}")

    def setUp(self):
        super().setUp()
        self.rundir = os.path.join(self.tmp.name, "rd")
        os.makedirs(self.rundir)
        self.sink = SinkService(os.path.join(self.tmp.name, "sink.db"))
        self.server = self.sink.make_server("127.0.0.1", 0)
        self.sink_port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever,
                         daemon=True).start()
        self.addCleanup(self.server.shutdown)
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.sink.ledger.close)
        self.sup = None
        self.addCleanup(self._stop_sup)

    def _stop_sup(self):
        if self.sup and self.sup.poll() is None:
            try:
                msg = ctlmsg.pack_ctl(kind=ctlmsg.CTL_STOP, nonce=NONCE,
                                      control_generation=900)
                ctlmsg.ctl_request("127.0.0.1", self.ctl_port(), msg,
                                   timeout=3)
            except Exception:
                self.sup.kill()
            try:
                self.sup.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.sup.kill()

    def register(self, mode="keyed"):
        st, body = http_json(f"http://127.0.0.1:{self.sink_port}/v1/runs",
                             {"runId": RUN, "runNonce": NONCE,
                              "mode": mode},
                             token=self.sink.admin_token)
        self.assertEqual(st, 200, body)
        return body

    def start_supervisor(self, mode="keyed"):
        toks = self.register(mode=mode)
        self._sup_log = open(os.path.join(self.tmp.name, "sup.log"), "w")
        self.sup = subprocess.Popen([
            os.path.join(BIN, "rp_supervisor"),
            "--rundir", self.rundir,
            "--worker-image", os.path.join(BIN, "rp_worker"),
            "--run", RUN, "--nonce", NONCE,
            "--sink", f"http://127.0.0.1:{self.sink_port}",
            "--writer-token", toks["writerToken"],
            "--ctl-port", "0", "--endpoint-base", self.rundir],
            stdout=self._sup_log, stderr=subprocess.STDOUT)
        self.addCleanup(self._sup_log.close)
        for _ in range(300):
            if os.path.exists(os.path.join(self.rundir, "ctl.json")):
                break
            if self.sup.poll() is not None:
                self.fail("supervisor exited early: "
                          + open(os.path.join(self.tmp.name,
                                              "sup.log")).read())
            time.sleep(0.02)
        else:
            self.fail("supervisor did not publish ctl.json")

    def ctl_port(self):
        with open(os.path.join(self.rundir, "ctl.json")) as fh:
            return json.load(fh)["ctlPort"]

    def ctl(self, kind, control_generation, nonce=NONCE):
        msg = ctlmsg.pack_ctl(kind=kind, nonce=nonce,
                              control_generation=control_generation)
        return ctlmsg.ctl_request("127.0.0.1", self.ctl_port(), msg)

    def start_client(self, hold_first=1, max_attempts=2,
                     report_name="client_report.json"):
        report = os.path.join(self.rundir, report_name)
        proc = subprocess.Popen([
            os.path.join(BIN, "rp_client"),
            "--rundir", self.rundir, "--run", RUN, "--nonce", NONCE,
            "--op", OP, "--payload-json", json.dumps(PAYLOAD),
            "--payload-digest", PDIGEST, "--attempt-prefix", "att",
            "--max-attempts", str(max_attempts),
            "--timeout-ms", "8000", "--hold-first", str(hold_first),
            "--report", report])
        return proc, report

    def events(self):
        path = os.path.join(self.rundir, "events.jsonl")
        if not os.path.exists(path):
            return []
        with open(path) as fh:
            return [json.loads(l) for l in fh if l.strip()]

    def wait_event(self, kind, timeout=15):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for e in self.events():
                if e["kind"] == kind:
                    return e
            time.sleep(0.05)
        self.fail(f"event {kind!r} not observed; have "
                  f"{[e['kind'] for e in self.events()]}")

    def effects(self):
        rows, more = self.sink.ledger.effect_rows(RUN)
        self.assertFalse(more)
        return rows


class TestT005BarrierRestart(HarnessCase):
    """T-005 — Normal workflow (host fixture)."""

    def test_commit_barrier_restart_rediscovery_keyed(self):
        self.start_supervisor()
        client, report = self.start_client()
        barrier = self.wait_event("barrier")
        self.assertEqual(barrier["gen"], 1)
        self.assertEqual(barrier["seq"], 1)  # receipt committed pre-reply

        reply = self.ctl(ctlmsg.CTL_RESTART, control_generation=1)
        self.assertEqual(reply["status"], ctlmsg.CTL_OK, reply)
        self.assertEqual(reply["generation"], 2)

        self.assertEqual(client.wait(timeout=30), 0)
        rep = _load_json(report)
        self.assertTrue(rep["completed"])
        a1, a2 = rep["attempts"]
        self.assertEqual(a1["attemptId"], "att-1")
        self.assertEqual(a1["generation"], 1)
        self.assertEqual(a1["sendResult"], "LOST_REPLY")
        self.assertEqual(a2["attemptId"], "att-2")
        self.assertEqual(a2["generation"], 2)   # fresh endpoint
        self.assertEqual(a2["sendResult"], "OK")
        self.assertEqual(a2["receiptSeq"], 1)   # keyed: same receipt back

        effects = self.effects()
        self.assertEqual(len(effects), 1)
        kinds = [e["kind"] for e in self.events()]
        self.assertIn("worker_stopped", kinds)
        self.assertIn("restart", kinds)

    def test_plain_records_two_distinct_effects(self):
        self.start_supervisor(mode="plain")
        client, report = self.start_client()
        self.wait_event("barrier")
        self.ctl(ctlmsg.CTL_RESTART, control_generation=1)
        self.assertEqual(client.wait(timeout=30), 0)
        rep = _load_json(report)
        self.assertEqual(rep["attempts"][0]["sendResult"], "LOST_REPLY")
        self.assertEqual(rep["attempts"][1]["sendResult"], "OK")
        self.assertEqual(rep["attempts"][1]["receiptSeq"], 2)
        self.assertEqual(len(self.effects()), 2,
                         "plain sink keeps both deliveries")


class TestT006NoBarrier(HarnessCase):
    """T-006 — Failure and recovery (host fixture)."""

    def test_restart_without_barrier_is_refused(self):
        self.start_supervisor()
        # client sends with hold_first=0: worker replies normally,
        # never reaches the barrier
        client, report = self.start_client(hold_first=0)
        self.assertEqual(client.wait(timeout=30), 0)
        rep = _load_json(report)
        self.assertEqual(rep["attempts"][0]["sendResult"], "OK")

        reply = self.ctl(ctlmsg.CTL_RESTART, control_generation=1)
        self.assertEqual(reply["status"], ctlmsg.CTL_ERR_NO_BARRIER)
        kinds = [e["kind"] for e in self.events()]
        self.assertIn("ctl_reject", kinds)
        self.assertNotIn("restart", kinds)
        self.assertNotIn("barrier", kinds)
        # one generation only — nothing may claim an interruption
        reg = _load_json(os.path.join(self.rundir, "registry.json"))
        self.assertEqual(reg["generation"], 1)


class TestControlIntegrity(HarnessCase):
    def test_stale_control_generation_quarantined(self):
        self.start_supervisor()
        client, report = self.start_client()
        self.wait_event("barrier")
        r1 = self.ctl(ctlmsg.CTL_RESTART, control_generation=5)
        self.assertEqual(r1["status"], ctlmsg.CTL_OK)
        # replay an older control generation: must be refused
        r2 = self.ctl(ctlmsg.CTL_RESTART, control_generation=2)
        self.assertEqual(r2["status"], ctlmsg.CTL_ERR_STALE)
        rejects = [e for e in self.events() if e["kind"] == "ctl_reject"]
        self.assertTrue(any(e.get("reason") == "stale_control"
                            for e in rejects))
        client.wait(timeout=30)

    def test_foreign_nonce_ctl_rejected(self):
        self.start_supervisor()
        r = self.ctl(ctlmsg.CTL_STATUS, control_generation=1,
                     nonce="wrongnonce00000000")
        self.assertEqual(r["status"], ctlmsg.CTL_ERR_NONCE)
        self.assertTrue(any(e.get("reason") == "nonce"
                            for e in self.events()
                            if e["kind"] == "ctl_reject"))


if __name__ == "__main__":
    unittest.main()
