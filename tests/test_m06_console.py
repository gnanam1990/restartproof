"""M06 — Lab console, operator bridge and release. T-011..T-014.

T-011/T-012: both run kinds reproducible; sink rows inspectable; viewer
             + JSON export; qnx-profile runs stay honestly unrun.
T-013: a ctl child outside the run registry is refused; a withheld seal
       reaches a diagnostic (REVIEW) inside the configured deadline.
T-014: the console distinguishes duplicate vs missing-seal outcomes
       with text, and its exported effect rows match the run's bundle.

The operator bridge is exercised for real here — the expressly started
harness below is spawned with the same argv a licensed target would
use, driven over the same TCP control channel (RP_CTL_EVENTS gather
included). All host execution remains HOST_FIXTURE evidence.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import signal
import subprocess
import time
import unittest

from helpers import RpTestCase, REPO_ROOT

from rp.runner import run_dir
from rp.store import Store

_HAVE_CC = any(shutil.which(x) for x in ("cc", "gcc", "clang"))
_BIN = os.path.join(REPO_ROOT, "guest", "bin")


def _bin(name):
    return os.path.join(_BIN, name)


@unittest.skipUnless(_HAVE_CC, "no host C compiler")
class BridgeCase(RpTestCase):
    """Helpers to expressly start the registered cooperating harness."""

    def setUp(self):
        super().setUp()
        os.environ["RP_BARRIER_WAIT_S"] = "8"
        os.environ["RP_CLIENT_WAIT_S"] = "20"
        os.environ["RP_EVENTS_POLL_S"] = "0.1"
        for v in ("RP_BARRIER_WAIT_S", "RP_CLIENT_WAIT_S",
                  "RP_EVENTS_POLL_S"):
            self.addCleanup(os.environ.pop, v, None)
        self._procs = []
        self._logs = []

    def tearDown(self):
        for p in self._procs:
            try:
                p.kill()
            except OSError:
                pass
        for p in self._procs:
            try:
                p.wait(timeout=5)
            except (OSError, subprocess.TimeoutExpired):
                pass
        for h in self._logs:
            try:
                h.close()
            except OSError:
                pass
        super().tearDown()

    def plan_run(self, client_key, profile="host-fixture", sink="keyed"):
        proc = self.rp("plan", "--scenario", "label-retry",
                       "--profile", profile, "--sink", sink,
                       "--client-key", client_key)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        return self.run_ids()[-1]

    def register(self, rid):
        proc = self.rp("operate", rid, "--register")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)

    def journal(self, rid):
        with Store(self.home) as store:
            return store.journal_read(rid)

    def manifest(self, rid):
        with Store(self.home) as store:
            return store.get_run(rid)["manifest"]

    def spawn_harness(self, rid):
        """Start supervisor+client with the documented operator argv —
        the same invocation the QNX runbook gives the target operator."""
        m = self.manifest(rid)
        sink = self.journal(rid)["sink.registered"]["state"]
        gdir = os.path.join(self.tmp.name, f"guest-{rid}")
        os.makedirs(gdir)
        ep = os.path.join("/tmp", f"rp-{rid}")
        os.makedirs(ep, exist_ok=True)

        sup_log = open(os.path.join(gdir, "supervisor.log"), "wb")
        self._logs.append(sup_log)
        sup = subprocess.Popen(
            [_bin("rp_supervisor"), "--rundir", gdir,
             "--worker-image", _bin("rp_worker"),
             "--run", rid, "--nonce", m["runNonce"],
             "--sink", f"http://127.0.0.1:{sink['sinkPort']}",
             "--writer-token", sink["writerToken"],
             "--ctl-port", "0", "--endpoint-base", ep],
            stdin=subprocess.DEVNULL, stdout=sup_log,
            stderr=subprocess.STDOUT)
        self._procs.append(sup)

        ctl_json = os.path.join(gdir, "ctl.json")
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and not os.path.exists(ctl_json):
            self.assertIsNone(sup.poll(), "supervisor exited early")
            time.sleep(0.05)
        self.assertTrue(os.path.exists(ctl_json),
                        "supervisor did not publish ctl.json")
        with open(ctl_json) as fh:
            ctl_port = json.load(fh)["ctlPort"]

        op = m["operations"][0]
        cli_log = open(os.path.join(gdir, "client.log"), "wb")
        self._logs.append(cli_log)
        client = subprocess.Popen(
            [_bin("rp_client"), "--rundir", gdir, "--run", rid,
             "--nonce", m["runNonce"], "--op", op["opId"],
             "--payload-json", json.dumps(op["payload"], sort_keys=True),
             "--payload-digest", op["payloadDigest"],
             "--attempt-prefix", "att",
             "--max-attempts", str(m["retry"]["maxAttempts"]),
             "--timeout-ms", "2000", "--hold-first", "1",
             "--report", os.path.join(gdir, "client_report.json")],
            stdin=subprocess.DEVNULL, stdout=cli_log,
            stderr=subprocess.STDOUT)
        self._procs.append(client)
        return {"gdir": gdir, "ctl_port": ctl_port, "sup": sup,
                "client": client}

    def wait_event(self, gdir, kind, timeout_s=10):
        path = os.path.join(gdir, "events.jsonl")
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            try:
                with open(path) as fh:
                    for line in fh:
                        try:
                            if json.loads(line).get("kind") == kind:
                                return True
                        except json.JSONDecodeError:
                            pass
            except OSError:
                pass
            time.sleep(0.05)
        return False


class TestOperatorBridge(BridgeCase):
    """The executable QNX operator path, exercised on the fixture."""

    def test_bridge_full_flow_collects_and_passes(self):
        rid = self.plan_run("m06-bridge")
        self.register(rid)
        h = self.spawn_harness(rid)

        proc = self.rp("operate", rid, "--ctl",
                       f"127.0.0.1:{h['ctl_port']}")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("operator bridge", proc.stdout)

        j = self.journal(rid)
        attached = j["operate.attached"]["state"]
        self.assertTrue(attached["attached"])
        self.assertEqual(attached["via"], "ctl",
                         "evidence must come over RP_CTL_EVENTS")
        self.assertTrue(attached.get("targetUname"),
                        "guest uname must be captured for provenance")
        self.assertTrue(j["barrier.observed"]["state"]["observed"])
        self.assertTrue(j["restart.sent"]["state"]["sent"])

        proc = self.rp("report", rid)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        with open(os.path.join(run_dir(self.home, rid),
                               "evidence.json")) as fh:
            bundle = json.load(fh)
        self.assertEqual(bundle["verdict"]["code"], "PASS")
        self.assertEqual(bundle["targetIdentity"]["execution"],
                         "operator-bridge")
        self.assertTrue(bundle["targetIdentity"]["uname"])
        self.assertEqual(bundle["effects"]["attributableCount"], 1)

    def test_bridge_guest_dir_mode(self):
        rid = self.plan_run("m06-gdir")
        self.register(rid)
        h = self.spawn_harness(rid)
        proc = self.rp("operate", rid, "--ctl",
                       f"127.0.0.1:{h['ctl_port']}",
                       "--guest-dir", h["gdir"])
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        j = self.journal(rid)
        self.assertEqual(j["operate.attached"]["state"]["via"],
                         "guest-dir")
        proc = self.rp("report", rid)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)

    def test_foreign_supervisor_refused(self):
        """T-013: a ctl child registered for a different run's nonce is
        rejected at attach, inside the control deadline."""
        rid_a = self.plan_run("m06-foreign-a")
        rid_b = self.plan_run("m06-foreign-b")
        self.register(rid_a)
        self.register(rid_b)
        h = self.spawn_harness(rid_a)   # bound to run A's nonce

        proc = self.rp("operate", rid_b, "--ctl",
                       f"127.0.0.1:{h['ctl_port']}")
        self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)
        self.assertIn("outside the run registry",
                      proc.stdout + proc.stderr)
        attached = self.journal(rid_b)["operate.attached"]["state"]
        self.assertFalse(attached["attached"])

    def test_unreachable_ctl_reports_diagnostic(self):
        """T-013: nothing listening -> bounded diagnostic, exit 2."""
        rid = self.plan_run("m06-dead")
        self.register(rid)
        start = time.monotonic()
        proc = self.rp("operate", rid, "--ctl", "127.0.0.1:1")
        self.assertLess(time.monotonic() - start, 15,
                        "attach must fail inside a bounded deadline")
        self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)

    def test_withheld_seal_reaches_review(self):
        """T-013: boundary withheld after the commit -> honest REVIEW."""
        rid = self.plan_run("m06-seal")
        self.register(rid)
        h = self.spawn_harness(rid)
        self.assertTrue(self.wait_event(h["gdir"], "barrier", 10),
                        "worker must reach the post-commit barrier")
        proc = self.rp("sink", "stop")   # withhold the boundary now
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)

        proc = self.rp("operate", rid, "--ctl",
                       f"127.0.0.1:{h['ctl_port']}")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        proc = self.rp("report", rid)
        self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)
        self.assertIn("REVIEW", proc.stdout)

    def test_operate_requires_register_or_ctl(self):
        rid = self.plan_run("m06-usage")
        proc = self.rp("operate", rid)
        self.assertEqual(proc.returncode, 2)
        proc = self.rp("operate", rid, "--ctl", "127.0.0.1:1")
        # never registered -> refuse before touching the endpoint
        self.assertEqual(proc.returncode, 2)
        self.assertIn("--register", proc.stderr)


class TestT012TargetHonesty(BridgeCase):
    """T-012 — without a licensed target the app reports setup and
    the run stays marked unrun; the host fixture cannot satisfy the
    QNX gate."""

    def test_qnx_profile_run_is_setup_not_fixture(self):
        proc = self.rp("run", "--scenario", "label-retry",
                       "--profile", "qnx8-qemu", "--sink", "keyed",
                       "--client-key", "m06-qnx")
        self.assertEqual(proc.returncode, 3, proc.stdout + proc.stderr)
        rid = self.run_ids()[-1]

        proc = self.rp("report", rid)
        self.assertEqual(proc.returncode, 3, proc.stdout + proc.stderr)
        with open(os.path.join(run_dir(self.home, rid),
                               "evidence.json")) as fh:
            bundle = json.load(fh)
        self.assertFalse(bundle["hostFixture"])
        self.assertEqual(bundle["provenance"], "QNX 8.0 QEMU guest")
        self.assertEqual(bundle["targetIdentity"]["execution"],
                         "not-run")
        self.assertIsNone(bundle["targetIdentity"]["uname"],
                          "no guest ever reported an identity")


class TestConsole(BridgeCase):
    """T-011/T-014 — the accessible viewer and its JSON export."""

    def _run(self, key, sink="keyed"):
        proc = self.rp("run", "--scenario", "label-retry",
                       "--profile", "host-fixture", "--sink", sink,
                       "--client-key", key)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        rid = self.run_ids()[-1]
        self.rp("report", rid)
        return rid

    def test_side_by_side_console_and_export(self):
        keyed = self._run("m06-keyed")
        plain = self._run("m06-plain", sink="plain")

        proc = self.rp("console", "--run", keyed, "--run", plain)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        path = os.path.join(self.home, "console.html")
        self.assertTrue(os.path.isfile(path))
        with open(path) as fh:
            doc = fh.read()

        # accessibility invariants — meaningful at 200% zoom, no JS
        for need in ('<html lang="en">', '<main', "scope='col'",
                     "<caption>", "<dl", 'aria-labelledby'):
            self.assertIn(need, doc, f"missing accessible markup: {need}")
        self.assertNotIn("<script", doc)
        self.assertNotIn("font-size: 0", doc)

        # both verdicts distinguishable as text, not just styling
        self.assertIn("PASS — expected effect count", doc)
        self.assertIn("FAIL — duplicate durable effects", doc)
        # boundary + provenance on every card (INV-06)
        self.assertIn("spool-accept", doc)
        self.assertIn("HOST_FIXTURE", doc)
        self.assertIn("not proof of execution on a licensed QNX", doc)

        # no private layout or credentials in the shared artifact
        self.assertNotIn(os.path.abspath(self.home), doc)
        with Store(self.home) as store:
            for rid in (keyed, plain):
                reg = store.journal_read(rid)["sink.registered"]["state"]
                self.assertNotIn(reg["readerToken"], doc)
                self.assertNotIn(reg["writerToken"], doc)

        # the JSON export exists at the linked relative path
        for rid in (keyed, plain):
            self.assertTrue(os.path.isfile(os.path.join(
                self.home, "runs", rid, "evidence.json")))

    def test_console_effect_rows_match_exported_bundle(self):
        """T-014 — exported effect rows are the same run's rows."""
        rid = self._run("m06-rows", sink="plain")
        self.rp("console", check=True)
        with open(os.path.join(run_dir(self.home, rid),
                               "evidence.json")) as fh:
            bundle = json.load(fh)
        with open(os.path.join(self.home, "console.html")) as fh:
            doc = fh.read()

        seqs = [str(r["seq"]) for r in bundle["effects"]["attributable"]]
        self.assertEqual(seqs, ["1", "2"],
                         "plain sink: two distinct exported rows")
        for seq in seqs:
            self.assertIn(f"<td>{seq}</td>", doc)

        # `rp effects` agrees with the bundle (same attribution source)
        proc = self.rp("effects", rid, check=True)
        out = json.loads(proc.stdout)
        self.assertEqual([r["seq"] for r in out["attributable"]],
                         [1, 2])

    def test_console_distinguishes_missing_seal_from_duplicate(self):
        """T-014 — a duplicate card and a missing-seal REVIEW card are
        textually distinct (no reliance on color)."""
        dup = self._run("m06-dup", sink="plain")

        # synthesize a withheld-seal run: collected, one observed
        # effect, but the seal step never landed -> REVIEW
        proc = self.plan(client_key="m06-noseal")
        self.assertEqual(proc.returncode, 0)
        review = self.run_ids()[-1]
        with Store(self.home) as store:
            store.set_status(review, "collected")
            store.journal_write(review, "sink.registered", 1,
                                {"sinkId": "s", "sinkPort": 1})
            store.journal_write(review, "client.started", 2,
                                {"holdFirst": True})
            store.journal_write(review, "barrier.observed", 3,
                                {"observed": True})
            store.journal_write(review, "restart.sent", 4, {"sent": True})
            store.record_fact(review, "sink.effects",
                              {"sinkId": "s", "effects": [{
                                  "seq": 1, "run_id": review,
                                  "op_id": "label-job-0001",
                                  "attempt_id": "a1",
                                  "payload_digest": "p",
                                  "payload_json": "{}",
                                  "received_utc": "t",
                                  "effectDigest": "d"}]})
            store.record_fact(review, "client.report",
                              {"attempts": [{"i": 0}]})
        proc = self.rp("report", review)
        self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)

        self.rp("console", "--run", dup, "--run", review, check=True)
        with open(os.path.join(self.home, "console.html")) as fh:
            doc = fh.read()
        self.assertIn("FAIL — duplicate durable effects", doc)
        self.assertIn("REVIEW — evidence incomplete", doc)
        self.assertNotEqual(
            re.search(r'<span class="verdict[^"]*">([^<]+)', doc).group(1),
            "REVIEW")


if __name__ == "__main__":
    unittest.main()
