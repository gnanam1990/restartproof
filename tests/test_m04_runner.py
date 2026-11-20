"""M04 — Runner and retry. Covers T-007 and T-008 at HOST_FIXTURE.

T-007: stop the collector between steps and reattach to the original
       supervisor run — `rp collect` never sends another effect-producing
       command (no new client, no new delivery).
T-008: replayed/stale control generations are refused by the supervisor
       and quarantined in evidence; each attempt stays associated with
       its own worker generation.

All host execution is HOST_FIXTURE evidence — not QNX proof.
"""

from __future__ import annotations

import json
import os
import shutil
import unittest

from helpers import RpTestCase

from rp import ctlmsg
from rp.runner import run_dir
from rp.sink import http_json, read_service_info
from rp.store import Store

_HAVE_CC = any(shutil.which(x) for x in ("cc", "gcc", "clang"))


@unittest.skipUnless(_HAVE_CC, "no host C compiler")
class RunnerCase(RpTestCase):
    def setUp(self):
        super().setUp()
        # keep the bounded waits small for the suite; they are still real
        os.environ["RP_BARRIER_WAIT_S"] = "8"
        self.addCleanup(os.environ.pop, "RP_BARRIER_WAIT_S", None)
        # ensure binaries exist once per test (make is idempotent)
        from rp.runner import Runner  # noqa: F401  (import check)

    def rp_run(self, client_key, *extra, sink="keyed"):
        proc = self.rp("run", "--scenario", "label-retry",
                       "--profile", "host-fixture", "--sink", sink,
                       "--client-key", client_key, *extra)
        return proc

    def latest_run_id(self):
        ids = self.run_ids()
        self.assertTrue(ids, "no runs recorded")
        return ids[-1]

    def journal(self, rid):
        with Store(self.home) as store:
            return store.journal_read(rid)

    def tearDown(self):
        # stop any leftover cooperating test children (owned spawns only)
        import signal
        runs_root = os.path.join(self.home, "runs")
        if os.path.isdir(runs_root):
            for rid in os.listdir(runs_root):
                try:
                    j = self.journal(rid)
                except Exception:
                    continue
                for step in ("supervisor.started", "client.started"):
                    pid = ((j.get(step) or {}).get("state") or {}).get("pid")
                    if pid:
                        try:
                            os.kill(pid, signal.SIGKILL)
                        except OSError:
                            pass
        super().tearDown()

    def facts(self, rid, kind=None):
        with Store(self.home) as store:
            return store.facts(rid, kind)

    def effects(self, rid):
        info = read_service_info(self.home)
        rd = self.journal(rid)["sink.registered"]["state"]
        st, body = http_json(
            f"http://127.0.0.1:{info['port']}/v1/runs/{rid}/effects",
            token=rd["readerToken"])
        self.assertEqual(st, 200, body)
        return body["effects"]

    def client_report(self, rid):
        path = os.path.join(run_dir(self.home, rid), "client_report.json")
        with open(path) as fh:
            return json.load(fh)


class TestRunEndToEnd(RunnerCase):
    def test_keyed_run_collects_one_effect(self):
        proc = self.rp_run("k-e2e")
        self.assertEqual(proc.returncode, 0,
                         proc.stdout + proc.stderr)
        rid = self.latest_run_id()

        effects = self.effects(rid)
        self.assertEqual(len(effects), 1,
                         "keyed sink must dedup the retry to one receipt")
        report = self.client_report(rid)
        self.assertEqual(len(report["attempts"]), 2)
        a1, a2 = report["attempts"]
        self.assertEqual(a1["sendResult"], "LOST_REPLY")
        self.assertEqual(a2["sendResult"], "OK")
        self.assertEqual(a2["generation"], a1["generation"] + 1)

        kinds = [f["kind"] for f in self.facts(rid)]
        for expect in ("guest.worker_committed", "guest.barrier",
                       "guest.restart", "guest.attempt",
                       "client.report"):
            self.assertIn(expect, kinds)
        seal = self.journal(rid)["seal.stored"]["state"]["seal"]
        self.assertTrue(seal["closedEpoch"])
        self.assertEqual(seal["count"], 1)

    def test_plain_run_records_two_effects(self):
        proc = self.rp_run("k-plain", sink="plain")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        rid = self.latest_run_id()
        self.assertEqual(len(self.effects(rid)), 2,
                         "plain sink keeps both deliveries — the measured "
                         "duplicate")

    def test_no_hold_never_claims_interruption(self):
        proc = self.rp_run("k-nohold", "--no-hold")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        rid = self.latest_run_id()
        j = self.journal(rid)
        self.assertFalse(j["barrier.observed"]["state"]["observed"])
        restart = j["restart.sent"]["state"]
        self.assertFalse(restart["sent"],
                         "no barrier -> no restart may be claimed")
        self.assertEqual(len(self.effects(rid)), 1)


class TestT007CollectorReattach(RunnerCase):
    """T-007 — restart the host collector between steps, reattach."""

    def test_collect_resumes_without_new_operations(self):
        # the "collector" exits right after sending RESTART; the detached
        # supervisor + client keep running
        proc = self.rp_run("k-t007", "--stop-after", "restart.sent")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        rid = self.latest_run_id()
        j = self.journal(rid)
        self.assertIn("client.started", j)
        self.assertIn("restart.sent", j)
        self.assertNotIn("seal.stored", j)

        # reattach: must finish collection with no new client spawn and
        # no extra delivery
        proc = self.rp("collect", rid)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)

        j = self.journal(rid)
        self.assertIn("seal.stored", j)
        started = j["client.started"]["state"]
        self.assertEqual(len(self.effects(rid)), 1)
        report = self.client_report(rid)
        self.assertEqual(len(report["attempts"]), 2,
                         "exactly the pinned attempts — nothing re-sent")
        # one client report, one supervisor dir — nothing was relaunched
        self.assertTrue(os.path.isfile(started["reportPath"]))

        # a second collect is a no-op on evidence
        facts_before = len(self.facts(rid))
        proc = self.rp("collect", rid)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(len(self.facts(rid)), facts_before,
                         "collection retry must not fabricate facts "
                         "(INV-04)")
        self.assertEqual(len(self.effects(rid)), 1)


class TestT008StaleControl(RunnerCase):
    """T-008 — stale generations are quarantined, attempts stay bound
    to their own worker."""

    def test_stale_restart_and_stale_barrier_rejected(self):
        proc = self.rp_run("k-t008", "--stop-after", "restart.sent")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        rid = self.latest_run_id()
        j = self.journal(rid)
        self.assertTrue(j["restart.sent"]["state"].get("sent"),
                        "fixture must have restarted before testing "
                        "stale control")
        port = j["supervisor.started"]["state"]["ctlPort"]

        # replay the already-used control generation (runner used 5 here:
        # four journal marks precede the restart's generation draw; any
        # older generation is equally stale — use 1)
        run_nonce = self.facts(rid, "run.pinned")[0]["payload"]["runNonce"]
        stale = ctlmsg.pack_ctl(kind=ctlmsg.CTL_RESTART,
                                nonce=run_nonce, control_generation=1)
        reply = ctlmsg.ctl_request("127.0.0.1", port, stale)
        self.assertEqual(reply["status"], ctlmsg.CTL_ERR_STALE, reply)

        # a delayed barrier announcement for the dead generation is
        # likewise refused
        old_barrier = ctlmsg.pack_ctl(kind=ctlmsg.CTL_BARRIER,
                                      nonce=run_nonce, generation=1)
        reply = ctlmsg.ctl_request("127.0.0.1", port, old_barrier)
        self.assertEqual(reply["status"], ctlmsg.CTL_ERR_STALE, reply)

        # finish collection; the rejects must be part of the evidence
        proc = self.rp("collect", rid)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        rejects = [f["payload"] for f in self.facts(rid, "guest.ctl_reject")]
        reasons = {r.get("reason") for r in rejects}
        self.assertIn("stale_control", reasons)
        self.assertIn("stale_barrier", reasons)

        report = self.client_report(rid)
        self.assertEqual(report["attempts"][1]["generation"], 2,
                         "the retry stayed bound to its own generation")

        # the stale control generation did NOT kill the gen-2 worker:
        # exactly the pinned two deliveries, one keyed effect
        self.assertEqual(len(self.effects(rid)), 1)


if __name__ == "__main__":
    unittest.main()
