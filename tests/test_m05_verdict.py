"""M05 — Verdict and evidence. Covers T-009 and T-010 at HOST_FIXTURE.

T-009: plain two effects -> FAIL_DUPLICATE; keyed one covered effect ->
       PASS; authenticated closed empty interval -> FAIL_MISSING.
T-010: same sink/run/seq read twice counts once; conflicting copies are
       quarantined and REVIEW when they compromise the verdict; a single
       effect without a seal is REVIEW; two trustworthy distinct effects
       keep FAIL_DUPLICATE while marked incomplete.

All host execution is HOST_FIXTURE evidence — not QNX proof.
"""

from __future__ import annotations

import json
import os
import shutil
import unittest

from helpers import RpTestCase

from rp import verdict
from rp.runner import run_dir
from rp.store import Store

_HAVE_CC = any(shutil.which(x) for x in ("cc", "gcc", "clang"))

RID = "run-test"


def _manifest(expected=1):
    return {
        "runNonce": "nonce-t",
        "manifestDigest": "md",
        "target": {"label": "HOST_FIXTURE", "qnx": False},
        "effectBoundary": "spool-accept",
        "retry": {"maxAttempts": 2},
        "operations": [{"opId": "op-1", "expectedCount": expected,
                        "payloadDigest": "pd"}],
    }


def _run(status="collected", manifest=None):
    return {"run_id": RID, "manifest": manifest or _manifest(),
            "status": status, "diagnostic": None}


def _seal(empty=False, complete=True, count=1):
    return {"closedEpoch": True, "writerDrained": True, "empty": empty,
            "coverage": "complete" if complete else "partial",
            "count": 0 if empty else count, "firstSeq": None if empty else 1,
            "lastSeq": None if empty else count}


def _journal(seal=None, barrier=True, restart=True, hold=True):
    j = {
        "client.started": {"state": {"holdFirst": hold}},
        "barrier.observed": {"state": {"observed": barrier}},
        "restart.sent": {"state": {"sent": restart}},
    }
    if seal is not None:
        j["seal.stored"] = {"state": {"seal": seal}}
    return j


def _facts(attempts=2):
    return [{"kind": "client.report",
             "payload": {"attempts": [{"i": n} for n in range(attempts)]}}]


def _row(seq, op="op-1", attempt="a-1", content="same"):
    return {"seq": seq, "run_id": RID, "op_id": op, "attempt_id": attempt,
            "payload_digest": "pd", "payload_json": content,
            "received_utc": "t", "effectDigest": f"d-{seq}-{content}",
            "sinkId": "sink-1"}


class TestDedup(unittest.TestCase):
    """T-010 first clause — the same receipt read twice counts once."""

    def test_identical_copies_count_once(self):
        rows = [_row(1), _row(1)]
        ded = verdict.dedup_observations(rows)
        self.assertEqual(len(ded["trustworthy"]), 1)
        self.assertEqual(ded["copies"], 1)
        self.assertFalse(ded["conflicted"])

    def test_conflicting_copies_quarantined(self):
        rows = [_row(1, content="A"), _row(1, content="B")]
        ded = verdict.dedup_observations(rows)
        self.assertEqual(len(ded["trustworthy"]), 0)
        self.assertEqual(len(ded["conflicted"]), 1)


class TestT009Verdicts(unittest.TestCase):
    """T-009 — the three exported classifications."""

    def test_keyed_single_covered_effect_passes(self):
        out = verdict.reduce_verdict(
            _run(), _journal(seal=_seal()), _facts(), [_row(1)])
        v = out["verdict"]
        self.assertEqual(v["code"], verdict.VERDICT_PASS)
        self.assertEqual(v["exitCode"], 0)

    def test_plain_two_effects_fail_duplicate(self):
        out = verdict.reduce_verdict(
            _run(), _journal(seal=_seal(count=2)), _facts(),
            [_row(1), _row(2, attempt="a-2")])
        v = out["verdict"]
        self.assertEqual(v["code"], verdict.VERDICT_FAIL_DUPLICATE)
        self.assertEqual(v["exitCode"], 1)

    def test_closed_empty_interval_fails_missing(self):
        out = verdict.reduce_verdict(
            _run(), _journal(seal=_seal(empty=True, count=0)),
            _facts(), [])
        v = out["verdict"]
        self.assertEqual(v["code"], verdict.VERDICT_FAIL_MISSING)
        self.assertEqual(v["exitCode"], 1)

    def test_unsupported_run_maps_setup(self):
        run = _run(status="unsupported")
        out = verdict.reduce_verdict(run, {}, [], [])
        self.assertEqual(out["verdict"]["code"], verdict.VERDICT_SETUP)
        self.assertEqual(out["verdict"]["exitCode"], 3)


class TestT010EvidenceIntegrity(unittest.TestCase):
    """T-010 — dedup, quarantine, missing-seal review, incomplete fail."""

    def test_same_seq_twice_still_passes(self):
        out = verdict.reduce_verdict(
            _run(), _journal(seal=_seal()), _facts(),
            [_row(1), dict(_row(1))])
        self.assertEqual(out["verdict"]["code"], verdict.VERDICT_PASS)
        self.assertEqual(out["analysis"]["duplicateReads"], 1)

    def test_conflict_compromising_verdict_reviews(self):
        # one conflicting seq: could be 0 or 1 trustworthy effects —
        # the verdict cannot be claimed
        rows = [_row(1, content="A"), _row(1, content="B")]
        out = verdict.reduce_verdict(
            _run(), _journal(seal=_seal()), _facts(), rows)
        v = out["verdict"]
        self.assertEqual(v["code"], verdict.VERDICT_REVIEW)
        self.assertTrue(v["incomplete"])

    def test_conflict_under_duplicate_failure_stays_fail(self):
        # two trustworthy distinct effects already exceed the declared
        # cardinality; a conflicting third copy cannot rescue it
        rows = [_row(1), _row(2, attempt="a-2"),
                _row(3, content="X"), _row(3, content="Y")]
        out = verdict.reduce_verdict(
            _run(), _journal(seal=_seal(count=3)), _facts(), rows)
        v = out["verdict"]
        self.assertEqual(v["code"], verdict.VERDICT_FAIL_DUPLICATE)
        self.assertTrue(v["incomplete"],
                        "conflicted copies must mark the observation "
                        "incomplete")
        self.assertEqual(len(out["analysis"]["conflicted"]), 1)

    def test_single_effect_without_seal_reviews(self):
        out = verdict.reduce_verdict(
            _run(), _journal(seal=None), _facts(), [_row(1)])
        v = out["verdict"]
        self.assertEqual(v["code"], verdict.VERDICT_REVIEW)
        self.assertEqual(v["exitCode"], 2)
        self.assertIn("seal", v["reason"])

    def test_no_effects_without_seal_reviews(self):
        out = verdict.reduce_verdict(
            _run(), _journal(seal=None), _facts(), [])
        self.assertEqual(out["verdict"]["code"], verdict.VERDICT_REVIEW)


@unittest.skipUnless(_HAVE_CC, "no host C compiler")
class TestReportEndToEnd(RpTestCase):
    """Real run -> `rp report` through the normal entry point."""

    def setUp(self):
        super().setUp()
        os.environ["RP_BARRIER_WAIT_S"] = "8"
        self.addCleanup(os.environ.pop, "RP_BARRIER_WAIT_S", None)

    def _run_fixture(self, client_key, sink="keyed"):
        proc = self.rp("run", "--scenario", "label-retry",
                       "--profile", "host-fixture", "--sink", sink,
                       "--client-key", client_key)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        return self.run_ids()[-1]

    def tearDown(self):
        import signal
        runs_root = os.path.join(self.home, "runs")
        if os.path.isdir(runs_root):
            for rid in os.listdir(runs_root):
                try:
                    with Store(self.home) as store:
                        j = store.journal_read(rid)
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

    def test_report_keyed_run_passes_and_bundles(self):
        rid = self._run_fixture("m05-keyed")
        proc = self.rp("report", rid)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("PASS", proc.stdout)

        bundle_path = os.path.join(run_dir(self.home, rid), "evidence.json")
        self.assertTrue(os.path.isfile(bundle_path))
        with open(bundle_path) as fh:
            bundle = json.load(fh)
        self.assertEqual(bundle["bundleVersion"], 1)
        self.assertEqual(bundle["verdict"]["code"], "PASS")
        self.assertEqual(bundle["effects"]["attributableCount"], 1)
        self.assertTrue(bundle["coverage"]["closedEpoch"])
        self.assertTrue(bundle["hostFixture"])
        self.assertEqual(bundle["provenance"], "HOST_FIXTURE")
        self.assertEqual(
            bundle["operations"][0]["attempts"][0]["sendResult"],
            "LOST_REPLY")

    def test_report_plain_run_fails_duplicate(self):
        rid = self._run_fixture("m05-plain", sink="plain")
        proc = self.rp("report", rid)
        self.assertEqual(proc.returncode, 1, proc.stdout + proc.stderr)
        self.assertIn("FAIL_DUPLICATE", proc.stdout)

        with open(os.path.join(run_dir(self.home, rid),
                               "evidence.json")) as fh:
            bundle = json.load(fh)
        self.assertEqual(bundle["effects"]["attributableCount"], 2)

    def test_report_works_offline_from_snapshot(self):
        # snapshot + live read of the same rows must still count once,
        # and a dead sink must not block classification (durable facts)
        rid = self._run_fixture("m05-offline")
        proc = self.rp("sink", "stop")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        proc = self.rp("report", rid)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        with open(os.path.join(run_dir(self.home, rid),
                               "evidence.json")) as fh:
            bundle = json.load(fh)
        self.assertFalse(bundle["evidenceSources"]["liveSinkRead"])
        self.assertEqual(bundle["effects"]["attributableCount"], 1)
        self.assertEqual(bundle["verdict"]["code"], "PASS")


if __name__ == "__main__":
    unittest.main()
