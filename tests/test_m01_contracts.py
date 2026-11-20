"""M01 — Scenario contracts. Covers T-001 and T-002.

T-001: start a scenario twice with one client key; retrieve one run and a
       manifest that survives a process restart.
T-002: supply a stale profile or mismatched nonce; prevent launch or
       quarantine evidence and expose the reason.
"""

from __future__ import annotations

import json
import os
import unittest

from helpers import RpTestCase

from rp.store import Store


class TestT001ClientKeyIdempotency(RpTestCase):
    """T-001 — Normal workflow."""

    def test_same_client_key_retrieves_one_run_across_processes(self):
        p1 = self.plan(client_key="release-team-7")
        self.assertEqual(p1.returncode, 0, p1.stderr)
        run_id_first = self.run_ids()
        self.assertEqual(len(run_id_first), 1)

        # Second invocation is a separate process — the manifest must come
        # back out of the durable store, byte-identical.
        p2 = self.plan(client_key="release-team-7")
        self.assertEqual(p2.returncode, 0, p2.stderr)
        self.assertIn("client key reuse", p2.stdout)
        self.assertEqual(self.run_ids(), run_id_first)

        m1 = self.manifest_of(run_id_first[0])
        self.assertEqual(m1["runId"], run_id_first[0])
        self.assertEqual(m1["clientKey"], "release-team-7")
        self.assertEqual(m1["profileId"], "host-fixture")
        self.assertEqual(m1["sinkMode"], "keyed")
        self.assertEqual(m1["effectBoundary"], "spool-accept")
        self.assertTrue(m1["runNonce"])
        self.assertTrue(m1["scenarioDigest"])
        self.assertEqual(len(m1["operations"]), 1)
        self.assertEqual(m1["operations"][0]["opId"], "label-job-0001")
        self.assertEqual(m1["operations"][0]["expectedCount"], 1)
        self.assertTrue(m1["operations"][0]["payloadDigest"])
        self.assertTrue(m1["buildDigests"]["host"])

        # Retrieval by client key resolves the same manifest.
        m2 = self.manifest_of("release-team-7")
        self.assertEqual(m2, m1)

    def test_different_client_key_makes_distinct_run(self):
        self.plan(client_key="k-a")
        self.plan(client_key="k-b")
        ids = self.run_ids()
        self.assertEqual(len(ids), 2)
        n1 = self.manifest_of(ids[0])["runNonce"]
        n2 = self.manifest_of(ids[1])["runNonce"]
        self.assertNotEqual(n1, n2, "distinct runs must get distinct nonces")

    def test_client_key_reuse_with_different_request_is_rejected(self):
        self.plan(client_key="k-x", sink="keyed")
        proc = self.plan(client_key="k-x", sink="plain")
        self.assertEqual(proc.returncode, 2)
        self.assertIn("different scenario/profile/sink", proc.stderr)

    def test_manifest_fields_match_scenario_payload(self):
        from rp.manifest import digest_obj
        self.plan(client_key="k-payload")
        m = self.manifest_of(self.run_ids()[0])
        expected = digest_obj({"media": "label-2x1", "text": "RP-DEMO-0001",
                               "copies": 1})
        self.assertEqual(m["operations"][0]["payloadDigest"], expected)


class TestT002DiagnosticOutcomes(RpTestCase):
    """T-002 — Failure and recovery."""

    def test_stale_profile_is_recorded_diagnostic_and_refused(self):
        proc = self.plan(client_key="k-stale", profile="stale-v0")
        self.assertEqual(proc.returncode, 3, proc.stderr)
        self.assertIn("unsupported", proc.stderr.lower())
        self.assertIn("schemaVersion", proc.stderr)

        # The refusal itself is durable evidence (FR-02).
        ids = self.run_ids()
        self.assertEqual(len(ids), 1)
        proc = self.rp("runs", "--format", "json", check=True)
        run = json.loads(proc.stdout)[0]
        self.assertEqual(run["status"], "unsupported")
        self.assertIn("schemaVersion 0", run["diagnostic"])

        # Repeating with the same client key retrieves the same diagnostic.
        proc2 = self.plan(client_key="k-stale", profile="stale-v0")
        self.assertEqual(proc2.returncode, 3)
        self.assertEqual(len(self.run_ids()), 1)

    def test_missing_document_is_usage_error_not_crash(self):
        proc = self.plan(client_key="k-none", profile="does-not-exist")
        self.assertEqual(proc.returncode, 2)
        self.assertIn("unknown profile", proc.stderr)
        self.assertEqual(self.run_ids(), [])

    def test_foreign_nonce_fact_is_quarantined(self):
        self.plan(client_key="k-nonce")
        run_id = self.run_ids()[0]
        nonce = self.manifest_of(run_id)["runNonce"]
        with Store(self.home) as store:
            good = store.record_fact(run_id, "attempt", {
                "runNonce": nonce, "attemptId": "att-x",
                "sendResult": "ETIMEDOUT"})
            self.assertFalse(good["quarantined"])
            bad = store.record_fact(run_id, "attempt", {
                "runNonce": "deadbeef" * 2, "attemptId": "att-foreign"})
            self.assertTrue(bad["quarantined"])
            self.assertEqual(len(store.facts(run_id, "attempt")), 1,
                             "foreign-nonce fact must not be attributed")
            q = store.quarantined(run_id)
            self.assertEqual(len(q), 1)
            self.assertIn("INV-01", q[0]["reason"])

    def test_pinned_nonce_is_stable_across_reuse(self):
        self.plan(client_key="k-n2", nonce="abc123")
        m = self.manifest_of(self.run_ids()[0])
        self.assertEqual(m["runNonce"], "abc123")


class TestStoreInvariants(RpTestCase):
    def test_facts_are_append_only_and_ordered(self):
        self.plan(client_key="k-seq")
        run_id = self.run_ids()[0]
        nonce = self.manifest_of(run_id)["runNonce"]
        with Store(self.home) as store:
            for i in range(5):
                store.record_fact(run_id, "tick", {"runNonce": nonce,
                                                   "i": i})
            kinds = [f["payload"]["i"] for f in store.facts(run_id, "tick")]
            self.assertEqual(kinds, [0, 1, 2, 3, 4])

    def test_unknown_run_fact_rejected(self):
        with Store(self.home) as store:
            with self.assertRaises(Exception):
                store.record_fact("run-nope", "x", {})


if __name__ == "__main__":
    unittest.main()
