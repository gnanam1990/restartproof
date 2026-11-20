"""M02 — Receipt sink. Covers T-003 and T-004.

T-003: deliver two attempts concurrently in keyed mode and prove a single
       durable effect after reopening the sink database.
T-004: commit the first effect and lose its response; plain mode accepts
       two while keyed mode returns the original effect even after a sink
       restart.

Also covers FR-03 (plain+keyed adapters, drainable writer boundary),
FR-04 (existing receipt returned / conflicting payload rejected) and the
close→drain→seal protocol.
"""

from __future__ import annotations

import os
import threading
import unittest

from helpers import RpTestCase

from rp.sink import (ADAPTER_VERSION, Ledger, SinkService, http_json)
from rp.util import digest_obj

RUN = "run-test-sink"
NONCE = "0123456789abcdef"
OP = "label-job-0001"
PAYLOAD = {"media": "label-2x1", "text": "RP-DEMO-0001", "copies": 1}
PDIGEST = digest_obj(PAYLOAD)


class SinkHttpCase(RpTestCase):
    def setUp(self):
        super().setUp()
        self.db = os.path.join(self.tmp.name, "sink.db")
        self._start_service()

    def _start_service(self):
        self.service = SinkService(self.db)
        self.server = self.service.make_server("127.0.0.1", 0)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       daemon=True)
        self.thread.start()

    def _stop_service(self):
        self.server.shutdown()
        self.server.server_close()
        self.service.ledger.close()
        self.thread.join(timeout=5)

    def tearDown(self):
        self._stop_service()
        super().tearDown()

    def url(self, path):
        return f"http://127.0.0.1:{self.port}{path}"

    def register(self, run_id=RUN, nonce=NONCE, mode="keyed"):
        status, body = http_json(self.url("/v1/runs"), {
            "runId": run_id, "runNonce": nonce, "mode": mode},
            token=self.service.admin_token)
        self.assertEqual(status, 200, body)
        return body

    def deliver(self, token, run_id=RUN, nonce=NONCE, op=OP,
                attempt="att-a", digest=PDIGEST, payload=PAYLOAD):
        return http_json(self.url(f"/v1/runs/{run_id}/deliver"), {
            "runNonce": nonce, "opId": op, "attemptId": attempt,
            "payloadDigest": digest, "payload": payload}, token=token)


class TestT003KeyedConcurrency(SinkHttpCase):
    """T-003 — Normal workflow."""

    def test_concurrent_keyed_attempts_commit_one_effect(self):
        tokens = self.register(mode="keyed")
        barrier = threading.Barrier(2)
        results = {}

        def go(name, attempt):
            barrier.wait()
            results[name] = self.deliver(
                tokens["writerToken"], attempt=attempt)

        threads = [threading.Thread(target=go, args=(n, a))
                   for n, a in (("A", "att-a"), ("B", "att-b"))]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=15)

        self.assertEqual(results["A"][0], 200, results["A"])
        self.assertEqual(results["B"][0], 200, results["B"])
        r_a = results["A"][1]["receipt"]["seq"]
        r_b = results["B"][1]["receipt"]["seq"]
        self.assertEqual(r_a, r_b,
                         "keyed dedup must return the same receipt")
        self.assertTrue(results["A"][1]["receipt"]["duplicate"]
                        or results["B"][1]["receipt"]["duplicate"])

        # Durability: reopen the database; exactly one effect exists.
        self._stop_service()
        ledger = Ledger(self.db)
        try:
            rows, more = ledger.effect_rows(RUN)
            self.assertFalse(more)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["op_id"], OP)
            self.assertEqual(rows[0]["payload_digest"], PDIGEST)
        finally:
            ledger.close()
        self._start_service()  # leave a live service for tearDown

    def test_keyed_conflict_digest_rejected(self):
        tokens = self.register(mode="keyed")
        s1, _ = self.deliver(tokens["writerToken"], attempt="att-1")
        self.assertEqual(s1, 200)
        bad = dict(PAYLOAD, text="DIFFERENT")
        status, body = self.deliver(tokens["writerToken"], attempt="att-2",
                                    digest=digest_obj(bad), payload=bad)
        self.assertEqual(status, 409)
        self.assertEqual(body["error"], "payload_conflict")

    def test_declared_digest_must_match_payload(self):
        tokens = self.register(mode="keyed")
        status, body = self.deliver(tokens["writerToken"],
                                    digest="0" * 64)
        self.assertEqual(status, 400)
        self.assertEqual(body["error"], "digest_mismatch")


class TestT004LostReplyRetry(SinkHttpCase):
    """T-004 — Failure and recovery."""

    def test_plain_accepts_two_effects_for_same_op(self):
        tokens = self.register(mode="plain")
        s1, b1 = self.deliver(tokens["writerToken"], attempt="att-1")
        s2, b2 = self.deliver(tokens["writerToken"], attempt="att-2")
        self.assertEqual((s1, s2), (200, 200))
        seqs = {b1["receipt"]["seq"], b2["receipt"]["seq"]}
        self.assertEqual(len(seqs), 2,
                         "plain sink must keep both deliveries")

    def test_keyed_returns_original_receipt_after_sink_restart(self):
        tokens = self.register(mode="keyed")
        s1, b1 = self.deliver(tokens["writerToken"], attempt="att-1")
        self.assertEqual(s1, 200)
        original_seq = b1["receipt"]["seq"]

        # The response is "lost" (worker never sees it), and the whole
        # sink service restarts before the retry arrives.
        self._stop_service()
        self._start_service()

        # Retry with a fresh attempt id but the same operation key.
        s2, b2 = self.deliver(tokens["writerToken"], attempt="att-2")
        self.assertEqual(s2, 200)
        self.assertEqual(b2["receipt"]["seq"], original_seq)
        self.assertTrue(b2["receipt"]["duplicate"])

        self._stop_service()
        ledger = Ledger(self.db)
        try:
            rows, _ = ledger.effect_rows(RUN)
            self.assertEqual(len(rows), 1)
        finally:
            ledger.close()
        self._start_service()


class TestSealProtocol(SinkHttpCase):
    def test_close_drain_seal_nonempty(self):
        tokens = self.register(mode="keyed")
        self.deliver(tokens["writerToken"], attempt="att-1")
        self.deliver(tokens["writerToken"], attempt="att-2")  # dedup
        status, body = http_json(
            self.url(f"/v1/runs/{RUN}/close"), {},
            token=tokens["readerToken"])
        self.assertEqual(status, 200, body)
        self.assertTrue(body["drained"])
        status, body = http_json(
            self.url(f"/v1/runs/{RUN}/seal"), {},
            token=tokens["readerToken"])
        self.assertEqual(status, 200, body)
        seal = body["seal"]
        self.assertEqual(seal["adapterVersion"], ADAPTER_VERSION)
        self.assertEqual(seal["runNonce"], NONCE)
        self.assertEqual(seal["firstSeq"], 1)
        self.assertEqual(seal["lastSeq"], 1)
        self.assertFalse(seal["empty"])
        self.assertEqual(seal["coverage"], "complete")
        self.assertTrue(seal["writerDrained"])
        self.assertTrue(seal["sealDigest"])

        # Seal is idempotent.
        _, again = http_json(self.url(f"/v1/runs/{RUN}/seal"), {},
                             token=tokens["readerToken"])
        self.assertEqual(again["seal"]["sealDigest"], seal["sealDigest"])

        # Delivery after close is refused.
        status, body = self.deliver(tokens["writerToken"], attempt="att-3")
        self.assertEqual(status, 409)
        self.assertIn("epoch", body["error"])

    def test_empty_interval_seal(self):
        tokens = self.register(mode="keyed")
        http_json(self.url(f"/v1/runs/{RUN}/close"), {},
                  token=tokens["readerToken"])
        _, body = http_json(self.url(f"/v1/runs/{RUN}/seal"), {},
                            token=tokens["readerToken"])
        seal = body["seal"]
        self.assertTrue(seal["empty"])
        self.assertIsNone(seal["firstSeq"])
        self.assertIsNone(seal["lastSeq"])
        self.assertEqual(seal["coverage"], "complete")

    def test_seal_requires_closed_epoch(self):
        tokens = self.register(mode="keyed")
        status, body = http_json(self.url(f"/v1/runs/{RUN}/seal"), {},
                                 token=tokens["readerToken"])
        self.assertEqual(status, 409)
        self.assertEqual(body["error"], "epoch_open")

    def test_nonce_and_token_boundaries(self):
        tokens = self.register(mode="keyed")
        # wrong writer token
        status, _ = self.deliver("bad-token")
        self.assertEqual(status, 403)
        # wrong nonce
        status, body = self.deliver(tokens["writerToken"], nonce="ff" * 8)
        self.assertEqual(status, 403)
        self.assertEqual(body["error"], "nonce_mismatch")
        # foreign run
        status, _ = http_json(self.url("/v1/runs/run-foreign/deliver"), {
            "runNonce": NONCE, "opId": OP, "attemptId": "a",
            "payloadDigest": PDIGEST}, token=tokens["writerToken"])
        self.assertEqual(status, 404)
        # reader token cannot write
        status, _ = self.deliver(tokens["readerToken"])
        self.assertEqual(status, 403)
        # writer token cannot seal/read
        status, _ = http_json(self.url(f"/v1/runs/{RUN}/effects"),
                              token=tokens["writerToken"])
        self.assertEqual(status, 403)
        # register same run with different nonce → refused
        status, _ = http_json(self.url("/v1/runs"), {
            "runId": RUN, "runNonce": "ee" * 8, "mode": "keyed"},
            token=self.service.admin_token)
        self.assertEqual(status, 403)


class TestSinkCliLifecycle(RpTestCase):
    def test_start_status_stop_via_cli(self):
        proc = self.rp("sink", "start", check=True)
        self.assertIn("receipt sink", proc.stdout)
        proc = self.rp("sink", "status", check=True)
        self.assertIn("adapterVersion", proc.stdout)
        self.assertIn(ADAPTER_VERSION, proc.stdout)
        proc = self.rp("sink", "stop", check=True)
        self.assertIn("stopped", proc.stdout)
        proc = self.rp("sink", "status")
        self.assertEqual(proc.returncode, 2)


if __name__ == "__main__":
    unittest.main()
