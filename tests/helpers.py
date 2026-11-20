"""Shared test helpers: isolated run homes and real CLI invocation."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class RpTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="rp-test-")
        self.addCleanup(self.tmp.cleanup)
        self.home = os.path.join(self.tmp.name, "home")
        os.makedirs(self.home)

    def rp(self, *args, check=False):
        """Invoke the real CLI entry point as a subprocess (fresh process)."""
        proc = subprocess.run(
            [sys.executable, "-m", "rp", "--home", self.home, *args],
            cwd=REPO_ROOT, capture_output=True, text=True, timeout=60)
        if check and proc.returncode != 0:
            self.fail(f"rp {' '.join(args)} exited {proc.returncode}:\n"
                      f"{proc.stdout}{proc.stderr}")
        return proc

    def plan(self, client_key="k-1", **kw):
        args = ["plan", "--scenario", kw.pop("scenario", "label-retry"),
                "--profile", kw.pop("profile", "host-fixture"),
                "--sink", kw.pop("sink", "keyed"),
                "--client-key", client_key]
        for k, v in kw.items():
            args += [f"--{k.replace('_', '-')}", str(v)]
        return self.rp(*args)

    def run_ids(self):
        proc = self.rp("runs", "--format", "json", check=True)
        return [r["run_id"] for r in json.loads(proc.stdout)]

    def manifest_of(self, run_id):
        proc = self.rp("manifest", run_id, check=True)
        return json.loads(proc.stdout)
