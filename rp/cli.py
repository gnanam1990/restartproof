"""RestartProof command line entry point.

Implemented subcommands grow with the modules:

  rp plan      M01 — validate inputs, pin the manifest, persist a run.
  rp runs      M01 — list runs and their status.
  rp manifest  M01 — print a run's pinned manifest.
  rp doctor    environment/toolchain check.

Exit codes: 0 pass, 1 measured failure, 2 usage/review, 3 setup/unsupported.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys

from . import __version__
from .buildid import build_identities
from .errors import RpError, UnsupportedError, UsageError
from .runner import STEPS
from .manifest import (SINK_MODES, load_document, pin_manifest,
                       resolve_document, validate_profile, validate_scenario)
from .store import Store
from .util import new_run_id, utc_now

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_HOME = os.environ.get("RP_HOME", os.path.join(os.getcwd(), ".rp"))


def _create_run(home: str, args):
    """Validate inputs, pin the manifest, persist/retrieve the run.

    Shared by `rp plan` and `rp run`. Returns (run_row, manifest, created).
    """
    client_key = args.client_key or f"cli-{os.getpid()}-{utc_now()}"
    try:
        scenario = validate_scenario(load_document(
            resolve_document(REPO_ROOT, "scenario", args.scenario)))
        profile = validate_profile(load_document(
            resolve_document(REPO_ROOT, "profile", args.profile)))
    except UnsupportedError as exc:
        # FR-02: record unsupported/invalid inputs as a diagnostic outcome,
        # retrievable later via `rp runs` / `rp manifest`, then fail.
        with Store(home) as store:
            run_id = new_run_id()
            store.create_run(
                run_id=run_id, client_key=client_key,
                manifest={"profileId": args.profile,
                          "scenarioId": args.scenario,
                          "sinkMode": args.sink,
                          "runNonce": args.nonce,
                          "schemaVersion": None},
                status="unsupported", diagnostic=str(exc))
        raise UnsupportedError(
            f"{exc} (recorded as diagnostic run for key {client_key!r})",
        ) from exc
    builds = build_identities(REPO_ROOT)
    run_id = new_run_id()
    manifest = pin_manifest(
        run_id=run_id, client_key=client_key, scenario=scenario,
        profile=profile, sink_mode=args.sink, nonce=args.nonce,
        builds=builds, created_utc=utc_now())
    with Store(home) as store:
        run, created = store.create_run(
            run_id=run_id, client_key=client_key, manifest=manifest,
            status="planned")
        store.record_fact(run["run_id"], "run.pinned", {
            "runNonce": manifest["runNonce"],
            "manifestDigest": manifest["manifestDigest"],
            "clientKey": client_key,
            "reusedClientKey": not created,
        })
    return run, manifest, created


def _plan(args) -> int:
    run, manifest, created = _create_run(args.home, args)
    verb = "planned" if created else "retrieved (client key reuse)"
    print(f"run {verb}: {run['run_id']}")
    print(f"  scenario:    {manifest['scenarioId']} "
          f"({manifest['scenarioDigest'][:16]}…)")
    print(f"  profile:     {manifest['profileId']} "
          f"[{manifest['target']['label']}]")
    print(f"  sink mode:   {manifest['sinkMode']}")
    print(f"  run nonce:   {manifest['runNonce']}")
    print(f"  boundary:    {manifest['effectBoundary']}")
    for op in manifest["operations"]:
        print(f"  op:          {op['opId']} expected={op['expectedCount']} "
              f"payload={op['payloadDigest'][:16]}…")
    print(f"  manifest:    {manifest['manifestDigest'][:16]}…  "
          f"(persisted at {os.path.join(args.home, 'state.db')})")
    return 0


def _run(args) -> int:
    from .runner import Runner
    run, manifest, _ = _create_run(args.home, args)
    with Store(args.home) as store:
        run = store.get_run(run["run_id"])
        result = Runner(args.home, store).execute(
            run, hold_first=not args.no_hold,
            stop_after=args.stop_after)
    if "stoppedAfter" in result:
        print(f"run {result['runId']}: stopped after "
              f"{result['stoppedAfter']} (test flag); finish with "
              f"`rp collect --run {result['runId']}`")
        return 0
    return _print_run_result(result, manifest)


def _collect(args) -> int:
    from .runner import Runner
    with Store(args.home) as store:
        run = store.get_run(args.run) or store.get_by_client_key(args.run)
        if run is None:
            raise UsageError(f"unknown run {args.run}")
        result = Runner(args.home, store).collect(
            run, stop_after=args.stop_after)
        manifest = run["manifest"] or {}
    if "stoppedAfter" in result:
        print(f"run {result['runId']}: stopped after "
              f"{result['stoppedAfter']} (test flag)")
        return 0
    return _print_run_result(result, manifest)


def _print_run_result(result: dict, manifest: dict) -> int:
    rid = result["runId"]
    label = (manifest.get("target") or {}).get("label", "?")
    print(f"run {rid} collected [{label}]")
    print(f"  evidence dir: {result.get('rundir')}")
    print("  verdict:      pending — run `rp report` for the sealed "
          "classification")
    return 0


def _report(args) -> int:
    from . import verdict as verdict_mod
    with Store(args.home) as store:
        run = store.get_run(args.run) or store.get_by_client_key(args.run)
        if run is None:
            raise UsageError(f"unknown run {args.run}")
        v = verdict_mod.report(store, run, out_path=args.out)
    if args.format == "json":
        print(json.dumps(v, indent=2, sort_keys=True))
    else:
        print(f"verdict {v['code']}: {v['reason']}")
        if v.get("incomplete"):
            print("  note:       observation marked incomplete")
        print(f"  bundle:     {v['bundlePath']}")
        print(f"  digest:     {v['bundleDigest'][:16]}…")
    return v["exitCode"]


def _runs(args) -> int:
    with Store(args.home) as store:
        runs = store.list_runs()
    if args.format == "json":
        home_abs = os.path.abspath(args.home)
        rows = []
        for r in runs:
            row = {k: v for k, v in r.items() if k != "manifest"}
            if isinstance(row.get("diagnostic"), str):
                row["diagnostic"] = row["diagnostic"].replace(
                    home_abs, "<rp-home>")
            rows.append(row)
        print(json.dumps(rows, indent=2))
        return 0
    if not runs:
        print("no runs recorded; create one with: rp plan --scenario "
              "label-retry --profile host-fixture --client-key KEY")
        return 0
    print(f"{'RUN ID':<26} {'STATUS':<12} {'PROFILE':<14} {'SINK':<7} "
          f"{'SCENARIO':<14} CREATED (UTC)")
    for r in runs:
        print(f"{r['run_id']:<26} {r['status']:<12} "
              f"{(r['profile_id'] or '-'):<14} {(r['sink_mode'] or '-'):<7} "
              f"{(r['scenario_id'] or '-'):<14} {r['created_utc']}")
    return 0


def _manifest(args) -> int:
    with Store(args.home) as store:
        run = store.get_run(args.run) or store.get_by_client_key(args.run)
    if run is None:
        raise UsageError(f"unknown run {args.run}")
    print(json.dumps(run["manifest"], indent=2, sort_keys=True))
    return 0


def _doctor(args) -> int:
    """Report environment capabilities honestly (feeds T-012 gating)."""
    ok = True
    print(f"rp version:       {__version__}")
    print(f"python:           {sys.version.split()[0]}")
    print(f"repo root:        {REPO_ROOT}")
    cc = shutil.which("cc") or shutil.which("gcc") or shutil.which("clang")
    print(f"host C compiler:  {cc or 'ABSENT (host fixture build unavailable)'}")
    ok = ok and bool(cc)
    qcc = (shutil.which("qcc") or shutil.which("ntox86_64-gcc")
           or shutil.which("ntoaarch64-gcc"))
    qnx_sdp = os.environ.get("QNX_HOST") or os.environ.get("QNX_TARGET")
    if qcc:
        print(f"QNX toolchain:    {qcc}")
    else:
        print("QNX toolchain:    ABSENT — profile qnx8-qemu will report "
              "setup/unsupported (exit 3)")
    if qnx_sdp:
        print(f"QNX SDP env:      {qnx_sdp}")
    delegate = os.environ.get("RP_QNX_GUEST_ADDR")
    delegate_msg = delegate or (
        "ABSENT (set RP_QNX_GUEST_ADDR when a licensed QEMU guest is running)")
    print(f"QNX guest agent:  {delegate_msg}")
    return 0 if ok else 3


def _sink(args) -> int:
    from . import sink as sinkmod
    if args.sink_cmd == "serve":
        # Diagnostic backstop: if startup blocks anywhere (dispatch, DB
        # init, socket bind), dump a traceback instead of hanging mute.
        import faulthandler
        faulthandler.dump_traceback_later(60, exit=True)
        print("sink: dispatch reached", file=sys.stderr, flush=True)
        db = args.db or os.path.join(args.home, "sink.db")
        service = sinkmod.SinkService(db)
        print("sink: ledger ready", file=sys.stderr, flush=True)
        server = service.make_server(args.host, args.port)
        print("sink: server bound", file=sys.stderr, flush=True)
        port = server.server_address[1]
        sinkmod.write_service_info(args.home, {
            "pid": os.getpid(), "port": port, "sinkId": service.sink_id,
            "adminToken": service.admin_token, "dbPath": db,
            "adapterVersion": sinkmod.ADAPTER_VERSION})
        print(f"receipt sink {service.sink_id} serving on "
              f"{args.host}:{port} (db {db})", flush=True)
        faulthandler.cancel_dump_traceback_later()

        import signal as _sig
        import threading as _th
        def _term(*_):
            # shutdown() must not run on the serve_forever thread.
            _th.Thread(target=server.shutdown, daemon=True).start()
        _sig.signal(_sig.SIGTERM, _term)
        server.serve_forever()
        server.server_close()
        service.ledger.close()
        return 0
    if args.sink_cmd == "start":
        info = sinkmod.ensure_sink(args.home)
        print(f"receipt sink {info['sinkId']} on port {info['port']} "
              f"(pid {info['pid']})")
        return 0
    if args.sink_cmd == "stop":
        stopped = sinkmod.stop_sink(args.home)
        print("sink stopped" if stopped else "no sink running")
        return 0
    if args.sink_cmd == "status":
        info = sinkmod.read_service_info(args.home)
        if sinkmod.sink_alive(info):
            status, health = sinkmod.http_json(
                f"http://127.0.0.1:{info['port']}/v1/health")
            print(json.dumps(health, indent=2))
        else:
            print("no live sink for this home")
            return 2
        return 0
    raise UsageError(f"unknown sink command {args.sink_cmd}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="rp",
        description="RestartProof — count externally observable effects "
                    "that survive a worker restart and retry.")
    p.add_argument("--home", default=DEFAULT_HOME,
                   help="run state directory (default: ./.rp or $RP_HOME)")
    p.add_argument("--version", action="version",
                   version=f"rp {__version__}")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("plan", help="validate inputs and pin a run manifest")
    sp.add_argument("--scenario", required=True)
    sp.add_argument("--profile", required=True)
    sp.add_argument("--sink", choices=SINK_MODES, default="keyed")
    sp.add_argument("--client-key", default=None,
                    help="idempotent launch key; reuse retrieves the run")
    sp.add_argument("--nonce", default=None,
                    help="pin a specific run nonce (testing only)")
    sp.set_defaults(func=_plan)

    sp = sub.add_parser("runs", help="list recorded runs")
    sp.add_argument("--format", choices=["text", "json"], default="text")
    sp.set_defaults(func=_runs)

    sp = sub.add_parser("manifest", help="print a run's pinned manifest")
    sp.add_argument("run", help="run id or client key")
    sp.set_defaults(func=_manifest)

    sp = sub.add_parser(
        "run", help="plan, launch and collect a bounded run (host fixture)")
    sp.add_argument("--scenario", required=True)
    sp.add_argument("--profile", required=True)
    sp.add_argument("--sink", choices=SINK_MODES, default="keyed")
    sp.add_argument("--client-key", default=None,
                    help="idempotent launch key; reuse retrieves the run")
    sp.add_argument("--nonce", default=None,
                    help="pin a specific run nonce (testing only)")
    sp.add_argument("--no-hold", action="store_true",
                    help="first attempt does NOT hold at the post-commit "
                         "barrier (failure-path measurement)")
    sp.add_argument("--stop-after", default=None,
                    choices=STEPS,
                    help="exit after a named journal step (simulates a "
                         "collector restart; finish with `rp collect`)")
    sp.set_defaults(func=_run)

    sp = sub.add_parser(
        "collect", help="reattach to a run and finish collection "
                        "(no new operations are ever sent)")
    sp.add_argument("run", help="run id or client key")
    sp.add_argument("--stop-after", default=None,
                    choices=STEPS,
                    help="exit after a named journal step (test flag)")
    sp.set_defaults(func=_collect)

    sp = sub.add_parser(
        "report", help="reduce the evidence bundle and classify the run "
                       "(exit 0 pass, 1 fail, 2 review, 3 setup)")
    sp.add_argument("run", help="run id or client key")
    sp.add_argument("--out", default=None,
                    help="bundle path (default <rundir>/evidence.json)")
    sp.add_argument("--format", choices=["text", "json"], default="text")
    sp.set_defaults(func=_report)

    sp = sub.add_parser("doctor", help="check toolchain and target access")
    sp.set_defaults(func=_doctor)

    sp = sub.add_parser("sink", help="receipt sink service lifecycle")
    sp.add_argument("sink_cmd",
                    choices=["serve", "start", "stop", "status"])
    sp.add_argument("--port", type=int, default=0,
                    help="serve port (0 = ephemeral, default)")
    sp.add_argument("--host", default="127.0.0.1",
                    help="bind address; use 0.0.0.0 only when a remote "
                         "guest must reach the sink")
    sp.add_argument("--db", default=None,
                    help="ledger path (default <home>/sink.db)")
    sp.set_defaults(func=_sink)

    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except RpError as exc:
        print(f"rp: {exc}", file=sys.stderr)
        return exc.exit_code
    except subprocess.CalledProcessError as exc:
        print(f"rp: external command failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
