"""M01 — Scenario contracts.

Loads and validates scenario and profile documents, then pins an immutable
run manifest before launch (FR-01). Unsupported schema versions and unknown
profiles are surfaced as diagnostic outcomes, not crashes (FR-02).
"""

from __future__ import annotations

import json
import os

from . import SCHEMA_VERSION
from .errors import UnsupportedError, UsageError
from .util import canonical_json, digest_obj, new_nonce

SINK_MODES = ("plain", "keyed")
EFFECT_BOUNDARIES = ("spool-accept",)


def load_document(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        raise UsageError(f"document not found: {path}")
    except json.JSONDecodeError as exc:
        raise UsageError(f"invalid JSON in {path}: {exc}")


def _check_version(obj: dict, kind: str) -> None:
    ver = obj.get("schemaVersion")
    if ver != SCHEMA_VERSION:
        raise UnsupportedError(
            f"unsupported {kind} schemaVersion {ver!r}; "
            f"this build accepts schemaVersion {SCHEMA_VERSION}")


def _require(obj: dict, fields, kind: str) -> None:
    missing = [f for f in fields if f not in obj]
    if missing:
        raise UnsupportedError(
            f"{kind} document missing required field(s): {', '.join(missing)}")


def validate_profile(doc: dict) -> dict:
    """Validate a target profile document. Raises UnsupportedError (FR-02)."""
    _check_version(doc, "profile")
    _require(doc, ["profileId", "target"], "profile")
    target = doc["target"]
    if not isinstance(target, dict) or "label" not in target \
            or "os" not in target:
        raise UnsupportedError(
            "profile target must declare 'os' and 'label'")
    caps = doc.get("capabilities", {})
    profile = {
        "schemaVersion": doc["schemaVersion"],
        "profileId": doc["profileId"],
        "target": {
            "os": target["os"],
            "label": target["label"],
            "version": target.get("version"),
            "arch": target.get("arch"),
            "qnx": bool(target.get("qnx", False)),
        },
        "capabilities": {
            "nativeQnxIpc": bool(caps.get("nativeQnxIpc", False)),
        },
        "requires": doc.get("requires", {}),
        "description": doc.get("description", ""),
    }
    return profile


def validate_scenario(doc: dict) -> dict:
    """Validate a scenario document. Raises UnsupportedError (FR-02)."""
    _check_version(doc, "scenario")
    _require(doc, ["scenarioId", "operations", "effectBoundary"], "scenario")
    if doc["effectBoundary"] not in EFFECT_BOUNDARIES:
        raise UnsupportedError(
            f"unsupported effectBoundary {doc['effectBoundary']!r}; "
            f"known boundaries: {', '.join(EFFECT_BOUNDARIES)}")
    ops = []
    seen = set()
    for i, op in enumerate(doc["operations"]):
        for field in ("opId", "expectedCount", "payload"):
            if field not in op:
                raise UnsupportedError(
                    f"scenario operation[{i}] missing '{field}'")
        if op["opId"] in seen:
            raise UnsupportedError(
                f"duplicate opId {op['opId']!r} in scenario")
        seen.add(op["opId"])
        if not isinstance(op["expectedCount"], int) or op["expectedCount"] < 0:
            raise UnsupportedError(
                f"operation {op['opId']}: expectedCount must be a "
                "non-negative integer")
        if len(canonical_json(op["payload"])) > 512:
            raise UnsupportedError(
                f"operation {op['opId']}: payload exceeds the 512-byte "
                "wire contract")
        ops.append({
            "opId": op["opId"],
            "effect": op.get("effect", doc["effectBoundary"]),
            "expectedCount": op["expectedCount"],
            "payload": op["payload"],
            "payloadDigest": digest_obj(op["payload"]),
        })
    if not ops:
        raise UnsupportedError("scenario must declare at least one operation")
    scenario = {
        "schemaVersion": doc["schemaVersion"],
        "scenarioId": doc["scenarioId"],
        "title": doc.get("title", doc["scenarioId"]),
        "effectBoundary": doc["effectBoundary"],
        "retry": {"maxAttempts": int(doc.get("retry", {}).get(
            "maxAttempts", 2))},
        "operations": ops,
    }
    return scenario


def scenario_digest(scenario: dict) -> str:
    """Digest over the validated scenario identity (not run state)."""
    return digest_obj({
        "scenarioId": scenario["scenarioId"],
        "effectBoundary": scenario["effectBoundary"],
        "operations": [
            {"opId": o["opId"], "expectedCount": o["expectedCount"],
             "payloadDigest": o["payloadDigest"]}
            for o in scenario["operations"]
        ],
        "retry": scenario["retry"],
    })


def pin_manifest(*, run_id: str, client_key: str, scenario: dict,
                 profile: dict, sink_mode: str, nonce: str | None,
                 builds: dict, created_utc: str) -> dict:
    """Pin the immutable launch-time contract (FR-01).

    Everything a reviewer needs to attribute effects is frozen here: target
    profile, run nonce, operation identities, payload digests and the declared
    effect boundary.
    """
    if sink_mode not in SINK_MODES:
        raise UsageError(
            f"sink mode must be one of {', '.join(SINK_MODES)}")
    manifest = {
        "schemaVersion": SCHEMA_VERSION,
        "runId": run_id,
        "clientKey": client_key,
        "runNonce": nonce or new_nonce(),
        "createdUtc": created_utc,
        "scenarioId": scenario["scenarioId"],
        "scenarioDigest": scenario_digest(scenario),
        "profileId": profile["profileId"],
        "target": profile["target"],
        "sinkMode": sink_mode,
        "effectBoundary": scenario["effectBoundary"],
        "operations": [
            {"opId": o["opId"], "payloadDigest": o["payloadDigest"],
             "expectedCount": o["expectedCount"],
             "payload": o["payload"]}
            for o in scenario["operations"]
        ],
        "retry": scenario["retry"],
        "buildDigests": builds,
    }
    manifest["manifestDigest"] = digest_obj(
        {k: v for k, v in manifest.items() if k != "manifestDigest"})
    return manifest


def resolve_document(repo_root: str, kind: str, name: str) -> str:
    """Resolve a profile/scenario name to a file inside the repo tree.

    Accepts a bare name (looked up under profiles/ or scenarios/) or an
    explicit path. Bare names never escape the repo directory.
    """
    if os.sep in name or (os.altsep and os.altsep in name):
        if not os.path.isfile(name):
            raise UsageError(f"{kind} file not found: {name}")
        return name
    subdir = {"profile": "profiles", "scenario": "scenarios"}[kind]
    candidate = os.path.join(repo_root, subdir, f"{name}.json")
    if not os.path.isfile(candidate):
        raise UsageError(f"unknown {kind} {name!r} (looked for {candidate})")
    return candidate
