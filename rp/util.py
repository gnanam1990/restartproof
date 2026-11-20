"""Small shared helpers: canonical JSON, digests, ids, utc timestamps."""

from __future__ import annotations

import hashlib
import json
import secrets
from datetime import datetime, timezone


def canonical_json(obj) -> bytes:
    """Deterministic serialization used for every digest in the product."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True).encode("utf-8")


def digest_obj(obj) -> str:
    return hashlib.sha256(canonical_json(obj)).hexdigest()


def digest_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def new_nonce() -> str:
    return secrets.token_hex(8)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def new_run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return f"run-{stamp}-{secrets.token_hex(3)}"


def new_attempt_id() -> str:
    return f"att-{secrets.token_hex(4)}"
