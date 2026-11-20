"""Build identity: content digests over the running source tree.

The host digest covers the ``rp`` Python package. The guest digest covers the
compiled harness binary when present, otherwise the guest source tree so an
unbuilt state is still distinguishable and auditable.
"""

from __future__ import annotations

import hashlib
import os


def _tree_digest(root: str, subdir: str, exts=None) -> str:
    base = os.path.join(root, subdir)
    h = hashlib.sha256()
    found = False
    for dirpath, dirnames, filenames in sorted(os.walk(base)):
        dirnames.sort()
        for name in sorted(filenames):
            if exts and not name.endswith(exts):
                continue
            path = os.path.join(dirpath, name)
            rel = os.path.relpath(path, root)
            h.update(rel.encode("utf-8"))
            with open(path, "rb") as fh:
                h.update(fh.read())
            found = True
    return h.hexdigest() if found else "absent"


def host_build_digest(root: str) -> str:
    return _tree_digest(root, "rp", exts=(".py",))


def guest_build_digest(root: str) -> str:
    """Digest of the compiled guest harness if built, else of its sources."""
    bin_dir = os.path.join(root, "guest", "bin")
    if os.path.isdir(bin_dir) and any(
            f for f in os.listdir(bin_dir) if not f.startswith(".")):
        return "built:" + _tree_digest(root, "guest/bin")
    return "sources:" + _tree_digest(root, "guest",
                                   exts=(".c", ".h", ".mk", "Makefile"))


def build_identities(root: str) -> dict:
    return {
        "host": host_build_digest(root),
        "guest": guest_build_digest(root),
    }
