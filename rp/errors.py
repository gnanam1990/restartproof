"""Error types carrying CI exit-code semantics.

Exit mapping (see docs/ARCHITECTURE.md):
  0 measured pass, 1 measured failure, 2 review/incomplete or usage error,
  3 setup/unsupported.
"""


class RpError(Exception):
    exit_code = 2


class UsageError(RpError):
    """Bad operator input (unknown ids, conflicting client key, etc.)."""
    exit_code = 2


class UnsupportedError(RpError):
    """Unsupported profile/schema/target — a diagnostic outcome, not a crash."""
    exit_code = 3
