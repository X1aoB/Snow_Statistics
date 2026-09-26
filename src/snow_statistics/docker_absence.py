"""Exact Docker inspect absence parsing; no container ownership is inferred."""
import re


def inspect_missing(result, expected, *, formatted):
    """Check one failed inspect for an already validated exact CID or driver name."""
    if (not isinstance(expected, str)
            or re.fullmatch(r"[a-f0-9]{64}|snow-real-(?:hive|lake)-[a-f0-9]{20}", expected) is None
            or type(formatted) is not bool):
        raise ValueError("An exact registered Docker driver scope is required")
    if (type(result.returncode) is not int or result.returncode != 1
            or not isinstance(result.stdout, bytes) or not isinstance(result.stderr, bytes)
            or len(result.stdout) > 16 or len(result.stderr) > 256):
        return False
    if result.stdout.strip() != (b"" if formatted else b"[]"):
        return False
    prefix = rb"(?i:error: no such object: |error response from daemon: no such container: )"
    return re.fullmatch(prefix + re.escape(expected.encode("ascii")) + rb"\r?\n?", result.stderr) is not None
