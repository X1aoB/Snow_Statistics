"""Synthetic keys only; actual forwarding/session checks remain deployment gates."""
import base64
import importlib.util
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location("private_access", Path(__file__).resolve().parents[1] / "tools/private_access.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
KEY = "ssh-ed25519 " + base64.b64encode(b"\x00\x00\x00\x0bssh-ed25519\x00\x00\x00\x20" + bytes(range(32))).decode()


def policy():
    lines = module.configuration().decode().splitlines()
    return "\n".join(line.strip().lower() if "AuthorizedKeysFile" not in line else
                     "authorizedkeysfile " + module.KEY.as_posix() for line in lines if line.startswith("    "))


def test_plan_is_bound_to_key_and_contains_only_owned_paths():
    plan, files = module.plan(KEY)
    assert set(files) == {module.CONFIG.as_posix(), module.KEY.as_posix()}
    assert plan["tcp_destination"] == "127.0.0.1:8100" and plan["session_limit"] == 0
    assert not plan["collection_enabled"] and not plan["private_token_installed"]
    assert b'restrict,port-forwarding,permitopen="127.0.0.1:8100"' in files[module.KEY.as_posix()]
    assert module.plan(KEY + " fixture-comment")[0] == plan
    assert module.configuration().decode().splitlines()[1] == "Match User snow_stats_reader"


def test_unscoped_policies_and_malformed_keys_rejected():
    module.verify_policy(policy())
    for replacement in ("maxsessions 2", "permitopen any", "allowtcpforwarding yes", "permitlisten any",
                        "allowusers root", "passwordauthentication yes", "allowagentforwarding yes"):
        key = replacement.split()[0]
        broken = "\n".join(replacement if line.startswith(key + " ") else line for line in policy().splitlines())
        with pytest.raises(ValueError):
            module.verify_policy(broken)
    for key in (KEY + "\n" + KEY, "ssh-rsa invalid", "ssh-ed25519 AAAA"):
        with pytest.raises(ValueError):
            module.public_key(key)
