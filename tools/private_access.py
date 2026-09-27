"""Install one SSH forwarding-only account for the protected collector.

The public key is non-secret. This helper never handles a reader token or private
key, changes another user's configuration, or restarts any business container.
Run plan first, inspect it, then apply that exact plan on the production server.
"""
import argparse
import base64
import hashlib
import json
import os
import re
import stat
import subprocess
from pathlib import Path

USER = "snow_stats_reader"
HOME = Path("/var/lib/snow-statistics-access")
KEY = HOME / "authorized_keys"
CONFIG = Path("/etc/ssh/sshd_config.d/00-snow-statistics-reader.conf")
ROOT = Path("/opt/snow-statistics")


def run(args):
    result = subprocess.run(args, capture_output=True, timeout=30)
    if result.returncode:
        raise ValueError("Private access operation failed: " + Path(args[0]).name)
    return result.stdout


def public_key(value):
    fields = value.strip().split()
    if len(fields) not in (2, 3) or fields[0] != "ssh-ed25519":
        raise ValueError("Use a dedicated Ed25519 public key")
    raw = base64.b64decode(fields[1], validate=True)
    if len(raw) != 51 or raw[:19] != b"\x00\x00\x00\x0bssh-ed25519\x00\x00\x00\x20":
        raise ValueError("Malformed public key")
    return "ssh-ed25519 " + fields[1] + " snow-statistics-private-reader\n"


def configuration():
    return (f"# Owned exclusively by Snow_Statistics. No global authorization changes.\nMatch User {USER}\n"
            f"    AllowUsers {USER}\n    AuthorizedKeysFile {KEY.as_posix()}\n"
            "    AuthenticationMethods publickey\n    PubkeyAuthentication yes\n"
            "    PasswordAuthentication no\n    KbdInteractiveAuthentication no\n"
            "    AllowTcpForwarding local\n    PermitOpen 127.0.0.1:8100\n    PermitListen none\n"
            "    AllowStreamLocalForwarding no\n    AllowAgentForwarding no\n"
            "    X11Forwarding no\n    PermitTTY no\n    PermitTunnel no\n    PermitUserRC no\n"
            "    GatewayPorts no\n    MaxSessions 0\n    ForceCommand /usr/sbin/nologin\nMatch all\n").encode()


def plan(key):
    authorized = ('restrict,port-forwarding,permitopen="127.0.0.1:8100" ' + public_key(key)).encode()
    files = {CONFIG.as_posix(): configuration(), KEY.as_posix(): authorized}
    value = dict(schema_version=1, owner="Snow_Statistics", user=USER,
                 files={path: hashlib.sha256(body).hexdigest() for path, body in files.items()},
                 shell="/usr/sbin/nologin", session_limit=0, tcp_destination="127.0.0.1:8100",
                 actions=["create a new locked system account", "install only the listed new files",
                          "validate sshd and unchanged root/deploy policies", "reload ssh without restarting connections"],
                 collection_enabled=False, private_token_installed=False)
    raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return value | {"plan_sha256": hashlib.sha256(raw).hexdigest()}, files


def trusted(path, directory=False):
    info = path.lstat()
    if (stat.S_ISLNK(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022
            or directory and not stat.S_ISDIR(info.st_mode)):
        raise ValueError("Private access requires root-controlled paths")


def effective(user):
    return run(["/usr/sbin/sshd", "-T", "-C", f"user={user},addr=127.0.0.1,host=localhost"]).decode()


def verify_policy(text):
    values = dict(line.split(" ", 1) for line in text.splitlines() if " " in line)
    expected = dict(allowusers=USER, authorizedkeysfile=KEY.as_posix(), authenticationmethods="publickey",
                    pubkeyauthentication="yes", passwordauthentication="no", kbdinteractiveauthentication="no",
                    allowtcpforwarding="local", permitopen="127.0.0.1:8100", permitlisten="none",
                    allowstreamlocalforwarding="no", allowagentforwarding="no", x11forwarding="no",
                    permittty="no", permittunnel="no", permituserrc="no", gatewayports="no",
                    maxsessions="0", forcecommand="/usr/sbin/nologin")
    if any(values.get(name) != expected_value for name, expected_value in expected.items()):
        raise ValueError("Actual SSH policy differs from forwarding-only scope")


def install(key, expected):
    import pwd
    if os.name != "posix" or os.geteuid() != 0 or Path(__file__).resolve() != ROOT / "tools/private_access.py":
        raise ValueError("Apply only as root from the trusted /opt/snow-statistics installation")
    for path in (ROOT, ROOT / "tools", Path(__file__), CONFIG.parent, HOME.parent):
        for parent in (path, *path.parents):
            trusted(parent, directory=parent != Path(__file__))
    receipt, files = plan(key)
    if receipt["plan_sha256"] != expected:
        raise ValueError("The reviewed private access plan changed")
    if any(Path(path).exists() or Path(path).is_symlink() for path in files) or HOME.exists():
        raise ValueError("Existing private access resources require a separate reviewed update")
    try:
        pwd.getpwnam(USER)
    except KeyError:
        pass
    else:
        raise ValueError("Refusing to adopt an existing account")
    run(["/usr/sbin/sshd", "-t"])
    policies = {user: effective(user) for user in ("root", "deploy")}
    run(["/usr/sbin/useradd", "--system", "--user-group", "--no-create-home", "--home-dir", str(HOME),
         "--shell", "/usr/sbin/nologin", "--password", "*", USER])
    HOME.mkdir(mode=0o755)
    installed = []
    try:
        for path, body in files.items():
            with Path(path).open("xb") as stream:
                stream.write(body)
                stream.flush()
                os.fsync(stream.fileno())
            os.chmod(path, 0o644)
            installed.append(Path(path))
        run(["/usr/sbin/sshd", "-t"])
        verify_policy(effective(USER))
        if any(effective(user) != policy for user, policy in policies.items()):
            raise ValueError("Another SSH account's effective policy changed")
        run(["/usr/bin/systemctl", "reload", "ssh"])
        receipt.update(installed=True, account_policy_verified=True, other_policies_unchanged=True,
                       sessions_tested=False, target_http_tested=False)
        (HOME / "installation.json").write_text(json.dumps(receipt, indent=2) + "\n")
        return receipt
    except Exception:
        # Move only newly installed files out of active paths; keep failure evidence.
        for path in reversed(installed):
            path.rename(HOME / (path.name + ".disabled"))
        run(["/usr/sbin/sshd", "-t"])
        run(["/usr/bin/systemctl", "reload", "ssh"])
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("plan", "install"))
    parser.add_argument("--public-key-file", required=True)
    parser.add_argument("--expected-plan-sha256")
    args = parser.parse_args()
    key = Path(args.public_key_file).read_text()
    if len(key) > 1024 or re.search(r"[\r\n].+", key.strip()):
        raise ValueError("Only one dedicated public key is allowed")
    value = plan(key)[0] if args.action == "plan" else install(key, args.expected_plan_sha256)
    print(json.dumps(value, indent=2))


if __name__ == "__main__":
    main()
