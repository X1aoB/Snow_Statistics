"""Review/apply exact owned unit files. Does not enable, start, format, or deploy.

The separate configure-tunnel action installs a NEW tunnel only. Credentials are
validated in memory and never included in the plan, stdout, or source checkout.
"""
import argparse
import base64
import hashlib
import json
import os
import stat
import subprocess
import uuid
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[1]
PRODUCTION = Path("/opt/snow-statistics")
SYSTEMD = PurePosixPath("/etc/systemd/system")
CONFIG = Path("/etc/snow-statistics")
TOKEN_DROPIN = Path("/etc/systemd/system/snow-statistics-edge.service.d/20-remote-tunnel.conf")
FILES = {
    "deploy/production/state.mount.in": SYSTEMD / r"var-lib-snow\x2dstatistics-state.mount",
    "deploy/snow-statistics-lite.service": SYSTEMD / "snow-statistics-lite.service",
    **{f"deploy/production/{name}.service": SYSTEMD / f"{name}.service" for name in (
        "snow-statistics-edge", "snow-statistics-log-reader", "snow-statistics-log-forwarder")},
    "deploy/production/log-forwarder.env.example": PurePosixPath("/etc/snow-statistics/log-forwarder.env"),
}


def checksum(body):
    return hashlib.sha256(body).hexdigest()


def body(path):
    return path.read_bytes().replace(b"\r\n", b"\n")


def plan(root=ROOT):
    entries = [dict(source=source, destination=str(destination), sha256=checksum(body(root / source)))
               for source, destination in FILES.items()]
    value = dict(schema_version=1, owner="Snow_Statistics", files=entries,
                 actions=["create system user snow-statistics-log if missing", "copy listed units without enabling",
                          "systemctl daemon-reload"],
                 prohibited=["format state", "start services", "enable collection", "change business services", "delete data"])
    return value | {"plan_sha256": checksum(json.dumps(value, sort_keys=True, separators=(",", ":")).encode())}


def require_root():
    if os.name != "posix" or os.geteuid() != 0 or ROOT != PRODUCTION:
        raise ValueError("Apply only as root from /opt/snow-statistics")
    # Root helper source and every ancestor must be trusted, not deployment-user code.
    for path in (*ROOT.parents, ROOT, ROOT / "tools", ROOT / "deploy", ROOT / "deploy/production",
                 ROOT / "tools/production_log_reader.py", ROOT / "tools/production_log_forwarder.py", ROOT / "tools/production_install.py"):
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
            raise ValueError("Installation source is not exclusively root controlled")


def safe_destination(path):
    if str(path) not in {str(p) for p in FILES.values()} and path not in (
            CONFIG / "tunnel.yaml", CONFIG / "tunnel.json", CONFIG / "tunnel.token",
            CONFIG / "compose.edge-token.json", TOKEN_DROPIN):
        raise ValueError("Destination is not owned by this installer")
    for parent in (path.parent, *path.parent.parents):
        if parent.exists():
            info = parent.lstat()
            if stat.S_ISLNK(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
                raise ValueError("Unsafe target parent")
    if path.is_symlink() or path.exists() and not path.is_file():
        raise ValueError("Unexpected target type")


def install(expected):
    import grp
    import pwd
    require_root()
    receipt = plan()
    if expected != receipt["plan_sha256"]:
        raise ValueError("Reviewed plan changed")
    for source, destination_name in FILES.items():
        destination = Path(str(destination_name))
        safe_destination(destination)
        if destination.exists() and destination.read_bytes() != body(ROOT / source):
            raise ValueError("Existing unit/config differs; review an explicit upgrade instead")
    try:
        account = pwd.getpwnam("snow-statistics-log")
        if account.pw_uid == 0 or account.pw_shell not in ("/usr/sbin/nologin", "/sbin/nologin"):
            raise ValueError("Existing account is unsuitable")
    except KeyError:
        subprocess.run(["/usr/sbin/useradd", "--system", "--user-group", "--no-create-home", "--shell", "/usr/sbin/nologin", "snow-statistics-log"], check=True)
        account = pwd.getpwnam("snow-statistics-log")
    try:
        docker = grp.getgrnam("docker")
    except KeyError:
        docker = None
    if docker and (account.pw_gid == docker.gr_gid or account.pw_name in docker.gr_mem):
        raise ValueError("Forwarder account has Docker access")
    CONFIG.mkdir(mode=0o700, exist_ok=True)
    for source, destination_name in FILES.items():
        destination = Path(str(destination_name))
        if not destination.exists():
            with destination.open("xb") as stream:
                stream.write(body(ROOT / source))
                stream.flush()
                os.fsync(stream.fileno())
            os.chmod(destination, 0o600 if destination.parent == CONFIG else 0o644)
    subprocess.run(["/usr/bin/systemctl", "daemon-reload"], check=True, timeout=30)
    print(json.dumps(dict(plan_sha256=expected, installed=True, enabled=False, started=False)))


def tunnel_config(tunnel_id):
    ident = str(uuid.UUID(tunnel_id))
    return (f"tunnel: {ident}\ncredentials-file: /run/secrets/tunnel.json\n"
            "metrics: 127.0.0.1:2000\nloglevel: error\ningress:\n"
            "  - hostname: stats.xiaob.dev\n    service: http://gateway:8080\n"
            "    originRequest:\n      connectTimeout: 2s\n      httpHostHeader: stats.xiaob.dev\n"
            "  - service: http_status:404\n").encode()


def configure_tunnel(tunnel_id, credential):
    require_root()
    info = credential.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o077 or info.st_size > 8192:
        raise ValueError("Tunnel credential must be a small root-private regular file")
    content = credential.read_bytes()
    data = json.loads(content)
    ident = str(uuid.UUID(tunnel_id))
    if str(uuid.UUID(data["TunnelID"])) != ident or len(base64.b64decode(data["TunnelSecret"], validate=True)) != 32:
        raise ValueError("Credential does not match the chosen tunnel")
    if not isinstance(data["AccountTag"], str) or len(data["AccountTag"]) != 32:
        raise ValueError("Invalid tunnel account")
    files = {CONFIG / "tunnel.json": content, CONFIG / "tunnel.yaml": tunnel_config(ident)}
    for target in files:
        safe_destination(target)
        if target.exists():
            raise ValueError("Existing tunnel config retained; explicit rotation required")
    CONFIG.mkdir(mode=0o700, exist_ok=True)
    for target, value in files.items():
        with target.open("xb") as stream:
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(target, 0o400 if target.suffix == ".json" else 0o444)
        if target.suffix == ".json":
            os.chown(target, 10002, 10002)  # Only this container UID; parent remains root 0700.
    print(json.dumps(dict(tunnel_id=ident, configured=True, started=False, dns_changed=False)))


def validate_tunnel_token(content, tunnel_id, account_id):
    """Parse data only; never execute the copied vendor installation command."""
    ident = str(uuid.UUID(tunnel_id))
    token = content.strip()
    if len(token) > 8192 or not token or len(account_id) != 32 or any(c not in "0123456789abcdef" for c in account_id):
        raise ValueError("Invalid bounded tunnel token/account")
    data = json.loads(base64.b64decode(token, validate=True))
    if (set(data) != {"a", "t", "s"} or data["a"] != account_id or data["t"] != ident or
            not 32 <= len(base64.b64decode(data["s"], validate=True)) <= 128):
        raise ValueError("Token does not belong to the reviewed account and tunnel")
    return token


def remote_tunnel_spec(spec):
    """Freeze the rendered existing gateway and bounds; change only Tunnel auth."""
    spec = json.loads(json.dumps(spec))
    if spec.get("name") != "snow-statistics-edge" or set(spec["services"]) != {"gateway", "tunnel"}:
        raise ValueError("Unexpected public edge project")
    tunnel = spec["services"]["tunnel"]
    if (set(tunnel["networks"]) != {"tunnel"} or tunnel.get("user") != "10002:10002" or
            str(tunnel.get("mem_limit")) != str(128 * 1024**2) or float(tunnel.get("cpus", 0)) != 0.1 or
            not tunnel.get("read_only") or "@sha256:" not in tunnel["image"] or
            tunnel.get("environment") or tunnel.get("ports")):
        raise ValueError("Tunnel isolation or locked resource bounds changed")
    tunnel["command"] = ["tunnel", "--no-autoupdate", "--loglevel", "error", "run", "--token-file", "/run/secrets/tunnel.token"]
    tunnel["volumes"] = [dict(type="bind", source=str(CONFIG / "tunnel.token"),
                              target="/run/secrets/tunnel.token", read_only=True,
                              bind={"create_host_path": False})]
    return spec


def token_dropin():
    command = "/usr/bin/docker compose -f /etc/snow-statistics/compose.edge-token.json"
    return ("[Service]\nExecStart=\nExecStart=" + command + " up -d\nExecStop=\nExecStop=" + command + " stop\n").encode()


def configure_token(tunnel_id, account_id, credential):
    require_root()
    info = credential.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o077 or info.st_size > 8192:
        raise ValueError("Tunnel token must be a small root-private regular file")
    token = validate_tunnel_token(credential.read_bytes(), tunnel_id, account_id)
    targets = [CONFIG / "tunnel.token", CONFIG / "compose.edge-token.json", TOKEN_DROPIN]
    # No mode switch, credential rotation or partial-install overwrite is implicit.
    for target in [*targets, CONFIG / "tunnel.json", CONFIG / "tunnel.yaml"]:
        safe_destination(target)
        if target.exists():
            raise ValueError("Existing tunnel configuration retained; review explicit recovery/rotation")
    rendered = subprocess.check_output([
        "/usr/bin/docker", "compose", "--env-file", str(ROOT / "deploy/production/images.env"),
        "-f", str(ROOT / "deploy/production/compose.edge.yaml"), "config", "--format", "json"], timeout=30)
    spec = remote_tunnel_spec(json.loads(rendered))
    files = {targets[0]: token + b"\n", targets[1]: json.dumps(spec, sort_keys=True, indent=2).encode(),
             TOKEN_DROPIN: token_dropin()}
    CONFIG.mkdir(mode=0o700, exist_ok=True)
    TOKEN_DROPIN.parent.mkdir(mode=0o755, exist_ok=True)
    for target, value in files.items():
        safe_destination(target)
        with target.open("xb") as stream:
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(target, 0o400 if target == targets[0] else 0o600 if target.parent == CONFIG else 0o644)
        if target == targets[0]:
            os.chown(target, 10002, 10002)
    subprocess.run(["/usr/bin/systemctl", "daemon-reload"], check=True, timeout=30)
    print(json.dumps(dict(tunnel_id=str(uuid.UUID(tunnel_id)), configured=True, mode="remotely_managed",
                          compose_sha256=checksum(files[targets[1]]), started=False, enabled=False,
                          dns_changed=False, credential_values_reported=False)))


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("plan")
    apply = sub.add_parser("install")
    apply.add_argument("--expected-plan-sha256", required=True)
    tunnel = sub.add_parser("configure-tunnel")
    tunnel.add_argument("--tunnel-id", required=True)
    tunnel.add_argument("--credential-file", required=True, type=Path)
    token = sub.add_parser("configure-token")
    token.add_argument("--tunnel-id", required=True)
    token.add_argument("--account-id", required=True)
    token.add_argument("--token-file", required=True, type=Path)
    args = parser.parse_args()
    if args.action == "plan":
        print(json.dumps(plan(), indent=2))
    elif args.action == "install":
        install(args.expected_plan_sha256)
    elif args.action == "configure-tunnel":
        configure_tunnel(args.tunnel_id, args.credential_file)
    else:
        configure_token(args.tunnel_id, args.account_id, args.token_file)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError, TypeError, subprocess.SubprocessError):
        raise SystemExit("Production candidate operation failed; existing data retained") from None
