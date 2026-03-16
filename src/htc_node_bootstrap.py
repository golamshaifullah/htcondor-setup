#!/usr/bin/env python3
"""
Bootstrap a small Ubuntu-based HTCondor cluster node.

Supported roles:
  - head    : HTCondor Central Manager + Submit + NFS client mount
  - execute : HTCondor Execute node
  - storage : NFS server for job outputs

This script is intentionally conservative:
  - It supports Ubuntu 22.04 (jammy) and 24.04 (noble)
  - It does not enable firewall or SSH hardening unless you ask it to
  - It does not assume DNS; you can supply --add-host entries to seed /etc/hosts

Run as root (or via sudo).
"""

from __future__ import annotations

import argparse
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Iterable

HTCONDOR_KEY_URL = "https://htcss-downloads.chtc.wisc.edu/repo/keys/HTCondor-24.0-Key"
HTCONDOR_REPO_BASE = "https://htcss-downloads.chtc.wisc.edu/repo/ubuntu/24.0"
DEFAULT_HTCONDOR_PORT = 9618
DEFAULT_LOWPORT = 20000
DEFAULT_HIGHPORT = 20100


class BootstrapError(RuntimeError):
    pass


def log(msg: str) -> None:
    print(f"[+] {msg}")


def warn(msg: str) -> None:
    print(f"[!] {msg}")


def run(cmd: list[str], *, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    log("RUN " + shlex.join(cmd))
    return subprocess.run(cmd, check=check, text=True, capture_output=capture)


def require_root() -> None:
    if os.geteuid() != 0:
        raise BootstrapError("Run this script as root or via sudo.")


def parse_os_release() -> dict[str, str]:
    data: dict[str, str] = {}
    for line in Path("/etc/os-release").read_text().splitlines():
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        data[k] = v.strip().strip('"')
    return data


def assert_supported_ubuntu() -> str:
    info = parse_os_release()
    distro = info.get("ID", "")
    codename = info.get("VERSION_CODENAME", "")
    if distro != "ubuntu" or codename not in {"jammy", "noble"}:
        raise BootstrapError(
            f"Unsupported OS. Expected Ubuntu 22.04/24.04; got ID={distro!r} VERSION_CODENAME={codename!r}."
        )
    return codename


def apt_install(packages: Iterable[str]) -> None:
    pkgs = list(dict.fromkeys(packages))
    if not pkgs:
        return
    run(["apt-get", "update"])
    run(["apt-get", "install", "-y", *pkgs])


def ensure_dir(path: str | Path, mode: int = 0o755) -> None:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    os.chmod(p, mode)


def atomic_write(path: str | Path, content: str, mode: int = 0o644) -> bool:
    p = Path(path)
    old = p.read_text() if p.exists() else None
    if old == content:
        return False
    ensure_dir(p.parent)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(content)
    os.chmod(tmp, mode)
    tmp.replace(p)
    os.chmod(p, mode)
    return True


def replace_or_append_line(path: str | Path, pattern: str, newline: str, *, mode: int = 0o644) -> bool:
    p = Path(path)
    lines = p.read_text().splitlines() if p.exists() else []
    regex = re.compile(pattern)
    replaced = False
    out: list[str] = []
    for line in lines:
        if regex.search(line):
            if not replaced:
                out.append(newline)
                replaced = True
            # drop duplicates
        else:
            out.append(line)
    if not replaced:
        out.append(newline)
    new_text = "\n".join(out).rstrip() + "\n"
    return atomic_write(p, new_text, mode)


def update_hosts(entries: list[str]) -> None:
    if not entries:
        return
    hosts_path = Path("/etc/hosts")
    lines = hosts_path.read_text().splitlines() if hosts_path.exists() else []
    managed = []
    for raw in entries:
        parts = raw.split()
        if len(parts) < 2:
            raise BootstrapError(f"Bad --add-host value: {raw!r}. Expected 'IP HOSTNAME'.")
        ip = parts[0]
        names = parts[1:]
        managed.append((ip, names))

    # Remove exact duplicates we would manage, keep everything else.
    keep: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            keep.append(line)
            continue
        parts = stripped.split()
        if len(parts) >= 2 and any(parts[0] == ip and parts[1:] == names for ip, names in managed):
            continue
        keep.append(line)

    for ip, names in managed:
        keep.append(f"{ip} {' '.join(names)}")
    hosts_path.write_text("\n".join(keep).rstrip() + "\n")
    log("Updated /etc/hosts")


def set_hostname(hostname: str | None) -> None:
    if not hostname:
        return
    current = subprocess.run(["hostnamectl", "--static"], text=True, capture_output=True, check=True).stdout.strip()
    if current == hostname:
        return
    run(["hostnamectl", "set-hostname", hostname])


def install_htcondor_repo(codename: str) -> None:
    apt_install(["ca-certificates", "curl", "gnupg"])
    ensure_dir("/etc/apt/keyrings", 0o755)
    run(["curl", "-fsSL", HTCONDOR_KEY_URL, "-o", "/etc/apt/keyrings/htcondor.asc"])
    repo_line = f"deb [signed-by=/etc/apt/keyrings/htcondor.asc] {HTCONDOR_REPO_BASE} {codename} main\n"
    changed = atomic_write("/etc/apt/sources.list.d/htcondor.list", repo_line, 0o644)
    if changed:
        log("Installed HTCondor APT repository")


def install_htcondor_packages(codename: str) -> None:
    install_htcondor_repo(codename)
    apt_install(["condor"])


def configure_htcondor_base(head_host: str, uid_domain: str, password_file: str, lowport: int, highport: int) -> None:
    ensure_dir("/etc/condor/config.d", 0o755)
    ensure_dir(Path(password_file).parent, 0o700)
    content = f"""use SECURITY : Strong

CONDOR_HOST = {head_host}
COLLECTOR_HOST = $(CONDOR_HOST):{DEFAULT_HTCONDOR_PORT}
UID_DOMAIN = {uid_domain}

SEC_PASSWORD_FILE = {password_file}
SEC_DAEMON_AUTHENTICATION_METHODS = PASSWORD
SEC_NEGOTIATOR_AUTHENTICATION_METHODS = PASSWORD
SEC_CLIENT_AUTHENTICATION_METHODS = FS, PASSWORD

LOWPORT = {lowport}
HIGHPORT = {highport}
"""
    atomic_write("/etc/condor/config.d/10-base.conf", content, 0o644)


def configure_htcondor_role(role: str) -> None:
    role_lines = {
        "head": "use ROLE : CentralManager\nuse ROLE : Submit\n",
        "execute": "use ROLE : Execute\n",
    }
    atomic_write("/etc/condor/config.d/20-role.conf", role_lines[role], 0o644)


def set_pool_password(password_file: str, pool_password: str, overwrite: bool) -> None:
    pw = Path(password_file)
    if pw.exists() and not overwrite:
        log(f"Leaving existing pool password file in place: {password_file}")
        return
    if pw.exists() and overwrite:
        pw.unlink()
    run(["condor_store_cred", "add", "-f", password_file, "-p", pool_password])


def set_pool_password_interactive(password_file: str, overwrite: bool) -> None:
    pw = Path(password_file)
    if pw.exists() and not overwrite:
        log(f"Leaving existing pool password file in place: {password_file}")
        return
    if pw.exists() and overwrite:
        pw.unlink()
    log("HTCondor will now prompt for the pool password; it will not be placed on the command line.")
    run(["condor_store_cred", "add", "-f", password_file])


def push_pool_password_file(password_file: str, hosts: list[str], remote_user: str | None, remote_path: str) -> None:
    src = Path(password_file)
    if not src.exists():
        raise BootstrapError(f"Pool password file does not exist: {password_file}")
    remote_login_prefix = f"{remote_user}@" if remote_user else ""
    for host in hosts:
        target = f"{remote_login_prefix}{host}"
        tmp_path = f"{remote_path}.tmp"
        run(["scp", "-p", password_file, f"{target}:{tmp_path}"])
        remote_cmd = (
            "sudo install -d -m 700 $(dirname {path}) && "
            "sudo install -m 600 -o root -g root {tmp} {path} && "
            "rm -f {tmp}"
        ).format(path=shlex.quote(remote_path), tmp=shlex.quote(tmp_path))
        run(["ssh", target, remote_cmd])


def enable_service(name: str, state: str = "restart") -> None:
    if state == "restart":
        run(["systemctl", "enable", "--now", name])
        run(["systemctl", "restart", name])
    elif state == "start":
        run(["systemctl", "enable", "--now", name])
    else:
        raise ValueError(state)


def configure_storage(export_dir: str, results_mode: int, head_ip: str) -> None:
    apt_install(["nfs-kernel-server"])
    ensure_dir(export_dir, 0o755)
    ensure_dir(Path(export_dir) / "results", results_mode)
    content = f"{export_dir} {head_ip}(rw,sync,no_subtree_check)\n"
    atomic_write("/etc/exports.d/condor-output.exports", content, 0o644)
    run(["exportfs", "-rav"])
    enable_service("nfs-kernel-server", state="start")


def ensure_mount(path: str, source: str, fstype: str, opts: str) -> None:
    ensure_dir(path, 0o755)
    escaped_source = re.escape(source)
    escaped_path = re.escape(path)
    line = f"{source} {path} {fstype} {opts} 0 0"
    replace_or_append_line("/etc/fstab", rf"^{escaped_source}\s+{escaped_path}\s+{re.escape(fstype)}\s", line, mode=0o644)
    mounted = subprocess.run(["mountpoint", "-q", path]).returncode == 0
    if not mounted:
        run(["mount", path])


def configure_head_mount(storage_host: str, export_dir: str, mount_point: str) -> None:
    apt_install(["nfs-common"])
    ensure_mount(mount_point, f"{storage_host}:{export_dir}", "nfs", "defaults,_netdev")


def configure_fail2ban(ignore_ips: list[str]) -> None:
    apt_install(["fail2ban"])
    uniq = []
    for item in ["127.0.0.1/8", "::1", *ignore_ips]:
        if item not in uniq:
            uniq.append(item)
    content = """[DEFAULT]\nignoreip = {ignore}\nbantime = 1h\nfindtime = 10m\nmaxretry = 5\nbackend = systemd\n\n[sshd]\nenabled = true\n""".format(
        ignore=" ".join(uniq)
    )
    atomic_write("/etc/fail2ban/jail.d/sshd.local", content, 0o644)
    run(["fail2ban-client", "-t"])
    enable_service("fail2ban", state="start")


def configure_ssh_keys_only() -> None:
    ensure_dir("/etc/ssh/sshd_config.d", 0o755)
    content = """PermitRootLogin no
PasswordAuthentication no
KbdInteractiveAuthentication no
PubkeyAuthentication yes
X11Forwarding no
MaxAuthTries 3
"""
    atomic_write("/etc/ssh/sshd_config.d/99-cluster-hardening.conf", content, 0o644)
    run(["systemctl", "reload", "ssh"])


def ufw_status_enabled() -> bool:
    cp = run(["ufw", "status"], check=False, capture=True)
    return cp.returncode == 0 and cp.stdout.startswith("Status: active")


def configure_firewall(role: str, admin_allow: list[str], cluster_subnet: str | None, head_ip: str | None, lowport: int, highport: int) -> None:
    apt_install(["ufw"])
    run(["ufw", "--force", "reset"])
    run(["ufw", "default", "deny", "incoming"])
    run(["ufw", "default", "allow", "outgoing"])

    for src in admin_allow:
        run(["ufw", "allow", "from", src, "to", "any", "port", "22", "proto", "tcp"])
    run(["ufw", "limit", "22/tcp"])

    if role == "head":
        if not cluster_subnet:
            raise BootstrapError("--cluster-subnet is required for --enable-firewall on head nodes.")
        run(["ufw", "allow", "from", cluster_subnet, "to", "any", "port", str(DEFAULT_HTCONDOR_PORT), "proto", "tcp"])
        run(["ufw", "allow", "from", cluster_subnet, "to", "any", "port", f"{lowport}:{highport}", "proto", "tcp"])
    elif role == "execute":
        if not cluster_subnet:
            raise BootstrapError("--cluster-subnet is required for --enable-firewall on execute nodes.")
        run(["ufw", "allow", "from", cluster_subnet, "to", "any", "port", f"{lowport}:{highport}", "proto", "tcp"])
    elif role == "storage":
        if not head_ip:
            raise BootstrapError("--head-ip is required for --enable-firewall on storage nodes.")
        run(["ufw", "allow", "from", head_ip, "to", "any", "port", "2049", "proto", "tcp"])

    run(["ufw", "--force", "enable"])


def enable_unattended_upgrades() -> None:
    apt_install(["unattended-upgrades"])
    content = 'APT::Periodic::Update-Package-Lists "1";\nAPT::Periodic::Unattended-Upgrade "1";\n'
    atomic_write("/etc/apt/apt.conf.d/20auto-upgrades", content, 0o644)
    enable_service("unattended-upgrades", state="start")


def write_example_submit(mount_point: str) -> None:
    content = f"""universe = vanilla
executable = /bin/hostname

initialdir = {mount_point}/results/$(Owner)/$(Cluster).$(Process)
log    = condor.log
output = stdout.txt
error  = stderr.txt

should_transfer_files = YES
when_to_transfer_output = ON_EXIT

queue
"""
    atomic_write("/root/hello.sub", content, 0o644)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bootstrap a small Ubuntu HTCondor cluster node")
    p.add_argument("--role", required=True, choices=["head", "execute", "storage"])
    p.add_argument("--set-hostname", dest="set_hostname")
    p.add_argument("--head-host", help="Head node hostname or IP used by HTCondor")
    p.add_argument("--head-ip", help="Head node IP, used for NFS export/firewall rules")
    p.add_argument("--storage-host", help="Storage node hostname or IP (required on head for NFS mount)")
    p.add_argument("--uid-domain", default="cluster.local")
    p.add_argument("--pool-password", help="Shared HTCondor pool password (unsafe in shell history/argv; prefer --prompt-pool-password on the head node)")
    p.add_argument("--prompt-pool-password", action="store_true", help="Prompt interactively for the pool password instead of passing it on the command line")
    p.add_argument("--password-file", default="/etc/condor/passwords.d/pool_password")
    p.add_argument("--overwrite-pool-password", action="store_true")
    p.add_argument("--push-password-file-to", action="append", default=[], help="After creating the pool password file on the head node, copy it over SSH to this host; repeat as needed")
    p.add_argument("--remote-user", help="SSH user used with --push-password-file-to; default is the current user")
    p.add_argument("--cluster-subnet", help="CIDR for internal cluster traffic, e.g. 192.168.50.0/24")
    p.add_argument("--admin-allow", action="append", default=[], help="IP or CIDR allowed to SSH in; repeat as needed")
    p.add_argument("--add-host", action="append", default=[], help="Add /etc/hosts entry like: --add-host '192.168.50.10 htc-head'")
    p.add_argument("--export-dir", default="/srv/condor-output")
    p.add_argument("--mount-point", default="/cluster-output")
    p.add_argument("--lowport", type=int, default=DEFAULT_LOWPORT)
    p.add_argument("--highport", type=int, default=DEFAULT_HIGHPORT)
    p.add_argument("--results-mode", default="1777", help="Octal mode for storage results dir, default 1777")
    p.add_argument("--enable-firewall", action="store_true")
    p.add_argument("--enable-fail2ban", action="store_true")
    p.add_argument("--ssh-keys-only", action="store_true", help="Disable SSH password auth and root SSH login")
    p.add_argument("--enable-unattended-upgrades", action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    require_root()
    codename = assert_supported_ubuntu()

    if args.role in {"head", "execute"}:
        if not args.head_host:
            raise BootstrapError("--head-host is required for head and execute roles.")
        if args.pool_password and args.prompt_pool_password:
            raise BootstrapError("Use either --pool-password or --prompt-pool-password, not both.")
        if args.role == "execute" and args.prompt_pool_password:
            raise BootstrapError("--prompt-pool-password is intended for the head node. Execute nodes should receive the password file or use --pool-password.")
        if not args.pool_password and not args.prompt_pool_password and not Path(args.password_file).exists():
            raise BootstrapError(
                "First-time head/execute setup needs one of: existing --password-file, --prompt-pool-password, or --pool-password."
            )
    if args.role == "head" and not args.storage_host:
        raise BootstrapError("--storage-host is required for the head role.")
    if args.role == "storage" and not args.head_ip:
        raise BootstrapError("--head-ip is required for the storage role.")
    if args.enable_firewall and not args.admin_allow:
        raise BootstrapError("If you enable the firewall, provide at least one --admin-allow entry so you do not lock yourself out.")

    set_hostname(args.set_hostname)
    update_hosts(args.add_host)

    # Common utilities first.
    apt_install(["python3", "python3-apt", "ca-certificates", "curl", "gnupg"])

    if args.role == "storage":
        results_mode = int(args.results_mode, 8)
        configure_storage(args.export_dir, results_mode, args.head_ip)
    else:
        install_htcondor_packages(codename)
        configure_htcondor_base(args.head_host, args.uid_domain, args.password_file, args.lowport, args.highport)
        configure_htcondor_role(args.role)
        if args.prompt_pool_password:
            set_pool_password_interactive(args.password_file, args.overwrite_pool_password)
        else:
            set_pool_password(args.password_file, args.pool_password or "", args.overwrite_pool_password)
        if args.role == "head":
            configure_head_mount(args.storage_host, args.export_dir, args.mount_point)
            write_example_submit(args.mount_point)
            if args.push_password_file_to:
                push_pool_password_file(args.password_file, args.push_password_file_to, args.remote_user, args.password_file)
        enable_service("condor", state="restart")

    if args.enable_unattended_upgrades:
        enable_unattended_upgrades()

    # Hardening last, after services are up and tested.
    if args.ssh_keys_only:
        warn("Enabling SSH keys-only access. Make sure your admin key already works before using this option.")
        configure_ssh_keys_only()

    if args.enable_fail2ban:
        ignore_ips: list[str] = []
        ignore_ips.extend(args.admin_allow)
        for raw in args.add_host:
            ignore_ips.append(raw.split()[0])
        if args.head_ip:
            ignore_ips.append(args.head_ip)
        if args.cluster_subnet:
            ignore_ips.append(args.cluster_subnet)
        configure_fail2ban(ignore_ips)

    if args.enable_firewall:
        configure_firewall(args.role, args.admin_allow, args.cluster_subnet, args.head_ip, args.lowport, args.highport)

    log("Done.")
    if args.role == "head":
        log("On the head node, verify with: condor_status && mount | grep '{}'".format(args.mount_point))
        log("Example submit file written to /root/hello.sub")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except BootstrapError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2)
