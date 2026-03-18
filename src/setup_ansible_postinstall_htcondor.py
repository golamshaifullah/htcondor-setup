#!/usr/bin/env python3
"""
Generate a strict post-install Ansible bundle for an already-installed HTCondor cluster.

Design goals:
- Run on the head node / HTCondor manager after HTCondor is already installed everywhere.
- Optionally install ansible locally on the head node.
- Do NOT touch apt repositories or install packages on any node.
- Do NOT rewrite the head node's HTCondor config.
- Generate playbooks that manage execute-node overrides, copy the existing pool password
  file from the head to workers, restart condor on workers, and verify the cluster.
- Keep the "users SSH to the head node and submit jobs there" model explicit.
"""

from __future__ import annotations

import argparse
import getpass
import os
import pathlib
import re
import shutil
import socket
import subprocess
import sys
import tarfile
from textwrap import dedent
from typing import Optional


def run(cmd, check=True, capture=True, text=True, sudo=False):
    final = list(cmd)
    if sudo and os.geteuid() != 0:
        final = ["sudo"] + final
    return subprocess.run(final, check=check, capture_output=capture, text=text)


def command_exists(name: str) -> bool:
    return shutil.which(name) is not None


def read_os_release() -> dict[str, str]:
    data: dict[str, str] = {}
    path = pathlib.Path("/etc/os-release")
    if path.exists():
        for line in path.read_text().splitlines():
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            data[k] = v.strip().strip('"')
    return data


def get_ubuntu_codename() -> str:
    osr = read_os_release()
    codename = osr.get("VERSION_CODENAME")
    if not codename:
        raise RuntimeError("Could not determine Ubuntu codename from /etc/os-release")
    return codename


def condor_config_val(name: str) -> Optional[str]:
    if not command_exists("condor_config_val"):
        return None
    try:
        proc = run(["condor_config_val", name])
        val = proc.stdout.strip()
        return val or None
    except subprocess.CalledProcessError:
        return None


def derive_head_name() -> str:
    return socket.gethostname()


def derive_head_ip(default_name: str) -> str:
    condor_host = condor_config_val("CONDOR_HOST")
    if condor_host:
        cleaned = condor_host.strip()
        if re.fullmatch(r"\d+\.\d+\.\d+\.\d+", cleaned):
            return cleaned
    try:
        return socket.gethostbyname(default_name)
    except socket.gaierror:
        return "127.0.0.1"


def safe(text: Optional[str], default: str) -> str:
    return text.strip() if text and text.strip() else default


def install_ansible() -> None:
    """
    Install ansible on the local head node using pipx.
    """
    print("Installing Ansible locally on the head node...")
    if not command_exists("python3"):
        raise RuntimeError("python3 is required")

    try:
        run(["apt-get", "update"], sudo=True, capture=False)
        run(["apt-get", "install", "-y", "python3-pip", "python3-venv"], sudo=True, capture=False)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to install python3-pip/python3-venv: {e}") from e

    try:
        run(["python3", "-m", "pip", "install", "--user", "pipx"], capture=False)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to install pipx in user environment: {e}") from e

    try:
        run(["python3", "-m", "pipx", "ensurepath"], capture=False)
    except subprocess.CalledProcessError:
        pass

    ansible_bin = shutil.which("ansible")
    if ansible_bin:
        print(f"Ansible already present at {ansible_bin}")
        return

    try:
        run(["python3", "-m", "pipx", "install", "--include-deps", "ansible"], capture=False)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to install ansible with pipx: {e}") from e

    ansible_bin = shutil.which("ansible")
    if not ansible_bin:
        home_ansible = pathlib.Path.home() / ".local" / "bin" / "ansible"
        if home_ansible.exists():
            ansible_bin = str(home_ansible)

    if ansible_bin:
        print(f"Ansible installed at {ansible_bin}")
    else:
        print("Ansible installation completed, but ansible is not yet on PATH.")
        print("Log out and back in, or export PATH=$HOME/.local/bin:$PATH")


def parse_execute(entry: str) -> tuple[str, str]:
    if "=" not in entry:
        raise argparse.ArgumentTypeError("Use --execute name=ip")
    name, ip = entry.split("=", 1)
    name = name.strip()
    ip = ip.strip()
    if not name or not ip:
        raise argparse.ArgumentTypeError("Use --execute name=ip")
    return name, ip


def policy_snippet(policy: str) -> str:
    if policy == "dedicated":
        return "use ROLE : Execute\n"
    if policy == "desktop":
        return "use ROLE : Execute\nuse POLICY : Desktop\n"
    if policy == "desktop-idle":
        return "use ROLE : Execute\nuse POLICY : DESKTOP_IDLE()\n"
    raise ValueError(f"Unsupported policy {policy}")


def generate_files(
    project_dir: pathlib.Path,
    ansible_user: str,
    head_name: str,
    head_ip: str,
    executes: list[tuple[str, str]],
    uid_domain: str,
    sec_password_file: str,
    lowport: str,
    highport: str,
    worker_policy: str,
    override_path: str,
) -> None:
    project_dir.mkdir(parents=True, exist_ok=True)
    (project_dir / "group_vars").mkdir(exist_ok=True)

    inventory_lines = [
        "[head]",
        f"{head_name} ansible_host={head_ip}",
        "",
        "[execute]",
    ]
    for name, ip in executes:
        inventory_lines.append(f"{name} ansible_host={ip}")
    inventory_lines += ["", "[htcondor:children]", "head", "execute", ""]
    inventory = "\n".join(inventory_lines)

    ansible_cfg = dedent("""
    [defaults]
    inventory = inventory.ini
    retry_files_enabled = False
    stdout_callback = yaml
    interpreter_python = auto_silent
    """).lstrip()

    all_yml = dedent(f"""
    ansible_user: {ansible_user}
    ansible_become: true

    htcondor_head_name: {head_name}
    htcondor_head_ip: {head_ip}
    htcondor_uid_domain: {uid_domain}
    htcondor_sec_password_file: {sec_password_file}
    htcondor_lowport: "{lowport}"
    htcondor_highport: "{highport}"
    htcondor_worker_policy: {worker_policy}
    htcondor_worker_override_path: {override_path}
    """).lstrip()

    worker_policy_content = policy_snippet(worker_policy).rstrip()

    manage_workers = dedent(f"""
    ---
    - name: Strictly manage execute nodes for an existing HTCondor head node
      hosts: execute
      become: true
      serial: 1
      tasks:
        - name: Assert condor is already installed on worker
          ansible.builtin.command: command -v condor_config_val
          changed_when: false

        - name: Assert condor service unit exists on worker
          ansible.builtin.stat:
            path: /lib/systemd/system/condor.service
          register: condor_service_unit

        - name: Fail if condor service unit is missing
          ansible.builtin.assert:
            that:
              - condor_service_unit.stat.exists
            fail_msg: "HTCondor does not appear to be installed on {{{{ inventory_hostname }}}}. This strict bundle will not install packages."

        - name: Ensure HTCondor password directory exists
          ansible.builtin.file:
            path: "{{{{ htcondor_sec_password_file | dirname }}}}"
            state: directory
            owner: root
            group: root
            mode: "0700"

        - name: Copy existing pool password file from the head node
          ansible.builtin.copy:
            src: "{{{{ htcondor_sec_password_file }}}}"
            dest: "{{{{ htcondor_sec_password_file }}}}"
            owner: root
            group: root
            mode: "0600"

        - name: Install ansible-managed worker override
          ansible.builtin.copy:
            dest: "{{{{ htcondor_worker_override_path }}}}"
            owner: root
            group: root
            mode: "0644"
            backup: true
            content: |
              # Generated for an already-installed cluster.
              # Users should SSH to the head node and submit jobs there.
              CONDOR_HOST = {{{{ htcondor_head_ip }}}}
              COLLECTOR_HOST = $(CONDOR_HOST):9618
              UID_DOMAIN = {{{{ htcondor_uid_domain }}}}
              SEC_PASSWORD_FILE = {{{{ htcondor_sec_password_file }}}}
              SEC_DAEMON_AUTHENTICATION_METHODS = PASSWORD
              SEC_NEGOTIATOR_AUTHENTICATION_METHODS = PASSWORD
              SEC_CLIENT_AUTHENTICATION_METHODS = FS, PASSWORD
              LOWPORT = {{{{ htcondor_lowport }}}}
              HIGHPORT = {{{{ htcondor_highport }}}}

              {worker_policy_content}

        - name: Enable and restart condor on execute nodes
          ansible.builtin.service:
            name: condor
            state: restarted
            enabled: true
    """).lstrip()

    verify = dedent("""
    ---
    - name: Verify head node HTCondor daemons
      hosts: head
      become: false
      tasks:
        - name: Query collector ad
          ansible.builtin.command: condor_status -collector
          register: collector_status
          changed_when: false

        - name: Query schedd ad
          ansible.builtin.command: condor_status -schedd
          register: schedd_status
          changed_when: false

        - name: Show collector query
          ansible.builtin.debug:
            var: collector_status.stdout_lines

        - name: Show schedd query
          ansible.builtin.debug:
            var: schedd_status.stdout_lines

        - name: Query current pool slots
          ansible.builtin.command: condor_status
          register: pool_status
          changed_when: false

        - name: Show current pool slots
          ansible.builtin.debug:
            var: pool_status.stdout_lines

    - name: Verify execute-node condor service
      hosts: execute
      become: true
      tasks:
        - name: Gather service facts
          ansible.builtin.service_facts:

        - name: Show condor service state
          ansible.builtin.debug:
            msg: "condor service on {{ inventory_hostname }} is {{ ansible_facts.services['condor.service'].state | default('unknown') }}"
    """).lstrip()

    readme = dedent(f"""
    # Strict HTCondor post-install Ansible bundle

    This bundle is meant to be generated and run **after** the HTCondor cluster already exists.

    Design:
    - The **head node** is already your HTCondor manager and submit host.
    - Users log into the **head node** and submit jobs there.
    - Workers are managed as **execute-only** nodes by Ansible.
    - This bundle does **not** touch APT, repositories, or package installation.

    ## What this bundle changes

    - It does **not** rebuild the head node's HTCondor config.
    - It does **not** install or upgrade HTCondor packages.
    - It manages execute nodes by writing `{override_path}`.
    - It copies the existing pool password file from the head node to execute nodes.
    - It restarts `condor` on execute nodes.

    ## Before you run it

    1. Run it on the **head node**.
    2. Make sure the head node already has the pool password file at:
       `{sec_password_file}`
    3. Make sure the user running Ansible can SSH to workers and use `sudo`.
    4. Make sure HTCondor is already installed on every worker.

    ## Generated values

    - head name: `{head_name}`
    - head IP / CONDOR_HOST: `{head_ip}`
    - UID_DOMAIN: `{uid_domain}`
    - SEC_PASSWORD_FILE: `{sec_password_file}`
    - worker override path: `{override_path}`
    - worker policy: `{worker_policy}`
    - Ubuntu codename detected on head: `{get_ubuntu_codename()}`

    ## Suggested run order

    ```bash
    ansible -i inventory.ini all -m ping
    ansible-playbook manage_workers.yml --check --diff
    ansible-playbook manage_workers.yml
    ansible-playbook verify.yml
    ```

    ## Inventory

    Review `inventory.ini` before running anything. The generated workers are:

    {os.linesep.join(f"- {name} = {ip}" for name, ip in executes) if executes else "- none specified"}
    """).lstrip()

    (project_dir / "inventory.ini").write_text(inventory)
    (project_dir / "ansible.cfg").write_text(ansible_cfg)
    (project_dir / "group_vars" / "all.yml").write_text(all_yml)
    (project_dir / "manage_workers.yml").write_text(manage_workers)
    (project_dir / "verify.yml").write_text(verify)
    (project_dir / "README.md").write_text(readme)


def maybe_create_tarball(project_dir: pathlib.Path, tarball_path: pathlib.Path) -> None:
    with tarfile.open(tarball_path, "w:gz") as tf:
        tf.add(project_dir, arcname=project_dir.name)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Generate a strict post-install Ansible bundle for an existing HTCondor cluster."
    )
    p.add_argument("--project-dir", required=True, help="Where to write the generated bundle")
    p.add_argument("--install-ansible", action="store_true", help="Install ansible locally on the head node")
    p.add_argument("--head-name", help="Head node inventory name; defaults to local hostname")
    p.add_argument("--head-ip", help="Head node IP / CONDOR_HOST; defaults to current HTCondor config or local resolution")
    p.add_argument("--ansible-user", default=getpass.getuser(), help="SSH user for Ansible (default: current user)")
    p.add_argument("--execute", action="append", default=[], help="Execute node as name=ip; repeat for each worker")
    p.add_argument("--worker-policy", choices=["dedicated", "desktop", "desktop-idle"], default="desktop-idle")
    p.add_argument("--uid-domain", help="Override UID_DOMAIN from current HTCondor config")
    p.add_argument("--sec-password-file", help="Override SEC_PASSWORD_FILE from current HTCondor config")
    p.add_argument("--lowport", help="Override LOWPORT from current HTCondor config")
    p.add_argument("--highport", help="Override HIGHPORT from current HTCondor config")
    p.add_argument("--override-path", default="/etc/condor/config.d/90-ansible-execute.conf", help="Worker override file path")
    p.add_argument("--tarball", help="Optional .tar.gz path to create after generating the bundle")
    return p


def main() -> int:
    args = build_parser().parse_args()

    if args.install_ansible:
        install_ansible()

    if not command_exists("condor_config_val"):
        print("Warning: condor_config_val not found. Using fallbacks where needed.", file=sys.stderr)

    head_name = safe(args.head_name, derive_head_name())
    head_ip = safe(args.head_ip, derive_head_ip(head_name))
    uid_domain = safe(args.uid_domain, condor_config_val("UID_DOMAIN") or "cluster.local")
    sec_password_file = safe(args.sec_password_file, condor_config_val("SEC_PASSWORD_FILE") or "/etc/condor/passwords.d/POOL")
    lowport = safe(args.lowport, condor_config_val("LOWPORT") or "20000")
    highport = safe(args.highport, condor_config_val("HIGHPORT") or "20100")

    executes = [parse_execute(x) for x in args.execute]

    project_dir = pathlib.Path(args.project_dir).expanduser().resolve()
    generate_files(
        project_dir=project_dir,
        ansible_user=args.ansible_user,
        head_name=head_name,
        head_ip=head_ip,
        executes=executes,
        uid_domain=uid_domain,
        sec_password_file=sec_password_file,
        lowport=lowport,
        highport=highport,
        worker_policy=args.worker_policy,
        override_path=args.override_path,
    )

    if args.tarball:
        maybe_create_tarball(project_dir, pathlib.Path(args.tarball).expanduser().resolve())

    print(f"Generated strict Ansible bundle in {project_dir}")
    print("Next steps:")
    print(f"  cd {project_dir}")
    print("  ansible -i inventory.ini all -m ping")
    print("  ansible-playbook manage_workers.yml --check --diff")
    print("  ansible-playbook manage_workers.yml")
    print("  ansible-playbook verify.yml")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
