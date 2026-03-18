#!/usr/bin/env python3
"""Generate a minimal Ansible bundle for managing head-node users and optional hardening
on a preinstalled HTCondor cluster.

This generator intentionally does NOT modify HTCondor configuration.
It assumes users log into the head node (CM+AP) and submit jobs there.
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import shutil
import socket
import subprocess
import sys
import tarfile
import textwrap
from typing import List, Tuple


DEFAULT_SSHD_ALLOWED = [
    "PermitRootLogin no",
    "PasswordAuthentication no",
    "KbdInteractiveAuthentication no",
    "PubkeyAuthentication yes",
    "X11Forwarding no",
    "MaxAuthTries 3",
]


def run(cmd: List[str], check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, text=True, capture_output=True)


def install_ansible_with_pipx() -> None:
    # Follow official Ansible guidance: pipx/pip installs are the supported instructions.
    cmds = [
        [sys.executable, "-m", "pip", "install", "--user", "pipx"],
        [sys.executable, "-m", "pipx", "ensurepath"],
        [os.path.expanduser("~/.local/bin/pipx"), "install", "--include-deps", "ansible"],
        [os.path.expanduser("~/.local/bin/pipx"), "inject", "ansible", "ansible-lint"],
        [os.path.expanduser("~/.local/bin/pipx"), "inject", "ansible", "community.general"],
    ]
    for cmd in cmds:
        try:
            run(cmd)
        except Exception as exc:
            raise SystemExit(f"Failed while running {' '.join(cmd)}: {exc}")


def hostname_fallback() -> str:
    try:
        return socket.gethostname()
    except Exception:
        return "htc-head"


def guess_ip() -> str:
    try:
        return socket.gethostbyname(hostname_fallback())
    except Exception:
        return "127.0.0.1"


def parse_execs(items: List[str]) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    for item in items:
        if "=" not in item:
            raise SystemExit(f"Invalid --execute value '{item}'. Expected name=ip")
        name, ip = item.split("=", 1)
        out.append((name.strip(), ip.strip()))
    return out


README = """# Minimal Ansible for a preinstalled HTCondor cluster

This bundle assumes:
- HTCondor is already installed and working.
- Users log into the head node and submit jobs there.
- Worker HTCondor configuration should be left alone.

What it manages:
- Head-node Linux users
- SSH authorized keys for those users
- Optional head/all-node hardening (UFW, Fail2ban, SSH config)
- Verification

What it does NOT manage:
- `/etc/condor/config.d/*`
- HTCondor package installation
- HTCondor password files
- HTCondor role or policy changes

## Files
- `inventory.ini`
- `group_vars/all.yml`
- `users.yml`
- `hardening.yml`
- `verify.yml`
- `users.example.yml`

## Typical workflow

1. Review `inventory.ini` and `group_vars/all.yml`.
2. Copy `users.example.yml` to `users.yml` and edit it.
3. Verify SSH access:
   ```bash
   ansible -i inventory.ini all -m ping
   ```
4. Dry-run user management:
   ```bash
   ansible-playbook -i inventory.ini users.yml --check --diff
   ```
5. Apply user management:
   ```bash
   ansible-playbook -i inventory.ini users.yml
   ```
6. Dry-run hardening:
   ```bash
   ansible-playbook -i inventory.ini hardening.yml --check --diff
   ```
7. Apply hardening:
   ```bash
   ansible-playbook -i inventory.ini hardening.yml
   ```
8. Verify:
   ```bash
   ansible-playbook -i inventory.ini verify.yml
   ```

## User file format

Each user entry looks like this:

```yaml
cluster_users:
  - name: alice
    comment: Alice Example
    groups: ["clusterusers"]
    shell: /bin/bash
    state: present
    ssh_keys:
      - "ssh-ed25519 AAAA... alice@example"
  - name: olduser
    state: absent
    remove_home: true
```

Notes:
- `state: absent` removes the account.
- `remove_home: true` removes the home directory on deletion.
- Keys are written to `~/.ssh/authorized_keys`.

## Safety notes
- `users.yml` targets the **head node only** by default.
- `hardening.yml` does **not** modify HTCondor config.
- UFW rules allow SSH from the configured admin CIDRs and HTCondor traffic from cluster CIDRs.
- Fail2ban is limited to the `sshd` jail.
"""


def build_inventory(head_name: str, head_ip: str, executes: List[Tuple[str, str]]) -> str:
    lines = ["[head]", f"{head_name} ansible_host={head_ip}", "", "[execute]"]
    for name, ip in executes:
        lines.append(f"{name} ansible_host={ip}")
    lines.extend(["", "[cluster:children]", "head", "execute", ""])
    return "\n".join(lines)


def build_group_vars(ansible_user: str, admin_cidrs: List[str], cluster_cidrs: List[str], user_group: str) -> str:
    admin_str = json.dumps(admin_cidrs)
    cluster_str = json.dumps(cluster_cidrs)
    return textwrap.dedent(
        f"""
        ansible_user: {ansible_user}
        ansible_become: true

        cluster_user_group: {user_group}
        admin_allow_cidrs: {admin_str}
        cluster_allow_cidrs: {cluster_str}

        enable_ufw: true
        enable_fail2ban: true

        htcondor_collector_port: 9618
        htcondor_lowport: 20000
        htcondor_highport: 20100

        sshd_hardening_lines:
        """
    ).lstrip() + "\n" + "\n".join(f"  - {line}" for line in DEFAULT_SSHD_ALLOWED) + "\n"


def build_users_play() -> str:
    return textwrap.dedent(
        """
        ---
        - name: Manage head-node users only
          hosts: head
          become: true
          vars_files:
            - users.example.yml
          pre_tasks:
            - name: Ensure shared cluster user group exists
              ansible.builtin.group:
                name: "{{ cluster_user_group }}"
                state: present

          tasks:
            - name: Manage user accounts
              ansible.builtin.user:
                name: "{{ item.name }}"
                comment: "{{ item.comment | default(omit) }}"
                shell: "{{ item.shell | default('/bin/bash') }}"
                groups: "{{ (item.groups | default([])) + [cluster_user_group] }}"
                append: true
                state: "{{ item.state | default('present') }}"
                remove: "{{ item.remove_home | default(false) }}"
                create_home: true
              loop: "{{ cluster_users }}"
              loop_control:
                label: "{{ item.name }}"

            - name: Ensure .ssh directory exists for present users
              ansible.builtin.file:
                path: "/home/{{ item.name }}/.ssh"
                state: directory
                owner: "{{ item.name }}"
                group: "{{ item.name }}"
                mode: "0700"
              loop: "{{ cluster_users | selectattr('state', 'undefined') | list + (cluster_users | selectattr('state', 'equalto', 'present') | list) }}"
              loop_control:
                label: "{{ item.name }}"
              when: item.ssh_keys is defined and (item.ssh_keys | length) > 0

            - name: Install authorized SSH keys for present users
              ansible.posix.authorized_key:
                user: "{{ item.0.name }}"
                key: "{{ item.1 }}"
                state: present
              loop: "{{ cluster_users | subelements('ssh_keys', skip_missing=True) }}"
              loop_control:
                label: "{{ item.0.name }}"
              when: item.0.state | default('present') == 'present'
        """
    ).lstrip()


def build_hardening_play() -> str:
    return textwrap.dedent(
        """
        ---
        - name: Harden head and cluster nodes without touching HTCondor config
          hosts: cluster
          become: true
          tasks:
            - name: Install hardening packages
              ansible.builtin.apt:
                name:
                  - ufw
                  - fail2ban
                  - unattended-upgrades
                state: present
                update_cache: true

            - name: Install SSH hardening fragment
              ansible.builtin.copy:
                dest: /etc/ssh/sshd_config.d/99-cluster-hardening.conf
                owner: root
                group: root
                mode: "0644"
                content: |
                  {% for line in sshd_hardening_lines %}
                  {{ line }}
                  {% endfor %}
              notify: Reload ssh

            - name: Install Fail2ban SSH jail
              ansible.builtin.copy:
                dest: /etc/fail2ban/jail.d/sshd.local
                owner: root
                group: root
                mode: "0644"
                content: |
                  [DEFAULT]
                  ignoreip = 127.0.0.1/8 ::1 {% for cidr in admin_allow_cidrs %} {{ cidr }}{% endfor %}{% for host in groups['cluster'] %} {{ hostvars[host].ansible_host | default(host) }}{% endfor %}
                  bantime = 1h
                  findtime = 10m
                  maxretry = 5
                  backend = systemd

                  [sshd]
                  enabled = true
              notify: Restart fail2ban

            - name: Reset ufw to defaults
              community.general.ufw:
                state: reset
              when: enable_ufw | bool

            - name: Set default deny incoming
              community.general.ufw:
                policy: deny
                direction: incoming
              when: enable_ufw | bool

            - name: Set default allow outgoing
              community.general.ufw:
                policy: allow
                direction: outgoing
              when: enable_ufw | bool

            - name: Allow SSH from admin CIDRs
              community.general.ufw:
                rule: allow
                proto: tcp
                src: "{{ item }}"
                port: "22"
              loop: "{{ admin_allow_cidrs }}"
              when: enable_ufw | bool

            - name: Rate-limit SSH generally
              community.general.ufw:
                rule: limit
                proto: tcp
                port: "22"
              when: enable_ufw | bool

            - name: Allow HTCondor collector from cluster CIDRs on head only
              community.general.ufw:
                rule: allow
                proto: tcp
                src: "{{ item }}"
                port: "{{ htcondor_collector_port }}"
              loop: "{{ cluster_allow_cidrs }}"
              when: enable_ufw | bool and 'head' in group_names

            - name: Allow HTCondor worker port range from cluster CIDRs
              community.general.ufw:
                rule: allow
                proto: tcp
                src: "{{ item }}"
                port: "{{ htcondor_lowport }}:{{ htcondor_highport }}"
              loop: "{{ cluster_allow_cidrs }}"
              when: enable_ufw | bool

            - name: Enable ufw
              community.general.ufw:
                state: enabled
              when: enable_ufw | bool

            - name: Enable unattended-upgrades periodic config
              ansible.builtin.copy:
                dest: /etc/apt/apt.conf.d/20auto-upgrades
                owner: root
                group: root
                mode: "0644"
                content: |
                  APT::Periodic::Update-Package-Lists "1";
                  APT::Periodic::Unattended-Upgrade "1";

            - name: Enable services
              ansible.builtin.service:
                name: "{{ item }}"
                enabled: true
                state: started
              loop:
                - fail2ban

          handlers:
            - name: Reload ssh
              ansible.builtin.service:
                name: ssh
                state: reloaded

            - name: Restart fail2ban
              ansible.builtin.service:
                name: fail2ban
                state: restarted
        """
    ).lstrip()


def build_verify_play() -> str:
    return textwrap.dedent(
        """
        ---
        - name: Verify head users and cluster basics
          hosts: cluster
          become: true
          tasks:
            - name: Check condor binary exists
              ansible.builtin.command: command -v condor_status
              changed_when: false
              register: condor_status_bin

            - name: Check condor service state
              ansible.builtin.service_facts:

            - name: Show condor service presence
              ansible.builtin.debug:
                msg: >-
                  condor installed={{ condor_status_bin.rc == 0 }};
                  service state={{ ansible_facts.services['condor.service'].state if 'condor.service' in ansible_facts.services else 'missing' }}

        - name: Verify collector from head
          hosts: head
          become: true
          tasks:
            - name: Query collector
              ansible.builtin.command: condor_status -collector
              changed_when: false
              register: collector_query
              failed_when: false

            - name: Show collector output
              ansible.builtin.debug:
                var: collector_query.stdout_lines
        """
    ).lstrip()


def build_ansible_cfg() -> str:
    return textwrap.dedent(
        """
        [defaults]
        inventory = inventory.ini
        host_key_checking = False
        retry_files_enabled = False
        stdout_callback = yaml
        interpreter_python = auto_silent
        """
    ).lstrip()


def build_users_example() -> str:
    return textwrap.dedent(
        """
        cluster_users:
          - name: alice
            comment: Alice Example
            groups: ["research"]
            shell: /bin/bash
            state: present
            ssh_keys:
              - "ssh-ed25519 AAAA_REPLACE_ME alice@example"

          - name: bob
            comment: Bob Example
            groups: ["research"]
            shell: /bin/bash
            state: present
            ssh_keys:
              - "ssh-ed25519 AAAA_REPLACE_ME bob@example"

          - name: olduser
            state: absent
            remove_home: true
        """
    ).lstrip()


def ensure_clean_dir(path: pathlib.Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    (path / "group_vars").mkdir(parents=True, exist_ok=True)


def write_file(path: pathlib.Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def tar_dir(src: pathlib.Path, dest_tgz: pathlib.Path) -> None:
    with tarfile.open(dest_tgz, "w:gz") as tf:
        tf.add(src, arcname=src.name)


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate minimal post-install Ansible for head-node users on an existing HTCondor cluster")
    ap.add_argument("--install-ansible", action="store_true", help="Install Ansible on the local control/head node using pipx")
    ap.add_argument("--project-dir", required=True, help="Where to write the Ansible bundle")
    ap.add_argument("--head-name", default=hostname_fallback(), help="Head node inventory name (defaults to local hostname)")
    ap.add_argument("--head-ip", default=guess_ip(), help="Head node IP address (defaults to resolved local hostname)")
    ap.add_argument("--execute", action="append", default=[], help="Execute node in name=ip form; may be repeated")
    ap.add_argument("--ansible-user", default=os.environ.get("SUDO_USER") or os.environ.get("USER") or "ubuntu", help="SSH user Ansible should use")
    ap.add_argument("--admin-allow", action="append", default=["127.0.0.1/32"], help="CIDR allowed to SSH to managed hosts; may be repeated")
    ap.add_argument("--cluster-subnet", action="append", default=["127.0.0.1/32"], help="CIDR allowed for HTCondor inter-node traffic; may be repeated")
    ap.add_argument("--cluster-user-group", default="clusterusers", help="Shared Linux group for head-node user accounts")
    ap.add_argument("--make-tarball", action="store_true", help="Also create a .tar.gz next to the project dir")
    args = ap.parse_args()

    if args.install_ansible:
        install_ansible_with_pipx()

    executes = parse_execs(args.execute)
    project_dir = pathlib.Path(os.path.expanduser(args.project_dir)).resolve()
    ensure_clean_dir(project_dir)

    write_file(project_dir / "README.md", README)
    write_file(project_dir / "ansible.cfg", build_ansible_cfg())
    write_file(project_dir / "inventory.ini", build_inventory(args.head_name, args.head_ip, executes))
    write_file(project_dir / "group_vars" / "all.yml", build_group_vars(args.ansible_user, args.admin_allow, args.cluster_subnet, args.cluster_user_group))
    write_file(project_dir / "users.yml", build_users_play())
    write_file(project_dir / "hardening.yml", build_hardening_play())
    write_file(project_dir / "verify.yml", build_verify_play())
    write_file(project_dir / "users.example.yml", build_users_example())

    if args.make_tarball:
        tar_path = project_dir.parent / f"{project_dir.name}.tar.gz"
        tar_dir(project_dir, tar_path)
        print(f"Wrote tarball: {tar_path}")

    print(f"Wrote bundle to {project_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
