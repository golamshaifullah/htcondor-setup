HTCondor cluster bootstrap script
================================

Files
-----
- htc_node_bootstrap.py

Supported roles
---------------
- head    : HTCondor Central Manager + Submit + NFS client mount
- execute : HTCondor Execute node
- storage : NFS server for job outputs

Important notes
---------------
- Run as root or via sudo.
- Supported OS: Ubuntu 22.04 (jammy) and 24.04 (noble).
- Preferred secret flow:
  1) On the head node, use --prompt-pool-password so the password is not passed in argv.
  2) Optionally add --push-password-file-to HOST repeatedly so the generated
     /etc/condor/passwords.d/pool_password file is copied over SSH to execute nodes.
  3) Then run the execute-node script without --pool-password.
- --push-password-file-to assumes SSH access from the head node to the target host.
  If root SSH is disabled, use --remote-user USER and make sure that user has sudo.

Example host layout
-------------------
htc-head      192.168.50.10
htc-storage   192.168.50.20
htc-exec01    192.168.50.31
htc-exec02    192.168.50.32

1) Storage node
---------------
sudo python3 htc_node_bootstrap.py \
  --role storage \
  --set-hostname htc-storage \
  --head-ip 192.168.50.10 \
  --add-host '192.168.50.10 htc-head' \
  --add-host '192.168.50.20 htc-storage' \
  --add-host '192.168.50.31 htc-exec01' \
  --add-host '192.168.50.32 htc-exec02'

2) Head node (secure password prompt + optional file push)
----------------------------------------------------------
sudo python3 htc_node_bootstrap.py \
  --role head \
  --set-hostname htc-head \
  --head-host htc-head \
  --storage-host htc-storage \
  --prompt-pool-password \
  --push-password-file-to htc-exec01 \
  --push-password-file-to htc-exec02 \
  --remote-user YOURADMIN \
  --add-host '192.168.50.10 htc-head' \
  --add-host '192.168.50.20 htc-storage' \
  --add-host '192.168.50.31 htc-exec01' \
  --add-host '192.168.50.32 htc-exec02'

3) Execute nodes (password file already copied)
------------------------------------------------
sudo python3 htc_node_bootstrap.py \
  --role execute \
  --set-hostname htc-exec01 \
  --head-host htc-head \
  --add-host '192.168.50.10 htc-head' \
  --add-host '192.168.50.20 htc-storage' \
  --add-host '192.168.50.31 htc-exec01' \
  --add-host '192.168.50.32 htc-exec02'

Repeat on htc-exec02 with --set-hostname htc-exec02.

4) Verify on the head node
--------------------------
condor_status
mount | grep /cluster-output

5) Test job on the head node
----------------------------
A sample submit file is written to /root/hello.sub.

Submit it with:
condor_submit /root/hello.sub

Optional hardening flags
------------------------
- --enable-unattended-upgrades
- --enable-firewall --admin-allow 192.168.50.0/24 --cluster-subnet 192.168.50.0/24
- --enable-fail2ban
- --ssh-keys-only

Do not enable firewall/fail2ban/SSH hardening until condor_status works.
