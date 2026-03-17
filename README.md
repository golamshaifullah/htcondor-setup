### Easy HTCondor setups.

htc_node_bootstrap.py is a simple python script that tries to make setting up htcondor clusters a bit easier. Currently tested only for an Ubuntu 24.04 / 22.04 based cluster. 

Assumptions
- Ubuntu 22.04 or 24.04
- one head node (CM+Submit)
- one storage node (NFS)
- one or more execute nodes
- cluster subnet: 192.168.50.0/24
- nodes:
  192.168.50.10 htc-head
  192.168.50.20 htc-storage
  192.168.50.31 htc-exec01
  192.168.50.32 htc-exec02

Run order
1) storage node
2) head node
3) execute nodes
4) only after the pool works, rerun with hardening flags

1) Storage node
sudo python3 htc_node_bootstrap.py \
  --role storage \
  --set-hostname htc-storage \
  --head-ip 192.168.50.10 \
  --add-host '192.168.50.10 htc-head' \
  --add-host '192.168.50.20 htc-storage' \
  --add-host '192.168.50.31 htc-exec01' \
  --add-host '192.168.50.32 htc-exec02'

2) Head node
Preferred: prompt once for the pool password and push the resulting password file to workers over ssh/scp.
This avoids putting the secret in argv.

sudo python3 htc_node_bootstrap.py \
  --role head \
  --set-hostname htc-head \
  --head-host htc-head \
  --storage-host htc-storage \
  --prompt-pool-password \
  --push-password-file-to htc-exec01 \
  --push-password-file-to htc-exec02 \
  --remote-user YOURADMIN \
  --uid-domain cluster.local \
  --cluster-subnet 192.168.50.0/24 \
  --admin-allow 192.168.50.0/24 \
  --add-host '192.168.50.10 htc-head' \
  --add-host '192.168.50.20 htc-storage' \
  --add-host '192.168.50.31 htc-exec01' \
  --add-host '192.168.50.32 htc-exec02'

If you insist on passing the password directly instead:

sudo python3 htc_node_bootstrap.py \
  --role head \
  --set-hostname htc-head \
  --head-host htc-head \
  --storage-host htc-storage \
  --pool-password 'CHANGE-THIS-TO-A-LONG-RANDOM-SECRET' \
  --uid-domain cluster.local \
  --cluster-subnet 192.168.50.0/24 \
  --admin-allow 192.168.50.0/24 \
  --add-host '192.168.50.10 htc-head' \
  --add-host '192.168.50.20 htc-storage' \
  --add-host '192.168.50.31 htc-exec01' \
  --add-host '192.168.50.32 htc-exec02'

3) Execute node
If you already pushed the password file from the head, do not pass --pool-password again.

Dedicated worker:
sudo python3 htc_node_bootstrap.py \
  --role execute \
  --worker-policy dedicated \
  --set-hostname htc-exec01 \
  --head-host htc-head \
  --uid-domain cluster.local \
  --cluster-subnet 192.168.50.0/24 \
  --admin-allow 192.168.50.0/24 \
  --add-host '192.168.50.10 htc-head' \
  --add-host '192.168.50.20 htc-storage' \
  --add-host '192.168.50.31 htc-exec01' \
  --add-host '192.168.50.32 htc-exec02'

Workstation-style worker that only runs jobs when idle:
sudo python3 htc_node_bootstrap.py \
  --role execute \
  --worker-policy desktop-idle \
  --set-hostname htc-exec02 \
  --head-host htc-head \
  --uid-domain cluster.local \
  --cluster-subnet 192.168.50.0/24 \
  --admin-allow 192.168.50.0/24 \
  --add-host '192.168.50.10 htc-head' \
  --add-host '192.168.50.20 htc-storage' \
  --add-host '192.168.50.31 htc-exec01' \
  --add-host '192.168.50.32 htc-exec02'

If you did not pre-stage the password file, then pass either --pool-password or --prompt-pool-password on first configure.

4) Only after the pool works, rerun with hardening flags.
Example for the head node:
sudo python3 htc_node_bootstrap.py \
  --role head \
  --head-host htc-head \
  --storage-host htc-storage \
  --cluster-subnet 192.168.50.0/24 \
  --admin-allow 192.168.50.0/24 \
  --add-host '192.168.50.10 htc-head' \
  --add-host '192.168.50.20 htc-storage' \
  --add-host '192.168.50.31 htc-exec01' \
  --add-host '192.168.50.32 htc-exec02' \
  --enable-fail2ban \
  --enable-firewall

Optional: add --ssh-keys-only only after verifying your SSH public keys work.

5) Verify on the head node
condor_status
mount | grep /cluster-output
condor_submit /root/hello.sub
