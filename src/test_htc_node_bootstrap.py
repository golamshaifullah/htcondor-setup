import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

SCRIPT_PATH = "/mnt/data/htc_node_bootstrap.py"

spec = importlib.util.spec_from_file_location("htc_node_bootstrap", SCRIPT_PATH)
mod = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(mod)


class BootstrapScriptTests(unittest.TestCase):
    def test_parse_args_head_no_storage(self):
        args = mod.parse_args([
            "--role", "head",
            "--head-host", "htc-head",
            "--no-storage",
            "--prompt-pool-password",
        ])
        self.assertEqual(args.role, "head")
        self.assertTrue(args.no_storage)
        self.assertTrue(args.prompt_pool_password)
        self.assertEqual(args.head_host, "htc-head")

    def test_configure_execute_role_desktop_idle(self):
        writes = []
        with patch.object(mod, "atomic_write", side_effect=lambda path, content, mode=0o644: writes.append((path, content, mode)) or True):
            mod.configure_htcondor_role("execute", execute_policy="desktop-idle")
        self.assertEqual(len(writes), 1)
        path, content, mode = writes[0]
        self.assertEqual(path, "/etc/condor/config.d/20-role.conf")
        self.assertIn("use ROLE : Execute", content)
        self.assertIn("use POLICY : DESKTOP_IDLE()", content)
        self.assertEqual(mode, 0o644)

    def test_set_pool_password_prompt_avoids_dash_p(self):
        calls = []
        with tempfile.TemporaryDirectory() as td:
            password_file = str(Path(td) / "pool_password")
            with patch.object(mod, "run", side_effect=lambda cmd, **kwargs: calls.append(list(cmd))):
                mod.set_pool_password(password_file, None, overwrite=False, prompt=True)
        self.assertEqual(calls, [["condor_store_cred", "add", "-f", password_file]])

    def test_push_password_file_uses_scp_and_ssh(self):
        with tempfile.TemporaryDirectory() as td:
            password_file = Path(td) / "pool_password"
            password_file.write_text("secret")
            calls = []
            with patch.object(mod.shutil, "which", return_value="/usr/bin/fake"), \
                 patch.object(mod, "run_as_local_user", side_effect=lambda cmd, user: calls.append((list(cmd), user))):
                mod.push_password_file(str(password_file), ["node1"], "alice", overwrite=False)

        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0][1], "alice")
        self.assertEqual(calls[0][0][:2], ["scp", "-q"])
        self.assertIn("alice@node1:/tmp/pool_password.tmp", calls[0][0])
        self.assertEqual(calls[1][1], "alice")
        self.assertEqual(calls[1][0][0], "ssh")
        self.assertEqual(calls[1][0][1], "alice@node1")
        self.assertIn("sudo install -o root -g root -m 600", calls[1][0][2])

    def test_main_requires_storage_host_without_no_storage(self):
        argv = [
            "--role", "head",
            "--head-host", "htc-head",
            "--prompt-pool-password",
        ]
        with patch.object(mod, "require_root"), \
             patch.object(mod, "assert_supported_ubuntu", return_value="noble"):
            with self.assertRaises(mod.BootstrapError) as ctx:
                mod.main(argv)
        self.assertIn("--storage-host is required for the head role unless you use --no-storage", str(ctx.exception))

    def test_main_head_no_storage_runs_expected_flow(self):
        calls = []

        def record(name):
            def inner(*args, **kwargs):
                calls.append((name, args, kwargs))
            return inner

        argv = [
            "--role", "head",
            "--head-host", "htc-head",
            "--no-storage",
            "--prompt-pool-password",
            "--mount-point", "/tmp/cluster-output",
        ]

        with patch.object(mod, "require_root"), \
             patch.object(mod, "assert_supported_ubuntu", return_value="noble"), \
             patch.object(mod, "apt_install", side_effect=record("apt_install")), \
             patch.object(mod, "set_hostname", side_effect=record("set_hostname")), \
             patch.object(mod, "update_hosts", side_effect=record("update_hosts")), \
             patch.object(mod, "install_htcondor_packages", side_effect=record("install_htcondor_packages")), \
             patch.object(mod, "configure_htcondor_base", side_effect=record("configure_htcondor_base")), \
             patch.object(mod, "configure_htcondor_role", side_effect=record("configure_htcondor_role")), \
             patch.object(mod, "set_pool_password", side_effect=record("set_pool_password")), \
             patch.object(mod, "ensure_dir", side_effect=record("ensure_dir")), \
             patch.object(mod, "configure_head_mount", side_effect=record("configure_head_mount")), \
             patch.object(mod, "write_example_submit", side_effect=record("write_example_submit")), \
             patch.object(mod, "push_password_file", side_effect=record("push_password_file")), \
             patch.object(mod, "enable_service", side_effect=record("enable_service")):
            rc = mod.main(argv)

        self.assertEqual(rc, 0)
        names = [name for name, _, _ in calls]
        self.assertIn("install_htcondor_packages", names)
        self.assertIn("configure_htcondor_base", names)
        self.assertIn("configure_htcondor_role", names)
        self.assertIn("set_pool_password", names)
        self.assertIn("ensure_dir", names)
        self.assertIn("write_example_submit", names)
        self.assertIn("enable_service", names)
        self.assertNotIn("configure_head_mount", names)


if __name__ == "__main__":
    unittest.main(verbosity=2)
