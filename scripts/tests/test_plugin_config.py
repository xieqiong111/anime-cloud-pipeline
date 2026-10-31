"""Task 11 RED tests for plugin credential and snapshot safety."""
from __future__ import annotations

import contextlib
import io
import json
from pathlib import Path
import os
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import plugin_config

ORIGINAL_PLATFORM = sys.platform


class FakeClient:
    def __init__(self, secret="cookie-plain-value"):
        self.secret = secret
        self.conf = {"enabled": False, "cookies": secret, "ordinary": "x"}
        self.puts = []
        self.snapshot_modes = []

    def plugin_conf(self, _plugin):
        return dict(self.conf)

    def update_plugin_conf(self, _plugin, conf):
        self.puts.append(dict(conf))
        self.conf = dict(conf)

    def plugin_state(self, _plugin):
        return {"installed": True, "state": True, "plugin_version": "1"}


class PluginConfigTests(unittest.TestCase):
    def run_cli(self, *argv):
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = plugin_config.main(list(argv))
        return code, stdout.getvalue(), stderr.getvalue()

    def test_sensitive_set_is_rejected_before_fetch_without_echoing_value(self):
        secret = "cookie-plain-value"
        fetched = []

        def fail_build(_args):
            fetched.append(True)
            raise AssertionError("client must not be built")

        with mock.patch.object(plugin_config, "build_client", side_effect=fail_build):
            code, out, err = self.run_cli(
                "set", "--plugin", "CloudSubscribe", "--set", f"cookies={secret}"
            )
        self.assertEqual(code, 2)
        self.assertFalse(fetched)
        self.assertNotIn("cookies", err)
        self.assertNotIn(secret, out + err)

    def test_invalid_argv_is_static_and_does_not_echo_secret(self):
        secret = "query-secret-value"
        code, out, err = self.run_cli(
            "set", "--plugin", "CloudSubscribe", "--auth-mode", secret
        )
        self.assertEqual(code, 2)
        self.assertEqual(out, "")
        self.assertNotIn(secret, err)
        self.assertEqual(err.strip(), "error: invalid command-line arguments")

    def test_linux_plaintext_snapshot_requires_opt_in_and_is_cleaned(self):
        client = FakeClient()
        with tempfile.TemporaryDirectory(prefix="task11-plugin-") as tmp:
            snapshot = Path(tmp) / "plugin.SECRET-PLAINTEXT.json"
            with mock.patch.object(plugin_config.os, "name", "posix"), \
                 mock.patch.object(plugin_config.sys, "platform", "linux"), \
                 mock.patch.object(plugin_config, "build_client", return_value=client):
                code, _out, err = self.run_cli(
                    "set", "--plugin", "CloudSubscribe", "--set", "enabled=true",
                    "--snapshot", str(snapshot)
                )
            self.assertEqual(code, 2)
            self.assertFalse(snapshot.exists())
            self.assertIn("opt-in", err)

            def assert_snapshot_mode(_plugin, conf):
                self.assertTrue(snapshot.exists())
                # The bundled test runtime is Windows; chmod mode bits are
                # advisory there. Linux CI asserts the exact 0600 contract.
                if ORIGINAL_PLATFORM != "win32":
                    self.assertEqual(snapshot.stat().st_mode & 0o777, 0o600)
                client.conf = dict(conf)

            client.update_plugin_conf = assert_snapshot_mode
            with mock.patch.object(plugin_config.os, "name", "posix"), \
                 mock.patch.object(plugin_config.sys, "platform", "linux"), \
                 mock.patch.object(plugin_config, "build_client", return_value=client):
                code, out, err = self.run_cli(
                    "set", "--plugin", "CloudSubscribe", "--set", "enabled=true",
                    "--snapshot", str(snapshot), "--allow-plaintext-snapshot"
                )
            self.assertEqual(code, 0, err)
            self.assertFalse(snapshot.exists())
            self.assertNotIn(client.secret, out + err)

    def test_linux_plaintext_snapshot_is_removed_after_rollback(self):
        client = FakeClient()

        def failed_update(_plugin, conf):
            client.puts.append(dict(conf))
            # Simulate a remote server that fails to persist the changed field.
            client.conf = dict(conf)
            client.conf["enabled"] = False

        client.update_plugin_conf = failed_update
        with tempfile.TemporaryDirectory(prefix="task11-plugin-rollback-") as tmp:
            snapshot = Path(tmp) / "plugin.SECRET-PLAINTEXT.json"
            with mock.patch.object(plugin_config.os, "name", "posix"), \
                 mock.patch.object(plugin_config.sys, "platform", "linux"), \
                 mock.patch.object(plugin_config, "build_client", return_value=client):
                code, out, err = self.run_cli(
                    "set", "--plugin", "CloudSubscribe", "--set", "enabled=true",
                    "--snapshot", str(snapshot), "--allow-plaintext-snapshot"
                )
            self.assertEqual(code, 1)
            self.assertFalse(snapshot.exists())
            self.assertNotIn(client.secret, out + err)

    def test_linux_plaintext_fchmod_failure_is_cleaned_and_retry_succeeds(self):
        client = FakeClient()
        with tempfile.TemporaryDirectory(prefix="task11-plugin-fchmod-") as tmp:
            snapshot = Path(tmp) / "plugin.SECRET-PLAINTEXT.json"
            with mock.patch.object(plugin_config.os, "name", "posix"), \
                 mock.patch.object(plugin_config.sys, "platform", "linux"), \
                 mock.patch.object(plugin_config, "build_client", return_value=client), \
                 mock.patch.object(plugin_config.os, "fchmod", create=True, side_effect=OSError("fchmod")):
                code, out, err = self.run_cli(
                    "set", "--plugin", "CloudSubscribe", "--set", "enabled=true",
                    "--snapshot", str(snapshot), "--allow-plaintext-snapshot"
                )
            self.assertEqual(code, 1)
            self.assertFalse(snapshot.exists())
            self.assertNotIn(client.secret, out + err)

            with mock.patch.object(plugin_config.os, "name", "posix"), \
                 mock.patch.object(plugin_config.sys, "platform", "linux"), \
                 mock.patch.object(plugin_config, "build_client", return_value=client):
                retry, out, err = self.run_cli(
                    "set", "--plugin", "CloudSubscribe", "--set", "enabled=true",
                    "--snapshot", str(snapshot), "--allow-plaintext-snapshot"
                )
            self.assertEqual(retry, 0, err)
            self.assertFalse(snapshot.exists())

    def test_linux_plaintext_transient_prevalidation_failure_recovers(self):
        client = FakeClient()
        real_fstat = plugin_config.os.fstat
        first = True

        def fail_once(fd):
            nonlocal first
            if first:
                first = False
                raise OSError("prevalidation")
            return real_fstat(fd)

        with tempfile.TemporaryDirectory(prefix="task11-plugin-prevalidation-") as tmp:
            snapshot = Path(tmp) / "plugin.SECRET-PLAINTEXT.json"
            with mock.patch.object(plugin_config.os, "name", "posix"), \
                 mock.patch.object(plugin_config.sys, "platform", "linux"), \
                 mock.patch.object(plugin_config, "build_client", return_value=client), \
                 mock.patch.object(plugin_config.os, "fstat", side_effect=fail_once):
                code, out, err = self.run_cli(
                    "set", "--plugin", "CloudSubscribe", "--set", "enabled=true",
                    "--snapshot", str(snapshot), "--allow-plaintext-snapshot"
                )
            self.assertEqual(code, 0, err)
            self.assertFalse(snapshot.exists())
            self.assertNotIn(client.secret, out + err)

            with mock.patch.object(plugin_config.os, "name", "posix"), \
                 mock.patch.object(plugin_config.sys, "platform", "linux"), \
                 mock.patch.object(plugin_config, "build_client", return_value=client):
                retry, out, err = self.run_cli(
                    "set", "--plugin", "CloudSubscribe", "--set", "enabled=true",
                    "--snapshot", str(snapshot), "--allow-plaintext-snapshot"
                )
            self.assertEqual(retry, 0, err)
            self.assertFalse(snapshot.exists())

    def test_persistent_fstat_failure_preserves_unknown_path_and_cleans_known_path(self):
        real_fstat = os.fstat
        for fail_from in (1, 2, 3, 4):
            with self.subTest(fail_from=fail_from), tempfile.TemporaryDirectory() as tmp:
                client = FakeClient()
                snapshot = Path(tmp) / "plugin.SECRET-PLAINTEXT.json"
                calls = 0

                def fail_persistently(fd):
                    nonlocal calls
                    calls += 1
                    if calls >= fail_from:
                        raise OSError(client.secret)
                    return real_fstat(fd)

                argv = ("set", "--plugin", "CloudSubscribe", "--set", "enabled=true",
                        "--snapshot", str(snapshot), "--allow-plaintext-snapshot")
                with mock.patch.object(plugin_config.sys, "platform", "linux"), \
                     mock.patch.object(plugin_config.os, "name", "posix"), \
                     mock.patch.object(plugin_config, "build_client", return_value=client):
                    with mock.patch.object(plugin_config.os, "fstat", side_effect=fail_persistently):
                        code, out, err = self.run_cli(*argv)
                    self.assertEqual(code, 1)
                    self.assertNotIn(client.secret, out + err)
                    if snapshot.exists():
                        self.assertNotIn(client.secret.encode(), snapshot.read_bytes())
                    if fail_from == 1:
                        self.assertTrue(snapshot.exists())
                        self.assertEqual(snapshot.read_bytes(), b"")
                        self.assertEqual(err.strip(), "error: operation failed (OSError)")
                        retry, out, err = self.run_cli(*argv)
                        self.assertEqual(retry, 2)
                        self.assertEqual(snapshot.read_bytes(), b"")
                        # Simulate operator inspection and removal of stale file.
                        snapshot.unlink()
                    self.assertFalse(snapshot.exists())
                    retry, out, err = self.run_cli(*argv)
                    self.assertEqual(retry, 0, err)
                    self.assertFalse(snapshot.exists())

    def test_unknown_identity_preserves_replacement_after_close(self):
        client = FakeClient()
        real_close = plugin_config._close_lease
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = Path(tmp) / "plugin.SECRET-PLAINTEXT.json"
            moved = Path(tmp) / "original.json"

            def replace_after_close(lease):
                was_open = lease.fd >= 0
                real_close(lease)
                if was_open:
                    snapshot.rename(moved)
                    snapshot.write_bytes(b"unrelated replacement")

            with mock.patch.object(plugin_config.sys, "platform", "linux"), \
                 mock.patch.object(plugin_config.os, "name", "posix"), \
                 mock.patch.object(plugin_config, "_HOST_OS_NAME", "nt"), \
                 mock.patch.object(plugin_config, "build_client", return_value=client), \
                 mock.patch.object(plugin_config.os, "fstat", side_effect=OSError(client.secret)), \
                 mock.patch.object(plugin_config.os, "remove", side_effect=OSError("blocked")), \
                 mock.patch.object(plugin_config, "_close_lease", side_effect=replace_after_close):
                code, out, err = self.run_cli(
                    "set", "--plugin", "CloudSubscribe", "--set", "enabled=true",
                    "--snapshot", str(snapshot), "--allow-plaintext-snapshot")
            self.assertEqual(code, 1)
            self.assertEqual(moved.read_bytes(), b"")
            self.assertTrue(snapshot.exists())
            self.assertEqual(snapshot.read_bytes(), b"unrelated replacement")
            self.assertEqual(err.strip(), "error: operation failed (OSError)")
            self.assertNotIn(client.secret, out + err)

    def test_persistent_cleanup_fstat_failure_scrubs_existing_hardlink(self):
        client = FakeClient()
        real_fstat = os.fstat
        updating = False
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = Path(tmp) / "plugin.SECRET-PLAINTEXT.json"
            alias = Path(tmp) / "racing-link.json"

            def update(_plugin, conf):
                nonlocal updating
                os.link(snapshot, alias)
                client.conf = dict(conf)
                updating = True

            def fail_during_cleanup(fd):
                if updating:
                    raise OSError(client.secret)
                return real_fstat(fd)

            client.update_plugin_conf = update
            with mock.patch.object(plugin_config.sys, "platform", "linux"), \
                 mock.patch.object(plugin_config.os, "name", "posix"), \
                 mock.patch.object(plugin_config, "build_client", return_value=client), \
                 mock.patch.object(plugin_config.os, "fstat", side_effect=fail_during_cleanup):
                code, out, err = self.run_cli(
                    "set", "--plugin", "CloudSubscribe", "--set", "enabled=true",
                    "--snapshot", str(snapshot), "--allow-plaintext-snapshot")
            self.assertEqual(code, 1)
            self.assertEqual(alias.read_bytes(), b"")
            self.assertFalse(snapshot.exists())
            self.assertNotIn(client.secret, out + err)

    def test_linux_plaintext_snapshot_rejects_symlink_victim(self):
        client = FakeClient()
        with tempfile.TemporaryDirectory(prefix="task11-plugin-symlink-") as tmp:
            victim = Path(tmp) / "victim.json"
            snapshot = Path(tmp) / "plugin.SECRET-PLAINTEXT.json"
            victim.write_text("victim-secret", encoding="utf-8")
            try:
                os.symlink(victim, snapshot)
            except (AttributeError, OSError) as exc:
                self.skipTest(f"symlinks unavailable: {exc}")
            with mock.patch.object(plugin_config.os, "name", "posix"), \
                 mock.patch.object(plugin_config.sys, "platform", "linux"), \
                 mock.patch.object(plugin_config, "build_client", return_value=client):
                code, out, err = self.run_cli(
                    "set", "--plugin", "CloudSubscribe", "--set", "enabled=true",
                    "--snapshot", str(snapshot), "--allow-plaintext-snapshot"
                )
            self.assertNotEqual(code, 0)
            self.assertEqual(victim.read_text(encoding="utf-8"), "victim-secret")
            self.assertTrue(snapshot.is_symlink())
            self.assertNotIn("victim-secret", out + err)

    def test_cleanup_path_replacement_is_preserved_after_original_is_scrubbed(self):
        client = FakeClient()
        real_close = plugin_config._close_lease
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = Path(tmp) / "plugin.SECRET-PLAINTEXT.json"
            moved = Path(tmp) / "moved-original.json"

            def replace_after_close(lease):
                was_open = lease.fd >= 0
                real_close(lease)
                if was_open:
                    snapshot.rename(moved)
                    snapshot.write_bytes(b"unrelated replacement")

            # Exercise the real Windows close-before-unlink fallback; swap
            # the pathname in that window, after the original fd was scrubbed.
            with mock.patch.object(plugin_config.sys, "platform", "linux"), \
                 mock.patch.object(plugin_config.os, "name", "posix"), \
                 mock.patch.object(plugin_config, "_HOST_OS_NAME", "nt"), \
                 mock.patch.object(plugin_config, "build_client", return_value=client), \
                 mock.patch.object(plugin_config.os, "remove", side_effect=OSError("blocked")), \
                 mock.patch.object(plugin_config, "_close_lease", side_effect=replace_after_close):
                code, out, err = self.run_cli(
                    "set", "--plugin", "CloudSubscribe", "--set", "enabled=true",
                    "--snapshot", str(snapshot), "--allow-plaintext-snapshot")
            self.assertEqual(code, 1)
            self.assertEqual(moved.read_bytes(), b"")
            self.assertEqual(snapshot.read_bytes(), b"unrelated replacement")
            self.assertNotIn(client.secret, out + err)

    def test_cleanup_lstat_failure_still_scrubs_and_retries_path_verification(self):
        real_lstat = os.lstat
        for persistent in (False, True):
            with self.subTest(persistent=persistent), tempfile.TemporaryDirectory() as tmp:
                client = FakeClient()
                snapshot = Path(tmp) / "plugin.SECRET-PLAINTEXT.json"
                updating = False
                failed = False

                def update(_plugin, conf):
                    nonlocal updating
                    client.conf = dict(conf)
                    updating = True

                def fail_cleanup_lstat(path, *args, **kwargs):
                    nonlocal failed
                    if updating and str(path) == str(snapshot) and (persistent or not failed):
                        failed = True
                        raise OSError(client.secret)
                    return real_lstat(path, *args, **kwargs)

                client.update_plugin_conf = update
                with mock.patch.object(plugin_config.sys, "platform", "linux"), \
                     mock.patch.object(plugin_config.os, "name", "posix"), \
                     mock.patch.object(plugin_config, "build_client", return_value=client), \
                     mock.patch.object(plugin_config.os, "lstat", side_effect=fail_cleanup_lstat):
                    code, out, err = self.run_cli(
                        "set", "--plugin", "CloudSubscribe", "--set", "enabled=true",
                        "--snapshot", str(snapshot), "--allow-plaintext-snapshot")
                self.assertEqual(code, 1)
                self.assertNotIn(client.secret, out + err)
                if persistent:
                    # An unverifiable pathname may be a replacement: leave
                    # it untouched, but the inode we owned must be empty.
                    self.assertEqual(snapshot.read_bytes(), b"")
                else:
                    self.assertFalse(snapshot.exists())

    def test_linux_plaintext_snapshot_rejects_hardlink_victim(self):
        client = FakeClient()
        with tempfile.TemporaryDirectory(prefix="task11-plugin-hardlink-") as tmp:
            victim = Path(tmp) / "victim.json"
            snapshot = Path(tmp) / "plugin.SECRET-PLAINTEXT.json"
            victim.write_text("victim-secret", encoding="utf-8")
            try:
                os.link(victim, snapshot)
            except (AttributeError, OSError) as exc:
                self.skipTest(f"hardlinks unavailable: {exc}")
            with mock.patch.object(plugin_config.os, "name", "posix"), \
                 mock.patch.object(plugin_config.sys, "platform", "linux"), \
                 mock.patch.object(plugin_config, "build_client", return_value=client):
                code, out, err = self.run_cli(
                    "set", "--plugin", "CloudSubscribe", "--set", "enabled=true",
                    "--snapshot", str(snapshot), "--allow-plaintext-snapshot"
                )
            self.assertNotEqual(code, 0)
            self.assertEqual(victim.read_text(encoding="utf-8"), "victim-secret")
            self.assertEqual(snapshot.read_text(encoding="utf-8"), "victim-secret")
            self.assertNotIn("victim-secret", out + err)

    def test_linux_plaintext_cleanup_failure_fails_and_removes_plaintext(self):
        client = FakeClient()
        with tempfile.TemporaryDirectory(prefix="task11-plugin-cleanup-") as tmp:
            snapshot = Path(tmp) / "plugin.SECRET-PLAINTEXT.json"
            with mock.patch.object(plugin_config.os, "name", "posix"), \
                 mock.patch.object(plugin_config.sys, "platform", "linux"), \
                 mock.patch.object(plugin_config, "build_client", return_value=client), \
                 mock.patch.object(plugin_config.os, "remove", side_effect=OSError("blocked")):
                code, out, err = self.run_cli(
                    "set", "--plugin", "CloudSubscribe", "--set", "enabled=true",
                    "--snapshot", str(snapshot), "--allow-plaintext-snapshot"
                )
            self.assertEqual(code, 1)
            self.assertFalse(snapshot.exists())
            self.assertNotIn(client.secret, out + err)

    def test_linux_plaintext_racing_hardlink_is_scrubbed_and_fails(self):
        client = FakeClient()
        with tempfile.TemporaryDirectory(prefix="task11-plugin-race-") as tmp:
            snapshot = Path(tmp) / "plugin.SECRET-PLAINTEXT.json"
            victim = Path(tmp) / "race-victim.json"
            original_remove = plugin_config.os.remove

            def race_before_unlink(path):
                os.link(path, victim)
                return original_remove(path)

            with mock.patch.object(plugin_config.os, "name", "posix"), \
                 mock.patch.object(plugin_config.sys, "platform", "linux"), \
                 mock.patch.object(plugin_config, "build_client", return_value=client), \
                 mock.patch.object(plugin_config.os, "remove", side_effect=race_before_unlink):
                code, out, err = self.run_cli(
                    "set", "--plugin", "CloudSubscribe", "--set", "enabled=true",
                    "--snapshot", str(snapshot), "--allow-plaintext-snapshot"
                )
            self.assertEqual(code, 1)
            self.assertTrue(victim.exists())
            self.assertEqual(victim.read_bytes(), b"")
            self.assertNotIn(client.secret, out + err)

    def test_windows_dpapi_snapshot_path_still_works(self):
        client = FakeClient()
        with tempfile.TemporaryDirectory(prefix="task11-plugin-dpapi-") as tmp:
            snapshot = Path(tmp) / "plugin-config-snapshot.dpapi"
            with mock.patch.object(plugin_config.os, "name", "nt"), \
                 mock.patch.object(plugin_config.sys, "platform", "win32"), \
                 mock.patch.object(plugin_config, "dpapi_protect", return_value=b"encrypted"), \
                 mock.patch.object(plugin_config, "build_client", return_value=client):
                code, out, err = self.run_cli(
                    "set", "--plugin", "CloudSubscribe", "--set", "enabled=true",
                    "--snapshot", str(snapshot)
                )
            self.assertEqual(code, 0, err)
            self.assertEqual(snapshot.read_text(encoding="ascii"), "656e63727970746564")
            snapshot.unlink()

    def test_successful_set_reports_fields_without_original_values(self):
        values = {
            "infohash": "abcdef0123456789abcdef0123456789abcdef0123",
            "ordinary": "private-short-note",
            "source": "https://example.invalid/private-resource?credential=private-value",
            "remote_text": "eyJhbGciOiJIUzI1NiJ9.privatepayload.privatesignature",
        }
        for field, value in values.items():
            with self.subTest(field=field), tempfile.TemporaryDirectory() as tmp:
                client = FakeClient()
                snapshot = Path(tmp) / "plugin.SECRET-PLAINTEXT.json"
                with mock.patch.object(plugin_config.os, "name", "posix"), \
                     mock.patch.object(plugin_config.sys, "platform", "linux"), \
                     mock.patch.object(plugin_config, "build_client", return_value=client):
                    code, out, err = self.run_cli(
                        "set", "--plugin", "CloudSubscribe", "--set", f"{field}={value}",
                        "--snapshot", str(snapshot), "--allow-plaintext-snapshot")
                self.assertEqual(code, 0, err)
                self.assertEqual(client.conf[field], value)
                self.assertNotIn(value, out + err)
                self.assertEqual(json.loads(out)["changed"], {field: "[CHANGED]"})
                self.assertFalse(snapshot.exists())

    def test_show_is_a_structure_summary_and_shortens_hashes(self):
        client = FakeClient()
        client.conf.update({
            "remote_text": "arbitrary remote response that must not be printed",
            "config_hash": "abcdef0123456789abcdef0123456789",
            "enabled": True,
        })
        with mock.patch.object(plugin_config, "build_client", return_value=client):
            code, out, err = self.run_cli(
                "show", "--plugin", "CloudSubscribe"
            )
        self.assertEqual(code, 0, err)
        self.assertNotIn("arbitrary remote response", out)
        self.assertNotIn("abcdef0123456789abcdef0123456789", out)
        self.assertIn("abcdef01", out)


if __name__ == "__main__":
    unittest.main()
