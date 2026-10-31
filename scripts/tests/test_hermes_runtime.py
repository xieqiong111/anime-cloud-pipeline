from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
from contextlib import contextmanager
from dataclasses import FrozenInstanceError, replace
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

import scripts.hermes_runtime as hermes_runtime
from scripts.pipeline.config import ConfigSchema
from scripts.hermes_runtime import (
    build_pipeline_argv,
    CommandError,
    DoctorReport,
    EnvFileError,
    main,
    RuntimeLayout,
    parse_env_file,
    resolve_layout,
    run_doctor,
)


def fixture_install_root() -> Path:
    """Create a native absolute root name for semantic layout fixtures."""

    return (Path(tempfile.gettempdir()) / "hermes-runtime-fixture").resolve()


def make_layout(*, env_path: Path | None = None) -> RuntimeLayout:
    install_root = fixture_install_root()
    state_dir = install_root / "state"
    return RuntimeLayout(
        release_root=install_root / "releases" / "v5",
        install_root=install_root,
        env_path=env_path or install_root / "config" / "anime-cloud-pipeline.env",
        config_path=install_root / "config" / "config.v5.json",
        state_dir=state_dir,
        log_dir=install_root / "logs",
        lock_path=state_dir / "pipeline.lock",
        python=Path(sys.executable),
    )


def _write_manifest(root: Path) -> None:
    entries = []
    for path in sorted(root.rglob("*")):
        if path.is_file() and path.name != "MANIFEST.sha256":
            relative = path.relative_to(root).as_posix()
            entries.append(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {relative}\n")
    (root / "MANIFEST.sha256").write_text("".join(entries), encoding="utf-8")


@contextmanager
def prepared_doctor_layout(*, env: dict[str, str] | None = None):
    with tempfile.TemporaryDirectory() as tmp:
        install_root = Path(tmp).resolve() / "install"
        release_root = install_root / "releases" / "v5"
        (release_root / "scripts").mkdir(parents=True)
        (release_root / "scripts" / "hermes_runtime.py").write_text(
            "# runtime fixture\n", encoding="utf-8"
        )
        (release_root / "scripts" / "run_pipeline.py").write_text(
            "# pipeline fixture\n", encoding="utf-8"
        )
        (release_root / "RELEASE.json").write_text(
            '{"release_id":"v5","schema_version":1}\n', encoding="utf-8"
        )
        _write_manifest(release_root)

        config_dir = install_root / "config"
        config_dir.mkdir(parents=True)
        config_path = config_dir / "config.v5.json"
        example_config = Path(__file__).resolve().parents[2] / "deploy" / "config.v5.example.json"
        config_path.write_text(example_config.read_text(encoding="utf-8"), encoding="utf-8")
        env_path = config_dir / "anime-cloud-pipeline.env"
        values = {
            "ANIME_PIPELINE_CONFIG": str(config_path),
            "ANIME_PIPELINE_STATE_DIR": str(install_root / "state"),
            "ANIME_PIPELINE_LOG_DIR": str(install_root / "logs"),
            "ANIME_PIPELINE_LOCK_PATH": str(
                install_root / "state" / "pipeline.lock"
            ),
            "MOVIEPILOT_TOKEN": "moviepilot-secret",
            "P115_COOKIE": "p115-secret",
            "P115_ROOT_DIRECTORY_ID": "root-id",
            "LITEPAN_API_KEY": "litepan-secret",
            "EMBY_API_KEY": "emby-secret",
        }
        values.update(env or {})
        env_path.write_text(
            "".join(f"{name}={value}\n" for name, value in values.items()),
            encoding="utf-8",
        )
        if os.name == "posix":
            env_path.chmod(0o600)

        state_dir = install_root / "state"
        log_dir = install_root / "logs"
        state_dir.mkdir()
        log_dir.mkdir()
        yield RuntimeLayout(
            release_root=release_root,
            install_root=install_root,
            env_path=env_path,
            config_path=config_path,
            state_dir=state_dir,
            log_dir=log_dir,
            lock_path=state_dir / "pipeline.lock",
            python=Path(sys.executable).resolve(),
        )


def snapshot_tree(root: Path) -> dict[str, tuple[int, bytes | str | None]]:
    snapshot: dict[str, tuple[int, bytes | str | None]] = {}
    for path in sorted((root, *root.rglob("*"))):
        relative = "." if path == root else path.relative_to(root).as_posix()
        metadata = path.lstat()
        if path.is_symlink():
            payload: bytes | str | None = os.readlink(path)
        elif path.is_file():
            payload = path.read_bytes()
        else:
            payload = None
        snapshot[relative] = (metadata.st_mode, payload)
    return snapshot


def fake_success_runner(argv, **kwargs):
    del kwargs
    return subprocess.CompletedProcess(
        argv, 0, stdout='{"status":"ok"}\n', stderr=""
    )


class HermesEnvironmentTests(unittest.TestCase):
    def _create_directory_link(self, link: Path, target: Path) -> None:
        try:
            link.symlink_to(target, target_is_directory=True)
            return
        except OSError:
            if os.name != "nt":
                raise

        completed = subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(link), str(target)],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_env_parser_does_not_expand_or_execute_shell_text(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, "pipeline.env")
            path.write_text(
                "MOVIEPILOT_TOKEN=$(touch should-not-exist)\n"
                "P115_COOKIE=literal value # retained\n",
                encoding="utf-8",
            )

            values = parse_env_file(path, require_private_mode=False)

            self.assertEqual(values["MOVIEPILOT_TOKEN"], "$(touch should-not-exist)")
            self.assertEqual(values["P115_COOKIE"], "literal value # retained")
            self.assertFalse(Path(tmp, "should-not-exist").exists())

    def test_env_parser_ignores_blank_and_comment_lines_but_preserves_value_text(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, "pipeline.env")
            path.write_text(
                "\n   \n# comment\n  # indented comment\n"
                "VALUE=  leading and trailing  \n"
                "WITH_EQUALS=left=right\n",
                encoding="utf-8",
            )

            values = parse_env_file(
                path,
                require_private_mode=False,
                allowed_names={"VALUE", "WITH_EQUALS"},
            )

            self.assertEqual(
                values,
                {
                    "VALUE": "  leading and trailing  ",
                    "WITH_EQUALS": "left=right",
                },
            )

    def test_env_parser_rejects_duplicate_keys(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, "pipeline.env")
            path.write_text("MOVIEPILOT_TOKEN=first\nMOVIEPILOT_TOKEN=second\n", encoding="utf-8")

            with self.assertRaisesRegex(EnvFileError, "duplicate.*line 2"):
                parse_env_file(path, require_private_mode=False)

    def test_env_parser_rejects_malformed_keys_and_assignments(self):
        malformed = (
            "missing-separator",
            "lowercase=value",
            "1TOKEN=value",
            "TOKEN SPACE=value",
            "export TOKEN=value",
            "=value",
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, "pipeline.env")
            for raw in malformed:
                with self.subTest(raw=raw):
                    path.write_text(raw + "\n", encoding="utf-8")
                    with self.assertRaisesRegex(EnvFileError, "invalid.*line 1"):
                        parse_env_file(path, require_private_mode=False)

    def test_env_parser_rejects_nul_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, "pipeline.env")
            path.write_bytes(b"TOKEN=before\x00after\n")

            with self.assertRaisesRegex(EnvFileError, "NUL"):
                parse_env_file(path, require_private_mode=False)

    def test_env_parser_rejects_symlinks(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp, "real.env")
            target.write_text("TOKEN=value\n", encoding="utf-8")
            path = Path(tmp, "linked.env")
            try:
                path.symlink_to(target)
            except (NotImplementedError, OSError):
                linked_mode = SimpleNamespace(st_mode=stat.S_IFLNK | 0o777)
                with mock.patch.object(type(target), "lstat", return_value=linked_mode):
                    with self.assertRaisesRegex(EnvFileError, "non-symlink"):
                        parse_env_file(target, require_private_mode=False)
                return

            with self.assertRaisesRegex(EnvFileError, "non-symlink"):
                parse_env_file(path, require_private_mode=False)

    def test_env_parser_rejects_non_regular_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(EnvFileError, "regular"):
                parse_env_file(Path(tmp), require_private_mode=False)

    def test_env_parser_rejects_group_or_world_permission_bits(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, "pipeline.env")
            path.write_text("TOKEN=value\n", encoding="utf-8")
            if os.name == "posix":
                path.chmod(0o640)
                self.assertNotEqual(stat.S_IMODE(path.stat().st_mode) & 0o077, 0)
                with self.assertRaisesRegex(EnvFileError, "0600 or stricter"):
                    parse_env_file(path)
            else:
                permissive_mode = SimpleNamespace(st_mode=stat.S_IFREG | 0o640)
                posix_os = SimpleNamespace(name="posix")
                with mock.patch.object(hermes_runtime, "os", posix_os):
                    with mock.patch.object(
                        type(path), "lstat", return_value=permissive_mode
                    ):
                        with self.assertRaisesRegex(EnvFileError, "0600 or stricter"):
                            parse_env_file(path)

    def test_env_parser_accepts_an_empty_private_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, "pipeline.env")
            path.write_text("", encoding="utf-8")
            if os.name == "posix":
                path.chmod(0o600)

            self.assertEqual(parse_env_file(path), {})

    def test_env_parser_rejects_unknown_and_python_runtime_controls(self):
        for key in (
            "UNKNOWN_RUNTIME_CONTROL",
            "ANIME_PIPELINE_PYTHON",
            "PYTHONPATH",
            "PYTHONHOME",
            "LD_PRELOAD",
        ):
            with self.subTest(key=key), tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp, "pipeline.env")
                path.write_text(f"{key}=hostile\n", encoding="utf-8")

                with self.assertRaisesRegex(EnvFileError, "unsupported"):
                    parse_env_file(path, require_private_mode=False)

    def test_installed_layout_uses_external_config_state_and_logs(self):
        install_root = fixture_install_root()
        release = install_root / "releases" / "v5"

        layout = resolve_layout(release / "scripts" / "hermes_runtime.py", {})

        self.assertEqual(layout.release_root, release)
        self.assertEqual(layout.install_root, install_root)
        self.assertEqual(
            layout.env_path, install_root / "config" / "anime-cloud-pipeline.env"
        )
        self.assertEqual(layout.config_path, install_root / "config" / "config.v5.json")
        self.assertEqual(layout.state_dir, install_root / "state")
        self.assertEqual(layout.log_dir, install_root / "logs")
        self.assertEqual(layout.lock_path, install_root / "state" / "pipeline.lock")
        self.assertEqual(layout.python, Path(sys.executable))

    def test_source_layout_uses_repository_root(self):
        release = Path(__file__).resolve().parents[2]
        script = release / "scripts" / "hermes_runtime.py"

        layout = resolve_layout(script, {})

        self.assertEqual(layout.release_root, release)
        self.assertEqual(layout.install_root, release)
        self.assertEqual(
            layout.env_path, release / "config" / "anime-cloud-pipeline.env"
        )
        self.assertEqual(layout.config_path, release / "config" / "config.v5.json")
        self.assertEqual(layout.state_dir, release / "state")
        self.assertEqual(layout.log_dir, release / "logs")
        self.assertEqual(layout.lock_path, release / "state" / "pipeline.lock")

    def test_layout_accepts_documented_absolute_path_overrides(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            script = root / "checkout" / "scripts" / "hermes_runtime.py"
            overrides = {
                "ANIME_PIPELINE_CONFIG": str(root / "operator" / "config.json"),
                "ANIME_PIPELINE_STATE_DIR": str(root / "operator" / "state"),
                "ANIME_PIPELINE_LOG_DIR": str(root / "operator" / "logs"),
                "ANIME_PIPELINE_LOCK_PATH": str(root / "operator" / "run.lock"),
                "ANIME_PIPELINE_PYTHON": str(Path(sys.executable).resolve()),
            }

            layout = resolve_layout(script, overrides)

            self.assertEqual(layout.config_path, Path(overrides["ANIME_PIPELINE_CONFIG"]))
            self.assertEqual(layout.state_dir, Path(overrides["ANIME_PIPELINE_STATE_DIR"]))
            self.assertEqual(layout.log_dir, Path(overrides["ANIME_PIPELINE_LOG_DIR"]))
            self.assertEqual(layout.lock_path, Path(overrides["ANIME_PIPELINE_LOCK_PATH"]))
            self.assertEqual(layout.python, Path(overrides["ANIME_PIPELINE_PYTHON"]))

    def test_layout_rejects_relative_documented_path_overrides(self):
        script = Path(__file__).resolve().parents[2] / "scripts" / "hermes_runtime.py"
        for name in (
            "ANIME_PIPELINE_CONFIG",
            "ANIME_PIPELINE_STATE_DIR",
            "ANIME_PIPELINE_LOG_DIR",
            "ANIME_PIPELINE_LOCK_PATH",
            "ANIME_PIPELINE_PYTHON",
        ):
            with self.subTest(name=name):
                with self.assertRaisesRegex(CommandError, name):
                    resolve_layout(script, {name: "relative/path"})

    def test_installed_layout_rejects_external_paths_inside_release(self):
        release = fixture_install_root() / "releases" / "v5"
        script = release / "scripts" / "hermes_runtime.py"
        overrides = {
            "ANIME_PIPELINE_CONFIG": release / "config.json",
            "ANIME_PIPELINE_STATE_DIR": release / "state",
            "ANIME_PIPELINE_LOG_DIR": release / "logs",
            "ANIME_PIPELINE_LOCK_PATH": release / "run.lock",
        }

        for name, path in overrides.items():
            with self.subTest(name=name):
                with self.assertRaisesRegex(CommandError, "outside.*release"):
                    resolve_layout(script, {name: str(path)})

    def test_installed_layout_rejects_paths_inside_a_sibling_release(self):
        install_root = fixture_install_root()
        release = install_root / "releases" / "v5"
        sibling_state = install_root / "releases" / "v4" / "state"

        with self.assertRaisesRegex(CommandError, "outside.*release"):
            resolve_layout(
                release / "scripts" / "hermes_runtime.py",
                {"ANIME_PIPELINE_STATE_DIR": str(sibling_state)},
            )

    def test_installed_layout_resolves_a_symlink_parent_for_nonexistent_children(self):
        with tempfile.TemporaryDirectory() as tmp:
            install_root = Path(tmp).resolve() / "anime"
            release = install_root / "releases" / "v5"
            (release / "scripts").mkdir(parents=True)
            external_root = Path(tmp).resolve() / "external"
            external_root.mkdir()
            alias = external_root / "release-alias"
            self._create_directory_link(alias, release)
            hidden_state = alias / "not-created-state"
            self.assertFalse(hidden_state.exists())

            with self.assertRaisesRegex(CommandError, "outside.*release"):
                resolve_layout(
                    release / "scripts" / "hermes_runtime.py",
                    {"ANIME_PIPELINE_STATE_DIR": str(hidden_state)},
                )

    def test_runtime_layout_is_frozen(self):
        layout = resolve_layout(
            Path(__file__).resolve().parents[2] / "scripts" / "hermes_runtime.py",
            {},
        )

        with self.assertRaises(FrozenInstanceError):
            layout.config_path = Path("elsewhere")


class HermesDispatchTests(unittest.TestCase):
    def test_every_v5_command_family_maps_to_the_existing_pipeline_cli(self):
        layout = make_layout()
        pipeline = str(layout.release_root / "scripts" / "run_pipeline.py")
        prefix = [str(layout.python), "-B", pipeline]
        config = ["--config", str(layout.config_path)]
        cases = (
            (["preflight"], prefix + ["preflight"] + config),
            (
                [
                    "config-migrate",
                    "--legacy",
                    "/tmp/legacy.json",
                    "--output",
                    "/tmp/config.json",
                    "--report",
                    "/tmp/report.json",
                ],
                prefix
                + [
                    "config-migrate",
                    "--legacy",
                    "/tmp/legacy.json",
                    "--output",
                    "/tmp/config.json",
                    "--report",
                    "/tmp/report.json",
                ],
            ),
            (
                ["migrate", "--check-only"],
                prefix + ["migrate"] + config + ["--check-only"],
            ),
            (
                ["dry-run", "--previous-day"],
                prefix + ["dry-run"] + config + ["--previous-day"],
            ),
            (["qualify"], prefix + ["qualify"] + config),
            (["plan-resources"], prefix + ["plan-resources"] + config),
            (
                ["prepare-resources", "--apply"],
                prefix + ["prepare-resources"] + config + ["--apply"],
            ),
            (
                ["run", "--resume-only"],
                prefix + ["run"] + config + ["--resume-only"],
            ),
            (["status"], prefix + ["status"] + config),
            (
                ["reviews", "list"],
                prefix + ["reviews", "list"] + config,
            ),
            (
                [
                    "reviews",
                    "resolve",
                    "--id",
                    "17",
                    "--resolution",
                    "skip",
                    "--generation",
                    "3",
                ],
                prefix
                + ["reviews", "resolve"]
                + config
                + [
                    "--id",
                    "17",
                    "--resolution",
                    "skip",
                    "--generation",
                    "3",
                ],
            ),
            (["audit"], prefix + ["audit"] + config),
        )

        for source, expected in cases:
            with self.subTest(command=" ".join(source[:2])):
                self.assertEqual(build_pipeline_argv(layout, source), expected)

    def test_dispatch_preserves_each_caller_argument_as_one_element(self):
        layout = make_layout()
        shell_text = "2026-09-01 00:00; $(touch should-not-exist) *"

        result = build_pipeline_argv(layout, ["run", "--backfill", shell_text])

        self.assertEqual(result[-2:], ["--backfill", shell_text])

    def test_config_consuming_commands_reject_a_caller_supplied_second_config(self):
        cases = (
            ["preflight"],
            ["migrate"],
            ["dry-run"],
            ["qualify"],
            ["plan-resources"],
            ["prepare-resources", "--apply"],
            ["run"],
            ["status"],
            ["reviews", "list"],
            ["reviews", "resolve", "--id", "1", "--resolution", "skip"],
            ["audit"],
        )
        for command in cases:
            for supplied in (["--config", "/tmp/other.json"], ["--config=/tmp/other.json"]):
                with self.subTest(command=command, supplied=supplied):
                    with self.assertRaisesRegex(CommandError, "--config"):
                        build_pipeline_argv(make_layout(), command + supplied)

    def test_dispatch_rejects_argparse_config_abbreviations(self):
        for option in ("--c", "--co", "--con", "--conf", "--confi"):
            for supplied in ([option, "/tmp/other.json"], [f"{option}=/tmp/other.json"]):
                with self.subTest(supplied=supplied):
                    with self.assertRaisesRegex(CommandError, "--config"):
                        build_pipeline_argv(make_layout(), ["run", *supplied])

    def test_dispatch_rejects_empty_unknown_and_invalid_review_commands(self):
        for command in ([], ["unknown"], ["reviews"], ["reviews", "unknown"]):
            with self.subTest(command=command):
                with self.assertRaises(CommandError):
                    build_pipeline_argv(make_layout(), command)


class DoctorTests(unittest.TestCase):
    def test_doctor_reports_required_names_not_secret_values(self):
        secret = "never-print-this"
        with prepared_doctor_layout(env={"P115_COOKIE": secret}) as layout:
            report = run_doctor(layout, subprocess_runner=fake_success_runner)

        rendered = json.dumps(report.as_dict(), sort_keys=True)
        self.assertIsInstance(report, DoctorReport)
        self.assertIn("P115_COOKIE", rendered)
        self.assertNotIn(secret, rendered)

    def test_doctor_is_read_only(self):
        with prepared_doctor_layout() as layout:
            before = snapshot_tree(layout.install_root)

            report = run_doctor(layout, subprocess_runner=fake_success_runner)

            after = snapshot_tree(layout.install_root)
        self.assertTrue(report.ok, report.as_dict())
        self.assertEqual(after, before)

    def test_doctor_rejects_state_inside_release(self):
        with prepared_doctor_layout() as prepared:
            layout = replace(
                prepared, state_dir=prepared.release_root / "state"
            )

            report = run_doctor(layout, subprocess_runner=fake_success_runner)

        self.assertIn("state_dir_outside_release", report.failed_codes)

    def test_doctor_runs_only_read_only_pipeline_checks(self):
        calls: list[list[str]] = []

        def recording_runner(argv, **kwargs):
            self.assertTrue(kwargs["capture_output"])
            self.assertTrue(kwargs["text"])
            self.assertFalse(kwargs["check"])
            self.assertNotIn("PYTHONDONTWRITEBYTECODE", kwargs["env"])
            self.assertNotIn("PYTHONPATH", kwargs["env"])
            self.assertNotIn("ANIME_PIPELINE_PYTHON", kwargs["env"])
            calls.append(list(argv))
            return subprocess.CompletedProcess(
                argv, 0, stdout='{"status":"ok"}\n', stderr=""
            )

        with prepared_doctor_layout() as layout:
            environment = parse_env_file(layout.env_path)
            environment["ANIME_PIPELINE_PYTHON"] = "/operator/python"
            report = run_doctor(
                layout,
                environment,
                subprocess_runner=recording_runner,
            )

        self.assertTrue(report.ok, report.as_dict())
        self.assertEqual(
            calls,
            [
                [
                    str(layout.python),
                    "-B",
                    "-c",
                    mock.ANY,
                ],
                build_pipeline_argv(layout, ["preflight"]),
                build_pipeline_argv(layout, ["migrate", "--check-only"]),
            ],
        )
        rendered = " ".join(" ".join(call) for call in calls)
        self.assertNotIn(" dry-run", rendered)
        self.assertNotIn(" run ", f" {rendered} ")

    def test_doctor_short_circuits_after_python_failure(self):
        calls: list[list[str]] = []

        def recording_runner(argv, **kwargs):
            del kwargs
            calls.append(list(argv))
            return subprocess.CompletedProcess(
                argv, 1, stdout="", stderr="python failed"
            )

        with prepared_doctor_layout() as layout:
            report = run_doctor(layout, subprocess_runner=recording_runner)

        diagnostics = {item.code: item for item in report.diagnostics}
        self.assertEqual(len(calls), 1)
        self.assertFalse(diagnostics["python_sqlite"].ok)
        self.assertEqual(
            diagnostics["preflight"].detail,
            "skipped: python_sqlite failed",
        )
        self.assertEqual(
            diagnostics["migrate_check_only"].detail,
            "skipped: preflight skipped because python_sqlite failed",
        )

    def test_doctor_short_circuits_migrate_after_preflight_failure(self):
        calls: list[list[str]] = []

        def recording_runner(argv, **kwargs):
            del kwargs
            calls.append(list(argv))
            failed = len(calls) == 2
            return subprocess.CompletedProcess(
                argv,
                1 if failed else 0,
                stdout="",
                stderr="preflight failed" if failed else "",
            )

        with prepared_doctor_layout() as layout:
            report = run_doctor(layout, subprocess_runner=recording_runner)

        diagnostics = {item.code: item for item in report.diagnostics}
        self.assertEqual(len(calls), 2)
        self.assertTrue(diagnostics["python_sqlite"].ok)
        self.assertFalse(diagnostics["preflight"].ok)
        self.assertEqual(
            diagnostics["migrate_check_only"].detail,
            "skipped: preflight failed",
        )

    def test_doctor_skips_release_code_when_manifest_integrity_fails(self):
        calls: list[list[str]] = []

        def recording_runner(argv, **kwargs):
            del kwargs
            calls.append(list(argv))
            return subprocess.CompletedProcess(
                argv, 0, stdout='{"status":"ok"}\n', stderr=""
            )

        with prepared_doctor_layout() as layout:
            (layout.release_root / "scripts" / "run_pipeline.py").write_text(
                "# tampered pipeline fixture\n", encoding="utf-8"
            )

            report = run_doctor(layout, subprocess_runner=recording_runner)

        diagnostics = {item.code: item for item in report.diagnostics}
        self.assertFalse(diagnostics["release_manifest"].ok)
        for code in ("preflight", "migrate_check_only"):
            self.assertFalse(diagnostics[code].ok)
            self.assertEqual(
                diagnostics[code].detail,
                "skipped: local prerequisites failed",
            )
        pipeline = str(layout.release_root / "scripts" / "run_pipeline.py")
        self.assertFalse(
            any(pipeline in call for call in calls),
            calls,
        )
        self.assertEqual(calls, [])

    def test_doctor_skips_children_when_essential_paths_fail(self):
        for field in ("config_path", "env_path", "state_dir", "log_dir", "lock_path"):
            with self.subTest(field=field), prepared_doctor_layout() as prepared:
                layout = replace(prepared, **{field: prepared.release_root / "unsafe"})
                runner = mock.Mock(side_effect=AssertionError("child must not run"))
                report = run_doctor(layout, subprocess_runner=runner)

                self.assertFalse(report.ok)
                runner.assert_not_called()
                for code in ("python_sqlite", "preflight", "migrate_check_only"):
                    diagnostic = next(item for item in report.diagnostics if item.code == code)
                    self.assertFalse(diagnostic.ok)
                    self.assertEqual(diagnostic.detail, "skipped: local prerequisites failed")

    def test_doctor_sanitizes_child_output_and_fails_closed(self):
        secret = "runner-leaked-secret"

        def failing_runner(argv, **kwargs):
            del kwargs
            if "preflight" in argv:
                return subprocess.CompletedProcess(
                    argv,
                    2,
                    stdout=f'{{"token":"{secret}"}}\n',
                    stderr=f"Authorization: Bearer {secret}\n",
                )
            return subprocess.CompletedProcess(
                argv, 0, stdout='{"status":"ok"}\n', stderr=""
            )

        with prepared_doctor_layout(
            env={"MOVIEPILOT_TOKEN": secret}
        ) as layout:
            report = run_doctor(layout, subprocess_runner=failing_runner)

        rendered = json.dumps(report.as_dict(), sort_keys=True)
        self.assertFalse(report.ok)
        self.assertIn("preflight", report.failed_codes)
        self.assertNotIn(secret, rendered)

    def test_doctor_rejects_missing_required_environment_name(self):
        with prepared_doctor_layout() as layout:
            environment = parse_env_file(layout.env_path)
            environment.pop("P115_ROOT_DIRECTORY_ID")

            report = run_doctor(
                layout,
                environment,
                subprocess_runner=fake_success_runner,
            )

        self.assertIn("required_environment", report.failed_codes)
        diagnostic = next(
            item for item in report.diagnostics
            if item.code == "required_environment"
        )
        self.assertIn("P115_ROOT_DIRECTORY_ID", diagnostic.detail)

    def test_doctor_requires_explicit_deployment_path_names(self):
        with prepared_doctor_layout() as layout:
            environment = parse_env_file(layout.env_path)
            environment.pop("ANIME_PIPELINE_LOG_DIR")

            report = run_doctor(
                layout,
                environment,
                subprocess_runner=fake_success_runner,
            )

        diagnostic = next(
            item for item in report.diagnostics
            if item.code == "required_environment"
        )
        self.assertFalse(diagnostic.ok)
        self.assertIn("ANIME_PIPELINE_LOG_DIR", diagnostic.detail)

    def test_doctor_uses_configured_environment_names(self):
        configured_names = {
            "CUSTOM_MOVIEPILOT_TOKEN",
            "CUSTOM_P115_COOKIE",
            "CUSTOM_LITEPAN_KEY",
            "CUSTOM_EMBY_KEY",
        }
        with prepared_doctor_layout() as layout:
            custom_config = json.loads(layout.config_path.read_text(encoding="utf-8"))
            custom_config["services"]["moviepilot"]["token_env"] = "CUSTOM_MOVIEPILOT_TOKEN"
            custom_config["services"]["cloud"]["credential_env"] = "CUSTOM_P115_COOKIE"
            custom_config["services"]["litepan"]["api_key_env"] = "CUSTOM_LITEPAN_KEY"
            custom_config["services"]["emby"]["api_key_env"] = "CUSTOM_EMBY_KEY"
            layout.config_path.write_text(json.dumps(custom_config), encoding="utf-8")
            environment = {
                name: f"value-{index}"
                for index, name in enumerate(sorted(configured_names), start=1)
            }
            environment["P115_ROOT_DIRECTORY_ID"] = "root-id"
            environment.update(
                {
                    "ANIME_PIPELINE_CONFIG": str(layout.config_path),
                    "ANIME_PIPELINE_STATE_DIR": str(layout.state_dir),
                    "ANIME_PIPELINE_LOG_DIR": str(layout.log_dir),
                    "ANIME_PIPELINE_LOCK_PATH": str(layout.lock_path),
                }
            )

            report = run_doctor(
                layout,
                environment,
                subprocess_runner=fake_success_runner,
            )

        diagnostic = next(
            item for item in report.diagnostics
            if item.code == "required_environment"
        )
        self.assertTrue(diagnostic.ok, diagnostic.detail)
        for name in configured_names:
            self.assertIn(name, diagnostic.detail)

    def test_doctor_config_env_identity_fails_when_env_file_is_missing(self):
        with prepared_doctor_layout() as layout:
            environment = parse_env_file(layout.env_path)
            layout.env_path.unlink()

            report = run_doctor(
                layout,
                environment,
                subprocess_runner=fake_success_runner,
            )

        self.assertIn("environment_file", report.failed_codes)
        self.assertIn("config_env_identity", report.failed_codes)

    def test_doctor_rejects_manifest_authorized_release_config(self):
        secret = "release-local-secret"
        with prepared_doctor_layout() as layout:
            release_config = layout.release_root / "config" / "config.v5.json"
            release_config.parent.mkdir()
            release_config.write_text(secret, encoding="utf-8")
            _write_manifest(layout.release_root)

            report = run_doctor(
                layout,
                subprocess_runner=fake_success_runner,
            )

        rendered = json.dumps(report.as_dict(), sort_keys=True)
        self.assertIn("release_manifest", report.failed_codes)
        self.assertNotIn(secret, rendered)


class DeploymentExampleTests(unittest.TestCase):
    def test_examples_share_a_safe_complete_environment_contract(self):
        root = Path(__file__).resolve().parents[2]
        config_path = root / "deploy" / "config.v5.example.json"
        env_path = root / "deploy" / "anime-cloud-pipeline.env.example"

        raw = json.loads(config_path.read_text(encoding="utf-8"))
        environment = parse_env_file(env_path, require_private_mode=False)
        services = raw["services"]
        referenced_names = {
            service[key]
            for service in services.values()
            for key in ("token_env", "api_key_env", "credential_env", "secret_env")
            if key in service
        }

        self.assertTrue(referenced_names)
        self.assertTrue(referenced_names.issubset(environment))
        self.assertIn("P115_ROOT_DIRECTORY_ID", environment)
        self.assertNotIn("ANIME_PIPELINE_PYTHON", environment)
        self.assertNotIn("ANIME_PIPELINE_PYTHON", env_path.read_text(encoding="utf-8"))
        for name in referenced_names | {"P115_ROOT_DIRECTORY_ID"}:
            with self.subTest(name=name):
                self.assertEqual(environment[name], "")

        rendered = json.dumps(raw, sort_keys=True)
        self.assertNotIn("subscriptions", rendered)
        for service in services.values():
            if "base_url" in service:
                self.assertIn(".example.invalid", service["base_url"])
        for key, value in raw["paths"].items():
            if key in {"cloud_library_subdir", "litepan_publish_subdir"}:
                self.assertTrue(value)
                self.assertNotIn("/", value)
                self.assertNotIn("\\", value)
                self.assertNotIn(value, (".", ".."))
                continue
            self.assertTrue(value.startswith("/<"), value)
        self.assertTrue(services["cloud"]["root_directory_path"].startswith("/<"))

        secret_like_keys = {
            key
            for service in services.values()
            for key in service
            if any(part in key.casefold() for part in ("token", "key", "credential", "secret", "cookie"))
            and not key.endswith("_env")
        }
        self.assertEqual(secret_like_keys, set())
        placeholders = {
            name: f"placeholder-{index}"
            for index, name in enumerate(sorted(referenced_names), start=1)
        }
        parsed = ConfigSchema.from_mapping(raw, placeholders, "linux")
        self.assertEqual(parsed.schema_version, 5)


class HermesMainTests(unittest.TestCase):
    def _private_env_file(self, directory: str, text: str) -> Path:
        path = Path(directory, "operator.env")
        path.write_text(text, encoding="utf-8")
        if os.name == "posix":
            path.chmod(0o600)
        return path.resolve()

    def test_main_merges_explicit_env_over_a_copy_and_execs_pipeline(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            config = root / "operator config.json"
            config.write_text(
                (Path(__file__).resolve().parents[2] / "deploy" / "config.v5.example.json").read_text(
                    encoding="utf-8"
                ),
                encoding="utf-8",
            )
            env_path = self._private_env_file(
                tmp,
                f"ANIME_PIPELINE_CONFIG={config}\n"
                f"ANIME_PIPELINE_STATE_DIR={root / 'state'}\n"
                f"ANIME_PIPELINE_LOG_DIR={root / 'logs'}\n"
                f"ANIME_PIPELINE_LOCK_PATH={root / 'state' / 'run.lock'}\n"
                "P115_ROOT_DIRECTORY_ID=root-id\n"
                "MOVIEPILOT_TOKEN=$(touch should-not-exist)\n"
                "P115_COOKIE=p115-secret\n"
                "LITEPAN_API_KEY=litepan-secret\n"
                "EMBY_API_KEY=emby-secret\n",
            )
            caller = {
                "MOVIEPILOT_TOKEN": "old-value",
                "PYTHONPATH": "/hostile/path",
                "ANIME_PIPELINE_PYTHON": str(root / "operator-python"),
            }
            before = dict(caller)
            stdout = io.StringIO()
            stderr = io.StringIO()

            with mock.patch.object(hermes_runtime.os, "execve") as execve:
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    code = main(
                        ["--env-file", str(env_path), "status", "argument with spaces"],
                        environ=caller,
                    )

            self.assertEqual(code, 0)
            self.assertEqual(caller, before)
            execve.assert_called_once()
            executable, command, merged = execve.call_args.args
            self.assertEqual(executable, str(root / "operator-python"))
            self.assertEqual(
                command,
                [
                    str(root / "operator-python"),
                    "-B",
                    str(Path(hermes_runtime.__file__).resolve().parents[1] / "scripts" / "run_pipeline.py"),
                    "status",
                    "--config",
                    str(config),
                    "argument with spaces",
                ],
            )
            self.assertEqual(merged["MOVIEPILOT_TOKEN"], "$(touch should-not-exist)")
            self.assertNotIn("ANIME_PIPELINE_PYTHON", merged)
            self.assertNotIn("PYTHONPATH", merged)
            self.assertEqual(stdout.getvalue(), "")
            self.assertEqual(stderr.getvalue(), "")
            self.assertFalse(Path(tmp, "should-not-exist").exists())

    def test_resource_apply_passes_only_valid_one_shot_process_authorization(self):
        revision = "a" * 64
        with prepared_doctor_layout() as layout:
            caller = {
                "OCTOP_PIPELINE_ALLOW_RESOURCES": "1",
                "OCTOP_PIPELINE_RESOURCE_REVISION": revision,
                "UNRELATED": "must-not-pass",
            }
            before = dict(caller)
            with mock.patch.object(
                hermes_runtime, "resolve_layout", return_value=layout
            ), mock.patch.object(hermes_runtime.os, "execve") as execve:
                code = main(["prepare-resources", "--apply"], environ=caller)

            self.assertEqual(code, 0)
            self.assertEqual(caller, before)
            execve.assert_called_once()
            _, command, child_environment = execve.call_args.args
            self.assertEqual(command[3], "prepare-resources")
            self.assertEqual(child_environment["OCTOP_PIPELINE_ALLOW_RESOURCES"], "1")
            self.assertEqual(
                child_environment["OCTOP_PIPELINE_RESOURCE_REVISION"], revision
            )
            self.assertNotIn("UNRELATED", child_environment)

    def test_resource_authorization_is_dropped_for_read_only_or_non_apply_commands(self):
        caller = {
            "OCTOP_PIPELINE_ALLOW_RESOURCES": "1",
            "OCTOP_PIPELINE_RESOURCE_REVISION": "b" * 64,
        }
        cases = (
            ["qualify"],
            ["plan-resources"],
            ["prepare-resources"],
        )
        with prepared_doctor_layout() as layout:
            for arguments in cases:
                with self.subTest(arguments=arguments):
                    with mock.patch.object(
                        hermes_runtime, "resolve_layout", return_value=layout
                    ), mock.patch.object(hermes_runtime.os, "execve") as execve:
                        code = main(arguments, environ=caller)

                    self.assertEqual(code, 0)
                    execve.assert_called_once()
                    child_environment = execve.call_args.args[2]
                    self.assertNotIn(
                        "OCTOP_PIPELINE_ALLOW_RESOURCES", child_environment
                    )
                    self.assertNotIn(
                        "OCTOP_PIPELINE_RESOURCE_REVISION", child_environment
                    )

    def test_resource_apply_does_not_accept_authorization_from_persistent_env_file(self):
        with prepared_doctor_layout(
            env={
                "OCTOP_PIPELINE_ALLOW_RESOURCES": "1",
                "OCTOP_PIPELINE_RESOURCE_REVISION": "c" * 64,
            }
        ) as layout:
            stderr = io.StringIO()
            with mock.patch.object(
                hermes_runtime, "resolve_layout", return_value=layout
            ), mock.patch.object(hermes_runtime.os, "execve") as execve:
                with redirect_stderr(stderr):
                    code = main(["prepare-resources", "--apply"], environ={})

            self.assertEqual(code, 2)
            execve.assert_not_called()
            self.assertIn("unsupported environment variable", stderr.getvalue())

    def test_resource_apply_drops_invalid_one_shot_authorization(self):
        caller = {
            "OCTOP_PIPELINE_ALLOW_RESOURCES": "1",
            "OCTOP_PIPELINE_RESOURCE_REVISION": "D" * 64,
        }
        with prepared_doctor_layout() as layout:
            with mock.patch.object(
                hermes_runtime, "resolve_layout", return_value=layout
            ), mock.patch.object(hermes_runtime.os, "execve") as execve:
                code = main(["prepare-resources", "--apply"], environ=caller)

            self.assertEqual(code, 0)
            execve.assert_called_once()
            child_environment = execve.call_args.args[2]
            self.assertNotIn("OCTOP_PIPELINE_ALLOW_RESOURCES", child_environment)
            self.assertNotIn("OCTOP_PIPELINE_RESOURCE_REVISION", child_environment)

    def test_resource_authorization_names_cannot_be_configured_as_service_secrets(self):
        with prepared_doctor_layout() as layout:
            raw = json.loads(layout.config_path.read_text(encoding="utf-8"))
            raw["services"]["moviepilot"]["token_env"] = (
                "OCTOP_PIPELINE_ALLOW_RESOURCES"
            )
            layout.config_path.write_text(
                json.dumps(raw), encoding="utf-8"
            )
            stderr = io.StringIO()
            with mock.patch.object(
                hermes_runtime, "resolve_layout", return_value=layout
            ), mock.patch.object(hermes_runtime.os, "execve") as execve:
                with redirect_stderr(stderr):
                    code = main(
                        ["prepare-resources", "--apply"],
                        environ={
                            "OCTOP_PIPELINE_ALLOW_RESOURCES": "1",
                            "OCTOP_PIPELINE_RESOURCE_REVISION": "d" * 64,
                        },
                    )

            self.assertEqual(code, 2)
            execve.assert_not_called()
            self.assertIn("reserved resource authorization", stderr.getvalue())

    def test_main_passes_only_fixed_and_configured_environment_names(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            config = root / "config.json"
            config.write_text(
                (
                    Path(__file__).resolve().parents[2]
                    / "deploy"
                    / "config.v5.example.json"
                ).read_text(encoding="utf-8").replace(
                    "MOVIEPILOT_TOKEN", "CUSTOM_MOVIEPILOT_TOKEN"
                ),
                encoding="utf-8",
            )
            env_path = self._private_env_file(
                tmp,
                f"ANIME_PIPELINE_CONFIG={config}\n"
                f"ANIME_PIPELINE_STATE_DIR={root / 'state'}\n"
                f"ANIME_PIPELINE_LOG_DIR={root / 'logs'}\n"
                f"ANIME_PIPELINE_LOCK_PATH={root / 'state' / 'run.lock'}\n"
                "P115_ROOT_DIRECTORY_ID=root-id\n"
                "CUSTOM_MOVIEPILOT_TOKEN=config-secret\n"
                "P115_COOKIE=p115-secret\n",
            )
            caller = {
                "CUSTOM_MOVIEPILOT_TOKEN": "caller-secret",
                "PYTHONPATH": "/hostile/path",
                "LD_PRELOAD": "/hostile/lib.so",
                "UNRELATED": "must-not-pass",
            }

            with mock.patch.object(hermes_runtime.os, "execve") as execve:
                code = main(
                    ["--env-file", str(env_path), "status"],
                    environ=caller,
                )

            self.assertEqual(code, 0)
            _, _, merged = execve.call_args.args
            self.assertEqual(merged["CUSTOM_MOVIEPILOT_TOKEN"], "config-secret")
            self.assertIn("P115_COOKIE", merged)
            self.assertNotIn("PYTHONPATH", merged)
            self.assertNotIn("PYTHONHOME", merged)
            self.assertNotIn("LD_PRELOAD", merged)
            self.assertNotIn("UNRELATED", merged)

    def test_main_rejects_credentials_not_from_validated_service_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            config = root / "config.json"
            raw = json.loads(
                (
                    Path(__file__).resolve().parents[2]
                    / "deploy"
                    / "config.v5.example.json"
                ).read_text(encoding="utf-8")
            )
            raw["metadata"] = {"token_env": "UNEXPECTED_TOKEN"}
            config.write_text(json.dumps(raw), encoding="utf-8")
            env_path = self._private_env_file(
                tmp,
                f"ANIME_PIPELINE_CONFIG={config}\n"
                f"ANIME_PIPELINE_STATE_DIR={root / 'state'}\n"
                f"ANIME_PIPELINE_LOG_DIR={root / 'logs'}\n"
                f"ANIME_PIPELINE_LOCK_PATH={root / 'state' / 'run.lock'}\n"
                "P115_ROOT_DIRECTORY_ID=root-id\n"
                "UNEXPECTED_TOKEN=must-not-pass\n",
            )

            with mock.patch.object(hermes_runtime.os, "execve") as execve:
                code = main(
                    ["--env-file", str(env_path), "status"],
                    environ={},
                )

            self.assertEqual(code, 2)
            execve.assert_not_called()

    def test_main_uses_the_layout_default_env_file_when_option_is_omitted(self):
        with tempfile.TemporaryDirectory() as tmp:
            env_path = self._private_env_file(tmp, "P115_ROOT_DIRECTORY_ID=from-default\n")
            layout = make_layout(env_path=env_path)
            layout.config_path.parent.mkdir(parents=True, exist_ok=True)
            layout.config_path.write_text(
                (
                    Path(__file__).resolve().parents[2]
                    / "deploy"
                    / "config.v5.example.json"
                ).read_text(encoding="utf-8"),
                encoding="utf-8",
            )
            env_path.write_text(
                f"ANIME_PIPELINE_CONFIG={layout.config_path}\n"
                f"ANIME_PIPELINE_STATE_DIR={layout.state_dir}\n"
                f"ANIME_PIPELINE_LOG_DIR={layout.log_dir}\n"
                f"ANIME_PIPELINE_LOCK_PATH={layout.lock_path}\n"
                "P115_ROOT_DIRECTORY_ID=from-default\n"
                "MOVIEPILOT_TOKEN=moviepilot\n"
                "P115_COOKIE=p115\n"
                "LITEPAN_API_KEY=litepan\n"
                "EMBY_API_KEY=emby\n",
                encoding="utf-8",
            )
            with mock.patch.object(
                hermes_runtime, "resolve_layout", return_value=layout
            ), mock.patch.object(hermes_runtime.os, "execve") as execve:
                code = main(["audit"], environ={"BASE": "value"})

            self.assertEqual(code, 0)
            self.assertEqual(execve.call_args.args[2]["P115_ROOT_DIRECTORY_ID"], "from-default")

    def test_main_rejects_relative_env_file_without_exec(self):
        stderr = io.StringIO()
        with mock.patch.object(hermes_runtime.os, "execve") as execve:
            with redirect_stderr(stderr):
                code = main(["--env-file", "relative.env", "status"], environ={})

        self.assertEqual(code, 2)
        execve.assert_not_called()
        self.assertIn("absolute", stderr.getvalue())

    def test_main_rejects_an_env_file_inside_an_installed_release(self):
        with tempfile.TemporaryDirectory() as tmp:
            install_root = Path(tmp).resolve()
            release = install_root / "releases" / "v5"
            scripts = release / "scripts"
            scripts.mkdir(parents=True)
            runtime_path = scripts / "hermes_runtime.py"
            runtime_path.write_text("", encoding="utf-8")
            env_path = self._private_env_file(str(release), "TOKEN=value\n")
            stderr = io.StringIO()

            with mock.patch.object(hermes_runtime, "__file__", str(runtime_path)):
                with mock.patch.object(hermes_runtime.os, "execve") as execve:
                    with redirect_stderr(stderr):
                        code = main(
                            ["--env-file", str(env_path), "status"], environ={}
                        )

        self.assertEqual(code, 2)
        execve.assert_not_called()
        self.assertIn("outside", stderr.getvalue())

    def test_doctor_loads_environment_and_returns_structured_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            env_path = self._private_env_file(tmp, "P115_COOKIE=secret-value\n")
            layout = make_layout(env_path=env_path)
            layout.config_path.parent.mkdir(parents=True, exist_ok=True)
            layout.config_path.write_text(
                (
                    Path(__file__).resolve().parents[2]
                    / "deploy"
                    / "config.v5.example.json"
                ).read_text(encoding="utf-8"),
                encoding="utf-8",
            )
            report = DoctorReport(
                (hermes_runtime.Diagnostic("fixture", True, "checked"),)
            )
            stdout = io.StringIO()
            with mock.patch.object(
                hermes_runtime, "resolve_layout", return_value=layout
            ), mock.patch.object(
                hermes_runtime, "run_doctor", return_value=report
            ) as doctor, mock.patch.object(
                hermes_runtime.os, "execve"
            ) as execve:
                with redirect_stdout(stdout):
                    code = main(["doctor"], environ={})

        payload = json.loads(stdout.getvalue())
        self.assertEqual(code, 0)
        self.assertEqual(payload["command"], "doctor")
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["diagnostics"][0]["code"], "fixture")
        self.assertEqual(doctor.call_args.args[1]["P115_COOKIE"], "secret-value")
        execve.assert_not_called()

    def test_version_is_local_and_does_not_load_an_env_file(self):
        stdout = io.StringIO()
        with mock.patch.object(hermes_runtime.os, "execve") as execve:
            with redirect_stdout(stdout):
                code = main(["version"], environ={})

        payload = json.loads(stdout.getvalue())
        self.assertEqual(code, 0)
        self.assertEqual(payload["command"], "version")
        self.assertTrue(payload["release"])
        execve.assert_not_called()


@unittest.skipUnless(os.name == "posix", "POSIX shell required")
class HermesPosixLauncherTests(unittest.TestCase):
    def test_launcher_preserves_argument_boundaries_and_exit_code(self):
        launcher = Path(__file__).resolve().parents[1] / "hermes_anime.sh"
        self.assertTrue(launcher.stat().st_mode & stat.S_IXUSR)
        with tempfile.TemporaryDirectory() as tmp:
            fake_python = Path(tmp, "fake-python")
            fake_python.write_text(
                "#!/bin/sh\n"
                "if [ \"$1\" = '-c' ]; then exit 0; fi\n"
                "printf '%s\\n' \"$@\"\n"
                "exit 23\n",
                encoding="utf-8",
            )
            fake_python.chmod(0o700)
            environ = dict(os.environ)
            environ["ANIME_PIPELINE_PYTHON"] = str(fake_python)

            completed = subprocess.run(
                [
                    str(launcher),
                    "--env-file",
                    "/tmp/path with spaces.env",
                    "status",
                    "literal * $(touch no)",
                ],
                check=False,
                capture_output=True,
                text=True,
                env=environ,
            )

        self.assertEqual(completed.returncode, 23)
        self.assertEqual(
            completed.stdout.splitlines(),
            [
                "-B",
                str(launcher.parent / "hermes_runtime.py"),
                "--env-file",
                "/tmp/path with spaces.env",
                "status",
                "literal * $(touch no)",
            ],
        )

    def test_launcher_rejects_python_below_3_12_with_exit_64(self):
        launcher = Path(__file__).resolve().parents[1] / "hermes_anime.sh"
        with tempfile.TemporaryDirectory() as tmp:
            fake_python = Path(tmp, "fake-python")
            fake_python.write_text(
                "#!/bin/sh\n"
                "if [ \"$1\" = '-c' ]; then exit 64; fi\n"
                "exit 99\n",
                encoding="utf-8",
            )
            fake_python.chmod(0o700)
            environ = dict(os.environ)
            environ["ANIME_PIPELINE_PYTHON"] = str(fake_python)

            completed = subprocess.run(
                [str(launcher), "version"],
                check=False,
                capture_output=True,
                text=True,
                env=environ,
            )

        self.assertEqual(completed.returncode, 64)

    def test_launcher_uses_exported_interpreter_when_python3_is_unavailable(self):
        launcher = Path(__file__).resolve().parents[1] / "hermes_anime.sh"
        with tempfile.TemporaryDirectory() as tmp:
            fake_python = Path(tmp, "fake-python")
            fake_python.write_text(
                "#!/bin/sh\n"
                "if [ \"$1\" = '-c' ]; then exit 0; fi\n"
                "printf '%s\\n' \"$@\"\n",
                encoding="utf-8",
            )
            fake_python.chmod(0o700)
            environ = {"ANIME_PIPELINE_PYTHON": str(fake_python), "PATH": ""}

            completed = subprocess.run(
                [str(launcher), "version"],
                check=False,
                capture_output=True,
                text=True,
                env=environ,
            )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("version", completed.stdout)


if __name__ == "__main__":
    unittest.main()
