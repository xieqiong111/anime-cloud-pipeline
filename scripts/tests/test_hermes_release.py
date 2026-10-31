from __future__ import annotations

from contextlib import contextmanager, redirect_stderr, redirect_stdout
from dataclasses import replace
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
import zipfile

import deploy.hermes_release as hermes_release
from deploy.hermes_release import (
    InstallRequest,
    MANIFEST_NAME,
    ReleaseError,
    activate_release,
    build_archive,
    install_release,
    iter_release_files,
    main,
    rollback_release,
    validate_install_root,
    verify_manifest,
    write_manifest,
)


def _write(root: Path, relative: str, content: str = "fixture\n") -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8", newline="")
    return path


def _create_directory_link(link: Path, target: Path) -> None:
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
    if completed.returncode:
        raise OSError(completed.stderr)


def _remove_directory_link(link: Path) -> None:
    if getattr(os.path, "isjunction", lambda _: False)(link):
        link.rmdir()
    else:
        link.unlink()


def _successful_doctor_output() -> str:
    codes = (
        "release_manifest",
        "config_file",
        "environment_file",
        "config_env_identity",
        "environment_private_mode",
        "config_path_outside_release",
        "env_path_outside_release",
        "state_dir_outside_release",
        "log_dir_outside_release",
        "lock_path_outside_release",
        "state_dir_ready",
        "log_dir_ready",
        "lock_path_ready",
        "required_environment",
        "python_sqlite",
        "preflight",
        "migrate_check_only",
    )
    return json.dumps(
        {
            "command": "doctor",
            "ok": True,
            "failed_codes": [],
            "diagnostics": [
                {"code": code, "ok": True, "detail": "checked"}
                for code in codes
            ],
        },
        sort_keys=True,
    ) + "\n"


def _snapshot_tree(root: Path) -> dict[str, tuple[int, bytes | str | None]]:
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


@contextmanager
def real_candidate_doctor_fixture():
    with tempfile.TemporaryDirectory() as tmp:
        fixture_root = Path(tmp).resolve()
        install_root = fixture_root / "install"
        release = install_root / "releases" / "v5"
        candidate_scripts = release / "scripts"
        candidate_pipeline = candidate_scripts / "pipeline"
        candidate_pipeline.mkdir(parents=True)

        project_root = Path(__file__).resolve().parents[2]
        shutil.copy2(
            project_root / "scripts" / "hermes_runtime.py",
            candidate_scripts / "hermes_runtime.py",
        )
        shutil.copy2(
            project_root / "scripts" / "run_pipeline.py",
            candidate_scripts / "run_pipeline.py",
        )
        for source in sorted((project_root / "scripts" / "pipeline").glob("*.py")):
            shutil.copy2(source, candidate_pipeline / source.name)
        _write(
            release,
            "RELEASE.json",
            json.dumps({"release_id": "v5", "schema_version": 1}) + "\n",
        )
        write_manifest(release)

        config_path = install_root / "config" / "config.v5.json"
        config_path.parent.mkdir(parents=True)
        shutil.copy2(project_root / "deploy" / "config.v5.example.json", config_path)
        state_dir = install_root / "state"
        log_dir = install_root / "logs"
        state_dir.mkdir()
        log_dir.mkdir()
        env_file = install_root / "config" / "anime-cloud-pipeline.env"
        environment = {
            "ANIME_PIPELINE_CONFIG": str(config_path),
            "ANIME_PIPELINE_STATE_DIR": str(state_dir),
            "ANIME_PIPELINE_LOG_DIR": str(log_dir),
            "ANIME_PIPELINE_LOCK_PATH": str(state_dir / "pipeline.lock"),
            "MOVIEPILOT_TOKEN": "moviepilot-fixture-secret",
            "P115_COOKIE": "p115-fixture-secret",
            "P115_ROOT_DIRECTORY_ID": "root-fixture-id",
            "LITEPAN_API_KEY": "litepan-fixture-secret",
            "EMBY_API_KEY": "emby-fixture-secret",
        }
        env_file.write_text(
            "".join(f"{name}={value}\n" for name, value in environment.items()),
            encoding="utf-8",
        )
        if os.name == "posix":
            env_file.chmod(0o600)
        yield install_root, release, env_file


@contextmanager
def prepared_source():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for relative in (
            "SKILL.md",
            "CHANGELOG-V5.md",
            "docs/V5-UPGRADE.md",
            "assets/config.example.json",
            "scripts/run_pipeline.py",
            "scripts/hermes_anime.sh",
            "references/architecture.md",
            "deploy/placeholder.py",
        ):
            _write(root, relative)
        for relative in (
            "PROJECT_LOG.md",
            ".superpowers/plan.md",
            "scripts/__pycache__/cached.pyc",
            "scripts/runtime.db",
            "scripts/runtime.db-wal",
            "scripts/runtime.db-shm",
            "scripts/runtime.log",
            "scripts/old-release.zip",
            "build/generated.py",
            "dist/generated.py",
        ):
            _write(root, relative)
        yield root


@contextmanager
def staged_release():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _write(root, "SKILL.md")
        _write(root, "scripts/run_pipeline.py", "original\n")
        yield root


@contextmanager
def install_request_fixture(*, release_id: str = "v5", activate: bool = False):
    with tempfile.TemporaryDirectory() as tmp:
        fixture_root = Path(tmp).resolve()
        source = fixture_root / "source"
        source.mkdir()
        _write(source, "SKILL.md")
        _write(source, "bin/anime-cloud-pipeline", "#!/bin/sh\nexit 0\n")
        _write(source, "scripts/hermes_runtime.py", "# runtime fixture\n")
        _write(source, "scripts/run_pipeline.py", "print('fixture')\n")
        _write(
            source,
            "RELEASE.json",
            json.dumps(
                {
                    "release_id": release_id,
                    "schema_version": 1,
                    "source_revision": None,
                },
                sort_keys=True,
            )
            + "\n",
        )
        write_manifest(source)

        install_root = fixture_root / "install"
        config = _write(install_root, "config/config.v5.json", "{}\n")
        env_file = _write(
            install_root,
            "config/anime-cloud-pipeline.env",
            "ANIME_PIPELINE_TOKEN=fixture\n",
        )
        if os.name == "posix":
            env_file.chmod(0o600)

        yield InstallRequest(
            source=source,
            install_root=install_root,
            config=config,
            env_file=env_file,
            release_id=release_id,
            activate=activate,
        )


@contextmanager
def installed_release_trees_fixture(
    release_ids: tuple[str, ...] = ("v4", "v5", "v6"),
):
    with tempfile.TemporaryDirectory() as tmp:
        install_root = Path(tmp).resolve() / "install"
        releases = install_root / "releases"
        releases.mkdir(parents=True)
        for release_id in release_ids:
            release = releases / release_id
            _write(release, "SKILL.md", f"release {release_id}\n")
            _write(
                release,
                "RELEASE.json",
                json.dumps({"release_id": release_id, "schema_version": 1})
                + "\n",
            )
            write_manifest(release)
        yield install_root


@contextmanager
def installed_two_release_fixture(*, active: str = "v5"):
    with installed_release_trees_fixture(("v5", "v6")) as install_root:
        (install_root / "current").symlink_to(
            Path("releases") / active, target_is_directory=True
        )
        yield install_root


class ReleaseManifestTests(unittest.TestCase):
    def test_release_allowlist_packages_hermes_cron_helper(self):
        project_root = Path(__file__).resolve().parents[2]
        files = [path.as_posix() for path in iter_release_files(project_root)]

        self.assertIn("deploy/hermes_cron.py", files)

    def test_release_allowlist_excludes_development_and_runtime_files(self):
        with prepared_source() as source:
            files = [path.as_posix() for path in iter_release_files(source)]

        self.assertIn("SKILL.md", files)
        self.assertIn("scripts/run_pipeline.py", files)
        self.assertNotIn("PROJECT_LOG.md", files)
        self.assertFalse(any(".superpowers" in path for path in files))
        self.assertFalse(
            any(
                path.endswith((".db", ".db-wal", ".db-shm", ".log", ".zip"))
                for path in files
            )
        )
        self.assertEqual(files, sorted(files))

    def test_release_allowlist_excludes_live_config_but_keeps_examples(self):
        with prepared_source() as source:
            _write(source, "deploy/anime-cloud-pipeline.env", "TOKEN=live\n")
            _write(source, "deploy/config.v5.json", '{"live": true}\n')
            _write(source, "deploy/anime-cloud-pipeline.env.example")
            _write(source, "deploy/config.v5.example.json")

            files = [path.as_posix() for path in iter_release_files(source)]

        self.assertNotIn("deploy/anime-cloud-pipeline.env", files)
        self.assertNotIn("deploy/config.v5.json", files)
        self.assertIn("deploy/anime-cloud-pipeline.env.example", files)
        self.assertIn("deploy/config.v5.example.json", files)

    def test_release_allowlist_rejects_linked_product_directories(self):
        with prepared_source() as source, tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp)
            _write(target, "secret.py", "must not be packaged\n")
            link = source / "scripts" / "linked"
            try:
                _create_directory_link(link, target)
            except (NotImplementedError, OSError) as error:
                self.skipTest(f"directory links unavailable: {error}")

            with self.assertRaises(ReleaseError):
                list(iter_release_files(source))

    def test_release_allowlist_rejects_linked_parent_of_explicit_doc(self):
        with prepared_source() as source, tempfile.TemporaryDirectory() as tmp:
            shutil.rmtree(source / "docs")
            target = Path(tmp)
            _write(target, "V5-UPGRADE.md", "external documentation\n")
            link = source / "docs"
            try:
                _create_directory_link(link, target)
            except (NotImplementedError, OSError) as error:
                self.skipTest(f"directory links unavailable: {error}")

            try:
                with self.assertRaises(ReleaseError):
                    list(iter_release_files(source))
            finally:
                _remove_directory_link(link)

    def test_manifest_is_sorted_lowercase_sha256_and_excludes_itself(self):
        with staged_release() as root:
            manifest = write_manifest(root)
            lines = manifest.read_text(encoding="utf-8").splitlines()

        expected = [
            f"{hashlib.sha256(b'fixture\n').hexdigest()}  SKILL.md",
            f"{hashlib.sha256(b'original\n').hexdigest()}  scripts/run_pipeline.py",
        ]
        self.assertEqual(lines, expected)
        self.assertFalse(any(MANIFEST_NAME in line for line in lines))

    def test_manifest_detects_changed_files(self):
        with staged_release() as root:
            write_manifest(root)
            self.assertTrue(verify_manifest(root).ok)

            (root / "scripts/run_pipeline.py").write_text(
                "changed", encoding="utf-8"
            )
            report = verify_manifest(root)

        self.assertFalse(report.ok)
        self.assertEqual(report.changed, ("scripts/run_pipeline.py",))

    def test_manifest_detects_missing_files(self):
        with staged_release() as root:
            write_manifest(root)
            (root / "SKILL.md").unlink()

            report = verify_manifest(root)

        self.assertFalse(report.ok)
        self.assertEqual(report.missing, ("SKILL.md",))

    def test_manifest_detects_unexpected_files(self):
        with staged_release() as root:
            write_manifest(root)
            _write(root, "scripts/unlisted.py")

            report = verify_manifest(root)

        self.assertFalse(report.ok)
        self.assertEqual(report.unexpected, ("scripts/unlisted.py",))

    def test_manifest_rejects_absolute_and_parent_paths(self):
        digest = hashlib.sha256(b"fixture\n").hexdigest()
        for unsafe in ("/outside", "../outside", "scripts/../../outside"):
            with self.subTest(path=unsafe), staged_release() as root:
                (root / MANIFEST_NAME).write_text(
                    f"{digest}  {unsafe}\n", encoding="utf-8", newline=""
                )

                with self.assertRaises(ReleaseError):
                    verify_manifest(root)

    def test_manifest_rejects_duplicate_paths(self):
        with staged_release() as root:
            manifest = write_manifest(root)
            first_line = manifest.read_text(encoding="utf-8").splitlines()[0]
            with manifest.open("a", encoding="utf-8", newline="") as stream:
                stream.write(first_line + "\n")

            with self.assertRaises(ReleaseError):
                verify_manifest(root)

    def test_manifest_writer_rejects_forbidden_runtime_files(self):
        with staged_release() as root:
            _write(root, "scripts/runtime.log")

            with self.assertRaises(ReleaseError):
                write_manifest(root)

    def test_manifest_verifier_rejects_manifest_authorized_runtime_files(self):
        with staged_release() as root:
            manifest = write_manifest(root)
            runtime = _write(root, "scripts/runtime.db")
            with manifest.open("a", encoding="utf-8", newline="") as stream:
                stream.write(
                    f"{hashlib.sha256(runtime.read_bytes()).hexdigest()}  "
                    "scripts/runtime.db\n"
                )

            with self.assertRaises(ReleaseError):
                verify_manifest(root)

    def test_manifest_writer_and_verifier_reject_symlinks(self):
        with staged_release() as root, tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp)
            _write(target, "linked.py")
            link = root / "scripts" / "linked"
            try:
                _create_directory_link(link, target)
            except (NotImplementedError, OSError) as error:
                self.skipTest(f"directory links unavailable: {error}")

            with self.assertRaises(ReleaseError):
                write_manifest(root)

            _remove_directory_link(link)
            write_manifest(root)
            _create_directory_link(link, target)
            with self.assertRaises(ReleaseError):
                verify_manifest(root)


class ReleaseArchiveTests(unittest.TestCase):
    def test_build_archive_has_one_top_level_and_installed_wrapper(self):
        with prepared_source() as source, tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp, "release.zip")

            result = build_archive(source, output, "v5-test")

            with zipfile.ZipFile(output) as archive:
                names = archive.namelist()
                wrapper = archive.read(
                    "anime-cloud-pipeline-hermes/bin/anime-cloud-pipeline"
                )
        self.assertEqual(
            {name.split("/", 1)[0] for name in names},
            {"anime-cloud-pipeline-hermes"},
        )
        self.assertIn(
            "anime-cloud-pipeline-hermes/bin/anime-cloud-pipeline", names
        )
        self.assertIn("anime-cloud-pipeline-hermes/MANIFEST.sha256", names)
        self.assertEqual(wrapper, b"fixture\n")
        self.assertEqual(result.release_id, "v5-test")

    def test_build_archive_keeps_release_id_and_source_revision_distinct(self):
        with prepared_source() as source, tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp, "release.zip")

            result = build_archive(
                source,
                output,
                "v5-friendly-name",
                source_revision="commit-abc123",
            )

            with zipfile.ZipFile(output) as archive:
                metadata = json.loads(
                    archive.read(
                        "anime-cloud-pipeline-hermes/RELEASE.json"
                    ).decode("utf-8")
                )
        self.assertEqual(
            metadata,
            {
                "release_id": "v5-friendly-name",
                "schema_version": 1,
                "source_revision": "commit-abc123",
            },
        )
        self.assertEqual(result.release_id, "v5-friendly-name")
        self.assertEqual(result.source_revision, "commit-abc123")

    def test_build_archive_three_argument_form_does_not_invent_revision(self):
        with prepared_source() as source, tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp, "release.zip")

            result = build_archive(source, output, "v5-compatible")

            with zipfile.ZipFile(output) as archive:
                metadata = json.loads(
                    archive.read(
                        "anime-cloud-pipeline-hermes/RELEASE.json"
                    ).decode("utf-8")
                )
        self.assertIsNone(metadata["source_revision"])
        self.assertIsNone(result.source_revision)

    def test_build_archive_is_byte_reproducible_and_marks_wrapper_executable(self):
        with prepared_source() as source, tempfile.TemporaryDirectory() as tmp:
            first = Path(tmp, "first.zip")
            second = Path(tmp, "second.zip")

            build_archive(source, first, "v5-test")
            build_archive(source, second, "v5-test")

            with zipfile.ZipFile(first) as archive:
                wrapper_mode = (
                    archive.getinfo(
                        "anime-cloud-pipeline-hermes/bin/anime-cloud-pipeline"
                    ).external_attr
                    >> 16
                ) & 0o777
                source_wrapper_mode = (
                    archive.getinfo(
                        "anime-cloud-pipeline-hermes/scripts/hermes_anime.sh"
                    ).external_attr
                    >> 16
                ) & 0o777
            self.assertEqual(first.read_bytes(), second.read_bytes())
            self.assertEqual(wrapper_mode, 0o555)
            self.assertEqual(source_wrapper_mode, 0o555)

    def test_generated_archive_verifies_after_fresh_extraction(self):
        with prepared_source() as source, tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp, "release.zip")
            extracted = Path(tmp, "extracted")
            build_archive(source, output, "v5-test")

            with zipfile.ZipFile(output) as archive:
                archive.extractall(extracted)
            report = verify_manifest(extracted / "anime-cloud-pipeline-hermes")

        self.assertTrue(report.ok, report)

    def test_build_cli_passes_explicit_source_revision_to_archive(self):
        with prepared_source() as source, tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp, "release.zip")
            stdout = io.StringIO()

            with redirect_stdout(stdout):
                code = main(
                    [
                        "build",
                        "--source",
                        str(source),
                        "--output",
                        str(output),
                        "--release-id",
                        "v5-cli",
                        "--source-revision",
                        "commit-cli-123",
                    ]
                )

            with zipfile.ZipFile(output) as archive:
                metadata = json.loads(
                    archive.read(
                        "anime-cloud-pipeline-hermes/RELEASE.json"
                    ).decode("utf-8")
                )
        self.assertEqual(code, 0)
        self.assertEqual(
            json.loads(stdout.getvalue())["source_revision"], "commit-cli-123"
        )
        self.assertEqual(metadata["release_id"], "v5-cli")
        self.assertEqual(metadata["source_revision"], "commit-cli-123")


class InstallSafetyTests(unittest.TestCase):
    def test_install_rejects_root_relative_home_and_unnormalized_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            unnormalized = Path(tmp).resolve() / "child" / ".." / "install"
            for bad_root in (
                Path("/"),
                Path("relative"),
                Path.home(),
                unnormalized,
            ):
                with self.subTest(root=bad_root):
                    with self.assertRaises(ReleaseError):
                        validate_install_root(bad_root)

    def test_install_does_not_overwrite_existing_release(self):
        with install_request_fixture(release_id="v5") as request:
            first = install_release(request)
            installed_script = first.release_path / "scripts/run_pipeline.py"
            self.assertTrue(first.installed)
            self.assertEqual(
                installed_script.read_text(encoding="utf-8"),
                "print('fixture')\n",
            )

            _write(request.source, "scripts/run_pipeline.py", "print('changed')\n")
            write_manifest(request.source)

            with self.assertRaises(ReleaseError):
                install_release(request)
            self.assertEqual(
                installed_script.read_text(encoding="utf-8"),
                "print('fixture')\n",
            )

    def test_publish_refuses_release_path_created_after_precheck(self):
        with install_request_fixture(release_id="v5-race") as request:
            release_path = request.install_root / "releases/v5-race"
            real_verify_manifest = hermes_release.verify_manifest
            real_rename = Path.rename
            race_created = False

            def verify_and_create_race(root: Path):
                nonlocal race_created
                report = real_verify_manifest(root)
                if root.name.startswith(".staging-") and not race_created:
                    release_path.mkdir()
                    race_created = True
                return report

            def posix_rename(source: Path, destination: Path):
                if destination == release_path and destination.is_dir():
                    if not any(destination.iterdir()):
                        destination.rmdir()
                return real_rename(source, destination)

            with mock.patch.object(
                hermes_release,
                "verify_manifest",
                side_effect=verify_and_create_race,
            ), mock.patch.object(Path, "rename", new=posix_rename):
                with self.assertRaisesRegex(ReleaseError, "appeared"):
                    install_release(request)

            self.assertTrue(race_created)
            self.assertTrue(release_path.is_dir())
            self.assertEqual(list(release_path.iterdir()), [])

    def test_reinstalling_identical_verified_release_is_a_noop(self):
        with install_request_fixture() as request:
            first = install_release(request)
            second = install_release(request)

        self.assertTrue(first.installed)
        self.assertFalse(second.installed)
        self.assertEqual(second.release_path, first.release_path)
        self.assertFalse(second.activated)

    @unittest.skipUnless(os.name == "posix", "POSIX release modes required")
    def test_installed_release_tree_is_non_writable_with_executable_entrypoints(self):
        with install_request_fixture() as request:
            result = install_release(request)

            release = result.release_path
            directories = [path for path in (release, *release.rglob("*")) if path.is_dir()]
            files = [path for path in release.rglob("*") if path.is_file()]

            self.assertTrue(directories)
            for directory in directories:
                self.assertEqual(stat.S_IMODE(directory.stat().st_mode), 0o555, directory)
            for path in files:
                expected = 0o555 if path.name == "anime-cloud-pipeline" or path.suffix == ".sh" else 0o444
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), expected, path)

    def test_install_requires_config_and_env_at_fixed_in_place_paths(self):
        with install_request_fixture() as request:
            outside_config = _write(
                request.install_root.parent, "outside-config.json", "{}\n"
            )
            outside_env = _write(
                request.install_root.parent, "outside.env", "TOKEN=outside\n"
            )
            if os.name == "posix":
                outside_env.chmod(0o600)

            for changed in (
                replace(request, config=outside_config),
                replace(request, env_file=outside_env),
            ):
                with self.subTest(request=changed):
                    with self.assertRaises(ReleaseError):
                        install_release(changed)

    def test_install_rejects_hardlink_aliases_between_config_and_env(self):
        with install_request_fixture() as request:
            request.env_file.unlink()
            try:
                os.link(request.config, request.env_file)
            except OSError as error:
                self.skipTest(f"hard links unavailable: {error}")

            with self.assertRaisesRegex(ReleaseError, "must not alias"):
                install_release(request)

    def test_install_rejects_source_and_install_root_overlap(self):
        with install_request_fixture() as request:
            nested_root = request.source / "nested-install"
            nested_config = _write(
                nested_root, "config/config.v5.json", "{}\n"
            )
            nested_env = _write(
                nested_root,
                "config/anime-cloud-pipeline.env",
                "TOKEN=fixture\n",
            )
            if os.name == "posix":
                nested_env.chmod(0o600)

            with self.assertRaisesRegex(ReleaseError, "must not overlap"):
                install_release(
                    replace(
                        request,
                        install_root=nested_root,
                        config=nested_config,
                        env_file=nested_env,
                    )
                )

    def test_install_rejects_non_private_environment_permissions(self):
        with install_request_fixture() as request:
            request.env_file.chmod(0o644)
            with mock.patch.object(
                hermes_release, "_CHECK_POSIX_PERMISSIONS", True
            ):
                with self.assertRaisesRegex(ReleaseError, "0600 or stricter"):
                    install_release(request)

    def test_install_rejects_an_unrelated_existing_directory(self):
        with install_request_fixture() as request:
            _write(request.install_root, "unrelated.txt")

            with self.assertRaisesRegex(ReleaseError, "unrelated paths"):
                install_release(request)

    def test_install_rejects_invalid_release_ids(self):
        with install_request_fixture() as request:
            for release_id in ("", ".hidden", "../v5", "v5/next", "x" * 65):
                with self.subTest(release_id=release_id):
                    with self.assertRaises(ReleaseError):
                        install_release(replace(request, release_id=release_id))

    def test_install_never_copies_config_or_environment_secrets(self):
        with install_request_fixture() as request:
            _write(request.source, "config/config.v5.json", '{"secret": true}\n')
            _write(
                request.source,
                "config/anime-cloud-pipeline.env",
                "TOKEN=must-not-be-copied\n",
            )
            write_manifest(request.source)

            with self.assertRaisesRegex(ReleaseError, "external config"):
                install_release(request)

            self.assertFalse((request.install_root / "releases").exists())

    def test_failed_install_activation_keeps_current_unchanged_and_hides_output(self):
        with install_request_fixture(activate=True) as request:
            secret = "activation-runner-secret"
            request.env_file.write_text(
                f"P115_COOKIE={secret}\n", encoding="utf-8"
            )
            if os.name == "posix":
                request.env_file.chmod(0o600)
            completed = subprocess.CompletedProcess(
                [],
                1,
                stdout=f'{{"detail":"{secret}"}}\n',
                stderr=f"Authorization: Bearer {secret}\n",
            )

            with mock.patch.object(
                subprocess, "run", return_value=completed
            ) as runner, self.assertRaises(ReleaseError) as raised:
                install_release(request)

            self.assertNotIn(secret, str(raised.exception))
            self.assertTrue(
                (request.install_root / "releases" / request.release_id).is_dir()
            )
            self.assertFalse((request.install_root / "current").exists())
            runner.assert_called_once()
            command = runner.call_args.args[0]
            self.assertEqual(
                command,
                [
                    sys.executable,
                    "-B",
                    str(
                        request.install_root
                        / "releases"
                        / request.release_id
                        / "scripts"
                        / "hermes_runtime.py"
                    ),
                    "--env-file",
                    str(request.env_file),
                    "doctor",
                ],
            )
            self.assertTrue(runner.call_args.kwargs["capture_output"])
            self.assertTrue(runner.call_args.kwargs["text"])
            self.assertFalse(runner.call_args.kwargs["check"])

    def test_install_cli_uses_release_metadata_when_id_is_omitted(self):
        with install_request_fixture(release_id="v5-metadata") as request:
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                code = main(
                    [
                        "install",
                        "--source",
                        str(request.source),
                        "--install-root",
                        str(request.install_root),
                        "--config",
                        str(request.config),
                        "--env-file",
                        str(request.env_file),
                    ]
                )

            payload = json.loads(stdout.getvalue())
            self.assertEqual(code, 0)
            self.assertEqual(
                payload["release_path"],
                str(request.install_root / "releases/v5-metadata"),
            )
            self.assertTrue(payload["installed"])
            self.assertFalse(payload["activated"])

    def test_install_cli_activation_failure_leaves_candidate_installed(self):
        with install_request_fixture() as request:
            completed = subprocess.CompletedProcess(
                [], 1, stdout='{"ok":false}\n', stderr=""
            )
            with mock.patch.object(
                subprocess, "run", return_value=completed
            ), self.assertRaisesRegex(ReleaseError, "activation"):
                main(
                    [
                        "install",
                        "--source",
                        str(request.source),
                        "--install-root",
                        str(request.install_root),
                        "--config",
                        str(request.config),
                        "--env-file",
                        str(request.env_file),
                        "--activate",
                    ]
                )

            self.assertTrue(
                (request.install_root / "releases" / request.release_id).is_dir()
            )
            self.assertFalse((request.install_root / "current").exists())

    def test_install_activation_rejects_an_incomplete_doctor_report(self):
        with install_request_fixture(activate=True) as request:
            completed = subprocess.CompletedProcess(
                [], 0, stdout='{"command":"doctor","ok":true}\n', stderr=""
            )
            with mock.patch.object(
                subprocess, "run", return_value=completed
            ), self.assertRaisesRegex(
                ReleaseError, "validator rejected"
            ):
                install_release(request)

            self.assertTrue(
                (request.install_root / "releases" / request.release_id).is_dir()
            )
            self.assertFalse((request.install_root / "current").exists())

    def test_install_cli_does_not_abbreviate_activate(self):
        with install_request_fixture() as request:
            stderr = io.StringIO()
            with redirect_stderr(stderr), self.assertRaises(SystemExit) as raised:
                main(
                    [
                        "install",
                        "--source",
                        str(request.source),
                        "--install-root",
                        str(request.install_root),
                        "--config",
                        str(request.config),
                        "--env-file",
                        str(request.env_file),
                        "--act",
                    ]
                )

            self.assertEqual(raised.exception.code, 2)
            self.assertIn("unrecognized arguments: --act", stderr.getvalue())
            self.assertFalse((request.install_root / "releases").exists())


class LinkTransactionTests(unittest.TestCase):
    @contextmanager
    def patched_link_backend(
        self,
        topology: dict[str, str | None],
        *,
        fail_on_replacements: set[int],
    ):
        replacement_count = 0

        def read_link(install_root: Path, name: str, *, required: bool):
            release_id = topology[name]
            if release_id is None:
                if required:
                    raise ReleaseError(f"{name} release link is missing")
                return None
            release = hermes_release._verified_release(
                install_root, release_id
            )
            return release_id, release

        def replace_link(
            install_root: Path, name: str, release_id: str
        ) -> None:
            nonlocal replacement_count
            replacement_count += 1
            if replacement_count in fail_on_replacements:
                raise ReleaseError("injected link replacement failure")
            topology[name] = release_id

        with mock.patch.object(
            hermes_release, "_release_link", side_effect=read_link
        ), mock.patch.object(
            hermes_release,
            "_replace_release_link",
            side_effect=replace_link,
        ):
            yield

    def test_activation_second_replace_failure_restores_original_topology(self):
        with installed_release_trees_fixture() as install_root:
            topology = {"current": "v5", "previous": "v4"}
            with self.patched_link_backend(
                topology, fail_on_replacements={2}
            ):
                with self.assertRaises(ReleaseError):
                    activate_release(
                        install_root, "v6", validator=lambda _: True
                    )

            self.assertEqual(
                topology, {"current": "v5", "previous": "v4"}
            )
            self.assertFalse(
                (install_root / "state/.link-transaction.json").exists()
            )

    def test_rollback_retry_recovers_journal_then_completes_swap(self):
        with installed_release_trees_fixture() as install_root:
            topology = {"current": "v6", "previous": "v5"}
            transaction = install_root / "state/.link-transaction.json"
            with self.patched_link_backend(
                topology, fail_on_replacements={2, 3}
            ):
                with self.assertRaisesRegex(ReleaseError, "recovery pending"):
                    rollback_release(install_root)

                self.assertTrue(transaction.is_file())
                self.assertEqual(
                    topology, {"current": "v5", "previous": "v5"}
                )

                rollback_release(install_root)

            self.assertEqual(
                topology, {"current": "v5", "previous": "v6"}
            )
            self.assertFalse(transaction.exists())


class CandidateDoctorTests(unittest.TestCase):
    def test_real_candidate_doctor_rejects_missing_remote_resources_without_mutation(self):
        caller_environment = {
            name: value
            for name, value in os.environ.items()
            if name not in {"PYTHONDONTWRITEBYTECODE", "PYTHONPYCACHEPREFIX"}
        }
        with real_candidate_doctor_fixture() as (install_root, release, env_file):
            before = _snapshot_tree(install_root)

            with mock.patch.dict(os.environ, caller_environment, clear=True):
                self.assertNotIn("PYTHONDONTWRITEBYTECODE", os.environ)
                accepted = hermes_release._candidate_doctor_ok(release, env_file)

            after = _snapshot_tree(install_root)

        self.assertEqual(after, before)
        self.assertFalse(
            any(
                "__pycache__" in relative.split("/")
                or relative.endswith(".pyc")
                for relative in after
            ),
            sorted(after),
        )
        self.assertFalse(accepted)


class ActivationTests(unittest.TestCase):
    def setUp(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "target"
            target.mkdir()
            probe = root / "probe"
            try:
                probe.symlink_to(target, target_is_directory=True)
            except (NotImplementedError, OSError) as error:
                self.skipTest(f"directory symlinks unavailable: {error}")
            probe.unlink()

    def test_failed_validator_leaves_current_unchanged(self):
        with installed_two_release_fixture() as install_root:
            before = os.readlink(install_root / "current")
            with self.assertRaises(ReleaseError):
                activate_release(
                    install_root, "v6", validator=lambda _: False
                )
            self.assertEqual(os.readlink(install_root / "current"), before)
            self.assertFalse((install_root / "previous").exists())

    def test_install_activate_switches_link_only_after_candidate_doctor_succeeds(self):
        with install_request_fixture(activate=True) as request:
            completed = subprocess.CompletedProcess(
                [], 0, stdout=_successful_doctor_output(), stderr=""
            )
            with mock.patch.object(
                subprocess, "run", return_value=completed
            ) as runner:
                result = install_release(request)

            self.assertTrue(result.installed)
            self.assertTrue(result.activated)
            self.assertIsNone(result.previous_release)
            self.assertEqual(
                (request.install_root / "current").resolve(), result.release_path
            )
            self.assertFalse((result.release_path / "config").exists())
            runner.assert_called_once_with(
                [
                    sys.executable,
                    "-B",
                    str(result.release_path / "scripts" / "hermes_runtime.py"),
                    "--env-file",
                    str(request.env_file),
                    "doctor",
                ],
                check=False,
                capture_output=True,
                text=True,
                env=mock.ANY,
            )
            child_environment = runner.call_args.kwargs["env"]
            self.assertNotIn("PYTHONPATH", child_environment)
            self.assertNotIn("PYTHONHOME", child_environment)
            self.assertNotIn("LD_PRELOAD", child_environment)

    def test_install_cli_activate_reports_the_link_transition(self):
        with install_request_fixture() as request:
            completed = subprocess.CompletedProcess(
                [], 0, stdout=_successful_doctor_output(), stderr=""
            )
            stdout = io.StringIO()
            with mock.patch.object(
                subprocess, "run", return_value=completed
            ), redirect_stdout(stdout):
                code = main(
                    [
                        "install",
                        "--source",
                        str(request.source),
                        "--install-root",
                        str(request.install_root),
                        "--config",
                        str(request.config),
                        "--env-file",
                        str(request.env_file),
                        "--activate",
                    ]
                )

            payload = json.loads(stdout.getvalue())
            self.assertEqual(code, 0)
            self.assertTrue(payload["activated"])
            self.assertTrue(payload["installed"])
            self.assertIsNone(payload["previous_release"])
            self.assertEqual(
                (request.install_root / "current").resolve(),
                request.install_root / "releases" / request.release_id,
            )

    def test_activation_requires_an_injected_validator(self):
        with installed_two_release_fixture() as install_root:
            before = os.readlink(install_root / "current")
            with self.assertRaises(TypeError):
                activate_release(install_root, "v6")
            self.assertEqual(os.readlink(install_root / "current"), before)

    def test_upgrade_preserves_previous_and_rollback_swaps_links(self):
        with installed_two_release_fixture(active="v5") as install_root:
            activate_release(install_root, "v6", validator=lambda _: True)
            self.assertEqual((install_root / "current").resolve().name, "v6")
            self.assertEqual((install_root / "previous").resolve().name, "v5")

            rollback_release(install_root)

            self.assertEqual((install_root / "current").resolve().name, "v5")
            self.assertEqual((install_root / "previous").resolve().name, "v6")
            self.assertEqual(
                os.readlink(install_root / "current"),
                os.fspath(Path("releases/v5")),
            )
            self.assertEqual(
                os.readlink(install_root / "previous"),
                os.fspath(Path("releases/v6")),
            )

    def test_rollback_rejects_missing_or_unverified_links_without_changes(self):
        with installed_two_release_fixture(active="v5") as install_root:
            before = os.readlink(install_root / "current")
            with self.assertRaises(ReleaseError):
                rollback_release(install_root)
            self.assertEqual(os.readlink(install_root / "current"), before)

            (install_root / "previous").symlink_to(
                "releases/v6", target_is_directory=True
            )
            _write(install_root / "releases/v6", "SKILL.md", "corrupt\n")
            with self.assertRaises(ReleaseError):
                rollback_release(install_root)
            self.assertEqual(os.readlink(install_root / "current"), before)


if __name__ == "__main__":
    unittest.main()
