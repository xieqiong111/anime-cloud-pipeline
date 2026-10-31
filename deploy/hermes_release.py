from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
from contextlib import contextmanager
import ctypes
from dataclasses import dataclass
import errno
import hashlib
import json
import os
from pathlib import Path
from pathlib import PurePosixPath, PureWindowsPath
import re
import shutil
import stat
import subprocess
import sys
import tempfile
from typing import Iterator
import zipfile


MANIFEST_NAME = "MANIFEST.sha256"
PRODUCT_FILES = ("SKILL.md", "CHANGELOG-V5.md")
PRODUCT_TREES = ("assets", "scripts", "references", "deploy")
PRODUCT_DOCS = ("docs/V5-UPGRADE.md",)
FORBIDDEN_PARTS = {".git", ".superpowers", "__pycache__", "dist", "build"}
FORBIDDEN_SUFFIXES = (".pyc", ".db", ".db-wal", ".db-shm", ".log", ".zip")
FORBIDDEN_LIVE_FILES = {
    "deploy/anime-cloud-pipeline.env",
    "deploy/config.v5.json",
}
_MANIFEST_LINE = re.compile(r"([0-9a-f]{64})  (.+)")
_RELEASE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
_ARCHIVE_ROOT = "anime-cloud-pipeline-hermes"
_ZIP_TIMESTAMP = (1980, 1, 1, 0, 0, 0)
_CHECK_POSIX_PERMISSIONS = os.name == "posix"
_LINK_NAMES = ("current", "previous")
_LINK_TRANSACTION_NAME = ".link-transaction.json"
_LINK_LOCK_NAME = ".link-operation.lock"
_ACTIVATION_DIAGNOSTICS = frozenset(
    {
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
    }
)


class ReleaseError(RuntimeError):
    """Raised when a release tree cannot be safely processed."""


@dataclass(frozen=True)
class ManifestReport:
    missing: tuple[str, ...] = ()
    changed: tuple[str, ...] = ()
    unexpected: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return not (self.missing or self.changed or self.unexpected)


@dataclass(frozen=True)
class ArchiveResult:
    output: Path
    release_id: str
    source_revision: str | None
    sha256: str
    file_count: int


@dataclass(frozen=True)
class InstallRequest:
    source: Path
    install_root: Path
    config: Path
    env_file: Path
    release_id: str
    activate: bool = False


@dataclass(frozen=True)
class InstallResult:
    release_path: Path
    installed: bool
    activated: bool
    previous_release: str | None


def _is_allowed(relative: Path) -> bool:
    path = relative.as_posix()
    return not (
        any(part in FORBIDDEN_PARTS for part in relative.parts)
        or path.endswith(FORBIDDEN_SUFFIXES)
        or path.casefold() in FORBIDDEN_LIVE_FILES
    )


def iter_release_files(root: Path) -> Iterator[Path]:
    root = Path(root)
    if _is_link(root) or not root.is_dir():
        raise ReleaseError(f"source root is not a regular directory: {root}")
    selected: set[Path] = set()
    for name in (*PRODUCT_FILES, *PRODUCT_DOCS):
        relative = Path(name)
        path = _explicit_source_file(root, relative)
        if path is not None and _is_allowed(relative):
            selected.add(relative)
    for tree_name in PRODUCT_TREES:
        tree = root / tree_name
        if _is_link(tree):
            raise ReleaseError(f"release source tree must not be a link: {tree_name}")
        if not tree.is_dir():
            continue
        pending = [tree]
        while pending:
            directory = pending.pop()
            try:
                children = sorted(directory.iterdir(), key=lambda path: path.name)
            except OSError as error:
                raise ReleaseError(
                    f"cannot inspect release source directory: {directory}"
                ) from error
            for path in children:
                relative = path.relative_to(root)
                if not _is_allowed(relative):
                    continue
                if _is_link(path):
                    raise ReleaseError(
                        "release sources must not contain links: "
                        f"{relative.as_posix()}"
                    )
                try:
                    mode = path.lstat().st_mode
                except OSError as error:
                    raise ReleaseError(
                        f"cannot inspect release source path: {relative.as_posix()}"
                    ) from error
                if stat.S_ISDIR(mode):
                    pending.append(path)
                elif stat.S_ISREG(mode):
                    selected.add(relative)
                else:
                    raise ReleaseError(
                        "release sources must contain only regular files: "
                        f"{relative.as_posix()}"
                    )
    yield from sorted(selected, key=lambda path: path.as_posix())


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_link(path: Path) -> bool:
    try:
        status = path.lstat()
    except FileNotFoundError:
        return False
    except OSError:
        return path.is_symlink()
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    attributes = getattr(status, "st_file_attributes", 0)
    is_junction = getattr(os.path, "isjunction", None)
    return bool(
        stat.S_ISLNK(status.st_mode)
        or (reparse_flag and attributes & reparse_flag)
        or (is_junction and is_junction(path))
    )


def _explicit_source_file(root: Path, relative: Path) -> Path | None:
    current = root
    for index, part in enumerate(relative.parts):
        current /= part
        if _is_link(current):
            raise ReleaseError(
                "release sources must not contain linked path components: "
                f"{relative.as_posix()}"
            )
        try:
            mode = current.lstat().st_mode
        except FileNotFoundError:
            return None
        except OSError as error:
            raise ReleaseError(
                f"cannot inspect release source path: {relative.as_posix()}"
            ) from error
        is_leaf = index == len(relative.parts) - 1
        if not is_leaf and not stat.S_ISDIR(mode):
            raise ReleaseError(
                f"release source parent is not a directory: {relative.as_posix()}"
            )
        if is_leaf and not stat.S_ISREG(mode):
            raise ReleaseError(
                f"release source is not a regular file: {relative.as_posix()}"
            )
    return current


def _tree_files(root: Path) -> dict[str, Path]:
    if _is_link(root) or not root.is_dir():
        raise ReleaseError(f"release root is not a regular directory: {root}")

    files: dict[str, Path] = {}
    pending = [root]
    while pending:
        directory = pending.pop()
        try:
            children = sorted(directory.iterdir(), key=lambda path: path.name)
        except OSError as error:
            raise ReleaseError(f"cannot inspect release directory: {directory}") from error
        for path in children:
            relative = path.relative_to(root)
            if _is_link(path):
                raise ReleaseError(
                    f"release trees must not contain symlinks: {relative.as_posix()}"
                )
            try:
                mode = path.lstat().st_mode
            except OSError as error:
                raise ReleaseError(f"cannot inspect release path: {relative}") from error
            name = relative.as_posix()
            if name != MANIFEST_NAME and not _is_allowed(relative):
                raise ReleaseError(f"forbidden release path: {name}")
            if stat.S_ISDIR(mode):
                pending.append(path)
            elif stat.S_ISREG(mode):
                if name != MANIFEST_NAME:
                    files[name] = path
            else:
                raise ReleaseError(
                    f"release trees must contain only regular files: {relative.as_posix()}"
                )
    return files


def _parse_manifest(manifest: Path) -> dict[str, str]:
    if _is_link(manifest):
        raise ReleaseError(f"manifest must not be a symlink: {manifest}")
    try:
        mode = manifest.lstat().st_mode
    except FileNotFoundError as error:
        raise ReleaseError(f"manifest is missing: {manifest}") from error
    except OSError as error:
        raise ReleaseError(f"cannot inspect manifest: {manifest}") from error
    if not stat.S_ISREG(mode):
        raise ReleaseError(f"manifest is not a regular file: {manifest}")
    try:
        text = manifest.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise ReleaseError(f"cannot read manifest: {manifest}") from error

    entries: dict[str, str] = {}
    for line_number, line in enumerate(text.splitlines(), start=1):
        match = _MANIFEST_LINE.fullmatch(line)
        if match is None:
            raise ReleaseError(f"invalid manifest line {line_number}")
        digest, name = match.groups()
        relative = PurePosixPath(name)
        windows = PureWindowsPath(name)
        if (
            relative.is_absolute()
            or windows.is_absolute()
            or bool(windows.drive)
            or "\\" in name
            or relative.as_posix() != name
            or ".." in relative.parts
            or name == MANIFEST_NAME
        ):
            raise ReleaseError(f"unsafe manifest path on line {line_number}: {name}")
        if name in entries:
            raise ReleaseError(f"duplicate manifest path on line {line_number}: {name}")
        entries[name] = digest
    return entries


def write_manifest(root: Path) -> Path:
    root = Path(root)
    files = _tree_files(root)
    lines = [f"{_sha256(files[name])}  {name}" for name in sorted(files)]
    manifest = root / MANIFEST_NAME
    manifest.write_bytes("".join(f"{line}\n" for line in lines).encode("utf-8"))
    return manifest


def verify_manifest(root: Path) -> ManifestReport:
    root = Path(root)
    manifest = root / MANIFEST_NAME
    expected = _parse_manifest(manifest)

    actual = _tree_files(root)
    missing = tuple(sorted(set(expected) - set(actual)))
    unexpected = tuple(sorted(set(actual) - set(expected)))
    changed = tuple(
        name
        for name in sorted(set(expected) & set(actual))
        if _sha256(actual[name]) != expected[name]
    )
    return ManifestReport(missing=missing, changed=changed, unexpected=unexpected)


def _absolute_normalized_path(path: Path, description: str) -> Path:
    path = Path(path)
    if not path.is_absolute():
        raise ReleaseError(f"{description} must be an absolute path")
    normalized = path.resolve(strict=False)
    if path != normalized:
        raise ReleaseError(f"{description} must be normalized")
    return normalized


def validate_install_root(install_root: Path) -> Path:
    """Return a normalized install root after rejecting broad unsafe targets."""

    install_root = _absolute_normalized_path(install_root, "install root")
    if install_root == Path(install_root.anchor):
        raise ReleaseError("install root must not be a filesystem root")
    if install_root == Path.home().resolve(strict=False):
        raise ReleaseError("install root must not be the home directory")
    if _is_link(install_root):
        raise ReleaseError("install root must not be a link")
    if install_root.exists() and not install_root.is_dir():
        raise ReleaseError("install root must be a directory")
    return install_root


def _require_regular_file(path: Path, description: str) -> Path:
    path = _absolute_normalized_path(path, description)
    if _is_link(path):
        raise ReleaseError(f"{description} must not be a link")
    try:
        mode = path.lstat().st_mode
    except OSError as error:
        raise ReleaseError(f"cannot inspect {description}") from error
    if not stat.S_ISREG(mode):
        raise ReleaseError(f"{description} must be a regular file")
    return path


def _require_directory(path: Path, description: str) -> Path:
    path = _absolute_normalized_path(path, description)
    if _is_link(path):
        raise ReleaseError(f"{description} must not be a link")
    try:
        mode = path.lstat().st_mode
    except OSError as error:
        raise ReleaseError(f"cannot inspect {description}") from error
    if not stat.S_ISDIR(mode):
        raise ReleaseError(f"{description} must be a directory")
    return path


def _is_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _same_file(first: Path, second: Path) -> bool:
    try:
        return os.path.samefile(first, second)
    except OSError:
        return False


def _validate_install_request(
    request: InstallRequest,
) -> tuple[Path, Path, Path, Path]:
    if _RELEASE_ID.fullmatch(request.release_id) is None:
        raise ReleaseError(
            "release ID must match [A-Za-z0-9][A-Za-z0-9._-]{0,63}"
        )

    install_root = validate_install_root(request.install_root)
    source = _require_directory(request.source, "release source")
    config_dir = install_root / "config"
    config = _require_regular_file(request.config, "configuration file")
    env_file = _require_regular_file(request.env_file, "environment file")
    if config != config_dir / "config.v5.json":
        raise ReleaseError(
            "configuration file must be <install-root>/config/config.v5.json"
        )
    if env_file != config_dir / "anime-cloud-pipeline.env":
        raise ReleaseError(
            "environment file must be "
            "<install-root>/config/anime-cloud-pipeline.env"
        )
    _require_directory(config_dir, "configuration directory")
    if _same_file(config, env_file):
        raise ReleaseError("configuration and environment files must not alias")
    if (
        _CHECK_POSIX_PERMISSIONS
        and stat.S_IMODE(env_file.lstat().st_mode) & 0o077
    ):
        raise ReleaseError("environment file permissions must be 0600 or stricter")
    if _is_within(source, install_root) or _is_within(install_root, source):
        raise ReleaseError("release source and install root must not overlap")
    if any(
        PurePosixPath(name).parts[0] == "config"
        for name in _tree_files(source)
    ):
        raise ReleaseError(
            "release source must not contain external config or env files"
        )

    source_report = verify_manifest(source)
    if not source_report.ok:
        raise ReleaseError(f"release source did not verify: {source_report}")
    return source, install_root, config, env_file


def _prepare_install_layout(install_root: Path) -> Path:
    allowed = {
        "backups",
        "config",
        "current",
        "logs",
        "previous",
        "releases",
        "state",
    }
    try:
        unexpected = sorted(
            path.name
            for path in install_root.iterdir()
            if path.name not in allowed
        )
    except OSError as error:
        raise ReleaseError("cannot inspect install root") from error
    if unexpected:
        raise ReleaseError(
            "install root contains unrelated paths: " + ", ".join(unexpected)
        )

    layout_directories = [
        install_root / "releases",
        install_root / "state",
        install_root / "logs",
        install_root / "backups",
    ]
    for directory in layout_directories:
        try:
            directory.mkdir(exist_ok=True)
        except OSError as error:
            raise ReleaseError(
                f"cannot create deployment directory: {directory}"
            ) from error
        _require_directory(directory, f"deployment directory {directory.name}")

    for index, first in enumerate(layout_directories):
        for second in layout_directories[index + 1 :]:
            if _same_file(first, second):
                raise ReleaseError("deployment directories must not alias")
    return layout_directories[0]


def _remove_owned_staging(staging: Path, releases_root: Path) -> None:
    try:
        mode = staging.lstat().st_mode
    except FileNotFoundError:
        return
    except OSError as error:
        raise ReleaseError("cannot inspect staging directory") from error
    if (
        staging.parent != releases_root
        or not staging.name.startswith(".staging-")
        or _is_link(staging)
        or not stat.S_ISDIR(mode)
    ):
        raise ReleaseError("refusing to remove an unsafe staging path")
    _set_release_permissions(staging, immutable=False)
    shutil.rmtree(staging)


def _set_release_permissions(root: Path, *, immutable: bool) -> None:
    """Set release modes; POSIX ownership remains an operator concern."""

    if os.name != "posix":
        # Windows has no portable owner-mode equivalent.  The archive carries
        # read/execute mode metadata, while native ACL verification is a target
        # deployment check rather than a claim made by this installer.
        return

    root = Path(root)
    files = _tree_files(root)
    manifest = root / MANIFEST_NAME
    if _is_link(manifest):
        raise ReleaseError("release manifest must be a regular file")
    if manifest.is_file():
        files[MANIFEST_NAME] = manifest
    executable_names = {
        name for name in files if name == "bin/anime-cloud-pipeline" or name.endswith(".sh")
    }
    for name, path in files.items():
        mode = (
            0o555
            if immutable and name in executable_names
            else 0o444
            if immutable
            else 0o755
            if name in executable_names
            else 0o644
        )
        path.chmod(mode)

    directories = [path for path in (root, *root.rglob("*")) if path.is_dir()]
    for directory in directories:
        directory.chmod(0o555 if immutable else 0o755)


def _chmod_packaged_entrypoints(root: Path) -> None:
    _set_release_permissions(root, immutable=True)


def _manifests_match(first: Path, second: Path) -> bool:
    try:
        return (first / MANIFEST_NAME).read_bytes() == (
            second / MANIFEST_NAME
        ).read_bytes()
    except OSError as error:
        raise ReleaseError("cannot compare release manifests") from error


def _publish_release_no_replace(staging: Path, release_path: Path) -> None:
    """Atomically publish a directory without replacing any destination."""

    if os.name == "nt":
        os.rename(staging, release_path)
        return
    if os.name != "posix" or not sys.platform.startswith("linux"):
        raise ReleaseError(
            "atomic no-replace release publishing is unavailable"
        )

    try:
        renameat2 = ctypes.CDLL(None, use_errno=True).renameat2
    except (AttributeError, OSError) as error:
        raise ReleaseError(
            "atomic no-replace release publishing is unavailable"
        ) from error
    renameat2.argtypes = (
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    )
    renameat2.restype = ctypes.c_int
    result = renameat2(
        -100,
        os.fsencode(staging),
        -100,
        os.fsencode(release_path),
        1,
    )
    if result == 0:
        return
    error_number = ctypes.get_errno()
    if error_number in {errno.EEXIST, errno.ENOTEMPTY}:
        raise FileExistsError(
            error_number, os.strerror(error_number), release_path
        )
    if error_number in {errno.ENOSYS, errno.EINVAL, errno.ENOTSUP}:
        raise ReleaseError(
            "atomic no-replace release publishing is unavailable"
        )
    raise OSError(error_number, os.strerror(error_number), release_path)


def _candidate_doctor_ok(release: Path, env_file: Path) -> bool:
    try:
        runtime = _require_regular_file(
            release / "scripts" / "hermes_runtime.py",
            "candidate Hermes runtime",
        )
    except ReleaseError:
        return False
    command = [
        sys.executable,
        "-B",
        str(runtime),
        "--env-file",
        str(env_file),
        "doctor",
    ]
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            # The candidate reads its restricted env file.  Do not inherit
            # ambient Python loader/runtime controls into the bootstrap.
            env={},
        )
    except (OSError, subprocess.SubprocessError):
        return False
    if completed.returncode != 0:
        return False
    try:
        payload = json.loads(completed.stdout)
    except (TypeError, json.JSONDecodeError):
        return False
    if (
        not isinstance(payload, dict)
        or payload.get("command") != "doctor"
        or payload.get("ok") is not True
        or payload.get("failed_codes") != []
        or not isinstance(payload.get("diagnostics"), list)
    ):
        return False
    diagnostics = payload["diagnostics"]
    if any(
        not isinstance(item, dict)
        or not isinstance(item.get("code"), str)
        or item.get("ok") is not True
        for item in diagnostics
    ):
        return False
    codes = [item["code"] for item in diagnostics]
    return len(codes) == len(set(codes)) and _ACTIVATION_DIAGNOSTICS.issubset(codes)


def install_release(request: InstallRequest) -> InstallResult:
    """Install one verified release and optionally health-gate activation."""

    source, install_root, _, _ = _validate_install_request(request)
    releases_root = _prepare_install_layout(install_root)
    release_path = releases_root / request.release_id

    if release_path.exists() or _is_link(release_path):
        if _is_link(release_path) or not release_path.is_dir():
            raise ReleaseError("existing release path is not a regular directory")
        existing_report = verify_manifest(release_path)
        if existing_report.ok and _manifests_match(source, release_path):
            installed = False
        else:
            raise ReleaseError("refusing to overwrite an existing release")
    else:
        prefix = f".staging-{request.release_id}-"
        staging = Path(tempfile.mkdtemp(prefix=prefix, dir=releases_root))
        installed = False
        try:
            for name, source_file in _tree_files(source).items():
                destination = staging / Path(name)
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source_file, destination)
            shutil.copyfile(source / MANIFEST_NAME, staging / MANIFEST_NAME)
            _chmod_packaged_entrypoints(staging)
            staged_report = verify_manifest(staging)
            if not staged_report.ok:
                raise ReleaseError(f"staged release did not verify: {staged_report}")
            try:
                _publish_release_no_replace(staging, release_path)
            except FileExistsError as error:
                raise ReleaseError("release path appeared during installation") from error
            except OSError as error:
                raise ReleaseError("cannot publish staged release") from error
            installed = True
        finally:
            if not installed:
                _remove_owned_staging(staging, releases_root)

    activated = False
    previous_release = None
    if request.activate:
        previous_release = activate_release(
            install_root,
            request.release_id,
            validator=lambda candidate: _candidate_doctor_ok(candidate, request.env_file),
        )
        activated = True

    return InstallResult(
        release_path=release_path,
        installed=installed,
        activated=activated,
        previous_release=previous_release,
    )


def _verified_release(install_root: Path, release_id: str) -> Path:
    if _RELEASE_ID.fullmatch(release_id) is None:
        raise ReleaseError("invalid release ID")
    releases_root = _require_directory(
        install_root / "releases", "releases directory"
    )
    release = _require_directory(
        releases_root / release_id, f"release {release_id}"
    )
    report = verify_manifest(release)
    if not report.ok:
        raise ReleaseError(f"release {release_id} did not verify: {report}")
    return release


def _release_link(
    install_root: Path, name: str, *, required: bool
) -> tuple[str, Path] | None:
    link = install_root / name
    try:
        mode = link.lstat().st_mode
    except FileNotFoundError:
        if required:
            raise ReleaseError(f"{name} release link is missing")
        return None
    except OSError as error:
        raise ReleaseError(f"cannot inspect {name} release link") from error
    if not stat.S_ISLNK(mode):
        raise ReleaseError(f"{name} must be a symbolic link")
    try:
        target = Path(os.readlink(link))
    except OSError as error:
        raise ReleaseError(f"cannot read {name} release link") from error
    if target.is_absolute() or len(target.parts) != 2 or target.parts[0] != "releases":
        raise ReleaseError(f"{name} must point inside releases/")
    release_id = target.parts[1]
    release = _verified_release(install_root, release_id)
    return release_id, release


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        return
    try:
        try:
            os.fsync(descriptor)
        except OSError:
            pass
    finally:
        os.close(descriptor)


def _replace_release_link(install_root: Path, name: str, release_id: str) -> None:
    temporary_directory = Path(
        tempfile.mkdtemp(prefix=f".link-{name}-", dir=install_root)
    )
    temporary_link = temporary_directory / name
    destination = install_root / name
    try:
        temporary_link.symlink_to(
            Path("releases") / release_id, target_is_directory=True
        )
        os.replace(temporary_link, destination)
        _fsync_directory(install_root)
    except OSError as error:
        raise ReleaseError(f"cannot replace {name} release link") from error
    finally:
        try:
            temporary_link.unlink()
        except FileNotFoundError:
            pass
        try:
            temporary_directory.rmdir()
        except FileNotFoundError:
            pass


def _link_state_directory(install_root: Path, *, create: bool) -> Path | None:
    state_directory = install_root / "state"
    if not state_directory.exists() and not _is_link(state_directory):
        if not create:
            return None
        try:
            state_directory.mkdir()
        except FileExistsError:
            pass
        except OSError as error:
            raise ReleaseError("cannot create link transaction state") from error
    return _require_directory(state_directory, "link transaction state")


@contextmanager
def _link_operation_lock(install_root: Path):
    state_directory = _link_state_directory(install_root, create=True)
    assert state_directory is not None
    lock_path = state_directory / _LINK_LOCK_NAME
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except OSError as error:
        raise ReleaseError("cannot open link operation lock") from error

    platform = os.name
    try:
        if platform == "posix":
            import fcntl

            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        elif platform == "nt":
            import msvcrt

            if os.fstat(descriptor).st_size == 0:
                os.write(descriptor, b"\0")
                os.fsync(descriptor)
            os.lseek(descriptor, 0, os.SEEK_SET)
            msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
        else:
            raise ReleaseError("link operation locking is unavailable")
    except (OSError, ImportError) as error:
        os.close(descriptor)
        raise ReleaseError("another link operation is in progress") from error

    try:
        yield
    finally:
        try:
            if platform == "posix":
                import fcntl

                fcntl.flock(descriptor, fcntl.LOCK_UN)
            elif platform == "nt":
                import msvcrt

                os.lseek(descriptor, 0, os.SEEK_SET)
                msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
        finally:
            os.close(descriptor)


def _validate_link_topology(value: object, description: str) -> dict[str, str | None]:
    if not isinstance(value, dict) or set(value) != set(_LINK_NAMES):
        raise ReleaseError(f"invalid {description} link topology")
    topology: dict[str, str | None] = {}
    for name in _LINK_NAMES:
        release_id = value[name]
        if release_id is not None and (
            not isinstance(release_id, str)
            or _RELEASE_ID.fullmatch(release_id) is None
        ):
            raise ReleaseError(f"invalid {description} link topology")
        topology[name] = release_id
    return topology


def _transaction_payload(
    operation: str,
    before: dict[str, str | None],
    after: dict[str, str | None],
) -> dict[str, object]:
    if operation not in {"activate", "rollback"}:
        raise ReleaseError("invalid link transaction operation")
    return {
        "after": _validate_link_topology(after, "after"),
        "before": _validate_link_topology(before, "before"),
        "operation": operation,
        "schema_version": 1,
    }


def _write_link_transaction(
    install_root: Path,
    operation: str,
    before: dict[str, str | None],
    after: dict[str, str | None],
) -> Path:
    state_directory = _link_state_directory(install_root, create=True)
    assert state_directory is not None
    transaction = state_directory / _LINK_TRANSACTION_NAME
    payload = _transaction_payload(operation, before, after)
    encoded = (json.dumps(payload, sort_keys=True) + "\n").encode("utf-8")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".link-transaction-", dir=state_directory
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, transaction)
        except FileExistsError as error:
            raise ReleaseError("a pending link transaction already exists") from error
        _fsync_directory(state_directory)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    return transaction


def _read_link_transaction(install_root: Path) -> dict[str, object] | None:
    state_directory = _link_state_directory(install_root, create=False)
    if state_directory is None:
        return None
    transaction = state_directory / _LINK_TRANSACTION_NAME
    if not transaction.exists() and not _is_link(transaction):
        return None
    _require_regular_file(transaction, "link transaction journal")
    try:
        payload = json.loads(transaction.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ReleaseError("cannot read link transaction journal") from error
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != 1
        or set(payload) != {"after", "before", "operation", "schema_version"}
        or payload.get("operation") not in {"activate", "rollback"}
    ):
        raise ReleaseError("invalid link transaction journal")
    return _transaction_payload(
        payload["operation"],
        _validate_link_topology(payload["before"], "before"),
        _validate_link_topology(payload["after"], "after"),
    )


def _remove_link_transaction(install_root: Path) -> None:
    state_directory = _link_state_directory(install_root, create=False)
    if state_directory is None:
        return
    transaction = state_directory / _LINK_TRANSACTION_NAME
    try:
        transaction.unlink()
    except FileNotFoundError:
        return
    except OSError as error:
        raise ReleaseError("cannot remove link transaction journal") from error
    _fsync_directory(state_directory)


def _remove_release_link(install_root: Path, name: str) -> None:
    existing = _release_link(install_root, name, required=False)
    if existing is None:
        return
    try:
        (install_root / name).unlink()
    except OSError as error:
        raise ReleaseError(f"cannot remove {name} release link") from error
    _fsync_directory(install_root)


def _apply_link_topology(
    install_root: Path, topology: dict[str, str | None]
) -> None:
    topology = _validate_link_topology(topology, "requested")
    for name in _LINK_NAMES:
        desired = topology[name]
        existing = _release_link(install_root, name, required=False)
        existing_id = None if existing is None else existing[0]
        if existing_id == desired:
            continue
        if desired is None:
            _remove_release_link(install_root, name)
        else:
            _verified_release(install_root, desired)
            _replace_release_link(install_root, name, desired)


def _recover_link_transaction(install_root: Path) -> None:
    payload = _read_link_transaction(install_root)
    if payload is None:
        return
    try:
        _apply_link_topology(
            install_root,
            _validate_link_topology(payload["before"], "before"),
        )
        _remove_link_transaction(install_root)
    except Exception as error:
        raise ReleaseError("pending link transaction recovery failed") from error


def _run_link_transaction(
    install_root: Path,
    operation: str,
    before: dict[str, str | None],
    after: dict[str, str | None],
) -> None:
    _write_link_transaction(install_root, operation, before, after)
    try:
        _apply_link_topology(install_root, after)
        _remove_link_transaction(install_root)
    except Exception as error:
        try:
            _apply_link_topology(install_root, before)
            _remove_link_transaction(install_root)
        except Exception:
            raise ReleaseError(
                "link transaction failed; recovery pending"
            ) from error
        raise ReleaseError(
            "link transaction failed; original topology restored"
        ) from error


def activate_release(
    install_root: Path,
    release_id: str,
    *,
    validator: Callable[[Path], bool],
) -> str | None:
    """Atomically activate a verified release after an injected health gate."""

    install_root = validate_install_root(install_root)
    _require_directory(install_root, "install root")
    release = _verified_release(install_root, release_id)
    if not callable(validator):
        raise ReleaseError("activation requires a validator")
    try:
        valid = validator(release)
    except Exception as error:
        raise ReleaseError("release activation validator failed") from error
    if valid is not True:
        raise ReleaseError("release activation validator rejected the release")

    with _link_operation_lock(install_root):
        _recover_link_transaction(install_root)
        current = _release_link(install_root, "current", required=False)
        previous = _release_link(install_root, "previous", required=False)
        if current is not None and current[0] == release_id:
            return current[0]
        before = {
            "current": None if current is None else current[0],
            "previous": None if previous is None else previous[0],
        }
        after = {
            "current": release_id,
            "previous": before["previous"] if current is None else current[0],
        }
        _run_link_transaction(install_root, "activate", before, after)
        return before["current"]


def rollback_release(install_root: Path) -> tuple[str, str]:
    """Swap current and previous after verifying both release trees."""

    install_root = validate_install_root(install_root)
    _require_directory(install_root, "install root")
    with _link_operation_lock(install_root):
        _recover_link_transaction(install_root)
        current = _release_link(install_root, "current", required=True)
        previous = _release_link(install_root, "previous", required=True)
        if current is None or previous is None:
            raise ReleaseError("current and previous release links are required")
        if current[0] == previous[0]:
            raise ReleaseError(
                "current and previous must identify distinct releases"
            )
        before = {"current": current[0], "previous": previous[0]}
        after = {"current": previous[0], "previous": current[0]}
        _run_link_transaction(install_root, "rollback", before, after)
        return previous[0], current[0]


def _release_id_from_source(source: Path) -> str:
    source = _require_directory(source, "release source")
    metadata_path = _require_regular_file(
        source / "RELEASE.json", "release metadata"
    )
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ReleaseError("cannot read release metadata") from error
    release_id = metadata.get("release_id") if isinstance(metadata, dict) else None
    if not isinstance(release_id, str) or _RELEASE_ID.fullmatch(release_id) is None:
        raise ReleaseError("release metadata contains an invalid release ID")
    return release_id


def _write_zip(source: Path, output: Path) -> int:
    files = _tree_files(source)
    manifest = source / MANIFEST_NAME
    files[MANIFEST_NAME] = manifest
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_STORED) as archive:
        for name in sorted(files):
            archive_name = f"{_ARCHIVE_ROOT}/{name}"
            info = zipfile.ZipInfo(archive_name, date_time=_ZIP_TIMESTAMP)
            info.create_system = 3
            executable = name == "bin/anime-cloud-pipeline" or name.endswith(".sh")
            permissions = 0o555 if executable else 0o444
            info.external_attr = (stat.S_IFREG | permissions) << 16
            info.compress_type = zipfile.ZIP_STORED
            archive.writestr(info, files[name].read_bytes())
    return len(files)


def build_archive(
    source: Path,
    output: Path,
    release_id: str,
    source_revision: str | None = None,
) -> ArchiveResult:
    source = Path(source)
    output = Path(output)
    if not release_id:
        raise ReleaseError("release ID must not be empty")

    wrapper_source = source / "scripts" / "hermes_anime.sh"
    if not wrapper_source.is_file():
        raise ReleaseError(f"release wrapper is missing: {wrapper_source}")

    with tempfile.TemporaryDirectory(prefix="hermes-release-") as tmp:
        staged = Path(tmp, _ARCHIVE_ROOT)
        staged.mkdir()
        for relative in iter_release_files(source):
            destination = staged / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            source_bytes = (source / relative).read_bytes()
            if relative.suffix == ".sh":
                # POSIX entrypoints must remain runnable from Windows checkouts.
                source_bytes = source_bytes.replace(b"\r\n", b"\n")
            destination.write_bytes(source_bytes)

        installed_wrapper = staged / "bin" / "anime-cloud-pipeline"
        installed_wrapper.parent.mkdir(parents=True, exist_ok=True)
        installed_wrapper.write_bytes(wrapper_source.read_bytes().replace(b"\r\n", b"\n"))

        metadata = {
            "release_id": release_id,
            "schema_version": 1,
            "source_revision": source_revision,
        }
        (staged / "RELEASE.json").write_bytes(
            (json.dumps(metadata, indent=2, sort_keys=True) + "\n").encode("utf-8")
        )
        write_manifest(staged)
        report = verify_manifest(staged)
        if not report.ok:
            raise ReleaseError(f"staged release did not verify: {report}")

        output.parent.mkdir(parents=True, exist_ok=True)
        file_count = _write_zip(staged, output)

    return ArchiveResult(
        output=output,
        release_id=release_id,
        source_revision=source_revision,
        sha256=_sha256(output),
        file_count=file_count,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Manage verified Hermes releases", allow_abbrev=False
    )
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser(
        "build", help="build a deterministic release ZIP", allow_abbrev=False
    )
    build.add_argument("--source", type=Path, required=True)
    build.add_argument("--output", type=Path, required=True)
    build.add_argument("--release-id", required=True)
    build.add_argument("--source-revision")
    install = commands.add_parser(
        "install", help="install an immutable release", allow_abbrev=False
    )
    install.add_argument("--source", type=Path, required=True)
    install.add_argument("--install-root", type=Path, required=True)
    install.add_argument("--config", type=Path, required=True)
    install.add_argument("--env-file", type=Path, required=True)
    install.add_argument("--release-id")
    install.add_argument("--activate", action="store_true")
    rollback = commands.add_parser(
        "rollback", help="swap active release links", allow_abbrev=False
    )
    rollback.add_argument("--install-root", type=Path, required=True)
    arguments = parser.parse_args(argv)

    if arguments.command == "build":
        result = build_archive(
            arguments.source,
            arguments.output,
            arguments.release_id,
            source_revision=arguments.source_revision,
        )
        print(
            json.dumps(
                {
                    "file_count": result.file_count,
                    "output": str(result.output),
                    "release_id": result.release_id,
                    "sha256": result.sha256,
                    "source_revision": result.source_revision,
                },
                sort_keys=True,
            )
        )
        return 0
    if arguments.command == "install":
        release_id = arguments.release_id or _release_id_from_source(
            arguments.source
        )
        result = install_release(
            InstallRequest(
                source=arguments.source,
                install_root=arguments.install_root,
                config=arguments.config,
                env_file=arguments.env_file,
                release_id=release_id,
                activate=arguments.activate,
            )
        )
        print(
            json.dumps(
                {
                    "activated": result.activated,
                    "installed": result.installed,
                    "previous_release": result.previous_release,
                    "release_path": str(result.release_path),
                },
                sort_keys=True,
            )
        )
        return 0
    if arguments.command == "rollback":
        current, previous = rollback_release(arguments.install_root)
        print(
            json.dumps(
                {"current_release": current, "previous_release": previous},
                sort_keys=True,
            )
        )
        return 0
    raise AssertionError(f"unhandled command: {arguments.command}")


if __name__ == "__main__":
    raise SystemExit(main())
