"""Safe deployment runtime primitives for the Hermes NAS launcher.

This module owns deployment-only concerns.  The V5 business CLI remains in
``run_pipeline.py`` and is invoked without importing its implementation.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Collection
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass, replace
import hashlib
import json
import os
from pathlib import Path
from pathlib import PurePosixPath, PureWindowsPath
import re
import stat
import subprocess
import sys

# Keep a writable release immutable even when launched directly without -B.
# This must precede the first release-local import.
sys.dont_write_bytecode = True

try:
    from pipeline.config import ConfigError, ConfigSchema
    from pipeline.security import sanitize_text
except ModuleNotFoundError:  # Imported as ``scripts.hermes_runtime`` in tests.
    from scripts.pipeline.config import ConfigError, ConfigSchema
    from scripts.pipeline.security import sanitize_text


class EnvFileError(ValueError):
    """Raised when a deployment environment file is unsafe or malformed."""


class CommandError(ValueError):
    """Raised when deployment paths or command arguments are invalid."""


@dataclass(frozen=True)
class RuntimeLayout:
    release_root: Path
    install_root: Path
    env_path: Path
    config_path: Path
    state_dir: Path
    log_dir: Path
    lock_path: Path
    python: Path


@dataclass(frozen=True)
class Diagnostic:
    code: str
    ok: bool
    detail: str


@dataclass(frozen=True)
class DoctorReport:
    diagnostics: tuple[Diagnostic, ...]

    @property
    def ok(self) -> bool:
        return all(item.ok for item in self.diagnostics)

    @property
    def failed_codes(self) -> tuple[str, ...]:
        return tuple(item.code for item in self.diagnostics if not item.ok)

    def as_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "failed_codes": list(self.failed_codes),
            "diagnostics": [
                {"code": item.code, "ok": item.ok, "detail": item.detail}
                for item in self.diagnostics
            ],
        }


_ENV_KEY = re.compile(r"[A-Z][A-Z0-9_]*")
_CONFIG_COMMANDS = {
    "preflight",
    "migrate",
    "dry-run",
    "qualify",
    "plan-resources",
    "prepare-resources",
    "run",
    "status",
    "reviews",
    "audit",
}
_PIPELINE_COMMANDS = _CONFIG_COMMANDS | {"config-migrate"}
_LOCAL_COMMANDS = {"doctor", "version"}
_MANIFEST_LINE = re.compile(r"([0-9a-f]{64})  (.+)")
_MANIFEST_NAME = "MANIFEST.sha256"
_EXPECTED_RELEASE_FILES = frozenset(
    {"RELEASE.json", "scripts/hermes_runtime.py", "scripts/run_pipeline.py"}
)
_FORBIDDEN_RELEASE_PARTS = frozenset(
    {".git", ".superpowers", "__pycache__", "dist", "build"}
)
_FORBIDDEN_RELEASE_SUFFIXES = (
    ".pyc",
    ".db",
    ".db-wal",
    ".db-shm",
    ".log",
    ".zip",
)
_FORBIDDEN_RELEASE_FILES = frozenset(
    {"deploy/anime-cloud-pipeline.env", "deploy/config.v5.json"}
)
_DEFAULT_REQUIRED_ENVIRONMENT = frozenset(
    {
        "ANIME_PIPELINE_CONFIG",
        "ANIME_PIPELINE_STATE_DIR",
        "ANIME_PIPELINE_LOG_DIR",
        "ANIME_PIPELINE_LOCK_PATH",
        "P115_ROOT_DIRECTORY_ID",
    }
)
_DEFAULT_CREDENTIAL_ENVIRONMENT = frozenset(
    {
        "MOVIEPILOT_TOKEN",
        "P115_COOKIE",
        "LITEPAN_API_KEY",
        "EMBY_API_KEY",
    }
)
_FIXED_ENVIRONMENT = _DEFAULT_REQUIRED_ENVIRONMENT | _DEFAULT_CREDENTIAL_ENVIRONMENT
_PROCESS_ONLY_ENVIRONMENT = frozenset({"ANIME_PIPELINE_PYTHON"})
_RESOURCE_AUTHORIZATION_ENVIRONMENT = frozenset(
    {"OCTOP_PIPELINE_ALLOW_RESOURCES", "OCTOP_PIPELINE_RESOURCE_REVISION"}
)
_RESOURCE_REVISION = re.compile(r"[0-9a-f]{64}\Z")
_FORBIDDEN_ENVIRONMENT = frozenset(
    {"ANIME_PIPELINE_PYTHON", "PYTHONPATH", "PYTHONHOME", "LD_PRELOAD"}
)


def parse_env_file(
    path: Path,
    *,
    require_private_mode: bool = True,
    allowed_names: Collection[str] | None = None,
    allow_unknown_names: bool = False,
) -> dict[str, str]:
    """Parse strict ``KEY=value`` assignments without shell evaluation."""

    path = Path(path)
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise EnvFileError("unable to inspect environment file") from exc

    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise EnvFileError("environment file must be a regular non-symlink file")
    if (
        require_private_mode
        and os.name == "posix"
        and stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise EnvFileError("environment file permissions must be 0600 or stricter")

    try:
        raw_bytes = path.read_bytes()
    except OSError as exc:
        raise EnvFileError("unable to read environment file") from exc
    if b"\x00" in raw_bytes:
        raise EnvFileError("environment file must not contain NUL bytes")
    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise EnvFileError("environment file must be valid UTF-8") from exc

    values: dict[str, str] = {}
    allowed = _FIXED_ENVIRONMENT if allowed_names is None else frozenset(allowed_names)
    for number, raw_line in enumerate(text.splitlines(), 1):
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        key, separator, value = raw_line.partition("=")
        if not separator or _ENV_KEY.fullmatch(key) is None:
            raise EnvFileError(f"invalid environment assignment on line {number}")
        if key in _FORBIDDEN_ENVIRONMENT or (
            not allow_unknown_names and key not in allowed
        ):
            raise EnvFileError(f"unsupported environment variable on line {number}")
        if key in values:
            raise EnvFileError(f"duplicate environment variable on line {number}")
        values[key] = value
    return values


def _is_absolute_path(value: str | os.PathLike[str]) -> bool:
    """Recognize native and documented POSIX absolute path spellings."""

    rendered = os.fspath(value)
    return Path(rendered).is_absolute() or (
        os.name == "nt" and rendered.startswith(("/", "\\"))
    )


def _normalized_path(value: str | os.PathLike[str]) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path.resolve(strict=False)
    return Path(os.path.normpath(os.fspath(path)))


def _absolute_override(environ: Mapping[str, str], name: str, default: Path) -> Path:
    if name not in environ:
        return default
    raw = environ[name]
    if not _is_absolute_path(raw):
        raise CommandError(f"{name} must be an absolute path")
    return _normalized_path(raw)


def _is_within(path: Path, parent: Path) -> bool:
    try:
        _normalized_path(path).relative_to(_normalized_path(parent))
    except ValueError:
        return False
    return True


def _require_outside_installed_releases(
    release_root: Path, install_root: Path, paths: Sequence[Path]
) -> None:
    if release_root == install_root:
        return
    releases_root = install_root / "releases"
    if any(_is_within(path, releases_root) for path in paths):
        raise CommandError(
            "deployment paths must remain outside the installed releases directory"
        )


def resolve_layout(script_path: Path, environ: Mapping[str, str]) -> RuntimeLayout:
    """Resolve source or immutable-release paths without using the caller CWD."""

    if not _is_absolute_path(script_path):
        raise CommandError("runtime script path must be absolute")
    script = _normalized_path(script_path)
    if script.name != "hermes_runtime.py" or script.parent.name != "scripts":
        raise CommandError("runtime script must be located under a scripts directory")

    release_root = script.parent.parent
    if release_root.parent.name == "releases" and release_root.name:
        install_root = release_root.parent.parent
    else:
        install_root = release_root

    config_dir = install_root / "config"
    state_dir = _absolute_override(
        environ, "ANIME_PIPELINE_STATE_DIR", install_root / "state"
    )
    config_path = _absolute_override(
        environ, "ANIME_PIPELINE_CONFIG", config_dir / "config.v5.json"
    )
    log_dir = _absolute_override(
        environ, "ANIME_PIPELINE_LOG_DIR", install_root / "logs"
    )
    lock_path = _absolute_override(
        environ, "ANIME_PIPELINE_LOCK_PATH", state_dir / "pipeline.lock"
    )
    python = _absolute_override(
        environ, "ANIME_PIPELINE_PYTHON", Path(sys.executable)
    )
    env_path = config_dir / "anime-cloud-pipeline.env"
    _require_outside_installed_releases(
        release_root,
        install_root,
        (env_path, config_path, state_dir, log_dir, lock_path),
    )

    return RuntimeLayout(
        release_root=release_root,
        install_root=install_root,
        env_path=env_path,
        config_path=config_path,
        state_dir=state_dir,
        log_dir=log_dir,
        lock_path=lock_path,
        python=python,
    )


def build_pipeline_argv(layout: RuntimeLayout, argv: Sequence[str]) -> list[str]:
    """Build an argv vector for the existing V5 CLI without shell parsing."""

    if isinstance(argv, (str, bytes)):
        raise CommandError("command arguments must be a sequence of strings")
    arguments = list(argv)
    if not arguments or any(not isinstance(item, str) for item in arguments):
        raise CommandError("a supported pipeline command is required")

    command = arguments[0]
    if command not in _PIPELINE_COMMANDS:
        raise CommandError("unsupported command")

    if command == "reviews":
        if len(arguments) < 2 or arguments[1] not in {"list", "resolve"}:
            raise CommandError("reviews requires list or resolve")
        insertion_index = 2
    else:
        insertion_index = 1

    if command in _CONFIG_COMMANDS:
        if any(
            len(option := item.partition("=")[0]) > 2
            and "--config".startswith(option)
            for item in arguments[1:]
        ):
            raise CommandError("caller-supplied --config is not allowed")
        arguments[insertion_index:insertion_index] = [
            "--config",
            str(layout.config_path),
        ]

    return [
        str(layout.python),
        "-B",
        str(layout.release_root / "scripts" / "run_pipeline.py"),
        *arguments,
    ]


def _regular_file(path: Path) -> bool:
    try:
        mode = path.lstat().st_mode
    except OSError:
        return False
    return stat.S_ISREG(mode) and not stat.S_ISLNK(mode)


def _ready_directory(path: Path) -> bool:
    try:
        mode = path.lstat().st_mode
    except OSError:
        return False
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode) or not os.access(path, os.W_OK):
        return False
    if os.name == "posix" and hasattr(os, "geteuid"):
        try:
            return path.stat().st_uid == os.geteuid()
        except OSError:
            return False
    return True


def _ready_lock_path(path: Path, state_dir: Path) -> bool:
    if _normalized_path(path.parent) != _normalized_path(state_dir):
        return False
    if not _ready_directory(path.parent):
        return False
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        return True
    except OSError:
        return False
    return (
        stat.S_ISREG(mode)
        and not stat.S_ISLNK(mode)
        and os.access(path, os.W_OK)
    )


def _outside_installed_releases(layout: RuntimeLayout, path: Path) -> bool:
    if layout.release_root == layout.install_root:
        return True
    return not _is_within(path, layout.install_root / "releases")


def _safe_manifest_name(name: str) -> bool:
    posix = PurePosixPath(name)
    windows = PureWindowsPath(name)
    return not (
        not name
        or posix.is_absolute()
        or windows.is_absolute()
        or bool(windows.drive)
        or "\\" in name
        or posix.as_posix() != name
        or ".." in posix.parts
        or name == _MANIFEST_NAME
    )


def _release_tree_files(root: Path) -> dict[str, Path] | None:
    try:
        root_mode = root.lstat().st_mode
    except OSError:
        return None
    if stat.S_ISLNK(root_mode) or not stat.S_ISDIR(root_mode):
        return None

    files: dict[str, Path] = {}
    pending = [root]
    while pending:
        directory = pending.pop()
        try:
            children = list(directory.iterdir())
        except OSError:
            return None
        for path in children:
            relative = path.relative_to(root).as_posix()
            relative_path = PurePosixPath(relative)
            if (
                relative_path.parts[0] == "config"
                or any(part in _FORBIDDEN_RELEASE_PARTS for part in relative_path.parts)
                or relative.endswith(_FORBIDDEN_RELEASE_SUFFIXES)
                or relative.casefold() in _FORBIDDEN_RELEASE_FILES
            ):
                return None
            try:
                mode = path.lstat().st_mode
            except OSError:
                return None
            if stat.S_ISLNK(mode):
                return None
            if stat.S_ISDIR(mode):
                pending.append(path)
            elif stat.S_ISREG(mode):
                if relative != _MANIFEST_NAME:
                    files[relative] = path
            else:
                return None
    return files


def _release_manifest_ok(root: Path) -> bool:
    manifest = root / _MANIFEST_NAME
    if not _regular_file(manifest):
        return False
    try:
        lines = manifest.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError):
        return False

    expected: dict[str, str] = {}
    for line in lines:
        match = _MANIFEST_LINE.fullmatch(line)
        if match is None:
            return False
        digest, name = match.groups()
        if name in expected or not _safe_manifest_name(name):
            return False
        expected[name] = digest
    if not _EXPECTED_RELEASE_FILES.issubset(expected):
        return False

    actual = _release_tree_files(root)
    if actual is None or set(actual) != set(expected):
        return False
    for name, path in actual.items():
        try:
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
        except OSError:
            return False
        if digest != expected[name]:
            return False
    return True


def _validated_config_mapping(config_path: Path) -> Mapping[str, object]:
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise EnvFileError("deployment configuration is not valid JSON") from exc
    if not isinstance(raw, Mapping):
        raise EnvFileError("deployment configuration must be a JSON object")
    try:
        ConfigSchema.from_mapping(raw, {}, "linux")
    except ConfigError as exc:
        raise EnvFileError("deployment configuration failed schema validation") from exc
    return raw


def _configured_environment_names(config_path: Path) -> set[str]:
    raw = _validated_config_mapping(config_path)
    names = set(_DEFAULT_REQUIRED_ENVIRONMENT)
    try:
        validated = ConfigSchema.from_mapping(raw, {}, "linux")
    except ConfigError as exc:
        raise EnvFileError("deployment configuration failed schema validation") from exc
    for service in (
        validated.moviepilot,
        validated.cloud,
        validated.litepan,
        validated.emby,
    ):
        reference = service.secret_reference
        if reference is None or _ENV_KEY.fullmatch(reference):
            if reference:
                names.add(reference)
            continue
        raise EnvFileError("deployment configuration has an unsupported credential name")
    if names & _RESOURCE_AUTHORIZATION_ENVIRONMENT:
        raise EnvFileError(
            "deployment configuration uses reserved resource authorization names"
        )
    return names


def _resource_apply_authorization(
    arguments: Sequence[str], caller_environment: Mapping[str, str]
) -> dict[str, str]:
    """Pass one-shot resource authorization only to an explicit apply command.

    These names are intentionally excluded from the configured dotenv allowlist.
    A valid authorization must arrive in this process environment, alongside
    the exact command and apply flag; other commands never inherit it.
    """

    if (
        not arguments
        or arguments[0] != "prepare-resources"
        or "--apply" not in arguments[1:]
    ):
        return {}
    if caller_environment.get("OCTOP_PIPELINE_ALLOW_RESOURCES") != "1":
        return {}
    revision = caller_environment.get("OCTOP_PIPELINE_RESOURCE_REVISION")
    if not isinstance(revision, str) or not _RESOURCE_REVISION.fullmatch(revision):
        return {}
    return {
        "OCTOP_PIPELINE_ALLOW_RESOURCES": "1",
        "OCTOP_PIPELINE_RESOURCE_REVISION": revision,
    }


def _sanitized_child_detail(
    completed: subprocess.CompletedProcess[str], secrets: Sequence[str]
) -> str:
    output = "\n".join(
        part.strip()
        for part in (completed.stdout or "", completed.stderr or "")
        if part and part.strip()
    )
    if output:
        output = sanitize_text(output, secrets=secrets)[:600]
        return f"exit={completed.returncode}; output={output}"
    return f"exit={completed.returncode}"


def _run_diagnostic_command(
    code: str,
    argv: Sequence[str],
    environ: Mapping[str, str],
    allowed_names: Collection[str],
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]],
) -> Diagnostic:
    secrets = tuple(
        environ[name] for name in allowed_names if environ.get(name, "")
    )
    child_environment = {
        name: environ[name]
        for name in allowed_names
        if name not in _FORBIDDEN_ENVIRONMENT and name in environ
    }
    try:
        completed = subprocess_runner(
            list(argv),
            check=False,
            capture_output=True,
            text=True,
            env=child_environment,
        )
    except Exception as exc:
        return Diagnostic(code, False, f"operation failed ({type(exc).__name__})")
    return Diagnostic(
        code,
        completed.returncode == 0,
        _sanitized_child_detail(completed, secrets),
    )


def run_doctor(
    layout: RuntimeLayout,
    environ: Mapping[str, str] | None = None,
    *,
    subprocess_runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> DoctorReport:
    """Run local, read-only deployment diagnostics for one release."""

    config_schema_ok = True
    try:
        required_names = sorted(_configured_environment_names(layout.config_path))
    except EnvFileError:
        config_schema_ok = False
        required_names = sorted(_DEFAULT_REQUIRED_ENVIRONMENT)

    if environ is None:
        try:
            checked_environment = parse_env_file(
                layout.env_path, allowed_names=required_names
            )
        except EnvFileError:
            checked_environment = {}
    else:
        checked_environment = {
            name: environ[name] for name in required_names if name in environ
        }

    expected_config = layout.install_root / "config" / "config.v5.json"
    expected_env = layout.install_root / "config" / "anime-cloud-pipeline.env"
    config_ok = layout.config_path == expected_config and _regular_file(
        layout.config_path
    )
    env_ok = layout.env_path == expected_env and _regular_file(layout.env_path)
    distinct_files = False
    if config_ok and env_ok:
        try:
            distinct_files = not os.path.samefile(
                layout.config_path, layout.env_path
            )
        except OSError:
            distinct_files = False
    private_env = env_ok
    if private_env and os.name == "posix":
        try:
            private_env = not (
                stat.S_IMODE(layout.env_path.lstat().st_mode) & 0o077
            )
        except OSError:
            private_env = False

    manifest_ok = _release_manifest_ok(layout.release_root)
    diagnostics = [
        Diagnostic(
            "release_manifest",
            manifest_ok,
            "verified" if manifest_ok else "invalid",
        ),
        Diagnostic("config_file", config_ok, "valid" if config_ok else "invalid"),
        Diagnostic(
            "config_schema",
            config_schema_ok,
            "validated" if config_schema_ok else "invalid",
        ),
        Diagnostic("environment_file", env_ok, "valid" if env_ok else "invalid"),
        Diagnostic(
            "config_env_identity",
            distinct_files,
            "distinct" if distinct_files else "invalid",
        ),
        Diagnostic(
            "environment_private_mode",
            private_env,
            "private" if private_env else "invalid",
        ),
    ]
    for code, path in (
        ("config_path_outside_release", layout.config_path),
        ("env_path_outside_release", layout.env_path),
        ("state_dir_outside_release", layout.state_dir),
        ("log_dir_outside_release", layout.log_dir),
        ("lock_path_outside_release", layout.lock_path),
    ):
        outside = _outside_installed_releases(layout, path)
        diagnostics.append(
            Diagnostic(code, outside, "outside" if outside else "inside release")
        )

    state_ready = _ready_directory(layout.state_dir)
    log_ready = _ready_directory(layout.log_dir)
    lock_ready = _ready_lock_path(layout.lock_path, layout.state_dir)
    diagnostics.extend(
        (
            Diagnostic(
                "state_dir_ready", state_ready, "ready" if state_ready else "invalid"
            ),
            Diagnostic(
                "log_dir_ready", log_ready, "ready" if log_ready else "invalid"
            ),
            Diagnostic(
                "lock_path_ready", lock_ready, "ready" if lock_ready else "invalid"
            ),
        )
    )

    missing_names = [
        name for name in required_names if not checked_environment.get(name, "").strip()
    ]
    diagnostics.append(
        Diagnostic(
            "required_environment",
            not missing_names,
            (
                "present: " + ",".join(required_names)
                if not missing_names
                else "missing: " + ",".join(missing_names)
            ),
        )
    )

    if not all(item.ok for item in diagnostics):
        diagnostics.extend(
            Diagnostic(code, False, "skipped: local prerequisites failed")
            for code in ("python_sqlite", "preflight", "migrate_check_only")
        )
        return DoctorReport(tuple(diagnostics))

    python_probe = [
        str(layout.python),
        "-B",
        "-c",
        (
            "import sqlite3,sys;"
            "raise SystemExit(0 if sys.version_info >= (3, 12) "
            "and sqlite3.sqlite_version_info else 1)"
        ),
    ]
    diagnostics.append(
        _run_diagnostic_command(
            "python_sqlite",
            python_probe,
            checked_environment,
            required_names,
            subprocess_runner,
        )
    )
    if not diagnostics[-1].ok:
        diagnostics.extend(
            (
                Diagnostic("preflight", False, "skipped: python_sqlite failed"),
                Diagnostic(
                    "migrate_check_only",
                    False,
                    "skipped: preflight skipped because python_sqlite failed",
                ),
            )
        )
        return DoctorReport(tuple(diagnostics))

    diagnostics.append(
        _run_diagnostic_command(
            "preflight",
            build_pipeline_argv(layout, ["preflight"]),
            checked_environment,
            required_names,
            subprocess_runner,
        )
    )
    if not diagnostics[-1].ok:
        diagnostics.append(
            Diagnostic("migrate_check_only", False, "skipped: preflight failed")
        )
        return DoctorReport(tuple(diagnostics))

    diagnostics.append(
        _run_diagnostic_command(
            "migrate_check_only",
            build_pipeline_argv(layout, ["migrate", "--check-only"]),
            checked_environment,
            required_names,
            subprocess_runner,
        )
    )
    return DoctorReport(tuple(diagnostics))


def _split_runtime_arguments(argv: Sequence[str] | None) -> tuple[Path | None, list[str]]:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if any(not isinstance(item, str) for item in arguments):
        raise CommandError("command arguments must be strings")

    env_path: Path | None = None
    if arguments and arguments[0] == "--env-file":
        if len(arguments) < 2:
            raise CommandError("--env-file requires an absolute path")
        env_path = Path(arguments[1])
        arguments = arguments[2:]
    elif arguments and arguments[0].startswith("--env-file="):
        raw_path = arguments[0].partition("=")[2]
        if not raw_path:
            raise CommandError("--env-file requires an absolute path")
        env_path = Path(raw_path)
        arguments = arguments[1:]

    if any(
        item == "--env-file" or item.startswith("--env-file=") for item in arguments
    ):
        raise CommandError("--env-file must appear once before the command")
    if env_path is not None and not _is_absolute_path(env_path):
        raise CommandError("--env-file requires an absolute path")
    if not arguments:
        raise CommandError("a command is required")
    return env_path, arguments


def _print_json(payload: Mapping[str, object]) -> None:
    print(json.dumps(dict(payload), ensure_ascii=False, sort_keys=True))


def main(
    argv: Sequence[str] | None = None,
    environ: Mapping[str, str] | None = None,
) -> int:
    """Load deployment data and replace this process with the V5 CLI."""

    try:
        requested_env_path, arguments = _split_runtime_arguments(argv)
        command = arguments[0]
        if command not in _PIPELINE_COMMANDS | _LOCAL_COMMANDS:
            raise CommandError("unsupported command")
        if command in _LOCAL_COMMANDS and len(arguments) != 1:
            raise CommandError(f"{command} does not accept arguments")

        caller_environment = dict(os.environ if environ is None else environ)
        script_path = Path(__file__)
        initial_layout = resolve_layout(script_path, caller_environment)

        if command == "version":
            _print_json(
                {"command": "version", "release": initial_layout.release_root.name}
            )
            return 0

        env_path = requested_env_path or initial_layout.env_path
        _require_outside_installed_releases(
            initial_layout.release_root,
            initial_layout.install_root,
            (env_path,),
        )
        parsed_environment = parse_env_file(
            env_path,
            allow_unknown_names=True,
        )
        pre_layout_environment = {
            name: caller_environment[name]
            for name in _PROCESS_ONLY_ENVIRONMENT | _FIXED_ENVIRONMENT
            if name in caller_environment
        }
        pre_layout_environment.update(parsed_environment)
        pre_layout = resolve_layout(script_path, pre_layout_environment)
        allowed_names = _configured_environment_names(pre_layout.config_path)
        # Re-read through the strict allowlist so a custom credential name is
        # accepted only after the referenced config has passed schema checks.
        parsed_environment = parse_env_file(
            env_path,
            allowed_names=allowed_names,
        )
        merged_environment = {
            name: caller_environment[name]
            for name in allowed_names | _PROCESS_ONLY_ENVIRONMENT
            if name in caller_environment
        }
        merged_environment.update(parsed_environment)
        layout = replace(
            resolve_layout(script_path, merged_environment), env_path=env_path
        )
        runtime_environment = {
            name: value
            for name, value in merged_environment.items()
            if name not in _PROCESS_ONLY_ENVIRONMENT
        }
        runtime_environment.update(
            _resource_apply_authorization(arguments, caller_environment)
        )
        if command == "doctor":
            report = run_doctor(layout, runtime_environment)
            _print_json({"command": "doctor", **report.as_dict()})
            return 0 if report.ok else 1
        command_argv = build_pipeline_argv(layout, arguments)
        os.execve(str(layout.python), command_argv, runtime_environment)
        return 0
    except (CommandError, EnvFileError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except OSError as exc:
        print(f"error: runtime operation failed ({type(exc).__name__})", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
