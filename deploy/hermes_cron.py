"""Build and disable the one Hermes-owned cron entry without shell parsing."""
from __future__ import annotations

import argparse
import re
import shlex
import sys
from collections.abc import Iterable, Sequence


CRON_MARKER = "# anime-cloud-pipeline-managed"
_LOG_PATH_PATTERN = r"/[A-Za-z0-9._/-]+"


class CronTransformError(ValueError):
    """Raised when a captured crontab cannot be transformed unambiguously."""


def _validate_value(name: str, value: str) -> str:
    if not isinstance(value, str) or not value.strip() or "\n" in value or "\r" in value:
        raise CronTransformError(f"{name} must be a non-empty single-line value")
    return value.strip()


def _validate_marker(value: str) -> str:
    """Accept only the fixed comment that identifies Hermes-owned entries."""
    if type(value) is not str or value != CRON_MARKER:
        raise CronTransformError("marker is fixed to CRON_MARKER")
    return CRON_MARKER


def _validate_log_path(value: str) -> str:
    """Validate a literal absolute POSIX path safe for cron redirection."""
    if not isinstance(value, str) or not value:
        raise CronTransformError("log_path must be a non-empty absolute path")
    if any(
        ord(character) < 0x20
        or ord(character) == 0x7F
        or character.isspace()
        for character in value
    ):
        raise CronTransformError("log_path must not contain control or whitespace characters")
    if not value.startswith("/") or value.startswith("//"):
        raise CronTransformError("log_path must be an absolute POSIX path")
    if re.fullmatch(_LOG_PATH_PATTERN, value) is None:
        raise CronTransformError(
            "log_path contains characters outside the safe POSIX path allowlist"
        )
    if any(part in {"", ".", ".."} for part in value.split("/")[1:]):
        raise CronTransformError("log_path must use normalized semantic path segments")
    return value


def _canonical_command(value: str | Sequence[str]) -> tuple[tuple[str, ...], str]:
    """Return argv and its canonical shell spelling, rejecting ambiguity."""

    if isinstance(value, str):
        raw = _validate_value("command", value)
        try:
            argv = tuple(shlex.split(raw, posix=True))
        except ValueError as error:
            raise CronTransformError("command must be valid shell argv") from error
        if not argv:
            raise CronTransformError("command must contain at least one argv element")
        canonical = shlex.join(argv)
        if canonical != raw:
            raise CronTransformError(
                "command must use canonical shell quoting for unambiguous matching"
            )
        return argv, canonical

    if isinstance(value, (bytes, bytearray)):
        raise CronTransformError("command must be shell argv text")
    try:
        argv = tuple(value)
    except TypeError as error:
        raise CronTransformError("command must be shell argv text") from error
    if not argv or any(not isinstance(item, str) or not item for item in argv):
        raise CronTransformError("command argv must contain non-empty strings")
    return argv, shlex.join(argv)


def _newline_for(source: str) -> str:
    return "\r\n" if "\r\n" in source else "\n"


def _line_text(line: str) -> str:
    return line.rstrip("\r\n")


def _is_exact_marker(line: str, marker: str) -> bool:
    """Return whether a complete crontab line is exactly the ownership marker."""
    return _line_text(line) == marker


def _cron_payload(line: str) -> str | None:
    """Extract the shell command after a five-field or ``@name`` cron schedule."""
    text = _line_text(line)
    if not text.strip() or text.lstrip().startswith("#"):
        return None

    if text.lstrip().startswith("@"):
        parts = text.split(None, 1)
        return parts[1] if len(parts) == 2 else None

    parts = text.split(None, 5)
    return parts[5] if len(parts) == 6 else None


def _is_exact_canonical_command(payload: str | None, command: str) -> bool:
    """Match only a direct command plus the helper's bounded log redirection."""
    if payload is None:
        return False
    pattern = rf"{re.escape(command)}[ \t]+>>[ \t]+(?P<log_path>{_LOG_PATH_PATTERN})[ \t]+2>&1"
    match = re.fullmatch(pattern, payload)
    if match is None:
        return False
    try:
        _validate_log_path(match.group("log_path"))
    except CronTransformError:
        return False
    return True


def _shell_prefix(payload: str, count: int) -> tuple[str, ...]:
    """Parse only the first shell words, without expanding or executing them."""

    lexer = shlex.shlex(payload, posix=True, punctuation_chars=";&|<>")
    lexer.whitespace_split = True
    lexer.commenters = ""
    words: list[str] = []
    for _ in range(count):
        word = lexer.get_token()
        if word is None:
            break
        words.append(word)
    return tuple(words)


def _raw_first_word_matches(payload: str, first: str) -> bool:
    """Identify a managed-looking first word after a shell parse failure.

    ``shlex`` cannot return a token when a quote inside the first word is left
    open.  Decode the visible first word just far enough to determine whether
    it constructs the managed executable, but do not search for the executable
    as a substring: a wrapper such as ``...-wrapper`` must remain unrelated.
    """

    text = payload.lstrip()
    if not text:
        return False

    boundaries = frozenset(";&|<>")
    decoded: list[str] = []
    state = "unquoted"
    index = 0
    while index < len(text):
        character = text[index]
        if state == "unquoted":
            if character.isspace() or character in boundaries:
                break
            if character == "'":
                state = "single"
                index += 1
                continue
            if character == '"':
                state = "double"
                index += 1
                continue
            if character == "\\":
                if index + 1 >= len(text):
                    decoded.append(character)
                    index += 1
                else:
                    decoded.append(text[index + 1])
                    index += 2
                continue
            decoded.append(character)
            index += 1
            continue

        if state == "single":
            if character == "'":
                state = "unquoted"
                index += 1
            else:
                decoded.append(character)
                index += 1
            continue

        if character == '"':
            state = "unquoted"
            index += 1
        elif character == "\\":
            if index + 1 >= len(text):
                decoded.append(character)
                index += 1
            elif text[index + 1] in "$`\"\\\n":
                # POSIX double quotes only give backslash special meaning
                # before these five characters.  A backslash before an
                # ordinary character remains part of the word.
                if text[index + 1] != "\n":
                    decoded.append(text[index + 1])
                index += 2
            else:
                decoded.append(character)
                index += 1
        else:
            decoded.append(character)
            index += 1

    value = "".join(decoded)
    if value == first:
        return True
    if not value.startswith(first):
        return False
    suffix = value[len(first) :]
    return not suffix or suffix[0].isspace() or suffix[0] in boundaries


def _starts_with_command_field(payload: str | None, command: str) -> bool:
    """Identify a direct shell-equivalent command prefix safely."""
    if payload is None:
        return False
    argv, _ = _canonical_command(command)
    try:
        # Parse only the executable token.  Once it decodes to the exact
        # managed path, malformed or noncanonical remainder must fail closed;
        # parsing the whole line can throw before exposing that first token.
        if _shell_prefix(payload, 1) != (argv[0],):
            return False
        if len(argv) == 1:
            return True

        # Keep a command-name collision such as ``run-wrapper`` unrelated,
        # while treating a genuine second argv word (or malformed text) as a
        # direct managed-looking invocation.
        words = _shell_prefix(payload, 2)
        if len(words) < 2:
            return True
        second = words[1]
        if second == argv[1]:
            return True
        if second.startswith(argv[1]):
            suffix = second[len(argv[1]) :]
            if suffix and not suffix[0].isspace() and suffix[0] not in ";&|<>":
                return False
        return True
    except ValueError:
        return _raw_first_word_matches(payload, argv[0])


def _command_kind(line: str, command: str | Sequence[str]) -> str:
    """Classify a line as canonical, ambiguous direct invocation, or unrelated."""
    _, canonical = _canonical_command(command)
    payload = _cron_payload(line)
    if _is_exact_canonical_command(payload, canonical):
        return "canonical"
    if _starts_with_command_field(payload, canonical):
        return "ambiguous"
    return "other"


def _remove_marker_owned(lines: Iterable[str], marker: str, command: str) -> list[str]:
    """Remove exact marker/canonical pairs, failing closed for unsafe pairs."""
    remaining: list[str] = []
    materialized = list(lines)
    index = 0
    while index < len(materialized):
        line = materialized[index]
        if not _is_exact_marker(line, marker):
            remaining.append(line)
            index += 1
            continue

        next_index = index + 1
        while (
            next_index < len(materialized)
            and not _line_text(materialized[next_index]).strip()
        ):
            next_index += 1

        if next_index < len(materialized):
            kind = _command_kind(materialized[next_index], command)
            if kind == "canonical":
                index = next_index + 1
                continue
            if kind == "ambiguous":
                raise CronTransformError(
                    "marker-owned pipeline entry is not an exact canonical invocation"
                )

        remaining.append(line)
        index += 1
    return remaining


def _ensure_boundary(text: str, newline: str) -> str:
    if text and not text.endswith(("\n", "\r")):
        return text + newline
    return text


def _count_marker(text: str, marker: str) -> int:
    return sum(_is_exact_marker(line, marker) for line in text.splitlines(keepends=True))


def _count_command(text: str, command: str) -> int:
    return sum(
        _command_kind(line, command) == "canonical"
        for line in text.splitlines(keepends=True)
    )


def _filter_unmarked_entries(
    lines: Iterable[str], command: str, *, remove_canonical: bool
) -> list[str]:
    """Preserve unrelated lines and reject direct non-canonical pipeline entries."""
    result: list[str] = []
    for line in lines:
        kind = _command_kind(line, command)
        if kind == "canonical":
            if remove_canonical:
                continue
            raise CronTransformError(
                "unmarked exact canonical pipeline entry prevents normalization"
            )
        if kind == "ambiguous":
            raise CronTransformError(
                "ambiguous direct pipeline-looking entry prevents normalization"
            )
        result.append(line)
    return result


def _require_counts(text: str, *, marker: str, command: str, expected: int) -> None:
    marker_count = _count_marker(text, marker)
    command_count = _count_command(text, command)
    if marker_count != expected or command_count != expected:
        raise CronTransformError(
            "candidate crontab must contain exactly "
            f"{expected} marker and command entries; got "
            f"{marker_count} markers and {command_count} commands"
        )


def build_candidate_table(
    source: str,
    *,
    marker: str = CRON_MARKER,
    command: str | Sequence[str],
    schedule: str,
    log_path: str,
) -> str:
    """Replace marker-owned entries and append one bounded pipeline command."""
    marker = _validate_marker(marker)
    _, command = _canonical_command(command)
    schedule = _validate_value("schedule", schedule)
    log_path = _validate_log_path(log_path)
    if not isinstance(source, str):
        raise CronTransformError("captured crontab must be text")

    newline = _newline_for(source)
    remaining = _remove_marker_owned(source.splitlines(keepends=True), marker, command)
    remaining = _filter_unmarked_entries(remaining, command, remove_canonical=False)
    candidate = _ensure_boundary("".join(remaining), newline)
    candidate += (
        f"{marker}{newline}"
        f"{schedule} {command} >> {log_path} 2>&1{newline}"
    )
    _require_counts(candidate, marker=marker, command=command, expected=1)
    return candidate


def disable_pipeline_entries(
    source: str,
    *,
    marker: str = CRON_MARKER,
    command: str | Sequence[str],
) -> str:
    """Remove Hermes marker-owned and exact pipeline command lines."""
    marker = _validate_marker(marker)
    _, command = _canonical_command(command)
    if not isinstance(source, str):
        raise CronTransformError("captured crontab must be text")

    remaining = _remove_marker_owned(source.splitlines(keepends=True), marker, command)
    remaining = _filter_unmarked_entries(remaining, command, remove_canonical=True)
    result = "".join(remaining)
    _require_counts(result, marker=marker, command=command, expected=0)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Transform an already captured crontab safely."
    )
    subparsers = parser.add_subparsers(dest="operation", required=True)

    build = subparsers.add_parser("build", help="replace and append one pipeline entry")
    build.add_argument("--marker", default=CRON_MARKER)
    build.add_argument("--command", required=True)
    build.add_argument("--schedule", required=True)
    build.add_argument("--log-path", required=True)

    disable = subparsers.add_parser("disable", help="remove pipeline entries")
    disable.add_argument("--marker", default=CRON_MARKER)
    disable.add_argument("--command", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    source = sys.stdin.read()
    try:
        if args.operation == "build":
            result = build_candidate_table(
                source,
                marker=args.marker,
                command=args.command,
                schedule=args.schedule,
                log_path=args.log_path,
            )
        else:
            result = disable_pipeline_entries(
                source,
                marker=args.marker,
                command=args.command,
            )
    except CronTransformError as error:
        print(f"hermes-cron: {error}", file=sys.stderr)
        return 2
    sys.stdout.write(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
