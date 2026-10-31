"""Tests for the packaged Hermes candidate-crontab helper."""
from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
import importlib
import io
import shlex
import unittest
from unittest import mock


MARKER = "# anime-cloud-pipeline-managed"
COMMAND = "/install/current/bin/anime-cloud-pipeline run"
SCHEDULE = "0 2 * * *"
LOG_PATH = "/logs/anime-cloud-pipeline.log"


def _helper():
    try:
        return importlib.import_module("deploy.hermes_cron")
    except ModuleNotFoundError as error:
        raise AssertionError(f"deployment cron helper is missing: {error}") from error


class HermesCronTransformationTests(unittest.TestCase):
    def test_api_rejects_str_subclass_that_overrides_marker_comparison(self):
        helper = _helper()

        class DeceptiveMarker(str):
            def __ne__(self, other):
                return False

        marker = DeceptiveMarker(
            f"{MARKER}\n* * * * * /bin/touch /tmp/hermes-marker-injection"
        )
        source = f"{MARKER}\n{SCHEDULE} {COMMAND} >> {LOG_PATH} 2>&1\n"

        with self.assertRaises(helper.CronTransformError):
            helper.build_candidate_table(
                source,
                marker=marker,
                command=COMMAND,
                schedule=SCHEDULE,
                log_path=LOG_PATH,
            )
        with self.assertRaises(helper.CronTransformError):
            helper.disable_pipeline_entries(
                source,
                marker=marker,
                command=COMMAND,
            )

    def test_build_rejects_arbitrary_marker_before_emitting_output(self):
        helper = _helper()
        untrusted_markers = (
            "* * * * * /bin/touch /tmp/hermes-marker-injection",
            f"{MARKER}\n* * * * * /bin/touch /tmp/hermes-marker-injection",
            f"{MARKER}\r\n* * * * * /bin/touch /tmp/hermes-marker-injection",
            f"{MARKER}\x00",
            f"{MARKER} # comment-confusion",
            f" {MARKER}",
        )

        for marker in untrusted_markers:
            with self.subTest(marker=repr(marker)):
                with self.assertRaises(helper.CronTransformError):
                    helper.build_candidate_table(
                        "0 * * * * /other/job\n",
                        marker=marker,
                        command=COMMAND,
                        schedule=SCHEDULE,
                        log_path=LOG_PATH,
                    )

    def test_disable_rejects_arbitrary_marker_before_emitting_output(self):
        helper = _helper()
        source = f"{MARKER}\n{SCHEDULE} {COMMAND} >> {LOG_PATH} 2>&1\n"
        untrusted_markers = (
            "* * * * * /bin/touch /tmp/hermes-marker-injection",
            f"{MARKER}\n* * * * * /bin/touch /tmp/hermes-marker-injection",
            f"{MARKER}\r\n* * * * * /bin/touch /tmp/hermes-marker-injection",
            f"{MARKER}\x00",
            f"{MARKER} # comment-confusion",
            f" {MARKER}",
        )

        for marker in untrusted_markers:
            with self.subTest(marker=repr(marker)):
                with self.assertRaises(helper.CronTransformError):
                    helper.disable_pipeline_entries(
                        source,
                        marker=marker,
                        command=COMMAND,
                    )

    def test_build_adds_one_entry_to_an_empty_crontab(self):
        helper = _helper()

        result = helper.build_candidate_table(
            "",
            command=COMMAND,
            schedule=SCHEDULE,
            log_path=LOG_PATH,
        )

        self.assertEqual(helper._count_marker(result, helper.CRON_MARKER), 1)
        self.assertEqual(helper._count_command(result, COMMAND), 1)
        self.assertIn(helper.CRON_MARKER, result)
        self.assertIn(f"{SCHEDULE} {COMMAND} >> {LOG_PATH} 2>&1", result)

    def test_build_replaces_one_marker_owned_entry(self):
        helper = _helper()
        source = (
            "0 * * * * /other/job\n"
            f"{MARKER}\n"
            f"1 1 * * * {COMMAND} >> /old.log 2>&1\n"
        )

        result = helper.build_candidate_table(
            source,
            marker=MARKER,
            command=COMMAND,
            schedule=SCHEDULE,
            log_path=LOG_PATH,
        )

        self.assertIn("0 * * * * /other/job\n", result)
        self.assertNotIn("/old.log", result)
        self.assertEqual(result.count(MARKER), 1)
        self.assertEqual(result.count(COMMAND), 1)

    def test_build_removes_duplicate_marker_owned_entries(self):
        helper = _helper()
        source = (
            f"{MARKER}\n{SCHEDULE} {COMMAND} >> /one.log 2>&1\n"
            f"{MARKER}\n{SCHEDULE} {COMMAND} >> /two.log 2>&1\n"
            "0 * * * * /other/job\n"
        )

        result = helper.build_candidate_table(
            source,
            marker=MARKER,
            command=COMMAND,
            schedule=SCHEDULE,
            log_path=LOG_PATH,
        )

        self.assertNotIn("/one.log", result)
        self.assertNotIn("/two.log", result)
        self.assertIn("0 * * * * /other/job\n", result)
        self.assertEqual(result.count(MARKER), 1)
        self.assertEqual(result.count(COMMAND), 1)

    def test_build_preserves_duplicate_unrelated_entries(self):
        helper = _helper()
        source = (
            "0 * * * * /other/job\n"
            "0 * * * * /other/job\n"
            "# unrelated comment\n"
        )

        result = helper.build_candidate_table(
            source,
            marker=MARKER,
            command=COMMAND,
            schedule=SCHEDULE,
            log_path=LOG_PATH,
        )

        self.assertEqual(result.count("0 * * * * /other/job\n"), 2)
        self.assertIn("# unrelated comment\n", result)
        self.assertEqual(result.count(MARKER), 1)
        self.assertEqual(result.count(COMMAND), 1)

    def test_build_preserves_comments_monitors_wrappers_and_marker_prefix_collisions(self):
        helper = _helper()
        source = (
            f"{MARKER}-backup\n"
            f"# comment mentioning {COMMAND}\n"
            f"*/5 * * * * pgrep -af '{COMMAND}'\n"
            f"*/5 * * * * /usr/local/bin/monitor --command '{COMMAND}'\n"
            f"*/5 * * * * /bin/sh -c '{COMMAND} >> /other.log 2>&1; echo ok'\n"
        )

        result = helper.build_candidate_table(
            source,
            marker=MARKER,
            command=COMMAND,
            schedule=SCHEDULE,
            log_path=LOG_PATH,
        )

        self.assertTrue(result.startswith(source))
        self.assertEqual(result.count(MARKER), 2)
        self.assertEqual(result.count(COMMAND), 5)
        self.assertIn(f"{SCHEDULE} {COMMAND} >> {LOG_PATH} 2>&1", result)

    def test_build_rejects_prefix_collision_followed_by_exact_entry(self):
        helper = _helper()
        source = f"{MARKER}-backup\n{SCHEDULE} {COMMAND} >> /old.log 2>&1\n"

        with self.assertRaises(helper.CronTransformError):
            helper.build_candidate_table(
                source,
                marker=MARKER,
                command=COMMAND,
                schedule=SCHEDULE,
                log_path=LOG_PATH,
            )
        self.assertEqual(source, f"{MARKER}-backup\n{SCHEDULE} {COMMAND} >> /old.log 2>&1\n")

    def test_build_rejects_marker_owned_entry_with_extra_shell_operator(self):
        helper = _helper()
        source = f"{MARKER}\n{SCHEDULE} {COMMAND} && /usr/bin/echo monitor\n"

        with self.assertRaises(helper.CronTransformError):
            helper.build_candidate_table(
                source,
                marker=MARKER,
                command=COMMAND,
                schedule=SCHEDULE,
                log_path=LOG_PATH,
            )

    def test_build_rejects_no_space_shell_operator_after_command(self):
        helper = _helper()
        for operator in ("&&", ">>", ";", "|", "<", ">"):
            with self.subTest(operator=operator):
                source = (
                    f"{SCHEDULE} {COMMAND}{operator}/tmp/other.log 2>&1\n"
                )

                with self.assertRaises(helper.CronTransformError):
                    helper.build_candidate_table(
                        source,
                        marker=MARKER,
                        command=COMMAND,
                        schedule=SCHEDULE,
                        log_path=LOG_PATH,
                    )


    def test_build_preserves_command_prefix_collision(self):
        helper = _helper()
        source = f"0 * * * * {COMMAND}-wrapper --watch\n"

        result = helper.build_candidate_table(
            source,
            marker=MARKER,
            command=COMMAND,
            schedule=SCHEDULE,
            log_path=LOG_PATH,
        )

        self.assertTrue(result.startswith(source))


    def test_build_rejects_unmarked_pipeline_duplicates_without_mutating_source(self):
        helper = _helper()
        source = f"0 1 * * * {COMMAND} >> /unmarked.log 2>&1\n"

        with self.assertRaises(helper.CronTransformError):
            helper.build_candidate_table(
                source,
                marker=MARKER,
                command=COMMAND,
                schedule=SCHEDULE,
                log_path=LOG_PATH,
            )
        self.assertEqual(source.count(COMMAND), 1)

    def test_build_quotes_command_argv_and_classifies_the_generated_line(self):
        helper = _helper()
        command = shlex.join(["/install root/bin/anime;cloud", "run"])

        result = helper.build_candidate_table(
            "",
            marker=MARKER,
            command=command,
            schedule=SCHEDULE,
            log_path=LOG_PATH,
        )

        line = result.splitlines()[-1]
        payload = line.split(None, 5)[5]
        self.assertEqual(helper._command_kind(line, command), "canonical")
        self.assertEqual(
            shlex.split(payload.rsplit(" >> ", 1)[0]),
            ["/install root/bin/anime;cloud", "run"],
        )

    def test_build_rejects_ambiguous_unquoted_command_variant(self):
        helper = _helper()
        command = shlex.join(["/install root/bin/anime;cloud", "run"])
        source = (
            f"{MARKER}\n"
            "/install root/bin/anime;cloud run >> /old.log 2>&1\n"
        )

        with self.assertRaises(helper.CronTransformError):
            helper.build_candidate_table(
                source,
                marker=MARKER,
                command=command,
                schedule=SCHEDULE,
                log_path=LOG_PATH,
            )

    def test_build_rejects_double_quoted_equivalent_command_prefix(self):
        helper = _helper()
        source = (
            f"{MARKER}\n"
            f'{SCHEDULE} "/install/current/bin/anime-cloud-pipeline" run '
            ">> /old.log 2>&1\n"
        )

        self.assertEqual(
            helper._command_kind(source.splitlines()[1], COMMAND),
            "ambiguous",
        )
        with self.assertRaises(helper.CronTransformError):
            helper.build_candidate_table(
                source,
                marker=MARKER,
                command=COMMAND,
                schedule=SCHEDULE,
                log_path=LOG_PATH,
            )

    def test_disable_rejects_backslash_escaped_equivalent_command_prefix(self):
        helper = _helper()
        source = (
            f"{SCHEDULE} \\/install/current/bin/anime-cloud-pipeline run "
            ">> /old.log 2>&1\n"
        )

        self.assertEqual(helper._command_kind(source, COMMAND), "ambiguous")
        with self.assertRaises(helper.CronTransformError):
            helper.disable_pipeline_entries(
                source,
                marker=MARKER,
                command=COMMAND,
            )

    def test_build_rejects_malformed_shell_after_equivalent_command_prefix(self):
        helper = _helper()
        sources = (
            (
                f'{SCHEDULE} "/install/current/bin/anime-cloud-pipeline" run '
                '"unterminated\n'
            ),
            f'{SCHEDULE} "/install/current/bin/anime-cloud-pipeline run >> /old.log 2>&1\n',
        )

        for source in sources:
            with self.subTest(source=source):
                self.assertEqual(
                    helper._command_kind(source, COMMAND),
                    "ambiguous",
                )
                with self.assertRaises(helper.CronTransformError):
                    helper.build_candidate_table(
                        source,
                        marker=MARKER,
                        command=COMMAND,
                        schedule=SCHEDULE,
                        log_path=LOG_PATH,
                    )

    def test_build_and_disable_reject_equivalent_first_word_with_malformed_remainder(self):
        helper = _helper()
        path = COMMAND.split()[0]
        sources = (
            f'{SCHEDULE} "{path}" ru"n "unterminated\n',
            f'{SCHEDULE} ""{path} run "unterminated\n',
            f'{SCHEDULE} /install/current/bin/anime\\-cloud\\-pipeline run "unterminated\n',
            f'{SCHEDULE} \\/install/current/bin/anime-cloud-pipeline run "unterminated\n',
        )

        for source in sources:
            with self.subTest(source=source):
                self.assertEqual(
                    helper._command_kind(source, COMMAND),
                    "ambiguous",
                )
                with self.assertRaises(helper.CronTransformError):
                    helper.build_candidate_table(
                        source,
                        marker=MARKER,
                        command=COMMAND,
                        schedule=SCHEDULE,
                        log_path=LOG_PATH,
                    )
                with self.assertRaises(helper.CronTransformError):
                    helper.disable_pipeline_entries(
                        source,
                        marker=MARKER,
                        command=COMMAND,
                    )

    def test_build_and_disable_reject_managed_path_constructed_before_malformed_quote(self):
        helper = _helper()
        sources = (
            f"{SCHEDULE} /install/current/bin/'anime-cloud-pipeline run >> /old.log 2>&1\n",
            f"{SCHEDULE} /install/current/bin/anime-'cloud-pipeline run >> /old.log 2>&1\n",
            f"{SCHEDULE} /install/current/bin/anime-cloud-pipeline' run >> /old.log 2>&1\n",
        )

        for source in sources:
            with self.subTest(source=source):
                self.assertEqual(
                    helper._command_kind(source, COMMAND),
                    "ambiguous",
                )
                with self.assertRaises(helper.CronTransformError):
                    helper.build_candidate_table(
                        source,
                        marker=MARKER,
                        command=COMMAND,
                        schedule=SCHEDULE,
                        log_path=LOG_PATH,
                    )
                with self.assertRaises(helper.CronTransformError):
                    helper.disable_pipeline_entries(
                        source,
                        marker=MARKER,
                        command=COMMAND,
                    )

    def test_build_and_disable_preserve_unrelated_malformed_double_quoted_executables(self):
        helper = _helper()
        path = COMMAND.split()[0]
        escaped_hyphen = path.replace("-", "\\-")
        escaped_slash = path.replace("/", "\\/")
        sources = (
            f'{SCHEDULE} "{escaped_hyphen} run unterminated\n',
            f'{SCHEDULE} "{escaped_slash} run unterminated\n',
        )

        for source in sources:
            with self.subTest(source=source):
                self.assertEqual(helper._command_kind(source, COMMAND), "other")
                built = helper.build_candidate_table(
                    source,
                    marker=MARKER,
                    command=COMMAND,
                    schedule=SCHEDULE,
                    log_path=LOG_PATH,
                )
                self.assertTrue(built.startswith(source))
                self.assertEqual(
                    helper.disable_pipeline_entries(
                        source,
                        marker=MARKER,
                        command=COMMAND,
                    ),
                    source,
                )

    def test_disable_removes_pipeline_entries_and_preserves_unrelated_duplicates(self):
        helper = _helper()
        source = (
            "0 * * * * /other/job\n"
            "0 * * * * /other/job\n"
            f"{MARKER}\n{SCHEDULE} {COMMAND} >> /managed.log 2>&1\n"
            f"1 1 * * * {COMMAND} >> /unmarked.log 2>&1\n"
        )

        result = helper.disable_pipeline_entries(
            source,
            marker=MARKER,
            command=COMMAND,
        )

        self.assertNotIn(MARKER, result)
        self.assertNotIn(COMMAND, result)
        self.assertEqual(result.count("0 * * * * /other/job\n"), 2)

    def test_disable_removes_exact_entry_but_preserves_mentions_and_prefix_collisions(self):
        helper = _helper()
        exact = f"{SCHEDULE} {COMMAND} >> /managed.log 2>&1\n"
        source = (
            f"{MARKER}-backup\n"
            f"# comment mentioning {COMMAND}\n"
            f"*/5 * * * * pgrep -af '{COMMAND}'\n"
            f"*/5 * * * * /usr/local/bin/monitor --command '{COMMAND}'\n"
            f"{exact}"
        )

        result = helper.disable_pipeline_entries(
            source,
            marker=MARKER,
            command=COMMAND,
        )

        self.assertEqual(
            result,
            source.replace(exact, ""),
        )

    def test_disable_rejects_direct_pipeline_entry_with_extra_shell_operator(self):
        helper = _helper()
        source = f"{SCHEDULE} {COMMAND} && /usr/bin/echo monitor\n"

        with self.assertRaises(helper.CronTransformError):
            helper.disable_pipeline_entries(
                source,
                marker=MARKER,
                command=COMMAND,
            )

    def test_disable_rejects_no_space_shell_operator_after_command(self):
        helper = _helper()
        for operator in ("&&", ">>", ";", "|", "<", ">"):
            with self.subTest(operator=operator):
                source = (
                    f"{SCHEDULE} {COMMAND}{operator}/tmp/other.log 2>&1\n"
                )

                with self.assertRaises(helper.CronTransformError):
                    helper.disable_pipeline_entries(
                        source,
                        marker=MARKER,
                        command=COMMAND,
                    )


    def test_build_rejects_unsafe_log_paths_without_emitting_them(self):
        helper = _helper()
        invalid_paths = (
            "relative.log",
            "/managed log",
            "/managed.log\twith-tab",
            "/managed.log\x00with-nul",
            "/managed.log;id",
            "/managed.log&id",
            "/managed.log|id",
            "/managed.log<id",
            "/managed.log>id",
            "/managed.log`id`",
            "/managed.log$HOME",
            "/managed.log'quoted'",
            '/managed.log"quoted"',
            r"/managed.log\escaped",
            "/managed.log%id",
            "/managed/(pipeline).log",
            "/managed/*.log",
            "/managed/?.log",
            "/managed/[0].log",
            "/managed/../managed.log",
            "/managed//managed.log",
            "/managed/./managed.log",
            "//managed.log",
        )

        for invalid_path in invalid_paths:
            with self.subTest(log_path=repr(invalid_path)):
                with self.assertRaises(helper.CronTransformError):
                    helper.build_candidate_table(
                        "",
                        marker=MARKER,
                        command=COMMAND,
                        schedule=SCHEDULE,
                        log_path=invalid_path,
                    )

    def test_disable_rejects_unsafe_log_paths_in_existing_entries(self):
        helper = _helper()
        invalid_paths = (
            "/managed.log;id",
            "/managed.log`id`",
            "/managed.log$HOME",
            "/managed.log'quoted'",
            '/managed.log"quoted"',
            r"/managed.log\escaped",
            "/managed.log%id",
            "/managed/(pipeline).log",
            "/managed/*.log",
            "/managed/?.log",
            "/managed/[0].log",
            "/managed/../managed.log",
            "/managed//managed.log",
            "/managed/./managed.log",
        )

        for invalid_path in invalid_paths:
            with self.subTest(log_path=repr(invalid_path)):
                source = (
                    f"{MARKER}\n"
                    f"{SCHEDULE} {COMMAND} >> {invalid_path} 2>&1\n"
                )

                with self.assertRaises(helper.CronTransformError):
                    helper.disable_pipeline_entries(
                        source,
                        marker=MARKER,
                        command=COMMAND,
                    )

    def test_disable_classifies_glob_and_shell_word_syntax_as_ambiguous(self):
        helper = _helper()
        invalid_paths = (
            "/managed/(pipeline).log",
            "/managed/*.log",
            "/managed/?.log",
            "/managed/[0].log",
        )

        for invalid_path in invalid_paths:
            with self.subTest(log_path=invalid_path):
                line = f"{SCHEDULE} {COMMAND} >> {invalid_path} 2>&1\n"
                self.assertEqual(helper._command_kind(line, COMMAND), "ambiguous")
                with self.assertRaises(helper.CronTransformError):
                    helper.disable_pipeline_entries(
                        line,
                        marker=MARKER,
                        command=COMMAND,
                    )


class HermesCronCliTests(unittest.TestCase):
    def test_cli_rejects_arbitrary_marker_without_stdout(self):
        helper = _helper()
        untrusted_markers = (
            "* * * * * /bin/touch /tmp/hermes-marker-injection",
            f"{MARKER}\n* * * * * /bin/touch /tmp/hermes-marker-injection",
            f"{MARKER}\r\n* * * * * /bin/touch /tmp/hermes-marker-injection",
            f"{MARKER}\x00",
            f"{MARKER} # comment-confusion",
            f" {MARKER}",
        )

        for operation in ("build", "disable"):
            for marker in untrusted_markers:
                with self.subTest(operation=operation, marker=repr(marker)):
                    stdout = io.StringIO()
                    stderr = io.StringIO()
                    args = [
                        operation,
                        "--marker",
                        marker,
                        "--command",
                        COMMAND,
                    ]
                    if operation == "build":
                        args.extend(
                            [
                                "--schedule",
                                SCHEDULE,
                                "--log-path",
                                LOG_PATH,
                            ]
                        )
                    with redirect_stdout(stdout), redirect_stderr(stderr), mock.patch(
                        "sys.stdin", io.StringIO("0 * * * * /other/job\n")
                    ):
                        exit_code = helper.main(args)

                    self.assertEqual(exit_code, 2)
                    self.assertEqual(stdout.getvalue(), "")
                    self.assertIn("marker", stderr.getvalue())

    def test_cli_reads_captured_crontab_and_emits_candidate(self):
        helper = _helper()
        stdout = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(io.StringIO()), mock.patch(
            "sys.stdin", io.StringIO("0 * * * * /other/job\n")
        ):
            exit_code = helper.main(
                [
                    "build",
                    "--marker",
                    MARKER,
                    "--command",
                    COMMAND,
                    "--schedule",
                    SCHEDULE,
                    "--log-path",
                    LOG_PATH,
                ]
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(stdout.getvalue().count(MARKER), 1)
        self.assertEqual(stdout.getvalue().count(COMMAND), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
