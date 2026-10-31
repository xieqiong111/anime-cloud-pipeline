"""回归测试：覆盖 CLI/配置/认证修复，不访问真实服务。"""
import datetime as dt
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from anime_pipeline import _parse_backfill
from pipeline.parsers import SourceItem, published_on
from pipeline.real_adapters import MoviePilotClient


class RegressionTests(unittest.TestCase):
    def test_skill_exposes_hermes_entrypoint_and_single_scheduler(self):
        text = (ROOT / "SKILL.md").read_text(encoding="utf-8")
        self.assertIn("bin/anime-cloud-pipeline", text)
        self.assertIn("NAS cron", text)
        self.assertIn("references/deployment-hermes.md", text)
        self.assertNotRegex(text, r"192\.168\.|task_id\s*[=:]\s*\d+|library_id\s*[=:]\s*\d+")
        self.assertNotRegex(
            text,
            r"(?im)Hermes[^.\n]{0,120}(?:starts?|runs?|owns?|creates?|installs?)[^.\n]{0,80}(?:scheduler|loop)",
        )
        self.assertNotRegex(text, r"(?i)\bHermes\s+(?:scheduler|loop)\b")

    def test_all_skill_reference_links_exist(self):
        text = (ROOT / "SKILL.md").read_text(encoding="utf-8")
        targets = re.findall(r"\[[^\]]+\]\(([^)]+)\)", text)
        self.assertTrue(targets, "Skill must link to operator references")
        for target in targets:
            with self.subTest(target=target):
                self.assertTrue((ROOT / target.split("#", 1)[0]).is_file(), target)

    def test_deployable_docs_use_semantic_paths(self):
        product_files = (
            ROOT / "SKILL.md",
            ROOT / "references" / "deployment-hermes.md",
            ROOT / "references" / "deployment-local.md",
            ROOT / "docs" / "V5-UPGRADE.md",
            ROOT / "CHANGELOG-V5.md",
            ROOT / "assets" / "config.example.json",
            ROOT / "deploy" / "config.v5.example.json",
            ROOT / "deploy" / "anime-cloud-pipeline.env.example",
        )
        forbidden = re.compile(
            r"(?<![A-Za-z0-9])/(?:srv|mnt|var|opt|home|root|media|volume|share)/"
            r"|(?<![A-Za-z0-9])[A-Za-z]:[\\/]"
            r"|\\\\"
        )
        for path in product_files:
            with self.subTest(path=path):
                self.assertNotRegex(path.read_text(encoding="utf-8"), forbidden)

    def test_packaged_tests_use_portable_fixture_roots(self):
        """Packaged tests must not depend on the build host's filesystem roots."""

        # Build the needles from semantic fragments so this guard does not
        # reintroduce the prohibited path literals into the package itself.
        forbidden_roots = tuple(
            re.escape(value)
            for value in (
                "D:" + "/" + "n" + "as",
                "D:" + "\\" + "n" + "as",
                "/" + "n" + "as" + "/",
                "/" + "s" + "rv/" + "a" + "nime",
            )
        )
        forbidden = re.compile("|".join(forbidden_roots), re.IGNORECASE)
        # Scan every packaged test module, including the legacy top-level
        # suites.  Keep the needles assembled from fragments so this guard
        # does not match its own source or intentional rejection fixtures.
        test_root = ROOT / "scripts"
        for path in sorted(test_root.rglob("test*.py")):
            with self.subTest(path=path.relative_to(ROOT)):
                self.assertIsNone(forbidden.search(path.read_text(encoding="utf-8")))

    def test_portability_guard_catches_top_level_test_fixture(self):
        """The portability guard must inspect top-level packaged test modules."""

        bad_root = "D:" + "/" + "n" + "as" + "/fixture"
        with tempfile.TemporaryDirectory(prefix="portability-tests-") as temp:
            fake_root = Path(temp)
            fixture = fake_root / "scripts" / "test_portability_fixture.py"
            fixture.parent.mkdir()
            fixture.write_text(f'FIXTURE_ROOT = "{bad_root}"\n', encoding="utf-8")
            with patch(__name__ + ".ROOT", fake_root):
                result = unittest.TestResult()
                RegressionTests(
                    "test_packaged_tests_use_portable_fixture_roots"
                ).run(result)
            self.assertTrue(
                result.failures,
                "top-level test fixtures must be covered by the portability guard",
            )

    def test_hermes_cron_runbook_uses_packaged_fail_closed_helper(self):
        text = (ROOT / "references" / "deployment-hermes.md").read_text(encoding="utf-8")
        self.assertIn("deploy/hermes_cron.py", text)
        self.assertNotRegex(text, r"crontab -l[^\n]*\|\|")
        self.assertNotRegex(text, r"\|\|\s*:")
        self.assertNotIn("awk -v marker", text)

    def test_previous_day_uses_declared_timezone(self):
        item = SourceItem("x", "", "a" * 40, "", "Sun, 13 Sep 2026 00:30:00 +0000")
        self.assertTrue(published_on(item, dt.date(2026, 9, 13), "Asia/Shanghai"))
        self.assertFalse(published_on(item, dt.date(2026, 9, 12), "Asia/Shanghai"))

    def test_backfill_is_strict_and_bounded(self):
        self.assertEqual(_parse_backfill("295071:1:1-3"), ("295071", 1, 1, 3))
        with self.assertRaises(SystemExit):
            _parse_backfill("295071:1:3-1")
        with self.assertRaises(SystemExit):
            _parse_backfill("295071:1:1")

    def test_moviepilot_v3_uses_x_api_key(self):
        with tempfile.NamedTemporaryFile("w", delete=False) as f:
            f.write("secret")
            path = f.name
        try:
            client = MoviePilotClient("http://mp", path)
            with patch("pipeline.real_adapters._http_json", return_value=(200, {})) as call:
                client.version()
            self.assertEqual(call.call_args.kwargs["headers"], {"X-API-KEY": "secret"})
            self.assertNotIn("secret", call.call_args.args[1])
        finally:
            os.unlink(path)

    def test_example_config_matches_v5_contract(self):
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(root, "assets", "config.example.json"), encoding="utf-8") as f:
            cfg = json.load(f)
        self.assertEqual(cfg["schema_version"], 5)
        self.assertNotIn("subscriptions", cfg)
        self.assertIn("token_env", cfg["services"]["moviepilot"])
        self.assertIn("api_key_env", cfg["services"]["emby"])
        self.assertIn("cloud_source_root", cfg["paths"])
        self.assertIn("max_task_pages", cfg["limits"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
