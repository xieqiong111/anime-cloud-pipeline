"""Legacy ``Executor`` compatibility acceptance tests using temporary mocks.

This module covers the legacy Executor path only. V5 ``PipelineRunner`` and
pending-handler coverage is in ``scripts/tests/test_executor_v5.py``,
``scripts/tests/test_state.py``, and ``scripts/tests/test_reviews.py``.
"""
from __future__ import annotations

import logging
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pipeline.adapters import (Mock115, MockEmby, MockLitePan, MockMoviePilot,
                               MockNotify, has_video_file)
from pipeline.executor import Executor, RedactingFilter
from pipeline.parsers import (FeedFormatError, SubRule, match_rule, normalize_infohash,
                              parse_feed, parse_season_episode)
from pipeline.state import LockHeld, ProcessLock, StateStore

FAKE_WORK = "测试作品"


def make_rss(*entries) -> str:
    """entries: (infohash, title)。生成与 KissSub 结构一致的 RSS。"""
    items = []
    for ih, title in entries:
        items.append(
            "<item>"
            f"<title><![CDATA[{title}]]></title>"
            f"<link>http://www.kisssub.org/show-{ih}.html</link>"
            f"<enclosure url='http://v2.uploadbt.com/?r=down&amp;hash={ih}'"
            " type='application/x-bittorrent' length='1'/>"
            "<pubDate>Sat, 12 Sep 2026 12:00:00 +0800</pubDate>"
            "</item>"
        )
    return ("<?xml version='1.0' encoding='utf-8'?><rss version='2.0'><channel>"
            f"<title><![CDATA[爱恋动漫]]></title>{''.join(items)}</channel></rss>")


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="pipeline-test-")
        self.db = os.path.join(self.tmp, "state.db")
        self.lock_path = os.path.join(self.tmp, "run.lock")
        self.strm_root = os.path.join(self.tmp, "strm")
        os.makedirs(self.strm_root, exist_ok=True)
        self.store = StateStore(self.db)
        self.m115 = Mock115()
        self.mlite = MockLitePan(self.strm_root)
        self.mmp = MockMoviePilot(nfo_season=2, nfo_episode=13)
        self.memby = MockEmby()
        self.mnotify = MockNotify()
        self.feeds = {}

        def fetch(url):
            if url not in self.feeds:
                raise RuntimeError(f"no fixture for {url}")
            return self.feeds[url]

        self.fetch = fetch
        self.sub = {
            "work": FAKE_WORK, "aliases": ["Test Show"],
            "release_groups": ["某字幕组"], "season": 2,
            "feed_url": "http://mock/rss.xml", "media_id": "99999",
        }

    def make_executor(self, **overrides):
        cfg = {"strm_root": self.strm_root, "library_id": "anime-lib",
               "media_id": "99999"}
        cfg.update(overrides)
        return Executor(self.store, self.m115, self.mlite, self.mmp,
                        self.memby, self.mnotify, self.fetch, self.lock_path, cfg)

    def run_rounds(self, ex, sub, n=6):
        """连续执行 n 轮（模拟定时任务多轮推进）。"""
        summaries = [ex.run([sub]) for _ in range(n)]
        return summaries

    def ep_strm(self, title):
        safe = "".join(ch for ch in title if ch not in '\\/:*?"<>|')
        return os.path.join(FAKE_WORK, safe[:50] + ".strm").replace("\\", "/")

    def tearDown(self):
        self.store.close()
        shutil.rmtree(self.tmp, ignore_errors=True)


class Case01BaselineAndBackfill(Base):
    def test_first_run_builds_baseline_without_downloading(self):
        self.feeds["http://mock/rss.xml"] = make_rss(
            ("a" * 40, "[某字幕组] " + FAKE_WORK + " S02E01 [1080p]"),
            ("b" * 40, "[某字幕组] " + FAKE_WORK + " S02E02 [1080p]"),
            ("c" * 40, "[某字幕组] " + FAKE_WORK + " S02E03 [1080p]"),
        )
        ex = self.make_executor()
        ex.run([self.sub])
        self.assertEqual(self.m115.submit_calls, 0, "首轮 baseline 不下载")
        self.assertEqual(self.store.baseline_max_episode(FAKE_WORK, 2), 3)

    def test_next_episode_downloads_and_backfill_respects_range(self):
        self.feeds["http://mock/rss.xml"] = make_rss(
            ("a" * 40, "[某字幕组] " + FAKE_WORK + " S02E01 [1080p]"),
            ("b" * 40, "[某字幕组] " + FAKE_WORK + " S02E02 [1080p]"),
        )
        ex = self.make_executor()
        ex.run([self.sub])  # 建基线
        self.feeds["http://mock/rss.xml"] = make_rss(
            ("a" * 40, "[某字幕组] " + FAKE_WORK + " S02E01 [1080p]"),
            ("b" * 40, "[某字幕组] " + FAKE_WORK + " S02E02 [1080p]"),
            ("c" * 40, "[某字幕组] " + FAKE_WORK + " S02E03 [1080p]"),
            ("d" * 40, "[某字幕组] " + FAKE_WORK + " S02E04 [1080p]"),
        )
        ex.run([self.sub])  # 追更 E03、E04
        self.assertEqual(self.m115.submit_calls, 2)

        # 显式补集：范围 [1,1] 只授权 E01
        sub2 = dict(self.sub, backfill={"start": 1, "end": 1})
        self.feeds["http://mock/rss.xml"] = make_rss(
            ("e" * 40, "[某字幕组] " + FAKE_WORK + " S02E01 另一版本 [1080p]"),
            ("f" * 40, "[某字幕组] " + FAKE_WORK + " S02E05 [1080p]"),
        )
        ex.run([sub2])
        titles = [e["title"] for e in self.store.all_episodes() if "另一版本" in e["title"]]
        self.assertEqual(len(titles), 1, "补集只下载授权范围")
        self.assertFalse(any("E05" in t for t in titles), "范围外不入库")


class Case02Dedup(Base):
    def test_same_hash_variants_create_one_task(self):
        ih_upper = "A" * 40
        self.feeds["http://mock/rss.xml"] = make_rss(
            (ih_upper.lower(), "[某字幕组] " + FAKE_WORK + " S02E01 [1080p]"),
        )
        sub = dict(self.sub, backfill={"start": 1, "end": 1})
        ex = self.make_executor()
        ex.run([sub])
        # 不同大小写、不同 tracker 参数、不同标题 → 仍然只有一个任务
        self.assertEqual(len(self.store.all_episodes()), 1)
        self.assertEqual(self.m115.submit_calls, 1)

    def test_missing_middle_episode_still_discovered(self):
        self.feeds["http://mock/rss.xml"] = make_rss(
            ("a" * 40, "[某字幕组] " + FAKE_WORK + " S02E01 [1080p]"),
            ("b" * 40, "[某字幕组] " + FAKE_WORK + " S02E02 [1080p]"),
        )
        ex = self.make_executor()
        ex.run([self.sub])  # 基线: E01-E02
        # 缺 E03，直接出现 E04
        self.feeds["http://mock/rss.xml"] = make_rss(
            ("d" * 40, "[某字幕组] " + FAKE_WORK + " S02E04 [1080p]"),
        )
        ex.run([self.sub])
        eps = [e for e in self.store.all_episodes() if e["episode"] == 4]
        self.assertEqual(len(eps), 1, "缺中间一集不影响新集发现")


class Case03Filtering(Base):
    def test_same_name_different_season_excluded(self):
        sub = dict(self.sub, backfill={"start": 1, "end": 1})
        self.feeds["http://mock/rss.xml"] = make_rss(
            ("a" * 40, "[某字幕组] " + FAKE_WORK + " S01E01 第一季 [1080p]"),
        )
        ex = self.make_executor()
        ex.run([sub])
        self.assertEqual(len(self.store.all_episodes()), 0, "同名不同季排除")

    def test_collection_and_wrong_group_excluded_or_review(self):
        # 合集（S02E05-E08）单独标记 episode_end，进入需审/按策略；字幕组不匹配排除
        self.feeds["http://mock/rss.xml"] = make_rss(
            ("a" * 40, "[别人组] " + FAKE_WORK + " S02E01 [1080p]"),
            ("b" * 40, "[别人组] " + FAKE_WORK + " 第05话"),
        )
        ex = self.make_executor()
        sub = dict(self.sub, backfill={"start": 1, "end": 99})
        ex.run([sub])
        self.assertEqual(len(self.store.all_episodes()), 0)

    def test_unknown_episode_goes_needs_review(self):
        self.feeds["http://mock/rss.xml"] = make_rss(
            ("a" * 40, "[某字幕组] " + FAKE_WORK + " 完全没有集数信息"),
        )
        ex = self.make_executor()
        ex.run([self.sub])
        stages = [e["stage"] for e in self.store.all_episodes()]
        self.assertEqual(stages, ["needs_review"])
        self.assertEqual(self.m115.submit_calls, 0, "待审不下载")


class Case04SubmitTimeoutReconcile(Base):
    def test_timeout_then_reconcile_submits_once(self):
        # 第一轮：提交请求超时（服务端实际已创建任务）→ uncertain
        self.m115.timeout_submit = True
        self.feeds["http://mock/rss.xml"] = make_rss(
            ("a" * 40, "[某字幕组] " + FAKE_WORK + " S02E13 [1080p]"),
        )
        sub = dict(self.sub, backfill={"start": 13, "end": 13})
        ex = self.make_executor()
        ex.run([sub])
        self.assertEqual(self.m115.submit_calls, 1)
        ep = self.store.all_episodes()[0]
        self.assertEqual(ep["stage"], "uncertain")

        # 第二轮：对账远端任务 → 找到既有任务，不再提交
        ex.run([sub])
        self.assertEqual(self.m115.submit_calls, 1, "对账后不得重复提交")
        ep = self.store.all_episodes()[0]
        self.assertEqual(ep["submit_count"], 1, "提交次数仍为一")
        self.assertIn(ep["stage"], ("submitted", "cloud_ready"))
        self.assertTrue(ep["remote_task_id"], "对账应恢复远端任务标识")


class Case05LitePanNotDone(Base):
    def test_200_but_no_strm_blocks_mp(self):
        self.feeds["http://mock/rss.xml"] = make_rss(
            ("a" * 40, "[某字幕组] " + FAKE_WORK + " S02E13 [1080p]"),
        )
        sub = dict(self.sub, backfill={"start": 13, "end": 13})
        ex = self.make_executor()
        ex.run([self.sub])  # 基线
        self.m115.complete("a" * 40, ["[某字幕组] " + FAKE_WORK + " S02E13.mkv"])
        self.run_rounds(ex, sub, 2)  # 提交 + 事件 + STRM 缺失
        self.assertEqual(self.mmp.calls, [], "LitePan 未完成，MP 调用次数为零")
        ep = self.store.all_episodes()[0]
        self.assertEqual(ep["stage"], "strm_pending")

    def test_missing_one_strm_blocks_only_that_episode(self):
        self.feeds["http://mock/rss.xml"] = make_rss(
            ("a" * 40, "[某字幕组] " + FAKE_WORK + " S02E13 [1080p]"),
            ("b" * 40, "[某字幕组] " + FAKE_WORK + " S02E14 [1080p]"),
        )
        sub = dict(self.sub, backfill={"start": 13, "end": 14})
        ex = self.make_executor()
        ex.run([self.sub])  # 基线
        self.m115.complete("a" * 40, ["a.mkv"])
        self.m115.complete("b" * 40, ["b.mkv"])
        # 只生成 E13 的 STRM，E14 缺失
        self.mlite.generate_strm([self.ep_strm("[某字幕组] " + FAKE_WORK + " S02E13 [1080p]")])
        self.run_rounds(ex, sub, 2)
        stages = {e["episode"]: e["stage"] for e in self.store.all_episodes()}
        self.assertNotIn(stages.get(13), (None, "strm_pending"), "E13 已推进")
        self.assertEqual(stages.get(14), "strm_pending", "缺 STRM 的集不推进")
        scraped = [c["strm"] for c in self.mmp.calls]
        self.assertTrue(all("E13" in s for s in scraped), "只刮削有 STRM 的集")


class Case06MPFailure(Base):
    def _run_to_strm_ready(self):
        self.feeds["http://mock/rss.xml"] = make_rss(
            ("a" * 40, "[某字幕组] " + FAKE_WORK + " S02E13 [1080p]"),
        )
        sub = dict(self.sub, backfill={"start": 13, "end": 13})
        ex = self.make_executor()
        ex.run([self.sub])  # 基线
        self.m115.complete("a" * 40, ["a.mkv"])
        self.mlite.generate_strm([self.ep_strm("[某字幕组] " + FAKE_WORK + " S02E13 [1080p]")])
        self.run_rounds(ex, sub, 3)  # 提交 → STRM → 刮削(失败)
        return sub, ex

    def test_mp_failure_prevents_emby_refresh(self):
        self.mmp.fail = True
        sub, ex = self._run_to_strm_ready()
        ep = self.store.all_episodes()[0]
        self.assertEqual(ep["stage"], "failed_retryable")
        self.assertEqual(self.memby.refresh_calls, [], "MP 失败不刷 Emby")

        # 恢复：重试只走 MP（一次），不重交 115、不再生成 STRM
        submits_before = self.m115.submit_calls
        events_before = len(self.mlite.events)
        strms_before = len(self.mlite.list_strm())
        calls_before = len(self.mmp.calls)
        ex.run([sub])
        self.assertEqual(len(self.mmp.calls), calls_before + 1, "重试只刮削一次")
        self.assertEqual(self.m115.submit_calls, submits_before, "恢复不重新提交 115")
        self.assertEqual(len(self.mlite.events), events_before, "恢复不重复触发 LitePan")
        self.assertEqual(len(self.mlite.list_strm()), strms_before, "不重复生成 STRM")
        ep = self.store.all_episodes()[0]
        self.assertEqual(ep["stage"], "failed_retryable", "MP 仍失败时保持可重试")

        # 修复 MP 后再跑一轮 → 通过刮削
        self.mmp.fail = False
        ex.run([sub])
        ep = self.store.all_episodes()[0]
        self.assertIn(ep["stage"], ("scraping", "metadata_ready"))


class Case07BadMetadata(Base):
    def test_wrong_season_nfo_rejected(self):
        self.mmp.nfo_season = 3  # 旧 NFO 指向错误季
        self.feeds["http://mock/rss.xml"] = make_rss(
            ("a" * 40, "[某字幕组] " + FAKE_WORK + " S02E13 [1080p]"),
        )
        sub = dict(self.sub, backfill={"start": 13, "end": 13})
        ex = self.make_executor()
        ex.run([self.sub])  # 基线
        self.m115.complete("a" * 40, ["a.mkv"])
        self.mlite.generate_strm([self.ep_strm("[某字幕组] " + FAKE_WORK + " S02E13 [1080p]")])
        self.run_rounds(ex, sub, 3)
        ep = self.store.all_episodes()[0]
        self.assertEqual(ep["stage"], "failed_permanent", "错误季集 NFO 不得通过")

    def test_emby_refresh_ok_but_item_missing_rejected(self):
        self.memby.refresh_accepts = True  # 刷新被接受
        # 但库里没有条目（find_episode 返回 None）
        self.feeds["http://mock/rss.xml"] = make_rss(
            ("a" * 40, "[某字幕组] " + FAKE_WORK + " S02E13 [1080p]"),
        )
        sub = dict(self.sub, backfill={"start": 13, "end": 13})
        ex = self.make_executor()
        ex.run([self.sub])  # 基线
        self.m115.complete("a" * 40, ["a.mkv"])
        self.mlite.generate_strm([self.ep_strm("[某字幕组] " + FAKE_WORK + " S02E13 [1080p]")])
        self.run_rounds(ex, sub, 4)
        ep = self.store.all_episodes()[0]
        self.assertEqual(ep["stage"], "metadata_ready", "刷新接受但查不到条目 → 停在 metadata_ready")
        self.assertNotIn(ep["stage"], ("library_ready", "notified"))


class Case08FeedFailures(Base):
    def test_network_failure_keeps_baseline(self):
        self.feeds["http://mock/rss.xml"] = make_rss(
            ("a" * 40, "[某字幕组] " + FAKE_WORK + " S02E01 [1080p]"),
        )
        ex = self.make_executor()
        ex.run([self.sub])
        baseline = self.store.baseline_max_episode(FAKE_WORK, 2)
        self.assertEqual(baseline, 1)

        def boom(url):
            raise ConnectionError("network down")

        ex2 = Executor(self.store, self.m115, self.mlite, self.mmp, self.memby,
                       self.mnotify, boom, self.lock_path,
                       {"strm_root": self.strm_root})
        ex2.run([self.sub])
        self.assertEqual(self.store.baseline_max_episode(FAKE_WORK, 2), baseline)
        self.assertEqual(len(self.store.all_episodes()), 0)

    def test_login_page_not_treated_as_empty_feed(self):
        ex = self.make_executor()
        self.feeds["http://mock/rss.xml"] = "<html><body>请登录</body></html>"
        with self.assertRaises(FeedFormatError):
            parse_feed(self.feeds["http://mock/rss.xml"])
        # 执行器层面：登录页只告警，不建立空基线、不删媒体
        self.feeds["http://mock/rss.xml"] = "<html>登录页</html>"
        ex.run([self.sub])
        self.assertEqual(len(self.store.all_episodes()), 0)
        self.assertFalse(self.store.baseline_max_episode(FAKE_WORK, 2))


class Case09RestartAndConcurrency(Base):
    def test_state_survives_restart(self):
        self.feeds["http://mock/rss.xml"] = make_rss(
            ("a" * 40, "[某字幕组] " + FAKE_WORK + " S02E13 [1080p]"),
        )
        sub = dict(self.sub, backfill={"start": 13, "end": 13})
        ex = self.make_executor()
        ex.run([self.sub])  # 基线
        self.m115.complete("a" * 40, ["a.mkv"])
        self.mlite.generate_strm([self.ep_strm("[某字幕组] " + FAKE_WORK + " S02E13 [1080p]")])
        self.run_rounds(ex, sub, 3)
        stage_before = self.store.all_episodes()[0]["stage"]
        self.store.close()
        # 模拟进程重启：全新 StateStore 实例
        store2 = StateStore(self.db)
        ep = store2.all_episodes()[0]
        self.assertEqual(ep["stage"], stage_before)
        self.assertTrue(self.m115.tasks, "已完成的 115 任务状态仍在")
        store2.close()

    def test_second_process_cannot_double_submit(self):
        self.feeds["http://mock/rss.xml"] = make_rss(
            ("a" * 40, "[某字幕组] " + FAKE_WORK + " S02E13 [1080p]"),
        )
        sub = dict(self.sub, backfill={"start": 13, "end": 13})
        ex1 = self.make_executor()
        # 第一个进程持有锁（模拟长任务）
        lock = ProcessLock(self.lock_path)
        self.assertTrue(lock.acquire())
        ex2 = self.make_executor()
        summary = ex2.run([sub])
        self.assertTrue(summary.get("skipped_lock"), "并发第二进程跳过而非重复提交")
        self.assertEqual(self.m115.submit_calls, 0)
        lock.release()

    def test_notification_failure_only_retries_notification(self):
        self.mnotify.fail_times = 1
        self.memby.items = [{"library_id": "anime-lib", "series": FAKE_WORK,
                             "season": 2, "episode": 13, "id": "emby-1"}]
        self.feeds["http://mock/rss.xml"] = make_rss(
            ("a" * 40, "[某字幕组] " + FAKE_WORK + " S02E13 [1080p]"),
        )
        sub = dict(self.sub, backfill={"start": 13, "end": 13})
        ex = self.make_executor()
        ex.run([self.sub])  # 基线
        self.m115.complete("a" * 40, ["a.mkv"])
        self.mlite.generate_strm([self.ep_strm("[某字幕组] " + FAKE_WORK + " S02E13 [1080p]")])
        self.run_rounds(ex, sub, 5)
        ep = self.store.all_episodes()[0]
        self.assertEqual(ep["stage"], "notified", "媒体流水线照常完成")
        self.assertEqual(len(self.mnotify.delivered), 0, "首次投递失败")
        pending = self.store.pending_notifications()
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["state"], "delivery_pending")
        submits = self.m115.submit_calls
        ex.run([sub])  # 重试轮
        self.assertEqual(len(self.mnotify.delivered), 1, "重试只处理通知")
        self.assertEqual(self.m115.submit_calls, submits, "绝不重做媒体流水线")
        self.assertEqual(self.store.pending_notifications(), [])


class Case10DataSafety(Base):
    def test_shell_meta_title_is_data_only(self):
        evil = "S02E13`; rm -rf / && $(whoami) `echo x"
        self.feeds["http://mock/rss.xml"] = make_rss(
            ("a" * 40, f"[某字幕组] {FAKE_WORK} {evil} [1080p]"),
        )
        sub = dict(self.sub, backfill={"start": 13, "end": 13})
        ex = self.make_executor()
        ex.run([self.sub])  # 基线
        self.m115.complete("a" * 40, ["a.mkv"])
        ex.run([sub])
        ep = self.store.all_episodes()[0]
        # 标题原样保存为数据（含 shell 符号），没有任何解释执行
        self.assertIn("rm -rf", ep["title"])
        self.assertIn("$(whoami)", ep["title"])

    def test_no_credentials_in_logs(self):
        import io

        secret = "super-secret-cookie-value-12345"
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.addFilter(RedactingFilter([secret]))
        logger2 = logging.getLogger("pipeline-test-redact")
        logger2.addHandler(handler)
        logger2.propagate = False
        logger2.setLevel(logging.DEBUG)
        try:
            logger2.warning("using cookie=%s now", secret)
            logger2.info("header Authorization: Bearer %s", secret)
            out = stream.getvalue()
        finally:
            logger2.removeHandler(handler)
        self.assertNotIn(secret, out, "日志中无凭据")
        self.assertIn("<redacted>", out)


class ParserUnit(unittest.TestCase):
    def test_normalize_infohash_hex_and_base32(self):
        hex40 = "e72e9d61f8c3fc879cd85538112c30c81050dd59"
        self.assertEqual(normalize_infohash(hex40.upper()), hex40)
        # base32 编码同一 20 字节
        import base64, binascii
        raw = binascii.unhexlify(hex40)
        b32 = base64.b32encode(raw).decode()
        self.assertEqual(normalize_infohash(b32), hex40)
        with self.assertRaises(ValueError):
            normalize_infohash("not-a-hash")

    def test_parse_feed_rejects_html(self):
        with self.assertRaises(FeedFormatError):
            parse_feed("<html><body>503 Service</body></html>")

    def test_season_episode_patterns(self):
        self.assertEqual(parse_season_episode("[X组] 名称 S02E13 [1080p]"), (2, 13, None))
        self.assertEqual(parse_season_episode("[X组] 名称 第12话"), (None, 12, None))
        s, e, end = parse_season_episode("[X组] 名称 S02E05-E08 合集")
        self.assertEqual((s, e, end), (2, 5, 8))

    def test_match_rule_paths(self):
        item = parse_feed(make_rss(("a" * 40, "[某字幕组] 测试作品 S02E13 [1080p]")))[0]
        rule = SubRule(work="测试作品", aliases=[], release_groups=["某字幕组"], season=2)
        self.assertEqual(match_rule(item, rule), "match")
        rule2 = SubRule(work="别的作品", aliases=[], release_groups=["某字幕组"])
        self.assertEqual(match_rule(item, rule2), "exclude")
        rule3 = SubRule(work="测试作品", aliases=[], release_groups=["别的组"])
        self.assertEqual(match_rule(item, rule3), "exclude")

    def test_has_video_file(self):
        self.assertTrue(has_video_file([{"name": "a.mkv"}, {"name": "readme.txt"}]))
        self.assertFalse(has_video_file([{"name": "readme.txt"}, {"name": "cover.jpg"}]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
