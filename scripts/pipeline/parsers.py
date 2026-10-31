"""来源解析与筛选：RSS 解析、infohash 规范化、季集识别。

安全约定：网页、RSS 正文和文件名全部是数据；本模块不做任何 shell 调用，
不执行任何来自来源的字符串，仅做正则/解析。
"""
from __future__ import annotations

import base64
import binascii
import datetime as dt
import email.utils
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import List, Optional
from zoneinfo import ZoneInfo


@dataclass
class SourceItem:
    title: str
    link: str
    infohash: str
    torrent_url: str
    published: str = ""
    season: Optional[int] = None
    episode: Optional[int] = None
    episode_end: Optional[int] = None
    release_group: str = ""
    resolution: str = ""
    language: str = ""
    collection: bool = False
    raw: dict = field(default_factory=dict)


class FeedFormatError(Exception):
    """RSS 返回的不是可用 XML（如登录页/HTML 错误页）。"""


def normalize_infohash(btih: str) -> str:
    """规范化 btih：Base32 解码为字节后转小写 hex；hex 直接小写。

    不支持的哈希形式显式报错，绝不伪造。
    """
    s = (btih or "").strip()
    if not s:
        raise ValueError("空 infohash")
    if re.fullmatch(r"[0-9a-fA-F]{40}", s):
        return s.lower()
    if re.fullmatch(r"[0-9a-fA-F]{32}", s):
        return s.lower()  # MD5 形式的 v1 hash：保留原样并视为 32 位 hex
    if re.fullmatch(r"[A-Za-z2-7]{32}", s):
        try:
            return binascii.hexlify(base64.b32decode(s.upper())).decode()
        except binascii.Error as e:
            raise ValueError(f"Base32 infohash 解码失败: {e}") from e
    raise ValueError(f"不支持的 infohash 形式: 长度 {len(s)}")


def parse_feed(xml_text: str) -> List[SourceItem]:
    """解析 RSS XML。HTML/登录页/空内容抛 FeedFormatError，不当作空订阅。"""
    stripped = (xml_text or "").lstrip()
    if not stripped.startswith("<?xml") and not stripped.startswith("<rss"):
        # 常见误判来源：验证页、HTML 错误页
        raise FeedFormatError(
            f"来源返回的不是 RSS XML（头部 {stripped[:40]!r}），可能为验证页或错误页"
        )
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        raise FeedFormatError(f"RSS XML 解析失败: {e}") from e
    items: List[SourceItem] = []
    for node in root.findall(".//item"):
        title = (node.findtext("title") or "").strip()
        link = (node.findtext("link") or "").strip()
        if not title or not link:
            continue
        enc = node.find("enclosure")
        torrent_url = (enc.get("url") if enc is not None else "") or ""
        ih = extract_infohash_from_link(link) or extract_infohash_from_url(torrent_url)
        if not ih:
            continue
        group, res = parse_release_group(title), parse_resolution(title)
        season, ep, ep_end = parse_season_episode(title)
        items.append(SourceItem(
            title=title, link=link, infohash=ih, torrent_url=torrent_url,
            published=(node.findtext("pubDate") or "").strip(),
            season=season, episode=ep, episode_end=ep_end,
            release_group=group, resolution=res,
            raw={"title": title, "link": link, "torrent_url": torrent_url},
        ))
    return items


def published_on(item: SourceItem, date: dt.date, timezone: str = "Asia/Shanghai") -> bool:
    """判断 RSS 条目是否在指定时区的指定日期发布。"""
    if not item.published:
        return False
    try:
        parsed = email.utils.parsedate_to_datetime(item.published)
        if parsed is None:
            return False
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.astimezone(ZoneInfo(timezone)).date() == date
    except (TypeError, ValueError, KeyError):
        return False


def extract_infohash_from_link(link: str) -> Optional[str]:
    m = re.search(r"show-([0-9a-fA-F]{40})\.html", link or "")
    if m:
        return normalize_infohash(m.group(1))
    m = re.search(r"hash=([0-9a-fA-F]{40})", link or "")
    if m:
        return normalize_infohash(m.group(1))
    return None


def extract_infohash_from_url(url: str) -> Optional[str]:
    return extract_infohash_from_link(url or "")


def parse_resolution(title: str) -> str:
    m = re.search(r"(2160p|1080p|720p|4320p|4K)", title or "", re.I)
    return m.group(1).lower() if m else ""


def parse_release_group(title: str) -> str:
    m = re.match(r"\[([^\[\]]{1,40})\]", title or "")
    return m.group(1) if m else ""


_NON_EPISODE_MEDIA = re.compile(
    r"(?:^|[\s._\-\[\]()])"
    r"(?:NC?OP|NC?ED|OP|ED|PV|CM|SP|SPECIAL|TRAILER|PREVIEW|MENU|BONUS|"
    r"CREDIT(?:S)?|片头|片尾|预告|特典|特别篇|花絮|菜单)"
    r"(?:\d+)?"
    r"(?:$|[\s._\-\[\]()])",
    re.IGNORECASE,
)


def is_non_episode_media(title: str, *, marker: str = "") -> bool:
    """Return whether a title explicitly identifies non-episode media.

    Source titles are data; bounded marker matching avoids treating words such
    as ``Operation`` as opening themes.
    """

    value = " ".join(item for item in (title or "", marker or "") if item)
    return bool(_NON_EPISODE_MEDIA.search(value))


_PATTERNS_SE = [
    # S01E02 / S01E02E03 / S01E02-E04
    (re.compile(r"\bS(\d{1,2})E(\d{1,3})(?:\s*[-~E]\s*E?(\d{1,3}))?\b", re.I), 1),
    # 第12话 / 第12集 / 第 12 話
    (re.compile(r"第\s*(\d{1,4})\s*[话集話]"), 2),
    # [12] / - 12（季缺失按 1 处理）
    (re.compile(r"[\s\-\[](\d{2,4})\]?(?=[\s\.\[]|$)"), 3),
]

_PATTERNS_END = re.compile(r"\bS(\d{1,2})E(\d{1,3})\s*[-~]\s*E?(\d{1,3})\b", re.I)


def parse_season_episode(title: str):
    """返回 (season, episode, episode_end)；无法确定时 (None, None, None)。

    集合范围（合集）通过 episode_end 区分。季缺失按 None 交给调用方决策。
    """
    t = title or ""
    for pat, kind in _PATTERNS_SE:
        m = pat.search(t)
        if not m:
            continue
        if kind == 1:
            season = int(m.group(1))
            ep = int(m.group(2))
            end = int(m.group(3)) if m.group(3) else None
            return season, ep, end
        if kind == 2:
            return None, int(m.group(1)), None
        if kind == 3:
            n = int(m.group(1))
            if 1 <= n <= 9999:
                return None, n, None
    return None, None, None


@dataclass
class SubRule:
    work: str
    aliases: List[str]
    release_groups: List[str]
    language: Optional[str] = None
    resolution: Optional[str] = None
    season: Optional[int] = None
    exclude_keywords: List[str] = field(default_factory=list)


def match_rule(item: SourceItem, rule: SubRule) -> str:
    """返回匹配结论：'match' | 'exclude' | 'needs_review'。"""
    hay = item.title
    names = [rule.work] + list(rule.aliases or [])
    if not any(n and n.lower() in hay.lower() for n in names):
        return "exclude"
    for kw in rule.exclude_keywords or []:
        if kw.lower() in hay.lower():
            return "exclude"
    if rule.release_groups and item.release_group not in rule.release_groups:
        return "exclude"
    if item.episode is None:
        return "needs_review"
    if rule.release_groups and not item.release_group:
        return "needs_review"
    if rule.season is not None:
        if item.season is None:
            return "needs_review"
        if item.season != rule.season:
            return "exclude"
    if rule.resolution and item.resolution and item.resolution != rule.resolution.lower():
        return "exclude"
    return "match"
