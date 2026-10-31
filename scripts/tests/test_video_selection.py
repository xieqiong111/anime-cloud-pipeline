from __future__ import annotations

import os
import tempfile
import unittest

from pipeline.adapters import RemoteInventoryIncomplete
from pipeline.models import EpisodeKey, RemoteFile, ReviewReason
from pipeline.state import StateStore
from pipeline.video import VideoPolicy, VideoSelector


POLICY = VideoPolicy()


def key(season: int, episode: int) -> EpisodeKey:
    return EpisodeKey("tmdb", "111", season, episode)


def remote(
    filename: str,
    *,
    remote_file_id: str | None = None,
    size: int | None = 1_000_000_000,
    duration: float | None = 1_440,
    path: str | None = None,
) -> RemoteFile:
    return RemoteFile(
        remote_file_id=remote_file_id or f"id-{filename}",
        filename=filename,
        path=path or filename,
        size=size,
        duration=duration,
    )


class VideoSelectionTests(unittest.TestCase):
    def test_selects_main_episode_and_excludes_feature_media(self):
        files = [
            remote("Show.S01E08.mkv", size=1_500_000_000, duration=1_440),
            remote("Show.S01E08.NCOP.mkv", size=120_000_000, duration=90),
            remote("Show.S01E08.NCED.mkv", size=120_000_000, duration=90),
            remote("Show.S01E08.PV.mp4", size=80_000_000, duration=60),
            remote("Show.S01E08.SP01.mkv", size=200_000_000, duration=180),
        ]

        result = VideoSelector(POLICY).select(files, [key(1, 8)])

        self.assertEqual(result.mapping[key(1, 8)].filename, "Show.S01E08.mkv")
        self.assertEqual(len(result.excluded), 4)
        self.assertTrue(all(item.reason == "feature_marker" for item in result.excluded))

    def test_one_torrent_maps_three_files_to_three_episodes(self):
        files = [remote(f"Show.S01E{episode:02d}.mkv") for episode in (1, 2, 3)]

        result = VideoSelector(POLICY).select(
            files, [key(1, 1), key(1, 2), key(1, 3)]
        )

        self.assertEqual(
            set(result.mapping), {key(1, 1), key(1, 2), key(1, 3)}
        )

    def test_multi_season_markers_keep_explicit_pairs_without_cross_product(self):
        result = VideoSelector(POLICY).select(
            [remote("Show.S01E01.S02E02.mkv")],
            [key(1, 1), key(1, 2), key(2, 1), key(2, 2)],
        )

        self.assertEqual(set(result.mapping), {key(1, 1), key(2, 2)})
        self.assertEqual(result.reviews[key(1, 2)], ReviewReason.NO_VALID_VIDEO)
        self.assertEqual(result.reviews[key(2, 1)], ReviewReason.NO_VALID_VIDEO)

    def test_glued_feature_markers_are_hard_excluded(self):
        files = [
            remote("Show.S01E08NCOP.mkv"),
            remote("Show.S01E08SP01.mkv"),
            remote("Show.S01E08PV.mkv"),
        ]

        result = VideoSelector(POLICY).select(files, [key(1, 8)])

        self.assertNotIn(key(1, 8), result.mapping)
        self.assertEqual(result.reviews[key(1, 8)], ReviewReason.NO_VALID_VIDEO)
        self.assertEqual(len(result.excluded), 3)
        self.assertEqual(
            {item.detail for item in result.excluded}, {"NCOP", "SP", "PV"}
        )

    def test_two_equally_valid_files_require_review(self):
        result = VideoSelector(POLICY).select(
            [remote("Show.S01E08-A.mkv"), remote("Show.S01E08-B.mkv")],
            [key(1, 8)],
        )

        self.assertNotIn(key(1, 8), result.mapping)
        self.assertEqual(
            result.reviews[key(1, 8)], ReviewReason.MULTIPLE_VIDEO_FILES
        )
        self.assertEqual(len(result.candidates[key(1, 8)]), 2)

    def test_season_only_token_is_not_episode_and_conflicting_full_season_is_rejected(self):
        result = VideoSelector(POLICY).select(
            [remote("Show.S01.mkv"), remote("Show.S02E08.mkv")], [key(1, 8)]
        )

        self.assertNotIn(key(1, 8), result.mapping)
        self.assertEqual(result.reviews[key(1, 8)], ReviewReason.AMBIGUOUS_EPISODE)

    def test_explicit_identity_beats_duration_and_size(self):
        result = VideoSelector(POLICY).select(
            [
                remote("Show.S01E08.mkv", size=100, duration=60),
                remote("Show.E08.mkv", size=2_000, duration=3_600),
                remote("Show.S02E08.mkv", size=3_000, duration=7_200),
            ],
            [key(1, 8)],
        )

        self.assertEqual(result.mapping[key(1, 8)].filename, "Show.S01E08.mkv")

    def test_nested_metadata_hash_and_fingerprint_are_normalized_and_persisted(self):
        fingerprinted = RemoteFile.from_mapping(
            {
                "name": "Show.S01E08.mkv",
                "metadata": {
                    "fingerprint": "nested-fingerprint",
                    "infohash": "nested-infohash",
                },
            }
        )
        hashed = RemoteFile.from_mapping(
            {"name": "Show.S01E09.mkv", "metadata": {"hash": "nested-hash"}}
        )

        self.assertEqual(fingerprinted.fingerprint, "nested-fingerprint")
        self.assertEqual(fingerprinted.infohash, "nested-infohash")
        self.assertEqual(hashed.fingerprint, "nested-hash")
        self.assertEqual(
            fingerprinted.to_mapping()["fingerprint"], "nested-fingerprint"
        )
        with tempfile.TemporaryDirectory() as temp:
            store = StateStore(os.path.join(temp, "state.db"))
            file_id = store.record_download_files("d" * 40, [fingerprinted])[0]
            persisted = store.conn.execute(
                "SELECT fingerprint FROM download_files WHERE id=?", (file_id,)
            ).fetchone()
            self.assertEqual(persisted[0], "nested-fingerprint")
            store.close()

    def test_complete_inventory_persists_before_sources_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as temp:
            store = StateStore(os.path.join(temp, "state.db"))
            first = store.get_or_create_episode({"key": key(1, 1)})
            second = store.get_or_create_episode({"key": key(1, 2)})
            files = [
                RemoteFile(
                    filename="Show.S01E01.mkv", remote_file_id=None, path=None,
                    size=1_000_000_000, duration=1_440,
                ),
                RemoteFile(
                    filename="Show.S01E02.mkv", remote_file_id=None, path=None,
                    size=1_000_000_000, duration=1_440,
                ),
            ]
            result = VideoSelector(POLICY).select(
                files, [key(1, 1), key(1, 2)]
            )

            result.persist(store, "AA" * 20, episode_ids={key(1, 1): first, key(1, 2): second})
            result.persist(store, "aa" * 20, episode_ids={key(1, 1): first, key(1, 2): second})

            self.assertEqual(
                store.conn.execute("SELECT COUNT(*) FROM downloads").fetchone()[0], 1
            )
            self.assertEqual(
                store.conn.execute("SELECT infohash FROM downloads").fetchone()[0],
                "aa" * 20,
            )
            self.assertEqual(
                store.conn.execute("SELECT COUNT(*) FROM download_files").fetchone()[0], 2
            )
            self.assertEqual(
                store.conn.execute("SELECT COUNT(*) FROM episode_sources").fetchone()[0], 2
            )
            self.assertEqual(
                store.conn.execute(
                    "SELECT COUNT(*) FROM episode_sources WHERE selected_primary=1"
                ).fetchone()[0],
                2,
            )
            store.close()

    def test_incomplete_inventory_is_rejected_before_selection_or_persistence(self):
        def incomplete():
            yield remote("Show.S01E01.mkv")
            raise RemoteInventoryIncomplete("page cap")

        with self.assertRaises(RemoteInventoryIncomplete):
            VideoSelector(POLICY).select(incomplete(), [key(1, 1)])


if __name__ == "__main__":
    unittest.main(verbosity=2)
