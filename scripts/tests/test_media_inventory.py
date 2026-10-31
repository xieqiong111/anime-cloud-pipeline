"""Submission proof fixtures; no network or pipeline execution."""
from pathlib import Path
import sqlite3
import sys
import tempfile
from types import SimpleNamespace as NS
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline.media_inventory import prove_submission_inventory


class InventoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        # /tmp is itself a symlink on macOS; use the actual fixture root.
        self.root = Path(self.tmp.name).resolve()
        self.label = '2026-10月新番'
        raw, published = self.root / 'raw', self.root / 'published'
        (raw / self.label).mkdir(parents=True)
        (published / self.label).mkdir(parents=True)
        self.config = NS(limits=NS(max_recursive_depth=4, max_recursive_files=20),
                         paths=NS(cloud_library_root='/cloud/organized', litepan_raw_output_root=str(raw),
                                  litepan_output_root=str(published), emby_mount_root=str(published)),
                         runtime=NS(litepan_task_source_root='/cloud/organized/' + self.label,
                                    litepan_task_output_root=str(raw / self.label),
                                    emby_library_root=str(published / self.label), emby_library_name='quarter'))
        self.row = {'id': 1, 'media_source': 'tmdb', 'media_id': '12345', 'season': 1, 'episode': 1}
        db = sqlite3.connect(':memory:')
        self.addCleanup(db.close)
        db.executescript('''CREATE TABLE episodes_v5 (id INTEGER,media_source TEXT,media_id TEXT,season INTEGER,episode INTEGER,cloud_path TEXT,strm_path TEXT);
          CREATE TABLE download_files(id INTEGER,remote_file_id TEXT,path TEXT);
          CREATE TABLE episode_sources(episode_id INTEGER,download_file_id INTEGER,selected_primary INTEGER);''')
        self.store = NS(conn=db, get_subscription_season_air_date=mock.Mock(return_value='2026-10-01'))
        self.cloud = mock.Mock()
        self.cloud.resolve_directory_path.return_value = 'quarter-id'
        self.cloud.list_files.return_value = []
        self.emby = mock.Mock()
        self.emby.find_library.return_value = {'Id': 'library'}
        self.emby.iter_episodes.return_value = []
        self.emby._series_provider_map.return_value = {}

    def prove(self):
        return prove_submission_inventory(self.row, config=self.config, cloud=self.cloud, emby=self.emby, store=self.store)

    def own(self, *, other=False, path='/cloud/organized/2026-10月新番/video.mkv', remote_id='fid', host_path=None):
        self.store.conn.execute('INSERT INTO episodes_v5 VALUES(1,?,?,?,?,?,?)',
                                ('tmdb', '99' if other else '12345', 1, 1, path, host_path))
        self.store.conn.execute('INSERT INTO download_files VALUES(1,?,?)', (remote_id, path))
        self.store.conn.execute('INSERT INTO episode_sources VALUES(1,1,1)')

    def test_empty_complete_quarter_is_missing_and_scoped(self):
        result = self.prove()
        self.assertEqual(result['state'], 'missing')
        self.assertTrue(all(value == 0 for value in result['counts'].values()))
        self.cloud.resolve_directory_path.assert_called_once_with('/cloud/organized/' + self.label)
        self.cloud.list_files.assert_called_once_with('quarter-id', max_depth=4, max_files=20)
        self.emby.find_library.assert_called_once_with('quarter', self.config.runtime.emby_library_root)
        self.emby.iter_episodes.assert_called_once_with('library')
        self.emby._series_provider_map.assert_called_once_with('library')

    def test_cloud_partial_or_task_ledger_error_is_unknown_without_error_text(self):
        self.cloud.list_files.side_effect = RuntimeError('private-playback-url-token')
        result = self.prove()
        self.assertEqual(result['reason'], 'submission_inventory_cloud_unknown')
        self.assertNotIn('private', str(result))

    def test_unowned_cloud_file_never_excluded_by_filename(self):
        self.cloud.list_files.return_value = [{'file_id': '123', 'path': '/cloud/unrelated-title.mkv'}]
        self.assertEqual(self.prove()['state'], 'unknown')

    def test_owned_cloud_same_target_present_and_other_target_missing(self):
        for other in (False, True):
            self.store.conn.execute('DELETE FROM episode_sources')
            self.store.conn.execute('DELETE FROM download_files')
            self.store.conn.execute('DELETE FROM episodes_v5')
            self.own(other=other)
            self.cloud.list_files.return_value = [{'file_id': 'fid', 'path': '/different/path'}]
            self.assertEqual(self.prove()['state'], 'missing' if other else 'present')

    def test_host_nonempty_unknown_even_nfo_and_never_reads_content(self):
        path = Path(self.config.runtime.litepan_task_output_root) / 'unrelated.nfo'
        path.write_text('private contents')
        with mock.patch('builtins.open', side_effect=AssertionError('must not read contents')):
            self.assertEqual(self.prove()['state'], 'unknown')

    def test_exact_selected_state_host_path_proves_present(self):
        path = Path(self.config.paths.litepan_output_root) / self.label / 'episode.strm'
        path.write_text('private playback URL')
        self.own(host_path=str(path))
        self.assertEqual(self.prove()['state'], 'present')

    def test_host_symlink_and_missing_root_unknown(self):
        root = Path(self.config.runtime.litepan_task_output_root)
        (root / 'link').symlink_to(self.root / 'published', target_is_directory=True)
        self.assertEqual(self.prove()['reason'], 'submission_inventory_raw_unknown')
        (root / 'link').unlink()
        root.rmdir()
        self.assertEqual(self.prove()['state'], 'unknown')

    def test_host_depth_and_file_caps_unknown(self):
        root = Path(self.config.runtime.litepan_task_output_root)
        (root / 'nested').mkdir()
        self.config.limits.max_recursive_depth = 0
        self.assertEqual(self.prove()['state'], 'unknown')
        (root / 'nested').rmdir()
        self.config.limits.max_recursive_depth = 4
        self.config.limits.max_recursive_files = 1
        (root / 'a').touch()
        (root / 'b').touch()
        self.assertEqual(self.prove()['state'], 'unknown')

    def test_emby_series_identity_and_episode_numbers_not_path(self):
        self.emby.iter_episodes.return_value = [{'SeriesId': 's', 'ParentIndexNumber': 1, 'IndexNumber': 1,
                                                'Path': '/some/different/path'}]
        self.assertEqual(self.prove()['state'], 'unknown')
        self.emby._series_provider_map.return_value = {'s': '12345'}
        self.assertEqual(self.prove()['state'], 'present')
        self.emby._series_provider_map.return_value = {'s': '123'}
        self.assertEqual(self.prove()['state'], 'missing')

    def test_emby_pagination_failure_after_yield_is_unknown(self):
        def pages():
            yield {'SeriesId': 's', 'ParentIndexNumber': 1, 'IndexNumber': 1}
            raise ValueError('private error')
        self.emby.iter_episodes.side_effect = pages
        self.assertEqual(self.prove()['reason'], 'submission_inventory_emby_unknown')

    def test_unknown_blocks_even_when_another_inventory_present(self):
        self.own()
        self.cloud.list_files.return_value = [{'file_id': 'fid'}, {'file_id': 'unowned'}]
        self.assertEqual(self.prove()['state'], 'unknown')

    def test_wrong_quarter_or_emby_scope_unknown_before_cloud(self):
        self.store.get_subscription_season_air_date.return_value = '2026-07-01'
        self.assertEqual(self.prove()['reason'], 'submission_inventory_scope_unknown')
        self.cloud.resolve_directory_path.assert_not_called()
        self.store.get_subscription_season_air_date.return_value = '2026-10-01'
        self.config.runtime.emby_library_root = '/whole/library'
        self.assertEqual(self.prove()['state'], 'unknown')


    def test_themoviedb_source_alias_is_canonical_tmdb(self):
        self.row['media_source'] = 'themoviedb'
        self.assertEqual(self.prove()['state'], 'missing')

    def test_conflicting_selected_owners_unknown(self):
        self.own()
        self.store.conn.execute("INSERT INTO episodes_v5 VALUES(2,'tmdb','99',1,1,NULL,NULL)")
        self.store.conn.execute('INSERT INTO episode_sources VALUES(2,1,1)')
        self.cloud.list_files.return_value = [{'file_id': 'fid'}]
        self.assertEqual(self.prove()['reason'], 'submission_inventory_ownership_unknown')

    def test_state_and_series_inventory_failures_never_become_missing(self):
        self.emby._series_provider_map.side_effect = ValueError('private provider token')
        result = self.prove()
        self.assertEqual(result['reason'], 'submission_inventory_emby_unknown')
        self.assertNotIn('private', str(result))
        self.store.conn.execute('DROP TABLE episode_sources')
        self.assertEqual(self.prove()['reason'], 'submission_inventory_state_unknown')


if __name__ == '__main__':
    unittest.main()
