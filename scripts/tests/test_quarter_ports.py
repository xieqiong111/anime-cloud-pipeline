"""Local-only quarterly ports regressions; no service or pipeline execution."""
import copy
from datetime import datetime
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace as NS
import tempfile
import unittest
from unittest.mock import Mock, patch

from pipeline.quarter_layout import build_layout
from pipeline.quarter_litepan import QuarterLitePanClient
from pipeline.quarter_ports import QuarterResourcePorts, _emby_mount_verified, _local_state


class Cloud:
    def __init__(self, layout):
        self.layout = layout
        self.fail = False
        self.creates = []
        self._client = NS(fs_mkdir=lambda: None, user_id=77)
        self.root_directory_id = '10'
        self.root_directory_path = layout['cloud_library_root']
        self.ids = {layout['cloud_library_root']: '10', '/cloud/library/2026-07月新番': '20',
                    layout['targets']['cloud_quarter']: '30'}
        label = Path(layout['targets']['cloud_quarter']).name
        self.entries = {'10': [{'name': label, 'cid': '30'}],
                        '30': [{'name': '未整理', 'cid': '31'}, {'name': '整理', 'cid': '32'}],
                        '31': [], '32': []}

    def resolve_directory_path(self, path):
        if self.fail:
            raise RuntimeError('authorization_or_inventory_unavailable')
        return self.ids[path]

    def list_directory_entries(self, parent):
        if self.fail:
            raise RuntimeError('incomplete_inventory')
        return copy.deepcopy(self.entries[str(parent)])

    def fs_mkdir(self, payload, parent):
        self.creates.append((payload, parent))


class QuarterPortsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        host = self.root/'strm'
        raw = host/'raw'
        published = host/'published'
        raw.mkdir(parents=True)
        published.mkdir()
        self.db = self.root/'litepan.db'
        self.config = NS(
            paths=NS(cloud_source_root='/cloud/incoming', cloud_library_root='/cloud/library',
                     litepan_raw_output_root=str(raw), litepan_output_root=str(published),
                     moviepilot_mount_root='/mp', emby_mount_root='/media/追番',
                     cloud_library_subdir='整理', litepan_publish_subdir=None,
                     state_directory=str(self.root/'state')),
            runtime=NS(litepan_strm_host_root=str(host), litepan_task_db_path=str(self.db),
                       litepan_task_source_root='/cloud/library/2026-07月新番',
                       litepan_task_output_root=str(raw/'2026-07月新番'),
                       litepan_task_name='old-quarter', litepan_event_source='hermes-v5'),
            limits=NS(max_task_pages=20, max_recursive_files=10000),
            services=NS(cloud=NS(root_directory_path='/cloud', secret_reference='UNSET_TEST_CLOUD'),
                        litepan=NS(base_url='http://127.0.0.1:5211'),
                        emby=NS(base_url='http://127.0.0.1:8097', secret_reference='UNSET_TEST_EMBY')))
        with patch('pipeline.quarter_layout.datetime') as clock:
            clock.now.return_value = datetime(2026, 10, 28)
            self.layout = build_layout({}, self.config)
        self.cloud = Cloud(self.layout)
        self.emby = Mock()
        self.emby.inventory.return_value = {'state': 'reuse', 'reason': 'emby_exact'}
        self.emby.creation_blockers.return_value = []
        self.litepan = QuarterLitePanClient('http://127.0.0.1:5211', task_db_path=str(self.db))
        self.litepan.verify_account_binding = Mock(return_value={
            'state': 'verified', 'reason': 'LITEPAN_ACCOUNT_BINDING_VERIFIED'})
        self.anchor = dict(self.layout['litepan_task'], id=1, name='old-quarter', account_id=7,
                           parent_id='20', path='/cloud/library/2026-07月新番',
                           output_folder='2026-07月新番', extensions='',
                           exclude_dir_keywords='', exclude_file_keywords='')
        self.target = dict(self.layout['litepan_task'], id=2, account_id=7, parent_id='30',
                           extensions='', exclude_dir_keywords='', exclude_file_keywords='')
        self.rule = dict(self.layout['litepan_rule'], id=1,
                         actions=[{'type': 'strm', 'condition': 'always',
                                   'params': {'task_id': 2, 'run_mode': 'full'}}])
        self.write_db([self.anchor, self.target], [self.rule])
        for key in ('raw_dir', 'published_dir'):
            Path(self.layout['targets'][key]).mkdir()

    def write_db(self, tasks, rules):
        if self.db.exists():
            self.db.unlink()
        bools = {'recursive', 'sync_metadata', 'branch_check_enabled', 'time_window_enabled'}
        integers = bools | {'account_id', 'scan_interval', 'api_interval'}
        with sqlite3.connect(self.db) as db:
            fields = ['id INTEGER'] + [key + (' INTEGER' if key in integers else ' TEXT')
                                       for key in QuarterLitePanClient.TASK_PAYLOAD]
            db.execute('CREATE TABLE strm_tasks (' + ','.join(fields) + ')')
            keys = ('id',) + QuarterLitePanClient.TASK_PAYLOAD
            for task in tasks:
                values = [task.get(k, 0 if k in integers else '') for k in keys]
                db.execute('INSERT INTO strm_tasks VALUES (' + ','.join('?' for _ in keys) + ')', values)
            db.execute('CREATE TABLE automation_rules (id INTEGER,name TEXT,trigger_type TEXT,trigger_config TEXT,actions_json TEXT,status TEXT)')
            for rule in rules:
                db.execute('INSERT INTO automation_rules VALUES (?,?,?,?,?,?)',
                           (rule['id'], rule['name'], rule['trigger_type'], json.dumps(rule['trigger_config']),
                            json.dumps(rule['actions']), rule['status']))

    def ports(self):
        return QuarterResourcePorts(self.config, self.layout, cloud=self.cloud, litepan=self.litepan, emby=self.emby)

    def test_layout_preserves_roots_and_maps_raw_group_folder(self):
        layout = self.layout
        self.assertEqual(layout['quarter'], '2026-10')
        task = layout['litepan_task']
        output = Path(self.config.runtime.litepan_strm_host_root)/task['group_dir']/task['output_folder']
        self.assertEqual(str(output), layout['targets']['raw_dir'])
        self.assertEqual(layout['targets']['cloud_organized'], '/cloud/library/2026-10月新番/整理')
        self.assertEqual(task['scan_mode'], 'incremental_update')
        self.assertFalse(task['sync_metadata'])
        self.assertNotIn('paths.cloud_library_root', layout['patch'])

    def test_complete_anchor_binding_and_plan_inventory_do_not_write(self):
        before = self.db.read_bytes()
        files_before = sorted(str(p.relative_to(self.root)) for p in self.root.rglob('*'))
        ports = self.ports()
        snapshot = ports.inventory(self.layout)
        self.assertTrue(all(row['state'] == 'reuse' for row in snapshot.values()))
        self.assertEqual(ports._task_spec['account_id'], 7)
        self.assertEqual(ports._task_spec['parent_id'], '30')
        self.assertEqual(ports._rule_spec['actions'][0]['params']['task_id'], 2)
        self.litepan.verify_account_binding.assert_called_once_with(
            7, expected_user_id=77, root_directory_id='10',
            root_directory_path='/cloud/library', target_path='/cloud/library/2026-07月新番',
            expected_target_id='20', max_pages=20, max_entries=10000)
        self.assertEqual(self.db.read_bytes(), before)
        self.assertEqual(sorted(str(p.relative_to(self.root)) for p in self.root.rglob('*')), files_before)
        self.assertEqual(self.cloud.creates, [])
        self.emby.create_library.assert_not_called()

    def test_unknown_or_mismatched_oauth_binding_blocks_before_any_create(self):
        for reason in ('LITEPAN_ACCOUNT_BINDING_UNVERIFIED', 'LITEPAN_ACCOUNT_USER_MISMATCH',
                       'LITEPAN_ACCOUNT_ROOT_MISMATCH', 'LITEPAN_ACCOUNT_TARGET_MISMATCH'):
            self.litepan.verify_account_binding.return_value = {'state': 'unknown', 'reason': reason}
            before = self.db.read_bytes()
            ports = self.ports()
            snapshot = ports.inventory(self.layout)
            self.assertEqual(snapshot['litepan_task'], {'state': 'unknown', 'reason': reason.lower()})
            self.assertEqual(snapshot['litepan_rule'], {'state': 'unknown', 'reason': reason.lower()})
            self.assertFalse(ports._account_verified)
            with patch('pipeline.quarter_ports._emby_mount_verified', return_value=True):
                self.assertIn('litepan_account_binding_unverified', ports.creation_capabilities(self.layout))
            self.assertEqual(self.db.read_bytes(), before)
            self.assertEqual(self.cloud.creates, [])
            self.emby.create_library.assert_not_called()

    def test_anchor_parent_mismatch_or_duplicate_fails_closed(self):
        for rows in ([dict(self.anchor, parent_id='999'), self.target],
                     [self.anchor, dict(self.anchor, id=3), self.target]):
            self.write_db(rows, [self.rule])
            result = self.ports().inventory(self.layout)
            self.assertEqual(result['litepan_task']['state'], 'unknown')
            self.assertEqual(result['litepan_rule']['state'], 'unknown')

    def test_missing_quarter_requires_complete_parent_listing(self):
        self.cloud.entries['10'] = []
        ports = self.ports()
        result = ports._cloud_inventory(self.layout)
        self.assertEqual({row['state'] for row in result.values()}, {'create'})
        self.assertIsNone(ports._quarter_id)
        self.cloud.fail = True
        self.assertEqual({row['state'] for row in ports._cloud_inventory(self.layout).values()}, {'unknown'})

    def test_incomplete_child_inventory_is_unknown_not_missing(self):
        original = self.cloud.list_directory_entries
        def incomplete(parent):
            if str(parent) == '30':
                raise RuntimeError('page_two_unavailable')
            return original(parent)
        self.cloud.list_directory_entries = incomplete
        self.assertEqual({r['state'] for r in self.ports()._cloud_inventory(self.layout).values()}, {'unknown'})

    def test_layout_rejects_raw_output_outside_verified_host_mapping(self):
        self.config.paths.litepan_raw_output_root = str(self.root/'unrelated')
        with self.assertRaises(ValueError):
            build_layout({}, self.config)

    def test_cloud_same_name_file_and_duplicate_identity_fail_closed(self):
        label = Path(self.layout['targets']['cloud_quarter']).name
        self.cloud.entries['10'] = [{'name': label, 'fid': '9'}]
        self.assertEqual({r['state'] for r in self.ports()._cloud_inventory(self.layout).values()}, {'conflict'})
        for ids in (('30', '30'), ('30', '99')):
            self.cloud.entries['10'] = [{'name': label, 'cid': ident} for ident in ids]
            self.assertEqual({r['state'] for r in self.ports()._cloud_inventory(self.layout).values()}, {'unknown'})

    def test_database_required_even_when_admin_or_mock_client_available(self):
        self.config.runtime.litepan_task_db_path = None
        with patch.dict('os.environ', {'LITEPAN_ADMIN_COOKIE': 'admin_session=test'}), \
                patch.object(self.litepan, '_request', side_effect=AssertionError('HTTP must not run')):
            with self.assertRaisesRegex(ValueError, 'litepan_readonly_database_required'):
                self.ports()

    def test_placeholder_ids_cannot_reuse(self):
        self.cloud.entries['10'] = []
        pending_target = dict(self.target, parent_id='pending-quarter-id')
        pending_rule = copy.deepcopy(self.rule)
        pending_rule['actions'][0]['params']['task_id'] = 0
        self.write_db([self.anchor, pending_target], [pending_rule])
        result = self.ports().inventory(self.layout)
        self.assertEqual(result['litepan_task']['state'], 'conflict')
        self.assertEqual(result['litepan_rule']['state'], 'conflict')

    def test_missing_admin_only_blocks_creation_not_all_reuse(self):
        ports = self.ports()
        ports.inventory(self.layout)
        with patch('pipeline.quarter_ports._emby_mount_verified', return_value=True), \
                patch.object(self.litepan, 'creation_blockers', side_effect=AssertionError('no admin needed')):
            self.assertEqual(ports.creation_capabilities(self.layout), [])
        self.write_db([self.anchor], [])
        ports.inventory(self.layout)
        with patch('pipeline.quarter_ports._emby_mount_verified', return_value=True):
            self.assertIn('litepan_admin_session_required', ports.creation_capabilities(self.layout))

    def test_cloud_create_issues_exactly_one_mkdir(self):
        self.ports().create('cloud_quarter', self.layout)
        self.assertEqual(self.cloud.creates, [({'cname': '2026-10月新番'}, '10')])

    def test_symlink_local_target_and_nonempty_emby_directory_rejected(self):
        target = Path(self.layout['targets']['raw_dir'])
        target.rmdir()
        target.symlink_to(self.root, target_is_directory=True)
        self.assertEqual(_local_state(self.layout['raw_base'], str(target))['state'], 'conflict')
        with self.assertRaisesRegex(ValueError, 'local_create_scope_changed'):
            self.ports().create('raw_dir', self.layout)
        (Path(self.layout['targets']['published_dir'])/'existing.strm').write_text('fixture')
        with patch('pipeline.quarter_ports._emby_mount_verified', return_value=True):
            with self.assertRaisesRegex(ValueError, 'emby_initial_directory_not_empty'):
                self.ports().create('emby_library', self.layout)
        self.emby.create_library.assert_not_called()

    def mount_check(self, *, address='127.0.0.1', port='8097', mounts=None):
        if mounts is None:
            mounts = [{'Destination': '/media', 'Source': '/wrong-parent'},
                      {'Destination': '/media/追番', 'Source': self.layout['published_base']}]
        data = {'running': True, 'ports': {'8096/tcp': [{'HostPort': port}]}, 'mounts': mounts}
        outputs = [NS(stdout=json.dumps([{'addr_info': [{'local': '127.0.0.1'}]}])), NS(stdout=json.dumps(data))]
        with patch('pipeline.quarter_ports.socket.getaddrinfo', return_value=[(None, None, None, None, (address, 0))]), \
                patch('pipeline.quarter_ports.subprocess.run', side_effect=outputs) as run:
            result = _emby_mount_verified(self.config, self.layout)
        return result, run

    def test_emby_mapping_uses_deepest_destination_and_expected_port(self):
        result, run = self.mount_check()
        self.assertTrue(result)
        self.assertEqual(run.call_args_list[1].args[0][2], 'Emby')
        self.assertFalse(self.mount_check(port='8096')[0])
        wrong = [{'Destination': '/media', 'Source': self.layout['published_base']},
                 {'Destination': '/media/追番', 'Source': '/wrong'}]
        self.assertFalse(self.mount_check(mounts=wrong)[0])

    def test_remote_emby_endpoint_never_runs_docker_inspect(self):
        result, run = self.mount_check(address='203.0.113.10')
        self.assertFalse(result)
        self.assertEqual(run.call_count, 1)


if __name__ == '__main__':
    unittest.main()
