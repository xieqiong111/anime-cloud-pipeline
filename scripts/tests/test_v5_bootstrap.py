"""Offline bootstrap regressions: fixtures only, never invoke the pipeline."""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
sys.path.insert(0, str(ROOT / 'bin'))
import v5_bootstrap as bootstrap


def fixture():
    return {
        'moviepilot': {'token_env': 'FIXTURE_MP'},
        'p115': {'cookie_env': 'FIXTURE_COOKIE', 'target_directory_id': '123'},
        'litepan': {'api_key_env': 'FIXTURE_LITEPAN'},
        'emby': {'api_key_env': 'FIXTURE_EMBY'},
    }


class BootstrapTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.db_path = self.root / 'fixture ?#.db'

    def database(self, *, plugin=False, rows=1, enabled=1):
        with sqlite3.connect(self.db_path) as db:
            db.execute('CREATE TABLE systemconfig (key TEXT, value TEXT)')
            db.execute('INSERT INTO systemconfig VALUES (?, ?)',
                       ('key', json.dumps({'nested': [{'api_key': 'db-secret'}], 'cookie': 'stale'})))
            if plugin:
                db.execute('CREATE TABLE plugininstance (source_plugin_id TEXT, instance_id TEXT, is_enabled INTEGER, config_data TEXT)')
                for index in range(rows):
                    db.execute('INSERT INTO plugininstance VALUES (?, ?, ?, ?)',
                               ('P115Disk', str(index), enabled, json.dumps({'cookie': 'instance-secret'})))
        return self.db_path

    def test_minimal_schema_and_host_mapping(self):
        config = fixture()
        config['emby'] = {'api_key_file': '/config/credentials/emby'}
        path = self.root / 'bootstrap.json'
        path.write_text(json.dumps(config))
        loaded = bootstrap.load_bootstrap(path, self.root)
        self.assertEqual(loaded['emby']['api_key_file'], str(self.root / 'credentials/emby'))
        self.assertEqual(loaded['p115']['target_directory_id'], '123')

    def test_config_errors_have_fixed_code_only(self):
        path = self.root / 'bootstrap.json'
        invalid = [[], {'private-secret': 'sensitive'}, fixture()]
        invalid[-1]['p115']['target_directory_id'] = 'private-secret'
        for config in invalid:
            path.write_text(json.dumps(config))
            with self.assertRaisesRegex(bootstrap.BootstrapError, '^V5_BOOTSTRAP_CONFIG_INVALID$'):
                bootstrap.load_bootstrap(path, self.root)
        path.write_text('{private-secret')
        with self.assertRaisesRegex(bootstrap.BootstrapError, '^V5_BOOTSTRAP_CONFIG_INVALID$'):
            bootstrap.load_bootstrap(path, self.root)

    def test_env_priority_and_empty_env_fail_closed(self):
        section = {'token_env': 'FIXTURE_SECRET', 'token_moviepilot_db': 'absent', 'token_file': 'absent'}
        with mock.patch.dict(os.environ, {'FIXTURE_SECRET': 'env-secret'}, clear=True):
            self.assertEqual(bootstrap.read_config_secret(section, 'token'), 'env-secret')
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(bootstrap.BootstrapError, '^V5_BOOTSTRAP_SECRET_UNAVAILABLE$'):
                bootstrap.read_config_secret(section, 'token')

    def test_database_priority_nested_path_and_readonly_uri(self):
        self.database()
        real_connect = sqlite3.connect
        connections = []
        def connect(database_uri, **kwargs):
            self.assertTrue(database_uri.endswith('?mode=ro'))
            self.assertIn('%3F%23', database_uri)
            self.assertTrue(kwargs['uri'])
            db = real_connect(database_uri, **kwargs)
            connections.append(db)
            return db
        section = {'api_key_moviepilot_db': str(self.db_path), 'api_key_moviepilot_key': 'key',
                   'api_key_moviepilot_path': 'nested.0.api_key', 'api_key_file': 'absent'}
        before = self.db_path.read_bytes()
        with mock.patch.object(bootstrap.sqlite3, 'connect', side_effect=connect):
            self.assertEqual(bootstrap.read_config_secret(section, 'api_key'), 'db-secret')
        self.assertEqual(self.db_path.read_bytes(), before)
        self.assertEqual(connections[0].execute('PRAGMA query_only').fetchone()[0], 1)
        with self.assertRaises(sqlite3.OperationalError):
            connections[0].execute("INSERT INTO systemconfig VALUES ('x', 'y')")
        connections[0].close()

    def test_missing_database_is_not_created_and_no_file_fallback(self):
        path = self.root / 'missing.db'
        section = {'api_key_moviepilot_db': str(path), 'api_key_file': 'absent'}
        with self.assertRaisesRegex(bootstrap.BootstrapError, '^V5_BOOTSTRAP_SECRET_UNAVAILABLE$'):
            bootstrap.read_config_secret(section, 'api_key')
        self.assertFalse(path.exists())

    def test_file_uses_v5_reader_and_legacy_alias(self):
        path = self.root / 'secret'
        path.write_text('file-secret\n')
        self.assertEqual(bootstrap.read_config_secret({'credential_file': str(path)}, 'cookie',
                                                     legacy_file='credential_file'), 'file-secret')
        path.write_text(' \n')
        with self.assertRaises(bootstrap.BootstrapError):
            bootstrap.read_config_secret({'token_file': str(path)}, 'token')

    def test_plugininstance_authoritative_and_fail_closed(self):
        self.database(plugin=True)
        section = {'cookie_moviepilot_db': str(self.db_path), 'cookie_moviepilot_key': 'plugin.P115Disk'}
        config = fixture()
        config['p115'].update(section)
        del config['p115']['cookie_env']
        with mock.patch.dict(os.environ, {'FIXTURE_MP': 'a', 'FIXTURE_LITEPAN': 'b', 'FIXTURE_EMBY': 'c'}, clear=True):
            self.assertEqual(bootstrap.resolve_environment(config)['P115_COOKIE'], 'instance-secret')
            with sqlite3.connect(self.db_path) as db:
                db.execute('UPDATE plugininstance SET is_enabled=0')
            with self.assertRaisesRegex(bootstrap.BootstrapError, '^V5_BOOTSTRAP_SECRET_UNAVAILABLE$'):
                bootstrap.resolve_environment(config)

    def test_plugininstance_ambiguous_missing_or_invalid_never_falls_back(self):
        self.database(plugin=True)
        config = fixture()
        config['p115'] = {'cookie_moviepilot_db': str(self.db_path),
                          'cookie_moviepilot_key': 'plugin.P115Disk', 'target_directory_id': '123'}
        with mock.patch.dict(os.environ, {'FIXTURE_MP': 'a', 'FIXTURE_LITEPAN': 'b', 'FIXTURE_EMBY': 'c'}, clear=True):
            for mutation in (
                "UPDATE plugininstance SET config_data='private-secret'",
                "INSERT INTO plugininstance VALUES ('P115Disk','second',1,'{}')",
                'DELETE FROM plugininstance',
            ):
                with sqlite3.connect(self.db_path) as db:
                    db.execute(mutation)
                with self.assertRaisesRegex(bootstrap.BootstrapError, '^V5_BOOTSTRAP_SECRET_UNAVAILABLE$'):
                    bootstrap.resolve_environment(config)

    def test_plugin_legacy_systemconfig_without_current_table(self):
        self.database()
        config = fixture()
        config['p115'] = {'cookie_moviepilot_db': str(self.db_path),
                          'cookie_moviepilot_key': 'key', 'target_directory_id': '0'}
        with mock.patch.dict(os.environ, {'FIXTURE_MP': 'a', 'FIXTURE_LITEPAN': 'b', 'FIXTURE_EMBY': 'c'}, clear=True):
            result = bootstrap.resolve_environment(config)
        self.assertEqual(result['P115_COOKIE'], 'stale')
        self.assertEqual(result['P115_ROOT_DIRECTORY_ID'], '0')

    def test_resolution_is_atomic_and_does_not_mutate_environment(self):
        with mock.patch.dict(os.environ, {'FIXTURE_MP': 'a', 'FIXTURE_COOKIE': 'b'}, clear=True):
            before = dict(os.environ)
            with self.assertRaises(bootstrap.BootstrapError):
                bootstrap.resolve_environment(fixture())
            self.assertEqual(dict(os.environ), before)

    def test_launcher_import_is_inert_and_failure_never_starts_pipeline(self):
        spec = importlib.util.spec_from_file_location('fixture_run_v5', ROOT / 'bin/run_v5.py')
        launcher = importlib.util.module_from_spec(spec)
        with mock.patch('subprocess.check_output') as docker:
            spec.loader.exec_module(launcher)
            docker.assert_not_called()
        stderr = io.StringIO()
        with mock.patch.object(launcher, 'load_bootstrap', side_effect=bootstrap.BootstrapError('V5_BOOTSTRAP_CONFIG_INVALID')), \
                mock.patch('subprocess.check_output') as docker, contextlib.redirect_stderr(stderr):
            self.assertEqual(launcher.main(['preflight']), 2)
            docker.assert_not_called()
        self.assertEqual(stderr.getvalue(), 'V5_BOOTSTRAP_CONFIG_INVALID\n')


    def test_launcher_success_stays_in_memory_and_forwards_arguments(self):
        spec = importlib.util.spec_from_file_location('fixture_run_v5_success', ROOT / 'bin/run_v5.py')
        launcher = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(launcher)
        config = fixture()
        config['moviepilot']['token_env'] = 'API_TOKEN'
        for existing_api_token in (None, 'existing-token'):
            env = {'FIXTURE_COOKIE': 'fixture-cookie', 'FIXTURE_LITEPAN': 'fixture-litepan',
                   'FIXTURE_EMBY': 'fixture-emby'}
            if existing_api_token is not None:
                env['API_TOKEN'] = existing_api_token
            pipeline = mock.Mock()
            pipeline.main.return_value = 17
            output = io.StringIO()
            with mock.patch.dict(os.environ, env, clear=True), \
                    mock.patch.dict(sys.modules, {'run_pipeline': pipeline}), \
                    mock.patch.object(launcher, 'load_bootstrap', return_value=config), \
                    mock.patch('subprocess.check_output', return_value=b'container-token\n') as docker, \
                    mock.patch('builtins.open', mock.mock_open(read_data='PROXY_HOST=http://proxy.local\n')) as opened, \
                    contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
                self.assertEqual(launcher.main(['preflight', '--fixture']), 17)
                pipeline.main.assert_called_once_with(['preflight', '--fixture'])
                self.assertEqual(os.environ['MOVIEPILOT_TOKEN'], existing_api_token or 'container-token')
                self.assertEqual(os.environ['P115_ROOT_DIRECTORY_ID'], '123')
                self.assertEqual(os.environ['HTTPS_PROXY'], 'http://proxy.local')
                if existing_api_token:
                    docker.assert_not_called()
                else:
                    self.assertEqual(docker.call_args.args[0],
                                     ['docker', 'exec', 'moviepilot-v3', 'printenv', 'API_TOKEN'])
                opened.assert_called_once_with(launcher.HOST_CONFIG + '/app.env', encoding='utf-8')
            self.assertEqual(output.getvalue(), '')


    def test_launcher_explicit_empty_api_token_fails_without_container_fallback(self):
        spec = importlib.util.spec_from_file_location('fixture_run_v5_empty_token', ROOT / 'bin/run_v5.py')
        launcher = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(launcher)
        config = fixture()
        config['moviepilot']['token_env'] = 'API_TOKEN'
        env = {'API_TOKEN': '', 'FIXTURE_COOKIE': 'fixture-cookie',
               'FIXTURE_LITEPAN': 'fixture-litepan', 'FIXTURE_EMBY': 'fixture-emby'}
        pipeline = mock.Mock()
        output = io.StringIO()
        with mock.patch.dict(os.environ, env, clear=True), \
                mock.patch.dict(sys.modules, {'run_pipeline': pipeline}), \
                mock.patch.object(launcher, 'load_bootstrap', return_value=config), \
                mock.patch('subprocess.check_output') as docker, \
                contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
            self.assertEqual(launcher.main(['preflight']), 2)
            docker.assert_not_called()
            pipeline.main.assert_not_called()
            self.assertEqual(dict(os.environ), env)
        self.assertEqual(output.getvalue(), 'V5_BOOTSTRAP_SECRET_UNAVAILABLE\n')


if __name__ == '__main__':
    unittest.main()
