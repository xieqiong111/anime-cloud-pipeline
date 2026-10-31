"""V5 credential references. Secret values are resolved in memory only."""
import json
import os
from pathlib import Path
import sqlite3

from pipeline.real_adapters import read_secret_file
from v5_plugin_secret import read_p115_cookie


class BootstrapError(ValueError):
    """Only fixed, non-sensitive error codes leave the bootstrap boundary."""


SPECS = (
    ('moviepilot', 'token', 'token_file', 'MOVIEPILOT_TOKEN'),
    ('p115', 'cookie', 'credential_file', 'P115_COOKIE'),
    ('litepan', 'api_key', None, 'LITEPAN_API_KEY'),
    ('emby', 'api_key', 'api_key_file', 'EMBY_API_KEY'),
)


def _host_view(value, host_config):
    if isinstance(value, dict):
        return {key: _host_view(item, host_config) for key, item in value.items()}
    if isinstance(value, str) and value.startswith('/config/'):
        return str(host_config) + value[len('/config'):]
    return value


def load_bootstrap(path, host_config):
    try:
        with open(path, encoding='utf-8') as handle:
            config = json.load(handle)
        if not isinstance(config, dict) or set(config) != {spec[0] for spec in SPECS}:
            raise ValueError()
        for section, name, legacy_file, _ in SPECS:
            values = config[section]
            allowed = {f'{name}_{suffix}' for suffix in (
                'env', 'moviepilot_db', 'moviepilot_key', 'moviepilot_path', 'file')}
            if legacy_file:
                allowed.add(legacy_file)
            if section == 'p115':
                allowed.add('target_directory_id')
            if not isinstance(values, dict) or set(values) - allowed:
                raise ValueError()
            if any(not isinstance(value, str) for key, value in values.items()
                   if key != 'target_directory_id'):
                raise ValueError()
        root_id = config['p115'].get('target_directory_id')
        if isinstance(root_id, bool) or not isinstance(root_id, (str, int)):
            raise ValueError()
        if not str(root_id).isascii() or not str(root_id).isdigit():
            raise ValueError()
        return _host_view(config, host_config)
    except Exception:
        raise BootstrapError('V5_BOOTSTRAP_CONFIG_INVALID') from None


def read_config_secret(section, name, *, legacy_file=None):
    try:
        env_name = section.get(f'{name}_env')
        if env_name:
            value = os.environ.get(env_name, '')
        elif section.get(f'{name}_moviepilot_db'):
            uri = Path(section[f'{name}_moviepilot_db']).resolve().as_uri() + '?mode=ro'
            with sqlite3.connect(uri, uri=True, timeout=30) as db:
                db.execute('PRAGMA query_only=ON')
                row = db.execute('SELECT value FROM systemconfig WHERE key=?',
                                 (section.get(f'{name}_moviepilot_key') or '',)).fetchone()
            value = json.loads(row[0])
            for component in (section.get(f'{name}_moviepilot_path') or name).split('.'):
                value = value[int(component)] if isinstance(value, list) and component.isdigit() else value[component]
        else:
            path = section.get(f'{name}_file') or (section.get(legacy_file) if legacy_file else None)
            if not path:
                raise ValueError()
            value = read_secret_file(os.path.abspath(os.path.expandvars(path)))
        if not isinstance(value, str) or not value.strip():
            raise ValueError()
        return value
    except Exception:
        raise BootstrapError('V5_BOOTSTRAP_SECRET_UNAVAILABLE') from None


def resolve_environment(config):
    """Resolve all references before changing the process environment."""
    try:
        result = {}
        for section, name, legacy_file, env_name in SPECS:
            values = config[section]
            result[env_name] = (read_p115_cookie(values, read_config_secret)
                                if section == 'p115' else
                                read_config_secret(values, name, legacy_file=legacy_file))
        result['P115_ROOT_DIRECTORY_ID'] = str(config['p115']['target_directory_id'])
        return result
    except Exception:
        raise BootstrapError('V5_BOOTSTRAP_SECRET_UNAVAILABLE') from None
