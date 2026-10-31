"""V5-only compatibility with MoviePilot plugininstance credential storage.

No logging or persistence of secret values. Current schema is authoritative;
ambiguous/missing/disabled instances never fall back to stale systemconfig data.
"""
import json
import sqlite3
from pathlib import Path


def read_p115_cookie(section, legacy_reader):
    db_path = section.get('cookie_moviepilot_db')
    if (section.get('cookie_env') or not db_path
            or section.get('cookie_moviepilot_key') != 'plugin.P115Disk'
            or (section.get('cookie_moviepilot_path') or 'cookie') != 'cookie'):
        return legacy_reader(section, 'cookie', legacy_file='credential_file')
    with sqlite3.connect(Path(db_path).resolve().as_uri() + '?mode=ro', uri=True, timeout=30) as db:
        db.execute('PRAGMA query_only=ON')
        current = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='plugininstance'").fetchone()
        if not current:
            return legacy_reader(section, 'cookie', legacy_file='credential_file')
        rows = db.execute('SELECT instance_id, is_enabled FROM plugininstance WHERE source_plugin_id=? LIMIT 2', ('P115Disk',)).fetchall()
        if len(rows) != 1:
            raise ValueError('P115Disk credential instance missing or ambiguous')
        instance_id, enabled = rows[0]
        if enabled != 1:
            raise ValueError('P115Disk credential instance is disabled')
        row = db.execute('SELECT config_data FROM plugininstance WHERE source_plugin_id=? AND instance_id=?', ('P115Disk', instance_id)).fetchone()
        try:
            config = json.loads(row[0])
        except (TypeError, ValueError):
            raise ValueError('P115Disk instance configuration is invalid') from None
        value = config.get('cookie') if isinstance(config, dict) else None
        if not isinstance(value, str) or not value.strip():
            raise ValueError('P115Disk instance cookie is empty or invalid')
        return value
