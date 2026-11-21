"""Explicit, bounded naming repair for already-owned cloud episodes."""
from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
import posixpath
import sqlite3

from .evidence import SidecarEvidence, StrmEvidence
from .media_inventory import _HostInventory, _host_files, _litepan_file_reference, _owner_index, _selected_rows, _within
from .organizer import Organizer
from .paths import _remote_file_id_token, season_label_from_air_date
from .safe_diagnostics import safe_exception_diagnostics
from .state import StateStore


class NamingPlanError(ValueError):
    def __init__(self, reason):
        self.reason = reason
        super().__init__(reason)


def _require(condition, reason):
    if not condition:
        raise NamingPlanError(reason)


def _runtime(row):
    value = row.get('runtime_identity_json') or {}
    value = json.loads(value) if isinstance(value, str) else value
    _require(isinstance(value, dict), 'naming_runtime_invalid')
    return dict(value)


def _positive(value):
    text = str(value) if type(value) in (int, str) else ''
    return int(text) if text.isascii() and text.isdigit() and int(text) > 0 else None


def _identity(value):
    # find_file_by_identity retains the installed SDK's raw s/sha1 keys,
    # while selected inventory rows already use normalized size/fingerprint.
    def field(*keys):
        return next((value[key] for key in keys if value.get(key) not in (None, '')), None)
    file_id = field('remote_file_id', 'file_id', 'fid')
    size = _positive(field('size', 's', 'file_size'))
    fingerprint = field('fingerprint', 'sha1', 'sha', 'etag')
    _require(file_id not in (None, '') and not isinstance(file_id, bool), 'naming_file_id_missing')
    _require(size is not None, 'naming_file_size_missing')
    _require(isinstance(fingerprint, str) and 0 < len(fingerprint) <= 256, 'naming_fingerprint_missing')
    return {'remote_file_id': str(file_id), 'size': size, 'fingerprint': fingerprint}


def _same_content(left, right):
    return (str(left['remote_file_id']) == str(right['remote_file_id'])
            and left['size'] == right['size']
            and left['fingerprint'].casefold() == right['fingerprint'].casefold())


def _public(plan):
    row = plan['row']
    return {'episode_id': row['id'], 'media_source': row['media_source'],
            'media_id': row['media_id'], 'season': row['season'],
            'episode': row['episode'], 'action': plan['action']}


def collect_name_plans(store, cloud, mapper, config, scope, *, episode_id=None):
    """Inspect each existing selected identity; never discover or write media."""
    eligible = {(str(item['media_id']), item['season']) for item in scope['runnable']}
    records = _selected_rows(store, config.limits.max_recursive_files)
    _owner_index(records)
    label = scope['quarter'] + '月新番'
    output_root = posixpath.join(config.paths.litepan_output_root, label)
    files = _host_files(output_root, config.limits.max_recursive_depth, config.limits.max_recursive_files)
    reader = _HostInventory(output_root, files, config.limits.max_recursive_depth, config.limits.max_recursive_files)
    plans, blocked = [], []
    rows = [row for row in records
            if row.get('media_source') in ('tmdb', 'themoviedb')
            and (str(row['media_id']), row['season']) in eligible
            and (episode_id is None or row['id'] == episode_id)]
    if episode_id is not None:
        _require(len(rows) == 1, 'naming_episode_not_uniquely_qualified')
    for row in sorted(rows, key=lambda item: item['id']):
        if row['status'] not in ('library_ready', 'notified'):
            # A previously prepared row resumes through the normal runner.
            if _runtime(row).get('_cloud_name_intent'):
                plans.append({'row': row, 'action': 'resume_existing_intent'})
            continue
        try:
            runtime = _runtime(row)
            selected = StateStore.get_selected_download_file(store, row['id'])
            _require(selected is not None, 'naming_selection_not_unique')
            expected = _identity(selected)
            _require(str(runtime.get('remote_file_id') or '') == expected['remote_file_id'], 'naming_runtime_file_id_changed')
            directory_id = runtime.get('_target_directory_id')
            _require(directory_id not in (None, ''), 'naming_directory_id_missing')
            current_path = row.get('cloud_path')
            _require(_within(current_path, config.runtime.litepan_task_source_root), 'naming_cloud_scope_invalid')
            parent = posixpath.dirname(current_path)
            _require(posixpath.basename(parent) == _remote_file_id_token(expected['remote_file_id']), 'naming_isolated_token_mismatch')
            _require(_within(selected['path'], config.paths.cloud_source_root), 'naming_source_scope_invalid')
            season_date = StateStore.get_subscription_season_air_date(store, row['id'])
            _require(season_label_from_air_date(season_date) == label, 'naming_quarter_changed')
            naming_row = {**row, 'season_air_date': season_date, 'status': ''}
            naming_row.pop('cloud_path', None)
            canonical = mapper.canonical_for_episode(naming_row, selected)
            target_path = posixpath.join(parent, posixpath.basename(canonical))
            published = mapper.cloud_to_published_litepan(current_path, {**row, 'season_air_date': season_date})
            _require(published == row.get('strm_path'), 'naming_published_path_changed')
            _require(posixpath.splitext(current_path)[1] == posixpath.splitext(target_path)[1], 'naming_extension_changed')
            current = cloud.find_file_by_identity(expected, parent, directory_id=directory_id)
            _require(current is not None and current.get('path') == current_path, 'naming_current_file_not_found')
            _require(_same_content(expected, _identity(current)), 'naming_current_file_changed')
            _require(str(current.get('parent_id') or '') == str(directory_id), 'naming_current_parent_changed')
            entries = cloud.list_directory_entries(directory_id)
            _require(isinstance(entries, list) and len(entries) == 1, 'naming_isolated_directory_not_unique')
            evidence = StrmEvidence(reader).validate(published)
            _require(evidence.ok, 'naming_existing_strm_invalid')
            metadata = SidecarEvidence(reader).validate(row)
            _require(metadata.ok, 'naming_existing_metadata_invalid')
            reference = _litepan_file_reference(reader.read(published))
            _require(reference is not None and reference[1] == expected['remote_file_id']
                     and reference[2] == posixpath.basename(current_path), 'naming_existing_strm_identity_changed')
            intent = {'version': 1, 'from_cloud_path': current_path, 'to_cloud_path': target_path,
                      'source_path': selected['path'], 'file_id': expected['remote_file_id'],
                      'size': expected['size'], 'fingerprint': expected['fingerprint'],
                      'directory_id': str(directory_id), 'original_status': row['status'],
                      'published_sha256_before': evidence.detail['content_sha256']}
            plans.append({'row': row, 'runtime': runtime, 'identity': expected,
                          'intent': intent, 'action': 'already_named' if current_path == target_path else 'rename'})
        except Exception as exc:
            reason = exc.reason if isinstance(exc, NamingPlanError) else 'naming_inventory_unavailable'
            diagnostic = {} if isinstance(exc, NamingPlanError) else safe_exception_diagnostics(exc, stage='organize_inventory')
            blocked.append({'episode_id': row['id'], 'media_id': row['media_id'],
                            'season': row['season'], 'episode': row['episode'], 'reason': reason,
                            **diagnostic})
            if diagnostic.get('category') in ('rate_limit', 'transport', 'authentication'):
                break
    _require(reader.unchanged(), 'naming_published_snapshot_changed')
    return plans, blocked


def public_name_plan(plans, blocked):
    return {'episodes': [_public(plan) for plan in plans], 'blocked': blocked,
            'changes_needed': sum(plan['action'] == 'rename' for plan in plans),
            'already_named': sum(plan['action'] == 'already_named' for plan in plans),
            'resume_pending': sum(plan['action'] == 'resume_existing_intent' for plan in plans)}


def _state_backup(store):
    paths = [row[2] for row in store.conn.execute('PRAGMA database_list') if row[1] == 'main']
    _require(len(paths) == 1 and paths[0] and os.path.isabs(paths[0]), 'naming_state_location_invalid')
    root = Path(paths[0]).parent / 'cloud-naming-backups'
    _require(not root.is_symlink(), 'naming_backup_root_symlink')
    root.mkdir(mode=0o700, exist_ok=True)
    _require(root.is_dir() and root.stat().st_mode & 0o077 == 0, 'naming_backup_permissions_invalid')
    name = 'state-' + dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ') + '.db'
    destination = root / name
    descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    os.close(descriptor)
    backup = sqlite3.connect(destination)
    try:
        store.conn.backup(backup)
    finally:
        backup.close()
    descriptor = os.open(destination, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def apply_name_plans(store, cloud, mapper, config, plans, *, secrets=(), throttle_state=None):
    """CAS terminal owned rows into normal organization and run one stage."""
    changes = [plan for plan in plans if plan['action'] == 'rename']
    cap = config.limits.max_reconcile_batch
    _require(type(cap) is int and cap > 0, 'naming_batch_limit_invalid')
    selected = changes[:cap]
    if selected:
        _state_backup(store)
    organizer = Organizer(store, cloud, mapper, secrets=secrets, throttle_state=throttle_state)
    outcomes = []
    for plan in selected:
        row, intent = plan['row'], plan['intent']
        fresh = store.get_episode(row['id'])
        _require(fresh is not None and fresh['status'] == row['status']
                 and fresh.get('cloud_path') == row.get('cloud_path')
                 and fresh.get('runtime_identity_json') == row.get('runtime_identity_json'), 'naming_episode_changed')
        runtime = {**plan['runtime'], '_cloud_name_intent': intent}
        detail = {'reason': 'cloud_name_repair_requested', 'source_path': intent['source_path'],
                  'cloud_path': intent['to_cloud_path'], 'identity': runtime}
        applied = store.transition_if_current(
            row['id'], row['status'], 'organize_pending', detail,
            extra={'cloud_path': intent['to_cloud_path'], 'runtime_identity_json': runtime}, secrets=secrets)
        _require(applied, 'naming_episode_compare_and_set_failed')
        result = organizer.reconcile_or_apply(store.get_episode(row['id']), _stage_only=True)
        outcomes.append({**_public(plan), 'status': result.status,
                         'rename_count': result.rename_count, 'organized': result.organized})
        if not result.organized:
            break
    return {'prepared': len(outcomes), 'remaining': len(changes) - len(outcomes),
            'outcomes': outcomes, 'downstream_revalidation_required': bool(outcomes)}
