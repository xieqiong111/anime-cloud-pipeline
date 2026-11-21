"""Bounded, identity-verified migration of owned videos out of fid folders."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import posixpath
import sqlite3

from .evidence import SidecarEvidence, StrmEvidence
from .media_inventory import (
    _HostInventory,
    _host_files,
    _litepan_file_reference,
    _owner_index,
    _selected_rows,
    _within,
)
from .paths import _remote_file_id_token, season_label_from_air_date
from .safe_diagnostics import safe_exception_diagnostics
from .state import StateStore


class LayoutPlanError(ValueError):
    """A flat-layout move could not be proven safe from current evidence."""

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise LayoutPlanError(reason)


def _identity(value):
    # Keep the Organizer import graph acyclic while using the established V1
    # normalizer, including raw SDK aliases such as fid/s/sha1.
    from .cloud_naming import _identity as naming_identity
    return naming_identity(value)


def _same_content(left, right):
    from .cloud_naming import _same_content as same_content
    return same_content(left, right)


def _runtime(row):
    value = row.get('runtime_identity_json') or {}
    value = json.loads(value) if isinstance(value, str) else value
    _require(isinstance(value, dict), 'layout_runtime_invalid')
    return dict(value)


def _sha256(value):
    return isinstance(value, str) and len(value) == 64 and all(
        char in '0123456789abcdef' for char in value
    )


def _clean_episode(row, season_date):
    value = dict(row)
    value.pop('cloud_path', None)
    value['status'] = ''
    value['season_air_date'] = season_date
    return value


def _canonical_targets(row, selected, mapper, season_date, file_id):
    clean = _clean_episode(row, season_date)
    flat_path = mapper.flat_for_episode(clean, selected)
    season_path = posixpath.dirname(flat_path)
    staging_path = posixpath.join(
        season_path, _remote_file_id_token(file_id), posixpath.basename(flat_path)
    )
    archive_path = posixpath.join(
        mapper.layout_archive_root_for_episode(clean, selected),
        _remote_file_id_token(file_id),
    )
    return flat_path, staging_path, archive_path


def prepare_layout_runtime(
    row,
    selected,
    mapper,
    season_date,
    *,
    published_sha256_before=None,
):
    """Build a durable V1 name intent and V1 layout intent without writes.

    Existing V1 rename evidence is copied byte-for-byte as a mapping; this
    helper never rewrites its paths or historical STRM digest.
    """
    runtime = _runtime(row)
    identity = _identity(selected)
    source_path = selected.get('path')
    _require(isinstance(source_path, str) and source_path.startswith('/'), 'layout_source_path_missing')
    _require(season_label_from_air_date(season_date), 'layout_season_date_invalid')
    flat_path, stage_path, archive_path = _canonical_targets(
        row, selected, mapper, season_date, identity['remote_file_id']
    )
    current_path = row.get('cloud_path')
    prior_layout = runtime.get('_cloud_layout_intent')
    if prior_layout is not None:
        _require(isinstance(prior_layout, dict), 'layout_intent_invalid')
        layout = dict(prior_layout)
        _require(type(layout.get('version')) is int and layout['version'] == 1,
                 'layout_intent_version_unsupported')
        immutable = {
            'source_path': source_path,
            'from_cloud_path': stage_path,
            'to_cloud_path': flat_path,
            'file_id': identity['remote_file_id'],
            'size': identity['size'],
            'fingerprint': identity['fingerprint'],
            'staging_archive_path': archive_path,
        }
        for key, expected in immutable.items():
            if layout.get(key) not in (None, ''):
                _require(layout[key] == expected, 'layout_intent_immutable_changed')
            else:
                layout[key] = expected
        _require(isinstance(layout.get('name_intent'), dict), 'layout_name_intent_missing')
        _require(layout['name_intent'].get('to_cloud_path') == stage_path,
                 'layout_name_target_changed')
        _require(layout['name_intent'].get('source_path') in (None, source_path),
                 'layout_source_changed')
        _require(layout.get('original_status') not in (None, ''), 'layout_original_status_missing')
        layout.setdefault('from_directory_id', None)
        layout.setdefault('to_directory_id', None)
        layout.setdefault('verified', False)
        layout.setdefault('archive_directory_id', None)
        layout.setdefault('staging_archive_verified', False)
        runtime['_cloud_layout_intent'] = layout
        return runtime

    name_intent = runtime.get('_cloud_name_intent')
    current_status = str(row.get('status') or '')
    if name_intent is not None:
        _require(isinstance(name_intent, dict)
                 and type(name_intent.get('version')) is int
                 and name_intent['version'] == 1,
                 'layout_name_intent_invalid')
        # Keep every persisted V1 field, particularly old source/target paths,
        # original_status and published_sha256_before.
        name_intent = dict(name_intent)
        _require(name_intent.get('to_cloud_path') == stage_path, 'layout_name_target_changed')
        _require(name_intent.get('source_path') in (None, source_path), 'layout_source_changed')
        _require(str(name_intent.get('file_id') or identity['remote_file_id']) == identity['remote_file_id'],
                 'layout_name_file_id_changed')
        if name_intent.get('size') not in (None, ''):
            _require(type(name_intent['size']) is int and name_intent['size'] == identity['size'],
                     'layout_name_size_changed')
        if name_intent.get('fingerprint') not in (None, ''):
            _require(str(name_intent['fingerprint']).casefold() == identity['fingerprint'].casefold(),
                     'layout_name_fingerprint_changed')
        original_status = name_intent.get('original_status')
        _require(isinstance(original_status, str) and original_status, 'layout_original_status_missing')
        from_path = name_intent.get('from_cloud_path')
        _require(isinstance(from_path, str) and from_path.startswith('/'), 'layout_name_source_missing')
        if current_path:
            _require(current_path in (from_path, stage_path), 'layout_current_path_changed')
        directory_id = name_intent.get('directory_id') or runtime.get('_target_directory_id')
        if name_intent.get('rename_verified') is True:
            _require(name_intent.get('source_path') == source_path
                     and name_intent.get('file_id') == identity['remote_file_id']
                     and type(name_intent.get('size')) is int
                     and name_intent['size'] == identity['size']
                     and name_intent.get('fingerprint') == identity['fingerprint']
                     and isinstance(name_intent.get('directory_id'), str)
                     and bool(name_intent['directory_id']),
                     'layout_closed_name_intent_incomplete')
        else:
            if directory_id not in (None, ''):
                name_intent.setdefault('directory_id', str(directory_id))
            name_intent.setdefault('source_path', source_path)
            name_intent.setdefault('file_id', identity['remote_file_id'])
            name_intent.setdefault('size', identity['size'])
            name_intent.setdefault('fingerprint', identity['fingerprint'])
    else:
        directory_id = runtime.get('_target_directory_id')
        if (isinstance(current_path, str) and current_path.startswith('/')
                and posixpath.dirname(posixpath.dirname(current_path)) == posixpath.dirname(flat_path)
                and posixpath.basename(posixpath.dirname(current_path)) == _remote_file_id_token(identity['remote_file_id'])):
            from_path = current_path
            _require(directory_id not in (None, ''), 'layout_staging_directory_id_missing')
        else:
            from_path = mapper.isolated_for_episode(_clean_episode(row, season_date), selected)
        name_intent = {
            'version': 1,
            'from_cloud_path': from_path,
            'to_cloud_path': stage_path,
            'source_path': source_path,
            'file_id': identity['remote_file_id'],
            'size': identity['size'],
            'fingerprint': identity['fingerprint'],
            'original_status': current_status,
        }
        if directory_id not in (None, ''):
            name_intent['directory_id'] = str(directory_id)
        if published_sha256_before is not None:
            _require(_sha256(published_sha256_before), 'layout_published_digest_invalid')
            name_intent['published_sha256_before'] = published_sha256_before
        elif current_status in {'library_ready', 'notified', 'organized'}:
            raise LayoutPlanError('layout_published_digest_missing')

    _require(name_intent.get('to_cloud_path') == stage_path, 'layout_stage_target_changed')
    staging_directory_id = name_intent.get('directory_id') or runtime.get('_target_directory_id')
    original_status = name_intent.get('original_status') or current_status
    _require(isinstance(original_status, str) and original_status, 'layout_original_status_missing')
    layout = {
        'version': 1,
        'source_path': source_path,
        'from_cloud_path': stage_path,
        'to_cloud_path': flat_path,
        'file_id': identity['remote_file_id'],
        'size': identity['size'],
        'fingerprint': identity['fingerprint'],
        'from_directory_id': str(staging_directory_id) if staging_directory_id not in (None, '') else None,
        'to_directory_id': None,
        'name_intent': name_intent,
        'original_status': original_status,
        'verified': False,
        'staging_archive_path': archive_path,
        'archive_directory_id': None,
        'staging_archive_verified': False,
    }
    runtime['_cloud_name_intent'] = name_intent
    runtime['_cloud_layout_intent'] = layout
    return runtime


def _layout_for_row(row, selected, runtime, mapper, season_date):
    layout = runtime.get('_cloud_layout_intent')
    if isinstance(layout, dict):
        return dict(layout)
    runtime = prepare_layout_runtime(row, selected, mapper, season_date)
    return dict(runtime['_cloud_layout_intent'])


def _public(plan):
    row = plan['row']
    return {
        'episode_id': row['id'],
        'media_source': row['media_source'],
        'media_id': row['media_id'],
        'season': row['season'],
        'episode': row['episode'],
        'stage': plan['action'],
    }


def _blocked(row, reason, diagnostic=None):
    result = {
        'episode_id': row.get('id'),
        'media_id': row.get('media_id'),
        'season': row.get('season'),
        'episode': row.get('episode'),
        'reason': reason,
    }
    if diagnostic:
        result.update(diagnostic)
    return result


def _layout_published_reference_matches(record, runtime, reference, content, store, cap):
    """Accept an old published basename only through its exact closed V1 witness."""
    if (reference is None or reference[1] != str(record.get('inventory_file_id'))
            or not isinstance(content, bytes)):
        return False
    if reference[2] == posixpath.basename(record.get('cloud_path') or ''):
        return True
    from .media_inventory import _valid_cloud_name_intent
    intent = runtime.get('_cloud_name_intent')
    if (not _valid_cloud_name_intent(record, runtime, intent)
            or reference[2] != posixpath.basename(intent['from_cloud_path'])
            or hashlib.sha256(content.rstrip(b'\r\n')).hexdigest()
                != intent['published_sha256_before']
            or type(cap) is not int or cap <= 0):
        return False
    events = store.conn.execute(
        'SELECT id,to_status,detail_json FROM stage_history '
        'WHERE episode_id=? ORDER BY id LIMIT ?', (record['id'], cap + 1),
    ).fetchall()
    if len(events) > cap:
        return False
    pending, organized = [], []
    for event_id, status, raw in events:
        try:
            detail = json.loads(raw)
        except (TypeError, ValueError):
            return False
        if not isinstance(detail, dict):
            continue
        saved = detail.get('identity')
        if (detail.get('source_path') != intent['source_path']
                or detail.get('cloud_path') != intent['to_cloud_path']
                or not isinstance(saved, dict)
                or str(saved.get('remote_file_id')) != intent['file_id']
                or saved.get('_target_directory_id') != intent['directory_id']
                or saved.get('_cloud_name_intent') != intent):
            continue
        if status == 'organize_pending':
            pending.append(event_id)
        elif status == 'organized':
            organized.append(event_id)
    return any(start < end for start in pending for end in organized)


def collect_layout_plans(store, cloud, mapper, config, scope, *, episode_id=None):
    """Read current inventory and build plans for qualified owned episodes."""
    _require(scope.get('status') == 'ok', 'qualified_scope_blocked')
    eligible = {(str(item['media_id']), item['season']) for item in scope.get('runnable', [])}
    records = _selected_rows(store, config.limits.max_recursive_files)
    _owner_index(records)
    rows = [row for row in records
            if row.get('media_source') in ('tmdb', 'themoviedb')
            and (str(row.get('media_id')), row.get('season')) in eligible
            and (episode_id is None or row.get('id') == episode_id)]
    if episode_id is not None:
        _require(len(rows) == 1, 'layout_episode_not_uniquely_qualified')

    label = scope['quarter'] + '月新番'
    output_root = posixpath.join(config.paths.litepan_output_root, label)
    files = _host_files(output_root, config.limits.max_recursive_depth,
                        config.limits.max_recursive_files)
    reader = _HostInventory(output_root, files, config.limits.max_recursive_depth,
                            config.limits.max_recursive_files)
    plans, blocked = [], []
    for row in sorted(rows, key=lambda item: item['id']):
        try:
            runtime = _runtime(row)
            prior_layout = runtime.get('_cloud_layout_intent')
            if row.get('status') == 'organize_pending' and isinstance(prior_layout, dict):
                selected = StateStore.get_selected_download_file(store, row['id'])
                _require(selected is not None, 'layout_selection_not_unique')
                identity = _identity(selected)
                _require(str(runtime.get('remote_file_id') or '') == identity['remote_file_id'],
                         'layout_runtime_file_id_changed')
                season_date = StateStore.get_subscription_season_air_date(store, row['id'])
                _require(season_label_from_air_date(season_date) == label, 'layout_quarter_changed')
                prepared = prepare_layout_runtime(row, selected, mapper, season_date)
                layout = prepared['_cloud_layout_intent']
                plans.append({'row': row, 'selected': selected, 'runtime': prepared,
                              'identity': identity, 'intent': layout, 'action': 'resume_layout'})
                continue
            if row.get('status') not in {'library_ready', 'notified', 'organized'}:
                continue
            selected = StateStore.get_selected_download_file(store, row['id'])
            _require(selected is not None, 'layout_selection_not_unique')
            identity = _identity(selected)
            _require(str(runtime.get('remote_file_id') or '') == identity['remote_file_id'],
                     'layout_runtime_file_id_changed')
            _require(_within(selected.get('path'), config.paths.cloud_source_root),
                     'layout_source_scope_invalid')
            current_path = row.get('cloud_path')
            _require(_within(current_path, config.runtime.litepan_task_source_root),
                     'layout_current_path_invalid')
            current_parent = posixpath.dirname(current_path)
            season_date = StateStore.get_subscription_season_air_date(store, row['id'])
            _require(season_label_from_air_date(season_date) == label, 'layout_quarter_changed')
            clean = _clean_episode(row, season_date)
            canonical = mapper.flat_for_episode(clean, selected)
            if current_path == canonical:
                from .media_inventory import _valid_cloud_layout_intent, _cloud_layout_history_closed
                sealed = runtime.get('_cloud_layout_intent')
                _require(_valid_cloud_layout_intent(row, runtime, sealed)
                         and _cloud_layout_history_closed(store, row, runtime, sealed,
                                                          config.limits.max_recursive_files),
                         'layout_existing_proof_not_closed')
                season_id = str(cloud.resolve_directory_path(current_parent))
                _require(season_id == sealed['to_directory_id'], 'layout_final_parent_changed')
                current = cloud.find_file_by_identity(identity, current_parent, directory_id=season_id)
                _require(current is not None and current.get('path') == canonical
                         and _same_content(identity, _identity(current))
                         and str(current.get('parent_id')) == season_id,
                         'layout_final_file_changed')
                if sealed.get('staging_archive_state') == 'deleted':
                    entries = cloud.list_directory_entries(season_id)
                    _require(isinstance(entries, list), 'layout_season_inventory_unavailable')
                    _require(all(str(item.get('cid') or item.get('id') or '')
                                 != str(sealed['from_directory_id'])
                                 for item in entries if isinstance(item, dict)),
                             'layout_staging_delete_unverified')
                else:
                    _require(str(cloud.resolve_directory_path(sealed['staging_archive_path']))
                             == sealed['from_directory_id']
                             and cloud.list_directory_entries(sealed['from_directory_id']) == [],
                             'layout_archived_directory_changed')
                published = mapper.cloud_to_published_litepan(current_path, clean)
                _require(published == row.get('strm_path')
                         and StrmEvidence(reader).validate(published).ok
                         and SidecarEvidence(reader).validate(row).ok,
                         'layout_existing_projection_invalid')
                reference = _litepan_file_reference(reader.read(published))
                _require(reference is not None and reference[1] == identity['remote_file_id']
                         and reference[2] == posixpath.basename(canonical),
                         'layout_existing_strm_identity_changed')
                plans.append({'row': row, 'runtime': runtime, 'identity': identity,
                              'intent': sealed, 'action': 'already_flat'})
                continue
            _require(posixpath.basename(current_parent) == _remote_file_id_token(identity['remote_file_id']),
                     'layout_isolated_token_mismatch')
            _require(posixpath.dirname(current_parent) == posixpath.dirname(
                mapper.flat_for_episode(_clean_episode(row, StateStore.get_subscription_season_air_date(store, row['id'])), selected)
            ), 'layout_current_season_changed')
            season_date = StateStore.get_subscription_season_air_date(store, row['id'])
            _require(season_label_from_air_date(season_date) == label, 'layout_quarter_changed')
            directory_id = runtime.get('_target_directory_id')
            prior_name = runtime.get('_cloud_name_intent')
            if isinstance(prior_name, dict):
                directory_id = prior_name.get('directory_id') or directory_id
            _require(directory_id not in (None, ''), 'layout_staging_directory_id_missing')
            resolved_source_id = cloud.resolve_directory_path(current_parent)
            _require(str(resolved_source_id) == str(directory_id), 'layout_staging_directory_changed')
            current = cloud.find_file_by_identity(identity, current_parent, directory_id=directory_id)
            _require(current is not None and current.get('path') == current_path,
                     'layout_current_file_not_found')
            _require(_same_content(identity, _identity(current)), 'layout_current_file_changed')
            _require(str(current.get('parent_id') or '') == str(directory_id),
                     'layout_current_parent_changed')
            entries = cloud.list_directory_entries(directory_id)
            _require(isinstance(entries, list) and len(entries) == 1,
                     'layout_staging_inventory_not_singleton')

            clean = _clean_episode(row, season_date)
            flat_path, stage_path, _archive_path = _canonical_targets(
                row, selected, mapper, season_date, identity['remote_file_id']
            )
            _require(posixpath.dirname(flat_path) == posixpath.dirname(current_parent),
                     'layout_target_season_changed')
            _require(posixpath.splitext(current_path)[1] == posixpath.splitext(flat_path)[1],
                     'layout_extension_changed')
            name_intent = runtime.get('_cloud_name_intent')
            if isinstance(name_intent, dict):
                _require(name_intent.get('to_cloud_path') == stage_path,
                         'layout_name_target_changed')
                if name_intent.get('rename_verified') is True:
                    _require(current_path == stage_path, 'layout_rename_state_changed')
                else:
                    _require(current_path in (name_intent.get('from_cloud_path'), stage_path),
                             'layout_rename_state_changed')
            published = mapper.cloud_to_published_litepan(current_path, clean)
            _require(published == row.get('strm_path'), 'layout_published_path_changed')
            evidence = StrmEvidence(reader).validate(published)
            _require(evidence.ok, 'layout_existing_strm_invalid')
            metadata = SidecarEvidence(reader).validate(row)
            _require(metadata.ok, 'layout_existing_metadata_invalid')
            reference = _litepan_file_reference(reader.read(published))
            _require(_layout_published_reference_matches(
                row, runtime, reference, reader.read(published), store,
                config.limits.max_recursive_files,
            ), 'layout_existing_strm_identity_changed')
            prepared = prepare_layout_runtime(
                row, selected, mapper, season_date,
                published_sha256_before=evidence.detail['content_sha256'],
            )
            layout = prepared['_cloud_layout_intent']
            _require(layout.get('from_cloud_path') == stage_path
                     and layout.get('to_cloud_path') == flat_path,
                     'layout_target_changed')
            layout['from_directory_id'] = str(directory_id)
            layout['to_directory_id'] = str(cloud.resolve_directory_path(posixpath.dirname(flat_path)))
            owners = build_season_owners(store, mapper, config, posixpath.dirname(flat_path))
            _require(any(owner['file_id'] == identity['remote_file_id'] for owner in owners),
                     'layout_season_owner_missing')
            plans.append({'row': row, 'selected': selected, 'runtime': prepared,
                          'identity': identity, 'intent': layout,
                          'action': 'layout' if current_path != flat_path else 'already_flat'})
        except Exception as exc:
            reason = exc.reason if isinstance(exc, LayoutPlanError) else 'layout_inventory_unavailable'
            diagnostic = {} if isinstance(exc, LayoutPlanError) else safe_exception_diagnostics(
                exc, stage='organize_inventory'
            )
            blocked.append(_blocked(row, reason, diagnostic))
            if diagnostic.get('category') in ('rate_limit', 'transport', 'authentication'):
                break
    _require(reader.unchanged(), 'layout_published_snapshot_changed')
    return plans, blocked


def public_layout_plan(plans, blocked):
    return {
        'episodes': [_public(plan) for plan in plans],
        'blocked': [{key: item[key] for key in
                     ('episode_id', 'media_id', 'season', 'episode', 'reason')
                     if key in item} for item in blocked],
        'changes_needed': sum(plan['action'] == 'layout' for plan in plans),
        'already_flat': sum(plan['action'] == 'already_flat' for plan in plans),
        'resume_pending': sum(plan['action'] == 'resume_layout' for plan in plans),
    }


def _state_backup(store):
    paths = [row[2] for row in store.conn.execute('PRAGMA database_list') if row[1] == 'main']
    _require(len(paths) == 1 and paths[0] and os.path.isabs(paths[0]), 'layout_state_location_invalid')
    root = Path(paths[0]).parent / 'cloud-layout-backups'
    _require(not root.is_symlink(), 'layout_backup_root_symlink')
    root.mkdir(mode=0o700, exist_ok=True)
    _require(root.is_dir() and root.stat().st_mode & 0o077 == 0,
             'layout_backup_permissions_invalid')
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


def _cas_layout_pending(store, plan, *, secrets=()):
    row = plan['row']
    fresh = store.get_episode(row['id'])
    _require(fresh is not None
             and fresh.get('status') == row.get('status')
             and fresh.get('cloud_path') == row.get('cloud_path')
             and fresh.get('runtime_identity_json') == row.get('runtime_identity_json'),
             'layout_episode_changed')
    if row.get('status') == 'organize_pending':
        _require(plan['action'] == 'resume_layout', 'layout_pending_intent_missing')
        return
    detail = {
        'reason': 'season_flat_layout_requested',
        'source_path': plan['intent']['source_path'],
        'cloud_path': plan['intent']['from_cloud_path'],
        'identity': plan['runtime'],
    }
    _require(store.transition_if_current(
        row['id'], row['status'], 'organize_pending', detail,
        extra={'cloud_path': plan['intent']['from_cloud_path'],
               'runtime_identity_json': plan['runtime']},
        secrets=secrets,
    ), 'layout_episode_compare_and_set_failed')
    # Keep a freshly-read serialization check so a partial/future StateStore
    # projection cannot make the pending intent appear durable.
    persisted = store.get_episode(row['id'])
    _require(persisted is not None and persisted.get('status') == 'organize_pending'
             and persisted.get('cloud_path') == plan['intent']['from_cloud_path'],
             'layout_pending_intent_not_persisted')
    saved_runtime = persisted.get('runtime_identity_json') or '{}'
    saved_runtime = json.loads(saved_runtime) if isinstance(saved_runtime, str) else saved_runtime
    _require(isinstance(saved_runtime, dict)
             and saved_runtime.get('_cloud_layout_intent') == plan['intent'],
             'layout_pending_intent_not_persisted')


def apply_layout_plans(store, cloud, mapper, config, plans, *, secrets=(), throttle_state=None):
    """Persist each bounded intent and invoke the production Organizer once."""
    cap = config.limits.max_reconcile_batch
    _require(type(cap) is int and cap > 0, 'layout_batch_limit_invalid')
    selected = [plan for plan in plans if plan['action'] in {'layout', 'resume_layout'}][:cap]
    if not selected:
        return {'prepared': 0, 'remaining': 0, 'outcomes': [],
                'downstream_revalidation_required': False}
    _state_backup(store)
    from .organizer import Organizer

    organizer = Organizer(store, cloud, mapper, secrets=secrets,
                          throttle_state=throttle_state, config=config)
    outcomes = []
    for plan in selected:
        _cas_layout_pending(store, plan, secrets=secrets)
        result = organizer.reconcile_or_apply(store.get_episode(plan['row']['id']))
        fresh = store.get_episode(plan['row']['id'])
        runtime = _runtime(fresh or {})
        final_layout = runtime.get('_cloud_layout_intent')
        verified = (
            isinstance(final_layout, dict)
            and final_layout.get('verified') is True
            and final_layout.get('staging_archive_verified') is True
            and (fresh or {}).get('cloud_path') == plan['intent']['to_cloud_path']
        )
        organized = bool(result.organized and verified)
        outcomes.append({
            **_public(plan),
            'status': result.status,
            'organized': organized,
            'reason': None if organized else 'layout_verification_incomplete',
        })
        if not organized:
            break
    return {
        'prepared': len(outcomes),
        'remaining': max(0, len([plan for plan in plans if plan['action'] == 'layout']) - len(outcomes)),
        'outcomes': outcomes,
        'downstream_revalidation_required': bool(outcomes),
    }


def build_season_owners(store, mapper, config, parent_path, *, include_quarter=False):
    """Return exact selected-file owners whose canonical destination is one Season."""
    records = _selected_rows(store, config.limits.max_recursive_files)
    _owner_index(records)
    owners, by_id, by_path = [], {}, {}
    for record in records:
        season_date = StateStore.get_subscription_season_air_date(store, record['id'])
        current_path = record.get('cloud_path')
        current_season = None
        if isinstance(current_path, str) and current_path.startswith('/'):
            parent = posixpath.dirname(current_path)
            if posixpath.basename(parent).startswith('fid-'):
                current_season = posixpath.dirname(parent)
            elif posixpath.basename(parent).startswith('Season '):
                current_season = parent
        if not season_date:
            _require(current_season != parent_path, 'layout_owner_season_date_unknown')
            continue
        try:
            selected = StateStore.get_selected_download_file(store, record['id'])
            _require(selected is not None, 'layout_owner_selection_not_unique')
            identity = _identity(selected)
            clean = _clean_episode(record, season_date)
            canonical_path = mapper.flat_for_episode(clean, selected)
        except Exception as exc:
            if isinstance(exc, LayoutPlanError):
                raise
            if current_season == parent_path:
                raise LayoutPlanError('layout_owner_mapping_unknown') from None
            continue
        same_parent = posixpath.dirname(canonical_path) == parent_path
        target_quarter = mapper.relative_to(parent_path, mapper.roots.cloud_library_root,
                                             field='layout_owner_parent').split('/', 1)[0]
        owner_quarter = mapper.relative_to(canonical_path, mapper.roots.cloud_library_root,
                                            field='layout_owner_target').split('/', 1)[0]
        if not same_parent and not (include_quarter and target_quarter == owner_quarter):
            _require(current_season != parent_path, 'layout_owner_target_changed')
            continue
        runtime = _runtime(record)
        layout = runtime.get('_cloud_layout_intent')
        name_intent = runtime.get('_cloud_name_intent')
        staging_directory_id = None
        if isinstance(layout, dict):
            staging_directory_id = layout.get('from_directory_id')
        if staging_directory_id in (None, '') and isinstance(name_intent, dict):
            staging_directory_id = name_intent.get('directory_id')
        if staging_directory_id in (None, '') and current_season == parent_path:
            staging_directory_id = runtime.get('_target_directory_id')
        if current_season == parent_path and posixpath.basename(posixpath.dirname(current_path)) == _remote_file_id_token(identity['remote_file_id']):
            _require(staging_directory_id not in (None, ''), 'layout_owner_staging_id_unknown')
        item = {
            'file_id': identity['remote_file_id'],
            'size': identity['size'],
            'fingerprint': identity['fingerprint'],
            'canonical_path': canonical_path,
            'staging_directory_id': str(staging_directory_id) if staging_directory_id not in (None, '') else None,
        }
        _require(item['file_id'] not in by_id, 'layout_owner_file_id_conflict')
        _require(canonical_path not in by_path, 'layout_owner_canonical_path_conflict')
        by_id[item['file_id']] = item
        by_path[canonical_path] = item
        owners.append(item)
    return sorted(owners, key=lambda item: (item['canonical_path'], item['file_id']))
