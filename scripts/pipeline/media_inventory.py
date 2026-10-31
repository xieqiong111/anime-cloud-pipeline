"""Read-only, quarter-scoped submission inventory proof; never infer ownership by name."""
from collections.abc import Mapping
import os
from pathlib import Path
import posixpath
import re
import stat

from .paths import season_label_from_air_date


class _Unknown(ValueError):
    pass


def _identity(row):
    source = str(row.get('media_source') or '').strip().lower()
    if source == 'themoviedb':
        source = 'tmdb'
    media_id = str(row.get('media_id') or '')
    season, episode = row.get('season'), row.get('episode')
    if source != 'tmdb' or not media_id.isascii() or not media_id.isdigit():
        raise _Unknown()
    if any(isinstance(v, bool) or not isinstance(v, int) or v < 0 for v in (season, episode)):
        raise _Unknown()
    return source, media_id, season, episode


def _host_files(root, depth_cap, file_cap):
    """Missing roots are unknown; symlinks and special files are never followed."""
    root = Path(root)
    for component in (root, *root.parents):
        if stat.S_ISLNK(component.lstat().st_mode):
            raise _Unknown()
    if not root.is_dir():
        raise _Unknown()
    output, visited = [], 0
    def walk(directory, depth):
        nonlocal visited
        visited += 1
        if visited > file_cap:
            raise _Unknown()
        with os.scandir(directory) as entries:
            for entry in entries:
                mode = entry.stat(follow_symlinks=False).st_mode
                if stat.S_ISLNK(mode):
                    raise _Unknown()
                if stat.S_ISDIR(mode):
                    if depth >= depth_cap:
                        raise _Unknown()
                    walk(Path(entry.path), depth + 1)
                elif stat.S_ISREG(mode):
                    output.append(entry.path)
                    if len(output) > file_cap:
                        raise _Unknown()
                else:
                    raise _Unknown()
    walk(root, 0)
    return output


def _owners(store, cap):
    """Only selected V5 remote-file mappings establish exact ID/path ownership."""
    rows = store.conn.execute('''
        SELECT e.media_source,e.media_id,e.season,e.episode,
               f.remote_file_id,f.path,e.cloud_path,e.strm_path
        FROM episode_sources s JOIN episodes_v5 e ON e.id=s.episode_id
        JOIN download_files f ON f.id=s.download_file_id
        WHERE s.selected_primary=1 LIMIT ?''', (cap + 1,)).fetchall()
    if len(rows) > cap:
        raise _Unknown()
    ids, paths = {}, {}
    for row in rows:
        identity = _identity(dict(zip(('media_source', 'media_id', 'season', 'episode'), row[:4])))
        if row[4]:
            ids.setdefault(str(row[4]), set()).add(identity)
        for value in row[5:]:
            if isinstance(value, str) and value.startswith('/'):
                paths.setdefault(value, set()).add(identity)
    return ids, paths


def prove_submission_inventory(row, *, config, cloud, emby, store):
    """Return only a fixed reason and safe counts; missing requires all inventories complete.

    Cloud adapter list_files owns pagination, recursive caps and complete task
    ledger checks. Emby adapter methods own their complete paginated reads.
    No credential, NFO/STRM content, playback URL or external error is returned.
    """
    counts = {'cloud_files': 0, 'raw_files': 0, 'published_files': 0,
              'emby_episodes': 0, 'emby_series': 0, 'owned_files': 0}
    stage = 'scope'
    try:
        target = _identity(row)
        depth_cap = config.limits.max_recursive_depth
        file_cap = config.limits.max_recursive_files
        if (isinstance(depth_cap, bool) or not isinstance(depth_cap, int) or depth_cap < 0
                or isinstance(file_cap, bool) or not isinstance(file_cap, int) or file_cap < 1):
            raise _Unknown()
        source = config.runtime.litepan_task_source_root
        library = config.paths.cloud_library_root.rstrip('/')
        label = source[len(library) + 1:] if isinstance(source, str) and source.startswith(library + '/') else ''
        if not re.fullmatch(r'[0-9]{4}-(?:01|04|07|10)月新番', label):
            raise _Unknown()
        air_date = store.get_subscription_season_air_date(row['id'])
        if season_label_from_air_date(air_date) != label:
            raise _Unknown()
        raw = config.runtime.litepan_task_output_root
        expected_raw = posixpath.join(config.paths.litepan_raw_output_root, label)
        published = posixpath.join(config.paths.litepan_output_root, label)
        if raw != expected_raw or not str(raw).startswith('/') or published == raw:
            raise _Unknown()
        emby_root = config.runtime.emby_library_root
        if (emby_root != posixpath.join(config.paths.emby_mount_root, label)
                or not config.runtime.emby_library_name):
            raise _Unknown()

        stage = 'state'
        ids, paths = _owners(store, file_cap)
        stage = 'cloud'
        directory_id = cloud.resolve_directory_path(source)
        files = cloud.list_files(directory_id, max_depth=depth_cap, max_files=file_cap)
        if not isinstance(files, list) or len(files) > file_cap:
            raise _Unknown()
        counts['cloud_files'] = len(files)
        stage = 'raw'
        raw_files = _host_files(raw, depth_cap, file_cap)
        counts['raw_files'] = len(raw_files)
        stage = 'published'
        published_files = _host_files(published, depth_cap, file_cap)
        counts['published_files'] = len(published_files)
        stage = 'emby'
        library_info = emby.find_library(config.runtime.emby_library_name, emby_root)
        if not isinstance(library_info, Mapping):
            raise _Unknown()
        library_id = library_info.get('Id', library_info.get('id'))
        if library_id in (None, ''):
            raise _Unknown()
        episodes = []
        for item in emby.iter_episodes(str(library_id)):
            episodes.append(item)
            if len(episodes) > file_cap:
                raise _Unknown()
        series = emby._series_provider_map(str(library_id))
        if not isinstance(series, Mapping) or len(series) > file_cap:
            raise _Unknown()
        counts['emby_episodes'], counts['emby_series'] = len(episodes), len(series)

        stage = 'ownership'
        unknown, present = False, False
        for item in files:
            if not isinstance(item, Mapping):
                raise _Unknown()
            path = item.get('path')
            file_id = item.get('remote_file_id', item.get('file_id'))
            owners = set(paths.get(path, set())) if isinstance(path, str) else set()
            if file_id not in (None, ''):
                owners.update(ids.get(str(file_id), set()))
            if len(owners) != 1:
                unknown = True
            else:
                counts['owned_files'] += 1
                present = present or target in owners
        for path in raw_files + published_files:
            owners = paths.get(path, set())
            if len(owners) != 1:
                unknown = True
            else:
                counts['owned_files'] += 1
                present = present or target in owners
        for item in episodes:
            if not isinstance(item, Mapping):
                raise _Unknown()
            series_id = str(item.get('SeriesId') or '')
            tmdb = series.get(series_id)
            season, episode = item.get('ParentIndexNumber'), item.get('IndexNumber')
            if (not isinstance(tmdb, str) or not tmdb.isascii() or not tmdb.isdigit()
                    or any(isinstance(v, bool) or not isinstance(v, int) or v < 0 for v in (season, episode))):
                unknown = True
                continue
            end = item.get('IndexNumberEnd', episode)
            if isinstance(end, bool) or not isinstance(end, int) or end < episode:
                unknown = True
                continue
            if tmdb == target[1] and season == target[2] and episode <= target[3] <= end:
                present = True
        # Any uncertainty blocks submission even if another inventory proves presence.
        if unknown:
            return {'state': 'unknown', 'reason': 'submission_inventory_ownership_unknown', 'counts': counts}
        return {'state': 'present' if present else 'missing',
                'reason': 'submission_inventory_present' if present else 'submission_inventory_missing', 'counts': counts}
    except Exception:
        return {'state': 'unknown', 'reason': 'submission_inventory_' + stage + '_unknown', 'counts': counts}
