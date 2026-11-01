"""Read-only, quarter-scoped submission inventory proof; never infer ownership by name."""
from collections.abc import Mapping
import os
from pathlib import Path
import posixpath
import re
import stat

from .evidence import SidecarEvidence, StrmEvidence
from .paths import PathMapper, season_label_from_air_date


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


def _selected_rows(store, cap):
    cursor = store.conn.execute('''
        SELECT e.*, f.remote_file_id AS inventory_file_id,
               f.path AS inventory_file_path
        FROM episode_sources s JOIN episodes_v5 e ON e.id=s.episode_id
        JOIN download_files f ON f.id=s.download_file_id
        WHERE s.selected_primary=1 LIMIT ?''', (cap + 1,))
    rows = cursor.fetchall()
    if len(rows) > cap:
        raise _Unknown()
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in rows]


def _owner_index(rows):
    ids, paths, primary_files = {}, {}, {}
    for row in rows:
        identity = _identity(row)
        file_id = row['inventory_file_id']
        if file_id in (None, '') or isinstance(file_id, bool):
            raise _Unknown()
        primary_files.setdefault(identity, set()).add(str(file_id))
        if len(primary_files[identity]) != 1:
            raise _Unknown()
        ids.setdefault(str(file_id), set()).add(identity)
        for value in (row['inventory_file_path'], row.get('cloud_path'), row.get('strm_path')):
            if isinstance(value, str) and value.startswith('/'):
                paths.setdefault(value, set()).add(identity)
    if any(len(owners) != 1 for owners in (*ids.values(), *paths.values())):
        raise _Unknown()
    return ids, paths


def _owners(store, cap):
    """Only selected V5 remote-file mappings establish exact ID/path ownership."""
    return _owner_index(_selected_rows(store, cap))


def _within(path, root):
    if (not isinstance(path, str) or not path.startswith('/') or '\\' in path
            or path != posixpath.normpath(path) or path.startswith('//')):
        raise _Unknown()
    return path.startswith(root.rstrip('/') + '/')


def _fingerprint(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns,
            info.st_ctime_ns, info.st_mode)


class _HostInventory:
    """Exact no-follow reads from one enumerated tree, with bounded text reads."""

    def __init__(self, root, files, depth, cap):
        self.root, self.depth, self.cap = root, depth, cap
        self.snapshot = {name: _fingerprint(os.lstat(name)) for name in files}

    def read(self, path):
        if not _within(path, self.root):
            raise _Unknown()
        descriptor = os.open('/', os.O_RDONLY | os.O_DIRECTORY)
        try:
            parts = Path(path).parts[1:]
            for component in parts[:-1]:
                child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                dir_fd=descriptor)
                os.close(descriptor)
                descriptor = child
            fd = os.open(parts[-1], os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW,
                         dir_fd=descriptor)
            with os.fdopen(fd, 'rb') as handle:
                info = os.fstat(handle.fileno())
                if (not stat.S_ISREG(info.st_mode)
                        or self.snapshot.get(path) != _fingerprint(info)):
                    raise _Unknown()
                # Image ownership requires readability/non-emptiness, like the
                # existing metadata evidence; only NFO/STRM text is inspected.
                text = Path(path).suffix.casefold() in {'.nfo', '.strm'}
                content = handle.read(1024 * 1024 + 1 if text else 1)
                if ((text and len(content) > 1024 * 1024)
                        or _fingerprint(os.fstat(handle.fileno())) != _fingerprint(info)
                        or _fingerprint(os.lstat(path)) != _fingerprint(info)):
                    raise _Unknown()
                return content
        finally:
            os.close(descriptor)

    def unchanged(self):
        current = _host_files(self.root, self.depth, self.cap)
        return {name: _fingerprint(os.lstat(name)) for name in current} == self.snapshot


def _host_ownership(records, *, config, store, raw_reader, published_reader, label):
    """Anchor media in selected cloud identities, then classify exact local roles."""
    mapper = PathMapper(config.paths)
    raw_owners, published_owners, episode_sidecars, shared_sidecars = {}, {}, {}, {}
    published_evidence = {}

    def associate(index, path, owner):
        index.setdefault(path, set()).add(owner)

    for record in records:
        owner = _identity(record)
        cloud_path, published = record.get('cloud_path'), record.get('strm_path')
        if not published:
            continue
        if not _within(published, published_reader.root):
            if cloud_path and _within(cloud_path, config.runtime.litepan_task_source_root):
                raise _Unknown()
            continue
        if not cloud_path or not _within(cloud_path, config.runtime.litepan_task_source_root):
            raise _Unknown()
        date = store.get_subscription_season_air_date(record['id'])
        if season_label_from_air_date(date) != label:
            raise _Unknown()
        row = {**record, 'season_air_date': date}
        if mapper.cloud_to_published_litepan(cloud_path, row) != published:
            raise _Unknown()
        raw = mapper.cloud_to_raw_litepan(cloud_path, config.paths.litepan_raw_output_root)
        if not _within(raw, raw_reader.root):
            raise _Unknown()
        associate(published_owners, published, owner)
        associate(raw_owners, raw, owner)
        if published in published_reader.snapshot:
            evidence = StrmEvidence(published_reader).validate(published)
            if not evidence.ok:
                raise _Unknown()
            published_evidence[published] = evidence.detail['content_sha256']
        if raw in raw_reader.snapshot:
            evidence = StrmEvidence(raw_reader).validate(raw)
            if (not evidence.ok or published not in published_evidence
                    or evidence.detail['content_sha256'] != published_evidence[published]):
                raise _Unknown()

        sidecars = SidecarEvidence(published_reader).classify_existing(row)
        if not sidecars.ok:
            raise _Unknown()
        roles = sidecars.detail['paths_by_role']
        stem = published[:-5]
        season_dir = posixpath.dirname(published)
        work_dir = posixpath.dirname(season_dir)
        # Evidence results redact paths for reporting. Ownership uses the
        # canonical slots whose role names were validated, never a redacted
        # diagnostic string as a filesystem path.
        slots = {
            'episode_nfo': stem + '.nfo', 'episode_image': stem + '.jpg',
            'tvshow_nfo': posixpath.join(work_dir, 'tvshow.nfo'),
            'tvshow_poster': posixpath.join(work_dir, 'poster.jpg'),
            'season_nfo': posixpath.join(season_dir, 'season.nfo'),
            'season_poster': posixpath.join(season_dir, 'poster.jpg'),
        }
        for role in roles:
            path = slots[role]
            if role.startswith('episode_'):
                associate(episode_sidecars, path, owner)
            else:
                scope = (owner[0], owner[1], owner[2] if role.startswith('season_') else None)
                associate(shared_sidecars, path, scope)
        # These are exact, standard shared-art slots, not a blanket image
        # exemption. Their show/season NFO anchor must already be verified.
        extras = []
        if 'tvshow_nfo' in roles:
            extras += [(posixpath.join(work_dir, name), (owner[0], owner[1], None))
                       for name in ('fanart.jpg', 'backdrop.jpg')]
        if 'season_nfo' in roles:
            extras.append((posixpath.join(work_dir, f'season{owner[2]:02d}-poster.jpg'),
                           (owner[0], owner[1], owner[2])))
        for path, scope in extras:
            if path in published_reader.snapshot:
                if not published_reader.read(path):
                    raise _Unknown()
                associate(shared_sidecars, path, scope)
    return raw_owners, published_owners, episode_sidecars, shared_sidecars


def prove_submission_inventory(row, *, config, cloud, emby, store):
    """Return only a fixed reason and safe counts; missing requires all inventories complete.

    Cloud adapter list_files owns pagination, recursive caps and complete task
    ledger checks. Emby adapter methods own their complete paginated reads.
    No credential, NFO/STRM content, playback URL or external error is returned.
    """
    counts = {'cloud_files': 0, 'raw_files': 0, 'published_files': 0,
              'emby_episodes': 0, 'emby_series': 0, 'owned_files': 0,
              'owned_episode_metadata': 0, 'owned_shared_metadata': 0}
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
        records = _selected_rows(store, file_cap)
        stage = 'ownership'
        ids, paths = _owner_index(records)
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
        raw_reader = _HostInventory(raw, raw_files, depth_cap, file_cap)
        published_reader = _HostInventory(published, published_files, depth_cap, file_cap)
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
            by_path = set()
            if path not in (None, ''):
                if not _within(path, source):
                    raise _Unknown()
                by_path = set(paths.get(path, ()))
                if len(by_path) != 1:
                    raise _Unknown()
            owners = by_path
            if file_id not in (None, ''):
                by_id = set(ids.get(str(file_id), ()))
                if len(by_id) != 1 or (by_path and by_id != by_path):
                    raise _Unknown()
                owners = by_id
            if len(owners) != 1:
                unknown = True
            else:
                counts['owned_files'] += 1
                present = present or target in owners
        raw_owners, published_owners, episode_sidecars, shared_sidecars = ({}, {}, {}, {})
        if raw_files or published_files:
            raw_owners, published_owners, episode_sidecars, shared_sidecars = _host_ownership(
                records, config=config, store=store, raw_reader=raw_reader,
                published_reader=published_reader, label=label,
            )
        for path in raw_files + published_files:
            media_owners = (raw_owners if path in raw_reader.snapshot else published_owners).get(path, set())
            metadata_owners = episode_sidecars.get(path, set())
            shared_owners = shared_sidecars.get(path, set())
            populated = [owners for owners in (media_owners, metadata_owners, shared_owners) if owners]
            owners = populated[0] if len(populated) == 1 else set()
            if len(owners) != 1:
                unknown = True
            else:
                counts['owned_files'] += 1
                if media_owners:
                    present = present or target in media_owners
                elif metadata_owners:
                    counts['owned_episode_metadata'] += 1
                else:
                    counts['owned_shared_metadata'] += 1
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
        if not raw_reader.unchanged() or not published_reader.unchanged():
            raise _Unknown()
        # Any uncertainty blocks submission even if another inventory proves presence.
        if unknown:
            return {'state': 'unknown', 'reason': 'submission_inventory_ownership_unknown', 'counts': counts}
        return {'state': 'present' if present else 'missing',
                'reason': 'submission_inventory_present' if present else 'submission_inventory_missing', 'counts': counts}
    except Exception:
        return {'state': 'unknown', 'reason': 'submission_inventory_' + stage + '_unknown', 'counts': counts}
