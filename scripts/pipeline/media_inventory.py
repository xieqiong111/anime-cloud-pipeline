"""Read-only, quarter-scoped submission inventory proof; never infer ownership by name."""
from collections.abc import Mapping
import base64
import hashlib
import json
import os
from pathlib import Path
import posixpath
import re
import stat
import sqlite3
from urllib.parse import unquote, urlsplit

from .evidence import SidecarEvidence, StrmEvidence
from .paths import PathMapper, PathMappingError, _remote_file_id_token, season_label_from_air_date
from .safe_diagnostics import safe_exception_diagnostics


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
    file_columns = {row[1] for row in store.conn.execute('PRAGMA table_info(download_files)')}
    size_projection = 'f.size AS inventory_file_size' if 'size' in file_columns else 'NULL AS inventory_file_size'
    fingerprint_projection = ('f.fingerprint AS inventory_file_fingerprint'
                              if 'fingerprint' in file_columns
                              else 'NULL AS inventory_file_fingerprint')
    cursor = store.conn.execute('''
        SELECT e.*, f.remote_file_id AS inventory_file_id,
               f.path AS inventory_file_path,
               ''' + size_projection + ', ' + fingerprint_projection + '''
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
        unpublished_raw = False
        if published in published_reader.snapshot:
            evidence = StrmEvidence(published_reader).validate(published)
            if not evidence.ok:
                raise _Unknown()
            published_evidence[published] = evidence.detail['content_sha256']
        if raw in raw_reader.snapshot:
            evidence = StrmEvidence(raw_reader).validate(raw)
            if not evidence.ok:
                raise _Unknown()
            if published not in published_evidence:
                # A current LitePan scan can materialize raw before this pass
                # publishes the canonical STRM. Defer ownership to the exact
                # file-ID/history proof below; raw presence alone is not a
                # current published result and must not set `present`.
                unpublished_raw = True
            elif evidence.detail['content_sha256'] != published_evidence[published]:
                raise _Unknown()
        if not unpublished_raw:
            associate(raw_owners, raw, owner)

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
                       for name in ('fanart.jpg', 'backdrop.jpg', 'logo.png')]
        if 'season_nfo' in roles:
            extras.append((posixpath.join(work_dir, f'season{owner[2]:02d}-poster.jpg'),
                           (owner[0], owner[1], owner[2])))
        for path, scope in extras:
            if path in published_reader.snapshot:
                if not published_reader.read(path):
                    raise _Unknown()
                associate(shared_sidecars, path, scope)
    return raw_owners, published_owners, episode_sidecars, shared_sidecars


def _litepan_file_reference(content):
    """Decode only the observed unsigned, file-ID based LitePan format.

    The context includes the token and stays in memory; callers must never
    serialize it. Path-based/signed URLs and query/fragment variants are not
    ownership evidence for this narrow recovery path.
    """
    try:
        text = content.decode('utf-8') if isinstance(content, bytes) else content
        if not isinstance(text, str):
            return None
        text = text.rstrip('\r\n')
        if len(text) > 8192 or any(ord(c) <= 32 for c in text):
            return None
        url = urlsplit(text)
        if (url.scheme not in {'http', 'https'} or not url.hostname
                or url.username is not None or url.password is not None
                or '?' in text or '#' in text):
            return None
        match = re.fullmatch(
            r'/api/strm/play/([1-9][0-9]{0,18})/([A-Za-z0-9_-]{1,80})'
            r'/t/([^/\s]{1,512})/n/([^/?#\s]{1,2048})', url.path,
        )
        if match is None:
            return None
        key = match[2]
        decoded = base64.b64decode(key + '=' * ((-len(key)) % 4),
                                   altchars=b'-_', validate=True)
        file_id = decoded.decode('ascii')
        if (not re.fullmatch(r'[1-9][0-9]{0,24}', file_id)
                or base64.urlsafe_b64encode(decoded).decode().rstrip('=') != key):
            return None
        name = unquote(match[4], errors='strict')
        if not name or '/' in name or '\\' in name or any(ord(c) < 32 for c in name):
            return None
        port = url.port if url.port is not None else (443 if url.scheme == 'https' else 80)
        if port == 0:
            return None
        context = (url.scheme, url.hostname.casefold(), port, match[1], match[3])
        return context, file_id, name
    except (ValueError, UnicodeError):
        return None


def _record_runtime_identity(record):
    value = record.get('runtime_identity') or record.get('runtime_identity_json') or {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return {}
    return value if isinstance(value, Mapping) else {}


def _strict_positive_size(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, str) and value.isascii() and value.isdigit():
        normalized = int(value)
        return normalized if normalized > 0 else None
    return None


def _valid_cloud_name_intent(record, runtime, intent, *, intermediate=False):
    """Check the fixed v1 rename identity against selected V5 inventory state."""
    if (not isinstance(runtime, Mapping) or not isinstance(intent, Mapping)
            or type(intent.get('version')) is not int or intent.get('version') != 1
            or intent.get('rename_verified') is not True
            or intent.get('original_status') not in (
                {'cloud_ready', 'organize_pending'} if intermediate
                else {'library_ready', 'notified'})):
        return False
    source, old, current = (intent.get('source_path'), intent.get('from_cloud_path'),
                            intent.get('to_cloud_path'))
    if (source != record.get('inventory_file_path')
            or current != record.get('cloud_path')
            or not all(isinstance(value, str) for value in (source, old, current))):
        return False
    for value in (source, old, current):
        if (not value.startswith('/') or value.startswith('//') or '\\' in value
                or value != posixpath.normpath(value) or '?' in value or '#' in value):
            return False
    file_id = intent.get('file_id')
    if isinstance(file_id, bool) or not isinstance(file_id, (str, int)):
        return False
    file_id = str(file_id)
    if (not re.fullmatch(r'[1-9][0-9]{0,24}', file_id)
            or str(record.get('inventory_file_id') or '') != file_id
            or str(runtime.get('remote_file_id') or '') != file_id
            or record.get('file_id') not in (None, '', intent.get('file_id'), file_id)):
        return False
    old_parent, current_parent = posixpath.dirname(old), posixpath.dirname(current)
    if (old_parent != current_parent or posixpath.basename(old) in {'', '.', '..'}
            or posixpath.basename(current) in {'', '.', '..'}
            or posixpath.basename(old) == posixpath.basename(current)):
        return False
    try:
        token = _remote_file_id_token(intent.get('file_id'))
    except (PathMappingError, TypeError, ValueError):
        return False
    if posixpath.basename(old_parent) != token:
        return False
    directory_id = intent.get('directory_id')
    target_directory_id = runtime.get('_target_directory_id')
    if (isinstance(directory_id, bool) or directory_id in (None, '')
            or isinstance(target_directory_id, bool) or target_directory_id in (None, '')
            or str(directory_id) != str(target_directory_id)):
        return False
    size, fingerprint = intent.get('size'), intent.get('fingerprint')
    if (isinstance(size, bool) or not isinstance(size, int) or size <= 0
            or not isinstance(fingerprint, str) or not fingerprint.strip()
            or _strict_positive_size(record.get('inventory_file_size')) != size
            or record.get('inventory_file_fingerprint') != fingerprint
            or _strict_positive_size(runtime.get('size')) != size
            or runtime.get('fingerprint') != fingerprint):
        return False
    if intermediate:
        # A freshly moved file can be scanned under its original name before
        # rename recovery. This is ownership only, never published replacement.
        return (posixpath.basename(old) == posixpath.basename(source)
                and intent.get('published_sha256_before') is None)
    digest = intent.get('published_sha256_before')
    return isinstance(digest, str) and re.fullmatch(r'[0-9a-f]{64}', digest) is not None


def _valid_cloud_layout_intent(record, runtime, intent):
    """Validate a sealed fid-to-Season move without rewriting its v1 name proof."""
    if (not isinstance(record, Mapping) or not isinstance(runtime, Mapping)
            or not isinstance(intent, Mapping)
            or type(intent.get('version')) is not int or intent.get('version') != 1
            or type(intent.get('verified')) is not bool or intent.get('verified') is not True
            or type(intent.get('staging_archive_verified')) is not bool
            or intent.get('staging_archive_verified') is not True
            or intent.get('original_status') not in {
                'library_ready', 'notified', 'organized', 'cloud_ready',
                'organize_pending', 'strm_pending', 'strm_ready',
                'scrape_pending', 'scraping', 'metadata_ready', 'library_pending',
            }):
        return False

    source, from_path, to_path = (
        intent.get('source_path'), intent.get('from_cloud_path'),
        intent.get('to_cloud_path'),
    )
    if (not all(isinstance(value, str) for value in (source, from_path, to_path))
            or source != record.get('inventory_file_path')
            or to_path != record.get('cloud_path')):
        return False
    for value in (source, from_path, to_path):
        if (not value.startswith('/') or value.startswith('//') or '\\' in value
                or value != posixpath.normpath(value) or '?' in value or '#' in value):
            return False

    file_id = intent.get('file_id')
    if (not isinstance(file_id, str) or not re.fullmatch(r'[1-9][0-9]{0,24}', file_id)
            or str(record.get('inventory_file_id') or '') != file_id
            or str(runtime.get('remote_file_id') or '') != file_id
            or record.get('file_id') not in (None, '', file_id)):
        return False
    size = intent.get('size')
    fingerprint = intent.get('fingerprint')
    if (type(size) is not int or size <= 0
            or not isinstance(fingerprint, str) or not fingerprint.strip()
            or _strict_positive_size(record.get('inventory_file_size')) != size
            or _strict_positive_size(runtime.get('size')) != size
            or record.get('inventory_file_fingerprint') != fingerprint
            or runtime.get('fingerprint') != fingerprint):
        return False

    from_directory_id = intent.get('from_directory_id')
    to_directory_id = intent.get('to_directory_id')
    archive_directory_id = intent.get('archive_directory_id')
    # A deleted staging directory was never archived, so it has no archive
    # identity; only its own staging ID and the final Season ID are required.
    deleted_staging = intent.get('staging_archive_state') == 'deleted'
    required_directories = ((from_directory_id, to_directory_id)
                            if deleted_staging else
                            (from_directory_id, to_directory_id, archive_directory_id))
    if (not all(isinstance(value, str) and value.strip()
                for value in required_directories)
            or (not deleted_staging
                and not isinstance(archive_directory_id, str))
            or runtime.get('_target_directory_id') != to_directory_id):
        return False

    # The old intent remains a verbatim historical witness. Its final fid
    # path is the source of this layout edge; its directory ID is read from
    # the old intent/history, never from today's final-directory runtime ID.
    name_intent = intent.get('name_intent')
    runtime_name_intent = runtime.get('_cloud_name_intent')
    if (not isinstance(name_intent, Mapping)
            or not isinstance(runtime_name_intent, Mapping)
            or dict(name_intent) != dict(runtime_name_intent)
            or type(name_intent.get('version')) is not int
            or name_intent.get('version') != 1
            or name_intent.get('rename_verified') is not True
            or name_intent.get('source_path') != source
            or name_intent.get('to_cloud_path') != from_path
            or str(name_intent.get('file_id') or '') != file_id
            or type(name_intent.get('size')) is not int
            or name_intent.get('size') != size
            or name_intent.get('fingerprint') != fingerprint
            or name_intent.get('directory_id') != from_directory_id
            or name_intent.get('original_status') not in {
                'library_ready', 'notified', 'organized', 'cloud_ready',
                'organize_pending', 'strm_pending', 'strm_ready',
                'scrape_pending', 'scraping', 'metadata_ready', 'library_pending',
            }):
        return False

    name_from, name_to = name_intent.get('from_cloud_path'), name_intent.get('to_cloud_path')
    if not isinstance(name_from, str) or not isinstance(name_to, str):
        return False
    for value in (name_from, name_to):
        if (not value.startswith('/') or value.startswith('//') or '\\' in value
                or value != posixpath.normpath(value) or '?' in value or '#' in value):
            return False
    try:
        token = _remote_file_id_token(file_id)
    except (PathMappingError, TypeError, ValueError):
        return False
    season_dir = posixpath.dirname(to_path)
    fid_dir = posixpath.dirname(from_path)
    if (from_path != name_to or posixpath.dirname(name_from) != fid_dir
            or posixpath.dirname(name_to) != fid_dir
            or posixpath.basename(fid_dir) != token
            or posixpath.basename(name_from) in {'', '.', '..'}
            or posixpath.basename(to_path) != posixpath.basename(from_path)
            or season_dir != posixpath.dirname(fid_dir)
            or re.fullmatch(r'Season [0-9]+', posixpath.basename(season_dir)) is None):
        return False
    digest = name_intent.get('published_sha256_before')
    if digest is not None and (not isinstance(digest, str)
                               or re.fullmatch(r'[0-9a-f]{64}', digest) is None):
        return False
    if (name_intent.get('original_status') in {'library_ready', 'notified'}
            and posixpath.basename(name_from) != posixpath.basename(name_to)
            and (not isinstance(digest, str)
                 or re.fullmatch(r'[0-9a-f]{64}', digest) is None)):
        return False

    # Archive the emptied per-file fid directory below the quarter's private
    # staging root. The selected video itself must already be flat.
    if deleted_staging:
        # Deleting the emptied fid directory replaces archiving; the staging
        # archive path stays recorded as the route that was not taken.
        archive_path = intent.get('staging_archive_path')
        if not isinstance(archive_path, str) or not archive_path.startswith('/'):
            return False
        components = from_path.strip('/').split('/')
        quarter_indexes = [index for index, part in enumerate(components)
                           if re.fullmatch(r'[0-9]{4}-(?:01|04|07|10)月新番', part)]
        if len(quarter_indexes) != 1:
            return False
        token_expected = '/' + '/'.join(
            components[:quarter_indexes[0] + 1] + ['.pipeline-staging', token]
        )
        return archive_path == token_expected

    archive_path = intent.get('staging_archive_path')
    if (not isinstance(archive_path, str) or not archive_path.startswith('/')
            or archive_path.startswith('//') or '\\' in archive_path
            or archive_path != posixpath.normpath(archive_path)
            or '?' in archive_path or '#' in archive_path):
        return False
    components = from_path.strip('/').split('/')
    quarter_indexes = [index for index, part in enumerate(components)
                       if re.fullmatch(r'[0-9]{4}-(?:01|04|07|10)月新番', part)]
    if len(quarter_indexes) != 1:
        return False
    quarter_index = quarter_indexes[0]
    expected_archive = '/' + '/'.join(
        components[:quarter_index + 1] + ['.pipeline-staging', token]
    )
    return archive_path == expected_archive


def _cloud_layout_history_closed(store, record, runtime, intent, cap):
    """Require the exact old name closure and the later verified layout closure."""
    if (isinstance(cap, bool) or not isinstance(cap, int) or cap < 1
            or not _valid_cloud_layout_intent(record, runtime, intent)):
        return False
    conn = getattr(store, 'conn', store)
    if not callable(getattr(conn, 'execute', None)):
        return False
    episode_id = record.get('id')
    if isinstance(episode_id, bool) or not isinstance(episode_id, int):
        return False
    try:
        rows = conn.execute(
            'SELECT id,to_status,detail_json FROM stage_history '
            'WHERE episode_id=? ORDER BY id LIMIT ?', (episode_id, cap + 1),
        ).fetchall()
    except Exception:
        return False
    if len(rows) > cap:
        return False

    events = []
    for event_id, status, raw in rows:
        try:
            detail = json.loads(raw)
        except (TypeError, ValueError):
            return False
        if isinstance(detail, Mapping):
            events.append((event_id, status, detail))

    source = intent['source_path']
    file_id = intent['file_id']
    from_directory_id = intent['from_directory_id']
    to_directory_id = intent['to_directory_id']
    name_intent = intent['name_intent']
    layout_intent = dict(intent)

    def identity(detail):
        value = detail.get('identity')
        return value if isinstance(value, Mapping) else None

    def closes_name(detail):
        value = identity(detail)
        return (
            detail.get('source_path') == source
            and detail.get('cloud_path') == intent['from_cloud_path']
            and value is not None
            and str(value.get('remote_file_id') or '') == file_id
            and value.get('_target_directory_id') == from_directory_id
            and value.get('_cloud_name_intent') == name_intent
        )

    def same_layout(left):
        if not isinstance(left, Mapping):
            return False
        actual, expected = dict(left), dict(layout_intent)
        for key in ('verified', 'staging_archive_verified'):
            if type(actual.get(key)) is not bool:
                return False
            actual.pop(key, None)
            expected.pop(key, None)
        actual.pop('staging_archive_state', None)
        expected.pop('staging_archive_state', None)
        return actual == expected

    layout_pending = []
    layout_organized = []
    for event_id, status, detail in events:
        value = identity(detail)
        if (detail.get('source_path') != source
                or detail.get('cloud_path') != intent['to_cloud_path']
                or value is None
                or str(value.get('remote_file_id') or '') != file_id
                or value.get('_target_directory_id') != to_directory_id
                or value.get('_cloud_name_intent') != name_intent
                or not same_layout(value.get('_cloud_layout_intent'))):
            continue
        if status == 'organize_pending':
            layout_pending.append(event_id)
        elif (status == 'organized'
              and same_layout(value.get('_cloud_layout_intent'))):
            layout_organized.append(event_id)

    for pending_id in layout_pending:
        closing = [event_id for event_id in layout_organized if event_id > pending_id]
        if not closing:
            continue
        old_pending = [event_id for event_id, status, detail in events
                       if event_id < pending_id and status == 'organize_pending'
                       and closes_name(detail)]
        old_organized = [event_id for event_id, status, detail in events
                         if event_id < pending_id and status == 'organized'
                         and closes_name(detail)]
        if any(old_wait < old_done < pending_id
               for old_wait in old_pending for old_done in old_organized):
            return True
    return False


def _historical_raw_owners(records, *, config, store, cloud_files, raw_reader, raw_owners):
    """Prove moved or newly scanned raw projections; never prove presence."""
    mapper = PathMapper(config.paths)
    claims = {}
    for record in records:
        original, current = record.get('inventory_file_path'), record.get('cloud_path')
        if (not original or not current
                or not _within(original, config.paths.cloud_source_root)
                or not _within(original, config.runtime.litepan_task_source_root)
                or not _within(current, config.runtime.litepan_task_source_root)):
            continue
        if original != current:
            old_path = mapper.cloud_to_raw_litepan(
                original, config.paths.litepan_raw_output_root
            )
            if old_path in raw_reader.snapshot and not raw_owners.get(old_path):
                claims.setdefault(old_path, []).append((record, 'source_projection'))
        current_path = mapper.cloud_to_raw_litepan(
            current, config.paths.litepan_raw_output_root
        )
        if current_path in raw_reader.snapshot and not raw_owners.get(current_path):
            claims.setdefault(current_path, []).append((record, 'current_projection'))
        runtime = _record_runtime_identity(record)
        intent = runtime.get('_cloud_name_intent')
        terminal_rename = _valid_cloud_name_intent(record, runtime, intent)
        intermediate_rename = _valid_cloud_name_intent(record, runtime, intent, intermediate=True)
        if terminal_rename or intermediate_rename:
            from_path = intent['from_cloud_path']
            old_path = mapper.cloud_to_raw_litepan(
                from_path, config.paths.litepan_raw_output_root
            )
            source_projection = mapper.cloud_to_raw_litepan(
                original, config.paths.litepan_raw_output_root
            )
            if (old_path not in {source_projection, current_path}
                    and old_path in raw_reader.snapshot and not raw_owners.get(old_path)):
                claims.setdefault(old_path, []).append((record,
                    'rename_projection' if terminal_rename else 'intermediate_rename_projection'))
        layout_intent = runtime.get('_cloud_layout_intent')
        layout_proven = (
            _valid_cloud_layout_intent(record, runtime, layout_intent)
            and _cloud_layout_history_closed(
                store, record, runtime, layout_intent,
                config.limits.max_recursive_files,
            )
        )
        if layout_proven:
            historical_paths = {
                layout_intent['from_cloud_path']: 'layout_source_projection',
                layout_intent['name_intent']['from_cloud_path']:
                    'layout_original_name_projection',
            }
            for historical_cloud_path, projection in historical_paths.items():
                historical_raw_path = mapper.cloud_to_raw_litepan(
                    historical_cloud_path, config.paths.litepan_raw_output_root
                )
                if (historical_cloud_path not in {original, current}
                        and historical_raw_path in raw_reader.snapshot
                        and not raw_owners.get(historical_raw_path)):
                    claims.setdefault(historical_raw_path, []).append((record, projection))
    if not claims:
        return {}

    db_path = Path(config.runtime.litepan_task_db_path)
    if not db_path.is_absolute() or db_path.is_symlink() or not db_path.is_file():
        raise _Unknown()
    with sqlite3.connect(db_path.as_uri() + '?mode=ro', uri=True, timeout=5) as db:
        db.execute('PRAGMA query_only=ON')
        tasks = db.execute(
            'SELECT account_id,path,output_folder,group_dir,status,recursive FROM strm_tasks WHERE name=? LIMIT 2',
            (config.runtime.litepan_task_name,),
        ).fetchall()
    if len(tasks) != 1:
        raise _Unknown()
    account, source, folder, group, status, recursive = tasks[0]
    if (type(account) is not int or account <= 0
            or source != config.runtime.litepan_task_source_root
            or status != 'active' or type(recursive) is not int or recursive != 1):
        raise _Unknown()
    components = [str(p or '') for p in (group, folder)]
    if any(p in {'.', '..'} or '/' in p or '\\' in p for p in components):
        raise _Unknown()
    output = posixpath.join(config.runtime.litepan_strm_host_root,
                            *(p for p in components if p))
    if output != raw_reader.root or output != config.runtime.litepan_task_output_root:
        raise _Unknown()

    by_owner = {}
    for record in records:
        by_owner.setdefault(_identity(record), []).append(record)
    trusted_contexts = set()
    for path, owners in raw_owners.items():
        if len(owners) != 1 or path not in raw_reader.snapshot:
            continue
        selected = by_owner.get(next(iter(owners)), [])
        ref = _litepan_file_reference(raw_reader.read(path))
        if (len(selected) == 1 and ref is not None and ref[0][3] == str(account)
                and ref[1] == str(selected[0]['inventory_file_id'])):
            trusted_contexts.add(ref[0])
    if len(trusted_contexts) != 1:
        return {}

    historical = {}
    cap = config.limits.max_recursive_files
    for path, candidates in claims.items():
        if len(candidates) != 1:
            continue
        record, projection = candidates[0]
        file_id = str(record['inventory_file_id'])
        raw_content = raw_reader.read(path)
        ref = _litepan_file_reference(raw_content)
        if projection == 'source_projection':
            expected_name_path = record['inventory_file_path']
        elif projection == 'current_projection':
            expected_name_path = record['cloud_path']
        else:
            expected_name_path = None
        runtime = _record_runtime_identity(record)
        intent = runtime.get('_cloud_name_intent')
        is_rename = projection in {'rename_projection', 'intermediate_rename_projection'}
        is_layout = projection in {
            'layout_source_projection', 'layout_original_name_projection',
        }
        if is_rename:
            if not _valid_cloud_name_intent(record, runtime, intent,
                    intermediate=projection == 'intermediate_rename_projection'):
                continue
            expected_name_path = intent['from_cloud_path']
            if (projection == 'rename_projection'
                    and hashlib.sha256(raw_content.rstrip(b'\r\n')).hexdigest()
                        != intent['published_sha256_before']):
                continue
        elif is_layout:
            layout_intent = runtime.get('_cloud_layout_intent')
            if (not _valid_cloud_layout_intent(record, runtime, layout_intent)
                    or not _cloud_layout_history_closed(
                        store, record, runtime, layout_intent, cap,
                    )):
                continue
            if projection == 'layout_source_projection':
                expected_name_path = layout_intent['from_cloud_path']
            else:
                expected_name_path = layout_intent['name_intent']['from_cloud_path']
                digest = layout_intent['name_intent'].get('published_sha256_before')
                if (isinstance(digest, str) and hashlib.sha256(
                        raw_content.rstrip(b'\r\n')).hexdigest() != digest):
                    continue
        if (ref is None or ref[0] not in trusted_contexts or ref[1] != file_id
                or ref[2] != posixpath.basename(expected_name_path)
                or not isinstance(runtime, Mapping)
                or str(runtime.get('remote_file_id') or '') != file_id
                or not runtime.get('_target_directory_id')):
            continue
        matches = [f for f in cloud_files if isinstance(f, Mapping)
                   and str(f.get('remote_file_id', f.get('file_id'))) == file_id
                   and f.get('path') == record['cloud_path']]
        if len(matches) != 1:
            continue
        if is_layout:
            current = matches[0]
            layout_intent = runtime['_cloud_layout_intent']
            if (str(current.get('parent_id') or '') != layout_intent['to_directory_id']
                    or _strict_positive_size(current.get('size')) != layout_intent['size']
                    or current.get('fingerprint') != layout_intent['fingerprint']):
                continue
        if is_rename:
            current = matches[0]
            if (record.get('cloud_path') != intent['to_cloud_path']
                    or record.get('inventory_file_path') != intent['source_path']
                    or _strict_positive_size(record.get('inventory_file_size')) != intent['size']
                    or record.get('inventory_file_fingerprint') != intent['fingerprint']
                    or _remote_file_id_token(file_id) != posixpath.basename(
                        posixpath.dirname(intent['from_cloud_path'])
                    )
                    or _remote_file_id_token(file_id) != posixpath.basename(
                        posixpath.dirname(intent['to_cloud_path'])
                    )
                    or _strict_positive_size(current.get('size')) != intent['size']
                    or current.get('fingerprint') != intent['fingerprint']):
                continue
        history = store.conn.execute(
            'SELECT id,to_status,detail_json FROM stage_history WHERE episode_id=? ORDER BY id LIMIT ?',
            (record['id'], cap + 1),
        ).fetchall()
        if len(history) > cap:
            raise _Unknown()
        pending, organized = [], []
        for event_id, status, raw in history:
            detail = json.loads(raw)
            if not isinstance(detail, Mapping):
                continue
            identity = detail.get('identity')
            closes_identity = (
                detail.get('source_path') == record['inventory_file_path']
                and detail.get('cloud_path') == record['cloud_path']
                and isinstance(identity, Mapping)
                and str(identity.get('remote_file_id') or '') == file_id
                and str(identity.get('_target_directory_id') or '')
                == str(runtime['_target_directory_id'])
                and (not is_rename or identity.get('_cloud_name_intent') == intent)
            )
            if status == 'organize_pending' and closes_identity:
                pending.append(event_id)
            if status == 'organized':
                if is_rename:
                    if closes_identity:
                        organized.append(event_id)
                else:
                    organized.append(event_id)
        if pending and organized and max(organized) > min(pending):
            historical[path] = _identity(record)
    return historical


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
        raw_only_owners = {}
        if raw_files or published_files:
            raw_owners, published_owners, episode_sidecars, shared_sidecars = _host_ownership(
                records, config=config, store=store, raw_reader=raw_reader,
                published_reader=published_reader, label=label,
            )
            raw_only_owners = _historical_raw_owners(
                records, config=config, store=store, cloud_files=files,
                raw_reader=raw_reader, raw_owners=raw_owners,
            )
        for path in raw_files + published_files:
            if path in raw_only_owners:
                # A raw-only projection has proven provenance,
                # but cannot prove current presence, STRM readiness or import.
                counts['owned_files'] += 1
                continue
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
            return {'state': 'unknown', 'reason': 'submission_inventory_ownership_unknown',
                    'inventory_stage': stage, 'counts': counts}
        return {'state': 'present' if present else 'missing',
                'reason': 'submission_inventory_present' if present else 'submission_inventory_missing',
                'inventory_stage': stage, 'counts': counts}
    except Exception as exc:
        diagnostics = safe_exception_diagnostics(exc, stage=stage)
        return {'state': 'unknown', 'reason': 'submission_inventory_' + stage + '_unknown',
                'inventory_stage': stage, 'counts': counts, **diagnostics}
