# Hermes NAS deployment

This runbook covers a compatible Linux NAS with POSIX `sh`, Python 3.12 or
newer, SQLite support, a writable application-data root, and an operator-owned
environment file. It documents the package and local/POSIX checks; it does not
claim that any NAS, mount, service, credential, or media library is available.

## 1. Contract and ownership

Hermes operates one immutable release at a time through:

```text
<install-root>/current/bin/anime-cloud-pipeline <command> [arguments]
```

The release tree is code-only. The deployment owner (normally root or a
separate release-management account) owns `releases/<release-id>`, `bin/`,
`current`, and `previous`; published POSIX release files are `0444`, release
directories are `0555`, and entrypoints are `0555`. A dedicated service
account gets read/execute access to that tree but owns only the mutable
`config/`, `state/`, `logs/`, and `backups/` directories as required. Do not
use `chown -R` on the install root after installation, because that would put
the release owner and service account back into the same ownership boundary.
The environment file must be a regular non-symlink file with mode `0600` or
stricter; never put its values in command output, reports, or shell tracing.

POSIX modes prevent writes by the service account only when ownership is
separated. The release owner/root can still deliberately restore write bits or
change ownership, so this is an ownership boundary, not a claim of immutable
storage. Windows does not provide an equivalent portable owner-mode guarantee;
verify native ACLs separately and do not infer POSIX immutability from a local
Windows extraction.

The install root has this shape after the first successful install:

```text
<install-root>/
├── current -> releases/<release-id>
├── previous -> releases/<previous-release-id>   # optional
├── releases/<release-id>/                        # immutable code and manifest
├── config/config.v5.json
├── config/anime-cloud-pipeline.env
├── state/
├── logs/
└── backups/
```

`current` and `previous` are release anchors. Activation changes links only;
it does not migrate SQLite, enable cron, or call a remote service.

All angle-bracket values in this runbook are semantic deployment inputs, not
paths to copy from a development machine. Resolve them to normalized absolute
paths only at execution time. Pseudocode tests and local checks that need an
absolute filesystem path must allocate a disposable fixture root for the run
and clean it up afterward; they must not embed a workstation, user-profile,
NAS, or mount path. Use `<artifact-output>` for archives and reports written
outside the source tree.

## 2. Extract and verify the release

Extract the delivery archive into a disposable staging parent. The archive
must contain exactly one `anime-cloud-pipeline-hermes/` top-level directory.

```sh
STAGING_PARENT=<artifact-staging-root>
unzip -q <release-archive>.zip -d "$STAGING_PARENT"
STAGING_ROOT="$STAGING_PARENT/anime-cloud-pipeline-hermes"
cd "$STAGING_ROOT"
sha256sum -c MANIFEST.sha256
```

The manifest is the authoritative byte check; it must cover the extracted
product files and must not cover itself. The installer repeats manifest and
path checks, rejects symlinks and forbidden runtime files, and will not
overwrite an existing release. Stop if the archive has extra top-level paths,
an invalid manifest, a database/log/cache, or a packaged config/environment.

## 3. Prepare configuration without exposing secrets

Set placeholders for the authorized paths and account before running the
commands below. Every path passed to the installer must be absolute and
normalized.

```sh
INSTALL_ROOT=<install-root>
RELEASE_ID=<release-id>
CONFIG_FILE="$INSTALL_ROOT/config/config.v5.json"
ENV_FILE="$INSTALL_ROOT/config/anime-cloud-pipeline.env"

mkdir -p "$INSTALL_ROOT/config" "$INSTALL_ROOT/state" "$INSTALL_ROOT/logs" "$INSTALL_ROOT/backups"
test ! -e "$CONFIG_FILE"
test ! -e "$ENV_FILE"
cp "$STAGING_ROOT/deploy/config.v5.example.json" "$CONFIG_FILE"
cp "$STAGING_ROOT/deploy/anime-cloud-pipeline.env.example" "$ENV_FILE"
chmod 600 "$ENV_FILE"
```

Edit the copied files with a secret-safe editor. Replace example service roots,
mount roots, names, and environment-variable references with the authorized
deployment values; do not put credential values in JSON. Keep
`config.paths.state_directory` as the single source of truth for the database.
The pipeline database is `<config.paths.state_directory>/state.db`, and the
runner lock is `<config.paths.state_directory>/run.lock`.

The wrapper consumes `ANIME_PIPELINE_CONFIG` to select the external config.
`ANIME_PIPELINE_STATE_DIR`, `ANIME_PIPELINE_LOG_DIR`, and
`ANIME_PIPELINE_LOCK_PATH` describe external layout for Hermes `doctor`; they
are deployment diagnostics and do not redirect `run_pipeline.py` state or
locking. Set the state diagnostic to the same directory as
`config.paths.state_directory`, and set the lock diagnostic to that directory's
`run.lock`, so `doctor` checks the paths the runner actually uses. `P115_ROOT_DIRECTORY_ID`
and service credential variables are required by name, but their values must
never be printed. `ANIME_PIPELINE_PYTHON` is intentionally not a dotenv
assignment: if `python3` is unavailable, the scheduler or launcher process
must export an absolute Python 3.12+ interpreter path before invoking the
entrypoint, for example `ANIME_PIPELINE_PYTHON=<absolute-python-path>`. The
wrapper never sources or evaluates the dotenv file to select Python.

Create or verify ownership and permissions after editing, without displaying
the environment file:

```sh
chown <deployment-user>:<deployment-group> "$INSTALL_ROOT"
chown -R <deployment-user>:<deployment-group> "$INSTALL_ROOT/releases"
chown -h <deployment-user>:<deployment-group> "$INSTALL_ROOT/current" "$INSTALL_ROOT/previous"
chown -R <service-user>:<service-group> "$INSTALL_ROOT/config" "$INSTALL_ROOT/state" "$INSTALL_ROOT/logs" "$INSTALL_ROOT/backups"
chmod 700 "$INSTALL_ROOT/state" "$INSTALL_ROOT/logs" "$INSTALL_ROOT/backups"
chmod 600 "$ENV_FILE"
```

If `current` or `previous` is a symlink, use the platform's no-follow owner
operation (for example `chown -h`) rather than changing the release target.
Run the installer and activation/rollback commands as the deployment owner or
root; the service account should only run the installed pipeline entrypoint.

## 4. Install without activation

Keep the existing `current` link untouched while staging a release. The
installer creates `releases/`, `state/`, `logs/`, and `backups/` as needed,
copies an immutable release, and refuses to replace an existing release with
different bytes.

```sh
sh "$STAGING_ROOT/deploy/install.sh" \
  --source "$STAGING_ROOT" \
  --install-root "$INSTALL_ROOT" \
  --config "$CONFIG_FILE" \
  --env-file "$ENV_FILE" \
  --release-id "$RELEASE_ID"
```

Run candidate checks against the installed release path before activation. The
candidate wrapper accepts `--env-file` before its command and emits only safe
diagnostics:

```sh
CANDIDATE="$INSTALL_ROOT/releases/$RELEASE_ID"
"$CANDIDATE/bin/anime-cloud-pipeline" --env-file "$ENV_FILE" version
"$CANDIDATE/bin/anime-cloud-pipeline" --env-file "$ENV_FILE" doctor
```

`doctor` checks the manifest, file identity and permissions, external layout,
Python/SQLite, required variable names, `preflight`, and migration check-only.
The preflight reads the configured 115 source directory and local LitePan task
and webhook inventories; it does not mutate an external service. A failed check must
leave `current` unchanged; fix the staged configuration and rerun the check.

## 5. Read-only gate and explicit activation

After the candidate is installed and its diagnostics are accepted, use the
stable installed entrypoint for the read-only gate:

```sh
"$CANDIDATE/bin/anime-cloud-pipeline" doctor
"$CANDIDATE/bin/anime-cloud-pipeline" preflight
"$CANDIDATE/bin/anime-cloud-pipeline" migrate --check-only
"$CANDIDATE/bin/anime-cloud-pipeline" dry-run
```

`migrate --check-only` inspects the database at the configured
`config.paths.state_directory`; it does not apply schema changes. `dry-run` must not
create or modify `state.db`, WAL/SHM companions, or remote state. Review output
and local file ownership before authorizing activation.

Activation is a separate, explicit action. Reusing the same verified source is
idempotent; `--activate` changes `current` atomically and preserves the former
release as `previous`. It does not run database migration or change cron:

```sh
sh "$STAGING_ROOT/deploy/install.sh" \
  --source "$STAGING_ROOT" \
  --install-root "$INSTALL_ROOT" \
  --config "$CONFIG_FILE" \
  --env-file "$ENV_FILE" \
  --release-id "$RELEASE_ID" \
  --activate
```

Confirm the active link and rerun the gate through the stable path:

```sh
"$INSTALL_ROOT/current/bin/anime-cloud-pipeline" version
"$INSTALL_ROOT/current/bin/anime-cloud-pipeline" doctor
"$INSTALL_ROOT/current/bin/anime-cloud-pipeline" preflight
"$INSTALL_ROOT/current/bin/anime-cloud-pipeline" migrate --check-only
"$INSTALL_ROOT/current/bin/anime-cloud-pipeline" dry-run
```

Only after explicit approval and a reviewed backup may the operator apply the
separate writable migration:

```sh
"$INSTALL_ROOT/current/bin/anime-cloud-pipeline" migrate
```

## 6. Bounded authorized validation

Stop the existing scheduler before the first write validation and wait for any
bounded invocation to finish. Use a disposable or explicitly approved state
directory when validating a new deployment. Start with reconciliation when
there is already pending work:

```sh
"$INSTALL_ROOT/current/bin/anime-cloud-pipeline" run --resume-only
```

For an approved discovery window, use one bounded invocation with the existing
CLI option (`--previous-day` or a fully specified `--backfill` range). Do not
replace a bounded invocation with a shell loop or a long-running Hermes job.
After every invocation, inspect the durable state and evidence:

```sh
"$INSTALL_ROOT/current/bin/anime-cloud-pipeline" status
"$INSTALL_ROOT/current/bin/anime-cloud-pipeline" reviews list
"$INSTALL_ROOT/current/bin/anime-cloud-pipeline" audit
```

Accept a live item only when the selected cloud file has complete identity,
size, fingerprint, and canonical path evidence; the STRM points to the
expected source; in-place MoviePilot sidecars are present; and Emby exposes the
matching library path and episode. A successful HTTP response, remote task
acceptance, refresh response, or file existence alone is not acceptance.
Record each check as authorized live evidence, separate from local tests.

## 7. NAS cron is the only production scheduler

Hermes is an operator tool. Do not create a Hermes scheduler, container loop,
or second cron entry. After live validation is accepted, replace the NAS cron
table through its supported atomic import/replace operation. Build a candidate
table outside the active spool, preserve unrelated entries, and ensure exactly
one pipeline line points to `current`:

```text
<approved-schedule> <install-root>/current/bin/anime-cloud-pipeline run >> <log-file> 2>&1
```

For a POSIX crontab, use the packaged `deploy/hermes_cron.py` helper. It matches
the complete ownership marker and the exact direct command/redirection fields;
comments, monitoring or wrapper commands, quoted mentions, and marker-prefix
collisions remain byte-for-byte untouched. Build rejects any unmarked exact or
ambiguous direct invocation, while disable removes only exact canonical command
lines and marker-owned pairs. It appends one bounded command, verifies exactly
one marker and command, and exits before `crontab` on any ambiguity. A
`crontab -l` failure is always a hard stop; never turn it into an empty candidate
through a failure fallback. If the operator confirms
that the failure means this account truly has no crontab, initialize it
explicitly once, then rerun the capture workflow:

```sh
CRON_INIT=<cron-initialization-file>
printf '%s\n' '# operator-confirmed empty crontab' > "$CRON_INIT"
crontab "$CRON_INIT"
rm -f "$CRON_INIT"
```

Do not automate that initialization step: an unreadable or otherwise broken
crontab must not erase unrelated jobs.

```sh
set -eu
INSTALL_ROOT=<install-root>
PYTHON_BIN=<python3>
CRON_HELPER="$INSTALL_ROOT/current/deploy/hermes_cron.py"
CRON_CANDIDATE=<cron-candidate>
CRON_SOURCE="$CRON_CANDIDATE.source"
CRON_CAPTURE_ERROR="$CRON_SOURCE.error"
CRON_MARKER="# anime-cloud-pipeline-managed"
CRON_COMMAND=$("$PYTHON_BIN" -c 'import shlex,sys; print(shlex.join(sys.argv[1:]))' \
  "$INSTALL_ROOT/current/bin/anime-cloud-pipeline" run)
CRON_LOG=<cron-log-file>
CRON_SCHEDULE="<approved-schedule>"

if ! crontab -l > "$CRON_SOURCE" 2> "$CRON_CAPTURE_ERROR"; then
  rm -f "$CRON_SOURCE" "$CRON_CAPTURE_ERROR"
  printf '%s\n' 'crontab capture failed; refusing to modify the active table' >&2
  exit 1
fi
rm -f "$CRON_CAPTURE_ERROR"

"$PYTHON_BIN" "$CRON_HELPER" build \
  --marker "$CRON_MARKER" \
  --command "$CRON_COMMAND" \
  --schedule "$CRON_SCHEDULE" \
  --log-path "$CRON_LOG" \
  < "$CRON_SOURCE" > "$CRON_CANDIDATE"
# The helper exits nonzero unless the candidate has exactly one marker/command.
test -s "$CRON_CANDIDATE"

crontab "$CRON_CANDIDATE"
rm -f "$CRON_CANDIDATE" "$CRON_SOURCE"
```

Use the NAS UI's transactional schedule replacement when it owns crontab.
Verify the installed command, environment, log destination, user, and single
entry after replacement. Never edit a live cron spool in place or enable cron
before the code, config, backup, and bounded validation have been reviewed.

## 8. Status, audit, and review operation

The following commands are safe inspection operations and use the active
configuration automatically:

```sh
"$INSTALL_ROOT/current/bin/anime-cloud-pipeline" status
"$INSTALL_ROOT/current/bin/anime-cloud-pipeline" audit
"$INSTALL_ROOT/current/bin/anime-cloud-pipeline" reviews list
```

An open review blocks the affected stage. Reconcile remote inventory and read
the stored reason before resolving it. `reviews resolve` is a write operation;
submit only reason-specific JSON and the current generation when required:

```sh
"$INSTALL_ROOT/current/bin/anime-cloud-pipeline" reviews resolve \
  --id <review-id> --resolution '<JSON-object>' --generation <generation>
```

Do not infer a missing remote item, reuse a stale review generation, resubmit an
uncertain download, or repeat a move/refresh without durable evidence.

## 9. Upgrade and rollback as one release unit

### Create the matching backup before an upgrade

Disable the marker-owned cron entry and stop the active bounded writer before
touching the database. Wait until no process holds
`<config.paths.state_directory>/run.lock`; never delete the lock to force a
restore. Keep the code, restricted config/environment, and database backup
together. Name the database artifact with the exact code revision it matches:

```sh
set -eu
INSTALL_ROOT=<install-root>
STATE_DIR=<state-directory>
DB_PATH="${STATE_DIR}/state.db"
BACKUP_ROOT=<backup-root>
DB_BACKUP="${BACKUP_ROOT}/state-<release-id>.db"
PYTHON_BIN=<python3>
CRON_DISABLED_TABLE=<cron-candidate>
CRON_CAPTURE_ERROR="$CRON_DISABLED_TABLE.error"
CRON_HELPER="$INSTALL_ROOT/current/deploy/hermes_cron.py"
CRON_MARKER="# anime-cloud-pipeline-managed"
CRON_COMMAND=$("$PYTHON_BIN" -c 'import shlex,sys; print(shlex.join(sys.argv[1:]))' \
  "$INSTALL_ROOT/current/bin/anime-cloud-pipeline" run)
WRITER_STOP_COMMAND="<operator-supplied-writer-stop-command>"

mkdir -p "$BACKUP_ROOT"
test ! -e "$DB_BACKUP"
if ! crontab -l > "$CRON_DISABLED_TABLE" 2> "$CRON_CAPTURE_ERROR"; then
  rm -f "$CRON_DISABLED_TABLE" "$CRON_CAPTURE_ERROR"
  printf '%s\n' 'crontab capture failed; refusing to disable the scheduler' >&2
  exit 1
fi
rm -f "$CRON_CAPTURE_ERROR"
"$PYTHON_BIN" "$CRON_HELPER" disable \
  --marker "$CRON_MARKER" \
  --command "$CRON_COMMAND" \
  < "$CRON_DISABLED_TABLE" > "${CRON_DISABLED_TABLE}.next"
mv "${CRON_DISABLED_TABLE}.next" "$CRON_DISABLED_TABLE"
crontab "$CRON_DISABLED_TABLE"
rm -f "$CRON_DISABLED_TABLE"

# Stop every other process manager/container that can invoke the runner, then
# wait for the current bounded invocation to release "${STATE_DIR}/run.lock".
sh -c "$WRITER_STOP_COMMAND"
command -v fuser >/dev/null 2>&1 || {
  printf '%s\n' 'fuser is required to verify that the runner lock is free' >&2
  exit 1
}
while fuser "${STATE_DIR}/run.lock" >/dev/null 2>&1; do sleep 1; done

if [ -f "$DB_PATH" ]; then
  "$PYTHON_BIN" -B - "$DB_PATH" "$DB_BACKUP" <<'PY'
import sqlite3
import sys

source_path, backup_path = sys.argv[1:]
with sqlite3.connect(source_path) as source, sqlite3.connect(backup_path) as backup:
    source.backup(backup)
PY
  "$PYTHON_BIN" -B - "$DB_BACKUP" <<'PY'
import sqlite3
import sys

with sqlite3.connect(sys.argv[1]) as connection:
    result = connection.execute("PRAGMA integrity_check").fetchone()[0]
if result != "ok":
    raise SystemExit(f"backup integrity check failed: {result}")
PY
  chown <service-user>:<service-group> "$DB_BACKUP"
  chmod 600 "$DB_BACKUP"
  stat "$STATE_DIR" "$DB_BACKUP"
else
  printf '%s\n' 'state database is not initialized; record that no DB backup exists'
fi
```

The Python `sqlite3.Connection.backup` API creates a consistent artifact while
the writer is stopped. Do not raw-copy a live `state.db`, `state.db-wal`, or
`state.db-shm` pair. Keep the backup metadata with the code revision and
restrict its ownership and mode.

Install the new archive without activation, verify its manifest, run candidate
`doctor`, `preflight`, migration check-only, and `dry-run`, then activate only
after review. Apply `migrate` separately, perform one bounded validation, and
replace the single cron command. Keep `previous` until the upgrade is accepted.

### Restore the database, then roll back the links

Disable cron again immediately before restoration, stop every possible writer,
and wait for `run.lock` to be released. Set `DB_BACKUP` to the backup matching
the release that will become active; never restore an unlabelled artifact:

```sh
set -eu
INSTALL_ROOT=<install-root>
STATE_DIR=<state-directory>
DB_PATH="${STATE_DIR}/state.db"
BACKUP_ROOT=<backup-root>
DB_BACKUP="${BACKUP_ROOT}/state-<previous-release-id>.db"
RESTORE_TMP="${STATE_DIR}/.restore-state.db"
QUARANTINE_DIR="${STATE_DIR}/rollback-quarantine-<release-id>"
PYTHON_BIN=<python3>
CRON_DISABLED_TABLE=<cron-candidate>
CRON_CAPTURE_ERROR="$CRON_DISABLED_TABLE.error"
CRON_HELPER="$INSTALL_ROOT/current/deploy/hermes_cron.py"
CRON_MARKER="# anime-cloud-pipeline-managed"
CRON_COMMAND=$("$PYTHON_BIN" -c 'import shlex,sys; print(shlex.join(sys.argv[1:]))' \
  "$INSTALL_ROOT/current/bin/anime-cloud-pipeline" run)
WRITER_STOP_COMMAND="<operator-supplied-writer-stop-command>"

test -f "$DB_BACKUP"
test ! -e "$RESTORE_TMP"
test ! -e "$QUARANTINE_DIR"

if ! crontab -l > "$CRON_DISABLED_TABLE" 2> "$CRON_CAPTURE_ERROR"; then
  rm -f "$CRON_DISABLED_TABLE" "$CRON_CAPTURE_ERROR"
  printf '%s\n' 'crontab capture failed; refusing to disable the scheduler' >&2
  exit 1
fi
rm -f "$CRON_CAPTURE_ERROR"
"$PYTHON_BIN" "$CRON_HELPER" disable \
  --marker "$CRON_MARKER" \
  --command "$CRON_COMMAND" \
  < "$CRON_DISABLED_TABLE" > "${CRON_DISABLED_TABLE}.next"
mv "${CRON_DISABLED_TABLE}.next" "$CRON_DISABLED_TABLE"
crontab "$CRON_DISABLED_TABLE"
rm -f "$CRON_DISABLED_TABLE"

# Stop every other process manager/container that can invoke the runner now,
# and do not continue until that writer reports stopped.
sh -c "$WRITER_STOP_COMMAND"
command -v fuser >/dev/null 2>&1 || {
  printf '%s\n' 'fuser is required to verify that the runner lock is free' >&2
  exit 1
}
while fuser "${STATE_DIR}/run.lock" >/dev/null 2>&1; do sleep 1; done

mkdir "$QUARANTINE_DIR"

"$PYTHON_BIN" -B - "$DB_BACKUP" "$RESTORE_TMP" <<'PY'
import sqlite3
import sys

source_path, restore_path = sys.argv[1:]
with sqlite3.connect(source_path) as backup, sqlite3.connect(restore_path) as target:
    backup.backup(target)
PY
"$PYTHON_BIN" -B - "$RESTORE_TMP" <<'PY'
import sqlite3
import sys

with sqlite3.connect(sys.argv[1]) as connection:
    result = connection.execute("PRAGMA integrity_check").fetchone()[0]
if result != "ok":
    raise SystemExit(f"restore integrity check failed: {result}")
PY

if [ -e "$DB_PATH" ]; then mv "$DB_PATH" "$QUARANTINE_DIR/state.db"; fi
if [ -e "${DB_PATH}-wal" ]; then mv "${DB_PATH}-wal" "$QUARANTINE_DIR/state.db-wal"; fi
if [ -e "${DB_PATH}-shm" ]; then mv "${DB_PATH}-shm" "$QUARANTINE_DIR/state.db-shm"; fi
mv "$RESTORE_TMP" "$DB_PATH"
chown <service-user>:<service-group> "$STATE_DIR" "$DB_PATH"
chmod 700 "$STATE_DIR"
chmod 600 "$DB_PATH"
stat "$STATE_DIR" "$DB_PATH"

sh "$INSTALL_ROOT/current/deploy/rollback.sh" --install-root "$INSTALL_ROOT"
```

The rollback helper now validates the release manifests and swaps `current` and
`previous` atomically; it does not restore SQLite. The stopped-writer window
covers both the database replacement and link rollback. After the link swap,
run the read-only validation through the active entrypoint:

```sh
"$INSTALL_ROOT/current/bin/anime-cloud-pipeline" doctor
"$INSTALL_ROOT/current/bin/anime-cloud-pipeline" migrate --check-only
"$INSTALL_ROOT/current/bin/anime-cloud-pipeline" status
"$INSTALL_ROOT/current/bin/anime-cloud-pipeline" audit
```

Reconcile any external operation that may have happened before rollback before
reenabling the single cron entry. A rollback never authorizes blind resubmission,
duplicate moves, or an unverified library refresh.

## 10. Evidence and deployment limits

Keep command output, manifests, backup metadata, and live acceptance notes
separate from credentials. Local unit tests, archive verification, filesystem
checks, and `doctor` prove only the local/POSIX contract. Before production,
the authorized NAS operator must still verify architecture, ownership/ACLs,
mount visibility, service APIs, scheduler locking, one bounded end-to-end item,
and a code-plus-database rollback drill.
