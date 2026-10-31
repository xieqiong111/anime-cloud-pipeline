# V5 upgrade and rollback runbook

This runbook is for a controlled, side-by-side V5 rollout. The repository and
the state database are one release unit. The commands below use placeholders;
they do not imply that a NAS, service endpoint, mount, or credential has been
verified.

For a Linux NAS, follow the [Hermes NAS deployment runbook](../references/deployment-hermes.md)
for archive extraction, immutable release installation, NAS cron ownership, and
rollback. This document remains the detailed backup and migration rationale for
source-checkout or deployment review.

## 1. Back up the release unit

Before changing an installation, stop the scheduler and wait for any bounded
run to finish. Make two independent backups:

1. the complete code/configuration directory, excluding credentials and
   transient `*.db-wal`, `*.db-shm`, logs, and caches; and
2. the V5 state database together with its matching code revision. Use
   SQLite's backup API while the scheduler is stopped, or checkpoint WAL and
   copy only after the StateStore connection has been closed. A raw copy of a
   live `state.db` is not a valid backup.

For a local backup, the SQLite API keeps the source and destination
consistent:

```python
import sqlite3

source = sqlite3.connect("<state.db>")
destination = sqlite3.connect("<state-backup.db>")
try:
    source.backup(destination)
finally:
    destination.close()
    source.close()
```

If the deployment tooling cannot use `backup`, stop the scheduler, run
`PRAGMA wal_checkpoint(FULL)` on the open connection, close that connection,
and then copy `state.db` together with its matching code revision. Do not copy
or restore a partially written `*.db-wal`/`*.db-shm` pair.

Record the backup path, code revision, database file identity, and operator
approval. Never copy credential values into a config backup or report.

## 2. Validate and migrate in check-only mode

Run the V5 configuration checks without opening a writable state database:

```powershell
python scripts/run_pipeline.py preflight --config <v5-config.json>
python scripts/run_pipeline.py migrate --config <v5-config.json> --check-only
python scripts/run_pipeline.py dry-run --config <v5-config.json>
```

`preflight` must report all required secret references, the explicit
read-only source provider, the deployment-bound 115 root identity/path, and
both cloud roots contained by that common root. `migrate --check-only` must
report the schema version or `not_initialized`. `dry-run` must report
discovery results without creating `state.db`, `state.db-wal`, or
`state.db-shm` and without invoking a mutating client. Resolve any blocked
result before continuing.

`status`, `audit`, `reviews list`, and `migrate --check-only` read a private
temporary database image, including committed WAL, and use SQLite's backup API
to obtain a query-only in-memory snapshot. They do not attach SQLite to the
production files or create/update production WAL/SHM. Two consecutive images
must agree; a changing source is rejected after three attempts and can be
retried when writers are idle. Temporary copies are removed before returning.
This diagnostic mechanism is not a replacement for the backup procedure above.

At the first authorized write run, the common 115 root is read-only verified
from the account root through every component of `paths.cloud_source_root`.
The resulting source directory ID is the download target used in the submit
payload; the common root remains the base for path operations. A missing,
ambiguous, or incomplete directory listing is a pre-mutation failure and must
not be worked around by copying a path into an ID field.
The real P115 client must expose listing, mkdir, move, task submission/listing,
and an explicit atomic no-replace/conditional rename API. Ordinary `fs_rename`
does not satisfy the gate. Missing primitives block write mode before state
is opened or any remote mutation is attempted.

## 3. Prepare a side-by-side release

On a Linux NAS, the reviewed tree is installed below an immutable
`<install-root>/releases/<release-id>` directory. The installer changes
`<install-root>/current` atomically and preserves the formerly active release
at `<install-root>/previous`; configuration, state, logs, and backups remain
outside every versioned release. Use the stable command after activation:

```text
<install-root>/current/bin/anime-cloud-pipeline <command> [arguments]
```

Use environment references for credentials, `P115_ROOT_DIRECTORY_ID` (or
`P115_ROOT_ID`) for the 115 root identity, and
`services.cloud.root_directory_path` for the explicit common remote root; do
not put runtime IDs in JSON configuration. Configure the read-only `source`
provider before enabling discovery. The database is
`<config.paths.state_directory>/state.db` and the runner lock is
`<config.paths.state_directory>/run.lock`; the Hermes diagnostic path variables
do not override either value.

Run compilation and local tests from the new directory before connecting it to
the scheduler. Include the discovery-window boundary and local adapter/path
acceptance tests:

```powershell
python -m compileall -q scripts
python -m unittest discover -s scripts -p 'test*.py'
```

## 4. Perform bounded validation

Use a disposable or explicitly approved state directory for the first write
run. Start with `--resume-only` when validating already persisted pending
work. Inspect `status`, `audit`, and `reviews list` after each bounded run.
Confirm that each external stage has durable intent/evidence before permitting
the next bounded invocation. A missing adapter capability, path mapping, task,
library, or evidence must remain pending or become an explicit review; it must
not be repaired by an unbounded retry or an inferred runtime ID.

For the download-to-library acceptance check, verify that the selected P115
file row contains the common-root canonical source path, parent path, remote
identity, size, and fingerprint. The organizer must be invoked from the
StateStore episode row so this persisted evidence is the only source path
input. Discovery must likewise show a season-matched TMDB `air_date` in the
episode runtime evidence; a missing date is expected to reach runtime review
before any move. Backfill collections are accepted only when their complete
episode span is contained by the requested range.
For ambiguous inventory, resolve `MULTIPLE_VIDEO_FILES` and verify that the
selected normalized file row is projected transactionally into the episode's
source path, filename, extension, remote identity, size, and fingerprint.
Organizer must then continue from that persisted episode ID. Generic TMDB
metadata without a matching season number is not routing evidence, even if
only one metadata object was returned; an explicit season-keyed map is valid.

Do not enable cron until the code, configuration, credentials, mounts, and
database backup have been reviewed and the bounded validation evidence has
been accepted.

## 5. Enable the scheduler only after evidence

After authorized validation, use the Hermes runbook's atomic NAS cron-table
replacement so the single cron entry points to
`<install-root>/current/bin/anime-cloud-pipeline` and its matching external
config. Keep the invocation bounded and retain stdout/stderr in the
operator-approved log destination. Verify that a second concurrent invocation
observes the process lock and that read-only commands remain safe. Hermes is
not a second production scheduler.

## 6. Roll back code and database together

If the release must be rolled back, disable the new cron entry first, wait for
the active bounded run to finish, and atomically swap `current` and `previous`
with the installed rollback helper. Restore the database backup captured for
that code revision as a separate operation. Do not run old code against a
database created or migrated by a newer revision unless a reviewed migration
path explicitly permits it. Run `migrate --check-only`, `status`, and `audit`
against the restored pair before re-enabling the previous scheduler command.

If a run may have reached an external service before the rollback, reconcile
the persisted pending intent and remote inventory before allowing a retry. A
rollback is not permission to blindly resubmit downloads, repeat moves, or
refresh a library without evidence.

## 7. Checks still requiring authorized deployment access

Local fakes and filesystem/SQLite tests do not prove the following. Record each
as pending until an authorized operator runs and reviews it:

- MoviePilot authentication, subscription envelopes, scrape/sidecar behavior,
  and TMDB metadata availability;
- 115 credential validity, root identity/path chain, task/file pagination,
  no-replace move/rename behavior, and remote inventory latency;
- LitePan task identity, event acceptance, STRM generation latency, and
  playback URL evidence;
- filesystem mount roots, ownership/ACLs, no-follow symlink behavior, and
  sidecar visibility between LitePan, MoviePilot, and Emby;
- Emby library identity, exact episode/path matching, refresh latency, and
  API-key permissions;
- cron environment, lock behavior, log rotation, and scheduler recovery;
- a rollback drill that restores code and database as one unit; and
- Linux snapshot, ownership, mode, ACL, and database WAL/SHM permissions.

## 8. Independent V5 and native-agent compatibility

Version 5.4.5 has one bounded, authorized NAS download-to-Emby acceptance. That
evidence does not replace the instance-specific checks above or prove playback.
Existing scheduling ownership, models, credentials and unrelated media remain
outside a source update.

For independent host-Python deployments, keep the actual launcher and external
configuration. Never replace a newer production patch with the Skill's bundled
snapshot. The optional `scripts/v5_bootstrap.py` and `scripts/v5_plugin_secret.py`
helpers resolve environment, read-only MoviePilot configuration or private file
references in memory; they do not install or run a deployment. Current
P115Disk plugin instances are authoritative: missing, disabled, ambiguous or
invalid current instances cannot fall back to stale legacy cookies. A cookie
being present is not proof of a valid login.

Before metadata processing, verify the exact published STRM is visible through
MoviePilot's real container path and has the same digest as the host file.
Preserve separate raw/published roots. Missing mounts require a separately
authorized service configuration change; do not create an unrelated directory
inside the container merely to satisfy path checks. Preserve existing writable
application changes, mounted configuration, service settings and rollback
options when a container must be recreated.

Resource preparation uses a separate one-shot authorization and fresh config
revision; it does not migrate state or enable media actions. See the
[quarter resource contract](../references/quarter-resources.md). The current
resource adapters derive quarters in Asia/Shanghai; do not silently use them
with a different deployment timezone.
