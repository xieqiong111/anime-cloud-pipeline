# Local delivery and deployment boundary

For a Linux NAS, use the [Hermes NAS deployment runbook](deployment-hermes.md)
for extraction, installation, activation, NAS cron ownership, and rollback.
This document describes source-checkout and local-test boundaries only; it does
not install or enable a production scheduler.

## Configurable setup

Start from `assets/config.example.json`. Replace example service URLs, roots,
runtime names, schedule, limits, and environment/file references for the
authorized environment. Keep the five roots distinct and keep credentials out
of JSON. `config.paths.state_directory` is the source of truth for `state.db`;
the runner lock is its sibling `<state_directory>/run.lock`. Validate with
`preflight`; inspect the config and database with `migrate --check-only`, then
apply migration only to the intended local state directory.

The Hermes variables `ANIME_PIPELINE_STATE_DIR` and
`ANIME_PIPELINE_LOCK_PATH` are deployment diagnostics for `doctor`. They do
not silently redirect the local pipeline's database or runner lock.

The repository contains a bounded runner, not an installed scheduler. An
external scheduler may invoke `run` for discovery and `run --resume-only` for
reconciliation, but each invocation must finish without an in-process sleep
loop. Do not claim a scheduler, container, mount, endpoint, task, or library
is installed from local tests.

## Safe rollout sequence

1. Stop the scheduler, wait for writers, and back up code/config and SQLite using
   the [backup API or checkpoint procedure](../docs/V5-UPGRADE.md#1-back-up-the-release-unit).
   Do not independently copy live WAL/SHM companions.
2. Validate configuration and migrations on copies; keep the legacy input separate.
3. Run `preflight`, `migrate --check-only`, `status`, and `dry-run`.
4. Run one bounded authorized invocation and inspect status, reviews, audit, and evidence.
5. Enable an external schedule only after service-specific live checks pass.
6. On failure, stop new writes and restore the code plus complete database backup together.

## Checks that remain external

This local delivery has not verified a live MoviePilot response or subscription,
cloud-client signature or remote pagination, path/mount or ACL behavior,
LitePan task discovery/event/STRM output, MoviePilot sidecar timing, Emby
library discovery/refresh/path visibility, cron/lock permissions, or an
end-to-end `library_ready` episode. Any such check requires explicit access and
must be reported separately from local test evidence.

## Evidence discipline

Record command output as local or live evidence, never merge the two. A return
code, HTTP acceptance, remote task ID, refresh response, or file existence alone
does not prove the next stage. Use the focused references and source tests to
choose the exact evidence required for each stage.
