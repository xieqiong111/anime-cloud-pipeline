---
name: anime-cloud-pipeline
description: Use for V5 pipeline operations, previews, authorized current-quarter resource preparation, recovery, review, and troubleshooting from a checkout or authorized deployment.
metadata:
  version: 5.4.5
  tags: [anime, moviepilot, p115, litepan, emby, strm, sqlite]
  category: media
---

# Anime Cloud Pipeline V5

## Operator boundary

This skill describes the V5 operator contract. MoviePilot subscriptions are the
discovery source; the V5 SQLite database is durable local state. Preserve the
deployment's verified single scheduler owner: this may be Hermes with NAS cron
or an existing native agent task. Do not replace its scheduler, schedule, or
model, add a second owner, or introduce an in-process sleep loop.

Use the deployment's verified V5 operator entrypoint. Hermes installations use
`<install-root>/current/bin/anime-cloud-pipeline`; a native agent keeps its
existing invocation and task. Use the local Python entrypoint only from a
checkout. Do not print or commit credentials, playback URLs, live addresses,
runtime IDs, current-season labels, or incident-specific observations; use
external environment/config references.

## Preflight and authority

Before the first media write, run the verified deployment's supported read-only
gates. Hermes provides `doctor`, `preflight`, `migrate --check-only`, and
`dry-run`; an independent V5 must at least pass `preflight` and `dry-run`, plus
its supported schema check. Do not invent a `doctor` command for a plain CLI or
switch away from the verified NAS interpreter. Use quarter preparation below
when missing resources prevent the final gates from passing. `doctor`, `preflight`,
`migrate --check-only`, `dry-run`, `status`, `reviews list`, and `audit` are
read-only. `run`, database
`migrate`, and `reviews resolve` can write local state or call external services;
activation, rollback, and cron changes also require explicit operator approval.

The database is `<config.paths.state_directory>/state.db`; the runner lock is
`<config.paths.state_directory>/run.lock`. `ANIME_PIPELINE_STATE_DIR` and
`ANIME_PIPELINE_LOCK_PATH` are Hermes deployment diagnostics only. They do not
silently redirect the pipeline database or runner lock; keep them aligned with
the config when using `doctor`.

An explicitly recorded standing authorization for the fixed quarter-resource
template covers later rollovers within that template; do not ask again solely
because the quarter changed. A one-quarter approval covers that quarter only. Skill invocation and read-only inspection do
not grant initial resource-write authority. Resource preparation does not
authorize episode downloads, organization, STRM generation, scraping, refresh,
database migration, activation, or scheduler changes. Media writes require
separate authorization and their own bounded media scope.

## Current-quarter preparation

For an authorized fixed-template setup or rollover, follow
[quarter resource preparation](references/quarter-resources.md): run
`qualify`, then `plan-resources`, apply only with the authorization gate and
the fresh 64-character config revision from that plan, then run `preflight`
and `dry-run`. Qualification verifies subscription eligibility; planning also
reads the current resource inventory and proposes `reuse` or `create` actions.
Both are read-only, and apply must recheck qualification, the config snapshot,
and resource inventory. `preflight` and `dry-run` remain read-only and must
never create resources. Stop on incomplete inventories, unknown identity,
conflicting resources, an unsupported creation interface, or a
revision/quarter mismatch.

## Main flow and safety

The normal `dry-run`, `run`, and `run --resume-only` scope is the current
calendar quarter in `schedule.timezone`. Only subscriptions with a verified
TMDB season air date in that quarter are eligible. Older or undated pending
episodes remain in SQLite but are not advanced; a quarter change never
authorizes replaying their remote operations. The cloud destination, LitePan
task, webhook rule, and Emby library must all be scoped to that same quarter.
Keep the verified `admin` / `Bangumi订阅` source allowlist, genre 16 filter,
corresponding-season premiere-date gate, complete identity checks, and
deduplication intact. Never widen media scope to make qualification pass.
Before a writable run, reconcile quarter resources using the referenced
procedure; never select a task that scans the whole cloud library merely
because it already exists.

Each `run` invocation handles one snapshot and at most one external stage per
episode. Pending work is resumed by a later invocation; an accepted request is
not completion. The stable flow is
`discovered → download_pending → submitted → cloud_ready → organize_pending →
organized → strm_pending → strm_ready → scrape_pending → metadata_ready →
library_pending → library_ready → notified`.

`needs_review` stores a resume status and reason. Retryable failures retain a
recoverable pending state; uncertain remote inventory never proves absence.
Reviews require reason-specific JSON and transactional compare-and-set
resolution. Claim an episode ready only after exact path and identity evidence
for every stage; HTTP success, task submission, refresh acceptance, or a lone
STRM file is insufficient.

For new 115 selections, Organizer creates an isolated directory under the
target `Season N`, records its remote directory ID, verifies that it is empty,
and moves the selected file by remote file ID. It keeps the original basename
and never invokes an undocumented rename primitive. Recovery rechecks the
directory ID and selected file identity before continuing. LitePan scans the
nested cloud path recursively; the pipeline publishes its STRM at the standard
`Season N` path for MoviePilot and Emby. The LitePan raw task output and the
published STRM root must be separate directories.

## Commands

Use these subcommands through the already verified entrypoint. The Hermes
wrapper supplies the external config path; direct CLI use requires `--config`.
Do not replace or edit a native agent's established task to add these commands.

```text
<pipeline> version
<pipeline> doctor
<pipeline> qualify
<pipeline> plan-resources
OCTOP_PIPELINE_ALLOW_RESOURCES=1 OCTOP_PIPELINE_RESOURCE_REVISION=<fresh-64-character-config-revision> <pipeline> prepare-resources --apply
<pipeline> preflight
<pipeline> migrate --check-only
<pipeline> dry-run [--previous-day|--backfill <MEDIA_ID:SEASON:START-END>]
<pipeline> run [--previous-day|--backfill <MEDIA_ID:SEASON:START-END>] [--resume-only]
<pipeline> status
<pipeline> reviews list
<pipeline> reviews resolve --id <review-id> --resolution '<JSON-object>' [--generation <generation>]
<pipeline> audit
```

`<pipeline>` is the verified command entrypoint for the instance. For Hermes,
use the installed wrapper; direct checkout commands require `--config`. Pass
the exact fresh `config_revision` reported by
`plan-resources`; regenerate the plan if the config or quarter changes. Set
the resource authorization gate only for an already-authorized resource
workflow. Run `preflight` and `dry-run` after preparation. Continue to media
operations only when separately authorized for the exact requested media
scope.

`<pipeline> config-migrate --legacy <legacy.json> --output <v5.json> --report
<report.json>` is a separate
local/config conversion command. Details for immutable releases,
installation, bounded NAS validation, cron ownership, and rollback are in
[Hermes NAS deployment](references/deployment-hermes.md). Source-checkout
boundaries are in [local deployment](references/deployment-local.md).

## Evidence boundary

Local tests, fakes, filesystem/SQLite checks, and `doctor` do not certify a NAS,
mount, credential, endpoint, scheduler, service version, or media library.
Record local and authorized live evidence separately and stop on incomplete
remote evidence or an unresolved review.

## Reference index

- [architecture](references/architecture.md) · [state machine](references/state-machine.md)
- [discovery](references/discovery.md) · [candidate selection](references/candidate-selection.md)
- [MoviePilot](references/moviepilot.md) · [P115](references/p115.md)
- [LitePan](references/litepan.md) · [Emby](references/emby.md)
- [recovery](references/recovery.md) · [cleanup](references/cleanup.md)
- [Hermes NAS deployment](references/deployment-hermes.md) · [local deployment](references/deployment-local.md)
- [quarter resource provisioning](references/quarter-resources.md)
