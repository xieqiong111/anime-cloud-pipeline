---
name: anime-cloud-pipeline
description: Use for V5 pipeline operations, previews, authorized current-quarter resource preparation, recovery, review, and troubleshooting from a checkout or authorized deployment.
metadata:
  version: 5.4.9
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

## Automatic catch-up after subscribing

On a normal authorized pipeline run, discover earlier aired episodes for each
eligible current-quarter subscription as well as new releases. The shared
`dry-run` previews the same discovery. Use the verified pipeline entrypoint;
ordinary `run` enables automatic catch-up without a manually supplied range.
Explicit `--previous-day`, `--backfill`, and `--resume-only` keep their bounded
behavior. See [automatic catch-up](references/discovery.md) for calendar,
source-search limits, unknown-date handling, and diagnostic counts.

In DeepSeek Harness, use that installation's verified Skill directory and
pipeline command. Invoke an independent V5 launcher with `V5_ANIME_GUARD=1`
unless its verified config already enables the guard; do not rely on an
Octop wrapper to supply this environment. A loaded Skill does not itself receive MoviePilot events;
preserve the existing authorized trigger and single scheduler owner. Do not
substitute an Octop workspace or create a second polling task. If no automatic
trigger is configured, report that boundary before claiming subscription-time
automation. Never equate a missing RSS result or a local queue row with a
missing library episode; actual submission still requires complete inventories.

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

Sidecar acceptance covers episode NFO, episode image, show NFO/poster and
season NFO/poster. A missing episode still (`<stem>.jpg`) is tolerated only
when the episode NFO and every other required sidecar are already verified; the
result records reason `complete_without_episode_image` and
`optional_missing: ["episode_image"]`. Scrapers can emit no still for a
just-aired episode, and an image-only gap must not hold the library edge.

Stills are backfilled one day after airing. At the end of every `run`, missing `<stem>.jpg` files of `library_ready` episodes are fetched from TMDB using the deployment secret `config/bootstrap-secrets/tmdb.json` (`api_key`, optional `proxy`). Only episodes whose `air_date` is earlier than the current Asia/Shanghai date are touched; existing images are never overwritten, at most 20 episodes per run, and any failure leaves the run result unchanged.

`needs_review` stores a resume status and reason. Retryable failures retain a
recoverable pending state; uncertain remote inventory never proves absence.
Reviews require reason-specific JSON and transactional compare-and-set
resolution. Claim an episode ready only after exact path and identity evidence
for every stage; HTTP success, task submission, refresh acceptance, or a lone
STRM file is insufficient.

For new 115 selections, Organizer uses `fid-*` only as temporary isolated
staging for a same-file basename rename. Its final cloud layout is
`Work/Season N/Work - S01E01.ext`: videos are directly inside Season, with no
final per-file directory. After complete Season ownership and file-ID, size,
fingerprint, parent-ID and basename checks, the serial writer moves the video
by ID into Season and verifies the entire before/after inventory. It then
deletes the verified empty fid directory in place (`fs_delete`) instead of
moving it into the quarter's `.pipeline-staging`: the intent keeps
`staging_archive_path` for audit and records
`staging_archive_state: "deleted"`, and the delete is proven by re-listing the
Season and failing to list the removed directory. 115 retains the deleted
directory in its recycle bin, so recovery by ID is still possible.

`cloud-layout --config ...` previews qualified existing files; `--id N --apply`
backs up state and persists a bounded intent before using this same Organizer.
The original selected download record and old name witness remain immutable.
Only a sealed file move plus a closed empty-directory deletion (archive state
`deleted`, verified) or a closed empty-directory archive, with both closed
history edges, may advance to STRM. LitePan raw output, published STRM, metadata and exact
Emby identity/path must be revalidated through the normal runner. The daily
scheduler remains the sole owner; this command neither creates a schedule nor
resolves reviews. The LitePan raw and published roots remain separate.

The installed move endpoint has no proven server-side no-replace or atomic
contract. Keep the Season namespace dedicated to the pipeline, use the existing
run lock and a single writer, reject unknown entries or name conflicts, and
reconcile uncertain outcomes by ID after the normal cooldown. Do not repeat a
write merely because its acknowledgement was lost.

### Emby indexing lag at the library stage

A STRM written moments ago is not immediately matchable. Emby can list the
episode while its `ProviderIds` are still empty until the library scan
finishes, so exact identity matching fails on freshly written files even though the
cloud path, STRM, raw content and metadata are all correct. Observed
2026-11-14 on TMDB 283938 S01E06: `last_reason=episode_provider_ids_empty`
after three retries, `retry_exhausted`, review opened at generation 1, and the
whole 283938 subscription became blocked while 311842 was unaffected.

Treat this symptom as timing, not as an unknown remote state:

- Before spending retries, confirm the episode is visible in Emby with the
  expected TMDB provider ID and canonical path; if it is not, wait for the next
  snapshot or trigger the library scan instead of re-running the stage.
- A review opened this way still blocks the matched subscription by design.
  Do not resolve it inside a daily run and do not widen scope around it.

Recovery needs an explicit, per-review operator authorization:

1. Read-only verify the Emby item: provider ID and `Season N/Work - S01E0N.ext`
   path both match the intended identity.
2. `reviews resolve --config <cfg> --id <review_id> --generation <g>
   --resolution '{"action":"retry","note":"<reason>"}'`.
3. `run --resume-only --backfill <media_source>:<season>:<ep>-<ep>`.
4. Re-check the exact Emby identity/path and that the state reached
   `library_ready`; record the authorization and evidence.

Never resolve reviews outside the declared quarter, and never reuse one
authorization for another episode.

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
