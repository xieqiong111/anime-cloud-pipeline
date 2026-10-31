# Anime Cloud Pipeline V5

This changelog separates reusable source changes, offline checks, and bounded
deployment evidence. A successful deployment does not certify another instance.

## 5.4.5 — independent V5 runtime and quarter preparation

- Add subscription qualification, read-only resource planning, and explicitly
  authorized current-quarter preparation with config revision checks, shared
  locking, read-back evidence and recovery from uncertain creation results.
  Plan revisions bind both the exact config bytes and the preparation quarter;
  an unchanged config cannot make a previous quarter's token valid again.
- Keep resource preparation separate from media execution. Preserve the exact
  admin/Bangumi source policy, genre16 and matching-season premiere gate.
- Add bounded request/phase diagnostics without credential or playback URL
  output, and distinguish missing directories from unknown inventories.
- Submit canonical magnets through the SSP URL endpoint; validate positive
  receipts and matching identities, persist attempt/intent before submission,
  and stop automatic retries after unknown or rejected results.
- Enforce requested episode scope for resumed state and shared download files,
  and reconcile cloud, raw/published STRM and Emby before new submissions.
- Read MoviePilot P115Disk credentials from the authoritative plugininstance
  schema, with read-only legacy compatibility. Portable bootstrap helpers live
  in scripts; deployment-specific launchers and credentials remain external.
- Preserve organized-subdirectory geometry, exact Emby series/episode evidence,
  no-action resource creation, and process-lock ownership checks.
- Update the Hermes command bridge so new resource commands use the existing
  external config and resource approval is inherited only for one apply call.

One authorized single-episode NAS workflow completed real download, cloud
organization, STRM publication, six sidecars and exact Emby ingestion. No
playback test or all-subscription completion is claimed. Local fixtures and
reusable code are published; private NAS evidence and operation scripts are not.

Publication validation on macOS ran 807 offline tests: 793 passed, with the
same 14 failures/errors reproduced on the unmodified upstream baseline.
Those existing legacy acceptance and Linux atomic-publication checks are not
claimed green or removed to make this update pass. The affected Hermes runtime
suite passed 54 tests; the Skill checks also passed. Target Linux deployment
acceptance remains separate from these local results.

## Final integration fixes

- Ambiguous-file review resolution now atomically projects the selected
  normalized inventory identity and canonical source fields for Organizer.
- Read-only CLI diagnostics snapshot DB/WAL/SHM privately and back up into
  memory, preserving the production directory's file set and bytes.
- Write-mode validation checks the real P115 client's required primitives,
  including explicit atomic no-replace rename, before opening writable state.
- Single generic TMDB metadata records require matching season evidence;
  unnumbered dates cannot route the organizer.
- Recovery references align retry exhaustion with manual review and link the
  consistent SQLite backup/checkpoint procedure.

## P0 — safety boundaries

- Configuration is validated before the write runtime is assembled.
- `dry-run`, status, review listing, audit, and migration check-only paths stay
  read-only; dry-run does not construct 115, LitePan, or Emby clients and does
  not create SQLite, WAL, or SHM files.
- Write mode requires the deployment-bound `P115_ROOT_DIRECTORY_ID` (or
  `P115_ROOT_ID`) reference plus an explicit common remote root path. It fails
  closed before constructing the 115 cloud adapter when either is absent, and
  verifies both configured cloud roots are contained by that root.
- The 115 adapter keeps the verified common root identity/path separate from
  the download target. Before the writable graph or SQLite store is opened,
  it resolves the complete read-only directory chain for
  `paths.cloud_source_root`; missing, ambiguous, or incomplete evidence blocks
  the run. Path operations remain rooted at the common root and submission
  payloads use the resolved source directory ID.
- Discovery requires an explicit read-only source provider configuration;
  missing source configuration or adapter capability is a pre-mutation block.
- Secrets and adapter errors continue to use the existing redaction boundary.

## P1 — V5 runtime integration

- The CLI now builds a concrete `RuntimeBundle.services` graph for candidate
  selection, download reconciliation, organization, STRM evidence, metadata,
  and library evidence.
- `DiscoveryService` and `PipelineRunner` use the same validated config,
  `PathMapper`, secrets, and state boundary.
- The runtime bundle carries the concrete KissSub read-only provider into
  `DiscoveryService`; LitePan receives the organized library root as its task
  source root.
- KissSub results are gated by the shared discovery window: previous-day
  filtering uses the configured timezone and published date, while backfill
  filtering requires the subscription media identity, season, and inclusive
  episode range. Filtered results never become candidates or state rows.
- KissSub HTTP reads use the configured timeout and the shared `JsonHttpClient`
  retry policy while retaining the cookie jar; no fixed request timeout or
  unbounded retry path is used.
- Runtime validation checks every handler and its underlying adapter ports
  before the writable state is opened; the preflight path uses static
  descriptors and constructs no mutating client.
- The library handler is Task 9 `LibraryService`; the legacy refresh-first
  `Executor` is not part of the V5 CLI runtime.
- The PowerShell/Python compatibility wrapper delegates to the same V5
  `run_pipeline.main` entrypoint.

### Task 13 review follow-up — local evidence chain

- P115 recursive inventory now emits a verified common-root `path`,
  `parent_path`, basename, parent ID, size, and fingerprint for every file.
- `DownloadService` projects selected inventory evidence into the episode
  runtime identity; `Organizer` can consume a plain StateStore row without a
  hand-added source path or file identity.
- Discovery accepts only explicit, season-matched TMDB metadata for
  `season_air_date`, persists that evidence on the subscription snapshot and
  episode runtime identity, and leaves missing dates on the review path before
  any cloud mutation.
- Backfill collection candidates must be fully contained by the requested
  episode range. Overlapping collections are rejected before candidate or
  episode persistence.

## P2 — bounded retry and recovery

- Stage services, runner-created services, HTTP transport, and library leases
  consume one `RetryPolicy` type and shared bounded delay helper. Configured
  backoff, cap, jitter, and attempt limits are preserved through runner
  auto-wiring; jitter is clamped by the final cap.
- Retry randomness is injectable for deterministic tests.
- Exhausted configured attempts open an `UNEXPECTED_REMOTE_STATE` review with
  the pending stage retained as `resume_status`; operator resolution can resume
  the stage without losing retry evidence.
- Emby refresh leases use the same configured backoff and jitter semantics.

## P3 — verification and operations

- New local integration tests cover the complete service graph, read-only
  source discovery, fail-closed cloud/root/capability binding, deterministic
  retry timestamps, review escalation across download/STRM/scrape/library,
  stale-handler CAS protection, dry-run client isolation, one bounded
  pending-stage dispatch, and wrapper parity.
- Existing CLI, runner, service, state, and security tests remain part of the
  full `scripts` suite.

## P4 — delivery documentation

- `docs/V5-UPGRADE.md` describes backup, check-only migration, side-by-side
  rollout, bounded validation, cron enablement, and code-plus-database
  rollback.
- Live MoviePilot, 115, TMDB, LitePan, Emby, mount, ACL, symlink, scheduler,
  rollback-drill, and Linux snapshot checks remain explicitly pending
  authorized deployment verification.

## P5 — Hermes NAS delivery contract

- The deployable package exposes one stable
  `<install-root>/current/bin/anime-cloud-pipeline` operator entrypoint and
  keeps immutable `releases/<release-id>` trees behind atomic `current` and
  `previous` links.
- `doctor`, `preflight`, migration check-only, and `dry-run` remain the
  read-only gate; activation does not migrate SQLite or modify the scheduler.
- NAS cron remains the sole production scheduler. Hermes supplies operator
  commands and does not create a competing recurring job.
- The packaged `deploy/hermes_cron.py` helper transforms an already captured
  crontab with exact marker/command-field matching, byte-preserved unrelated
  entries, exact-one-entry checks, and fail-closed ambiguity handling.
- The configured `config.paths.state_directory` owns `state.db`, with the
  runner lock at `<state_directory>/run.lock`. Hermes environment state/lock
  variables are deployment diagnostics and do not silently redirect the
  pipeline.
- The new Hermes deployment reference documents external config and secret
  boundaries, manifest verification, bounded live validation, status/review
  operations, and code-plus-database rollback. Local/POSIX evidence is not a
  claim of real-NAS or live-service validation.
