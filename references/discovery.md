# Subscription discovery

## Source and preparation

`MoviePilotClient.list_subscriptions()` reads `/api/v1/subscribe/` and accepts
either a list or a constrained `data`-list envelope. Failure flags, malformed
entries, and unknown response shapes raise an adapter error; an empty fallback
must not erase the source snapshot.

`DiscoveryService.prepare(window, read_only=...)` creates a `DiscoveryPlan`.
It normalizes each subscription, asks each read-only `SourceProvider` for
results, applies title/season/episode identity checks, and returns decisions,
exclusions, and scoped errors. A provider failure is isolated to its
subscription. Secret-bearing values are sanitized before projections or
persistence; distinct secret identities remain distinct internally.

The optional `KissSubFetcher` uses a cookie jar and a browser-equivalent
visitor-test request when the source presents that page, then requires an RSS
document from `fetch_feed`. `feed_for_keyword` constructs the encoded keyword
feed path. `parse_feed` rejects HTML, login, malformed XML, and empty/non-RSS
responses instead of treating them as an empty source; it extracts title,
link, torrent URL, infohash, publish time, season/episode, group, resolution,
and range evidence.

`dry-run` and `run` use the same config loading, subscription retrieval,
normalization, provider calls, and candidate decisions. Read-only mode prevents
state writes and external writes through the port boundary, rather than by
assuming a caller will avoid them.

## Windows and limits

The optional `--previous-day` window derives one date from the configured IANA
timezone. The optional `--backfill` value must match
`MEDIA_ID:SEASON:START-END` and must have `START <= END`; it is a bounded input,
not a request to expand an entire season.

## Automatic catch-up after subscribing

Ordinary `run` and `dry-run` now read the verified subscription season's episode
calendar through MoviePilot's unpaginated TMDB season endpoint. Only episodes
with a known `air_date` on or before today in `schedule.timezone` enter automatic
catch-up. Missing dates exclude those episodes; an unavailable, empty, malformed,
or conflicting calendar stops discovery for that subscription and stops the
automatic runner before it can submit previously queued episodes. The username,
anime genre, season-premiere quarter, and identity gates run first.

The normal keyword RSS is still used. For aired episodes it does not cover,
KissSub is additionally searched using the work title and zero-padded episode
number. Every result must pass the existing work, media identity, season, exact
episode, exclusion, and guard checks. A broad search or a pack that extends
beyond that exact historical episode cannot widen the request. A missing source
is reported as unavailable and retried on a later scan; it never proves that a
library episode is missing.

Historical queries share one `limits.max_reconcile_batch` budget per invocation.
The existing subscription snapshot stores a per-provider last-searched episode
and a scan order; subsequent runs continue after that episode and prioritize
subscriptions searched least recently. Read-only previews use the same saved
cursor without advancing it. A release that cannot be found does not block
searches for later episodes, regardless of scheduler frequency. Already tracked
identities in the enabled guard resume through the existing queue. Run limits,
task reconciliation, submission intent, and complete pre-submit inventories still
control all actual downloads. Complete inventories proving presence skip that
episode without blocking later catch-up work; unknown/error inventories stop
the run. Presence alone never marks an episode as `library_ready`.
`backfill` report counts describe source discovery, not confirmed missing files
or completed downloads. `dry-run --details` includes safe error/exclusion reasons.

Explicit `--previous-day`, explicit `--backfill`, and `--resume-only` retain their
existing scopes. They do not enable automatic season expansion. Ordinary runs
continue catch-up on later invocations, so a late release can be found without
recreating the subscription. This does not install a scheduler or make a passive
Skill receive subscription events; the host Harness must invoke the ordinary
pipeline run through its existing authorized trigger.

Validated limits bound network reads, task pages, recursive file depth/count,
new submissions per run, and new submissions per subscription. `ConfigSchema`
parses and preserves `max_review_batch`; it is not a discovery batch loop.
`max_reconcile_batch` bounds historical source searches as described above.
The discovery plan reports
subscription, decision, error, and exclusion counts.

## Data and safety

Candidates are persisted only after identity, content type, episode range, and
safe trait validation. Discovery never submits a cloud task. Local subscription
catalogues and runtime-generated IDs are rejected by `ConfigSchema`; runtime
objects are looked up later by configured names and roots.

Executable checks include `test_dry_run_and_run_share_subscription_and_candidate_decisions`,
provider failure isolation, source identity conflicts, non-positive/reversed
episode ranges, and the no-submission read-only test in
`scripts/tests/test_discovery.py`.
