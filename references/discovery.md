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

Validated limits bound network reads, task pages, recursive file depth/count,
new submissions per run, and new submissions per subscription. `ConfigSchema`
parses and preserves `max_review_batch` and `max_reconcile_batch`, but the
current discovery execution does not wire those fields into batch loops; this
reference makes no runtime batching claim. The discovery plan reports
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
