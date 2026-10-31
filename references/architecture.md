# V5 architecture

## Boundaries

`scripts/run_pipeline.py` is the V5 state-machine entry point. It validates a
`ConfigSchema` before opening a client, writable store, or external operation.
Compatibility helpers do not define a second workflow.

MoviePilot is the subscription source. `DiscoveryService` reads and normalizes
that snapshot; it may persist an audit snapshot, but it never treats local
configuration as a second subscription catalogue. `StateStore` owns durable
episodes, candidates, downloads, file inventory, source mappings, history,
reviews, and notification outbox rows.

The stable episode key is `(media_source, media_id, season, episode)`. An
infohash or remote task identifier is evidence, not episode identity.

## Configuration

`ConfigSchema.from_mapping` requires schema version 5, four service sections,
five distinct path roots, scoring weights, bounded limits, retry policy, runtime
names, and a schedule timezone. Secrets are references (`token_env`,
`api_key_env`, `credential_env`, or `secret_env`), not values. `migrate_legacy_config`
removes local subscription catalogues and runtime IDs and reports identities to
verify in MoviePilot; incomplete mappings remain blocked.

`PathMapper` is the only cross-service path translator. It provides
`cloud_to_litepan`, `litepan_to_moviepilot`, `litepan_to_emby`,
`canonical_paths`, and containment validation. Season routing accepts only the
explicit strict TMDB season `air_date` form `YYYY-MM-DD` and returns a neutral
`YYYY-MM月新番` label; missing or invalid dates block writes.

## Services and ports

The runner connects these tested boundaries:

| Concern | Implementation | Completion evidence |
|---|---|---|
| subscription/discovery | `MoviePilotClient`, `DiscoveryService` | normalized decisions |
| download/inventory | `P115CloudAdapter`, `DownloadService` | complete remote evidence |
| organization | organizer service + `PathMapper` | identity-safe target |
| STRM | `LitePanClient`, `StrmService` | exact HTTP(S) STRM |
| metadata | `MoviePilotClient`, `MetadataService` | complete matching sidecars |
| library | `EmbyClient`, `LibraryService` | exact identity and current path |

The JSON-over-HTTP service clients for MoviePilot, LitePan, and Emby use
`JsonHttpClient`/`RetryPolicy` for bounded retries and sanitized errors.
`KissSubFetcher` is a dedicated RSS/visitor client, while `P115CloudAdapter`
uses its dedicated cloud client and port. Each adapter validates its own
response shape before business logic; see the focused adapter references.

## Local evidence

The implementation and tests use real SQLite/filesystem behavior and fakes for
external services. See `scripts/tests/test_config.py`, `test_discovery.py`,
`test_p115.py`, `test_litepan_service.py`, `test_metadata_service.py`, and
`test_library_service.py` for the executable contract. These tests do not prove
compatibility with an untested installed service version or deployment.
