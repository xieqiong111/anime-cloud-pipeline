# Emby library verification

## Library and inventory

`EmbyClient.find_library(name, root)` requires one unique library whose name and
normalized location match the configured values. Conflicting location aliases,
malformed entries, and duplicate matches fail closed.

`iter_episodes(library_id)` requests `Episode` items with `StartIndex`, `Limit`,
and `TotalRecordCount`. It validates IDs, paths, provider IDs, series identity,
season/episode numbers, stable totals, page ranges, duplicate IDs, and the page
cap. A short page before the declared total is incomplete, not success.

`find_episode(identity, expected_path)` filters season and episode and requires
the normalized current path. Ranking is provider media identity first, known
series identity second, aliases third, and canonical name last. A contradictory
provider identity cannot be rescued by a name; equal best matches are
ambiguous.

## Refresh and ready state

`refresh_library` reports an accepted refresh request. `LibraryService` queries
the exact episode/path before refreshing and queries again on a later bounded
turn. Only an exact current path can advance to `library_ready`; an old or
parallel path is evidence of absence for this contract. The ready transition
and deduplicated notification enqueue are atomic, and ready rows are not
refreshed again.

No live library, mount, refresh latency, provider identity, or endpoint
compatibility is established by this repository. Use `test_library_service.py`
and the adapter tests for local evidence only.
