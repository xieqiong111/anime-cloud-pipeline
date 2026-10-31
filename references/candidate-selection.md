# Candidate and video selection

## Candidate scoring

`CandidateSelector` applies hard exclusions before scoring. Validated weights
cover include/exclude expressions, language, release group, resolution, and
collection penalties. Explicit non-episode markers (OP, ED, PV, SP, trailers,
menus, and equivalent markers) are excluded before a candidate can be selected.

Among eligible candidates, a unique highest semantic score wins for the
episode. An exact semantic score tie, duplicate episode identity,
contradictory source signal, or missing mapping stays visible as a
review/selection decision; the selector does not use secondary metadata or list
order to break a tie.

`SubmissionBudget` enforces both the global and per-subscription limits. A
reconciled existing remote task does not consume a new-submission reservation.
Concurrent reservations are bounded by the budget lock.

## Remote video inventory

`VideoSelector` materializes a complete inventory before selection or
persistence. It accepts configured video extensions, rejects directories and
feature markers such as `NCOP`, `NCED`, `PV`, `SP`, `CM`, `Trailer`, `Preview`,
`Bonus`, and `Menu`, and honors optional duration/size thresholds.

Explicit `SxxExx` pairs outrank standalone episode markers. A season-only token
is not an episode. Multi-episode files may map to several target episodes;
cross-season contradictions are rejected. A unique best evidence tuple wins;
equal winners produce `MULTIPLE_VIDEO_FILES`, unknown episode identity produces
`AMBIGUOUS_EPISODE`, and no eligible file produces `NO_VALID_VIDEO`.

`VideoSelectionResult.persist` records the complete inventory first, then
episode-source mappings. Incomplete inventory raises `RemoteInventoryIncomplete`
before any selection or state write. Rejected files and review candidates are
retained as evidence; only a unique winner receives the primary flag.
