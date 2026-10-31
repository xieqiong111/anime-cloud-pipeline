# State machine and transactions

## Statuses

`EpisodeStatus` defines these persisted values:

`discovered`, `download_pending`, `submitted`, `cloud_ready`,
`organize_pending`, `organized`, `strm_pending`, `strm_ready`,
`scrape_pending`, `metadata_ready`, `library_pending`, `library_ready`,
`needs_review`, and `notified`.

The normal progression is:

```text
discovered -> download_pending -> submitted -> cloud_ready
  -> organize_pending -> organized -> strm_pending -> strm_ready
  -> scrape_pending -> metadata_ready -> library_pending
  -> library_ready -> notified
```

Stable rows create the next pending intent. `PipelineRunner` snapshots rows,
handles each row once, and invokes at most one external stage per turn. Its
pending handlers are download reconciliation, organization reconciliation,
STRM verification/trigger, sidecar verification/scrape, and Emby query/refresh.

`needs_review` is a side state with `resume_status`, `review_reason`, sanitized
detail, timestamps, resolution, and a monotonic generation. `failed_permanent`
is reserved for an unrecoverable invariant. Exhausted retry attempts enter
`needs_review` with reason `UNEXPECTED_REMOTE_STATE`, retaining the pending
stage as `resume_status`. Explicit review resolution resets retry metadata
and resumes that stage.

## Transaction rules

Every state transition and its `stage_history` row are one transaction. External
write intent is recorded before the call; success stores evidence and clears
retry metadata. A retry records a sanitized error and leaves the episode in a
pending state. `transition_library_ready_with_notification` atomically updates
the ready state and deduplicated outbox row; delivery is later and cannot roll
back media state.

The runner uses a process lock for writable turns. Read-only turns do not create
or mutate that lock. Shared infohash polling re-reads rows and uses compare-and-
set behavior so one remote result cannot advance an unrelated or already changed
episode.

## Review reasons

The public enum values are `MULTIPLE_VIDEO_FILES`, `AMBIGUOUS_EPISODE`,
`NO_VALID_VIDEO`, `PATH_CONFLICT`, `METADATA_CONFLICT`, `DUPLICATE_EPISODE`,
`UNEXPECTED_REMOTE_STATE`, `LEGACY_IDENTITY_UNRESOLVED`, and
`RUNTIME_MAPPING_AMBIGUOUS`. Reasons are evidence categories, not permission
to guess or overwrite a remote target.
