# Recovery, reviews, and rollback semantics

## Pending recovery

Before every external attempt, the service records a pending intent, attempt
metadata, and history. On restart the corresponding handler reconciles durable
evidence first:

| Status | First recovery check |
|---|---|
| `download_pending` | remote task by normalized infohash |
| `submitted` | complete file inventory and video mapping |
| `organize_pending` | source/target remote identities |
| `strm_pending` | exact STRM and runtime task evidence |
| `scrape_pending` | complete matching sidecars |
| `library_pending` | exact Emby episode/path |

Unknown or incomplete remote state stays pending with a sanitized error and
bounded `next_retry_at`; it is never interpreted as absence. Retry policy uses
validated attempts, exponential backoff, a cap, and optional jitter.
At the configured attempt limit, the episode enters `needs_review` with
`UNEXPECTED_REMOTE_STATE` and retains its pending stage as `resume_status`.

## Reviews

Use `reviews list` to retrieve active reviews. `reviews resolve` accepts a JSON
object validated against the stored reason. Inventory ambiguity requires a
selected file; duplicate candidates require a selected candidate; identity and
runtime mapping reviews require their corresponding identity fields. The full
allowed field set is implemented by `ReviewService`; unknown keys are rejected.

The reason-specific minimums are:

| Reason | One accepted resolution group |
|---|---|
| `MULTIPLE_VIDEO_FILES` | `selected_file_id` or `file_id` |
| `AMBIGUOUS_EPISODE` | file/choice, or both `season` and `episode` |
| `NO_VALID_VIDEO` | file, choice, or action |
| `PATH_CONFLICT` | action, or a selected file reference |
| `METADATA_CONFLICT` | action, choice, or confirm |
| `DUPLICATE_EPISODE` | `selected_candidate_id` or `candidate_id` |
| `UNEXPECTED_REMOTE_STATE` | action, choice, or `task_id` |
| `LEGACY_IDENTITY_UNRESOLVED` | choice, or `media_id` + `season` + `episode` |
| `RUNTIME_MAPPING_AMBIGUOUS` | `task_id` |

Resolution writes the episode, source/candidate selection, review fields,
retry reset, and history transactionally. The stored `review_reason` and
`review_generation` are compared again at commit; a stale resolver is rejected. A
resolved review returns to its saved `resume_status`, never directly to a
terminal success without evidence.

## Rollback principle

For a local upgrade, stop writes, preserve the code/config/database backup,
validate copies offline, and restore code and the consistent database backup
together if validation fails. Follow the [SQLite backup API or stopped-writer
checkpoint procedure](../docs/V5-UPGRADE.md#1-back-up-the-release-unit); copying
live database/WAL/SHM files independently is not a backup. Do not delete source
media or state to force a retry. Deployment-local sequencing is in
[deployment-local.md](deployment-local.md).
