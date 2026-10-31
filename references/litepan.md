# LitePan and STRM evidence

## Runtime adapter

`LitePanClient.health()` checks the health endpoint. Go LitePan v0.5.x exposes
no open task-list endpoint, so `list_tasks()` reads `strm_tasks` through a
read-only SQLite connection on the NAS. Set `runtime.litepan_task_db_path` and
`runtime.litepan_strm_host_root`. The task may scan a parent of the library:
set `runtime.litepan_task_source_root` and `runtime.litepan_task_output_root`
to its exact cloud source and host output roots. `trigger_event(event, source,
message)` sends the configured API-key event. Use a dedicated
`runtime.litepan_event_name` so another webhook rule is not triggered.
Before sending, the adapter checks that exactly one active rule for the
configured event/source targets the selected STRM task ID.

Runtime selection requires exactly one task matching configured name and source
root. Missing, duplicate, conflicting, inactive, or incomplete task evidence
opens `RUNTIME_MAPPING_AMBIGUOUS` or remains pending; a stale persisted task
identity is not accepted without a fresh complete inventory.

For current-quarter operation the selected task source is the quarter child of
`cloud_library_root`, not a parent that scans the entire cloud library. Set
`litepan_raw_output_root` to the common raw-output parent and
`litepan_task_output_root` to its quarter child. The configured event/source
must resolve to that task alone; see [quarter resource provisioning](quarter-resources.md).

## STRM contract

`StrmService` checks the exact mapped path before triggering an event. A valid
STRM is a regular non-symlink file containing one HTTP or HTTPS URL without
unsafe URL controls. Persisted evidence retains only safe URL facts such as
scheme, host, length, and digest; playback tokens, query values, credentials,
and complete URLs never enter state, history, logs, or notifications.

For an isolated 115 cloud directory, LitePan first generates a raw STRM under
`paths.litepan_raw_output_root`. After validating it, the pipeline publishes a
local regular STRM under the separate `paths.litepan_output_root` at the
standard `Season N/<work> - SxxExx.strm` path with an atomic no-replace link.
MoviePilot and Emby use only the published root. The LitePan task must scan
an ancestor of `cloud_library_root` recursively. `litepan_raw_output_root`
is the matching host directory inside the task output tree. Keep the raw
and published roots distinct so LitePan cleanup cannot remove the published
STRM.

An event response or HTTP 200 is only an accepted request. `strm_ready` requires
the exact file evidence after reconciliation. Existing exact evidence is used
before another trigger, including after a retry backoff.

`test_litepan_service.py` covers exact-path checks, task ambiguity, incomplete
envelopes, stale identity, symlinks, malformed URLs, event failures, and
reconciliation before trigger.
