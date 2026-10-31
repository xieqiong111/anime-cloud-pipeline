# Current-quarter resource preparation

This procedure applies to the deployment's fixed quarter-resource template.
When the user has authorized that template for recurring quarter preparation,
the same scope covers later quarter rollovers; do not ask for confirmation
again only because the quarter changed. It does not authorize media processing,
new resource types, credential changes, or scheduler/model changes. Merely
loading this skill, qualifying, or planning does not authorize resource writes.

## Scope and planning

The current implementation calculates the preparation quarter using the
`Asia/Shanghai` date. It does not establish support for an arbitrary resource
timezone. Confirm that this quarter agrees with `schedule.timezone`; if not,
stop and resolve the mismatch before planning or applying. The canonical
quarter key is `<YYYY>-<quarter-start-MM>` and the label is
`<quarter-key>月新番`.

The resource configuration must explicitly provide the supported cloud
subdirectory (`paths.cloud_library_subdir`) and LitePan raw output base
(`paths.litepan_raw_output_root`). Do not infer missing values or substitute a
legacy `litepan_publish_subdir` layout. Read the deployed config through the
authorized entrypoint; do not copy it to an unprotected workspace.

The current quarter adapters are supported only with LitePan `v0.5.5-beta`,
Emby `4.9.5.0`, and Linux/Docker on the same host, with the Emby container
named `Emby`. Other versions, container layouts, and host arrangements are
unsupported for this implementation; stop rather than guessing.

Run the read-only commands in this order:

1. `qualify` reads the complete MoviePilot subscription snapshot and verifies
   the exact `admin` / `Bangumi订阅` source allowlist, genre 16 filter,
   corresponding-season premiere-date gate, active TV identity, and duplicate
   handling within that snapshot. Incomplete inventory, unknown identity, or
   any other unknown result stops the workflow. If there are no eligible items
   and no unknowns, do not create quarter resources. Qualification does not
   open the V5 state database; normal V5 discovery must still retain its
   cross-state identity guard.
2. `plan-resources` reads the deployment config and current inventories for
   the eight quarter resources. It reports only derived targets, each
   resource's `reuse` / `create` / `conflict` / `unknown` state, and necessary
   config changes. It performs no writes. A `create` state is valid only when
   the relevant complete inventory proves absence; `unknown` or `conflict`
   stops the workflow. Record the returned `config_revision` exactly. It must
   be 64 lowercase hexadecimal characters and binds both the config snapshot
   and the preparation quarter. A token from another quarter is stale even
   when the config bytes are unchanged. Qualification and inventories are
   checked again during apply.

For installed use, the wrapper supplies the external config. Direct checkout
commands require `--config` for `qualify`, `plan-resources`, and
`prepare-resources`. Apply repeats qualification and resource inventory checks
before each create and reads the resources back afterward.

## Resource scope

Derive candidates from the verified deployment config; do not publish or
hard-code live paths or resource IDs.

| Resource | Required scope |
| --- | --- |
| 115 directories | Current-quarter root and configured cloud subdirectory below the verified cloud root; create only components proven absent. |
| LitePan task | One recursive task whose source is exactly the quarter cloud root and whose host output is the quarter child of the configured raw-output base. |
| Webhook rule | One active rule for the verified event/source targeting only the exact quarter task. |
| Published STRM tree | Separate from LitePan raw task output; do not merge, alias, or overlap the two roots. |
| Emby library | One TV library with the exact quarter location in the verified Emby view, corresponding to the published STRM tree. |

The planner may update only these seven resource-derived V5 fields:

```text
paths.cloud_source_root
runtime.litepan_task_name
runtime.litepan_task_source_root
runtime.litepan_task_output_root
runtime.litepan_event_name
runtime.emby_library_name
runtime.emby_library_root
```

Keep service credentials, `runtime.litepan_event_source`, base directories,
V5 state, and all other config fields unchanged.

Never use a whole-library LitePan task as a fallback. Keep old-quarter
resources, subscriptions, state, and permissions unchanged. Do not create
placeholder media, STRM files, or metadata to satisfy a resource check.

## Apply boundary

Only apply after the user has authorized this fixed resource template and all
qualification and inventory checks pass. Set the resource gate for this one
operation and pass the exact fresh revision from the immediately preceding
plan:

```sh
OCTOP_PIPELINE_ALLOW_RESOURCES=1 \
OCTOP_PIPELINE_RESOURCE_REVISION=<fresh-64-lowercase-hex-config_revision> \
  <install-root>/current/bin/anime-cloud-pipeline prepare-resources --apply
```

The installed wrapper supplies the config. Direct checkout use also requires
`--config <deployed-config>`. Never reuse a revision after a config or quarter
change. The gate and revision are per-operation safeguards, not credentials or
long-lived authorization. The Hermes wrapper forwards them only to this apply
operation; do not store them in dotenv files or reuse service-credential
variables for them. If the fixed-template authorization is absent, prepare
nothing. The Hermes wrapper does not forward a parent `LITEPAN_ADMIN_COOKIE`
environment value. The supported session-file fallback is
`<state_directory>/../config/litepan-admin-session`: at most 8192 bytes, a
regular file with no symlink or hard-link, and no group or other permission
bits. Do not invent another environment variable or use a service credential
as an admin session.

Preparation may create only the planned quarter resources and update only the
planned V5 mappings. Use supported authenticated service interfaces; never
write LitePan or Emby databases directly. Before applying, verify that the
configured parent paths exist, are accessible, and resolve within the intended
mounts. For MoviePilot, follow the
[host and container mount-view check](moviepilot.md#host-and-container-mount-view).
Verify the Emby library path in the actual supported container view as well; a
configured path or library location alone does not prove a live mount.
Reject symlink escapes, file placeholders, overlapping raw/published trees,
and unknown mount mappings. Do not create a mount or recursively build an
unverified root tree.

Read back each created or reused resource by its actual identity and exact
scope before applying dependent mappings. A successful request or HTTP status
is not read-back evidence. If a write times out or its result is ambiguous,
re-query the complete inventory before any further action; do not blindly
repeat a create. Leave partial resources intact for reconciliation and report
them; do not delete or rewrite existing resources to simulate rollback.

Resource preparation does not run `run`, `run --resume-only`, migration,
activation, a download, organization, STRM generation, metadata scraping,
library refresh, import, or scan. It does not replace or edit the verified
scheduler owner, schedule, or model. Resource authorization and media-write
authorization are separate.

## Final checks and evidence

After preparation and mapping updates, run `preflight` and `dry-run`. Both are
read-only and must remain so. A mismatch, unknown, or incomplete inventory
blocks continuation. Do not run media processing as part of this resource
workflow; continue only if a separate user authorization covers the exact
requested media scope.

Record whether resources were reused or created, the mapping fields changed,
read-back evidence, and any remaining `BLOCKED`, `NOT_RUN`, or
`MANUAL_PENDING` steps. Do not include credentials, playback URLs, live paths,
raw service responses, or unnecessary runtime IDs. A successful plan is not a
resource creation; resource readiness is not end-to-end media acceptance.
