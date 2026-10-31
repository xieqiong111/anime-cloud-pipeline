# MoviePilot adapter and metadata

## Client contract

`MoviePilotClient` reads subscriptions, storage listings, file content, media
scrape results, media-server libraries, and remote-existence data. Its default
auth mode is `x-api-key`; the legacy query-token mode is an explicit config
choice. Secret values are read from a file or environment reference and are not
included in errors, logs, reports, or committed JSON.

The implemented endpoints are:

| Method | Endpoint | Use |
|---|---|---|
| GET | `/api/v1/subscribe/` | subscription snapshot |
| GET | `/api/v1/system/env` | version metadata |
| POST | `/api/v1/storage/list` | bounded storage listing |
| POST | `/api/v1/storage/download` | read exact file content |
| POST | `/api/v1/media/scrape/local` | scrape one mounted path |
| GET | `/api/v1/mediaserver/library` | library lookup via MoviePilot |
| POST | `/api/v1/mediaserver/exists_remote` | remote existence query |
| GET/PUT | `/api/v1/plugin/{plugin_id}` | full plugin config workflow |
| GET | `/api/v1/plugin/` | plugin state summary |

`list_subscriptions()` validates the HTTP status and its supported bare-list or
`data`-list envelope. Other methods use endpoint-specific parsing: `version()`
reads `data.VERSION`, storage listing returns a list when one is provided,
file download reads the returned head content, scrape returns HTTP status and
detail, library lookup accepts its endpoint result, and plugin methods handle
their mapping/list results. There is no generic envelope contract for those
endpoints. A response body, cookie, API key, or query token is never copied
into a persisted diagnostic.

## Host and container mount view

Configured path mappings describe intended translation; they do not prove the
live MoviePilot mount. Before using a new quarter path, identify the actual
running MoviePilot container and inspect its effective mount source and
destination with the container runtime. Resolve the intended parent on the NAS
host and inspect the corresponding path from inside that container, then
confirm both views reach the same existing directory or file identity. After
an authorized directory creation, verify the new path in both views before
updating mappings.

Use read-only inspection and existing objects; do not create marker media or
assume a configured path, API response, or container name proves the mount.
Stop on a missing, inaccessible, overlapping, or ambiguous mapping. Do not
create or alter mounts as part of quarter preparation. Keep evidence for the
host and container views separate from API success or local path validation.

## Metadata evidence

`MetadataService` first reconciles existing sidecars, then invokes
`scrape_in_place` only when evidence is incomplete. For a standard `Season N`
STRM, the adapter asks MoviePilot to scrape the containing show directory so
the show, season, and episode sidecars are all generated. This MoviePilot call
can revisit other episodes of the same show. `SidecarEvidence` requires
the exact canonical STRM plus non-empty episode NFO/image, show NFO/poster, and
season NFO/poster. XML identity must match media, season, and episode; a
contradiction opens `METADATA_CONFLICT`, while transient absence remains
recoverable at `scrape_pending`.

An HTTP success or scrape response is not completion. The next stage starts
only after the sidecar evidence validator returns complete and safe.
