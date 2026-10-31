# Credential-safe cleanup and read-only operations

## Plugin configuration

`scripts/plugin_config.py show` prints only a bounded masked structure. `set`
rejects sensitive `--set` keys before fetching configuration, reads the full
plugin config, changes permitted fields in memory, performs a full replacement,
re-reads and verifies, and attempts rollback on mismatch. Changed values are
reported as `[CHANGED]`, never echoed.

Use `--config <config>` or `--mp-base <url> --token-file <path>`; do not put a
secret in an argument. The endpoint has full-replacement semantics, so a
partial payload is unsafe.

## Snapshot cleanup

Windows snapshots use the protected `.dpapi` path. On Linux, plaintext
snapshots are disabled unless explicitly opted in and must use the
`.SECRET-PLAINTEXT.json` suffix. The cleanup lease is created immediately after
the file descriptor is opened. Cleanup scrubs/truncates/fsyncs the held inode
through that descriptor and removes the pathname only after its identity is
proven.

If pathname identity is unknown or has changed, fail closed without unlinking
that pathname. Leave a replacement pathname exactly as found (it may be
non-empty) for manual verification; never scrub or unlink an unverified
replacement inode. Cleanup continues independent scrub/unlink attempts and
reports only safe error classes.

## Read-only guarantees

`preflight`, `dry-run`, `status`, `reviews list`, `audit`, and `migrate
--check-only` use non-writable paths. `config-migrate` writes only its explicit
output/report artifacts. A read-only command must not create a database, lock,
snapshot, or external write. `test_cli.py`, `test_plugin_config.py`, and
`test_security.py` are the executable safety contract.
