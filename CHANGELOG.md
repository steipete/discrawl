# Changelog

All notable changes to `discrawl` will be documented in this file.

## Unreleased

- `sync --full` no longer aborts when forum thread discovery hits Discord `403 Missing Access`; inaccessible channels are skipped and marked unavailable while accessible channels continue syncing
- startup now validates and stamps SQLite schema version via `PRAGMA user_version`, and fails fast if the local DB schema is newer than the running binary
- `init --from-openclaw` now supports `--account`, and OpenClaw token fields can use `${ENV_VAR}` placeholders

## 0.2.0 - 2026-03-26

- much faster `sync --full` behavior on large archives: incomplete backfills are auto-batched, active-thread discovery is more precise, and steady-state refreshes avoid re-scanning every archived thread once history is already complete
- `sync --since` now reliably honors the cutoff during bootstrap and full-history backfill, while still allowing a later `sync --full` without `--since` to continue older history
- full-sync progress is more resilient: slow member crawls no longer hold message sync hostage, and stale unavailable-channel markers are cleared so recovered channels can sync again
- offline member-profile search is now much richer: `members search` matches archived profile fields in addition to names
- `members show` now accepts either Discord IDs or queries and can include recent messages plus message stats for the resolved member
- archived profile extraction now surfaces stored fields like `bio`, `pronouns`, `location`, `website`, `x`, `github`, and discovered URLs when present
- `messages --sync` can do a blocking pre-query refresh for the matching channel or guild scope before reading the local archive
- `messages --hours` adds recent-hour slices without manual RFC3339 timestamps
- `messages --last` returns the newest matching rows while still printing them oldest-to-newest

## 0.1.0 - 2026-03-08

- initial public release of `discrawl`
- multi-guild Discord crawler with single-guild default UX
- local SQLite archive with FTS5 search
- commands: `init`, `sync`, `tail`, `search`, `messages`, `mentions`, `sql`, `members`, `channels`, `status`, `doctor`
- OpenClaw config reuse plus env-based bot token discovery
- resumable full-history sync, live gateway tailing, repair sync loop, targeted channel sync
- attachment-text indexing for small text-like uploads
- structured user and role mention indexing/querying
- empty-message filtering based on real searchable/displayable content instead of raw body only
- CI with lint, tests, secret scanning, and `80%+` coverage enforcement
- release plumbing via GoReleaser, GitHub Actions, and Homebrew tap packaging
- sync correctness fixes for empty channels, inaccessible channels, unknown channels, and large-channel resume behavior
- SQLite/FTS performance fixes for backfill throughput and lower write amplification
