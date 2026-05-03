# Changelog

All notable changes to `discrawl` will be documented in this file.

## 0.6.5 - 2026-05-03

### Fixes

- Scheduled Discord backup publishing now skips redundant pre-sync snapshot imports when the workflow DB cache is warm, keeping fresh Git snapshots from getting delayed by a full archive reimport.
- `discrawl sync` now keeps Git snapshot refreshes explicit by default; use `--update=auto` or `--update=force` when you want a sync run to pull/import the shared snapshot before live Discord or desktop-cache deltas.
- Snapshot imports now emit phase/table/file progress and keep the sync lock file updated with the active phase, making long update/import runs diagnosable instead of looking hung.
- Recent-message scans are backed by a plain `messages(created_at, id)` index so archive freshness and short-window analysis queries avoid full-table scans.

## 0.6.4 - 2026-05-03

### Fixes

- `discrawl` now handles SIGINT/SIGTERM by canceling active sync/import contexts so large SQLite and FTS writes can roll back and close cleanly instead of being terminated mid-transaction.

### Maintenance

- Refreshed dependency and CI tooling pins, including GoReleaser, `go-toml`, golangci-lint, and gosec.
- Tightened CI compatibility with the latest linters and made signal-cancellation and sync fixture tests deterministic under the race detector.

## 0.6.3 - 2026-05-01

### Fixes

- Added OS keyring fallback for Discord bot-token resolution, keeping env as the first source and documenting the default keyring item. (#17)
- Clarified and locked down FTS query normalization so operator-like search terms such as `AND`, `OR`, `NOT`, `NEAR`, and `*` stay parameterized and quoted before SQLite `MATCH`. Thanks @mvanhorn.

### Maintenance

- Tightened Go linting with additional golangci-lint checks for compiler directives, host/port formatting, predeclared identifiers, missing command contexts, and related code-quality regressions.
- Updated test subprocess helpers to use test-scoped contexts and cleaned up assertions so the stricter CI suite stays green.

## 0.6.2 - 2026-05-01

### Changes

- Added `discrawl digest` for per-channel activity summaries with messages, replies, active authors, top posters, and top mentions. Thanks @mvanhorn.
- Added `discrawl analytics quiet` and `discrawl analytics trends` for finding silent top-level channels and week-over-week channel volume. Thanks @mvanhorn.

### Fixes

- `discrawl digest` now reports reply counts as `replies` instead of mislabeling reply roots as Discord threads.
- `discrawl sync` now serializes concurrent runs with a local lock, preventing two refreshes from writing the archive at the same time.
- Git snapshot imports now keep SQLite crash recovery enabled and share the same archive lock as sync, update, tail, wiretap, embed, and auto-update reads so interrupted imports are less likely to corrupt the live database.
- Git snapshot imports now recover from corrupt local FTS tables by dropping and rebuilding search indexes, and repair missing guild IDs from channel metadata so shared archive reports stay fresh.
- Channel-history sync now falls back to the channel guild when Discord omits `message.guild_id`, keeping messages, attachments, mentions, and FTS rows correctly scoped.

## 0.6.1 - 2026-04-28

### Fixes

- Repeated `sync --source wiretap` runs now skip unchanged Discord Desktop cache files and report unchanged file counts, making steady-state local-cache refreshes much faster.
- `sync --full --skip-members` now also skips member crawls when resuming incomplete stored channels, so backfills do not unexpectedly refresh the full guild member list.

### Maintenance

- Refactored sync-mode handling so routine latest syncs, `--all-channels`, `--full`, and member-refresh decisions share clearer internal paths with regression coverage.
- Refreshed Go module dependencies and CI tool/action pins, including staticcheck, gofumpt, gosec, govulncheck, gitleaks, setup-node, and GoReleaser.
- Hardened report README writes and Discord Desktop cache reads with root-scoped filesystem access to satisfy the latest gosec checks.

## 0.6.0 - 2026-04-24

### Changes

- `dms` now lists local wiretap DM conversations and can read or search one DM thread with `--with`, `--last`, and `--search`, so common DM queries no longer require raw SQL.
- `search --dm` and `messages --dm` now target the local-only `@me` archive directly and skip Git snapshot auto-update, since DMs are never imported from the shared mirror.
- Go module dependencies and lint rules were refreshed for the current Go toolchain, including stricter JSON marshal checks and modern simplification rules.

### Fixes

- Wiretap now infers fallback DM channel names from cached Discord user/profile data, so channels discovered only from route/message cache entries resolve to names like `Vincent K` instead of `channel-*`.
- Wiretap message output now preserves sanitized author labels in stored metadata, improving `dms` and `messages` output without storing raw desktop cache payloads.

### Tests

- Added regression coverage for DM channel-name inference from cached profile data when Discord Desktop cache lacks explicit channel recipient metadata.
- Added coverage for local DM conversation listing/filtering, DM cleanup paths, share import/export helpers, CLI DM windows, and Discord Desktop import helper edge cases.
- CI now runs uncached test and race suites, checks `go mod tidy`, and performs a snapshot GoReleaser build before release tags.

## 0.5.1 - 2026-04-24

### Fixes

- Git snapshot export/import now keeps wiretap DMs strictly local: `@me` rows, wiretap sync state, and DM vectors are excluded from published snapshots while existing local DM rows are preserved on import.
- Publishing without `--with-embeddings` now omits old embedding manifests instead of carrying forward a stale vector bundle.

## 0.5.0 - 2026-04-24

### Changes

- `sync --source both|discord|wiretap` controls bot-token sync versus local Discord Desktop cache import; the default is `both`.
- `wiretap` imports classifiable cached Discord Desktop message payloads into the local archive, including proven DMs under synthetic guild id `@me`, without using user tokens.
- `sync` now defaults to the fast latest-message refresh path for untargeted runs; use `--all-channels` for the broad stored-channel repair sweep or `--full` for historical backfill.

## 0.4.1 - 2026-04-22

### Fixes

- existing archives that already report schema version 2 now self-heal missing embedding tables and columns before 0.4.x sync/update commands continue.

## 0.4.0 - 2026-04-22

### Changes

- semantic message search now ranks across the full compatible local vector set instead of only the newest candidate window. (#36) Thanks @GaosCode.
- hybrid message search now fuses FTS with local semantic vectors while avoiding embedding-provider calls when no local vectors exist. (#37) Thanks @GaosCode.
- local embedding providers now support OpenAI-compatible endpoints, Ollama, and llama.cpp, and `doctor` can probe the configured provider before you queue vectors
- `embed` now drains the queued embedding backlog in bounded batches, requeues safely on provider throttling, and drops stale stored vectors when messages no longer have embeddable content
- Git snapshot publishing can now opt in to backing up generated embedding vectors with `--with-embeddings` while still keeping embedding queue state local.
- Git-backed snapshot imports are now much faster on large archives by using import-only SQLite pragmas and bulk-load FTS5 settings during search index rebuilds
- `messages` and `mentions` now use composite read-path indexes so larger archives spend less time sorting/filtering common guild, channel, and author queries

### Fixes

- normalized message text is now sanitized before it reaches SQLite and FTS5, repairing malformed UTF-8 and stripping invisible/control-character noise that can poison search content
- Git-backed snapshots now keep embedding queue state and generated vectors local to each archive, so subscribers no longer inherit misleading embedding backlog metadata. (#38) Thanks @GaosCode.

### Docs

- docs now cover semantic and hybrid search setup, embedding privacy, Git snapshot behavior, and local vector rebuilds. (#39) Thanks @GaosCode.

### Tests

- Git embedding snapshot export/import now has CLI, share-package, and Docker E2E coverage.
- total Go test coverage now reaches the 85% line.

## 0.3.0 - 2026-04-21

- `sync --all` now bypasses `default_guild_id` so one run can fan out across every discovered guild without clearing the single-guild default first
- `sync --full` no longer aborts when forum thread discovery hits Discord `403 Missing Access`; inaccessible channels are skipped and marked unavailable while accessible channels continue syncing
- startup now validates and stamps SQLite schema version via `PRAGMA user_version`, and fails fast if the local DB schema is newer than the running binary
- git-backed archive sharing can now export/import compressed JSONL snapshots with manifests, subscribe to a Git repo as the data source, and run in git-only mode without Discord credentials
- `messages`, `search`, and reports can automatically refresh stale git-backed data, preferring the Git snapshot before falling back to live Discord when both sources are configured
- the Discord backup publisher workflow now syncs latest messages, publishes the archive to a private GitHub repo, serializes concurrent runs, validates required secrets, and skips the member crawl for faster updates
- the backup report workflow now updates README activity stats from the backup action and keeps those queries bounded with process timeouts
- `sync --latest-only` adds a lightweight refresh path for checking recent Discord messages without doing a full historical crawl
- repository imports now skip expensive rebuilds when the snapshot manifest is already current, and GitHub Actions persist the warmed SQLite database across runs
- the Docker git-source smoke test now verifies that a fresh install can subscribe to a repository-only archive and query messages, SQL, and reports
- CI now uses Go 1.26.2, `actions/setup-go` 6.4.0, cache actions 5.0.5, Node 24 for report generation, and refreshed SQLite dependencies

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
- env-based bot token discovery
- resumable full-history sync, live gateway tailing, repair sync loop, targeted channel sync
- attachment-text indexing for small text-like uploads
- structured user and role mention indexing/querying
- empty-message filtering based on real searchable/displayable content instead of raw body only
- CI with lint, tests, secret scanning, and coverage enforcement
- release plumbing via GoReleaser, GitHub Actions, and Homebrew tap packaging
- sync correctness fixes for empty channels, inaccessible channels, unknown channels, and large-channel resume behavior
- SQLite/FTS performance fixes for backfill throughput and lower write amplification
