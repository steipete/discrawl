# Changelog

All notable changes to `discrawl` will be documented in this file.

## 0.4.0 - Unreleased

- Git-backed snapshot imports are now much faster on large archives by using import-only SQLite pragmas and bulk-load FTS5 settings during search index rebuilds
- `messages` and `mentions` now use composite read-path indexes so larger archives spend less time sorting/filtering common guild, channel, and author queries
- normalized message text is now sanitized before it reaches SQLite and FTS5, repairing malformed UTF-8 and stripping invisible/control-character noise that can poison search content
- local embedding providers now support OpenAI-compatible endpoints, Ollama, and llama.cpp, and `doctor` can probe the configured provider before you queue vectors
- `embed` now drains the queued embedding backlog in bounded batches, requeues safely on provider throttling, and drops stale stored vectors when messages no longer have embeddable content

## 0.3.0 - 2026-04-21

- `sync --all` now bypasses `default_guild_id` so one run can fan out across every discovered guild without clearing the single-guild default first
- `sync --full` no longer aborts when forum thread discovery hits Discord `403 Missing Access`; inaccessible channels are skipped and marked unavailable while accessible channels continue syncing
- startup now validates and stamps SQLite schema version via `PRAGMA user_version`, and fails fast if the local DB schema is newer than the running binary
- `init --from-openclaw` now supports `--account`, and OpenClaw token fields can use `${ENV_VAR}` placeholders
- git-backed archive sharing can now export/import compressed JSONL snapshots with manifests, subscribe to a Git repo as the data source, and run in git-only mode without Discord credentials
- `messages`, `search`, and reports can automatically refresh stale git-backed data, preferring the Git snapshot before falling back to live Discord when both sources are configured
- the Discord backup publisher workflow now syncs latest messages, publishes the archive to a private GitHub repo, serializes concurrent runs, validates required secrets, and skips the member crawl for faster updates
- the backup report workflow now updates README activity stats, supports OpenClaw-generated field notes, runs the field-note logic from the backup action, and keeps those queries bounded with process timeouts
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
- OpenClaw config reuse plus env-based bot token discovery
- resumable full-history sync, live gateway tailing, repair sync loop, targeted channel sync
- attachment-text indexing for small text-like uploads
- structured user and role mention indexing/querying
- empty-message filtering based on real searchable/displayable content instead of raw body only
- CI with lint, tests, secret scanning, and `80%+` coverage enforcement
- release plumbing via GoReleaser, GitHub Actions, and Homebrew tap packaging
- sync correctness fixes for empty channels, inaccessible channels, unknown channels, and large-channel resume behavior
- SQLite/FTS performance fixes for backfill throughput and lower write amplification
