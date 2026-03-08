# Changelog

All notable changes to `discrawl` will be documented in this file.

## 0.2.0 - Unreleased

- offline member-profile search via `member_fts`
- `members search` now matches archived profile fields in addition to names
- `members show` now accepts ids or queries and shows recent messages plus message stats when uniquely resolved
- profile extraction surfaces stored fields like `bio`, `website`, `x`, `github`, and other archived URLs when present

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
