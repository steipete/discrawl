---
name: discrawl
description: Use for local Discord archive search, sync freshness, DMs, channel summaries, and Discrawl repo/release work.
---

# Discrawl

Use local archive data first for Discord questions. Browse or hit live APIs only when the local archive is stale or the user asks for current external context.

## Sources

- DB: `~/.discrawl/discrawl.db`
- Config: `~/.discrawl/config.toml`
- Repo: `~/Projects/discrawl`
- Preferred CLI: `discrawl`; fallback to repo binary if installed binary is stale

## Freshness

For recent/current questions, check freshness before analysis:

```bash
sqlite3 ~/.discrawl/discrawl.db \
  "select coalesce(max(updated_at),'') from sync_state where scope like 'channel:%';"
```

Routine refresh:

```bash
discrawl doctor
discrawl sync
```

Historical/backfill refresh:

```bash
discrawl sync --full
```

If SQLite reports busy/locked, check for stray `discrawl` processes before retrying.

## Query Workflow

1. Resolve scope: guild, channel, DM, author, keyword, date range.
2. Check freshness for recent/current requests.
3. Use CLI for normal reads; use SQL for precise counts/rankings.
4. Report absolute date spans, counts, channel/DM names, and known gaps.

Common commands:

```bash
discrawl search "query"
discrawl messages --channel '#maintainers' --days 7 --all
discrawl --json sql "select count(*) from messages;"
```

When the installed CLI lacks a new feature, build or run from `~/Projects/discrawl` before concluding the feature is missing.

## Discord DMs

Wiretap/Desktop cache DMs are local-only. Do not imply they are in the published Git snapshot. For missing recent DMs, refresh first; stale archive is a common cause.

## Verification

For repo edits, prefer existing Go gates:

```bash
go test ./...
```

Then run targeted CLI smoke for the touched surface, for example:

```bash
discrawl doctor
discrawl search "test" --limit 5
```
