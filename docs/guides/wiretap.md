# Desktop wiretap

`wiretap` imports classifiable Discord Desktop message payloads into the same local SQLite archive used by bot sync. It is the path for searchable DMs because bot tokens cannot read personal direct messages.

`wiretap` is also available through `discrawl sync --source wiretap` and is included in the default `discrawl sync --source both` path.

## What it does

- stores classifiable cache messages in the same `guilds`, `channels`, and `messages` tables used by bot sync
- stores proven DMs under the synthetic guild id `@me`
- preserves existing local `@me` guilds, channels, messages, and attachments when importing a Git snapshot, so a shared guild mirror refresh does not wipe local wiretap DM search
- drops message payloads whose channel cannot be classified from cached channel metadata or Discord route URLs; dropped rows are counted as `skipped_messages`
- imports what Discord Desktop has cached locally - not complete live DM history

## What it does not do

- does not extract, store, or print Discord auth tokens
- does not use a user token
- does not call the Discord API as your user
- does not run as a selfbot

## DM privacy: `@me` stays local

`@me` rows are local-only. Excluded from:

- `publish` (Git snapshot output)
- `subscribe` / Git snapshot import
- `--with-embeddings` snapshot export

Excluded categories: DM guilds, channels, messages, events, attachments, mentions, wiretap sync state, and vectors for DM messages.

## What gets scanned

- local `.ldb`, `.log`, `.json`, and `.txt` artifacts for Discord message JSON
- route-bearing Chromium HTTP cache entries by default
- `--full-cache` (or `desktop.full_cache = true`) enables exhaustive Chromium cache import for slower historical guild-cache archaeology
- `--max-file-bytes` skips unusually large files (default 64 MiB)

## Flags

```bash
discrawl wiretap
discrawl wiretap --path "$HOME/Library/Application Support/discord"
discrawl wiretap --dry-run
discrawl wiretap --full-cache
discrawl wiretap --watch-every 2m
```

`--watch-every` keeps the import running on a periodic loop. `--dry-run` reports what would be imported without writing anything.

## Default desktop paths

- macOS: `~/Library/Application Support/discord`
- Linux: `~/.config/discord`
- override via `--path` or `[desktop].path`

## See also

- [`wiretap`](../commands/wiretap.html)
- [`dms`](../commands/dms.html) - convenience layer over `@me`
- [Sync sources](sync-sources.html)
