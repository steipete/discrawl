# `wiretap`

Imports classifiable Discord Desktop message payloads into the same local SQLite archive.

This is the path for searchable DMs because bot tokens cannot read personal direct messages.

`wiretap` is also available through `discrawl sync --source wiretap` and is included in the default `discrawl sync --source both` path.

## Usage

```bash
discrawl wiretap
discrawl wiretap --path "$HOME/Library/Application Support/discord"
discrawl wiretap --dry-run
discrawl wiretap --full-cache
discrawl wiretap --watch-every 2m
```

## Flags

- `--path <dir>` - override the desktop data directory (default: platform-specific Discord cache path)
- `--dry-run` - report what would be imported without writing anything
- `--full-cache` - exhaustive Chromium HTTP cache import for historical guild-cache archaeology (slower)
- `--watch-every <duration>` - keep importing on a periodic loop
- `--max-file-bytes <n>` - skip unusually large files (default 64 MiB)

## Notes

- stores classifiable cache messages in the same `guilds`, `channels`, and `messages` tables used by bot sync
- stores proven DMs under the synthetic guild id `@me`
- `@me` rows stay local-only: never exported to `publish` / Git snapshot import / embedding snapshots
- preserves existing local `@me` rows when importing a Git snapshot
- drops message payloads whose channel cannot be classified from cached channel metadata or Discord route URLs; dropped rows are counted as `skipped_messages`
- imports what Discord Desktop has cached locally, not complete live DM history
- scans local `.ldb`, `.log`, `.json`, and `.txt` artifacts for Discord message JSON, plus route-bearing Chromium HTTP cache entries by default
- does not extract, store, or print Discord auth tokens

## Default desktop paths

- macOS: `~/Library/Application Support/discord`
- Linux: `~/.config/discord`

## See also

- [Wiretap guide](../guides/wiretap.html)
- [`dms`](dms.html)
- [`sync`](sync.html)
