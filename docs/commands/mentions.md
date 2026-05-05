# `mentions`

Lists structured user and role mentions extracted during sync.

## Usage

```bash
discrawl mentions --channel maintainers --days 7
discrawl mentions --target steipete --type user --limit 50
discrawl mentions --target 1456406468898197625
discrawl --json mentions --type role --days 1
```

## Flags

- `--target <id|name>` - user or role id, exact name, or partial match
- `--type <user|role>` - filter by mention type
- `--channel <id|name>` - same channel matching as [`messages`](messages.html)
- `--guild <id>` / `--guilds <id,id>` - restrict the guild scope
- `--days <n>` / `--since <RFC3339>` / `--before <RFC3339>` - time filters
- `--limit <n>` - cap result count

## Notes

- mentions are recorded structurally during sync, so this is a direct row read - no FTS parsing
- combine with `--type role` to find every mention of a role
- combine with `--target steipete` to find everywhere your account got pinged
