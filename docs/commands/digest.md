# `digest`

Summarizes per-channel activity for a lookback window.

## Usage

```bash
discrawl digest
discrawl digest --since 30d
discrawl digest --guild 123456789012345678
discrawl digest --channel general
discrawl --json digest --since 7d --top-n 5
```

## Flags

- `--since <duration>` - Go durations (`72h`, `30m`) and `Nd` shorthand (`7d`, `30d`)
- `--guild <id>` - scope to one guild; when omitted, `default_guild_id` is used if configured
- `--channel <id|name>` - scope to one channel
- `--top-n <n>` - how many top posters and mention targets per channel

## Output

For each channel in scope: message count, top posters, top mention targets, first/last activity in window.

## See also

- [`analytics`](analytics.html)
- [`mentions`](mentions.html)
