# `analytics`

Groups activity-style queries under one namespace.

## Usage

```bash
discrawl analytics
discrawl analytics quiet --since 30d
discrawl analytics quiet --guild 123456789012345678
discrawl analytics trends --weeks 8
discrawl analytics trends --weeks 12 --channel general
discrawl --json analytics quiet --since 60d
discrawl --json analytics trends --weeks 4
```

## Subcommands

### `quiet`

Top-level text/announcement channels with no messages in the lookback window, including never-active channels.

- `--since <duration>` - lookback window (e.g. `30d`, `60d`)
- `--guild <id>` - scope to one guild; when omitted, `default_guild_id` is used if configured

### `trends`

Monday-start UTC weekly message counts per message-capable channel.

- `--weeks <n>` - number of weeks to include
- `--channel <id|name>` - scope to one channel
- `--guild <id>` - scope to one guild

## See also

- [`digest`](digest.html)
- [`status`](status.html)
