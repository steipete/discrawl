# discrawl 🛰️ — Mirror Discord into SQLite; search server history locally

`discrawl` mirrors Discord guild data into local SQLite so you can search, inspect, and query server history without depending on Discord search.

It is a bot-token crawler. No user-token hacks. Data stays local.

## What It Does

- discovers every guild the configured bot can access
- syncs channels, threads, members, and message history into SQLite
- maintains FTS5 search indexes for fast local text search
- builds an offline member directory from archived profile payloads
- extracts small text-like attachments into the local search index
- records structured user and role mentions for direct querying
- tails Gateway events for live updates, with periodic repair syncs
- exposes read-only SQL for ad hoc analysis
- keeps schema multi-guild ready while preserving a simple single-guild default UX

Search defaults to all guilds. `sync` and `tail` default to the configured default guild when one exists, otherwise they fan out to all discovered guilds.

## Requirements

- Go `1.26+`
- a Discord bot token the bot can use to read the target guilds
- bot permissions for the channels you want archived

### Discord Bot Setup

`discrawl` needs a real bot token. Not a user token.

Minimum practical setup:

1. Create or reuse a Discord application in the Discord developer portal.
2. Add a bot user to that application.
3. Invite the bot to the target guilds.
4. Enable these intents for the bot:
   - `Server Members Intent`
   - `Message Content Intent`
5. Ensure the bot can at least:
   - view channels
   - read message history

Without those intents/permissions, `sync`, `tail`, member snapshots, or message content archiving will be partial or fail.

### Bot Token Sources

Token resolution:

1. OpenClaw config, if `discord.token_source` is not `env`
2. `DISCORD_BOT_TOKEN` or the configured `discord.token_env`

`discrawl` accepts either raw token text or a value prefixed with `Bot `. It normalizes that automatically.

Fastest env-only path:

```bash
export DISCORD_BOT_TOKEN="your-bot-token"
discrawl doctor
discrawl init
```

If you keep shell secrets in `~/.profile`, add:

```bash
export DISCORD_BOT_TOKEN="your-bot-token"
```

Then reload your shell before running `discrawl`.

If you already use OpenClaw, `discrawl` can reuse the Discord token from `~/.openclaw/openclaw.json` by default.

Default runtime paths:

- config: `~/.discrawl/config.toml`
- database: `~/.discrawl/discrawl.db`
- cache: `~/.discrawl/cache/`
- logs: `~/.discrawl/logs/`

## Install

Homebrew (recommended):

```bash
brew install steipete/tap/discrawl  # auto-taps steipete/tap
discrawl --version
```

Build from source:

```bash
git clone https://github.com/steipete/discrawl.git
cd discrawl
go build -o bin/discrawl ./cmd/discrawl
./bin/discrawl --version
```

Examples below assume `discrawl` is on `PATH`. If you built from source without installing it, replace `discrawl` with `./bin/discrawl`.

## Quick Start

Reuse an existing OpenClaw Discord bot config:

```bash
discrawl init --from-openclaw ~/.openclaw/openclaw.json
discrawl doctor
discrawl sync --full
discrawl search "panic: nil pointer"
discrawl tail
```

Multi-account OpenClaw setup:

```bash
discrawl init --from-openclaw ~/.openclaw/openclaw.json --account atlas
```

Env-only setup:

```bash
export DISCORD_BOT_TOKEN="..."
discrawl doctor
discrawl init
discrawl sync --full
```

`init` discovers accessible guilds and writes `~/.discrawl/config.toml`. If exactly one guild is available, that guild becomes the default automatically.

`doctor` is the fastest sanity check:

- confirms config can be loaded
- shows where the token was resolved from
- verifies bot auth
- shows how many guilds the bot can access
- verifies DB + FTS wiring

## Commands

### `init`

Creates the local config and discovers accessible guilds.

```bash
discrawl init
discrawl init --from-openclaw ~/.openclaw/openclaw.json
discrawl init --from-openclaw ~/.openclaw/openclaw.json --account atlas
discrawl init --guild 123456789012345678
discrawl init --db ~/data/discrawl.db
```

When OpenClaw config tokens use `${ENV_VAR}` placeholders, `init` and `doctor` resolve them before auth.

### `sync`

Backfills guild state into SQLite.

```bash
discrawl sync --full
discrawl sync --full --all
discrawl sync --guild 123456789012345678
discrawl sync --guilds 123,456 --concurrency 8
discrawl sync --channels 111,222 --since 2026-03-01T00:00:00Z
```

`sync` already uses parallel channel workers. `--concurrency` overrides the default, and the default is auto-sized from `GOMAXPROCS` with a floor of `8` and a cap of `32`.
`--all` ignores `default_guild_id` and fans out across every discovered guild the bot can access.
When `--channels` includes a forum channel id, `discrawl` expands that forum's threads and syncs their messages as part of the targeted run.
`--since` limits initial history/bootstrap and full-history backfill to messages at or after the given RFC3339 timestamp. It does not mark older history as complete, so a later `sync --full` without `--since` can continue the backfill.
Long runs now emit periodic progress logs to stderr so large backfills do not look hung.
If in-flight channels stop completing for a while, `discrawl` now emits `message sync waiting` heartbeat logs with the oldest active channel, per-channel page activity, and skip/defer counters, and every run ends with a `message sync finished` summary.
Each channel crawl also has a bounded runtime budget, so a pathological channel is deferred and retried on the next sync instead of pinning a worker forever.
Full sync member refresh is best-effort and currently gives up after five minutes without a caller-supplied deadline, so message sync completion is not held hostage by a slow guild member crawl.
When the archive is already complete, `sync --full` now reuses the stored backlog markers and limits steady-state refresh to live top-level channels plus active threads instead of revisiting every stored archived thread.
If a guild already has a local member snapshot, routine syncs reuse it and skip another full member crawl until that snapshot ages out.

### `tail`

Runs the live Gateway tail and periodic repair loop.

```bash
discrawl tail
discrawl tail --guild 123456789012345678
discrawl tail --repair-every 30m
```

### `search`

Runs FTS search over archived messages.

```bash
discrawl search "panic: nil pointer"
discrawl search --guild 123456789012345678 "payment failed"
discrawl search --channel billing --author steipete --limit 50 "invoice"
discrawl search --include-empty "GitHub"
discrawl --json search "websocket closed"
```

By default, `search` skips rows with no searchable content. Attachment text, attachment filenames, embeds, and replies still count as content. Use `--include-empty` to opt back in.

### `messages`

Lists exact message slices by channel, author, and time range.

```bash
discrawl messages --channel maintainers --days 7 --all
discrawl messages --channel maintainers --hours 6 --all
discrawl messages --channel "#maintainers" --since 2026-03-01T00:00:00Z
discrawl messages --channel 1456744319972282449 --author steipete --limit 50
discrawl messages --channel maintainers --last 100 --sync
discrawl messages --channel maintainers --days 7 --all --include-empty
discrawl --json messages --channel maintainers --days 3
```

Notes:

- `--channel` accepts a channel id, exact name, `#name`, or partial name match
- `--hours` is shorthand for "since now minus N hours"
- `--days` is shorthand for "since now minus N days"
- `--last` returns the newest `N` matching messages, then prints them oldest-to-newest
- `--all` removes the safety limit; default is `200`
- `--sync` runs a blocking pre-query sync for the matching channel or guild scope before reading the local DB
- rows with no displayable/searchable content are skipped by default; `--include-empty` opts back in
- at least one filter is required

### `mentions`

Lists structured user and role mentions.

```bash
discrawl mentions --channel maintainers --days 7
discrawl mentions --target steipete --type user --limit 50
discrawl mentions --target 1456406468898197625
discrawl --json mentions --type role --days 1
```

Notes:

- `--target` accepts an id, exact name, or partial name match
- `--type` can be `user` or `role`
- same guild/time filters as `messages`

### `sql`

Runs read-only SQL against the local database.

```bash
discrawl sql 'select count(*) as messages from messages'
echo 'select guild_id, count(*) from messages group by guild_id' | discrawl sql -
```

### `members`

```bash
discrawl members list
discrawl members show 123456789012345678
discrawl members show --messages 10 steipete
discrawl members search "peter"
discrawl members search "github"
discrawl members search "https://github.com/steipete"
```

Notes:

- `search` matches names plus any offline profile fields present in the archived member payload
- `show` accepts a user id or query; if it resolves to one member, it also shows recent messages
- extracted profile fields may include `bio`, `pronouns`, `location`, `website`, `x`, `github`, and discovered URLs
- if the bot cannot see a field from Discord, `discrawl` cannot invent it; this is strictly archive-based offline data

Typical workflow:

```bash
discrawl sync --full
discrawl members search "design engineer"
discrawl members search "github"
discrawl members show --messages 25 steipete
discrawl messages --author steipete --days 30 --all
```

Typical `members show` output:

```text
guild=1456350064065904867
user=37658261826043904
username=steipete
display=Peter Steinberger
joined=2026-03-08T16:03:14Z
bot=false
x=steipete
github=steipete
website=https://steipete.me
bio=Builds native apps and tooling.
urls=https://steipete.me, https://github.com/steipete
message_count=1284
first_message=2026-02-01T09:00:00Z
last_message=2026-03-08T15:59:58Z
```

Searchable member data comes from:

- Discord member/user payload fields archived into `members.raw_json`
- explicit profile fields when Discord exposes them
- URLs and social handles inferred from archived profile text
- current member snapshot data such as names, nick, roles, and join time

### `channels`

```bash
discrawl channels list
discrawl channels show 123456789012345678
```

### `status`

Shows local archive status.

```bash
discrawl status
```

### `doctor`

Checks config, auth, DB, and FTS wiring.

```bash
discrawl doctor
```

## Configuration

`init` writes a complete config, so most users should not hand-edit anything initially.

Typical config shape:

```toml
version = 1
default_guild_id = ""
guild_ids = []
db_path = "~/.discrawl/discrawl.db"
cache_dir = "~/.discrawl/cache"
log_dir = "~/.discrawl/logs"

[discord]
token_source = "openclaw"
openclaw_config = "~/.openclaw/openclaw.json"
account = "default"
token_env = "DISCORD_BOT_TOKEN"

[sync]
concurrency = 16
repair_every = "6h"
full_history = true
attachment_text = true

[search]
default_mode = "fts"

[search.embeddings]
enabled = false
provider = "openai"
model = "text-embedding-3-small"
api_key_env = "OPENAI_API_KEY"
batch_size = 64
```

The value above is an example. `init` writes an auto-sized default based on the host: `min(32, max(8, GOMAXPROCS*2))`.

Config override rules:

- `--config` beats everything
- `DISCRAWL_CONFIG` overrides the default config path
- `discord.token_source = "env"` forces env-only token lookup

## Embeddings

Embeddings are optional. FTS is the default search path and the primary verification target.

If enabled, embeddings are intended to enrich recall in background batches, not block the hot sync path.

```bash
export OPENAI_API_KEY="..."
discrawl init --with-embeddings
discrawl sync --with-embeddings
```

## Data Stored Locally

- guild metadata
- channels and threads in one table
- current member snapshot
- canonical message rows
- append-only message event records
- FTS index rows
- optional embedding backlog metadata

SQLite schema migrations are versioned with `PRAGMA user_version`. Startup now fails fast when a local DB schema is newer than the supported binary.

Attachment binaries are not stored in SQLite.

Set `sync.attachment_text = false` if you want to keep attachment metadata and filenames but disable attachment body fetches for text indexing.

## Security

- do not commit bot tokens or API keys
- default config lives in your home directory, not inside the repo
- CI runs secret scanning with `gitleaks`
- `doctor` reports token source, not token contents

## Development

Local gate:

```bash
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.11.1 run
go test ./... -coverprofile=/tmp/discrawl.cover
go tool cover -func=/tmp/discrawl.cover | tail -n 1
go build ./cmd/discrawl
```

Target coverage is `>= 80%`.

CI runs:

- `golangci-lint`
- `go test` with coverage threshold enforcement
- `go build ./cmd/discrawl`
- `gitleaks` against git history and the working tree

## Notes

- the schema is multi-guild ready even when the common UX stays single-guild simple
- threads are stored as channels because that matches the Discord model
- archived threads are part of the sync surface
- live sync is resumable; large guilds still take time because Discord rate limits history backfill

## License

MIT. See [LICENSE](LICENSE).
