# discrawl 🛰️ — Mirror Discord into SQLite; search server history locally

`discrawl` mirrors Discord guild data into local SQLite so you can search, inspect, and query server history without depending on Discord search. Teams can also publish that archive as a private Git snapshot repo, so readers get fresh org memory without Discord bot credentials.

Live sync uses real bot tokens. No user-token hacks. Data stays local unless you explicitly publish a Git-backed snapshot.

## What It Does

- discovers every guild the configured bot can access
- syncs channels, threads, members, and message history into SQLite
- maintains FTS5 search indexes for fast local text search
- builds an offline member directory from archived profile payloads
- extracts small text-like attachments into the local search index
- records structured user and role mentions for direct querying
- tails Gateway events for live updates, with periodic repair syncs
- publishes and imports private Git-backed archive snapshots for org-wide read access
- supports Git-only read mode with no Discord credentials on reader machines
- generates backup README activity reports, with optional AI-written field notes
- exposes read-only SQL for ad hoc analysis
- keeps schema multi-guild ready while preserving a simple single-guild default UX

Search defaults to all guilds. `sync` and `tail` default to the configured default guild when one exists, otherwise they fan out to all discovered guilds.

## Requirements

- Go `1.26+`
- for publishing/syncing: a Discord bot token the bot can use to read the target guilds
- for read-only Git-backed access: access to a private snapshot repo, no Discord credentials required
- bot permissions for the channels you want archived when running `sync` or `tail`

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

Git-only reader setup:

```bash
discrawl subscribe https://github.com/openclaw/discord-backup.git
discrawl search "launch checklist"
discrawl messages --channel general --hours 24
```

`init` discovers accessible guilds and writes `~/.discrawl/config.toml`. If exactly one guild is available, that guild becomes the default automatically.
`subscribe` writes a token-free config, imports the private Git snapshot, and read commands auto-refresh when the local snapshot is older than `15m`.

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
discrawl sync --guild 123456789012345678 --skip-members --latest-only
discrawl sync --channels 111,222 --since 2026-03-01T00:00:00Z
```

`sync` already uses parallel channel workers. `--concurrency` overrides the default, and the default is auto-sized from `GOMAXPROCS` with a floor of `8` and a cap of `32`.
`--all` ignores `default_guild_id` and fans out across every discovered guild the bot can access.
`--skip-members` refreshes guild/channel/message data without crawling the full member list, which is useful for frequent Git snapshot publishers that only need latest messages.
`--latest-only` skips message bootstrapping for channels without a stored latest cursor, so Git-backed publisher jobs only fill deltas on already-archived channels.
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

Searches archived messages. FTS is the default mode and works without embeddings.

```bash
discrawl search "panic: nil pointer"
discrawl search --mode fts "panic: nil pointer"
discrawl search --mode semantic "missing launch checklist"
discrawl search --mode hybrid "database timeout"
discrawl search --guild 123456789012345678 "payment failed"
discrawl search --channel billing --author steipete --limit 50 "invoice"
discrawl search --include-empty "GitHub"
discrawl --json search "websocket closed"
```

By default, `search` skips rows with no searchable content. Attachment text, attachment filenames, embeds, and replies still count as content. Use `--include-empty` to opt back in.

Modes:

- `fts` searches the local FTS index and returns the newest matching messages first.
- `semantic` embeds the query, searches locally stored message vectors, and returns a clear error if embeddings are disabled or no compatible vectors exist.
- `hybrid` runs FTS and semantic search, deduplicates by message id, and falls back to FTS when semantic search is unavailable.

Semantic and hybrid search require `[search.embeddings]` plus local `message_embeddings` rows for the configured provider, model, and input version. Run `discrawl sync --with-embeddings` to enqueue changed messages, then `discrawl embed` to generate vectors. The input version is currently `message_normalized_v1`, so vectors are tied to normalized message text rather than raw Discord payloads.

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

### Git-backed sharing

`discrawl` can publish the SQLite archive as sharded, compressed NDJSON snapshots in a private Git repo, then auto-import that repo before local read commands.

Publisher:

```bash
discrawl publish --remote https://github.com/openclaw/discord-backup.git --push
discrawl publish --readme path/to/discord-backup/README.md --push
```

Subscriber:

```bash
discrawl subscribe https://github.com/openclaw/discord-backup.git
discrawl search "launch checklist"
discrawl messages --channel general --hours 24
```

`subscribe` is the Git-only setup path. It writes a config with `discord.token_source = "none"`, imports the snapshot, and does not require a Discord bot token. `sync` and `tail` remain disabled in this mode because they need live Discord access.

Configure freshness:

```bash
discrawl subscribe --stale-after 15m https://github.com/openclaw/discord-backup.git
discrawl subscribe --no-auto-update https://github.com/openclaw/discord-backup.git
```

Once `share.remote` is configured, read commands auto-fetch and import when the local share import is older than `share.stale_after` (default `15m`). `discrawl update` forces the same pull/import step manually.

Hybrid mode is supported too: keep normal Discord credentials configured and set `share.remote`. `discrawl sync` and `discrawl messages --sync` import the Git snapshot first, then use live Discord only to fill anything newer or missing. This keeps day-to-day sync fast while preserving live repair behavior.

Git snapshots publish archive tables by default. Embedding queue state stays local to each machine, and Git-only readers can use FTS immediately without an embedding provider.

Generated vectors can be backed up explicitly:

```bash
discrawl publish --with-embeddings --push
discrawl subscribe --with-embeddings https://github.com/openclaw/discord-backup.git
discrawl update --with-embeddings
```

`--with-embeddings` exports stored `message_embeddings` rows for the configured `[search.embeddings]` provider/model plus the current input version. The snapshot stores those vectors under `embeddings/<provider>/<model>/<input_version>/...` and records that identity in `manifest.json`. Import only restores matching embedding manifests, so an Ollama/nomic subscriber does not accidentally import OpenAI/text-embedding vectors into semantic search. `embedding_jobs` is never exported; subscribers that want fresh local vectors can run `discrawl embed --rebuild` to create their own queue and vectors.

The Docker smoke test installs `discrawl` in a clean Go container, subscribes to a Git snapshot repo, then checks `search`, `messages`, `sql`, and `report`:

```bash
DISCRAWL_DOCKER_TEST=1 go test ./internal/cli -run TestDockerGitSourceSmoke -count=1
```

### `report`

Generates the Markdown activity block used by the shared backup repo README.

```bash
discrawl report
discrawl report --readme path/to/discord-backup/README.md
```

Every scheduled snapshot publish updates deterministic README stats: latest update time, latest archived message, archive totals, and day/week/month activity.

The backup README field notes are intentionally a separate daily workflow, not part of `discrawl report`, so model latency or quota cannot block the 15-minute data publish path. `.github/workflows/discord-backup-report.yml` installs `openclaw@latest`, runs `openclaw agent --local` with OpenAI, and inserts a separate `discrawl-field-notes` block with:

- what people seem to love
- what people complain about
- complaint topics correlated with recent GitHub issue and PR clusters
- the likely best PR to watch

Configure `OPENAI_API_KEY` in the discrawl repo secrets to enable agent-written field notes. `DISCORD_BACKUP_TOKEN` still needs write access to `openclaw/discord-backup`. If the GitHub repo used for issue/PR correlation is private, also set `DISCORD_FIELD_NOTES_GITHUB_TOKEN` with read access to that repo.

The backup workflows restore and save `.discrawl-ci/discrawl.db` with `actions/cache`. On a warm runner cache, `discrawl update` compares the cached DB's last imported snapshot timestamp with `manifest.json` and skips the full sharded import when they match. Cache misses and newer backup manifests still take the normal pull/import path.

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
token_source = "openclaw" # use "none" for Git-only read access
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

[share]
remote = ""
repo_path = "~/.discrawl/share"
branch = "main"
auto_update = true
stale_after = "15m"
```

The value above is an example. `init` writes an auto-sized default based on the host: `min(32, max(8, GOMAXPROCS*2))`.

Config override rules:

- `--config` beats everything
- `DISCRAWL_CONFIG` overrides the default config path
- `discord.token_source = "env"` forces env-only token lookup
- `DISCRAWL_NO_AUTO_UPDATE=1` disables Git snapshot auto-update for read commands in one process, useful for report jobs that already imported a fresh backup.

## Embeddings

Embeddings are optional. FTS is the default search path and the primary verification target.

If enabled, embeddings are intended to enrich recall in background batches, not block the hot sync path.

```bash
export OPENAI_API_KEY="..."
discrawl init --with-embeddings
discrawl sync --with-embeddings
discrawl embed --limit 1000
discrawl search --mode semantic "launch checklist"
discrawl search --mode hybrid "launch checklist"
```

Embedding creation has two phases:

1. `sync --with-embeddings` queues changed messages by writing `embedding_jobs` rows. New messages, changed normalized text, and messages that do not already have a job are queued. This phase does not call the embedding provider.
2. `discrawl embed` drains pending jobs in bounded batches, calls the configured provider, and writes vectors to `message_embeddings` with provider, model, input version, dimensions, and binary vector data.

During drain, `discrawl` claims jobs with a short lock so overlapping runs do not process the same batch. Rate limits requeue the batch and stop that drain run cleanly. Provider or validation failures retry up to three attempts before the job is marked failed. Messages with no normalized text are marked done and any stale vector for that message is removed.

The provider/model/input-version identity is stored on each job and vector. If you change provider or model, pending jobs are retargeted to the new identity and prior attempts are reset. Existing vectors for another identity remain in SQLite, but semantic search only reads vectors compatible with the current config.

Use `--rebuild` when changing provider, model, or input settings and you want to regenerate vectors for the existing archive:

```bash
discrawl embed --rebuild --limit 1000
```

Local providers can keep message and query embedding on the same machine:

```toml
[search.embeddings]
enabled = true
provider = "ollama"
model = "nomic-embed-text"
```

With remote providers, message text is sent during `discrawl embed`, and search query text is sent when using `--mode semantic` or `--mode hybrid`. Stored message text is not sent during local vector scoring.

## Data Stored Locally

- guild metadata
- channels and threads in one table
- current member snapshot
- canonical message rows
- append-only message event records
- FTS index rows
- optional local embedding queue metadata and vectors

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
