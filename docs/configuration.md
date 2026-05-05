# Configuration

`discrawl init` writes a complete config so most users do not hand-edit anything initially. This page documents the full shape and override rules for when you do.

## File layout

```toml
version = 1
default_guild_id = ""
guild_ids = []
db_path = "~/.discrawl/discrawl.db"
cache_dir = "~/.discrawl/cache"
log_dir = "~/.discrawl/logs"

[discord]
token_source = "env" # use "none" for Git-only read access
token_env = "DISCORD_BOT_TOKEN"
token_keyring_service = "discrawl"
token_keyring_account = "discord_bot_token"

[sync]
source = "both" # "discord" for bot-only sync, "wiretap" for desktop-cache-only import
concurrency = 16
repair_every = "6h"
full_history = true
attachment_text = true

[desktop]
path = "~/.config/discord" # macOS default: "~/Library/Application Support/discord"
max_file_bytes = 67108864
full_cache = false

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

`concurrency` is auto-sized at `init` to `min(32, max(8, GOMAXPROCS*2))`.

## Token resolution

In order:

1. `DISCORD_BOT_TOKEN`, or the env var named in `discord.token_env`
2. OS keyring item `discrawl` / `discord_bot_token`, or the configured keyring service/account

`discrawl` accepts either raw token text or a value prefixed with `Bot `. Normalization is automatic.

Set `discord.token_source = "keyring"` if you want to require keyring lookup and skip env entirely. Set it to `"none"` for a Git-only reader.

## Override rules

- `--config <path>` beats everything
- `DISCRAWL_CONFIG=<path>` overrides the default config path
- `discord.token_source = "none"` disables live Discord access for Git-only readers
- `discord.token_source = "keyring"` skips env lookup
- `DISCRAWL_NO_AUTO_UPDATE=1` disables Git snapshot auto-update for read commands in one process

## Notes

- `default_guild_id` is the implicit scope for `sync`, `tail`, `digest`, and `analytics` when `--guild` is not passed
- `guild_ids` is reserved for explicit multi-guild fan-out; usually you do not set this directly
- changing `[search.embeddings]` provider/model/input version retargets pending jobs and resets prior attempts; existing vectors for another identity remain in SQLite but are not used for semantic search
- changing `db_path` does not migrate existing data; copy the file yourself if you want to keep history
