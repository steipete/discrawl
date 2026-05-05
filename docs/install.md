# Install

Discrawl is a single Go binary. Install via Homebrew or build from source.

## Homebrew

```bash
brew install steipete/tap/discrawl
discrawl --version
```

The tap auto-installs from `steipete/tap`.

## From source

Requires Go `1.26+`.

```bash
git clone https://github.com/openclaw/discrawl.git
cd discrawl
go build -o bin/discrawl ./cmd/discrawl
./bin/discrawl --version
```

If you do not put `discrawl` on `PATH`, replace `discrawl` with `./bin/discrawl` in any example below.

## Quick start (with bot token)

```bash
export DISCORD_BOT_TOKEN="your-bot-token"
discrawl init
discrawl doctor
discrawl sync --full
discrawl sync
discrawl search "panic: nil pointer"
discrawl tail
```

`init` discovers accessible guilds and writes `~/.discrawl/config.toml`. If exactly one guild is available, it becomes the default automatically.

`doctor` verifies the config loads, the token resolves, the bot can reach the Gateway, and the local DB and FTS index are wired up.

## Quick start (Git-only reader)

No Discord credentials required. You read a private Git snapshot another machine published.

```bash
discrawl subscribe https://github.com/example/discord-archive.git
discrawl search "launch checklist"
discrawl messages --channel general --hours 24
```

`subscribe` writes a token-free config (`discord.token_source = "none"`) and imports the snapshot. Read commands auto-refresh when the local snapshot is older than `15m`.

## Default runtime paths

- config: `~/.discrawl/config.toml`
- database: `~/.discrawl/discrawl.db`
- cache: `~/.discrawl/cache/`
- logs: `~/.discrawl/logs/`

## Next steps

- [Bot setup](bot-setup.html) - intents, permissions, token sources
- [Configuration](configuration.html) - the full TOML shape and override rules
- [`sync`](commands/sync.html) - the main archive command
