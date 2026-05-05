# Discrawl

Mirror Discord guilds into local SQLite. Search server history without depending on Discord search. Bring a bot token, or read everything offline from a Git snapshot.

## What it does

- discovers every guild a bot can access and syncs channels, threads, members, and message history into SQLite
- maintains FTS5 indexes for fast literal search; optional embeddings for semantic and hybrid recall
- imports classifiable Discord Desktop cache messages with `wiretap`, including proven DMs under `@me`
- tails the Gateway for live updates with periodic repair sweeps
- publishes the archive as sharded NDJSON snapshots in a private Git repo so readers can search offline with no Discord credentials
- exposes read-only SQL, channel/member directories, mention queries, digests, and trend analytics

## Pick your path

- **New here?** Read [Install](install.html) and run `discrawl init`.
- **Already have a bot?** Jump to [`sync`](commands/sync.html) and [`search`](commands/search.html).
- **Just want to read a shared archive?** Use [`subscribe`](commands/subscribe.html) - no token needed.
- **Need DM search?** [`wiretap`](commands/wiretap.html) imports local Discord Desktop cache.
- **Want semantic search?** Configure [Embeddings](guides/embeddings.html), then run [`embed`](commands/embed.html).

## At a glance

```bash
export DISCORD_BOT_TOKEN="..."
discrawl init
discrawl doctor
discrawl sync --full
discrawl search "panic: nil pointer"
discrawl tail
```

## Sections

- **[Start](install.html)** - install, configure, set up the Discord bot, security notes, contact
- **[Guides](guides/)** - sync sources, wiretap internals, search modes, embeddings, Git snapshots, data layout
- **[Commands](commands/)** - one page per CLI command

## Where to file issues

`https://github.com/openclaw/discrawl/issues`. Contact: [steipete@gmail.com](contact.html).
