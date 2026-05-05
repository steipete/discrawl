# Git-backed snapshots

Discrawl can publish the SQLite archive as sharded, compressed NDJSON snapshots in a private Git repo, then auto-import that repo before local read commands. This gives readers org memory without Discord credentials.

## Publisher

```bash
discrawl publish --remote https://github.com/example/discord-archive.git --push
discrawl publish --readme path/to/discord-backup/README.md --push
```

The publisher uses your existing bot-synced archive. It exports non-DM tables only.

## Subscriber

```bash
discrawl subscribe https://github.com/example/discord-archive.git
discrawl search "launch checklist"
discrawl messages --channel general --hours 24
```

`subscribe` is the Git-only setup path. It writes a config with `discord.token_source = "none"`, imports the snapshot, and does not require a Discord bot token. `sync` and `tail` remain disabled in this mode because they need live Discord access.

## Auto-update

Once `share.remote` is configured, read commands auto-fetch and import when the local share import is older than `share.stale_after` (default `15m`):

```bash
discrawl subscribe --stale-after 15m https://github.com/example/discord-archive.git
discrawl subscribe --no-auto-update https://github.com/example/discord-archive.git
```

`discrawl update` forces the same pull/import step manually.

`discrawl sync` does **not** auto-import the share unless `--update=auto` or `--update=force` is provided, so routine live refreshes stay fast.

## Hybrid mode

Keep normal Discord credentials configured **and** set `share.remote`:

```bash
discrawl sync --update=auto       # import snapshot first, then live deltas
discrawl messages --sync          # blocking pre-query sync for matched scope
discrawl sync --all-channels      # broader live repair
discrawl sync --full              # historical backfill
```

## What is published

- non-DM archive tables (DM `@me` rows are always excluded)
- README activity block - latest update time, latest archived message, archive totals, day/week/month activity
- `embedding_jobs` is never exported

## Backing up vectors

```bash
discrawl publish --with-embeddings --push
discrawl subscribe --with-embeddings https://github.com/example/discord-archive.git
discrawl update --with-embeddings
```

Stored under `embeddings/<provider>/<model>/<input_version>/...`. Import only restores matching identities; Ollama/nomic subscribers do not accidentally pick up OpenAI/text-embedding vectors. Publishing without `--with-embeddings` omits embedding manifests instead of carrying forward an older bundle.

## CI

The Docker smoke test installs `discrawl` in a clean Go container, subscribes to a Git snapshot repo, then checks `search`, `messages`, `sql`, and `report`:

```bash
DISCRAWL_DOCKER_TEST=1 go test ./internal/cli -run TestDockerGitSourceSmoke -count=1
```

The backup workflows restore and save `.discrawl-ci/discrawl.db` with `actions/cache`. On a warm runner cache, scheduled publishers skip the pre-sync snapshot import and go straight to the live latest-message delta before publishing. Cache misses still import the latest published snapshot first so `--latest-only` has channel cursors to resume from.

## See also

- [`publish`](../commands/publish.html)
- [`subscribe`](../commands/subscribe.html)
- [`update`](../commands/update.html)
- [`report`](../commands/report.html)
