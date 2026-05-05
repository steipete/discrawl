# `publish`

Publishes the local SQLite archive as sharded, compressed NDJSON snapshots in a private Git repo.

## Usage

```bash
discrawl publish --remote https://github.com/example/discord-archive.git --push
discrawl publish --readme path/to/discord-backup/README.md --push
discrawl publish --message "sync: discord archive" --push
discrawl publish --with-embeddings --push
```

## Flags

- `--repo <path>` - local snapshot repo path (defaults to `[share].repo_path`)
- `--remote <url>` - target Git remote (defaults to `[share].remote`)
- `--branch <name>` - snapshot branch (defaults to `[share].branch`)
- `--message <text>` - commit message (default: `sync: discord archive`)
- `--no-commit` - write/export files without creating a Git commit
- `--push` - push the snapshot commit after writing it
- `--readme <path>` - update the activity block in this README file too
- `--with-embeddings` - also export stored `message_embeddings` rows

## What is published

- non-DM archive tables (DM `@me` rows are always excluded)
- README activity block (latest update, latest message, totals, day/week/month activity)
- with `--with-embeddings`: vectors for the configured `[search.embeddings]` provider/model/input version, plus identity manifest

## What is not published

- `@me` DM guilds, channels, messages, events, attachments, mentions, wiretap sync state
- `embedding_jobs`
- raw bot tokens or any local secret

## See also

- [Git snapshots guide](../guides/git-snapshots.html)
- [`subscribe`](subscribe.html)
- [`update`](update.html)
- [`report`](report.html)
