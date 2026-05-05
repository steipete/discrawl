# Embeddings

Embeddings are optional. FTS is the default search path and the primary verification target. Embeddings enrich recall in background batches; they do not block the hot sync path.

## Quick path

```bash
export OPENAI_API_KEY="..."
discrawl init --with-embeddings
discrawl sync --with-embeddings
discrawl embed --limit 1000
discrawl search --mode semantic "launch checklist"
discrawl search --mode hybrid "launch checklist"
```

## Two-phase pipeline

1. **Queue** - `sync --with-embeddings` writes `embedding_jobs` rows for new messages, changed normalized text, and messages without an existing job. The embedding provider is **not** called in this phase.
2. **Drain** - `discrawl embed` claims pending jobs with a short lock so overlapping runs do not process the same batch. It calls the configured provider, writes vectors to `message_embeddings` with provider, model, input version, dimensions, and binary vector data.

Behavior during drain:

- rate limits requeue the batch and stop that drain run cleanly
- provider or validation failures retry up to three attempts before marking the job failed
- messages with no normalized text are marked done and any stale vector for that message is removed

## Identity (provider, model, input version)

Stored on each job and vector. If you change provider or model:

- pending jobs are retargeted to the new identity
- prior attempts are reset
- existing vectors for another identity remain in SQLite but are not used for semantic search

Use `--rebuild` when you want to regenerate vectors for the existing archive after a config change:

```bash
discrawl embed --rebuild --limit 1000
```

## Local provider example

```toml
[search.embeddings]
enabled = true
provider = "ollama"
model = "nomic-embed-text"
```

With local providers, message and query embedding both happen on the same machine. With remote providers, message text is sent during `discrawl embed`, and search query text is sent during `--mode semantic` or `--mode hybrid` calls.

## Git snapshot interaction

By default, `publish` does not export embeddings. Use `--with-embeddings`:

```bash
discrawl publish --with-embeddings --push
discrawl subscribe --with-embeddings https://github.com/example/discord-archive.git
discrawl update --with-embeddings
```

The snapshot stores vectors under `embeddings/<provider>/<model>/<input_version>/...` and records that identity in `manifest.json`. Only vectors for non-DM messages are exported. Import only restores matching embedding manifests, so an Ollama/nomic subscriber does not accidentally import OpenAI/text-embedding vectors. `embedding_jobs` is never exported; subscribers that want fresh local vectors run `discrawl embed --rebuild`. Publishing without `--with-embeddings` omits embedding manifests instead of carrying forward an older bundle.

## See also

- [Search modes](search-modes.html)
- [`embed`](../commands/embed.html)
- [Configuration](../configuration.html)
