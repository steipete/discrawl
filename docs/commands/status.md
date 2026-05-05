# `status`

Shows local archive status.

## Usage

```bash
discrawl status
```

## Reports

- where the local database lives
- guild count and per-guild totals
- channel and thread counts
- message totals
- latest archived message time
- whether the Git share is configured and how stale the local import is
- embeddings status if `[search.embeddings]` is enabled

## See also

- [`doctor`](doctor.html) - liveness check (config, auth, DB, FTS wiring)
- [`report`](report.html) - Markdown activity block for the shared backup README
