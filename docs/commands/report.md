# `report`

Generates the Markdown activity block used by the shared backup repo README.

## Usage

```bash
discrawl report
discrawl report --readme path/to/discord-backup/README.md
```

## Flags

- `--readme <path>` - update the activity block in the given README file in place

## What gets rendered

Deterministic README stats:

- latest update time
- latest archived message
- archive totals
- day / week / month activity

Every scheduled snapshot publish updates this block.

## CI integration

The backup workflows restore and save `.discrawl-ci/discrawl.db` with `actions/cache`. On a warm runner cache, scheduled publishers skip the pre-sync snapshot import and go straight to the live latest-message delta before publishing. Cache misses still import the latest published snapshot first so `--latest-only` has channel cursors to resume from.

## See also

- [`publish`](publish.html)
- [Git snapshots](../guides/git-snapshots.html)
- [`status`](status.html)
