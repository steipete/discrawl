# `sql`

Runs read-only SQL against the local database.

## Usage

```bash
discrawl sql 'select count(*) as messages from messages'
echo 'select guild_id, count(*) from messages group by guild_id' | discrawl sql -
```

`-` reads SQL from stdin.

## Notes

- read-only - writes are blocked at the connection level
- `--unsafe --confirm` opens the escape hatch for deliberate write/admin SQL
- the schema is multi-guild ready; threads are stored as channels because that matches the Discord model
- proven DMs use the synthetic guild id `@me`
- SQLite schema migrations are versioned with `PRAGMA user_version`; startup fails fast when a local DB schema is newer than the supported binary

## See also

- [Data layout](../guides/data-storage.html) - what tables exist
- [`status`](status.html) - high-level archive numbers without raw SQL
