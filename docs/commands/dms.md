# `dms`

Lists local wiretap DM conversations or reads one DM thread. Convenience layer over the synthetic `@me` guild id.

## Usage

```bash
discrawl dms
discrawl dms --with Molty --last 20
discrawl dms --with 1456464433768300635 --all
discrawl dms --search "launch checklist"
discrawl dms --with Molty --search "invoice"
```

## Default output

`discrawl dms` (no flags) shows one row per local DM channel with:

- message count
- author count
- first/last cached message times

## Flags

- `--with <name|id>` - switches to message output for that DM conversation (unless `--list` is also set)
- `--list` - keep the channel-summary listing even when `--with` is set
- `--search <query>` - search only local DM messages
- `--last <n>` / `--all` / `--limit <n>` - same slicing as [`messages`](messages.html)

## Notes

- only sees data imported by [`wiretap`](wiretap.html) - Discord Desktop cache, not live DM history
- skips Git snapshot auto-update because DMs are never imported from the shared mirror
- DMs are local-only and never published

## See also

- [Wiretap guide](../guides/wiretap.html)
- [`messages --dm`](messages.html)
