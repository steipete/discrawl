# `channels`

Browse the offline channel directory.

## Usage

```bash
discrawl channels list
discrawl channels show 123456789012345678
```

## Subcommands

- `list` - dump every channel and thread in the local archive
- `show <id>` - show metadata for one channel/thread

## Notes

- threads are stored as channels because that matches the Discord model
- archived threads are part of the sync surface and appear here too

## See also

- [`members`](members.html)
- [Data layout](../guides/data-storage.html)
