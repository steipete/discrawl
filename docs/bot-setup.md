# Discord bot setup

Discrawl needs a real Discord bot token to run `sync` or `tail`. Not a user token. The desktop `wiretap` import does not need any token.

## Minimum setup

1. Create or reuse a Discord application in the [Discord developer portal](https://discord.com/developers/applications).
2. Add a bot user to that application.
3. Invite the bot to the target guilds.
4. Enable these intents for the bot:
   - **Server Members Intent**
   - **Message Content Intent**
5. Ensure the bot can at least:
   - view channels
   - read message history

Without those intents/permissions, `sync`, `tail`, member snapshots, and message content archiving will be partial or fail outright.

## Provide the token

### Environment variable

```bash
export DISCORD_BOT_TOKEN="your-bot-token"
discrawl doctor
```

If you keep shell secrets in `~/.profile`, add the export there and reload your shell.

### OS keyring

If you prefer the OS keyring, keep the token out of config and store it in the default keyring item:

```bash
# macOS Keychain
security add-generic-password -U -s discrawl -a discord_bot_token -w "$DISCORD_BOT_TOKEN"

# Linux Secret Service / libsecret
printf %s "$DISCORD_BOT_TOKEN" | secret-tool store --label="discrawl Discord bot token" service discrawl username discord_bot_token

# Windows Credential Manager
cmdkey /generic:discrawl:discord_bot_token /user:discord_bot_token /pass:%DISCORD_BOT_TOKEN%
```

Set `discord.token_source = "keyring"` if you want to require the keyring and skip env entirely.

## Verify

```bash
discrawl doctor
```

`doctor` reports the token source (env or keyring), confirms bot auth, lists how many guilds the bot can access, and verifies the local DB plus FTS wiring. It does not print the token contents.

## Wiretap-only setup

If you only want to import local Discord Desktop cache messages and not run a bot, skip everything above and run:

```bash
discrawl sync --source wiretap
```

Or `discrawl wiretap` directly. See the [wiretap guide](guides/wiretap.html).
