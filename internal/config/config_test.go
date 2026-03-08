package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeFillsDefaults(t *testing.T) {
	t.Parallel()

	cfg := Config{}
	require.NoError(t, cfg.Normalize())
	require.Equal(t, 1, cfg.Version)
	require.Equal(t, "openclaw", cfg.Discord.TokenSource)
	require.Equal(t, "default", cfg.Discord.Account)
	require.Equal(t, DefaultTokenEnv, cfg.Discord.TokenEnv)
	require.Equal(t, defaultSyncConcurrency(), cfg.Sync.Concurrency)
	require.GreaterOrEqual(t, cfg.Sync.Concurrency, 8)
	require.LessOrEqual(t, cfg.Sync.Concurrency, 32)
	require.NotNil(t, cfg.Sync.AttachmentText)
	require.True(t, *cfg.Sync.AttachmentText)
	require.Equal(t, "fts", cfg.Search.DefaultMode)
}

func TestResolveDiscordTokenPrefersOpenClaw(t *testing.T) {
	dir := t.TempDir()
	openClawPath := filepath.Join(dir, "openclaw.json")
	require.NoError(t, os.WriteFile(openClawPath, []byte(`{
		"channels": {
			"discord": {
				"token": "Bot config-token",
				"guilds": { "g1": {}, "g2": {} }
			}
		}
	}`), 0o600))
	t.Setenv(DefaultTokenEnv, "env-token")

	cfg := Default()
	cfg.Discord.OpenClawConfig = openClawPath

	token, err := ResolveDiscordToken(cfg)
	require.NoError(t, err)
	require.Equal(t, "config-token", token.Token)
	require.Equal(t, "openclaw", token.Source)
}

func TestResolveDiscordTokenFallsBackToEnv(t *testing.T) {
	cfg := Default()
	cfg.Discord.TokenSource = "env"
	cfg.Discord.OpenClawConfig = filepath.Join(t.TempDir(), "missing.json")
	t.Setenv(DefaultTokenEnv, "Bot env-token")

	token, err := ResolveDiscordToken(cfg)
	require.NoError(t, err)
	require.Equal(t, "env-token", token.Token)
	require.Equal(t, "env", token.Source)
}

func TestLoadOpenClawDiscordFromAccount(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	openClawPath := filepath.Join(dir, "openclaw.json")
	require.NoError(t, os.WriteFile(openClawPath, []byte(`{
		"channels": {
			"discord": {
				"accounts": {
					"default": {
						"token": "acct-token",
						"guilds": { "g3": {} }
					}
				}
			}
		}
	}`), 0o600))

	info, err := LoadOpenClawDiscord(openClawPath, "default")
	require.NoError(t, err)
	require.Equal(t, "acct-token", info.Token)
	require.Equal(t, []string{"g3"}, info.GuildIDs)
}

func TestWriteAndLoadRoundTrip(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.toml")
	cfg := Default()
	cfg.DefaultGuildID = "g1"
	cfg.GuildIDs = []string{"g1", "g2"}
	require.NoError(t, Write(path, cfg))

	loaded, err := Load(path)
	require.NoError(t, err)
	require.Equal(t, "g1", loaded.EffectiveDefaultGuildID())
	require.Equal(t, []string{"g1", "g2"}, loaded.GuildIDs)
	require.NotNil(t, loaded.Sync.AttachmentText)
	require.True(t, *loaded.Sync.AttachmentText)
}

func TestAttachmentTextExplicitFalseSurvivesNormalize(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Sync.AttachmentText = boolPtr(false)
	require.NoError(t, cfg.Normalize())
	require.False(t, cfg.AttachmentTextEnabled())
}

func TestExpandPath(t *testing.T) {
	t.Parallel()

	path, err := ExpandPath("~/discrawl-test")
	require.NoError(t, err)
	require.Contains(t, path, "discrawl-test")
}

func TestOpenClawCandidatesIncludesBackups(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	base := filepath.Join(dir, "openclaw.json")
	require.NoError(t, os.WriteFile(base, []byte(`{}`), 0o600))
	require.NoError(t, os.WriteFile(base+".bak", []byte(`{}`), 0o600))
	require.NoError(t, os.WriteFile(base+".bak.1", []byte(`{}`), 0o600))

	paths, err := openClawCandidates(base)
	require.NoError(t, err)
	require.Len(t, paths, 3)
}

func TestEffectiveDefaultGuildAndDirs(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.GuildIDs = []string{"g1"}
	require.Equal(t, "g1", cfg.EffectiveDefaultGuildID())
	require.Nil(t, cfg.SearchGuildDefaults())

	cfg.CacheDir = filepath.Join(t.TempDir(), "cache")
	cfg.LogDir = filepath.Join(t.TempDir(), "logs")
	cfg.DBPath = filepath.Join(t.TempDir(), "db", "discrawl.db")
	require.NoError(t, EnsureRuntimeDirs(cfg))
}

func TestResolvePathUsesEnv(t *testing.T) {
	t.Setenv(DefaultConfigEnv, "/tmp/custom.toml")
	require.Equal(t, "/tmp/custom.toml", ResolvePath(""))
	require.Equal(t, "/tmp/flag.toml", ResolvePath("/tmp/flag.toml"))
}

func TestConfigErrorsAndBackupFallback(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(DefaultTokenEnv, "")

	_, err := ExpandPath("")
	require.Error(t, err)

	bad := filepath.Join(dir, "bad.toml")
	require.NoError(t, os.WriteFile(bad, []byte("not = [toml"), 0o600))
	_, err = Load(bad)
	require.Error(t, err)

	cfg := Default()
	cfg.Discord.OpenClawConfig = filepath.Join(dir, "missing.json")
	_, err = ResolveDiscordToken(cfg)
	require.Error(t, err)

	base := filepath.Join(dir, "openclaw.json")
	backup := base + ".bak"
	require.NoError(t, os.WriteFile(base, []byte(`{}`), 0o600))
	require.NoError(t, os.WriteFile(backup, []byte(`{"channels":{"discord":{"token":"backup-token"}}}`), 0o600))
	info, err := LoadOpenClawDiscord(base, "default")
	require.NoError(t, err)
	require.Equal(t, "backup-token", info.Token)
}
