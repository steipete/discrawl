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
	require.Equal(t, "main", cfg.Share.Branch)
	require.Equal(t, "15m", cfg.Share.StaleAfter)
	require.True(t, Default().Share.AutoUpdate)
	require.False(t, cfg.ShareEnabled())
	cfg.Share.Remote = "git@example.com:org/archive.git"
	require.True(t, cfg.ShareEnabled())
	require.Equal(t, "openai", cfg.Search.Embeddings.Provider)
	require.Equal(t, "text-embedding-3-small", cfg.Search.Embeddings.Model)
	require.Empty(t, cfg.Search.Embeddings.BaseURL)
	require.Equal(t, "OPENAI_API_KEY", cfg.Search.Embeddings.APIKeyEnv)
	require.Equal(t, 64, cfg.Search.Embeddings.BatchSize)
	require.Equal(t, 12000, cfg.Search.Embeddings.MaxInputChars)
	require.Equal(t, "2m", cfg.Search.Embeddings.RequestTimeout)
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

func TestResolveDiscordTokenDisabled(t *testing.T) {
	cfg := Default()
	cfg.Discord.TokenSource = "none"
	t.Setenv(DefaultTokenEnv, "env-token")

	_, err := ResolveDiscordToken(cfg)
	require.ErrorContains(t, err, "discord token disabled")
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

func TestLoadOpenClawDiscordExpandsEnvToken(t *testing.T) {
	dir := t.TempDir()
	openClawPath := filepath.Join(dir, "openclaw.json")
	t.Setenv("DISCRAWL_TEST_TOKEN", "Bot env-expanded-token")
	require.NoError(t, os.WriteFile(openClawPath, []byte(`{
		"channels": {
			"discord": {
				"accounts": {
					"default": {
						"token": "${DISCRAWL_TEST_TOKEN}",
						"guilds": { "g3": {} }
					}
				}
			}
		}
	}`), 0o600))

	info, err := LoadOpenClawDiscord(openClawPath, "default")
	require.NoError(t, err)
	require.Equal(t, "env-expanded-token", info.Token)
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

func TestNormalizeEmbeddingProviderDefaults(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Search.Embeddings.Provider = "OLLAMA"
	require.NoError(t, cfg.Normalize())
	require.Equal(t, "ollama", cfg.Search.Embeddings.Provider)
	require.Equal(t, "text-embedding-3-small", cfg.Search.Embeddings.Model)
	require.Empty(t, cfg.Search.Embeddings.APIKeyEnv)
	require.Empty(t, cfg.Search.Embeddings.BaseURL)
	require.Equal(t, "2m", cfg.Search.Embeddings.RequestTimeout)

	cfg = Config{}
	cfg.Search.Embeddings.Provider = "llamacpp"
	require.NoError(t, cfg.Normalize())
	require.Equal(t, "nomic-embed-text", cfg.Search.Embeddings.Model)
	require.Empty(t, cfg.Search.Embeddings.APIKeyEnv)

	cfg = Config{}
	cfg.Search.Embeddings.Provider = "openai_compatible"
	cfg.Search.Embeddings.BaseURL = " http://127.0.0.1:9999/v1/ "
	require.NoError(t, cfg.Normalize())
	require.Equal(t, "openai_compatible", cfg.Search.Embeddings.Provider)
	require.Equal(t, "http://127.0.0.1:9999/v1", cfg.Search.Embeddings.BaseURL)
	require.Equal(t, "text-embedding-3-small", cfg.Search.Embeddings.Model)
	require.Empty(t, cfg.Search.Embeddings.APIKeyEnv)

	cfg = Config{}
	cfg.Search.Embeddings.Provider = "openai_compatible"
	cfg.Search.Embeddings.APIKeyEnv = "OPENAI_API_KEY"
	require.NoError(t, cfg.Normalize())
	require.Equal(t, "OPENAI_API_KEY", cfg.Search.Embeddings.APIKeyEnv)
}

func TestLoadLegacyEmbeddingConfigDefaults(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(`
db_path = "/tmp/discrawl.db"
cache_dir = "/tmp/discrawl-cache"
log_dir = "/tmp/discrawl-logs"

[search.embeddings]
enabled = true
provider = "openai"
model = "text-embedding-3-small"
api_key_env = "OPENAI_API_KEY"
batch_size = 64
`), 0o600))

	cfg, err := Load(path)
	require.NoError(t, err)
	require.True(t, cfg.Search.Embeddings.Enabled)
	require.Equal(t, "openai", cfg.Search.Embeddings.Provider)
	require.Empty(t, cfg.Search.Embeddings.BaseURL)
	require.Equal(t, 12000, cfg.Search.Embeddings.MaxInputChars)
	require.Equal(t, "2m", cfg.Search.Embeddings.RequestTimeout)
}

func TestNormalizeRejectsInvalidEmbeddingTimeout(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Search.Embeddings.RequestTimeout = "0s"
	require.ErrorContains(t, cfg.Normalize(), "must be positive")

	cfg = Default()
	cfg.Search.Embeddings.RequestTimeout = "soon"
	require.ErrorContains(t, cfg.Normalize(), "parse search.embeddings.request_timeout")
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
