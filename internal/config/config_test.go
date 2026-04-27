package config

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeFillsDefaults(t *testing.T) {
	t.Parallel()

	cfg := Config{}
	require.NoError(t, cfg.Normalize())
	require.Equal(t, 1, cfg.Version)
	require.Equal(t, "env", cfg.Discord.TokenSource)
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

func TestDefaultSyncConcurrencyBounds(t *testing.T) {
	old := runtime.GOMAXPROCS(0)
	t.Cleanup(func() { runtime.GOMAXPROCS(old) })

	runtime.GOMAXPROCS(1)
	require.Equal(t, 8, defaultSyncConcurrency())

	runtime.GOMAXPROCS(8)
	require.Equal(t, 16, defaultSyncConcurrency())

	runtime.GOMAXPROCS(100)
	require.Equal(t, 32, defaultSyncConcurrency())
}

func TestResolveDiscordTokenFromEnv(t *testing.T) {
	cfg := Default()
	t.Setenv(DefaultTokenEnv, "Bot env-token")

	token, err := ResolveDiscordToken(cfg)
	require.NoError(t, err)
	require.Equal(t, "env-token", token.Token)
	require.Equal(t, "env", token.Source)
}

func TestResolveDiscordTokenFromCustomEnv(t *testing.T) {
	cfg := Default()
	cfg.Discord.TokenEnv = "DISCRAWL_TEST_DISCORD_TOKEN"
	t.Setenv("DISCRAWL_TEST_DISCORD_TOKEN", "custom-env-token")

	token, err := ResolveDiscordToken(cfg)
	require.NoError(t, err)
	require.Equal(t, "custom-env-token", token.Token)
	require.Equal(t, "DISCRAWL_TEST_DISCORD_TOKEN", token.Path)
}

func TestResolveDiscordTokenRequiresEnvValue(t *testing.T) {
	cfg := Default()
	t.Setenv(DefaultTokenEnv, "")

	_, err := ResolveDiscordToken(cfg)
	require.ErrorContains(t, err, `discord token not found in environment variable "DISCORD_BOT_TOKEN"`)
}

func TestResolveDiscordTokenDisabled(t *testing.T) {
	cfg := Default()
	cfg.Discord.TokenSource = "none"
	t.Setenv(DefaultTokenEnv, "env-token")

	_, err := ResolveDiscordToken(cfg)
	require.ErrorContains(t, err, "discord token disabled")
}

func TestResolveDiscordTokenRejectsUnsupportedSource(t *testing.T) {
	cfg := Default()
	cfg.Discord.TokenSource = "legacy"

	_, err := ResolveDiscordToken(cfg)
	require.ErrorContains(t, err, `unsupported discord token_source "legacy"`)
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

func TestWriteRejectsNonPositiveEmbeddingTimeout(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Search.Embeddings.RequestTimeout = "0s"
	err := Write(filepath.Join(t.TempDir(), "config.toml"), cfg)
	require.ErrorContains(t, err, "must be positive")

	cfg.Search.Embeddings.RequestTimeout = "not-a-duration"
	err = cfg.Normalize()
	require.ErrorContains(t, err, "parse search.embeddings.request_timeout")
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
	cfg.Sync.AttachmentText = new(false)
	require.NoError(t, cfg.Normalize())
	require.False(t, cfg.AttachmentTextEnabled())
}

func TestExpandPath(t *testing.T) {
	t.Parallel()

	path, err := ExpandPath("~/discrawl-test")
	require.NoError(t, err)
	require.Contains(t, path, "discrawl-test")
}

func TestResolvePath(t *testing.T) {
	dir := t.TempDir()
	envPath := filepath.Join(dir, "env.toml")
	t.Setenv(DefaultConfigEnv, envPath)
	require.Equal(t, "flag.toml", ResolvePath("flag.toml"))
	require.Equal(t, envPath, ResolvePath(""))
	t.Setenv(DefaultConfigEnv, "")
	require.Contains(t, ResolvePath(""), filepath.Join(".discrawl", "config.toml"))
	_, err := ExpandPath("")
	require.ErrorContains(t, err, "empty path")
}

func TestEffectiveDefaultGuildAndDirs(t *testing.T) {
	t.Parallel()

	require.Equal(t, "explicit", Config{DefaultGuildID: "explicit", GuildIDs: []string{"g1"}}.EffectiveDefaultGuildID())
	require.Empty(t, Config{GuildIDs: []string{"g1", "g2"}}.EffectiveDefaultGuildID())
	require.Equal(t, []string{"a", "b"}, uniqueStrings([]string{" a ", "", "b", "a"}))
	require.Equal(t, "token", NormalizeBotToken(" token "))
	require.Nil(t, uniqueStrings(nil))

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
	_, err = ResolveDiscordToken(cfg)
	require.Error(t, err)
}
