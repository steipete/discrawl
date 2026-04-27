package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"
)

const (
	DefaultConfigEnv = "DISCRAWL_CONFIG"
	DefaultTokenEnv  = "DISCORD_BOT_TOKEN"
)

type Config struct {
	Version        int           `toml:"version"`
	GuildID        string        `toml:"guild_id,omitempty"`
	DefaultGuildID string        `toml:"default_guild_id,omitempty"`
	GuildIDs       []string      `toml:"guild_ids,omitempty"`
	DBPath         string        `toml:"db_path"`
	CacheDir       string        `toml:"cache_dir"`
	LogDir         string        `toml:"log_dir"`
	Discord        DiscordConfig `toml:"discord"`
	Desktop        DesktopConfig `toml:"desktop"`
	Sync           SyncConfig    `toml:"sync"`
	Search         SearchConfig  `toml:"search"`
	Share          ShareConfig   `toml:"share"`
}

type DiscordConfig struct {
	TokenSource    string `toml:"token_source"`
	OpenClawConfig string `toml:"openclaw_config"`
	Account        string `toml:"account"`
	TokenEnv       string `toml:"token_env"`
}

type DesktopConfig struct {
	Path         string `toml:"path"`
	MaxFileBytes int64  `toml:"max_file_bytes"`
}

type SyncConfig struct {
	Source         string `toml:"source"`
	Concurrency    int    `toml:"concurrency"`
	RepairEvery    string `toml:"repair_every"`
	FullHistory    bool   `toml:"full_history"`
	AttachmentText *bool  `toml:"attachment_text"`
}

type SearchConfig struct {
	DefaultMode string           `toml:"default_mode"`
	Embeddings  EmbeddingsConfig `toml:"embeddings"`
}

type ShareConfig struct {
	Remote     string `toml:"remote,omitempty"`
	RepoPath   string `toml:"repo_path,omitempty"`
	Branch     string `toml:"branch,omitempty"`
	AutoUpdate bool   `toml:"auto_update"`
	StaleAfter string `toml:"stale_after"`
}

type EmbeddingsConfig struct {
	Enabled        bool   `toml:"enabled"`
	Provider       string `toml:"provider"`
	Model          string `toml:"model"`
	BaseURL        string `toml:"base_url"`
	APIKeyEnv      string `toml:"api_key_env"`
	BatchSize      int    `toml:"batch_size"`
	MaxInputChars  int    `toml:"max_input_chars"`
	RequestTimeout string `toml:"request_timeout"`
}

type TokenResolution struct {
	Token  string
	Source string
	Path   string
}

type OpenClawDiscord struct {
	Token    string
	GuildIDs []string
	Path     string
}

type openClawConfig struct {
	Channels struct {
		Discord openClawDiscord `json:"discord"`
	} `json:"channels"`
	Secrets openClawSecrets `json:"secrets"`
}

type openClawDiscord struct {
	Token    openClawSecretValue            `json:"token"`
	Accounts map[string]openClawDiscordAcct `json:"accounts"`
	Guilds   map[string]json.RawMessage     `json:"guilds"`
}

type openClawDiscordAcct struct {
	Token  openClawSecretValue        `json:"token"`
	Guilds map[string]json.RawMessage `json:"guilds"`
}

type openClawSecretValue struct {
	Plain string
	Ref   *openClawSecretRef
}

type openClawSecretRef struct {
	Source   string `json:"source"`
	Provider string `json:"provider"`
	ID       string `json:"id"`
}

type openClawSecrets struct {
	Providers map[string]openClawSecretProvider
	Aliases   map[string]openClawSecretProvider
	Defaults  openClawSecretDefaults
}

type openClawSecretProvider struct {
	Source    string   `json:"source"`
	Path      string   `json:"path"`
	Mode      string   `json:"mode"`
	Allowlist []string `json:"allowlist"`
}

type openClawSecretDefaults struct {
	Env  string `json:"env"`
	File string `json:"file"`
	Exec string `json:"exec"`
}

func (v *openClawSecretValue) UnmarshalJSON(data []byte) error {
	var plain string
	if err := json.Unmarshal(data, &plain); err == nil {
		v.Plain = plain
		v.Ref = nil
		return nil
	}
	var ref openClawSecretRef
	if err := json.Unmarshal(data, &ref); err != nil {
		return err
	}
	v.Plain = ""
	v.Ref = &ref
	return nil
}

func (s *openClawSecrets) UnmarshalJSON(data []byte) error {
	var aux struct {
		Providers map[string]openClawSecretProvider `json:"providers"`
		Defaults  openClawSecretDefaults            `json:"defaults"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	aliases := map[string]openClawSecretProvider{}
	for name, value := range raw {
		if name == "providers" || name == "defaults" {
			continue
		}
		var provider openClawSecretProvider
		if err := json.Unmarshal(value, &provider); err == nil && (provider.Source != "" || provider.Path != "") {
			aliases[name] = provider
		}
	}
	s.Providers = aux.Providers
	s.Aliases = aliases
	s.Defaults = aux.Defaults
	return nil
}

func (s openClawSecrets) provider(name string) (openClawSecretProvider, bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		name = "default"
	}
	if provider, ok := s.Providers[name]; ok {
		return provider, true
	}
	provider, ok := s.Aliases[name]
	return provider, ok
}

func Default() Config {
	home, _ := os.UserHomeDir()
	base := filepath.Join(home, ".discrawl")
	return Config{
		Version:        1,
		DBPath:         filepath.Join(base, "discrawl.db"),
		CacheDir:       filepath.Join(base, "cache"),
		LogDir:         filepath.Join(base, "logs"),
		DefaultGuildID: "",
		Discord: DiscordConfig{
			TokenSource:    "openclaw",
			OpenClawConfig: filepath.Join(home, ".openclaw", "openclaw.json"),
			Account:        "default",
			TokenEnv:       DefaultTokenEnv,
		},
		Desktop: DesktopConfig{
			Path:         defaultDiscordDesktopPath(home),
			MaxFileBytes: 64 << 20,
		},
		Sync: SyncConfig{
			Source:         "both",
			Concurrency:    defaultSyncConcurrency(),
			RepairEvery:    "6h",
			FullHistory:    true,
			AttachmentText: new(true),
		},
		Search: SearchConfig{
			DefaultMode: "fts",
			Embeddings: EmbeddingsConfig{
				Enabled:        false,
				Provider:       "openai",
				Model:          "text-embedding-3-small",
				APIKeyEnv:      "OPENAI_API_KEY",
				BatchSize:      64,
				MaxInputChars:  12000,
				RequestTimeout: "2m",
			},
		},
		Share: ShareConfig{
			RepoPath:   filepath.Join(base, "share"),
			Branch:     "main",
			AutoUpdate: true,
			StaleAfter: "15m",
		},
	}
}

func defaultSyncConcurrency() int {
	workers := runtime.GOMAXPROCS(0) * 2
	switch {
	case workers < 8:
		return 8
	case workers > 32:
		return 32
	default:
		return workers
	}
}

func ResolvePath(flagPath string) string {
	if strings.TrimSpace(flagPath) != "" {
		return flagPath
	}
	if envPath := strings.TrimSpace(os.Getenv(DefaultConfigEnv)); envPath != "" {
		return envPath
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".discrawl", "config.toml")
}

func Load(path string) (Config, error) {
	cfg := Default()
	expanded, err := ExpandPath(path)
	if err != nil {
		return Config{}, err
	}
	data, err := os.ReadFile(expanded)
	if err != nil {
		return Config{}, err
	}
	if err := toml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.Normalize(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func Write(path string, cfg Config) error {
	if err := cfg.Normalize(); err != nil {
		return err
	}
	expanded, err := ExpandPath(path)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(expanded), 0o755); err != nil {
		return fmt.Errorf("mkdir config dir: %w", err)
	}
	data, err := toml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	return os.WriteFile(expanded, data, 0o600)
}

func (c *Config) Normalize() error {
	if c.Version == 0 {
		c.Version = 1
	}
	if c.DefaultGuildID == "" && c.GuildID != "" {
		c.DefaultGuildID = c.GuildID
	}
	if c.DBPath == "" || c.CacheDir == "" || c.LogDir == "" {
		def := Default()
		if c.DBPath == "" {
			c.DBPath = def.DBPath
		}
		if c.CacheDir == "" {
			c.CacheDir = def.CacheDir
		}
		if c.LogDir == "" {
			c.LogDir = def.LogDir
		}
	}
	if c.Discord.TokenSource == "" {
		c.Discord.TokenSource = "openclaw"
	}
	if c.Discord.OpenClawConfig == "" {
		c.Discord.OpenClawConfig = Default().Discord.OpenClawConfig
	}
	if c.Discord.Account == "" {
		c.Discord.Account = "default"
	}
	if c.Discord.TokenEnv == "" {
		c.Discord.TokenEnv = DefaultTokenEnv
	}
	if c.Desktop.Path == "" {
		c.Desktop.Path = defaultDiscordDesktopPath(homeDir())
	}
	if c.Desktop.MaxFileBytes <= 0 {
		c.Desktop.MaxFileBytes = 64 << 20
	}
	if c.Sync.Concurrency <= 0 {
		c.Sync.Concurrency = defaultSyncConcurrency()
	}
	c.Sync.Source = strings.ToLower(strings.TrimSpace(c.Sync.Source))
	if c.Sync.Source == "" {
		c.Sync.Source = "both"
	}
	if c.Sync.RepairEvery == "" {
		c.Sync.RepairEvery = "6h"
	}
	if c.Sync.AttachmentText == nil {
		c.Sync.AttachmentText = new(true)
	}
	if c.Search.DefaultMode == "" {
		c.Search.DefaultMode = "fts"
	}
	c.Search.Embeddings.Provider = strings.ToLower(strings.TrimSpace(c.Search.Embeddings.Provider))
	c.Search.Embeddings.Model = strings.TrimSpace(c.Search.Embeddings.Model)
	c.Search.Embeddings.BaseURL = strings.TrimRight(strings.TrimSpace(c.Search.Embeddings.BaseURL), "/")
	c.Search.Embeddings.APIKeyEnv = strings.TrimSpace(c.Search.Embeddings.APIKeyEnv)
	c.Search.Embeddings.RequestTimeout = strings.TrimSpace(c.Search.Embeddings.RequestTimeout)
	if c.Search.Embeddings.Provider == "" {
		c.Search.Embeddings.Provider = "openai"
	}
	if c.Search.Embeddings.Model == "" {
		switch strings.ToLower(strings.TrimSpace(c.Search.Embeddings.Provider)) {
		case "ollama", "llamacpp":
			c.Search.Embeddings.Model = "nomic-embed-text"
		default:
			c.Search.Embeddings.Model = "text-embedding-3-small"
		}
	}
	if c.Search.Embeddings.APIKeyEnv == "" && c.Search.Embeddings.Provider == "openai" {
		c.Search.Embeddings.APIKeyEnv = "OPENAI_API_KEY"
	}
	if (c.Search.Embeddings.Provider == "ollama" || c.Search.Embeddings.Provider == "llamacpp") && c.Search.Embeddings.APIKeyEnv == "OPENAI_API_KEY" {
		c.Search.Embeddings.APIKeyEnv = ""
	}
	if c.Search.Embeddings.BatchSize <= 0 {
		c.Search.Embeddings.BatchSize = 64
	}
	if c.Share.RepoPath == "" {
		c.Share.RepoPath = Default().Share.RepoPath
	}
	if c.Share.Branch == "" {
		c.Share.Branch = "main"
	}
	if c.Share.StaleAfter == "" {
		c.Share.StaleAfter = "15m"
	}
	if c.Search.Embeddings.MaxInputChars <= 0 {
		c.Search.Embeddings.MaxInputChars = 12000
	}
	if c.Search.Embeddings.RequestTimeout == "" {
		c.Search.Embeddings.RequestTimeout = "2m"
	}
	if timeout, err := time.ParseDuration(c.Search.Embeddings.RequestTimeout); err != nil {
		return fmt.Errorf("parse search.embeddings.request_timeout: %w", err)
	} else if timeout <= 0 {
		return errors.New("search.embeddings.request_timeout must be positive")
	}
	c.GuildIDs = uniqueStrings(c.GuildIDs)
	return nil
}

func defaultDiscordDesktopPath(home string) string {
	switch runtime.GOOS {
	case "darwin":
		return filepath.Join(home, "Library", "Application Support", "discord")
	case "windows":
		if appData := strings.TrimSpace(os.Getenv("APPDATA")); appData != "" {
			return filepath.Join(appData, "discord")
		}
		return filepath.Join(home, "AppData", "Roaming", "discord")
	default:
		if configHome := strings.TrimSpace(os.Getenv("XDG_CONFIG_HOME")); configHome != "" {
			return filepath.Join(configHome, "discord")
		}
		return filepath.Join(home, ".config", "discord")
	}
}

func homeDir() string {
	home, _ := os.UserHomeDir()
	return home
}

func (c Config) EffectiveDefaultGuildID() string {
	if c.DefaultGuildID != "" {
		return c.DefaultGuildID
	}
	if len(c.GuildIDs) == 1 {
		return c.GuildIDs[0]
	}
	return ""
}

func (c Config) SearchGuildDefaults() []string {
	return nil
}

func (c Config) AttachmentTextEnabled() bool {
	return c.Sync.AttachmentText == nil || *c.Sync.AttachmentText
}

func (c Config) ShareEnabled() bool {
	return strings.TrimSpace(c.Share.Remote) != ""
}

func EnsureRuntimeDirs(cfg Config) error {
	paths := []string{cfg.CacheDir, cfg.LogDir, filepath.Dir(cfg.DBPath)}
	for _, path := range paths {
		expanded, err := ExpandPath(path)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(expanded, 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", expanded, err)
		}
	}
	return nil
}

func ExpandPath(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("empty path")
	}
	if strings.HasPrefix(path, "~/") || path == "~" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("home dir: %w", err)
		}
		if path == "~" {
			path = home
		} else {
			path = filepath.Join(home, strings.TrimPrefix(path, "~/"))
		}
	}
	return filepath.Clean(os.ExpandEnv(path)), nil
}

func ResolveDiscordToken(cfg Config) (TokenResolution, error) {
	if err := cfg.Normalize(); err != nil {
		return TokenResolution{}, err
	}
	if cfg.Discord.TokenSource == "none" {
		return TokenResolution{}, errors.New("discord token disabled by config")
	}
	if cfg.Discord.TokenSource != "env" {
		openClaw, err := LoadOpenClawDiscord(cfg.Discord.OpenClawConfig, cfg.Discord.Account)
		if err == nil && openClaw.Token != "" {
			return TokenResolution{Token: openClaw.Token, Source: "openclaw", Path: openClaw.Path}, nil
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return TokenResolution{}, err
		}
	}
	if envToken := NormalizeBotToken(os.Getenv(cfg.Discord.TokenEnv)); envToken != "" {
		return TokenResolution{Token: envToken, Source: "env", Path: cfg.Discord.TokenEnv}, nil
	}
	return TokenResolution{}, errors.New("discord token not found in env or openclaw config")
}

func LoadOpenClawDiscord(path, account string) (OpenClawDiscord, error) {
	paths, err := openClawCandidates(path)
	if err != nil {
		return OpenClawDiscord{}, err
	}
	for _, candidate := range paths {
		info, err := loadOpenClawDiscordFile(candidate, account)
		if err == nil && info.Token != "" {
			return info, nil
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return OpenClawDiscord{}, err
		}
	}
	return OpenClawDiscord{}, os.ErrNotExist
}

func loadOpenClawDiscordFile(path, account string) (OpenClawDiscord, error) {
	expanded, err := ExpandPath(path)
	if err != nil {
		return OpenClawDiscord{}, err
	}
	data, err := os.ReadFile(expanded)
	if err != nil {
		return OpenClawDiscord{}, err
	}
	var payload openClawConfig
	if err := json.Unmarshal(data, &payload); err != nil {
		return OpenClawDiscord{}, fmt.Errorf("parse openclaw config: %w", err)
	}
	discord := payload.Channels.Discord
	token, err := resolveOpenClawSecretValue(discord.Token, payload.Secrets, expanded)
	if err != nil {
		return OpenClawDiscord{}, fmt.Errorf("resolve openclaw discord token: %w", err)
	}
	guildIDs := mapKeys(discord.Guilds)
	if token == "" {
		acct := discord.Accounts[normalizeAccount(account)]
		if acct.Token.empty() && account != normalizeAccount(account) {
			acct = discord.Accounts[account]
		}
		token, err = resolveOpenClawSecretValue(acct.Token, payload.Secrets, expanded)
		if err != nil {
			return OpenClawDiscord{}, fmt.Errorf("resolve openclaw discord account token: %w", err)
		}
		if len(guildIDs) == 0 {
			guildIDs = mapKeys(acct.Guilds)
		}
	}
	return OpenClawDiscord{
		Token:    token,
		GuildIDs: guildIDs,
		Path:     expanded,
	}, nil
}

func openClawCandidates(path string) ([]string, error) {
	expanded, err := ExpandPath(path)
	if err != nil {
		return nil, err
	}
	candidates := []string{expanded}
	matches, err := filepath.Glob(expanded + ".bak*")
	if err != nil {
		return nil, err
	}
	sort.Strings(matches)
	candidates = append(candidates, matches...)
	return uniqueStrings(candidates), nil
}

func NormalizeBotToken(raw string) string {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "Bot ")
	return strings.TrimSpace(raw)
}

func (v openClawSecretValue) empty() bool {
	return strings.TrimSpace(v.Plain) == "" && v.Ref == nil
}

func resolveOpenClawSecretValue(value openClawSecretValue, secrets openClawSecrets, configPath string) (string, error) {
	if value.Ref == nil {
		return NormalizeBotToken(os.ExpandEnv(value.Plain)), nil
	}
	ref := *value.Ref
	source := strings.ToLower(strings.TrimSpace(ref.Source))
	switch source {
	case "env":
		return resolveOpenClawEnvSecret(ref, secrets)
	case "file":
		return resolveOpenClawFileSecret(ref, secrets, configPath)
	case "":
		return "", errors.New("secret ref missing source")
	default:
		return "", fmt.Errorf("unsupported secret ref source %q", ref.Source)
	}
}

func resolveOpenClawEnvSecret(ref openClawSecretRef, secrets openClawSecrets) (string, error) {
	id := strings.TrimSpace(ref.ID)
	if id == "" {
		return "", errors.New("env secret ref missing id")
	}
	providerName := strings.TrimSpace(ref.Provider)
	if providerName == "" {
		providerName = defaultOpenClawSecretProvider(secrets.Defaults.Env, "default")
	}
	if provider, ok := secrets.provider(providerName); ok {
		source := strings.ToLower(strings.TrimSpace(provider.Source))
		if source != "" && source != "env" {
			return "", fmt.Errorf("secret provider %q has source %q, want env", providerName, provider.Source)
		}
		if len(provider.Allowlist) > 0 && !stringInSlice(id, provider.Allowlist) {
			return "", fmt.Errorf("environment variable %q is not allowlisted in secret provider %q", id, providerName)
		}
	} else if providerName != defaultOpenClawSecretProvider(secrets.Defaults.Env, "default") {
		return "", fmt.Errorf("secret provider %q not found", providerName)
	}
	value, ok := os.LookupEnv(id)
	if !ok || strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("environment variable %q is missing or empty", id)
	}
	return NormalizeBotToken(value), nil
}

func resolveOpenClawFileSecret(ref openClawSecretRef, secrets openClawSecrets, configPath string) (string, error) {
	providerName := strings.TrimSpace(ref.Provider)
	if providerName == "" {
		providerName = defaultOpenClawSecretProvider(secrets.Defaults.File, "filemain")
	}
	provider, ok := secrets.provider(providerName)
	if !ok {
		return "", fmt.Errorf("secret provider %q not found", providerName)
	}
	source := strings.ToLower(strings.TrimSpace(provider.Source))
	if source != "" && source != "file" {
		return "", fmt.Errorf("secret provider %q has source %q, want file", providerName, provider.Source)
	}
	path := strings.TrimSpace(provider.Path)
	if path == "" {
		return "", fmt.Errorf("secret provider %q missing path", providerName)
	}
	expanded, err := expandOpenClawSecretPath(path, configPath)
	if err != nil {
		return "", err
	}
	data, err := os.ReadFile(expanded)
	if err != nil {
		return "", err
	}
	mode := strings.ToLower(strings.TrimSpace(provider.Mode))
	if mode == "" {
		mode = "json"
	}
	switch mode {
	case "json":
		token, err := readJSONPointerString(data, ref.ID)
		if err != nil {
			return "", fmt.Errorf("read secret provider %q id %q: %w", providerName, ref.ID, err)
		}
		return NormalizeBotToken(os.ExpandEnv(token)), nil
	case "singlevalue":
		if strings.TrimSpace(ref.ID) != "value" {
			return "", fmt.Errorf("secret provider %q singleValue mode requires id %q", providerName, "value")
		}
		return NormalizeBotToken(os.ExpandEnv(string(data))), nil
	default:
		return "", fmt.Errorf("unsupported secret provider %q mode %q", providerName, provider.Mode)
	}
}

func expandOpenClawSecretPath(path, configPath string) (string, error) {
	path = os.ExpandEnv(strings.TrimSpace(path))
	if path == "" {
		return "", errors.New("empty secret provider path")
	}
	if strings.HasPrefix(path, "~") || filepath.IsAbs(path) {
		return ExpandPath(path)
	}
	return filepath.Clean(filepath.Join(filepath.Dir(configPath), path)), nil
}

func readJSONPointerString(data []byte, pointer string) (string, error) {
	pointer = strings.TrimSpace(pointer)
	if pointer == "" || pointer[0] != '/' {
		return "", errors.New("id must be an absolute JSON pointer")
	}
	var current any
	if err := json.Unmarshal(data, &current); err != nil {
		return "", fmt.Errorf("parse secret file: %w", err)
	}
	for _, rawSegment := range strings.Split(pointer[1:], "/") {
		segment := strings.ReplaceAll(strings.ReplaceAll(rawSegment, "~1", "/"), "~0", "~")
		object, ok := current.(map[string]any)
		if !ok {
			return "", fmt.Errorf("segment %q is not an object", segment)
		}
		next, ok := object[segment]
		if !ok {
			return "", fmt.Errorf("segment %q not found", segment)
		}
		current = next
	}
	secret, ok := current.(string)
	if !ok {
		return "", errors.New("secret value is not a string")
	}
	return secret, nil
}

func defaultOpenClawSecretProvider(configured, fallback string) string {
	configured = strings.TrimSpace(configured)
	if configured != "" {
		return configured
	}
	return fallback
}

func normalizeAccount(account string) string {
	account = strings.TrimSpace(strings.ToLower(account))
	if account == "" {
		return "default"
	}
	return account
}

func mapKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func uniqueStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, item := range in {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

func stringInSlice(needle string, haystack []string) bool {
	for _, item := range haystack {
		if item == needle {
			return true
		}
	}
	return false
}
