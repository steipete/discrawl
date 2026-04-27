package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

func ResolveDiscordToken(cfg Config) (TokenResolution, error) {
	if err := cfg.Normalize(); err != nil {
		return TokenResolution{}, err
	}
	switch cfg.Discord.TokenSource {
	case "none":
		return TokenResolution{}, errors.New("discord token disabled by config")
	case "env":
		envToken := NormalizeBotToken(os.Getenv(cfg.Discord.TokenEnv))
		if envToken == "" {
			return TokenResolution{}, fmt.Errorf("discord token not found in environment variable %q", cfg.Discord.TokenEnv)
		}
		return TokenResolution{Token: envToken, Source: "env", Path: cfg.Discord.TokenEnv}, nil
	default:
		return TokenResolution{}, fmt.Errorf("unsupported discord token_source %q", cfg.Discord.TokenSource)
	}
}

func NormalizeBotToken(raw string) string {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "Bot ")
	return strings.TrimSpace(raw)
}
