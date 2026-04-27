package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenClawSecretResolverUsesFileDefaultProvider(t *testing.T) {
	dir := t.TempDir()
	secretsPath := filepath.Join(dir, "secrets.json")
	require.NoError(t, writeTestFile(secretsPath, `{
		"discord": {
			"token": "Bot default-file-token"
		}
	}`))

	resolver := newOpenClawSecretResolver(openClawSecrets{
		Defaults: openClawSecretDefaults{File: "vaultfile"},
		Providers: map[string]openClawSecretProvider{
			"vaultfile": {
				Source: "file",
				Path:   "secrets.json",
				Mode:   "json",
			},
		},
	}, filepath.Join(dir, "openclaw.json"))

	token, err := resolver.resolve(openClawSecretValue{Ref: &openClawSecretRef{
		Source: "file",
		ID:     "/discord/token",
	}})
	require.NoError(t, err)
	require.Equal(t, "default-file-token", token)
}

func TestOpenClawSecretResolverReadsSingleValueFile(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, writeTestFile(filepath.Join(dir, "discord-token.txt"), "Bot single-value-token\n"))
	resolver := newOpenClawSecretResolver(openClawSecrets{
		Providers: map[string]openClawSecretProvider{
			"filemain": {
				Source: "file",
				Path:   "discord-token.txt",
				Mode:   "singleValue",
			},
		},
	}, filepath.Join(dir, "openclaw.json"))

	token, err := resolver.resolve(openClawSecretValue{Ref: &openClawSecretRef{
		Source:   "file",
		Provider: "filemain",
		ID:       "value",
	}})
	require.NoError(t, err)
	require.Equal(t, "single-value-token", token)
}

func TestOpenClawSecretResolverRejectsSingleValueFilePointerIDs(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, writeTestFile(filepath.Join(dir, "discord-token.txt"), "token"))
	resolver := newOpenClawSecretResolver(openClawSecrets{
		Providers: map[string]openClawSecretProvider{
			"filemain": {
				Source: "file",
				Path:   "discord-token.txt",
				Mode:   "singleValue",
			},
		},
	}, filepath.Join(dir, "openclaw.json"))

	_, err := resolver.resolve(openClawSecretValue{Ref: &openClawSecretRef{
		Source:   "file",
		Provider: "filemain",
		ID:       "/discord/token",
	}})
	require.ErrorContains(t, err, `singleValue mode requires id "value"`)
}

func TestOpenClawSecretResolverRejectsEmptyFileSecret(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, writeTestFile(filepath.Join(dir, "secrets.json"), `{"discord":{"token":"   "}}`))
	resolver := newOpenClawSecretResolver(openClawSecrets{
		Providers: map[string]openClawSecretProvider{
			"filemain": {
				Source: "file",
				Path:   "secrets.json",
				Mode:   "json",
			},
		},
	}, filepath.Join(dir, "openclaw.json"))

	_, err := resolver.resolve(openClawSecretValue{Ref: &openClawSecretRef{
		Source:   "file",
		Provider: "filemain",
		ID:       "/discord/token",
	}})
	require.ErrorContains(t, err, "secret reference resolved to an empty value")
}

func TestOpenClawSecretResolverRejectsFileProviderSourceMismatch(t *testing.T) {
	dir := t.TempDir()
	resolver := newOpenClawSecretResolver(openClawSecrets{
		Providers: map[string]openClawSecretProvider{
			"filemain": {
				Source: "env",
				Path:   "secrets.json",
			},
		},
	}, filepath.Join(dir, "openclaw.json"))

	_, err := resolver.resolve(openClawSecretValue{Ref: &openClawSecretRef{
		Source:   "file",
		Provider: "filemain",
		ID:       "/discord/token",
	}})
	require.ErrorContains(t, err, `secret provider "filemain" has source "env", want file`)
}

func TestOpenClawSecretResolverRejectsExecRefsClearly(t *testing.T) {
	resolver := newOpenClawSecretResolver(openClawSecrets{}, filepath.Join(t.TempDir(), "openclaw.json"))

	_, err := resolver.resolve(openClawSecretValue{Ref: &openClawSecretRef{
		Source:   "exec",
		Provider: "vault",
		ID:       "discord/token",
	}})
	require.ErrorContains(t, err, "exec SecretRefs are not supported by discrawl init --from-openclaw")
}

func writeTestFile(path, contents string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(contents), 0o600)
}
