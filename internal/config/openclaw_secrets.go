package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

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

type openClawSecretResolver struct {
	secrets    openClawSecrets
	configPath string
	env        func(string) (string, bool)
	readFile   func(string) ([]byte, error)
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

func newOpenClawSecretResolver(secrets openClawSecrets, configPath string) openClawSecretResolver {
	return openClawSecretResolver{
		secrets:    secrets,
		configPath: configPath,
		env:        os.LookupEnv,
		readFile:   os.ReadFile,
	}
}

func (v openClawSecretValue) empty() bool {
	return strings.TrimSpace(v.Plain) == "" && v.Ref == nil
}

func (r openClawSecretResolver) resolve(value openClawSecretValue) (string, error) {
	if value.Ref == nil {
		return NormalizeBotToken(os.ExpandEnv(value.Plain)), nil
	}
	token, err := r.resolveRef(*value.Ref)
	if err != nil {
		return "", err
	}
	if token == "" {
		return "", errors.New("secret reference resolved to an empty value")
	}
	return token, nil
}

func (r openClawSecretResolver) resolveRef(ref openClawSecretRef) (string, error) {
	source := strings.ToLower(strings.TrimSpace(ref.Source))
	switch source {
	case "env":
		return r.resolveEnv(ref)
	case "file":
		return r.resolveFile(ref)
	case "exec":
		return "", errors.New("exec SecretRefs are not supported by discrawl init --from-openclaw")
	case "":
		return "", errors.New("secret ref missing source")
	default:
		return "", fmt.Errorf("unsupported secret ref source %q", ref.Source)
	}
}

func (r openClawSecretResolver) resolveEnv(ref openClawSecretRef) (string, error) {
	id := strings.TrimSpace(ref.ID)
	if id == "" {
		return "", errors.New("env secret ref missing id")
	}
	providerName := strings.TrimSpace(ref.Provider)
	if providerName == "" {
		providerName = defaultOpenClawSecretProvider(r.secrets.Defaults.Env, "default")
	}
	if provider, ok := r.secrets.provider(providerName); ok {
		source := strings.ToLower(strings.TrimSpace(provider.Source))
		if source != "" && source != "env" {
			return "", fmt.Errorf("secret provider %q has source %q, want env", providerName, provider.Source)
		}
		if len(provider.Allowlist) > 0 && !stringInSlice(id, provider.Allowlist) {
			return "", fmt.Errorf("environment variable %q is not allowlisted in secret provider %q", id, providerName)
		}
	} else if providerName != defaultOpenClawSecretProvider(r.secrets.Defaults.Env, "default") {
		return "", fmt.Errorf("secret provider %q not found", providerName)
	}
	value, ok := r.env(id)
	if !ok || strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("environment variable %q is missing or empty", id)
	}
	return NormalizeBotToken(value), nil
}

func (r openClawSecretResolver) resolveFile(ref openClawSecretRef) (string, error) {
	providerName := strings.TrimSpace(ref.Provider)
	if providerName == "" {
		providerName = defaultOpenClawSecretProvider(r.secrets.Defaults.File, "filemain")
	}
	provider, ok := r.secrets.provider(providerName)
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
	expanded, err := expandOpenClawSecretPath(path, r.configPath)
	if err != nil {
		return "", err
	}
	data, err := r.readFile(expanded)
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

func stringInSlice(needle string, haystack []string) bool {
	for _, item := range haystack {
		if item == needle {
			return true
		}
	}
	return false
}
