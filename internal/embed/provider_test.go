package embed

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/steipete/discrawl/internal/config"
)

func TestOllamaProviderEmbeds(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/embed", r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)
		var req ollamaEmbedRequest
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		assert.Equal(t, "nomic-embed-text", req.Model)
		assert.Equal(t, []string{"abcd", "xy"}, req.Input)
		_, _ = w.Write([]byte(`{"model":"nomic-embed-text","embeddings":[[1,2,3],[4,5,6]]}`))
	}))
	defer server.Close()

	provider, err := NewProvider(config.EmbeddingsConfig{
		Provider:       ProviderOllama,
		Model:          "nomic-embed-text",
		BaseURL:        server.URL,
		MaxInputChars:  4,
		RequestTimeout: "5s",
	})
	require.NoError(t, err)

	batch, err := provider.Embed(context.Background(), []string{"abcdef", "xy"})
	require.NoError(t, err)
	require.Equal(t, "nomic-embed-text", batch.Model)
	require.Equal(t, 3, batch.Dimensions)
	require.Equal(t, [][]float32{{1, 2, 3}, {4, 5, 6}}, batch.Vectors)
}

func TestOpenAICompatibleProviderEmbedsAndUsesAuth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/embeddings", r.URL.Path)
		assert.Equal(t, "Bearer secret", r.Header.Get("Authorization"))
		var req openAIEmbeddingRequest
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		assert.Equal(t, "local-model", req.Model)
		assert.Equal(t, []string{"one", "two"}, req.Input)
		_, _ = w.Write([]byte(`{
			"model":"local-model",
			"data":[
				{"index":1,"embedding":[3,4]},
				{"index":0,"embedding":[1,2]}
			]
		}`))
	}))
	defer server.Close()
	t.Setenv("DISCRAWL_EMBED_KEY", "secret")

	provider, err := NewProvider(config.EmbeddingsConfig{
		Provider:       ProviderOpenAICompatible,
		Model:          "local-model",
		BaseURL:        server.URL,
		APIKeyEnv:      "DISCRAWL_EMBED_KEY",
		RequestTimeout: "5s",
	})
	require.NoError(t, err)

	batch, err := provider.Embed(context.Background(), []string{"one", "two"})
	require.NoError(t, err)
	require.Equal(t, "local-model", batch.Model)
	require.Equal(t, 2, batch.Dimensions)
	require.Equal(t, [][]float32{{1, 2}, {3, 4}}, batch.Vectors)
}

func TestProviderFactoryDefaultsAndValidation(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "openai-secret")

	openAI, err := resolveProviderConfig(config.EmbeddingsConfig{
		Provider:       ProviderOpenAI,
		RequestTimeout: "5s",
	}, true)
	require.NoError(t, err)
	require.Equal(t, DefaultOpenAIBaseURL, openAI.BaseURL)
	require.Equal(t, DefaultOpenAIModel, openAI.Model)
	require.Equal(t, "openai-secret", openAI.APIKey)

	ollama, err := resolveProviderConfig(config.EmbeddingsConfig{
		Provider:       ProviderOllama,
		RequestTimeout: "5s",
	}, true)
	require.NoError(t, err)
	require.Equal(t, DefaultOllamaBaseURL, ollama.BaseURL)
	require.Equal(t, DefaultLocalEmbeddingModel, ollama.Model)

	llamaCpp, err := resolveProviderConfig(config.EmbeddingsConfig{
		Provider:       ProviderLlamaCpp,
		RequestTimeout: "5s",
	}, true)
	require.NoError(t, err)
	require.Equal(t, DefaultLlamaCppBaseURL, llamaCpp.BaseURL)

	_, err = resolveProviderConfig(config.EmbeddingsConfig{
		Provider:       ProviderOpenAICompatible,
		RequestTimeout: "5s",
	}, true)
	require.ErrorContains(t, err, "requires base_url")
}

func TestProviderFactoryRequiresOpenAIAPIKey(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "")

	_, err := NewProvider(config.EmbeddingsConfig{
		Provider:       ProviderOpenAI,
		RequestTimeout: "5s",
	})
	require.ErrorContains(t, err, "requires API key env OPENAI_API_KEY")
}

func TestProviderFactoryReportsUnsupportedProviderBeforeAPIKey(t *testing.T) {
	t.Setenv("MISSING_EMBED_KEY", "")

	_, err := NewProvider(config.EmbeddingsConfig{
		Provider:       "bogus",
		APIKeyEnv:      "MISSING_EMBED_KEY",
		RequestTimeout: "5s",
	})
	require.ErrorContains(t, err, "unsupported embedding provider \"bogus\"")
}

func TestCheckProviderProbesLocalProvider(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/embed", r.URL.Path)
		_, _ = w.Write([]byte(`{"model":"nomic-embed-text","embeddings":[[1,2]]}`))
	}))
	defer server.Close()

	result := CheckProvider(context.Background(), config.EmbeddingsConfig{
		Provider:       ProviderOllama,
		Model:          "nomic-embed-text",
		BaseURL:        server.URL,
		RequestTimeout: "5s",
	})
	require.Equal(t, "ok", result.Status)
	require.True(t, result.Probed)
	require.Empty(t, result.Warning)
	require.Equal(t, server.URL, result.BaseURL)
}

func TestCheckProviderWarnsOnLocalProbeFailure(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	result := CheckProvider(context.Background(), config.EmbeddingsConfig{
		Provider:       ProviderOllama,
		Model:          "nomic-embed-text",
		BaseURL:        server.URL,
		RequestTimeout: "5s",
	})
	require.Equal(t, "warning", result.Status)
	require.Contains(t, result.Warning, "HTTP 503")
	require.False(t, result.Probed)
}

func TestProviderExposesRateLimitErrors(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	defer server.Close()

	provider, err := NewProvider(config.EmbeddingsConfig{
		Provider:       ProviderOpenAICompatible,
		Model:          "local-model",
		BaseURL:        server.URL,
		RequestTimeout: "5s",
	})
	require.NoError(t, err)

	_, err = provider.Embed(context.Background(), []string{"one"})
	require.ErrorContains(t, err, "HTTP 429")
	require.True(t, IsRateLimitError(err))
}

func TestProviderRejectsInvalidResponses(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":[{"index":0,"embedding":[1]},{"index":1,"embedding":[2,3]}]}`))
	}))
	defer server.Close()

	provider, err := NewProvider(config.EmbeddingsConfig{
		Provider:       ProviderOpenAICompatible,
		Model:          "local-model",
		BaseURL:        server.URL,
		RequestTimeout: "5s",
	})
	require.NoError(t, err)

	_, err = provider.Embed(context.Background(), []string{"one", "two"})
	require.ErrorContains(t, err, "dimensions mismatch")
}

func TestEmbeddingProvidersHandleEmptyInputsAndIndexErrors(t *testing.T) {
	t.Parallel()

	settings := providerSettings{
		Name:          ProviderOllama,
		Model:         "model",
		BaseURL:       "http://127.0.0.1:1",
		MaxInputChars: 10,
		HTTPClient:    http.DefaultClient,
	}
	ollama := newOllamaProvider(settings)
	batch, err := ollama.Embed(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, "model", batch.Model)

	settings.Name = ProviderOpenAICompatible
	openai := newOpenAICompatibleProvider(settings)
	batch, err = openai.Embed(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, "model", batch.Model)

	tests := []struct {
		name   string
		body   string
		inputs []string
		want   string
	}{
		{
			name:   "count",
			body:   `{"data":[]}`,
			inputs: []string{"one"},
			want:   "returned 0 vectors for 1 inputs",
		},
		{
			name:   "range",
			body:   `{"data":[{"index":2,"embedding":[1]}]}`,
			inputs: []string{"one"},
			want:   "index 2 out of range",
		},
		{
			name:   "duplicate",
			body:   `{"data":[{"index":0,"embedding":[1]},{"index":0,"embedding":[2]}]}`,
			inputs: []string{"one", "two"},
			want:   "duplicated index 0",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			provider, err := NewProvider(config.EmbeddingsConfig{
				Provider:       ProviderOpenAICompatible,
				Model:          "model",
				BaseURL:        server.URL,
				RequestTimeout: "5s",
			})
			require.NoError(t, err)
			_, err = provider.Embed(context.Background(), tc.inputs)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestProviderOptionsAndProbeDecisions(t *testing.T) {
	t.Parallel()

	client := &http.Client{Timeout: time.Second}
	settings, err := resolveProviderConfig(config.EmbeddingsConfig{
		Provider:       ProviderOllama,
		BaseURL:        "http://127.0.0.1:11434/",
		RequestTimeout: "30s",
	}, true, WithHTTPClient(client), WithRequestTimeout(50*time.Millisecond))
	require.NoError(t, err)
	require.Same(t, client, settings.HTTPClient)
	require.Equal(t, 50*time.Millisecond, settings.Timeout)
	require.Equal(t, "http://127.0.0.1:11434", settings.BaseURL)
	require.True(t, shouldProbe(settings))

	require.True(t, isLoopbackBaseURL("http://localhost:8080/v1"))
	require.True(t, isLoopbackBaseURL("http://[::1]:8080/v1"))
	require.False(t, isLoopbackBaseURL("https://api.example.com/v1"))
	require.False(t, isLoopbackBaseURL("://bad"))
	require.False(t, shouldProbe(providerSettings{Name: ProviderOpenAI}))
	require.True(t, shouldProbe(providerSettings{Name: ProviderOpenAICompatible, BaseURL: "http://localhost:8080/v1"}))
	require.False(t, shouldProbe(providerSettings{Name: ProviderOpenAICompatible, BaseURL: "https://api.example.com/v1"}))
}

func TestCheckProviderSkipsRemoteCompatibleProbe(t *testing.T) {
	t.Parallel()

	result := CheckProvider(context.Background(), config.EmbeddingsConfig{
		Provider:       ProviderOpenAICompatible,
		Model:          "remote-model",
		BaseURL:        "https://api.example.com/v1",
		RequestTimeout: "5s",
	})
	require.Equal(t, "ok", result.Status)
	require.False(t, result.Probed)
	require.Empty(t, result.Warning)
}
