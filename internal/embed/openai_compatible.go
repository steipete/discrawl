package embed

import (
	"context"
	"fmt"
	"net/http"
)

type openAICompatibleProvider struct {
	client        *http.Client
	baseURL       string
	apiKey        string
	model         string
	maxInputChars int
}

type openAIEmbeddingRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type openAIEmbeddingResponse struct {
	Model string                `json:"model"`
	Data  []openAIEmbeddingItem `json:"data"`
}

type openAIEmbeddingItem struct {
	Index     *int      `json:"index"`
	Embedding []float32 `json:"embedding"`
}

func newOpenAICompatibleProvider(settings providerSettings) Provider {
	return &openAICompatibleProvider{
		client:        settings.HTTPClient,
		baseURL:       settings.BaseURL,
		apiKey:        settings.APIKey,
		model:         settings.Model,
		maxInputChars: settings.MaxInputChars,
	}
}

func (p *openAICompatibleProvider) Embed(ctx context.Context, inputs []string) (EmbeddingBatch, error) {
	if len(inputs) == 0 {
		return EmbeddingBatch{Model: p.model}, nil
	}
	payload := openAIEmbeddingRequest{
		Model: p.model,
		Input: trimInputs(inputs, p.maxInputChars),
	}
	var response openAIEmbeddingResponse
	if err := postJSON(ctx, p.client, p.baseURL+"/embeddings", p.apiKey, payload, &response); err != nil {
		return EmbeddingBatch{}, err
	}
	if len(response.Data) != len(inputs) {
		return EmbeddingBatch{}, fmt.Errorf("openai-compatible embedding response returned %d vectors for %d inputs", len(response.Data), len(inputs))
	}
	vectors := make([][]float32, len(inputs))
	seen := make([]bool, len(inputs))
	for position, item := range response.Data {
		index := position
		if item.Index != nil {
			index = *item.Index
		}
		if index < 0 || index >= len(inputs) {
			return EmbeddingBatch{}, fmt.Errorf("openai-compatible embedding response index %d out of range", index)
		}
		if seen[index] {
			return EmbeddingBatch{}, fmt.Errorf("openai-compatible embedding response duplicated index %d", index)
		}
		seen[index] = true
		vectors[index] = item.Embedding
	}
	dimensions, err := inferDimensions(vectors)
	if err != nil {
		return EmbeddingBatch{}, err
	}
	model := response.Model
	if model == "" {
		model = p.model
	}
	return EmbeddingBatch{Model: model, Dimensions: dimensions, Vectors: vectors}, nil
}
