package report

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	defaultAIModel     = "gpt-5.2"
	defaultAIKeyEnv    = "OPENAI_API_KEY"
	defaultAIBaseURL   = "https://api.openai.com/v1/responses"
	defaultHTTPTimeout = 45 * time.Second
)

func GenerateAISummary(ctx context.Context, report ActivityReport, opts AIOptions) (string, error) {
	model := strings.TrimSpace(opts.Model)
	if model == "" {
		model = defaultAIModel
	}
	keyEnv := strings.TrimSpace(opts.APIKeyEnv)
	if keyEnv == "" {
		keyEnv = defaultAIKeyEnv
	}
	apiKey := strings.TrimSpace(os.Getenv(keyEnv))
	if apiKey == "" {
		return "", fmt.Errorf("%s is not set", keyEnv)
	}
	baseURL := strings.TrimSpace(opts.BaseURL)
	if baseURL == "" {
		baseURL = defaultAIBaseURL
	}

	body := map[string]any{
		"model": model,
		"instructions": strings.Join([]string{
			"You write a short private Discord archive field report.",
			"Tone: funny, warm, dry, and useful. No cringe. No bullying individual people.",
			"Use only the provided statistics and message samples.",
			"Do not expose secrets, tokens, private URLs, or raw IDs.",
			"Return Markdown with 3 bullets: one funny observation, one useful trend, one thing worth following up.",
		}, " "),
		"input":             aiInput(report),
		"max_output_tokens": 500,
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL, bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: defaultHTTPTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("openai response %s: %s", resp.Status, strings.TrimSpace(string(respBody)))
	}
	text := extractResponseText(respBody)
	if strings.TrimSpace(text) == "" {
		return "", fmt.Errorf("openai response did not contain output text")
	}
	return text, nil
}

func aiInput(report ActivityReport) string {
	var b strings.Builder
	_, _ = fmt.Fprintf(&b, "Generated at: %s\nLatest message: %s\nArchive totals: %d messages, %d channels, %d members\n\n",
		formatTime(report.GeneratedAt), formatTime(report.LatestMessageAt), report.TotalMessages, report.TotalChannels, report.TotalMembers)
	_, _ = fmt.Fprintln(&b, "Activity windows:")
	for _, window := range report.Windows {
		_, _ = fmt.Fprintf(&b, "- last %s: %d messages, %d people, %d channels, %d attachments\n", window.Label, window.Messages, window.ActiveAuthors, window.ActiveChannels, window.Attachments)
	}
	writeRanks := func(title string, rows []RankedCount) {
		_, _ = fmt.Fprintf(&b, "\n%s:\n", title)
		for _, row := range rows {
			_, _ = fmt.Fprintf(&b, "- %s: %d\n", row.Name, row.Count)
		}
	}
	writeRanks("Top channels this week", report.TopChannels)
	writeRanks("Top posters this week", report.TopAuthors)
	writeRanks("Busiest days this month", report.BusiestDays)
	_, _ = fmt.Fprintln(&b, "\nRecent message samples:")
	for _, sample := range report.RecentSamples {
		_, _ = fmt.Fprintf(&b, "- [%s] %s: %s\n", sample.Channel, sample.Author, sample.Content)
	}
	return b.String()
}

func extractResponseText(body []byte) string {
	var decoded struct {
		Output []struct {
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"output"`
		OutputText string `json:"output_text"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return ""
	}
	if strings.TrimSpace(decoded.OutputText) != "" {
		return decoded.OutputText
	}
	var parts []string
	for _, output := range decoded.Output {
		for _, content := range output.Content {
			if strings.TrimSpace(content.Text) != "" {
				parts = append(parts, content.Text)
			}
		}
	}
	return strings.Join(parts, "\n\n")
}
