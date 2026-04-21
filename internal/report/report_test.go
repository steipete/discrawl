package report

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/steipete/discrawl/internal/store"
)

func TestBuildRenderAndUpdateReadme(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "discrawl.db"))
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	now := time.Date(2026, 4, 21, 5, 0, 0, 0, time.UTC)
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMember(ctx, store.MemberRecord{GuildID: "g1", UserID: "u1", Username: "peter", DisplayName: "Peter", RoleIDsJSON: `[]`, RawJSON: `{}`}))
	require.NoError(t, s.UpsertMessages(ctx, []store.MessageMutation{{
		Record: store.MessageRecord{
			ID:                "m1",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u1",
			AuthorName:        "Peter",
			MessageType:       0,
			CreatedAt:         now.Add(-time.Hour).Format(time.RFC3339Nano),
			Content:           "shipping the snapshot report",
			NormalizedContent: "shipping the snapshot report",
			RawJSON:           `{}`,
		},
	}}))

	activity, err := Build(ctx, s, Options{Now: now})
	require.NoError(t, err)
	require.Equal(t, 1, activity.TotalMessages)
	require.Equal(t, "general", activity.TopChannels[0].Name)
	require.Equal(t, "Peter", activity.TopAuthors[0].Name)

	section, err := RenderMarkdown(activity)
	require.NoError(t, err)
	require.Contains(t, section, "Discord Activity Report")
	require.Contains(t, section, "Latest archived message: 2026-04-21 04:00 UTC")

	readme := UpdateReadme([]byte("# Backup\n"), section)
	require.Contains(t, string(readme), StartMarker)
	require.Contains(t, string(readme), EndMarker)
	updated := UpdateReadme(readme, "## New\n")
	require.Contains(t, string(updated), "## New")
	require.NotContains(t, string(updated), "Latest archived message")
}

func TestUpdateReadmePreservesAIFieldNotes(t *testing.T) {
	existing := []byte(`# Backup

<!-- discrawl-report:start -->
## Discord Activity Report

Last updated: 2026-04-21 05:00 UTC

### AI Field Notes

- Funny: Build logs learned patience.
- Trend: Activity clustered around launch prep.
<!-- discrawl-report:end -->
`)
	next := `## Discord Activity Report

Last updated: 2026-04-21 06:00 UTC

### AI Field Notes

` + aiDigestPlaceholder + `
`
	updated := string(UpdateReadme(existing, next))
	require.Contains(t, updated, "Last updated: 2026-04-21 06:00 UTC")
	require.Contains(t, updated, "- Funny: Build logs learned patience.")
	require.NotContains(t, updated, aiDigestPlaceholder)
}

func TestUpdateReadmeLetsAIReportReplaceFieldNotes(t *testing.T) {
	existing := []byte(`# Backup

<!-- discrawl-report:start -->
## Discord Activity Report

### AI Field Notes

- Old: keep only until a new AI report lands.
<!-- discrawl-report:end -->
`)
	next := `## Discord Activity Report

### AI Field Notes

- New: fresh report.
`
	updated := string(UpdateReadme(existing, next))
	require.Contains(t, updated, "- New: fresh report.")
	require.NotContains(t, updated, "- Old: keep only until")
}

func TestWriteReadmeCreatesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "README.md")
	require.NoError(t, WriteReadme(path, "## Report\n"))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(data), "## Report")
}

func TestExtractResponseText(t *testing.T) {
	body := []byte(`{"output":[{"content":[{"type":"output_text","text":"hello"}]}]}`)
	require.Equal(t, "hello", extractResponseText(body))
}

func TestGenerateAISummary(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want %s", r.Method, http.MethodPost)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Errorf("authorization = %q", got)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode body: %v", err)
			return
		}
		if body["model"] != "test-model" {
			t.Errorf("model = %v", body["model"])
		}
		input, _ := body["input"].(string)
		if !strings.Contains(input, "Top channels this week") {
			t.Errorf("input missing top channels: %s", input)
		}
		_, _ = w.Write([]byte(`{"output":[{"content":[{"type":"output_text","text":"- Funny: ship it.\n- Trend: busy week.\n- Follow up: check support."}]}]}`))
	}))
	defer server.Close()
	t.Setenv("TEST_OPENAI_KEY", "test-key")

	summary, err := GenerateAISummary(context.Background(), ActivityReport{
		GeneratedAt:     time.Date(2026, 4, 21, 5, 0, 0, 0, time.UTC),
		LatestMessageAt: time.Date(2026, 4, 21, 4, 0, 0, 0, time.UTC),
		TotalMessages:   10,
		TotalChannels:   2,
		TotalMembers:    3,
		Windows:         []WindowStats{{Label: "7 days", Messages: 10, ActiveAuthors: 2, ActiveChannels: 1, Attachments: 1}},
		TopChannels:     []RankedCount{{Name: "general", Count: 10}},
		TopAuthors:      []RankedCount{{Name: "Peter", Count: 5}},
		BusiestDays:     []RankedCount{{Name: "2026-04-21", Count: 10}},
		RecentSamples:   []MessageSample{{Channel: "general", Author: "Peter", Content: "good report"}},
	}, AIOptions{BaseURL: server.URL, APIKeyEnv: "TEST_OPENAI_KEY", Model: "test-model"})
	require.NoError(t, err)
	require.Contains(t, summary, "Funny")
}

func TestGenerateAISummaryMissingKey(t *testing.T) {
	_, err := GenerateAISummary(context.Background(), ActivityReport{}, AIOptions{APIKeyEnv: "MISSING_TEST_OPENAI_KEY"})
	require.ErrorContains(t, err, "MISSING_TEST_OPENAI_KEY")
}

func TestGenerateAISummaryRejectsBadStatusAndEmptyText(t *testing.T) {
	t.Setenv("TEST_OPENAI_KEY", "test-key")
	badStatus := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusTeapot)
	}))
	defer badStatus.Close()
	_, err := GenerateAISummary(context.Background(), ActivityReport{}, AIOptions{BaseURL: badStatus.URL, APIKeyEnv: "TEST_OPENAI_KEY"})
	require.ErrorContains(t, err, "418")

	emptyText := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"output":[{"content":[]}]}`))
	}))
	defer emptyText.Close()
	_, err = GenerateAISummary(context.Background(), ActivityReport{}, AIOptions{BaseURL: emptyText.URL, APIKeyEnv: "TEST_OPENAI_KEY"})
	require.ErrorContains(t, err, "output text")
}

func TestHelpersHandleEmptyAndLongContent(t *testing.T) {
	require.Equal(t, "unknown", escapeMD(""))
	require.Equal(t, "a\\|b", escapeMD("a|b"))
	require.True(t, strings.HasSuffix(clipWhitespace(strings.Repeat("x", 300), 20), "..."))
	require.Equal(t, "n/a", formatTime(time.Time{}))
}
