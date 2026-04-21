package report

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"html"
	"os"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/steipete/discrawl/internal/store"
)

const (
	StartMarker = "<!-- discrawl-report:start -->"
	EndMarker   = "<!-- discrawl-report:end -->"
)

const (
	aiFieldNotesHeading = "### AI Field Notes"
	aiDigestPlaceholder = "_AI digest not generated in this run. The daily report job fills this in when `OPENAI_API_KEY` is configured._"
)

type Options struct {
	Now time.Time
	AI  AIOptions
}

type AIOptions struct {
	Enabled   bool
	Model     string
	APIKeyEnv string
	BaseURL   string
}

type ActivityReport struct {
	GeneratedAt     time.Time
	LatestMessageAt time.Time
	TotalMessages   int
	TotalChannels   int
	TotalMembers    int
	Windows         []WindowStats
	TopChannels     []RankedCount
	TopAuthors      []RankedCount
	BusiestDays     []RankedCount
	RecentSamples   []MessageSample
	AISummary       string
}

type WindowStats struct {
	Label          string
	Since          time.Time
	Messages       int
	ActiveAuthors  int
	ActiveChannels int
	Attachments    int
}

type RankedCount struct {
	Name  string
	Count int
}

type MessageSample struct {
	Channel   string
	Author    string
	Content   string
	CreatedAt time.Time
}

func Build(ctx context.Context, s *store.Store, opts Options) (ActivityReport, error) {
	now := opts.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	report := ActivityReport{GeneratedAt: now.UTC()}
	if err := scanTotals(ctx, s.DB(), &report); err != nil {
		return ActivityReport{}, err
	}
	anchor := report.LatestMessageAt
	if anchor.IsZero() {
		anchor = now
	}
	windows := []struct {
		label string
		dur   time.Duration
	}{
		{"24 hours", 24 * time.Hour},
		{"7 days", 7 * 24 * time.Hour},
		{"30 days", 30 * 24 * time.Hour},
	}
	for _, window := range windows {
		stats, err := scanWindow(ctx, s.DB(), window.label, anchor.Add(-window.dur))
		if err != nil {
			return ActivityReport{}, err
		}
		report.Windows = append(report.Windows, stats)
	}
	weekSince := anchor.Add(-7 * 24 * time.Hour)
	monthSince := anchor.Add(-30 * 24 * time.Hour)
	var err error
	report.TopChannels, err = topChannels(ctx, s.DB(), weekSince, 8)
	if err != nil {
		return ActivityReport{}, err
	}
	report.TopAuthors, err = topAuthors(ctx, s.DB(), weekSince, 8)
	if err != nil {
		return ActivityReport{}, err
	}
	report.BusiestDays, err = busiestDays(ctx, s.DB(), monthSince, 7)
	if err != nil {
		return ActivityReport{}, err
	}
	report.RecentSamples, err = recentSamples(ctx, s.DB(), weekSince, 40)
	if err != nil {
		return ActivityReport{}, err
	}
	if opts.AI.Enabled {
		summary, err := GenerateAISummary(ctx, report, opts.AI)
		if err != nil {
			return ActivityReport{}, err
		}
		report.AISummary = strings.TrimSpace(summary)
	}
	return report, nil
}

func scanTotals(ctx context.Context, db *sql.DB, report *ActivityReport) error {
	var latest sql.NullString
	if err := db.QueryRowContext(ctx, `
		select
			(select count(*) from messages),
			(select count(*) from channels),
			(select count(*) from members),
			(select max(created_at) from messages)
	`).Scan(&report.TotalMessages, &report.TotalChannels, &report.TotalMembers, &latest); err != nil {
		return fmt.Errorf("scan report totals: %w", err)
	}
	report.LatestMessageAt = parseTime(latest.String)
	return nil
}

func scanWindow(ctx context.Context, db *sql.DB, label string, since time.Time) (WindowStats, error) {
	stats := WindowStats{Label: label, Since: since.UTC()}
	if err := db.QueryRowContext(ctx, `
		select
			count(*),
			count(distinct nullif(author_id, '')),
			count(distinct nullif(channel_id, '')),
			coalesce(sum(case when has_attachments then 1 else 0 end), 0)
		from messages
		where created_at >= ?
	`, since.UTC().Format(time.RFC3339Nano)).Scan(&stats.Messages, &stats.ActiveAuthors, &stats.ActiveChannels, &stats.Attachments); err != nil {
		return WindowStats{}, fmt.Errorf("scan %s stats: %w", label, err)
	}
	return stats, nil
}

func topChannels(ctx context.Context, db *sql.DB, since time.Time, limit int) ([]RankedCount, error) {
	return ranked(ctx, db, `
		select coalesce(nullif(c.name, ''), m.channel_id) as name, count(*) as total
		from messages m
		left join channels c on c.id = m.channel_id
		where m.created_at >= ?
		group by m.channel_id, coalesce(nullif(c.name, ''), m.channel_id)
		order by total desc, name asc
		limit ?
	`, since.UTC().Format(time.RFC3339Nano), limit)
}

func topAuthors(ctx context.Context, db *sql.DB, since time.Time, limit int) ([]RankedCount, error) {
	return ranked(ctx, db, `
		select
			coalesce(
				nullif(mem.display_name, ''),
				nullif(mem.nick, ''),
				nullif(mem.global_name, ''),
				nullif(mem.username, ''),
				nullif(json_extract(m.raw_json, '$.author.global_name'), ''),
				nullif(json_extract(m.raw_json, '$.author.username'), ''),
				nullif(m.author_id, ''),
				'unknown'
			) as name,
			count(*) as total
		from messages m
		left join members mem on mem.guild_id = m.guild_id and mem.user_id = m.author_id
		where m.created_at >= ?
		group by m.author_id, name
		order by total desc, name asc
		limit ?
	`, since.UTC().Format(time.RFC3339Nano), limit)
}

func busiestDays(ctx context.Context, db *sql.DB, since time.Time, limit int) ([]RankedCount, error) {
	return ranked(ctx, db, `
		select substr(created_at, 1, 10) as name, count(*) as total
		from messages
		where created_at >= ?
		group by substr(created_at, 1, 10)
		order by total desc, name desc
		limit ?
	`, since.UTC().Format(time.RFC3339Nano), limit)
}

func ranked(ctx context.Context, db *sql.DB, query string, args ...any) ([]RankedCount, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []RankedCount
	for rows.Next() {
		var row RankedCount
		if err := rows.Scan(&row.Name, &row.Count); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func recentSamples(ctx context.Context, db *sql.DB, since time.Time, limit int) ([]MessageSample, error) {
	rows, err := db.QueryContext(ctx, `
		select
			coalesce(nullif(c.name, ''), m.channel_id),
			coalesce(
				nullif(mem.display_name, ''),
				nullif(mem.nick, ''),
				nullif(mem.global_name, ''),
				nullif(mem.username, ''),
				nullif(json_extract(m.raw_json, '$.author.username'), ''),
				nullif(m.author_id, ''),
				'unknown'
			),
			case
				when trim(coalesce(m.content, '')) <> '' then m.content
				else m.normalized_content
			end,
			m.created_at
		from messages m
		left join channels c on c.id = m.channel_id
		left join members mem on mem.guild_id = m.guild_id and mem.user_id = m.author_id
		where m.created_at >= ?
		  and trim(coalesce(m.normalized_content, m.content, '')) <> ''
		order by m.created_at desc, m.id desc
		limit ?
	`, since.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []MessageSample
	for rows.Next() {
		var row MessageSample
		var created string
		if err := rows.Scan(&row.Channel, &row.Author, &row.Content, &created); err != nil {
			return nil, err
		}
		row.CreatedAt = parseTime(created)
		row.Content = clipWhitespace(row.Content, 220)
		out = append(out, row)
	}
	return out, rows.Err()
}

func RenderMarkdown(report ActivityReport) (string, error) {
	var body bytes.Buffer
	if err := reportTemplate.Execute(&body, report); err != nil {
		return "", err
	}
	return strings.TrimSpace(body.String()) + "\n", nil
}

func UpdateReadme(readme []byte, section string) []byte {
	section = strings.TrimSpace(section)
	text := string(readme)
	start := strings.Index(text, StartMarker)
	end := strings.Index(text, EndMarker)
	if start >= 0 && end >= start {
		existingSection := text[start+len(StartMarker) : end]
		section = preserveAIFieldNotes(existingSection, section)
		end += len(EndMarker)
		replacement := StartMarker + "\n" + section + "\n" + EndMarker
		return []byte(text[:start] + replacement + text[end:])
	}
	replacement := StartMarker + "\n" + section + "\n" + EndMarker
	text = strings.TrimRight(text, "\n")
	if text == "" {
		return []byte(replacement + "\n")
	}
	return []byte(text + "\n\n" + replacement + "\n")
}

func preserveAIFieldNotes(existingSection string, nextSection string) string {
	if !strings.Contains(nextSection, aiDigestPlaceholder) {
		return nextSection
	}
	existingNotes := extractAIFieldNotes(existingSection)
	if existingNotes == "" || strings.Contains(existingNotes, aiDigestPlaceholder) {
		return nextSection
	}
	return replaceAIFieldNotes(nextSection, existingNotes)
}

func extractAIFieldNotes(section string) string {
	idx := strings.Index(section, aiFieldNotesHeading)
	if idx < 0 {
		return ""
	}
	notes := section[idx+len(aiFieldNotesHeading):]
	if next := strings.Index(notes, "\n### "); next >= 0 {
		notes = notes[:next]
	}
	return strings.TrimSpace(notes)
}

func replaceAIFieldNotes(section string, notes string) string {
	idx := strings.Index(section, aiFieldNotesHeading)
	if idx < 0 {
		return section
	}
	start := idx + len(aiFieldNotesHeading)
	tail := section[start:]
	end := len(tail)
	if next := strings.Index(tail, "\n### "); next >= 0 {
		end = next
	}
	return section[:start] + "\n\n" + strings.TrimSpace(notes) + strings.TrimRight(tail[end:], "\n")
}

func WriteReadme(path string, section string) error {
	current, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	updated := UpdateReadme(current, section)
	return os.WriteFile(path, updated, 0o600)
}

func MarkdownTable(rows []RankedCount, nameTitle string) string {
	if len(rows) == 0 {
		return "_No activity._"
	}
	var b strings.Builder
	_, _ = fmt.Fprintf(&b, "| %s | Messages |\n| --- | ---: |\n", nameTitle)
	for _, row := range rows {
		_, _ = fmt.Fprintf(&b, "| %s | %s |\n", escapeMD(row.Name), formatInt(row.Count))
	}
	return strings.TrimRight(b.String(), "\n")
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return "n/a"
	}
	return t.UTC().Format("2006-01-02 15:04 UTC")
}

func formatInt(v int) string {
	return strconv.FormatInt(int64(v), 10)
}

func escapeMD(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "unknown"
	}
	s = html.EscapeString(s)
	s = strings.ReplaceAll(s, "|", "\\|")
	return s
}

func clipWhitespace(s string, limit int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) <= limit {
		return s
	}
	return strings.TrimSpace(s[:limit]) + "..."
}

func parseTime(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05"} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

var reportTemplate = template.Must(template.New("report").Funcs(template.FuncMap{
	"formatTime":    formatTime,
	"formatInt":     formatInt,
	"rankedTable":   MarkdownTable,
	"escapeMD":      escapeMD,
	"sampleContent": clipWhitespace,
}).Parse(`## Discord Activity Report

Last updated: {{ formatTime .GeneratedAt }}
Latest archived message: {{ formatTime .LatestMessageAt }}

Archive size: {{ formatInt .TotalMessages }} messages, {{ formatInt .TotalChannels }} channels, {{ formatInt .TotalMembers }} members.

### Activity

| Window | Messages | Active people | Active channels | Attachments |
| --- | ---: | ---: | ---: | ---: |
{{- range .Windows }}
| Last {{ .Label }} | {{ formatInt .Messages }} | {{ formatInt .ActiveAuthors }} | {{ formatInt .ActiveChannels }} | {{ formatInt .Attachments }} |
{{- end }}

### Hot Channels This Week

{{ rankedTable .TopChannels "Channel" }}

### Top Posters This Week

{{ rankedTable .TopAuthors "Person" }}

### Busiest Days This Month

{{ rankedTable .BusiestDays "Day" }}

### AI Field Notes

{{- if .AISummary }}
{{ .AISummary }}
{{- else }}
` + aiDigestPlaceholder + `
{{- end }}
`))
