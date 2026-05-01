package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/steipete/discrawl/internal/config"
	discordclient "github.com/steipete/discrawl/internal/discord"
	"github.com/steipete/discrawl/internal/share"
	"github.com/steipete/discrawl/internal/store"
	"github.com/steipete/discrawl/internal/syncer"
)

func TestHelpAndVersion(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	require.NoError(t, Run(context.Background(), []string{"help"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "discrawl")

	out.Reset()
	require.NoError(t, Run(context.Background(), []string{"--version"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "0.6.2")

	err := Run(context.Background(), []string{"bogus"}, &out, &bytes.Buffer{})
	require.Equal(t, 2, ExitCode(err))
}

func TestStatusSearchSQLAndListings(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.DefaultGuildID = "g1"
	require.NoError(t, config.Write(cfgPath, cfg))

	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMember(ctx, store.MemberRecord{
		GuildID:     "g1",
		UserID:      "u1",
		Username:    "peter",
		DisplayName: "Peter",
		RoleIDsJSON: `[]`,
		RawJSON:     `{"bio":"Maintainer","github":"steipete","website":"https://steipete.me"}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "panic locked database",
		NormalizedContent: "panic locked database",
		RawJSON:           `{}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "m2",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Add(time.Second).Format(time.RFC3339Nano),
		Content:           "",
		NormalizedContent: "",
		RawJSON:           `{"author":{"username":"Peter"}}`,
	}))
	require.NoError(t, s.UpsertMessages(ctx, []store.MessageMutation{{
		Record: store.MessageRecord{
			ID:                "m3",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u1",
			AuthorName:        "Peter",
			MessageType:       0,
			CreatedAt:         time.Now().UTC().Add(2 * time.Second).Format(time.RFC3339Nano),
			Content:           "",
			NormalizedContent: "trace.txt stack trace line one",
			HasAttachments:    true,
			RawJSON:           `{"author":{"username":"Peter"}}`,
		},
		Mentions: []store.MentionEventRecord{{
			MessageID:  "m3",
			GuildID:    "g1",
			ChannelID:  "c1",
			AuthorID:   "u1",
			TargetType: "user",
			TargetID:   "u2",
			TargetName: "Shadow",
			EventAt:    time.Now().UTC().Add(2 * time.Second).Format(time.RFC3339Nano),
		}},
	}}))
	require.NoError(t, s.Close())

	tests := [][]string{
		{"--config", cfgPath, "status"},
		{"--config", cfgPath, "search", "panic"},
		{"--config", cfgPath, "search", "stack"},
		{"--config", cfgPath, "search", "--include-empty", "Peter"},
		{"--config", cfgPath, "messages", "--channel", "general", "--days", "7", "--all"},
		{"--config", cfgPath, "messages", "--channel", "general", "--days", "7", "--all", "--include-empty"},
		{"--config", cfgPath, "mentions", "--target", "Shadow", "--limit", "10"},
		{"--config", cfgPath, "sql", "select count(*) as total from messages"},
		{"--config", cfgPath, "members", "list"},
		{"--config", cfgPath, "members", "search", "Maintainer"},
		{"--config", cfgPath, "members", "show", "u1"},
		{"--config", cfgPath, "channels", "list"},
		{"--config", cfgPath, "report"},
	}
	for _, args := range tests {
		var out bytes.Buffer
		require.NoError(t, Run(ctx, args, &out, &bytes.Buffer{}))
		require.NotEmpty(t, out.String())
	}
}

func TestWiretapImportsDesktopDirectMessages(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")
	desktopPath := filepath.Join(dir, "discord")
	require.NoError(t, os.MkdirAll(filepath.Join(desktopPath, "IndexedDB"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(desktopPath, "IndexedDB", "000001.log"), []byte(`{"id":"111111111111111111","type":1,"recipients":[{"id":"222222222222222222","username":"alice","global_name":"Alice"}]}
{"id":"333333333333333333","channel_id":"111111111111111111","content":"secret DM launch plan","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222222","username":"alice","global_name":"Alice"}}`), 0o600))

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.Desktop.Path = desktopPath
	cfg.Discord.TokenSource = "none"
	require.NoError(t, config.Write(cfgPath, cfg))

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "wiretap"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "messages=1")

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "search", "launch"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "secret DM launch plan")
	require.Contains(t, out.String(), "@me")

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "dms"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "Alice")
	require.Contains(t, out.String(), "111111111111111111")

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "dms", "--with", "Alice", "--last", "1"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "secret DM launch plan")
	require.Contains(t, out.String(), "@me")

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "search", "--dm", "launch"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "secret DM launch plan")

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "messages", "--dm", "--channel", "Alice", "--last", "1"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "secret DM launch plan")
}

func TestParseMessageWindow(t *testing.T) {
	rt := &runtime{now: func() time.Time {
		return time.Date(2026, 4, 24, 12, 0, 0, 0, time.UTC)
	}}

	since, before, err := rt.parseMessageWindow(6, 0, "", "")
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, 4, 24, 6, 0, 0, 0, time.UTC), since)
	require.True(t, before.IsZero())

	since, before, err = rt.parseMessageWindow(0, 2, "", "")
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC), since)
	require.True(t, before.IsZero())

	since, before, err = rt.parseMessageWindow(0, 0, "2026-04-20T10:00:00Z", "2026-04-21T10:00:00Z")
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, 4, 20, 10, 0, 0, 0, time.UTC), since)
	require.Equal(t, time.Date(2026, 4, 21, 10, 0, 0, 0, time.UTC), before)

	_, _, err = rt.parseMessageWindow(0, 0, "bad", "")
	require.Equal(t, 2, ExitCode(err))
	_, _, err = rt.parseMessageWindow(0, 0, "", "bad")
	require.Equal(t, 2, ExitCode(err))
}

func TestWiretapAndSearchWorkWithoutConfig(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	home := filepath.Join(dir, "home")
	desktopPath := filepath.Join(dir, "discord")
	require.NoError(t, os.MkdirAll(filepath.Join(desktopPath, "IndexedDB"), 0o755))
	require.NoError(t, os.MkdirAll(home, 0o755))
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	require.NoError(t, os.WriteFile(filepath.Join(desktopPath, "IndexedDB", "000001.log"), []byte(`{"id":"111111111111111112","type":1,"recipients":[{"id":"222222222222222223","username":"alice","global_name":"Alice"}]}
{"id":"333333333333333334","channel_id":"111111111111111112","content":"local-only DM import","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222223","username":"alice","global_name":"Alice"}}`), 0o600))

	cfgPath := filepath.Join(dir, "missing.toml")
	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "wiretap", "--path", desktopPath}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "messages=1")

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "search", "local-only"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "local-only DM import")
	require.Contains(t, out.String(), "@me")
}

func TestSyncWiretapSourceImportsDesktopMessages(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")
	desktopPath := filepath.Join(dir, "discord")
	require.NoError(t, os.MkdirAll(filepath.Join(desktopPath, "IndexedDB"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(desktopPath, "IndexedDB", "000001.log"), []byte(`{"id":"111111111111111117","type":1,"recipients":[{"id":"222222222222222228","username":"alice","global_name":"Alice"}]}
{"id":"333333333333333339","channel_id":"111111111111111117","content":"sync wiretap import","timestamp":"2026-04-23T18:20:43Z","author":{"id":"222222222222222228","username":"alice","global_name":"Alice"}}`), 0o600))

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.Desktop.Path = desktopPath
	cfg.Discord.TokenSource = "none"
	require.NoError(t, config.Write(cfgPath, cfg))

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "sync", "--source", "wiretap"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "dm_messages=1")

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "search", "sync wiretap"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "sync wiretap import")
	require.Contains(t, out.String(), "@me")
}

func TestParseSyncSources(t *testing.T) {
	for _, tc := range []struct {
		raw     string
		name    string
		discord bool
		wiretap bool
	}{
		{"", "both", true, true},
		{"both", "both", true, true},
		{"key", "discord", true, false},
		{"discord", "discord", true, false},
		{"wiretap", "wiretap", false, true},
		{"key+wiretap", "both", true, true},
	} {
		sources, err := parseSyncSources(tc.raw)
		require.NoError(t, err)
		require.Equal(t, tc.name, sources.name)
		require.Equal(t, tc.discord, sources.discord)
		require.Equal(t, tc.wiretap, sources.wiretap)
	}
	_, err := parseSyncSources("nope")
	require.Error(t, err)
}

func TestReadCommandsAutoImportStaleShare(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourceDB := filepath.Join(dir, "source.db")
	source := seedCLIStore(t, sourceDB)
	defer func() { _ = source.Close() }()

	workRepo := filepath.Join(dir, "work")
	remoteRepo := filepath.Join(dir, "remote.git")
	opts := share.Options{RepoPath: workRepo, Branch: "main"}
	_, err := share.Export(ctx, source, opts)
	require.NoError(t, err)
	runGit(t, workRepo, "config", "user.name", "discrawl test")
	runGit(t, workRepo, "config", "user.email", "discrawl@example.com")
	committed, err := share.Commit(ctx, opts, "test: snapshot")
	require.NoError(t, err)
	require.True(t, committed)
	runGit(t, dir, "init", "--bare", remoteRepo)
	runGit(t, workRepo, "remote", "add", "origin", remoteRepo)
	runGit(t, workRepo, "push", "-u", "origin", "main")

	cfgPath := filepath.Join(dir, "config.toml")
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "reader.db")
	cfg.Share.Remote = remoteRepo
	cfg.Share.RepoPath = filepath.Join(dir, "reader-share")
	cfg.Share.StaleAfter = "15m"
	cfg.Share.AutoUpdate = true
	require.NoError(t, config.Write(cfgPath, cfg))

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "search", "automatic"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "automatic updates work")

	reader, err := store.Open(ctx, cfg.DBPath)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
	lastImport, err := reader.GetSyncState(ctx, share.LastImportSyncScope)
	require.NoError(t, err)
	require.NotEmpty(t, lastImport)
}

func TestReadCommandsCanDisableAutoImportWithEnv(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	sourceDB := filepath.Join(dir, "source.db")
	source := seedCLIStore(t, sourceDB)
	defer func() { _ = source.Close() }()

	workRepo := filepath.Join(dir, "work")
	remoteRepo := filepath.Join(dir, "remote.git")
	opts := share.Options{RepoPath: workRepo, Branch: "main"}
	_, err := share.Export(ctx, source, opts)
	require.NoError(t, err)
	runGit(t, workRepo, "config", "user.name", "discrawl test")
	runGit(t, workRepo, "config", "user.email", "discrawl@example.com")
	committed, err := share.Commit(ctx, opts, "test: snapshot")
	require.NoError(t, err)
	require.True(t, committed)
	runGit(t, dir, "init", "--bare", remoteRepo)
	runGit(t, workRepo, "remote", "add", "origin", remoteRepo)
	runGit(t, workRepo, "push", "-u", "origin", "main")

	cfgPath := filepath.Join(dir, "config.toml")
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "reader.db")
	cfg.Share.Remote = remoteRepo
	cfg.Share.RepoPath = filepath.Join(dir, "reader-share")
	cfg.Share.StaleAfter = "15m"
	cfg.Share.AutoUpdate = true
	require.NoError(t, config.Write(cfgPath, cfg))

	t.Setenv("DISCRAWL_NO_AUTO_UPDATE", "1")
	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "search", "automatic"}, &out, &bytes.Buffer{}))
	require.NotContains(t, out.String(), "automatic updates work")

	reader, err := store.Open(ctx, cfg.DBPath)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
	lastImport, err := reader.GetSyncState(ctx, share.LastImportSyncScope)
	require.NoError(t, err)
	require.Empty(t, lastImport)
}

func TestShareCommandsPublishSubscribeAndUpdate(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	remoteRepo := filepath.Join(dir, "remote.git")
	runGit(t, dir, "init", "--bare", remoteRepo)

	cfgPath := filepath.Join(dir, "config.toml")
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "publisher.db")
	cfg.Share.Remote = remoteRepo
	cfg.Share.RepoPath = filepath.Join(dir, "publisher-share")
	require.NoError(t, config.Write(cfgPath, cfg))
	publisher := seedCLIStore(t, cfg.DBPath)
	require.NoError(t, publisher.Close())

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath,
		"publish",
		"--repo", cfg.Share.RepoPath,
		"--remote", remoteRepo,
		"--readme", filepath.Join(cfg.Share.RepoPath, "README.md"),
		"--no-commit",
	}, &out, &bytes.Buffer{}))
	require.FileExists(t, filepath.Join(cfg.Share.RepoPath, share.ManifestName))
	require.FileExists(t, filepath.Join(cfg.Share.RepoPath, "README.md"))

	runGit(t, cfg.Share.RepoPath, "config", "user.name", "discrawl test")
	runGit(t, cfg.Share.RepoPath, "config", "user.email", "discrawl@example.com")
	committed, err := share.Commit(ctx, share.Options{RepoPath: cfg.Share.RepoPath, Remote: remoteRepo, Branch: "main"}, "test: snapshot")
	require.NoError(t, err)
	require.True(t, committed)
	require.NoError(t, share.Push(ctx, share.Options{RepoPath: cfg.Share.RepoPath, Remote: remoteRepo, Branch: "main"}))

	readerCfgPath := filepath.Join(dir, "reader.toml")
	require.NoError(t, Run(ctx, []string{
		"--config", readerCfgPath,
		"subscribe",
		"--repo", filepath.Join(dir, "reader-share"),
		"--no-import",
		remoteRepo,
	}, &bytes.Buffer{}, &bytes.Buffer{}))
	readerCfg, err := config.Load(readerCfgPath)
	require.NoError(t, err)
	require.Equal(t, remoteRepo, readerCfg.Share.Remote)
	require.Equal(t, "none", readerCfg.Discord.TokenSource)

	readerCfg.DBPath = filepath.Join(dir, "reader.db")
	require.NoError(t, config.Write(readerCfgPath, readerCfg))
	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", readerCfgPath, "update"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "generated_at")
	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", readerCfgPath, "search", "automatic"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "automatic updates work")
}

func TestShareCommandsRoundTripEmbeddings(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	remoteRepo := filepath.Join(dir, "remote.git")
	runGit(t, dir, "init", "--bare", remoteRepo)

	cfgPath := filepath.Join(dir, "publisher.toml")
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "publisher.db")
	cfg.Share.Remote = remoteRepo
	cfg.Share.RepoPath = filepath.Join(dir, "publisher-share")
	cfg.Search.Embeddings.Enabled = true
	cfg.Search.Embeddings.Provider = "openai_compatible"
	cfg.Search.Embeddings.Model = "local-model"
	cfg.Search.Embeddings.APIKeyEnv = ""
	require.NoError(t, config.Write(cfgPath, cfg))

	publisher := seedCLIStore(t, cfg.DBPath)
	require.NoError(t, insertCLIEmbedding(ctx, publisher, "m100", "openai_compatible", "local-model", []float32{1, 0}))
	require.NoError(t, publisher.Close())
	require.NoError(t, os.MkdirAll(cfg.Share.RepoPath, 0o755))
	runGit(t, cfg.Share.RepoPath, "init")
	runGit(t, cfg.Share.RepoPath, "checkout", "-B", "main")
	runGit(t, cfg.Share.RepoPath, "config", "user.name", "discrawl test")
	runGit(t, cfg.Share.RepoPath, "config", "user.email", "discrawl@example.com")
	runGit(t, cfg.Share.RepoPath, "remote", "add", "origin", remoteRepo)

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath,
		"publish",
		"--repo", cfg.Share.RepoPath,
		"--remote", remoteRepo,
		"--with-embeddings",
		"--push",
	}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "embeddings=[")

	readerCfgPath := filepath.Join(dir, "reader.toml")
	readerCfg := config.Default()
	readerCfg.DBPath = filepath.Join(dir, "reader.db")
	readerCfg.Search.Embeddings.Enabled = true
	readerCfg.Search.Embeddings.Provider = "openai_compatible"
	readerCfg.Search.Embeddings.Model = "local-model"
	readerCfg.Search.Embeddings.APIKeyEnv = ""
	require.NoError(t, config.Write(readerCfgPath, readerCfg))

	out.Reset()
	require.NoError(t, Run(ctx, []string{
		"--config", readerCfgPath,
		"subscribe",
		"--repo", filepath.Join(dir, "reader-share"),
		"--with-embeddings",
		remoteRepo,
	}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "imported=true")
	require.Contains(t, out.String(), "embeddings=[")

	reader, err := store.Open(ctx, readerCfg.DBPath)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
	_, rows, err := reader.ReadOnlyQuery(ctx, "select provider, model, count(*) from message_embeddings group by provider, model")
	require.NoError(t, err)
	require.Equal(t, [][]string{{"openai_compatible", "local-model", "1"}}, rows)
}

func TestSubscribeGitOnlyModeNeedsNoDiscordCredentials(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	remoteRepo := filepath.Join(dir, "remote.git")
	runGit(t, dir, "init", "--bare", remoteRepo)

	publisherDB := filepath.Join(dir, "publisher.db")
	publisher := seedCLIStore(t, publisherDB)
	defer func() { _ = publisher.Close() }()
	publisherRepo := filepath.Join(dir, "publisher-share")
	opts := share.Options{RepoPath: publisherRepo, Remote: remoteRepo, Branch: "main"}
	_, err := share.Export(ctx, publisher, opts)
	require.NoError(t, err)
	runGit(t, publisherRepo, "config", "user.name", "discrawl test")
	runGit(t, publisherRepo, "config", "user.email", "discrawl@example.com")
	committed, err := share.Commit(ctx, opts, "test: snapshot")
	require.NoError(t, err)
	require.True(t, committed)
	require.NoError(t, share.Push(ctx, opts))

	cfgPath := filepath.Join(dir, "reader.toml")
	readerDB := filepath.Join(dir, "reader.db")
	readerCfg := config.Default()
	readerCfg.DBPath = readerDB
	require.NoError(t, config.Write(cfgPath, readerCfg))
	t.Setenv(config.DefaultTokenEnv, "")
	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{
		"--config", cfgPath,
		"subscribe",
		"--repo", filepath.Join(dir, "reader-share"),
		"--stale-after", "1m",
		remoteRepo,
	}, &out, &bytes.Buffer{}))

	cfg, err := config.Load(cfgPath)
	require.NoError(t, err)
	require.Equal(t, "none", cfg.Discord.TokenSource)
	require.True(t, cfg.Share.AutoUpdate)
	require.Equal(t, "1m", cfg.Share.StaleAfter)

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "search", "automatic"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "automatic updates work")

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "doctor"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "disabled (git share mode)")

	err = Run(ctx, []string{"--config", cfgPath, "sync", "--all"}, &out, &bytes.Buffer{})
	require.Equal(t, 4, ExitCode(err))
	require.Contains(t, err.Error(), "discord token disabled")
}

func TestShareUpdateImportsNewRemoteSnapshot(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	remoteRepo := filepath.Join(dir, "remote.git")
	runGit(t, dir, "init", "--bare", remoteRepo)

	publisherDB := filepath.Join(dir, "publisher.db")
	publisher := seedCLIStore(t, publisherDB)
	defer func() { _ = publisher.Close() }()
	publisherRepo := filepath.Join(dir, "publisher-share")
	opts := share.Options{RepoPath: publisherRepo, Remote: remoteRepo, Branch: "main"}
	publishSnapshot(t, ctx, publisher, opts, "test: old snapshot")

	readerCfgPath := filepath.Join(dir, "reader.toml")
	readerCfg := config.Default()
	readerCfg.DBPath = filepath.Join(dir, "reader.db")
	readerCfg.Share.Remote = remoteRepo
	readerCfg.Share.RepoPath = filepath.Join(dir, "reader-share")
	readerCfg.Share.AutoUpdate = true
	readerCfg.Share.StaleAfter = "15m"
	readerCfg.Discord.TokenSource = "none"
	require.NoError(t, config.Write(readerCfgPath, readerCfg))

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", readerCfgPath, "update"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "imported=true")
	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", readerCfgPath, "update"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "imported=false")
	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", readerCfgPath, "search", "automatic"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "automatic updates work")

	require.NoError(t, publisher.UpsertMessage(ctx, store.MessageRecord{
		ID:                "m200",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano),
		Content:           "newer git snapshot arrived",
		NormalizedContent: "newer git snapshot arrived",
		RawJSON:           `{}`,
	}))
	publishSnapshot(t, ctx, publisher, opts, "test: new snapshot")

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", readerCfgPath, "update"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "imported=true")
	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", readerCfgPath, "search", "newer snapshot"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "newer git snapshot arrived")
}

func TestSyncImportsGitShareBeforeLiveDiscord(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	remoteRepo := filepath.Join(dir, "remote.git")
	runGit(t, dir, "init", "--bare", remoteRepo)

	publisherDB := filepath.Join(dir, "publisher.db")
	publisher := seedCLIStore(t, publisherDB)
	defer func() { _ = publisher.Close() }()
	publisherRepo := filepath.Join(dir, "publisher-share")
	opts := share.Options{RepoPath: publisherRepo, Remote: remoteRepo, Branch: "main"}
	publishSnapshot(t, ctx, publisher, opts, "test: git snapshot")

	cfgPath := filepath.Join(dir, "config.toml")
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "reader.db")
	cfg.DefaultGuildID = "g1"
	cfg.Share.Remote = remoteRepo
	cfg.Share.RepoPath = filepath.Join(dir, "reader-share")
	cfg.Share.AutoUpdate = true
	cfg.Share.StaleAfter = "15m"
	require.NoError(t, config.Write(cfgPath, cfg))

	hybrid := &hybridSyncService{}
	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		stdout:     &bytes.Buffer{},
		stderr:     &bytes.Buffer{},
		logger:     discardLogger(),
		openStore:  store.Open,
		newDiscord: func(config.Config) (discordClient, error) {
			return &fakeDiscordClient{guilds: []*discordgo.UserGuild{{ID: "g1"}}, self: &discordgo.User{ID: "bot"}}, nil
		},
		newSyncer: func(_ syncer.Client, s *store.Store, _ *slog.Logger) syncService {
			hybrid.store = s
			return hybrid
		},
	}

	require.NoError(t, rt.dispatch([]string{"sync", "--all"}))
	require.True(t, hybrid.sawGitMessage)

	reader, err := store.Open(ctx, cfg.DBPath)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
	rows, err := reader.ListMessages(ctx, store.MessageListOptions{Channel: "general", IncludeEmpty: true})
	require.NoError(t, err)
	contents := make([]string, 0, len(rows))
	for _, row := range rows {
		contents = append(contents, row.Content)
	}
	require.Contains(t, contents, "automatic updates work")
	require.Contains(t, contents, "live discord filled the delta")
}

func TestSyncLockSerializesConcurrentRuns(t *testing.T) {
	if goruntime.GOOS == "windows" {
		t.Skip("sync lock is currently a no-op on Windows")
	}
	ctx := context.Background()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "discrawl.db")
	cfgPath := filepath.Join(dir, "config.toml")
	require.NoError(t, config.Write(cfgPath, cfg))

	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		cfg:        cfg,
	}
	firstRelease, err := acquireSyncLock(ctx, filepath.Join(dir, ".discrawl-sync.lock"))
	require.NoError(t, err)
	defer func() { _ = firstRelease() }()

	waitCtx, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
	defer cancel()
	rt.ctx = waitCtx
	err = rt.withSyncLock(func() error { return nil })
	require.ErrorIs(t, err, context.DeadlineExceeded)

	waitCtx, cancel = context.WithTimeout(ctx, 25*time.Millisecond)
	defer cancel()
	rt.ctx = waitCtx
	err = rt.dispatch([]string{"update"})
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func seedCLIStore(t *testing.T, path string) *store.Store {
	t.Helper()
	ctx := context.Background()
	s, err := store.Open(ctx, path)
	require.NoError(t, err)
	now := time.Now().UTC()
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "m100",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         now.Format(time.RFC3339Nano),
		Content:           "automatic updates work",
		NormalizedContent: "automatic updates work",
		RawJSON:           `{}`,
	}))
	return s
}

func publishSnapshot(t *testing.T, ctx context.Context, s *store.Store, opts share.Options, message string) {
	t.Helper()
	_, err := share.Export(ctx, s, opts)
	require.NoError(t, err)
	runGit(t, opts.RepoPath, "config", "user.name", "discrawl test")
	runGit(t, opts.RepoPath, "config", "user.email", "discrawl@example.com")
	committed, err := share.Commit(ctx, opts, message)
	require.NoError(t, err)
	require.True(t, committed)
	require.NoError(t, share.Push(ctx, opts))
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	// #nosec G204 -- fixed git argv in test setup.
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
}

func TestEmbedCommandDrainsBoundedBacklog(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/embeddings", r.URL.Path)
		var req struct {
			Input []string `json:"input"`
		}
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		assert.Len(t, req.Input, 1)
		_, _ = w.Write([]byte(`{"data":[{"index":0,"embedding":[1,2]}]}`))
	}))
	defer server.Close()

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.Search.Embeddings.Enabled = true
	cfg.Search.Embeddings.Provider = "openai_compatible"
	cfg.Search.Embeddings.Model = "local-model"
	cfg.Search.Embeddings.BaseURL = server.URL
	cfg.Search.Embeddings.APIKeyEnv = ""
	require.NoError(t, config.Write(cfgPath, cfg))

	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	for _, id := range []string{"m1", "m2"} {
		require.NoError(t, s.UpsertMessageWithOptions(ctx, store.MessageRecord{
			ID:                id,
			GuildID:           "g1",
			ChannelID:         "c1",
			MessageType:       0,
			CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
			Content:           "hello",
			NormalizedContent: "hello",
			RawJSON:           `{}`,
		}, store.WriteOptions{EnqueueEmbedding: true}))
	}
	require.NoError(t, s.Close())

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "embed", "--limit", "1"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "processed=1")
	require.Contains(t, out.String(), "succeeded=1")
	require.Contains(t, out.String(), "remaining_backlog=1")
	require.Contains(t, out.String(), "provider=openai_compatible")

	s, err = store.Open(ctx, dbPath)
	require.NoError(t, err)
	_, rows, err := s.ReadOnlyQuery(ctx, "select count(*) from message_embeddings")
	require.NoError(t, err)
	require.Equal(t, "1", rows[0][0])
	require.NoError(t, s.Close())

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "embed", "--rebuild", "--limit", "1"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "processed=1")
	require.Contains(t, out.String(), "succeeded=1")
	require.Contains(t, out.String(), "remaining_backlog=1")
	require.Contains(t, out.String(), "requeued=2")
}

func TestSearchSemanticCommandUsesStoredEmbeddings(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		assert.Equal(t, "/embeddings", r.URL.Path)
		var req struct {
			Model string   `json:"model"`
			Input []string `json:"input"`
		}
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		assert.Equal(t, "local-model", req.Model)
		assert.Equal(t, []string{"cats"}, req.Input)
		_, _ = w.Write([]byte(`{"model":"local-model","data":[{"index":0,"embedding":[1,0]}]}`))
	}))
	defer server.Close()

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.Search.DefaultMode = "semantic"
	cfg.Search.Embeddings.Enabled = true
	cfg.Search.Embeddings.Provider = "openai_compatible"
	cfg.Search.Embeddings.Model = "local-model"
	cfg.Search.Embeddings.BaseURL = server.URL
	cfg.Search.Embeddings.APIKeyEnv = ""
	require.NoError(t, config.Write(cfgPath, cfg))

	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	base := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Alice",
		MessageType:       0,
		CreatedAt:         base.Format(time.RFC3339Nano),
		Content:           "database migration discussion",
		NormalizedContent: "database migration discussion",
		RawJSON:           `{"author":{"username":"Alice"}}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "m2",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u2",
		AuthorName:        "Bob",
		MessageType:       0,
		CreatedAt:         base.Add(time.Minute).Format(time.RFC3339Nano),
		Content:           "cats in semantic search",
		NormalizedContent: "cats in semantic search",
		RawJSON:           `{"author":{"username":"Bob"}}`,
	}))
	require.NoError(t, insertCLIEmbedding(ctx, s, "m1", "openai_compatible", "local-model", []float32{1, 0}))
	require.NoError(t, insertCLIEmbedding(ctx, s, "m2", "openai_compatible", "local-model", []float32{0.8, 0.2}))
	require.NoError(t, s.Close())

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "search", "--limit", "1", "cats"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "database migration discussion")
	require.NotContains(t, out.String(), "cats in semantic search")
	require.Equal(t, 1, requests)

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "search", "--mode", "semantic", "--channel", "general", "--author", "Alice", "cats"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "database migration discussion")
	require.NotContains(t, out.String(), "cats in semantic search")
	require.Equal(t, 2, requests)
}

func TestSearchHybridCommandFusesResults(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/embeddings", r.URL.Path)
		var req struct {
			Model string   `json:"model"`
			Input []string `json:"input"`
		}
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		assert.Equal(t, "local-model", req.Model)
		assert.Equal(t, []string{"panic"}, req.Input)
		_, _ = w.Write([]byte(`{"model":"local-model","data":[{"index":0,"embedding":[1,0]}]}`))
	}))
	defer server.Close()

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.Search.DefaultMode = "hybrid"
	cfg.Search.Embeddings.Enabled = true
	cfg.Search.Embeddings.Provider = "openai_compatible"
	cfg.Search.Embeddings.Model = "local-model"
	cfg.Search.Embeddings.BaseURL = server.URL
	cfg.Search.Embeddings.APIKeyEnv = ""
	require.NoError(t, config.Write(cfgPath, cfg))

	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	base := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "m3",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Alice",
		MessageType:       0,
		CreatedAt:         base.Format(time.RFC3339Nano),
		Content:           "panic stack trace",
		NormalizedContent: "panic stack trace",
		RawJSON:           `{"author":{"username":"Alice"}}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "m2",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u2",
		AuthorName:        "Bob",
		MessageType:       0,
		CreatedAt:         base.Add(time.Minute).Format(time.RFC3339Nano),
		Content:           "database worker stalled",
		NormalizedContent: "database worker stalled",
		RawJSON:           `{"author":{"username":"Bob"}}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u3",
		AuthorName:        "Carol",
		MessageType:       0,
		CreatedAt:         base.Add(2 * time.Minute).Format(time.RFC3339Nano),
		Content:           "panic database lock",
		NormalizedContent: "panic database lock",
		RawJSON:           `{"author":{"username":"Carol"}}`,
	}))
	require.NoError(t, insertCLIEmbedding(ctx, s, "m1", "openai_compatible", "local-model", []float32{0.9, 0.1}))
	require.NoError(t, insertCLIEmbedding(ctx, s, "m2", "openai_compatible", "local-model", []float32{1, 0}))
	require.NoError(t, insertCLIEmbedding(ctx, s, "m3", "openai_compatible", "local-model", []float32{0, 1}))
	require.NoError(t, s.Close())

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "search", "--limit", "3", "panic"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "panic database lock")
	require.Contains(t, out.String(), "database worker stalled")
	require.Contains(t, out.String(), "panic stack trace")
}

func TestSearchSemanticCommandErrors(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	cfg := config.Default()
	cfg.DBPath = dbPath
	require.NoError(t, config.Write(cfgPath, cfg))
	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	err = Run(ctx, []string{"--config", cfgPath, "search", "--mode", "bogus", "cats"}, &bytes.Buffer{}, &bytes.Buffer{})
	require.Equal(t, 2, ExitCode(err))
	require.ErrorContains(t, err, `unsupported search mode "bogus"`)

	err = Run(ctx, []string{"--config", cfgPath, "search", "--mode", "semantic", "cats"}, &bytes.Buffer{}, &bytes.Buffer{})
	require.Equal(t, 1, ExitCode(err))
	require.ErrorContains(t, err, "embeddings are disabled")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer server.Close()
	cfg.Search.Embeddings.Enabled = true
	cfg.Search.Embeddings.Provider = "openai_compatible"
	cfg.Search.Embeddings.Model = "local-model"
	cfg.Search.Embeddings.BaseURL = server.URL
	cfg.Search.Embeddings.APIKeyEnv = ""
	require.NoError(t, config.Write(cfgPath, cfg))

	err = Run(ctx, []string{"--config", cfgPath, "search", "--mode", "semantic", "cats"}, &bytes.Buffer{}, &bytes.Buffer{})
	require.Equal(t, 1, ExitCode(err))
	require.ErrorContains(t, err, "embedding query failed")
}

func TestSearchHybridCommandFallsBackToFTS(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	cfg := config.Default()
	cfg.DBPath = dbPath
	require.NoError(t, config.Write(cfgPath, cfg))

	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Alice",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "panic exact match",
		NormalizedContent: "panic exact match",
		RawJSON:           `{"author":{"username":"Alice"}}`,
	}))
	require.NoError(t, s.Close())

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "search", "--mode", "hybrid", "panic"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "panic exact match")

	okRequests := 0
	okServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		okRequests++
		_, _ = w.Write([]byte(`{"model":"local-model","data":[{"index":0,"embedding":[1,0]}]}`))
	}))
	defer okServer.Close()
	cfg.Search.Embeddings.Enabled = true
	cfg.Search.Embeddings.Provider = "openai_compatible"
	cfg.Search.Embeddings.Model = "local-model"
	cfg.Search.Embeddings.BaseURL = okServer.URL
	cfg.Search.Embeddings.APIKeyEnv = ""
	require.NoError(t, config.Write(cfgPath, cfg))

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "search", "--mode", "hybrid", "panic"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "panic exact match")
	require.Equal(t, 0, okRequests)

	s, err = store.Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, insertCLIEmbedding(ctx, s, "m1", "openai_compatible", "local-model", []float32{1, 0}))
	require.NoError(t, s.Close())

	failedRequests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		failedRequests++
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer server.Close()
	cfg.Search.Embeddings.Enabled = true
	cfg.Search.Embeddings.Provider = "openai_compatible"
	cfg.Search.Embeddings.Model = "local-model"
	cfg.Search.Embeddings.BaseURL = server.URL
	cfg.Search.Embeddings.APIKeyEnv = ""
	require.NoError(t, config.Write(cfgPath, cfg))

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "search", "--mode", "hybrid", "panic"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "panic exact match")
	require.Equal(t, 1, failedRequests)
}

func insertCLIEmbedding(ctx context.Context, s *store.Store, messageID, provider, model string, vector []float32) error {
	blob, err := store.EncodeEmbeddingVector(vector)
	if err != nil {
		return err
	}
	_, err = s.DB().ExecContext(ctx, `
		insert into message_embeddings(
			message_id, provider, model, input_version, dimensions, embedding_blob, embedded_at
		) values(?, ?, ?, ?, ?, ?, ?)
	`, messageID, provider, model, store.EmbeddingInputVersion, len(vector), blob, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

type fakeDiscordClient struct {
	guilds []*discordgo.UserGuild
	self   *discordgo.User
}

func (f *fakeDiscordClient) Close() error { return nil }
func (f *fakeDiscordClient) Self(context.Context) (*discordgo.User, error) {
	return f.self, nil
}

func (f *fakeDiscordClient) Guilds(context.Context) ([]*discordgo.UserGuild, error) {
	return f.guilds, nil
}

func (f *fakeDiscordClient) Guild(context.Context, string) (*discordgo.Guild, error) {
	return &discordgo.Guild{}, nil
}

func (f *fakeDiscordClient) GuildChannels(context.Context, string) ([]*discordgo.Channel, error) {
	return nil, nil
}

func (f *fakeDiscordClient) ThreadsActive(context.Context, string) ([]*discordgo.Channel, error) {
	return nil, nil
}

func (f *fakeDiscordClient) GuildThreadsActive(context.Context, string) ([]*discordgo.Channel, error) {
	return nil, nil
}

func (f *fakeDiscordClient) ThreadsArchived(context.Context, string, bool) ([]*discordgo.Channel, error) {
	return nil, nil
}

func (f *fakeDiscordClient) GuildMembers(context.Context, string) ([]*discordgo.Member, error) {
	return nil, nil
}

func (f *fakeDiscordClient) ChannelMessages(context.Context, string, int, string, string) ([]*discordgo.Message, error) {
	return nil, nil
}

func (f *fakeDiscordClient) ChannelMessage(context.Context, string, string) (*discordgo.Message, error) {
	return nil, nil
}

func (f *fakeDiscordClient) Tail(context.Context, discordclient.EventHandler) error {
	return nil
}

type fakeSyncService struct {
	discovered            []*discordgo.UserGuild
	lastSync              syncer.SyncOptions
	lastTail              []string
	lastRepair            time.Duration
	attachmentTextEnabled bool
}

func (f *fakeSyncService) DiscoverGuilds(context.Context) ([]*discordgo.UserGuild, error) {
	return f.discovered, nil
}

func (f *fakeSyncService) Sync(_ context.Context, opts syncer.SyncOptions) (syncer.SyncStats, error) {
	f.lastSync = opts
	return syncer.SyncStats{Guilds: len(opts.GuildIDs), Messages: 3}, nil
}

func (f *fakeSyncService) RunTail(_ context.Context, guildIDs []string, repairEvery time.Duration) error {
	f.lastTail = guildIDs
	f.lastRepair = repairEvery
	return nil
}

func (f *fakeSyncService) SetAttachmentTextEnabled(enabled bool) {
	f.attachmentTextEnabled = enabled
}

type hybridSyncService struct {
	store         *store.Store
	sawGitMessage bool
}

func (f *hybridSyncService) DiscoverGuilds(context.Context) ([]*discordgo.UserGuild, error) {
	return []*discordgo.UserGuild{{ID: "g1"}}, nil
}

func (f *hybridSyncService) Sync(ctx context.Context, opts syncer.SyncOptions) (syncer.SyncStats, error) {
	rows, err := f.store.ListMessages(ctx, store.MessageListOptions{Channel: "general", IncludeEmpty: true})
	if err != nil {
		return syncer.SyncStats{}, err
	}
	for _, row := range rows {
		if row.Content == "automatic updates work" {
			f.sawGitMessage = true
			break
		}
	}
	if err := f.store.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}); err != nil {
		return syncer.SyncStats{}, err
	}
	if err := f.store.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}); err != nil {
		return syncer.SyncStats{}, err
	}
	if err := f.store.UpsertMessage(ctx, store.MessageRecord{
		ID:                "m-live",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano),
		Content:           "live discord filled the delta",
		NormalizedContent: "live discord filled the delta",
		RawJSON:           `{}`,
	}); err != nil {
		return syncer.SyncStats{}, err
	}
	return syncer.SyncStats{Guilds: len(opts.GuildIDs), Messages: 1}, nil
}

func (f *hybridSyncService) RunTail(context.Context, []string, time.Duration) error {
	return nil
}

func TestRuntimeInitSyncTailAndDoctor(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")
	t.Setenv(config.DefaultTokenEnv, "env-token")

	fakeDiscord := &fakeDiscordClient{
		guilds: []*discordgo.UserGuild{{ID: "g1"}, {ID: "g2"}},
		self:   &discordgo.User{ID: "bot"},
	}
	fakeSync := &fakeSyncService{discovered: fakeDiscord.guilds}

	newRuntime := func() *runtime {
		return &runtime{
			ctx:        ctx,
			configPath: cfgPath,
			stdout:     &bytes.Buffer{},
			stderr:     &bytes.Buffer{},
			logger:     discardLogger(),
			openStore:  store.Open,
			newDiscord: func(config.Config) (discordClient, error) { return fakeDiscord, nil },
			newSyncer: func(syncer.Client, *store.Store, *slog.Logger) syncService {
				return fakeSync
			},
		}
	}

	rt := newRuntime()
	require.NoError(t, rt.runInit([]string{"--db", dbPath, "--with-embeddings", "--guild", "g2"}))

	cfg, err := config.Load(cfgPath)
	require.NoError(t, err)
	require.Equal(t, []string{"g1", "g2"}, cfg.GuildIDs)
	require.Equal(t, "g2", cfg.DefaultGuildID)
	require.True(t, cfg.Search.Embeddings.Enabled)
	cfg.Desktop.Path = filepath.Join(dir, "empty-discord")
	require.NoError(t, os.MkdirAll(cfg.Desktop.Path, 0o755))
	require.NoError(t, config.Write(cfgPath, cfg))

	rt = newRuntime()
	require.NoError(t, rt.withServices(true, func() error { return rt.runSync([]string{"--guilds", "g2"}) }))
	require.Equal(t, []string{"g2"}, fakeSync.lastSync.GuildIDs)
	require.True(t, fakeSync.lastSync.LatestOnly)
	require.True(t, fakeSync.lastSync.SkipMembers)
	require.True(t, fakeSync.attachmentTextEnabled)

	rt = newRuntime()
	require.NoError(t, rt.withServices(true, func() error { return rt.runSync([]string{"--all"}) }))
	require.Nil(t, fakeSync.lastSync.GuildIDs)
	require.True(t, fakeSync.lastSync.LatestOnly)
	require.True(t, fakeSync.lastSync.SkipMembers)

	rt = newRuntime()
	require.NoError(t, rt.withServices(true, func() error { return rt.runSync([]string{"--guilds", "g2", "--all-channels"}) }))
	require.Equal(t, []string{"g2"}, fakeSync.lastSync.GuildIDs)
	require.False(t, fakeSync.lastSync.LatestOnly)
	require.False(t, fakeSync.lastSync.SkipMembers)

	rt = newRuntime()
	require.NoError(t, rt.withServices(true, func() error { return rt.runTail([]string{"--repair-every", "30s"}) }))
	require.Equal(t, []string{"g2"}, fakeSync.lastTail)
	require.Equal(t, 30*time.Second, fakeSync.lastRepair)

	rt = newRuntime()
	var out bytes.Buffer
	rt.stdout = &out
	require.NoError(t, rt.runDoctor(nil))
	require.Contains(t, out.String(), "discord_auth=ok")
}

func TestSyncModeDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		full           bool
		allChannels    bool
		since          string
		channels       string
		defaultLatest  bool
		latestOnly     bool
		skipMembers    bool
		explicitLatest bool
		explicitSkip   bool
	}{
		{name: "routine", defaultLatest: true, latestOnly: true, skipMembers: true},
		{name: "all channels", allChannels: true},
		{name: "full", full: true},
		{name: "since", since: "2026-04-27T20:00:00Z"},
		{name: "channels", channels: "c1"},
		{name: "explicit latest", allChannels: true, explicitLatest: true, latestOnly: true},
		{name: "explicit skip members", allChannels: true, explicitSkip: true, skipMembers: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			defaultLatest := defaultLatestSyncMode(tt.full, tt.allChannels, tt.since, tt.channels)
			require.Equal(t, tt.defaultLatest, defaultLatest)
			require.Equal(t, tt.latestOnly, syncLatestOnly(tt.explicitLatest, defaultLatest))
			require.Equal(t, tt.skipMembers, syncSkipsMembers(tt.explicitSkip, defaultLatest))
		})
	}
}

func TestDoctorChecksEnabledLocalEmbeddingProvider(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/embed", r.URL.Path)
		_, _ = w.Write([]byte(`{"model":"nomic-embed-text","embeddings":[[1,2,3]]}`))
	}))
	defer server.Close()

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.Search.Embeddings.Enabled = true
	cfg.Search.Embeddings.Provider = "ollama"
	cfg.Search.Embeddings.Model = "nomic-embed-text"
	cfg.Search.Embeddings.BaseURL = server.URL
	require.NoError(t, config.Write(cfgPath, cfg))

	var out bytes.Buffer
	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		stdout:     &out,
		stderr:     &bytes.Buffer{},
		logger:     discardLogger(),
	}
	require.NoError(t, rt.runDoctor(nil))
	require.Contains(t, out.String(), "embeddings=ok")
	require.Contains(t, out.String(), "embeddings_provider=ollama")
	require.Contains(t, out.String(), "embeddings_probe=ok")
}

func TestDoctorReportsEmbeddingProviderWarningsNonFatally(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")
	t.Setenv("OPENAI_API_KEY", "")

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.Search.Embeddings.Enabled = true
	cfg.Search.Embeddings.Provider = "openai"
	require.NoError(t, config.Write(cfgPath, cfg))

	var out bytes.Buffer
	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		stdout:     &out,
		stderr:     &bytes.Buffer{},
		logger:     discardLogger(),
	}
	require.NoError(t, rt.runDoctor(nil))
	require.Contains(t, out.String(), "embeddings=warning")
	require.Contains(t, out.String(), "embeddings_warning=embedding provider \"openai\" requires API key env OPENAI_API_KEY")
}

func TestDoctorReportsUnsupportedEmbeddingProviderNonFatally(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.Search.Embeddings.Enabled = true
	cfg.Search.Embeddings.Provider = "bogus"
	cfg.Search.Embeddings.Model = "custom"
	cfg.Search.Embeddings.APIKeyEnv = ""
	require.NoError(t, config.Write(cfgPath, cfg))

	var out bytes.Buffer
	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		stdout:     &out,
		stderr:     &bytes.Buffer{},
		logger:     discardLogger(),
	}
	require.NoError(t, rt.runDoctor(nil))
	require.Contains(t, out.String(), "embeddings=warning")
	require.Contains(t, out.String(), "embeddings_warning=unsupported embedding provider \"bogus\"")
}

func TestRuntimeConfiguresAttachmentTextOnSyncer(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")
	t.Setenv(config.DefaultTokenEnv, "env-token")

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.Sync.AttachmentText = nil
	require.NoError(t, config.Write(cfgPath, cfg))

	fakeSync := &fakeSyncService{}
	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		stdout:     &bytes.Buffer{},
		stderr:     &bytes.Buffer{},
		logger:     discardLogger(),
		openStore:  store.Open,
		newDiscord: func(config.Config) (discordClient, error) { return &fakeDiscordClient{}, nil },
		newSyncer: func(syncer.Client, *store.Store, *slog.Logger) syncService {
			return fakeSync
		},
	}
	require.NoError(t, rt.withServices(true, func() error { return nil }))
	require.True(t, fakeSync.attachmentTextEnabled)

	cfg.Sync.AttachmentText = new(false)
	require.NoError(t, config.Write(cfgPath, cfg))
	require.NoError(t, rt.withServices(true, func() error { return nil }))
	require.False(t, fakeSync.attachmentTextEnabled)
}

func TestSQLRejectsMutationsByDefaultAndAllowsUnsafeConfirm(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	cfg := config.Default()
	cfg.DBPath = dbPath
	require.NoError(t, config.Write(cfgPath, cfg))

	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "hello",
		NormalizedContent: "hello",
		RawJSON:           `{}`,
	}))
	require.NoError(t, s.Close())

	err = Run(ctx, []string{"--config", cfgPath, "sql", "delete from messages"}, &bytes.Buffer{}, &bytes.Buffer{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "only read-only sql is allowed")

	var out bytes.Buffer
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "sql", "--unsafe", "--confirm", "select count(*) as total from messages"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "total")

	out.Reset()
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "sql", "--unsafe", "--confirm", "delete from messages"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "rows_affected=1")

	s, err = store.Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	_, rows, err := s.ReadOnlyQuery(ctx, "select count(*) from messages")
	require.NoError(t, err)
	require.Equal(t, "0", rows[0][0])
}

func TestCommandUsageBranches(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")
	cfg := config.Default()
	cfg.DBPath = dbPath
	require.NoError(t, config.Write(cfgPath, cfg))
	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	cases := []struct {
		args []string
		want string
	}{
		{[]string{"--config", cfgPath, "sql", "--confirm", "select 1"}, "--confirm requires --unsafe"},
		{[]string{"--config", cfgPath, "sql", "--unsafe", "delete from messages"}, "--unsafe requires --confirm"},
		{[]string{"--config", cfgPath, "search"}, "search requires a query"},
		{[]string{"--config", cfgPath, "search", "--dm", "--guild", "g1", "panic"}, "use either --dm or --guild/--guilds"},
		{[]string{"--config", cfgPath, "messages", "--dm", "--guild", "g1"}, "use either --dm or --guild/--guilds"},
		{[]string{"--config", cfgPath, "messages", "--dm", "--sync"}, "messages --sync is not supported with --dm"},
		{[]string{"--config", cfgPath, "dms", "extra"}, "dms takes flags only"},
		{[]string{"--config", cfgPath, "wiretap", "extra"}, "wiretap takes flags only"},
		{[]string{"--config", cfgPath, "wiretap", "--max-file-bytes", "0"}, "--max-file-bytes must be positive"},
		{[]string{"--config", cfgPath, "wiretap", "--watch-every", "1ms"}, "--watch-every must be at least 1s"},
		{[]string{"--config", cfgPath, "members"}, "members requires a subcommand"},
		{[]string{"--config", cfgPath, "members", "search"}, "members search requires a query"},
		{[]string{"--config", cfgPath, "members", "bogus"}, `unknown members subcommand "bogus"`},
		{[]string{"--config", cfgPath, "channels"}, "channels requires a subcommand"},
		{[]string{"--config", cfgPath, "channels", "bogus"}, `unknown channels subcommand "bogus"`},
		{[]string{"--config", cfgPath, "status", "extra"}, "status takes no arguments"},
		{[]string{"--config", cfgPath, "report", "extra"}, "report takes no positional arguments"},
		{[]string{"--config", cfgPath, "embed"}, "embeddings are disabled"},
		{[]string{"--config", cfgPath, "embed", "--limit", "0"}, "--limit must be positive"},
		{[]string{"--config", cfgPath, "embed", "--batch-size", "0"}, "--batch-size must be positive"},
		{[]string{"--config", cfgPath, "publish", "extra"}, "publish takes no positional arguments"},
		{[]string{"--config", cfgPath, "update", "extra"}, "update takes no positional arguments"},
		{[]string{"--config", cfgPath, "subscribe"}, "subscribe requires one remote"},
		{[]string{"--config", cfgPath, "subscribe", "one", "two"}, "subscribe requires one remote"},
	}
	for _, tc := range cases {
		err := Run(ctx, tc.args, &bytes.Buffer{}, &bytes.Buffer{})
		require.Equal(t, 2, ExitCode(err), tc.args)
		require.ErrorContains(t, err, tc.want, tc.args)
	}
}

func TestHelpers(t *testing.T) {
	t.Parallel()

	require.Equal(t, []string{"a", "b"}, csvList("a,b,a"))
	require.Equal(t, "x", (&cliError{code: 2, err: assertErr("x")}).Error())
	require.Equal(t, 2, ExitCode(usageErr(assertErr("x"))))
	require.Equal(t, 4, ExitCode(authErr(assertErr("x"))))
	require.Equal(t, 5, ExitCode(dbErr(assertErr("x"))))
	require.Equal(t, 3, ExitCode(configErr(assertErr("x"))))
	require.Equal(t, 1, ExitCode(assertErr("x")))
	require.True(t, hybridSemanticUnavailable(store.ErrNoCompatibleEmbeddings))
	require.True(t, hybridSemanticUnavailable(assertErr("semantic query embedding missing")))
	require.False(t, hybridSemanticUnavailable(assertErr("other")))
	opts, err := shareOptionsFromFlags("~/share", "git@example.com:org/archive.git", "")
	require.NoError(t, err)
	require.Equal(t, "git@example.com:org/archive.git", opts.Remote)
	require.Equal(t, "main", opts.Branch)
	var out bytes.Buffer
	require.NoError(t, printHuman(&out, syncer.SyncStats{Guilds: 1}))
	require.Contains(t, out.String(), "guilds=1")
	require.Contains(t, formatTime(time.Unix(1, 0).UTC()), "1970")
	require.Equal(t, "x", firstNonEmpty("", "x", "y"))
}

type assertErr string

func (e assertErr) Error() string { return string(e) }

func discardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func TestRuntimeHelpersAndSubcommands(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.DefaultGuildID = "g1"
	require.NoError(t, config.Write(cfgPath, cfg))

	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMember(ctx, store.MemberRecord{GuildID: "g1", UserID: "u1", Username: "peter", RoleIDsJSON: `[]`, RawJSON: `{}`}))
	require.NoError(t, s.Close())

	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		stdout:     &bytes.Buffer{},
		stderr:     &bytes.Buffer{},
		logger:     discardLogger(),
	}
	require.NoError(t, rt.withServices(false, func() error {
		require.Equal(t, []string{"g1"}, rt.resolveSyncGuilds("", ""))
		require.Nil(t, rt.resolveSearchGuilds("", ""))
		require.NoError(t, rt.runMembers([]string{"show", "u1"}))
		require.NoError(t, rt.runMembers([]string{"search", "pet"}))
		require.NoError(t, rt.runMembers([]string{"list"}))
		rt.now = func() time.Time { return time.Date(2026, 3, 8, 12, 0, 0, 0, time.UTC) }
		require.NoError(t, rt.runMessages([]string{"--channel", "#general", "--hours", "6", "--last", "1"}))
		require.NoError(t, rt.runMessages([]string{"--channel", "#general", "--days", "7", "--all"}))
		require.NoError(t, rt.runMessages([]string{"--channel", "#general", "--days", "7", "--all", "--include-empty"}))
		require.NoError(t, rt.runMentions([]string{"--channel", "#general", "--target", "u2"}))
		require.NoError(t, rt.runSearch([]string{"--include-empty", "Peter"}))
		require.NoError(t, rt.runChannels([]string{"show", "c1"}))
		require.NoError(t, rt.runChannels([]string{"list"}))
		require.NoError(t, rt.runStatus(nil))
		return nil
	}))
}

func TestRunInitWritesDiscoveredGuildConfig(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")
	t.Setenv(config.DefaultTokenEnv, "env-token")

	fakeSync := &fakeSyncService{discovered: []*discordgo.UserGuild{{ID: "g1"}, {ID: "g2"}}}
	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		stdout:     &bytes.Buffer{},
		stderr:     &bytes.Buffer{},
		logger:     discardLogger(),
		newDiscord: func(config.Config) (discordClient, error) { return &fakeDiscordClient{}, nil },
		newSyncer: func(syncer.Client, *store.Store, *slog.Logger) syncService {
			return fakeSync
		},
	}

	require.NoError(t, rt.runInit([]string{"--db", dbPath, "--guild", "g2", "--with-embeddings"}))
	cfg, err := config.Load(cfgPath)
	require.NoError(t, err)
	require.Equal(t, dbPath, cfg.DBPath)
	require.Equal(t, []string{"g1", "g2"}, cfg.GuildIDs)
	require.Equal(t, "g2", cfg.DefaultGuildID)
	require.True(t, cfg.Search.Embeddings.Enabled)
	require.Contains(t, rt.stdout.(*bytes.Buffer).String(), "g2")
}

func TestRunMembersShowUsesDefaultGuildForAmbiguousQuery(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.DefaultGuildID = "g1"
	require.NoError(t, config.Write(cfgPath, cfg))

	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMember(ctx, store.MemberRecord{
		GuildID:     "g1",
		UserID:      "u1",
		Username:    "same",
		DisplayName: "Same",
		RoleIDsJSON: `[]`,
		RawJSON:     `{"github":"steipete"}`,
	}))
	require.NoError(t, s.UpsertMember(ctx, store.MemberRecord{
		GuildID:     "g2",
		UserID:      "u2",
		Username:    "same",
		DisplayName: "Same",
		RoleIDsJSON: `[]`,
		RawJSON:     `{"github":"other"}`,
	}))
	require.NoError(t, s.UpsertMessage(ctx, store.MessageRecord{
		ID:                "m1",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Same",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Format(time.RFC3339Nano),
		Content:           "hello",
		NormalizedContent: "hello",
		RawJSON:           `{}`,
	}))
	require.NoError(t, s.Close())

	var out bytes.Buffer
	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		stdout:     &out,
		stderr:     &bytes.Buffer{},
		logger:     discardLogger(),
	}
	require.NoError(t, rt.withServices(false, func() error {
		return rt.runMembers([]string{"show", "same"})
	}))
	require.Contains(t, out.String(), "guild=g1")
	require.Contains(t, out.String(), "github=steipete")
}

func TestRunMembersShowReturnsListWhenStillAmbiguous(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	cfg := config.Default()
	cfg.DBPath = dbPath
	require.NoError(t, config.Write(cfgPath, cfg))

	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, s.UpsertMember(ctx, store.MemberRecord{GuildID: "g1", UserID: "u1", Username: "same", DisplayName: "Same", RoleIDsJSON: `[]`, RawJSON: `{}`}))
	require.NoError(t, s.UpsertMember(ctx, store.MemberRecord{GuildID: "g2", UserID: "u2", Username: "same", DisplayName: "Same", RoleIDsJSON: `[]`, RawJSON: `{}`}))
	require.NoError(t, s.Close())

	var out bytes.Buffer
	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		stdout:     &out,
		stderr:     &bytes.Buffer{},
		logger:     discardLogger(),
	}
	require.NoError(t, rt.withServices(false, func() error {
		return rt.runMembers([]string{"show", "same"})
	}))
	require.Contains(t, out.String(), "GUILD")
	require.Contains(t, out.String(), "u1")
	require.Contains(t, out.String(), "u2")
}

func TestRunMessagesSyncTargetsResolvedChannel(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")
	t.Setenv(config.DefaultTokenEnv, "env-token")

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.DefaultGuildID = "g1"
	require.NoError(t, config.Write(cfgPath, cfg))

	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.Close())

	fakeSync := &fakeSyncService{}
	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		stdout:     &bytes.Buffer{},
		stderr:     &bytes.Buffer{},
		logger:     discardLogger(),
		openStore:  store.Open,
		newDiscord: func(config.Config) (discordClient, error) { return &fakeDiscordClient{}, nil },
		newSyncer: func(syncer.Client, *store.Store, *slog.Logger) syncService {
			return fakeSync
		},
	}
	rt.now = func() time.Time { return time.Date(2026, 3, 8, 12, 0, 0, 0, time.UTC) }

	require.NoError(t, rt.withServices(true, func() error {
		return rt.runMessages([]string{"--channel", "#general", "--hours", "6", "--last", "1", "--sync"})
	}))
	require.Equal(t, []string{"g1"}, fakeSync.lastSync.GuildIDs)
	require.Equal(t, []string{"c1"}, fakeSync.lastSync.ChannelIDs)
}

func TestRunMessagesSyncFallsBackToGuildSyncForUnknownChannel(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")
	t.Setenv(config.DefaultTokenEnv, "env-token")

	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.DefaultGuildID = "g1"
	require.NoError(t, config.Write(cfgPath, cfg))

	fakeSync := &fakeSyncService{}
	rt := &runtime{
		ctx:        ctx,
		configPath: cfgPath,
		stdout:     &bytes.Buffer{},
		stderr:     &bytes.Buffer{},
		logger:     discardLogger(),
		openStore:  store.Open,
		newDiscord: func(config.Config) (discordClient, error) { return &fakeDiscordClient{}, nil },
		newSyncer: func(syncer.Client, *store.Store, *slog.Logger) syncService {
			return fakeSync
		},
	}

	require.NoError(t, rt.withServices(true, func() error {
		return rt.runMessages([]string{"--channel", "new-channel", "--days", "1", "--sync"})
	}))
	require.Equal(t, []string{"g1"}, fakeSync.lastSync.GuildIDs)
	require.Empty(t, fakeSync.lastSync.ChannelIDs)
}

func TestRunMentionsValidation(t *testing.T) {
	t.Parallel()

	rt := &runtime{stderr: &bytes.Buffer{}}
	rt.now = func() time.Time { return time.Date(2026, 3, 8, 12, 0, 0, 0, time.UTC) }

	require.Equal(t, 2, ExitCode(rt.runDirectMessages([]string{"extra"})))
	require.Equal(t, 2, ExitCode(rt.runDirectMessages([]string{"--hours", "-1"})))
	require.Equal(t, 2, ExitCode(rt.runDirectMessages([]string{"--days", "-1"})))
	require.Equal(t, 2, ExitCode(rt.runDirectMessages([]string{"--hours", "1", "--days", "1"})))
	require.Equal(t, 2, ExitCode(rt.runDirectMessages([]string{"--hours", "1", "--since", "2026-03-01T00:00:00Z"})))
	require.Equal(t, 2, ExitCode(rt.runDirectMessages([]string{"--limit", "-1"})))
	require.Equal(t, 2, ExitCode(rt.runDirectMessages([]string{"--last", "-1"})))
	require.Equal(t, 2, ExitCode(rt.runDirectMessages([]string{"--all", "--last", "1"})))
	require.Equal(t, 2, ExitCode(rt.runDirectMessages([]string{"--limit", "1", "--last", "1"})))
	require.Equal(t, 2, ExitCode(rt.runDirectMessages([]string{"--since", "bad"})))
	require.Equal(t, 2, ExitCode(rt.runDirectMessages([]string{"--before", "bad"})))
	require.Equal(t, 2, ExitCode(rt.runMessages([]string{"--hours", "-1", "--channel", "general"})))
	require.Equal(t, 2, ExitCode(rt.runMessages([]string{"--hours", "1", "--days", "1", "--channel", "general"})))
	require.Equal(t, 2, ExitCode(rt.runMessages([]string{"--hours", "1", "--since", "2026-03-01T00:00:00Z", "--channel", "general"})))
	require.Equal(t, 2, ExitCode(rt.runMessages([]string{"--last", "-1", "--channel", "general"})))
	require.Equal(t, 2, ExitCode(rt.runMessages([]string{"--last", "1", "--limit", "20", "--channel", "general"})))
	require.Equal(t, 2, ExitCode(rt.runMessages([]string{"--last", "1", "--all", "--channel", "general"})))
	require.Equal(t, 2, ExitCode(rt.runMentions([]string{"--days", "-1", "--target", "u1"})))
	require.Equal(t, 2, ExitCode(rt.runMentions([]string{"--days", "1", "--since", "2026-03-01T00:00:00Z", "--target", "u1"})))
	require.Equal(t, 2, ExitCode(rt.runMentions([]string{"--since", "bad", "--target", "u1"})))
	require.Equal(t, 2, ExitCode(rt.runMentions([]string{"--type", "nope", "--target", "u1"})))
	require.Equal(t, 2, ExitCode(rt.runMentions([]string{})))
}

func TestPrintJSONAndPlain(t *testing.T) {
	t.Parallel()

	rt := &runtime{stdout: &bytes.Buffer{}, json: true}
	require.NoError(t, rt.print(map[string]any{"ok": true}))
	require.Contains(t, rt.stdout.(*bytes.Buffer).String(), "\"ok\": true")

	rt = &runtime{stdout: &bytes.Buffer{}, plain: true}
	require.NoError(t, rt.print([]store.ChannelRow{{GuildID: "g1", ID: "c1", Kind: "text", Name: "general"}}))
	require.Contains(t, rt.stdout.(*bytes.Buffer).String(), "general")

	rt = &runtime{stdout: &bytes.Buffer{}}
	require.NoError(t, rt.print([]store.SearchResult{{GuildID: "g1", ChannelName: "general", AuthorName: "Peter", Content: "hello"}}))
	require.Contains(t, rt.stdout.(*bytes.Buffer).String(), "hello")

	rt = &runtime{stdout: &bytes.Buffer{}}
	require.NoError(t, rt.print([]store.MentionRow{{GuildID: "g1", ChannelName: "general", AuthorName: "Peter", TargetType: "user", TargetName: "Shadow", Content: "hello"}}))
	require.Contains(t, rt.stdout.(*bytes.Buffer).String(), "Shadow")

	rt = &runtime{stdout: &bytes.Buffer{}, plain: true}
	require.NoError(t, rt.print([]store.MemberRow{{GuildID: "g1", UserID: "u1", Username: "peter"}}))
	require.Contains(t, rt.stdout.(*bytes.Buffer).String(), "peter")

	rt = &runtime{stdout: &bytes.Buffer{}, plain: true}
	require.NoError(t, rt.print([]store.SearchResult{{GuildID: "g1", ChannelID: "c1", AuthorID: "u1", Content: "hello"}}))
	require.Contains(t, rt.stdout.(*bytes.Buffer).String(), "hello")

	rt = &runtime{stdout: &bytes.Buffer{}, plain: true}
	require.NoError(t, rt.print([]store.MessageRow{{GuildID: "g1", ChannelID: "c1", AuthorID: "u1", MessageID: "m1", Content: "hello", CreatedAt: time.Unix(1, 0).UTC()}}))
	require.Contains(t, rt.stdout.(*bytes.Buffer).String(), "m1")

	rt = &runtime{stdout: &bytes.Buffer{}, plain: true}
	require.NoError(t, rt.print([]store.MentionRow{{GuildID: "g1", ChannelID: "c1", AuthorID: "u1", TargetType: "user", TargetID: "u2", Content: "hello", CreatedAt: time.Unix(1, 0).UTC()}}))
	require.Contains(t, rt.stdout.(*bytes.Buffer).String(), "u2")

	rt = &runtime{stdout: &bytes.Buffer{}}
	require.NoError(t, rt.print(struct{ OK bool }{OK: true}))
	require.Contains(t, rt.stdout.(*bytes.Buffer).String(), "\"OK\": true")
}

func TestWithServicesErrors(t *testing.T) {
	t.Parallel()

	rt := &runtime{ctx: context.Background(), configPath: filepath.Join(t.TempDir(), "missing.toml"), stdout: &bytes.Buffer{}, stderr: &bytes.Buffer{}}
	err := rt.withServices(false, func() error { return nil })
	require.Equal(t, 3, ExitCode(err))

	cfgPath := filepath.Join(t.TempDir(), "config.toml")
	cfg := config.Default()
	cfg.DBPath = filepath.Join(t.TempDir(), "discrawl.db")
	require.NoError(t, config.Write(cfgPath, cfg))
	rt = &runtime{
		ctx:        context.Background(),
		configPath: cfgPath,
		stdout:     &bytes.Buffer{},
		stderr:     &bytes.Buffer{},
		openStore: func(context.Context, string) (*store.Store, error) {
			return nil, assertErr("db")
		},
	}
	err = rt.withServices(false, func() error { return nil })
	require.Equal(t, 5, ExitCode(err))

	rt = &runtime{
		ctx:        context.Background(),
		configPath: cfgPath,
		stdout:     &bytes.Buffer{},
		stderr:     &bytes.Buffer{},
		openStore:  store.Open,
		newDiscord: func(config.Config) (discordClient, error) { return nil, assertErr("auth") },
	}
	err = rt.withServices(true, func() error { return nil })
	require.Equal(t, 4, ExitCode(err))
}

func TestCommandUsageErrors(t *testing.T) {
	t.Parallel()

	rt := &runtime{}
	require.Equal(t, 2, ExitCode(rt.runMembers(nil)))
	require.Equal(t, 2, ExitCode(rt.runMembers([]string{"nope"})))
	require.Equal(t, 2, ExitCode(rt.runMessages(nil)))
	require.Equal(t, 2, ExitCode(rt.runMessages([]string{"--days", "-1"})))
	require.Equal(t, 2, ExitCode(rt.runMessages([]string{"--days", "1", "--since", "2026-03-01T00:00:00Z"})))
	require.Equal(t, 2, ExitCode(rt.runSync([]string{"--all", "--guild", "g1"})))
	require.Equal(t, 2, ExitCode(rt.runChannels(nil)))
	require.Equal(t, 2, ExitCode(rt.runStatus([]string{"extra"})))
	require.NoError(t, (&runtime{stdout: &bytes.Buffer{}}).runDoctor(nil))
}

func TestRunSQLReadsStdin(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbPath := filepath.Join(dir, "discrawl.db")

	cfg := config.Default()
	cfg.DBPath = dbPath
	require.NoError(t, config.Write(cfgPath, cfg))

	s, err := store.Open(ctx, dbPath)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	oldStdin := os.Stdin
	defer func() { os.Stdin = oldStdin }()
	file, err := os.CreateTemp(dir, "stdin.sql")
	require.NoError(t, err)
	_, err = file.WriteString("select 1 as one")
	require.NoError(t, err)
	require.NoError(t, file.Close())
	file, err = os.Open(file.Name())
	require.NoError(t, err)
	os.Stdin = file

	rt := &runtime{ctx: ctx, configPath: cfgPath, stdout: &bytes.Buffer{}, stderr: &bytes.Buffer{}, logger: discardLogger()}
	require.NoError(t, rt.withServices(false, func() error { return rt.runSQL([]string{"-"}) }))
}
