package cli

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
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
	require.Contains(t, out.String(), "0.2.0")

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
	require.NoError(t, rt.runInit([]string{"--db", dbPath, "--with-embeddings", "--guild", "g2", "--account", "atlas"}))

	cfg, err := config.Load(cfgPath)
	require.NoError(t, err)
	require.Equal(t, []string{"g1", "g2"}, cfg.GuildIDs)
	require.Equal(t, "g2", cfg.DefaultGuildID)
	require.Equal(t, "atlas", cfg.Discord.Account)
	require.True(t, cfg.Search.Embeddings.Enabled)

	rt = newRuntime()
	require.NoError(t, rt.withServices(true, func() error { return rt.runSync([]string{"--guilds", "g2"}) }))
	require.Equal(t, []string{"g2"}, fakeSync.lastSync.GuildIDs)
	require.True(t, fakeSync.attachmentTextEnabled)

	rt = newRuntime()
	require.NoError(t, rt.withServices(true, func() error { return rt.runSync([]string{"--all"}) }))
	require.Nil(t, fakeSync.lastSync.GuildIDs)

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

	cfg.Sync.AttachmentText = ptrBool(false)
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
	require.NoError(t, Run(ctx, []string{"--config", cfgPath, "sql", "--unsafe", "--confirm", "delete from messages"}, &out, &bytes.Buffer{}))
	require.Contains(t, out.String(), "rows_affected=1")

	s, err = store.Open(ctx, dbPath)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	_, rows, err := s.ReadOnlyQuery(ctx, "select count(*) from messages")
	require.NoError(t, err)
	require.Equal(t, "0", rows[0][0])
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

func ptrBool(value bool) *bool {
	return &value
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
