package share

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/steipete/discrawl/internal/store"
)

func TestExportImportRoundTrip(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()

	repo := filepath.Join(t.TempDir(), "share")
	manifest, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.NotEmpty(t, manifest.Tables)
	require.FileExists(t, filepath.Join(repo, ManifestName))
	require.NotEmpty(t, tableEntry(t, manifest, "messages").Files)

	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()

	imported, err := Import(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.Equal(t, manifest.GeneratedAt, imported.GeneratedAt)

	results, err := dst.SearchMessages(ctx, store.SearchOptions{Query: "launch", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "m1", results[0].MessageID)

	mentions, err := dst.ListMentions(ctx, store.MentionListOptions{Target: "Ops", Limit: 10})
	require.NoError(t, err)
	require.Len(t, mentions, 1)

	lastImport, err := dst.GetSyncState(ctx, LastImportSyncScope)
	require.NoError(t, err)
	require.NotEmpty(t, lastImport)
	lastManifest, err := dst.GetSyncState(ctx, LastImportManifestSyncScope)
	require.NoError(t, err)
	require.Equal(t, manifest.GeneratedAt.Format(time.RFC3339Nano), lastManifest)
	require.False(t, NeedsImport(ctx, dst, 15*time.Minute))
}

func TestSnapshotExcludesLocalEmbeddingState(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()

	_, err := src.DB().ExecContext(ctx, `
		insert into embedding_jobs(message_id, state, attempts, provider, model, input_version, updated_at)
		values ('m1', 'done', 0, 'ollama', 'nomic-embed-text', ?, ?)
	`, store.EmbeddingInputVersion, time.Now().UTC().Format(time.RFC3339Nano))
	require.NoError(t, err)
	_, err = src.DB().ExecContext(ctx, `
		insert into message_embeddings(
			message_id, provider, model, input_version, dimensions, embedding_blob, embedded_at
		) values ('m1', 'ollama', 'nomic-embed-text', ?, 2, ?, ?)
	`, store.EmbeddingInputVersion, []byte{0, 0, 0, 0, 0, 0, 0, 0}, time.Now().UTC().Format(time.RFC3339Nano))
	require.NoError(t, err)

	repo := filepath.Join(t.TempDir(), "share")
	manifest, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.NotContains(t, tableNames(manifest), "embedding_jobs")
	require.NotContains(t, tableNames(manifest), "message_embeddings")

	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()
	_, err = dst.DB().ExecContext(ctx, `
		insert into embedding_jobs(message_id, state, attempts, provider, model, input_version, updated_at)
		values ('m1', 'pending', 0, 'ollama', 'nomic-embed-text', ?, ?)
	`, store.EmbeddingInputVersion, time.Now().UTC().Format(time.RFC3339Nano))
	require.NoError(t, err)

	_, err = Import(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	var state string
	require.NoError(t, dst.DB().QueryRowContext(ctx, `
		select state from embedding_jobs where message_id = 'm1'
	`).Scan(&state))
	require.Equal(t, "pending", state)
}

func TestImportIfChangedSkipsSameManifest(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()

	repo := filepath.Join(t.TempDir(), "share")
	manifest, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)

	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()

	importedManifest, imported, err := ImportIfChanged(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.True(t, imported)
	require.Equal(t, manifest.GeneratedAt, importedManifest.GeneratedAt)

	require.NoError(t, dst.UpsertMessage(ctx, store.MessageRecord{
		ID:                "local-only",
		GuildID:           "g1",
		ChannelID:         "c1",
		ChannelName:       "general",
		AuthorID:          "u1",
		AuthorName:        "Peter",
		MessageType:       0,
		CreatedAt:         time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano),
		Content:           "live delta preserved",
		NormalizedContent: "live delta preserved",
		RawJSON:           `{}`,
	}))

	_, imported, err = ImportIfChanged(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	require.False(t, imported)
	results, err := dst.SearchMessages(ctx, store.SearchOptions{Query: "live delta", Limit: 10})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "local-only", results[0].MessageID)
}

func TestExportShardsLargeTables(t *testing.T) {
	ctx := context.Background()
	prevMaxShardBytes := maxShardBytes
	maxShardBytes = 150
	t.Cleanup(func() { maxShardBytes = prevMaxShardBytes })

	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()
	now := time.Now().UTC()
	for i := range 25 {
		id := "extra-" + strconv.Itoa(i)
		require.NoError(t, src.UpsertMessages(ctx, []store.MessageMutation{{
			Record: store.MessageRecord{
				ID:                id,
				GuildID:           "g1",
				ChannelID:         "c1",
				ChannelName:       "general",
				AuthorID:          "u1",
				AuthorName:        "Peter",
				MessageType:       0,
				CreatedAt:         now.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano),
				Content:           strings.Repeat("unique launch shard payload "+id+" ", 8),
				NormalizedContent: strings.Repeat("unique launch shard payload "+id+" ", 8),
				RawJSON:           `{}`,
			},
			EventType:   "upsert",
			PayloadJSON: `{"id":"` + id + `"}`,
			Options:     store.WriteOptions{AppendEvent: true},
		}}))
	}

	repo := filepath.Join(t.TempDir(), "share")
	manifest, err := Export(ctx, src, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	messages := tableEntry(t, manifest, "messages")
	require.Greater(t, len(messages.Files), 1)
	require.Empty(t, messages.File)
	for _, rel := range messages.Files {
		info, err := os.Stat(filepath.Join(repo, filepath.FromSlash(rel)))
		require.NoError(t, err)
		require.Less(t, info.Size(), int64(100*1024*1024))
	}

	dst, err := store.Open(ctx, filepath.Join(t.TempDir(), "dst.db"))
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()
	_, err = Import(ctx, dst, Options{RepoPath: repo, Branch: "main"})
	require.NoError(t, err)
	results, err := dst.SearchMessages(ctx, store.SearchOptions{Query: "shard payload", Limit: 50})
	require.NoError(t, err)
	require.Len(t, results, 25)
}

func TestGitCommitDetectsNoChanges(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()

	repo := filepath.Join(t.TempDir(), "share")
	opts := Options{RepoPath: repo, Branch: "main"}
	_, err := Export(ctx, src, opts)
	require.NoError(t, err)
	// #nosec G204 -- fixed git argv in test setup.
	require.NoError(t, exec.Command("git", "-C", repo, "config", "user.name", "discrawl test").Run())
	// #nosec G204 -- fixed git argv in test setup.
	require.NoError(t, exec.Command("git", "-C", repo, "config", "user.email", "discrawl@example.com").Run())

	committed, err := Commit(ctx, opts, "test: snapshot")
	require.NoError(t, err)
	require.True(t, committed)

	committed, err = Commit(ctx, opts, "test: snapshot")
	require.NoError(t, err)
	require.False(t, committed)
}

func TestPullAndPushWithBareRemote(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()

	dir := t.TempDir()
	remote := filepath.Join(dir, "remote.git")
	// #nosec G204 -- fixed git argv in test setup.
	require.NoError(t, exec.Command("git", "-C", dir, "init", "--bare", remote).Run())

	publisher := filepath.Join(dir, "publisher")
	opts := Options{RepoPath: publisher, Remote: remote, Branch: "main"}
	_, err := Export(ctx, src, opts)
	require.NoError(t, err)
	// #nosec G204 -- fixed git argv in test setup.
	require.NoError(t, exec.Command("git", "-C", publisher, "config", "user.name", "discrawl test").Run())
	// #nosec G204 -- fixed git argv in test setup.
	require.NoError(t, exec.Command("git", "-C", publisher, "config", "user.email", "discrawl@example.com").Run())
	committed, err := Commit(ctx, opts, "test: snapshot")
	require.NoError(t, err)
	require.True(t, committed)
	require.NoError(t, Push(ctx, opts))

	subscriber := filepath.Join(dir, "subscriber")
	subOpts := Options{RepoPath: subscriber, Remote: remote, Branch: "main"}
	require.NoError(t, Pull(ctx, subOpts))
	require.FileExists(t, filepath.Join(subscriber, ManifestName))
}

func TestPushRebasesRemoteReadmeUpdates(t *testing.T) {
	ctx := context.Background()
	src := seedStore(t, filepath.Join(t.TempDir(), "src.db"))
	defer func() { _ = src.Close() }()

	dir := t.TempDir()
	remote := filepath.Join(dir, "remote.git")
	// #nosec G204 -- fixed git argv in test setup.
	require.NoError(t, exec.Command("git", "-C", dir, "init", "--bare", remote).Run())

	publisher := filepath.Join(dir, "publisher")
	opts := Options{RepoPath: publisher, Remote: remote, Branch: "main"}
	_, err := Export(ctx, src, opts)
	require.NoError(t, err)
	configureGitUser(t, publisher)
	require.NoError(t, os.WriteFile(filepath.Join(publisher, "README.md"), []byte("report: first\n\nfield notes: old\n"), 0o600))
	committed, err := Commit(ctx, opts, "test: initial snapshot")
	require.NoError(t, err)
	require.True(t, committed)
	require.NoError(t, Push(ctx, opts))

	reporter := filepath.Join(dir, "reporter")
	require.NoError(t, run(ctx, dir, "git", "clone", "--branch", "main", remote, reporter))
	configureGitUser(t, reporter)
	require.NoError(t, os.WriteFile(filepath.Join(reporter, "README.md"), []byte("report: first\n\nfield notes: fresh\n"), 0o600))
	require.NoError(t, run(ctx, reporter, "git", "commit", "-am", "docs: update field notes"))
	require.NoError(t, run(ctx, reporter, "git", "push", "-u", "origin", "main"))

	require.NoError(t, os.WriteFile(filepath.Join(publisher, "README.md"), []byte("report: second\n\nfield notes: old\n"), 0o600))
	committed, err = Commit(ctx, opts, "test: update report")
	require.NoError(t, err)
	require.True(t, committed)
	require.NoError(t, Push(ctx, opts))

	subscriber := filepath.Join(dir, "subscriber")
	require.NoError(t, Pull(ctx, Options{RepoPath: subscriber, Remote: remote, Branch: "main"}))
	body, err := os.ReadFile(filepath.Join(subscriber, "README.md"))
	require.NoError(t, err)
	require.Contains(t, string(body), "report: second")
	require.Contains(t, string(body), "field notes: fresh")
}

func seedStore(t *testing.T, path string) *store.Store {
	t.Helper()
	ctx := context.Background()
	s, err := store.Open(ctx, path)
	require.NoError(t, err)
	now := time.Now().UTC()
	require.NoError(t, s.UpsertGuild(ctx, store.GuildRecord{ID: "g1", Name: "Guild", RawJSON: `{}`}))
	require.NoError(t, s.UpsertChannel(ctx, store.ChannelRecord{ID: "c1", GuildID: "g1", Kind: "text", Name: "general", RawJSON: `{}`}))
	require.NoError(t, s.UpsertMember(ctx, store.MemberRecord{
		GuildID:     "g1",
		UserID:      "u1",
		Username:    "peter",
		DisplayName: "Peter",
		RoleIDsJSON: `[]`,
		RawJSON:     `{"bio":"Runs launch ops"}`,
	}))
	require.NoError(t, s.UpsertMessages(ctx, []store.MessageMutation{{
		Record: store.MessageRecord{
			ID:                "m1",
			GuildID:           "g1",
			ChannelID:         "c1",
			ChannelName:       "general",
			AuthorID:          "u1",
			AuthorName:        "Peter",
			MessageType:       0,
			CreatedAt:         now.Format(time.RFC3339Nano),
			Content:           "launch checklist ready",
			NormalizedContent: "launch checklist ready",
			RawJSON:           `{}`,
		},
		EventType:   "upsert",
		PayloadJSON: `{"id":"m1"}`,
		Options:     store.WriteOptions{AppendEvent: true},
		Mentions: []store.MentionEventRecord{{
			MessageID:  "m1",
			GuildID:    "g1",
			ChannelID:  "c1",
			AuthorID:   "u1",
			TargetType: "role",
			TargetID:   "r1",
			TargetName: "Ops",
			EventAt:    now.Format(time.RFC3339Nano),
		}},
	}}))
	return s
}

func configureGitUser(t *testing.T, repo string) {
	t.Helper()
	// #nosec G204 -- fixed git argv in test setup.
	require.NoError(t, exec.Command("git", "-C", repo, "config", "user.name", "discrawl test").Run())
	// #nosec G204 -- fixed git argv in test setup.
	require.NoError(t, exec.Command("git", "-C", repo, "config", "user.email", "discrawl@example.com").Run())
}

func tableEntry(t *testing.T, manifest Manifest, name string) TableManifest {
	t.Helper()
	for _, table := range manifest.Tables {
		if table.Name == name {
			return table
		}
	}
	t.Fatalf("table %s not found", name)
	return TableManifest{}
}

func tableNames(manifest Manifest) []string {
	names := make([]string, 0, len(manifest.Tables))
	for _, table := range manifest.Tables {
		names = append(names, table.Name)
	}
	return names
}
