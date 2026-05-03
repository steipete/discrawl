package share

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/steipete/discrawl/internal/store"
)

const (
	ManifestName                = "manifest.json"
	LastImportSyncScope         = "share:last_import_at"
	LastImportManifestSyncScope = "share:last_import_manifest_generated_at"
	directMessageGuildID        = "@me"
)

var ErrNoManifest = errors.New("share manifest not found")

const shardFlushRows = 1024

var maxShardBytes int64 = 40 * 1024 * 1024

var SnapshotTables = []string{
	"guilds",
	"channels",
	"members",
	"messages",
	"message_events",
	"message_attachments",
	"mention_events",
	"sync_state",
}

type Options struct {
	RepoPath              string
	Remote                string
	Branch                string
	IncludeEmbeddings     bool
	EmbeddingProvider     string
	EmbeddingModel        string
	EmbeddingInputVersion string
}

type Manifest struct {
	Version     int                 `json:"version"`
	GeneratedAt time.Time           `json:"generated_at"`
	Tables      []TableManifest     `json:"tables"`
	Embeddings  []EmbeddingManifest `json:"embeddings,omitempty"`
	Files       map[string]string   `json:"files,omitempty"`
}

type TableManifest struct {
	Name    string   `json:"name"`
	File    string   `json:"file,omitempty"`
	Files   []string `json:"files,omitempty"`
	Columns []string `json:"columns"`
	Rows    int      `json:"rows"`
}

type EmbeddingManifest struct {
	Provider     string   `json:"provider"`
	Model        string   `json:"model"`
	InputVersion string   `json:"input_version"`
	Files        []string `json:"files"`
	Columns      []string `json:"columns"`
	Rows         int      `json:"rows"`
}

func EnsureRepo(ctx context.Context, opts Options) error {
	if strings.TrimSpace(opts.RepoPath) == "" {
		return errors.New("share repo path is empty")
	}
	if _, err := os.Stat(filepath.Join(opts.RepoPath, ".git")); err == nil {
		return nil
	}
	if strings.TrimSpace(opts.Remote) != "" {
		if err := os.MkdirAll(filepath.Dir(opts.RepoPath), 0o755); err != nil {
			return fmt.Errorf("mkdir share parent: %w", err)
		}
		if err := run(ctx, "", "git", "clone", opts.Remote, opts.RepoPath); err != nil {
			return err
		}
		if strings.TrimSpace(opts.Branch) != "" {
			if err := run(ctx, opts.RepoPath, "git", "checkout", "-B", opts.Branch); err != nil {
				return err
			}
		}
		return nil
	}
	if err := os.MkdirAll(opts.RepoPath, 0o755); err != nil {
		return fmt.Errorf("mkdir share repo: %w", err)
	}
	if err := run(ctx, opts.RepoPath, "git", "init"); err != nil {
		return err
	}
	if strings.TrimSpace(opts.Branch) != "" {
		if err := run(ctx, opts.RepoPath, "git", "checkout", "-B", opts.Branch); err != nil {
			return err
		}
	}
	return nil
}

func Pull(ctx context.Context, opts Options) error {
	if strings.TrimSpace(opts.Remote) == "" {
		return nil
	}
	if err := EnsureRepo(ctx, opts); err != nil {
		return err
	}
	if err := run(ctx, opts.RepoPath, "git", "fetch", "--prune", "origin"); err != nil {
		return err
	}
	branch := opts.Branch
	if strings.TrimSpace(branch) == "" {
		branch = "main"
	}
	remoteRef := "refs/remotes/origin/" + branch
	if _, err := output(ctx, opts.RepoPath, "git", "rev-parse", "--verify", remoteRef); err != nil {
		return run(ctx, opts.RepoPath, "git", "checkout", "-B", branch)
	}
	if err := run(ctx, opts.RepoPath, "git", "checkout", "-B", branch, "origin/"+branch); err != nil {
		return err
	}
	return run(ctx, opts.RepoPath, "git", "pull", "--ff-only", "origin", branch)
}

func Commit(ctx context.Context, opts Options, message string) (bool, error) {
	if err := run(ctx, opts.RepoPath, "git", "add", "."); err != nil {
		return false, err
	}
	out, err := output(ctx, opts.RepoPath, "git", "status", "--porcelain")
	if err != nil {
		return false, err
	}
	if strings.TrimSpace(out) == "" {
		return false, nil
	}
	if strings.TrimSpace(message) == "" {
		message = "sync: discord archive"
	}
	if err := run(ctx, opts.RepoPath, "git", "commit", "-m", message); err != nil {
		return false, err
	}
	return true, nil
}

func Push(ctx context.Context, opts Options) error {
	branch := opts.Branch
	if strings.TrimSpace(branch) == "" {
		branch = "main"
	}
	out, err := output(ctx, opts.RepoPath, "git", "push", "-u", "origin", branch)
	if err == nil {
		return nil
	}
	if !isNonFastForwardPush(out) {
		return fmt.Errorf("git push -u origin %s: %w\n%s", branch, err, strings.TrimSpace(out))
	}
	if pullErr := run(ctx, opts.RepoPath, "git", "pull", "--rebase", "--autostash", "origin", branch); pullErr != nil {
		return fmt.Errorf("rebase before push retry: %w", pullErr)
	}
	return run(ctx, opts.RepoPath, "git", "push", "-u", "origin", branch)
}

func Export(ctx context.Context, s *store.Store, opts Options) (Manifest, error) {
	if err := EnsureRepo(ctx, opts); err != nil {
		return Manifest{}, err
	}
	if err := os.RemoveAll(filepath.Join(opts.RepoPath, "tables")); err != nil {
		return Manifest{}, fmt.Errorf("reset tables dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(opts.RepoPath, "tables"), 0o755); err != nil {
		return Manifest{}, fmt.Errorf("mkdir tables dir: %w", err)
	}
	manifest := Manifest{
		Version:     1,
		GeneratedAt: time.Now().UTC(),
		Files:       map[string]string{"manifest": ManifestName},
	}
	for _, table := range SnapshotTables {
		entry, err := exportTable(ctx, s.DB(), opts.RepoPath, table)
		if err != nil {
			return Manifest{}, err
		}
		manifest.Tables = append(manifest.Tables, entry)
	}
	if opts.IncludeEmbeddings {
		entry, err := exportEmbeddings(ctx, s.DB(), opts)
		if err != nil {
			return Manifest{}, err
		}
		manifest.Embeddings = []EmbeddingManifest{entry}
	}
	body, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return Manifest{}, err
	}
	body = append(body, '\n')
	if err := os.WriteFile(filepath.Join(opts.RepoPath, ManifestName), body, 0o600); err != nil {
		return Manifest{}, fmt.Errorf("write manifest: %w", err)
	}
	return manifest, nil
}

func Import(ctx context.Context, s *store.Store, opts Options) (Manifest, error) {
	manifest, err := ReadManifest(opts.RepoPath)
	if err != nil {
		return Manifest{}, err
	}
	restorePragmas, err := applyImportPragmas(ctx, s.DB())
	if err != nil {
		return Manifest{}, err
	}
	pragmasRestored := false
	defer func() {
		if !pragmasRestored {
			_ = restorePragmas(ctx)
		}
	}()
	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		return Manifest{}, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	for _, table := range []string{"message_fts", "member_fts"} {
		if _, err := tx.ExecContext(ctx, "drop table if exists "+table); err != nil {
			return Manifest{}, fmt.Errorf("drop %s: %w", table, err)
		}
	}
	for _, table := range slices.Backward(SnapshotTables) {
		query, args := snapshotDeleteQuery(table)
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return Manifest{}, fmt.Errorf("clear %s: %w", table, err)
		}
	}
	for _, table := range manifest.Tables {
		if err := importTable(ctx, tx, opts.RepoPath, table); err != nil {
			return Manifest{}, err
		}
	}
	if err := repairImportedGuildIDs(ctx, tx); err != nil {
		return Manifest{}, err
	}
	if opts.IncludeEmbeddings {
		if err := importEmbeddings(ctx, tx, opts, manifest.Embeddings); err != nil {
			return Manifest{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return Manifest{}, err
	}
	committed = true
	if err := s.RebuildSearchIndexes(ctx); err != nil {
		return Manifest{}, err
	}
	if err := MarkImported(ctx, s, manifest); err != nil {
		return Manifest{}, err
	}
	if err := restorePragmas(ctx); err != nil {
		return Manifest{}, err
	}
	pragmasRestored = true
	return manifest, nil
}

func applyImportPragmas(ctx context.Context, db *sql.DB) (func(context.Context) error, error) {
	// Snapshot imports touch most of the archive. Keep SQLite's crash recovery
	// enabled; journal_mode=off can leave the live DB malformed if the process
	// or host dies mid-import.
	for _, stmt := range []string{
		`pragma temp_store = memory`,
		`pragma cache_size = -262144`,
		`pragma journal_mode = wal`,
		`pragma synchronous = normal`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return nil, fmt.Errorf("apply import pragma %q: %w", stmt, err)
		}
	}
	return func(ctx context.Context) error {
		for _, stmt := range []string{
			`pragma journal_mode = wal`,
			`pragma synchronous = normal`,
		} {
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("restore import pragma %q: %w", stmt, err)
			}
		}
		return nil
	}, nil
}

func ImportIfChanged(ctx context.Context, s *store.Store, opts Options) (Manifest, bool, error) {
	manifest, err := ReadManifest(opts.RepoPath)
	if err != nil {
		return Manifest{}, false, err
	}
	if ManifestAlreadyImported(ctx, s, manifest) {
		if opts.IncludeEmbeddings {
			if err := ImportEmbeddings(ctx, s, opts, manifest); err != nil {
				return Manifest{}, false, err
			}
		}
		if err := MarkImported(ctx, s, manifest); err != nil {
			return Manifest{}, false, err
		}
		return manifest, false, nil
	}
	imported, err := Import(ctx, s, opts)
	if err != nil {
		return Manifest{}, false, err
	}
	return imported, true, nil
}

func ImportEmbeddings(ctx context.Context, s *store.Store, opts Options, manifest Manifest) error {
	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if err := importEmbeddings(ctx, tx, opts, manifest.Embeddings); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

func ManifestAlreadyImported(ctx context.Context, s *store.Store, manifest Manifest) bool {
	if manifest.GeneratedAt.IsZero() {
		return false
	}
	last, err := s.GetSyncState(ctx, LastImportManifestSyncScope)
	if err != nil || strings.TrimSpace(last) == "" {
		return false
	}
	t, err := time.Parse(time.RFC3339Nano, last)
	if err != nil {
		return false
	}
	return t.Equal(manifest.GeneratedAt)
}

func MarkImported(ctx context.Context, s *store.Store, manifest Manifest) error {
	if err := s.SetSyncState(ctx, LastImportSyncScope, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return err
	}
	if manifest.GeneratedAt.IsZero() {
		return nil
	}
	return s.SetSyncState(ctx, LastImportManifestSyncScope, manifest.GeneratedAt.Format(time.RFC3339Nano))
}

func ReadManifest(repoPath string) (Manifest, error) {
	data, err := os.ReadFile(filepath.Join(repoPath, ManifestName))
	if err != nil {
		if os.IsNotExist(err) {
			return Manifest{}, ErrNoManifest
		}
		return Manifest{}, fmt.Errorf("read share manifest: %w", err)
	}
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return Manifest{}, fmt.Errorf("parse share manifest: %w", err)
	}
	if manifest.Version != 1 {
		return Manifest{}, fmt.Errorf("unsupported share manifest version %d", manifest.Version)
	}
	return manifest, nil
}

func NeedsImport(ctx context.Context, s *store.Store, staleAfter time.Duration) bool {
	if staleAfter <= 0 {
		staleAfter = 15 * time.Minute
	}
	last, err := s.GetSyncState(ctx, LastImportSyncScope)
	if err != nil || strings.TrimSpace(last) == "" {
		return true
	}
	t, err := time.Parse(time.RFC3339Nano, last)
	if err != nil {
		return true
	}
	return time.Since(t) >= staleAfter
}

func exportTable(ctx context.Context, db *sql.DB, repoPath, table string) (TableManifest, error) {
	query, args := snapshotExportQuery(table)
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return TableManifest{}, fmt.Errorf("query %s: %w", table, err)
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		return TableManifest{}, fmt.Errorf("columns %s: %w", table, err)
	}
	tableDir := filepath.Join(repoPath, "tables", table)
	if err := os.MkdirAll(tableDir, 0o755); err != nil {
		return TableManifest{}, fmt.Errorf("mkdir %s: %w", table, err)
	}
	writer := tableShardWriter{rootDir: repoPath, relDir: filepath.ToSlash(filepath.Join("tables", table)), label: table}
	if err := writer.open(); err != nil {
		return TableManifest{}, err
	}
	defer func() { _ = writer.close() }()

	count := 0
	values := make([]any, len(columns))
	ptrs := make([]any, len(columns))
	for i := range values {
		ptrs[i] = &values[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return TableManifest{}, fmt.Errorf("scan %s: %w", table, err)
		}
		row := make(map[string]any, len(columns))
		for i, column := range columns {
			row[column] = exportValue(values[i])
		}
		body, err := json.Marshal(row)
		if err != nil {
			return TableManifest{}, fmt.Errorf("marshal %s row: %w", table, err)
		}
		if err := writer.rotateIfNeeded(); err != nil {
			return TableManifest{}, err
		}
		if _, err := writer.Write(body); err != nil {
			return TableManifest{}, fmt.Errorf("write %s row: %w", table, err)
		}
		if _, err := writer.Write([]byte{'\n'}); err != nil {
			return TableManifest{}, fmt.Errorf("write %s newline: %w", table, err)
		}
		count++
		if err := writer.finishRow(); err != nil {
			return TableManifest{}, err
		}
	}
	if err := rows.Err(); err != nil {
		return TableManifest{}, fmt.Errorf("iterate %s: %w", table, err)
	}
	if err := writer.close(); err != nil {
		return TableManifest{}, err
	}
	return TableManifest{Name: table, Files: writer.files, Columns: columns, Rows: count}, nil
}

func exportEmbeddings(ctx context.Context, db *sql.DB, opts Options) (EmbeddingManifest, error) {
	provider := strings.ToLower(strings.TrimSpace(opts.EmbeddingProvider))
	model := strings.TrimSpace(opts.EmbeddingModel)
	inputVersion := strings.TrimSpace(opts.EmbeddingInputVersion)
	if inputVersion == "" {
		inputVersion = store.EmbeddingInputVersion
	}
	if provider == "" || model == "" {
		return EmbeddingManifest{}, errors.New("embedding provider and model are required")
	}
	relDir := filepath.ToSlash(filepath.Join("embeddings", safePathSegment(provider), safePathSegment(model), safePathSegment(inputVersion)))
	if err := os.RemoveAll(filepath.Join(opts.RepoPath, "embeddings")); err != nil {
		return EmbeddingManifest{}, fmt.Errorf("reset embeddings dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(opts.RepoPath, filepath.FromSlash(relDir)), 0o755); err != nil {
		return EmbeddingManifest{}, fmt.Errorf("mkdir %s: %w", relDir, err)
	}
	rows, err := db.QueryContext(ctx, `
		select e.message_id, e.provider, e.model, e.input_version, e.dimensions, e.embedding_blob, e.embedded_at
		from message_embeddings e
		join messages m on m.id = e.message_id
		where e.provider = ? and e.model = ? and e.input_version = ? and m.guild_id <> ?
		order by e.message_id
	`, provider, model, inputVersion, directMessageGuildID)
	if err != nil {
		return EmbeddingManifest{}, fmt.Errorf("query message_embeddings: %w", err)
	}
	defer func() { _ = rows.Close() }()
	writer := tableShardWriter{rootDir: opts.RepoPath, relDir: relDir, label: "message_embeddings"}
	if err := writer.open(); err != nil {
		return EmbeddingManifest{}, err
	}
	defer func() { _ = writer.close() }()
	columns := []string{"message_id", "provider", "model", "input_version", "dimensions", "embedding_blob", "embedded_at"}
	count := 0
	for rows.Next() {
		var (
			messageID  string
			rowProv    string
			rowModel   string
			rowInput   string
			dimensions int
			blob       []byte
			embeddedAt string
		)
		if err := rows.Scan(&messageID, &rowProv, &rowModel, &rowInput, &dimensions, &blob, &embeddedAt); err != nil {
			return EmbeddingManifest{}, fmt.Errorf("scan message_embeddings: %w", err)
		}
		body, err := json.Marshal(map[string]any{
			"message_id":     messageID,
			"provider":       rowProv,
			"model":          rowModel,
			"input_version":  rowInput,
			"dimensions":     dimensions,
			"embedding_blob": base64.StdEncoding.EncodeToString(blob),
			"embedded_at":    embeddedAt,
		})
		if err != nil {
			return EmbeddingManifest{}, fmt.Errorf("marshal message_embeddings row: %w", err)
		}
		if err := writer.rotateIfNeeded(); err != nil {
			return EmbeddingManifest{}, err
		}
		if _, err := writer.Write(body); err != nil {
			return EmbeddingManifest{}, fmt.Errorf("write message_embeddings row: %w", err)
		}
		if _, err := writer.Write([]byte{'\n'}); err != nil {
			return EmbeddingManifest{}, fmt.Errorf("write message_embeddings newline: %w", err)
		}
		count++
		if err := writer.finishRow(); err != nil {
			return EmbeddingManifest{}, err
		}
	}
	if err := rows.Err(); err != nil {
		return EmbeddingManifest{}, fmt.Errorf("iterate message_embeddings: %w", err)
	}
	if err := writer.close(); err != nil {
		return EmbeddingManifest{}, err
	}
	return EmbeddingManifest{
		Provider:     provider,
		Model:        model,
		InputVersion: inputVersion,
		Files:        writer.files,
		Columns:      columns,
		Rows:         count,
	}, nil
}

func importTable(ctx context.Context, tx *sql.Tx, repoPath string, table TableManifest) error {
	files := table.Files
	if len(files) == 0 && strings.TrimSpace(table.File) != "" {
		files = []string{table.File}
	}
	if len(files) == 0 {
		return fmt.Errorf("manifest table %s has no files", table.Name)
	}
	columns := importColumns(table)
	stmt, err := tx.PrepareContext(ctx, insertSQL(table.Name, columns))
	if err != nil {
		return fmt.Errorf("prepare import %s: %w", table.Name, err)
	}
	defer func() { _ = stmt.Close() }()
	for _, rel := range files {
		if err := importTableFile(ctx, stmt, repoPath, table, columns, rel); err != nil {
			return err
		}
	}
	return nil
}

func importTableFile(ctx context.Context, stmt *sql.Stmt, repoPath string, table TableManifest, columns []string, rel string) error {
	path := filepath.Join(repoPath, filepath.FromSlash(rel))
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", rel, err)
	}
	defer func() { _ = file.Close() }()
	gz, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("read gzip %s: %w", rel, err)
	}
	defer func() { _ = gz.Close() }()
	dec := json.NewDecoder(gz)
	dec.UseNumber()
	for {
		row := map[string]any{}
		err := dec.Decode(&row)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("decode %s: %w", rel, err)
		}
		if isDirectMessageSnapshotRow(table.Name, row) {
			continue
		}
		values := make([]any, len(columns))
		for i, column := range columns {
			values[i] = importValue(row[column])
		}
		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			return fmt.Errorf("insert %s: %w", table.Name, err)
		}
	}
	return nil
}

func repairImportedGuildIDs(ctx context.Context, tx *sql.Tx) error {
	repairs := []struct {
		table string
		query string
	}{
		{"messages", `
			update messages
			set guild_id = (
				select c.guild_id
				from channels c
				where c.id = messages.channel_id
			)
			where coalesce(guild_id, '') = ''
			  and exists (
				select 1
				from channels c
				where c.id = messages.channel_id
				  and coalesce(c.guild_id, '') != ''
			  )`},
		{"message_attachments", `
			update message_attachments
			set guild_id = coalesce(
				nullif((select m.guild_id from messages m where m.id = message_attachments.message_id), ''),
				(select c.guild_id from channels c where c.id = message_attachments.channel_id)
			)
			where coalesce(guild_id, '') = ''
			  and coalesce(
				nullif((select m.guild_id from messages m where m.id = message_attachments.message_id), ''),
				(select c.guild_id from channels c where c.id = message_attachments.channel_id)
			  ) is not null`},
		{"message_events", `
			update message_events
			set guild_id = coalesce(
				nullif((select m.guild_id from messages m where m.id = message_events.message_id), ''),
				(select c.guild_id from channels c where c.id = message_events.channel_id)
			)
			where coalesce(guild_id, '') = ''
			  and coalesce(
				nullif((select m.guild_id from messages m where m.id = message_events.message_id), ''),
				(select c.guild_id from channels c where c.id = message_events.channel_id)
			  ) is not null`},
		{"mention_events", `
			update mention_events
			set guild_id = coalesce(
				nullif((select m.guild_id from messages m where m.id = mention_events.message_id), ''),
				(select c.guild_id from channels c where c.id = mention_events.channel_id)
			)
			where coalesce(guild_id, '') = ''
			  and coalesce(
				nullif((select m.guild_id from messages m where m.id = mention_events.message_id), ''),
				(select c.guild_id from channels c where c.id = mention_events.channel_id)
			  ) is not null`},
	}
	for _, repair := range repairs {
		if _, err := tx.ExecContext(ctx, repair.query); err != nil {
			return fmt.Errorf("repair imported %s guild ids: %w", repair.table, err)
		}
	}
	return nil
}

func importColumns(table TableManifest) []string {
	if table.Name != "message_events" && table.Name != "mention_events" {
		return table.Columns
	}
	columns := make([]string, 0, len(table.Columns))
	for _, column := range table.Columns {
		if column != "event_id" {
			columns = append(columns, column)
		}
	}
	return columns
}

func snapshotExportQuery(table string) (string, []any) {
	switch table {
	case "guilds":
		return "select * from guilds where id != ?", []any{directMessageGuildID}
	case "channels", "members", "messages", "message_events", "message_attachments", "mention_events":
		return "select * from " + table + " where guild_id != ?", []any{directMessageGuildID}
	case "sync_state":
		return "select * from sync_state where scope not like 'wiretap:%'", nil
	default:
		return "select * from " + table, nil
	}
}

func snapshotDeleteQuery(table string) (string, []any) {
	switch table {
	case "guilds":
		return "delete from guilds where id != ?", []any{directMessageGuildID}
	case "message_events", "mention_events":
		return "delete from " + table + " where guild_id != ?", []any{directMessageGuildID}
	case "channels", "members", "messages", "message_attachments":
		return "delete from " + table + " where guild_id != ?", []any{directMessageGuildID}
	case "sync_state":
		return "delete from sync_state where scope not like 'wiretap:%'", nil
	default:
		return "delete from " + table, nil
	}
}

func isDirectMessageSnapshotRow(table string, row map[string]any) bool {
	switch table {
	case "guilds":
		return isLocalOnlyGuildID(stringValue(row["id"]))
	case "channels", "members", "messages", "message_events", "message_attachments", "mention_events":
		return isLocalOnlyGuildID(stringValue(row["guild_id"]))
	case "sync_state":
		scope := stringValue(row["scope"])
		return strings.HasPrefix(scope, "wiretap:")
	default:
		return false
	}
}

func isLocalOnlyGuildID(guildID string) bool {
	return guildID == directMessageGuildID
}

func importEmbeddings(ctx context.Context, tx *sql.Tx, opts Options, manifests []EmbeddingManifest) error {
	if len(manifests) == 0 {
		return nil
	}
	stmt, err := tx.PrepareContext(ctx, `
		insert into message_embeddings(
			message_id, provider, model, input_version, dimensions, embedding_blob, embedded_at
		) values(?, ?, ?, ?, ?, ?, ?)
		on conflict(message_id, provider, model, input_version) do update set
			dimensions = excluded.dimensions,
			embedding_blob = excluded.embedding_blob,
			embedded_at = excluded.embedded_at
	`)
	if err != nil {
		return fmt.Errorf("prepare import message_embeddings: %w", err)
	}
	defer func() { _ = stmt.Close() }()
	for _, manifest := range manifests {
		if !embeddingManifestMatches(opts, manifest) {
			continue
		}
		files := manifest.Files
		if len(files) == 0 {
			return fmt.Errorf("embedding manifest %s/%s/%s has no files", manifest.Provider, manifest.Model, manifest.InputVersion)
		}
		for _, rel := range files {
			if err := importEmbeddingFile(ctx, stmt, opts.RepoPath, rel); err != nil {
				return err
			}
		}
	}
	return nil
}

func importEmbeddingFile(ctx context.Context, stmt *sql.Stmt, repoPath, rel string) error {
	path := filepath.Join(repoPath, filepath.FromSlash(rel))
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", rel, err)
	}
	defer func() { _ = file.Close() }()
	gz, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("read gzip %s: %w", rel, err)
	}
	defer func() { _ = gz.Close() }()
	dec := json.NewDecoder(gz)
	dec.UseNumber()
	for {
		var row struct {
			MessageID     string      `json:"message_id"`
			Provider      string      `json:"provider"`
			Model         string      `json:"model"`
			InputVersion  string      `json:"input_version"`
			Dimensions    json.Number `json:"dimensions"`
			EmbeddingBlob string      `json:"embedding_blob"`
			EmbeddedAt    string      `json:"embedded_at"`
		}
		err := dec.Decode(&row)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("decode %s: %w", rel, err)
		}
		dimensions, err := strconv.Atoi(row.Dimensions.String())
		if err != nil {
			return fmt.Errorf("decode dimensions in %s: %w", rel, err)
		}
		blob, err := base64.StdEncoding.DecodeString(row.EmbeddingBlob)
		if err != nil {
			return fmt.Errorf("decode embedding blob in %s: %w", rel, err)
		}
		if _, err := stmt.ExecContext(ctx, row.MessageID, row.Provider, row.Model, row.InputVersion, dimensions, blob, row.EmbeddedAt); err != nil {
			return fmt.Errorf("insert message_embeddings: %w", err)
		}
	}
	return nil
}

func embeddingManifestMatches(opts Options, manifest EmbeddingManifest) bool {
	if strings.TrimSpace(opts.EmbeddingProvider) != "" && manifest.Provider != strings.ToLower(strings.TrimSpace(opts.EmbeddingProvider)) {
		return false
	}
	if strings.TrimSpace(opts.EmbeddingModel) != "" && manifest.Model != strings.TrimSpace(opts.EmbeddingModel) {
		return false
	}
	inputVersion := strings.TrimSpace(opts.EmbeddingInputVersion)
	if inputVersion == "" {
		inputVersion = store.EmbeddingInputVersion
	}
	return manifest.InputVersion == inputVersion
}

type tableShardWriter struct {
	rootDir     string
	relDir      string
	label       string
	nextShard   int
	rowsInShard int
	files       []string
	file        *os.File
	counter     *countingWriter
	gz          *gzip.Writer
}

func (w *tableShardWriter) open() error {
	rel := filepath.ToSlash(filepath.Join(w.relDir, fmt.Sprintf("%06d.jsonl.gz", w.nextShard)))
	path := filepath.Join(w.rootDir, filepath.FromSlash(rel))
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", rel, err)
	}
	w.nextShard++
	w.rowsInShard = 0
	w.files = append(w.files, rel)
	w.file = file
	w.counter = &countingWriter{w: file}
	w.gz = gzip.NewWriter(w.counter)
	return nil
}

func (w *tableShardWriter) Write(p []byte) (int, error) {
	return w.gz.Write(p)
}

func (w *tableShardWriter) rotateIfNeeded() error {
	if maxShardBytes <= 0 || w.rowsInShard == 0 || w.counter.n < maxShardBytes {
		return nil
	}
	if err := w.close(); err != nil {
		return err
	}
	return w.open()
}

func (w *tableShardWriter) finishRow() error {
	w.rowsInShard++
	if maxShardBytes > 1024*1024 && w.rowsInShard%shardFlushRows != 0 {
		return nil
	}
	if err := w.gz.Flush(); err != nil {
		return fmt.Errorf("flush %s shard: %w", w.label, err)
	}
	return nil
}

func (w *tableShardWriter) close() error {
	var closeErr error
	if w.gz != nil {
		if err := w.gz.Close(); err != nil {
			closeErr = err
		}
		w.gz = nil
	}
	if w.file != nil {
		if err := w.file.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
		w.file = nil
	}
	if closeErr != nil {
		return fmt.Errorf("close %s shard: %w", w.label, closeErr)
	}
	return nil
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += int64(n)
	return n, err
}

func exportValue(value any) any {
	switch v := value.(type) {
	case []byte:
		return string(v)
	default:
		return v
	}
}

func importValue(value any) any {
	switch v := value.(type) {
	case json.Number:
		if i, err := strconv.ParseInt(v.String(), 10, 64); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(v.String(), 64); err == nil {
			return f
		}
		return v.String()
	default:
		return v
	}
}

func stringValue(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case json.Number:
		return v.String()
	default:
		return ""
	}
}

func insertSQL(table string, columns []string) string {
	quoted := make([]string, len(columns))
	placeholders := make([]string, len(columns))
	for i, column := range columns {
		quoted[i] = quoteIdent(column)
		placeholders[i] = "?"
	}
	return "insert into " + quoteIdent(table) + "(" + strings.Join(quoted, ",") + ") values(" + strings.Join(placeholders, ",") + ")"
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func safePathSegment(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "_"
	}
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_' || r == '.':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return b.String()
}

func run(ctx context.Context, dir, name string, args ...string) error {
	out, err := output(ctx, dir, name, args...)
	if err != nil {
		return fmt.Errorf("%s %s: %w\n%s", name, strings.Join(args, " "), err, strings.TrimSpace(out))
	}
	return nil
}

func output(ctx context.Context, dir, name string, args ...string) (string, error) {
	// #nosec G204 -- discrawl invokes the Git executable with argv, never through a shell.
	cmd := exec.CommandContext(ctx, name, args...)
	if dir != "" {
		cmd.Dir = dir
	}
	body, err := cmd.CombinedOutput()
	return string(body), err
}

func isNonFastForwardPush(out string) bool {
	lower := strings.ToLower(out)
	return strings.Contains(lower, "non-fast-forward") ||
		strings.Contains(lower, "fetch first") ||
		strings.Contains(lower, "failed to push some refs")
}
