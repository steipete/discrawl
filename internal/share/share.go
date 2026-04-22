package share

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/steipete/discrawl/internal/store"
)

const (
	ManifestName                = "manifest.json"
	LastImportSyncScope         = "share:last_import_at"
	LastImportManifestSyncScope = "share:last_import_manifest_generated_at"
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
	RepoPath string
	Remote   string
	Branch   string
}

type Manifest struct {
	Version     int               `json:"version"`
	GeneratedAt time.Time         `json:"generated_at"`
	Tables      []TableManifest   `json:"tables"`
	Files       map[string]string `json:"files,omitempty"`
}

type TableManifest struct {
	Name    string   `json:"name"`
	File    string   `json:"file,omitempty"`
	Files   []string `json:"files,omitempty"`
	Columns []string `json:"columns"`
	Rows    int      `json:"rows"`
}

func EnsureRepo(ctx context.Context, opts Options) error {
	if strings.TrimSpace(opts.RepoPath) == "" {
		return fmt.Errorf("share repo path is empty")
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
	dataDir := filepath.Join(opts.RepoPath, "tables")
	if err := os.RemoveAll(dataDir); err != nil {
		return Manifest{}, fmt.Errorf("reset tables dir: %w", err)
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return Manifest{}, fmt.Errorf("mkdir tables dir: %w", err)
	}
	manifest := Manifest{
		Version:     1,
		GeneratedAt: time.Now().UTC(),
		Files:       map[string]string{"manifest": ManifestName},
	}
	for _, table := range SnapshotTables {
		entry, err := exportTable(ctx, s.DB(), dataDir, table)
		if err != nil {
			return Manifest{}, err
		}
		manifest.Tables = append(manifest.Tables, entry)
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
		if _, err := tx.ExecContext(ctx, "delete from "+table); err != nil {
			return Manifest{}, fmt.Errorf("clear %s: %w", table, err)
		}
	}
	for i := len(SnapshotTables) - 1; i >= 0; i-- {
		table := SnapshotTables[i]
		if _, err := tx.ExecContext(ctx, "delete from "+table); err != nil {
			return Manifest{}, fmt.Errorf("clear %s: %w", table, err)
		}
	}
	for _, table := range manifest.Tables {
		if err := importTable(ctx, tx, opts.RepoPath, table); err != nil {
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
	// Snapshot imports replace derived local state; disabling the write journal
	// avoids preserving a multi-GB cache that can be recreated from Git.
	for _, stmt := range []string{
		`pragma temp_store = memory`,
		`pragma cache_size = -262144`,
		`pragma synchronous = off`,
		`pragma journal_mode = off`,
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

func exportTable(ctx context.Context, db *sql.DB, dataDir, table string) (TableManifest, error) {
	query := "select * from " + table
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return TableManifest{}, fmt.Errorf("query %s: %w", table, err)
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		return TableManifest{}, fmt.Errorf("columns %s: %w", table, err)
	}
	tableDir := filepath.Join(dataDir, table)
	if err := os.MkdirAll(tableDir, 0o755); err != nil {
		return TableManifest{}, fmt.Errorf("mkdir %s: %w", table, err)
	}
	writer := tableShardWriter{dataDir: dataDir, table: table}
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

func importTable(ctx context.Context, tx *sql.Tx, repoPath string, table TableManifest) error {
	files := table.Files
	if len(files) == 0 && strings.TrimSpace(table.File) != "" {
		files = []string{table.File}
	}
	if len(files) == 0 {
		return fmt.Errorf("manifest table %s has no files", table.Name)
	}
	stmt, err := tx.PrepareContext(ctx, insertSQL(table.Name, table.Columns))
	if err != nil {
		return fmt.Errorf("prepare import %s: %w", table.Name, err)
	}
	defer func() { _ = stmt.Close() }()
	for _, rel := range files {
		if err := importTableFile(ctx, stmt, repoPath, table, rel); err != nil {
			return err
		}
	}
	return nil
}

func importTableFile(ctx context.Context, stmt *sql.Stmt, repoPath string, table TableManifest, rel string) error {
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
		values := make([]any, len(table.Columns))
		for i, column := range table.Columns {
			values[i] = importValue(row[column])
		}
		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			return fmt.Errorf("insert %s: %w", table.Name, err)
		}
	}
	return nil
}

type tableShardWriter struct {
	dataDir     string
	table       string
	nextShard   int
	rowsInShard int
	files       []string
	file        *os.File
	counter     *countingWriter
	gz          *gzip.Writer
}

func (w *tableShardWriter) open() error {
	rel := filepath.ToSlash(filepath.Join("tables", w.table, fmt.Sprintf("%06d.jsonl.gz", w.nextShard)))
	path := filepath.Join(w.dataDir, w.table, fmt.Sprintf("%06d.jsonl.gz", w.nextShard))
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
		return fmt.Errorf("flush %s shard: %w", w.table, err)
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
		return fmt.Errorf("close %s shard: %w", w.table, closeErr)
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
