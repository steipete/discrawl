package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

// MetaStore manages cross-guild metadata: guild registry, sync state, embedding jobs.
type MetaStore struct {
	db   *sql.DB
	path string
}

// MetaGuild represents a guild entry in meta.db.
type MetaGuild struct {
	ID        string
	Name      string
	Icon      string
	DBPath    string // relative path: guilds/123456789.db
	UpdatedAt time.Time
}

// OpenMetaStore opens or creates the meta.db database.
func OpenMetaStore(ctx context.Context, path string) (*MetaStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir meta db dir: %w", err)
	}
	if err := ensureDBFile(path); err != nil {
		return nil, err
	}
	dsn := fmt.Sprintf(
		"file:%s?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=temp_store(MEMORY)&_pragma=busy_timeout(5000)",
		path,
	)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open meta sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping meta sqlite: %w", err)
	}
	if runtime.GOOS != "windows" {
		_ = os.Chmod(path, 0o600)
	}
	ms := &MetaStore{db: db, path: path}
	if err := ms.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return ms, nil
}

func (ms *MetaStore) migrate(ctx context.Context) error {
	stmts := []string{
		`create table if not exists guilds (
			id text primary key,
			name text not null,
			icon text,
			db_path text not null,
			updated_at text not null
		);`,
		`create table if not exists sync_state (
			guild_id text not null,
			scope text not null,
			cursor text,
			updated_at text not null,
			primary key (guild_id, scope)
		);`,
		`create table if not exists embedding_jobs (
			guild_id text not null,
			message_id text not null,
			state text not null default 'pending',
			attempts integer not null default 0,
			primary key (guild_id, message_id)
		);`,
		`create table if not exists users (
			id text primary key,
			username text not null,
			avatar text,
			access_token_enc text,
			refresh_token_enc text,
			token_expiry text,
			created_at text,
			updated_at text
		);`,
		`create table if not exists user_guilds (
			user_id text not null,
			guild_id text not null,
			guild_name text,
			primary key (user_id, guild_id)
		);`,
		`create table if not exists sessions (
			token text primary key,
			data blob not null,
			expiry real not null
		);`,
		`create index if not exists idx_sessions_expiry on sessions(expiry);`,
		`create table if not exists alerts (
			id text primary key,
			guild_id text not null,
			user_id text not null,
			keywords text not null,
			created_at text not null
		);`,
		`create index if not exists idx_alerts_guild on alerts(guild_id);`,
	}
	for _, stmt := range stmts {
		if _, err := ms.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("migrate meta db: %w", err)
		}
	}
	return nil
}

// RegisterGuild adds or updates a guild in the registry.
func (ms *MetaStore) RegisterGuild(ctx context.Context, guild MetaGuild) error {
	now := time.Now().UTC().Format(timeLayout)
	_, err := ms.db.ExecContext(ctx, `
		insert into guilds(id, name, icon, db_path, updated_at)
		values(?, ?, ?, ?, ?)
		on conflict(id) do update set
			name=excluded.name, icon=excluded.icon,
			db_path=excluded.db_path, updated_at=excluded.updated_at
	`, guild.ID, guild.Name, guild.Icon, guild.DBPath, now)
	return err
}

// ListGuilds returns all registered guilds.
func (ms *MetaStore) ListGuilds(ctx context.Context) ([]MetaGuild, error) {
	rows, err := ms.db.QueryContext(ctx, `select id, name, coalesce(icon, ''), db_path, updated_at from guilds order by id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []MetaGuild
	for rows.Next() {
		var g MetaGuild
		var updatedAt string
		if err := rows.Scan(&g.ID, &g.Name, &g.Icon, &g.DBPath, &updatedAt); err != nil {
			return nil, err
		}
		g.UpdatedAt = parseTime(updatedAt)
		out = append(out, g)
	}
	return out, rows.Err()
}

// SetSyncState sets a sync state checkpoint scoped to a guild.
func (ms *MetaStore) SetSyncState(ctx context.Context, guildID, scope, cursor string) error {
	now := time.Now().UTC().Format(timeLayout)
	_, err := ms.db.ExecContext(ctx, `
		insert into sync_state(guild_id, scope, cursor, updated_at)
		values(?, ?, ?, ?)
		on conflict(guild_id, scope) do update set
			cursor=excluded.cursor, updated_at=excluded.updated_at
	`, guildID, scope, cursor, now)
	return err
}

// GetSyncState retrieves a sync state cursor for a guild.
func (ms *MetaStore) GetSyncState(ctx context.Context, guildID, scope string) (string, error) {
	var cursor sql.NullString
	err := ms.db.QueryRowContext(ctx, `
		select cursor from sync_state where guild_id = ? and scope = ?
	`, guildID, scope).Scan(&cursor)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", err
	}
	return cursor.String, nil
}

// UserRecord holds a Discord user's auth data for storage.
type UserRecord struct {
	ID           string
	Username     string
	Avatar       string
	AccessToken  string
	RefreshToken string
	TokenExpiry  string
	CreatedAt    string
	UpdatedAt    string
}

// UserGuildRecord links a user to a guild they belong to.
type UserGuildRecord struct {
	UserID    string
	GuildID   string
	GuildName string
}

// UpsertUser inserts or updates a user record.
func (ms *MetaStore) UpsertUser(ctx context.Context, u UserRecord) error {
	now := time.Now().UTC().Format(timeLayout)
	_, err := ms.db.ExecContext(ctx, `
		insert into users(id, username, avatar, access_token_enc, refresh_token_enc, token_expiry, created_at, updated_at)
		values(?, ?, ?, ?, ?, ?, coalesce(?, ?), ?)
		on conflict(id) do update set
			username=excluded.username,
			avatar=excluded.avatar,
			access_token_enc=excluded.access_token_enc,
			refresh_token_enc=excluded.refresh_token_enc,
			token_expiry=excluded.token_expiry,
			updated_at=excluded.updated_at
	`, u.ID, u.Username, u.Avatar, u.AccessToken, u.RefreshToken, u.TokenExpiry, u.CreatedAt, now, now)
	return err
}

// GetUser retrieves a user by ID.
func (ms *MetaStore) GetUser(ctx context.Context, userID string) (UserRecord, error) {
	var u UserRecord
	err := ms.db.QueryRowContext(ctx, `
		select id, username, coalesce(avatar,''), coalesce(access_token_enc,''),
		       coalesce(refresh_token_enc,''), coalesce(token_expiry,''),
		       coalesce(created_at,''), coalesce(updated_at,'')
		from users where id = ?
	`, userID).Scan(&u.ID, &u.Username, &u.Avatar, &u.AccessToken, &u.RefreshToken, &u.TokenExpiry, &u.CreatedAt, &u.UpdatedAt)
	if err != nil {
		return UserRecord{}, err
	}
	return u, nil
}

// UpsertUserGuild inserts or updates a user-guild association.
func (ms *MetaStore) UpsertUserGuild(ctx context.Context, ug UserGuildRecord) error {
	_, err := ms.db.ExecContext(ctx, `
		insert into user_guilds(user_id, guild_id, guild_name)
		values(?, ?, ?)
		on conflict(user_id, guild_id) do update set guild_name=excluded.guild_name
	`, ug.UserID, ug.GuildID, ug.GuildName)
	return err
}

// UserGuilds returns all guild associations for a user.
func (ms *MetaStore) UserGuilds(ctx context.Context, userID string) ([]UserGuildRecord, error) {
	rows, err := ms.db.QueryContext(ctx, `
		select user_id, guild_id, coalesce(guild_name,'') from user_guilds where user_id = ? order by guild_id
	`, userID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []UserGuildRecord
	for rows.Next() {
		var ug UserGuildRecord
		if err := rows.Scan(&ug.UserID, &ug.GuildID, &ug.GuildName); err != nil {
			return nil, err
		}
		out = append(out, ug)
	}
	return out, rows.Err()
}

// UserHasGuild returns true if the user has an association with the given guild.
func (ms *MetaStore) UserHasGuild(ctx context.Context, userID, guildID string) (bool, error) {
	var count int
	err := ms.db.QueryRowContext(ctx, `
		select count(*) from user_guilds where user_id = ? and guild_id = ?
	`, userID, guildID).Scan(&count)
	return count > 0, err
}

// DB returns the underlying database connection.
func (ms *MetaStore) DB() *sql.DB {
	return ms.db
}

// AlertRecord represents a keyword alert configuration.
type AlertRecord struct {
	ID        string
	GuildID   string
	UserID    string
	Keywords  string // comma-separated list
	CreatedAt string
}

// CreateAlert inserts a new alert.
func (ms *MetaStore) CreateAlert(ctx context.Context, alert AlertRecord) error {
	now := time.Now().UTC().Format(timeLayout)
	_, err := ms.db.ExecContext(ctx, `
		insert into alerts(id, guild_id, user_id, keywords, created_at)
		values(?, ?, ?, ?, ?)
	`, alert.ID, alert.GuildID, alert.UserID, alert.Keywords, now)
	return err
}

// GetAlert retrieves an alert by ID.
func (ms *MetaStore) GetAlert(ctx context.Context, id string) (AlertRecord, error) {
	var alert AlertRecord
	err := ms.db.QueryRowContext(ctx, `
		select id, guild_id, user_id, keywords, created_at
		from alerts where id = ?
	`, id).Scan(&alert.ID, &alert.GuildID, &alert.UserID, &alert.Keywords, &alert.CreatedAt)
	if err != nil {
		return AlertRecord{}, err
	}
	return alert, nil
}

// ListAlerts returns all alerts for a guild.
func (ms *MetaStore) ListAlerts(ctx context.Context, guildID string) ([]AlertRecord, error) {
	rows, err := ms.db.QueryContext(ctx, `
		select id, guild_id, user_id, keywords, created_at
		from alerts where guild_id = ? order by created_at desc
	`, guildID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []AlertRecord
	for rows.Next() {
		var alert AlertRecord
		if err := rows.Scan(&alert.ID, &alert.GuildID, &alert.UserID, &alert.Keywords, &alert.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, alert)
	}
	return out, rows.Err()
}

// UpdateAlert updates an alert's keywords.
func (ms *MetaStore) UpdateAlert(ctx context.Context, id, keywords string) error {
	_, err := ms.db.ExecContext(ctx, `
		update alerts set keywords = ? where id = ?
	`, keywords, id)
	return err
}

// DeleteAlert removes an alert.
func (ms *MetaStore) DeleteAlert(ctx context.Context, id string) error {
	_, err := ms.db.ExecContext(ctx, `delete from alerts where id = ?`, id)
	return err
}

// Close closes the meta database.
func (ms *MetaStore) Close() error {
	if ms == nil || ms.db == nil {
		return nil
	}
	return ms.db.Close()
}
