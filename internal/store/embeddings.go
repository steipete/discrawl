package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/openclaw/discrawl/internal/embed"
)

const (
	EmbeddingInputVersion = "message_normalized_v1"
	defaultEmbedLimit     = 1000
	maxEmbeddingAttempts  = 3
	maxStoredErrorChars   = 500
	embeddingLockTimeout  = 15 * time.Minute
)

type EmbeddingDrainOptions struct {
	Provider      string
	Model         string
	InputVersion  string
	Limit         int
	BatchSize     int
	MaxInputChars int
	Now           func() time.Time
}

type EmbeddingDrainStats struct {
	Processed        int    `json:"processed"`
	Succeeded        int    `json:"succeeded"`
	Failed           int    `json:"failed"`
	Skipped          int    `json:"skipped"`
	Requeued         int    `json:"requeued,omitempty"`
	RemainingBacklog int    `json:"remaining_backlog"`
	Provider         string `json:"provider"`
	Model            string `json:"model"`
	InputVersion     string `json:"input_version"`
	RateLimited      bool   `json:"rate_limited,omitempty"`
}

type embeddingJob struct {
	MessageID         string
	NormalizedContent string
	Attempts          int
	Provider          string
	Model             string
	InputVersion      string
}

func DefaultEmbedLimit() int {
	return defaultEmbedLimit
}

func (s *Store) DrainEmbeddingJobs(ctx context.Context, provider embed.Provider, opts EmbeddingDrainOptions) (EmbeddingDrainStats, error) {
	opts = normalizeEmbeddingDrainOptions(opts)
	stats := EmbeddingDrainStats{
		Provider:     opts.Provider,
		Model:        opts.Model,
		InputVersion: opts.InputVersion,
	}
	if provider == nil {
		return stats, errors.New("embedding provider is nil")
	}
	now := opts.Now()
	staleBefore := now.Add(-embeddingLockTimeout).Format(timeLayout)
	jobs, err := s.pendingEmbeddingJobs(ctx, opts.Limit, staleBefore)
	if err != nil {
		return stats, err
	}
	var batch []embeddingJob
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		rateLimited, err := s.processEmbeddingBatch(ctx, provider, opts, batch, &stats)
		batch = batch[:0]
		if err != nil {
			return err
		}
		if rateLimited {
			stats.RateLimited = true
		}
		return nil
	}
	for _, job := range jobs {
		if !sameEmbeddingIdentity(job, opts) {
			resetAttempts := !emptyEmbeddingIdentity(job)
			if err := s.resetEmbeddingJobIdentity(ctx, job.MessageID, opts, resetAttempts); err != nil {
				return stats, err
			}
			job.Provider = opts.Provider
			job.Model = opts.Model
			job.InputVersion = opts.InputVersion
			if resetAttempts {
				job.Attempts = 0
			}
		}
		if strings.TrimSpace(job.NormalizedContent) == "" {
			if err := s.markEmbeddingJobsDone(ctx, opts, []embeddingJob{job}); err != nil {
				return stats, err
			}
			stats.Processed++
			stats.Skipped++
			continue
		}
		batch = append(batch, job)
		if len(batch) >= opts.BatchSize {
			if err := flush(); err != nil {
				return stats, err
			}
			if stats.RateLimited {
				break
			}
		}
	}
	if !stats.RateLimited {
		if err := flush(); err != nil {
			return stats, err
		}
	}
	stats.RemainingBacklog, err = s.EmbeddingBacklog(ctx)
	if err != nil {
		return stats, err
	}
	return stats, nil
}

func normalizeEmbeddingDrainOptions(opts EmbeddingDrainOptions) EmbeddingDrainOptions {
	opts.Provider = strings.ToLower(strings.TrimSpace(opts.Provider))
	opts.Model = strings.TrimSpace(opts.Model)
	opts.InputVersion = strings.TrimSpace(opts.InputVersion)
	if opts.InputVersion == "" {
		opts.InputVersion = EmbeddingInputVersion
	}
	if opts.Limit <= 0 {
		opts.Limit = defaultEmbedLimit
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = embed.DefaultBatchSize
	}
	if opts.BatchSize > opts.Limit {
		opts.BatchSize = opts.Limit
	}
	if opts.MaxInputChars <= 0 {
		opts.MaxInputChars = embed.DefaultMaxInputChars
	}
	if opts.Now == nil {
		opts.Now = func() time.Time { return time.Now().UTC() }
	}
	return opts
}

func sameEmbeddingIdentity(job embeddingJob, opts EmbeddingDrainOptions) bool {
	return job.Provider == opts.Provider && job.Model == opts.Model && job.InputVersion == opts.InputVersion
}

func emptyEmbeddingIdentity(job embeddingJob) bool {
	return job.Provider == "" && job.Model == "" && job.InputVersion == ""
}

func (s *Store) pendingEmbeddingJobs(ctx context.Context, limit int, staleBefore string) ([]embeddingJob, error) {
	rows, err := s.db.QueryContext(ctx, `
		select
			j.message_id,
			m.normalized_content,
			j.attempts,
			j.provider,
			j.model,
			j.input_version
		from embedding_jobs j
		join messages m on m.id = j.message_id
		where j.state = 'pending'
		  and (j.locked_at is null or j.locked_at = '' or j.locked_at < ?)
		order by j.updated_at, j.message_id
		limit ?
	`, staleBefore, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var jobs []embeddingJob
	for rows.Next() {
		var job embeddingJob
		if err := rows.Scan(&job.MessageID, &job.NormalizedContent, &job.Attempts, &job.Provider, &job.Model, &job.InputVersion); err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (s *Store) resetEmbeddingJobIdentity(ctx context.Context, messageID string, opts EmbeddingDrainOptions, resetAttempts bool) error {
	if resetAttempts {
		_, err := s.db.ExecContext(ctx, `
			update embedding_jobs
			set provider = ?,
				model = ?,
				input_version = ?,
				attempts = 0,
				last_error = '',
				locked_at = null,
				updated_at = ?
			where message_id = ?
		`, opts.Provider, opts.Model, opts.InputVersion, opts.Now().Format(timeLayout), messageID)
		return err
	}
	_, err := s.db.ExecContext(ctx, `
		update embedding_jobs
		set provider = ?,
			model = ?,
			input_version = ?,
			last_error = '',
			locked_at = null,
			updated_at = ?
		where message_id = ?
	`, opts.Provider, opts.Model, opts.InputVersion, opts.Now().Format(timeLayout), messageID)
	return err
}

func (s *Store) processEmbeddingBatch(ctx context.Context, provider embed.Provider, opts EmbeddingDrainOptions, jobs []embeddingJob, stats *EmbeddingDrainStats) (bool, error) {
	now := opts.Now()
	lockedAt := now.Format(timeLayout)
	staleBefore := now.Add(-embeddingLockTimeout).Format(timeLayout)
	claimed, err := s.lockEmbeddingJobs(ctx, jobs, lockedAt, staleBefore)
	if err != nil {
		return false, err
	}
	if len(claimed) == 0 {
		return false, nil
	}
	jobs = claimed
	inputs := make([]string, 0, len(jobs))
	for _, job := range jobs {
		inputs = append(inputs, capRunes(job.NormalizedContent, opts.MaxInputChars))
	}
	batch, err := provider.Embed(ctx, inputs)
	if err != nil {
		if embed.IsRateLimitError(err) {
			if markErr := s.markEmbeddingJobsRateLimited(ctx, opts, jobs, err); markErr != nil {
				return false, markErr
			}
			stats.Requeued += len(jobs)
			return true, nil
		}
		if markErr := s.markEmbeddingJobsFailed(ctx, opts, jobs, err); markErr != nil {
			return false, markErr
		}
		stats.Processed += len(jobs)
		stats.Failed += len(jobs)
		return embed.IsRateLimitError(err), nil
	}
	dimensions, err := validateEmbeddingBatch(batch, len(jobs))
	if err != nil {
		if markErr := s.markEmbeddingJobsFailed(ctx, opts, jobs, err); markErr != nil {
			return false, markErr
		}
		stats.Processed += len(jobs)
		stats.Failed += len(jobs)
		return false, nil
	}
	if err := s.storeEmbeddingBatch(ctx, opts, jobs, batch.Vectors, dimensions); err != nil {
		return false, err
	}
	stats.Processed += len(jobs)
	stats.Succeeded += len(jobs)
	return false, nil
}

func (s *Store) lockEmbeddingJobs(ctx context.Context, jobs []embeddingJob, lockedAt, staleBefore string) ([]embeddingJob, error) {
	if len(jobs) == 0 {
		return nil, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	claimed := make([]embeddingJob, 0, len(jobs))
	for _, job := range jobs {
		result, err := tx.ExecContext(ctx, `
			update embedding_jobs
			set locked_at = ?, updated_at = ?
			where message_id = ?
			  and state = 'pending'
			  and (locked_at is null or locked_at = '' or locked_at < ?)
		`, lockedAt, lockedAt, job.MessageID, staleBefore)
		if err != nil {
			return nil, err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return nil, err
		}
		if rows == 1 {
			claimed = append(claimed, job)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return claimed, nil
}

func validateEmbeddingBatch(batch embed.EmbeddingBatch, expected int) (int, error) {
	if len(batch.Vectors) != expected {
		return 0, fmt.Errorf("embedding provider returned %d vectors for %d inputs", len(batch.Vectors), expected)
	}
	dimensions := batch.Dimensions
	for _, vector := range batch.Vectors {
		if len(vector) == 0 {
			return 0, errors.New("embedding provider returned an empty vector")
		}
		if dimensions == 0 {
			dimensions = len(vector)
			continue
		}
		if len(vector) != dimensions {
			return 0, fmt.Errorf("embedding provider dimensions mismatch: got %d want %d", len(vector), dimensions)
		}
	}
	return dimensions, nil
}

func (s *Store) storeEmbeddingBatch(ctx context.Context, opts EmbeddingDrainOptions, jobs []embeddingJob, vectors [][]float32, dimensions int) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	embeddedAt := opts.Now().Format(timeLayout)
	for i, job := range jobs {
		blob, err := EncodeEmbeddingVector(vectors[i])
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			insert into message_embeddings(
				message_id, provider, model, input_version, dimensions, embedding_blob, embedded_at
			) values(?, ?, ?, ?, ?, ?, ?)
			on conflict(message_id, provider, model, input_version) do update set
				dimensions = excluded.dimensions,
				embedding_blob = excluded.embedding_blob,
				embedded_at = excluded.embedded_at
		`, job.MessageID, opts.Provider, opts.Model, opts.InputVersion, dimensions, blob, embeddedAt); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			update embedding_jobs
			set state = 'done',
				attempts = 0,
				provider = ?,
				model = ?,
				input_version = ?,
				last_error = '',
				locked_at = null,
				updated_at = ?
			where message_id = ?
		`, opts.Provider, opts.Model, opts.InputVersion, embeddedAt, job.MessageID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) markEmbeddingJobsDone(ctx context.Context, opts EmbeddingDrainOptions, jobs []embeddingJob) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	now := opts.Now().Format(timeLayout)
	for _, job := range jobs {
		if _, err := tx.ExecContext(ctx, `delete from message_embeddings where message_id = ?`, job.MessageID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			update embedding_jobs
			set state = 'done',
				provider = ?,
				model = ?,
				input_version = ?,
				last_error = '',
				locked_at = null,
				updated_at = ?
			where message_id = ?
		`, opts.Provider, opts.Model, opts.InputVersion, now, job.MessageID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) markEmbeddingJobsRateLimited(ctx context.Context, opts EmbeddingDrainOptions, jobs []embeddingJob, cause error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	now := opts.Now().Format(timeLayout)
	lastError := trimStoredError(cause)
	for _, job := range jobs {
		if _, err := tx.ExecContext(ctx, `
			update embedding_jobs
			set state = 'pending',
				provider = ?,
				model = ?,
				input_version = ?,
				last_error = ?,
				locked_at = null,
				updated_at = ?
			where message_id = ?
		`, opts.Provider, opts.Model, opts.InputVersion, lastError, now, job.MessageID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) markEmbeddingJobsFailed(ctx context.Context, opts EmbeddingDrainOptions, jobs []embeddingJob, cause error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	now := opts.Now().Format(timeLayout)
	lastError := trimStoredError(cause)
	for _, job := range jobs {
		attempts := job.Attempts + 1
		state := "pending"
		if attempts >= maxEmbeddingAttempts {
			state = "failed"
		}
		if _, err := tx.ExecContext(ctx, `
			update embedding_jobs
			set state = ?,
				attempts = ?,
				provider = ?,
				model = ?,
				input_version = ?,
				last_error = ?,
				locked_at = null,
				updated_at = ?
			where message_id = ?
		`, state, attempts, opts.Provider, opts.Model, opts.InputVersion, lastError, now, job.MessageID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func trimStoredError(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.TrimSpace(err.Error())
	runes := []rune(msg)
	if len(runes) > maxStoredErrorChars {
		msg = string(runes[:maxStoredErrorChars])
	}
	return msg
}

func capRunes(value string, maxChars int) string {
	if maxChars <= 0 {
		return value
	}
	runes := []rune(value)
	if len(runes) <= maxChars {
		return value
	}
	return string(runes[:maxChars])
}

func EncodeEmbeddingVector(vector []float32) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, len(vector)*4))
	for _, value := range vector {
		if err := binary.Write(buf, binary.LittleEndian, value); err != nil {
			return nil, fmt.Errorf("encode embedding vector: %w", err)
		}
	}
	return buf.Bytes(), nil
}

func DecodeEmbeddingVector(blob []byte) ([]float32, error) {
	if len(blob)%4 != 0 {
		return nil, fmt.Errorf("embedding blob length %d is not a float32 multiple", len(blob))
	}
	out := make([]float32, len(blob)/4)
	reader := bytes.NewReader(blob)
	for i := range out {
		if err := binary.Read(reader, binary.LittleEndian, &out[i]); err != nil {
			return nil, fmt.Errorf("decode embedding vector: %w", err)
		}
	}
	return out, nil
}

func (s *Store) EmbeddingBacklog(ctx context.Context) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `select count(*) from embedding_jobs where state = 'pending'`).Scan(&count)
	return count, err
}

func (s *Store) RequeueAllEmbeddingJobs(ctx context.Context, opts EmbeddingDrainOptions) (int, error) {
	opts = normalizeEmbeddingDrainOptions(opts)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer rollback(tx)
	now := opts.Now().Format(timeLayout)
	if _, err := tx.ExecContext(ctx, `
		insert or ignore into embedding_jobs(
			message_id, state, attempts, provider, model, input_version, last_error, locked_at, updated_at
		)
		select id, 'pending', 0, ?, ?, ?, '', null, ?
		from messages
	`, opts.Provider, opts.Model, opts.InputVersion, now); err != nil {
		return 0, err
	}
	result, err := tx.ExecContext(ctx, `
		update embedding_jobs
		set state = 'pending',
			attempts = 0,
			provider = ?,
			model = ?,
			input_version = ?,
			last_error = '',
			locked_at = null,
			updated_at = ?
		where message_id in (select id from messages)
	`, opts.Provider, opts.Model, opts.InputVersion, now)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(affected), nil
}
