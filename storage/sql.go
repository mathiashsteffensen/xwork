package storage

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"github.com/mathiashsteffensen/xwork/v2"
)

type QueryObject interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

type SQL struct {
	db *sql.DB
	q  QueryObject
}

func NewSQL(db *sql.DB) SQL {
	return newSQL(db, db)
}

func newSQL(db *sql.DB, q QueryObject) SQL {
	return SQL{db: db, q: q}

}

func (s SQL) Initialize() error {
	_, err := s.db.Exec(
		`CREATE TABLE IF NOT EXISTS xwork_schedule (
				id UUID PRIMARY KEY,
				name TEXT NOT NULL,
				queue TEXT NOT NULL,
				payload JSONB NOT NULL,
				enqueue_at TIMESTAMP NOT NULL,
				scheduled_at TIMESTAMP NOT NULL
		)`,
	)
	if err != nil {
		return err
	}

	_, err = s.db.Exec(
		"CREATE INDEX IF NOT EXISTS idx_xwork_schedule_enqueue_at ON xwork_schedule (enqueue_at)",
	)
	if err != nil {
		return err
	}

	_, err = s.db.Exec(
		`CREATE TABLE IF NOT EXISTS xwork_queue (
				id UUID PRIMARY KEY,
				name TEXT NOT NULL,
				queue TEXT NOT NULL,
				payload JSONB NOT NULL,
				retry_count INT DEFAULT 0,
				enqueued_at TIMESTAMP NOT NULL,
				scheduled_at TIMESTAMP NOT NULL
		)`,
	)
	if err != nil {
		return err
	}

	_, err = s.db.Exec(
		"CREATE INDEX IF NOT EXISTS idx_xwork_queue_queue ON xwork_queue (queue)",
	)
	if err != nil {
		return err
	}

	_, err = s.db.Exec(
		`CREATE TABLE IF NOT EXISTS xwork_processing (
				id UUID PRIMARY KEY,
				name TEXT NOT NULL,
				queue TEXT NOT NULL,
				payload JSONB NOT NULL,
				retry_count INT DEFAULT 0,
				started_at TIMESTAMP NOT NULL,
				enqueued_at TIMESTAMP NOT NULL,
				scheduled_at TIMESTAMP NOT NULL,
				last_heartbeat_at TIMESTAMP NOT NULL
		)`,
	)
	if err != nil {
		return err
	}

	_, err = s.db.Exec(
		"CREATE INDEX IF NOT EXISTS idx_xwork_processing_started_at ON xwork_processing (started_at)",
	)
	if err != nil {
		return err
	}

	_, err = s.db.Exec(
		`CREATE TABLE IF NOT EXISTS xwork_processed (
				id UUID PRIMARY KEY,
				name TEXT NOT NULL,
				queue TEXT NOT NULL,
				payload JSONB NOT NULL,
				started_at TIMESTAMP NOT NULL,
				completed_at TIMESTAMP NOT NULL,
				enqueued_at TIMESTAMP NOT NULL,
				scheduled_at TIMESTAMP NOT NULL
		)`,
	)
	if err != nil {
		return err
	}

	_, err = s.db.Exec(
		"CREATE INDEX IF NOT EXISTS idx_xwork_processed_started_at ON xwork_processing (started_at)",
	)
	if err != nil {
		return err
	}

	_, err = s.db.Exec(
		`CREATE TABLE IF NOT EXISTS xwork_failed (
				id UUID PRIMARY KEY,
				name TEXT NOT NULL,
				queue TEXT NOT NULL,
				payload JSONB NOT NULL,
				error TEXT NOT NULL,
				last_retry_at TIMESTAMP NOT NULL,
				next_retry_at TIMESTAMP NOT NULL,
				retry_count INT DEFAULT 0,
				scheduled_at TIMESTAMP NOT NULL
		)`,
	)
	if err != nil {
		return err
	}

	_, err = s.db.Exec(
		"CREATE INDEX IF NOT EXISTS idx_xwork_failed_next_retry_at ON xwork_failed (next_retry_at)",
	)
	if err != nil {
		return err
	}

	return nil
}

func insertToScheduled(db QueryObject, job *xwork.ScheduledJob) error {
	payload, err := job.Payload.Value()
	if err != nil {
		return err
	}

	_, err = db.Exec(
		"INSERT INTO xwork_schedule (id, name, queue, payload, enqueue_at, scheduled_at) VALUES ($1, $2, $3, $4, $5, $6)",
		job.ID, job.Name, job.Queue, payload, job.EnqueueAt, job.ScheduledAt,
	)
	return err
}

func nextFromScheduled(db QueryObject) ([]*xwork.ScheduledJob, error) {
	rows, err := db.Query(
		`SELECT id, name, queue, payload, enqueue_at, scheduled_at
			FROM xwork_schedule
			WHERE enqueue_at <= $1`,
		time.Now(),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs := make([]*xwork.ScheduledJob, 0)
	for rows.Next() {
		job := &xwork.ScheduledJob{}
		err = rows.Scan(&job.ID, &job.Name, &job.Queue, &job.Payload, &job.EnqueueAt, &job.ScheduledAt)
		if err != nil {
			return nil, err
		}

		jobs = append(jobs, job)
	}

	return jobs, nil
}

func deleteFromScheduled(db QueryObject, id uuid.UUID) error {
	_, err := db.Exec(
		"DELETE FROM xwork_schedule WHERE id = $1",
		id,
	)
	return err
}

func listScheduled(db QueryObject, limit, offset uint) ([]*xwork.ScheduledJob, error) {
	if limit <= 0 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}

	rows, err := db.Query(
		`SELECT id, name, queue, payload, enqueue_at, scheduled_at
			FROM xwork_schedule
			ORDER BY enqueue_at DESC LIMIT $1 OFFSET $2`,
		limit, offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs := make([]*xwork.ScheduledJob, 0, limit)
	for rows.Next() {
		job := &xwork.ScheduledJob{}
		err = rows.Scan(&job.ID, &job.Name, &job.Queue, &job.Payload, &job.EnqueueAt, &job.ScheduledAt)
		if err != nil {
			return nil, err
		}

		jobs = append(jobs, job)
	}

	return jobs, nil
}

func insertToQueue(db QueryObject, job *xwork.EnqueuedJob) error {
	payload, err := job.Payload.Value()
	if err != nil {
		return err
	}
	result, err := db.Exec(
		"INSERT INTO xwork_queue (id, name, queue, payload, retry_count, enqueued_at, scheduled_at) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (id) DO NOTHING",
		job.ID, job.Name, job.Queue, payload, job.RetryCount, job.EnqueuedAt, job.ScheduledAt,
	)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return ErrAlreadyEnqueued
	}
	return nil
}

func getFromQueue(db QueryObject, queue string) (*xwork.EnqueuedJob, error) {
	row := db.QueryRow(
		"SELECT id, name, queue, payload, retry_count, enqueued_at, scheduled_at FROM xwork_queue WHERE queue = $1 ORDER BY enqueued_at ASC LIMIT 1",
		queue,
	)

	job := &xwork.EnqueuedJob{}
	err := row.Scan(&job.ID, &job.Name, &job.Queue, &job.Payload, &job.RetryCount, &job.EnqueuedAt, &job.ScheduledAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		return nil, err
	}

	return job, nil
}

func deleteFromQueue(db QueryObject, queue string) (*xwork.EnqueuedJob, error) {
	row := db.QueryRow(
		`DELETE FROM xwork_queue
			WHERE id = (
				SELECT id FROM xwork_queue
				WHERE queue = $1
				ORDER BY enqueued_at ASC, id ASC
				LIMIT 1
			)
			RETURNING id, name, queue, payload, retry_count, enqueued_at, scheduled_at`,
		queue,
	)

	job := &xwork.EnqueuedJob{}
	err := row.Scan(&job.ID, &job.Name, &job.Queue, &job.Payload, &job.RetryCount, &job.EnqueuedAt, &job.ScheduledAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return job, nil
}

func listEnqueued(db QueryObject, queue string, limit, offset uint) ([]*xwork.EnqueuedJob, error) {
	if limit <= 0 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}

	rows, err := db.Query(
		`SELECT id, name, queue, payload, retry_count, enqueued_at, scheduled_at
			FROM xwork_queue
			WHERE queue = $1
			ORDER BY enqueued_at ASC LIMIT $2 OFFSET $3`,
		queue, limit, offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs := make([]*xwork.EnqueuedJob, 0, limit)
	for rows.Next() {
		job := &xwork.EnqueuedJob{}
		err = rows.Scan(&job.ID, &job.Name, &job.Queue, &job.Payload, &job.RetryCount, &job.EnqueuedAt, &job.ScheduledAt)
		if err != nil {
			return nil, err
		}

		jobs = append(jobs, job)
	}

	return jobs, nil
}

func insertToProcessing(db QueryObject, job *xwork.ProcessingJob) error {
	payload, err := job.Payload.Value()
	if err != nil {
		return err
	}

	_, err = db.Exec(
		"INSERT INTO xwork_processing (id, name, queue, payload, retry_count, started_at, enqueued_at, scheduled_at, last_heartbeat_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
		job.ID, job.Name, job.Queue, payload, job.RetryCount, job.StartedAt, job.EnqueuedAt, job.ScheduledAt, job.StartedAt,
	)
	return err

}

func emitHeartbeat(db QueryObject, job *xwork.ProcessingJob) error {
	_, err := db.Exec("UPDATE xwork_processing SET last_heartbeat_at = $1 WHERE id = $2", time.Now(), job.ID)
	return err
}

func getByLastHeartbeatBefore(db QueryObject, lastHeartbeatBefore time.Time) ([]*xwork.ProcessingJob, error) {
	rows, err := db.Query(
		"SELECT id, name, queue, payload, retry_count, started_at, enqueued_at, scheduled_at FROM xwork_processing WHERE last_heartbeat_at < $1",
		lastHeartbeatBefore,
	)
	if err != nil {
		return nil, err
	}

	jobs := make([]*xwork.ProcessingJob, 0)
	for rows.Next() {
		job := &xwork.ProcessingJob{}
		err = rows.Scan(&job.ID, &job.Name, &job.Queue, &job.Payload, &job.RetryCount, &job.StartedAt, &job.EnqueuedAt, &job.ScheduledAt)
		if err != nil {
			return nil, err
		}

		jobs = append(jobs, job)
	}

	return jobs, nil
}

func deleteFromProcessing(db QueryObject, id uuid.UUID) error {
	_, err := db.Exec(
		"DELETE FROM xwork_processing WHERE id = $1",
		id,
	)
	return err
}

func listProcessing(db QueryObject, limit, offset uint) ([]*xwork.ProcessingJob, error) {
	if limit <= 0 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}

	rows, err := db.Query(
		`SELECT id, name, queue, payload, retry_count, started_at, enqueued_at, scheduled_at
			FROM xwork_processing
			ORDER BY enqueued_at DESC LIMIT $1 OFFSET $2`,
		limit, offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs := make([]*xwork.ProcessingJob, 0, limit)
	for rows.Next() {
		job := &xwork.ProcessingJob{}
		err = rows.Scan(&job.ID, &job.Name, &job.Queue, &job.Payload, &job.RetryCount, &job.StartedAt, &job.EnqueuedAt, &job.ScheduledAt)
		if err != nil {
			return nil, err
		}

		jobs = append(jobs, job)
	}

	return jobs, nil
}

func insertToProcessed(db QueryObject, job *xwork.ProcessedJob) error {
	payload, err := job.Payload.Value()
	if err != nil {
		return err
	}

	_, err = db.Exec(
		"INSERT INTO xwork_processed (id, name, queue, payload, started_at, completed_at, enqueued_at, scheduled_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
		job.ID, job.Name, job.Queue, payload, job.StartedAt, job.CompletedAt, job.EnqueuedAt, job.ScheduledAt,
	)
	return err
}

func listProcessed(db QueryObject, limit, offset uint) ([]*xwork.ProcessedJob, error) {
	if limit <= 0 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}

	rows, err := db.Query(
		`SELECT id, name, queue, payload, started_at, completed_at, enqueued_at, scheduled_at
			FROM xwork_processed
			ORDER BY completed_at DESC, id ASC LIMIT $1 OFFSET $2`,
		limit, offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs := make([]*xwork.ProcessedJob, 0, limit)
	for rows.Next() {
		job := &xwork.ProcessedJob{}
		err = rows.Scan(&job.ID, &job.Name, &job.Queue, &job.Payload, &job.StartedAt, &job.CompletedAt, &job.EnqueuedAt, &job.ScheduledAt)
		if err != nil {
			return nil, err
		}

		jobs = append(jobs, job)
	}

	return jobs, nil
}

func insertToFailed(db QueryObject, job *xwork.FailedJob) error {
	payload, err := job.Payload.Value()
	if err != nil {
		return err
	}

	_, err = db.Exec(
		"INSERT INTO xwork_failed (id, name, queue, payload, error, last_retry_at, next_retry_at, retry_count, scheduled_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
		job.ID, job.Name, job.Queue, payload, job.Error, job.LastRetryAt, job.NextRetryAt, job.RetryCount, job.ScheduledAt,
	)
	return err
}

func nextFromFailed(db QueryObject) ([]*xwork.FailedJob, error) {
	rows, err := db.Query(
		`SELECT id, name, queue, payload, error, last_retry_at, next_retry_at, retry_count, scheduled_at
			FROM xwork_failed
			WHERE next_retry_at <= $1`,
		time.Now(),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs := make([]*xwork.FailedJob, 0)
	for rows.Next() {
		job := &xwork.FailedJob{}
		err = rows.Scan(&job.ID, &job.Name, &job.Queue, &job.Payload, &job.Error, &job.LastRetryAt, &job.NextRetryAt, &job.RetryCount, &job.ScheduledAt)
		if err != nil {
			return nil, err
		}

		jobs = append(jobs, job)
	}

	return jobs, nil

}

func deleteFromFailed(db QueryObject, id uuid.UUID) error {
	_, err := db.Exec(
		"DELETE FROM xwork_failed WHERE id = $1",
		id,
	)
	return err
}

func getFailed(db QueryObject, id uuid.UUID) (*xwork.FailedJob, error) {
	row := db.QueryRow(
		`SELECT id, name, queue, payload, error, last_retry_at, next_retry_at, retry_count, scheduled_at
			FROM xwork_failed
			WHERE id = $1`,
		id,
	)

	job := &xwork.FailedJob{}
	err := row.Scan(&job.ID, &job.Name, &job.Queue, &job.Payload, &job.Error, &job.LastRetryAt, &job.NextRetryAt, &job.RetryCount, &job.ScheduledAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return job, nil
}

func claimFailed(db QueryObject, id uuid.UUID) (*xwork.FailedJob, error) {
	row := db.QueryRow(
		`DELETE FROM xwork_failed
			WHERE id = $1
			RETURNING id, name, queue, payload, error, last_retry_at, next_retry_at, retry_count, scheduled_at`,
		id,
	)

	job := &xwork.FailedJob{}
	err := row.Scan(&job.ID, &job.Name, &job.Queue, &job.Payload, &job.Error, &job.LastRetryAt, &job.NextRetryAt, &job.RetryCount, &job.ScheduledAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return job, nil
}

func listFailed(db QueryObject, limit, offset uint) ([]*xwork.FailedJob, error) {
	rows, err := db.Query(
		`SELECT id, name, queue, payload, error, last_retry_at, next_retry_at, retry_count, scheduled_at
			FROM xwork_failed
			ORDER BY next_retry_at DESC LIMIT $1 OFFSET $2`,
		limit, offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs := make([]*xwork.FailedJob, 0, limit)
	for rows.Next() {
		job := &xwork.FailedJob{}
		err = rows.Scan(&job.ID, &job.Name, &job.Queue, &job.Payload, &job.Error, &job.LastRetryAt, &job.NextRetryAt, &job.RetryCount, &job.ScheduledAt)
		if err != nil {
			return nil, err
		}

		jobs = append(jobs, job)
	}

	return jobs, nil

}

func listJobs(db QueryObject, jobType xwork.JobType, jobQuery xwork.JobQuery) (any, bool, error) {
	if jobType == xwork.JobTypeEnqueued && jobQuery.Queue == "" && !jobQuery.AllQueues {
		jobQuery.Queue = xwork.DefaultQueue
	}

	var table, columns, orderBy string
	switch jobType {
	case xwork.JobTypeScheduled:
		table = "xwork_schedule"
		columns = "id, name, queue, payload, enqueue_at, scheduled_at"
		orderBy = "enqueue_at DESC, id ASC"
	case xwork.JobTypeEnqueued:
		table = "xwork_queue"
		columns = "id, name, queue, payload, retry_count, enqueued_at, scheduled_at"
		orderBy = "enqueued_at ASC, id ASC"
	case xwork.JobTypeProcessing:
		table = "xwork_processing"
		columns = "id, name, queue, payload, retry_count, started_at, enqueued_at, scheduled_at"
		orderBy = "enqueued_at DESC, id ASC"
	case xwork.JobTypeProcessed:
		table = "xwork_processed"
		columns = "id, name, queue, payload, started_at, completed_at, enqueued_at, scheduled_at"
		orderBy = "completed_at DESC, id ASC"
	case xwork.JobTypeFailed:
		table = "xwork_failed"
		columns = "id, name, queue, payload, error, last_retry_at, next_retry_at, retry_count, scheduled_at"
		orderBy = "next_retry_at DESC, id ASC"
	default:
		return nil, false, errors.New("unknown job type")
	}

	args := make([]interface{}, 0, 4)
	conditions := make([]string, 0, 2)
	if jobQuery.Queue != "" {
		args = append(args, jobQuery.Queue)
		conditions = append(conditions, fmt.Sprintf("queue = $%d", len(args)))
	}
	if query := strings.ToLower(strings.TrimSpace(jobQuery.Query)); query != "" {
		query = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(query)
		args = append(args, "%"+query+"%")
		placeholder := fmt.Sprintf("$%d", len(args))
		conditions = append(conditions, fmt.Sprintf("(LOWER(name) LIKE %s ESCAPE '\\' OR LOWER(CAST(id AS TEXT)) LIKE %s ESCAPE '\\')", placeholder, placeholder))
	}

	query := fmt.Sprintf("SELECT %s FROM %s", columns, table)
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	query += " ORDER BY " + orderBy

	limit := normalizeJobQueryLimit(jobQuery.Limit)
	args = append(args, limit+1)
	limitPlaceholder := fmt.Sprintf("$%d", len(args))
	args = append(args, jobQuery.Offset)
	offsetPlaceholder := fmt.Sprintf("$%d", len(args))
	query += fmt.Sprintf(" LIMIT %s OFFSET %s", limitPlaceholder, offsetPlaceholder)

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	switch jobType {
	case xwork.JobTypeScheduled:
		return collectJobRows(rows, limit, func(rows *sql.Rows) (*xwork.ScheduledJob, error) {
			job := &xwork.ScheduledJob{}
			err := rows.Scan(&job.ID, &job.Name, &job.Queue, &job.Payload, &job.EnqueueAt, &job.ScheduledAt)
			return job, err
		})
	case xwork.JobTypeEnqueued:
		return collectJobRows(rows, limit, func(rows *sql.Rows) (*xwork.EnqueuedJob, error) {
			job := &xwork.EnqueuedJob{}
			err := rows.Scan(&job.ID, &job.Name, &job.Queue, &job.Payload, &job.RetryCount, &job.EnqueuedAt, &job.ScheduledAt)
			return job, err
		})
	case xwork.JobTypeProcessing:
		return collectJobRows(rows, limit, func(rows *sql.Rows) (*xwork.ProcessingJob, error) {
			job := &xwork.ProcessingJob{}
			err := rows.Scan(&job.ID, &job.Name, &job.Queue, &job.Payload, &job.RetryCount, &job.StartedAt, &job.EnqueuedAt, &job.ScheduledAt)
			return job, err
		})
	case xwork.JobTypeProcessed:
		return collectJobRows(rows, limit, func(rows *sql.Rows) (*xwork.ProcessedJob, error) {
			job := &xwork.ProcessedJob{}
			err := rows.Scan(&job.ID, &job.Name, &job.Queue, &job.Payload, &job.StartedAt, &job.CompletedAt, &job.EnqueuedAt, &job.ScheduledAt)
			return job, err
		})
	case xwork.JobTypeFailed:
		return collectJobRows(rows, limit, func(rows *sql.Rows) (*xwork.FailedJob, error) {
			job := &xwork.FailedJob{}
			err := rows.Scan(&job.ID, &job.Name, &job.Queue, &job.Payload, &job.Error, &job.LastRetryAt, &job.NextRetryAt, &job.RetryCount, &job.ScheduledAt)
			return job, err
		})
	default:
		panic("validated job type became invalid")
	}
}

func collectJobRows[T any](rows *sql.Rows, limit uint, scan func(*sql.Rows) (T, error)) ([]T, bool, error) {
	jobs := make([]T, 0, limit+1)
	for rows.Next() {
		job, err := scan(rows)
		if err != nil {
			return nil, false, err
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}

	hasMore := uint(len(jobs)) > limit
	if hasMore {
		jobs = jobs[:limit]
	}
	return jobs, hasMore, nil
}

func count(db QueryObject, jobType xwork.JobType) (int64, error) {
	var tableName string
	switch jobType {
	case xwork.JobTypeScheduled:
		tableName = "xwork_schedule"
	case xwork.JobTypeEnqueued:
		tableName = "xwork_queue"
	case xwork.JobTypeProcessing:
		tableName = "xwork_processing"
	case xwork.JobTypeProcessed:
		tableName = "xwork_processed"
	case xwork.JobTypeFailed:
		tableName = "xwork_failed"
	default:
		return 0, errors.New("unknown job type")
	}

	var count int64

	row := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName))
	err := row.Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s SQL) Transact(f func(adapter xwork.StorageAdapter) error) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	err = f(newSQL(s.db, tx))

	if err != nil {
		return errors.Join(err, tx.Rollback())
	}

	return tx.Commit()
}

func (s SQL) InsertToScheduled(job *xwork.ScheduledJob) error {
	return insertToScheduled(s.q, job)
}

func (s SQL) NextFromScheduled() ([]*xwork.ScheduledJob, error) {
	return nextFromScheduled(s.q)
}

func (s SQL) DeleteFromScheduled(id uuid.UUID) error {
	return deleteFromScheduled(s.q, id)
}

func (s SQL) ListScheduled(limit, offset uint) ([]*xwork.ScheduledJob, error) {
	return listScheduled(s.q, limit, offset)
}

func (s SQL) InsertToQueue(job *xwork.EnqueuedJob) error {
	return insertToQueue(s.q, job)
}

func (s SQL) GetFromQueue(queue string) (*xwork.EnqueuedJob, error) {
	return getFromQueue(s.q, queue)
}

func (s SQL) DeleteFromQueue(queue string) (*xwork.EnqueuedJob, error) {
	return deleteFromQueue(s.q, queue)
}

func (s SQL) ListEnqueued(queue string, limit, offset uint) ([]*xwork.EnqueuedJob, error) {
	return listEnqueued(s.q, queue, limit, offset)
}

func (s SQL) InsertToProcessing(job *xwork.ProcessingJob) error {
	return insertToProcessing(s.q, job)
}

func (s SQL) EmitHeartbeat(job *xwork.ProcessingJob) error {
	return emitHeartbeat(s.q, job)
}

func (s SQL) GetByLastHeartbeatBefore(timestamp time.Time) ([]*xwork.ProcessingJob, error) {
	return getByLastHeartbeatBefore(s.q, timestamp)
}

func (s SQL) DeleteFromProcessing(id uuid.UUID) error {
	return deleteFromProcessing(s.q, id)
}

func (s SQL) ListProcessing(limit, offset uint) ([]*xwork.ProcessingJob, error) {
	return listProcessing(s.q, limit, offset)
}

func (s SQL) InsertToProcessed(job *xwork.ProcessedJob) error {
	return insertToProcessed(s.q, job)
}

func (s SQL) ListProcessed(limit, offset uint) ([]*xwork.ProcessedJob, error) {
	return listProcessed(s.q, limit, offset)
}

func (s SQL) InsertToFailed(job *xwork.FailedJob) error {
	return insertToFailed(s.q, job)
}

func (s SQL) NextFromFailed() ([]*xwork.FailedJob, error) {
	return nextFromFailed(s.q)
}

func (s SQL) DeleteFromFailed(id uuid.UUID) error {
	return deleteFromFailed(s.q, id)
}

func (s SQL) GetFailed(id uuid.UUID) (*xwork.FailedJob, error) {
	return getFailed(s.q, id)
}

func (s SQL) ClaimFailed(id uuid.UUID) (*xwork.FailedJob, error) {
	return claimFailed(s.q, id)
}

func (s SQL) ListFailed(limit, offset uint) ([]*xwork.FailedJob, error) {
	return listFailed(s.q, limit, offset)
}

func (s SQL) ListJobs(jobType xwork.JobType, query xwork.JobQuery) (any, bool, error) {
	return listJobs(s.q, jobType, query)
}

func (s SQL) Count(jobType xwork.JobType) (int64, error) {
	return count(s.q, jobType)
}
