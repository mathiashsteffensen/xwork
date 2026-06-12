package storage_adapters

import (
	"database/sql"
	"errors"
	"time"

	"github.com/gofrs/uuid"
	"github.com/mathiashsteffensen/xwork"
)

type QueryObject interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

type SQLStorageAdapter struct {
	db *sql.DB
	q  QueryObject
}

func NewSQLStorageAdapter(db *sql.DB) SQLStorageAdapter {
	return newSQLStorageAdapter(db, db)
}

func newSQLStorageAdapter(db *sql.DB, q QueryObject) SQLStorageAdapter {
	return SQLStorageAdapter{db: db, q: q}

}

func (s SQLStorageAdapter) Initialize() error {
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
				scheduled_at TIMESTAMP NOT NULL
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
	_, err := db.Exec(
		"INSERT INTO xwork_schedule (id, name, queue, payload, enqueue_at, scheduled_at) VALUES ($1, $2, $3, $4, $5, $6)",
		job.ID, job.Name, job.Queue, job.Payload, job.EnqueueAt, job.ScheduledAt,
	)
	return err
}

func nextFromScheduled(db QueryObject) ([]*xwork.ScheduledJob, error) {
	rows, err := db.Query(
		`SELECT id, name, queue, payload, enqueue_at, scheduled_at
			FROM xwork_schedule
			WHERE enqueue_at <= $1
			FOR UPDATE`,
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

func insertToQueue(db QueryObject, job *xwork.EnqueuedJob) error {
	_, err := db.Exec(
		"INSERT INTO xwork_queue (id, name, queue, payload, retry_count, enqueued_at, scheduled_at) VALUES ($1, $2, $3, $4, $5, $6, $7)",
		job.ID, job.Name, job.Queue, job.Payload, job.RetryCount, job.EnqueuedAt, job.ScheduledAt,
	)
	return err
}

func getFromQueue(db QueryObject, queue string) (*xwork.EnqueuedJob, error) {
	row := db.QueryRow(
		"SELECT id, name, queue, payload, retry_count, enqueued_at, scheduled_at FROM xwork_queue WHERE queue = $1 ORDER BY enqueued_at ASC LIMIT 1 FOR UPDATE",
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

func deleteFromQueue(db QueryObject, id uuid.UUID) error {
	_, err := db.Exec(
		"DELETE FROM xwork_queue WHERE id = $1",
		id,
	)
	return err
}

func insertToProcessing(db QueryObject, job *xwork.ProcessingJob) error {
	_, err := db.Exec(
		"INSERT INTO xwork_processing (id, name, queue, payload, retry_count, started_at, enqueued_at, scheduled_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
		job.ID, job.Name, job.Queue, job.Payload, job.RetryCount, job.StartedAt, job.EnqueuedAt, job.ScheduledAt,
	)
	return err

}

func deleteFromProcessing(db QueryObject, id uuid.UUID) error {
	_, err := db.Exec(
		"DELETE FROM xwork_processing WHERE id = $1",
		id,
	)
	return err
}

func insertToProcessed(db QueryObject, job *xwork.ProcessedJob) error {
	_, err := db.Exec(
		"INSERT INTO xwork_processed (id, name, queue, payload, started_at, completed_at, enqueued_at, scheduled_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
		job.ID, job.Name, job.Queue, job.Payload, job.StartedAt, job.CompletedAt, job.EnqueuedAt, job.ScheduledAt,
	)
	return err
}

func insertToFailed(db QueryObject, job *xwork.FailedJob) error {
	_, err := db.Exec(
		"INSERT INTO xwork_failed (id, name, queue, payload, error, last_retry_at, next_retry_at, retry_count, scheduled_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
		job.ID, job.Name, job.Queue, job.Payload, job.Error, job.LastRetryAt, job.NextRetryAt, job.RetryCount, job.ScheduledAt,
	)
	return err
}

func nextFromFailed(db QueryObject) ([]*xwork.FailedJob, error) {
	rows, err := db.Query(
		`SELECT id, name, queue, payload, error, last_retry_at, next_retry_at, retry_count, scheduled_at
			FROM xwork_failed
			WHERE next_retry_at <= $1
			FOR UPDATE`,
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

func (s SQLStorageAdapter) Transact(f func(adapter xwork.StorageAdapter) error) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	err = f(newSQLStorageAdapter(s.db, tx))

	if err != nil {
		return errors.Join(err, tx.Rollback())
	}

	return tx.Commit()
}

func (s SQLStorageAdapter) InsertToScheduled(job *xwork.ScheduledJob) error {
	return insertToScheduled(s.q, job)
}

func (s SQLStorageAdapter) NextFromScheduled() ([]*xwork.ScheduledJob, error) {
	return nextFromScheduled(s.q)
}

func (s SQLStorageAdapter) DeleteFromScheduled(id uuid.UUID) error {
	return deleteFromScheduled(s.q, id)
}

func (s SQLStorageAdapter) InsertToQueue(job *xwork.EnqueuedJob) error {
	return insertToQueue(s.q, job)
}

func (s SQLStorageAdapter) GetFromQueue(queue string) (*xwork.EnqueuedJob, error) {
	return getFromQueue(s.q, queue)
}

func (s SQLStorageAdapter) DeleteFromQueue(id uuid.UUID) error {
	return deleteFromQueue(s.q, id)
}

func (s SQLStorageAdapter) InsertToProcessing(job *xwork.ProcessingJob) error {
	return insertToProcessing(s.q, job)
}

func (s SQLStorageAdapter) DeleteFromProcessing(id uuid.UUID) error {
	return deleteFromProcessing(s.q, id)
}

func (s SQLStorageAdapter) InsertToProcessed(job *xwork.ProcessedJob) error {
	return insertToProcessed(s.q, job)
}

func (s SQLStorageAdapter) InsertToFailed(job *xwork.FailedJob) error {
	return insertToFailed(s.q, job)
}

func (s SQLStorageAdapter) NextFromFailed() ([]*xwork.FailedJob, error) {
	return nextFromFailed(s.q)
}

func (s SQLStorageAdapter) DeleteFromFailed(id uuid.UUID) error {
	return deleteFromFailed(s.q, id)
}
