package storage

import (
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/mathiashsteffensen/xwork/v2"
	_ "github.com/mattn/go-sqlite3"
)

func TestSQLListJobsAndFailedLookup(t *testing.T) {
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "xwork.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewSQL(db)
	if err := store.Initialize(); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	first := &xwork.FailedJob{
		ID: newTestUUID(t), Name: "billing.charge", Queue: "critical", Payload: xwork.JobPayload{"invoice": "one"},
		Error: `{"message":"declined"}`, RetryCount: 2, LastRetryAt: now.Add(-time.Minute),
		NextRetryAt: now.Add(time.Minute), ScheduledAt: now.Add(-time.Hour),
	}
	second := &xwork.FailedJob{
		ID: newTestUUID(t), Name: "billing.charge", Queue: "critical", Payload: xwork.JobPayload{"invoice": "two"},
		Error: `{"message":"timeout"}`, RetryCount: 3, LastRetryAt: now,
		NextRetryAt: now.Add(2 * time.Minute), ScheduledAt: now.Add(-2 * time.Hour),
	}
	other := &xwork.FailedJob{
		ID: newTestUUID(t), Name: "billing.charge", Queue: "default", Payload: xwork.JobPayload{},
		Error: `{}`, LastRetryAt: now, NextRetryAt: now, ScheduledAt: now,
	}
	for _, job := range []*xwork.FailedJob{second, other, first} {
		if err := store.InsertToFailed(job); err != nil {
			t.Fatal(err)
		}
	}

	result, hasMore, err := store.ListJobs(xwork.JobTypeFailed, xwork.JobQuery{
		Query: "BILLING", Queue: "critical", Limit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	jobs, ok := result.([]*xwork.FailedJob)
	if !ok {
		t.Fatalf("expected failed jobs, got %T", result)
	}
	if len(jobs) != 1 || jobs[0].ID != second.ID {
		t.Fatalf("expected legacy latest-retry ordering, got %+v", jobs)
	}
	if !hasMore {
		t.Fatal("expected lookahead to report another result")
	}

	result, hasMore, err = store.ListJobs(xwork.JobTypeFailed, xwork.JobQuery{
		Query: second.ID.String()[0:8], Limit: 25,
	})
	if err != nil {
		t.Fatal(err)
	}
	jobs = result.([]*xwork.FailedJob)
	if len(jobs) != 1 || jobs[0].ID != second.ID || hasMore {
		t.Fatalf("expected ID search to return one row, got %+v, hasMore=%v", jobs, hasMore)
	}

	found, err := store.GetFailed(first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if found == nil || found.ID != first.ID || found.RetryCount != first.RetryCount {
		t.Fatalf("unexpected failed lookup: %+v", found)
	}
	missing, err := store.GetFailed(newTestUUID(t))
	if err != nil || missing != nil {
		t.Fatalf("expected missing lookup, got job=%v err=%v", missing, err)
	}
}

func TestSQLClaimFailedIsAtomic(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "xwork.db") + "?_busy_timeout=5000&_journal_mode=WAL"
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(16)

	store := NewSQL(db)
	if err := store.Initialize(); err != nil {
		t.Fatal(err)
	}
	job := &xwork.FailedJob{
		ID: newTestUUID(t), Name: "job", Queue: "default", Payload: xwork.JobPayload{}, Error: `{}`,
		LastRetryAt: time.Now(), NextRetryAt: time.Now(), ScheduledAt: time.Now(),
	}
	if err := store.InsertToFailed(job); err != nil {
		t.Fatal(err)
	}

	const callers = 8
	type claimResult struct {
		job *xwork.FailedJob
		err error
	}
	start := make(chan struct{})
	results := make(chan claimResult, callers)
	for range callers {
		go func() {
			<-start
			claimed, err := store.ClaimFailed(job.ID)
			results <- claimResult{job: claimed, err: err}
		}()
	}
	close(start)

	claimedCount := 0
	for range callers {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.job != nil {
			claimedCount++
		}
	}
	if claimedCount != 1 {
		t.Fatalf("expected exactly one successful claim, got %d", claimedCount)
	}
}

func TestSQLListsMostRecentlyCompletedJobsFirst(t *testing.T) {
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "xwork.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewSQL(db)
	if err := store.Initialize(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	older := &xwork.ProcessedJob{
		ID: newTestUUID(t), Name: "older", Queue: "default", Payload: xwork.JobPayload{},
		StartedAt: now.Add(-time.Hour), CompletedAt: now.Add(-time.Minute), EnqueuedAt: now, ScheduledAt: now,
	}
	newer := &xwork.ProcessedJob{
		ID: newTestUUID(t), Name: "newer", Queue: "default", Payload: xwork.JobPayload{},
		StartedAt: now.Add(-time.Hour), CompletedAt: now, EnqueuedAt: now.Add(-time.Hour), ScheduledAt: now,
	}
	for _, job := range []*xwork.ProcessedJob{older, newer} {
		if err := store.InsertToProcessed(job); err != nil {
			t.Fatal(err)
		}
	}

	jobs, err := store.ListProcessed(25, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 2 || jobs[0].ID != newer.ID {
		t.Fatalf("expected newest completion first, got %+v", jobs)
	}

	result, _, err := store.ListJobs(xwork.JobTypeProcessed, xwork.JobQuery{Limit: 25})
	if err != nil {
		t.Fatal(err)
	}
	queried := result.([]*xwork.ProcessedJob)
	if len(queried) != 2 || queried[0].ID != newer.ID {
		t.Fatalf("expected dashboard query newest completion first, got %+v", queried)
	}
}

func TestSQLClaimFailedRollsBackWithTransaction(t *testing.T) {
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "xwork.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewSQL(db)
	if err := store.Initialize(); err != nil {
		t.Fatal(err)
	}
	job := &xwork.FailedJob{
		ID: newTestUUID(t), Name: "job", Queue: "default", Payload: xwork.JobPayload{}, Error: `{}`,
		LastRetryAt: time.Now(), NextRetryAt: time.Now(), ScheduledAt: time.Now(),
	}
	if err := store.InsertToFailed(job); err != nil {
		t.Fatal(err)
	}

	expectedErr := errors.New("rollback")
	err = store.Transact(func(adapter xwork.StorageAdapter) error {
		claimer, ok := adapter.(xwork.FailedJobClaimer)
		if !ok {
			return errors.New("transaction adapter does not implement FailedJobClaimer")
		}
		claimed, err := claimer.ClaimFailed(job.ID)
		if err != nil {
			return err
		}
		if claimed == nil || claimed.ID != job.ID {
			return errors.New("transaction claimed the wrong job")
		}
		return expectedErr
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected rollback error, got %v", err)
	}
	found, err := store.GetFailed(job.ID)
	if err != nil || found == nil {
		t.Fatalf("expected rollback to restore claimed job, got job=%v err=%v", found, err)
	}
}

func TestSQLRetryDropsFailedJobAlreadyInQueue(t *testing.T) {
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "xwork.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewSQL(db)
	if err := store.Initialize(); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	id := newTestUUID(t)
	queued := &xwork.EnqueuedJob{
		ID: id, Name: "queued", Queue: "default", Payload: xwork.JobPayload{"source": "queue"},
		EnqueuedAt: now, ScheduledAt: now,
	}
	failed := &xwork.FailedJob{
		ID: id, Name: "failed", Queue: "default", Payload: xwork.JobPayload{"source": "failed"},
		Error: `{}`, LastRetryAt: now, NextRetryAt: now, ScheduledAt: now,
	}
	if err := store.InsertToQueue(queued); err != nil {
		t.Fatal(err)
	}
	if err := store.InsertToFailed(failed); err != nil {
		t.Fatal(err)
	}

	err = store.Transact(func(adapter xwork.StorageAdapter) error {
		claimed, err := adapter.(xwork.FailedJobClaimer).ClaimFailed(id)
		if err != nil {
			return err
		}
		retry := &xwork.EnqueuedJob{
			ID: claimed.ID, Name: claimed.Name, Queue: claimed.Queue, Payload: claimed.Payload,
			EnqueuedAt: now, ScheduledAt: claimed.ScheduledAt,
		}
		err = adapter.InsertToQueue(retry)
		if errors.Is(err, ErrAlreadyEnqueued) {
			return nil
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if found, err := store.GetFailed(id); err != nil || found != nil {
		t.Fatalf("expected duplicate failed job to be removed, got job=%v err=%v", found, err)
	}
	found, err := store.GetFromQueue("default")
	if err != nil {
		t.Fatal(err)
	}
	if found == nil || found.Name != queued.Name || found.Payload["source"] != queued.Payload["source"] {
		t.Fatalf("expected existing queued job to remain unchanged, got %+v", found)
	}
}
