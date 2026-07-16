package storage

import (
	"errors"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/mathiashsteffensen/xwork/v2"
)

func TestMemoryGetFromQueueReturnsOldestJobForQueue(t *testing.T) {
	store := NewMemory()
	now := time.Now()

	firstID := newTestUUID(t)
	secondID := newTestUUID(t)

	err := store.InsertToQueue(&xwork.EnqueuedJob{
		ID:          secondID,
		Name:        "newer",
		Queue:       "default",
		EnqueuedAt:  now.Add(time.Minute),
		ScheduledAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = store.InsertToQueue(&xwork.EnqueuedJob{
		ID:          firstID,
		Name:        "older",
		Queue:       "default",
		EnqueuedAt:  now,
		ScheduledAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}

	job, err := store.GetFromQueue("default")
	if err != nil {
		t.Fatal(err)
	}
	if job == nil {
		t.Fatal("expected job")
	}
	if job.ID != firstID {
		t.Fatalf("expected oldest job %s, got %s", firstID, job.ID)
	}
}

func TestMemoryTransactRollsBackOnError(t *testing.T) {
	store := NewMemory()
	id := newTestUUID(t)
	expectedErr := errors.New("rollback")

	err := store.Transact(func(adapter xwork.StorageAdapter) error {
		err := adapter.InsertToQueue(&xwork.EnqueuedJob{
			ID:          id,
			Name:        "job",
			Queue:       "default",
			EnqueuedAt:  time.Now(),
			ScheduledAt: time.Now(),
		})
		if err != nil {
			return err
		}
		return expectedErr
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected rollback error, got %v", err)
	}

	count, err := store.Count(xwork.JobTypeEnqueued)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("expected no enqueued jobs after rollback, got %d", count)
	}
}

func TestMemoryListJobsFiltersSortsAndPaginates(t *testing.T) {
	store := NewMemory()
	now := time.Now()
	oldest := &xwork.ProcessingJob{
		ID: newTestUUID(t), Name: "reports.generate", Queue: "critical",
		StartedAt: now.Add(-time.Hour), EnqueuedAt: now.Add(-2 * time.Hour), ScheduledAt: now.Add(-3 * time.Hour),
	}
	newer := &xwork.ProcessingJob{
		ID: newTestUUID(t), Name: "reports.generate", Queue: "critical",
		StartedAt: now.Add(-time.Minute), EnqueuedAt: now.Add(-2 * time.Minute), ScheduledAt: now.Add(-3 * time.Minute),
	}
	otherQueue := &xwork.ProcessingJob{
		ID: newTestUUID(t), Name: "reports.generate", Queue: "default",
		StartedAt: now.Add(-2 * time.Hour), EnqueuedAt: now.Add(-3 * time.Hour), ScheduledAt: now.Add(-4 * time.Hour),
	}
	for _, job := range []*xwork.ProcessingJob{newer, otherQueue, oldest} {
		if err := store.InsertToProcessing(job); err != nil {
			t.Fatal(err)
		}
	}

	result, hasMore, err := store.ListJobs(xwork.JobTypeProcessing, xwork.JobQuery{
		Query: "REPORTS", Queue: "critical", Limit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	jobs, ok := result.([]*xwork.ProcessingJob)
	if !ok {
		t.Fatalf("expected processing jobs, got %T", result)
	}
	if len(jobs) != 1 || jobs[0].ID != newer.ID {
		t.Fatalf("expected legacy newest-enqueued ordering, got %+v", jobs)
	}
	if !hasMore {
		t.Fatal("expected another matching result")
	}

	result, hasMore, err = store.ListJobs(xwork.JobTypeProcessing, xwork.JobQuery{
		Query: oldest.ID.String()[0:8], Queue: "critical", Limit: 25,
	})
	if err != nil {
		t.Fatal(err)
	}
	jobs = result.([]*xwork.ProcessingJob)
	if len(jobs) != 1 || jobs[0].ID != oldest.ID || hasMore {
		t.Fatalf("expected partial ID search to return one job, got %+v, hasMore=%v", jobs, hasMore)
	}
}

func TestMemoryListsMostRecentlyCompletedJobsFirst(t *testing.T) {
	store := NewMemory()
	now := time.Now()
	older := &xwork.ProcessedJob{
		ID: newTestUUID(t), Name: "older", Queue: "default",
		StartedAt: now.Add(-time.Hour), CompletedAt: now.Add(-time.Minute), EnqueuedAt: now, ScheduledAt: now,
	}
	newer := &xwork.ProcessedJob{
		ID: newTestUUID(t), Name: "newer", Queue: "default",
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

func TestMemoryGetFailedReturnsClone(t *testing.T) {
	store := NewMemory()
	job := &xwork.FailedJob{
		ID: newTestUUID(t), Name: "job", Queue: "default",
		Payload: xwork.JobPayload{"nested": "value"}, NextRetryAt: time.Now(), ScheduledAt: time.Now(),
	}
	if err := store.InsertToFailed(job); err != nil {
		t.Fatal(err)
	}

	found, err := store.GetFailed(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if found == nil || found.ID != job.ID {
		t.Fatalf("expected failed job, got %+v", found)
	}
	found.Payload["nested"] = "changed"
	again, err := store.GetFailed(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if again.Payload["nested"] != "value" {
		t.Fatal("expected lookup to isolate stored payload")
	}
}

func TestMemoryClaimFailedIsAtomic(t *testing.T) {
	store := NewMemory()
	job := &xwork.FailedJob{
		ID: newTestUUID(t), Name: "job", Queue: "default", Payload: xwork.JobPayload{},
		NextRetryAt: time.Now(), ScheduledAt: time.Now(),
	}
	if err := store.InsertToFailed(job); err != nil {
		t.Fatal(err)
	}

	const callers = 16
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
			if result.job.ID != job.ID {
				t.Fatalf("claimed wrong job: %+v", result.job)
			}
		}
	}
	if claimedCount != 1 {
		t.Fatalf("expected exactly one successful claim, got %d", claimedCount)
	}
	if found, err := store.GetFailed(job.ID); err != nil || found != nil {
		t.Fatalf("expected claimed job to be removed, got job=%v err=%v", found, err)
	}
}

func newTestUUID(t *testing.T) uuid.UUID {
	t.Helper()

	id, err := uuid.NewV4()
	if err != nil {
		t.Fatal(err)
	}
	return id
}
